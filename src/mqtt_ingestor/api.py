import os
import time
import threading
import queue

from mqtt_ingestor.storage import mongodb, base, postgres, sqlalchemy, noop
from mqtt_ingestor.mqtt import create_client, DocumentPayload
from mqtt_ingestor.model import DocumentPayload
from mqtt_ingestor.logger import get_logger
from mqtt_ingestor.filter import load_filter, DocumentPayloadFilter


class MqttIngestor:

    storage: base.BaseStorage | None = None

    def __init__(self):

        self.logger = get_logger(__name__)

        self.msg_queue = queue.Queue(maxsize=1000)  # Buffer up to 1000 messages
        self.last_arrival_ts = time.time()  # Initialize with start time
        self.exit_event = threading.Event()

        # detect if a message has arrived within the timeout or exit
        self.WATCHDOG_TIMEOUT = int(os.getenv("WATCHDOG_TIMEOUT", 60))

        self.MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt")
        self.MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
        self.MQTT_USER = os.getenv("MQTT_USER", "mqtt_user")
        self.MQTT_PASS = os.getenv("MQTT_PASS", "secretpass")
        self.MQTT_TRANSPORT = os.getenv("MQTT_TRANSPORT", "tcp")
        self.MQTT_TLS = os.getenv("MQTT_TLS", "0") == "1"
        self.MQTT_TOPICS = os.getenv("MQTT_TOPICS", "#")
        self.MQTT_IGNORE_CERTS = os.getenv("MQTT_IGNORE_CERTS", "false")
        self.MQTT_FILTER = os.getenv("MQTT_FILTER", None)

        self.STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "postgres").strip().lower()

        # postgres
        self.POSTGRES_DSN = os.getenv(
            "POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/mqtt"
        )
        self.POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "mqtt_messages")
        self.POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")

        # sqlalchemy
        self.SQLALCHEMY_DSN = os.getenv(
            "SQLALCHEMY_DSN",
            "postgresql+psycopg2://postgres:postgres@postgres:5432/mqtt",
        )
        self.SQLALCHEMY_SCHEMA = os.getenv("SQLALCHEMY_SCHEMA", "public")
        self.SQLALCHEMY_TABLE = os.getenv("SQLALCHEMY_TABLE", "mqtt_messages")

        # mongodb
        self.MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
        self.MONGO_DB = os.getenv("MONGO_DB", "mqtt_data")
        self.MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "data")

    def get_storage(self) -> base.BaseStorage | None:

        if self.storage:
            return self.storage

        backend = self.STORAGE_BACKEND
        self.logger.info(f"Creating {backend} storage")

        try:

            if "noop" in backend:
                self.storage = noop.NoopStorage()
            elif "postgre" in backend or "pg" in backend:
                if not self.POSTGRES_DSN or not self.POSTGRES_TABLE:
                    raise Exception(
                        f"{backend} backend requires env POSTGRES_DSN, POSTGRES_TABLE"
                    )
                self.storage = postgres.PostgresStorage(
                    dsn=self.POSTGRES_DSN,
                    table=self.POSTGRES_TABLE,
                    schema=self.POSTGRES_SCHEMA,
                )
            elif "sqlalchemy" in backend:
                if not self.SQLALCHEMY_DSN or not self.SQLALCHEMY_TABLE:
                    raise Exception(
                        f"{backend} backend requires env SQLALCHEMY_DSN, SQLALCHEMY_TABLE"
                    )
                self.storage = sqlalchemy.SQLAlchemyStorage(
                    dsn=self.SQLALCHEMY_DSN,
                    table=self.SQLALCHEMY_TABLE,
                    schema=self.SQLALCHEMY_SCHEMA,
                )
            else:
                if not self.MONGO_URI or not self.MONGO_DB or not self.MONGO_COLLECTION:
                    raise Exception(
                        f"{backend} backend requres env MONGO_URI, MONGO_DB, MONGO_COLLECTION"
                    )
                self.storage = mongodb.MongoStorage(
                    mongo_uri=self.MONGO_URI,
                    collection_name=self.MONGO_COLLECTION,
                    db_name=self.MONGO_DB,
                )
        except Exception as e:
            self.logger.error(f"Connection to {backend} failed: {e}")
            return None

        return self.storage

    def _worker(self):
        """Background thread to process database saves."""

        storage = self.get_storage()
        if not storage:
            self.logger.error("Worker: Failed to connect to storage")
            return

        while not self.exit_event.is_set():
            try:
                # Block for a short time to check exit_event periodically
                document = self.msg_queue.get(timeout=1.0)
                storage.save(document)
                self.logger.debug(f"Saved record from {document.topic}")
                self.msg_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Worker: Failed to save message: {e}")
                # If DB is down, we might want to exit to trigger container restart
                self.exit_event.set()
                os._exit(1)

    def _watchdog(self):
        """Monitor time since last message."""
        while not self.exit_event.is_set():
            time.sleep(10)
            seconds_since_last = time.time() - self.last_arrival_ts

            if seconds_since_last > self.WATCHDOG_TIMEOUT:
                self.logger.critical(
                    f"No messages received for {seconds_since_last:.1f}s. Exiting."
                )
                self.exit_event.set()
                os._exit(1)

    def start(self):

        # Start the decoupled worker and health monitor
        threading.Thread(target=self._worker, daemon=True).start()
        threading.Thread(target=self._watchdog, daemon=True).start()

        storage = self.get_storage()
        filter: DocumentPayloadFilter | None = (
            load_filter(self.MQTT_FILTER) if self.MQTT_FILTER else None
        )

        if not storage:
            self.logger.warning("Failed to connect to storage")
            return

        def on_document(document: DocumentPayload):
            # Track last message arrival
            self.last_arrival_ts = time.time()
            self.logger.debug(f"Got broker message")

            try:
                if filter:
                    keep_document = filter(document)
                    if not keep_document:
                        self.logger.debug(
                            f"Skip record due to filter from {document.topic}"
                        )
                        return

                try:
                    self.logger.debug(f"Add message to processing queue")
                    self.msg_queue.put(document, block=False)
                except queue.Full:
                    self.logger.warning("Queue full, dropping message")

            except Exception as e:
                self.logger.error(f"Failed to save message: {e}")

        client = create_client(
            on_document,
            mqtt_user=self.MQTT_USER,
            mqtt_pass=self.MQTT_PASS,
            mqtt_transport=self.MQTT_TRANSPORT,
            mqtt_tls=self.MQTT_TLS,
            mqtt_topics=self.MQTT_TOPICS,
            mqtt_ignore_certs=self.MQTT_IGNORE_CERTS,
        )

        try:

            user = f"{self.MQTT_USER}@" if self.MQTT_USER else ""
            self.logger.debug(
                f"MQTT connecting to {self.MQTT_TRANSPORT}://{user}{self.MQTT_BROKER}:{self.MQTT_PORT}"
            )
            client.connect(self.MQTT_BROKER, self.MQTT_PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
        finally:
            self.exit_event.set()
            storage.close()
            self.logger.info("Exit")
