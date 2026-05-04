import os
import time
import threading
import queue

from mqtt_ingestor.storage import mongodb, base, postgres, sqlalchemy, noop, jsonl
from mqtt_ingestor.mqtt import create_client
from mqtt_ingestor.model import DocumentPayload
from mqtt_ingestor.logger import get_logger
from mqtt_ingestor.filter import load_filter, DocumentPayloadFilter
from mqtt_ingestor.settings import settings


class MqttIngestor:

    storage: base.BaseStorage | None = None

    def __init__(self):

        self.logger = get_logger(__name__)

        self.msg_queue = queue.Queue(maxsize=1000)  # Buffer up to 1000 messages
        self.last_arrival_ts = time.time()  # Initialize with start time
        self.exit_event = threading.Event()

    def get_storage(self) -> base.BaseStorage | None:

        if self.storage:
            return self.storage

        backend = settings.storage_backend
        self.logger.info(f"Creating {backend} storage")

        try:

            if "noop" in backend:
                self.storage = noop.NoopStorage()
            elif "postgre" in backend or "pg" in backend:
                self.storage = postgres.PostgresStorage(
                    dsn=settings.postgres_dsn,
                    table=settings.postgres_table,
                    schema=settings.postgres_schema,
                )
            elif "sqlalchemy" in backend:
                self.storage = sqlalchemy.SQLAlchemyStorage(
                    dsn=settings.sqlalchemy_dsn,
                    table=settings.sqlalchemy_table,
                    schema=settings.sqlalchemy_schema,
                )
            elif "jsonl" in backend:
                self.storage = jsonl.JsonlStorage(
                    path=settings.jsonl_path,
                )
            else:
                self.storage = mongodb.MongoStorage(
                    mongo_uri=settings.mongo_uri,
                    collection_name=settings.mongo_collection,
                    db_name=settings.mongo_db,
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

            if seconds_since_last > settings.watchdog_timeout:
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
            load_filter(settings.mqtt_filter) if settings.mqtt_filter else None
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
            mqtt_user=settings.mqtt_user,
            mqtt_pass=settings.mqtt_pass,
            mqtt_transport=settings.mqtt_transport,
            mqtt_tls=settings.mqtt_tls,
            mqtt_topics=settings.mqtt_topics,
            mqtt_ignore_certs=settings.mqtt_ignore_certs,
        )

        try:

            user = f"{settings.mqtt_user}@" if settings.mqtt_user else ""
            self.logger.debug(
                f"MQTT connecting to {settings.mqtt_transport}://{user}{settings.mqtt_broker}:{settings.mqtt_port}"
            )
            client.connect(settings.mqtt_broker, settings.mqtt_port, keepalive=60)
            client.loop_forever()
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
        finally:
            self.exit_event.set()
            storage.close()
            self.logger.info("Exit")
