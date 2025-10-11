import os
from mqtt_ingestor.storage import mongodb, base, postgres, sqlalchemy
from mqtt_ingestor.mqtt import create_client, DocumentPayload
from mqtt_ingestor.model import DocumentPayload
from mqtt_ingestor.logger import get_logger
from dotenv import load_dotenv

logger = get_logger(__name__)


class MqttIngestor:

    storage: base.BaseStorage | None = None

    def __init__(self):

        load_dotenv()
        # load local override
        load_dotenv(".env.local", override=True)
        load_dotenv(".env.dev", override=True)

        self.MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt")
        self.MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
        self.MQTT_USER = os.getenv("MQTT_USER", "mqtt_user")
        self.MQTT_PASS = os.getenv("MQTT_PASS", "secretpass")
        self.MQTT_TRANSPORT = os.getenv("MQTT_TRANSPORT", "tcp")
        self.MQTT_TLS = os.getenv("MQTT_TLS", "0") == "1"
        self.MQTT_TOPICS = os.getenv("MQTT_TOPICS", "#")

        self.STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "postgres").strip().lower()

        # postgres
        self.POSTGRES_DSN = os.getenv(
            "POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/mqtt"
        )
        self.POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "mqtt_messages")

        # sqlalchemy
        self.SQLALCHEMY_DSN = os.getenv(
            "SQLALCHEMY_DSN",
            "postgresql+psycopg2://postgres:postgres@postgres:5432/mqtt",
        )
        self.SQLALCHEMY_TABLE = os.getenv("SQLALCHEMY_TABLE", "mqtt_messages")

        # mongodb
        self.MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
        self.MONGO_DB = os.getenv("MONGO_DB", "mqtt_data")
        self.MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "data")

    def get_storage(self) -> base.BaseStorage | None:

        if self.storage:
            return self.storage

        backend = self.STORAGE_BACKEND
        logger.info(f"Creating {backend} storage")

        try:
            if "postgre" in backend or "pg" in backend:
                if not self.POSTGRES_DSN or not self.POSTGRES_TABLE:
                    raise Exception(
                        f"{backend} backend requires env POSTGRES_DSN, POSTGRES_TABLE"
                    )
                self.storage = postgres.PostgresStorage(
                    dsn=self.POSTGRES_DSN, table=self.POSTGRES_TABLE
                )
            elif "sqlalchemy" in backend:
                if not self.SQLALCHEMY_DSN or not self.SQLALCHEMY_TABLE:
                    raise Exception(
                        f"{backend} backend requires env SQLALCHEMY_DSN, SQLALCHEMY_TABLE"
                    )
                self.storage = sqlalchemy.SQLAlchemyStorage(
                    dsn=self.SQLALCHEMY_DSN, table=self.SQLALCHEMY_TABLE
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
            logger.error(f"Connection to {backend} failed: {e}")
            return None

        return self.storage

    def start(self):

        storage = self.get_storage()

        if not storage:
            logger.warning("Failed to connect to storage")
            return

        def on_document(document: DocumentPayload):
            try:
                storage.save(document)
                logger.debug(f"Saved record from {document.topic}")
            except Exception as e:
                logger.error(f"Failed to save message: {e}")

        client = create_client(
            on_document,
            mqtt_user=self.MQTT_USER,
            mqtt_pass=self.MQTT_PASS,
            mqtt_transport=self.MQTT_TRANSPORT,
            mqtt_tls=self.MQTT_TLS,
            mqtt_topics=self.MQTT_TOPICS,
        )

        try:

            user = f"{self.MQTT_USER}@" if self.MQTT_USER else ""
            logger.debug(
                f"MQTT connecting to {self.MQTT_TRANSPORT}://{user}{self.MQTT_BROKER}:{self.MQTT_PORT}"
            )
            client.connect(self.MQTT_BROKER, self.MQTT_PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
        finally:
            logger.info("Exit")
            storage.close()
