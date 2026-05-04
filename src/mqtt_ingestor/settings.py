from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", ".env.local", ".env.dev", ".env.prod"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    log_level: str = "INFO"
    watchdog_timeout: int = 60

    mqtt_broker: str = "mqtt"
    mqtt_port: int = 1883
    mqtt_user: str = "mqtt_user"
    mqtt_pass: str = "secretpass"
    mqtt_transport: str = "tcp"
    mqtt_tls: bool = False
    mqtt_topics: str = "#"
    mqtt_ignore_certs: bool = False
    mqtt_filter: str | None = None

    storage_backend: str = "postgres"

    postgres_dsn: str = "postgresql://postgres:postgres@postgres:5432/mqtt"
    postgres_table: str = "mqtt_messages"
    postgres_schema: str = "public"

    sqlalchemy_dsn: str = "postgresql+psycopg2://postgres:postgres@postgres:5432/mqtt"
    sqlalchemy_schema: str = "public"
    sqlalchemy_table: str = "mqtt_messages"

    mongo_uri: str = "mongodb://mongo:27017/"
    mongo_db: str = "mqtt_data"
    mongo_collection: str = "data"

    jsonl_path: str = "mqtt_messages.jsonl"

    @field_validator("storage_backend", mode="before")
    @classmethod
    def normalize_backend(cls, v: str) -> str:
        return v.strip().lower()


settings = Settings()
