from __future__ import annotations
from datetime import datetime

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    JSON,
    TIMESTAMP,
    func,
    inspect,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from mqtt_ingestor.model import DocumentPayload
from mqtt_ingestor.storage.base import BaseStorage


Base = declarative_base()


class Message(Base):
    """ORM model for MQTT message rows."""

    __tablename__ = "mqtt_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    ts = Column(TIMESTAMP(timezone=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())


class SQLAlchemyStorage(BaseStorage):
    """
    SQLAlchemy-backed storage for MQTT ingestion.

    Stores messages in structured columns:
      id | topic | payload | ts | created_at
    """

    def __init__(self, dsn: str, table: str, schema: str | None = None) -> None:
        self._engine = create_engine(dsn, pool_pre_ping=True)
        self._Session = sessionmaker(bind=self._engine, expire_on_commit=False)
        self._table_name = table
        self._schema = schema

        # Dynamically set table name if overridden
        if self._table_name != Message.__tablename__:
            Message.__tablename__ = self._table_name

        if self._schema:
            Message.__table__.schema = self._schema

        self._ensure_table()

    # -------------------------------------------------------------------------

    def _ensure_table(self) -> None:
        """Create the table if it does not exist."""
        inspector = inspect(self._engine)
        if self._table_name not in inspector.get_table_names():
            Base.metadata.create_all(self._engine)

    # -------------------------------------------------------------------------

    def save(self, document: DocumentPayload) -> None:
        """Insert a single MQTT message as a structured row."""
        if not isinstance(document, DocumentPayload):
            raise TypeError("Expected DocumentPayload instance")

        try:
            ts = datetime.fromisoformat(document.ts.replace("Z", "+00:00"))
        except Exception:
            ts = datetime.utcnow()

        session: Session
        with self._Session() as session:
            try:
                row = Message(topic=document.topic, payload=document.payload, ts=ts)
                session.add(row)
                session.commit()
            except OperationalError as e:
                session.rollback()
                raise RuntimeError(f"Failed to save message: {e}") from e

    # -------------------------------------------------------------------------

    def close(self) -> None:
        """Dispose the connection pool."""
        try:
            self._engine.dispose()
        except Exception:
            pass
