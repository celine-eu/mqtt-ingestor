from __future__ import annotations
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from psycopg2 import OperationalError, InterfaceError
from datetime import datetime
from mqtt_ingestor.model import DocumentPayload
from mqtt_ingestor.storage.base import BaseStorage
from typing import Any


class PostgresStorage(BaseStorage):
    """
    Stores each MQTT message as a structured row:
      topic TEXT,
      payload JSONB,
      ts TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT now()
    """

    _conn: Any = None

    def __init__(self, dsn: str, table: str, schema: str = "public") -> None:
        from psycopg2 import sql

        self._sql = sql

        self._schema = schema
        self._table = table
        self._dsn = dsn
        self._conn = None

        self._ensure_connection()

    def _ensure_connection(self) -> None:
        """Ensure we have a live connection, reconnect if needed."""
        if self._conn is not None:
            try:
                with self._conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return  # connection is alive
            except (InterfaceError, OperationalError):
                try:
                    self._conn.close()
                except Exception:
                    pass

        # reconnect
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = True
        self._create_table()

    def _create_table(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    CREATE SCHEMA IF NOT EXISTS {};
                    CREATE TABLE IF NOT EXISTS {} (
                        id BIGSERIAL PRIMARY KEY,
                        topic TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        ts TIMESTAMPTZ NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                ).format(
                    sql.Identifier(self._schema),
                    sql.Identifier(self._schema, self._table),
                )
            )

    def save(self, document: DocumentPayload) -> None:
        """Insert a single MQTT document into the table."""

        self._ensure_connection()

        if not isinstance(document, DocumentPayload):
            raise TypeError("Expected DocumentPayload instance")

        # Validate timestamp string (optional)
        try:
            ts = datetime.fromisoformat(document.ts.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"Invalid timestamp format: {document.ts}")

        with self._conn.cursor() as cur:
            cur.execute(
                self._sql.SQL(
                    """
                    INSERT INTO {} (topic, payload, ts)
                    VALUES (%s, %s, %s)
                    """
                ).format(self._sql.Identifier(self._schema, self._table)),
                (document.topic, Json(document.payload), ts),
            )

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass
