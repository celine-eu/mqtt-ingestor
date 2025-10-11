from __future__ import annotations
from pymongo import MongoClient, errors, collection
from mqtt_ingestor.storage.base import BaseStorage
from mqtt_ingestor.model import DocumentPayload


class MongoStorage(BaseStorage):
    """
    MongoDB-backed storage for MQTT documents.

    Each DocumentPayload is stored as a structured document:
      { "topic": ..., "payload": ..., "ts": ... }
    """

    _collection: collection.Collection | None = None

    def __init__(
        self,
        mongo_uri: str,
        db_name: str,
        collection_name: str,
    ) -> None:
        self._mongo_uri = mongo_uri
        self._db_name = db_name
        self._collection_name = collection_name

        self._client: MongoClient | None = None
        self._connect()

    def _connect(self) -> MongoClient:
        """(Re)connect to MongoDB if needed."""

        if self._client:
            try:
                # Ping to check if still alive
                self._client.admin.command("ping")
            except Exception:
                self.close()

        if self._client == None:
            self._client = MongoClient(self._mongo_uri, serverSelectionTimeoutMS=3000)

        return self._client

    def _get_collection(self) -> collection.Collection:
        client = self._client
        if client == None:
            client = self._connect()
        return client[self._db_name][self._collection_name]

    def save(self, document: DocumentPayload) -> None:
        """Insert a document, reconnecting if the client dropped."""
        from dataclasses import asdict

        if not isinstance(document, DocumentPayload):
            raise TypeError("Expected DocumentPayload instance")

        try:
            self._get_collection().insert_one(asdict(document))
        except errors.AutoReconnect:
            # try once more after reconnect
            self._get_collection().insert_one(asdict(document))
        except Exception as e:
            raise RuntimeError(f"Failed to save document to MongoDB: {e}") from e

    def close(self) -> None:
        if self._client:
            try:
                self._client.close()
                self._client = None
            except Exception:
                pass
