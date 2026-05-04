import queue
import threading
import time
from unittest.mock import patch, MagicMock

import pytest
from mqtt_ingestor.api import MqttIngestor
from mqtt_ingestor.model import DocumentPayload
from mqtt_ingestor.storage.noop import NoopStorage
from mqtt_ingestor.storage.jsonl import JsonlStorage


class TestGetStorage:
    def test_noop_backend(self):
        with patch("mqtt_ingestor.api.settings") as mock_settings:
            mock_settings.storage_backend = "noop"
            ingestor = MqttIngestor()
            storage = ingestor.get_storage()
            assert isinstance(storage, NoopStorage)

    def test_jsonl_backend(self, tmp_path):
        path = str(tmp_path / "test.jsonl")
        with patch("mqtt_ingestor.api.settings") as mock_settings:
            mock_settings.storage_backend = "jsonl"
            mock_settings.jsonl_path = path
            ingestor = MqttIngestor()
            storage = ingestor.get_storage()
            assert isinstance(storage, JsonlStorage)
            storage.close()

    def test_caches_storage_instance(self):
        with patch("mqtt_ingestor.api.settings") as mock_settings:
            mock_settings.storage_backend = "noop"
            ingestor = MqttIngestor()
            s1 = ingestor.get_storage()
            s2 = ingestor.get_storage()
            assert s1 is s2

    def test_returns_none_on_connection_failure(self):
        with patch("mqtt_ingestor.api.settings") as mock_settings:
            mock_settings.storage_backend = "postgres"
            mock_settings.postgres_dsn = "postgresql://bad:bad@localhost:1/nope"
            mock_settings.postgres_table = "t"
            mock_settings.postgres_schema = "public"
            ingestor = MqttIngestor()
            assert ingestor.get_storage() is None

    def test_unknown_backend_falls_through_to_mongo(self):
        with patch("mqtt_ingestor.api.settings") as mock_settings:
            mock_settings.storage_backend = "unknown"
            mock_settings.mongo_uri = "mongodb://localhost:27017/"
            mock_settings.mongo_db = "db"
            mock_settings.mongo_collection = "col"
            ingestor = MqttIngestor()
            storage = ingestor.get_storage()
            # MongoClient is lazy — init succeeds even with unreachable host
            from mqtt_ingestor.storage.mongodb import MongoStorage

            assert isinstance(storage, MongoStorage)
            storage.close()


class TestWorker:
    def test_worker_processes_queue(self, tmp_path):
        path = str(tmp_path / "worker.jsonl")
        with patch("mqtt_ingestor.api.settings") as mock_settings:
            mock_settings.storage_backend = "jsonl"
            mock_settings.jsonl_path = path

            ingestor = MqttIngestor()
            doc = DocumentPayload(topic="t", payload={"v": 1}, ts="ts")
            ingestor.msg_queue.put(doc)

            t = threading.Thread(target=ingestor._worker, daemon=True)
            t.start()

            time.sleep(0.5)
            ingestor.exit_event.set()
            t.join(timeout=2)

            ingestor.storage.close()

            with open(path) as f:
                lines = f.readlines()
            assert len(lines) == 1

    def test_worker_exits_when_no_storage(self):
        with patch("mqtt_ingestor.api.settings") as mock_settings:
            mock_settings.storage_backend = "postgres"
            mock_settings.postgres_dsn = "postgresql://bad:bad@localhost:1/nope"
            mock_settings.postgres_table = "t"
            mock_settings.postgres_schema = "public"

            ingestor = MqttIngestor()
            t = threading.Thread(target=ingestor._worker, daemon=True)
            t.start()
            t.join(timeout=3)
            assert not t.is_alive()


class TestOnDocument:
    def test_queue_receives_document(self):
        with patch("mqtt_ingestor.api.settings") as mock_settings:
            mock_settings.storage_backend = "noop"
            mock_settings.mqtt_filter = None

            ingestor = MqttIngestor()
            doc = DocumentPayload(topic="t", payload={}, ts="ts")
            ingestor.msg_queue.put(doc, block=False)

            assert ingestor.msg_queue.qsize() == 1
            assert ingestor.msg_queue.get().topic == "t"
