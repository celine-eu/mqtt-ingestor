import json

import pytest
from mqtt_ingestor.model import DocumentPayload
from mqtt_ingestor.storage.jsonl import JsonlStorage


@pytest.fixture
def jsonl_path(tmp_path):
    return str(tmp_path / "out.jsonl")


def test_save_writes_valid_jsonl(jsonl_path, sample_doc):
    storage = JsonlStorage(path=jsonl_path)
    storage.save(sample_doc)
    storage.close()

    with open(jsonl_path) as f:
        line = f.readline()

    record = json.loads(line)
    assert record["topic"] == "test/topic"
    assert record["payload"] == {"temperature": 22.5, "unit": "C"}
    assert record["ts"] == "2025-06-01T12:00:00Z"


def test_save_multiple_lines(jsonl_path):
    storage = JsonlStorage(path=jsonl_path)
    for i in range(5):
        storage.save(DocumentPayload(topic=f"t/{i}", payload={"i": i}, ts="ts"))
    storage.close()

    with open(jsonl_path) as f:
        lines = f.readlines()

    assert len(lines) == 5
    for i, line in enumerate(lines):
        assert json.loads(line)["topic"] == f"t/{i}"


def test_appends_to_existing_file(jsonl_path):
    with open(jsonl_path, "w") as f:
        f.write('{"existing": true}\n')

    storage = JsonlStorage(path=jsonl_path)
    storage.save(DocumentPayload(topic="new", payload={}, ts="ts"))
    storage.close()

    with open(jsonl_path) as f:
        lines = f.readlines()

    assert len(lines) == 2
    assert json.loads(lines[0]) == {"existing": True}
    assert json.loads(lines[1])["topic"] == "new"


def test_save_rejects_non_document(jsonl_path):
    storage = JsonlStorage(path=jsonl_path)
    with pytest.raises(TypeError, match="Expected DocumentPayload"):
        storage.save({"not": "a dataclass"})
    storage.close()


def test_close_is_idempotent(jsonl_path, sample_doc):
    storage = JsonlStorage(path=jsonl_path)
    storage.save(sample_doc)
    storage.close()
    storage.close()


def test_handles_unicode_payload(jsonl_path):
    doc = DocumentPayload(topic="t", payload={"msg": "café ☃"}, ts="ts")
    storage = JsonlStorage(path=jsonl_path)
    storage.save(doc)
    storage.close()

    with open(jsonl_path, encoding="utf-8") as f:
        record = json.loads(f.readline())
    assert record["payload"]["msg"] == "café ☃"
