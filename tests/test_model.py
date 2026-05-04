from dataclasses import asdict
from mqtt_ingestor.model import DocumentPayload


def test_document_payload_fields():
    doc = DocumentPayload(topic="a/b", payload={"k": 1}, ts="2025-01-01T00:00:00Z")
    assert doc.topic == "a/b"
    assert doc.payload == {"k": 1}
    assert doc.ts == "2025-01-01T00:00:00Z"


def test_document_payload_asdict():
    doc = DocumentPayload(topic="x", payload=[1, 2], ts="t")
    d = asdict(doc)
    assert d == {"topic": "x", "payload": [1, 2], "ts": "t"}


def test_document_payload_any_payload_type():
    for payload in [None, 42, "hello", [1, 2], {"nested": {"a": 1}}]:
        doc = DocumentPayload(topic="t", payload=payload, ts="t")
        assert doc.payload == payload
