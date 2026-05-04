import pytest
from mqtt_ingestor.model import DocumentPayload


@pytest.fixture
def sample_doc():
    return DocumentPayload(
        topic="test/topic",
        payload={"temperature": 22.5, "unit": "C"},
        ts="2025-06-01T12:00:00Z",
    )


@pytest.fixture
def sample_doc_non_dict():
    return DocumentPayload(
        topic="test/raw",
        payload="just a string",
        ts="2025-06-01T12:00:00Z",
    )
