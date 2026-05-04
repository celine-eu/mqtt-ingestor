import pytest
from mqtt_ingestor.filter import load_filter
from mqtt_ingestor.model import DocumentPayload


def test_load_filter_with_function_name():
    fn = load_filter("mqtt_ingestor.filters.chain2:filter")
    assert callable(fn)


def test_load_filter_default_function_name():
    fn = load_filter("mqtt_ingestor.filters.chain2")
    assert callable(fn)


def test_load_filter_missing_function():
    with pytest.raises(ValueError, match="not found"):
        load_filter("mqtt_ingestor.filters.chain2:nonexistent")


def test_load_filter_missing_module():
    with pytest.raises(ModuleNotFoundError):
        load_filter("mqtt_ingestor.filters.does_not_exist:fn")


def test_chain2_filter_keeps_normal_doc():
    fn = load_filter("mqtt_ingestor.filters.chain2:filter")
    doc = DocumentPayload(topic="t", payload={"temperature": 22}, ts="t")
    assert fn(doc) is True


def test_chain2_filter_drops_chain2response():
    fn = load_filter("mqtt_ingestor.filters.chain2:filter")
    doc = DocumentPayload(topic="t", payload={"Chain2ResponseFoo": 1}, ts="t")
    assert fn(doc) is False


def test_chain2_filter_drops_chain2info():
    fn = load_filter("mqtt_ingestor.filters.chain2:filter")
    doc = DocumentPayload(topic="t", payload={"Chain2InfoBar": 1}, ts="t")
    assert fn(doc) is False


def test_chain2_filter_passes_non_dict():
    fn = load_filter("mqtt_ingestor.filters.chain2:filter")
    doc = DocumentPayload(topic="t", payload="string", ts="t")
    assert fn(doc) is True
