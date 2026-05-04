import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from mqtt_ingestor.model import DocumentPayload
from mqtt_ingestor.mqtt import make_on_message, make_on_connect, create_client


def _fake_msg(topic: str, payload: dict | str):
    raw = payload if isinstance(payload, str) else json.dumps(payload)
    return SimpleNamespace(topic=topic, payload=raw.encode())


class TestMakeOnMessage:
    def test_valid_json_calls_callback(self):
        received = []
        on_msg = make_on_message(lambda doc: received.append(doc))
        on_msg(None, None, _fake_msg("a/b", {"val": 1}))

        assert len(received) == 1
        doc = received[0]
        assert isinstance(doc, DocumentPayload)
        assert doc.topic == "a/b"
        assert doc.payload == {"val": 1}
        assert doc.ts  # non-empty ISO timestamp

    def test_invalid_json_skipped(self):
        received = []
        on_msg = make_on_message(lambda doc: received.append(doc))
        on_msg(None, None, _fake_msg("t", "not{json"))

        assert len(received) == 0

    def test_callback_exception_does_not_propagate(self):
        def boom(doc):
            raise RuntimeError("fail")

        on_msg = make_on_message(boom)
        on_msg(None, None, _fake_msg("t", {"ok": True}))


class TestMakeOnConnect:
    def test_subscribes_to_single_topic(self):
        client = MagicMock()
        on_conn = make_on_connect("sensors/temp")
        on_conn(client, None, None, 0, None)

        client.subscribe.assert_called_once_with("sensors/temp")

    def test_subscribes_to_multiple_topics(self):
        client = MagicMock()
        on_conn = make_on_connect("a/b, c/d, e/f")
        on_conn(client, None, None, 0, None)

        topics = [call.args[0] for call in client.subscribe.call_args_list]
        assert topics == ["a/b", "c/d", "e/f"]

    def test_defaults_to_wildcard(self):
        client = MagicMock()
        on_conn = make_on_connect(None)
        on_conn(client, None, None, 0, None)

        client.subscribe.assert_called_once_with("#")


class TestCreateClient:
    def test_returns_client_with_callbacks(self):
        client = create_client(lambda doc: None, mqtt_topics="test/#")
        assert client.on_connect is not None
        assert client.on_message is not None

    def test_websockets_transport(self):
        client = create_client(lambda doc: None, mqtt_transport="websockets")
        assert client._transport == "websockets"

    def test_tcp_transport_default(self):
        client = create_client(lambda doc: None)
        assert client._transport == "tcp"
