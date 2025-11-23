from paho.mqtt.client import Client
from paho.mqtt.enums import CallbackAPIVersion

import json
from datetime import datetime, timezone
from mqtt_ingestor.model import DocumentCallback, DocumentPayload
from mqtt_ingestor.logger import get_logger

import ssl

logger = get_logger(__name__)


def _iso_utc_now() -> str:
    # RFC 3339 / ISO 8601 'Z' style (e.g., 2025-10-11T12:34:56Z)
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_on_connect(mqtt_topics: str | None = None):

    mqtt_topics = mqtt_topics or "#"

    topics = mqtt_topics.split(",")

    def on_connect(client, userdata, flags, reason_code, properties):
        logger.info(f"Connection result: {reason_code}")

        for topic in topics:
            topic = topic.strip()
            logger.debug(f"Subscribing to {topic}")
            client.subscribe(topic)

    return on_connect


def make_on_message(on_document: DocumentCallback):
    def on_message(client, userdata, msg):
        try:
            payload_str = msg.payload.decode()
            data = json.loads(payload_str)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON: {msg.payload}")
            return

        document = DocumentPayload(payload=data, topic=msg.topic, ts=_iso_utc_now())

        try:
            on_document(document)
        except Exception as e:
            logger.error(f"Failed to save message: {e}")

    return on_message


def create_client(
    on_document: DocumentCallback,
    mqtt_user: str | None = None,
    mqtt_pass: str | None = None,
    mqtt_transport: str | None = None,
    mqtt_tls: bool = False,
    mqtt_topics: str | None = None,
    mqtt_ignore_certs: str | None = None,
):

    client = Client(
        reconnect_on_failure=True,
        callback_api_version=CallbackAPIVersion.VERSION2,
        transport="websockets" if mqtt_transport == "websockets" else "tcp",
    )

    if mqtt_tls:
        client.tls_set(
            cert_reqs=(
                ssl.CERT_NONE if mqtt_ignore_certs == "true" else ssl.CERT_REQUIRED
            )
        )

    if mqtt_pass and mqtt_user:
        client.username_pw_set(mqtt_user, mqtt_pass)

    client.on_connect = make_on_connect(mqtt_topics)
    client.on_message = make_on_message(on_document)

    return client
