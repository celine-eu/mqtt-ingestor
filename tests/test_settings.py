import os
import textwrap
from pathlib import Path

import pytest
from mqtt_ingestor.settings import Settings


def test_defaults():
    s = Settings(_env_file=())
    assert s.log_level == "INFO"
    assert s.watchdog_timeout == 60
    assert s.mqtt_broker == "mqtt"
    assert s.mqtt_port == 1883
    assert s.mqtt_tls is False
    assert s.mqtt_ignore_certs is False
    assert s.mqtt_filter is None
    assert s.storage_backend == "postgres"
    assert s.jsonl_path == "mqtt_messages.jsonl"


def test_env_vars_override_defaults(monkeypatch):
    monkeypatch.setenv("MQTT_BROKER", "custom-broker")
    monkeypatch.setenv("MQTT_PORT", "9999")
    monkeypatch.setenv("MQTT_TLS", "1")
    monkeypatch.setenv("STORAGE_BACKEND", "jsonl")
    s = Settings(_env_file=())
    assert s.mqtt_broker == "custom-broker"
    assert s.mqtt_port == 9999
    assert s.mqtt_tls is True
    assert s.storage_backend == "jsonl"


def test_storage_backend_normalized(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "  PostgreS  ")
    s = Settings(_env_file=())
    assert s.storage_backend == "postgres"


def test_mqtt_tls_bool_coercion(monkeypatch):
    for truthy in ["1", "true", "True", "yes"]:
        monkeypatch.setenv("MQTT_TLS", truthy)
        assert Settings(_env_file=()).mqtt_tls is True

    for falsy in ["0", "false", "False", "no"]:
        monkeypatch.setenv("MQTT_TLS", falsy)
        assert Settings(_env_file=()).mqtt_tls is False


def test_mqtt_ignore_certs_bool_coercion(monkeypatch):
    monkeypatch.setenv("MQTT_IGNORE_CERTS", "true")
    assert Settings(_env_file=()).mqtt_ignore_certs is True
    monkeypatch.setenv("MQTT_IGNORE_CERTS", "false")
    assert Settings(_env_file=()).mqtt_ignore_certs is False


def test_env_file_cascade(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    base = tmp_path / ".env"
    base.write_text("MQTT_BROKER=from-base\nMQTT_PORT=1000\n")

    local = tmp_path / ".env.local"
    local.write_text("MQTT_BROKER=from-local\n")

    dev = tmp_path / ".env.dev"
    dev.write_text("MQTT_BROKER=from-dev\n")

    s = Settings(
        _env_file=(str(base), str(local), str(dev)),
    )
    assert s.mqtt_broker == "from-dev"
    assert s.mqtt_port == 1000


def test_missing_env_files_skipped(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    base = tmp_path / ".env"
    base.write_text("MQTT_BROKER=only-base\n")

    s = Settings(
        _env_file=(
            str(base),
            str(tmp_path / ".env.local"),
            str(tmp_path / ".env.dev"),
            str(tmp_path / ".env.prod"),
        ),
    )
    assert s.mqtt_broker == "only-base"


def test_env_var_takes_precedence_over_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    base = tmp_path / ".env"
    base.write_text("MQTT_BROKER=from-file\n")
    monkeypatch.setenv("MQTT_BROKER", "from-env")

    s = Settings(_env_file=(str(base),))
    assert s.mqtt_broker == "from-env"


def test_extra_env_vars_ignored(monkeypatch):
    monkeypatch.setenv("TOTALLY_UNKNOWN_VAR", "whatever")
    s = Settings(_env_file=())
    assert not hasattr(s, "TOTALLY_UNKNOWN_VAR")
