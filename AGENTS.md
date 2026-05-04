# AGENTS.md — mqtt-ingestor

## Project overview

MQTT message ingestor that subscribes to broker topics and persists messages to configurable storage backends. Runs as a long-lived process with a background worker thread and a watchdog.

## Tech stack

- Python 3.13+, managed with `uv`
- paho-mqtt for MQTT connectivity
- Pydantic Settings for configuration (env file cascade)
- Storage: PostgreSQL (psycopg2), SQLAlchemy, MongoDB, JSONL, NoOp
- Semantic release with conventional commits

## Key patterns

### Configuration

All config lives in `src/mqtt_ingestor/settings.py` as a Pydantic `Settings` singleton. Env files load in order: `.env` → `.env.local` → `.env.dev` → `.env.prod` (later overrides earlier, missing files skipped). Access via `from mqtt_ingestor.settings import settings`. Never use `os.getenv` directly.

### Storage backends

Every backend extends `BaseStorage` in `src/mqtt_ingestor/storage/base.py` and implements `save(document)` and `close()`. Backend selection happens in `api.py:get_storage()` via substring match on `settings.storage_backend`. To add a new backend: create a module in `storage/`, subclass `BaseStorage`, wire it into `get_storage()`.

### Message flow

MQTT callback → optional filter → thread-safe queue (max 1000) → background worker → `storage.save()`. The worker exits on DB failure to trigger container restart.

### Adding a new env variable

Add the field with a default to `Settings` in `settings.py`. Reference it as `settings.field_name`. No other wiring needed.

### Filters

Optional message filters loaded from `MQTT_FILTER` env var (format: `module.path:function_name`). Filter is a `Callable[[DocumentPayload], bool]` — return `False` to drop.

### Conventions

- Conventional commits (`feat:`, `fix:`, `chore:`, `perf:`)
- `uv` for dependency management — run `uv lock` after changing `pyproject.toml`
- Entry point: `mqtt-ingestor` CLI or `python -m mqtt_ingestor`
