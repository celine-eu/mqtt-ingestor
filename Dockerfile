FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.9.2 /uv /uvx /bin/

WORKDIR /app

COPY . .

RUN uv sync

ENTRYPOINT [ "python" ]
CMD ["-m", "mqtt_ingestor"]