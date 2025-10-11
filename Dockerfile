FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.9.2 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock* ./
RUN uv sync

COPY . .

ENTRYPOINT [ "python" ]
CMD ["-m", "mqtt_ingestor"]