FROM python:3.13-slim AS build

COPY --from=ghcr.io/astral-sh/uv:0.9.2 /uv /uvx /bin/

WORKDIR /app

COPY . .

RUN uv build .

FROM python:3.13-slim

COPY --from=build /app/dist /dist

RUN pip install /dist/mqtt_ingestor*.whl

ENTRYPOINT ["python"]
CMD ["-m", "mqtt_ingestor"]