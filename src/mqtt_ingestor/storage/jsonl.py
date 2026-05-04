from __future__ import annotations
import json
from dataclasses import asdict
from pathlib import Path

from mqtt_ingestor.storage.base import BaseStorage
from mqtt_ingestor.model import DocumentPayload
from mqtt_ingestor.logger import get_logger


class JsonlStorage(BaseStorage):

    def __init__(self, path: str) -> None:
        self._logger = get_logger(__name__)
        self._path = Path(path)
        self._file = open(self._path, "a")
        self._logger.info(f"JsonlStorage writing to {self._path.resolve()}")

    def save(self, document: DocumentPayload) -> None:
        if not isinstance(document, DocumentPayload):
            raise TypeError("Expected DocumentPayload instance")
        line = json.dumps(asdict(document), ensure_ascii=False)
        self._file.write(line + "\n")
        self._file.flush()

    def close(self) -> None:
        try:
            self._file.close()
        except Exception:
            pass
