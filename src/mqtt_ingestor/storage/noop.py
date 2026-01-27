from mqtt_ingestor.storage.base import BaseStorage
from mqtt_ingestor.model import DocumentPayload
from mqtt_ingestor.logger import get_logger


class NoopStorage(BaseStorage):
    """
    No-op storage backend for development and testing.
    Does not store data anywhere.
    """

    def __init__(self) -> None:
        self.logger = get_logger(__name__)
        self.logger.info("NoopStorage initialized - data will be dropped.")

    def save(self, document: DocumentPayload) -> None:
        """Log receipt and drop the document."""
        self.logger.debug(f"NoopStorage: received document from {document.topic}")

    def close(self) -> None:
        pass
