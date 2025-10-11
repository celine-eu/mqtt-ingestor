from mqtt_ingestor.model import DocumentPayload


class BaseStorage:
    def save(self, document: DocumentPayload) -> None:
        raise NotImplementedError

    def close(self) -> None:
        pass
