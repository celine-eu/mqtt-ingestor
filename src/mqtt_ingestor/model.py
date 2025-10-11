from typing import Callable, Any
from dataclasses import dataclass


@dataclass
class DocumentPayload:
    topic: str
    payload: Any
    ts: str


DocumentCallback = Callable[[DocumentPayload], None]
