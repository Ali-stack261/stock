from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from producer.normalize_event import normalize_event


class MarketWebSocketClient:
    """A lightweight abstraction for ingesting and normalizing market events."""

    def __init__(self, source: str = "unknown", handler: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.source = source
        self.handler = handler

    def process_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        event = normalize_event(payload, source=self.source)
        if self.handler is not None:
            self.handler(event)
        return event
