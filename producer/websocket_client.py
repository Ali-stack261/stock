from __future__ import annotations

import json
import random
import time
from typing import Any, Callable, Dict, List, Optional

from producer.factory import build_transport
from producer.normalize_event import normalize_event


class MarketWebSocketClient:
    """A resilient streaming client for ingesting and normalizing market events."""

    def __init__(
        self,
        source: str = "unknown",
        sources: Optional[List[str]] = None,
        handler: Optional[Callable[[Dict[str, Any]], None]] = None,
        transport_factory: Optional[Callable[[str], Any]] = None,
        sleep_func: Optional[Callable[[float], None]] = None,
    ):
        self.source = source
        self.sources = [source, *(sources or [])]
        self.handler = handler
        self.transport_factory = transport_factory or build_transport
        self.sleep_func = sleep_func or time.sleep
        self.current_source = source
        self._transport = None

    def process_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        event = normalize_event(payload, source=self.current_source)
        if self.handler is not None:
            self.handler(event)
        return event

    def connect(self, source: Optional[str] = None) -> None:
        source_name = source or self.current_source
        self._transport = self.transport_factory(source_name)
        if self._transport is None:
            raise RuntimeError(f"No transport available for source {source_name}")
        self._transport.connect()
        self.current_source = source_name

    def _connect_with_retry(self, source: str) -> None:
        for _ in range(2):
            try:
                self.connect(source)
                return
            except Exception:
                self._retry_with_backoff()
        self.connect(source)

    def listen(self, max_messages: int = 1, heartbeat_interval: float = 5.0, stale_after: float = 10.0) -> None:
        message_count = 0
        last_seen = time.time()

        while message_count < max_messages:
            try:
                if self._transport is None:
                    self._connect_with_retry(self.current_source)

                payload = self._transport.receive(timeout=heartbeat_interval)
                if payload is None:
                    raise ConnectionError("no payload received")

                last_seen = time.time()
                message_count += 1
                if isinstance(payload, str):
                    payload = json.loads(payload)
                if hasattr(self._transport, "send_ping"):
                    self._transport.send_ping()
                self.process_message(payload)
            except Exception:
                if self._transport is not None:
                    self._transport.close()
                    self._transport = None

                for candidate in self.sources:
                    if candidate == self.current_source:
                        continue
                    try:
                        self._connect_with_retry(candidate)
                        self.current_source = candidate
                        break
                    except Exception:
                        continue
                else:
                    try:
                        self._connect_with_retry(self.current_source)
                    except Exception:
                        self._retry_with_backoff()

                if self._transport is None:
                    raise

                if time.time() - last_seen > stale_after:
                    self._retry_with_backoff()

    def _retry_with_backoff(self) -> None:
        backoff = min(2.0 + random.uniform(0, 0.5), 8.0)
        self.sleep_func(backoff)

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None
