from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

try:
    from kafka import KafkaProducer  # type: ignore
except Exception:  # pragma: no cover - exercised in environments without a compatible kafka-python install
    KafkaProducer = None


class MarketKafkaProducer:
    def __init__(self, bootstrap_servers: Optional[list[str]] = None, topic: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")]
        self.topic = topic or os.getenv("KAFKA_TOPIC", "stock_prices")
        self._producer = None

        if KafkaProducer is not None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
                enable_idempotence=True,
                retries=5,
            )

    def send_event(self, event: Dict[str, Any]) -> None:
        if self._producer is None:
            raise RuntimeError("NoBrokersAvailable: kafka-python is not installed or no Kafka broker is reachable")

        try:
            self._producer.send(self.topic, key=str(event["symbol"]).encode("utf-8"), value=event)
            self._producer.flush()
        except Exception as exc:  # pragma: no cover - depends on runtime broker availability
            raise RuntimeError(f"NoBrokersAvailable: {exc}") from exc

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()
