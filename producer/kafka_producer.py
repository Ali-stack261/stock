from __future__ import annotations

import json
import os
from typing import Any

try:
    from kafka import KafkaProducer  # type: ignore
    from kafka.errors import KafkaError  # type: ignore
except ImportError:  # pragma: no cover - exercised in environments without a compatible kafka-python install
    KafkaProducer = None
    KafkaError = Exception


class MarketKafkaProducer:
    def __init__(self, bootstrap_servers: list[str] | None = None, topic: str | None = None):
        self.bootstrap_servers = bootstrap_servers or [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")]
        self.topic = topic or os.getenv("KAFKA_TOPIC", "stock_prices")
        self._producer = None

    def _build_producer(self) -> None:
        if KafkaProducer is None:
            return

        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
                enable_idempotence=True,
                retries=5,
                bootstrap_timeout_ms=5000,
            )
        except KafkaError as exc:
            raise RuntimeError(f"NoBrokersAvailable: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"NoBrokersAvailable: {exc}") from exc

    def send_event(self, event: dict[str, Any]) -> None:
        if self._producer is None:
            self._build_producer()

        if self._producer is None:
            raise RuntimeError("NoBrokersAvailable: kafka-python is not installed or no Kafka broker is reachable")

        try:
            self._producer.send(self.topic, key=str(event["symbol"]).encode("utf-8"), value=event)
            self._producer.flush()
        except KafkaError as exc:
            raise RuntimeError(f"NoBrokersAvailable: {exc}") from exc
        except Exception as exc:  # pragma: no cover - depends on runtime broker availability
            raise RuntimeError(f"NoBrokersAvailable: {exc}") from exc

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()
