from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def normalize_event(payload: dict[str, Any], source: str = "unknown") -> dict[str, Any]:
    """Normalize an incoming market payload into a consistent event shape."""
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")

    required_fields = {"symbol", "price", "volume", "timestamp"}
    missing = required_fields.difference(payload.keys())
    if missing:
        raise ValueError(f"missing required fields: {sorted(missing)}")

    price = float(payload["price"])
    volume = float(payload["volume"])
    if price <= 0 or volume < 0:
        raise ValueError("price must be > 0 and volume must be >= 0")

    timestamp = payload["timestamp"]
    if not isinstance(timestamp, str) or not timestamp:
        raise ValueError("timestamp must be a non-empty string")

    normalized = {
        "symbol": str(payload["symbol"]),
        "price": price,
        "volume": volume,
        "timestamp": timestamp,
        "source": source,
        "idempotency_key": f"{payload['symbol']}-{timestamp}-{source}",
        "received_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    return normalized
