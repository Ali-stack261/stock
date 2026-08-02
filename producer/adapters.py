from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict


def binance_to_market_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Binance trade payloads into the normalized event shape."""
    if not isinstance(raw, dict):
        raise ValueError("Binance payload must be a dictionary")

    if all(key in raw for key in ("symbol", "price", "volume", "timestamp")):
        return {
            "symbol": str(raw["symbol"]),
            "price": float(raw["price"]),
            "volume": float(raw["volume"]),
            "timestamp": str(raw["timestamp"]),
        }

    symbol = raw.get("s")
    price = raw.get("p")
    quantity = raw.get("q")
    trade_time = raw.get("T")

    if not symbol or price is None or quantity is None or trade_time is None:
        raise ValueError("Binance payload is missing required trade fields")

    timestamp = datetime.fromtimestamp(int(trade_time) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "symbol": str(symbol),
        "price": float(price),
        "volume": float(quantity),
        "timestamp": timestamp,
    }


def finnhub_to_market_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Finnhub trade payloads into the normalized event shape."""
    if not isinstance(raw, dict):
        raise ValueError("Finnhub payload must be a dictionary")

    if all(key in raw for key in ("symbol", "price", "volume", "timestamp")):
        return {
            "symbol": str(raw["symbol"]),
            "price": float(raw["price"]),
            "volume": float(raw["volume"]),
            "timestamp": str(raw["timestamp"]),
        }

    data = raw.get("data")
    if not isinstance(data, list) or not data:
        raise ValueError("Finnhub payload must contain a non-empty data array")

    item = data[0]
    symbol = item.get("s")
    price = item.get("p")
    volume = item.get("v")
    trade_time = item.get("t")

    if not symbol or price is None or volume is None or trade_time is None:
        raise ValueError("Finnhub payload is missing required trade fields")

    timestamp = datetime.fromtimestamp(int(trade_time) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "symbol": str(symbol).replace("BINANCE:", ""),
        "price": float(price),
        "volume": float(volume),
        "timestamp": timestamp,
    }


def adapt_payload(payload: Dict[str, Any], source: str) -> Dict[str, Any]:
    """Adapt an exchange-specific payload into the internal normalized event shape."""
    if source == "binance":
        return binance_to_market_event(payload)
    if source == "finnhub":
        return finnhub_to_market_event(payload)
    return payload
