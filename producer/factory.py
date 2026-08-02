from __future__ import annotations

from typing import Any

from producer.transports import BinanceTransport, FinnhubTransport


def build_transport(source: str) -> Any:
    if source == "binance":
        return BinanceTransport()
    if source == "finnhub":
        return FinnhubTransport()
    raise ValueError(f"unsupported source: {source}")
