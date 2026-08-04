from __future__ import annotations

import asyncio
import json
import os

import websockets


class BaseWebSocketTransport:
    def __init__(self, url: str, source: str):
        self.url = url
        self.source = source
        self._ws = None
        self._loop = None

    def connect(self) -> None:
        if self._ws is not None:
            return
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._ws = self._loop.run_until_complete(websockets.connect(self.url))
        if self.source == "finnhub":
            self._loop.run_until_complete(self._ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"})))

    def receive(self, timeout: float | None = None) -> str | None:
        if self._ws is None:
            raise RuntimeError("Transport is not connected")
        if timeout is not None:
            return self._loop.run_until_complete(asyncio.wait_for(self._ws.recv(), timeout=timeout))
        return self._loop.run_until_complete(self._ws.recv())

    def send_ping(self) -> None:
        if self._ws is not None:
            self._loop.run_until_complete(self._ws.ping())

    def close(self) -> None:
        if self._ws is not None:
            self._loop.run_until_complete(self._ws.close())
            self._ws = None


class BinanceTransport(BaseWebSocketTransport):
    def __init__(self, url: str | None = None):
        super().__init__(url or os.getenv("BINANCE_WEBSOCKET_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade"), "binance")


class FinnhubTransport(BaseWebSocketTransport):
    def __init__(self, url: str | None = None):
        super().__init__(url or os.getenv("FINNHUB_WEBSOCKET_URL", "wss://ws.finnhub.io?token=demo"), "finnhub")
