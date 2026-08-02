# Phase 1 — Real-Time Data Collection: Status Check

Repo: `Ali-stack261/stock`
Commit checked: `e252290` — "Add real Binance/Finnhub websocket transports"

## ✅ Now fixed (since last check)

| Item | Status |
|---|---|
| Real `BinanceTransport` / `FinnhubTransport` using the `websockets` library | Added (`producer/transports.py`) |
| Transport factory to wire into `MarketWebSocketClient` | Added (`producer/factory.py`) |
| Transport-level unit tests | Added (`tests/unit/test_transports.py`) |

All 7 tests pass:
```
test_client_fails_over_to_secondary_source_on_stale_connection ... ok
test_client_reconnects_after_initial_connection_error ... ok
test_normalize_event_adds_metadata ... ok
test_websocket_client_normalizes_and_forwards_event ... ok
test_websocket_client_rejects_invalid_payload ... ok
test_binance_transport_uses_expected_defaults ... ok
test_finnhub_transport_uses_expected_defaults ... ok
```

## ❌ New gap found: raw payload shape mismatch

`normalize_event()` requires exact keys: `symbol`, `price`, `volume`, `timestamp`.

Real exchange payloads don't look like that:

**Binance trade stream:**
```json
{"e":"trade","s":"BTCUSDT","p":"118420.52","q":"0.42","T":1690886434000}
```
Keys are `s` / `p` / `q` / `T`, not `symbol` / `price` / `volume` / `timestamp`.

**Finnhub trade message:**
```json
{"type":"trade","data":[{"s":"BINANCE:BTCUSDT","p":118420.52,"v":0.42,"t":1690886434000}]}
```
Nested under a `data` array, different keys again. Finnhub also requires sending
`{"type":"subscribe","symbol":"..."}` right after connecting — `FinnhubTransport`
never sends this, so it would receive nothing even once connected.

`listen()` currently does `json.loads(payload)` and passes the result straight into
`normalize_event()` with no translation step. Against real traffic this raises
`ValueError: missing required fields` on the first message.

**Why the tests don't catch it:** `test_transports.py` only checks that `transport.url`
contains the source name — nothing exercises an actual (or even fixture) raw
Binance/Finnhub message through `normalize_event()`.

*Note: I couldn't verify this by connecting live — Binance/Finnhub aren't reachable from
this environment's network allowlist — but the mismatch is visible directly from the
code and the documented message formats of both exchanges.*

## Net effect

The transport layer (connect/reconnect/failover/heartbeat) is real and solid. The missing
piece is a per-exchange adapter function — e.g. `binance_to_market_event(raw: dict) -> dict`
and `finnhub_to_market_event(raw: dict) -> dict` — that maps each exchange's native message
shape into the `symbol/price/volume/timestamp` format `normalize_event()` expects, plus a
subscribe step for Finnhub. Without that, Phase 1 will connect successfully and then fail on
the very first real message.
