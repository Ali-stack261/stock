# Phase 1 — Real-Time Data Collection: Missing Pieces

Repo: `Ali-stack261/stock`

## What's done
- `normalize_event.py` — field validation, idempotency key, `received_at` timestamp
- `market_event.avsc` — Avro schema matching the normalized event
- 3 passing unit tests for normalization and payload rejection

## What's missing

### 1. No actual WebSocket connection
`MarketWebSocketClient` has no `connect()` method or event loop. `process_message()` only accepts a dict you hand it manually — there's nothing that opens a socket to Binance, Finnhub, or any live feed.

### 2. No WebSocket dependency installed
`requirements.txt` contains only `pytest`. No `websockets`, `websocket-client`, or exchange SDK — so a live connection couldn't be made even if the code called for one.

### 3. No reconnect logic
No exponential backoff + jitter for dropped connections. A socket drop currently has no recovery path.

### 4. No heartbeat / ping-pong monitoring
Nothing detects a socket that looks open but has stopped sending data (silent connection death).

### 5. No multi-source failover
No logic to switch to a secondary feed (e.g. Finnhub) if the primary (e.g. Binance) goes stale.

### 6. No end-to-end streaming test
Existing tests simulate an already-received payload — none of them exercise an actual open connection receiving live or mocked streaming messages.

## Net effect
The validation/normalization boundary is solid, but the piece that makes this "real-time data collection" — an actual live, resilient connection to a market feed — hasn't been written yet.
