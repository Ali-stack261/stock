#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://localhost:8000}"
API_KEY="${STAGING_API_KEY:-test-key}"

echo "Running smoke tests against $BASE_URL"

echo "[1/2] Health check..."
HEALTH=$(curl -sf "$BASE_URL/health" || true)
if ! echo "$HEALTH" | grep -q '"status"'; then
  echo "FAIL: health check did not return status field. Response: $HEALTH"
  exit 1
fi
echo "Health check passed: $HEALTH"

echo "[2/2] Predict endpoint..."
RESPONSE=$(curl -sf -X POST "$BASE_URL/predict" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"symbol":"BTCUSDT","current_price":100.0,"price_return":0.001,"volume_change":0.0,"ma5_ratio":1.0,"ma20_ratio":1.0,"vwap_ratio":1.0,"price_range_ratio":0.01}' || true)
if ! echo "$RESPONSE" | grep -q "predicted_price"; then
  echo "FAIL: predict did not return predicted_price. Response: $RESPONSE"
  exit 1
fi
echo "Predict smoke test passed."

echo "All smoke tests passed."
