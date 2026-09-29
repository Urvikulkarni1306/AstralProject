#!/bin/sh
set -e
cd "$(dirname "$0")/.."

SOAK="data/soak-2026-09-29"
BIN="./target/debug/astra-record"
DURATION=259200
RECONNECTS=10000

streams() {
  echo "binance-spot-btc binance spot BTC/USDT book_diff wss://data-stream.binance.vision/ws/btcusdt@depth@100ms"
  echo "binance-spot-eth binance spot ETH/USDT book_diff wss://data-stream.binance.vision/ws/ethusdt@depth@100ms"
  echo "bybit-spot-btc bybit spot BTC/USDT book_diff"
  echo "bybit-perp-btc bybit perp BTC/USDT book_diff"
}

cmd_start() {
  cargo build -p astra-record
  mkdir -p "$SOAK"
  streams | while read -r name venue market symbol channel url; do
    out="$SOAK/$name"
    mkdir -p "$out"
    if [ -n "$url" ]; then url_flag="--url $url"; else url_flag=""; fi
    # shellcheck disable=SC2086
    nohup $BIN capture --output "$out" \
      --venue "$venue" --market "$market" --symbol "$symbol" --channel "$channel" \
      --duration-secs "$DURATION" --max-reconnects "$RECONNECTS" $url_flag \
      > "$out.log" 2>&1 &
    echo "$name pid $!"
  done
}

cmd_status() {
  streams | while read -r name venue market symbol channel url; do
    frames=$(grep -h "^frames" "$SOAK/$name.log" 2>/dev/null || echo "not finished")
    if pgrep -f "astra-record capture --output $SOAK/$name" > /dev/null; then
      echo "$name RUNNING ($frames)"
    else
      echo "$name STOPPED ($frames)"
    fi
  done
}

cmd_check() {
  streams | while read -r name venue market symbol channel url; do
    echo "=== $name ==="
    $BIN check --input "$SOAK/$name" 2>&1 | tail -n 14 || true
  done
}

case "${1:-}" in
  start) cmd_start ;;
  status) cmd_status ;;
  check) cmd_check ;;
  *) echo "usage: $0 {start|status|check}" >&2; exit 1 ;;
esac
