#!/bin/bash
# Run a single VWAP bot for a given asset
# Usage: ./run_bot.sh NEAR|SUI|DOGE

ASSET=$1
if [ -z "$ASSET" ]; then
    echo "Usage: $0 ASSET"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BOT_DIR="$SCRIPT_DIR/bots/$ASSET"

if [ ! -d "$BOT_DIR" ]; then
    echo "Error: Bot directory not found: $BOT_DIR"
    exit 1
fi

# Load environment
export $(grep -v '^#' ~/clawd/.env | xargs)
export KALSHI_KEY_PATH=~/clawd/keys/kalshi_private.pem

cd "$BOT_DIR"
exec python3 vwap_reversal_bot.py --execute --config config.json
