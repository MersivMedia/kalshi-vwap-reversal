#!/bin/bash
# Start all VWAP reversal bots

cd "$(dirname "$0")"

# Load environment
export $(grep -v '^#' ~/clawd/.env | xargs)
export KALSHI_KEY_PATH=~/clawd/keys/kalshi_private.pem

# Start each bot
for asset in NEAR SUI DOGE; do
    echo "Starting $asset bot..."
    cd bots/$asset
    nohup python3 vwap_reversal_bot.py --execute --config config.json > bot.log 2>&1 &
    echo "  PID: $!"
    cd ../..
    sleep 2
done

echo ""
echo "All bots started. Check status with: ./status.sh"
