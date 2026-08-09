#!/bin/bash
# Restart all VWAP reversal bots
# Usage: ./restart_all.sh [--execute]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOT_SCRIPT="$SCRIPT_DIR/../scripts/vwap_reversal_bot.py"

# Get Monday of current week and month for log organization
WEEK_START=$(date -d "last monday" '+%Y-%m-%d' 2>/dev/null || date -d "monday" '+%Y-%m-%d')
MONTH=$(date '+%Y-%m')

# Parse args
EXECUTE_FLAG=""
if [[ "$1" == "--execute" ]]; then
    EXECUTE_FLAG="--execute"
fi

# Load env
export $(grep -v '^#' ~/clawd/.env | xargs)
export KALSHI_KEY_PATH=~/clawd/keys/kalshi_private.pem

# Kill existing bots
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stopping existing bots..."
pkill -f "vwap_reversal_bot.py.*NEAR/config.json" 2>/dev/null || true
pkill -f "vwap_reversal_bot.py.*SUI/config.json" 2>/dev/null || true
pkill -f "vwap_reversal_bot.py.*DOGE/config.json" 2>/dev/null || true
sleep 2

# Start each bot with per-asset logs organized by month
for ASSET in NEAR SUI DOGE; do
    CONFIG="$SCRIPT_DIR/$ASSET/config.json"
    LOG_DIR="$SCRIPT_DIR/$ASSET/logs/$MONTH"
    LOG="$LOG_DIR/week_${WEEK_START}.log"
    
    mkdir -p "$LOG_DIR"
    
    if [[ -f "$CONFIG" ]]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting $ASSET bot..."
        cd "$SCRIPT_DIR/$ASSET"
        nohup python3 "$BOT_SCRIPT" --config "$CONFIG" $EXECUTE_FLAG >> "$LOG" 2>&1 &
        echo "  PID: $! -> $LOG"
    else
        echo "  ⚠️  Config not found: $CONFIG"
    fi
done

sleep 3

# Verify running
echo ""
echo "=== Running Bots ==="
pgrep -fa "vwap_reversal_bot.py" || echo "No bots running!"

echo ""
echo "=== Recent Logs ==="
for ASSET in NEAR SUI DOGE; do
    LOG="$SCRIPT_DIR/$ASSET/logs/$MONTH/week_${WEEK_START}.log"
    if [[ -f "$LOG" ]]; then
        echo "--- $ASSET (last 3 lines) ---"
        tail -3 "$LOG" 2>/dev/null || true
    fi
done

echo ""
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Restart complete"
