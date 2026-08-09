#!/bin/bash
# Watchdog - restart bots that have stopped

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOT_SCRIPT="$SCRIPT_DIR/../scripts/vwap_reversal_bot.py"

# Get Monday of current week and month
WEEK_START=$(date -d "last monday" '+%Y-%m-%d' 2>/dev/null || date -d "monday" '+%Y-%m-%d')
MONTH=$(date '+%Y-%m')

# Load env
export $(grep -v '^#' ~/clawd/.env | xargs)
export KALSHI_KEY_PATH=~/clawd/keys/kalshi_private.pem

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Check each bot
for ASSET in NEAR SUI DOGE; do
    CONFIG="$SCRIPT_DIR/$ASSET/config.json"
    LOG_DIR="$SCRIPT_DIR/$ASSET/logs/$MONTH"
    LOG="$LOG_DIR/week_${WEEK_START}.log"
    
    if [[ ! -f "$CONFIG" ]]; then
        continue
    fi
    
    # Skip disabled assets
    ENABLED=$(python3 -c "import json; cfg=json.load(open('$CONFIG')); print('true' if list(cfg['assets'].values())[0].get('enabled', False) else 'false')" 2>/dev/null)
    if [[ "$ENABLED" != "true" ]]; then
        continue
    fi
    
    mkdir -p "$LOG_DIR"
    
    PID=$(pgrep -f "vwap_reversal_bot.py.*$ASSET/config.json" || true)
    
    if [[ -z "$PID" ]]; then
        log "⚠️  $ASSET bot not running - restarting..."
        cd "$SCRIPT_DIR/$ASSET"
        nohup python3 "$BOT_SCRIPT" --config "$CONFIG" --execute >> "$LOG" 2>&1 &
        log "✅ $ASSET started with PID $!"
    fi
done
