#!/bin/bash
#
# VWAP Bot Watchdog
# Ensures the bot is running, restarts if crashed
# DOES NOT restart if circuit breaker was triggered
#
# Add to crontab:
#   * * * * * /home/clawdbot/clawd/projects/kalshi-vwap-reversal/watchdog.sh >> /home/clawdbot/clawd/projects/kalshi-vwap-reversal/logs/watchdog.log 2>&1
#

BOT_DIR="/home/clawdbot/clawd/projects/kalshi-vwap-reversal"
BOT_SCRIPT="scripts/vwap_reversal_bot.py"
PID_FILE="$BOT_DIR/bot.pid"
LOG_FILE="$BOT_DIR/bot.log"
CIRCUIT_BREAKER_FILE="$BOT_DIR/state/circuit_breaker.json"

cd "$BOT_DIR" || exit 1

# Check if circuit breaker is tripped
if [ -f "$CIRCUIT_BREAKER_FILE" ]; then
    # Parse JSON to check if tripped
    TRIPPED=$(python3 -c "import json; print(json.load(open('$CIRCUIT_BREAKER_FILE')).get('tripped', False))" 2>/dev/null)
    
    if [ "$TRIPPED" = "True" ]; then
        REASON=$(python3 -c "import json; print(json.load(open('$CIRCUIT_BREAKER_FILE')).get('reason', 'unknown'))" 2>/dev/null)
        TRIPPED_AT=$(python3 -c "import json; print(json.load(open('$CIRCUIT_BREAKER_FILE')).get('tripped_at_iso', 'unknown'))" 2>/dev/null)
        
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🚨 CIRCUIT BREAKER TRIPPED - NOT restarting"
        echo "    Reason: $REASON"
        echo "    Tripped at: $TRIPPED_AT"
        echo "    To reset: rm $CIRCUIT_BREAKER_FILE"
        exit 0
    fi
fi

# Check if bot is running
if pgrep -f "vwap_reversal_bot.py" > /dev/null; then
    # Bot is running
    PID=$(pgrep -f "vwap_reversal_bot.py" | head -1)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Bot running (PID $PID)"
else
    # Bot is not running - restart it
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️ Bot not running - restarting..."
    
    # Start the bot
    nohup python3 -u "$BOT_SCRIPT" >> "$LOG_FILE" 2>&1 &
    NEW_PID=$!
    echo $NEW_PID > "$PID_FILE"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Bot restarted (PID $NEW_PID)"
    
    # Wait and verify
    sleep 5
    if pgrep -f "vwap_reversal_bot.py" > /dev/null; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Bot verified running"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ Bot failed to start!"
    fi
fi

# Also check log file size (rotate if > 10MB)
if [ -f "$LOG_FILE" ]; then
    SIZE=$(stat -f%z "$LOG_FILE" 2>/dev/null || stat -c%s "$LOG_FILE" 2>/dev/null)
    if [ "$SIZE" -gt 10000000 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Rotating log file..."
        mv "$LOG_FILE" "$LOG_FILE.$(date '+%Y%m%d_%H%M%S')"
        touch "$LOG_FILE"
    fi
fi
