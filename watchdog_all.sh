#!/bin/bash
# Watchdog for all VWAP reversal bots
# Restarts any bot that has stopped

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment
export $(grep -v '^#' ~/clawd/.env | xargs)
export KALSHI_KEY_PATH=~/clawd/keys/kalshi_private.pem

ASSETS="NEAR SUI DOGE"
LOG_FILE="$SCRIPT_DIR/watchdog.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

get_bot_pid() {
    local asset=$1
    local bot_dir="$SCRIPT_DIR/bots/$asset"
    
    # Find python process with this specific working directory
    for pid in $(pgrep -f "vwap_reversal_bot.py"); do
        local cwd=$(readlink -f /proc/$pid/cwd 2>/dev/null)
        if [ "$cwd" = "$bot_dir" ]; then
            echo $pid
            return
        fi
    done
}

restart_bot() {
    local asset=$1
    local bot_dir="$SCRIPT_DIR/bots/$asset"
    
    log "Restarting $asset bot..."
    cd "$bot_dir"
    nohup python3 vwap_reversal_bot.py --execute --config config.json > bot.log 2>&1 &
    local pid=$!
    cd "$SCRIPT_DIR"
    log "$asset bot started with PID $pid"
    sleep 2
}

check_bot() {
    local asset=$1
    local pid=$(get_bot_pid $asset)
    
    if [ -z "$pid" ]; then
        log "⚠️ $asset bot not running!"
        restart_bot $asset
        return 1
    fi
    
    # Check if bot log is being updated (stale = dead)
    local log_file="$SCRIPT_DIR/bots/$asset/bot.log"
    if [ -f "$log_file" ]; then
        local age=$(( $(date +%s) - $(stat -c %Y "$log_file") ))
        if [ $age -gt 120 ]; then
            log "⚠️ $asset bot log stale (${age}s), killing and restarting..."
            kill $pid 2>/dev/null
            sleep 2
            restart_bot $asset
            return 1
        fi
    fi
    
    return 0
}

log "=== Watchdog started ==="

# Initial startup - ensure all bots running
for asset in $ASSETS; do
    pid=$(get_bot_pid $asset)
    if [ -z "$pid" ]; then
        restart_bot $asset
    else
        log "$asset bot already running (PID $pid)"
    fi
done

# Main loop
while true; do
    sleep 30
    for asset in $ASSETS; do
        check_bot $asset
    done
done
