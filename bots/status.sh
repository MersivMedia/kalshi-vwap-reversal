#!/bin/bash
# Show status of all VWAP reversal bots

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get Monday of current week and month
WEEK_START=$(date -d "last monday" '+%Y-%m-%d' 2>/dev/null || date -d "monday" '+%Y-%m-%d')
MONTH=$(date '+%Y-%m')

echo "=== VWAP Reversal Bots Status ==="
echo "Time: $(date '+%Y-%m-%d %H:%M:%S UTC')"
echo "Month: $MONTH | Week: $WEEK_START"
echo ""

# Check processes
echo "--- Running Processes ---"
for ASSET in NEAR SUI DOGE; do
    PID=$(pgrep -f "vwap_reversal_bot.py.*$ASSET/config.json" || true)
    if [[ -n "$PID" ]]; then
        echo "✅ $ASSET: PID $PID"
    else
        echo "❌ $ASSET: NOT RUNNING"
    fi
done

echo ""
echo "--- Recent Activity ---"
for ASSET in NEAR SUI DOGE; do
    LOG="$SCRIPT_DIR/$ASSET/logs/$MONTH/week_${WEEK_START}.log"
    if [[ -f "$LOG" ]]; then
        SIZE=$(du -h "$LOG" | cut -f1)
        echo ""
        echo "--- $ASSET ($SIZE) ---"
        tail -5 "$LOG" 2>/dev/null || echo "  (no log)"
    else
        echo ""
        echo "--- $ASSET ---"
        echo "  (no log for this week)"
    fi
done

echo ""
echo "--- Log Archive ---"
for ASSET in NEAR SUI DOGE; do
    COUNT=$(find "$SCRIPT_DIR/$ASSET/logs" -name "*.log" 2>/dev/null | wc -l)
    TOTAL=$(du -sh "$SCRIPT_DIR/$ASSET/logs" 2>/dev/null | cut -f1)
    echo "$ASSET: $COUNT files, $TOTAL total"
done
