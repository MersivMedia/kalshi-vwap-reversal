#!/bin/bash
# Check status of all VWAP reversal bots

cd "$(dirname "$0")"

echo "=== VWAP Reversal Bots Status ==="
echo ""

for asset in NEAR SUI DOGE; do
    # Check if bot is running by looking at the working directory
    pid=$(ps aux | grep "python3 vwap_reversal_bot.py" | grep -v grep | awk '{print $2}' | while read p; do
        cwd=$(readlink -f /proc/$p/cwd 2>/dev/null)
        if [[ "$cwd" == *"$asset"* ]]; then
            echo $p
            break
        fi
    done)
    
    if [ -n "$pid" ]; then
        echo "✅ $asset: Running (PID $pid)"
        tail -2 bots/$asset/bot.log 2>/dev/null | sed 's/^/   /'
    else
        # Fallback: check log freshness
        if [ -f "bots/$asset/bot.log" ]; then
            age=$(( $(date +%s) - $(stat -c %Y "bots/$asset/bot.log") ))
            if [ $age -lt 60 ]; then
                echo "✅ $asset: Running (log active ${age}s ago)"
                tail -2 bots/$asset/bot.log 2>/dev/null | sed 's/^/   /'
            else
                echo "❌ $asset: NOT RUNNING (log stale ${age}s)"
            fi
        else
            echo "❌ $asset: NOT RUNNING"
        fi
    fi
    echo ""
done

echo "Raw processes:"
ps aux | grep vwap_reversal | grep -v grep
