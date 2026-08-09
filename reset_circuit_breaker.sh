#!/bin/bash
#
# Reset Circuit Breaker
# Run this after manual review to allow bot to restart
#

BOT_DIR="/home/clawdbot/clawd/projects/kalshi-vwap-reversal"
CIRCUIT_BREAKER_FILE="$BOT_DIR/state/circuit_breaker.json"

if [ -f "$CIRCUIT_BREAKER_FILE" ]; then
    echo "Circuit breaker state:"
    cat "$CIRCUIT_BREAKER_FILE"
    echo ""
    
    read -p "Clear circuit breaker and allow bot to restart? (y/N) " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm "$CIRCUIT_BREAKER_FILE"
        echo "✅ Circuit breaker cleared. Watchdog will restart bot on next check."
    else
        echo "Cancelled."
    fi
else
    echo "No circuit breaker file found - bot should be running normally."
fi
