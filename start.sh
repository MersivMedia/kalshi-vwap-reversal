#!/bin/bash
cd /home/clawdbot/clawd/projects/kalshi-vwap-reversal
pkill -9 -f vwap_reversal_bot 2>/dev/null
sleep 1
nohup python3 -u scripts/vwap_reversal_bot.py >> bot.log 2>&1 &
echo "Started PID $!"
sleep 3
tail -20 bot.log
