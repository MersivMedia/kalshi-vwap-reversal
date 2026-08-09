#!/usr/bin/env python3
"""
Telegram Notifier for VWAP Reversal Bot

Sends alerts for:
- Trade executions (entry/exit)
- Safety gate blocks
- Circuit breaker trips
- P&L updates
"""

import os
import json
import requests
from typing import Optional
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/home/clawdbot/clawd/.env')

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Notification settings
NOTIFY_ENTRIES = True
NOTIFY_EXITS = True
NOTIFY_BLOCKS = False  # Too noisy - only log these
NOTIFY_CIRCUIT_BREAKER = True
NOTIFY_ERRORS = True


def send_telegram(message: str, parse_mode: str = 'HTML') -> bool:
    """Send message to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    try:
        url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': parse_mode,
            'disable_web_page_preview': True
        }
        resp = requests.post(url, json=payload, timeout=5)
        return resp.status_code == 200
    except Exception as e:
        print(f"[NOTIFY] Telegram error: {e}")
        return False


def notify_entry(symbol: str, side: str, contracts: int, entry_price: float, 
                 stop_loss: float, target: float, gate_info: str = ""):
    """Notify on trade entry."""
    if not NOTIFY_ENTRIES:
        return
    
    emoji = "🟢" if side == "long" else "🔴"
    msg = f"""
{emoji} <b>ENTRY: {side.upper()} {symbol}</b>

📍 Entry: <code>${entry_price:,.2f}</code>
🎯 Target: <code>${target:,.2f}</code>
🛑 Stop: <code>${stop_loss:,.2f}</code>
📦 Size: <code>{contracts}</code> contracts

{gate_info}
"""
    send_telegram(msg.strip())


def notify_exit(symbol: str, side: str, exit_price: float, pnl: float, 
                reason: str, consecutive_losses: int = 0):
    """Notify on trade exit."""
    if not NOTIFY_EXITS:
        return
    
    if pnl >= 0:
        emoji = "✅"
        pnl_str = f"+${pnl:.2f}"
    else:
        emoji = "❌"
        pnl_str = f"-${abs(pnl):.2f}"
    
    loss_warning = ""
    if "STOP" in reason.upper() and consecutive_losses > 0:
        loss_warning = f"\n⚠️ Consecutive losses: {consecutive_losses}/3"
    
    msg = f"""
{emoji} <b>EXIT: {side.upper()} {symbol}</b>

💰 P&L: <code>{pnl_str}</code>
📍 Exit: <code>${exit_price:,.2f}</code>
📝 Reason: {reason}{loss_warning}
"""
    send_telegram(msg.strip())


def notify_circuit_breaker(consecutive_losses: int, reason: str = ""):
    """Notify when circuit breaker trips."""
    if not NOTIFY_CIRCUIT_BREAKER:
        return
    
    msg = f"""
🚨 <b>CIRCUIT BREAKER TRIPPED</b>

⚠️ {consecutive_losses} consecutive stop-losses
🛑 Trading halted until manual reset

To reset:
<code>./reset_circuit_breaker.sh</code>
"""
    send_telegram(msg.strip())


def notify_error(error_type: str, details: str):
    """Notify on critical errors."""
    if not NOTIFY_ERRORS:
        return
    
    msg = f"""
⚠️ <b>BOT ERROR: {error_type}</b>

{details}

Check logs: <code>tail -f bot.log</code>
"""
    send_telegram(msg.strip())


def notify_startup(balance: float):
    """Notify on bot startup."""
    msg = f"""
🤖 <b>VWAP Bot Started</b>

💰 Balance: <code>${balance:.2f}</code>
📊 Assets: BTC, ETH
⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
"""
    send_telegram(msg.strip())


def notify_daily_summary(total_trades: int, wins: int, losses: int, 
                         total_pnl: float, balance: float):
    """Send daily P&L summary."""
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    pnl_emoji = "📈" if total_pnl >= 0 else "📉"
    pnl_str = f"+${total_pnl:.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):.2f}"
    
    msg = f"""
{pnl_emoji} <b>Daily Summary</b>

📊 Trades: {total_trades} ({wins}W / {losses}L)
🎯 Win Rate: {win_rate:.1f}%
💰 P&L: <code>{pnl_str}</code>
💵 Balance: <code>${balance:.2f}</code>
"""
    send_telegram(msg.strip())


# Test if module is run directly
if __name__ == "__main__":
    print("Testing Telegram notifications...")
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
        print("Example:")
        print("  TELEGRAM_BOT_TOKEN=123456:ABC-DEF...")
        print("  TELEGRAM_CHAT_ID=6854193499")
    else:
        result = send_telegram("🧪 VWAP Bot notification test")
        print(f"Test message sent: {result}")
