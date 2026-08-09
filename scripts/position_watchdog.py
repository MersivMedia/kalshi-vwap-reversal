#!/usr/bin/env python3
"""
Position Watchdog - Emergency exit monitor for Kalshi VWAP bot.

Runs independently of the main bot. Monitors open positions and triggers
emergency exits if:
1. Position exceeds maximum loss threshold
2. Main bot hasn't updated state file recently (presumed dead)

This provides a second layer of protection since Kalshi doesn't support
exchange-side stop orders.

Usage:
    python position_watchdog.py              # Run watchdog
    python position_watchdog.py --once       # Single check (for cron)
    python position_watchdog.py --dry-run    # Check without placing orders
"""

import os
import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Add parent for imports
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
_script_dir = Path(__file__).parent.parent
load_dotenv(_script_dir.parent / '.env')
load_dotenv(_script_dir / '.env')

from kalshi_client import KalshiClient
from notifier import notify_error
from config import cfg

# ============================================================
# CONFIGURATION
# ============================================================

# Emergency exit thresholds
MAX_LOSS_PCT = 0.05  # 5% max loss per position before emergency exit
BOT_HEARTBEAT_TIMEOUT = 300  # 5 minutes - if state not updated, assume bot is dead

# State file location
STATE_DIR = Path(__file__).parent.parent / 'state'
STATE_FILE = STATE_DIR / 'bot_state.json'

# Build from config (same as main bot)
CONTRACT_SIZES = {sym: asset.contract_size for sym, asset in cfg.assets.items() if asset.enabled}
PERP_TICKERS = {sym: asset.kalshi_ticker for sym, asset in cfg.assets.items() if asset.enabled}


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [WATCHDOG] {msg}")


def contract_to_spot_price(symbol: str, contract_price: float) -> float:
    """Convert contract price to spot price."""
    return contract_price / CONTRACT_SIZES.get(symbol, 0.0001)


def spot_to_contract_price(symbol: str, spot_price: float) -> float:
    """Convert spot price to contract price."""
    return spot_price * CONTRACT_SIZES.get(symbol, 0.0001)


def get_bot_state() -> dict:
    """Load bot state from state file."""
    if not STATE_FILE.exists():
        return {}
    
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception as e:
        log(f"Error reading state file: {e}")
        return {}


def is_bot_alive(state: dict) -> bool:
    """Check if main bot is still updating state."""
    saved_at = state.get('saved_at', 0)
    if saved_at == 0:
        return False
    
    age = time.time() - saved_at
    return age < BOT_HEARTBEAT_TIMEOUT


def get_current_prices(client: KalshiClient) -> dict:
    """Get current spot prices from Kalshi."""
    prices = {}
    for symbol, ticker in PERP_TICKERS.items():
        try:
            bid, ask = client.get_best_prices(ticker)
            mid = (bid + ask) / 2
            prices[symbol] = contract_to_spot_price(symbol, mid)
        except Exception as e:
            log(f"Error getting price for {symbol}: {e}")
    return prices


def check_position_health(position: dict, current_price: float, exit_targets: dict) -> tuple:
    """
    Check if position needs emergency exit.
    Returns (needs_exit, reason, loss_pct).
    """
    ticker = position['ticker']
    side = position['side']
    
    # Find symbol from ticker first
    symbol = None
    for sym, tick in PERP_TICKERS.items():
        if tick == ticker:
            symbol = sym
            break
    
    if not symbol:
        return (False, "Unknown ticker", 0)
    
    # Now convert entry price using the correct symbol
    entry_price = contract_to_spot_price(symbol, position['entry_price'])
    
    # Calculate current PnL
    if side == 'long':
        pnl_pct = (current_price - entry_price) / entry_price
    else:
        pnl_pct = (entry_price - current_price) / entry_price
    
    # Check if exceeds max loss
    if pnl_pct < -MAX_LOSS_PCT:
        return (True, f"Max loss exceeded: {pnl_pct*100:.2f}%", pnl_pct)
    
    # Check exit targets if bot has them
    targets = exit_targets.get(ticker, {})
    stop_loss = targets.get('stop_loss', 0)
    
    if stop_loss > 0:
        if side == 'long' and current_price <= stop_loss:
            return (True, f"Stop loss hit: ${current_price:.2f} <= ${stop_loss:.2f}", pnl_pct)
        elif side == 'short' and current_price >= stop_loss:
            return (True, f"Stop loss hit: ${current_price:.2f} >= ${stop_loss:.2f}", pnl_pct)
    
    return (False, "Position healthy", pnl_pct)


def emergency_exit(client: KalshiClient, position: dict, reason: str, dry_run: bool = False):
    """Execute emergency exit for a position."""
    ticker = position['ticker']
    side = position['side']
    contracts = position['contracts']
    
    exit_side = 'sell' if side == 'long' else 'buy'
    
    # Get current price
    bid, ask = client.get_best_prices(ticker)
    exit_price = ask if exit_side == 'buy' else bid
    
    log(f"🚨 EMERGENCY EXIT: {ticker} {side.upper()} {contracts} @ ${exit_price:.4f}")
    log(f"   Reason: {reason}")
    
    if dry_run:
        log("   [DRY RUN] Would place exit order")
        return True
    
    try:
        result = client.place_order(
            ticker,
            exit_side,
            contracts,
            exit_price,
            reduce_only=True
        )
        
        if result.get('order') or result.get('order_id'):
            log(f"   ✅ Exit order placed: {result.get('order_id', 'unknown')}")
            
            # Notify via Telegram
            notify_error(f"🚨 WATCHDOG EMERGENCY EXIT\n\n"
                        f"Position: {ticker} {side.upper()} {contracts}\n"
                        f"Reason: {reason}\n"
                        f"Exit price: ${exit_price:.4f}")
            return True
        else:
            log(f"   ❌ Exit failed: {result}")
            return False
            
    except Exception as e:
        log(f"   ❌ Exit error: {e}")
        return False


def run_check(client: KalshiClient, dry_run: bool = False) -> int:
    """
    Run a single watchdog check.
    Returns number of emergency exits triggered.
    """
    exits_triggered = 0
    
    # Get bot state
    bot_state = get_bot_state()
    bot_alive = is_bot_alive(bot_state)
    
    if bot_alive:
        log("Main bot is alive (state updated recently)")
    else:
        log("⚠️ Main bot may be dead (state stale or missing)")
    
    # Get exit targets from bot state
    exit_targets = bot_state.get('exit_targets', {})
    
    # Get current positions from Kalshi
    positions = client.get_positions()
    
    if not positions:
        log("No open positions")
        return 0
    
    log(f"Found {len(positions)} open position(s)")
    
    # Get current prices
    prices = get_current_prices(client)
    
    for pos in positions:
        ticker = pos['ticker']
        
        # Find symbol
        symbol = None
        for sym, tick in PERP_TICKERS.items():
            if tick == ticker:
                symbol = sym
                break
        
        if not symbol or symbol not in prices:
            log(f"  {ticker}: Cannot get price, skipping")
            continue
        
        current_price = prices[symbol]
        needs_exit, reason, loss_pct = check_position_health(pos, current_price, exit_targets)
        
        entry_spot = contract_to_spot_price(symbol, pos['entry_price'])
        log(f"  {symbol} {pos['side'].upper()} {pos['contracts']} @ ${entry_spot:.2f}")
        log(f"    Current: ${current_price:.2f} | PnL: {loss_pct*100:+.2f}%")
        
        if needs_exit:
            log(f"    ⚠️ {reason}")
            
            # Only emergency exit if bot is dead OR loss exceeds threshold
            if not bot_alive or loss_pct < -MAX_LOSS_PCT:
                if emergency_exit(client, pos, reason, dry_run):
                    exits_triggered += 1
            else:
                log(f"    Bot is alive, deferring to main bot for exit")
        else:
            log(f"    ✅ {reason}")
    
    return exits_triggered


def main():
    parser = argparse.ArgumentParser(description='Position Watchdog')
    parser.add_argument('--once', action='store_true', help='Run single check and exit')
    parser.add_argument('--dry-run', action='store_true', help='Check without placing orders')
    parser.add_argument('--interval', type=int, default=30, help='Check interval in seconds')
    args = parser.parse_args()
    
    log("=" * 50)
    log("POSITION WATCHDOG")
    log(f"Max loss threshold: {MAX_LOSS_PCT*100}%")
    log(f"Bot heartbeat timeout: {BOT_HEARTBEAT_TIMEOUT}s")
    if args.dry_run:
        log("🔸 DRY RUN MODE")
    log("=" * 50)
    
    client = KalshiClient()
    
    try:
        balance = client.get_balance()
        log(f"Connected. Balance: ${balance:.2f}")
    except Exception as e:
        log(f"❌ Failed to connect: {e}")
        return 1
    
    if args.once:
        exits = run_check(client, args.dry_run)
        return 0 if exits == 0 else 1
    
    # Continuous monitoring
    log(f"Starting continuous monitoring (interval: {args.interval}s)")
    
    while True:
        try:
            run_check(client, args.dry_run)
        except Exception as e:
            log(f"Error in check: {e}")
        
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
