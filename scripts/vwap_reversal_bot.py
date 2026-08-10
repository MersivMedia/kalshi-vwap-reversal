#!/usr/bin/env python3
"""
Kalshi Perps VWAP Reversal Bot v2.7.6

Strategy:
- VWAP with configurable σ bands (default ±2σ)
- CVD (Cumulative Volume Delta) for order flow exhaustion
- Entry on band pierce with CVD divergence
- Single VWAP target exit
- Safety gates: Data freshness, Fee hurdle, Spread corridor, ADX, OBI, Circuit breaker

Data feeds: Coinbase WebSocket (VWAP/CVD/ADX) + Kalshi WebSocket (execution/OBI)

Usage:
    python vwap_reversal_bot.py              # Dry run (no orders)
    python vwap_reversal_bot.py --execute    # Live trading
    python vwap_reversal_bot.py --help       # Show help
"""

import sys
sys.stdout.reconfigure(line_buffering=True)

import os
import json
import time
import asyncio
import argparse
import websockets
import base64
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, field
from collections import deque

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

from dotenv import load_dotenv
import pathlib
_script_dir = pathlib.Path(__file__).parent.parent
load_dotenv(_script_dir.parent / '.env')
load_dotenv(_script_dir / '.env')

from state_manager import save_state, load_state, clear_state, set_state_dir
from notifier import (notify_entry, notify_exit, notify_circuit_breaker, 
                      notify_startup, notify_error)
import config as config_module
from config import load_config
from kalshi_client import KalshiClient
from indicators import VWAPState, CVDState, ADXState, Candle

# ============================================================
# CLI ARGUMENTS
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Kalshi Perps VWAP Reversal Bot',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python vwap_reversal_bot.py              # Dry run (signals logged, no orders)
  python vwap_reversal_bot.py --execute    # Live trading
  python vwap_reversal_bot.py -v           # Verbose logging
        """
    )
    parser.add_argument('--execute', '-x', action='store_true',
                        help='Execute live trades (default is dry-run)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose logging')
    parser.add_argument('--config', '-c', type=str, default=None,
                        help='Path to config.json')
    return parser.parse_args()

# Parse args at module load
args = parse_args()
DRY_RUN = not args.execute
VERBOSE = args.verbose

# Reload config if custom path provided, otherwise use default
if args.config:
    cfg = load_config(args.config)
    config_module.cfg = cfg  # Update module-level reference
    # Set per-bot state directory based on config location
    config_path = Path(args.config)
    state_dir = config_path.parent / 'state'
    set_state_dir(state_dir)
else:
    cfg = config_module.cfg

if DRY_RUN:
    print("=" * 60)
    print("🔸 DRY RUN MODE - No orders will be placed")
    print("🔸 Use --execute to enable live trading")
    print("=" * 60)

# ============================================================
# CONFIGURATION (loaded from config.json via config.py)
# ============================================================

# WebSocket URLs
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"

# API credentials (from env)
API_KEY = os.getenv('KALSHI_API_KEY_ID')
KEY_PATH = os.getenv('KALSHI_KEY_PATH', 'keys/kalshi_private.pem')
if not Path(KEY_PATH).is_absolute():
    KEY_PATH = Path(__file__).parent.parent / KEY_PATH

# Build asset mappings from config
PERP_TICKERS = {sym: asset.kalshi_ticker for sym, asset in cfg.assets.items() if asset.enabled}
COINBASE_PRODUCTS = [asset.coinbase_symbol for asset in cfg.assets.values() if asset.enabled]
CONTRACT_SIZES = {sym: asset.contract_size for sym, asset in cfg.assets.items() if asset.enabled}

def spot_to_contract_price(symbol: str, spot_price: float) -> float:
    """Convert spot price to contract price."""
    return spot_price * CONTRACT_SIZES.get(symbol, 0.0001)

def contract_to_spot_price(symbol: str, contract_price: float) -> float:
    """Convert contract price to spot price."""
    return contract_price / CONTRACT_SIZES.get(symbol, 0.0001)

# Map config values to module-level constants (for backwards compatibility)
STD_DEV_MULTIPLIER = cfg.entry_band_sd
MIN_CANDLES_FOR_VWAP = cfg.min_candles_for_vwap
VWAP_RESET_HOUR_UTC = cfg.vwap_reset_hour_utc

CVD_DIVERGENCE_WINDOW_MINUTES = cfg.cvd_divergence_window_minutes
CVD_RESET_HOURS = cfg.cvd_reset_hours

STOP_LOSS_BEYOND_WICK_PCT = cfg.stop_beyond_wick_pct

MAKER_FEE_RATE = cfg.maker_fee_rate
TAKER_FEE_RATE = cfg.taker_fee_rate
TOTAL_FEE_RATE = cfg.total_fee_rate
MIN_PROFIT_MARGIN = cfg.fee_hurdle_min_profit_pct

# === Safety Gates (from config) ===
SPREAD_CORRIDOR_MAX_PCT = cfg.spread_corridor_max_pct
ADX_PERIOD = cfg.adx_period
ADX_TREND_THRESHOLD = cfg.adx_trend_threshold  # Default fallback
ADX_COOLDOWN_THRESHOLD = cfg.adx_cooldown_threshold

# Per-asset ADX thresholds (from config)
ADX_THRESHOLDS = {sym: asset.adx_threshold for sym, asset in cfg.assets.items() if asset.enabled}

def get_adx_threshold(symbol: str) -> float:
    """Get the ADX threshold for a specific asset."""
    return ADX_THRESHOLDS.get(symbol, ADX_TREND_THRESHOLD)
OBI_MIN_THRESHOLD = cfg.obi_min_threshold
OBI_DEPTH_LEVELS = cfg.obi_depth_levels
MAX_DATA_LAG_SECONDS = cfg.data_freshness_max_lag_seconds
CIRCUIT_BREAKER_CONSECUTIVE_LOSSES = cfg.circuit_breaker_consecutive_losses

# === Risk Management (from config) ===
MAX_RISK_PER_TRADE_PCT = cfg.max_risk_per_trade_pct
MAX_MARGIN_PCT = cfg.max_margin_pct
MAX_LEVERAGE = cfg.max_leverage
MIN_STOP_DISTANCE_PCT = cfg.min_stop_distance_pct

# === Rate Limiting (from config) ===
POLL_INTERVAL = cfg.poll_interval_seconds
MAX_TRADES_PER_HOUR = cfg.max_trades_per_hour

# Logging
LOG_DIR = Path(__file__).parent.parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# Status logging interval
STATUS_LOG_INTERVAL = 30  # seconds

# State persistence
STATE_SAVE_INTERVAL = 60  # Save state every 60 seconds

# ============================================================
# ASYNC HELPERS
# ============================================================

async def run_sync(func, *args, **kwargs):
    """Run a synchronous function in a thread pool to avoid blocking the event loop."""
    import functools
    return await asyncio.get_event_loop().run_in_executor(
        None, functools.partial(func, *args, **kwargs)
    )


# ============================================================
# INDICATORS - imported from indicators.py
# ============================================================
# VWAPState, CVDState, ADXState, Candle are now in indicators.py

# ============================================================
# SAFETY GATES
# ============================================================

def check_data_freshness() -> Tuple[bool, str]:
    """
    Gate 0: Ensure websocket data is fresh.
    Prevents trading on stale data during network issues.
    
    Returns (is_fresh, lag_info).
    """
    current_time = time.time()
    
    cb_lag = current_time - state.ws_last_message.get('coinbase', 0)
    kalshi_lag = current_time - state.ws_last_message.get('kalshi', 0)
    
    # If we've never received a message, allow (startup grace period)
    if state.ws_last_message.get('coinbase', 0) == 0 or state.ws_last_message.get('kalshi', 0) == 0:
        return (True, "Startup grace period")
    
    if cb_lag > MAX_DATA_LAG_SECONDS:
        return (False, f"Coinbase data stale: {cb_lag:.2f}s lag")
    
    if kalshi_lag > MAX_DATA_LAG_SECONDS:
        return (False, f"Kalshi data stale: {kalshi_lag:.2f}s lag")
    
    return (True, f"Data fresh (CB: {cb_lag:.2f}s, Kalshi: {kalshi_lag:.2f}s)")


def check_spread_corridor(symbol: str) -> Tuple[bool, float]:
    """
    Gate 2: Check if Kalshi and Coinbase prices are within acceptable divergence.
    Returns (is_safe, divergence_pct).
    
    If divergence > SPREAD_CORRIDOR_MAX_PCT, trading should halt.
    """
    kalshi_price = state.kalshi_prices.get(symbol, 0)
    coinbase_price = state.coinbase_prices.get(symbol, 0)
    
    if kalshi_price == 0 or coinbase_price == 0:
        return (True, 0.0)  # Can't check, allow trading
    
    divergence = abs(kalshi_price - coinbase_price) / coinbase_price
    is_safe = divergence <= SPREAD_CORRIDOR_MAX_PCT
    
    return (is_safe, divergence)


def check_adx_hysteresis(symbol: str) -> Tuple[bool, str]:
    """
    Gate 3: ADX trend filter with hysteresis to prevent flapping.
    
    - Block when ADX >= 25 (trend detected)
    - Stay blocked until ADX < 22 (trend must cool down significantly)
    
    This prevents rapid on/off switching at the threshold boundary.
    """
    
    if symbol not in state.adx_states or not state.adx_states[symbol].is_valid():
        return (True, "ADX not ready")
    
    adx = state.adx_states[symbol].get_adx()
    is_blocked = state.adx_trend_blocked.get(symbol, False)
    
    # Get per-asset ADX threshold
    threshold = get_adx_threshold(symbol)
    cooldown = ADX_COOLDOWN_THRESHOLD
    
    if is_blocked:
        # Currently blocked - need ADX to cool down below cooldown threshold to unblock
        if adx < cooldown:
            state.adx_trend_blocked[symbol] = False
            log(f"🔄 {symbol} ADX cooled to {adx:.1f} - mean-reversion unlocked")
            return (True, f"ADX cooled: {adx:.1f} < {cooldown}")
        else:
            return (False, f"ADX still trending: {adx:.1f} (needs < {cooldown} to unlock)")
    else:
        # Not blocked - check if we should block (using per-asset threshold)
        if adx >= threshold:
            state.adx_trend_blocked[symbol] = True
            log(f"🚫 {symbol} ADX trending at {adx:.1f} - mean-reversion blocked (threshold: {threshold})")
            return (False, f"ADX trending: {adx:.1f} >= {threshold}")
        else:
            return (True, f"ADX ranging: {adx:.1f} < {threshold}")


async def calculate_obi(client, ticker: str) -> float:
    """
    Calculate Order Book Imbalance (OBI) from Kalshi orderbook.
    
    OBI = (Bid Volume - Ask Volume) / Total Volume
    
    Range: -1.0 (all asks) to +1.0 (all bids)
    Positive OBI = more resting buy orders (supports longs)
    Negative OBI = more resting sell orders (supports shorts)
    """
    try:
        ob = await run_sync(client.get_orderbook, ticker, OBI_DEPTH_LEVELS)
        orderbook = ob.get('orderbook', {})
        
        bids = orderbook.get('bids', [])
        asks = orderbook.get('asks', [])
        
        # Sum volume at each level
        # Format: [[price, size], ...]
        bid_volume = sum(float(level[1]) for level in bids[:OBI_DEPTH_LEVELS])
        ask_volume = sum(float(level[1]) for level in asks[:OBI_DEPTH_LEVELS])
        
        total_volume = bid_volume + ask_volume
        
        if total_volume == 0:
            return 0.0
        
        obi = (bid_volume - ask_volume) / total_volume
        return obi
        
    except Exception as e:
        log(f"OBI calculation error for {ticker}: {e}")
        return 0.0


def check_circuit_breaker() -> Tuple[bool, str]:
    """
    Gate 5: Circuit breaker - halt trading after consecutive losses.
    Prevents catastrophic drawdown during adverse conditions.
    Saves state to disk so watchdog won't restart the bot.
    """
    
    if state.circuit_breaker_tripped:
        return (False, f"Circuit breaker TRIPPED: {state.consecutive_losses} consecutive losses")
    
    if state.consecutive_losses >= CIRCUIT_BREAKER_CONSECUTIVE_LOSSES:
        state.circuit_breaker_tripped = True
        log(f"🚨 CIRCUIT BREAKER TRIPPED: {state.consecutive_losses} consecutive losses!")
        
        # Save to disk so watchdog knows not to restart
        from state_manager import save_circuit_breaker
        save_circuit_breaker(state.consecutive_losses)
        
        # Send Telegram alert
        notify_circuit_breaker(state.consecutive_losses)
        
        return (False, f"Circuit breaker triggered: {state.consecutive_losses} consecutive losses")
    
    return (True, f"Circuit breaker OK ({state.consecutive_losses} consecutive losses)")


async def validate_entry_gates(client, symbol: str, side: str, entry_price: float, target_price: float) -> Tuple[bool, str]:
    """
    Run all safety gates before allowing entry.
    Returns (can_enter, reason).
    
    Gates:
    0. Data freshness (websocket lag < 1s)
    1. Fee hurdle (profit > fees + margin)
    2. Spread corridor (Kalshi/Coinbase alignment)
    3. ADX trend filter with hysteresis
    4. OBI confirmation (orderbook supports direction)
    5. Circuit breaker (consecutive losses)
    """
    
    ticker = PERP_TICKERS[symbol]
    
    # Gate 0: Data Freshness
    is_fresh, freshness_info = check_data_freshness()
    if not is_fresh:
        return (False, f"Data stale: {freshness_info}")
    
    # Gate 5: Circuit Breaker (check early to fail fast)
    cb_ok, cb_info = check_circuit_breaker()
    if not cb_ok:
        return (False, cb_info)
    
    # Gate 1: Fee Hurdle
    profit_distance = abs(entry_price - target_price)
    min_required = entry_price * (TOTAL_FEE_RATE + MIN_PROFIT_MARGIN)
    
    if profit_distance < min_required:
        return (False, f"Fee hurdle: profit ${profit_distance:.2f} < min ${min_required:.2f}")
    
    # Gate 2: Spread Corridor
    is_safe, divergence = check_spread_corridor(symbol)
    if not is_safe:
        state.trading_halted = True
        state.halt_reason = f"{symbol} spread corridor breach: {divergence*100:.3f}%"
        return (False, f"Spread corridor: {divergence*100:.3f}% > {SPREAD_CORRIDOR_MAX_PCT*100:.2f}% max")
    else:
        state.trading_halted = False
        state.halt_reason = ""
    
    # Gate 3: ADX Trend Filter with Hysteresis
    adx_ok, adx_info = check_adx_hysteresis(symbol)
    if not adx_ok:
        return (False, adx_info)
    
    # Gate 4: OBI Confirmation (async)
    obi = await calculate_obi(client, ticker)
    
    if side == 'long' and obi < OBI_MIN_THRESHOLD:
        return (False, f"OBI unsupportive for long: {obi:+.2f} < +{OBI_MIN_THRESHOLD}")
    elif side == 'short' and obi > -OBI_MIN_THRESHOLD:
        return (False, f"OBI unsupportive for short: {obi:+.2f} > -{OBI_MIN_THRESHOLD}")
    
    # All gates passed
    adx_value = state.adx_states[symbol].get_adx() if symbol in state.adx_states and state.adx_states[symbol].is_valid() else 0.0
    return (True, f"All gates passed (OBI: {obi:+.2f}, ADX: {adx_value:.1f})")


# ============================================================
# GLOBAL STATE
# ============================================================
# BOT STATE - All mutable state in one place for testability
# ============================================================

@dataclass
class BotState:
    """Encapsulates all mutable bot state for cleaner testing and lifecycle."""
    
    # WebSocket connection state
    ws_connected: Dict[str, bool] = field(default_factory=lambda: {'kalshi': False, 'coinbase': False})
    ws_last_message: Dict[str, float] = field(default_factory=lambda: {'kalshi': 0.0, 'coinbase': 0.0})
    
    # Per-symbol indicator state
    vwap_states: Dict[str, VWAPState] = field(default_factory=dict)
    cvd_states: Dict[str, CVDState] = field(default_factory=dict)
    adx_states: Dict[str, ADXState] = field(default_factory=dict)
    
    # Price history - COINBASE ONLY for swing detection (clean data source)
    coinbase_price_history: Dict[str, deque] = field(default_factory=dict)
    
    # Cross-venue price tracking for spread corridor
    kalshi_prices: Dict[str, float] = field(default_factory=dict)
    coinbase_prices: Dict[str, float] = field(default_factory=dict)
    
    # Trading halt state
    trading_halted: bool = False
    halt_reason: str = ""
    
    # ADX Hysteresis state (prevents flapping at threshold boundary)
    adx_trend_blocked: Dict[str, bool] = field(default_factory=dict)
    
    # Circuit breaker state
    consecutive_losses: int = 0
    circuit_breaker_tripped: bool = False
    
    # Exit targets - stored locally for each position (by ticker)
    exit_targets: Dict[str, dict] = field(default_factory=dict)
    
    # Pending orders - track unfilled orders
    pending_orders: Dict[str, dict] = field(default_factory=dict)
    
    # Rate limiting
    trades_this_hour: int = 0
    last_hour: int = field(default_factory=lambda: datetime.now().hour)
    
    # Session PnL tracking
    total_pnl: float = 0.0
    
    # Status logging
    last_status_log: float = 0.0
    last_state_save: float = 0.0


# Global bot state instance
state = BotState()

# Constants (not state)
STALE_ORDER_PRICE_THRESHOLD = 0.005  # 0.5% price deviation = cancel order

# ============================================================
# LOGGING
# ============================================================

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def log_trade(data: dict):
    """Log trade to JSONL file."""
    with open(LOG_DIR / 'trades.jsonl', 'a') as f:
        f.write(json.dumps({**data, 'logged_at': datetime.now().isoformat()}) + '\n')

def log_status(data: dict):
    """Log status snapshot to JSONL file."""
    with open(LOG_DIR / 'status.jsonl', 'a') as f:
        f.write(json.dumps({**data, 'logged_at': datetime.now().isoformat()}) + '\n')

def log_data(data: dict):
    """Log market data to JSONL file."""
    with open(LOG_DIR / 'market_data.jsonl', 'a') as f:
        f.write(json.dumps({**data, 'logged_at': datetime.now().isoformat()}) + '\n')

# ============================================================
# KALSHI CLIENT - imported from kalshi_client.py
# ============================================================
# (KalshiClient is now in a separate module with rate limiting and retries)

# ============================================================
# WEBSOCKET HANDLERS
# ============================================================

# Watchdog settings
WS_HEARTBEAT_TIMEOUT = 30   # Force reconnect if no message in 30 seconds
WS_WATCHDOG_INTERVAL = 5    # Check every 5 seconds

# Global reference to WebSocket tasks for watchdog to restart them
ws_tasks = {}


async def ws_watchdog():
    """Monitor WebSocket connections and force reconnect on silent disconnect."""
    while True:
        try:
            await asyncio.sleep(WS_WATCHDOG_INTERVAL)
            now = time.time()
            
            # Check Kalshi WebSocket
            kalshi_last = state.ws_last_message.get('kalshi', 0)
            kalshi_age = now - kalshi_last if kalshi_last > 0 else 0
            
            if kalshi_last > 0 and kalshi_age > WS_HEARTBEAT_TIMEOUT:
                log(f"[WATCHDOG] Kalshi WS stale ({kalshi_age:.0f}s), forcing reconnect...")
                if 'kalshi' in ws_tasks and not ws_tasks['kalshi'].done():
                    ws_tasks['kalshi'].cancel()
                    try:
                        await ws_tasks['kalshi']
                    except asyncio.CancelledError:
                        pass
                # Restart the task
                ws_tasks['kalshi'] = asyncio.create_task(kalshi_websocket(), name="kalshi_ws")
                state.ws_last_message['kalshi'] = now  # Reset to avoid immediate re-trigger
            
            # Check Coinbase WebSocket  
            coinbase_last = state.ws_last_message.get('coinbase', 0)
            coinbase_age = now - coinbase_last if coinbase_last > 0 else 0
            
            if coinbase_last > 0 and coinbase_age > WS_HEARTBEAT_TIMEOUT:
                log(f"[WATCHDOG] Coinbase WS stale ({coinbase_age:.0f}s), forcing reconnect...")
                if 'coinbase' in ws_tasks and not ws_tasks['coinbase'].done():
                    ws_tasks['coinbase'].cancel()
                    try:
                        await ws_tasks['coinbase']
                    except asyncio.CancelledError:
                        pass
                # Restart the task
                ws_tasks['coinbase'] = asyncio.create_task(coinbase_websocket(), name="coinbase_ws")
                state.ws_last_message['coinbase'] = now
                
        except asyncio.CancelledError:
            log("[WATCHDOG] Shutting down...")
            break
        except Exception as e:
            log(f"[WATCHDOG] Error: {e}")


async def kalshi_websocket():
    """Kalshi WebSocket for real-time perp data with exponential backoff."""
    
    reconnect_delay = 1.0  # Start with 1 second
    max_delay = 60.0  # Cap at 60 seconds
    
    while True:
        try:
            ts = str(int(time.time() * 1000))
            path = "/trade-api/ws/v2"
            
            with open(KEY_PATH, 'rb') as f:
                private_key = serialization.load_pem_private_key(
                    f.read(), password=None, backend=default_backend()
                )
            
            msg = f"{ts}GET{path}".encode('utf-8')
            signature = private_key.sign(
                msg,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256()
            )
            
            headers = {
                'KALSHI-ACCESS-KEY': API_KEY,
                'KALSHI-ACCESS-SIGNATURE': base64.b64encode(signature).decode('utf-8'),
                'KALSHI-ACCESS-TIMESTAMP': ts,
            }
            
            log("[KALSHI WS] Connecting...")
            async with websockets.connect(KALSHI_WS_URL, extra_headers=headers, ping_interval=20, ping_timeout=10) as ws:
                state.ws_connected['kalshi'] = True
                reconnect_delay = 1.0  # Reset on successful connection
                log("[KALSHI WS] Connected!")
                
                # Subscribe to ticker and trades
                for ticker in PERP_TICKERS.values():
                    subscribe_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {"channels": ["ticker", "trade"], "market_tickers": [ticker]}
                    }
                    await ws.send(json.dumps(subscribe_msg))
                
                log(f"[KALSHI WS] Subscribed to {list(PERP_TICKERS.values())}")
                
                async for message in ws:
                    try:
                        data = json.loads(message)
                        handle_kalshi_message(data)
                    except json.JSONDecodeError:
                        pass
        
        except asyncio.CancelledError:
            log("[KALSHI WS] Cancelled, shutting down...")
            break
        except Exception as e:
            state.ws_connected['kalshi'] = False
            log(f"[KALSHI WS] Error: {e}, reconnecting in {reconnect_delay:.1f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)  # Exponential backoff


def handle_kalshi_message(data: dict):
    """Process Kalshi WebSocket message."""
    state.ws_last_message['kalshi'] = time.time()
    
    msg_type = data.get('type')
    
    if msg_type == 'trade':
        # Trade execution - track Kalshi price for spread corridor only
        # NOTE: VWAP/CVD/ADX are driven by Coinbase to avoid mixing venues
        ticker = data.get('msg', {}).get('ticker', '')
        symbol = next((s for s, t in PERP_TICKERS.items() if t == ticker), None)
        
        if symbol:
            trade = data.get('msg', {})
            contract_price = float(trade.get('price', 0))
            
            # Convert contract price to spot for spread corridor tracking
            spot_price = contract_to_spot_price(symbol, contract_price)
            state.kalshi_prices[symbol] = spot_price
    
    elif msg_type == 'ticker':
        # Price update
        ticker = data.get('msg', {}).get('ticker', '')
        symbol = next((s for s, t in PERP_TICKERS.items() if t == ticker), None)
        
        if symbol:
            contract_price = float(data.get('msg', {}).get('last_price', 0))
            if contract_price > 0:
                # Convert to spot price for spread corridor only
                # NOTE: Price history is Coinbase-only to avoid mixing venues
                spot_price = contract_to_spot_price(symbol, contract_price)
                state.kalshi_prices[symbol] = spot_price


async def coinbase_websocket():
    """Coinbase Advanced Trade WebSocket for live market data with exponential backoff."""
    
    # Use config-driven product list
    products = COINBASE_PRODUCTS
    reconnect_delay = 1.0  # Start with 1 second
    max_delay = 60.0  # Cap at 60 seconds
    
    while True:
        try:
            log("[COINBASE WS] Connecting...")
            async with websockets.connect(COINBASE_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                state.ws_connected['coinbase'] = True
                reconnect_delay = 1.0  # Reset on successful connection
                log("[COINBASE WS] Connected!")
                
                # Advanced Trade API subscribe format
                subscribe_msg = {
                    "type": "subscribe",
                    "product_ids": products,
                    "channel": "market_trades"
                }
                await ws.send(json.dumps(subscribe_msg))
                
                async for message in ws:
                    try:
                        data = json.loads(message)
                        handle_coinbase_message(data)
                    except json.JSONDecodeError:
                        pass
                        
        except asyncio.CancelledError:
            log("[COINBASE WS] Cancelled, shutting down...")
            break
        except Exception as e:
            state.ws_connected['coinbase'] = False
            log(f"[COINBASE WS] Error: {e}, reconnecting in {reconnect_delay:.1f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)  # Exponential backoff


def handle_coinbase_message(data: dict):
    """Process Coinbase Advanced Trade WebSocket message."""
    state.ws_last_message['coinbase'] = time.time()
    
    # Advanced Trade API format: market_trades channel
    if data.get('channel') == 'market_trades' and 'events' in data:
        for event in data['events']:
            for trade in event.get('trades', []):
                product = trade.get('product_id', '')
                symbol = product.split('-')[0]
                
                if symbol in PERP_TICKERS:
                    price = float(trade.get('price', 0))
                    size = float(trade.get('size', 0))
                    side = trade.get('side', 'UNKNOWN')  # 'BUY' or 'SELL'
                    
                    # Track Coinbase spot price for spread corridor
                    state.coinbase_prices[symbol] = price
                    
                    # Update VWAP (candle-based)
                    if symbol not in state.vwap_states:
                        state.vwap_states[symbol] = VWAPState(
                            reset_hour_utc=cfg.vwap_reset_hour_utc,
                            min_candles=cfg.min_candles_for_vwap
                        )
                    
                    # Track candle count before processing
                    prev_candle_count = len(state.vwap_states[symbol].candles)
                    state.vwap_states[symbol].process_trade(price, size, side)
                    new_candle_count = len(state.vwap_states[symbol].candles)
                    
                    # Update CVD
                    if symbol not in state.cvd_states:
                        state.cvd_states[symbol] = CVDState(
                            divergence_window_minutes=cfg.cvd_divergence_window_minutes,
                            reset_hours=cfg.cvd_reset_hours
                        )
                    state.cvd_states[symbol].add_trade(price, size, side)
                    
                    # Update ADX when a new candle completes
                    if symbol not in state.adx_states:
                        state.adx_states[symbol] = ADXState(period=cfg.adx_period)
                    
                    # If a candle just completed, update ADX with it
                    if new_candle_count > prev_candle_count and state.vwap_states[symbol].candles:
                        completed = state.vwap_states[symbol].candles[-1]
                        state.adx_states[symbol].add_candle(completed.high, completed.low, completed.close)
                    
                    # Update Coinbase-only price history for swing detection
                    if symbol not in state.coinbase_price_history:
                        state.coinbase_price_history[symbol] = deque(maxlen=100)
                    state.coinbase_price_history[symbol].append({'time': time.time(), 'price': price})

# ============================================================
# STRATEGY LOGIC
# ============================================================

# NOTE: No daily VWAP reset - we use a rolling window instead


def check_entry_signal(symbol: str, current_price: float) -> Optional[dict]:
    """
    Check for VWAP reversal entry signal.
    
    Entry Rules:
    - Price > VWAP + 2σ AND CVD falling/flat → SHORT
    - Price < VWAP - 2σ AND CVD rising/flat → LONG
    
    Target: VWAP central line
    Stop: Last swing high/low ± 0.1%
    """
    
    # Rate limit
    current_hour = datetime.now().hour
    if current_hour != state.last_hour:
        state.trades_this_hour = 0
        state.last_hour = current_hour
    
    if state.trades_this_hour >= MAX_TRADES_PER_HOUR:
        return None
    
    # Skip if already in position or pending order
    # Check state.exit_targets (which tracks positions we're managing)
    ticker = PERP_TICKERS.get(symbol)
    if ticker in state.exit_targets or symbol in state.pending_orders:
        return None
    
    # Get VWAP state
    if symbol not in state.vwap_states:
        return None
    
    vwap_state = state.vwap_states[symbol]
    cvd_state = state.cvd_states.get(symbol, CVDState(
        divergence_window_minutes=cfg.cvd_divergence_window_minutes,
        reset_hours=cfg.cvd_reset_hours
    ))
    
    # Require valid VWAP data
    if not vwap_state.is_valid():
        return None
    
    # Get VWAP and bands
    lower_band, vwap, upper_band = vwap_state.get_bands(STD_DEV_MULTIPLIER)
    
    if vwap == 0:
        return None
    
    # Get CVD trend with configurable strictness
    cvd_trend = cvd_state.get_cvd_trend()
    
    # Check minimum CVD delta if configured
    cvd_delta_ok = True
    if cfg.cvd_min_delta_pct > 0:
        # Get CVD change over window
        cvd_now = cvd_state.running_cvd
        cvd_start, _ = cvd_state.get_cvd_at_time(cfg.cvd_divergence_window_minutes)
        cvd_change = abs(cvd_now - cvd_start)
        cvd_base = max(abs(cvd_now), abs(cvd_start), 1.0)  # Avoid div by zero
        cvd_delta_pct = cvd_change / cvd_base
        cvd_delta_ok = cvd_delta_pct >= cfg.cvd_min_delta_pct
    
    # Determine acceptable CVD trends based on config
    if cfg.cvd_strict_divergence:
        # Strict mode: require clear directional divergence (no 'flat')
        short_cvd_ok = cvd_trend == 'falling' and cvd_delta_ok
        long_cvd_ok = cvd_trend == 'rising' and cvd_delta_ok
    else:
        # Default: accept flat as "not confirming the move"
        short_cvd_ok = cvd_trend in ('falling', 'flat') and cvd_delta_ok
        long_cvd_ok = cvd_trend in ('rising', 'flat') and cvd_delta_ok
    
    # SHORT SIGNAL: Price > VWAP + 2σ AND CVD falling (or flat if not strict)
    if current_price >= upper_band:
        if short_cvd_ok:
            # Find last swing high for stop (use recent window, not all history)
            recent_prices = list(state.coinbase_price_history.get(symbol, [{'price': current_price}]))[-20:]
            swing_high = max(p['price'] for p in recent_prices)
            stop_loss = swing_high * (1 + STOP_LOSS_BEYOND_WICK_PCT)
            # Apply minimum stop distance (consistent with recovery/manage paths)
            min_stop = current_price * (1 + MIN_STOP_DISTANCE_PCT)
            stop_loss = max(stop_loss, min_stop)
            
            log(f"🔴 SHORT SIGNAL: {symbol}")
            log(f"   Price ${current_price:,.2f} > Upper ${upper_band:,.2f}")
            log(f"   Target VWAP: ${vwap:,.2f} | Stop: ${stop_loss:,.2f}")
            log(f"   CVD trend: {cvd_trend}")
            
            # Fee hurdle checked in validate_entry_gates
            return {
                'symbol': symbol,
                'ticker': PERP_TICKERS[symbol],
                'side': 'short',
                'entry_price': current_price,
                'stop_loss': stop_loss,
                'target_price': vwap,
                'reason': f'Price above +2σ, CVD {cvd_trend}'
            }
    
    # LONG SIGNAL: Price < VWAP - 2σ AND CVD rising (or flat if not strict)
    elif current_price <= lower_band:
        if long_cvd_ok:
            # Find last swing low for stop (use recent window, not all history)
            recent_prices = list(state.coinbase_price_history.get(symbol, [{'price': current_price}]))[-20:]
            swing_low = min(p['price'] for p in recent_prices)
            stop_loss = swing_low * (1 - STOP_LOSS_BEYOND_WICK_PCT)
            # Apply minimum stop distance (consistent with recovery/manage paths)
            min_stop = current_price * (1 - MIN_STOP_DISTANCE_PCT)
            stop_loss = min(stop_loss, min_stop)
            
            log(f"🟢 LONG SIGNAL: {symbol}")
            log(f"   Price ${current_price:,.2f} < Lower ${lower_band:,.2f}")
            log(f"   Target VWAP: ${vwap:,.2f} | Stop: ${stop_loss:,.2f}")
            log(f"   CVD trend: {cvd_trend}")
            
            # Fee hurdle checked in validate_entry_gates
            return {
                'symbol': symbol,
                'ticker': PERP_TICKERS[symbol],
                'side': 'long',
                'entry_price': current_price,
                'stop_loss': stop_loss,
                'target_price': vwap,
                'reason': f'Price below -2σ, CVD {cvd_trend}'
            }
    
    return None


async def calculate_position_size(client: KalshiClient, symbol: str, 
                                  entry_price: float, stop_loss: float) -> int:
    """
    Calculate position size with conservative margin constraints from config.
    
    Uses config values for:
    - max_risk_per_trade_pct: Max loss at stop as % of balance
    - max_margin_pct: Max margin usage as % of balance  
    - max_leverage: Effective leverage for margin calculation
    - min_stop_distance_pct: Floor for stop distance
    """
    balance = await run_sync(client.get_balance)
    if balance <= 0:
        return 0
    
    contract_size = CONTRACT_SIZES.get(symbol, 0.0001)
    contract_price = entry_price * contract_size
    
    # Stop distance in spot price
    stop_distance_spot = abs(entry_price - stop_loss)
    
    # Minimum stop distance from config (prevents oversized positions from tight stops)
    min_stop_distance = entry_price * MIN_STOP_DISTANCE_PCT
    if stop_distance_spot < min_stop_distance:
        log(f"  ⚠️ Stop too tight ({stop_distance_spot:.2f}), using min {min_stop_distance:.2f}")
        stop_distance_spot = min_stop_distance
    
    # Loss per contract at stop
    loss_per_contract = stop_distance_spot * contract_size
    
    # CONSTRAINT 1: Max risk (loss at stop) from config
    max_risk = balance * MAX_RISK_PER_TRADE_PCT
    contracts_from_risk = int(max_risk / loss_per_contract)
    
    # CONSTRAINT 2: Max margin usage from config
    max_margin = balance * MAX_MARGIN_PCT
    margin_per_contract = contract_price / MAX_LEVERAGE
    contracts_from_margin = int(max_margin / margin_per_contract)
    
    # Use the most restrictive constraint
    contracts = min(contracts_from_risk, contracts_from_margin)
    contracts = max(1, contracts)  # At least 1
    
    # Calculate actual values
    actual_margin = contracts * margin_per_contract
    actual_margin_pct = (actual_margin / balance) * 100
    actual_risk = contracts * loss_per_contract
    actual_risk_pct = (actual_risk / balance) * 100
    
    # Determine which constraint was binding
    if contracts_from_margin < contracts_from_risk:
        constraint = "MARGIN-LIMITED"
    else:
        constraint = "RISK-LIMITED"
    
    log(f"  Position sizing [{constraint}]:")
    log(f"    Balance: ${balance:.2f}, Stop distance: ${stop_distance_spot:.2f}")
    log(f"    Contracts: {contracts} (risk-cap: {contracts_from_risk}, margin-cap: {contracts_from_margin})")
    log(f"    Est margin: ${actual_margin:.2f} ({actual_margin_pct:.1f}%)")
    log(f"    Risk at stop: ${actual_risk:.2f} ({actual_risk_pct:.1f}%)")
    
    return contracts


async def execute_entry(client: KalshiClient, signal: dict):
    """Execute entry trade after passing all safety gates."""
    
    symbol = signal['symbol']
    ticker = signal['ticker']
    
    log("=" * 50)
    log(f"ENTRY SIGNAL: {signal['side'].upper()} {symbol}")
    log(f"  {signal['reason']}")
    log(f"  Entry: ${signal['entry_price']:,.2f}")
    log(f"  Stop: ${signal['stop_loss']:,.2f}")
    log(f"  Target (VWAP): ${signal['target_price']:,.2f}")
    
    # === SAFETY GATES (async) ===
    can_enter, gate_reason = await validate_entry_gates(
        client, 
        symbol, 
        signal['side'], 
        signal['entry_price'], 
        signal['target_price']
    )
    
    if not can_enter:
        log(f"  ❌ BLOCKED: {gate_reason}")
        log_trade({
            'type': 'signal_blocked',
            'symbol': symbol,
            'side': signal['side'],
            'reason': gate_reason,
            'entry_price': signal['entry_price']
        })
        return
    
    log(f"  ✅ {gate_reason}")
    
    # Calculate size (number of contracts) - async to avoid blocking
    contracts = await calculate_position_size(client, symbol, signal['entry_price'], signal['stop_loss'])
    if contracts <= 0:
        log("  Size too small, skipping")
        return
    
    # Convert spot price to contract price for order
    contract_price = spot_to_contract_price(symbol, signal['entry_price'])
    
    log(f"  Contracts: {contracts}")
    log(f"  Contract price: ${contract_price:.4f}")
    
    # DRY RUN: Log but don't place order
    if DRY_RUN:
        log(f"  🔸 DRY RUN: Would place {signal['side'].upper()} order for {contracts} contracts")
        log_trade({
            'type': 'dry_run_signal',
            **signal,
            'contracts': contracts,
            'would_execute': True
        })
        return
    
    # Place order (with post_only for maker fees) - async to avoid blocking
    result = await run_sync(
        client.place_order,
        ticker, 
        signal['side'], 
        contracts, 
        contract_price,
        False  # reduce_only
    )
    
    # Check for post_only rejection (order would cross spread)
    if result.get('error') or result.get('status') == 'rejected':
        error_msg = result.get('error', result.get('reason', 'unknown'))
        
        # Handle post_only rejection specifically
        if 'post_only' in str(error_msg).lower() or 'cross' in str(error_msg).lower():
            log(f"  ⚠️ POST_ONLY REJECTED: Order would cross spread. State reset.")
            log_trade({
                'type': 'post_only_rejected',
                'symbol': symbol,
                'side': signal['side'],
                'reason': error_msg,
                'entry_price': signal['entry_price']
            })
        else:
            log(f"  ❌ Order failed: {error_msg}")
            log_trade({
                'type': 'order_failed',
                'symbol': symbol,
                'side': signal['side'],
                'reason': error_msg,
                'entry_price': signal['entry_price']
            })
        
        # Ensure clean state - no phantom pending orders
        if symbol in state.pending_orders:
            del state.pending_orders[symbol]
        return
    
    if result.get('order') or result.get('order_id'):
        order_id = result.get('order_id', result.get('order', {}).get('order_id', 'unknown'))
        log(f"  ✅ Order placed: {order_id}")
        
        # Track as pending order (not filled position yet)
        state.pending_orders[symbol] = {
            'order_id': order_id,
            'ticker': ticker,
            'symbol': symbol,
            'side': signal['side'],
            'entry_price': signal['entry_price'],
            'order_price': contract_price,
            'contracts': contracts,
            'stop_loss': signal['stop_loss'],
            'target_price': signal['target_price'],
            'placed_time': time.time(),
        }
        
        state.trades_this_hour += 1
        
        log_trade({
            'type': 'order_placed',
            **signal,
            'contracts': contracts,
            'order_id': order_id
        })
    else:
        log(f"  ❌ Order failed: {result}")
        log_trade({
            'type': 'order_failed',
            'symbol': symbol,
            'reason': str(result)
        })


async def manage_pending_orders(client: KalshiClient):
    """
    Check pending orders:
    1. Cancel stale orders (price moved too far)
    2. Promote filled orders to positions
    """
    for symbol in list(state.pending_orders.keys()):
        pending = state.pending_orders[symbol]
        
        # Get current price
        if symbol not in state.coinbase_price_history or not state.coinbase_price_history[symbol]:
            continue
        
        current_price = state.coinbase_price_history[symbol][-1]['price']
        order_spot_price = pending['entry_price']
        
        # Check if order is stale (price moved too far)
        price_deviation = abs(current_price - order_spot_price) / order_spot_price
        
        if price_deviation > STALE_ORDER_PRICE_THRESHOLD:
            # Cancel stale order (async to avoid blocking)
            log(f"⚠️ STALE ORDER: {symbol} @ ${order_spot_price:,.2f} (current: ${current_price:,.2f}, {price_deviation*100:.2f}% deviation)")
            try:
                result = await run_sync(client.cancel_order, pending['order_id'])
                log(f"   Cancelled order {pending['order_id'][:8]}...")
                del state.pending_orders[symbol]
                log_trade({
                    'type': 'order_cancelled',
                    'reason': 'stale_price',
                    'symbol': symbol,
                    'order_id': pending['order_id'],
                    'order_price': order_spot_price,
                    'current_price': current_price,
                    'deviation': price_deviation
                })
            except Exception as e:
                log(f"   Cancel failed: {e}")


def sync_orders_on_startup(client: KalshiClient):
    """
    On startup, check for any existing open orders and cancel them.
    This ensures clean state.
    """
    import requests
    
    log("Syncing orders on startup...")
    
    try:
        path = '/trade-api/v2/margin/orders'
        resp = requests.get(client.base_url + path, headers=client._headers('GET', path), timeout=10)
        orders = resp.json()
        
        cancelled = 0
        for o in orders.get('orders', []):
            remaining = float(o.get('remaining_count', 0))
            if remaining > 0:
                order_id = o.get('order_id')
                ticker = o.get('ticker', 'unknown')
                price = float(o.get('price', 0))
                log(f"  Found open order: {ticker} @ ${price:.4f}, cancelling...")
                try:
                    client.cancel_order(order_id)
                    cancelled += 1
                except Exception as e:
                    log(f"    Cancel failed: {e}")
        
        if cancelled > 0:
            log(f"  Cancelled {cancelled} stale orders")
        else:
            log("  No stale orders found")
            
    except Exception as e:
        log(f"  Order sync failed: {e}")


def sync_positions_on_startup(client: KalshiClient):
    """
    On startup, check for any existing positions in Kalshi.
    Create state.exit_targets for positions that don't have them.
    """
    
    log("Syncing positions from Kalshi...")
    
    live_positions = client.get_positions()
    
    if not live_positions:
        log("  No open positions")
        return
    
    for pos in live_positions:
        ticker = pos['ticker']
        side = pos['side']
        contracts = pos['contracts']
        entry_price_contract = pos['entry_price']
        
        # Find symbol
        symbol = None
        for sym, tick in PERP_TICKERS.items():
            if tick == ticker:
                symbol = sym
                break
        
        if not symbol:
            log(f"  Unknown ticker: {ticker}")
            continue
        
        # Convert contract price to spot
        spot_price = contract_to_spot_price(symbol, entry_price_contract)
        
        # Validate entry price - can't calculate stops without it
        if spot_price <= 0 or entry_price_contract <= 0:
            log(f"  ⚠️ {symbol}: Invalid entry price ({entry_price_contract}), cannot set stop")
            continue
        
        # Check if we already have valid exit targets (stop must be non-zero)
        if ticker in state.exit_targets:
            existing_stop = state.exit_targets[ticker].get('stop_loss', 0)
            if existing_stop > 0:
                log(f"  {symbol}: Already tracking exit targets (stop=${existing_stop:,.4f})")
                continue
            else:
                log(f"  {symbol}: Invalid stop ($0), recalculating...")
        
        log(f"  Found {symbol}: {side.upper()} {contracts} @ ${spot_price:,.2f}")
        
        # Create exit targets using proper strategy logic
        # Target: VWAP if available
        target = state.vwap_states[symbol].vwap if symbol in state.vwap_states else spot_price
        
        # Stop: Use recent swing + buffer if we have price history, otherwise fallback
        if symbol in state.coinbase_price_history and len(state.coinbase_price_history[symbol]) >= 5:
            recent_prices = list(state.coinbase_price_history[symbol])[-20:]
            if side == 'long':
                swing_low = min(p['price'] for p in recent_prices)
                stop_loss = swing_low * (1 - STOP_LOSS_BEYOND_WICK_PCT)
                # Ensure minimum stop distance
                min_stop = spot_price * (1 - MIN_STOP_DISTANCE_PCT)
                stop_loss = min(stop_loss, min_stop)
            else:
                swing_high = max(p['price'] for p in recent_prices)
                stop_loss = swing_high * (1 + STOP_LOSS_BEYOND_WICK_PCT)
                # Ensure minimum stop distance
                min_stop = spot_price * (1 + MIN_STOP_DISTANCE_PCT)
                stop_loss = max(stop_loss, min_stop)
            log(f"    Using swing-based stop: ${stop_loss:,.2f}")
        else:
            # Fallback: use minimum stop distance from config
            if side == 'long':
                stop_loss = spot_price * (1 - max(MIN_STOP_DISTANCE_PCT, 0.02))
            else:
                stop_loss = spot_price * (1 + max(MIN_STOP_DISTANCE_PCT, 0.02))
            log(f"    Using fallback stop (no price history): ${stop_loss:,.2f}")
        
        state.exit_targets[ticker] = {
            'stop_loss': stop_loss,
            'target_price': target,
            'side': side,
            'entry_price': spot_price
        }
        
        log(f"    Stop: ${stop_loss:,.2f} | Target: ${target:,.2f}")


async def check_order_fills(client: KalshiClient):
    """
    Check if any pending orders have filled by querying the API.
    Called periodically since we may miss WebSocket fill events.
    """
    import requests
    
    for symbol in list(state.pending_orders.keys()):
        pending = state.pending_orders[symbol]
        order_id = pending['order_id']
        
        try:
            # Run sync HTTP in thread pool to avoid blocking
            def _fetch_order():
                path = f'/trade-api/v2/margin/orders/{order_id}'
                resp = requests.get(client.base_url + path, headers=client._headers('GET', path), timeout=10)
                return resp.json()
            
            order = await run_sync(_fetch_order)
            
            remaining = float(order.get('remaining_count', 0))
            filled = float(order.get('fill_count', 0))
            
            if remaining == 0 and filled > 0:
                # Order fully filled - store exit targets
                log(f"✅ ORDER FILLED: {symbol} {filled:.0f} contracts")
                
                # Store exit targets (position will be tracked via Kalshi API)
                state.exit_targets[pending['ticker']] = {
                    'stop_loss': pending['stop_loss'],
                    'target_price': pending['target_price'],
                    'side': pending['side'],
                    'entry_price': pending['entry_price']
                }
                
                # Send Telegram notification
                notify_entry(
                    symbol=symbol,
                    side=pending['side'],
                    contracts=int(filled),
                    entry_price=pending['entry_price'],
                    stop_loss=pending['stop_loss'],
                    target=pending['target_price']
                )
                
                del state.pending_orders[symbol]
                
                log_trade({
                    'type': 'order_filled',
                    'symbol': symbol,
                    'contracts': filled,
                    'order_id': order_id
                })
                
            elif remaining == 0 and filled == 0:
                # Order was cancelled externally
                log(f"⚠️ Order {order_id[:8]}... was cancelled externally")
                del state.pending_orders[symbol]
                
        except Exception as e:
            log(f"Error checking order {order_id[:8]}...: {e}")


async def manage_positions(client: KalshiClient):
    """
    Query Kalshi for live positions and check TP/SL.
    Uses state.exit_targets dict for stored stop_loss and target_price.
    """
    
    # Get LIVE positions from Kalshi (async)
    live_positions = await run_sync(client.get_positions)
    
    # Clean up state.exit_targets for positions that no longer exist
    # This is where we confirm exits and book PnL
    live_tickers = {p['ticker'] for p in live_positions}
    for ticker in list(state.exit_targets.keys()):
        if ticker not in live_tickers:
            targets = state.exit_targets[ticker]
            
            # If exit was pending, now we can confirm it and book PnL
            if targets.get('exit_pending') and targets.get('position_pnl') is not None:
                symbol = targets.get('symbol', ticker)
                position_pnl = targets['position_pnl']  # Position-specific, no multi-position contamination
                equity_before = targets.get('equity_before', 0)
                exit_reason = targets.get('exit_reason', 'unknown')
                side = targets.get('side', 'unknown')
                exit_price = targets.get('exit_price', 0)
                
                # Get current equity for sanity check (not used for PnL)
                equity_after = await run_sync(client.get_equity)
                equity_delta = equity_after - equity_before
                
                # Use position-specific PnL (avoids contamination from other positions)
                pnl = position_pnl
                
                log(f"[POSITION] Exit confirmed for {symbol}")
                log(f"  Realized PnL: ${pnl:+,.2f} (position-specific)")
                log(f"  Equity check: ${equity_before:,.2f} → ${equity_after:,.2f} (Δ${equity_delta:+,.2f})")
                
                # === NOW book PnL ===
                state.total_pnl += pnl
                log(f"  Total session PnL: ${state.total_pnl:+,.2f}")
                
                # === NOW update circuit breaker (based on actual PnL, not reason text) ===
                is_loss = pnl < 0
                if is_loss:
                    state.consecutive_losses += 1
                    log(f"  ⚠️ Consecutive losses: {state.consecutive_losses}/{CIRCUIT_BREAKER_CONSECUTIVE_LOSSES}")
                else:
                    if state.consecutive_losses > 0:
                        log(f"  ✅ Win! Resetting consecutive loss counter (was {state.consecutive_losses})")
                    state.consecutive_losses = 0
                
                # === NOW send notifications ===
                notify_exit(
                    symbol=symbol,
                    side=side,
                    exit_price=exit_price,
                    pnl=pnl,
                    reason=exit_reason,
                    consecutive_losses=state.consecutive_losses,
                    balance=equity_after
                )
                
                log_trade({
                    'type': 'exit_confirmed',
                    'symbol': symbol,
                    'reason': exit_reason,
                    'exit_price': exit_price,
                    'position_pnl': pnl,
                    'equity_before': equity_before,
                    'equity_after': equity_after,
                    'equity_delta': equity_delta,
                    'consecutive_losses': state.consecutive_losses,
                    'is_loss': is_loss
                })
            else:
                log(f"[POSITION] Removing stale exit target for {ticker}")
            
            del state.exit_targets[ticker]
    
    for pos in live_positions:
        ticker = pos['ticker']
        side = pos['side']
        contracts = pos['contracts']
        entry_price_contract = pos['entry_price']  # Contract price
        kalshi_unrealized_pnl = pos['unrealized_pnl']  # Actual PnL from Kalshi
        
        # Find symbol from ticker
        symbol = None
        for sym, tick in PERP_TICKERS.items():
            if tick == ticker:
                symbol = sym
                break
        
        if not symbol:
            continue
        
        # Convert contract price to spot price
        entry_price = contract_to_spot_price(symbol, entry_price_contract)
        
        # Get current spot price
        if symbol not in state.coinbase_price_history or not state.coinbase_price_history[symbol]:
            continue
        current_price = state.coinbase_price_history[symbol][-1]['price']
        
        # Get or create exit targets for this position
        if ticker not in state.exit_targets:
            # New position - calculate exit targets using consistent formula
            vwap = state.vwap_states[symbol].vwap if symbol in state.vwap_states else entry_price
            
            # Use swing-based stops if we have price history, otherwise fallback
            if symbol in state.coinbase_price_history and len(state.coinbase_price_history[symbol]) >= 5:
                recent_prices = list(state.coinbase_price_history[symbol])[-20:]
                if side == 'long':
                    swing_low = min(p['price'] for p in recent_prices)
                    stop_loss = swing_low * (1 - STOP_LOSS_BEYOND_WICK_PCT)
                    min_stop = entry_price * (1 - MIN_STOP_DISTANCE_PCT)
                    stop_loss = min(stop_loss, min_stop)
                else:
                    swing_high = max(p['price'] for p in recent_prices)
                    stop_loss = swing_high * (1 + STOP_LOSS_BEYOND_WICK_PCT)
                    min_stop = entry_price * (1 + MIN_STOP_DISTANCE_PCT)
                    stop_loss = max(stop_loss, min_stop)
            else:
                # Fallback: use minimum stop distance from config
                if side == 'long':
                    stop_loss = entry_price * (1 - max(MIN_STOP_DISTANCE_PCT, 0.02))
                else:
                    stop_loss = entry_price * (1 + max(MIN_STOP_DISTANCE_PCT, 0.02))
            
            state.exit_targets[ticker] = {
                'stop_loss': stop_loss,
                'target_price': vwap,
                'side': side,
                'entry_price': entry_price
            }
            log(f"[POSITION] New exit targets for {symbol} {side.upper()}: Stop ${stop_loss:,.2f}, Target ${vwap:,.2f}")
        
        targets = state.exit_targets[ticker]
        
        # Skip if exit already pending (waiting for position to close)
        if targets.get('exit_pending'):
            age = time.time() - targets.get('exit_order_time', 0)
            if age > 60:  # If pending > 60s, position may have failed to close - retry
                log(f"[POSITION] {symbol} exit pending > 60s, will retry (keeping original equity snapshot)")
                targets['exit_pending'] = False
                # Keep original equity_before and position_pnl - don't re-snapshot
            else:
                continue  # Skip, exit order already sent
        
        stop_loss = targets['stop_loss']
        target_price = targets['target_price']
        
        # Update target to current VWAP (dynamic target)
        if symbol in state.vwap_states:
            target_price = state.vwap_states[symbol].vwap
            state.exit_targets[ticker]['target_price'] = target_price
        
        # Check exit conditions
        should_exit = False
        exit_reason = ""
        
        if side == 'long':
            if current_price <= stop_loss:
                should_exit = True
                exit_reason = f"STOP LOSS @ ${current_price:,.2f}"
            elif current_price >= target_price:
                should_exit = True
                exit_reason = f"TARGET (VWAP) @ ${current_price:,.2f}"
        else:  # short
            if current_price >= stop_loss:
                should_exit = True
                exit_reason = f"STOP LOSS @ ${current_price:,.2f}"
            elif current_price <= target_price:
                should_exit = True
                exit_reason = f"TARGET (VWAP) @ ${current_price:,.2f}"
        
        if should_exit:
            log(f"EXIT: {symbol} - {exit_reason}")
            
            # Convert to contract price for order
            exit_contract_price = spot_to_contract_price(symbol, current_price)
            
            # DRY RUN: Log but don't place order
            if DRY_RUN:
                log(f"  🔸 DRY RUN: Would exit with PnL ${kalshi_unrealized_pnl:+.2f} (position unrealized)")
                # Don't delete state.exit_targets in dry run so we can keep tracking
                continue
            
            # Capture equity BEFORE exit (only if not already set from prior retry)
            # Also store position-specific unrealized_pnl as primary PnL (avoids multi-position contamination)
            if 'equity_before' not in state.exit_targets[ticker]:
                state.exit_targets[ticker]['equity_before'] = await run_sync(client.get_equity)
                state.exit_targets[ticker]['position_pnl'] = kalshi_unrealized_pnl  # Position-specific
            
            # Place exit order (async to avoid blocking)
            exit_side = 'sell' if side == 'long' else 'buy'
            result = await run_sync(
                client.place_order,
                ticker,
                exit_side,
                contracts,
                exit_contract_price,
                True  # reduce_only
            )
            
            if result.get('order') or result.get('order_id'):
                equity_before = state.exit_targets[ticker]['equity_before']
                position_pnl = state.exit_targets[ticker]['position_pnl']
                
                log(f"  Exit order placed for {contracts} contracts")
                log(f"  Position PnL: ${position_pnl:+,.2f} (from Kalshi, position-specific)")
                log(f"  Equity snapshot: ${equity_before:,.2f} (for sanity check)")
                
                # Mark exit pending
                state.exit_targets[ticker]['exit_pending'] = True
                state.exit_targets[ticker]['exit_order_time'] = time.time()
                state.exit_targets[ticker]['exit_reason'] = exit_reason
                state.exit_targets[ticker]['exit_price'] = current_price
                state.exit_targets[ticker]['symbol'] = symbol
                
                log_trade({
                    'type': 'exit_order_placed',
                    'symbol': symbol,
                    'reason': exit_reason,
                    'exit_price': current_price,
                    'contracts': contracts,
                    'position_pnl': position_pnl,
                    'equity_before': equity_before
                })
            else:
                log(f"  ❌ Exit failed: {result}")


# ============================================================
# MAIN LOOP
# ============================================================

async def log_comprehensive_status(client: KalshiClient):
    """Log comprehensive status snapshot (async to avoid blocking event loop)."""
    
    now = time.time()
    if now - state.last_status_log < STATUS_LOG_INTERVAL:
        return
    state.last_status_log = now
    
    try:
        # Run sync API calls in thread pool
        balance = await run_sync(client.get_balance)
        
        # Get prices for all configured assets
        prices = {}
        for symbol, ticker in PERP_TICKERS.items():
            bid, ask = await run_sync(client.get_best_prices, ticker)
            spot = contract_to_spot_price(symbol, bid)
            prices[symbol] = {'bid': bid, 'ask': ask, 'spot': spot}
        
        # VWAP states
        vwap_info = {}
        for symbol in PERP_TICKERS:
            if symbol in state.vwap_states:
                vwap_s = state.vwap_states[symbol]
                lower, vwap, upper = vwap_s.get_bands(STD_DEV_MULTIPLIER)
                current = vwap_s.current_price
                dev, direction = vwap_s.get_deviation(current)
                
                vwap_info[symbol] = {
                    'vwap': round(vwap, 2),
                    'lower_band': round(lower, 2),
                    'upper_band': round(upper, 2),
                    'current_price': round(current, 2),
                    'deviation_sd': round(dev, 2),
                    'direction': direction,
                    'candle_count': vwap_s.candle_count
                }
        
        # CVD states
        cvd_info = {}
        for symbol in PERP_TICKERS:
            if symbol in state.cvd_states:
                cvd = state.cvd_states[symbol].get_cvd()
                trend = state.cvd_states[symbol].get_cvd_trend()
                cvd_info[symbol] = {'value': round(cvd, 2), 'trend': trend}
        
        # Get LIVE positions from Kalshi (async)
        live_positions = await run_sync(client.get_positions)
        pos_info = {}
        for pos in live_positions:
            ticker = pos['ticker']
            # Find symbol
            for sym, tick in PERP_TICKERS.items():
                if tick == ticker:
                    entry_spot = contract_to_spot_price(sym, pos['entry_price'])
                    targets = state.exit_targets.get(ticker, {})
                    pos_info[sym] = {
                        'side': pos['side'],
                        'contracts': pos['contracts'],
                        'entry_price': entry_spot,
                        'stop_loss': targets.get('stop_loss', 0),
                        'target': targets.get('target_price', 0),
                        'pnl': pos['unrealized_pnl']
                    }
                    break
        
        # ADX states
        adx_info = {}
        for symbol in PERP_TICKERS:
            if symbol in state.adx_states and state.adx_states[symbol].is_valid():
                adx = state.adx_states[symbol].get_adx()
                trending = state.adx_states[symbol].is_trending(ADX_TREND_THRESHOLD)
                adx_info[symbol] = {'value': round(adx, 1), 'trending': trending}
        
        # Spread corridor status
        spread_info = {}
        for symbol in PERP_TICKERS:
            is_safe, divergence = check_spread_corridor(symbol)
            spread_info[symbol] = {'safe': is_safe, 'divergence': round(divergence * 100, 3)}
        
        # Console output
        log("-" * 50)
        halt_status = f" | ⚠️ HALTED: {state.halt_reason}" if state.trading_halted else ""
        log(f"STATUS | Balance: ${balance:.2f} | Positions: {len(live_positions)} | Pending: {len(state.pending_orders)}{halt_status}")
        price_str = " | ".join(f"{sym}: ${p['spot']:,.0f}" for sym, p in prices.items())
        log(f"  {price_str}")
        
        for symbol, info in vwap_info.items():
            if info['vwap'] > 0:
                adx_str = f" | ADX: {adx_info.get(symbol, {}).get('value', 'N/A')}" if symbol in adx_info else ""
                spread_str = f" | Spread: {spread_info.get(symbol, {}).get('divergence', 0):.3f}%"
                log(f"  {symbol} VWAP: ${info['vwap']:,.0f} | ±2σ: ${info['lower_band']:,.0f}-${info['upper_band']:,.0f} | Dev: {info['deviation_sd']:.1f}σ {info['direction']}{adx_str}{spread_str}")
        
        for symbol, cvd in cvd_info.items():
            log(f"  {symbol} CVD: {cvd['value']:+.1f} ({cvd['trend']})")
        
        for symbol, pending in state.pending_orders.items():
            log(f"  PENDING {symbol}: {pending['side'].upper()} {pending['contracts']} @ ${pending['entry_price']:,.0f}")
        
        for symbol, pos in pos_info.items():
            log(f"  POSITION {symbol}: {pos['side'].upper()} {pos['contracts']} @ ${pos['entry_price']:,.0f}")
        
        # Log to file
        log_status({
            'balance': balance,
            'prices': prices,
            'vwap': vwap_info,
            'cvd': cvd_info,
            'positions': pos_info,
            'ws_connected': state.ws_connected.copy(),
            'trades_this_hour': state.trades_this_hour
        })
        
    except Exception as e:
        log(f"Status logging error: {e}")


async def trading_loop(client: KalshiClient):
    """Main trading loop."""
    log("Starting trading loop...")
    
    while True:
        try:
            # Comprehensive status logging (async)
            await log_comprehensive_status(client)
            
            # Check for entry signals
            for symbol in PERP_TICKERS:
                if symbol in state.coinbase_price_history and state.coinbase_price_history[symbol]:
                    current_price = state.coinbase_price_history[symbol][-1]['price']
                    
                    signal = check_entry_signal(symbol, current_price)
                    if signal:
                        await execute_entry(client, signal)
            
            # Manage pending orders (check fills, cancel stale)
            await check_order_fills(client)
            await manage_pending_orders(client)
            
            # Manage open positions
            await manage_positions(client)
            
            # Periodic state save
            periodic_state_save()
            
            await asyncio.sleep(POLL_INTERVAL)
            
        except Exception as e:
            log(f"Error in trading loop: {e}")
            # Save state on error
            try:
                save_state(state.exit_targets, state.trades_this_hour, state.total_pnl)
            except:
                pass
            await asyncio.sleep(5)


def recover_state():
    """Recover state.exit_targets from saved state file."""
    
    saved = load_state()
    if not saved:
        log("[STATE] No saved state found, starting fresh")
        return
    
    log(f"[STATE] Found saved state from {saved.get('saved_at_iso', 'unknown')}")
    
    # Recover exit targets (not positions - those come from Kalshi API)
    for ticker, target_data in saved.get('exit_targets', {}).items():
        state.exit_targets[ticker] = target_data
        log(f"[STATE] Recovered exit targets for {ticker}")
    
    # Note: VWAP and positions come from live sources
    log(f"[STATE] VWAP will be seeded fresh, positions from Kalshi API")
    
    state.trades_this_hour = saved.get('trades_this_hour', 0)
    state.total_pnl = saved.get('total_pnl', 0.0)
    
    log(f"[STATE] Recovery complete: {len(state.exit_targets)} exit targets, PnL: ${state.total_pnl:+,.2f}")


def periodic_state_save():
    """Save state periodically."""
    
    now = time.time()
    if now - state.last_state_save < STATE_SAVE_INTERVAL:
        return
    
    state.last_state_save = now
    try:
        save_state(state.exit_targets, state.trades_this_hour, state.total_pnl)
    except Exception as e:
        log(f"[STATE] Error saving state: {e}")


def seed_vwap_from_history():
    """
    Seed VWAP calculations with historical OHLCV candles from Coinbase.
    Uses the candles API for proper minute-by-minute data.
    """
    import requests
    
    log("Seeding VWAP from Coinbase candles...")
    
    # Build symbol -> product mapping from config
    coinbase_products = {
        sym: asset.coinbase_symbol 
        for sym, asset in cfg.assets.items() 
        if asset.enabled
    }
    
    for symbol, product_id in coinbase_products.items():
        try:
            # Fetch 1-minute candles for the VWAP window
            # Coinbase Advanced Trade API: GET /products/{product_id}/candles
            url = f'https://api.exchange.coinbase.com/products/{product_id}/candles'
            params = {
                'granularity': 60,  # 1 minute = 60 seconds
            }
            headers = {'Accept': 'application/json'}
            
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code != 200:
                log(f"  {symbol}: Failed to fetch candles (HTTP {resp.status_code})")
                continue
            
            candles_data = resp.json()
            
            if not candles_data:
                log(f"  {symbol}: No candles returned")
                continue
            
            # Initialize VWAP state with config values
            if symbol not in state.vwap_states:
                state.vwap_states[symbol] = VWAPState(
                    reset_hour_utc=cfg.vwap_reset_hour_utc,
                    min_candles=cfg.min_candles_for_vwap
                )
            
            # Initialize CVD state with config values
            if symbol not in state.cvd_states:
                state.cvd_states[symbol] = CVDState(
                    divergence_window_minutes=cfg.cvd_divergence_window_minutes,
                    reset_hours=cfg.cvd_reset_hours
                )
            
            # Initialize ADX state with config values
            if symbol not in state.adx_states:
                state.adx_states[symbol] = ADXState(period=cfg.adx_period)
            
            # Coinbase candles format: [timestamp, low, high, open, close, volume]
            # They come newest first, so reverse for chronological order
            candles_added = 0
            for candle_data in reversed(candles_data):
                if len(candle_data) < 6:
                    continue
                    
                timestamp = int(candle_data[0])
                low = float(candle_data[1])
                high = float(candle_data[2])
                open_price = float(candle_data[3])
                close = float(candle_data[4])
                volume = float(candle_data[5])
                
                if volume <= 0:
                    continue
                
                # Create candle with proper timestamp
                candle_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                
                # Only keep candles from today's session (since 00:00 UTC)
                today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                if candle_time < today_start:
                    continue
                
                candle = Candle(
                    time=candle_time,
                    open=open_price,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume
                )
                state.vwap_states[symbol].candles.append(candle)
                candles_added += 1
                
                # Update ADX with historical candle
                state.adx_states[symbol].add_candle(high, low, close)
                
                # Update price history
                if symbol not in state.coinbase_price_history:
                    state.coinbase_price_history[symbol] = deque(maxlen=100)
                state.coinbase_price_history[symbol].append({'time': timestamp, 'price': close})
            
            # Set current candle to most recent
            if state.vwap_states[symbol].candles:
                state.vwap_states[symbol].current_candle = state.vwap_states[symbol].candles.pop()
            
            # Mark as today's session so check_reset doesn't clear seeded data
            state.vwap_states[symbol].last_reset_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            
            vwap = state.vwap_states[symbol].vwap
            lower, _, upper = state.vwap_states[symbol].get_bands(STD_DEV_MULTIPLIER)
            std = state.vwap_states[symbol].std_dev
            adx = state.adx_states[symbol].get_adx() if state.adx_states[symbol].is_valid() else 0.0
            log(f"  {symbol}: {candles_added} candles, VWAP: ${vwap:,.2f}, ±2σ: ${lower:,.2f}-${upper:,.2f}, ADX: {adx:.1f}")
            
        except Exception as e:
            log(f"  {symbol}: Error seeding - {e}")


async def shutdown(tasks: List[asyncio.Task], client: KalshiClient = None):
    """Graceful shutdown: cancel tasks, save state, close connections."""
    log("🛑 Shutting down...")
    
    # Save final state
    try:
        save_state(state.exit_targets, state.trades_this_hour, state.total_pnl)
        log("  State saved")
    except Exception as e:
        log(f"  State save failed: {e}")
    
    # Cancel all tasks
    for task in tasks:
        if not task.done():
            task.cancel()
    
    # Wait for cancellation to complete
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
        log("  Tasks cancelled")
    
    log("  Shutdown complete")


async def main():
    log("=" * 60)
    log("KALSHI VWAP REVERSAL BOT")
    log("=" * 60)
    log(f"Assets: {list(PERP_TICKERS.keys())}")
    log(f"Entry: ±{STD_DEV_MULTIPLIER}σ bands | Session VWAP (resets 00:00 UTC)")
    log(f"Max margin: {cfg.max_margin_pct*100:.0f}% | Max risk: {cfg.max_risk_per_trade_pct*100:.0f}%")
    log(f"Target: VWAP | Stop: Swing ±{cfg.stop_beyond_wick_pct*100:.1f}%")
    log("=" * 60)
    
    # Recover state from previous run
    recover_state()
    
    # Seed VWAP from historical trades if we don't have enough data
    needs_seeding = True
    for symbol in PERP_TICKERS:
        if symbol in state.vwap_states and state.vwap_states[symbol].is_valid():
            needs_seeding = False
            break
    
    if needs_seeding:
        seed_vwap_from_history()
    else:
        log("VWAP already has sufficient data from saved state")
    
    client = KalshiClient()
    balance = client.get_balance()
    log(f"Account balance: ${balance:,.2f}")
    
    if balance <= 0:
        log("❌ No balance!")
        return
    
    # Send startup notification
    notify_startup(balance, list(PERP_TICKERS.keys()))
    
    # Cancel any stale orders from previous runs
    sync_orders_on_startup(client)
    
    # Sync existing positions from Kalshi
    sync_positions_on_startup(client)
    
    # Create tasks for concurrent execution
    # Store WS tasks in global dict so watchdog can restart them
    global ws_tasks
    ws_tasks['kalshi'] = asyncio.create_task(kalshi_websocket(), name="kalshi_ws")
    ws_tasks['coinbase'] = asyncio.create_task(coinbase_websocket(), name="coinbase_ws")
    
    # Essential tasks that should never complete - if they do, we shut down
    # WS tasks are managed by watchdog and can be restarted without killing the bot
    essential_tasks = [
        asyncio.create_task(trading_loop(client), name="trading_loop"),
        asyncio.create_task(ws_watchdog(), name="ws_watchdog"),
    ]
    
    # All tasks for cleanup
    all_tasks = essential_tasks + [ws_tasks['kalshi'], ws_tasks['coinbase']]
    
    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()
    import signal
    
    def handle_signal(sig):
        log(f"Received signal {sig.name}")
        # Gather all current tasks including any restarted WS tasks
        current_tasks = essential_tasks + list(ws_tasks.values())
        asyncio.create_task(shutdown(current_tasks, client))
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))
    
    try:
        # Only watch essential tasks - WS tasks are managed by watchdog
        done, pending = await asyncio.wait(essential_tasks, return_when=asyncio.FIRST_COMPLETED)
        
        # If an essential task finished unexpectedly, log it and shut down
        for task in done:
            if task.exception():
                log(f"Essential task {task.get_name()} failed: {task.exception()}")
            else:
                log(f"Essential task {task.get_name()} completed unexpectedly")
        
        # Shutdown remaining tasks including WS tasks
        current_ws_tasks = list(ws_tasks.values())
        await shutdown(list(pending) + current_ws_tasks, client)
        
    except asyncio.CancelledError:
        log("Main cancelled")
        current_ws_tasks = list(ws_tasks.values())
        await shutdown(essential_tasks + current_ws_tasks, client)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Interrupted by user")
