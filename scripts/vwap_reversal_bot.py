#!/usr/bin/env python3
"""
Kalshi Perps VWAP Reversal Bot

Strategy:
- VWAP with ±1σ, ±2σ, ±3σ bands
- CVD (Cumulative Volume Delta) for order flow exhaustion
- Entry on ±2σ/±3σ pierce with CVD divergence
- Exit at ±1σ and VWAP
- Isolated margin, 1-2% max risk per trade

Data feeds: Kalshi WebSocket (perps) + Coinbase WebSocket (spot)
"""

import sys
sys.stdout.reconfigure(line_buffering=True)

import os
import json
import time
import asyncio
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
load_dotenv('/home/clawdbot/clawd/.env')

from state_manager import save_state, load_state, clear_state

# ============================================================
# CONFIGURATION
# ============================================================

API_KEY = os.getenv('KALSHI_API_KEY_ID')
KEY_PATH = os.getenv('KALSHI_KEY_PATH', 'keys/kalshi_private.pem')

# WebSocket URLs
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"  # Advanced Trade API

# Perp tickers and contract sizes
PERP_TICKERS = {
    'BTC': 'KXBTCPERP',
    'ETH': 'KXETHPERP',
}

CONTRACT_SIZES = {
    'BTC': 0.0001,  # 1 contract = 0.0001 BTC
    'ETH': 0.001,   # 1 contract = 0.001 ETH
}

def spot_to_contract_price(symbol: str, spot_price: float) -> float:
    """Convert spot price to contract price."""
    return spot_price * CONTRACT_SIZES.get(symbol, 0.0001)

def contract_to_spot_price(symbol: str, contract_price: float) -> float:
    """Convert contract price to spot price."""
    return contract_price / CONTRACT_SIZES.get(symbol, 0.0001)

# VWAP Settings (Session-anchored, daily reset at 00:00 UTC)
STD_DEV_MULTIPLIER = 2.0    # ±2σ bands for entry signals
MIN_CANDLES_FOR_VWAP = 5    # Minimum candles before VWAP is valid
VWAP_RESET_HOUR_UTC = 0     # Reset VWAP at midnight UTC

# CVD Settings
CVD_DIVERGENCE_WINDOW_MINUTES = 30  # Look back 30 min for divergence
CVD_RESET_HOURS = 12        # Reset CVD every 12 hours to keep numbers manageable

# Entry/Exit Rules
STOP_LOSS_BEYOND_WICK_PCT = 0.001  # 0.1% past last swing high/low

# Kalshi Perps Fee Structure
MAKER_FEE_RATE = 0.0001   # 0.01% maker fee
TAKER_FEE_RATE = 0.00035  # 0.035% taker fee
# Assume maker entry + taker exit (worst case for stop loss)
TOTAL_FEE_RATE = MAKER_FEE_RATE + TAKER_FEE_RATE  # 0.045%
MIN_PROFIT_MARGIN = 0.001  # Require at least 0.1% net profit after fees

# === NEW: Safety Gates ===
# Spread Corridor: Halt if Kalshi and Coinbase diverge too much
SPREAD_CORRIDOR_MAX_PCT = 0.0015  # 0.15% max divergence before halting

# ADX Trend Filter: Don't fade bands during strong trends
ADX_PERIOD = 14
ADX_TREND_THRESHOLD = 25  # ADX > 25 = trending market, skip mean reversion
ADX_COOLDOWN_THRESHOLD = 22  # ADX must drop below 22 to re-enable after blocking

# Order Book Imbalance (OBI): Confirm passive flow supports trade direction
OBI_MIN_THRESHOLD = 0.20  # Require +0.20 OBI for longs, -0.20 for shorts
OBI_DEPTH_LEVELS = 5  # Use top 5 levels of orderbook

# Data Freshness: Reject signals if websocket data is stale
MAX_DATA_LAG_SECONDS = 1.0  # Max 1 second lag before halting

# Circuit Breaker: Stop trading after consecutive losses
CIRCUIT_BREAKER_CONSECUTIVE_LOSSES = 3  # Shutdown after 3 consecutive stops

# Risk Management
MAX_RISK_PER_TRADE_PCT = 0.30  # 30% max risk (aggressive for small account)
MAX_ACCOUNT_RISK_PCT = 0.50  # 50% max total exposure
USE_ISOLATED_MARGIN = True
MAX_LEVERAGE = 10

# CVD Settings
CVD_DIVERGENCE_THRESHOLD = 0.7  # CVD must diverge by 70%+ vs price
CVD_WINDOW_SECONDS = 60  # Look back window for CVD

# Rate Limiting
POLL_INTERVAL = 0.5  # seconds
MAX_TRADES_PER_HOUR = 10

# Logging
LOG_DIR = Path(__file__).parent.parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# Status logging interval
STATUS_LOG_INTERVAL = 30  # seconds
last_status_log = 0

# State persistence
STATE_SAVE_INTERVAL = 60  # Save state every 60 seconds
last_state_save = 0
total_pnl = 0.0

# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class Candle:
    """1-minute OHLCV candle."""
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    @property
    def typical_price(self) -> float:
        """Typical price = (H + L + C) / 3"""
        return (self.high + self.low + self.close) / 3


class VWAPState:
    """
    Session-anchored VWAP with daily reset at 00:00 UTC.
    Uses typical price (HLC/3) for accurate VWAP calculation.
    Accumulates all candles for the current session (not rolling).
    """
    def __init__(self):
        self.candles: List[Candle] = []
        self.current_candle: Optional[Candle] = None
        self.last_reset_date: Optional[datetime] = None
        
    def check_reset(self):
        """Check if we need to reset for new session (00:00 UTC)."""
        now = datetime.now(timezone.utc)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        if self.last_reset_date is None or self.last_reset_date < today:
            if self.candles or self.current_candle:
                log(f"[VWAP] Session reset - new trading day")
            self.candles = []
            self.current_candle = None
            self.last_reset_date = today
        
    def process_trade(self, price: float, size: float, side: str):
        """Process incoming trade tick into candle bars."""
        # Check for daily reset first
        self.check_reset()
        
        now = datetime.now(timezone.utc)
        current_minute = now.replace(second=0, microsecond=0)
        
        if self.current_candle is None or self.current_candle.time != current_minute:
            # Start new candle - first finalize the previous one
            if self.current_candle is not None:
                self.candles.append(self.current_candle)
                # Return completed candle info for ADX update
                self._last_completed_candle = self.current_candle
            
            self.current_candle = Candle(
                time=current_minute,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=size
            )
            # NO rolling window - keep all candles for the session
        else:
            # Update current candle
            self.current_candle.high = max(self.current_candle.high, price)
            self.current_candle.low = min(self.current_candle.low, price)
            self.current_candle.close = price
            self.current_candle.volume += size
    
    def _all_candles(self) -> List[Candle]:
        """Get all candles including current."""
        if self.current_candle:
            return self.candles + [self.current_candle]
        return self.candles
    
    @property
    def vwap(self) -> float:
        """Calculate VWAP from candles using typical price."""
        candles = self._all_candles()
        if not candles:
            return 0.0
        
        total_tp_vol = sum(c.typical_price * c.volume for c in candles)
        total_volume = sum(c.volume for c in candles)
        
        if total_volume == 0:
            return 0.0
        return total_tp_vol / total_volume
    
    @property
    def std_dev(self) -> float:
        """Calculate standard deviation of typical prices."""
        candles = self._all_candles()
        if len(candles) < 2:
            return 0.0
        
        vwap = self.vwap
        if vwap == 0:
            return 0.0
        
        variance = sum((c.typical_price - vwap) ** 2 for c in candles) / len(candles)
        return math.sqrt(variance)
    
    def get_bands(self, num_std: float = 2.0) -> Tuple[float, float, float]:
        """Returns (lower_band, vwap, upper_band)."""
        vwap = self.vwap
        std = self.std_dev
        return (vwap - num_std * std, vwap, vwap + num_std * std)
    
    @property
    def current_price(self) -> float:
        """Get most recent price."""
        if self.current_candle:
            return self.current_candle.close
        if self.candles:
            return self.candles[-1].close
        return 0.0
    
    @property
    def candle_count(self) -> int:
        return len(self._all_candles())
    
    def is_valid(self) -> bool:
        """VWAP is valid when we have enough data."""
        return self.candle_count >= MIN_CANDLES_FOR_VWAP
    
    def get_deviation(self, price: float) -> Tuple[float, str]:
        """Get deviation in standard deviations."""
        vwap = self.vwap
        std = self.std_dev
        
        if vwap == 0 or std == 0:
            return (0.0, 'neutral')
        
        deviation = (price - vwap) / std
        direction = 'above' if deviation > 0 else 'below'
        return (abs(deviation), direction)

class CVDState:
    """
    Cumulative Volume Delta tracking with periodic reset.
    BUY = positive delta (buying pressure)
    SELL = negative delta (selling pressure)
    
    Uses 30-60 minute window for divergence detection.
    Resets every CVD_RESET_HOURS to keep numbers manageable.
    """
    def __init__(self):
        self.running_cvd: float = 0.0
        self.cvd_history: deque = deque(maxlen=5000)  # (timestamp, cvd_value, price)
        self.last_reset: float = time.time()
    
    def check_reset(self):
        """Reset CVD periodically to keep numbers manageable."""
        hours_since_reset = (time.time() - self.last_reset) / 3600
        if hours_since_reset >= CVD_RESET_HOURS:
            log(f"[CVD] Periodic reset (was {self.running_cvd:+.2f})")
            self.running_cvd = 0.0
            self.cvd_history.clear()
            self.last_reset = time.time()
    
    def add_trade(self, price: float, size: float, side: str):
        """Process trade and update CVD."""
        self.check_reset()
        
        # Market buys push CVD up, market sells drag it down
        if side.upper() == 'BUY':
            delta = size
        elif side.upper() == 'SELL':
            delta = -size
        else:
            delta = 0
        
        self.running_cvd += delta
        self.cvd_history.append((time.time(), self.running_cvd, price))
    
    def get_cvd(self) -> float:
        """Get current running CVD."""
        return self.running_cvd
    
    def get_cvd_30min_ago(self) -> Tuple[float, float]:
        """Get CVD value and price from 30 minutes ago."""
        cutoff = time.time() - (CVD_DIVERGENCE_WINDOW_MINUTES * 60)
        
        # Find oldest entry within window
        for t, cvd, price in self.cvd_history:
            if t >= cutoff:
                return (cvd, price)
        
        # Not enough history
        return (self.running_cvd, 0.0)
    
    def get_cvd_trend(self, window_minutes: int = None) -> str:
        """
        Determine CVD trend over divergence window.
        Returns: 'rising', 'falling', or 'flat'
        """
        if window_minutes is None:
            window_minutes = CVD_DIVERGENCE_WINDOW_MINUTES
            
        cutoff = time.time() - (window_minutes * 60)
        recent = [(t, cvd, p) for t, cvd, p in self.cvd_history if t > cutoff]
        
        if len(recent) < 2:
            return 'flat'
        
        cvd_start = recent[0][1]
        cvd_end = recent[-1][1]
        cvd_change = cvd_end - cvd_start
        
        # Threshold for "flat" - small absolute change
        flat_threshold = max(abs(cvd_end) * 0.05, 0.5)
        
        if cvd_change > flat_threshold:
            return 'rising'
        elif cvd_change < -flat_threshold:
            return 'falling'
        else:
            return 'flat'
    
    def is_diverging_bearish(self) -> bool:
        """CVD falling or flat while price high = bearish divergence."""
        trend = self.get_cvd_trend()
        return trend in ('falling', 'flat')
    
    def is_diverging_bullish(self) -> bool:
        """CVD rising or flat while price low = bullish divergence."""
        trend = self.get_cvd_trend()
        return trend in ('rising', 'flat')


class ADXState:
    """
    Average Directional Index (ADX) for trend strength detection.
    ADX > 25 = trending market (don't fade bands)
    ADX < 20 = ranging market (mean reversion favorable)
    
    Uses Wilder's smoothing with configurable period.
    """
    def __init__(self, period: int = 14):
        self.period = period
        self.price_history: deque = deque(maxlen=period * 3)  # high, low, close
        self.tr_history: deque = deque(maxlen=period + 1)
        self.plus_dm_history: deque = deque(maxlen=period + 1)
        self.minus_dm_history: deque = deque(maxlen=period + 1)
        self.smoothed_tr: float = 0.0
        self.smoothed_plus_dm: float = 0.0
        self.smoothed_minus_dm: float = 0.0
        self.plus_di: float = 0.0
        self.minus_di: float = 0.0
        self.adx: float = 0.0
        self.dx_history: deque = deque(maxlen=period + 1)
    
    def add_candle(self, high: float, low: float, close: float):
        """Process a new candle and update ADX."""
        if len(self.price_history) == 0:
            self.price_history.append((high, low, close))
            return
        
        prev_high, prev_low, prev_close = self.price_history[-1]
        self.price_history.append((high, low, close))
        
        # True Range
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        self.tr_history.append(tr)
        
        # Directional Movement
        up_move = high - prev_high
        down_move = prev_low - low
        
        plus_dm = up_move if up_move > down_move and up_move > 0 else 0
        minus_dm = down_move if down_move > up_move and down_move > 0 else 0
        
        self.plus_dm_history.append(plus_dm)
        self.minus_dm_history.append(minus_dm)
        
        # Need enough data
        if len(self.tr_history) < self.period:
            return
        
        # Wilder's smoothing
        if self.smoothed_tr == 0:
            # First calculation - simple sum
            self.smoothed_tr = sum(list(self.tr_history)[-self.period:])
            self.smoothed_plus_dm = sum(list(self.plus_dm_history)[-self.period:])
            self.smoothed_minus_dm = sum(list(self.minus_dm_history)[-self.period:])
        else:
            # Wilder smoothing: prev - (prev/period) + current
            self.smoothed_tr = self.smoothed_tr - (self.smoothed_tr / self.period) + tr
            self.smoothed_plus_dm = self.smoothed_plus_dm - (self.smoothed_plus_dm / self.period) + plus_dm
            self.smoothed_minus_dm = self.smoothed_minus_dm - (self.smoothed_minus_dm / self.period) + minus_dm
        
        # Directional Indicators
        if self.smoothed_tr > 0:
            self.plus_di = 100 * self.smoothed_plus_dm / self.smoothed_tr
            self.minus_di = 100 * self.smoothed_minus_dm / self.smoothed_tr
        
        # Directional Index (DX)
        di_sum = self.plus_di + self.minus_di
        if di_sum > 0:
            dx = 100 * abs(self.plus_di - self.minus_di) / di_sum
            self.dx_history.append(dx)
            
            # ADX is smoothed DX
            if len(self.dx_history) >= self.period:
                if self.adx == 0:
                    self.adx = sum(self.dx_history) / len(self.dx_history)
                else:
                    self.adx = ((self.adx * (self.period - 1)) + dx) / self.period
    
    def get_adx(self) -> float:
        """Return current ADX value."""
        return self.adx
    
    def is_trending(self, threshold: float = 25.0) -> bool:
        """Returns True if market is in a strong trend."""
        return self.adx >= threshold
    
    def is_valid(self) -> bool:
        """ADX needs enough candles to be meaningful."""
        return len(self.dx_history) >= self.period


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
    
    cb_lag = current_time - ws_last_message.get('coinbase', 0)
    kalshi_lag = current_time - ws_last_message.get('kalshi', 0)
    
    # If we've never received a message, allow (startup grace period)
    if ws_last_message.get('coinbase', 0) == 0 or ws_last_message.get('kalshi', 0) == 0:
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
    kalshi_price = kalshi_prices.get(symbol, 0)
    coinbase_price = coinbase_prices.get(symbol, 0)
    
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
    global adx_trend_blocked
    
    if symbol not in adx_states or not adx_states[symbol].is_valid():
        return (True, "ADX not ready")
    
    adx = adx_states[symbol].get_adx()
    is_blocked = adx_trend_blocked.get(symbol, False)
    
    if is_blocked:
        # Currently blocked - need ADX to cool down below 22 to unblock
        if adx < ADX_COOLDOWN_THRESHOLD:
            adx_trend_blocked[symbol] = False
            log(f"🔄 {symbol} ADX cooled to {adx:.1f} - mean-reversion unlocked")
            return (True, f"ADX cooled: {adx:.1f} < {ADX_COOLDOWN_THRESHOLD}")
        else:
            return (False, f"ADX still trending: {adx:.1f} (needs < {ADX_COOLDOWN_THRESHOLD} to unlock)")
    else:
        # Not blocked - check if we should block
        if adx >= ADX_TREND_THRESHOLD:
            adx_trend_blocked[symbol] = True
            log(f"🚫 {symbol} ADX trending at {adx:.1f} - mean-reversion blocked")
            return (False, f"ADX trending: {adx:.1f} >= {ADX_TREND_THRESHOLD}")
        else:
            return (True, f"ADX ranging: {adx:.1f}")


def calculate_obi(client, ticker: str) -> float:
    """
    Calculate Order Book Imbalance (OBI) from Kalshi orderbook.
    
    OBI = (Bid Volume - Ask Volume) / Total Volume
    
    Range: -1.0 (all asks) to +1.0 (all bids)
    Positive OBI = more resting buy orders (supports longs)
    Negative OBI = more resting sell orders (supports shorts)
    """
    try:
        ob = client.get_orderbook(ticker, depth=OBI_DEPTH_LEVELS)
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
    global circuit_breaker_tripped
    
    if circuit_breaker_tripped:
        return (False, f"Circuit breaker TRIPPED: {consecutive_losses} consecutive losses")
    
    if consecutive_losses >= CIRCUIT_BREAKER_CONSECUTIVE_LOSSES:
        circuit_breaker_tripped = True
        log(f"🚨 CIRCUIT BREAKER TRIPPED: {consecutive_losses} consecutive stop-losses!")
        
        # Save to disk so watchdog knows not to restart
        from state_manager import save_circuit_breaker
        save_circuit_breaker(consecutive_losses)
        
        return (False, f"Circuit breaker triggered: {consecutive_losses} consecutive losses")
    
    return (True, f"Circuit breaker OK ({consecutive_losses} consecutive losses)")


def validate_entry_gates(client, symbol: str, side: str, entry_price: float, target_price: float) -> Tuple[bool, str]:
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
    global trading_halted, halt_reason
    
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
        trading_halted = True
        halt_reason = f"{symbol} spread corridor breach: {divergence*100:.3f}%"
        return (False, f"Spread corridor: {divergence*100:.3f}% > {SPREAD_CORRIDOR_MAX_PCT*100:.2f}% max")
    else:
        trading_halted = False
        halt_reason = ""
    
    # Gate 3: ADX Trend Filter with Hysteresis
    adx_ok, adx_info = check_adx_hysteresis(symbol)
    if not adx_ok:
        return (False, adx_info)
    
    # Gate 4: OBI Confirmation
    obi = calculate_obi(client, ticker)
    
    if side == 'long' and obi < OBI_MIN_THRESHOLD:
        return (False, f"OBI unsupportive for long: {obi:+.2f} < +{OBI_MIN_THRESHOLD}")
    elif side == 'short' and obi > -OBI_MIN_THRESHOLD:
        return (False, f"OBI unsupportive for short: {obi:+.2f} > -{OBI_MIN_THRESHOLD}")
    
    # All gates passed
    adx_value = adx_states[symbol].get_adx() if symbol in adx_states and adx_states[symbol].is_valid() else 0.0
    return (True, f"All gates passed (OBI: {obi:+.2f}, ADX: {adx_value:.1f})")


# ============================================================
# GLOBAL STATE
# ============================================================

ws_connected = {'kalshi': False, 'coinbase': False}
ws_last_message = {'kalshi': 0.0, 'coinbase': 0.0}

# Per-symbol state
vwap_states: Dict[str, VWAPState] = {}
cvd_states: Dict[str, CVDState] = {}
adx_states: Dict[str, 'ADXState'] = {}  # ADX trend filter
price_history: Dict[str, deque] = {}  # For high/low detection (Coinbase)

# Cross-venue price tracking for spread corridor
kalshi_prices: Dict[str, float] = {}   # Latest Kalshi mid prices
coinbase_prices: Dict[str, float] = {} # Latest Coinbase spot prices

# Trading halted flag (spread corridor breach)
trading_halted: bool = False
halt_reason: str = ""

# ADX Hysteresis state (prevents flapping at threshold boundary)
adx_trend_blocked: Dict[str, bool] = {}  # Per-symbol trend block state

# Circuit breaker state
consecutive_losses: int = 0
circuit_breaker_tripped: bool = False

# Exit targets - stored locally for each position (by ticker)
# Format: {ticker: {'stop_loss': float, 'target_price': float, 'side': str, 'entry_price': float}}
exit_targets: Dict[str, dict] = {}

# Pending orders - track unfilled orders
pending_orders: Dict[str, dict] = {}

# Stale order threshold - cancel if price moved more than this from order
STALE_ORDER_PRICE_THRESHOLD = 0.005  # 0.5% price deviation = cancel order

# Rate limiting
trades_this_hour = 0
last_hour = datetime.now().hour

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
# KALSHI CLIENT (from existing bot)
# ============================================================

class KalshiClient:
    def __init__(self):
        self.base_url = 'https://api.elections.kalshi.com'
        with open(KEY_PATH, 'rb') as f:
            self.private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
    
    def _sign(self, ts: str, method: str, path: str) -> str:
        msg = f"{ts}{method}{path}".encode('utf-8')
        signature = self.private_key.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')
    
    def _headers(self, method: str, path: str) -> dict:
        ts = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        return {
            'KALSHI-ACCESS-KEY': API_KEY,
            'KALSHI-ACCESS-SIGNATURE': self._sign(ts, method, path),
            'KALSHI-ACCESS-TIMESTAMP': ts,
            'Content-Type': 'application/json'
        }
    
    def get_balance(self) -> float:
        """Get PERPS margin balance (not predictions balance)."""
        import requests
        # Perps uses /margin/balance endpoint
        path = '/trade-api/v2/margin/balance'
        r = requests.get(self.base_url + path, headers=self._headers('GET', path), timeout=10)
        data = r.json()
        
        # Parse perps balance structure
        # Check main subaccount (id=0) for AVAILABLE balance
        for sub in data.get('subaccount_balances', []):
            if sub.get('subaccount') == 0:
                available = float(sub.get('available_balance', 0))
                if available > 0:
                    return available
                # Fall back to account_equity if available is 0
                equity = float(sub.get('account_equity', 0))
                if equity > 0:
                    return equity
        
        # Last resort: settled_funds
        return float(data.get('settled_funds', 0))
    
    def place_order(self, ticker: str, side: str, count: int, price: float, 
                    reduce_only: bool = False) -> dict:
        """
        Place limit order on Kalshi PERPS.
        
        Args:
            ticker: Market ticker (e.g., 'KXBTCPERP')
            side: 'long'/'buy' or 'short'/'sell'
            count: Number of contracts (integer)
            price: Price per contract in dollars (e.g., 6.47 for BTC at ~$64,700)
            reduce_only: If True, only reduces existing position
        """
        import requests
        import uuid
        
        # PERPS endpoint (not predictions)
        path = '/trade-api/v2/margin/orders'
        
        # Convert side to bid/ask
        api_side = 'bid' if side.lower() in ('long', 'buy', 'bid') else 'ask'
        
        # Time in force and post_only for maker fees
        # Entry orders: GTC + post_only = maker fee (0.01%)
        # Exit orders: IOC = may be taker (0.035%) but ensures execution
        tif = 'immediate_or_cancel' if reduce_only else 'good_till_canceled'
        
        order_data = {
            'ticker': ticker,
            'client_order_id': str(uuid.uuid4()),
            'side': api_side,
            'count': str(int(count)),  # Integer string
            'price': f'{price:.4f}',
            'time_in_force': tif,
            'self_trade_prevention_type': 'taker_at_cross'
        }
        
        # Entry orders use post_only for guaranteed maker fees
        if not reduce_only:
            order_data['post_only'] = True
        
        if reduce_only:
            order_data['reduce_only'] = True
        
        try:
            r = requests.post(
                self.base_url + path, 
                headers=self._headers('POST', path), 
                json=order_data, 
                timeout=10
            )
            return r.json()
        except Exception as e:
            return {'error': str(e)}
    
    def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order by order_id."""
        import requests
        
        path = f'/trade-api/v2/margin/orders/{order_id}'
        try:
            r = requests.delete(
                self.base_url + path,
                headers=self._headers('DELETE', path),
                timeout=10
            )
            return r.json()
        except Exception as e:
            return {'error': str(e)}
    
    def get_positions(self) -> List[dict]:
        """Get all open positions from Kalshi (live data)."""
        import requests
        
        path = '/trade-api/v2/margin/positions'
        try:
            r = requests.get(self.base_url + path, headers=self._headers('GET', path), timeout=10)
            data = r.json()
            
            positions = []
            for p in data.get('positions', []):
                pos_size = float(p.get('position', 0))
                if pos_size != 0:
                    positions.append({
                        'ticker': p.get('market_ticker', ''),
                        'size': pos_size,  # Negative = short
                        'side': 'long' if pos_size > 0 else 'short',
                        'contracts': abs(int(pos_size)),
                        'entry_price': float(p.get('entry_price', 0)),
                        'margin_used': float(p.get('margin_used', 0)),
                        'unrealized_pnl': float(p.get('unrealized_pnl', 0))
                    })
            return positions
        except Exception as e:
            log(f"Error fetching positions: {e}")
            return []
    
    def get_orderbook(self, ticker: str, depth: int = 5) -> dict:
        """Get orderbook for a perp market."""
        import requests
        path = f'/trade-api/v2/margin/markets/{ticker}/orderbook?depth={depth}'
        r = requests.get(self.base_url + path, headers=self._headers('GET', path.split('?')[0]), timeout=10)
        return r.json()
    
    def get_best_prices(self, ticker: str) -> Tuple[float, float]:
        """Get best bid and ask prices."""
        ob = self.get_orderbook(ticker)
        orderbook = ob.get('orderbook', {})
        bids = orderbook.get('bids', [])
        asks = orderbook.get('asks', [])
        
        best_bid = float(bids[0][0]) if bids else 0
        best_ask = float(asks[0][0]) if asks else 0
        
        return (best_bid, best_ask)

# ============================================================
# WEBSOCKET HANDLERS
# ============================================================

async def kalshi_websocket():
    """Kalshi WebSocket for real-time perp data with exponential backoff."""
    global ws_connected
    
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
                ws_connected['kalshi'] = True
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
            ws_connected['kalshi'] = False
            log(f"[KALSHI WS] Error: {e}, reconnecting in {reconnect_delay:.1f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)  # Exponential backoff


def handle_kalshi_message(data: dict):
    """Process Kalshi WebSocket message."""
    ws_last_message['kalshi'] = time.time()
    
    msg_type = data.get('type')
    
    if msg_type == 'trade':
        # Trade execution - update CVD
        ticker = data.get('msg', {}).get('ticker', '')
        symbol = next((s for s, t in PERP_TICKERS.items() if t == ticker), None)
        
        if symbol:
            trade = data.get('msg', {})
            contract_price = float(trade.get('price', 0))
            size = float(trade.get('count', 0))
            side = 'buy' if trade.get('taker_side') == 'yes' else 'sell'
            
            # Convert contract price to spot for spread corridor tracking
            spot_price = contract_to_spot_price(symbol, contract_price)
            kalshi_prices[symbol] = spot_price
            
            # Update CVD
            if symbol not in cvd_states:
                cvd_states[symbol] = CVDState()
            cvd_states[symbol].add_trade(spot_price, size, side)
            
            # Update VWAP (candle-based)
            if symbol not in vwap_states:
                vwap_states[symbol] = VWAPState()
            vwap_states[symbol].process_trade(spot_price, size, side)
    
    elif msg_type == 'ticker':
        # Price update
        ticker = data.get('msg', {}).get('ticker', '')
        symbol = next((s for s, t in PERP_TICKERS.items() if t == ticker), None)
        
        if symbol:
            contract_price = float(data.get('msg', {}).get('last_price', 0))
            if contract_price > 0:
                # Convert to spot price for spread corridor
                spot_price = contract_to_spot_price(symbol, contract_price)
                kalshi_prices[symbol] = spot_price
                
                if symbol not in price_history:
                    price_history[symbol] = deque(maxlen=100)
                price_history[symbol].append({'time': time.time(), 'price': spot_price})


async def coinbase_websocket():
    """Coinbase Advanced Trade WebSocket for live market data with exponential backoff."""
    global ws_connected
    
    products = ['BTC-USD', 'ETH-USD']
    reconnect_delay = 1.0  # Start with 1 second
    max_delay = 60.0  # Cap at 60 seconds
    
    while True:
        try:
            log("[COINBASE WS] Connecting...")
            async with websockets.connect(COINBASE_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                ws_connected['coinbase'] = True
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
            ws_connected['coinbase'] = False
            log(f"[COINBASE WS] Error: {e}, reconnecting in {reconnect_delay:.1f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)  # Exponential backoff


def handle_coinbase_message(data: dict):
    """Process Coinbase Advanced Trade WebSocket message."""
    ws_last_message['coinbase'] = time.time()
    
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
                    coinbase_prices[symbol] = price
                    
                    # Update VWAP (candle-based)
                    if symbol not in vwap_states:
                        vwap_states[symbol] = VWAPState()
                    
                    # Track candle count before processing
                    prev_candle_count = len(vwap_states[symbol].candles)
                    vwap_states[symbol].process_trade(price, size, side)
                    new_candle_count = len(vwap_states[symbol].candles)
                    
                    # Update CVD
                    if symbol not in cvd_states:
                        cvd_states[symbol] = CVDState()
                    cvd_states[symbol].add_trade(price, size, side)
                    
                    # Update ADX when a new candle completes
                    if symbol not in adx_states:
                        adx_states[symbol] = ADXState(period=ADX_PERIOD)
                    
                    # If a candle just completed, update ADX with it
                    if new_candle_count > prev_candle_count and vwap_states[symbol].candles:
                        completed = vwap_states[symbol].candles[-1]
                        adx_states[symbol].add_candle(completed.high, completed.low, completed.close)
                    
                    # Update price history for swing detection
                    if symbol not in price_history:
                        price_history[symbol] = deque(maxlen=100)
                    price_history[symbol].append({'time': time.time(), 'price': price})

# ============================================================
# STRATEGY LOGIC
# ============================================================

# NOTE: No daily VWAP reset - we use a rolling window instead


def detect_price_extreme(symbol: str) -> Tuple[bool, bool]:
    """Detect if price just made a new high or low in recent window."""
    if symbol not in price_history or len(price_history[symbol]) < 10:
        return (False, False)
    
    prices = [p['price'] for p in price_history[symbol]]
    current = prices[-1]
    recent_high = max(prices[:-1])
    recent_low = min(prices[:-1])
    
    made_higher_high = current > recent_high
    made_lower_low = current < recent_low
    
    return (made_higher_high, made_lower_low)


def check_entry_signal(symbol: str, current_price: float) -> Optional[dict]:
    """
    Check for VWAP reversal entry signal.
    
    Entry Rules:
    - Price > VWAP + 2σ AND CVD falling/flat → SHORT
    - Price < VWAP - 2σ AND CVD rising/flat → LONG
    
    Target: VWAP central line
    Stop: Last swing high/low ± 0.1%
    """
    global trades_this_hour, last_hour
    
    # Rate limit
    current_hour = datetime.now().hour
    if current_hour != last_hour:
        trades_this_hour = 0
        last_hour = current_hour
    
    if trades_this_hour >= MAX_TRADES_PER_HOUR:
        return None
    
    # Skip if already in position or pending order
    # Check exit_targets (which tracks positions we're managing)
    ticker = PERP_TICKERS.get(symbol)
    if ticker in exit_targets or symbol in pending_orders:
        return None
    
    # Get VWAP state
    if symbol not in vwap_states:
        return None
    
    vwap_state = vwap_states[symbol]
    cvd_state = cvd_states.get(symbol, CVDState())
    
    # Require valid VWAP data
    if not vwap_state.is_valid():
        return None
    
    # Get VWAP and bands
    lower_band, vwap, upper_band = vwap_state.get_bands(STD_DEV_MULTIPLIER)
    
    if vwap == 0:
        return None
    
    # Get CVD trend
    cvd_trend = cvd_state.get_cvd_trend()
    
    # SHORT SIGNAL: Price > VWAP + 2σ AND CVD falling/flat
    if current_price >= upper_band:
        if cvd_trend in ('falling', 'flat'):
            # Calculate profit distance to VWAP
            profit_distance = current_price - vwap
            
            # Fee check: profit must exceed fees + minimum margin
            fee_cost = current_price * TOTAL_FEE_RATE
            min_required = current_price * (TOTAL_FEE_RATE + MIN_PROFIT_MARGIN)
            
            if profit_distance < min_required:
                log(f"⚠️ SHORT {symbol}: Skipping - profit ${profit_distance:.2f} < min ${min_required:.2f} (fees)")
                return None
            
            # Find last swing high for stop
            swing_high = max(p['price'] for p in price_history.get(symbol, [{'price': current_price}]))
            stop_loss = swing_high * (1 + STOP_LOSS_BEYOND_WICK_PCT)
            
            log(f"🔴 SHORT SIGNAL: {symbol}")
            log(f"   Price ${current_price:,.2f} > Upper ${upper_band:,.2f}")
            log(f"   Profit to VWAP: ${profit_distance:,.2f} (fee break-even: ${fee_cost:.2f})")
            log(f"   CVD trend: {cvd_trend}")
            
            return {
                'symbol': symbol,
                'ticker': PERP_TICKERS[symbol],
                'side': 'short',
                'entry_price': current_price,
                'stop_loss': stop_loss,
                'target_price': vwap,
                'reason': f'Price above +2σ, CVD {cvd_trend}'
            }
    
    # LONG SIGNAL: Price < VWAP - 2σ AND CVD rising/flat
    elif current_price <= lower_band:
        if cvd_trend in ('rising', 'flat'):
            # Calculate profit distance to VWAP
            profit_distance = vwap - current_price
            
            # Fee check: profit must exceed fees + minimum margin
            fee_cost = current_price * TOTAL_FEE_RATE
            min_required = current_price * (TOTAL_FEE_RATE + MIN_PROFIT_MARGIN)
            
            if profit_distance < min_required:
                log(f"⚠️ LONG {symbol}: Skipping - profit ${profit_distance:.2f} < min ${min_required:.2f} (fees)")
                return None
            
            # Find last swing low for stop
            swing_low = min(p['price'] for p in price_history.get(symbol, [{'price': current_price}]))
            stop_loss = swing_low * (1 - STOP_LOSS_BEYOND_WICK_PCT)
            
            log(f"🟢 LONG SIGNAL: {symbol}")
            log(f"   Price ${current_price:,.2f} < Lower ${lower_band:,.2f}")
            log(f"   Profit to VWAP: ${profit_distance:,.2f} (fee break-even: ${fee_cost:.2f})")
            log(f"   CVD trend: {cvd_trend}")
            
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


def calculate_position_size(client: KalshiClient, symbol: str, 
                           entry_price: float, stop_loss: float) -> int:
    """
    Calculate position size with CONSERVATIVE margin constraints.
    
    Hard caps:
    1. Max 30% of balance used as margin
    2. Max 10% risk per trade (loss at stop)
    3. Assumes 5x effective leverage (conservative)
    
    Args:
        client: Kalshi client
        symbol: 'BTC' or 'ETH'
        entry_price: Spot price at entry
        stop_loss: Spot price for stop loss
    
    Returns:
        Number of contracts (integer)
    """
    balance = client.get_balance()
    if balance <= 0:
        return 0
    
    contract_size = CONTRACT_SIZES.get(symbol, 0.0001)
    contract_price = entry_price * contract_size
    
    # Stop distance in spot price
    stop_distance_spot = abs(entry_price - stop_loss)
    
    # Minimum stop distance: 0.3% (prevents oversized positions from tight stops)
    min_stop_distance = entry_price * 0.003
    if stop_distance_spot < min_stop_distance:
        log(f"  ⚠️ Stop too tight ({stop_distance_spot:.2f}), using min {min_stop_distance:.2f}")
        stop_distance_spot = min_stop_distance
    
    # Loss per contract at stop
    loss_per_contract = stop_distance_spot * contract_size
    
    # CONSTRAINT 1: Max 10% risk (loss at stop)
    max_risk = balance * 0.10  # 10% max loss
    contracts_from_risk = int(max_risk / loss_per_contract)
    
    # CONSTRAINT 2: Max 30% margin usage (hard cap)
    max_margin = balance * 0.30
    # Assume 5x effective leverage (conservative - Kalshi often uses less)
    effective_leverage = 5.0
    margin_per_contract = contract_price / effective_leverage
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
    global trades_this_hour
    
    symbol = signal['symbol']
    ticker = signal['ticker']
    
    log("=" * 50)
    log(f"ENTRY SIGNAL: {signal['side'].upper()} {symbol}")
    log(f"  {signal['reason']}")
    log(f"  Entry: ${signal['entry_price']:,.2f}")
    log(f"  Stop: ${signal['stop_loss']:,.2f}")
    log(f"  Target (VWAP): ${signal['target_price']:,.2f}")
    
    # === SAFETY GATES ===
    can_enter, gate_reason = validate_entry_gates(
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
    
    # Calculate size (number of contracts)
    contracts = calculate_position_size(client, symbol, signal['entry_price'], signal['stop_loss'])
    if contracts <= 0:
        log("  Size too small, skipping")
        return
    
    # Convert spot price to contract price for order
    contract_price = spot_to_contract_price(symbol, signal['entry_price'])
    
    log(f"  Contracts: {contracts}")
    log(f"  Contract price: ${contract_price:.4f}")
    
    # Place order (with post_only for maker fees)
    result = client.place_order(
        ticker, 
        signal['side'], 
        contracts, 
        contract_price,
        reduce_only=False
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
        if symbol in pending_orders:
            del pending_orders[symbol]
        return
    
    if result.get('order') or result.get('order_id'):
        order_id = result.get('order_id', result.get('order', {}).get('order_id', 'unknown'))
        log(f"  ✅ Order placed: {order_id}")
        
        # Track as pending order (not filled position yet)
        pending_orders[symbol] = {
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
        
        trades_this_hour += 1
        
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
    for symbol in list(pending_orders.keys()):
        pending = pending_orders[symbol]
        
        # Get current price
        if symbol not in price_history or not price_history[symbol]:
            continue
        
        current_price = price_history[symbol][-1]['price']
        order_spot_price = pending['entry_price']
        
        # Check if order is stale (price moved too far)
        price_deviation = abs(current_price - order_spot_price) / order_spot_price
        
        if price_deviation > STALE_ORDER_PRICE_THRESHOLD:
            # Cancel stale order
            log(f"⚠️ STALE ORDER: {symbol} @ ${order_spot_price:,.2f} (current: ${current_price:,.2f}, {price_deviation*100:.2f}% deviation)")
            try:
                result = client.cancel_order(pending['order_id'])
                log(f"   Cancelled order {pending['order_id'][:8]}...")
                del pending_orders[symbol]
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
    Create exit_targets for positions that don't have them.
    """
    global exit_targets
    
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
        
        # Check if we already have exit targets
        if ticker in exit_targets:
            log(f"  {symbol}: Already tracking exit targets")
            continue
        
        log(f"  Found {symbol}: {side.upper()} {contracts} @ ${spot_price:,.2f}")
        
        # Create exit targets
        target = vwap_states[symbol].vwap if symbol in vwap_states else spot_price
        
        if side == 'long':
            stop_loss = spot_price * 0.98  # 2% below
        else:
            stop_loss = spot_price * 1.02  # 2% above
        
        exit_targets[ticker] = {
            'stop_loss': stop_loss,
            'target_price': target,
            'side': side,
            'entry_price': spot_price
        }
        
        log(f"    Stop: ${stop_loss:,.2f} | Target: ${target:,.2f}")


def check_order_fills(client: KalshiClient):
    """
    Check if any pending orders have filled by querying the API.
    Called periodically since we may miss WebSocket fill events.
    """
    import requests
    
    for symbol in list(pending_orders.keys()):
        pending = pending_orders[symbol]
        order_id = pending['order_id']
        
        try:
            path = f'/trade-api/v2/margin/orders/{order_id}'
            resp = requests.get(client.base_url + path, headers=client._headers('GET', path), timeout=10)
            order = resp.json()
            
            remaining = float(order.get('remaining_count', 0))
            filled = float(order.get('fill_count', 0))
            
            if remaining == 0 and filled > 0:
                # Order fully filled - store exit targets
                log(f"✅ ORDER FILLED: {symbol} {filled:.0f} contracts")
                
                # Store exit targets (position will be tracked via Kalshi API)
                exit_targets[pending['ticker']] = {
                    'stop_loss': pending['stop_loss'],
                    'target_price': pending['target_price'],
                    'side': pending['side'],
                    'entry_price': pending['entry_price']
                }
                
                del pending_orders[symbol]
                
                log_trade({
                    'type': 'order_filled',
                    'symbol': symbol,
                    'contracts': filled,
                    'order_id': order_id
                })
                
            elif remaining == 0 and filled == 0:
                # Order was cancelled externally
                log(f"⚠️ Order {order_id[:8]}... was cancelled externally")
                del pending_orders[symbol]
                
        except Exception as e:
            log(f"Error checking order {order_id[:8]}...: {e}")


async def manage_positions(client: KalshiClient):
    """
    Query Kalshi for live positions and check TP/SL.
    Uses exit_targets dict for stored stop_loss and target_price.
    """
    global exit_targets
    
    # Get LIVE positions from Kalshi
    live_positions = client.get_positions()
    
    # Clean up exit_targets for positions that no longer exist
    live_tickers = {p['ticker'] for p in live_positions}
    for ticker in list(exit_targets.keys()):
        if ticker not in live_tickers:
            log(f"[POSITION] Removing stale exit target for {ticker}")
            del exit_targets[ticker]
    
    for pos in live_positions:
        ticker = pos['ticker']
        side = pos['side']
        contracts = pos['contracts']
        entry_price_contract = pos['entry_price']  # Contract price
        
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
        if symbol not in price_history or not price_history[symbol]:
            continue
        current_price = price_history[symbol][-1]['price']
        
        # Get or create exit targets for this position
        if ticker not in exit_targets:
            # New position - calculate exit targets
            vwap = vwap_states[symbol].vwap if symbol in vwap_states else entry_price
            
            if side == 'long':
                stop_loss = entry_price * 0.98  # 2% below entry
                target_price = vwap
            else:
                stop_loss = entry_price * 1.02  # 2% above entry
                target_price = vwap
            
            exit_targets[ticker] = {
                'stop_loss': stop_loss,
                'target_price': target_price,
                'side': side,
                'entry_price': entry_price
            }
            log(f"[POSITION] New exit targets for {symbol} {side.upper()}: Stop ${stop_loss:,.2f}, Target ${target_price:,.2f}")
        
        targets = exit_targets[ticker]
        stop_loss = targets['stop_loss']
        target_price = targets['target_price']
        
        # Update target to current VWAP (dynamic target)
        if symbol in vwap_states:
            target_price = vwap_states[symbol].vwap
            exit_targets[ticker]['target_price'] = target_price
        
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
            
            # Place exit order
            exit_side = 'sell' if side == 'long' else 'buy'
            result = client.place_order(
                ticker,
                exit_side,
                contracts,
                exit_contract_price,
                reduce_only=True
            )
            
            if result.get('order') or result.get('order_id'):
                # PnL calculation
                contract_size = CONTRACT_SIZES.get(symbol, 0.0001)
                price_diff = current_price - entry_price
                if side == 'short':
                    price_diff = -price_diff
                pnl = price_diff * contract_size * contracts
                
                log(f"  Exit contracts: {contracts}")
                log(f"  PnL: ${pnl:+,.2f}")
                
                # === CIRCUIT BREAKER: Track consecutive losses ===
                global consecutive_losses
                is_stop_loss = "STOP LOSS" in exit_reason
                
                if is_stop_loss:
                    consecutive_losses += 1
                    log(f"  ⚠️ Consecutive losses: {consecutive_losses}/{CIRCUIT_BREAKER_CONSECUTIVE_LOSSES}")
                    if consecutive_losses >= CIRCUIT_BREAKER_CONSECUTIVE_LOSSES:
                        log(f"  🚨 CIRCUIT BREAKER will trip on next signal check!")
                else:
                    # Win - reset consecutive loss counter
                    if consecutive_losses > 0:
                        log(f"  ✅ Win! Resetting consecutive loss counter (was {consecutive_losses})")
                    consecutive_losses = 0
                
                # Remove exit targets
                del exit_targets[ticker]
                
                log_trade({
                    'type': 'exit',
                    'symbol': symbol,
                    'reason': exit_reason,
                    'exit_price': current_price,
                    'contracts': contracts,
                    'pnl': pnl,
                    'consecutive_losses': consecutive_losses,
                    'is_stop_loss': is_stop_loss
                })
            else:
                log(f"  ❌ Exit failed: {result}")
            
            if should_exit:
                del positions[symbol]


# ============================================================
# MAIN LOOP
# ============================================================

def log_comprehensive_status(client: KalshiClient):
    """Log comprehensive status snapshot."""
    global last_status_log
    
    now = time.time()
    if now - last_status_log < STATUS_LOG_INTERVAL:
        return
    last_status_log = now
    
    try:
        # Get balance
        balance = client.get_balance()
        
        # Get prices
        btc_bid, btc_ask = client.get_best_prices('KXBTCPERP')
        eth_bid, eth_ask = client.get_best_prices('KXETHPERP')
        
        btc_spot = contract_to_spot_price('BTC', btc_bid)
        eth_spot = contract_to_spot_price('ETH', eth_bid)
        
        # VWAP states
        vwap_info = {}
        for symbol in PERP_TICKERS:
            if symbol in vwap_states:
                state = vwap_states[symbol]
                lower, vwap, upper = state.get_bands(STD_DEV_MULTIPLIER)
                current = state.current_price
                dev, direction = state.get_deviation(current)
                
                vwap_info[symbol] = {
                    'vwap': round(vwap, 2),
                    'lower_band': round(lower, 2),
                    'upper_band': round(upper, 2),
                    'current_price': round(current, 2),
                    'deviation_sd': round(dev, 2),
                    'direction': direction,
                    'candle_count': state.candle_count
                }
        
        # CVD states
        cvd_info = {}
        for symbol in PERP_TICKERS:
            if symbol in cvd_states:
                cvd = cvd_states[symbol].get_cvd()
                trend = cvd_states[symbol].get_cvd_trend()
                cvd_info[symbol] = {'value': round(cvd, 2), 'trend': trend}
        
        # Get LIVE positions from Kalshi
        live_positions = client.get_positions()
        pos_info = {}
        for pos in live_positions:
            ticker = pos['ticker']
            # Find symbol
            for sym, tick in PERP_TICKERS.items():
                if tick == ticker:
                    entry_spot = contract_to_spot_price(sym, pos['entry_price'])
                    targets = exit_targets.get(ticker, {})
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
            if symbol in adx_states and adx_states[symbol].is_valid():
                adx = adx_states[symbol].get_adx()
                trending = adx_states[symbol].is_trending(ADX_TREND_THRESHOLD)
                adx_info[symbol] = {'value': round(adx, 1), 'trending': trending}
        
        # Spread corridor status
        spread_info = {}
        for symbol in PERP_TICKERS:
            is_safe, divergence = check_spread_corridor(symbol)
            spread_info[symbol] = {'safe': is_safe, 'divergence': round(divergence * 100, 3)}
        
        # Console output
        log("-" * 50)
        halt_status = f" | ⚠️ HALTED: {halt_reason}" if trading_halted else ""
        log(f"STATUS | Balance: ${balance:.2f} | Positions: {len(live_positions)} | Pending: {len(pending_orders)}{halt_status}")
        log(f"  BTC: ${btc_spot:,.0f} | ETH: ${eth_spot:,.0f}")
        
        for symbol, info in vwap_info.items():
            if info['vwap'] > 0:
                adx_str = f" | ADX: {adx_info.get(symbol, {}).get('value', 'N/A')}" if symbol in adx_info else ""
                spread_str = f" | Spread: {spread_info.get(symbol, {}).get('divergence', 0):.3f}%"
                log(f"  {symbol} VWAP: ${info['vwap']:,.0f} | ±2σ: ${info['lower_band']:,.0f}-${info['upper_band']:,.0f} | Dev: {info['deviation_sd']:.1f}σ {info['direction']}{adx_str}{spread_str}")
        
        for symbol, cvd in cvd_info.items():
            log(f"  {symbol} CVD: {cvd['value']:+.1f} ({cvd['trend']})")
        
        for symbol, pending in pending_orders.items():
            log(f"  PENDING {symbol}: {pending['side'].upper()} {pending['contracts']} @ ${pending['entry_price']:,.0f}")
        
        for symbol, pos in pos_info.items():
            log(f"  POSITION {symbol}: {pos['side'].upper()} {pos['contracts']} @ ${pos['entry_price']:,.0f}")
        
        # Log to file
        log_status({
            'balance': balance,
            'prices': {
                'BTC': {'bid': btc_bid, 'ask': btc_ask, 'spot': btc_spot},
                'ETH': {'bid': eth_bid, 'ask': eth_ask, 'spot': eth_spot}
            },
            'vwap': vwap_info,
            'cvd': cvd_info,
            'positions': pos_info,
            'ws_connected': ws_connected.copy(),
            'trades_this_hour': trades_this_hour
        })
        
    except Exception as e:
        log(f"Status logging error: {e}")


async def trading_loop(client: KalshiClient):
    """Main trading loop."""
    log("Starting trading loop...")
    
    while True:
        try:
            # Comprehensive status logging
            log_comprehensive_status(client)
            
            # Check for entry signals
            for symbol in PERP_TICKERS:
                if symbol in price_history and price_history[symbol]:
                    current_price = price_history[symbol][-1]['price']
                    
                    signal = check_entry_signal(symbol, current_price)
                    if signal:
                        await execute_entry(client, signal)
            
            # Manage pending orders (check fills, cancel stale)
            check_order_fills(client)
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
                save_state(exit_targets, trades_this_hour, total_pnl)
            except:
                pass
            await asyncio.sleep(5)


def recover_state():
    """Recover exit_targets from saved state file."""
    global exit_targets, trades_this_hour, total_pnl
    
    saved = load_state()
    if not saved:
        log("[STATE] No saved state found, starting fresh")
        return
    
    log(f"[STATE] Found saved state from {saved.get('saved_at_iso', 'unknown')}")
    
    # Recover exit targets (not positions - those come from Kalshi API)
    for ticker, target_data in saved.get('exit_targets', {}).items():
        exit_targets[ticker] = target_data
        log(f"[STATE] Recovered exit targets for {ticker}")
    
    # Note: VWAP and positions come from live sources
    log(f"[STATE] VWAP will be seeded fresh, positions from Kalshi API")
    
    trades_this_hour = saved.get('trades_this_hour', 0)
    total_pnl = saved.get('total_pnl', 0.0)
    
    log(f"[STATE] Recovery complete: {len(exit_targets)} exit targets, PnL: ${total_pnl:+,.2f}")


def periodic_state_save():
    """Save state periodically."""
    global last_state_save
    
    now = time.time()
    if now - last_state_save < STATE_SAVE_INTERVAL:
        return
    
    last_state_save = now
    try:
        save_state(exit_targets, trades_this_hour, total_pnl)
    except Exception as e:
        log(f"[STATE] Error saving state: {e}")


def seed_vwap_from_history():
    """
    Seed VWAP calculations with historical OHLCV candles from Coinbase.
    Uses the candles API for proper minute-by-minute data.
    """
    import requests
    
    log("Seeding VWAP from Coinbase candles...")
    
    coinbase_products = {
        'BTC': 'BTC-USD',
        'ETH': 'ETH-USD'
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
            
            # Initialize VWAP state
            if symbol not in vwap_states:
                vwap_states[symbol] = VWAPState()
            
            # Initialize CVD state
            if symbol not in cvd_states:
                cvd_states[symbol] = CVDState()
            
            # Initialize ADX state
            if symbol not in adx_states:
                adx_states[symbol] = ADXState(period=ADX_PERIOD)
            
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
                vwap_states[symbol].candles.append(candle)
                candles_added += 1
                
                # Update ADX with historical candle
                adx_states[symbol].add_candle(high, low, close)
                
                # Update price history
                if symbol not in price_history:
                    price_history[symbol] = deque(maxlen=100)
                price_history[symbol].append({'time': timestamp, 'price': close})
            
            # Set current candle to most recent
            if vwap_states[symbol].candles:
                vwap_states[symbol].current_candle = vwap_states[symbol].candles.pop()
            
            # Mark as today's session so check_reset doesn't clear seeded data
            vwap_states[symbol].last_reset_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            
            vwap = vwap_states[symbol].vwap
            lower, _, upper = vwap_states[symbol].get_bands(STD_DEV_MULTIPLIER)
            std = vwap_states[symbol].std_dev
            adx = adx_states[symbol].get_adx() if adx_states[symbol].is_valid() else 0.0
            log(f"  {symbol}: {candles_added} candles, VWAP: ${vwap:,.2f}, ±2σ: ${lower:,.2f}-${upper:,.2f}, ADX: {adx:.1f}")
            
        except Exception as e:
            log(f"  {symbol}: Error seeding - {e}")


async def main():
    log("=" * 60)
    log("KALSHI VWAP REVERSAL BOT")
    log("=" * 60)
    log(f"Assets: {list(PERP_TICKERS.keys())}")
    log(f"Entry: ±{STD_DEV_MULTIPLIER}σ bands | Session VWAP (resets 00:00 UTC)")
    log(f"Max margin: 30% | Max risk: 10%")
    log(f"Target: VWAP | Stop: Swing ±0.1%")
    log("=" * 60)
    
    # Recover state from previous run
    recover_state()
    
    # Seed VWAP from historical trades if we don't have enough data
    needs_seeding = True
    for symbol in PERP_TICKERS:
        if symbol in vwap_states and vwap_states[symbol].is_valid():
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
    
    # Cancel any stale orders from previous runs
    sync_orders_on_startup(client)
    
    # Sync existing positions from Kalshi
    sync_positions_on_startup(client)
    
    # Start WebSocket connections and trading loop
    await asyncio.gather(
        kalshi_websocket(),
        coinbase_websocket(),
        trading_loop(client)
    )


if __name__ == "__main__":
    asyncio.run(main())
