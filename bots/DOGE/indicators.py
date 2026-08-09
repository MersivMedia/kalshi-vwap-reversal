#!/usr/bin/env python3
"""
Technical Indicators for VWAP Reversal Bot

Contains:
- VWAPState: Session-anchored VWAP with standard deviation bands
- CVDState: Cumulative Volume Delta with trend detection
- ADXState: Average Directional Index for trend strength
"""

import math
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import List, Tuple, Optional
from collections import deque


@dataclass
class Candle:
    """OHLCV candle data."""
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
    def __init__(self, reset_hour_utc: int = 0, min_candles: int = 5):
        self.reset_hour_utc = reset_hour_utc
        self.min_candles = min_candles
        self.candles: List[Candle] = []
        self.current_candle: Optional[Candle] = None
        self.last_reset_date: Optional[datetime] = None
        
    def check_reset(self):
        """Check if we need to reset for new session."""
        now = datetime.now(timezone.utc)
        today = now.replace(hour=self.reset_hour_utc, minute=0, second=0, microsecond=0)
        
        if self.last_reset_date is None or self.last_reset_date < today:
            if self.candles or self.current_candle:
                print(f"[VWAP] Session reset - new trading day")
            self.candles = []
            self.current_candle = None
            self.last_reset_date = today
        
    def process_trade(self, price: float, size: float, side: str):
        """Process incoming trade tick into candle bars."""
        self.check_reset()
        
        now = datetime.now(timezone.utc)
        current_minute = now.replace(second=0, microsecond=0)
        
        if self.current_candle is None or self.current_candle.time != current_minute:
            # Start new candle - first finalize the previous one
            if self.current_candle is not None:
                self.candles.append(self.current_candle)
            
            self.current_candle = Candle(
                time=current_minute,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=size
            )
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
        return self.candle_count >= self.min_candles
    
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
    
    Uses configurable window for divergence detection.
    Resets periodically to keep numbers manageable.
    """
    def __init__(self, divergence_window_minutes: int = 30, reset_hours: int = 12):
        self.divergence_window_minutes = divergence_window_minutes
        self.reset_hours = reset_hours
        self.running_cvd: float = 0.0
        self.cvd_history: deque = deque(maxlen=5000)  # (timestamp, cvd_value, price)
        self.last_reset: float = time.time()
    
    def check_reset(self):
        """Reset CVD periodically to keep numbers manageable."""
        hours_since_reset = (time.time() - self.last_reset) / 3600
        if hours_since_reset >= self.reset_hours:
            print(f"[CVD] Periodic reset (was {self.running_cvd:+.2f})")
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
    
    def get_cvd_at_time(self, minutes_ago: int) -> Tuple[float, float]:
        """Get CVD value and price from N minutes ago."""
        cutoff = time.time() - (minutes_ago * 60)
        
        for t, cvd, price in self.cvd_history:
            if t >= cutoff:
                return (cvd, price)
        
        return (self.running_cvd, 0.0)
    
    def get_cvd_trend(self, window_minutes: int = None) -> str:
        """
        Determine CVD trend over window.
        Returns: 'rising', 'falling', or 'flat'
        """
        if window_minutes is None:
            window_minutes = self.divergence_window_minutes
            
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
        return self.get_cvd_trend() in ('falling', 'flat')
    
    def is_diverging_bullish(self) -> bool:
        """CVD rising or flat while price low = bullish divergence."""
        return self.get_cvd_trend() in ('rising', 'flat')


class ADXState:
    """
    Average Directional Index (ADX) for trend strength detection.
    ADX > 25 = trending market (don't fade bands)
    ADX < 20 = ranging market (mean reversion favorable)
    
    Uses Wilder's smoothing with configurable period.
    Supports hysteresis to prevent flapping at threshold.
    """
    def __init__(self, period: int = 14):
        self.period = period
        self.price_history: deque = deque(maxlen=period * 3)
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
            self.smoothed_tr = sum(list(self.tr_history)[-self.period:])
            self.smoothed_plus_dm = sum(list(self.plus_dm_history)[-self.period:])
            self.smoothed_minus_dm = sum(list(self.minus_dm_history)[-self.period:])
        else:
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
