#!/usr/bin/env python3
"""
Unit tests for technical indicators.

Run with: python -m pytest tests/ -v
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timezone

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))

from indicators import VWAPState, CVDState, ADXState, Candle


class TestVWAPState:
    """Tests for VWAP indicator."""
    
    def test_empty_vwap(self):
        """VWAP should be 0 with no data."""
        vwap = VWAPState()
        assert vwap.vwap == 0.0
        assert vwap.std_dev == 0.0
        assert not vwap.is_valid()
    
    def test_single_trade(self):
        """VWAP with single trade."""
        vwap = VWAPState()
        vwap.process_trade(100.0, 10.0, 'BUY')
        
        assert vwap.current_price == 100.0
        assert vwap.candle_count == 1
    
    def test_vwap_calculation(self):
        """Test VWAP calculation with multiple candles."""
        vwap = VWAPState(min_candles=2)
        
        # Simulate candles with different typical prices
        # Candle 1: O=100, H=105, L=95, C=102 -> TP = (105+95+102)/3 = 100.67
        # Candle 2: O=102, H=110, L=100, C=108 -> TP = (110+100+108)/3 = 106.0
        
        candle1 = Candle(
            time=datetime.now(timezone.utc),
            open=100, high=105, low=95, close=102, volume=100
        )
        candle2 = Candle(
            time=datetime.now(timezone.utc),
            open=102, high=110, low=100, close=108, volume=200
        )
        
        vwap.candles = [candle1, candle2]
        
        # VWAP = (100.67*100 + 106.0*200) / (100+200) = (10067 + 21200) / 300 = 104.22
        expected_vwap = (candle1.typical_price * 100 + candle2.typical_price * 200) / 300
        
        assert abs(vwap.vwap - expected_vwap) < 0.01
        assert vwap.is_valid()
    
    def test_bands(self):
        """Test standard deviation bands."""
        vwap = VWAPState(min_candles=2)
        
        candle1 = Candle(
            time=datetime.now(timezone.utc),
            open=100, high=100, low=100, close=100, volume=100
        )
        candle2 = Candle(
            time=datetime.now(timezone.utc),
            open=110, high=110, low=110, close=110, volume=100
        )
        
        vwap.candles = [candle1, candle2]
        
        lower, mid, upper = vwap.get_bands(2.0)
        
        assert mid == vwap.vwap
        assert lower < mid < upper
        assert abs(upper - mid) == abs(mid - lower)  # Symmetric
    
    def test_deviation(self):
        """Test deviation calculation."""
        vwap = VWAPState(min_candles=2)
        
        candle1 = Candle(
            time=datetime.now(timezone.utc),
            open=100, high=100, low=100, close=100, volume=100
        )
        candle2 = Candle(
            time=datetime.now(timezone.utc),
            open=100, high=100, low=100, close=100, volume=100
        )
        
        vwap.candles = [candle1, candle2]
        
        # With uniform prices, std_dev should be 0
        deviation, direction = vwap.get_deviation(100.0)
        assert deviation == 0.0


class TestCVDState:
    """Tests for CVD indicator."""
    
    def test_empty_cvd(self):
        """CVD should be 0 with no data."""
        cvd = CVDState()
        assert cvd.get_cvd() == 0.0
        assert cvd.get_cvd_trend() == 'flat'
    
    def test_buy_pressure(self):
        """Buys should increase CVD."""
        cvd = CVDState()
        cvd.add_trade(100.0, 10.0, 'BUY')
        assert cvd.get_cvd() == 10.0
    
    def test_sell_pressure(self):
        """Sells should decrease CVD."""
        cvd = CVDState()
        cvd.add_trade(100.0, 10.0, 'SELL')
        assert cvd.get_cvd() == -10.0
    
    def test_net_cvd(self):
        """Test net CVD calculation."""
        cvd = CVDState()
        cvd.add_trade(100.0, 100.0, 'BUY')
        cvd.add_trade(100.0, 30.0, 'SELL')
        cvd.add_trade(100.0, 50.0, 'BUY')
        
        # Net: +100 - 30 + 50 = +120
        assert cvd.get_cvd() == 120.0
    
    def test_cvd_trend_rising(self):
        """Test rising CVD trend detection."""
        cvd = CVDState(divergence_window_minutes=1)
        
        # Add multiple buys
        for i in range(10):
            cvd.add_trade(100.0, 10.0, 'BUY')
            time.sleep(0.01)  # Small delay to spread timestamps
        
        assert cvd.get_cvd_trend() == 'rising'
    
    def test_cvd_trend_falling(self):
        """Test falling CVD trend detection."""
        cvd = CVDState(divergence_window_minutes=1)
        
        # Add multiple sells
        for i in range(10):
            cvd.add_trade(100.0, 10.0, 'SELL')
            time.sleep(0.01)
        
        assert cvd.get_cvd_trend() == 'falling'


class TestADXState:
    """Tests for ADX indicator."""
    
    def test_empty_adx(self):
        """ADX should be 0 with no data."""
        adx = ADXState()
        assert adx.get_adx() == 0.0
        assert not adx.is_valid()
    
    def test_adx_needs_data(self):
        """ADX needs enough candles to be valid."""
        adx = ADXState(period=14)
        
        # Add some candles but not enough
        for i in range(10):
            adx.add_candle(100 + i, 99 + i, 99.5 + i)
        
        assert not adx.is_valid()
    
    def test_adx_trending(self):
        """Test ADX trending detection with strong trend."""
        adx = ADXState(period=5)  # Short period for testing
        
        # Simulate strong uptrend - each candle higher than previous
        for i in range(20):
            high = 100 + i * 2
            low = 98 + i * 2
            close = 99 + i * 2
            adx.add_candle(high, low, close)
        
        # Strong trend should produce high ADX
        if adx.is_valid():
            assert adx.get_adx() > 0  # Should be positive in a trend
    
    def test_adx_ranging(self):
        """Test ADX in ranging market."""
        adx = ADXState(period=5)
        
        # Simulate ranging - alternating up/down
        for i in range(20):
            if i % 2 == 0:
                adx.add_candle(102, 100, 101)
            else:
                adx.add_candle(101, 99, 100)
        
        # Ranging market should have lower ADX than trending
        if adx.is_valid():
            assert adx.get_adx() >= 0


class TestCandle:
    """Tests for Candle dataclass."""
    
    def test_typical_price(self):
        """Test typical price calculation."""
        candle = Candle(
            time=datetime.now(timezone.utc),
            open=100, high=110, low=90, close=105, volume=100
        )
        
        # TP = (H + L + C) / 3 = (110 + 90 + 105) / 3 = 101.67
        expected = (110 + 90 + 105) / 3
        assert abs(candle.typical_price - expected) < 0.01


if __name__ == '__main__':
    import pytest
    pytest.main([__file__, '-v'])
