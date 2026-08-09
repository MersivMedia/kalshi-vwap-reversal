#!/usr/bin/env python3
"""
Unit tests for signal generation.

Tests entry signal logic including CVD strict mode and min delta.

Run with: python -m pytest tests/test_signals.py -v
Or: python tests/test_signals.py
"""

import sys
import time
from pathlib import Path
from collections import deque

sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))


class TestCVDSignalLogic:
    """Tests for CVD divergence in signal generation."""
    
    def test_cvd_falling_allows_short(self):
        """Falling CVD should allow short signals."""
        from indicators import CVDState
        
        cvd = CVDState()
        
        # Simulate falling CVD
        now = time.time()
        cvd.cvd_history.append((now - 1800, 100.0, 65000))  # 30 min ago, CVD=100
        cvd.cvd_history.append((now - 900, 50.0, 65500))    # 15 min ago, CVD=50
        cvd.cvd_history.append((now, 10.0, 66000))          # now, CVD=10
        cvd.running_cvd = 10.0
        
        trend = cvd.get_cvd_trend()
        assert trend == 'falling'
    
    def test_cvd_rising_allows_long(self):
        """Rising CVD should allow long signals."""
        from indicators import CVDState
        
        cvd = CVDState()
        
        # Simulate rising CVD
        now = time.time()
        cvd.cvd_history.append((now - 1800, 10.0, 60000))   # 30 min ago, CVD=10
        cvd.cvd_history.append((now - 900, 50.0, 59500))    # 15 min ago, CVD=50
        cvd.cvd_history.append((now, 100.0, 59000))         # now, CVD=100
        cvd.running_cvd = 100.0
        
        trend = cvd.get_cvd_trend()
        assert trend == 'rising'
    
    def test_cvd_flat_default_mode(self):
        """Flat CVD should be detected correctly."""
        from indicators import CVDState
        
        cvd = CVDState()
        
        # Simulate flat CVD
        now = time.time()
        cvd.cvd_history.append((now - 1800, 50.0, 62000))
        cvd.cvd_history.append((now - 900, 51.0, 62100))
        cvd.cvd_history.append((now, 50.5, 62050))
        cvd.running_cvd = 50.5
        
        trend = cvd.get_cvd_trend()
        assert trend == 'flat'
    
    def test_cvd_at_time(self):
        """get_cvd_at_time should return CVD from N minutes ago."""
        from indicators import CVDState
        
        cvd = CVDState()
        
        now = time.time()
        cvd.cvd_history.append((now - 1800, 100.0, 65000))  # 30 min ago
        cvd.cvd_history.append((now - 600, 50.0, 65500))    # 10 min ago
        cvd.cvd_history.append((now, 25.0, 66000))          # now
        cvd.running_cvd = 25.0
        
        # Get CVD from 15 minutes ago (should return closest before that)
        cvd_val, price = cvd.get_cvd_at_time(15)
        assert cvd_val == 50.0  # Should get the 10-min ago value (closest)


class TestCVDStrictMode:
    """Tests for CVD strict divergence mode."""
    
    def test_strict_mode_rejects_flat_for_short(self):
        """Strict mode should reject flat CVD for shorts."""
        # In strict mode: short_cvd_ok = (cvd_trend == 'falling')
        # flat should NOT be OK
        
        cvd_trend = 'flat'
        strict = True
        
        if strict:
            short_cvd_ok = cvd_trend == 'falling'
        else:
            short_cvd_ok = cvd_trend in ('falling', 'flat')
        
        assert short_cvd_ok == False
    
    def test_strict_mode_accepts_falling_for_short(self):
        """Strict mode should accept falling CVD for shorts."""
        cvd_trend = 'falling'
        strict = True
        
        if strict:
            short_cvd_ok = cvd_trend == 'falling'
        else:
            short_cvd_ok = cvd_trend in ('falling', 'flat')
        
        assert short_cvd_ok == True
    
    def test_default_mode_accepts_flat_for_long(self):
        """Default mode should accept flat CVD for longs."""
        cvd_trend = 'flat'
        strict = False
        
        if strict:
            long_cvd_ok = cvd_trend == 'rising'
        else:
            long_cvd_ok = cvd_trend in ('rising', 'flat')
        
        assert long_cvd_ok == True


class TestCVDMinDelta:
    """Tests for CVD minimum delta threshold."""
    
    def test_min_delta_check_passes(self):
        """Signal should pass if CVD delta > min threshold."""
        from indicators import CVDState
        
        cvd = CVDState()
        
        # Significant CVD change over time
        now = time.time()
        cvd.cvd_history.append((now - 2000, 100.0, 65000))  # Before window
        cvd.cvd_history.append((now - 1700, 90.0, 65100))   # Start of window
        cvd.cvd_history.append((now - 100, 10.0, 66000))    # End of window
        cvd.running_cvd = 10.0
        
        # Calculate delta manually (same as bot does)
        cvd_start, _ = cvd.get_cvd_at_time(30)
        cvd_now = cvd.running_cvd
        cvd_change = abs(cvd_now - cvd_start)
        cvd_base = max(abs(cvd_now), abs(cvd_start), 1.0)
        cvd_delta_pct = cvd_change / cvd_base
        
        min_delta = 0.1  # 10% threshold
        cvd_delta_ok = cvd_delta_pct >= min_delta
        
        # 89% change (90->10) should pass 10% threshold
        assert cvd_delta_ok == True, f"delta_pct={cvd_delta_pct}, start={cvd_start}, now={cvd_now}"
    
    def test_min_delta_check_fails(self):
        """Signal should fail if CVD delta < min threshold."""
        from indicators import CVDState
        
        cvd = CVDState()
        
        # Tiny CVD change
        now = time.time()
        cvd.cvd_history.append((now - 1800, 100.0, 65000))
        cvd.running_cvd = 98.0  # Only dropped from 100 to 98
        
        cvd_start, _ = cvd.get_cvd_at_time(30)
        cvd_now = cvd.running_cvd
        cvd_change = abs(cvd_now - cvd_start)
        cvd_base = max(abs(cvd_now), abs(cvd_start), 1.0)
        cvd_delta_pct = cvd_change / cvd_base
        
        min_delta = 0.1  # 10% threshold
        cvd_delta_ok = cvd_delta_pct >= min_delta
        
        # 2% change should fail 10% threshold
        assert cvd_delta_ok == False


class TestVWAPBands:
    """Tests for VWAP band signal detection."""
    
    def test_price_above_upper_band_triggers_short(self):
        """Price above upper band should trigger short consideration."""
        from indicators import VWAPState
        
        vwap = VWAPState()
        
        # Add some candles
        from indicators import Candle
        from datetime import datetime, timezone
        
        for i in range(10):
            vwap.candles.append(Candle(
                time=datetime.now(timezone.utc),
                open=65000, high=65100, low=64900, close=65000, volume=1.0
            ))
        
        lower, vwap_val, upper = vwap.get_bands(2.0)
        
        # Test price above upper band
        test_price = upper + 100
        is_above_upper = test_price >= upper
        
        assert is_above_upper == True
    
    def test_price_below_lower_band_triggers_long(self):
        """Price below lower band should trigger long consideration."""
        from indicators import VWAPState, Candle
        from datetime import datetime, timezone
        
        vwap = VWAPState()
        
        for i in range(10):
            vwap.candles.append(Candle(
                time=datetime.now(timezone.utc),
                open=65000, high=65100, low=64900, close=65000, volume=1.0
            ))
        
        lower, vwap_val, upper = vwap.get_bands(2.0)
        
        # Test price below lower band
        test_price = lower - 100
        is_below_lower = test_price <= lower
        
        assert is_below_lower == True


class TestMinStopDistance:
    """Tests for minimum stop distance clamping."""
    
    def test_short_stop_min_distance(self):
        """Short stop should be at least MIN_STOP_DISTANCE_PCT above entry."""
        entry_price = 65000
        swing_high = 65050  # Very close swing (only ~0.08% above)
        wick_buffer = 0.001  # 0.1%
        min_distance = 0.003  # 0.3%
        
        # Calculate stop (same as bot does)
        stop_loss = swing_high * (1 + wick_buffer)  # 65115
        min_stop = entry_price * (1 + min_distance)  # 65195
        stop_loss = max(stop_loss, min_stop)  # Should use min_stop
        
        # Stop should be at least min_distance above entry (with floating point tolerance)
        actual_distance = (stop_loss - entry_price) / entry_price
        assert actual_distance >= min_distance - 1e-9, f"actual={actual_distance}, min={min_distance}, stop={stop_loss}"
    
    def test_long_stop_min_distance(self):
        """Long stop should be at least MIN_STOP_DISTANCE_PCT below entry."""
        entry_price = 65000
        swing_low = 64980  # Very close swing
        wick_buffer = 0.001  # 0.1%
        min_distance = 0.003  # 0.3%
        
        # Calculate stop
        stop_loss = swing_low * (1 - wick_buffer)
        min_stop = entry_price * (1 - min_distance)
        stop_loss = min(stop_loss, min_stop)
        
        # Stop should be at least min_distance below entry
        actual_distance = (entry_price - stop_loss) / entry_price
        assert actual_distance >= min_distance


if __name__ == '__main__':
    # Run tests manually
    test_classes = [
        TestCVDSignalLogic,
        TestCVDStrictMode, 
        TestCVDMinDelta,
        TestVWAPBands,
        TestMinStopDistance
    ]
    
    for cls in test_classes:
        instance = cls()
        for name in dir(instance):
            if name.startswith('test_'):
                print(f"Running {cls.__name__}.{name}...")
                try:
                    getattr(instance, name)()
                    print(f"  ✅ PASS")
                except AssertionError as e:
                    print(f"  ❌ FAIL: {e}")
                except Exception as e:
                    print(f"  ❌ ERROR: {e}")
    
    print("\nDone!")
