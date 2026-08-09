#!/usr/bin/env python3
"""
Unit tests for safety gates.

Run with: python -m pytest tests/ -v
"""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))


class TestDataFreshnessGate:
    """Tests for data freshness gate."""
    
    def test_fresh_data_passes(self):
        """Fresh data should pass the gate."""
        # Import after adding path
        import vwap_reversal_bot as bot
        
        # Set recent timestamps
        bot.ws_last_message['coinbase'] = time.time()
        bot.ws_last_message['kalshi'] = time.time()
        
        is_fresh, msg = bot.check_data_freshness()
        assert is_fresh
    
    def test_stale_coinbase_fails(self):
        """Stale Coinbase data should fail."""
        import vwap_reversal_bot as bot
        
        # Set old Coinbase timestamp
        bot.ws_last_message['coinbase'] = time.time() - 5.0  # 5 seconds old
        bot.ws_last_message['kalshi'] = time.time()
        
        is_fresh, msg = bot.check_data_freshness()
        assert not is_fresh
        assert 'Coinbase' in msg
    
    def test_stale_kalshi_fails(self):
        """Stale Kalshi data should fail."""
        import vwap_reversal_bot as bot
        
        bot.ws_last_message['coinbase'] = time.time()
        bot.ws_last_message['kalshi'] = time.time() - 5.0  # 5 seconds old
        
        is_fresh, msg = bot.check_data_freshness()
        assert not is_fresh
        assert 'Kalshi' in msg


class TestSpreadCorridorGate:
    """Tests for spread corridor gate."""
    
    def test_aligned_prices_pass(self):
        """Similar prices should pass."""
        import vwap_reversal_bot as bot
        
        bot.kalshi_prices['BTC'] = 65000.0
        bot.coinbase_prices['BTC'] = 65000.0
        
        is_safe, divergence = bot.check_spread_corridor('BTC')
        assert is_safe
        assert divergence == 0.0
    
    def test_small_divergence_passes(self):
        """Small divergence should pass."""
        import vwap_reversal_bot as bot
        
        bot.kalshi_prices['BTC'] = 65000.0
        bot.coinbase_prices['BTC'] = 65050.0  # 0.077% difference
        
        is_safe, divergence = bot.check_spread_corridor('BTC')
        assert is_safe
        assert divergence < 0.001
    
    def test_large_divergence_fails(self):
        """Large divergence should fail."""
        import vwap_reversal_bot as bot
        
        bot.kalshi_prices['BTC'] = 65000.0
        bot.coinbase_prices['BTC'] = 65200.0  # 0.31% difference
        
        is_safe, divergence = bot.check_spread_corridor('BTC')
        assert not is_safe
        assert divergence > 0.002


class TestADXHysteresisGate:
    """Tests for ADX hysteresis gate."""
    
    def test_low_adx_passes(self):
        """Low ADX should pass."""
        import vwap_reversal_bot as bot
        from indicators import ADXState
        
        # Create ADX state with low value
        adx = ADXState(period=5)
        adx.adx = 15.0
        adx.dx_history = list(range(10))  # Make valid
        
        bot.adx_states['BTC'] = adx
        bot.adx_trend_blocked['BTC'] = False
        
        is_ok, msg = bot.check_adx_hysteresis('BTC')
        assert is_ok
    
    def test_high_adx_blocks(self):
        """High ADX should block."""
        import vwap_reversal_bot as bot
        from indicators import ADXState
        
        adx = ADXState(period=5)
        adx.adx = 30.0
        adx.dx_history = list(range(10))
        
        bot.adx_states['BTC'] = adx
        bot.adx_trend_blocked['BTC'] = False
        
        is_ok, msg = bot.check_adx_hysteresis('BTC')
        assert not is_ok
        assert bot.adx_trend_blocked['BTC'] == True
    
    def test_hysteresis_stays_blocked(self):
        """Should stay blocked until ADX < 22."""
        import vwap_reversal_bot as bot
        from indicators import ADXState
        
        adx = ADXState(period=5)
        adx.adx = 24.0  # Above 22 cooldown, below 25 threshold
        adx.dx_history = list(range(10))
        
        bot.adx_states['BTC'] = adx
        bot.adx_trend_blocked['BTC'] = True  # Already blocked
        
        is_ok, msg = bot.check_adx_hysteresis('BTC')
        assert not is_ok  # Should stay blocked
    
    def test_hysteresis_unblocks(self):
        """Should unblock when ADX < 22."""
        import vwap_reversal_bot as bot
        from indicators import ADXState
        
        adx = ADXState(period=5)
        adx.adx = 20.0  # Below 22 cooldown
        adx.dx_history = list(range(10))
        
        bot.adx_states['BTC'] = adx
        bot.adx_trend_blocked['BTC'] = True  # Was blocked
        
        is_ok, msg = bot.check_adx_hysteresis('BTC')
        assert is_ok  # Should unblock
        assert bot.adx_trend_blocked['BTC'] == False


class TestCircuitBreakerGate:
    """Tests for circuit breaker gate."""
    
    def test_no_losses_passes(self):
        """No losses should pass."""
        import vwap_reversal_bot as bot
        
        bot.consecutive_losses = 0
        bot.circuit_breaker_tripped = False
        
        is_ok, msg = bot.check_circuit_breaker()
        assert is_ok
    
    def test_few_losses_passes(self):
        """Less than 3 losses should pass."""
        import vwap_reversal_bot as bot
        
        bot.consecutive_losses = 2
        bot.circuit_breaker_tripped = False
        
        is_ok, msg = bot.check_circuit_breaker()
        assert is_ok
    
    def test_three_losses_trips(self):
        """3 consecutive losses should trip."""
        import vwap_reversal_bot as bot
        
        bot.consecutive_losses = 3
        bot.circuit_breaker_tripped = False
        
        # Mock save_circuit_breaker to avoid file I/O
        with patch.object(bot, 'notify_circuit_breaker', MagicMock()):
            with patch('state_manager.save_circuit_breaker', MagicMock()):
                is_ok, msg = bot.check_circuit_breaker()
        
        assert not is_ok
        assert bot.circuit_breaker_tripped
    
    def test_tripped_stays_tripped(self):
        """Tripped breaker should stay tripped."""
        import vwap_reversal_bot as bot
        
        bot.consecutive_losses = 0
        bot.circuit_breaker_tripped = True
        
        is_ok, msg = bot.check_circuit_breaker()
        assert not is_ok


if __name__ == '__main__':
    import pytest
    pytest.main([__file__, '-v'])
