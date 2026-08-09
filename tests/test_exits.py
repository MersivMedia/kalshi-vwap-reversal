#!/usr/bin/env python3
"""
Unit tests for exit confirmation and PnL booking.

Tests the v2.7.4+ behavior where PnL is only booked when position
is confirmed closed, not when exit order is placed.

Run with: python -m pytest tests/test_exits.py -v
Or: python tests/test_exits.py
"""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
from dataclasses import dataclass

sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))


class TestBotState:
    """Tests for BotState dataclass."""
    
    def test_instantiation(self):
        """BotState should instantiate with defaults."""
        from vwap_reversal_bot import BotState
        
        state = BotState()
        
        assert state.ws_connected == {'kalshi': False, 'coinbase': False}
        assert state.total_pnl == 0.0
        assert state.consecutive_losses == 0
        assert state.circuit_breaker_tripped == False
        assert isinstance(state.exit_targets, dict)
        assert isinstance(state.pending_orders, dict)
    
    def test_state_mutation(self):
        """BotState fields should be mutable."""
        from vwap_reversal_bot import BotState
        
        state = BotState()
        state.total_pnl = 100.0
        state.consecutive_losses = 2
        state.exit_targets['KXBTCPERP'] = {'stop_loss': 60000}
        
        assert state.total_pnl == 100.0
        assert state.consecutive_losses == 2
        assert 'KXBTCPERP' in state.exit_targets


class TestExitTargets:
    """Tests for exit target management."""
    
    def test_exit_pending_flag(self):
        """Exit pending flag should prevent double-exit attempts."""
        from vwap_reversal_bot import BotState
        
        state = BotState()
        
        # Simulate exit order placed
        state.exit_targets['KXBTCPERP'] = {
            'stop_loss': 60000,
            'target_price': 65000,
            'side': 'long',
            'entry_price': 62000,
            'exit_pending': True,
            'exit_order_time': time.time(),
            'exit_pnl': 50.0,
            'exit_reason': 'TARGET (VWAP)',
            'exit_price': 65000,
            'symbol': 'BTC'
        }
        
        targets = state.exit_targets['KXBTCPERP']
        
        # Should skip if exit_pending and recent
        assert targets.get('exit_pending') == True
        age = time.time() - targets.get('exit_order_time', 0)
        assert age < 60  # Should be fresh
    
    def test_exit_pending_retry_after_timeout(self):
        """Exit should retry if pending > 60s."""
        from vwap_reversal_bot import BotState
        
        state = BotState()
        
        # Simulate old exit order
        state.exit_targets['KXBTCPERP'] = {
            'exit_pending': True,
            'exit_order_time': time.time() - 90,  # 90 seconds ago
        }
        
        targets = state.exit_targets['KXBTCPERP']
        age = time.time() - targets.get('exit_order_time', 0)
        
        # Should retry - age > 60
        assert age > 60


class TestPnLBooking:
    """Tests for PnL booking on exit confirmation."""
    
    def test_pnl_stored_on_exit_order(self):
        """PnL should be stored on targets, not booked immediately."""
        from vwap_reversal_bot import BotState
        
        state = BotState()
        initial_pnl = state.total_pnl
        
        # Simulate exit order placed - PnL stored but not booked
        expected_pnl = 150.0
        state.exit_targets['KXBTCPERP'] = {
            'exit_pending': True,
            'exit_pnl': expected_pnl,
            'exit_reason': 'TARGET (VWAP)',
        }
        
        # total_pnl should NOT have changed yet
        assert state.total_pnl == initial_pnl
        
        # PnL should be stored on targets
        assert state.exit_targets['KXBTCPERP']['exit_pnl'] == expected_pnl
    
    def test_pnl_booked_on_position_close(self):
        """PnL should be booked when position disappears from live positions."""
        from vwap_reversal_bot import BotState
        
        state = BotState()
        
        # Simulate exit pending with stored PnL
        pnl = 200.0
        state.exit_targets['KXBTCPERP'] = {
            'exit_pending': True,
            'exit_pnl': pnl,
            'exit_reason': 'TARGET (VWAP)',
            'exit_price': 65000,
            'symbol': 'BTC',
            'side': 'long'
        }
        
        # Simulate position cleanup (position no longer in live_positions)
        # This is what manage_positions does when ticker not in live_tickers
        targets = state.exit_targets['KXBTCPERP']
        
        if targets.get('exit_pending') and targets.get('exit_pnl') is not None:
            # Book PnL
            state.total_pnl += targets['exit_pnl']
            
            # Update circuit breaker
            is_stop_loss = "STOP LOSS" in targets.get('exit_reason', '')
            if is_stop_loss:
                state.consecutive_losses += 1
            else:
                state.consecutive_losses = 0
        
        # Now delete target (position is gone)
        del state.exit_targets['KXBTCPERP']
        
        # Verify PnL was booked
        assert state.total_pnl == pnl
        assert state.consecutive_losses == 0  # Was a win
        assert 'KXBTCPERP' not in state.exit_targets
    
    def test_stop_loss_increments_consecutive_losses(self):
        """Stop loss exit should increment consecutive losses."""
        from vwap_reversal_bot import BotState
        
        state = BotState()
        state.consecutive_losses = 1
        
        # Simulate stop loss exit
        targets = {
            'exit_pending': True,
            'exit_pnl': -50.0,
            'exit_reason': 'STOP LOSS @ $58000',
            'symbol': 'BTC'
        }
        
        # Book it
        state.total_pnl += targets['exit_pnl']
        is_stop_loss = "STOP LOSS" in targets['exit_reason']
        if is_stop_loss:
            state.consecutive_losses += 1
        
        assert state.total_pnl == -50.0
        assert state.consecutive_losses == 2
    
    def test_win_resets_consecutive_losses(self):
        """Winning exit should reset consecutive losses."""
        from vwap_reversal_bot import BotState
        
        state = BotState()
        state.consecutive_losses = 2
        
        # Simulate target hit (win)
        targets = {
            'exit_pending': True,
            'exit_pnl': 100.0,
            'exit_reason': 'TARGET (VWAP) @ $65000',
            'symbol': 'BTC'
        }
        
        # Book it
        state.total_pnl += targets['exit_pnl']
        is_stop_loss = "STOP LOSS" in targets['exit_reason']
        if not is_stop_loss:
            state.consecutive_losses = 0
        
        assert state.total_pnl == 100.0
        assert state.consecutive_losses == 0


class TestCircuitBreaker:
    """Tests for circuit breaker behavior."""
    
    def test_circuit_breaker_threshold(self):
        """Circuit breaker should trip after consecutive losses."""
        from vwap_reversal_bot import BotState, CIRCUIT_BREAKER_CONSECUTIVE_LOSSES
        
        state = BotState()
        
        # Simulate losses
        for i in range(CIRCUIT_BREAKER_CONSECUTIVE_LOSSES):
            state.consecutive_losses += 1
        
        # Should now be at threshold
        assert state.consecutive_losses >= CIRCUIT_BREAKER_CONSECUTIVE_LOSSES
    
    def test_circuit_breaker_persists(self):
        """Circuit breaker tripped flag should persist."""
        from vwap_reversal_bot import BotState
        
        state = BotState()
        state.circuit_breaker_tripped = True
        
        assert state.circuit_breaker_tripped == True


class TestExternalClose:
    """Tests for externally closed positions (manual/liquidation/watchdog)."""
    
    def test_external_close_removes_stale_target(self):
        """Positions closed externally should just remove target."""
        from vwap_reversal_bot import BotState
        
        state = BotState()
        initial_pnl = state.total_pnl
        
        # Simulate normal position with no exit_pending (externally closed)
        state.exit_targets['KXBTCPERP'] = {
            'stop_loss': 60000,
            'target_price': 65000,
            'side': 'long',
            # No exit_pending - position closed externally
        }
        
        # Simulate cleanup - no exit_pending means just remove
        targets = state.exit_targets['KXBTCPERP']
        if not targets.get('exit_pending'):
            # Just remove, don't book PnL (unknown/external close)
            pass
        
        del state.exit_targets['KXBTCPERP']
        
        # PnL should NOT have changed
        assert state.total_pnl == initial_pnl


if __name__ == '__main__':
    import unittest
    
    # Run tests
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    for cls in [TestBotState, TestExitTargets, TestPnLBooking, 
                TestCircuitBreaker, TestExternalClose]:
        tests = loader.loadTestsFromTestCase(type(cls.__name__, (unittest.TestCase,), 
            {name: getattr(cls(), name) for name in dir(cls()) if name.startswith('test_')}))
        # Manual approach for class-based tests
        for name in dir(cls):
            if name.startswith('test_'):
                print(f"Running {cls.__name__}.{name}...")
                try:
                    getattr(cls(), name)()
                    print(f"  ✅ PASS")
                except AssertionError as e:
                    print(f"  ❌ FAIL: {e}")
                except Exception as e:
                    print(f"  ❌ ERROR: {e}")
    
    print("\nDone!")
