#!/usr/bin/env python3
"""
Unit tests for configuration loading.

Run with: python -m pytest tests/ -v
"""

import sys
import json
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))

from config import Config, load_config, AssetConfig


class TestConfig:
    """Tests for Config dataclass."""
    
    def test_default_config(self):
        """Test default configuration values."""
        cfg = Config()
        
        # Check defaults
        assert cfg.entry_band_sd == 2.0
        assert cfg.adx_trend_threshold == 25.0
        assert cfg.adx_cooldown_threshold == 22.0
        assert cfg.circuit_breaker_consecutive_losses == 3
        assert cfg.max_risk_per_trade_pct == 0.10
        assert cfg.max_margin_pct == 0.30
    
    def test_total_fee_rate(self):
        """Test fee rate calculation."""
        cfg = Config()
        cfg.maker_fee_rate = 0.0001
        cfg.taker_fee_rate = 0.00035
        
        assert cfg.total_fee_rate == 0.00045
    
    def test_min_candles(self):
        """Test minimum candles property."""
        cfg = Config()
        assert cfg.min_candles_for_vwap == 5


class TestAssetConfig:
    """Tests for AssetConfig dataclass."""
    
    def test_asset_defaults(self):
        """Test default asset configuration."""
        asset = AssetConfig('KXBTCPERP', 'BTC-USD')
        
        assert asset.kalshi_ticker == 'KXBTCPERP'
        assert asset.coinbase_symbol == 'BTC-USD'
        assert asset.enabled == True
        assert asset.size_multiplier == 1.0
    
    def test_disabled_asset(self):
        """Test disabled asset configuration."""
        asset = AssetConfig('KXSOLPERP', 'SOL-USD', enabled=False)
        
        assert not asset.enabled


class TestLoadConfig:
    """Tests for config file loading."""
    
    def test_load_missing_file(self):
        """Loading missing config should use defaults."""
        # Temporarily rename config file if it exists
        import config as config_module
        original_file = config_module.CONFIG_FILE
        config_module.CONFIG_FILE = Path('/nonexistent/config.json')
        
        try:
            cfg = load_config()
            assert len(cfg.assets) == 2  # Default BTC and ETH
            assert 'BTC' in cfg.assets
            assert 'ETH' in cfg.assets
        finally:
            config_module.CONFIG_FILE = original_file
    
    def test_load_valid_config(self):
        """Loading valid config file."""
        config_data = {
            "assets": {
                "BTC": {
                    "kalshi_ticker": "KXBTCPERP",
                    "coinbase_symbol": "BTC-USD",
                    "enabled": True
                }
            },
            "vwap": {
                "entry_band": 2.5
            },
            "safety_gates": {
                "adx_trend_threshold": 30
            },
            "risk": {
                "max_risk_per_trade_pct": 0.05
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_path = Path(f.name)
        
        try:
            import config as config_module
            original_file = config_module.CONFIG_FILE
            config_module.CONFIG_FILE = temp_path
            
            cfg = load_config()
            
            assert cfg.entry_band_sd == 2.5
            assert cfg.adx_trend_threshold == 30
            assert cfg.max_risk_per_trade_pct == 0.05
            assert 'BTC' in cfg.assets
        finally:
            config_module.CONFIG_FILE = original_file
            temp_path.unlink()


if __name__ == '__main__':
    import pytest
    pytest.main([__file__, '-v'])
