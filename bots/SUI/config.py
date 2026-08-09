#!/usr/bin/env python3
"""
Configuration loader for VWAP Reversal Bot.
Loads from config.json with defaults for missing values.
"""

import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional

DEFAULT_CONFIG_FILE = Path(__file__).parent.parent / 'config.json'

# Module-level override (set via load_config(path=...))
_config_path_override: Path = None


@dataclass
class AssetConfig:
    kalshi_ticker: str
    coinbase_symbol: str
    enabled: bool = True
    size_multiplier: float = 1.0
    contract_size: float = 0.0001  # 1 contract = this many units (BTC: 0.0001, ETH: 0.001)
    adx_threshold: float = 25  # Per-asset ADX threshold (default 25)


@dataclass
class Config:
    """Bot configuration with defaults."""
    # Assets
    assets: Dict[str, AssetConfig] = field(default_factory=dict)
    
    # VWAP
    vwap_reset_hour_utc: int = 0
    entry_band_sd: float = 2.0
    
    # Entry/Exit
    min_deviation_sd: float = 2.0
    require_cvd_divergence: bool = True
    stop_beyond_wick_pct: float = 0.001
    
    # Safety Gates
    data_freshness_max_lag_seconds: float = 1.0
    fee_hurdle_min_profit_pct: float = 0.001
    spread_corridor_max_pct: float = 0.0015
    adx_period: int = 14
    adx_trend_threshold: float = 25.0
    adx_cooldown_threshold: float = 22.0
    obi_min_threshold: float = 0.20
    obi_depth_levels: int = 5
    circuit_breaker_consecutive_losses: int = 3
    
    # Fees
    maker_fee_rate: float = 0.0001
    taker_fee_rate: float = 0.00035
    
    # Risk
    max_risk_per_trade_pct: float = 0.10
    max_margin_pct: float = 0.30
    max_leverage: float = 5.0
    min_stop_distance_pct: float = 0.003
    
    # Rate Limits
    max_trades_per_hour: int = 10
    poll_interval_seconds: float = 0.5
    
    # CVD
    cvd_divergence_window_minutes: int = 30
    cvd_reset_hours: int = 12
    cvd_strict_divergence: bool = False  # If True, require clear directional CVD (no 'flat')
    cvd_min_delta_pct: float = 0.0  # Minimum CVD change % to count as diverging (0 = any)
    
    @property
    def total_fee_rate(self) -> float:
        """Maker entry + taker exit."""
        return self.maker_fee_rate + self.taker_fee_rate
    
    @property
    def min_candles_for_vwap(self) -> int:
        return 5


def load_config(config_path: str = None) -> Config:
    """Load configuration from config.json.
    
    Args:
        config_path: Optional path to config file. If None, uses default.
    """
    global _config_path_override
    
    if config_path:
        _config_path_override = Path(config_path)
    
    config_file = _config_path_override or DEFAULT_CONFIG_FILE
    config = Config()
    
    if not config_file.exists():
        print(f"[CONFIG] No config file at {config_file}, using defaults")
        # Set default assets with contract sizes
        config.assets = {
            'BTC': AssetConfig('KXBTCPERP', 'BTC-USD', contract_size=0.0001),
            'ETH': AssetConfig('KXETHPERP', 'ETH-USD', contract_size=0.001),
        }
        return config
    
    try:
        with open(config_file, 'r') as f:
            data = json.load(f)
        
        # Parse assets
        # Default contract sizes if not in config
        default_contract_sizes = {'BTC': 0.0001, 'ETH': 0.001}
        for symbol, asset_data in data.get('assets', {}).items():
            config.assets[symbol] = AssetConfig(
                kalshi_ticker=asset_data.get('kalshi_ticker', f'KX{symbol}PERP'),
                coinbase_symbol=asset_data.get('coinbase_symbol', f'{symbol}-USD'),
                enabled=asset_data.get('enabled', True),
                size_multiplier=asset_data.get('size_multiplier', 1.0),
                contract_size=asset_data.get('contract_size', default_contract_sizes.get(symbol, 0.0001)),
                adx_threshold=asset_data.get('adx_threshold', 25)
            )
        
        # VWAP
        vwap = data.get('vwap', {})
        config.vwap_reset_hour_utc = vwap.get('reset_hour_utc', 0)
        config.entry_band_sd = vwap.get('entry_band', 2.0)
        
        # Entry
        entry = data.get('entry', {})
        config.min_deviation_sd = entry.get('min_deviation_sd', 2.0)
        config.require_cvd_divergence = entry.get('require_cvd_divergence', True)
        
        # Exit
        exit_cfg = data.get('exit', {})
        config.stop_beyond_wick_pct = exit_cfg.get('stop_beyond_wick_pct', 0.001)
        
        # Safety Gates
        gates = data.get('safety_gates', {})
        config.data_freshness_max_lag_seconds = gates.get('data_freshness_max_lag_seconds', 1.0)
        config.fee_hurdle_min_profit_pct = gates.get('fee_hurdle_min_profit_pct', 0.001)
        config.spread_corridor_max_pct = gates.get('spread_corridor_max_pct', 0.0015)
        config.adx_period = gates.get('adx_period', 14)
        config.adx_trend_threshold = gates.get('adx_trend_threshold', 25.0)
        config.adx_cooldown_threshold = gates.get('adx_cooldown_threshold', 22.0)
        config.obi_min_threshold = gates.get('obi_min_threshold', 0.20)
        config.obi_depth_levels = gates.get('obi_depth_levels', 5)
        config.circuit_breaker_consecutive_losses = gates.get('circuit_breaker_consecutive_losses', 3)
        
        # Fees
        fees = data.get('fees', {})
        config.maker_fee_rate = fees.get('maker_rate', 0.0001)
        config.taker_fee_rate = fees.get('taker_rate', 0.00035)
        
        # Risk
        risk = data.get('risk', {})
        config.max_risk_per_trade_pct = risk.get('max_risk_per_trade_pct', 0.10)
        config.max_margin_pct = risk.get('max_margin_pct', 0.30)
        config.max_leverage = risk.get('max_leverage', 5.0)
        config.min_stop_distance_pct = risk.get('min_stop_distance_pct', 0.003)
        
        # Rate Limits
        rate_limits = data.get('rate_limits', {})
        config.max_trades_per_hour = rate_limits.get('max_trades_per_hour', 10)
        config.poll_interval_seconds = rate_limits.get('poll_interval_seconds', 0.5)
        
        # CVD settings
        cvd = data.get('cvd', {})
        config.cvd_divergence_window_minutes = cvd.get('divergence_window_minutes', 30)
        config.cvd_reset_hours = cvd.get('reset_hours', 12)
        config.cvd_strict_divergence = cvd.get('strict_divergence', False)
        config.cvd_min_delta_pct = cvd.get('min_delta_pct', 0.0)
        
        print(f"[CONFIG] Loaded from {config_file}")
        print(f"[CONFIG] Assets: {list(config.assets.keys())}")
        print(f"[CONFIG] Entry band: ±{config.entry_band_sd}σ")
        print(f"[CONFIG] Risk: {config.max_risk_per_trade_pct*100:.0f}% max, {config.max_margin_pct*100:.0f}% margin")
        
        return config
        
    except Exception as e:
        print(f"[CONFIG] Error loading config: {e}, using defaults")
        config.assets = {
            'BTC': AssetConfig('KXBTCPERP', 'BTC-USD', contract_size=0.0001),
            'ETH': AssetConfig('KXETHPERP', 'ETH-USD', contract_size=0.001),
        }
        return config


# Global config instance
cfg = load_config()
