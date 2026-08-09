# Kalshi Perps VWAP Reversal Bot v2.7.1

Mean-reversion scalping on Kalshi perpetual futures using VWAP bands, order flow confirmation, and multi-layer safety gates.

## Strategy Overview

```
         +3σ ═══════════════════════  EXTREME OVERBOUGHT (short zone)
         +2σ ───────────────────────  Entry zone for shorts
              
         VWAP ════════════════════════  Fair value (TARGET)
              
         -2σ ───────────────────────  Entry zone for longs
         -3σ ═══════════════════════  EXTREME OVERSOLD (long zone)
```

**v2.7.1 Fixes:**
- Fixed BotState field names (sed rename accident)
- Fixed state recovery JSON keys
- Re-exported API_KEY/KEY_PATH for Kalshi WS auth
- Wired cvd_min_delta_pct into signal logic
- Consistent swing-based stops in manage_positions

**v2.7 Changes:**
- **BotState dataclass** — All mutable state encapsulated in `BotState` class for testability
- **Position watchdog** — `position_watchdog.py` monitors positions independently for emergency exits
- **CVD strictness config** — New `cvd_strict_divergence` and `cvd_min_delta_pct` config options
- **Coinbase-only price history** — Swing detection uses only Coinbase data (no venue mixing)
- **Graceful shutdown** — Signal handlers save state and cancel tasks cleanly
- **Removed dead code** — Unused `detect_price_extreme` function removed

**v2.6 Changes:**
- **Non-blocking async I/O** — All Kalshi API calls now run in thread pool, event loop no longer blocked
- **--config flag works** — Can now specify custom config path via CLI
- **PnL accumulation** — `total_pnl` properly updated after each exit
- **Smarter recovery stops** — Orphaned positions use VWAP + swing-based stops instead of flat 2%

**v2.5 Changes:**
- Modular split (`indicators.py`, `kalshi_client.py`, `kalshi_async.py`)
- Unit tests for indicators, config, and gates
- Async client with rate limiting (under-used until v2.6)

**v2.0-2.3 Changes:**
- Single VWAP target (no TP1/TP2 split) — reduces fee friction
- 6 safety gates before entry — prevents bad trades
- `post_only` orders — guaranteed maker fees (0.01%)
- ADX trend filter with hysteresis — no fading strong trends
- Spread corridor killswitch — halts on cross-venue divergence
- Dry-run by default — requires `--execute` for live trading

## Entry Rules

### Signal Detection
1. **Extension**: Price pierces ±2σ band
2. **CVD Divergence**: Price vs cumulative volume delta diverging
3. **Trigger**: All safety gates pass

### Safety Gates (All Must Pass)

| Gate | Check | Threshold |
|------|-------|-----------|
| 0. Data Freshness | WebSocket lag | < 1.0s both feeds |
| 1. Fee Hurdle | Profit distance > fees + margin | > 0.145% (0.045% fees + 0.1% profit) |
| 2. Spread Corridor | \|Kalshi - Coinbase\| | < 0.15% |
| 3. ADX Filter | Trend strength with hysteresis | Block at ≥25, unlock at <22 |
| 4. OBI Confirmation | Order book imbalance | ±0.20 supporting direction |
| 5. Circuit Breaker | Consecutive stop-losses | < 3 losses in a row |

### Exit
- **Target**: VWAP (single exit, no partial scaling)
- **Stop**: 0.1% past extreme wick

## Key Indicators

### VWAP (Volume-Weighted Average Price)
```
VWAP = Σ(Price × Volume) / Σ(Volume)
```
Resets at midnight UTC. Acts as "fair value" magnet.

### CVD (Cumulative Volume Delta)
```
CVD = Σ(Buy Volume) - Σ(Sell Volume)
```
Tracks aggressive buying vs selling. Divergence = exhaustion signal.

### ADX (Average Directional Index)
```
ADX = Smoothed(|+DI - -DI| / |+DI + -DI|) × 100
```
- ADX > 25 = Strong trend (DON'T fade bands)
- ADX < 20 = Ranging market (safe for mean reversion)

### OBI (Order Book Imbalance)
```
OBI = (Bid Volume - Ask Volume) / Total Volume
```
- OBI > +0.20 = Buyers resting (supports longs)
- OBI < -0.20 = Sellers resting (supports shorts)

## Fee Structure

| Type | Rate | When |
|------|------|------|
| Maker | 0.01% | Entry (post_only) |
| Taker | 0.035% | Exit (IOC) |
| **Round-trip** | **0.045%** | Total cost |

Break-even distances:
- BTC: ~$29 profit needed
- ETH: ~$0.86 profit needed

## Risk Management

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Max margin/trade | 30% | Conservative exposure |
| Max risk/trade | 10% | Loss at stop |
| Effective leverage | 5x | Keep liquidation far |
| Min stop distance | 0.3% | Prevent oversizing |
| Margin mode | ISOLATED | Protect portfolio |

## Data Feeds

| Source | Data | Purpose |
|--------|------|---------|
| Kalshi WebSocket | Perp prices, trades | Execution, OBI |
| Coinbase WebSocket | Spot prices, trades | VWAP, CVD, ADX, spread corridor |

## Files

```
kalshi-vwap-reversal/
├── scripts/
│   ├── vwap_reversal_bot.py   # Main trading bot
│   └── state_manager.py       # State persistence
├── logs/
│   └── trades.jsonl           # Trade log
├── state/
│   └── bot_state.json         # Persisted state
├── config.json                # Configuration
└── README.md
```

## Setup

### Environment Variables

```bash
export KALSHI_API_KEY_ID="your-api-key-id"
export KALSHI_KEY_PATH="/path/to/kalshi_private.pem"
```

### Get API Credentials

1. Go to [Kalshi API Settings](https://kalshi.com/account/api)
2. Generate new API key (download the private key `.pem` file)
3. Store the key file securely and set environment variables

### Install Dependencies

```bash
pip install websockets aiohttp cryptography python-dotenv
```

## Usage

```bash
cd kalshi-vwap-reversal

# Dry run (default) - signals logged, no orders placed
python scripts/vwap_reversal_bot.py

# Live trading
python scripts/vwap_reversal_bot.py --execute

# Verbose logging
python scripts/vwap_reversal_bot.py --execute --verbose

# Background
nohup python scripts/vwap_reversal_bot.py --execute > bot.log 2>&1 &

# Monitor
tail -f bot.log

# Help
python scripts/vwap_reversal_bot.py --help
```

## Status Output

```
STATUS | Balance: $700.46 | Positions: 0 | Pending: 0
  BTC: $64,782 | ETH: $1,914
  BTC VWAP: $64,788 | ±2σ: $64,651-$64,925 | Dev: 0.2σ below | ADX: 18.3 | Spread: 0.012%
  ETH VWAP: $1,914 | ±2σ: $1,911-$1,917 | Dev: 0.4σ above | ADX: 22.1 | Spread: 0.008%
  BTC CVD: +0.0 (flat)
  ETH CVD: +0.0 (flat)
```

## When Trading is Blocked

The bot will log blocked signals with reasons:

```
❌ BLOCKED: ADX trending: 28.3 > 25 threshold
❌ BLOCKED: Spread corridor: 0.182% > 0.15% max
❌ BLOCKED: OBI unsupportive for long: -0.15 < +0.20
❌ BLOCKED: Fee hurdle: profit $18.50 < min $29.15
```

## Telegram Notifications

Set in `.env`:
```bash
TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
TELEGRAM_CHAT_ID=6854193499
```

Notifications sent for:
- 🟢/🔴 Trade entries (filled orders)
- ✅/❌ Trade exits (P&L)
- 🚨 Circuit breaker trips

## systemd Service

Install service for auto-restart on reboot:
```bash
./setup_systemd.sh

# Control
sudo systemctl start vwap-bot
sudo systemctl stop vwap-bot
sudo systemctl status vwap-bot
```

Service respects circuit breaker — won't start if tripped.

## Position Watchdog

Independent safety monitor that provides second-layer exit protection:

```bash
# Run watchdog continuously
python scripts/position_watchdog.py

# Single check (for cron)
python scripts/position_watchdog.py --once

# Dry run (check without placing orders)
python scripts/position_watchdog.py --dry-run
```

**What it monitors:**
- Positions exceeding 5% loss → emergency exit
- Main bot heartbeat → exits if bot appears dead
- Stop loss levels from bot state → enforces exits if bot missed them

**Install as service:**
```bash
sudo cp vwap-watchdog.service /etc/systemd/system/
sudo systemctl enable vwap-watchdog
sudo systemctl start vwap-watchdog
```

Run both `vwap-bot` and `vwap-watchdog` for redundant exit protection.

## License

MIT
