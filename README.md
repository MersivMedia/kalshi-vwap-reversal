# Kalshi Perps VWAP Reversal Bot

Mean-reversion scalping on Kalshi perpetual futures using VWAP bands and order flow confirmation.

## Strategy Overview

```
         +3σ ═══════════════════════  EXTREME OVERBOUGHT (short zone)
         +2σ ───────────────────────  Entry zone for shorts
         +1σ - - - - - - - - - - - -  TP1 for shorts
              
         VWAP ════════════════════════  Fair value (TP2)
              
         -1σ - - - - - - - - - - - -  TP1 for longs  
         -2σ ───────────────────────  Entry zone for longs
         -3σ ═══════════════════════  EXTREME OVERSOLD (long zone)
```

## Entry Rules

### Long Setup (Fading Lower Bands)
1. **Extension**: Price pierces below -2σ band
2. **Order Flow Exhaustion**: Red delta bars leading into extreme, then absorption
3. **CVD Divergence**: Price makes lower low, CVD makes higher low
4. **Trigger**: Enter long when price closes back inside -2σ band
5. **Stop**: 0.1% below the extreme wick
6. **TP1**: -1σ band (take 50%)
7. **TP2**: VWAP (close remaining)

### Short Setup (Fading Upper Bands)
1. **Extension**: Price pierces above +2σ band  
2. **Order Flow Exhaustion**: Green delta bars into extreme, then rejection
3. **CVD Divergence**: Price makes higher high, CVD makes lower high
4. **Trigger**: Enter short when price breaks back through +2σ
5. **Stop**: 0.1% above the extreme wick
6. **TP1**: +1σ band (take 50%)
7. **TP2**: VWAP (close remaining)

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

### Standard Deviation Bands
```
Upper Band = VWAP + (n × σ)
Lower Band = VWAP - (n × σ)
```
Dynamic support/resistance based on volume distribution.

## Risk Management

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Max risk/trade | 1% | Fixed fractional method |
| Position sizing | Risk / Stop Distance | Never arbitrary leverage |
| Margin mode | ISOLATED | Protect portfolio from anomalies |
| Max leverage | 10x | Keep liquidation far from stop |
| Max trades/hour | 10 | Prevent overtrading |

### Position Size Formula
```
Size = (Account × Risk%) / |Entry - Stop|

Example:
- Account: $5,000
- Risk: 1% = $50
- Entry: $150
- Stop: $142.50
- Distance: $7.50
- Size: $50 / $7.50 = 6.67 contracts
```

## Data Feeds

| Source | Data | Purpose |
|--------|------|---------|
| Kalshi WebSocket | Perp prices, trades | Primary execution |
| Coinbase WebSocket | Spot prices, trades | Cross-market confirmation |

## Files

```
kalshi-vwap-reversal/
├── scripts/
│   └── vwap_reversal_bot.py   # Main trading bot
├── logs/
│   └── trades.jsonl           # Trade log
├── config.json                 # Configuration
└── README.md                   # This file
```

## Setup

### Environment Variables

```bash
export KALSHI_API_KEY_ID="your-api-key-id"
export KALSHI_KEY_PATH="/path/to/kalshi_private.pem"  # Optional, defaults to keys/kalshi_private.pem
```

### Get API Credentials

1. Go to [Kalshi API Settings](https://kalshi.com/account/api)
2. Generate new API key (download the private key `.pem` file)
3. Store the key file securely and set environment variables

### Install Dependencies

```bash
pip install websockets aiohttp cryptography
```

## Usage

```bash
cd kalshi-vwap-reversal

# Dry run (paper trade)
python scripts/vwap_reversal_bot.py --dry-run

# Live trading
python scripts/vwap_reversal_bot.py --execute

# Background
nohup python scripts/vwap_reversal_bot.py --execute > bot.log 2>&1 &
```

## When NOT to Trade

- **Strong trend days**: VWAP slope > 0.1% per hour
- **Low volume**: Below average session volume
- **News events**: Avoid first 15 min after major announcements
- **Weekend sessions**: Lower liquidity, wider spreads

## Performance Tracking

After 2-3 weeks with consistent sizing:
```
Win Rate: ____%
Avg Win: $____
Avg Loss: $____
R:R Ratio: ____
Expectancy: $____ per trade
```

Only scale up after proving positive expectancy.
