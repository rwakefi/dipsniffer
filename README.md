# 🐊 DipSniffer (AKA YOLOBot) — Kraken Swing Trading Bot

Auto-executing swing trader that monitors 17 coins on Kraken.
It concentrates capital into one asset at a time using three entry paths:

- RSI/Bollinger dip-buy
- Bollinger squeeze breakout
- momentum continuation

Those entries are filtered by Gemini-based risk vetos, ranked with a composite strength model, and managed with an ATR-based dynamic trailing stop plus stale-position rotation logic.

**Script:** `kraken-swing-bot.py`
**State:** `~/.config/kraken/swing-bot-state.json`
**Log:** `~/.config/kraken/swing-bot.log`

---

## The Strategy

| Parameter | Value |
|---|---|
| **BUY when (Dip)** | RSI < `RSI_OVERSOLD` AND price ≤ lower BB AND volume spike |
| **BUY when (Squeeze)** | BB squeeze breakout AND volume spike AND price has headroom below upper BB |
| **BUY when (Momentum)** | RSI in `[MOMENTUM_RSI_MIN, MOMENTUM_RSI_MAX]` AND BB position > `MOMENTUM_BB_POS_MIN` AND RSI slope > `MOMENTUM_SLOPE_MIN` AND consecutive green candles ≥ `MOMENTUM_GREEN_CANDLES` |
| **SELL when** | RSI > `RSI_OVERBOUGHT` OR (price ≥ upper BB AND `bb_width > BB_MIN_WIDTH_PCT`) OR stop-loss hit |
| **Stop-loss** | ATR dynamic trailing (`ATR_MULT_WIDE` at entry → `ATR_MULT_TIGHT` at ≥ `ATR_TIGHTEN_GAIN`, `STOP_LOSS_FALLBACK` if ATR unavailable) |
| **Vetoes** | Sell Walls (3× imbalance), BTC Crash Guard (RSI < 35 + BB < 0.15), Daily RSI Falling Knife |
| **Boosts** | RSI Bullish Divergence, Funding Rate Squeeze, Relative Strength vs BTC/ETH |
| **Candles** | 1-hour OHLC data |
| **Poll interval** | Every 60 seconds |
| **Assets** | 17 coins across 3 tiers (see below) |
| **Min trade** | $5 USD (Kraken minimum) |

### The 3 Intelligence Layers
YOLOBot uses deterministic math for its baseline targets, but gates its actions through three intelligence filters to prevent buying into crashes and to squeeze more juice out of winners.

1. **Layer 1: Deterministic Crash Guard (Math-driven)**
   - **Blocks all buys** if BTC's RSI < 35 AND its Bollinger position < 0.15.
   - Prevents YOLOBot from catching falling knives during macro/geopolitical market dumps where everything looks "oversold".
2. **Layer 2: Gemini Buy Veto (LLM-driven)**
   - When a buy signal fires, YOLOBot asks Gemini Flash to quickly read recent news about the highest-ranked candidate it just picked.
   - Outputs `SAFE` (proceed) or `RISK` (block entry).
   - Functions strictly as a fundamental safety interlock to catch hacks, regulatory crackdowns, or sudden market-wide shocks that math can't see.
3. **Layer 3: Gemini Sell Extension (LLM-driven)**
   - When a sell signal fires (e.g. RSI > 70), YOLOBot asks Gemini Flash if there's an ongoing bullish catalyst.
   - Outputs `SELL` (take profit) or `HOLD` (extend trade, trailing stop stays active).
   - **Never overrides stop-loss.** Stop-losses always fire instantly.

### The YOLO Hunt
When the market is quiet and YOLOBot is idle in cash, it goes on the offensive.
1. **Conditions:** No existing position + No math-based buy signals (including momentum) + Idle for ≥ 3 hours + Alternative.me Fear & Greed Index ≤ 50.
2. **The Hunt:** Instead of waiting for standard buy signals, YOLOBot ranks all assets by a composite strength score (RSI, Bollinger Band positioning, funding rates, block volume, and relative strength vs BTC/ETH) to identify the single strongest candidate.
3. **The Veto:** The top candidate is then structurally vetted by the Layer 2 Gemini Buy Veto (checks for news of hacks/scams/black swans) before execution.
4. **Role:** With `momentum_buy` handling trend continuation, YOLO is now focused more on narrative/macro judgment when the market goes quiet.

### Additional Features
- **Anti-Churn Cooldown**: Recently sold assets are dropped into a strict 2-hour penalty box, with a bypass if they flash-crash >2% below the exit price.
- **Stale Position Eject**: If a trade is held past configurable age/profit thresholds without making new highs, YOLOBot identifies stronger candidates and can rotate capital after a fresh veto pass.
- **SQLite Data Log Engine**: Every market cycle, snapshot, and trade decision metadata is logged to `~/.config/kraken/market_history.db` for offline backtesting and performance attribution.
- **Order Book Veto**: Scans the Kraken order book ±1% for sell walls. Buys are vetoed if sell volume > 3× buy volume in the near-term range.
- **Funding Rate Overlay**: Adds a configurable composite strength bonus for coins in active short squeezes (negative funding rates).
- **Relative Strength Overlay**: Rewards assets outperforming BTC/ETH leadership.
- **Auto Quant Optimizer**: `auto_quant.py` replays the strategy deterministically to mutate and score parameter sets against historical data.

---

## Operations

### Quick Commands

```bash
# Setup: We recommend asking your LLM Agent (like Antigravity) to read `agent_setup.md` to handle dependencies and authorization dynamically!

# Run once (check signals, no loop)
python3 kraken-swing-bot.py --status

# Start in background natively with Dashboard
./start-dipsniffer.sh

# Stop (find PID and kill)
pkill -f kraken-swing-bot
```

### tmux (Recommended)

```bash
# Start YOLOBot in a named tmux session
tmux new -s yolobot "./start-dipsniffer.sh"

# Detach (leave YOLOBot running, return to terminal): Ctrl+B, then D

# Re-attach (check on YOLOBot)
tmux attach -t yolobot

# Quick peek without attaching
tmux capture-pane -t yolobot -p | tail -10

# Kill the session entirely
tmux kill-session -t yolobot
```

### Logs

```bash
# Watch live
tail -f ~/.config/kraken/swing-bot.log

# Last 20 entries
tail -20 ~/.config/kraken/swing-bot.log

# See all trades only
grep "BOUGHT\|SOLD" ~/.config/kraken/swing-bot.log
```

## Watchlist (17 Coins)

| Tier | Assets | Why |
|---|---|---|
| **Tier 1** — Large cap | BTC, ETH | Stable, reliable signals |
| **Tier 2** — Mid cap | SOL, AVAX, LINK, DOT, ATOM, NEAR, SUI, INJ | Good volatility, decent swings |
| **Tier 3** — High vol | DOGE, FET, RENDER, PEPE, HYPE, ONDO, TAO | Frequent RSI extremes, wilder swings, momentum narratives |

*(Edit `PAIRS` in the script to add/remove coins)*

## Run Modes
```bash
--status     # Show signals only, no trades (skips Gemini logic for speed)
--dry-run    # Show what it WOULD do, no trades (runs full logic flow)
--loop       # Run continuously every 60 seconds
(no flags)   # Run once and exit
```

---
*Note: YOLOBot talks to Kraken through the Python `ccxt` client and reads `~/.config/kraken/config.toml` API credentials unless `KRAKEN_API_KEY` and `KRAKEN_API_SECRET` are set in the environment.*
