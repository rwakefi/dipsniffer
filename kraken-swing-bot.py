#!/usr/bin/env python3
"""
Kraken Swing Trading Bot — DipSniffer 🐊
Monitors 17 coins on Kraken and concentrates capital
into the asset showing the strongest swing entry signal.

Strategy:
  - ENTRY: RSI(14) < 30 AND price <= lower Bollinger Band(20, 2σ)
  - EXIT:  RSI(14) > 70 OR price >= upper Bollinger Band OR stop-loss hit
  - ATR-based dynamic trailing stop (3× ATR at entry → 1.5× ATR at ≥10% gain)
  - Concentrates full balance into one asset at a time

Gemini Layers:
  - Layer 1: BTC crash guard (deterministic — blocks all buys if BTC RSI < 35 + BB < 0.15)
  - Layer 2: Buy sentiment filter (Gemini Flash — SAFE/RISK)
  - Layer 3: Sell extension (Gemini Flash — SELL/HOLD, never overrides stop-loss)
  - YOLO Hunt: Fear & Greed triggered Gemini pick when idle in cash

Usage:
  python3 kraken-swing-bot.py              # Run once (cron mode)
  python3 kraken-swing-bot.py --loop       # Run continuously (every 5 min)
  python3 kraken-swing-bot.py --dry-run    # Show signals without trading
  python3 kraken-swing-bot.py --status     # Show current position & signals
"""

import atexit
import json
import subprocess
import sys
import time
import os
import math
import site
import sqlite3
import threading
import secrets
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import URLError
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import fcntl


def load_optional_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        for raw_line in path.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key or key in os.environ:
                continue
            value = value.strip().strip('"').strip("'")
            os.environ[key] = value
    except Exception:
        pass


load_optional_env_file(Path.home() / ".config/kraken/dipsniffer.env")

# Ensure user site-packages is available (for nohup/safe_exec environments)
_user_site = site.getusersitepackages()
if _user_site not in sys.path:
    sys.path.insert(0, _user_site)
import ccxt

# ─── Configuration ───────────────────────────────────────────────
STATE_FILE = os.path.expanduser("~/.config/kraken/swing-bot-state.json")
CONTROL_FILE = os.path.expanduser("~/.config/kraken/swing-bot-control.json")
CONTROL_TOKEN_FILE = os.path.expanduser("~/.config/kraken/swing-bot-control.token")
LOG_FILE = os.path.expanduser("~/.config/kraken/swing-bot.log")
DASHBOARD_DIR = os.path.expanduser("~/.config/kraken/dashboard")
STATUS_FILE = os.path.join(DASHBOARD_DIR, "status.json")
LOCK_FILE = os.path.expanduser("~/.config/kraken/swing-bot.lock")
CONTROL_API_HOST = os.getenv("DIPSNIFFER_CONTROL_HOST", "0.0.0.0")
CONTROL_API_PORT = int(os.getenv("DIPSNIFFER_CONTROL_PORT", "8078"))
NTFY_BASE_URL = os.getenv("DIPSNIFFER_NTFY_BASE_URL", "https://ntfy.sh").rstrip("/")
NTFY_TOPIC = os.getenv("DIPSNIFFER_NTFY_TOPIC", "").strip()
NTFY_URL = os.getenv("DIPSNIFFER_NTFY_URL", "").strip()
NTFY_CLICK_URL = os.getenv("DIPSNIFFER_NTFY_CLICK_URL", "").strip()

_bot_lock_handle = None
_control_server = None
_control_server_thread = None
_control_wake_event = threading.Event()

class SQLiteLogger:
    def __init__(self, db_path="~/.config/kraken/market_history.db"):
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path, timeout=5.0)

    def _init_db(self):
        try:
            with self._get_conn() as conn:
                try:
                    conn.execute("PRAGMA journal_mode=WAL;")
                except sqlite3.Error:
                    pass

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cycles (
                        cycle_id TEXT PRIMARY KEY,
                        timestamp TEXT NOT NULL,
                        mode TEXT NOT NULL,
                        fear_greed_index INTEGER NULL,
                        btc_crash_guard_active INTEGER NOT NULL DEFAULT 0
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS snapshots (
                        cycle_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        price REAL,
                        rsi REAL,
                        bb_lower REAL,
                        bb_middle REAL,
                        bb_upper REAL,
                        bb_width REAL,
                        bb_position REAL,
                        change_24h REAL NULL,
                        change_72h REAL NULL,
                        vol_ratio REAL,
                        vol_spike INTEGER,
                        buy_signal INTEGER,
                        sell_signal INTEGER,
                        squeeze_buy INTEGER,
                        bb_squeezing INTEGER,
                        bb_squeeze_breakout INTEGER,
                        rsi_divergence INTEGER,
                        band_walking INTEGER,
                        band_walk_count INTEGER,
                        strength REAL,
                        funding_rate REAL NULL,
                        funding_squeezed INTEGER,
                        rel_strength_24h REAL NULL,
                        rel_strength_72h REAL NULL,
                        rel_strength_score REAL,
                        sell_buy_ratio REAL NULL,
                        gemini_verdict TEXT NULL,
                        PRIMARY KEY (cycle_id, symbol)
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS trade_attribution (
                        position_id TEXT PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        entry_time TEXT NOT NULL,
                        exit_time TEXT NULL,
                        entry_price REAL,
                        exit_price REAL NULL,
                        quantity REAL,
                        signal_family TEXT,
                        entry_reason TEXT,
                        exit_reason TEXT NULL,
                        decision_context TEXT,
                        strength_score REAL NULL,
                        pnl REAL NULL,
                        pnl_pct REAL NULL,
                        is_closed INTEGER NOT NULL DEFAULT 0,
                        price_6h REAL NULL,
                        price_24h REAL NULL,
                        price_72h REAL NULL,
                        measured_at_6h TEXT NULL,
                        measured_at_24h TEXT NULL,
                        measured_at_72h TEXT NULL
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS veto_events (
                        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        cycle_id TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        candidate_symbol TEXT NOT NULL,
                        decision_context TEXT NOT NULL,
                        decision_stage TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        veto_reason TEXT NULL,
                        strength_score REAL NULL,
                        rsi REAL NULL,
                        bb_position REAL NULL,
                        metadata_json TEXT NULL
                    )
                """)
        except sqlite3.Error as e:
            print(f"ERROR: SQLite INIT failed - {e}")

    def consume_cycle(self, telemetry: dict):
        if not telemetry:
            return
            
        cycle = telemetry.get("cycle")
        if not cycle or not cycle.get("cycle_id"):
            return
            
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO cycles (
                        cycle_id, timestamp, mode, fear_greed_index, btc_crash_guard_active
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    cycle["cycle_id"],
                    cycle["timestamp"],
                    cycle["mode"],
                    cycle.get("fear_greed_index"),
                    1 if cycle.get("btc_crash_guard_active") else 0
                ))
                
                analyses = telemetry.get("all_analyses", {})
                for sym, a in analyses.items():
                    conn.execute("""
                        INSERT OR IGNORE INTO snapshots (
                            cycle_id, symbol, price, rsi, bb_lower, bb_middle, bb_upper,
                            bb_width, bb_position, change_24h, change_72h, vol_ratio,
                            vol_spike, buy_signal, sell_signal, squeeze_buy, bb_squeezing,
                            bb_squeeze_breakout, rsi_divergence, band_walking, band_walk_count,
                            strength, funding_rate, funding_squeezed, rel_strength_24h,
                            rel_strength_72h, rel_strength_score, sell_buy_ratio, gemini_verdict
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        cycle["cycle_id"],
                        sym,
                        a.get("price"),
                        a.get("rsi"),
                        a.get("bb_lower"),
                        a.get("bb_middle"),
                        a.get("bb_upper"),
                        a.get("bb_width"),
                        a.get("bb_position"),
                        a.get("change_24h"),
                        a.get("change_72h"),
                        a.get("vol_ratio"),
                        1 if a.get("vol_spike") else 0,
                        1 if a.get("buy_signal") else 0,
                        1 if a.get("sell_signal") else 0,
                        1 if a.get("squeeze_buy") else 0,
                        1 if a.get("bb_squeezing") else 0,
                        1 if a.get("bb_squeeze_breakout") else 0,
                        1 if a.get("rsi_divergence") else 0,
                        1 if a.get("band_walking") else 0,
                        a.get("band_walk_count"),
                        a.get("strength"),
                        a.get("funding_rate"),
                        1 if a.get("funding_squeezed") else 0,
                        a.get("rel_strength_24h"),
                        a.get("rel_strength_72h"),
                        a.get("rel_strength_score"),
                        a.get("sell_buy_ratio"),
                        a.get("gemini_verdict")
                    ))
                
                trades = telemetry.get("executed_trades", [])
                for trade in trades:
                    if trade.get("action") == "ENTRY":
                        conn.execute("""
                            INSERT OR IGNORE INTO trade_attribution (
                                position_id, symbol, entry_time, entry_price, quantity,
                                signal_family, entry_reason, decision_context, strength_score
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            trade["position_id"],
                            trade["symbol"],
                            trade.get("time"),
                            trade.get("price"),
                            trade.get("quantity"),
                            trade.get("signal_family"),
                            trade.get("entry_reason"),
                            trade.get("decision_context"),
                            trade.get("strength_score")
                        ))
                    elif trade.get("action") == "EXIT":
                        conn.execute("""
                            UPDATE trade_attribution SET
                                exit_time = ?,
                                exit_price = ?,
                                exit_reason = ?,
                                pnl = ?,
                                pnl_pct = ?,
                                is_closed = 1
                            WHERE position_id = ?
                        """, (
                            trade.get("time"),
                            trade.get("price"),
                            trade.get("reason"),
                            trade.get("pnl"),
                            trade.get("pnl_pct"),
                            trade["position_id"]
                        ))

                decisions = telemetry.get("decision_events", [])
                for d in decisions:
                    conn.execute("""
                        INSERT INTO veto_events (
                            cycle_id, timestamp, candidate_symbol, decision_context,
                            decision_stage, event_type, veto_reason, strength_score,
                            rsi, bb_position, metadata_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        cycle["cycle_id"],
                        d.get("timestamp", cycle["timestamp"]),
                        d.get("candidate_symbol", ""),
                        d.get("decision_context", ""),
                        d.get("decision_stage", ""),
                        d.get("event_type", ""),
                        d.get("veto_reason"),
                        d.get("strength_score"),
                        d.get("rsi"),
                        d.get("bb_position"),
                        d.get("metadata_json")
                    ))

        except sqlite3.Error as e:
            print(f"ERROR: SQLite consume_cycle failed - {e}")

    def evaluate_closed_trades(self):
        try:
            with self._get_conn() as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("""
                    SELECT * FROM trade_attribution 
                    WHERE is_closed = 1 
                      AND (price_6h IS NULL OR price_24h IS NULL OR price_72h IS NULL)
                """)
                rows = cur.fetchall()
                
                for row in rows:
                    exit_time_str = row["exit_time"]
                    if not exit_time_str:
                        continue
                        
                    try:
                        exit_dt = datetime.fromisoformat(exit_time_str)
                        exit_ts = exit_dt.timestamp()
                        
                        symbol = row["symbol"]
                        pos_id = row["position_id"]
                        
                        t_6h = exit_ts + 6 * 3600
                        t_24h = exit_ts + 24 * 3600
                        t_72h = exit_ts + 72 * 3600
                        
                        def get_price_at(target_ts):
                            cur.execute("""
                                SELECT price, timestamp FROM snapshots s
                                JOIN cycles c ON s.cycle_id = c.cycle_id
                                WHERE s.symbol = ? AND c.timestamp > ?
                                ORDER BY c.timestamp ASC LIMIT 1
                            """, (symbol, datetime.fromtimestamp(target_ts, timezone.utc).isoformat()))
                            return cur.fetchone()
                            
                        now_ts = time.time()
                        updates = {}
                        
                        if row["price_6h"] is None and now_ts > t_6h:
                            match = get_price_at(t_6h)
                            if match:
                                updates["price_6h"] = match["price"]
                                updates["measured_at_6h"] = match["timestamp"]
                                
                        if row["price_24h"] is None and now_ts > t_24h:
                            match = get_price_at(t_24h)
                            if match:
                                updates["price_24h"] = match["price"]
                                updates["measured_at_24h"] = match["timestamp"]

                        if row["price_72h"] is None and now_ts > t_72h:
                            match = get_price_at(t_72h)
                            if match:
                                updates["price_72h"] = match["price"]
                                updates["measured_at_72h"] = match["timestamp"]

                        if updates:
                            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
                            values = list(updates.values())
                            values.append(pos_id)
                            conn.execute(f"UPDATE trade_attribution SET {set_clause} WHERE position_id = ?", values)

                    except Exception as e:
                        pass
        except sqlite3.Error as e:
            print(f"ERROR: SQLite evaluate_closed_trades failed - {e}")

# CCXT exchange instance (initialized once at module load)
# API keys: env vars KRAKEN_API_KEY / KRAKEN_API_SECRET, or auto-read from
# existing Kraken CLI config at ~/.config/kraken/config.toml
def _load_kraken_keys() -> tuple[str, str]:
    """Load API keys from env vars, falling back to Kraken CLI config.toml."""
    api_key = os.environ.get('KRAKEN_API_KEY', '')
    api_secret = os.environ.get('KRAKEN_API_SECRET', '')
    if api_key and api_secret:
        return api_key, api_secret
    # Fallback: read from Kraken CLI config
    config_path = os.path.expanduser("~/.config/kraken/config.toml")
    if os.path.exists(config_path):
        with open(config_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("api_key"):
                    api_key = line.split("=", 1)[1].strip().strip('"')
                elif line.startswith("api_secret"):
                    api_secret = line.split("=", 1)[1].strip().strip('"')
    return api_key, api_secret

# Retry configuration (#5)
NETWORK_RETRIES = 3
BASE_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 30.0

# ─── CCXT Exchange Initialization ─────────────────────────────────
def _init_exchanges():
    api_key, api_secret = _load_kraken_keys()
    exc = ccxt.kraken({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    })
    exc.nonce = lambda: int(time.time() * 1000000)
    f_exc = ccxt.krakenfutures({'enableRateLimit': True})
    return exc, f_exc

try:
    _exchange, _futures_exchange = _init_exchanges()
except ccxt.BaseError as e:
    print(f"ERROR: Failed to initialize exchanges: {e}")
    sys.exit(1)

_perp_symbol_map: dict[str, str] | None = None

# Caching for API efficiency (#2)
_cached_markets: dict | None = None
_funding_rates_cache: dict[str, tuple[float | None, bool]] = {}

PAIRS = {
    # Tier 1 — Large cap, reliable
    "BTC":    {"symbol": "BTC/USD"},
    "ETH":    {"symbol": "ETH/USD"},
    # Tier 2 — Mid cap, good volatility
    "SOL":    {"symbol": "SOL/USD"},
    "AVAX":   {"symbol": "AVAX/USD"},
    "LINK":   {"symbol": "LINK/USD"},
    "DOT":    {"symbol": "DOT/USD"},
    "ATOM":   {"symbol": "ATOM/USD"},
    "NEAR":   {"symbol": "NEAR/USD"},
    "INJ":    {"symbol": "INJ/USD"},
    "SUI":    {"symbol": "SUI/USD"},
    # Tier 3 — High volatility, narrative-driven
    "DOGE":   {"symbol": "DOGE/USD"},
    "FET":    {"symbol": "FET/USD"},
    "RENDER": {"symbol": "RENDER/USD"},
    "PEPE":   {"symbol": "PEPE/USD"},
    "HYPE":   {"symbol": "HYPE/USD"},
    "ONDO":   {"symbol": "ONDO/USD"},
    "TAO":    {"symbol": "TAO/USD"},
}

# Strategy parameters
RSI_PERIOD = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70
BB_PERIOD = 20
BB_STD_DEV = 2.0

# Floating-point precision constants (#1)
EPSILON = 1e-9

# ATR-based dynamic trailing stop-loss
ATR_PERIOD = 14            # ATR lookback (matches RSI period)
ATR_MULT_WIDE = 3.0        # Multiplier at entry (wide, gives room to breathe)
ATR_MULT_TIGHT = 1.5       # Multiplier when fully tightened
ATR_TIGHTEN_GAIN = 0.10    # Gain threshold at which stop is fully tight (10%)
STOP_LOSS_FALLBACK = 0.08  # Fallback fixed % if ATR unavailable
BB_MIN_WIDTH_PCT = 3.0     # Ignore upper-BB exit if band width < 3% (squeeze filter)
SQUEEZE_BB_POS_MAX = 0.92  # Require some headroom below the upper band for squeeze entries
ASSUMED_ROUND_TRIP_COST_PCT = 0.80  # Conservative taker+taker fee model for market-order churn
MIN_NET_PROFIT_BUFFER_PCT = 0.25    # Extra room above fee drag before taking discretionary profits
VOLUME_SPIKE_MULT = 1.5    # Buy requires volume >= this × 20-candle average
VOLUME_LOOKBACK = 20       # Candles to average for volume baseline
LOOP_INTERVAL_SEC = 60    # 1 minute
OHLC_INTERVAL = 60         # 1-hour candles
MIN_TRADE_USD = 5.0        # Kraken minimum order ~$5

# YOLO Hunt — Fear & Greed triggered proactive trading
FEAR_GREED_URL = "https://api.alternative.me/fng/"
YOLO_FEAR_THRESHOLD = 40        # Only hunt when Fear & Greed <= this
YOLO_IDLE_HOURS = 6             # Minimum hours in cash before hunting
YOLO_COOLDOWN_HOURS = 12        # Don't ask Gemini again within this window
SQUEEZE_LOOKBACK = 20           # Candles to scan for BB width minimum
SQUEEZE_EXPAND_CANDLES = 2      # How many recent candles must show widening
BAND_WALK_MIN = 3               # Consecutive candles above mid-BB to flag band walk
DAILY_RSI_KNIFE = 25            # Daily RSI below this + declining = falling knife

# Order Book Imbalance Veto (#17)
ORDER_BOOK_DEPTH = 10            # Order book levels to scan
SELL_WALL_RATIO = 3.0            # Veto if sell volume > this × buy volume in range
SELL_WALL_PRICE_RANGE = 0.01     # 1% above/below price = the scan zone

# Funding Rate Squeeze Overlay (#19)
FUNDING_RATE_NEGATIVE_THRESHOLD = -0.0001  # Below this = shorts being squeezed
FUNDING_RATE_SCORE_BONUS = 25              # Strength bonus for negative funding

# Relative Strength Overlay (#21)
REL_STRENGTH_24H_LOOKBACK = 24
REL_STRENGTH_72H_LOOKBACK = 72
REL_STRENGTH_24H_MULT = 1.2
REL_STRENGTH_72H_MULT = 0.8
REL_STRENGTH_SCORE_CAP = 20.0

# Stale Position Eject (#15a)
# TEMPORARY OVERRIDE: Bumped to 48h to let TAO ride out BTC options expiry volatility
STALE_EJECT_MIN_HOURS = 48
STALE_EJECT_MAX_PNL_PCT = 1.5
STALE_EJECT_MIN_HOURS_SINCE_HIGH = 12
STALE_EJECT_MIN_STRENGTH_GAP = 12.0
MIN_STALE_ROTATION_STRENGTH_GAP = 10.0  # Hard floor so stale swaps must clear a meaningful edge hurdle
STALE_EJECT_MIN_TARGET_STRENGTH = 55.0

# Momentum Continuation Buy (#30 — additive entry path for trending assets)
MOMENTUM_RSI_MIN = 40.0              # Min RSI for momentum entry (trending, not bottoming)
MOMENTUM_RSI_MAX = 70.0              # Max RSI for momentum entry (not overbought)
MOMENTUM_BB_POS_MIN = 0.4            # Price must be above this BB position
MOMENTUM_SLOPE_MIN = 2.0             # Min RSI slope over 3 periods (accelerating)
MOMENTUM_GREEN_CANDLES = 3           # Consecutive rising closes required
MOMENTUM_VOL_MIN_RATIO = 0.5         # Min volume ratio (permissive for momentum)


# ─── CCXT API Wrapper ────────────────────────────────────────────

# Map OHLC intervals (minutes) to CCXT timeframe strings
_TIMEFRAME_MAP = {
    1: '1m', 5: '5m', 15: '15m', 30: '30m',
    60: '1h', 240: '4h', 1440: '1d', 10080: '1w',
}


def get_ohlc(symbol: str, interval: int = OHLC_INTERVAL) -> list[dict] | None:
    """Fetch OHLC candles for a symbol via CCXT."""
    timeframe = _TIMEFRAME_MAP.get(interval)
    if not timeframe:
        log(f"ERROR: unsupported OHLC interval {interval}")
        return None
    try:
        # Fetch enough candles for our longest lookback (BB_PERIOD + SQUEEZE_LOOKBACK + buffer)
        ohlcv = _with_retry(_exchange.fetch_ohlcv, symbol, timeframe, limit=200)
        if not ohlcv:
            return None
        return [
            {"time": c[0] // 1000, "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
            for c in ohlcv
        ]
    except ccxt.BaseError as e:
        log(f"ERROR: fetch_ohlcv {symbol} → {e}")
        return None


def get_ticker(symbol: str) -> float | None:
    """Get current price for a symbol via CCXT."""
    try:
        ticker = _with_retry(_exchange.fetch_ticker, symbol)
        return float(ticker['last'])
    except ccxt.BaseError as e:
        log(f"ERROR: fetch_ticker {symbol} → {e}")
        return None


def get_balance() -> dict[str, float] | None:
    """Get all non-zero spendable balances via CCXT.
    Returns {currency: free_amount} with normalized keys (e.g. 'BTC', 'USD').
    Returns None on auth/network errors (distinct from empty dict = genuinely no assets)."""
    try:
        bal = _with_retry(_exchange.fetch_balance)
        free = bal.get('free', {})
        return {k: float(v) for k, v in free.items() if float(v) > 0.0001}
    except ccxt.BaseError as e:
        log(f"ERROR: fetch_balance → {e}")
        return None  # None = error, {} = genuinely empty


def truncate_amount(symbol: str, amount: float) -> float:
    """Truncate order size using Kraken's live CCXT amount precision."""
    if amount <= 0:
        return 0.0
    try:
        if not getattr(_exchange, 'markets', None):
            _exchange.load_markets()
        return float(_exchange.amount_to_precision(symbol, amount))
    except (ccxt.BaseError, ccxt.InvalidOrder) as e:
        log(f"ERROR: amount precision {symbol} → {e}")
        return 0.0


def check_sell_wall(symbol: str, price: float) -> tuple[bool, float]:
    """Check order book for sell wall above current price.
    Returns (wall_detected, sell_buy_ratio). Fails open on error."""
    ccxt_symbol = PAIRS.get(symbol, {}).get("symbol")
    if not ccxt_symbol or price <= 0:
        return False, 0.0
    try:
        global _cached_markets
        if _cached_markets is None:
            _exchange.load_markets()
            _cached_markets = dict(_exchange.markets)
        ob = _with_retry(_exchange.fetch_order_book, ccxt_symbol, limit=ORDER_BOOK_DEPTH)
        upper = price * (1 + SELL_WALL_PRICE_RANGE)
        lower = price * (1 - SELL_WALL_PRICE_RANGE)
        ask_vol = sum(v for p, v, *_ in ob.get('asks', []) if p <= upper)
        bid_vol = sum(v for p, v, *_ in ob.get('bids', []) if p >= lower)
        if bid_vol < 0.0001:
            return False, 0.0
        ratio = ask_vol / bid_vol
        wall = ratio > SELL_WALL_RATIO
        log(f"  📊 Order book {symbol}: sell/buy ratio {ratio:.1f}× "
            f"(asks {ask_vol:.2f} vs bids {bid_vol:.2f} within ±1%)" +
            (f" 🧱 SELL WALL DETECTED" if wall else ""))
        return wall, ratio
    except ccxt.BaseError as e:
        log(f"  ⚠️ Order book check failed for {symbol}: {e}")
        return False, 0.0  # Fail open


def _load_perp_map() -> dict[str, str]:
    """Build mapping of spot symbols to Kraken Futures perpetual symbols.
    E.g. {'BTC': 'BTC/USD:USD', 'ETH': 'ETH/USD:USD', ...}"""
    global _perp_symbol_map
    if _perp_symbol_map is not None:
        return _perp_symbol_map
    _perp_symbol_map = {}
    try:
        global _cached_markets
        if _cached_markets is None:
            _futures_exchange.load_markets()
            _cached_markets = dict(_futures_exchange.markets)
        for sym in PAIRS:
            perp = f"{sym}/USD:USD"
            if perp in _cached_markets:
                _perp_symbol_map[sym] = perp
        log(f"  Loaded {len(_perp_symbol_map)} perp mappings: {list(_perp_symbol_map.keys())}")
    except ccxt.BaseError as e:
        log(f"  ⚠️ Failed to load futures markets: {e}")
    return _perp_symbol_map


def get_funding_rate(symbol: str) -> tuple[float | None, bool]:
    """Get perpetual funding rate for a symbol.
    Returns (funding_rate, is_short_squeeze). Fails safe on error."""
    perp_map = _load_perp_map()
    perp_sym = perp_map.get(symbol)
    if not perp_sym:
        return None, False
    try:
        global _funding_rates_cache
        cache_key = str(perp_sym)
        if cache_key in _funding_rates_cache:
            return _funding_rates_cache[cache_key]
        fr = _with_retry(_futures_exchange.fetch_funding_rate, perp_sym)
        rate = fr.get('fundingRate')
        if rate is None:
            return None, False
        rate = float(rate)
        squeezed = rate < FUNDING_RATE_NEGATIVE_THRESHOLD
        _funding_rates_cache[cache_key] = (rate, squeezed)
        return rate, squeezed
    except ccxt.BaseError as e:
        log(f"  ⚠️ Funding rate check failed for {symbol}: {e}")
        return None, False


# ─── Gemini SDK Sentiment Filter ─────────────────────────────────
def _load_gemini_api_key() -> str:
    key = os.getenv("GEMINI_API_KEY") or os.environ.get("GOOGLE_GENERATIVE_AI_API_KEY", "")
    if key: return key
    secrets_env = os.path.expanduser("~/.config/secrets/system.env")
    if os.path.exists(secrets_env):
        with open(secrets_env, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or "=" not in line: continue
                k, v = line.split("=", 1)
                k = k.strip()
                if k == "GEMINI_API_KEY":
                    return v.strip().strip('"').strip("'")
    return ""

def gemini_sentiment(prompt: str) -> str | None:
    """Call Gemini SDK (Flash Lite) for a one-word sentiment check.
    Returns the parsed response or None on failure."""
    try:
        from google import genai
    except ImportError:
        log("  ⚠️ google-genai SDK not installed. Please pip install google-genai.")
        return None
        
    api_key = _load_gemini_api_key()
    if not api_key:
        log("  ⚠️ No Gemini API key found to run sentiment filter.")
        return None
        
    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model='gemini-3.1-flash-lite-preview',
            contents=prompt,
        )
        if not response.text:
            return None
            
        verdict_text = response.text.strip().upper()
        # Fallback for unexpected output shapes or preamble noise.
        lines = [l.strip() for l in verdict_text.splitlines() if l.strip()]
        return lines[-1] if lines else None
    except Exception as e:
        log(f"  ⚠️ Gemini SDK error: {e}")
        return None


def get_fear_greed() -> int | None:
    """Fetch the Crypto Fear & Greed Index (0-100). Returns None on failure."""
    try:
        data = json.loads(urlopen(FEAR_GREED_URL, timeout=10).read())
        value = int(data["data"][0]["value"])
        classification = data["data"][0]["value_classification"]
        log(f"  🌡️ Fear & Greed Index: {value} ({classification})")
        return value
    except (URLError, KeyError, ValueError, Exception) as e:
        log(f"  ⚠️ Fear & Greed API error: {e}")
        return None


def gemini_yolo_pick(analyses: dict, excluded_symbols: set = None, telemetry: dict = None) -> tuple[str | None, bool]:
    """Ask Gemini Flash to vet the top mechanically scored swing trade.
    Returns (symbol, consulted) — symbol is from PAIRS or None,
    consulted is True if Gemini was actually asked (for cooldown tracking)."""
    excluded = excluded_symbols or set()
    fng = telemetry["cycle"]["fear_greed_index"] if telemetry else get_fear_greed()
    if fng is None:
        return None, False
    if fng > YOLO_FEAR_THRESHOLD:
        log(f"  🌡️ Fear & Greed {fng} > {YOLO_FEAR_THRESHOLD} — market not fearful enough for YOLO")
        return None, False

    # Pre-screen: only send coins that look bounce-able (not overbought)
    candidates = {sym: a for sym, a in analyses.items()
                  if a['rsi'] < 50 and a['bb_position'] < 0.60 and sym not in excluded}
    if len(candidates) < 3:
        log(f"  🐊 YOLO hunt: only {len(candidates)} coins below RSI 50 / BB 0.60 — skipping")
        return None, False
    log(f"  🐊 YOLO pre-screen: {len(candidates)}/{len(analyses)} coins qualify")

    # Sort candidates deterministically by their composite strength score
    best_sym = sorted(candidates.keys(), key=lambda s: candidates[s].get('strength', 0), reverse=True)[0]
    best_a = candidates[best_sym]
    funding_rate = best_a.get("funding_rate")
    funding_squeezed = best_a.get("funding_squeezed", False)
    if funding_rate is None:
        funding_rate, funding_squeezed = get_funding_rate(best_sym)
        best_a["funding_rate"] = funding_rate
        best_a["funding_squeezed"] = funding_squeezed

    coin_summary = (
        f"{best_sym}\n"
        f"Price: {format_price(best_a['price'])}\n"
        f"RSI: {best_a['rsi']}\n"
        f"BB position: {best_a['bb_position']:.2f}\n"
        f"Volume ratio: {best_a.get('vol_ratio', 0)}x\n"
        f"RSI divergence: {'yes' if best_a.get('rsi_divergence') else 'no'}\n"
        f"Squeeze breakout: {'yes' if best_a.get('bb_squeeze_breakout') else 'no'}"
    )
    funding_ctx = ""
    if funding_rate is not None:
        funding_ctx = f"Funding rate: {funding_rate:+.6f}."
        funding_ctx += "\n\n"

    prompt = (f"Search for very recent news about {best_sym} before deciding.\n\n"
              f"A separate deterministic trading system has already selected {best_sym}. "
              f"Do not re-evaluate market fear/greed or the overall strategy. Your only job "
              f"is to check whether there is a concrete current reason this coin is unsafe "
              f"for a swing entry right now.\n\n"
              f"Candidate:\n"
              f"{coin_summary}\n\n"
              f"{funding_ctx}"
              f"Output RISK only if you find a specific issue such as hack/exploit, fraud, "
              f"insolvency, regulatory action, major token unlock or dilution, delisting or "
              f"liquidity problem, chain outage, or a very recent market-wide shock with "
              f"direct impact on {best_sym}.\n\n"
              f"If no concrete current risk is found, output SAFE.\n\n"
              f"Think silently. Output exactly one word: SAFE or RISK.")

    response = gemini_sentiment(prompt)
    if response is None:
        log(f"  ⚠️ Gemini unreachable for YOLO pick")
        return None, False

    verdict = response.strip().upper().split()[0] if response.strip() else ""
    if verdict == "SAFE":
        log(f"  🤖 Gemini YOLO pick: {best_sym} (SAFE)")
        return best_sym, True
    elif verdict == "RISK":
        log(f"  🤖 Gemini YOLO pick: {best_sym} (RISK - Vetoed)")
        if telemetry:
            telemetry["decision_events"].append({
                "candidate_symbol": best_sym,
                "decision_context": "gemini_prompt",
                "decision_stage": "yolo_hunt",
                "event_type": "VETO",
                "veto_reason": "gemini_yolo_veto",
                "strength_score": best_a.get("strength"),
                "rsi": best_a.get("rsi"),
                "bb_position": best_a.get("bb_position"),
            })
        return None, True
    else:
        log(f"  ⚠️ Gemini YOLO response unparseable (first token: '{verdict}'): {response[:80]}")
        return None, True


def check_btc_crash(analyses: dict, telemetry: dict = None) -> bool:
    """Layer 1: Check if BTC has crashed (price near lower BB with very low RSI).
    Returns True if crash detected, False if market is stable."""
    btc = analyses.get("BTC")
    if not btc:
        return False  # Can't determine, allow trades
    # BTC RSI below 35 AND near/below lower BB = systemic crash
    if btc["rsi"] < 35 and btc["bb_position"] < 0.15:
        log(f"  🛑 BTC CRASH GUARD: BTC RSI={btc['rsi']}, BB pos={btc['bb_position']:.2f} — blocking buys")
        if telemetry:
            telemetry["decision_events"].append({
                "candidate_symbol": "ALL",
                "decision_context": "system",
                "decision_stage": "pre_flight",
                "event_type": "VETO",
                "veto_reason": "btc_crash_guard",
                "rsi": btc["rsi"],
                "bb_position": btc["bb_position"],
            })
            if "cycle" in telemetry:
                telemetry["cycle"]["btc_crash_guard_active"] = True
        return True
    return False


def gemini_buy_check(symbol: str, rsi: float, bb_position: float, analyses: dict, telemetry: dict = None) -> bool:
    """Layer 2: Ask Gemini Flash if it's safe to buy.
    Returns True if safe to buy, False if should wait."""
    btc = analyses.get("BTC", {})
    btc_rsi = btc.get("rsi", "N/A")
    btc_bb = f"{btc.get('bb_position', 0):.2f}" if btc else "N/A"
    prompt = (f"Search for very recent news about {symbol} before deciding.\n\n"
              f"A separate deterministic trading system has already identified a technical "
              f"buy setup in {symbol}. Your only job is to check whether there is a concrete "
              f"current reason this entry is unsafe right now.\n\n"
              f"Candidate:\n"
              f"{symbol}\n"
              f"RSI: {rsi}\n"
              f"BB position: {bb_position:.2f}\n\n"
              f"BTC context:\n"
              f"RSI: {btc_rsi}\n"
              f"BB position: {btc_bb}\n\n"
              f"Output RISK only if you find a specific issue such as hack/exploit, fraud, "
              f"insolvency, regulatory action, major token unlock or dilution, delisting or "
              f"liquidity problem, chain outage, or a very recent market-wide shock that "
              f"directly makes this entry dangerous.\n\n"
              f"Do not veto only because the market is weak or fearful in general. BTC "
              f"context is provided only to judge whether there is an active broad-market "
              f"danger, not to re-evaluate the strategy.\n\n"
              f"If no concrete current risk is found, output SAFE.\n\n"
              f"Think silently. Output exactly one word: SAFE or RISK.")
    response = gemini_sentiment(prompt)
    if response is None:
        log(f"  ⚠️ Gemini unreachable — proceeding with buy (Layer 1 passed)")
        return True  # Fail-open: Layer 1 already cleared
    verdict = response.strip().split()[0].upper() if response.strip() else "RISK"
    if verdict not in ("SAFE", "RISK"):
        log(f"  ⚠️ Gemini buy response unexpected ('{verdict}'), defaulting to RISK")
        verdict = "RISK"
    log(f"  🤖 Gemini buy check: {verdict}")
    if verdict == "RISK" and telemetry:
        telemetry["decision_events"].append({
            "candidate_symbol": symbol,
            "decision_context": "gemini_prompt",
            "decision_stage": "gemini_approval",
            "event_type": "VETO",
            "veto_reason": "gemini_buy_veto",
            "rsi": rsi,
            "bb_position": bb_position,
        })
    elif verdict == "SAFE" and telemetry and "all_analyses" in telemetry:
        if symbol in telemetry["all_analyses"]:
            telemetry["all_analyses"][symbol]["gemini_verdict"] = "SAFE"

    return verdict == "SAFE"


def gemini_sell_check(symbol: str, rsi: float, entry_price: float, current_price: float,
                      bb_position: float, band_walk_candles: int = 0, telemetry: dict = None) -> bool:
    """Layer 3: Ask Gemini Flash if we should hold longer on a winning trade.
    Returns True if should sell, False if should hold.
    Only called for RSI-based sells, NEVER for stop-loss."""
    pnl_pct = (current_price / entry_price - 1) * 100
    walk_ctx = ""
    if band_walk_candles >= BAND_WALK_MIN:
        walk_ctx = (f"Trend context: price has closed above the middle Bollinger Band for "
                    f"{band_walk_candles} consecutive candles.\n")
    prompt = (f"Search for very recent news about {symbol} before deciding.\n\n"
              f"A separate deterministic trading system has already triggered a take-profit "
              f"/ overextension sell signal. Your job is to decide whether there is a "
              f"concrete current reason to override that sell and keep holding.\n\n"
              f"Position:\n"
              f"{symbol}\n"
              f"Entry price: {format_price(entry_price)}\n"
              f"Current price: {format_price(current_price)}\n"
              f"P&L: {pnl_pct:+.1f}%\n"
              f"RSI: {rsi}\n"
              f"BB position: {bb_position:.2f}\n"
              f"{walk_ctx}\n"
              f"Output HOLD only if you find a specific, current reason that continued "
              f"upside is still likely in the near term, such as an active catalyst, "
              f"confirmed breakout continuation, major positive announcement, or unusually "
              f"strong trend conditions.\n\n"
              f"Do not output HOLD based only on generic optimism, vague long-term "
              f"potential, or normal crypto bullishness.\n\n"
              f"If no concrete current reason to override the sell signal is found, output "
              f"SELL.\n\n"
              f"Think silently. Output exactly one word: SELL or HOLD.")
    response = gemini_sentiment(prompt)
    if response is None:
        log(f"  ⚠️ Gemini unreachable — defaulting to SELL (take profit)")
        return True  # Fail-safe: take the profit
    verdict = response.strip().split()[0].upper() if response.strip() else "SELL"
    if verdict not in ("SELL", "HOLD"):
        log(f"  ⚠️ Gemini sell response unexpected ('{verdict}'), defaulting to SELL")
        verdict = "SELL"
    log(f"  🤖 Gemini sell check: {verdict}")
    if verdict == "HOLD" and telemetry:
        telemetry["decision_events"].append({
            "candidate_symbol": symbol,
            "decision_context": f"pnl_pct={pnl_pct:.1f}% bb_pos={bb_position:.2f}",
            "decision_stage": "gemini_approval",
            "event_type": "VETO",
            "veto_reason": "gemini_hold_override",
            "rsi": rsi,
            "bb_position": bb_position,
        })
    elif verdict == "SELL" and telemetry and "all_analyses" in telemetry:
        if symbol in telemetry["all_analyses"]:
            telemetry["all_analyses"][symbol]["gemini_verdict"] = "SELL"
    return verdict == "SELL"


def place_order(side: str, symbol: str, volume: float, order_type: str = "market") -> dict | None:
    """Place a market or limit order via CCXT."""
    vol_str = f"{volume:.8f}".rstrip("0").rstrip(".")
    log(f"ORDER: {side.upper()} {vol_str} {symbol} ({order_type})")
    try:
        if side == "buy":
            result = _with_retry(_exchange.create_market_buy_order, symbol, volume)
        else:
            result = _with_retry(_exchange.create_market_sell_order, symbol, volume)
        return result
    except ccxt.InsufficientFunds as e:
        log(f"ORDER FAILED: Insufficient funds — {e}")
        return None
    except ccxt.BaseError as e:
        log(f"ORDER FAILED: {side.upper()} {vol_str} {symbol} → {e}")
        return None


def required_profit_take_pct() -> float:
    """Minimum gross gain required before discretionary profit-taking is allowed."""
    return ASSUMED_ROUND_TRIP_COST_PCT + MIN_NET_PROFIT_BUFFER_PCT


# ─── Network Retry Logic ─────────────────────────────────────────
def _with_retry(func, *args, **kwargs):
    max_retries = NETWORK_RETRIES
    base_delay = BASE_RETRY_DELAY
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except (ccxt.NetworkError, ccxt.ExchangeError, URLError, TimeoutError) as e:
            if attempt == max_retries - 1:
                log(f"  ❌ {func.__name__} failed after {max_retries} retries: {e}")
                raise
            delay = base_delay * (2**attempt)
            log(f"  ⏱️ Retry {attempt + 1}/{max_retries} after {delay:.1f}s: {type(e).__name__}")
            time.sleep(delay)

# ─── Technical Indicators ────────────────────────────────────────
def calc_rsi(closes: list[float], period: int = None) -> float | None:
    """Calculate RSI from close prices."""
    if period is None: period = RSI_PERIOD
    if len(closes) < period + 1:
        return None

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(0.0, d) for d in deltas]
    losses = [max(0.0, -d) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss < EPSILON:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calc_bollinger(closes: list[float], period: int = None, std_dev: float = None) -> tuple[float, float, float] | None:
    """Calculate Bollinger Bands. Returns (lower, middle, upper)."""
    if period is None: period = BB_PERIOD
    if std_dev is None: std_dev = BB_STD_DEV
    if len(closes) < period:
        return None

    window = closes[-period:]
    middle = sum(window) / period
    variance = sum([(x - middle) ** 2 for x in window]) / period
    std = math.sqrt(variance)

    return (middle - std_dev * std, middle, middle + std_dev * std)


def calc_atr(candles: list[dict], period: int = None) -> float | None:
    """Calculate Average True Range from OHLC candles."""
    if period is None: period = ATR_PERIOD
    if len(candles) < period + 1:
        return None
    true_ranges = []
    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
    atr = sum(true_ranges[:period]) / period
    for tr in true_ranges[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def calc_volume_spike(candles: list[dict], lookback: int = None,
                      threshold: float = None) -> tuple[bool, float]:
    """Check if recent volume is a spike above the average.
    Returns (is_spike, volume_ratio) where ratio = max(latest, prev) / avg."""
    if lookback is None: lookback = VOLUME_LOOKBACK
    if threshold is None: threshold = VOLUME_SPIKE_MULT
    if len(candles) < lookback + 2:
        return False, 0.0
    recent = candles[-(lookback + 2):-2]  # 20 candles BEFORE the last 2
    avg_vol = sum(c["volume"] for c in recent) / len(recent)
    if avg_vol <= 0:
        return False, 0.0
    max_recent_vol = max(candles[-1]["volume"], candles[-2]["volume"])
    ratio = max_recent_vol / avg_vol
    return ratio >= threshold, round(ratio, 2)


def calc_rsi_divergence(closes: list[float], period: int = None,
                        lookback: int = 30) -> bool:
    """Detect bullish RSI divergence: price makes lower low but RSI makes higher low.
    Scans the last `lookback` candles for local lows and compares the two most recent.
    Returns True if bullish divergence is present."""
    if period is None: period = RSI_PERIOD
    if len(closes) < max(period + 1, lookback):
        return False

    window = closes[-lookback:]

    # Compute per-candle RSI across the window using a sliding approach
    # We need RSI values aligned to each candle in the window
    # First, compute RSI for every candle position using full history
    all_rsis = []
    for i in range(lookback):
        end_idx = len(closes) - lookback + i + 1
        if end_idx < period + 1:
            all_rsis.append(None)
            continue
        rsi_val = calc_rsi(closes[:end_idx], period)
        all_rsis.append(rsi_val)

    # Find local lows in the window (candle lower than both neighbors)
    # Skip first and last candle (need neighbors)
    local_lows = []
    for i in range(1, len(window) - 1):
        if window[i] <= window[i - 1] and window[i] <= window[i + 1]:
            rsi_at_low = all_rsis[i]
            if rsi_at_low is not None:
                local_lows.append((i, window[i], rsi_at_low))

    if len(local_lows) < 2:
        return False

    # Compare the two most recent local lows
    prev_low = local_lows[-2]  # (index, price, rsi)
    curr_low = local_lows[-1]

    # Bullish divergence: price lower low + RSI higher low
    return curr_low[1] < prev_low[1] and curr_low[2] > prev_low[2]


def calc_bb_squeeze(candles: list[dict], period: int = None,
                    std_dev: float = None,
                    lookback: int = None,
                    expand_candles: int = None) -> tuple[bool, bool, float]:
    """Detect Bollinger Band squeeze and upward breakout.
    A squeeze is when bb_width hits its tightest point in `lookback` candles
    and then starts expanding with price above the middle band.
    Returns (is_squeezing, is_breakout_up, squeeze_tightness).
    squeeze_tightness: 0.0-1.0, where 1.0 = tightest possible (current width == minimum)."""
    if period is None: period = BB_PERIOD
    if std_dev is None: std_dev = BB_STD_DEV
    if lookback is None: lookback = SQUEEZE_LOOKBACK
    if expand_candles is None: expand_candles = SQUEEZE_EXPAND_CANDLES
    if len(candles) < period + lookback:
        return False, False, 0.0

    # Compute bb_width for each candle position over the lookback window
    widths = []
    for i in range(lookback):
        end_idx = len(candles) - lookback + i + 1
        closes = [c["close"] for c in candles[:end_idx]]
        if len(closes) < period:
            continue
        window = closes[-period:]
        middle = sum(window) / period
        if middle <= 0:
            continue
        variance = sum((x - middle) ** 2 for x in window) / period
        std = variance ** 0.5
        width = (2 * std_dev * std) / middle * 100  # Width as % of middle
        widths.append(width)

    if len(widths) < lookback:
        return False, False, 0.0

    min_width = min(widths)
    max_width = max(widths)
    curr_width = widths[-1]

    if max_width <= min_width or min_width <= 0:
        return False, False, 0.0

    # Squeeze tightness: how close current width is to the minimum (1.0 = at minimum)
    tightness = 1.0 - (curr_width - min_width) / (max_width - min_width)

    # Squeezing: current width within the bottom 20% of the range
    is_squeezing = tightness >= 0.80

    # Breakout: bands were at/near minimum recently and now expanding
    # Check that the last `expand_candles` widths are increasing
    recent = widths[-expand_candles - 1:]
    is_expanding = all(recent[j + 1] > recent[j] for j in range(len(recent) - 1))

    # Was squeezing recently (minimum was within last few candles)
    min_idx = widths.index(min_width)
    was_squeezed = min_idx >= len(widths) - expand_candles - 3  # Minimum was recent

    # Upward breakout: expanding after squeeze + price above middle band
    closes = [c["close"] for c in candles]
    bb = calc_bollinger(closes)
    if bb is None:
        return is_squeezing, False, round(tightness, 2)
    _, bb_middle, _ = bb
    price_above_mid = closes[-1] > bb_middle

    is_breakout_up = was_squeezed and is_expanding and price_above_mid

    return is_squeezing, is_breakout_up, round(tightness, 2)


def detect_band_walk(candles: list[dict], bb_period: int = None,
                     bb_std: float = None,
                     min_candles: int = None) -> tuple[bool, int]:
    """Detect if price is 'walking the upper band' — closing above the middle BB
    with bb_position well above center and a rising middle band.
    Returns (is_walking, consecutive_count)."""
    if bb_period is None: bb_period = BB_PERIOD
    if bb_std is None: bb_std = BB_STD_DEV
    if min_candles is None: min_candles = BAND_WALK_MIN
    if len(candles) < bb_period + min_candles:
        return False, 0

    # Check each recent candle's close vs a tighter threshold:
    # close must be above middle BB, BB position >= 0.65, and middle BB rising
    count = 0
    prev_middle = None
    for i in range(len(candles) - 1, bb_period - 1, -1):
        closes = [c["close"] for c in candles[:i + 1]]
        window = closes[-bb_period:]
        middle = sum(window) / bb_period
        variance = sum((x - middle) ** 2 for x in window) / bb_period
        std = variance ** 0.5
        upper = middle + bb_std * std
        lower = middle - bb_std * std
        pos = (closes[-1] - lower) / (upper - lower) if upper != lower else 0.5

        # Tighter checks: above mid, BB pos >= 0.65, and middle BB rising
        # (prev_middle is from a NEWER candle since we iterate newest→oldest)
        mid_rising = prev_middle is None or prev_middle >= middle
        if closes[-1] > middle and pos >= 0.65 and mid_rising:
            count += 1
            prev_middle = middle
        else:
            break

    return count >= min_candles, count


def calc_dynamic_stop(price: float, entry_price: float, atr: float | None) -> float:
    """Calculate trailing stop distance using ATR with gain-scaled multiplier.
    Wide at entry (3× ATR), tightens to 1.5× ATR as gains reach 10%.
    Falls back to fixed 8% if ATR unavailable."""
    if atr is None or atr <= 0:
        return price * (1 - STOP_LOSS_FALLBACK)
    gain_pct = max(0, (price - entry_price) / entry_price)
    ratio = min(gain_pct / ATR_TIGHTEN_GAIN, 1.0)
    multiplier = ATR_MULT_WIDE - (ATR_MULT_WIDE - ATR_MULT_TIGHT) * ratio
    return price - (atr * multiplier)


def round_price(price: float) -> float:
    """Round a price to appropriate precision for its magnitude."""
    if price >= 1000: return round(price, 2)
    if price >= 1:    return round(price, 4)
    if price >= 0.001: return round(price, 6)
    return round(price, 9)


def format_price(price: float) -> str:
    """Format a price with appropriate precision for prompts and logs."""
    if price >= 1000: return f"${price:,.2f}"
    if price >= 1:    return f"${price:.4f}"
    if price >= 0.001: return f"${price:.6f}"
    return f"${price:.9f}"


def calc_pct_change(closes: list[float], lookback: int) -> float | None:
    """Calculate percentage change over a candle lookback."""
    if len(closes) < lookback + 1:
        return None
    old_price = closes[-(lookback + 1)]
    if old_price <= 0:
        return None
    return (closes[-1] / old_price - 1) * 100


def hours_since(ts: str | None, now: datetime | None = None) -> float | None:
    """Return hours since an ISO timestamp."""
    if not ts:
        return None
    try:
        ref = now or datetime.now(timezone.utc)
        return (ref - datetime.fromisoformat(ts)).total_seconds() / 3600
    except ValueError:
        return None


def apply_relative_strength_overlay(analyses: dict[str, dict]):
    """Apply a relative-strength bonus versus BTC/ETH leadership."""
    benchmark_24_vals = [
        analyses[s]["change_24h"] for s in ("BTC", "ETH")
        if analyses.get(s) and analyses[s].get("change_24h") is not None
    ]
    benchmark_72_vals = [
        analyses[s]["change_72h"] for s in ("BTC", "ETH")
        if analyses.get(s) and analyses[s].get("change_72h") is not None
    ]
    benchmark_24 = (sum(benchmark_24_vals) / len(benchmark_24_vals)) if benchmark_24_vals else None
    benchmark_72 = (sum(benchmark_72_vals) / len(benchmark_72_vals)) if benchmark_72_vals else None

    for analysis in analyses.values():
        rel24 = None
        rel72 = None
        rel_score = 0.0
        if benchmark_24 is not None and analysis.get("change_24h") is not None:
            rel24 = analysis["change_24h"] - benchmark_24
            rel_score += max(0.0, rel24) * REL_STRENGTH_24H_MULT
        if benchmark_72 is not None and analysis.get("change_72h") is not None:
            rel72 = analysis["change_72h"] - benchmark_72
            rel_score += max(0.0, rel72) * REL_STRENGTH_72H_MULT
        rel_score = min(rel_score, REL_STRENGTH_SCORE_CAP)

        analysis["rel_strength_24h"] = round(rel24, 2) if rel24 is not None else None
        analysis["rel_strength_72h"] = round(rel72, 2) if rel72 is not None else None
        analysis["rel_strength_score"] = round(rel_score, 1)

        # Relative strength should help choose leaders, not exaggerate sell scores.
        if rel_score > 0 and not analysis.get("sell_signal"):
            analysis["strength"] = round(analysis["strength"] + rel_score, 1)


# ─── Signal Analysis ─────────────────────────────────────────────
def analyze_asset(symbol: str, is_held_squeeze: bool = False) -> dict | None:
    """Analyze an asset and return signal data."""
    info = PAIRS[symbol]
    candles = get_ohlc(info["symbol"])
    if not candles or len(candles) < BB_PERIOD + 5:
        log(f"  {symbol}: insufficient OHLC data")
        return None

    closes = [c["close"] for c in candles]
    price = closes[-1]
    rsi = calc_rsi(closes)
    bb = calc_bollinger(closes)
    atr = calc_atr(candles)
    vol_spike, vol_ratio = calc_volume_spike(candles)
    rsi_divergence = calc_rsi_divergence(closes)
    bb_squeezing, bb_squeeze_breakout, bb_squeeze_tightness = calc_bb_squeeze(candles)
    band_walking, band_walk_count = detect_band_walk(candles)

    if rsi is None or bb is None:
        return None

    bb_lower, bb_middle, bb_upper = bb
    bb_width = (bb_upper - bb_lower) / bb_middle * 100  # Width as % of middle
    bb_position = (price - bb_lower) / (bb_upper - bb_lower) if bb_upper != bb_lower else 0.5
    upper_band_runway_pct = max(0.0, ((bb_upper - price) / price) * 100) if price > 0 else 0.0
    change_24h = calc_pct_change(closes, REL_STRENGTH_24H_LOOKBACK)
    change_72h = calc_pct_change(closes, REL_STRENGTH_72H_LOOKBACK)

    # Signal scoring — volume confirmation required for math-based buys
    buy_signal = rsi < RSI_OVERSOLD and price <= bb_lower and vol_spike
    above_upper_band = price >= bb_upper
    # Upper-BB exit only fires when bands are wide enough (prevents squeeze false exits)
    bb_exit = above_upper_band and bb_width > BB_MIN_WIDTH_PCT

    # Squeeze breakout buy: bands expanding after squeeze + price above middle + volume
    # Explicitly bans buying if price is already above the upper band, even when the
    # width-filtered bb_exit stays quiet during a tight squeeze.
    squeeze_buy = (
        bb_squeeze_breakout
        and vol_spike
        and not above_upper_band
        and bb_position <= SQUEEZE_BB_POS_MAX
    )

    # Momentum Continuation Buy (#30 — additive entry path for trending assets)
    # Catches assets that are steadily climbing, not dipping
    consecutive_green = 0
    for k in range(len(candles) - 1, max(len(candles) - 10, 0), -1):
        if candles[k]["close"] > candles[k - 1]["close"]:
            consecutive_green += 1
        else:
            break

    # RSI slope over 3 periods
    rsi_slope = 0.0
    if len(closes) >= RSI_PERIOD + 3:
        r_prev = calc_rsi(closes[:-2])
        if r_prev is not None:
            rsi_slope = rsi - r_prev

    momentum_buy = (
        MOMENTUM_RSI_MIN < rsi < MOMENTUM_RSI_MAX
        and bb_position > MOMENTUM_BB_POS_MIN
        and rsi_slope > MOMENTUM_SLOPE_MIN
        and consecutive_green >= MOMENTUM_GREEN_CANDLES
        and vol_ratio >= MOMENTUM_VOL_MIN_RATIO
    )

    # Prevent immediate exit churn on fresh breakouts by raising the RSI exit threshold
    dynamic_overbought = 80 if (squeeze_buy or is_held_squeeze) else RSI_OVERBOUGHT
    sell_signal = rsi > dynamic_overbought or bb_exit

    # Composite Signal Strength
    funding_rate_val = None
    funding_squeezed = False
    if momentum_buy and not buy_signal and not squeeze_buy:
        # Momentum-specific strength scoring:
        # Rewards RSI slope (acceleration), trend persistence, and volume
        slope_score = min(rsi_slope, 15.0) * 3.0        # RSI acceleration is primary signal
        trend_score = min(consecutive_green, 6) * 5.0     # Persistence bonus
        vol_score = min(vol_ratio, 3.0) * 5              # Modest volume bonus
        momentum_pos_score = bb_position * 15             # Higher BB position = stronger trend
        # Funding rate squeeze bonus (#19)
        funding_rate_val, funding_squeezed = get_funding_rate(symbol)
        funding_score = FUNDING_RATE_SCORE_BONUS if funding_squeezed else 0

        strength = slope_score + trend_score + vol_score + momentum_pos_score + funding_score
    elif buy_signal or squeeze_buy:
        # Base oversold depth score (original dip-buy scoring)
        depth_score = max(0, 40 - rsi) * 1.5 + max(0, 1 - bb_position) * 30
        # Volume burst bonus
        vol_score = min(vol_ratio, 3.0) * 10
        # Divergence bonus
        div_score = 20 if rsi_divergence else 0
        # Squeeze tightness bonus
        squeeze_score = bb_squeeze_tightness * 40 if bb_squeeze_tightness > 0 else 0
        # Funding rate squeeze bonus (#19 — lazy, only when buy signal active)
        funding_rate_val, funding_squeezed = get_funding_rate(symbol)
        funding_score = FUNDING_RATE_SCORE_BONUS if funding_squeezed else 0
        if funding_rate_val is not None:
            funding_log = f" | 💰FR: {funding_rate_val:+.6f}"
            if funding_squeezed:
                funding_log += " 🔥SHORT SQUEEZE"
            log(f"  {symbol}{funding_log}")

        strength = depth_score + vol_score + div_score + squeeze_score + funding_score
    elif sell_signal:
        strength = (rsi - RSI_OVERBOUGHT) * 2 + (bb_position) * 50
    else:
        # Base strength for YOLO ranking
        depth_score = max(0, 50 - rsi) * 1.0 + max(0, 1 - bb_position) * 10
        vol_score = min(float(vol_ratio), 3.0) * 5  # Modest volume bonus for waking up
        div_score = 10 if rsi_divergence else 0
        squeeze_score = bb_squeeze_tightness * 20 if bb_squeeze_tightness > 0 else 0
        # Funding rate squeeze bonus (#19 — lazy, only when buy signal active)
        funding_rate_val, funding_squeezed = get_funding_rate(symbol)
        funding_score = FUNDING_RATE_SCORE_BONUS if funding_squeezed else 0
        if funding_rate_val is not None:
            funding_log = f" | 💰FR: {funding_rate_val:+.6f}"
            if funding_squeezed:
                funding_log += " 🔥SHORT SQUEEZE"
            log(f"  {symbol}{funding_log}")

        strength = depth_score + vol_score + div_score + squeeze_score + funding_score

    return {
        "symbol": symbol,
        "price": price,
        "rsi": round(rsi, 1),
        "bb_lower": round(bb_lower, 2),
        "bb_middle": round(bb_middle, 2),
        "bb_upper": round(bb_upper, 2),
        "bb_position": round(bb_position, 3),
        "bb_width": round(bb_width, 2),
        "upper_band_runway_pct": round(upper_band_runway_pct, 3),
        "change_24h": round(change_24h, 2) if change_24h is not None else None,
        "change_72h": round(change_72h, 2) if change_72h is not None else None,
        "vol_ratio": vol_ratio,
        "vol_spike": vol_spike,
        "rsi_divergence": rsi_divergence,
        "bb_squeezing": bb_squeezing,
        "bb_squeeze_breakout": bb_squeeze_breakout,
        "bb_squeeze_tightness": bb_squeeze_tightness,
        "bb_exit": bb_exit,
        "band_walking": band_walking,
        "band_walk_count": band_walk_count,
        "buy_signal": buy_signal,
        "squeeze_buy": squeeze_buy,
        "momentum_buy": momentum_buy,
        "sell_signal": sell_signal,
        "strength": round(strength, 1),
        "atr": round(atr, 6) if atr else None,
        "funding_rate": funding_rate_val,
        "funding_squeezed": funding_squeezed,
        "rsi_slope": round(rsi_slope, 2),
        "consecutive_green": consecutive_green,
        "rel_strength_24h": None,
        "rel_strength_72h": None,
        "rel_strength_score": 0.0,
    }


def get_daily_rsi(symbol: str) -> tuple[float | None, bool]:
    """Fetch daily candles and compute daily RSI for a symbol.
    Returns (daily_rsi, is_declining).
    is_declining = True if daily RSI has dropped over the last 3 candles."""
    ccxt_sym = PAIRS[symbol]["symbol"]
    candles = get_ohlc(ccxt_sym, interval=1440)
    if not candles or len(candles) < RSI_PERIOD + 4:
        return None, False

    closes = [c["close"] for c in candles]
    daily_rsi = calc_rsi(closes)
    if daily_rsi is None:
        return None, False

    # Check if RSI is declining across last 3 daily candles
    recent_rsis = []
    for i in range(3):
        end_idx = len(closes) - i
        if end_idx >= RSI_PERIOD + 1:
            r = calc_rsi(closes[:end_idx])
            if r is not None:
                recent_rsis.append(r)
    recent_rsis.reverse()  # Oldest first

    is_declining = (len(recent_rsis) >= 3 and
                    recent_rsis[-1] < recent_rsis[-2] < recent_rsis[-3])

    return round(daily_rsi, 1), is_declining


def select_best_entry_candidate(analyses: dict[str, dict],
                                excluded_symbols: set[str] | None = None,
                                telemetry: dict = None) -> dict | None:
    """Return the best buy candidate after all deterministic guards."""
    excluded = excluded_symbols or set()
    if check_btc_crash(analyses, telemetry=telemetry):
        log(f"  Skipping all buys — market crash detected")
        return None

    buy_candidates = [
        a for a in analyses.values()
        if (a["buy_signal"] or a.get("squeeze_buy") or a.get("momentum_buy")) and a["symbol"] not in excluded
    ]

    while buy_candidates:
        best = max(buy_candidates, key=lambda x: x["strength"])
        if best.get('momentum_buy') and not best.get('buy_signal') and not best.get('squeeze_buy'):
            sig_type = "MOMENTUM"
        elif best.get('squeeze_buy') and not best['buy_signal']:
            sig_type = "SQUEEZE BREAKOUT"
        else:
            sig_type = "RSI/BB"
        log(f"  Best entry: {best['symbol']} ({sig_type}, strength: {best['strength']})")
        daily_rsi, daily_declining = get_daily_rsi(best['symbol'])
        if daily_rsi is not None:
            log(f"  📅 Daily RSI: {daily_rsi}{' ↘ declining' if daily_declining else ' stable'}")
            if daily_rsi < DAILY_RSI_KNIFE and daily_declining:
                log(f"  🔪 FALLING KNIFE — daily RSI {daily_rsi} < {DAILY_RSI_KNIFE} and declining, skipping {best['symbol']}")
                if telemetry:
                    telemetry["decision_events"].append({
                        "candidate_symbol": best["symbol"],
                        "decision_context": f"daily_rsi={daily_rsi}",
                        "decision_stage": "veto_checks",
                        "event_type": "VETO",
                        "veto_reason": "falling_knife",
                        "strength_score": best.get("strength"),
                        "rsi": best.get("rsi"),
                        "bb_position": best.get("bb_position"),
                    })
                buy_candidates = [c for c in buy_candidates if c['symbol'] != best['symbol']]
                continue
        wall_detected, wall_ratio = check_sell_wall(best['symbol'], best['price'])
        best['sell_buy_ratio'] = round(wall_ratio, 2)
        if wall_detected:
            log(f"  🧱 SELL WALL VETO — {best['symbol']} has {wall_ratio:.1f}× sell/buy imbalance, skipping")
            if telemetry:
                telemetry["decision_events"].append({
                    "candidate_symbol": best["symbol"],
                    "decision_context": f"wall_ratio={wall_ratio:.1f}",
                    "decision_stage": "veto_checks",
                    "event_type": "VETO",
                    "veto_reason": "sell_wall_imbalance",
                    "strength_score": best.get("strength"),
                    "rsi": best.get("rsi"),
                    "bb_position": best.get("bb_position"),
                })
            buy_candidates = [c for c in buy_candidates if c['symbol'] != best['symbol']]
            continue
        if best.get("squeeze_buy") and not best.get("buy_signal"):
            required_runway_pct = required_profit_take_pct()
            runway_pct = best.get("upper_band_runway_pct", 0.0)
            if runway_pct < required_runway_pct:
                log(
                    f"  🪙 Fee Gate: {best['symbol']} squeeze runway {runway_pct:.2f}% < "
                    f"required {required_runway_pct:.2f}% — skipping hot breakout"
                )
                if telemetry:
                    telemetry["decision_events"].append({
                        "candidate_symbol": best["symbol"],
                        "decision_context": f"runway_pct={runway_pct:.2f} required={required_runway_pct:.2f}",
                        "decision_stage": "veto_checks",
                        "event_type": "VETO",
                        "veto_reason": "fee_aware_squeeze_runway",
                        "strength_score": best.get("strength"),
                        "rsi": best.get("rsi"),
                        "bb_position": best.get("bb_position"),
                    })
                buy_candidates = [c for c in buy_candidates if c['symbol'] != best['symbol']]
                continue
        return best

    return None


def select_best_gemini_approved_candidate(analyses: dict[str, dict],
                                          excluded_symbols: set[str] | None = None,
                                          skip_gemini: bool = False,
                                          telemetry: dict = None) -> dict | None:
    """Return the best candidate that also passes Gemini buy veto, if enabled."""
    excluded = set(excluded_symbols or set())
    attempted: set[str] = set()

    while True:
        candidate = select_best_entry_candidate(analyses, excluded_symbols=excluded | attempted, telemetry=telemetry)
        if not candidate:
            return None
        if skip_gemini:
            return candidate
        if gemini_buy_check(candidate["symbol"], candidate["rsi"], candidate["bb_position"], analyses, telemetry=telemetry):
            return candidate
        log(f"  🤖 Gemini says RISK on {candidate['symbol']} — trying next-best candidate")
        attempted.add(candidate["symbol"])


# ─── State Management ────────────────────────────────────────────
def load_state() -> dict:
    """Load bot state from file."""
    if Path(STATE_FILE).exists():
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {
        "position": None,       # Current asset held (e.g., "BTC")
        "entry_price": 0,       # Price we entered at
        "entry_time": None,     # ISO timestamp
        "entry_reason": None,   # Why we entered the current position
        "quantity": 0,          # How much we hold
        "stop_loss": 0,         # Stop-loss price
        "highest_since_entry": 0,  # For trailing stop
        "highest_time": None,   # When we last printed a new high
        "trades": [],           # Trade history
        "total_pnl": 0,         # Running P&L
        "last_sell_time": None, # ISO timestamp — for YOLO idle tracking
        "cooldowns": {},        # Anti-churn history: {symbol: {time: iso, price: float}}
        "last_yolo_attempt": None,  # ISO timestamp — YOLO cooldown
    }


def save_state(state: dict):
    """Save bot state to file using fcntl locking."""
    Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(STATE_FILE, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
    try:
        f = os.fdopen(fd, "w")
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            json.dump(state, f, indent=2)
        finally:
            f.flush()
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    finally:
        f.close()


def default_control_state() -> dict:
    return {
        "paused": False,
        "pause_since": None,
        "pause_reason": None,
        "pending_action": None,
        "last_action": None,
    }


def load_control_state() -> dict:
    if Path(CONTROL_FILE).exists():
        try:
            with open(CONTROL_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return {**default_control_state(), **data}
        except Exception:
            pass
    return default_control_state()


def save_control_state(control_state: dict):
    Path(CONTROL_FILE).parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(CONTROL_FILE, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    try:
        f = os.fdopen(fd, "w")
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            json.dump(control_state, f, indent=2)
        finally:
            f.flush()
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    finally:
        f.close()


def get_or_create_control_token() -> str:
    token_path = Path(CONTROL_TOKEN_FILE)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    if token_path.exists():
        token = token_path.read_text().strip()
        if token:
            return token

    token = secrets.token_urlsafe(24)
    with open(token_path, "w") as f:
        f.write(token + "\n")
    os.chmod(token_path, 0o600)
    return token


def acquire_singleton_lock() -> bool:
    """Prevent multiple live bot processes from trading simultaneously."""
    global _bot_lock_handle

    if _bot_lock_handle is not None:
        return True

    Path(LOCK_FILE).parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(LOCK_FILE, os.O_CREAT | os.O_RDWR, 0o644)
    lock_handle = os.fdopen(fd, "r+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_handle.close()
        return False

    lock_handle.seek(0)
    lock_handle.truncate()
    lock_handle.write(
        json.dumps(
            {
                "pid": os.getpid(),
                "started_at": datetime.now(timezone.utc).isoformat(),
                "cmd": " ".join(sys.argv),
            },
            indent=2,
        )
    )
    lock_handle.flush()
    _bot_lock_handle = lock_handle
    return True


def release_singleton_lock():
    """Release the bot singleton lock on clean shutdown."""
    global _bot_lock_handle
    if _bot_lock_handle is None:
        return
    try:
        fcntl.flock(_bot_lock_handle.fileno(), fcntl.LOCK_UN)
    finally:
        _bot_lock_handle.close()
        _bot_lock_handle = None


# ─── Logging ─────────────────────────────────────────────────────
def log(msg: str):
    """Log message to file and stdout."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


class ControlRequestHandler(BaseHTTPRequestHandler):
    server_version = "DipSnifferControl/1.0"

    def log_message(self, fmt, *args):
        return

    def _set_headers(self, status_code=200):
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-DipSniffer-Token")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def _write_json(self, status_code, payload):
        self._set_headers(status_code)
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def _read_json_body(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length <= 0:
            return {}
        raw = self.rfile.read(content_length).decode("utf-8")
        return json.loads(raw) if raw else {}

    def _require_token(self):
        supplied = (self.headers.get("X-DipSniffer-Token") or "").strip()
        expected = get_or_create_control_token()
        return supplied and secrets.compare_digest(supplied, expected)

    def do_OPTIONS(self):
        self._set_headers(204)

    def do_GET(self):
        if self.path != "/control/status":
            self._write_json(404, {"ok": False, "error": "not_found"})
            return

        state = load_state()
        control = load_control_state()
        self._write_json(200, {
            "ok": True,
            "paused": control.get("paused", False),
            "pause_since": control.get("pause_since"),
            "pause_reason": control.get("pause_reason"),
            "pending_action": control.get("pending_action"),
            "last_action": control.get("last_action"),
            "position": state.get("position"),
            "control_port": CONTROL_API_PORT,
        })

    def do_POST(self):
        if not self._require_token():
            self._write_json(403, {"ok": False, "error": "invalid_token"})
            return

        try:
            payload = self._read_json_body()
        except Exception:
            self._write_json(400, {"ok": False, "error": "invalid_json"})
            return

        if self.path == "/control/pause":
            paused = bool(payload.get("paused"))
            reason = (payload.get("reason") or "dashboard").strip()
            control = load_control_state()
            control["paused"] = paused
            if paused:
                control["pause_since"] = datetime.now(timezone.utc).isoformat()
                control["pause_reason"] = reason
                log(f"⏸️ Manual pause engaged via control API ({reason})")
            else:
                log("▶️ Manual pause cleared via control API")
                control["pause_since"] = None
                control["pause_reason"] = None
            control["last_action"] = {
                "type": "pause_toggle",
                "paused": paused,
                "requested_at": datetime.now(timezone.utc).isoformat(),
                "source": reason,
            }
            save_control_state(control)
            _control_wake_event.set()
            self._write_json(200, {
                "ok": True,
                "paused": control["paused"],
                "pause_since": control["pause_since"],
                "pause_reason": control["pause_reason"],
            })
            return

        if self.path == "/control/force-eject":
            state = load_state()
            position = state.get("position")
            if not position:
                self._write_json(409, {"ok": False, "error": "no_open_position"})
                return

            expected_phrase = f"EJECT {position}"
            confirm_text = (payload.get("confirm_text") or "").strip().upper()
            if confirm_text != expected_phrase:
                self._write_json(400, {
                    "ok": False,
                    "error": "confirmation_mismatch",
                    "expected": expected_phrase,
                })
                return

            control = load_control_state()
            control["pending_action"] = {
                "type": "force_eject",
                "symbol": position,
                "requested_at": datetime.now(timezone.utc).isoformat(),
                "source": "dashboard",
            }
            control["last_action"] = {
                "type": "force_eject_requested",
                "symbol": position,
                "requested_at": control["pending_action"]["requested_at"],
                "source": "dashboard",
            }
            save_control_state(control)
            log(f"🚨 Manual force-eject requested for {position} via control API")
            _control_wake_event.set()
            self._write_json(202, {
                "ok": True,
                "queued": True,
                "symbol": position,
                "paused": control.get("paused", False),
            })
            return

        self._write_json(404, {"ok": False, "error": "not_found"})


def start_control_server():
    global _control_server, _control_server_thread

    if _control_server is not None:
        return True

    get_or_create_control_token()

    try:
        server = ThreadingHTTPServer((CONTROL_API_HOST, CONTROL_API_PORT), ControlRequestHandler)
    except OSError as e:
        log(f"⚠️ Control API failed to start on {CONTROL_API_HOST}:{CONTROL_API_PORT}: {e}")
        return False

    thread = threading.Thread(target=server.serve_forever, daemon=True, name="dipsniffer-control-api")
    thread.start()
    _control_server = server
    _control_server_thread = thread
    log(f"🛟 Control API listening on {CONTROL_API_HOST}:{CONTROL_API_PORT}")
    log(f"🔐 Control token file: {CONTROL_TOKEN_FILE}")
    return True


def stop_control_server():
    global _control_server, _control_server_thread
    if _control_server is None:
        return
    try:
        _control_server.shutdown()
        _control_server.server_close()
    finally:
        _control_server = None
        _control_server_thread = None


# ─── Configuration Loader ──────────────────────────────────────────
def load_strategy_config():
    """Load optimized parameters from JSON, falling back to built-in defaults."""
    global RSI_PERIOD, RSI_OVERSOLD, RSI_OVERBOUGHT, BB_PERIOD, BB_STD_DEV
    global ATR_PERIOD, ATR_MULT_WIDE, ATR_MULT_TIGHT, ATR_TIGHTEN_GAIN, STOP_LOSS_FALLBACK, BB_MIN_WIDTH_PCT, SQUEEZE_BB_POS_MAX
    global ASSUMED_ROUND_TRIP_COST_PCT, MIN_NET_PROFIT_BUFFER_PCT
    global VOLUME_SPIKE_MULT, VOLUME_LOOKBACK, YOLO_FEAR_THRESHOLD
    global YOLO_IDLE_HOURS, YOLO_COOLDOWN_HOURS, SQUEEZE_LOOKBACK, SQUEEZE_EXPAND_CANDLES
    global BAND_WALK_MIN, DAILY_RSI_KNIFE, FUNDING_RATE_NEGATIVE_THRESHOLD, FUNDING_RATE_SCORE_BONUS
    global REL_STRENGTH_24H_MULT, REL_STRENGTH_72H_MULT, REL_STRENGTH_SCORE_CAP
    global STALE_EJECT_MIN_HOURS, STALE_EJECT_MAX_PNL_PCT, STALE_EJECT_MIN_HOURS_SINCE_HIGH, STALE_EJECT_MIN_STRENGTH_GAP, MIN_STALE_ROTATION_STRENGTH_GAP, STALE_EJECT_MIN_TARGET_STRENGTH
    global MOMENTUM_RSI_MIN, MOMENTUM_RSI_MAX, MOMENTUM_BB_POS_MIN, MOMENTUM_SLOPE_MIN, MOMENTUM_GREEN_CANDLES, MOMENTUM_VOL_MIN_RATIO

    config_path = os.path.expanduser("~/.config/kraken/strategy_config.json")
    
    defaults = {
        "RSI_PERIOD": RSI_PERIOD,
        "RSI_OVERSOLD": RSI_OVERSOLD,
        "RSI_OVERBOUGHT": RSI_OVERBOUGHT,
        "BB_PERIOD": BB_PERIOD,
        "BB_STD_DEV": BB_STD_DEV,
        "ATR_PERIOD": ATR_PERIOD,
        "ATR_MULT_WIDE": ATR_MULT_WIDE,
        "ATR_MULT_TIGHT": ATR_MULT_TIGHT,
        "ATR_TIGHTEN_GAIN": ATR_TIGHTEN_GAIN,
        "STOP_LOSS_FALLBACK": STOP_LOSS_FALLBACK,
        "BB_MIN_WIDTH_PCT": BB_MIN_WIDTH_PCT,
        "SQUEEZE_BB_POS_MAX": SQUEEZE_BB_POS_MAX,
        "ASSUMED_ROUND_TRIP_COST_PCT": ASSUMED_ROUND_TRIP_COST_PCT,
        "MIN_NET_PROFIT_BUFFER_PCT": MIN_NET_PROFIT_BUFFER_PCT,
        "VOLUME_SPIKE_MULT": VOLUME_SPIKE_MULT,
        "VOLUME_LOOKBACK": VOLUME_LOOKBACK,
        "YOLO_FEAR_THRESHOLD": YOLO_FEAR_THRESHOLD,
        "YOLO_IDLE_HOURS": YOLO_IDLE_HOURS,
        "YOLO_COOLDOWN_HOURS": YOLO_COOLDOWN_HOURS,
        "SQUEEZE_LOOKBACK": SQUEEZE_LOOKBACK,
        "SQUEEZE_EXPAND_CANDLES": SQUEEZE_EXPAND_CANDLES,
        "BAND_WALK_MIN": BAND_WALK_MIN,
        "DAILY_RSI_KNIFE": DAILY_RSI_KNIFE,
        "FUNDING_RATE_NEGATIVE_THRESHOLD": FUNDING_RATE_NEGATIVE_THRESHOLD,
        "FUNDING_RATE_SCORE_BONUS": FUNDING_RATE_SCORE_BONUS,
        "REL_STRENGTH_24H_MULT": REL_STRENGTH_24H_MULT,
        "REL_STRENGTH_72H_MULT": REL_STRENGTH_72H_MULT,
        "REL_STRENGTH_SCORE_CAP": REL_STRENGTH_SCORE_CAP,
        "STALE_EJECT_MIN_HOURS": STALE_EJECT_MIN_HOURS,
        "STALE_EJECT_MAX_PNL_PCT": STALE_EJECT_MAX_PNL_PCT,
        "STALE_EJECT_MIN_HOURS_SINCE_HIGH": STALE_EJECT_MIN_HOURS_SINCE_HIGH,
        "STALE_EJECT_MIN_STRENGTH_GAP": STALE_EJECT_MIN_STRENGTH_GAP,
        "MIN_STALE_ROTATION_STRENGTH_GAP": MIN_STALE_ROTATION_STRENGTH_GAP,
        "STALE_EJECT_MIN_TARGET_STRENGTH": STALE_EJECT_MIN_TARGET_STRENGTH,
        "MOMENTUM_RSI_MIN": MOMENTUM_RSI_MIN,
        "MOMENTUM_RSI_MAX": MOMENTUM_RSI_MAX,
        "MOMENTUM_BB_POS_MIN": MOMENTUM_BB_POS_MIN,
        "MOMENTUM_SLOPE_MIN": MOMENTUM_SLOPE_MIN,
        "MOMENTUM_GREEN_CANDLES": MOMENTUM_GREEN_CANDLES,
        "MOMENTUM_VOL_MIN_RATIO": MOMENTUM_VOL_MIN_RATIO
    }
    
    if not os.path.exists(config_path):
        log(f"No strategy_config.json found. Creating default at {config_path}")
        try:
            Path(config_path).parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, "w") as f:
                json.dump(defaults, f, indent=4)
        except Exception as e:
            log(f"⚠️ ERROR: Failed to write default strategy_config.json: {e}")
        return
        
    try:
        with open(config_path, "r") as f:
            cfg = json.load(f)
    except Exception as e:
        log(f"⚠️ ERROR: Failed to parse strategy_config.json: {e}. Falling back to defaults.")
        return
        
    def _parse(key, ctype, min_val=None, max_val=None):
        if key not in cfg:
            return defaults[key]
        try:
            val = ctype(cfg[key])
            if min_val is not None and val < min_val:
                raise ValueError(f"Value {val} < minimum {min_val}")
            if max_val is not None and val > max_val:
                raise ValueError(f"Value {val} > maximum {max_val}")
            return val
        except Exception as e:
            log(f"⚠️ ERROR: Invalid config value for {key}: {cfg.get(key)} -> {e}. Using default {defaults[key]}")
            return defaults[key]
            
    tmp_oversold = _parse("RSI_OVERSOLD", int, 0, 100)
    tmp_overbought = _parse("RSI_OVERBOUGHT", int, 0, 100)
    if tmp_oversold >= tmp_overbought:
        log(f"⚠️ ERROR: RSI_OVERSOLD ({tmp_oversold}) >= RSI_OVERBOUGHT ({tmp_overbought}). Using defaults.")
        tmp_oversold = defaults["RSI_OVERSOLD"]
        tmp_overbought = defaults["RSI_OVERBOUGHT"]
        
    RSI_OVERSOLD = tmp_oversold
    RSI_OVERBOUGHT = tmp_overbought
    RSI_PERIOD = _parse("RSI_PERIOD", int, 1)
    BB_PERIOD = _parse("BB_PERIOD", int, 1)
    BB_STD_DEV = _parse("BB_STD_DEV", float, 0.1)
    ATR_PERIOD = _parse("ATR_PERIOD", int, 1)
    ATR_MULT_WIDE = _parse("ATR_MULT_WIDE", float, 0.1)
    ATR_MULT_TIGHT = _parse("ATR_MULT_TIGHT", float, 0.1)
    ATR_TIGHTEN_GAIN = _parse("ATR_TIGHTEN_GAIN", float, 0.0)
    STOP_LOSS_FALLBACK = _parse("STOP_LOSS_FALLBACK", float, 0.01)
    BB_MIN_WIDTH_PCT = _parse("BB_MIN_WIDTH_PCT", float, 0.0)
    SQUEEZE_BB_POS_MAX = _parse("SQUEEZE_BB_POS_MAX", float, 0.0, 1.0)
    ASSUMED_ROUND_TRIP_COST_PCT = _parse("ASSUMED_ROUND_TRIP_COST_PCT", float, 0.0, 10.0)
    MIN_NET_PROFIT_BUFFER_PCT = _parse("MIN_NET_PROFIT_BUFFER_PCT", float, 0.0, 10.0)
    VOLUME_SPIKE_MULT = _parse("VOLUME_SPIKE_MULT", float, 0.0)
    VOLUME_LOOKBACK = _parse("VOLUME_LOOKBACK", int, 1)
    YOLO_FEAR_THRESHOLD = _parse("YOLO_FEAR_THRESHOLD", int, 0, 100)
    YOLO_IDLE_HOURS = _parse("YOLO_IDLE_HOURS", float, 0.0)
    YOLO_COOLDOWN_HOURS = _parse("YOLO_COOLDOWN_HOURS", float, 0.0)
    SQUEEZE_LOOKBACK = _parse("SQUEEZE_LOOKBACK", int, 1)
    SQUEEZE_EXPAND_CANDLES = _parse("SQUEEZE_EXPAND_CANDLES", int, 1)
    BAND_WALK_MIN = _parse("BAND_WALK_MIN", int, 1)
    DAILY_RSI_KNIFE = _parse("DAILY_RSI_KNIFE", int, 0, 100)
    FUNDING_RATE_NEGATIVE_THRESHOLD = _parse("FUNDING_RATE_NEGATIVE_THRESHOLD", float)
    FUNDING_RATE_SCORE_BONUS = _parse("FUNDING_RATE_SCORE_BONUS", float, 0.0)
    REL_STRENGTH_24H_MULT = _parse("REL_STRENGTH_24H_MULT", float, 0.0)
    REL_STRENGTH_72H_MULT = _parse("REL_STRENGTH_72H_MULT", float, 0.0)
    REL_STRENGTH_SCORE_CAP = _parse("REL_STRENGTH_SCORE_CAP", float, 0.0)
    STALE_EJECT_MIN_HOURS = _parse("STALE_EJECT_MIN_HOURS", float, 0.0)
    STALE_EJECT_MAX_PNL_PCT = _parse("STALE_EJECT_MAX_PNL_PCT", float)
    STALE_EJECT_MIN_HOURS_SINCE_HIGH = _parse("STALE_EJECT_MIN_HOURS_SINCE_HIGH", float, 0.0)
    STALE_EJECT_MIN_STRENGTH_GAP = _parse("STALE_EJECT_MIN_STRENGTH_GAP", float, 0.0)
    MIN_STALE_ROTATION_STRENGTH_GAP = _parse("MIN_STALE_ROTATION_STRENGTH_GAP", float, 0.0)
    STALE_EJECT_MIN_TARGET_STRENGTH = _parse("STALE_EJECT_MIN_TARGET_STRENGTH", float, 0.0)
    MOMENTUM_RSI_MIN = _parse("MOMENTUM_RSI_MIN", float, 0.0, 100.0)
    MOMENTUM_RSI_MAX = _parse("MOMENTUM_RSI_MAX", float, 0.0, 100.0)
    MOMENTUM_BB_POS_MIN = _parse("MOMENTUM_BB_POS_MIN", float, 0.0, 1.0)
    MOMENTUM_SLOPE_MIN = _parse("MOMENTUM_SLOPE_MIN", float)
    MOMENTUM_GREEN_CANDLES = _parse("MOMENTUM_GREEN_CANDLES", int, 1, 20)
    MOMENTUM_VOL_MIN_RATIO = _parse("MOMENTUM_VOL_MIN_RATIO", float, 0.0)
    log(f"✅ Strategy parameters safely loaded from config.")
    log(
        f"🪙 Fee-aware gates: round-trip {ASSUMED_ROUND_TRIP_COST_PCT:.2f}% + "
        f"buffer {MIN_NET_PROFIT_BUFFER_PCT:.2f}% = "
        f"{required_profit_take_pct():.2f}% min discretionary profit"
    )


def ntfy_endpoint():
    if NTFY_URL:
        return NTFY_URL
    if NTFY_TOPIC:
        return f"{NTFY_BASE_URL}/{quote(NTFY_TOPIC, safe='')}"
    return None


def classify_trade_notification(event: str, reason: str = ""):
    reason_lower = (reason or "").lower()
    if event == "BUY":
        return {
            "priority": "default",
            "tags": ["green_circle", "shopping_cart", "chart_with_upwards_trend"],
        }
    if "manual force eject" in reason_lower or "force-eject" in reason_lower:
        return {
            "priority": "high",
            "tags": ["rotating_light", "door", "money_with_wings"],
        }
    if "stop-loss" in reason_lower:
        return {
            "priority": "high",
            "tags": ["warning", "octagonal_sign", "chart_with_downwards_trend"],
        }
    return {
        "priority": "default",
        "tags": ["white_check_mark", "money_with_wings", "chart_with_upwards_trend"],
    }


def format_hold_duration(entry_time_iso: str | None) -> str | None:
    if not entry_time_iso:
        return None
    try:
        started = datetime.fromisoformat(entry_time_iso)
        elapsed = datetime.now(timezone.utc) - started
        total_minutes = max(0, int(elapsed.total_seconds() // 60))
        hours, minutes = divmod(total_minutes, 60)
        if hours:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"
    except Exception:
        return None


def send_trade_notification(
    title: str,
    message: str,
    priority: str = "default",
    tags: list[str] | None = None,
    click: str | None = None,
) -> None:
    endpoint = ntfy_endpoint()
    if not endpoint:
        return

    data = message.encode("utf-8")
    headers = {
        "Title": title,
        "Priority": priority,
        "Content-Type": "text/plain; charset=utf-8",
    }
    if tags:
        headers["Tags"] = ",".join(tags)
    if click:
        headers["Click"] = click
    request = Request(
        endpoint,
        data=data,
        method="POST",
        headers=headers,
    )
    try:
        with urlopen(request, timeout=5) as response:
            response.read()
    except Exception as exc:
        log(f"  ⚠️ ntfy notification failed: {exc}")

# Run configuration loader 
load_strategy_config()
if ntfy_endpoint():
    log(f"🔔 ntfy trade notifications enabled: {ntfy_endpoint()}")

# ─── Trade Execution ─────────────────────────────────────────────
def execute_buy(state: dict, analysis: dict, usd_available: float, entry_reason: str = "", telemetry: dict = None) -> dict:
    """Execute a buy: sell current position if any, buy the new asset."""
    symbol = analysis["symbol"]
    price = analysis["price"]
    ccxt_symbol = PAIRS[symbol]["symbol"]

    # Calculate quantity to buy (leave small buffer for fees)
    buy_amount = usd_available * 0.995  # 0.5% buffer for fees
    if buy_amount < MIN_TRADE_USD:
        log(f"  Skipping buy: ${buy_amount:.2f} below minimum ${MIN_TRADE_USD}")
        return state

    quantity = truncate_amount(ccxt_symbol, buy_amount / price)
    if quantity <= 0:
        log(f"  Skipping buy: computed quantity for {symbol} is below Kraken minimum precision")
        return state

    # Place market buy
    result = place_order("buy", ccxt_symbol, quantity)
    if result is None:
        log(f"  BUY FAILED for {symbol}")
        return state

    atr = analysis.get("atr")
    stop_loss = calc_dynamic_stop(price, price, atr)
    state["position"] = symbol
    state["entry_price"] = price
    state["entry_time"] = datetime.now(timezone.utc).isoformat()
    state["quantity"] = quantity
    state["stop_loss"] = round_price(stop_loss)
    state["highest_since_entry"] = price
    state["highest_time"] = state["entry_time"]
    reason = entry_reason or "signal"
    state["entry_reason"] = reason
    
    position_id = f"pos_{int(time.time() * 1000000)}_{symbol}"
    state["position_id"] = position_id

    log(f"  ✅ BOUGHT {quantity} {symbol} @ ${price:.2f} (${buy_amount:.2f})")
    log(f"     Stop-loss: ${stop_loss:.2f} | RSI: {analysis['rsi']} | BB pos: {analysis['bb_position']}")
    notification_meta = classify_trade_notification("BUY", reason)
    send_trade_notification(
        f"DipSniffer BUY {symbol}",
        (
            f"Bought {quantity} {symbol} @ ${price:.4f}\n"
            f"Capital deployed: ${buy_amount:.2f}\n"
            f"Entry reason: {reason}\n"
            f"Stop-loss: ${stop_loss:.4f}\n"
            f"RSI / BB: {analysis['rsi']} / {analysis['bb_position']:.2f}\n"
            f"Strength: {analysis.get('strength', 0):.2f}"
        ),
        priority=notification_meta["priority"],
        tags=notification_meta["tags"],
        click=NTFY_CLICK_URL or None,
    )

    state["trades"].append({
        "action": "BUY",
        "symbol": symbol,
        "price": price,
        "quantity": quantity,
        "time": state["entry_time"],
        "rsi": analysis["rsi"],
        "reason": reason,
    })

    if telemetry is not None:
        telemetry["executed_trades"].append({
            "action": "ENTRY",
            "position_id": position_id,
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "time": state["entry_time"],
            "signal_family": (
                "squeeze" if entry_reason == "squeeze_buy"
                else ("momentum" if entry_reason == "momentum_buy"
                      else ("volatility" if "yolo" in entry_reason else "rsi_bb"))
            ),
            "entry_reason": entry_reason,
            "decision_context": f"bb_pos={analysis.get('bb_position')} rsi={analysis.get('rsi')}",
            "strength_score": analysis.get("strength"),
        })

    return state


def execute_sell(state: dict, reason: str, current_price: float, telemetry: dict = None) -> dict:
    """Execute a sell of current position."""
    symbol = state["position"]
    ccxt_symbol = PAIRS[symbol]["symbol"]
    quantity = state["quantity"]

    # Use actual balance (may differ from state due to trading fees)
    balances = get_balance() or {}  # Fail-safe: if auth fails, use state qty
    actual_qty = balances.get(symbol, 0)
    if actual_qty > 0:
        quantity = truncate_amount(ccxt_symbol, actual_qty)

    if quantity <= 0:
        log(f"  SELL FAILED for {symbol}: no spendable balance available")
        return state

    # Place market sell
    result = place_order("sell", ccxt_symbol, quantity)
    if result is None:
        log(f"  SELL FAILED for {symbol}")
        return state

    hold_duration = format_hold_duration(state.get("entry_time"))
    pnl = (current_price - state["entry_price"]) * quantity
    pnl_pct = (current_price / state["entry_price"] - 1) * 100
    state["total_pnl"] += pnl

    log(f"  ✅ SOLD {quantity} {symbol} @ ${current_price:.2f} ({reason})")
    log(f"     P&L: ${pnl:+.2f} ({pnl_pct:+.1f}%) | Total P&L: ${state['total_pnl']:+.2f}")
    notification_meta = classify_trade_notification("SELL", reason)
    hold_line = f"Hold time: {hold_duration}\n" if hold_duration else ""
    send_trade_notification(
        f"DipSniffer SELL {symbol}",
        (
            f"Sold {quantity} {symbol} @ ${current_price:.4f}\n"
            f"Reason: {reason}\n"
            f"{hold_line}"
            f"P&L: ${pnl:+.2f} ({pnl_pct:+.2f}%)\n"
            f"Total P&L: ${state['total_pnl']:+.2f}"
        ),
        priority=notification_meta["priority"],
        tags=notification_meta["tags"],
        click=NTFY_CLICK_URL or None,
    )

    state["trades"].append({
        "action": "SELL",
        "symbol": symbol,
        "price": current_price,
        "quantity": quantity,
        "time": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl_pct, 2),
    })

    if telemetry is not None and state.get("position_id"):
        telemetry["executed_trades"].append({
            "action": "EXIT",
            "position_id": state["position_id"],
            "symbol": symbol,
            "price": current_price,
            "quantity": quantity,
            "time": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
        })

    state["position"] = None
    state["position_id"] = None
    state["entry_price"] = 0
    state["entry_time"] = None
    state["quantity"] = 0
    state["stop_loss"] = 0
    state["highest_since_entry"] = 0
    state["highest_time"] = None
    state.pop("entry_reason", None)
    state["last_sell_time"] = datetime.now(timezone.utc).isoformat()
    if "cooldowns" not in state:
        state["cooldowns"] = {}
    state["cooldowns"][symbol] = {
        "time": state["last_sell_time"],
        "price": current_price
    }

    return state


# ─── Main Logic ──────────────────────────────────────────────────
def run_cycle(dry_run: bool = False, status_only: bool = False) -> dict:
    """Run one analysis + trade cycle."""
    log("═" * 50)
    log("Swing Bot cycle starting")
    
    # Hot-reload strategy configuration discovered by DeepSeek Auto Quant
    load_strategy_config()

    telemetry = {
        "cycle": {
            "cycle_id": f"cyc_{int(time.time()*1000)}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": "status" if status_only else ("dry_run" if dry_run else "live"),
            "fear_greed_index": get_fear_greed(),
            "btc_crash_guard_active": False,
            "paused": False,
        },
        "all_analyses": {},
        "decision_events": [],
        "executed_trades": [],
    }

    state = load_state()
    control_state = load_control_state()
    telemetry["cycle"]["paused"] = control_state.get("paused", False)

    # Analyze all assets
    analyses = {}
    for symbol in PAIRS:
        held_squeeze = (state.get("position") == symbol and state.get("entry_reason") == "squeeze_buy")
        analysis = analyze_asset(symbol, is_held_squeeze=held_squeeze)
        if analysis:
            analyses[symbol] = analysis

    if not analyses:
        log("  No data available for any asset")
        return telemetry

    apply_relative_strength_overlay(analyses)
    telemetry["all_analyses"] = analyses

    for symbol in PAIRS:
        analysis = analyses.get(symbol)
        if not analysis:
            continue
        flag = ""
        if analysis["buy_signal"]:
            flag = " 🟢 BUY SIGNAL"
        elif analysis["sell_signal"]:
            flag = " 🔴 SELL SIGNAL"
        vol_tag = f" | vol: {analysis['vol_ratio']}×" if analysis.get('vol_ratio', 0) > 0 else ""
        div_tag = " ⬆DIV" if analysis.get('rsi_divergence') else ""
        sq_tag = " 💥SQZ" if analysis.get('bb_squeeze_breakout') else (" 🟠sqz" if analysis.get('bb_squeezing') else "")
        walk_tag = f" 🔥WALK({analysis['band_walk_count']})" if analysis.get('band_walking') else ""
        funding_tag = ""
        if analysis.get("funding_rate") is not None:
            funding_tag = f" | 💰FR: {analysis['funding_rate']:+.6f}"
            if analysis.get("funding_squeezed"):
                funding_tag += " 🔥"
        rel_tag = ""
        if analysis.get("rel_strength_score", 0) > 0:
            rel_tag = f" | ⚡RS:+{analysis['rel_strength_score']:.1f}"
        log(f"  {symbol}: ${analysis['price']:.2f} | RSI: {analysis['rsi']} | "
            f"BB: [{analysis['bb_lower']:.2f} - {analysis['bb_upper']:.2f}] "
            f"(pos: {analysis['bb_position']:.2f}){vol_tag}{div_tag}{sq_tag}{walk_tag}{funding_tag}{rel_tag}{flag}")

    pending_action = control_state.get("pending_action")
    if pending_action and pending_action.get("type") == "force_eject":
        target_symbol = pending_action.get("symbol")
        if not state.get("position"):
            log("  🛟 Manual force-eject request found, but no open position remains")
            control_state["pending_action"] = None
            control_state["last_action"] = {
                "type": "force_eject",
                "status": "no_position",
                "requested_at": pending_action.get("requested_at"),
                "handled_at": datetime.now(timezone.utc).isoformat(),
                "source": pending_action.get("source", "dashboard"),
            }
            save_control_state(control_state)
        elif state["position"] != target_symbol:
            log(
                f"  🛟 Manual force-eject target mismatch ({target_symbol}) vs current hold "
                f"({state['position']}) — ejecting current hold instead"
            )
        if state.get("position"):
            current_symbol = state["position"]
            current_analysis = analyses.get(current_symbol)
            if current_analysis is None:
                log(f"  ⚠️ Manual force-eject queued for {current_symbol}, but no market data is available yet")
            else:
                log(f"  🚨 Executing manual force-eject for {current_symbol}")
                state = execute_sell(
                    state,
                    "MANUAL FORCE EJECT (dashboard override)",
                    current_analysis["price"],
                    telemetry=telemetry,
                )
                control_state["pending_action"] = None
                control_state["last_action"] = {
                    "type": "force_eject",
                    "status": "executed",
                    "symbol": current_symbol,
                    "requested_at": pending_action.get("requested_at"),
                    "handled_at": datetime.now(timezone.utc).isoformat(),
                    "source": pending_action.get("source", "dashboard"),
                }
                save_control_state(control_state)
                save_state(state)
                write_dashboard_status(state, analyses)
                log("  🛟 Manual force-eject complete — skipping automatic re-entry until next cycle")
                log("Cycle complete")
                return telemetry

    if control_state.get("paused"):
        pause_since = control_state.get("pause_since")
        pause_reason = control_state.get("pause_reason") or "manual override"
        if state.get("position"):
            log(
                f"  ⏸️ BOT PAUSED since {pause_since or 'unknown'} ({pause_reason}) — "
                f"monitoring {state['position']} only, no automated buys or sells"
            )
        else:
            log(
                f"  ⏸️ BOT PAUSED since {pause_since or 'unknown'} ({pause_reason}) — "
                f"cash mode frozen, no automated buys"
            )
        save_state(state)
        write_dashboard_status(state, analyses)
        log("Cycle complete")
        return telemetry

    # ─── Currently holding a position ───
    if state["position"]:
        symbol = state["position"]

        # Ghost position check: verify we actually hold the asset on Kraken
        # CRITICAL: Only clear position if balance fetch SUCCEEDED and returned 0.
        # If fetch_balance() returned None (auth/network error), we must NOT
        # clear the position — we simply can't verify it right now.
        balances = get_balance()
        if balances is None:
            log(f"  ⚠️ Balance fetch failed — skipping ghost position check for {symbol}")
            log(f"  ⚠️ Cannot verify holdings or manage stop-loss this cycle")
            return telemetry  # Skip entire position management this cycle
        actual_balance = balances.get(symbol, 0)
        if actual_balance < 0.0001:
            log(f"  ⚠️ Ghost position detected: state says holding {symbol} "
                f"but Kraken balance is {actual_balance:.6f}")
            log(f"  Resetting to cash mode (manual sell detected?)")
            state["position"] = None
            state["entry_price"] = 0
            state["quantity"] = 0
            state["stop_loss"] = 0
            state["highest_since_entry"] = 0
            state["highest_time"] = None
            state["entry_time"] = None
            save_state(state)
            # Fall through to the "not holding" block below
        elif symbol not in analyses:
            log(f"  WARNING: No data for current position {symbol}")
            save_state(state)
            return telemetry

        else:
            analysis = analyses[symbol]
            price = analysis["price"]
            gemini_hold_override = False

            # Update trailing stop (highest price since entry)
            if price > state["highest_since_entry"]:
                state["highest_since_entry"] = price
                state["highest_time"] = datetime.now(timezone.utc).isoformat()
                atr = analysis.get("atr")
                new_stop = round_price(calc_dynamic_stop(price, state["entry_price"], atr))
                state["stop_loss"] = max(state["stop_loss"], new_stop)

            hold_pnl = (price - state["entry_price"]) * state["quantity"]
            hold_pct = (price / state["entry_price"] - 1) * 100
            log(f"  HOLDING: {state['quantity']} {symbol} @ ${state['entry_price']:.2f} → "
                f"${price:.2f} ({hold_pct:+.1f}%, ${hold_pnl:+.2f})")
            log(f"  Stop-loss: ${state['stop_loss']:.2f} | Trailing high: ${state['highest_since_entry']:.2f}")

            # Check exit signals
            should_sell = False
            reason = ""
            rotation_target = None

            if price <= state["stop_loss"]:
                should_sell = True
                reason = f"STOP-LOSS (${state['stop_loss']:.2f})"
            elif analysis["sell_signal"]:
                # Ignore pure RSI sell alarms if we bought a massive momentum setup.
                # Force the trade to run until the trailing stop catches it or it pierces the upper band.
                is_momentum_trade = state.get("entry_reason") in ("squeeze_buy", "momentum_buy")
                is_pure_rsi_alarm = analysis["rsi"] > RSI_OVERBOUGHT and not analysis.get("bb_exit", False)
                min_profit_take_pct = required_profit_take_pct()
                clears_profit_gate = hold_pct >= min_profit_take_pct

                if is_pure_rsi_alarm and not clears_profit_gate:
                    if is_momentum_trade:
                        log(
                            f"  🟢 Momentum Bypass: Ignoring RSI={analysis['rsi']:.1f} exit alarm. "
                            f"Profit {hold_pct:+.2f}% has not cleared fee-aware gate "
                            f"({min_profit_take_pct:.2f}%). Trusting Trailing Stop (${state['stop_loss']:.2f})"
                        )
                    else:
                        log(
                            f"  🪙 Fee Gate: Ignoring discretionary RSI exit at {hold_pct:+.2f}% "
                            f"(need at least {min_profit_take_pct:.2f}% to clear assumed costs)"
                        )
                else:
                    # Layer 3: Ask Gemini if we should hold longer (skip in status mode)
                    if status_only:
                        log(f"  🔴 Sell signal: RSI={analysis['rsi']}/BB={analysis['bb_position']:.2f} (Gemini skipped in status mode)")
                    elif gemini_sell_check(symbol, analysis["rsi"], state["entry_price"], price,
                                           analysis["bb_position"], analysis.get("band_walk_count", 0), telemetry=telemetry):
                        should_sell = True
                        reason = f"RSI={analysis['rsi']}/BB={analysis['bb_position']:.2f} (Gemini: SELL)"
                    else:
                        gemini_hold_override = True
                        log(f"  🤖 Gemini says HOLD — bullish catalyst detected, keeping position")
                        log(f"  Trailing stop still active at ${state['stop_loss']:.2f}")

            if not should_sell and not gemini_hold_override and not status_only:
                now = datetime.now(timezone.utc)
                held_hours = hours_since(state.get("entry_time"), now)
                hours_since_high = hours_since(state.get("highest_time") or state.get("entry_time"), now)
                stale_enough = (
                    held_hours is not None and held_hours >= STALE_EJECT_MIN_HOURS and
                    hold_pct <= STALE_EJECT_MAX_PNL_PCT and
                    hours_since_high is not None and hours_since_high >= STALE_EJECT_MIN_HOURS_SINCE_HIGH
                )
                if stale_enough:
                    log(f"  💤 STALE CHECK: held {held_hours:.1f}h, P&L {hold_pct:+.1f}%, no new high for {hours_since_high:.1f}h")
                    candidate = select_best_entry_candidate(analyses, excluded_symbols={symbol}, telemetry=telemetry)
                    if candidate:
                        strength_gap = candidate["strength"] - analysis.get("strength", 0)
                        required_strength_gap = max(STALE_EJECT_MIN_STRENGTH_GAP, MIN_STALE_ROTATION_STRENGTH_GAP)
                        if (candidate["strength"] >= STALE_EJECT_MIN_TARGET_STRENGTH and
                                strength_gap >= required_strength_gap):
                            if gemini_buy_check(candidate["symbol"], candidate["rsi"],
                                                candidate["bb_position"], analyses, telemetry=telemetry):
                                should_sell = True
                                reason = (f"STALE EJECT → {candidate['symbol']} "
                                          f"(held {held_hours:.1f}h, {hold_pct:+.1f}%, "
                                          f"gap +{strength_gap:.1f})")
                                rotation_target = candidate
                                log(f"  🚀 STALE EJECT: rotating from {symbol} into stronger setup {candidate['symbol']}")
                            else:
                                log(f"  🤖 Gemini says RISK on stale rotation target {candidate['symbol']} — keeping current position")
                        else:
                            log(
                                f"  💤 Stale but no elite replacement — best alt {candidate['symbol']} "
                                f"gap +{strength_gap:.1f} (need +{required_strength_gap:.1f})"
                            )

            if should_sell:
                if dry_run or status_only:
                    log(f"  [DRY RUN] Would SELL {symbol}: {reason}")
                    if rotation_target:
                        log(f"  [DRY RUN] Would ROTATE into {rotation_target['symbol']} after stale eject")
                else:
                    state = execute_sell(state, reason, price, telemetry=telemetry)
                    if rotation_target and state.get("position") is None:
                        balances = get_balance() or {}
                        usd = balances.get("USD", 0)
                        if usd >= MIN_TRADE_USD:
                            if rotation_target.get("momentum_buy") and not rotation_target.get("buy_signal") and not rotation_target.get("squeeze_buy"):
                                rotate_reason = "momentum_buy"
                            elif rotation_target.get("squeeze_buy") and not rotation_target.get("buy_signal"):
                                rotate_reason = "squeeze_buy"
                            else:
                                rotate_reason = "stale_rotation"
                            state = execute_buy(state, rotation_target, usd, entry_reason=rotate_reason, telemetry=telemetry)
                        else:
                            log(f"  Stale eject completed but insufficient USD to rotate: ${usd:.2f}")
            else:
                log(f"  No exit signal — holding")

    # ─── Not holding — look for entries ───
    else:
        # Get available USD balance
        balances = get_balance()
        if balances is None:
            log(f"  ⚠️ Balance fetch failed — cannot evaluate entries this cycle")
            save_state(state)
            write_dashboard_status(state, analyses)
            log("Cycle complete")
            return
        usd = balances.get("USD", 0)

        # Also check if we're holding any crypto we can sell
        crypto_value = 0
        for sym, info in PAIRS.items():
            bal = balances.get(sym, 0)
            if bal > 0 and sym in analyses:
                crypto_value += bal * analyses[sym]["price"]

        total_available = usd + crypto_value
        log(f"  Available: ${usd:.2f} USD + ${crypto_value:.2f} in crypto = ${total_available:.2f} total")

        # Anti-churn cooldown (2.0 hours or 2% drop)
        excluded_symbols = set()
        cooldowns = state.get("cooldowns", {})
        
        for sym, data in cooldowns.items():
            if sym in analyses:
                hrs_since = hours_since(data["time"])
                if hrs_since is not None and hrs_since < 2.0:
                    current_price = analyses[sym]["price"]
                    if data["price"] > 0 and current_price <= data["price"] * 0.98:
                        log(f"  📉 Anti-churn bypass: {sym} dropped >2% from sell price (${data['price']:.2f} -> ${current_price:.2f})")
                    else:
                        log(f"  ⏳ {sym} is on anti-churn cooldown for {2.0 - hrs_since:.2f} more hours")
                        excluded_symbols.add(sym)

        best = select_best_gemini_approved_candidate(
            analyses,
            excluded_symbols=excluded_symbols,
            skip_gemini=(dry_run or status_only),
            telemetry=telemetry
        )

        if best:
            if dry_run or status_only:
                log(f"  [DRY RUN] Would BUY {best['symbol']} with ${total_available:.2f}")
            else:
                # If holding crypto, sell it first to consolidate into USD
                for sym, info in PAIRS.items():
                    bal = balances.get(sym, 0)
                    if bal > 0.0001 and sym != best["symbol"]:
                        qty = truncate_amount(info["symbol"], bal)
                        if qty * analyses.get(sym, {}).get("price", 0) >= MIN_TRADE_USD:
                            if sym == state.get("position"):
                                log(f"  Rotating tracked position {sym} -> {best['symbol']}...")
                                state = execute_sell(state, f"Auto-rotate to stronger setup: {best['symbol']}", analyses[sym]["price"], telemetry=telemetry)
                            else:
                                log(f"  Selling untracked {qty} {sym} to consolidate...")
                                place_order("sell", info["symbol"], qty)
                            time.sleep(2)

                # Re-check balance after sells
                balances = get_balance() or {}
                usd = balances.get("USD", 0)

                # Also use any existing balance of the target asset
                existing = balances.get(best["symbol"], 0)

                if usd >= MIN_TRADE_USD:
                    if best.get("momentum_buy") and not best.get("buy_signal") and not best.get("squeeze_buy"):
                        reason = "momentum_buy"
                    elif best.get("squeeze_buy") and not best.get("buy_signal"):
                        reason = "squeeze_buy"
                    else:
                        reason = "buy_signal"
                    state = execute_buy(state, best, usd, entry_reason=reason, telemetry=telemetry)
                elif existing > 0:
                    # Already holding the target asset
                    state["position"] = best["symbol"]
                    state["entry_price"] = best["price"]
                    state["entry_time"] = datetime.now(timezone.utc).isoformat()
                    state["quantity"] = existing
                    if best.get("momentum_buy") and not best.get("buy_signal") and not best.get("squeeze_buy"):
                        state["entry_reason"] = "momentum_buy"
                    elif best.get("squeeze_buy") and not best.get("buy_signal"):
                        state["entry_reason"] = "squeeze_buy"
                    else:
                        state["entry_reason"] = "buy_signal"
                    atr = best.get("atr")
                    state["stop_loss"] = round_price(calc_dynamic_stop(best["price"], best["price"], atr))
                    state["highest_since_entry"] = best["price"]
                    state["highest_time"] = state["entry_time"]
                    log(f"  Already holding {existing} {best['symbol']}, tracking position")
                else:
                    log(f"  Insufficient funds: ${usd:.2f} USD available")
        else:
            log(f"  No buy signals — waiting")
            if status_only:
                log(f"  Closest to buy: " + ", ".join(
                    f"{a['symbol']}(RSI={a['rsi']})" for a in sorted(analyses.values(), key=lambda x: x['rsi'])[:2]
                ))

            # ─── YOLO Hunt: proactive Gemini pick when idle + fearful market ───
            elif not dry_run and total_available >= MIN_TRADE_USD:
                now = datetime.now(timezone.utc)
                last_sell = state.get("last_sell_time")
                last_yolo = state.get("last_yolo_attempt")
                idle_ok = True
                cooldown_ok = True

                if last_sell:
                    idle_hours = (now - datetime.fromisoformat(last_sell)).total_seconds() / 3600
                    idle_ok = idle_hours >= YOLO_IDLE_HOURS
                    if not idle_ok:
                        log(f"  🐊 YOLO hunt: idle {idle_hours:.1f}h < {YOLO_IDLE_HOURS}h — too soon")

                if last_yolo:
                    cooldown_hours = (now - datetime.fromisoformat(last_yolo)).total_seconds() / 3600
                    cooldown_ok = cooldown_hours >= YOLO_COOLDOWN_HOURS
                    if not cooldown_ok:
                        log(f"  🐊 YOLO hunt: last attempt {cooldown_hours:.1f}h ago < {YOLO_COOLDOWN_HOURS}h cooldown")

                if idle_ok and cooldown_ok:
                    log(f"  🐊 YOLO HUNT: No signals + idle long enough — consulting Gemini...")
                    pick, consulted = gemini_yolo_pick(analyses, excluded_symbols=excluded_symbols, telemetry=telemetry)
                    # Only burn cooldown if Gemini was actually consulted
                    # (pre-screen rejection and API failures don't count)
                    if consulted:
                        state["last_yolo_attempt"] = now.isoformat()

                    if pick and not check_btc_crash(analyses, telemetry=telemetry):
                        # Run through Layer 2 (buy sentiment) with the picked coin's data
                        picked = analyses[pick]
                        # Daily RSI falling-knife check for YOLO picks too
                        daily_rsi, daily_declining = get_daily_rsi(pick)
                        if daily_rsi is not None and daily_rsi < DAILY_RSI_KNIFE and daily_declining:
                            log(f"  🔪 YOLO KNIFE — {pick} daily RSI {daily_rsi} declining, too dangerous")
                        else:
                            # Order book sell-wall check for YOLO picks (#17)
                            wall_detected, wall_ratio = check_sell_wall(pick, picked['price'])
                            picked['sell_buy_ratio'] = round(wall_ratio, 2)
                            if wall_detected:
                                log(f"  🧱 YOLO WALL VETO — {pick} has {wall_ratio:.1f}× sell wall, too risky")
                            else:
                                log(f"  🎯 YOLO BUY: Gemini vetted {pick} as SAFE — executing!")
                                # Consolidate crypto if needed
                                for sym, info in PAIRS.items():
                                    bal = balances.get(sym, 0)
                                    if bal > 0.0001 and sym != pick:
                                        qty = truncate_amount(info["symbol"], bal)
                                        if qty * analyses.get(sym, {}).get("price", 0) >= MIN_TRADE_USD:
                                            log(f"  Selling {qty} {sym} to consolidate...")
                                            place_order("sell", info["symbol"], qty)
                                            time.sleep(2)
                                balances = get_balance() or {}
                                usd = balances.get("USD", 0)
                                if usd >= MIN_TRADE_USD:
                                    state = execute_buy(state, picked, usd, entry_reason="yolo", telemetry=telemetry)
                                else:
                                    log(f"  Insufficient funds after consolidation: ${usd:.2f}")
                    elif pick:
                        log(f"  🛑 Gemini picked {pick} but BTC crash guard blocked it")

    save_state(state)

    # Write dashboard status
    write_dashboard_status(state, analyses)

    log("Cycle complete")
    return telemetry


# ─── Dashboard Status ────────────────────────────────────────────
def write_dashboard_status(state: dict, analyses: dict):
    """Write JSON status for the HTML dashboard."""
    control_state = load_control_state()
    coins = []
    for sym in PAIRS:
        a = analyses.get(sym)
        if a:
            # Calculate proximity to buy (0 = buy signal, 100 = far from buy)
            buy_proximity = max(0, min(100, (a["rsi"] - RSI_OVERSOLD) / (50 - RSI_OVERSOLD) * 100))
            coins.append({
                "symbol": sym,
                "price": a["price"],
                "rsi": a["rsi"],
                "bb_lower": a["bb_lower"],
                "bb_middle": a["bb_middle"],
                "bb_upper": a["bb_upper"],
                "bb_position": a["bb_position"],
                "buy_signal": a["buy_signal"],
                "sell_signal": a["sell_signal"],
                "strength": a["strength"],
                "buy_proximity": round(buy_proximity, 1),
                "change_24h": a.get("change_24h"),
                "change_72h": a.get("change_72h"),
                "vol_ratio": a.get("vol_ratio", 0),
                "rsi_divergence": a.get("rsi_divergence", False),
                "bb_squeezing": a.get("bb_squeezing", False),
                "bb_squeeze_breakout": a.get("bb_squeeze_breakout", False),
                "squeeze_buy": a.get("squeeze_buy", False),
                "momentum_buy": a.get("momentum_buy", False),
                "band_walking": a.get("band_walking", False),
                "band_walk_count": a.get("band_walk_count", 0),
                "funding_rate": a.get("funding_rate"),
                "funding_squeezed": a.get("funding_squeezed", False),
                "rel_strength_24h": a.get("rel_strength_24h"),
                "rel_strength_72h": a.get("rel_strength_72h"),
                "rel_strength_score": a.get("rel_strength_score", 0),
            })

    status = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bot_running": True,
        "paused": control_state.get("paused", False),
        "pause_since": control_state.get("pause_since"),
        "pause_reason": control_state.get("pause_reason"),
        "pending_action": control_state.get("pending_action"),
        "last_manual_action": control_state.get("last_action"),
        "control_api_port": CONTROL_API_PORT,
        "position": state.get("position"),
        "entry_price": state.get("entry_price", 0),
        "entry_time": state.get("entry_time"),
        "quantity": state.get("quantity", 0),
        "stop_loss": state.get("stop_loss", 0),
        "highest_since_entry": state.get("highest_since_entry", 0),
        "highest_time": state.get("highest_time"),
        "total_pnl": state.get("total_pnl", 0),
        "trade_count": len(state.get("trades", [])),
        "last_trades": state.get("trades", [])[-20:],
        "coins": sorted(coins, key=lambda x: x["rsi"]),
        "usd_balance": 0,  # Will be filled if available
    }

    # Try to get USD balance and compute extended metrics
    try:
        balances = get_balance() or {}
        usd_balance = balances.get("USD", 0)
        status["usd_balance"] = round(usd_balance, 2)
        
        total_equity = usd_balance
        if state.get("position") and state["position"] in analyses:
            price = analyses[state["position"]]["price"]
            qty = state.get("quantity", 0)
            entry = state.get("entry_price", price)
            position_value = qty * price
            
            status["position_value"] = round(position_value, 2)
            status["unrealized_pnl"] = round(position_value - (qty * entry), 2)
            status["unrealized_pnl_pct"] = round(((price - entry) / entry) * 100, 2) if entry > 0 else 0.0
            
            total_equity += position_value
            
        status["total_equity"] = round(total_equity, 2)
        
        # Approximate ROI based on current equity vs starting equity (equity - pnl)
        total_pnl = state.get("total_pnl", 0)
        starting_equity = total_equity - total_pnl
        status["roi_pct"] = round((total_pnl / starting_equity) * 100, 2) if starting_equity > 0 else 0.0

        # Optional Fee Tracking (if future orders inject fee data)
        trades = state.get("trades", [])
        status["total_fees"] = round(sum(t.get("fee", 0) for t in trades), 2)
        status["last_fee"] = round(trades[-1].get("fee", 0), 4) if trades else 0.0
        
    except Exception:
        pass

    Path(DASHBOARD_DIR).mkdir(parents=True, exist_ok=True)
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2)


# ─── Entry Points ────────────────────────────────────────────────
def main():
    if not acquire_singleton_lock():
        log("⚠️ Another DipSniffer bot process is already running — exiting this instance.")
        return
    atexit.register(release_singleton_lock)
    atexit.register(stop_control_server)

    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    status_only = "--status" in args
    loop = "--loop" in args

    logger = SQLiteLogger()

    if dry_run:
        log("🔒 DRY RUN MODE — no trades will be executed")
    if status_only:
        log("📊 STATUS MODE — showing signals only")

    if loop and not dry_run and not status_only:
        start_control_server()

    if loop:
        log(f"🔄 LOOP MODE — running every {LOOP_INTERVAL_SEC}s (Ctrl+C to stop)")
        while True:
            try:
                telemetry = run_cycle(dry_run=dry_run, status_only=status_only)
                if telemetry:
                    logger.consume_cycle(telemetry)
                    logger.evaluate_closed_trades()
                log(f"Next cycle in {LOOP_INTERVAL_SEC}s...")
                _control_wake_event.wait(timeout=LOOP_INTERVAL_SEC)
                _control_wake_event.clear()
            except KeyboardInterrupt:
                log("Bot stopped by user")
                break
    else:
        telemetry = run_cycle(dry_run=dry_run, status_only=status_only)
        if telemetry:
            logger.consume_cycle(telemetry)
            logger.evaluate_closed_trades()


if __name__ == "__main__":
    main()
