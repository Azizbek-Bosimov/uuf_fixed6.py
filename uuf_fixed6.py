
"""
GOLD (XAUUSDT) 5m/15m SMC + ICT Bot - RAILWAY READY (v7)
To'liq tuzatilgan: thread-safe, gunicorn-compatible, Railway Volume support.
"""

import datetime
import html
import json
import logging
import math
import os
import sqlite3
import threading
import time
import traceback
from collections import deque

import requests
from flask import Flask, request, jsonify

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("gold_smc_bot")

START_TIME = time.time()

# ==================== KONFIGURATSIYA ====================
SYMBOL = "XAUUSDT"
BTC_SYMBOL = "BTCUSDT"
EUR_SYMBOL = "EURUSDT"
LIMIT = 200
RR1, RR2 = 1.5, 3.0
SWING_LEFT, SWING_RIGHT = 3, 3
CHECK_INTERVAL_SEC = 5
ZONE_MAX_DISTANCE_PCT = 0.4
MAX_RISK_PCT = 0.02
TRADE_STALE_WARNING_HOURS = 6

# ✅ FIX: Railway Volume uchun DATA_DIR
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", BASE_DIR)
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    DATA_DIR = BASE_DIR

LOG_FILE = os.path.join(DATA_DIR, "trade_log.json")
STATUS_FILE = os.path.join(DATA_DIR, "status.json")
DB_FILE = os.path.join(DATA_DIR, "trades.db")
ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")
SUBSCRIBERS_FILE = os.path.join(DATA_DIR, "subscribers.json")

DEFAULT_RISK_PER_TRADE_PCT = 1.0
MAX_RISK_PER_TRADE_PCT = 5.0
XAUUSD_LOT_UNITS = 100
MIN_LOT_STEP = 0.01

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

KILLZONES_ENABLED = True
KILLZONES = [
    {"start": datetime.time(7, 0), "end": datetime.time(11, 0)},
    {"start": datetime.time(12, 0), "end": datetime.time(16, 0)},
]

# ==================== SECRETS ====================
def _get_secret(name):
    value = os.environ.get(name)
    if value:
        return value
    try:
        import config
        return getattr(config, name)
    except (ImportError, AttributeError):
        raise RuntimeError(f"'{name}' topilmadi. Environment variable sifatida o'rnating.")

BOT_TOKEN = _get_secret("BOT_TOKEN")
CHAT_ID = _get_secret("CHAT_ID")
ADMIN_CHAT_ID = str(CHAT_ID)
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "change_me_secret")
PRICE_OFFSET = 0.0

# ==================== ATOMIC WRITE ====================
def _atomic_write(path, data):
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)

def _safe_log_error(error):
    msg = str(error).replace(BOT_TOKEN, "***").replace(WEBHOOK_SECRET, "***")
    return msg

# ==================== LOCKS ====================
state_lock = threading.RLock()
_news_lock = threading.Lock()
_log_lock = threading.Lock()
_htf_lock = threading.Lock()
_db_lock = threading.Lock()
_telegram_lock = threading.Lock()
_zones_cache_lock = threading.Lock()

# ==================== STATE ====================
SUBSCRIBERS = set()
ACCOUNT = {"balance": None, "risk_pct": DEFAULT_RISK_PER_TRADE_PCT}
current_trade = None
warned_flip = False
paused = False
last_impulse_ts = None
post_trade = None

IMPULSE_LOOKBACK = 20
IMPULSE_THRESHOLD = 2.5
POST_TRADE_CHECKS = 20
POST_TRADE_MIN_CONTINUATION_ATR_MULT = 1.5

PRICE_HISTORY_MAXLEN = 300
PRICE_HISTORY = deque(maxlen=PRICE_HISTORY_MAXLEN)
last_impulse_info = None

last_status = {
    "price": None, "bias5": None, "bias15": None,
    "bias1h": None, "bias4h": None, "bias1d": None,
    "checked_at": None,
}

RSI_PERIOD = 14
ATR_PERIOD = 14
VWAP_LOOKBACK = 48
MIN_CONFIRMATIONS = 2
ATR_MIN_RISK_MULT = 0.3

HTF_FILTER_ENABLED = True
STRONG_HTF_FILTER_ENABLED = True
DAILY_HTF_FILTER_ENABLED = True
ZONE_MAX_AGE_BARS = 40
SL_BUFFER_ATR_MULT = 0.15

NEWS_FILTER_ENABLED = True
NEWS_BLOCK_MINUTES_BEFORE = 30
NEWS_BLOCK_MINUTES_AFTER = 30
NEWS_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
NEWS_CACHE_TTL_SEC = 3600
_news_cache = {"events": None, "fetched_at": None}

_htf_cache = {"bias1h": None, "bias4h": None, "bias1d": None, "fetched_at": None}
HTF_CACHE_TTL_SEC = 60

_zones_cache = {"fvg": None, "ob": None, "last_len": 0}

O, H, L, C = 1, 2, 3, 4

# ==================== METRICS ====================
_metrics = {
    "signals_generated": 0,
    "trades_closed": 0,
    "wins": 0,
    "losses": 0,
    "errors": 0,
    "start_time": time.time(),
}

# ==================== DATABASE (SQLite) ====================
def _init_db():
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                signal TEXT,
                entry REAL,
                sl REAL,
                tp1 REAL,
                tp2 REAL,
                result TEXT,
                close_price REAL,
                opened_at TEXT,
                closed_at TEXT,
                lot REAL,
                risk_usd REAL,
                confirmations TEXT
            )
        """)
        conn.commit()
        return conn

_db_conn = _init_db()

def db_insert_trade(trade, result, close_price, lot=None, risk_usd=None, confirmations=""):
    with _db_lock:
        try:
            _db_conn.execute("""
                INSERT INTO trades (symbol, signal, entry, sl, tp1, tp2, result,
                                    close_price, opened_at, closed_at, lot, risk_usd, confirmations)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                SYMBOL, trade["signal"], trade["entry"], trade["sl"],
                trade["tp1"], trade["tp2"], result, close_price,
                trade.get("opened_at"), datetime.datetime.now(datetime.timezone.utc).isoformat(),
                lot, risk_usd, confirmations,
            ))
            _db_conn.commit()
        except Exception as error:
            logger.error(f"DB insert xato: {_safe_log_error(error)}")

def db_get_recent_trades(limit=50):
    with _db_lock:
        try:
            cursor = _db_conn.execute(
                "SELECT signal, result, entry, close_price, closed_at FROM trades "
                "ORDER BY id DESC LIMIT ?", (limit,)
            )
            return cursor.fetchall()
        except Exception as error:
            logger.error(f"DB select xato: {_safe_log_error(error)}")
            return []

def db_stats():
    with _db_lock:
        try:
            cursor = _db_conn.execute("""
                SELECT result, COUNT(*) FROM trades GROUP BY result
            """)
            return dict(cursor.fetchall())
        except Exception:
            return {}

# ==================== SUBSCRIBERS ====================
def load_subscribers():
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            with open(SUBSCRIBERS_FILE, encoding='utf-8') as file:
                return set(json.load(file))
        except Exception as error:
            logger.error(f"Obunachilarni o'qishda xato: {_safe_log_error(error)}")
    return {ADMIN_CHAT_ID}

def save_subscribers():
    try:
        with state_lock:
            snapshot = sorted(list(SUBSCRIBERS))
        _atomic_write(SUBSCRIBERS_FILE, snapshot)
    except Exception as error:
        logger.error(f"Obunachilarni saqlashda xato: {_safe_log_error(error)}")

# ==================== ACCOUNT ====================
def load_account():
    if os.path.exists(ACCOUNT_FILE):
        try:
            with open(ACCOUNT_FILE, encoding='utf-8') as file:
                data = json.load(file)
                return {
                    "balance": data.get("balance"),
                    "risk_pct": data.get("risk_pct", DEFAULT_RISK_PER_TRADE_PCT),
                }
        except Exception as error:
            logger.error(f"Hisob ma'lumotini o'qishda xato: {_safe_log_error(error)}")
    return {"balance": None, "risk_pct": DEFAULT_RISK_PER_TRADE_PCT}

def save_account():
    _atomic_write(ACCOUNT_FILE, ACCOUNT)

# ==================== HELPERS ====================
def closed_only(candles):
    return candles[:-1] if len(candles) > 1 else candles

def in_killzone():
    if not KILLZONES_ENABLED:
        return True
    now_utc = datetime.datetime.now(datetime.timezone.utc).time()
    for kz in KILLZONES:
        if kz["start"] <= now_utc <= kz["end"]:
            return True
    return False

def detect_impulse(candles, lookback=IMPULSE_LOOKBACK, threshold=IMPULSE_THRESHOLD):
    if len(candles) < lookback + 1:
        return None
    ranges = [c[H] - c[L] for c in candles]
    avg_range = sum(ranges[-lookback - 1:-1]) / lookback
    last = candles[-1]
    last_range = last[H] - last[L]
    if avg_range == 0:
        return None
    ratio = last_range / avg_range
    if ratio >= threshold:
        direction = "yuqoriga" if last[C] > last[O] else "pastga"
        return {
            "ts": last[0], "ratio": round(ratio, 1), "direction": direction,
            "range": round(last_range, 2), "price": round(last[C], 2),
        }
    return None

# ==================== LOG (JSON) ====================
_log_cache = {"data": None, "mtime": None}

def load_log():
    with _log_lock:
        if not os.path.exists(LOG_FILE):
            return []
        try:
            mtime = os.path.getmtime(LOG_FILE)
            if _log_cache["mtime"] == mtime and _log_cache["data"] is not None:
                return _log_cache["data"]
            with open(LOG_FILE, encoding='utf-8') as file:
                data = json.load(file)
            _log_cache["data"] = data
            _log_cache["mtime"] = mtime
            return data
        except json.JSONDecodeError:
            return []
        except Exception as error:
            logger.error(f"Log o'qishda xato: {_safe_log_error(error)}")
            return _log_cache["data"] if _log_cache["data"] is not None else []

def save_log(log):
    with _log_lock:
        _atomic_write(LOG_FILE, log)
        _log_cache["data"] = log
        try:
            _log_cache["mtime"] = os.path.getmtime(LOG_FILE)
        except OSError:
            _log_cache["mtime"] = None

# ==================== STATUS ====================
def save_status(price, bias5, bias15, bias1h=None, bias4h=None, bias1d=None):
    with state_lock:
        trade_snapshot = dict(current_trade) if current_trade else None
    _atomic_write(STATUS_FILE, {
        "currentTrade": trade_snapshot,
        "lastPrice": price,
        "lastCheckedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "bias5m": bias5, "bias15m": bias15,
        "bias1h": bias1h, "bias4h": bias4h, "bias1d": bias1d,
    })

def win_rate_text(log):
    if not log:
        return "Hali statistika yo'q"
    wins = sum(1 for t in log if t["result"] in ("TP1", "TP2"))
    breakevens = sum(1 for t in log if "BE" in t["result"] or "Trailing" in t["result"])
    total = len(log)
    if total == 0:
        return "Hali statistika yo'q"
    return f"{wins / total * 100:.1f}% g'alaba, {breakevens} ta BE/Trailing, jami {total} ta savdo"

# ==================== BYBIT API ====================
def fetch_ohlcv(timeframe, symbol=SYMBOL):
    interval_map = {"1m": "1", "5m": "5", "15m": "15", "30m": "30",
                    "1h": "60", "4h": "240", "1d": "D"}
    interval = interval_map.get(timeframe, timeframe)
    response = requests.get(
        "https://api.bybit.com/v5/market/kline",
        params={"category": "linear", "symbol": symbol, "interval": interval, "limit": LIMIT},
        headers=HEADERS, timeout=8,
    )
    response.raise_for_status()
    data = response.json()
    rows = data.get("result", {}).get("list", [])
    if not rows:
        raise RuntimeError(f"Bybit'dan candle ma'lumoti kelmadi: {data}")
    rows.sort(key=lambda row: int(row[0]))
    return [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])] for r in rows]

# ==================== SWINGS ====================
def find_swings(candles, left=SWING_LEFT, right=SWING_RIGHT):
    highs, lows = [], []
    n = len(candles)
    if n < left + right + 1:
        return highs, lows
    for index in range(left, n - right):
        current_high = candles[index][H]
        current_low = candles[index][L]
        is_high = True
        is_low = True
        for offset in range(-left, right + 1):
            if offset == 0:
                continue
            other = candles[index + offset]
            if other[H] > current_high:
                is_high = False
            if other[L] < current_low:
                is_low = False
            if not is_high and not is_low:
                break
        if is_high:
            highs.append((index, current_high))
        if is_low:
            lows.append((index, current_low))
    return highs, lows

def confirmed_structure_bias(candles, left=SWING_LEFT, right=SWING_RIGHT):
    highs, lows = find_swings(candles, left, right)
    if not highs or not lows:
        return None
    last_high_index, last_high_price = highs[-1]
    last_low_index, last_low_price = lows[-1]
    start_index = max(last_high_index, last_low_index) + 1
    bias = None
    for index in range(start_index, len(candles)):
        close = candles[index][C]
        if close > last_high_price:
            bias = "bullish"
            last_low_price = min(last_low_price, candles[index][L])
        elif close < last_low_price:
            bias = "bearish"
            last_high_price = max(last_high_price, candles[index][H])
    return bias

# ==================== LIQUIDITY SWEEP ====================
def detect_liquidity_sweep(candles, left=SWING_LEFT, right=SWING_RIGHT):
    highs, lows = find_swings(candles, left, right)
    if not highs or not lows:
        return None
    last_high_price = highs[-1][1]
    last_low_price = lows[-1][1]
    candidates = [candles[-1]]
    if len(candles) >= 2:
        candidates.append(candles[-2])
    for candle in candidates:
        if candle[H] > last_high_price and candle[C] < last_high_price:
            return "bearish_sweep"
        if candle[L] < last_low_price and candle[C] > last_low_price:
            return "bullish_sweep"
    return None

# ==================== FVG + OB ====================
def detect_fvg(candles):
    fvgs = []
    for index in range(2, len(candles)):
        first, third = candles[index - 2], candles[index]
        if third[L] > first[H]:
            fvgs.append({"type": "bullish", "kind": "fvg",
                         "top": third[L], "bottom": first[H], "index": index})
        elif third[H] < first[L]:
            fvgs.append({"type": "bearish", "kind": "fvg",
                         "top": first[L], "bottom": third[H], "index": index})
    return fvgs

def detect_order_blocks(candles):
    bodies = [abs(c[C] - c[O]) for c in candles]
    volumes = [c[5] if len(c) > 5 else 0 for c in candles]
    order_blocks = []
    for index in range(10, len(candles) - 1):
        avg_body = sum(bodies[index - 10:index]) / 10
        avg_vol = sum(volumes[index - 10:index]) / 10
        if avg_body == 0 or avg_vol == 0:
            continue
        impulsive_body = bodies[index + 1] > avg_body * 1.5
        impulsive_vol = volumes[index + 1] > avg_vol * 2.0
        current, following = candles[index], candles[index + 1]
        bullish_ob = current[C] < current[O] and following[C] > following[O]
        bearish_ob = current[C] > current[O] and following[C] < following[O]
        if impulsive_body and impulsive_vol and bullish_ob:
            order_blocks.append({"type": "bullish", "kind": "ob",
                                 "top": current[O], "bottom": current[L], "index": index})
        if impulsive_body and impulsive_vol and bearish_ob:
            order_blocks.append({"type": "bearish", "kind": "ob",
                                 "top": current[H], "bottom": current[O], "index": index})
    return order_blocks

def get_zones_cached(candles):
    with _zones_cache_lock:
        if _zones_cache["last_len"] == len(candles) and _zones_cache["fvg"] is not None:
            return _zones_cache["fvg"] + _zones_cache["ob"]
        fvg = detect_fvg(candles)
        ob = detect_order_blocks(candles)
        _zones_cache["fvg"] = fvg
        _zones_cache["ob"] = ob
        _zones_cache["last_len"] = len(candles)
        return fvg + ob

# ==================== INDIKATORLAR ====================
def calculate_rsi(candles, period=RSI_PERIOD):
    closes = [c[C] for c in candles]
    count = len(closes)
    if count < period + 1:
        return [None] * count
    rsis = [None] * period
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rsis.append(100 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss)))
    for i in range(period + 1, count):
        diff = closes[i] - closes[i - 1]
        gain = max(diff, 0)
        loss = max(-diff, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rsis.append(100 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss)))
    return rsis

def calculate_atr(candles, period=ATR_PERIOD):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i][H], candles[i][L], candles[i - 1][C]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / period

def calculate_vwap(candles, lookback=VWAP_LOOKBACK):
    cpv = 0
    cv = 0
    for candle in candles[-lookback:]:
        tp = (candle[H] + candle[L] + candle[C]) / 3
        vol = candle[5] if len(candle) > 5 else 0
        cpv += tp * vol
        cv += vol
    if cv == 0:
        return None
    return cpv / cv

def detect_rsi_divergence(candles, rsis, lookback=20):
    if len(candles) < lookback or lookback < 5:
        return False, False
    window = candles[-lookback:]
    offset = len(candles) - lookback
    highs, lows = find_swings(window)
    bullish_div, bearish_div = False, False
    if len(lows) >= 2:
        fi, fp = lows[-2]
        si, sp = lows[-1]
        fr, sr = rsis[offset + fi], rsis[offset + si]
        if fr is not None and sr is not None and sp < fp and sr > fr:
            bullish_div = True
    if len(highs) >= 2:
        fi, fp = highs[-2]
        si, sp = highs[-1]
        fr, sr = rsis[offset + fi], rsis[offset + si]
        if fr is not None and sr is not None and sp > fp and sr < fr:
            bearish_div = True
    return bullish_div, bearish_div

# ==================== VOLUME PROFILE ====================
def volume_profile(candles, bins=20, lookback=100):
    window = candles[-lookback:]
    if not window:
        return None
    highs = [c[H] for c in window]
    lows = [c[L] for c in window]
    price_min, price_max = min(lows), max(highs)
    if price_max == price_min:
        return None
    bin_size = (price_max - price_min) / bins
    profile = [0.0] * bins
    for c in window:
        vol = c[5] if len(c) > 5 else 0
        idx = int((c[C] - price_min) / bin_size)
        idx = max(0, min(bins - 1, idx))
        profile[idx] += vol
    poc_idx = profile.index(max(profile))
    poc = price_min + (poc_idx + 0.5) * bin_size
    total_vol = sum(profile)
    target = total_vol * 0.7
    accumulated = profile[poc_idx]
    upper = poc_idx
    lower = poc_idx
    while accumulated < target and (upper < bins - 1 or lower > 0):
        up_vol = profile[upper + 1] if upper < bins - 1 else -1
        dn_vol = profile[lower - 1] if lower > 0 else -1
        if up_vol >= dn_vol and upper < bins - 1:
            upper += 1
            accumulated += profile[upper]
        elif lower > 0:
            lower -= 1
            accumulated += profile[lower]
        else:
            break
    vah = price_min + (upper + 1) * bin_size
    val = price_min + lower * bin_size
    return {"poc": round(poc, 2), "vah": round(vah, 2), "val": round(val, 2)}

# ==================== ORDER FLOW ====================
def order_flow_delta(candles, lookback=20):
    window = candles[-lookback:]
    delta = 0
    for c in window:
        rng = c[H] - c[L]
        if rng == 0:
            continue
        vol = c[5] if len(c) > 5 else 0
        delta += ((c[C] - c[O]) / rng) * vol
    return round(delta, 2)

# ==================== SESSION ====================
def current_session():
    hour = datetime.datetime.now(datetime.timezone.utc).hour
    if 0 <= hour < 7:
        return "asia"
    if 7 <= hour < 12:
        return "london"
    if 12 <= hour < 16:
        return "london_ny_overlap"
    if 16 <= hour < 21:
        return "newyork"
    return "after_hours"

# ==================== ADAPTIVE RISK ====================
def adaptive_risk_pct(base_risk):
    stats = db_stats()
    wins = stats.get("TP1", 0) + stats.get("TP2", 0)
    losses = stats.get("SL", 0)
    total = wins + losses
    if total < 10:
        return base_risk
    win_rate = wins / total
    if win_rate > 0.65:
        return min(base_risk * 1.5, MAX_RISK_PER_TRADE_PCT)
    elif win_rate < 0.4:
        return max(base_risk * 0.5, 0.1)
    return base_risk

# ==================== CONFIRMATION ====================
def confirmation_check(bias, price, vwap, bull_div, bear_div,
                       btc_bias=None, eur_bias=None,
                       zone_confluence=False, sweep=None, delta=None,
                       vp=None, session=None):
    confirmations = []
    if bias == "bullish":
        if bull_div:
            confirmations.append("RSI divergence (bullish)")
        if vwap is not None and price > vwap:
            confirmations.append("Narx VWAP ustida")
        if btc_bias == "bullish" and eur_bias == "bullish":
            confirmations.append("SMT: BTC + EUR mos")
        elif btc_bias == "bullish":
            confirmations.append("SMT: BTC bullish")
        elif eur_bias == "bullish":
            confirmations.append("SMT: EUR bullish")
        if sweep == "bullish_sweep":
            confirmations.append("Likvidlik yig'ildi (Bullish Sweep)")
        if delta is not None and delta > 0:
            confirmations.append(f"Order Flow delta +{delta}")
        if vp and price < vp["val"]:
            confirmations.append("Narx Value Area pastida")
        if session in ("london", "london_ny_overlap"):
            confirmations.append(f"Sessiya: {session}")
    else:
        if bear_div:
            confirmations.append("RSI divergence (bearish)")
        if vwap is not None and price < vwap:
            confirmations.append("Narx VWAP ostida")
        if btc_bias == "bearish" and eur_bias == "bearish":
            confirmations.append("SMT: BTC + EUR mos")
        elif btc_bias == "bearish":
            confirmations.append("SMT: BTC bearish")
        elif eur_bias == "bearish":
            confirmations.append("SMT: EUR bearish")
        if sweep == "bearish_sweep":
            confirmations.append("Likvidlik yig'ildi (Bearish Sweep)")
        if delta is not None and delta < 0:
            confirmations.append(f"Order Flow delta {delta}")
        if vp and price > vp["vah"]:
            confirmations.append("Narx Value Area ustida")
        if session in ("london", "london_ny_overlap"):
            confirmations.append(f"Sessiya: {session}")
    if zone_confluence:
        confirmations.append("Zone confluence (FVG + OB)")
    return confirmations

# ==================== ZONE FRESHNESS ====================
def _zone_is_fresh(zone, candles, max_age_bars=ZONE_MAX_AGE_BARS):
    formation_index = zone["index"]
    last_index = len(candles) - 1
    if last_index - formation_index > max_age_bars:
        return False
    for i in range(formation_index + 1, len(candles)):
        close = candles[i][C]
        if zone["type"] == "bullish" and close < zone["bottom"]:
            return False
        if zone["type"] == "bearish" and close > zone["top"]:
            return False
    return True

# ==================== BUILD TRADE ====================
def build_trade(candles, bias, price, atr=None):
    all_zones = [
        z for z in get_zones_cached(candles)
        if z["type"] == bias and _zone_is_fresh(z, candles)
    ]
    if not all_zones:
        return None
    all_zones.sort(key=lambda z: abs(price - (z["top"] + z["bottom"]) / 2))
    zone = all_zones[0]
    lower, upper = min(zone["bottom"], zone["top"]), max(zone["bottom"], zone["top"])
    tolerance = price * (ZONE_MAX_DISTANCE_PCT / 100)
    if not (lower - tolerance <= price <= upper + tolerance):
        return None

    confluence = any(
        (other["index"] != zone["index"] or other["kind"] != zone["kind"])
        and other["kind"] != zone["kind"]
        and other["bottom"] <= zone["top"]
        and other["top"] >= zone["bottom"]
        for other in all_zones
    )

    sl_buffer = atr * SL_BUFFER_ATR_MULT if atr else price * 0.001
    entry = price
    if bias == "bullish":
        sl = zone["bottom"] - sl_buffer
        risk = entry - sl
        if risk <= 0 or risk > entry * MAX_RISK_PCT:
            return None
        tp1, tp2 = entry + risk * RR1, entry + risk * RR2
        side = "LONG"
    else:
        sl = zone["top"] + sl_buffer
        risk = sl - entry
        if risk <= 0 or risk > entry * MAX_RISK_PCT:
            return None
        tp1, tp2 = entry - risk * RR1, entry - risk * RR2
        side = "SHORT"

    return {
        "signal": side, "bias": bias,
        "entry": round(entry, 2), "sl": round(sl, 2),
        "tp1": round(tp1, 2), "tp2": round(tp2, 2),
        "confluence": confluence,
        "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "breakeven": False, "tp1_notified": False,
    }

# ==================== POSITION SIZING ====================
def calculate_position_size(entry, sl, balance, risk_pct):
    if not balance or balance <= 0:
        return None
    price_risk = abs(entry - sl)
    if price_risk <= 0:
        return None
    risk_usd_target = balance * (risk_pct / 100)
    raw_lot = risk_usd_target / (price_risk * XAUUSD_LOT_UNITS)
    lot = math.floor(raw_lot / MIN_LOT_STEP + 1e-9) * MIN_LOT_STEP
    lot = round(lot, 2)
    undersized = lot < MIN_LOT_STEP
    if undersized:
        lot = MIN_LOT_STEP
    risk_usd_actual = lot * price_risk * XAUUSD_LOT_UNITS
    risk_pct_actual = (risk_usd_actual / balance) * 100
    return {
        "lot": lot, "risk_usd": round(risk_usd_actual, 2),
        "risk_pct_actual": round(risk_pct_actual, 2),
        "undersized": undersized,
    }

# ==================== TELEGRAM ====================
def send_telegram(text, with_keyboard=False, chat_id=None):
    with state_lock:
        targets = [chat_id] if chat_id else list(SUBSCRIBERS)
    reply_markup = json.dumps({"keyboard": [["📊 Signal"]], "resize_keyboard": True}) if with_keyboard else None
    for i, target in enumerate(targets):
        payload = {"chat_id": target, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            with _telegram_lock:
                requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                              data=payload, timeout=10)
        except Exception as e:
            logger.error(f"Telegram xato ({target}): {_safe_log_error(e)}")
        if i < len(targets) - 1:
            time.sleep(0.05)

def _trade_age_text(opened_at_iso):
    if not opened_at_iso:
        return ""
    try:
        opened_at = datetime.datetime.fromisoformat(opened_at_iso)
        elapsed = datetime.datetime.now(datetime.timezone.utc) - opened_at
        hours = elapsed.total_seconds() / 3600
        return f" (ochilganiga {hours:.1f} soat bo'ldi)"
    except Exception:
        return ""

def build_signal_status_text():
    with state_lock:
        status_snapshot = dict(last_status)
        trade_snapshot = dict(current_trade) if current_trade else None
        is_paused = paused
    if status_snapshot["price"] is None:
        return "Bot hali birinchi tekshiruvni bajarmadi, biroz kuting."
    lines = [
        f"Holat: {'⏸ PAUZADA' if is_paused else '▶️ Ishlamoqda'}",
        f"Oxirgi tekshiruv: {status_snapshot['checked_at']}",
        f"Narx: {round(status_snapshot['price'], 2)}",
        f"5m: {status_snapshot['bias5'] or 'n/a'} | 15m: {status_snapshot['bias15'] or 'n/a'}",
        f"1h: {status_snapshot.get('bias1h') or 'n/a'} | 4h: {status_snapshot.get('bias4h') or 'n/a'}",
        f"1D: {status_snapshot.get('bias1d') or 'n/a'}",
        f"Sessiya: {current_session()}",
    ]
    if trade_snapshot:
        age_text = _trade_age_text(trade_snapshot.get("opened_at"))
        lines.extend([
            "", f"OCHIQ BITIM: {trade_snapshot['signal']}{age_text}",
            f"Entry: {trade_snapshot['entry']} | SL: {trade_snapshot['sl']}",
            f"TP1: {trade_snapshot['tp1']} | TP2: {trade_snapshot['tp2']}",
        ])
        with state_lock:
            balance = ACCOUNT.get("balance")
            risk_pct = ACCOUNT.get("risk_pct", DEFAULT_RISK_PER_TRADE_PCT)
        if balance:
            adj_risk = adaptive_risk_pct(risk_pct)
            sizing = calculate_position_size(trade_snapshot["entry"], trade_snapshot["sl"], balance, adj_risk)
            if sizing:
                lines.append(f"Lot: {sizing['lot']} (~{sizing['risk_usd']}$ / {sizing['risk_pct_actual']}%)")
    else:
        lines.extend(["", "Ochiq bitim yo'q - signal kutilmoqda."])
    with state_lock:
        balance = ACCOUNT.get("balance")
        risk_pct = ACCOUNT.get("risk_pct", DEFAULT_RISK_PER_TRADE_PCT)
    if balance:
        lines.append(f"\nHisob: {balance:.2f}$ | Risk: {risk_pct}%")
    else:
        lines.append("\n/balance <miqdor> kiriting.")
    lines.append(f"Statistika: {win_rate_text(load_log())}")
    return "\n".join(lines)

def register_bot_commands():
    commands = [
        {"command": "start", "description": "Obuna / holat"},
        {"command": "stop", "description": "Obunani bekor qilish"},
        {"command": "signal", "description": "Joriy holat"},
        {"command": "stats", "description": "Statistika"},
        {"command": "pause", "description": "Pauza (admin)"},
        {"command": "resume", "description": "Davom (admin)"},
        {"command": "close", "description": "Yopish (admin)"},
        {"command": "balance", "description": "Balans (admin)"},
        {"command": "risk", "description": "Risk % (admin)"},
    ]
    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
                          json={"commands": commands}, timeout=10)
        if r.status_code == 200 and r.json().get("ok"):
            logger.info("Buyruqlar menyusi o'rnatildi.")
    except Exception as e:
        logger.error(f"Buyruqlar xato: {_safe_log_error(e)}")

# ==================== TELEGRAM LISTENER ====================
def telegram_listener():
    global paused
    offset = None
    backoff = 5
    max_backoff = 60
    try:
        r = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                         params={"timeout": 1}, timeout=10)
        if r.status_code == 200:
            results = r.json().get("result", [])
            if results:
                offset = results[-1]["update_id"] + 1
    except Exception as e:
        logger.error(f"Listener start xato: {_safe_log_error(e)}")

    while True:
        try:
            params = {"timeout": 25}
            if offset is not None:
                params["offset"] = offset
            r = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                             params=params, timeout=30)
            if r.status_code != 200:
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue
            backoff = 5
            for update in r.json().get("result", []):
                offset = update["update_id"] + 1
                try:
                    msg = update.get("message", {})
                    text = (msg.get("text") or "").strip()
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    if not chat_id:
                        continue
                    is_admin = chat_id == ADMIN_CHAT_ID

                    if text == "/start":
                        with state_lock:
                            is_new = chat_id not in SUBSCRIBERS
                            SUBSCRIBERS.add(chat_id)
                        save_subscribers()
                        if is_new:
                            send_telegram(
                                "✅ Obuna bo'ldingiz!\n"
                                "/signal - holat\n/stats - statistika\n"
                                "/balance <miqdor>\n/risk <foiz>\n/stop - bekor",
                                with_keyboard=True, chat_id=chat_id)
                        else:
                            send_telegram(build_signal_status_text(), with_keyboard=True, chat_id=chat_id)
                    elif text == "/stop":
                        with state_lock:
                            was = chat_id in SUBSCRIBERS
                            SUBSCRIBERS.discard(chat_id)
                        save_subscribers()
                        if was:
                            send_telegram("🔕 Obuna bekor qilindi.", chat_id=chat_id)
                    elif text in ("/signal", "📊 Signal"):
                        send_telegram(build_signal_status_text(), with_keyboard=True, chat_id=chat_id)
                    elif text == "/stats":
                        stats = db_stats()
                        log = load_log()
                        msg_text = (
                            f"📊 Statistika:\n"
                            f"Jami savdo: {len(log)}\n"
                            f"TP1: {stats.get('TP1', 0)}\n"
                            f"TP2: {stats.get('TP2', 0)}\n"
                            f"SL: {stats.get('SL', 0)}\n"
                            f"BE: {stats.get('BE', 0) + stats.get('BE/Trailing SL', 0)}\n"
                            f"Win rate: {win_rate_text(log)}"
                        )
                        send_telegram(msg_text, chat_id=chat_id)
                    elif text == "/pause":
                        if not is_admin:
                            send_telegram("⛔ Faqat admin.", chat_id=chat_id)
                        else:
                            with state_lock:
                                paused = True
                            send_telegram("⏸ Pauza.")
                    elif text == "/resume":
                        if not is_admin:
                            send_telegram("⛔ Faqat admin.", chat_id=chat_id)
                        else:
                            with state_lock:
                                paused = False
                            send_telegram("▶️ Davom.")
                    elif text == "/close":
                        if not is_admin:
                            send_telegram("⛔ Faqat admin.", chat_id=chat_id)
                        else:
                            with state_lock:
                                exists = current_trade is not None
                                price_now = last_status.get("price")
                            if not exists:
                                send_telegram("Ochiq bitim yo'q.", chat_id=chat_id)
                            elif price_now:
                                close_trade("MANUAL", price_now)
                                send_telegram("🛑 Yopildi.", chat_id=chat_id)
                    elif text.startswith("/balance"):
                        if not is_admin:
                            send_telegram("⛔ Faqat admin.", chat_id=chat_id)
                        else:
                            parts = text.split()
                            if len(parts) != 2:
                                send_telegram("Foydalanish: /balance 500", chat_id=chat_id)
                            else:
                                try:
                                    nb = float(parts[1].replace(",", "."))
                                    if nb <= 0:
                                        raise ValueError
                                    with state_lock:
                                        ACCOUNT["balance"] = nb
                                    save_account()
                                    send_telegram(f"✅ Balans {nb:.2f}$ saqlandi.", chat_id=chat_id)
                                except ValueError:
                                    send_telegram("Noto'g'ri. /balance 500", chat_id=chat_id)
                    elif text.startswith("/risk"):
                        if not is_admin:
                            send_telegram("⛔ Faqat admin.", chat_id=chat_id)
                        else:
                            parts = text.split()
                            if len(parts) != 2:
                                send_telegram(f"/risk 1 (maks {MAX_RISK_PER_TRADE_PCT}%)", chat_id=chat_id)
                            else:
                                try:
                                    nr = float(parts[1].replace(",", "."))
                                    if not (0 < nr <= MAX_RISK_PER_TRADE_PCT):
                                        raise ValueError
                                    with state_lock:
                                        ACCOUNT["risk_pct"] = nr
                                    save_account()
                                    send_telegram(f"✅ Risk {nr}% saqlandi.", chat_id=chat_id)
                                except ValueError:
                                    send_telegram(f"0-{MAX_RISK_PER_TRADE_PCT}% oralig'ida.", chat_id=chat_id)
                except Exception as ue:
                    logger.error(f"Update xato: {_safe_log_error(ue)}")
        except Exception as e:
            logger.error(f"Listener xato: {_safe_log_error(e)}, {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

# ==================== POST TRADE ====================
def start_post_trade_tracking(side, close_price, atr=None):
    global post_trade
    with state_lock:
        post_trade = {
            "side": side, "close_price": close_price,
            "extreme": close_price, "checks": 0, "atr": atr or 5.0,
        }

def update_post_trade(price):
    global post_trade
    with state_lock:
        if post_trade is None:
            return
        if post_trade["side"] == "LONG":
            post_trade["extreme"] = max(post_trade["extreme"], price)
        else:
            post_trade["extreme"] = min(post_trade["extreme"], price)
        post_trade["checks"] += 1
        if post_trade["checks"] >= POST_TRADE_CHECKS:
            moved = abs(post_trade["extreme"] - post_trade["close_price"])
            threshold = post_trade["atr"] * POST_TRADE_MIN_CONTINUATION_ATR_MULT
            if moved >= threshold:
                extreme = post_trade["extreme"]
                side = post_trade["side"]
                post_trade = None
                send_telegram(
                    f"ℹ️ Oldingi {side} yopilgandan keyin narx {round(moved, 2)}$ "
                    f"davom etdi (eng yaxshi: {round(extreme, 2)})."
                )
            else:
                post_trade = None

# ==================== CLOSE TRADE ====================
def close_trade(result, price):
    global current_trade, warned_flip
    with state_lock:
        trade = current_trade
        if trade is None:
            return
        current_trade = None
        warned_flip = False
        balance = ACCOUNT.get("balance")
        risk_pct = ACCOUNT.get("risk_pct", DEFAULT_RISK_PER_TRADE_PCT)

    log = load_log()
    log.append({
        "id": len(log) + 1, "symbol": SYMBOL,
        "signal": trade["signal"], "entry": trade["entry"],
        "result": result, "close_price": price,
        "closed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    })
    save_log(log)

    sizing = calculate_position_size(trade["entry"], trade["sl"], balance, risk_pct) if balance else None
    db_insert_trade(
        trade, result, price,
        lot=sizing["lot"] if sizing else None,
        risk_usd=sizing["risk_usd"] if sizing else None,
        confirmations=trade.get("confirmations", ""),
    )

    _metrics["trades_closed"] += 1
    if result in ("TP1", "TP2"):
        _metrics["wins"] += 1
    elif result == "SL":
        _metrics["losses"] += 1

    emoji = {"TP2": "✅", "SL": "❌", "BE": "➖", "MANUAL": "🛑",
             "Trailing SL": "🛡", "BE/Trailing SL": "🛡"}.get(result, "ℹ️")
    send_telegram(
        f"GOLD {trade['signal']} yopildi {emoji} - {result} (narx {price})\n"
        f"Statistika: {win_rate_text(log)}"
    )
    start_post_trade_tracking(trade["signal"], price)
    save_status(price, last_status.get("bias5"), last_status.get("bias15"),
                last_status.get("bias1h"), last_status.get("bias4h"), last_status.get("bias1d"))

# ==================== MONITOR ====================
def monitor_open_trade(price, bias15):
    global current_trade, warned_flip
    notifications = []
    trade_to_close = None
    sl_changed = False

    with state_lock:
        trade = current_trade
        if trade is None:
            return
        side = trade["signal"]

        tp1_hit = price >= trade["tp1"] if side == "LONG" else price <= trade["tp1"]
        if tp1_hit and not trade.get("tp1_notified"):
            trade["tp1_notified"] = True
            notifications.append(f"🎯 TP1 ga yetdi: {trade['tp1']}")

        if side == "LONG":
            if price >= trade["tp1"]:
                new_sl = trade["entry"] + (price - trade["entry"]) * 0.5
                if new_sl > trade["sl"]:
                    trade["sl"] = round(new_sl, 2)
                    trade["breakeven"] = True
                    sl_changed = True
                    notifications.append(f"🔥 LONG trailing SL: {trade['sl']}")
        else:
            if price <= trade["tp1"]:
                new_sl = trade["entry"] - (trade["entry"] - price) * 0.5
                if new_sl < trade["sl"]:
                    trade["sl"] = round(new_sl, 2)
                    trade["breakeven"] = True
                    sl_changed = True
                    notifications.append(f"🔥 SHORT trailing SL: {trade['sl']}")

        sl_hit = price <= trade["sl"] if side == "LONG" else price >= trade["sl"]
        tp2_hit = price >= trade["tp2"] if side == "LONG" else price <= trade["tp2"]

        if sl_hit:
            trade_to_close = ("BE/Trailing SL" if trade.get("breakeven") else "SL", price)
        elif tp2_hit:
            trade_to_close = ("TP2", price)
        elif bias15 is not None and bias15 != trade["bias"] and not warned_flip:
            notifications.append(f"⚠️ {side} ochiq, lekin struktura {bias15}ga o'zgardi.")
            warned_flip = True

    for note in notifications:
        send_telegram(note)
    if trade_to_close:
        close_trade(*trade_to_close)
    if sl_changed and not trade_to_close:
        with state_lock:
            t = current_trade
        if t:
            save_status(last_status.get("price"), last_status.get("bias5"),
                        last_status.get("bias15"), last_status.get("bias1h"),
                        last_status.get("bias4h"), last_status.get("bias1d"))

# ==================== NEWS ====================
def fetch_high_impact_news():
    now = datetime.datetime.now(datetime.timezone.utc)
    with _news_lock:
        cached = _news_cache.get("events")
        fetched_at = _news_cache.get("fetched_at")
        if cached is not None and fetched_at is not None and (now - fetched_at).total_seconds() < NEWS_CACHE_TTL_SEC:
            return cached
        events = []
        try:
            r = requests.get(NEWS_CALENDAR_URL, headers=HEADERS, timeout=10)
            r.raise_for_status()
            for item in r.json():
                if item.get("country") != "USD" or item.get("impact") != "High":
                    continue
                date_str = item["date"]
                try:
                    event_time = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    if event_time.tzinfo is None:
                        event_time = event_time.replace(tzinfo=datetime.timezone.utc)
                except Exception:
                    continue
                events.append({"title": item.get("title", "?"), "time": event_time})
            _news_cache["events"] = events
            _news_cache["fetched_at"] = now
            return events
        except Exception as e:
            logger.warning(f"Yangiliklar olinmadi: {_safe_log_error(e)}")
            return cached if cached is not None else []

def is_news_blackout():
    if not NEWS_FILTER_ENABLED:
        return False, None
    now = datetime.datetime.now(datetime.timezone.utc)
    for event in fetch_high_impact_news():
        delta = (event["time"] - now).total_seconds() / 60
        if -NEWS_BLOCK_MINUTES_AFTER <= delta <= NEWS_BLOCK_MINUTES_BEFORE:
            return True, event["title"]
    return False, None

# ==================== HTF BIAS ====================
def _fetch_htf_bias():
    now = time.monotonic()
    with _htf_lock:
        if _htf_cache["fetched_at"] is not None and (now - _htf_cache["fetched_at"]) < HTF_CACHE_TTL_SEC:
            return _htf_cache["bias1h"], _htf_cache["bias4h"], _htf_cache["bias1d"]
    b1h, b4h, b1d = None, None, None
    try:
        b1h = confirmed_structure_bias(closed_only(fetch_ohlcv("1h")))
    except Exception as e:
        logger.debug(f"1h: {_safe_log_error(e)}")
    try:
        b4h = confirmed_structure_bias(closed_only(fetch_ohlcv("4h")))
    except Exception as e:
        logger.debug(f"4h: {_safe_log_error(e)}")
    try:
        b1d = confirmed_structure_bias(closed_only(fetch_ohlcv("1d")))
    except Exception as e:
        logger.debug(f"1d: {_safe_log_error(e)}")
    with _htf_lock:
        _htf_cache["bias1h"] = b1h
        _htf_cache["bias4h"] = b4h
        _htf_cache["bias1d"] = b1d
        _htf_cache["fetched_at"] = time.monotonic()
    return b1h, b4h, b1d

# ==================== MTF CONFLUENCE ====================
def mtf_confluence_score(b5, b15, b1h, b4h):
    score = 0
    if b5 and b15 and b5 == b15:
        score += 1
    if b15 and b1h and b15 == b1h:
        score += 1
    if b1h and b4h and b1h == b4h:
        score += 1
    return score

# ==================== MAIN RUN ====================
def run():
    global current_trade, last_impulse_ts, last_impulse_info

    try:
        candles15_raw = fetch_ohlcv("15m")
        candles5_raw = fetch_ohlcv("5m")
    except Exception as e:
        logger.error(f"Candle yuklashda xato: {_safe_log_error(e)}")
        _metrics["errors"] += 1
        return

    closed15 = closed_only(candles15_raw)
    closed5 = closed_only(candles5_raw)
    price = candles15_raw[-1][C] + PRICE_OFFSET

    bias15 = confirmed_structure_bias(closed15)
    bias5 = confirmed_structure_bias(closed5)
    atr15 = calculate_atr(closed15)
    bias1h, bias4h, bias1d = _fetch_htf_bias()

    with state_lock:
        last_status.update({
            "price": price, "bias5": bias5, "bias15": bias15,
            "bias1h": bias1h, "bias4h": bias4h, "bias1d": bias1d,
            "checked_at": time.strftime("%H:%M:%S"),
        })
        PRICE_HISTORY.append(round(price, 2))

    save_status(round(price, 2), bias5, bias15, bias1h, bias4h, bias1d)

    impulse = detect_impulse(closed5)
    if impulse and impulse["ts"] != last_impulse_ts:
        last_impulse_ts = impulse["ts"]
        with state_lock:
            last_impulse_info = dict(impulse)
            last_impulse_info["detected_at"] = time.strftime("%H:%M:%S")
        send_telegram(
            f"⚡ IMPULS (5m): {impulse['direction']} {impulse['range']}$ "
            f"({impulse['ratio']}x). Narx: {impulse['price']}."
        )

    with state_lock:
        has_open_trade = current_trade is not None
        is_paused = paused

    if has_open_trade:
        monitor_open_trade(price, bias15)
        return

    update_post_trade(price)

    if is_paused:
        logger.info("NONE - Pauzada")
        return

    if KILLZONES_ENABLED and not in_killzone():
        logger.info("NONE - Killzone tashqarisi")
        return

    blackout, event_title = is_news_blackout()
    if blackout:
        logger.info(f"NONE - Yangilik: {event_title}")
        return

    if bias5 is None or bias15 is None or bias5 != bias15:
        logger.info(f"NONE - 5m={bias5}, 15m={bias15}")
        return

    if HTF_FILTER_ENABLED and bias1h is not None and bias1h != bias15:
        logger.info(f"NONE - 1h qarshi")
        return

    if STRONG_HTF_FILTER_ENABLED and bias4h is not None and bias4h != bias15:
        logger.info(f"NONE - 4h qarshi")
        return

    if DAILY_HTF_FILTER_ENABLED and bias1d is not None and bias1d != bias15:
        logger.info(f"NONE - 1D qarshi")
        return

    trade = build_trade(closed15, bias15, price, atr15)
    if trade is None:
        logger.info("NONE - Zona topilmadi")
        return

    risk = abs(trade["entry"] - trade["sl"])
    if atr15 and risk < atr15 * ATR_MIN_RISK_MULT:
        logger.info(f"NONE - SL juda tor")
        return

    rsis15 = calculate_rsi(closed15)
    vwap15 = calculate_vwap(closed15)
    bull_div, bear_div = detect_rsi_divergence(closed15, rsis15)
    sweep15 = detect_liquidity_sweep(closed15)
    delta15 = order_flow_delta(closed15)
    vp15 = volume_profile(closed15)
    session = current_session()
    mtf_score = mtf_confluence_score(bias5, bias15, bias1h, bias4h)

    btc_bias15, eur_bias15 = None, None
    try:
        btc_bias15 = confirmed_structure_bias(closed_only(fetch_ohlcv("15m", symbol=BTC_SYMBOL)))
    except Exception as e:
        logger.warning(f"BTC: {_safe_log_error(e)}")
    try:
        eur_bias15 = confirmed_structure_bias(closed_only(fetch_ohlcv("15m", symbol=EUR_SYMBOL)))
    except Exception as e:
        logger.warning(f"EUR: {_safe_log_error(e)}")

    confirmations = confirmation_check(
        bias15, price, vwap15, bull_div, bear_div,
        btc_bias15, eur_bias15, trade.get("confluence", False),
        sweep=sweep15, delta=delta15, vp=vp15, session=session,
    )

    if mtf_score >= 3:
        confirmations.append(f"MTF confluence: {mtf_score}/3 (kuchli)")
    elif mtf_score == 2:
        confirmations.append(f"MTF confluence: {mtf_score}/3")

    if len(confirmations) < MIN_CONFIRMATIONS:
        logger.info(f"NONE - Tasdiq kam ({len(confirmations)}/{MIN_CONFIRMATIONS})")
        return

    with state_lock:
        balance = ACCOUNT.get("balance")
        base_risk = ACCOUNT.get("risk_pct", DEFAULT_RISK_PER_TRADE_PCT)
    adj_risk = adaptive_risk_pct(base_risk)

    if balance:
        sizing = calculate_position_size(trade["entry"], trade["sl"], balance, adj_risk)
        if sizing is None:
            position_line = "\nLot: hisoblab bo'lmadi."
        elif sizing["undersized"]:
            position_line = f"\n⚠️ Lot: {sizing['lot']} (min) - ~{sizing['risk_pct_actual']}%"
        else:
            position_line = f"\nLot: {sizing['lot']} (~{sizing['risk_usd']}$ / {sizing['risk_pct_actual']}%)"
    else:
        position_line = "\n/balance <miqdor> kiriting."

    trade["confirmations"] = ", ".join(confirmations)
    confluence_text = "ha" if trade.get("confluence") else "yoq"
    message = (
        f"GOLD XAUUSDT - {trade['signal']}\n"
        f"5m/15m: {bias15} | 1h: {bias1h or 'n/a'} | 4h: {bias4h or 'n/a'} | 1D: {bias1d or 'n/a'}\n"
        f"MTF: {mtf_score}/3 | Sessiya: {session}\n"
        f"Narx: {round(price, 2)}\n"
        f"Entry: {trade['entry']} | SL: {trade['sl']}\n"
        f"TP1: {trade['tp1']} | TP2: {trade['tp2']}\n"
        f"ATR(15m): {round(atr15, 2) if atr15 else 'n/a'}\n"
        f"BTC: {btc_bias15 or 'n/a'} | EUR: {eur_bias15 or 'n/a'}\n"
        f"VP: POC={vp15['poc'] if vp15 else 'n/a'} VAH={vp15['vah'] if vp15 else 'n/a'} VAL={vp15['val'] if vp15 else 'n/a'}\n"
        f"Delta: {delta15}\n"
        f"Confluence: {confluence_text}\n"
        f"Tasdiq: {', '.join(confirmations)}"
        f"{position_line}\n"
        f"Win rate: {win_rate_text(load_log())}"
    )
    logger.info(message.replace("\n", " | "))
    send_telegram(message)

    with state_lock:
        current_trade = trade
    _metrics["signals_generated"] += 1
    save_status(round(price, 2), bias5, bias15, bias1h, bias4h, bias1d)

# ==================== FLASK ====================
app = Flask(__name__)
BIAS_LABELS_UZ = {"bullish": "ko'tarilish", "bearish": "pasayish"}

def _bias_label(bias):
    return BIAS_LABELS_UZ.get(bias, "aniqlanmagan")

def _bias_dot_color(bias):
    if bias == "bullish":
        return "#5B9C6D"
    if bias == "bearish":
        return "#C0553B"
    return "#5B5445"

def _render_bias_chip(tf_label, bias):
    color = _bias_dot_color(bias)
    text = _bias_label(bias)
    return (
        f'<div class="chip"><span class="chip-dot" style="background:{color}"></span>'
        f'<span class="chip-tf">{html.escape(tf_label)}</span>'
        f'<span class="chip-val">{html.escape(text)}</span></div>'
    )

def _build_sparkline(prices, width=272, height=54, color="#C9A227"):
    if not prices or len(prices) < 2:
        return '<div class="spark-empty">narx tarixi to\'planmoqda&hellip;</div>'
    min_p, max_p = min(prices), max(prices)
    span = max_p - min_p
    if span == 0:
        span = max(abs(min_p) * 0.001, 1)
    step = width / (len(prices) - 1)
    pts = []
    for i, price in enumerate(prices):
        x = i * step
        y = height - ((price - min_p) / span) * (height - 8) - 4
        pts.append((x, y))
    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    last_x, last_y = pts[-1]
    fill_path = f"M0,{height} L" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts) + f" L{width},{height} Z"
    return f"""
    <svg viewBox="0 0 {width} {height}" width="100%" height="{height}" preserveAspectRatio="none" class="spark">
      <path d="{fill_path}" fill="url(#sparkfade)" stroke="none"/>
      <polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="1.6"
        stroke-linecap="round" stroke-linejoin="round"/>
      <circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="2.6" fill="{color}"/>
      <defs>
        <linearGradient id="sparkfade" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="{color}" stop-opacity="0.28"/>
          <stop offset="100%" stop-color="{color}" stop-opacity="0"/>
        </linearGradient>
      </defs>
    </svg>
    """

def _render_recent_trades(log, limit=5):
    if not log:
        return '<div class="muted-line">Hali yopilgan savdo yo\'q.</div>'
    icons = {"TP1": "✓", "TP2": "✓", "SL": "✗", "BE": "•", "MANUAL": "•", "BE/Trailing SL": "🛡"}
    colors = {"TP1": "#5B9C6D", "TP2": "#5B9C6D", "SL": "#C0553B", "BE": "#8C8271",
              "MANUAL": "#8C8271", "BE/Trailing SL": "#5B9C6D"}
    rows = []
    for entry in reversed(log[-limit:]):
        result = entry.get("result", "?")
        closed_at = (entry.get("closed_at") or "")[11:16]
        rows.append(
            f'<div class="trade-row">'
            f'<span class="trade-icon" style="color:{colors.get(result, "#8C8271")}">{icons.get(result, "•")}</span>'
            f'<span class="trade-side">{html.escape(str(entry.get("signal", "?")))}</span>'
            f'<span class="trade-result" style="color:{colors.get(result, "#8C8271")}">{html.escape(result)}</span>'
            f'<span class="trade-time">{html.escape(closed_at)}</span></div>'
        )
    return "".join(rows)

# ==================== ROUTES ====================
@app.route("/start_bot")
def start_bot():
    global paused
    with state_lock:
        paused = False
    return home()

@app.route("/pause_bot")
def pause_bot():
    global paused
    with state_lock:
        paused = True
    return home()

@app.route("/health")
def health():
    with state_lock:
        uptime = time.time() - _metrics["start_time"]
        has_trade = current_trade is not None
        subs_count = len(SUBSCRIBERS)
    return jsonify({
        "status": "ok",
        "uptime_sec": round(uptime, 1),
        "paused": paused,
        "has_open_trade": has_trade,
        "subscribers": subs_count,
        "metrics": _metrics,
    })

@app.route("/metrics")
def metrics():
    lines = [
        f"signals_total {_metrics['signals_generated']}",
        f"trades_closed_total {_metrics['trades_closed']}",
        f"wins_total {_metrics['wins']}",
        f"losses_total {_metrics['losses']}",
        f"errors_total {_metrics['errors']}",
        f"uptime_seconds {round(time.time() - _metrics['start_time'], 1)}",
    ]
    return "\n".join(lines), 200, {"Content-Type": "text/plain"}

@app.route("/stats")
def stats_page():
    return jsonify({"db_stats": db_stats(), "recent": db_get_recent_trades(20)})

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True, silent=True) or {}
        if data.get("secret") != WEBHOOK_SECRET:
            return jsonify({"ok": False, "error": "invalid secret"}), 403
        action = data.get("action", "").upper()
        if action in ("LONG", "SHORT"):
            send_telegram(
                f"📩 TradingView signal: {action} {data.get('symbol', '?')}\n"
                f"Narx: {data.get('price', '?')}\n"
                f"Xabar: {data.get('message', '')}"
            )
        return jsonify({"ok": True})
    except Exception as e:
        logger.error(f"Webhook xato: {_safe_log_error(e)}")
        return jsonify({"ok": False}), 500

@app.route("/")
def home():
    with state_lock:
        trade = dict(current_trade) if current_trade else None
        status_snapshot = dict(last_status)
        is_paused = paused
        price_history = list(PRICE_HISTORY)
        impulse_snapshot = dict(last_impulse_info) if last_impulse_info else None
        balance = ACCOUNT.get("balance")
        risk_pct = ACCOUNT.get("risk_pct", DEFAULT_RISK_PER_TRADE_PCT)

    price_value = status_snapshot["price"]
    price_display = f"${price_value:,.2f}" if price_value is not None else "&mdash;"
    time_display = status_snapshot["checked_at"] or "hali yangilanmadi"
    killzone_now = in_killzone()
    blackout_now, blackout_title = is_news_blackout()
    session = current_session()
    mtf_score = mtf_confluence_score(
        status_snapshot.get("bias5"), status_snapshot.get("bias15"),
        status_snapshot.get("bias1h"), status_snapshot.get("bias4h"),
    )

    if is_paused:
        control_button = '<a href="/start_bot" class="ctrl-btn ctrl-start"><span class="ctrl-icon"></span>start</a>'
    else:
        control_button = '<a href="/pause_bot" class="ctrl-btn ctrl-pause"><span class="ctrl-icon"></span>pause</a>'

    status_chips = "".join([
        f'<span class="tag">{"⏸ pauzada" if is_paused else "▶ ishlamoqda"}</span>',
        f'<span class="tag">{"🟢 killzone" if killzone_now else "⚪ tashqarida"}</span>',
        f'<span class="tag">🌐 {html.escape(session)}</span>',
        f'<span class="tag">📊 MTF {mtf_score}/3</span>',
        f'<span class="tag tag-warn">📰 {html.escape(blackout_title or "")}</span>' if blackout_now else "",
    ])

    bias_grid = "".join([
        _render_bias_chip("5m", status_snapshot.get("bias5")),
        _render_bias_chip("15m", status_snapshot.get("bias15")),
        _render_bias_chip("1h", status_snapshot.get("bias1h")),
        _render_bias_chip("4h", status_snapshot.get("bias4h")),
        _render_bias_chip("1D", status_snapshot.get("bias1d")),
    ])

    if trade:
        side_color = "#5B9C6D" if trade["signal"] == "LONG" else "#C0553B"
        trade_block = f"""
        <div class="trade-open">
          <span class="side-tag" style="background:{side_color}">{html.escape(trade['signal'])}</span>
          <div class="trade-grid">
            <div><span class="k">entry</span><span class="v">{trade['entry']}</span></div>
            <div><span class="k">sl</span><span class="v">{trade['sl']}</span></div>
            <div><span class="k">tp1</span><span class="v">{trade['tp1']}</span></div>
            <div><span class="k">tp2</span><span class="v">{trade['tp2']}</span></div>
          </div>
        </div>
        """
    else:
        trade_block = '<div class="muted-line italic">Hozircha ochiq savdo yo\'q.</div>'

    impulse_block = ""
    if impulse_snapshot:
        impulse_block = f"""
        <div class="section">
          <div class="section-title">So'nggi impuls</div>
          <div class="muted-line">
            {html.escape(impulse_snapshot['direction'])} {impulse_snapshot['range']}$
            &middot; {impulse_snapshot['ratio']}x &middot; {html.escape(impulse_snapshot['detected_at'])}
          </div>
        </div>
        """

    log = load_log()
    wins = sum(1 for e in log if e.get("result") in ("TP1", "TP2"))
    total = len(log)
    win_pct = f"{wins / total * 100:.0f}%" if total else "&mdash;"

    balance_line = f"${balance:,.2f} | risk {risk_pct}%" if balance else "/balance kiriting"

    return f"""<!doctype html>
    <html lang="uz"><head>
    <meta charset="utf-8">
    <meta http-equiv="refresh" content="5">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Gold Ticket &middot; XAUUSDT</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,400;0,9..144,600;1,9..144,500&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
      :root {{
        --bg:#14110D; --surface:#1C1710; --border:#33291C; --text:#EDE3CF;
        --muted:#8C8271; --gold:#C9A227; --gold-bright:#E4C047;
        --green:#5B9C6D; --red:#C0553B;
      }}
      * {{ box-sizing:border-box; }}
      body {{
        background:var(--bg); color:var(--text); margin:0; padding:32px 16px;
        font-family:'IBM Plex Mono', monospace;
        display:flex; justify-content:center;
      }}
      .ticket {{
        width:100%; max-width:400px; background:var(--surface);
        border:1px solid var(--border); border-radius:18px;
        padding:28px 26px 22px; position:relative; overflow:hidden;
        box-shadow:0 20px 50px rgba(0,0,0,0.45);
      }}
      .ticket::before {{
        content:""; position:absolute; top:0; left:0; right:0; height:10px;
        background:radial-gradient(circle at 10px 0, transparent 6px, var(--bg) 6.5px) repeat-x;
        background-size:20px 20px; background-position:-4px -5px;
      }}
      .eyebrow {{ font-size:0.78em; color:var(--muted); letter-spacing:0.02em; margin:6px 0 2px; }}
      .hero-price {{
        font-family:'Fraunces', serif; font-weight:600; font-size:2.7em;
        color:var(--text); line-height:1.1; margin:0;
      }}
      .hero-sub {{ color:var(--muted); font-size:0.82em; margin:4px 0 14px; }}
      .spark {{ display:block; margin:0 0 18px; }}
      .spark-empty {{ color:var(--muted); font-size:0.8em; margin:8px 0 18px; }}
      .tags {{ display:flex; flex-wrap:wrap; gap:6px; margin-bottom:16px; }}
      .tag {{
        font-size:0.72em; color:var(--muted); border:1px solid var(--border);
        border-radius:999px; padding:4px 10px;
      }}
      .tag-warn {{ color:var(--gold-bright); border-color:var(--gold); }}
      .ctrl-btn {{
        display:flex; align-items:center; justify-content:center; gap:10px; width:100%;
        padding:13px 20px; border-radius:12px; margin-bottom:20px;
        font-family:'IBM Plex Mono', monospace; font-weight:600; font-size:1em;
        text-decoration:none; color:#14110D; border:none;
      }}
      .ctrl-icon {{ width:16px; height:16px; background:#14110D; border-radius:4px; display:inline-block; }}
      .ctrl-start {{ background:linear-gradient(180deg, var(--gold-bright), var(--gold)); }}
      .ctrl-pause {{ background:linear-gradient(180deg, #d98a76, var(--red)); color:#1a0e0a; }}
      .ctrl-pause .ctrl-icon {{ background:#1a0e0a; }}
      .section {{ margin:18px 0; }}
      .section-title {{ font-size:0.78em; color:var(--muted); margin-bottom:8px; }}
      .chips {{ display:flex; flex-wrap:wrap; gap:8px; }}
      .chip {{
        display:flex; align-items:center; gap:6px; font-size:0.78em;
        border:1px solid var(--border); border-radius:8px; padding:6px 9px;
      }}
      .chip-dot {{ width:7px; height:7px; border-radius:50%; display:inline-block; }}
      .chip-tf {{ color:var(--muted); }}
      .chip-val {{ color:var(--text); }}
      .divider {{ border:none; height:0; border-top:1px dashed var(--border); margin:18px 0; }}
      .side-tag {{
        display:inline-block; font-size:0.75em; font-weight:600; color:#14110D;
        border-radius:6px; padding:3px 9px; margin-bottom:10px;
      }}
      .trade-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:8px 16px; }}
      .trade-grid .k {{ display:block; color:var(--muted); font-size:0.72em; }}
      .trade-grid .v {{ display:block; color:var(--text); font-size:0.95em; }}
      .muted-line {{ color:var(--muted); font-size:0.85em; }}
      .italic {{ font-family:'Fraunces', serif; font-style:italic; }}
      .stat-row {{ display:flex; align-items:baseline; gap:8px; margin-bottom:10px; }}
      .stat-num {{ font-family:'Fraunces', serif; font-size:1.6em; color:var(--gold-bright); }}
      .stat-label {{ color:var(--muted); font-size:0.78em; }}
      .trade-row {{
        display:flex; align-items:center; gap:10px; font-size:0.82em;
        padding:5px 0; border-bottom:1px solid var(--border);
      }}
      .trade-row:last-child {{ border-bottom:none; }}
      .trade-icon {{ width:14px; text-align:center; }}
      .trade-side {{ color:var(--text); flex:1; }}
      .trade-result {{ font-weight:600; }}
      .trade-time {{ color:var(--muted); }}
      .footer-note {{ color:var(--muted); font-size:0.72em; margin-top:20px; text-align:center; }}
      .balance-line {{ color:var(--gold-bright); font-size:0.82em; margin-top:8px; }}
    </style>
    </head><body>
      <div class="ticket">
        <div class="eyebrow">Gold ticket &middot; XAUUSDT</div>
        <div class="hero-price">{price_display}</div>
        <div class="hero-sub">yangilangan {html.escape(time_display)}</div>
        {_build_sparkline(price_history)}

        <div class="tags">{status_chips}</div>
        {control_button}

        <div class="section">
          <div class="section-title">Trend yo'nalishi</div>
          <div class="chips">{bias_grid}</div>
        </div>

        <hr class="divider">

        <div class="section">
          <div class="section-title">Ochiq savdo</div>
          {trade_block}
        </div>

        {impulse_block}

        <hr class="divider">

        <div class="section">
          <div class="stat-row">
            <span class="stat-num">{win_pct}</span>
            <span class="stat-label">g'alaba &middot; jami {total} ta savdo</span>
          </div>
          {_render_recent_trades(log)}
        </div>

        <div class="balance-line">Hisob: {balance_line}</div>
        <div class="footer-note">Signal/monitoring bot &middot; moliyaviy maslahat emas</div>
      </div>
    </body></html>
    """

# ==================== LOOP ====================
def loop():
    logger.info(f"Bot ishga tushdi - har {CHECK_INTERVAL_SEC}s")
    next_run = time.monotonic()
    while True:
        try:
            run()
        except Exception as e:
            logger.error(f"Xatolik: {_safe_log_error(e)}")
            logger.error(traceback.format_exc())
            _metrics["errors"] += 1
        next_run += CHECK_INTERVAL_SEC
        sleep_time = max(0, next_run - time.monotonic())
        if sleep_time > CHECK_INTERVAL_SEC * 2:
            next_run = time.monotonic() + CHECK_INTERVAL_SEC
            sleep_time = CHECK_INTERVAL_SEC
        time.sleep(sleep_time)

# ==================== RESTORE ====================
def restore_state():
    global current_trade, warned_flip
    if not os.path.exists(STATUS_FILE):
        return
    try:
        with open(STATUS_FILE, encoding='utf-8') as file:
            status = json.load(file)
        saved = status.get("currentTrade")
        if saved:
            with state_lock:
                current_trade = saved
                warned_flip = False
            age_text = _trade_age_text(saved.get("opened_at"))
            logger.info(f"Tiklandi: {saved.get('signal')} {saved.get('entry')}{age_text}")
            opened_at_iso = saved.get("opened_at")
            if opened_at_iso:
                try:
                    opened_at = datetime.datetime.fromisoformat(opened_at_iso)
                    elapsed_h = (datetime.datetime.now(datetime.timezone.utc) - opened_at).total_seconds() / 3600
                    if elapsed_h > TRADE_STALE_WARNING_HOURS:
                        send_telegram(
                            f"⚠️ Bot qayta ishga tushdi. {saved.get('signal')} "
                            f"savdo {elapsed_h:.1f} soat oldin ochilgan."
                        )
                except Exception:
                    pass
        else:
            logger.info("Ochiq bitim yo'q.")
    except json.JSONDecodeError:
        logger.error("status.json buzilgan.")
    except Exception as e:
        logger.error(f"State xato: {_safe_log_error(e)}")

# ==================== INIT (Railway / gunicorn uchun) ====================
SUBSCRIBERS = load_subscribers()
ACCOUNT = load_account()
restore_state()

_bot_started = False
_bot_lock = threading.Lock()


def _start_background_threads():
    """Thread'larni faqat BIR MARTA ishga tushirish."""
    global _bot_started
    with _bot_lock:
        if _bot_started:
            logger.info("Background thread'lar allaqachon ishga tushgan.")
            return
        _bot_started = True

    logger.info("Background thread'lar ishga tushirilmoqda...")
    threading.Thread(target=loop, daemon=True, name="main_loop").start()
    register_bot_commands()
    threading.Thread(target=telegram_listener, daemon=True, name="tg_listener").start()
    logger.info("✅ Background thread'lar ishga tushdi (loop + listener)")


# ✅ gunicorn ham, python ham ishlatishi uchun modul darajasida
_start_background_threads()


# ==================== ENTRY (faqat lokal test uchun) ====================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8100))
    logger.info(f"Flask {port} portda ishga tushmoqda (lokal rejim)")
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)
```

