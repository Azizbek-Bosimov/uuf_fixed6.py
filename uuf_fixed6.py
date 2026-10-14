"""
GOLD (XAUUSDT) 5m/15m SMC + ICT Bot — v12 FIXED FINAL
=====================================================
Barcha mantiqiy, texnik va xavfsizlik xatolari to'liq tuzatilgan versiya.
"""

import asyncio
import aiohttp
import aiosqlite
import datetime
import html
import json
import logging
import math
import os
import secrets
import time
import traceback
from collections import deque

try:
    from zoneinfo import ZoneInfo
    _HAS_ZONEINFO = True
except ImportError:
    _HAS_ZONEINFO = False

from aiohttp import web

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("gold_smc_bot_v12")

# ==================== KONFIGURATSIYA ====================
SYMBOL = "XAUUSDT"
SMT_SYMBOL = "XAGUSDT"
BTC_SYMBOL = "BTCUSDT"
LIMIT = 200
HTF_LIMIT = 100
RR1, RR2 = 1.5, 3.0
SWING_LEFT, SWING_RIGHT = 3, 3
CHECK_INTERVAL_SEC = 5
ZONE_MAX_DISTANCE_PCT = 0.4
MAX_RISK_PCT = 0.02
TRADE_STALE_WARNING_HOURS = 6
FLIP_WARN_INTERVAL_CHECKS = 10

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", BASE_DIR)
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    DATA_DIR = BASE_DIR
DB_FILE = os.path.join(DATA_DIR, "gold_bot.sqlite")

DEFAULT_RISK_PER_TRADE_PCT = 1.0
MAX_RISK_PER_TRADE_PCT = 5.0

# Bybit XAUUSDT perpetual uchun 1 lot = 1 troya unsiyasi.
XAUUSD_LOT_UNITS = 1
MIN_LOT_STEP = 0.01

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

KILLZONES_ENABLED = True
KILLZONES = [
    {"start": datetime.time(7, 0), "end": datetime.time(11, 0)},
    {"start": datetime.time(12, 0), "end": datetime.time(16, 0)},
]
SESSION_TZ = {
    "asia": "Asia/Tokyo",
    "london": "Europe/London",
    "newyork": "America/New_York",
}

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
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")
WEBHOOK_ENABLED = bool(WEBHOOK_SECRET)
if not WEBHOOK_ENABLED:
    logger.warning("⚠️ WEBHOOK_SECRET env da yo'q. Webhook endpoint o'chirilgan.")

PRICE_OFFSET = 0.0

# ==================== HELPERS ====================
def _safe_log_error(error):
    msg = str(error)
    if BOT_TOKEN:
        msg = msg.replace(BOT_TOKEN, "***")
    if WEBHOOK_SECRET:
        msg = msg.replace(WEBHOOK_SECRET, "***")
    return msg

def closed_only(candles):
    return candles[:-1] if len(candles) > 1 else candles

def _candles_signature(candles):
    """Har bir symbol va timeframe uchun unikal kesh kaliti."""
    if not candles:
        return None
    return (len(candles), candles[0][0], candles[-1][0], round(candles[0][1], 4))

def in_killzone():
    if not KILLZONES_ENABLED:
        return True
    now_utc = datetime.datetime.now(datetime.timezone.utc).time()
    for kz in KILLZONES:
        if kz["start"] <= now_utc <= kz["end"]:
            return True
    return False

def current_session():
    if _HAS_ZONEINFO:
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            lon_h = now_utc.astimezone(ZoneInfo(SESSION_TZ["london"])).hour
            ny_h = now_utc.astimezone(ZoneInfo(SESSION_TZ["newyork"])).hour
            tok_h = now_utc.astimezone(ZoneInfo(SESSION_TZ["asia"])).hour
            lon_act = 7 <= lon_h < 16
            ny_act = 8 <= ny_h < 17
            tok_act = 9 <= tok_h < 18
            if lon_act and ny_act:
                return "london_ny_overlap"
            if lon_act:
                return "london"
            if ny_act:
                return "newyork"
            if tok_act:
                return "asia"
            return "after_hours"
        except Exception:
            pass
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

# ==================== DATABASE (aiosqlite) ====================
class DatabaseManager:
    """Non-blocking SQLite state manager."""

    def __init__(self, db_path):
        self.db_path = db_path
        self._conn = None
        self._lock = asyncio.Lock()

    async def init(self):
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY, value TEXT
            )
        """)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, signal TEXT, entry REAL, sl REAL, tp1 REAL, tp2 REAL,
                result TEXT, close_price REAL, opened_at TEXT, closed_at TEXT,
                lot REAL, risk_usd REAL, confirmations TEXT
            )
        """)
        await self._conn.commit()
        logger.info(f"DB tayyor: {self.db_path}")

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def state_get(self, key, default=None):
        async with self._lock:
            async with self._conn.execute(
                "SELECT value FROM state WHERE key=?", (key,)
            ) as cur:
                row = await cur.fetchone()
                return json.loads(row[0]) if row else default

    async def state_set(self, key, value):
        async with self._lock:
            await self._conn.execute(
                "INSERT INTO state (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json.dumps(value, ensure_ascii=False)),
            )
            await self._conn.commit()

    async def insert_trade(self, trade, result, close_price, lot, risk_usd, confs):
        async with self._lock:
            await self._conn.execute("""
                INSERT INTO trades (symbol, signal, entry, sl, tp1, tp2, result,
                                    close_price, opened_at, closed_at, lot, risk_usd, confirmations)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                SYMBOL, trade["signal"], trade["entry"], trade["sl"],
                trade["tp1"], trade["tp2"], result, close_price,
                trade.get("opened_at"),
                datetime.datetime.now(datetime.timezone.utc).isoformat(),
                lot, risk_usd, confs,
            ))
            await self._conn.commit()

    async def get_recent_trades(self, limit=50):
        async with self._lock:
            async with self._conn.execute(
                "SELECT signal, result, entry, close_price, closed_at "
                "FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            ) as cur:
                rows = await cur.fetchall()
                return [
                    {"signal": r[0], "result": r[1], "entry": r[2],
                     "close_price": r[3], "closed_at": r[4]}
                    for r in rows
                ]

    async def get_all_results(self):
        async with self._lock:
            async with self._conn.execute("SELECT result FROM trades") as cur:
                rows = await cur.fetchall()
                return [r[0] for r in rows]

db = DatabaseManager(DB_FILE)

# ==================== MEMORY CACHE ====================
PRICE_HISTORY = deque(maxlen=500)
_zones_cache = {}
_swings_cache = {}
_zone_fresh_cache = {}
_metrics = {
    "signals": 0, "closed": 0, "wins": 0, "losses": 0,
    "errors": 0, "tg_errors": 0, "start": time.time(),
}

O, H, L, C = 1, 2, 3, 4
WIN_RESULTS = ("TP1", "TP2", "TP1 (Trailing)")

# ==================== SUBSCRIBERS / ACCOUNT ====================
async def get_subs():
    return set(await db.state_get("subscribers", [ADMIN_CHAT_ID]))

async def add_sub(chat_id):
    s = await get_subs()
    s.add(chat_id)
    await db.state_set("subscribers", list(s))

async def remove_sub(chat_id):
    s = await get_subs()
    s.discard(chat_id)
    await db.state_set("subscribers", list(s))

async def get_account():
    return await db.state_get(
        "account",
        {"balance": None, "risk_pct": DEFAULT_RISK_PER_TRADE_PCT},
    )

async def update_account(balance=None, risk=None):
    acc = await get_account()
    if balance is not None:
        acc["balance"] = balance
    if risk is not None:
        acc["risk_pct"] = risk
    await db.state_set("account", acc)

async def win_rate_text():
    results = await db.get_all_results()
    total = len(results)
    if not total:
        return "Hali statistika yo'q"
    wins = sum(1 for r in results if r in WIN_RESULTS)
    breakevens = sum(1 for r in results if r in ("BE", "BE/Trailing SL"))
    return f"{wins / total * 100:.1f}% g'alaba ({wins}/{total}), {breakevens} ta BE"

# ==================== BYBIT API (async) ====================
async def fetch_ohlcv(session, timeframe, symbol=SYMBOL, limit=LIMIT):
    imap = {"1m": "1", "5m": "5", "15m": "15", "30m": "30",
            "1h": "60", "4h": "240", "1d": "D"}
    interval = imap.get(timeframe, timeframe)
    url = "https://api.bybit.com/v5/market/kline"
    params = {"category": "linear", "symbol": symbol,
              "interval": interval, "limit": limit}
    async with session.get(url, params=params, headers=HEADERS, timeout=10) as r:
        r.raise_for_status()
        data = await r.json()
        rows = data.get("result", {}).get("list", [])
        if not rows:
            raise RuntimeError(f"Kline bo'sh: {symbol} {timeframe}")
        rows.sort(key=lambda x: int(x[0]))
        return [[int(x[0]), float(x[1]), float(x[2]), float(x[3]),
                 float(x[4]), float(x[5])] for x in rows]

# ==================== INDICATORS & SMC LOGIC ====================
def find_swings(candles, left=SWING_LEFT, right=SWING_RIGHT):
    highs, lows = [], []
    n = len(candles)
    if n < left + right + 1:
        return highs, lows
    for i in range(left, n - right):
        ch, cl = candles[i][H], candles[i][L]
        is_high, is_low = True, True
        for offset in range(-left, right + 1):
            if offset == 0:
                continue
            other = candles[i + offset]
            if other[H] >= ch and offset < 0:
                is_high = False
            elif other[H] > ch:
                is_high = False
            if other[L] <= cl and offset < 0:
                is_low = False
            elif other[L] < cl:
                is_low = False
            if not is_high and not is_low:
                break
        if is_high:
            highs.append((i, ch))
        if is_low:
            lows.append((i, cl))
    return highs, lows

def get_swings_cached(candles, left=SWING_LEFT, right=SWING_RIGHT):
    if not candles:
        return [], []
    sig = (_candles_signature(candles), left, right)
    if sig in _swings_cache:
        return _swings_cache[sig]
    h, l = find_swings(candles, left, right)
    if len(_swings_cache) > 100:
        _swings_cache.clear()
    _swings_cache[sig] = (h, l)
    return h, l

def confirmed_structure_bias(candles):
    """
    To'liq BOS / CHoCH holat mashinasi:
    Korreksiya (pullback) vaqtida ham oxirgi tasdiqlangan struktura yo'nalishini saqlab qoladi.
    """
    highs, lows = get_swings_cached(candles)
    if not highs or not lows:
        return None

    h_map = {idx: price for idx, price in highs}
    l_map = {idx: price for idx, price in lows}

    active_h = None
    active_l = None
    bias = None

    for i in range(len(candles)):
        c = candles[i][C]
        if active_h is not None and c > active_h:
            bias = "bullish"
            active_h = None  # Keyingi swing high hosil bo'lguncha kutadi
        elif active_l is not None and c < active_l:
            bias = "bearish"
            active_l = None

        # Swing nuqta o'ng tarafdan tasdiqlangach faollashadi
        confirm_idx = i - SWING_RIGHT
        if confirm_idx in h_map:
            active_h = h_map[confirm_idx]
        if confirm_idx in l_map:
            active_l = l_map[confirm_idx]

    return bias

def detect_impulse(candles, lookback=20, threshold=2.2):
    if len(candles) < lookback + 1:
        return None
    ranges = [c[H] - c[L] for c in candles]
    avg_range = sum(ranges[-lookback - 1:-1]) / lookback
    if avg_range == 0:
        return None
    last = candles[-1]
    last_range = last[H] - last[L]
    ratio = last_range / avg_range
    if ratio >= threshold:
        return {
            "ts": last[0],
            "direction": "yuqoriga" if last[C] > last[O] else "pastga",
            "range": round(last_range, 2),
            "ratio": round(ratio, 1),
            "price": round(last[C], 2),
        }
    return None

def detect_fvg(candles):
    fvgs = []
    for i in range(2, len(candles)):
        f, t = candles[i - 2], candles[i]
        if t[L] > f[H]:
            fvgs.append({"type": "bullish", "kind": "fvg",
                         "top": t[L], "bottom": f[H], "index": i})
        elif t[H] < f[L]:
            fvgs.append({"type": "bearish", "kind": "fvg",
                         "top": f[L], "bottom": t[H], "index": i})
    return fvgs

def detect_order_blocks(candles):
    bodies = [abs(c[C] - c[O]) for c in candles]
    vols = [c[5] if len(c) > 5 else 0 for c in candles]
    obs = []
    n = len(candles)
    for i in range(10, n - 1):
        ab = sum(bodies[i - 10:i]) / 10
        av = sum(vols[i - 10:i]) / 10
        if ab == 0 or av == 0:
            continue
        cur, fol = candles[i], candles[i + 1]
        ib = bodies[i + 1] > ab * 1.5
        iv = vols[i + 1] > av * 1.5

        # Displacement / Imbalance tekshiruvi
        has_fvg_bull = (i + 2 < n and candles[i + 2][L] > cur[H])
        has_fvg_bear = (i + 2 < n and candles[i + 2][H] < cur[L])

        bob = cur[C] < cur[O] and fol[C] > cur[H] and (ib and (iv or has_fvg_bull))
        beob = cur[C] > cur[O] and fol[C] < cur[L] and (ib and (iv or has_fvg_bear))

        if bob:
            obs.append({"type": "bullish", "kind": "ob",
                        "top": cur[O], "bottom": cur[L], "index": i})
        if beob:
            obs.append({"type": "bearish", "kind": "ob",
                        "top": cur[H], "bottom": cur[O], "index": i})
    return obs

def get_zones_cached(candles):
    if not candles:
        return []
    sig = _candles_signature(candles)
    if sig in _zones_cache:
        return _zones_cache[sig]
    zones = detect_fvg(candles) + detect_order_blocks(candles)
    if len(_zones_cache) > 100:
        _zones_cache.clear()
    _zones_cache[sig] = zones
    return zones

def _zone_is_fresh(zone, candles, max_age_bars=80):
    fi = zone["index"]
    n = len(candles)
    if n - 1 - fi > max_age_bars:
        return False
    sig = (_candles_signature(candles), fi, zone["kind"], zone["type"])
    if sig in _zone_fresh_cache:
        return _zone_fresh_cache[sig]

    res = True
    mid = (zone["top"] + zone["bottom"]) / 2.0

    # Impuls shamidan keyingi shamlarda zona buzilgani yoki 50% dan chuqur mitigated bo'lganini tekshirish
    start_check = fi + 2 if zone["kind"] == "ob" else fi + 1
    for i in range(start_check, n):
        c = candles[i]
        if zone["type"] == "bullish":
            if c[C] < zone["bottom"]:
                res = False
                break
            # Agar avvalgi shamlarda zonaning 50% qismiga kirib chiqib ketgan bo'lsa (mitigated)
            if i < n - 2 and c[L] <= mid and c[C] > zone["top"]:
                res = False
                break
        else:
            if c[C] > zone["top"]:
                res = False
                break
            if i < n - 2 and c[H] >= mid and c[C] < zone["bottom"]:
                res = False
                break

    if len(_zone_fresh_cache) > 500:
        _zone_fresh_cache.clear()
    _zone_fresh_cache[sig] = res
    return res

def calculate_atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        trs.append(max(
            candles[i][H] - candles[i][L],
            abs(candles[i][H] - candles[i - 1][C]),
            abs(candles[i][L] - candles[i - 1][C]),
        ))
    return sum(trs[-period:]) / period

def calculate_vwap(candles, fallback_lookback=48):
    """Kunlik (00:00 UTC dan boshlangan) haqiqiy Session VWAP."""
    if not candles:
        return None
    last_day = candles[-1][0] // 86_400_000
    session_candles = [c for c in candles if (c[0] // 86_400_000) == last_day]
    if len(session_candles) < 4:
        session_candles = candles[-fallback_lookback:]
    cpv, cv = 0.0, 0.0
    for c in session_candles:
        tp = (c[H] + c[L] + c[C]) / 3.0
        v = c[5] if len(c) > 5 else 0.0
        cpv += tp * v
        cv += v
    return cpv / cv if cv > 0 else None

async def adaptive_risk_pct(base_risk):
    results = await db.get_all_results()
    if len(results) < 10:
        return base_risk
    wins = sum(1 for r in results if r in WIN_RESULTS)
    losses = sum(1 for r in results if r == "SL")
    denom = wins + losses
    if denom < 10:
        return base_risk
    wr = wins / denom
    if wr > 0.65:
        return min(base_risk * 1.5, MAX_RISK_PER_TRADE_PCT)
    if wr < 0.40:
        return max(base_risk * 0.5, 0.1)
    return base_risk

def build_trade(candles, bias, price, atr):
    all_zones = [
        z for z in get_zones_cached(candles)
        if z["type"] == bias and _zone_is_fresh(z, candles)
    ]
    if not all_zones:
        return None
    all_zones.sort(key=lambda z: abs(price - (z["top"] + z["bottom"]) / 2))
    zone = all_zones[0]
    lower = min(zone["bottom"], zone["top"])
    upper = max(zone["bottom"], zone["top"])
    tol = price * (ZONE_MAX_DISTANCE_PCT / 100)
    if not (lower - tol <= price <= upper + tol):
        return None

    confluence = any(
        other["kind"] != zone["kind"]
        and other["bottom"] <= zone["top"]
        and other["top"] >= zone["bottom"]
        for other in all_zones
        if other["index"] != zone["index"] or other["kind"] != zone["kind"]
    )

    sl_buf = atr * 0.20 if atr else price * 0.001
    entry = price
    if bias == "bullish":
        sl = zone["bottom"] - sl_buf
        risk = entry - sl
        if risk <= 0 or risk > entry * MAX_RISK_PCT:
            return None
        return {
            "signal": "LONG", "bias": bias,
            "entry": round(entry, 2), "sl": round(sl, 2),
            "tp1": round(entry + risk * RR1, 2),
            "tp2": round(entry + risk * RR2, 2),
            "confluence": confluence,
            "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "breakeven": False, "tp1_notified": False, "stale_warned": False,
        }
    else:
        sl = zone["top"] + sl_buf
        risk = sl - entry
        if risk <= 0 or risk > entry * MAX_RISK_PCT:
            return None
        return {
            "signal": "SHORT", "bias": bias,
            "entry": round(entry, 2), "sl": round(sl, 2),
            "tp1": round(entry - risk * RR1, 2),
            "tp2": round(entry - risk * RR2, 2),
            "confluence": confluence,
            "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "breakeven": False, "tp1_notified": False, "stale_warned": False,
        }

def calc_lot(entry, sl, balance, risk_pct):
    if not balance or balance <= 0:
        return None
    r_usd = balance * (risk_pct / 100)
    p_risk = abs(entry - sl)
    if p_risk <= 0:
        return None
    lot = math.floor((r_usd / (p_risk * XAUUSD_LOT_UNITS)) / MIN_LOT_STEP + 1e-9) * MIN_LOT_STEP
    return round(max(lot, MIN_LOT_STEP), 2)

# ==================== TELEGRAM ====================
async def send_telegram(session, text, with_keyboard=False, chat_id=None):
    targets = [chat_id] if chat_id else list(await get_subs())
    if not targets:
        return
    markup = json.dumps(
        {"keyboard": [["📊 Signal"]], "resize_keyboard": True}
    ) if with_keyboard else None
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    for target in targets:
        payload = {"chat_id": target, "text": text}
        if markup:
            payload["reply_markup"] = markup
        for attempt in range(3):
            try:
                async with session.post(url, data=payload, timeout=10) as r:
                    if r.status == 429:
                        data = await r.json()
                        retry_after = data.get("parameters", {}).get("retry_after", 1)
                        logger.warning(f"TG 429: {retry_after}s kutish")
                        await asyncio.sleep(retry_after)
                        continue
                    if r.status == 200:
                        break
                    logger.error(f"TG xato {r.status} -> {target}")
                    _metrics["tg_errors"] += 1
                    break
            except Exception as e:
                logger.error(f"TG exception ({target}): {_safe_log_error(e)}")
                _metrics["tg_errors"] += 1
                break
        await asyncio.sleep(0.05)

# ==================== TRADING LOGIC ====================
async def close_trade(session, trade, result, price):
    await db.state_set("current_trade", None)
    acc = await get_account()
    adj_risk = await adaptive_risk_pct(acc["risk_pct"])
    lot = calc_lot(trade["entry"], trade["sl"], acc["balance"], adj_risk) if acc["balance"] else None
    usd_risk = round(lot * abs(trade["entry"] - trade["sl"]) * XAUUSD_LOT_UNITS, 2) if lot else None

    await db.insert_trade(trade, result, price, lot, usd_risk, trade.get("confirmations", ""))
    _metrics["closed"] += 1
    if result in WIN_RESULTS:
        _metrics["wins"] += 1
    elif result == "SL":
        _metrics["losses"] += 1

    emoji = {
        "TP2": "✅", "TP1 (Trailing)": "✅🛡", "SL": "❌",
        "BE": "➖", "MANUAL": "🛑", "BE/Trailing SL": "🛡",
    }.get(result, "ℹ️")
    stat = await win_rate_text()
    await send_telegram(
        session,
        f"GOLD {trade['signal']} yopildi {emoji} — {result} (narx {price})\n"
        f"Statistika: {stat}",
    )

async def monitor_trade(session, price, candle_high, candle_low, bias15):
    trade = await db.state_get("current_trade")
    if not trade:
        return

    side = trade["signal"]
    entry = trade["entry"]
    tp1 = trade["tp1"]
    tp2 = trade["tp2"]
    notes = []
    to_close = None

    high_p = max(price, candle_high)
    low_p = min(price, candle_low)

    # Oldingi SL holati bo'yicha birinchi tekshirish
    sl_hit = (side == "LONG" and low_p <= trade["sl"]) or (side == "SHORT" and high_p >= trade["sl"])
    tp2_hit = (side == "LONG" and high_p >= tp2) or (side == "SHORT" and low_p <= tp2)

    if sl_hit:
        if trade.get("tp1_notified"):
            to_close = ("TP1 (Trailing)", trade["sl"])
        elif trade.get("breakeven"):
            to_close = ("BE", trade["sl"])
        else:
            to_close = ("SL", trade["sl"])
    elif tp2_hit:
        to_close = ("TP2", tp2)
    else:
        tp1_hit = (side == "LONG" and high_p >= tp1) or (side == "SHORT" and low_p <= tp1)
        if tp1_hit and not trade.get("tp1_notified"):
            trade["tp1_notified"] = True
            notes.append(f"🎯 TP1 ga yetdi: {tp1}")

        if side == "LONG" and high_p >= tp1:
            new_sl = entry + (high_p - entry) * 0.5
            if new_sl > trade["sl"]:
                trade["sl"] = round(new_sl, 2)
                trade["breakeven"] = True
                notes.append(f"🔥 LONG trailing SL: {trade['sl']}")
        elif side == "SHORT" and low_p <= tp1:
            new_sl = entry - (entry - low_p) * 0.5
            if new_sl < trade["sl"]:
                trade["sl"] = round(new_sl, 2)
                trade["breakeven"] = True
                notes.append(f"🔥 SHORT trailing SL: {trade['sl']}")

        if bias15 is not None and bias15 != trade["bias"]:
            counter = (await db.state_get("flip_counter", 0)) + 1
            await db.state_set("flip_counter", counter)
            if counter >= FLIP_WARN_INTERVAL_CHECKS:
                notes.append(f"⚠️ {side} ochiq, lekin 15m struktura {bias15} ga o'zgardi.")
                await db.state_set("flip_counter", 0)

        # Eskirgan bitim (Stale trade) ogohlantirishi
        if trade.get("opened_at") and not trade.get("stale_warned"):
            try:
                opened_dt = datetime.datetime.fromisoformat(trade["opened_at"])
                age_hours = (datetime.datetime.now(datetime.timezone.utc) - opened_dt).total_seconds() / 3600
                if age_hours >= TRADE_STALE_WARNING_HOURS:
                    trade["stale_warned"] = True
                    notes.append(f"⏳ Bitim {TRADE_STALE_WARNING_HOURS} soatdan beri ochiq ({side}, Entry: {entry}).")
            except Exception:
                pass

    await db.state_set("current_trade", trade)
    for n in notes:
        await send_telegram(session, n)
    if to_close:
        await close_trade(session, trade, *to_close)
        await db.state_set("flip_counter", 0)

# ==================== NEWS ====================
_news_cache = {"events": None, "fetched_at": None}
NEWS_CACHE_TTL_SEC = 3600
NEWS_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
NEWS_BLOCK_MINUTES_BEFORE = 30
NEWS_BLOCK_MINUTES_AFTER = 30

async def fetch_high_impact_news(session):
    now = datetime.datetime.now(datetime.timezone.utc)
    cached = _news_cache.get("events")
    fetched_at = _news_cache.get("fetched_at")
    if cached is not None and fetched_at and (now - fetched_at).total_seconds() < NEWS_CACHE_TTL_SEC:
        return cached
    events = []
    try:
        async with session.get(NEWS_CALENDAR_URL, headers=HEADERS, timeout=10) as r:
            r.raise_for_status()
            data = await r.json()
            for item in data:
                if item.get("country") != "USD" or item.get("impact") != "High":
                    continue
                date_str = item.get("date", "")
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
        logger.warning(f"News fetch xato: {_safe_log_error(e)}")
        return cached if cached is not None else []

async def is_news_blackout(session):
    now = datetime.datetime.now(datetime.timezone.utc)
    for event in await fetch_high_impact_news(session):
        delta = (event["time"] - now).total_seconds() / 60
        if -NEWS_BLOCK_MINUTES_AFTER <= delta <= NEWS_BLOCK_MINUTES_BEFORE:
            return True, event["title"]
    return False, None

# ==================== HTF ====================
async def fetch_htf_bias(session):
    c_time = time.time()
    cache = await db.state_get("htf_cache", {"b1h": None, "b4h": None, "b1d": None, "ts": 0})
    if 0 <= c_time - cache.get("ts", 0) < 60:
        return cache["b1h"], cache["b4h"], cache["b1d"]

    tasks = [
        fetch_ohlcv(session, "1h", limit=HTF_LIMIT),
        fetch_ohlcv(session, "4h", limit=HTF_LIMIT),
        fetch_ohlcv(session, "1d", limit=HTF_LIMIT),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    def safe_bias(idx):
        r = results[idx]
        if isinstance(r, Exception):
            logger.debug(f"HTF[{idx}] xato: {_safe_log_error(r)}")
            return None
        return confirmed_structure_bias(closed_only(r))

    b1h, b4h, b1d = safe_bias(0), safe_bias(1), safe_bias(2)
    await db.state_set("htf_cache", {"b1h": b1h, "b4h": b4h, "b1d": b1d, "ts": c_time})
    return b1h, b4h, b1d

def mtf_alignment_score(b5, b15, b1h, b4h):
    score = 0
    if b5 and b15 and b5 == b15:
        score += 1
    if b15 and b1h and b15 == b1h:
        score += 1
    if b1h and b4h and b1h == b4h:
        score += 1
    return score

# ==================== SMT DIVERGENCE ====================
def detect_smt_divergence(gold_candles, ref_candles):
    """
    Haqiqiy ICT SMT Divergence:
    Oltin va korrelyatsiyadagi aktivning (XAGUSDT yoki BTCUSDT) oxirgi 2 ta swing nuqtalarini solishtiradi.
    """
    if not gold_candles or not ref_candles:
        return None
    gh, gl = get_swings_cached(gold_candles)
    rh, rl = get_swings_cached(ref_candles)

    bull_smt, bear_smt = False, False
    if len(gl) >= 2 and len(rl) >= 2:
        g_ll = gl[-1][1] < gl[-2][1]
        r_hl = rl[-1][1] > rl[-2][1]
        g_hl = gl[-1][1] > gl[-2][1]
        r_ll = rl[-1][1] < rl[-2][1]
        if (g_ll and r_hl) or (g_hl and r_ll):
            bull_smt = True

    if len(gh) >= 2 and len(rh) >= 2:
        g_hh = gh[-1][1] > gh[-2][1]
        r_lh = rh[-1][1] < rh[-2][1]
        g_lh = gh[-1][1] < gh[-2][1]
        r_hh = rh[-1][1] > rh[-2][1]
        if (g_hh and r_lh) or (g_lh and r_hh):
            bear_smt = True

    if bull_smt and not bear_smt:
        return "bullish"
    if bear_smt and not bull_smt:
        return "bearish"
    return None

# ==================== MAIN CYCLE ====================
async def run_bot_cycle(session):
    try:
        c15, c5 = await asyncio.gather(
            fetch_ohlcv(session, "15m"),
            fetch_ohlcv(session, "5m"),
        )
        b1h, b4h, b1d = await fetch_htf_bias(session)

        cl15 = closed_only(c15)
        cl5 = closed_only(c5)
        price = c15[-1][C] + PRICE_OFFSET
        candle_high = c5[-1][H] + PRICE_OFFSET
        candle_low = c5[-1][L] + PRICE_OFFSET

        b15 = confirmed_structure_bias(cl15)
        b5 = confirmed_structure_bias(cl5)
        PRICE_HISTORY.append(round(price, 2))

        status = {
            "price": price, "b5": b5, "b15": b15,
            "b1h": b1h, "b4h": b4h, "b1d": b1d,
            "time": time.strftime("%H:%M:%S"),
        }
        await db.state_set("last_status", status)

        impulse = detect_impulse(cl5)
        if impulse:
            last_ts = await db.state_get("last_impulse_ts")
            if impulse["ts"] != last_ts:
                await db.state_set("last_impulse_ts", impulse["ts"])
                await db.state_set("impulse_info", impulse)
                await send_telegram(
                    session,
                    f"⚡ IMPULS (5m): {impulse['direction']} {impulse['range']}$ "
                    f"({impulse['ratio']}x). Narx: {impulse['price']}.",
                )

        # Ochiq bitim bo'lsa — monitoring
        trade = await db.state_get("current_trade")
        if trade:
            await monitor_trade(session, price, candle_high, candle_low, b15)
            return

        # Pauza va Filtrlar
        if await db.state_get("paused", False):
            return
        if KILLZONES_ENABLED and not in_killzone():
            return

        blackout, title = await is_news_blackout(session)
        if blackout:
            logger.info(f"NONE — Yangilik: {title}")
            return

        if not b5 or not b15 or b5 != b15:
            return
        if b1h and b1h != b15:
            return
        if b4h and b4h != b15:
            return
        if b1d and b1d != b15:
            return

        atr15 = calculate_atr(cl15)
        new_trade = build_trade(cl15, b15, price, atr15)
        if not new_trade:
            return
        if atr15 and abs(new_trade["entry"] - new_trade["sl"]) < atr15 * 0.8:
            return

        # Confirmations
        rsis15 = calculate_rsi(cl15)
        vwap15 = calculate_vwap(cl15)
        bull_div, bear_div = detect_rsi_divergence(cl15, rsis15)
        sweep15 = detect_liquidity_sweep(cl15)
        delta15 = order_flow_delta(cl15)
        vp15 = volume_profile(cl15)
        session_name = current_session()

        btc_bias, smt_dir, xag_bias = None, None, None
        try:
            btc_c = await fetch_ohlcv(session, "15m", symbol=BTC_SYMBOL)
            btc_cl = closed_only(btc_c)
            btc_bias = confirmed_structure_bias(btc_cl)
        except Exception as e:
            logger.warning(f"BTC fetch: {_safe_log_error(e)}")

        try:
            xag_c = await fetch_ohlcv(session, "15m", symbol=SMT_SYMBOL)
            xag_cl = closed_only(xag_c)
            xag_bias = confirmed_structure_bias(xag_cl)
            smt_dir = detect_smt_divergence(cl15, xag_cl)
        except Exception:
            if btc_bias is not None:
                smt_dir = detect_smt_divergence(cl15, btc_cl)

        confirmations = confirmation_check(
            b15, price, vwap15, bull_div, bear_div,
            btc_bias, xag_bias, smt_dir,
            new_trade.get("confluence", False),
            sweep=sweep15, delta=delta15, vp=vp15,
        )

        # Kamida 2 ta HAQIQIY texnik tasdiq bo'lishi shart
        if len(confirmations) < 2:
            return

        mtf = mtf_alignment_score(b5, b15, b1h, b4h)
        acc = await get_account()
        adj_risk = await adaptive_risk_pct(acc["risk_pct"])
        lot = calc_lot(new_trade["entry"], new_trade["sl"], acc["balance"], adj_risk) if acc["balance"] else None

        new_trade["confirmations"] = ", ".join(confirmations)
        msg = (
            f"GOLD XAUUSDT — {new_trade['signal']}\n"
            f"5m/15m: {b15} | 1h: {b1h or 'n/a'} | 4h: {b4h or 'n/a'} | 1D: {b1d or 'n/a'}\n"
            f"MTF: {mtf}/3 | Sessiya: {session_name}\n"
            f"Narx: {round(price, 2)}\n"
            f"Entry: {new_trade['entry']} | SL: {new_trade['sl']}\n"
            f"TP1: {new_trade['tp1']} | TP2: {new_trade['tp2']}\n"
            f"ATR(15m): {round(atr15, 2) if atr15 else 'n/a'}\n"
            f"BTC: {btc_bias or 'n/a'} | XAG: {xag_bias or 'n/a'} | SMT: {smt_dir or 'yoq'}\n"
            f"VP: POC={vp15['poc'] if vp15 else 'n/a'} "
            f"VAH={vp15['vah'] if vp15 else 'n/a'} "
            f"VAL={vp15['val'] if vp15 else 'n/a'}\n"
            f"Delta: {delta15}\n"
            f"Confluence: {'ha' if new_trade.get('confluence') else 'yoq'}\n"
            f"Tasdiq ({len(confirmations)}): {', '.join(confirmations)}\n"
            f"Lot: {lot or 'Kiritilmagan'}\n"
            f"Win rate: {await win_rate_text()}"
        )
        logger.info(msg.replace("\n", " | "))
        await db.state_set("current_trade", new_trade)
        _metrics["signals"] += 1
        await send_telegram(session, msg)

    except Exception as e:
        logger.error(f"Cycle xato: {_safe_log_error(e)}")
        logger.error(traceback.format_exc())
        _metrics["errors"] += 1

# ==================== CONFIRMATIONS ====================
def confirmation_check(bias, price, vwap, bull_div, bear_div,
                       btc_bias=None, xag_bias=None, smt_dir=None,
                       zone_confluence=False, sweep=None, delta=None,
                       vp=None):
    confs = []
    if bias == "bullish":
        if bull_div:
            confs.append("RSI divergence (bullish)")
        if vwap is not None and price > vwap:
            confs.append("Narx Session VWAP ustida")
        if smt_dir == "bullish":
            confs.append("SMT Divergence (Bullish)")
        elif btc_bias == "bullish" and xag_bias == "bullish":
            confs.append("Korrelyatsiya: BTC + XAG bullish")
        if sweep == "bullish_sweep":
            confs.append("Likvidlik yig'ildi (Bullish Sweep)")
        if delta is not None and delta > 0:
            confs.append(f"Order Flow delta +{delta}")
        if vp and price <= vp["poc"]:
            confs.append("Discount (POC/VAL ostida)")
    else:
        if bear_div:
            confs.append("RSI divergence (bearish)")
        if vwap is not None and price < vwap:
            confs.append("Narx Session VWAP ostida")
        if smt_dir == "bearish":
            confs.append("SMT Divergence (Bearish)")
        elif btc_bias == "bearish" and xag_bias == "bearish":
            confs.append("Korrelyatsiya: BTC + XAG bearish")
        if sweep == "bearish_sweep":
            confs.append("Likvidlik yig'ildi (Bearish Sweep)")
        if delta is not None and delta < 0:
            confs.append(f"Order Flow delta {delta}")
        if vp and price >= vp["poc"]:
            confs.append("Premium (POC/VAH ustida)")

    if zone_confluence:
        confs.append("Zone confluence (FVG + OB)")
    return confs

# ==================== RSI / DIVERGENCE ====================
def calculate_rsi(candles, period=14):
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

def detect_rsi_divergence(candles, rsis, lookback=30):
    if len(candles) < lookback or lookback < 10:
        return False, False
    window = candles[-lookback:]
    offset = len(candles) - lookback
    highs, lows = find_swings(window)
    bull, bear = False, False
    if len(lows) >= 2:
        fi, fp = lows[-2]
        si, sp = lows[-1]
        fr, sr = rsis[offset + fi], rsis[offset + si]
        if fr is not None and sr is not None and sp < fp and sr > fr:
            bull = True
    if len(highs) >= 2:
        fi, fp = highs[-2]
        si, sp = highs[-1]
        fr, sr = rsis[offset + fi], rsis[offset + si]
        if fr is not None and sr is not None and sp > fp and sr < fr:
            bear = True
    return bull, bear

# ==================== LIQUIDITY SWEEP ====================
def detect_liquidity_sweep(candles):
    highs, lows = get_swings_cached(candles)
    if not highs or not lows:
        return None
    last_high = highs[-1][1]
    last_low = lows[-1][1]
    candidates = candles[-3:] if len(candles) >= 3 else candles[-1:]
    for c in reversed(candidates):
        if c[H] > last_high and c[C] < last_high:
            return "bearish_sweep"
        if c[L] < last_low and c[C] > last_low:
            return "bullish_sweep"
    return None

# ==================== VOLUME PROFILE ====================
def volume_profile(candles, bins=24, lookback=100):
    """Hajmni shamning Low va High oralig'idagi barcha binlarga proporsional taqsimlaydi."""
    window = candles[-lookback:] if len(candles) >= lookback else candles
    if len(window) < 2:
        return None
    highs = [c[H] for c in window]
    lows = [c[L] for c in window]
    pmin, pmax = min(lows), max(highs)
    if pmax <= pmin:
        return None
    bins = min(bins, max(2, len(window) // 2))
    bsize = (pmax - pmin) / bins
    profile = [0.0] * bins

    for c in window:
        v = c[5] if len(c) > 5 else 0.0
        if v <= 0:
            continue
        start_bin = max(0, min(bins - 1, int((c[L] - pmin) / bsize)))
        end_bin = max(0, min(bins - 1, int((c[H] - pmin) / bsize)))
        span = end_bin - start_bin + 1
        vol_per_bin = v / span
        for b in range(start_bin, end_bin + 1):
            profile[b] += vol_per_bin

    total = sum(profile)
    if total <= 0:
        return None

    poc_idx = profile.index(max(profile))
    poc = pmin + (poc_idx + 0.5) * bsize
    target = total * 0.70
    acc = profile[poc_idx]
    up, dn = poc_idx, poc_idx
    while acc < target and (up < bins - 1 or dn > 0):
        upv = profile[up + 1] if up < bins - 1 else -1
        dnv = profile[dn - 1] if dn > 0 else -1
        if upv >= dnv and up < bins - 1:
            up += 1
            acc += profile[up]
        elif dn > 0:
            dn -= 1
            acc += profile[dn]
        else:
            break
    vah = pmin + (up + 1) * bsize
    val = pmin + dn * bsize
    return {"poc": round(poc, 2), "vah": round(vah, 2), "val": round(val, 2)}

def order_flow_delta(candles, lookback=20):
    delta = 0.0
    for c in candles[-lookback:]:
        rng = c[H] - c[L]
        if rng == 0:
            continue
        v = c[5] if len(c) > 5 else 0.0
        delta += ((c[C] - c[O]) / rng) * v
    return round(delta, 2)

# ==================== TELEGRAM LISTENER ====================
async def telegram_listener(app):
    session = app["session"]
    offset = await db.state_get("tg_offset")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"

    while True:
        try:
            params = {"timeout": 25}
            if offset:
                params["offset"] = offset
            async with session.get(url, params=params, timeout=30) as r:
                if r.status != 200:
                    await asyncio.sleep(5)
                    continue
                data = await r.json()
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    await db.state_set("tg_offset", offset)
                    await _handle_update(app, upd)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"TG poller xato: {_safe_log_error(e)}")
            await asyncio.sleep(5)

async def _handle_update(app, upd):
    session = app["session"]
    msg = upd.get("message", {})
    text = (msg.get("text") or "").strip()
    cid = str(msg.get("chat", {}).get("id", ""))
    if not cid:
        return
    is_admin = cid == ADMIN_CHAT_ID

    if text == "/start":
        was = cid in await get_subs()
        await add_sub(cid)
        if not was:
            await send_telegram(
                session,
                "✅ Obuna bo'ldingiz!\n/signal — holat\n/stats — statistika\n"
                "/stop — bekor",
                with_keyboard=True, chat_id=cid,
            )
        else:
            await send_telegram(session, await build_status_text(), with_keyboard=True, chat_id=cid)
    elif text == "/stop":
        await remove_sub(cid)
        await send_telegram(session, "🔕 Obuna bekor qilindi.", chat_id=cid)
    elif text in ("/signal", "📊 Signal"):
        await send_telegram(session, await build_status_text(), with_keyboard=True, chat_id=cid)
    elif text == "/stats":
        await send_telegram(session, f"📊 {await win_rate_text()}", chat_id=cid)
    elif text == "/pause" and is_admin:
        await db.state_set("paused", True)
        await send_telegram(session, "⏸ Pauza.", chat_id=cid)
    elif text == "/resume" and is_admin:
        await db.state_set("paused", False)
        await send_telegram(session, "▶️ Davom.", chat_id=cid)
    elif text == "/close" and is_admin:
        t = await db.state_get("current_trade")
        st = await db.state_get("last_status", {})
        p = st.get("price")
        if t and p:
            await close_trade(session, t, "MANUAL", p)
            await send_telegram(session, "🛑 Yopildi.", chat_id=cid)
        else:
            await send_telegram(session, "Ochiq bitim yo'q.", chat_id=cid)
    elif text.startswith("/balance") and is_admin:
        parts = text.split()
        if len(parts) != 2:
            await send_telegram(session, "Foydalanish: /balance 500", chat_id=cid)
            return
        try:
            nb = float(parts[1].replace(",", "."))
            if nb <= 0:
                raise ValueError
            await update_account(balance=nb)
            await send_telegram(session, f"✅ Balans {nb:.2f}$ saqlandi.", chat_id=cid)
        except ValueError:
            await send_telegram(session, "Noto'g'ri. /balance 500", chat_id=cid)
    elif text.startswith("/risk") and is_admin:
        parts = text.split()
        if len(parts) != 2:
            await send_telegram(session, f"/risk 1 (maks {MAX_RISK_PER_TRADE_PCT}%)", chat_id=cid)
            return
        try:
            nr = float(parts[1].replace(",", "."))
            if not (0 < nr <= MAX_RISK_PER_TRADE_PCT):
                raise ValueError
            await update_account(risk=nr)
            await send_telegram(session, f"✅ Risk {nr}% saqlandi.", chat_id=cid)
        except ValueError:
            await send_telegram(session, f"0-{MAX_RISK_PER_TRADE_PCT}% oralig'ida.", chat_id=cid)

async def build_status_text():
    s = await db.state_get("last_status", {})
    if not s or s.get("price") is None:
        return "Bot hali birinchi tekshiruvni bajarmadi."
    paused = await db.state_get("paused", False)
    t = await db.state_get("current_trade")
    lines = [
        f"Holat: {'⏸ PAUZADA' if paused else '▶️ Ishlamoqda'}",
        f"Oxirgi tekshiruv: {s.get('time')}",
        f"Narx: {round(s['price'], 2)}",
        f"5m: {s.get('b5') or 'n/a'} | 15m: {s.get('b15') or 'n/a'}",
        f"1h: {s.get('b1h') or 'n/a'} | 4h: {s.get('b4h') or 'n/a'} | 1D: {s.get('b1d') or 'n/a'}",
        f"Sessiya: {current_session()}",
    ]
    if t:
        lines += [
            "",
            f"OCHIQ BITIM: {t['signal']}",
            f"Entry: {t['entry']} | SL: {t['sl']}",
            f"TP1: {t['tp1']} | TP2: {t['tp2']}",
        ]
    else:
        lines += ["", "Ochiq bitim yo'q — signal kutilmoqda."]
    lines.append(f"Statistika: {await win_rate_text()}")
    return "\n".join(lines)

# ==================== WEB ROUTES ====================
routes = web.RouteTableDef()

@routes.get("/")
async def home(request):
    s = await db.state_get("last_status", {})
    t = await db.state_get("current_trade")
    paused = await db.state_get("paused", False)
    acc = await get_account()
    price = s.get("price")
    price_disp = f"${price:,.2f}" if price else "---"

    trade_html = "Yo'q"
    if t:
        trade_html = html.escape(
            f"{t['signal']} | Entry: {t['entry']} | SL: {t['sl']} | "
            f"TP1: {t['tp1']} | TP2: {t['tp2']}"
        )

    html_page = f"""<!doctype html>
<html lang="uz"><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Gold SMC v12</title>
<style>
  body {{ background:#14110D; color:#EDE3CF; font-family: monospace; padding:20px; margin:0; }}
  .card {{ background:#1C1710; border:1px solid #33291C; border-radius:14px;
           padding:22px; max-width:420px; margin:auto; }}
  h2 {{ font-size:2em; margin:0 0 8px; }}
  .tag {{ display:inline-block; padding:4px 10px; border-radius:8px;
          background:#33291C; margin:4px 4px 4px 0; font-size:0.85em; }}
  hr {{ border:none; border-top:1px dashed #33291C; margin:16px 0; }}
  .muted {{ color:#8C8271; font-size:0.85em; }}
  .gold {{ color:#E4C047; }}
</style>
</head><body>
  <div class="card">
    <h2>{price_disp}</h2>
    <div class="muted">yangilangan: {html.escape(str(s.get('time', '---')))}</div>
    <div style="margin-top:10px">
      <span class="tag">{'⏸ Pauza (TG orqali)' if paused else '▶ Ishlamoqda'}</span>
      <span class="tag">5m: {html.escape(str(s.get('b5', '-')))}</span>
      <span class="tag">15m: {html.escape(str(s.get('b15', '-')))}</span>
      <span class="tag">1h: {html.escape(str(s.get('b1h', '-')))}</span>
      <span class="tag">4h: {html.escape(str(s.get('b4h', '-')))}</span>
      <span class="tag">1D: {html.escape(str(s.get('b1d', '-')))}</span>
    </div>
    <hr>
    <h3>Ochiq bitim</h3>
    <div>{trade_html}</div>
    <hr>
    <div class="muted">Hisob: ${acc.get('balance') or 0} | Risk: {acc.get('risk_pct')}%</div>
    <div class="gold" style="margin-top:8px">{html.escape(await win_rate_text())}</div>
  </div>
</body></html>"""
    return web.Response(text=html_page, content_type="text/html")

@routes.get("/health")
async def health(request):
    return web.json_response({
        "status": "ok",
        "uptime": round(time.time() - _metrics["start"], 1),
        "paused": await db.state_get("paused", False),
        "has_open_trade": bool(await db.state_get("current_trade")),
        "subscribers": len(await get_subs()),
        "webhook_enabled": WEBHOOK_ENABLED,
        "metrics": _metrics,
    })

@routes.get("/metrics")
async def metrics(request):
    lines = [
        f"signals_total {_metrics['signals']}",
        f"trades_closed_total {_metrics['closed']}",
        f"wins_total {_metrics['wins']}",
        f"losses_total {_metrics['losses']}",
        f"errors_total {_metrics['errors']}",
        f"telegram_errors_total {_metrics['tg_errors']}",
        f"uptime_seconds {round(time.time() - _metrics['start'], 1)}",
    ]
    return web.Response(text="\n".join(lines), content_type="text/plain")

@routes.get("/stats")
async def stats_page(request):
    recent = await db.get_recent_trades(20)
    return web.json_response({"recent": recent})

@routes.post("/webhook")
async def webhook(request):
    if not WEBHOOK_ENABLED:
        return web.json_response({"ok": False, "error": "disabled"}, status=403)
    try:
        data = await request.json()
        provided_secret = str(data.get("secret", ""))
        if not secrets.compare_digest(provided_secret, str(WEBHOOK_SECRET)):
            return web.json_response({"ok": False, "error": "invalid secret"}, status=403)

        action = str(data.get("action", "")).upper()
        if action not in ("LONG", "SHORT"):
            return web.json_response({"ok": False, "error": "invalid action"}, status=400)

        s = await db.state_get("last_status", {})
        b15 = s.get("b15")
        if await db.state_get("paused", False):
            return web.json_response({"ok": True, "skipped": "paused"})
        if await db.state_get("current_trade"):
            return web.json_response({"ok": True, "skipped": "trade_open"})

        expected = "bullish" if action == "LONG" else "bearish"
        if b15 and b15 != expected:
            asyncio.create_task(send_telegram(
                request.app["session"],
                f"📩 TV signal rad etildi: {action} (bias={b15})",
            ))
            return web.json_response({"ok": True, "skipped": "bias_mismatch"})

        # Agar webhook'da entry va sl berilgan bo'lsa, bitimni avto-kuzatuvga oladi
        if "entry" in data and "sl" in data:
            entry = float(data["entry"])
            sl = float(data["sl"])
            risk = abs(entry - sl)
            tp1 = float(data.get("tp1", entry + risk * RR1 if action == "LONG" else entry - risk * RR1))
            tp2 = float(data.get("tp2", entry + risk * RR2 if action == "LONG" else entry - risk * RR2))
            tv_trade = {
                "signal": action, "bias": expected,
                "entry": round(entry, 2), "sl": round(sl, 2),
                "tp1": round(tp1, 2), "tp2": round(tp2, 2),
                "confluence": True, "confirmations": "TradingView Webhook",
                "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "breakeven": False, "tp1_notified": False, "stale_warned": False,
            }
            await db.state_set("current_trade", tv_trade)
            _metrics["signals"] += 1

        asyncio.create_task(send_telegram(
            request.app["session"],
            f"📩 TradingView: {action} {data.get('symbol', SYMBOL)}\n"
            f"Narx: {data.get('price', data.get('entry', '?'))}\n{data.get('message', '')}",
        ))
        return web.json_response({"ok": True})
    except Exception as e:
        logger.error(f"Webhook xato: {_safe_log_error(e)}")
        return web.json_response({"ok": False}, status=500)

# ==================== BACKGROUND LOOPS ====================
async def trading_loop(app):
    session = app["session"]
    next_run = time.monotonic()
    while True:
        try:
            await run_bot_cycle(session)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Trading loop xato: {_safe_log_error(e)}")
            logger.error(traceback.format_exc())
            _metrics["errors"] += 1
        next_run += CHECK_INTERVAL_SEC
        sleep_time = max(0, next_run - time.monotonic())
        if sleep_time > CHECK_INTERVAL_SEC * 2:
            next_run = time.monotonic() + CHECK_INTERVAL_SEC
            sleep_time = CHECK_INTERVAL_SEC
        await asyncio.sleep(sleep_time)

async def register_bot_commands(session):
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
        async with session.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
            json={"commands": commands}, timeout=10,
        ) as r:
            if r.status == 200:
                data = await r.json()
                if data.get("ok"):
                    logger.info("Buyruqlar menyusi o'rnatildi.")
    except Exception as e:
        logger.error(f"setMyCommands xato: {_safe_log_error(e)}")

# ==================== STARTUP / CLEANUP ====================
async def startup_tasks(app):
    await db.init()
    app["session"] = aiohttp.ClientSession()
    await register_bot_commands(app["session"])
    app["bot_task"] = asyncio.create_task(trading_loop(app), name="trading_loop")
    app["tg_task"] = asyncio.create_task(telegram_listener(app), name="tg_listener")
    logger.info("✅ Bot ishga tushdi (v12 FIXED FINAL)")

async def cleanup_tasks(app):
    for key in ("bot_task", "tg_task"):
        t = app.get(key)
        if t:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
    if app.get("session"):
        await app["session"].close()
    await db.close()
    logger.info("🛑 Bot to'xtatildi.")

# ==================== APP FACTORY ====================
def create_app():
    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(startup_tasks)
    app.on_cleanup.append(cleanup_tasks)
    return app

app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8100))
    logger.info(f"Lokal rejim: http://0.0.0.0:{port}")
    web.run_app(app, host="0.0.0.0", port=port, access_log=None)