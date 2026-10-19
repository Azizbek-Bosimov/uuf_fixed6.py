"""
GOLD (XM / XAUUSDT) SMC + ICT Bot — v14 TITANIUM (REAL-TIME WS + GEMINI AI + 20$ SNIPER)
========================================================================================
Barcha modullar bitta faylga birlashtirildi:
  ✅ Real-time Bybit WebSocket (wss://stream.bybit.com/v5/public/linear) + Auto-Reconnect
  ✅ XM Broker 100% OHLC sinxronizatsiyasi (Default PRICE_OFFSET = -8.21 + /sync buyrug'i)
  ✅ 20$ Small Account Sniper Mode (15m POI + 5m Tight SL: 1.2$ - 2.6$ risk himoyasi)
  ✅ Gemini AI Tahlilchi (/ai buyrug'i + har bir signalda AI xulosasi, qo'shimcha kutubxonasiz)
  ✅ 5m LTF Trigger (CHoCH/BOS, 5m FVG, Pinbar/Engulfing) + Breaker Blocks + OTE + PDH/PDL/Asia Sweep
  ✅ Ichki PNG Grafik Chizma Dvigateli (/chart va signalda avtomatik rasm)
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
import struct
import time
import traceback
import zlib
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
logger = logging.getLogger("gold_smc_v14")

# ==================== KONFIGURATSIYA ====================
SYMBOL = "XAUUSDT"
BTC_SYMBOL = "BTCUSDT"
EUR_SYMBOL = "EURUSDT"
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

LIMIT = 240
HTF_LIMIT = 120

RR1, RR2, RR3 = 1.5, 3.0, 4.5
SWING_LEFT, SWING_RIGHT = 3, 3
CHECK_INTERVAL_SEC = 5
ZONE_MAX_DISTANCE_PCT = 0.35
MAX_RISK_PCT = 0.02
MIN_SIGNAL_SCORE = 55
TRADE_STALE_WARNING_HOURS = 6
FLIP_WARN_INTERVAL_CHECKS = 10

# 20$ Kichik balans uchun Sniper SL chegaralari (1 unsiya = 0.01 lot XM Standard da $ farqi)
DEFAULT_BALANCE_USD = 20.0
DEFAULT_RISK_PER_TRADE_PCT = 10.0  # 20$ balansda ~2.0$ risk
MAX_RISK_PER_TRADE_PCT = 25.0
SNIPER_MIN_SL_POINTS = 1.20        # Minimal 1.20$ SL
SNIPER_MAX_SL_POINTS_SMALL = 2.65  # 20$-50$ balansda maksimal 2.65$ SL (0.01 lotda -2.65$ dan oshmaydi)

# XM Brokerda 0.01 lot Standard = 1 troya unsiya (1$ harakat = 1$ foyda/zarar)
XM_MIN_LOT = 0.01
XM_OZ_PER_MIN_LOT = 1.0

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", BASE_DIR)
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    DATA_DIR = BASE_DIR
DB_FILE = os.path.join(DATA_DIR, "gold_bot.sqlite")

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

KILLZONES_ENABLED = True
KILLZONES = [
    {"name": "London Open", "start": datetime.time(6, 0), "end": datetime.time(11, 30)},
    {"name": "NY Open / Overlap", "start": datetime.time(12, 0), "end": datetime.time(17, 0)},
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
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

# XM va Bybit orasidagi default farq (-8.21)
DEFAULT_PRICE_OFFSET = float(os.environ.get("PRICE_OFFSET", "-8.21"))
_CURRENT_OFFSET = DEFAULT_PRICE_OFFSET
_LIVE_BYBIT_RAW_PRICE = None
_LIVE_PRICE = None
_WS_CONNECTED = False
_LAST_WS_MONITOR_TS = 0.0

# ==================== HELPERS ====================
def _safe_log_error(error):
    msg = str(error)
    for sec in (BOT_TOKEN, WEBHOOK_SECRET, GEMINI_API_KEY):
        if sec:
            msg = msg.replace(sec, "***")
    return msg

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
    def __init__(self, db_path):
        self.db_path = db_path
        self._conn = None
        self._lock = asyncio.Lock()

    async def init(self):
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
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
                SYMBOL, trade["signal"], trade["entry"], trade.get("initial_sl", trade["sl"]),
                trade["tp1"], trade["tp2"], result, close_price,
                trade.get("opened_at"),
                datetime.datetime.now(datetime.timezone.utc).isoformat(),
                lot, risk_usd, confs,
            ))
            await self._conn.commit()

    async def get_recent_trades(self, limit=30):
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
_LAST_CANDLES_15M = []
_LAST_STRUCT_15M = {}
_zones_cache = {}
_swings_cache = {}
_metrics = {
    "signals": 0, "closed": 0, "wins": 0, "losses": 0,
    "errors": 0, "tg_errors": 0, "ws_ticks": 0, "start": time.time(),
}

O, H, L, C, V = 1, 2, 3, 4, 5

def clear_analysis_caches():
    _zones_cache.clear()
    _swings_cache.clear()

# ==================== SUBSCRIBERS / ACCOUNT ====================
async def get_subs():
    return set(await db.state_get("subscribers", [ADMIN_CHAT_ID]))

async def add_sub(chat_id):
    s = await get_subs()
    s.add(str(chat_id))
    await db.state_set("subscribers", list(s))

async def remove_sub(chat_id):
    s = await get_subs()
    s.discard(str(chat_id))
    await db.state_set("subscribers", list(s))

async def get_account():
    acc = await db.state_get(
        "account",
        {"balance": DEFAULT_BALANCE_USD, "risk_pct": DEFAULT_RISK_PER_TRADE_PCT},
    )
    if not acc.get("balance"):
        acc["balance"] = DEFAULT_BALANCE_USD
    return acc

async def update_account(balance=None, risk=None):
    acc = await get_account()
    if balance is not None:
        acc["balance"] = balance
    if risk is not None:
        acc["risk_pct"] = risk
    await db.state_set("account", acc)

def _is_win_result(r):
    return r in ("TP1", "TP2", "TP3", "TP1 + Trailing SL")

async def win_rate_text():
    results = await db.get_all_results()
    total = len(results)
    if not total:
        return "Hali statistika yo'q"
    wins = sum(1 for r in results if _is_win_result(r))
    losses = sum(1 for r in results if r == "SL")
    be = total - wins - losses
    denom = wins + losses
    wr = (wins / denom * 100) if denom > 0 else 0.0
    return f"WR: {wr:.1f}% ({wins}✅ / {losses}❌ / {be}🛡 | Jami: {total})"

# ==================== GEMINI AI ENGINE (Zero-Dependency) ====================
async def ask_gemini_ai(session, prompt_text, system_instruction=None):
    """
    GEMINI_API_KEY bo'lsa, aiohttp orqali Gemini AI ga murojaat qiladi.
    """
    if not GEMINI_API_KEY:
        return None
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt_text}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 350},
    }
    if system_instruction:
        payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}
    try:
        async with session.post(url, json=payload, timeout=12) as r:
            if r.status != 200:
                logger.warning(f"Gemini API status: {r.status}")
                return None
            data = await r.json()
            candidates = data.get("candidates", [])
            if not candidates:
                return None
            parts = candidates[0].get("content", {}).get("parts", [])
            text = "".join(p.get("text", "") for p in parts).strip()
            return text or None
    except Exception as e:
        logger.warning(f"Gemini AI xato: {_safe_log_error(e)}")
        return None

async def generate_ai_signal_review(session, trade, status, confirmations):
    if not GEMINI_API_KEY:
        return None
    sys_prompt = (
        "Sen professional SMC va ICT oltin (XAUUSD) treyderisan. "
        "Foydalanuvchi XM brokerda 20$ kichik balans (0.01 lot) bilan savdo qiladi. "
        "Berilgan signalga 2-3 jumlada juda qisqa, aniq o'zbek tilida xulosa ber: "
        "kirish qanchalik xavfsiz, likvidlik tuzog'i (inducement) yo'qmi va TP1 da nima qilish kerak."
    )
    user_prompt = (
        f"Signal: {trade['signal']} | Grade: {trade.get('grade')} ({trade.get('score')}/100)\n"
        f"Entry: {trade['entry']} | SL: {trade['sl']} (Risk: {abs(trade['entry']-trade['sl']):.2f}$) | "
        f"TP1: {trade['tp1']} | TP2: {trade['tp2']}\n"
        f"MTF: 5m={status.get('b5')}, 15m={status.get('b15')}, 1H={status.get('b1h')}, 4H={status.get('b4h')}\n"
        f"EQ(50%): {status.get('eq15')} | PDH/PDL: {status.get('pdh')}/{status.get('pdl')}\n"
        f"Tasdiqlar: {', '.join(confirmations)}"
    )
    return await ask_gemini_ai(session, user_prompt, system_instruction=sys_prompt)

# ==================== PURE-PYTHON PNG CHART RENDERER ====================
_FONT_5X7 = {
    " ": [0, 0, 0, 0, 0, 0, 0],
    "0": [14, 17, 19, 21, 25, 17, 14],
    "1": [4, 12, 4, 4, 4, 4, 14],
    "2": [14, 17, 1, 6, 8, 16, 31],
    "3": [31, 2, 4, 2, 1, 17, 14],
    "4": [2, 6, 10, 18, 31, 2, 2],
    "5": [31, 16, 30, 1, 1, 17, 14],
    "6": [6, 8, 16, 30, 17, 17, 14],
    "7": [31, 1, 2, 4, 8, 8, 8],
    "8": [14, 17, 17, 14, 17, 17, 14],
    "9": [14, 17, 17, 15, 1, 2, 12],
    "A": [14, 17, 17, 31, 17, 17, 17],
    "B": [30, 17, 17, 30, 17, 17, 30],
    "C": [14, 17, 16, 16, 16, 17, 14],
    "D": [28, 18, 17, 17, 17, 18, 28],
    "E": [31, 16, 16, 30, 16, 16, 31],
    "F": [31, 16, 16, 30, 16, 16, 16],
    "G": [14, 17, 16, 23, 17, 17, 15],
    "H": [17, 17, 17, 31, 17, 17, 17],
    "I": [14, 4, 4, 4, 4, 4, 14],
    "J": [7, 2, 2, 2, 2, 18, 12],
    "K": [17, 18, 20, 24, 20, 18, 17],
    "L": [16, 16, 16, 16, 16, 16, 31],
    "M": [17, 27, 21, 21, 17, 17, 17],
    "N": [17, 25, 21, 19, 17, 17, 17],
    "O": [14, 17, 17, 17, 17, 17, 14],
    "P": [30, 17, 17, 30, 16, 16, 16],
    "Q": [14, 17, 17, 17, 21, 18, 13],
    "R": [30, 17, 17, 30, 20, 18, 17],
    "S": [15, 16, 16, 14, 1, 1, 30],
    "T": [31, 4, 4, 4, 4, 4, 4],
    "U": [17, 17, 17, 17, 17, 17, 14],
    "V": [17, 17, 17, 17, 17, 10, 4],
    "W": [17, 17, 17, 21, 21, 27, 17],
    "X": [17, 17, 10, 4, 10, 17, 17],
    "Y": [17, 17, 10, 4, 4, 4, 4],
    "Z": [31, 1, 2, 4, 8, 16, 31],
    ".": [0, 0, 0, 0, 0, 12, 12],
    ":": [0, 12, 12, 0, 12, 12, 0],
    "-": [0, 0, 0, 31, 0, 0, 0],
    "+": [0, 4, 4, 31, 4, 4, 0],
    "/": [1, 2, 4, 4, 8, 16, 0],
    "(": [2, 4, 8, 8, 8, 4, 2],
    ")": [8, 4, 2, 2, 2, 4, 8],
    "%": [25, 26, 4, 4, 8, 19, 19],
    "$": [4, 15, 20, 14, 5, 30, 4],
    "=": [0, 0, 31, 0, 31, 0, 0],
}

def render_smc_chart_png(candles, zones, struct_info, trade=None, width=820, height=460):
    window = candles[-65:] if len(candles) >= 65 else candles
    if len(window) < 10:
        return None

    bg_r, bg_g, bg_b = 16, 19, 26
    row_bytes = width * 3
    buf = bytearray([bg_r, bg_g, bg_b] * (width * height))

    def set_px(x, y, c):
        if 0 <= x < width and 0 <= y < height:
            idx = (y * width + x) * 3
            buf[idx], buf[idx + 1], buf[idx + 2] = c[0], c[1], c[2]

    def fill_rect(x0, y0, x1, y1, c, alpha=1.0):
        xa, xb = max(0, min(x0, x1)), min(width - 1, max(x0, x1))
        ya, yb = max(0, min(y0, y1)), min(height - 1, max(y0, y1))
        if alpha >= 0.99:
            for y in range(ya, yb + 1):
                base = y * row_bytes
                for x in range(xa, xb + 1):
                    i = base + x * 3
                    buf[i], buf[i + 1], buf[i + 2] = c[0], c[1], c[2]
        else:
            inv = 1.0 - alpha
            for y in range(ya, yb + 1):
                base = y * row_bytes
                for x in range(xa, xb + 1):
                    i = base + x * 3
                    buf[i] = int(buf[i] * inv + c[0] * alpha)
                    buf[i + 1] = int(buf[i + 1] * inv + c[1] * alpha)
                    buf[i + 2] = int(buf[i + 2] * inv + c[2] * alpha)

    def draw_hline(y, x0, x1, c, dashed=False):
        if not (0 <= y < height):
            return
        xa, xb = max(0, min(x0, x1)), min(width - 1, max(x0, x1))
        for x in range(xa, xb + 1):
            if not dashed or (x // 5) % 2 == 0:
                set_px(x, y, c)

    def draw_text(x, y, text, c, scale=1):
        cx = x
        for ch in str(text).upper():
            glyph = _FONT_5X7.get(ch, _FONT_5X7[" "])
            for gy in range(7):
                row_bits = glyph[gy]
                for gx in range(5):
                    if (row_bits >> (4 - gx)) & 1:
                        for sy in range(scale):
                            for sx in range(scale):
                                set_px(cx + gx * scale + sx, y + gy * scale + sy, c)
            cx += 6 * scale

    p_highs = [c[H] for c in window]
    p_lows = [c[L] for c in window]
    if trade:
        p_highs.extend([trade["entry"], trade["sl"], trade["tp1"], trade["tp2"]])
        p_lows.extend([trade["entry"], trade["sl"], trade["tp1"], trade["tp2"]])
    p_max = max(p_highs)
    p_min = min(p_lows)
    pad = max((p_max - p_min) * 0.08, 1.0)
    p_max += pad
    p_min -= pad

    margin_left, margin_right = 16, 118
    margin_top, margin_bottom = 38, 24
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    def price_to_y(p):
        return int(margin_top + (p_max - p) / (p_max - p_min) * plot_h)

    for i in range(6):
        gp = p_min + (p_max - p_min) * (i / 5.0)
        gy = price_to_y(gp)
        draw_hline(gy, margin_left, width - margin_right, (32, 38, 50), dashed=True)
        draw_text(width - margin_right + 8, gy - 3, f"{gp:.2f}", (135, 145, 165), scale=1)

    start_idx = len(candles) - len(window)
    n_bars = len(window)
    step_x = plot_w / max(n_bars, 1)

    for z in zones[-8:]:
        zy1 = price_to_y(z["top"])
        zy2 = price_to_y(z["bottom"])
        rel_idx = max(0, z["index"] - start_idx)
        zx1 = int(margin_left + rel_idx * step_x)
        zx2 = width - margin_right
        if z["type"] == "bullish":
            col = (22, 165, 115) if z["kind"] != "BB" else (55, 135, 225)
        else:
            col = (215, 60, 75) if z["kind"] != "BB" else (225, 145, 40)
        fill_rect(zx1, zy1, zx2, zy2, col, alpha=0.22)
        draw_hline(zy1, zx1, zx2, col)
        draw_hline(zy2, zx1, zx2, col)
        draw_text(zx2 - 42, min(zy1, zy2) + 3, z["kind"], col, scale=1)

    eq = struct_info.get("eq")
    if eq and p_min <= eq <= p_max:
        eq_y = price_to_y(eq)
        draw_hline(eq_y, margin_left, width - margin_right, (220, 185, 65), dashed=True)
        draw_text(margin_left + 4, eq_y - 9, f"EQ 50%: {eq:.2f}", (220, 185, 65), scale=1)

    candle_w = max(3, int(step_x * 0.64))
    for i, c in enumerate(window):
        cx = int(margin_left + (i + 0.5) * step_x)
        yh = price_to_y(c[H])
        yl = price_to_y(c[L])
        yo = price_to_y(c[O])
        yc = price_to_y(c[C])
        bull = c[C] >= c[O]
        col = (38, 198, 130) if bull else (234, 75, 90)
        fill_rect(cx, yh, cx, yl, col)
        b_top, b_bot = min(yo, yc), max(yo, yc)
        if b_bot == b_top:
            b_bot += 1
        fill_rect(cx - candle_w // 2, b_top, cx + candle_w // 2, b_bot, col)

    if trade:
        levels = [
            ("ENTRY", trade["entry"], (240, 240, 255)),
            ("SL", trade["sl"], (255, 75, 85)),
            ("TP1", trade["tp1"], (45, 210, 140)),
            ("TP2", trade["tp2"], (30, 235, 180)),
        ]
        for lbl, val, col in levels:
            if p_min <= val <= p_max:
                ly = price_to_y(val)
                draw_hline(ly, margin_left, width - margin_right, col, dashed=(lbl != "ENTRY"))
                draw_text(width - margin_right + 8, ly - 4, f"{lbl}:{val:.1f}", col, scale=1)

    last_p = _LIVE_PRICE if _LIVE_PRICE else window[-1][C]
    bias_str = (struct_info.get("bias") or "NEUTRAL").upper()
    hdr = f"XM GOLD 15M SMC | PRICE: {last_p:.2f} | BIAS: {bias_str}"
    draw_text(margin_left, 12, hdr, (235, 215, 155), scale=2)

    raw_data = bytearray()
    for y in range(height):
        raw_data.append(0)
        raw_data.extend(buf[y * row_bytes:(y + 1) * row_bytes])

    def png_chunk(c_type, c_data):
        crc = zlib.crc32(c_type + c_data) & 0xFFFFFFFF
        return struct.pack(">I", len(c_data)) + c_type + c_data + struct.pack(">I", crc)

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    idat = zlib.compress(bytes(raw_data), level=6)
    return (
        b"\x89PNG\r\n\x1a\n"
        + png_chunk(b"IHDR", ihdr)
        + png_chunk(b"IDAT", idat)
        + png_chunk(b"IEND", b"")
    )

# ==================== BYBIT API (Full OHLC XM Offset Sync) ====================
async def fetch_ohlcv(session, timeframe, symbol=SYMBOL, limit=LIMIT):
    """
    XAUUSDT so'ralganda _CURRENT_OFFSET ni har bir shamning O, H, L, C qiymatlariga
    qo'shadi — shunda barcha FVG/OB zonalar, EQ va PDH/PDL XM narxiga 100% mos tushadi.
    """
    imap = {"1m": "1", "5m": "5", "15m": "15", "30m": "30",
            "1h": "60", "4h": "240", "1d": "D"}
    interval = imap.get(timeframe, timeframe)
    url = "https://api.bybit.com/v5/market/kline"
    params = {"category": "linear", "symbol": symbol,
              "interval": interval, "limit": limit}

    offset = _CURRENT_OFFSET if symbol == SYMBOL else 0.0
    last_err = None
    for attempt in range(3):
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=10) as r:
                r.raise_for_status()
                data = await r.json()
                rows = data.get("result", {}).get("list", [])
                if not rows:
                    raise RuntimeError(f"Kline bo'sh: {symbol} {timeframe}")
                rows.sort(key=lambda x: int(x[0]))
                return [
                    [
                        int(x[0]),
                        round(float(x[1]) + offset, 2),
                        round(float(x[2]) + offset, 2),
                        round(float(x[3]) + offset, 2),
                        round(float(x[4]) + offset, 2),
                        float(x[5]),
                    ]
                    for x in rows
                ]
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.4 * (attempt + 1))
    raise last_err

# ==================== REAL-TIME WEBSOCKET LOOP ====================
async def bybit_ws_loop(app):
    """
    Bybit WebSocket orqali XAUUSDT narxini jonli (real-time, ~0.1s) oladi.
    Ochiq bitim bo'lsa, darhol monitor_trade() ni ishga tushiradi.
    """
    global _LIVE_BYBIT_RAW_PRICE, _LIVE_PRICE, _WS_CONNECTED, _LAST_WS_MONITOR_TS
    session = app["session"]

    while True:
        try:
            async with session.ws_connect(BYBIT_WS_URL, heartbeat=20, timeout=15) as ws:
                _WS_CONNECTED = True
                await ws.send_json({"op": "subscribe", "args": [f"tickers.{SYMBOL}"]})
                logger.info("⚡ Real-time Bybit WebSocket ulandi!")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        payload = json.loads(msg.data)
                        data = payload.get("data", {})
                        last_p_str = data.get("lastPrice")
                        if last_p_str:
                            raw_p = float(last_p_str)
                            _LIVE_BYBIT_RAW_PRICE = raw_p
                            live_xm = round(raw_p + _CURRENT_OFFSET, 2)
                            _LIVE_PRICE = live_xm
                            _metrics["ws_ticks"] += 1

                            now_mono = time.monotonic()
                            if now_mono - _LAST_WS_MONITOR_TS >= 0.35:
                                _LAST_WS_MONITOR_TS = now_mono
                                trade = await db.state_get("current_trade")
                                if trade:
                                    b15 = _LAST_STRUCT_15M.get("bias")
                                    await monitor_trade(session, live_xm, b15)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _WS_CONNECTED = False
            logger.warning(f"WS uzildi, 3s dan so'ng qayta ulanadi: {_safe_log_error(e)}")
            await asyncio.sleep(3)

# ==================== ADVANCED SMC & ICT ENGINE ====================
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
            if other[H] > ch:
                is_high = False
            if other[L] < cl:
                is_low = False
            if not is_high and not is_low:
                break
        if is_high:
            highs.append((i, ch))
        if is_low:
            lows.append((i, cl))
    return highs, lows

def get_swings_cached(candles, cache_key="default", left=SWING_LEFT, right=SWING_RIGHT):
    if not candles:
        return [], []
    ts = candles[-1][0]
    full_key = (cache_key, left, right, _CURRENT_OFFSET)
    cached = _swings_cache.get(full_key)
    if cached and cached["ts"] == ts:
        return cached["highs"], cached["lows"]
    h, l = find_swings(candles, left, right)
    _swings_cache[full_key] = {"ts": ts, "highs": h, "lows": l}
    return h, l

def analyze_market_structure(candles, cache_key="default"):
    highs, lows = get_swings_cached(candles, cache_key=cache_key)
    if len(highs) < 2 or len(lows) < 2:
        return {
            "bias": None, "event": None, "event_bar": -1,
            "dealing_high": None, "dealing_low": None, "eq": None,
            "ote_top": None, "ote_bottom": None,
        }

    events = [(idx, "H", p) for idx, p in highs] + [(idx, "L", p) for idx, p in lows]
    events.sort(key=lambda x: x[0])

    last_h = highs[0][1]
    last_l = lows[0][1]
    bias = None
    last_event = None
    event_bar = -1

    for i in range(1, len(candles)):
        close_p = candles[i][C]
        for s_idx, s_type, s_price in events:
            if s_idx + SWING_RIGHT == i:
                if s_type == "H":
                    last_h = s_price
                else:
                    last_l = s_price

        if close_p > last_h:
            last_event = "CHoCH_BULL" if bias == "bearish" else "BOS_BULL"
            bias = "bullish"
            last_h = candles[i][H]
            event_bar = i
        elif close_p < last_l:
            last_event = "CHoCH_BEAR" if bias == "bullish" else "BOS_BEAR"
            bias = "bearish"
            last_l = candles[i][L]
            event_bar = i

    if bias is None:
        if highs[-1][1] > highs[-2][1] and lows[-1][1] >= lows[-2][1]:
            bias = "bullish"
        elif lows[-1][1] < lows[-2][1] and highs[-1][1] <= highs[-2][1]:
            bias = "bearish"

    recent_highs = [p for _, p in highs[-4:]]
    recent_lows = [p for _, p in lows[-4:]]
    d_high = max(recent_highs) if recent_highs else candles[-1][H]
    d_low = min(recent_lows) if recent_lows else candles[-1][L]
    rng = max(d_high - d_low, 0.01)
    eq = (d_high + d_low) / 2.0

    if bias == "bullish":
        ote_top = d_high - rng * 0.618
        ote_bottom = d_high - rng * 0.786
    else:
        ote_bottom = d_low + rng * 0.618
        ote_top = d_low + rng * 0.786

    return {
        "bias": bias,
        "event": last_event,
        "event_bar": event_bar,
        "dealing_high": round(d_high, 2),
        "dealing_low": round(d_low, 2),
        "eq": round(eq, 2),
        "ote_top": round(max(ote_top, ote_bottom), 2),
        "ote_bottom": round(min(ote_top, ote_bottom), 2),
    }

def confirmed_structure_bias(candles, cache_key="default"):
    return analyze_market_structure(candles, cache_key=cache_key)["bias"]

def detect_equal_highs_lows(candles, cache_key="XAU_15m", tol_pct=0.06):
    highs, lows = get_swings_cached(candles, cache_key=cache_key)
    eqh, eql = None, None
    if len(highs) >= 2:
        h1, h2 = highs[-2][1], highs[-1][1]
        if abs(h1 - h2) / h2 * 100 <= tol_pct:
            eqh = round((h1 + h2) / 2.0, 2)
    if len(lows) >= 2:
        l1, l2 = lows[-2][1], lows[-1][1]
        if abs(l1 - l2) / l2 * 100 <= tol_pct:
            eql = round((l1 + l2) / 2.0, 2)
    return eqh, eql

def detect_asia_and_pd_sweeps(candles_15m, pdh, pdl):
    asia_high, asia_low = None, None
    now_date = datetime.datetime.now(datetime.timezone.utc).date()
    asia_bars = []
    for c in candles_15m[-60:]:
        dt = datetime.datetime.fromtimestamp(c[0] / 1000.0, tz=datetime.timezone.utc)
        if dt.date() == now_date and 0 <= dt.hour < 7:
            asia_bars.append(c)
    if len(asia_bars) >= 4:
        asia_high = round(max(c[H] for c in asia_bars), 2)
        asia_low = round(min(c[L] for c in asia_bars), 2)

    recent = candles_15m[-4:]
    bull_sweeps, bear_sweeps = [], []

    for c in recent:
        if pdl and c[L] < pdl and c[C] > pdl:
            bull_sweeps.append("PDL Sweep (Kecha Low likvidligi olindi)")
        if pdh and c[H] > pdh and c[C] < pdh:
            bear_sweeps.append("PDH Sweep (Kecha High likvidligi olindi)")
        if asia_low and c[L] < asia_low and c[C] > asia_low:
            bull_sweeps.append("Asia Low Sweep (Judas Swing)")
        if asia_high and c[H] > asia_high and c[C] < asia_high:
            bear_sweeps.append("Asia High Sweep (Judas Swing)")

    return {
        "asia_high": asia_high,
        "asia_low": asia_low,
        "bull_sweep": bull_sweeps[0] if bull_sweeps else None,
        "bear_sweep": bear_sweeps[0] if bear_sweeps else None,
    }

def detect_fvg(candles):
    fvgs = []
    for i in range(2, len(candles)):
        f, m, t = candles[i - 2], candles[i - 1], candles[i]
        mid_body = abs(m[C] - m[O])
        if t[L] > f[H] and mid_body > 0:
            fvgs.append({
                "type": "bullish", "kind": "FVG",
                "top": t[L], "bottom": f[H],
                "ce": (t[L] + f[H]) / 2.0, "index": i,
            })
        elif t[H] < f[L] and mid_body > 0:
            fvgs.append({
                "type": "bearish", "kind": "FVG",
                "top": f[L], "bottom": t[H],
                "ce": (f[L] + t[H]) / 2.0, "index": i,
            })
    return fvgs

def detect_ob_and_breakers(candles):
    bodies = [abs(c[C] - c[O]) for c in candles]
    vols = [c[V] if len(c) > 5 else 0 for c in candles]
    raw_obs = []
    for i in range(10, len(candles) - 2):
        ab = sum(bodies[i - 10:i]) / 10
        av = sum(vols[i - 10:i]) / 10
        if ab == 0 or av == 0:
            continue
        cur, fol = candles[i], candles[i + 1]
        ib = bodies[i + 1] > ab * 1.35
        iv = vols[i + 1] > av * 1.4
        bob = cur[C] < cur[O] and fol[C] > fol[O] and fol[C] > cur[H]
        beob = cur[C] > cur[O] and fol[C] < fol[O] and fol[C] < cur[L]
        if ib and iv and bob:
            raw_obs.append({
                "type": "bullish", "kind": "OB",
                "top": max(cur[O], cur[C]), "bottom": cur[L],
                "ce": (max(cur[O], cur[C]) + cur[L]) / 2.0, "index": i,
            })
        if ib and iv and beob:
            raw_obs.append({
                "type": "bearish", "kind": "OB",
                "top": cur[H], "bottom": min(cur[O], cur[C]),
                "ce": (cur[H] + min(cur[O], cur[C])) / 2.0, "index": i,
            })

    zones = []
    n = len(candles)
    for ob in raw_obs:
        fi = ob["index"]
        if n - 1 - fi > 60:
            continue
        broken_idx = None
        check_end = max(fi + 1, n - 2)
        for j in range(fi + 1, check_end):
            c = candles[j]
            if ob["type"] == "bullish" and c[C] < ob["bottom"]:
                broken_idx = j
                break
            if ob["type"] == "bearish" and c[C] > ob["top"]:
                broken_idx = j
                break

        if broken_idx is None:
            zones.append(ob)
        else:
            bb_type = "bearish" if ob["type"] == "bullish" else "bullish"
            bb_valid = True
            for k in range(broken_idx + 1, check_end):
                ck = candles[k]
                if bb_type == "bullish" and ck[C] < ob["bottom"]:
                    bb_valid = False
                    break
                if bb_type == "bearish" and ck[C] > ob["top"]:
                    bb_valid = False
                    break
            if bb_valid and (n - 1 - broken_idx <= 35):
                zones.append({
                    "type": bb_type, "kind": "BB",
                    "top": ob["top"], "bottom": ob["bottom"],
                    "ce": ob["ce"], "index": broken_idx,
                })
    return zones

def _fvg_is_fresh(zone, candles, max_age_bars=45):
    fi = zone["index"]
    if len(candles) - 1 - fi > max_age_bars:
        return False
    check_end = max(fi + 1, len(candles) - 2)
    for i in range(fi + 1, check_end):
        c = candles[i]
        if zone["type"] == "bullish" and c[C] < zone["bottom"]:
            return False
        if zone["type"] == "bearish" and c[C] > zone["top"]:
            return False
    return True

def get_active_zones(candles, cache_key="XAU_15m"):
    if not candles:
        return []
    ts = candles[-1][0]
    full_key = (cache_key, _CURRENT_OFFSET)
    cached = _zones_cache.get(full_key)
    if cached and cached["ts"] == ts:
        return cached["zones"]

    fvgs = [z for z in detect_fvg(candles) if _fvg_is_fresh(z, candles)]
    obs_bbs = detect_ob_and_breakers(candles)
    all_fresh = fvgs + obs_bbs
    _zones_cache[full_key] = {"ts": ts, "zones": all_fresh}
    return all_fresh

def check_5m_ltf_trigger(cl5, struct5, target_bias):
    triggers = []
    if struct5["bias"] == target_bias:
        ev = struct5.get("event") or ""
        if "CHoCH" in ev:
            triggers.append("5m CHoCH (MSS)")
        elif "BOS" in ev:
            triggers.append("5m BOS")

    zones_5m = get_active_zones(cl5, cache_key="XAU_5m")
    recent_5m_zones = [
        z for z in zones_5m
        if z["type"] == target_bias and (len(cl5) - 1 - z["index"] <= 6)
    ]
    if recent_5m_zones:
        triggers.append(f"5m {recent_5m_zones[-1]['kind']} Displacement")

    if len(cl5) >= 2:
        last, prev = cl5[-1], cl5[-2]
        rng = max(last[H] - last[L], 0.01)
        upper_wick = last[H] - max(last[O], last[C])
        lower_wick = min(last[O], last[C]) - last[L]

        if target_bias == "bullish":
            if last[C] > last[O] and prev[C] < prev[O] and last[C] > prev[O]:
                triggers.append("5m Bullish Engulfing")
            elif lower_wick / rng >= 0.50 and last[C] >= last[O]:
                triggers.append("5m Bullish Pinbar")
        else:
            if last[C] < last[O] and prev[C] > prev[O] and last[C] < prev[O]:
                triggers.append("5m Bearish Engulfing")
            elif upper_wick / rng >= 0.50 and last[C] <= last[O]:
                triggers.append("5m Bearish Pinbar")

    return triggers

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
            "direction": "yuqoriga 🚀" if last[C] > last[O] else "pastga 🩸",
            "range": round(last_range, 2),
            "ratio": round(ratio, 1),
            "price": round(last[C], 2),
        }
    return None

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

def calculate_vwap(candles, lookback=48):
    cpv, cv = 0.0, 0.0
    for c in candles[-lookback:]:
        tp = (c[H] + c[L] + c[C]) / 3.0
        v = c[V] if len(c) > 5 else 0.0
        cpv += tp * v
        cv += v
    return cpv / cv if cv > 0 else None

# ==================== 20$ SNIPER TRADE BUILDER ====================
def build_sniper_trade(cl15, cl5, struct15, price, atr15, balance):
    """
    15m POI ichida 5m LTF sham/zona yordamida 20$ balansga mos
    (1.20$ - 2.65$ SL) snayper bitim quradi.
    """
    bias = struct15["bias"]
    all_zones_15m = [
        z for z in get_active_zones(cl15, "XAU_15m")
        if z["type"] == bias
    ]
    if not all_zones_15m:
        return None

    all_zones_15m.sort(key=lambda z: abs(price - z["ce"]))
    zone = all_zones_15m[0]
    lower = min(zone["bottom"], zone["top"])
    upper = max(zone["bottom"], zone["top"])
    tol = price * (ZONE_MAX_DISTANCE_PCT / 100.0)
    if not (lower - tol <= price <= upper + tol):
        return None

    confluence = any(
        other["kind"] != zone["kind"]
        and other["bottom"] <= zone["top"]
        and other["top"] >= zone["bottom"]
        for other in all_zones_15m
    )

    is_small_acc = (balance or DEFAULT_BALANCE_USD) <= 60.0
    max_sl_pts = SNIPER_MAX_SL_POINTS_SMALL if is_small_acc else (atr15 * 2.5 if atr15 else 6.0)
    min_sl_pts = SNIPER_MIN_SL_POINTS

    # 5m oxirgi 4 ta shamning ekstremumi orqali tor Sniper SL topish
    recent_5m_low = min(c[L] for c in cl5[-4:]) if len(cl5) >= 4 else zone["bottom"]
    recent_5m_high = max(c[H] for c in cl5[-4:]) if len(cl5) >= 4 else zone["top"]
    entry = round(price, 2)

    if bias == "bullish":
        # 5m swing low yoki 15m zona osti + 0.25$ bufer
        candidate_sl = max(zone["bottom"] - 0.30, recent_5m_low - 0.25)
        raw_risk = entry - candidate_sl
        risk = min(max(raw_risk, min_sl_pts), max_sl_pts)
        sl = round(entry - risk, 2)
        return {
            "signal": "LONG", "bias": bias,
            "zone_kind": zone["kind"],
            "entry": entry, "sl": sl, "initial_sl": sl,
            "tp1": round(entry + risk * RR1, 2),
            "tp2": round(entry + risk * RR2, 2),
            "tp3": round(entry + risk * RR3, 2),
            "confluence": confluence,
            "sniper_mode": is_small_acc,
            "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "breakeven": False, "tp1_notified": False, "stale_warned": False,
        }
    else:
        candidate_sl = min(zone["top"] + 0.30, recent_5m_high + 0.25)
        raw_risk = candidate_sl - entry
        risk = min(max(raw_risk, min_sl_pts), max_sl_pts)
        sl = round(entry + risk, 2)
        return {
            "signal": "SHORT", "bias": bias,
            "zone_kind": zone["kind"],
            "entry": entry, "sl": sl, "initial_sl": sl,
            "tp1": round(entry - risk * RR1, 2),
            "tp2": round(entry - risk * RR2, 2),
            "tp3": round(entry - risk * RR3, 2),
            "confluence": confluence,
            "sniper_mode": is_small_acc,
            "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "breakeven": False, "tp1_notified": False, "stale_warned": False,
        }

def calc_xm_lot_and_risk(entry, sl, balance, risk_pct):
    """
    XM Standard/Ultra Low hisobi uchun:
    0.01 lot = 1 troya unsiya (1$ harakat = 1$ PnL).
    """
    bal = balance or DEFAULT_BALANCE_USD
    sl_dist = abs(entry - sl)
    if sl_dist <= 0:
        return XM_MIN_LOT, 0.0

    target_risk_usd = bal * (risk_pct / 100.0)
    raw_lots = (target_risk_usd / sl_dist) * XM_MIN_LOT
    lot = max(XM_MIN_LOT, math.floor(raw_lots / XM_MIN_LOT + 1e-9) * XM_MIN_LOT)
    lot = round(lot, 2)
    actual_risk_usd = round((lot / XM_MIN_LOT) * sl_dist * XM_OZ_PER_MIN_LOT, 2)
    return lot, actual_risk_usd

# ==================== TELEGRAM (Text + Photo) ====================
async def send_telegram(session, text, with_keyboard=False, chat_id=None, parse_mode="HTML"):
    targets = [chat_id] if chat_id else list(await get_subs())
    if not targets:
        return
    markup = json.dumps({
        "keyboard": [
            ["📊 Signal", "🖼 Grafik", "🧠 AI Tahlil"],
            ["🎯 Zonalar", "📰 Yangiliklar", "📈 Statistika"],
        ],
        "resize_keyboard": True,
    }) if with_keyboard else None
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    for target in targets:
        payload = {"chat_id": target, "text": text}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if markup:
            payload["reply_markup"] = markup
        for attempt in range(3):
            try:
                async with session.post(url, data=payload, timeout=10) as r:
                    if r.status == 429:
                        data = await r.json()
                        await asyncio.sleep(data.get("parameters", {}).get("retry_after", 1))
                        continue
                    if r.status == 200:
                        break
                    _metrics["tg_errors"] += 1
                    break
            except Exception as e:
                logger.error(f"TG exception ({target}): {_safe_log_error(e)}")
                _metrics["tg_errors"] += 1
                break
        await asyncio.sleep(0.05)

async def send_telegram_photo(session, png_bytes, caption="", chat_id=None):
    if not png_bytes:
        return
    targets = [chat_id] if chat_id else list(await get_subs())
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"

    for target in targets:
        for attempt in range(2):
            try:
                form = aiohttp.FormData()
                form.add_field("chat_id", str(target))
                form.add_field("caption", caption[:1024])
                form.add_field("parse_mode", "HTML")
                form.add_field("photo", png_bytes, filename="xm_gold_smc.png", content_type="image/png")
                async with session.post(url, data=form, timeout=15) as r:
                    if r.status == 200:
                        break
                    if r.status == 429:
                        data = await r.json()
                        await asyncio.sleep(data.get("parameters", {}).get("retry_after", 1))
                        continue
                    break
            except Exception as e:
                logger.error(f"TG photo xato ({target}): {_safe_log_error(e)}")
                break
        await asyncio.sleep(0.05)

# ==================== TRADING MANAGEMENT ====================
_monitor_lock = asyncio.Lock()

async def close_trade(session, trade, result, price):
    await db.state_set("current_trade", None)
    acc = await get_account()
    init_sl = trade.get("initial_sl", trade["sl"])
    lot, usd_risk = calc_xm_lot_and_risk(trade["entry"], init_sl, acc["balance"], acc["risk_pct"])

    await db.insert_trade(trade, result, price, lot, usd_risk, trade.get("confirmations", ""))
    _metrics["closed"] += 1
    if _is_win_result(result):
        _metrics["wins"] += 1
    elif result == "SL":
        _metrics["losses"] += 1

    pnl_pts = (price - trade["entry"]) if trade["signal"] == "LONG" else (trade["entry"] - price)
    pnl_usd = round((lot / XM_MIN_LOT) * pnl_pts, 2)

    emoji = {
        "TP2": "✅🏆", "TP3": "🚀👑", "TP1 + Trailing SL": "✅🛡",
        "SL": "❌", "BE": "➖", "MANUAL": "🛑",
    }.get(result, "ℹ️")
    stat = await win_rate_text()
    await send_telegram(
        session,
        f"<b>XM GOLD {trade['signal']} YOPILDI</b> {emoji}\n"
        f"Natija: <b>{html.escape(result)}</b> | Narx: <code>{price}</code>\n"
        f"💰 <b>Natija ({lot} lot):</b> <code>{pnl_usd:+.2f}$</code>\n"
        f"📊 {html.escape(stat)}",
    )

async def monitor_trade(session, price, bias15):
    if _monitor_lock.locked():
        return
    async with _monitor_lock:
        trade = await db.state_get("current_trade")
        if not trade:
            return

        side = trade["signal"]
        entry = trade["entry"]
        tp1 = trade["tp1"]
        tp2 = trade["tp2"]
        notes = []
        to_close = None

        tp1_hit = (side == "LONG" and price >= tp1) or (side == "SHORT" and price <= tp1)
        if tp1_hit and not trade.get("tp1_notified"):
            trade["tp1_notified"] = True
            trade["breakeven"] = True
            be_lock = max(abs(tp1 - entry) * 0.25, 0.30)
            new_sl = round(entry + be_lock, 2) if side == "LONG" else round(entry - be_lock, 2)
            if (side == "LONG" and new_sl > trade["sl"]) or (side == "SHORT" and new_sl < trade["sl"]):
                trade["sl"] = new_sl
            notes.append(
                f"🎯 <b>TP1 OLINDI ({tp1})!</b>\n"
                f"🛡 SL xavfsiz foydaga tortildi (BE+): <code>{trade['sl']}</code>\n"
                f"💡 <i>20$ balans tavsiyasi: bitimning 50% qismini yopib, qolganini TP2 ga ushlang!</i>"
            )

        if trade.get("tp1_notified"):
            if side == "LONG" and price > tp1:
                trail_sl = round(entry + (price - entry) * 0.55, 2)
                if trail_sl > trade["sl"] + 0.35:
                    trade["sl"] = trail_sl
                    notes.append(f"🔥 <b>LONG Real-time Trailing SL:</b> <code>{trade['sl']}</code>")
            elif side == "SHORT" and price < tp1:
                trail_sl = round(entry - (entry - price) * 0.55, 2)
                if trail_sl < trade["sl"] - 0.35:
                    trade["sl"] = trail_sl
                    notes.append(f"🔥 <b>SHORT Real-time Trailing SL:</b> <code>{trade['sl']}</code>")

        sl_hit = (side == "LONG" and price <= trade["sl"]) or (side == "SHORT" and price >= trade["sl"])
        tp2_hit = (side == "LONG" and price >= tp2) or (side == "SHORT" and price <= tp2)

        if sl_hit:
            res_label = "TP1 + Trailing SL" if trade.get("tp1_notified") else ("BE" if trade.get("breakeven") else "SL")
            to_close = (res_label, price)
        elif tp2_hit:
            to_close = ("TP2", price)
        elif bias15 is not None and bias15 != trade["bias"]:
            counter = (await db.state_get("flip_counter", 0)) + 1
            await db.state_set("flip_counter", counter)
            if counter >= FLIP_WARN_INTERVAL_CHECKS:
                notes.append(f"⚠️ <b>Diqqat:</b> {side} ochiq, lekin 15m struktura <b>{bias15.upper()}</b> ga o'zgardi!")
                await db.state_set("flip_counter", 0)

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
        delta = (event["time"] - now).total_seconds() / 60.0
        if -NEWS_BLOCK_MINUTES_AFTER <= delta <= NEWS_BLOCK_MINUTES_BEFORE:
            return True, event["title"]
    return False, None

# ==================== HTF & PDH/PDL ====================
async def fetch_htf_context(session):
    c_time = time.monotonic()
    cache = await db.state_get(
        "htf_cache",
        {"b1h": None, "b4h": None, "b1d": None, "pdh": None, "pdl": None, "ts": 0, "offset": None},
    )
    if c_time - cache.get("ts", 0) < 60 and cache.get("offset") == _CURRENT_OFFSET:
        return cache

    tasks = [
        fetch_ohlcv(session, "1h", limit=HTF_LIMIT),
        fetch_ohlcv(session, "4h", limit=HTF_LIMIT),
        fetch_ohlcv(session, "1d", limit=HTF_LIMIT),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    def safe_bias(idx, tf_name):
        r = results[idx]
        if isinstance(r, Exception):
            return None
        return confirmed_structure_bias(closed_only(r), cache_key=f"XAU_{tf_name}")

    b1h = safe_bias(0, "1h")
    b4h = safe_bias(1, "4h")
    b1d = safe_bias(2, "1d")

    pdh, pdl = None, None
    if not isinstance(results[2], Exception) and len(results[2]) >= 2:
        prev_day = results[2][-2]
        pdh = round(prev_day[H], 2)
        pdl = round(prev_day[L], 2)

    data = {
        "b1h": b1h, "b4h": b4h, "b1d": b1d,
        "pdh": pdh, "pdl": pdl, "ts": c_time, "offset": _CURRENT_OFFSET,
    }
    await db.state_set("htf_cache", data)
    return data

def mtf_alignment_score(b5, b15, b1h, b4h):
    score = 0
    if b5 and b15 and b5 == b15:
        score += 1
    if b15 and b1h and b15 == b1h:
        score += 1
    if b1h and b4h and b1h == b4h:
        score += 1
    return score

# ==================== INDICATORS & INSTITUTIONAL SCORING ====================
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

def detect_rsi_divergence(candles, rsis, lookback=24):
    if len(candles) < lookback or lookback < 7:
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

def detect_liquidity_sweep(candles, cache_key="XAU_15m"):
    highs, lows = get_swings_cached(candles, cache_key=cache_key)
    if not highs or not lows:
        return None
    last_high = highs[-1][1]
    last_low = lows[-1][1]
    for c in candles[-3:]:
        if c[H] > last_high and c[C] < last_high:
            return "bearish_sweep"
        if c[L] < last_low and c[C] > last_low:
            return "bullish_sweep"
    return None

def detect_smt_divergence(gold_candles, other_candles, other_key):
    if not gold_candles or not other_candles:
        return None
    gh, gl = get_swings_cached(gold_candles, cache_key="XAU_15m")
    oh, ol = get_swings_cached(other_candles, cache_key=other_key)
    if len(gl) >= 2 and len(ol) >= 2:
        if gl[-1][1] < gl[-2][1] and ol[-1][1] > ol[-2][1]:
            return "bullish"
    if len(gh) >= 2 and len(oh) >= 2:
        if gh[-1][1] > gh[-2][1] and oh[-1][1] < oh[-2][1]:
            return "bearish"
    return None

def volume_profile(candles, bins=20, lookback=100):
    window = candles[-lookback:] if len(candles) >= lookback else candles
    if len(window) < 2:
        return None
    highs = [c[H] for c in window]
    lows = [c[L] for c in window]
    pmin, pmax = min(lows), max(highs)
    if pmax == pmin:
        return None
    bins = min(bins, max(2, len(window) // 2))
    bsize = (pmax - pmin) / bins
    profile = [0.0] * bins
    for c in window:
        v = c[V] if len(c) > 5 else 0
        idx = int((c[C] - pmin) / bsize)
        idx = max(0, min(bins - 1, idx))
        profile[idx] += v
    poc_idx = profile.index(max(profile))
    poc = pmin + (poc_idx + 0.5) * bsize
    total = sum(profile)
    target = total * 0.7
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
        v = c[V] if len(c) > 5 else 0.0
        delta += ((c[C] - c[O]) / rng) * v
    return round(delta, 2)

def evaluate_institutional_score(
    struct15, price, ltf_triggers, mtf_score, vwap, bull_div, bear_div,
    btc_bias, eur_bias, smt_eur, smt_btc, zone_confluence, sweep15,
    pd_asia_info, eqh, eql, delta, vp, session_name
):
    bias = struct15["bias"]
    eq = struct15.get("eq")
    ote_top = struct15.get("ote_top")
    ote_bot = struct15.get("ote_bottom")
    event = struct15.get("event")

    score = 0
    confs = []

    if ltf_triggers:
        score += 20
        confs.append(" + ".join(ltf_triggers[:2]))

    if mtf_score >= 3:
        score += 15
        confs.append("MTF 3/3 (Full)")
    elif mtf_score == 2:
        score += 10
        confs.append("MTF 2/3")

    if ote_bot and ote_top and ote_bot <= price <= ote_top:
        score += 15
        confs.append("ICT OTE (0.618-0.786)")
    elif bias == "bullish" and eq and price <= eq:
        score += 10
        confs.append("ICT Discount (<50% EQ)")
    elif bias == "bearish" and eq and price >= eq:
        score += 10
        confs.append("ICT Premium (>50% EQ)")

    if bias == "bullish":
        if pd_asia_info.get("bull_sweep"):
            score += 15
            confs.append(pd_asia_info["bull_sweep"])
        elif sweep15 == "bullish_sweep":
            score += 12
            confs.append("15m SSL Sweep")
        if eqh and eqh > price:
            score += 5
            confs.append(f"Target BSL (EQH @ {eqh})")
    else:
        if pd_asia_info.get("bear_sweep"):
            score += 15
            confs.append(pd_asia_info["bear_sweep"])
        elif sweep15 == "bearish_sweep":
            score += 12
            confs.append("15m BSL Sweep")
        if eql and eql < price:
            score += 5
            confs.append(f"Target SSL (EQL @ {eql})")

    if zone_confluence:
        score += 8
        confs.append("FVG + OB/BB Confluence")
    if event and ("CHoCH" in event or "BOS" in event):
        score += 5
        confs.append(f"15m {event}")

    if bias == "bullish":
        if smt_eur == "bullish" or smt_btc == "bullish":
            score += 10
            confs.append("ICT SMT Divergence (Bullish)")
        elif btc_bias == "bullish" and eur_bias == "bullish":
            score += 6
            confs.append("SMT: BTC+EUR Bullish")
    else:
        if smt_eur == "bearish" or smt_btc == "bearish":
            score += 10
            confs.append("ICT SMT Divergence (Bearish)")
        elif btc_bias == "bearish" and eur_bias == "bearish":
            score += 6
            confs.append("SMT: BTC+EUR Bearish")

    if bias == "bullish":
        if bull_div:
            score += 6
            confs.append("RSI Bullish Div")
        if vwap and price > vwap:
            score += 4
            confs.append("VWAP ustida")
        if delta and delta > 0:
            score += 4
            confs.append(f"Delta (+{delta})")
        if vp and price <= vp["val"]:
            score += 4
            confs.append("VP VAL Discount")
    else:
        if bear_div:
            score += 6
            confs.append("RSI Bearish Div")
        if vwap and price < vwap:
            score += 4
            confs.append("VWAP ostida")
        if delta and delta < 0:
            score += 4
            confs.append(f"Delta ({delta})")
        if vp and price >= vp["vah"]:
            score += 4
            confs.append("VP VAH Premium")

    if session_name in ("london", "london_ny_overlap", "newyork"):
        score += 5
        confs.append(f"Killzone ({session_name})")

    score = min(score, 100)
    grade = "A+" if score >= 80 else ("A" if score >= 68 else "B+")
    return score, grade, confs

# ==================== MAIN CYCLE ====================
async def run_bot_cycle(session):
    global _LAST_CANDLES_15M, _LAST_STRUCT_15M, _LIVE_BYBIT_RAW_PRICE, _LIVE_PRICE
    try:
        c15, c5 = await asyncio.gather(
            fetch_ohlcv(session, "15m"),
            fetch_ohlcv(session, "5m"),
        )
        htf = await fetch_htf_context(session)
        b1h, b4h, b1d = htf["b1h"], htf["b4h"], htf["b1d"]
        pdh, pdl = htf["pdh"], htf["pdl"]

        cl15 = closed_only(c15)
        cl5 = closed_only(c5)

        # Agar WebSocket ishlayotgan bo'lsa jonli narxni olamiz, aks holda REST narxini
        rest_xm_price = round(c15[-1][C], 2)
        if _LIVE_BYBIT_RAW_PRICE is None:
            _LIVE_BYBIT_RAW_PRICE = round(rest_xm_price - _CURRENT_OFFSET, 2)
        price = _LIVE_PRICE if (_WS_CONNECTED and _LIVE_PRICE) else rest_xm_price

        struct15 = analyze_market_structure(cl15, cache_key="XAU_15m")
        struct5 = analyze_market_structure(cl5, cache_key="XAU_5m")
        b15 = struct15["bias"]
        b5 = struct5["bias"]
        atr15 = calculate_atr(cl15)
        pd_asia = detect_asia_and_pd_sweeps(cl15, pdh, pdl)
        eqh, eql = detect_equal_highs_lows(cl15, cache_key="XAU_15m")

        _LAST_CANDLES_15M = cl15
        _LAST_STRUCT_15M = struct15
        PRICE_HISTORY.append(price)

        status = {
            "price": price, "b5": b5, "b15": b15,
            "b1h": b1h, "b4h": b4h, "b1d": b1d,
            "eq15": struct15.get("eq"),
            "ote_top": struct15.get("ote_top"),
            "ote_bottom": struct15.get("ote_bottom"),
            "pdh": pdh, "pdl": pdl,
            "asia_high": pd_asia["asia_high"], "asia_low": pd_asia["asia_low"],
            "eqh": eqh, "eql": eql,
            "event15": struct15.get("event"),
            "atr15": round(atr15, 2) if atr15 else None,
            "offset": _CURRENT_OFFSET,
            "ws": _WS_CONNECTED,
            "time": time.strftime("%H:%M:%S"),
        }
        await db.state_set("last_status", status)

        impulse = detect_impulse(cl5)
        if impulse:
            last_ts = await db.state_get("last_impulse_ts")
            if impulse["ts"] != last_ts:
                await db.state_set("last_impulse_ts", impulse["ts"])
                await send_telegram(
                    session,
                    f"⚡ <b>XM GOLD IMPULS (5m):</b> {impulse['direction']} <code>{impulse['range']}$</code> "
                    f"({impulse['ratio']}x) | Narx: <code>{impulse['price']}</code>",
                )

        trade = await db.state_get("current_trade")
        if trade:
            await monitor_trade(session, price, b15)
            return

        if await db.state_get("paused", False):
            return
        if KILLZONES_ENABLED and not in_killzone():
            return

        blackout, title = await is_news_blackout(session)
        if blackout:
            logger.info(f"Signal bloklandi — Yangilik: {title}")
            return

        if not b15:
            return
        if b1h and b4h and b1h != b15 and b4h != b15:
            return

        acc = await get_account()
        new_trade = build_sniper_trade(cl15, cl5, struct15, price, atr15, acc["balance"])
        if not new_trade:
            return

        ltf_triggers = check_5m_ltf_trigger(cl5, struct5, b15)
        if not ltf_triggers:
            return

        rsis15 = calculate_rsi(cl15)
        vwap15 = calculate_vwap(cl15)
        bull_div, bear_div = detect_rsi_divergence(cl15, rsis15)
        sweep15 = detect_liquidity_sweep(cl15, cache_key="XAU_15m")
        delta15 = order_flow_delta(cl15)
        vp15 = volume_profile(cl15)
        session_name = current_session()

        btc_res, eur_res = await asyncio.gather(
            fetch_ohlcv(session, "15m", symbol=BTC_SYMBOL),
            fetch_ohlcv(session, "15m", symbol=EUR_SYMBOL),
            return_exceptions=True,
        )
        btc_cl = closed_only(btc_res) if not isinstance(btc_res, Exception) else None
        eur_cl = closed_only(eur_res) if not isinstance(eur_res, Exception) else None

        btc_bias = confirmed_structure_bias(btc_cl, cache_key="BTC_15m") if btc_cl else None
        eur_bias = confirmed_structure_bias(eur_cl, cache_key="EUR_15m") if eur_cl else None
        smt_btc = detect_smt_divergence(cl15, btc_cl, "BTC_15m") if btc_cl else None
        smt_eur = detect_smt_divergence(cl15, eur_cl, "EUR_15m") if eur_cl else None

        mtf = mtf_alignment_score(b5, b15, b1h, b4h)
        score, grade, confirmations = evaluate_institutional_score(
            struct15, price, ltf_triggers, mtf, vwap15, bull_div, bear_div,
            btc_bias, eur_bias, smt_eur, smt_btc, new_trade.get("confluence", False),
            sweep15, pd_asia, eqh, eql, delta15, vp15, session_name
        )

        if score < MIN_SIGNAL_SCORE:
            return

        lot, risk_usd = calc_xm_lot_and_risk(
            new_trade["entry"], new_trade["sl"], acc["balance"], acc["risk_pct"]
        )
        tp1_usd = round(risk_usd * RR1, 2)
        tp2_usd = round(risk_usd * RR2, 2)

        new_trade["score"] = score
        new_trade["grade"] = grade
        new_trade["confirmations"] = ", ".join(confirmations)

        # Gemini AI tahlili (agar GEMINI_API_KEY kiritilgan bo'lsa)
        ai_comment = await generate_ai_signal_review(session, new_trade, status, confirmations)
        ai_block = f"\n🧠 <b>Gemini AI Xulosasi:</b>\n<i>{html.escape(ai_comment)}</i>\n" if ai_comment else ""

        dir_emoji = "🟢 LONG (BUY)" if new_trade["signal"] == "LONG" else "🔴 SHORT (SELL)"
        msg = (
            f"🎯 <b>XM GOLD — {dir_emoji}</b>\n"
            f"⭐️ <b>Sifat:</b> <code>{grade} ({score}/100)</code> | {'⚡ 20$ Sniper SL' if new_trade.get('sniper_mode') else 'Standart'}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📌 <b>Kirish (Entry):</b> <code>{new_trade['entry']}</code> (15m {new_trade['zone_kind']})\n"
            f"⚡ <b>5m Trigger:</b> <code>{html.escape(', '.join(ltf_triggers))}</code>\n"
            f"🛑 <b>Stop Loss:</b> <code>{new_trade['sl']}</code> (<code>-{risk_usd}$</code>)\n"
            f"🎯 <b>TP1 (1.5R):</b> <code>{new_trade['tp1']}</code> (<code>+{tp1_usd}$</code>)\n"
            f"🎯 <b>TP2 (3.0R):</b> <code>{new_trade['tp2']}</code> (<code>+{tp2_usd}$</code>)\n"
            f"🚀 <b>TP3 (4.5R):</b> <code>{new_trade['tp3']}</code>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💼 <b>XM Tavsiya Lot:</b> <code>{lot} lot</code> (Balans: ${acc['balance']})\n"
            f"🧭 <b>MTF:</b> 5m={b5 or '-'} | 15m={b15.upper()} | 1H={b1h or '-'} | 4H={b4h or '-'}\n"
            f"⚖️ <b>EQ (50%):</b> <code>{struct15.get('eq')}</code> | <b>OTE:</b> <code>{struct15.get('ote_bottom')}-{struct15.get('ote_top')}</code>\n"
            f"🏛 <b>PDH/PDL:</b> <code>{pdh or '-'} / {pdl or '-'}</code> | <b>Asia:</b> <code>{pd_asia['asia_high'] or '-'} / {pd_asia['asia_low'] or '-'}</code>\n"
            f"✅ <b>Tasdiqlar:</b> {html.escape(', '.join(confirmations))}"
            f"{ai_block}"
        )
        logger.info(f"YANGI SIGNAL [{grade} {score}/100]: {new_trade['signal']} @ {new_trade['entry']}")
        await db.state_set("current_trade", new_trade)
        _metrics["signals"] += 1
        await send_telegram(session, msg)

        zones_15m = get_active_zones(cl15, "XAU_15m")
        png_bytes = render_smc_chart_png(cl15, zones_15m, struct15, trade=new_trade)
        if png_bytes:
            caption = f"🖼 <b>XM GOLD 15m SMC Chart</b> — {new_trade['signal']} ({grade} | {score}/100)"
            await send_telegram_photo(session, png_bytes, caption=caption)

    except Exception as e:
        logger.error(f"Cycle xato: {_safe_log_error(e)}")
        logger.error(traceback.format_exc())
        _metrics["errors"] += 1

# ==================== TELEGRAM LISTENER & COMMANDS ====================
async def build_status_text():
    s = await db.state_get("last_status", {})
    if not s or s.get("price") is None:
        return "⏳ Bot hali birinchi bozor tekshiruvini bajarmadi."
    paused = await db.state_get("paused", False)
    t = await db.state_get("current_trade")
    acc = await get_account()
    live_p = _LIVE_PRICE if (_WS_CONNECTED and _LIVE_PRICE) else s["price"]
    ws_badge = "⚡ WS Real-Time" if _WS_CONNECTED else "🔄 REST (5s)"

    lines = [
        f"🤖 <b>XM GOLD SMC v14 TITANIUM</b> — {'⏸ PAUZADA' if paused else '🟢 FAOL'}",
        f"📡 Aloqa: <b>{ws_badge}</b> | Offset: <code>{_CURRENT_OFFSET:+.2f}$</code>",
        f"💰 <b>XM Narx:</b> <code>{live_p:.2f}</code> (ATR: <code>{s.get('atr15') or '-'}</code>)",
        f"📐 <b>15m EQ (50%):</b> <code>{s.get('eq15') or '-'}</code> | <b>OTE:</b> <code>{s.get('ote_bottom') or '-'}-{s.get('ote_top') or '-'}</code>",
        f"🏛 <b>PDH / PDL:</b> <code>{s.get('pdh') or '-'} / {s.get('pdl') or '-'}</code>",
        f"🌏 <b>Asia H / L:</b> <code>{s.get('asia_high') or '-'} / {s.get('asia_low') or '-'}</code>",
        f"💧 <b>Pools:</b> EQH=<code>{s.get('eqh') or '-'}</code> | EQL=<code>{s.get('eql') or '-'}</code>",
        f"📈 <b>LTF:</b> 5m={s.get('b5') or '-'} | 15m={s.get('b15') or '-'} ({s.get('event15') or '-'})",
        f"🏛 <b>HTF:</b> 1H={s.get('b1h') or '-'} | 4H={s.get('b4h') or '-'} | 1D={s.get('b1d') or '-'}",
        f"💼 <b>Hisob:</b> <code>${acc['balance']}</code> (Sniper Risk: <code>{acc['risk_pct']}%</code>)",
    ]
    if t:
        pnl_pts = round((live_p - t["entry"]) if t["signal"] == "LONG" else (t["entry"] - live_p), 2)
        lines += [
            "━━━━━━━━━━━━━━━━━━",
            f"🔥 <b>OCHIQ BITIM: {t['signal']} [{t.get('grade', 'A')}]</b> (<code>{pnl_pts:+.2f}$</code>)",
            f"Entry: <code>{t['entry']}</code> | SL: <code>{t['sl']}</code>",
            f"TP1: <code>{t['tp1']}</code> | TP2: <code>{t['tp2']}</code> | TP3: <code>{t.get('tp3', '-')}</code>",
        ]
    else:
        lines += ["━━━━━━━━━━━━━━━━━━", "🔍 <i>Ochiq bitim yo'q — 15m POI + 5m Sniper trigger kutilmoqda.</i>"]
    lines.append(f"📊 {html.escape(await win_rate_text())}")
    return "\n".join(lines)

async def build_zones_text():
    zones_list = get_active_zones(_LAST_CANDLES_15M, "XAU_15m") if _LAST_CANDLES_15M else []
    s = await db.state_get("last_status", {})
    price = _LIVE_PRICE or s.get("price")
    if not zones_list or not price:
        return "🎯 Hozircha 15m da faol FVG / OB / Breaker zonalari topilmadi."
    zones = sorted(zones_list, key=lambda z: abs(price - z["ce"]))[:7]
    lines = [f"🎯 <b>Eng yaqin XM 15m SMC Zonalar (Narx: {price:.2f}):</b>"]
    for z in zones:
        icon = "🟢" if z["type"] == "bullish" else "🔴"
        dist = round(abs(price - z["ce"]), 2)
        lines.append(
            f"{icon} <b>{z['kind']} ({z['type'].upper()})</b>: "
            f"<code>{round(z['bottom'], 2)} - {round(z['top'], 2)}</code> (Masofa: {dist}$)"
        )
    return "\n".join(lines)

async def build_news_text(session):
    events = await fetch_high_impact_news(session)
    now = datetime.datetime.now(datetime.timezone.utc)
    upcoming = [e for e in events if (e["time"] - now).total_seconds() > -3600][:6]
    if not upcoming:
        return "📰 Yaqin orada yuqori ta'sirli (High Impact USD) yangiliklar yo'q."
    lines = ["📰 <b>Shu haftadagi muhim USD yangiliklar (UTC):</b>"]
    for e in upcoming:
        dt_str = e["time"].strftime("%d-%b %H:%M")
        lines.append(f"🔴 <code>{dt_str}</code> — {html.escape(e['title'])}")
    return "\n".join(lines)

async def build_ai_market_analysis(session, custom_question=None):
    if not GEMINI_API_KEY:
        return (
            "🧠 <b>Gemini AI hali yoqilmagan!</b>\n\n"
            "Uni yoqish uchun Google AI Studio (aistudio.google.com) dan bepul API key olib, "
            "Railway <b>Variables</b> bo'limiga qo'shing:\n"
            "<code>GEMINI_API_KEY = sizning_kalitingiz</code>"
        )
    s = await db.state_get("last_status", {})
    zones_txt = await build_zones_text()
    sys_prompt = (
        "Sen XM brokerda 20$ balans (0.01 lot) bilan ishlovchi treyderning shaxsiy SMC va ICT "
        "yordamchisisan (Gemini AI). O'zbek tilida qisqa, aniq va tushunarli tahlil ber: "
        "1) Hozirgi struktura va likvidlik qayerda? "
        "2) Qaysi narxdan LONG yoki SHORT poylash kerak (Sniper zona)? "
        "3) 20$ balans uchun xavfsizlik maslahati."
    )
    context = (
        f"Joriy XM Narx: {_LIVE_PRICE or s.get('price')}\n"
        f"5m bias: {s.get('b5')} | 15m bias: {s.get('b15')} ({s.get('event15')})\n"
        f"1H: {s.get('b1h')} | 4H: {s.get('b4h')} | 1D: {s.get('b1d')}\n"
        f"15m EQ (50%): {s.get('eq15')} | OTE: {s.get('ote_bottom')}-{s.get('ote_top')}\n"
        f"PDH/PDL: {s.get('pdh')}/{s.get('pdl')} | Asia H/L: {s.get('asia_high')}/{s.get('asia_low')}\n"
        f"EQH/EQL: {s.get('eqh')}/{s.get('eql')}\n"
        f"Zonalar:\n{zones_txt}\n"
    )
    if custom_question:
        context += f"\nFoydalanuvchi savoli: {custom_question}"
    res = await ask_gemini_ai(session, context, system_instruction=sys_prompt)
    if not res:
        return "⚠️ Gemini AI bilan bog'lanishda vaqtinchalik xatolik yuz berdi."
    return f"🧠 <b>Gemini AI SMC/ICT Tahlili:</b>\n\n{html.escape(res)}"

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
    global _CURRENT_OFFSET, _LIVE_PRICE
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
                "✅ <b>XM GOLD SMC v14 TITANIUM (Real-Time + AI + 20$ Sniper)</b>\n\n"
                "📊 /signal — Jonli holat va MTF tahlil\n"
                "🖼 /chart — 15m SMC Grafik chizmasi (PNG)\n"
                "🧠 /ai — Gemini AI bozor tahlili\n"
                "🎯 /zones — Faol FVG, OB va Breaker zonalar\n"
                "🔄 /sync <code>4167.85</code> — XM narxini 1 soniyada sinxronlash\n"
                "💵 /balance <code>20</code> — Balansni o'rnatish\n"
                "📈 /stats — Savdo statistikasi",
                with_keyboard=True, chat_id=cid,
            )
        else:
            await send_telegram(session, await build_status_text(), with_keyboard=True, chat_id=cid)
    elif text == "/stop":
        await remove_sub(cid)
        await send_telegram(session, "🔕 Obuna bekor qilindi.", chat_id=cid)
    elif text in ("/signal", "📊 Signal"):
        await send_telegram(session, await build_status_text(), with_keyboard=True, chat_id=cid)
    elif text in ("/chart", "🖼 Grafik"):
        if not _LAST_CANDLES_15M:
            await send_telegram(session, "⏳ Grafik yuklanmoqda, 3 soniyadan so'ng bosing.", chat_id=cid)
            return
        t = await db.state_get("current_trade")
        zones = get_active_zones(_LAST_CANDLES_15M, "XAU_15m")
        png = render_smc_chart_png(_LAST_CANDLES_15M, zones, _LAST_STRUCT_15M, trade=t)
        s = await db.state_get("last_status", {})
        p_now = _LIVE_PRICE or s.get("price")
        cap = (
            f"🖼 <b>XM GOLD 15m SMC/ICT Grafik</b>\n"
            f"Narx: <code>{p_now}</code> | 15m Bias: <b>{(s.get('b15') or '-').upper()}</b> | "
            f"EQ: <code>{s.get('eq15') or '-'}</code>"
        )
        await send_telegram_photo(session, png, caption=cap, chat_id=cid)
    elif text in ("/zones", "🎯 Zonalar"):
        await send_telegram(session, await build_zones_text(), with_keyboard=True, chat_id=cid)
    elif text in ("/news", "📰 Yangiliklar"):
        await send_telegram(session, await build_news_text(session), with_keyboard=True, chat_id=cid)
    elif text in ("/stats", "📈 Statistika"):
        await send_telegram(session, f"📊 <b>{html.escape(await win_rate_text())}</b>", with_keyboard=True, chat_id=cid)
    elif text == "🧠 AI Tahlil" or text == "/ai" or text.startswith("/ai "):
        q = text[3:].strip() if text.startswith("/ai") else None
        await send_telegram(session, "🧠 <i>Gemini AI bozorni tahlil qilmoqda...</i>", chat_id=cid)
        ans = await build_ai_market_analysis(session, custom_question=q)
        await send_telegram(session, ans, with_keyboard=True, chat_id=cid)
    elif text.startswith("/sync") and is_admin:
        parts = text.split()
        if len(parts) != 2 or not _LIVE_BYBIT_RAW_PRICE:
            await send_telegram(
                session,
                "🔄 <b>XM narxini sinxronlash:</b>\n"
                "XM MT5 dagi joriy GOLD narxini yozing. Masalan:\n"
                "<code>/sync 4167.85</code>",
                chat_id=cid,
            )
            return
        try:
            xm_p = float(parts[1].replace(",", "."))
            new_off = round(xm_p - _LIVE_BYBIT_RAW_PRICE, 2)
            _CURRENT_OFFSET = new_off
            _LIVE_PRICE = round(_LIVE_BYBIT_RAW_PRICE + _CURRENT_OFFSET, 2)
            await db.state_set("price_offset", _CURRENT_OFFSET)
            clear_analysis_caches()
            await send_telegram(
                session,
                f"✅ <b>XM bilan 100% sinxronlandi!</b>\n"
                f"Yangi PRICE_OFFSET: <code>{_CURRENT_OFFSET:+.2f}$</code>\n"
                f"Joriy bot narxi: <code>{_LIVE_PRICE:.2f}</code>",
                chat_id=cid,
            )
        except ValueError:
            await send_telegram(session, "Noto'g'ri format. Misol: <code>/sync 4167.85</code>", chat_id=cid)
    elif text.startswith("/offset") and is_admin:
        parts = text.split()
        if len(parts) != 2:
            await send_telegram(
                session,
                f"Joriy offset: <code>{_CURRENT_OFFSET:+.2f}$</code>\n"
                f"O'zgartirish: <code>/offset -8.21</code>",
                chat_id=cid,
            )
            return
        try:
            _CURRENT_OFFSET = round(float(parts[1].replace(",", ".")), 2)
            if _LIVE_BYBIT_RAW_PRICE:
                _LIVE_PRICE = round(_LIVE_BYBIT_RAW_PRICE + _CURRENT_OFFSET, 2)
            await db.state_set("price_offset", _CURRENT_OFFSET)
            clear_analysis_caches()
            await send_telegram(session, f"✅ Offset <code>{_CURRENT_OFFSET:+.2f}$</code> saqlandi.", chat_id=cid)
        except ValueError:
            await send_telegram(session, "Misol: <code>/offset -8.21</code>", chat_id=cid)
    elif text == "/pause" and is_admin:
        await db.state_set("paused", True)
        await send_telegram(session, "⏸ Bot pauzaga qo'yildi.", chat_id=cid)
    elif text == "/resume" and is_admin:
        await db.state_set("paused", False)
        await send_telegram(session, "▶️ Bot faoliyatini davom ettirmoqda.", chat_id=cid)
    elif text == "/close" and is_admin:
        t = await db.state_get("current_trade")
        st = await db.state_get("last_status", {})
        p = _LIVE_PRICE or st.get("price")
        if t and p:
            await close_trade(session, t, "MANUAL", p)
        else:
            await send_telegram(session, "Ochiq bitim yo'q.", chat_id=cid)
    elif text.startswith("/balance") and is_admin:
        parts = text.split()
        if len(parts) != 2:
            await send_telegram(session, "Foydalanish: <code>/balance 20</code>", chat_id=cid)
            return
        try:
            nb = float(parts[1].replace(",", "."))
            if nb <= 0:
                raise ValueError
            await update_account(balance=nb)
            await send_telegram(session, f"✅ Balans <b>{nb:.2f}$</b> saqlandi.", chat_id=cid)
        except ValueError:
            await send_telegram(session, "Noto'g'ri qiymat. Misol: <code>/balance 20</code>", chat_id=cid)
    elif text.startswith("/risk") and is_admin:
        parts = text.split()
        if len(parts) != 2:
            await send_telegram(session, f"Foydalanish: <code>/risk 10</code> (maks {MAX_RISK_PER_TRADE_PCT}%)", chat_id=cid)
            return
        try:
            nr = float(parts[1].replace(",", "."))
            if not (0 < nr <= MAX_RISK_PER_TRADE_PCT):
                raise ValueError
            await update_account(risk=nr)
            await send_telegram(session, f"✅ Risk <b>{nr}%</b> saqlandi.", chat_id=cid)
        except ValueError:
            await send_telegram(session, f"0 va {MAX_RISK_PER_TRADE_PCT}% oralig'ida kiriting.", chat_id=cid)

# ==================== WEB ROUTES ====================
routes = web.RouteTableDef()

@routes.get("/")
async def home(request):
    s = await db.state_get("last_status", {})
    t = await db.state_get("current_trade")
    paused = await db.state_get("paused", False)
    acc = await get_account()
    price = _LIVE_PRICE or s.get("price")
    price_disp = f"${price:,.2f}" if price else "---"
    ws_txt = "⚡ WS Real-Time" if _WS_CONNECTED else "🔄 REST"

    trade_html = "Ochiq bitim yo'q"
    if t:
        trade_html = (
            f"<b>{t['signal']} [{t.get('grade', 'A')}]</b> | Entry: {t['entry']} | "
            f"SL: {t['sl']} | TP1: {t['tp1']} | TP2: {t['tp2']}"
        )

    html_page = f"""<!doctype html>
<html lang="uz"><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="2">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>XM Gold SMC v14 TITANIUM</title>
<style>
  body {{ background:#0B0D12; color:#EDE3CF; font-family: monospace; padding:20px; margin:0; }}
  .card {{ background:#141821; border:1px solid #2A3142; border-radius:14px;
           padding:22px; max-width:480px; margin:auto; box-shadow: 0 8px 24px rgba(0,0,0,0.65); }}
  h2 {{ font-size:2.3em; margin:4px 0 6px; color:#E4C047; }}
  .tag {{ display:inline-block; padding:4px 10px; border-radius:8px;
          background:#212736; margin:4px 4px 4px 0; font-size:0.85em; }}
  hr {{ border:none; border-top:1px dashed #2A3142; margin:16px 0; }}
  .muted {{ color:#949BA6; font-size:0.85em; }}
  .gold {{ color:#E4C047; font-weight:bold; }}
</style>
</head><body>
  <div class="card">
    <div class="muted">XM GOLD — SMC v14 TITANIUM ({ws_txt})</div>
    <h2>{price_disp}</h2>
    <div class="muted">Offset: {_CURRENT_OFFSET:+.2f}$ | Sessiya: {current_session()}</div>
    <div style="margin-top:12px">
      <span class="tag">{'⏸ Pauza' if paused else '🟢 Faol'}</span>
      <span class="tag">5m: {s.get('b5', '-')}</span>
      <span class="tag">15m: {s.get('b15', '-')}</span>
      <span class="tag">1H: {s.get('b1h', '-')}</span>
      <span class="tag">4H: {s.get('b4h', '-')}</span>
      <span class="tag">1D: {s.get('b1d', '-')}</span>
      <span class="tag">EQ: {s.get('eq15', '-')}</span>
      <span class="tag">PDH/L: {s.get('pdh', '-')}/{s.get('pdl', '-')}</span>
    </div>
    <hr>
    <h3>Joriy Bitim</h3>
    <div>{trade_html}</div>
    <hr>
    <div class="muted">Balans: ${acc.get('balance')} | Risk: {acc.get('risk_pct')}% | AI: {'Yoqilgan' if GEMINI_API_KEY else 'Ochiq'}</div>
    <div class="gold" style="margin-top:8px">{html.escape(await win_rate_text())}</div>
  </div>
</body></html>"""
    return web.Response(text=html_page, content_type="text/html")

@routes.get("/health")
async def health(request):
    return web.json_response({
        "status": "ok",
        "version": "v14_titanium_realtime_ai",
        "ws_connected": _WS_CONNECTED,
        "live_price": _LIVE_PRICE,
        "price_offset": _CURRENT_OFFSET,
        "ai_enabled": bool(GEMINI_API_KEY),
        "uptime": round(time.time() - _metrics["start"], 1),
        "paused": await db.state_get("paused", False),
        "has_open_trade": bool(await db.state_get("current_trade")),
        "metrics": _metrics,
    })

@routes.get("/metrics")
async def metrics(request):
    lines = [
        f"signals_total {_metrics['signals']}",
        f"trades_closed_total {_metrics['closed']}",
        f"wins_total {_metrics['wins']}",
        f"losses_total {_metrics['losses']}",
        f"ws_ticks_total {_metrics['ws_ticks']}",
        f"errors_total {_metrics['errors']}",
        f"uptime_seconds {round(time.time() - _metrics['start'], 1)}",
    ]
    return web.Response(text="\n".join(lines), content_type="text/plain")

@routes.get("/stats")
async def stats_page(request):
    recent = await db.get_recent_trades(30)
    return web.json_response({"recent": recent})

@routes.post("/webhook")
async def webhook(request):
    if not WEBHOOK_ENABLED:
        return web.json_response({"ok": False, "error": "disabled"}, status=403)
    try:
        data = await request.json()
        if data.get("secret") != WEBHOOK_SECRET:
            return web.json_response({"ok": False, "error": "invalid secret"}, status=403)

        action = data.get("action", "").upper()
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
                f"📩 <b>TradingView signali rad etildi:</b> {action} (15m bias={b15})",
            ))
            return web.json_response({"ok": True, "skipped": "bias_mismatch"})

        asyncio.create_task(send_telegram(
            request.app["session"],
            f"📩 <b>TradingView Webhook:</b> {action} {html.escape(str(data.get('symbol', SYMBOL)))}\n"
            f"Narx: <code>{html.escape(str(data.get('price', '?')))}</code>\n"
            f"{html.escape(str(data.get('message', '')))}",
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
            _metrics["errors"] += 1
        next_run += CHECK_INTERVAL_SEC
        sleep_time = max(0, next_run - time.monotonic())
        if sleep_time > CHECK_INTERVAL_SEC * 2:
            next_run = time.monotonic() + CHECK_INTERVAL_SEC
            sleep_time = CHECK_INTERVAL_SEC
        await asyncio.sleep(sleep_time)

async def register_bot_commands(session):
    commands = [
        {"command": "start", "description": "Menyu va holat"},
        {"command": "signal", "description": "Jonli XM narx va MTF tahlil"},
        {"command": "chart", "description": "15m SMC grafik chizmasi (PNG)"},
        {"command": "ai", "description": "Gemini AI bozor tahlili"},
        {"command": "zones", "description": "Faol 15m FVG, OB va BB zonalar"},
        {"command": "sync", "description": "XM narxiga sinxronlash (/sync 4167.85)"},
        {"command": "offset", "description": "Narx farqini o'rnatish (/offset -8.21)"},
        {"command": "news", "description": "Muhim USD yangiliklar"},
        {"command": "stats", "description": "Win-rate va statistika"},
        {"command": "balance", "description": "Balans kiritish (/balance 20)"},
        {"command": "risk", "description": "Risk % kiritish (/risk 10)"},
        {"command": "close", "description": "Ochiq bitimni yopish (admin)"},
        {"command": "pause", "description": "Botni pauza qilish (admin)"},
        {"command": "resume", "description": "Botni yoqish (admin)"},
    ]
    try:
        async with session.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
            json={"commands": commands}, timeout=10,
        ) as r:
            if r.status == 200:
                logger.info("Buyruqlar menyusi yangilandi.")
    except Exception as e:
        logger.error(f"setMyCommands xato: {_safe_log_error(e)}")

# ==================== STARTUP / CLEANUP ====================
async def startup_tasks(app):
    global _CURRENT_OFFSET
    await db.init()
    saved_offset = await db.state_get("price_offset")
    if saved_offset is not None:
        _CURRENT_OFFSET = float(saved_offset)
    app["session"] = aiohttp.ClientSession()
    await register_bot_commands(app["session"])
    app["ws_task"] = asyncio.create_task(bybit_ws_loop(app), name="bybit_ws_loop")
    app["bot_task"] = asyncio.create_task(trading_loop(app), name="trading_loop")
    app["tg_task"] = asyncio.create_task(telegram_listener(app), name="tg_listener")
    logger.info(f"✅ Bot ishga tushdi (v14 TITANIUM | Offset={_CURRENT_OFFSET:+.2f}$)")

async def cleanup_tasks(app):
    for key in ("ws_task", "bot_task", "tg_task"):
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

def create_app():
    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(startup_tasks)
    app.on_cleanup.append(cleanup_tasks)
    return app

app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8100))
    web.run_app(app, host="0.0.0.0", port=port, access_log=None)