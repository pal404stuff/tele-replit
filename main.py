# IndiQuant Apex AutoScan — Ultimate Edition (v4.2.1)
# NSE/BSE • IST • Hands‑Free • Persistent • Portfolio Monitor • Multi‑TP & Trailing
# -----------------------------------------------------------------------------
# Educational only — not investment advice. Not SEBI-registered. Markets carry risk.
# -----------------------------------------------------------------------------
# Replit setup
# 1) Add a .env (or Replit Secrets) with TELEGRAM_BOT_TOKEN="<token>" and optional ADMIN_CHAT_ID
# 2) requirements.txt (pin versions):
#    python-telegram-bot==21.4
#    yfinance==0.2.43
#    pandas==2.2.2
#    numpy==1.26.4
#    requests==2.32.3
#    python-dotenv==1.0.1   # optional (script runs without it)
# 3) Click Run.

from __future__ import annotations
import os, json, uuid, logging, math, asyncio, textwrap, time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, time as dtime, date
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf

# Optional: load .env if available (works without python-dotenv)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # Replit secrets already populate os.environ; .env is optional
    pass

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, Application, CommandHandler, MessageHandler, ContextTypes, JobQueue, filters
)

# ============================== CONSTANTS ==================================
IST = ZoneInfo("Asia/Kolkata")
BOT_NAME = "IndiQuant Apex Ultimate"
VERSION = "v4.2.1"
COMPLIANCE = "Educational only — not investment advice. Not SEBI-registered. Markets carry risk."

STATE_PATH = "/mnt/data/indiquant_state.json"
NSE_CACHE_PATH = "/mnt/data/nse_cache.json"
HOLIDAY_CACHE_PATH = "/mnt/data/nse_holidays.json"
DATA_CACHE_TTL = 60  # seconds
NSE_INDEX_TTL = 12*3600

NSE_BASE = "https://www.nseindia.com"
NSE_EQ_INDEX_URL = f"{NSE_BASE}/api/equity-stockIndices"
NSE_INDEX_LIST_URL = f"{NSE_BASE}/api/allIndices"
NSE_HOLIDAY_URL = f"{NSE_BASE}/api/holiday-master?type=trading"
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/market-data/live-equity-market",
}

# ============================== LOGGING ====================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("indiquant.ultimate")

# ============================== SECRETS ====================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")  # optional default chat id

TIP_UPI_ID   = os.getenv("TIP_UPI_ID", "")
TIP_BMC      = os.getenv("TIP_BMC", "")
TIP_KOFI     = os.getenv("TIP_KOFI", "")
TIP_STRIPE   = os.getenv("TIP_STRIPE", "")
TIP_RAZORPAY = os.getenv("TIP_RAZORPAY", "")

# ============================== DEFAULT STATE ===============================
DEFAULT_STATE: Dict = {
    "config": {
        "$schema": "indiquant.config.v4",
        "timezone": "Asia/Kolkata",
        "universe": ["NIFTYBEES", "BANKBEES", "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "SBIN", "INFY"],
        "timeframe": "D",            # default for /alert
        "session": "swing",           # swing for >=1h/D, intraday for <=15m
        "square_off_time": "15:20",
        "data": {"source": "yfinance", "adjusted": True},
        "signal": {"name": "auto", "params": {
            "ema_fast": 20, "ema_slow": 50, "rsi2_th": 10, "donchian_n": 20,
            "vol_pctile_low": 40, "vol_pctile_high": 70,
            "mtf": {"enable": True, "htf": "D"},
            "rs": {"enable": True, "benchmark": "NIFTYBEES", "min_rating": 70}
        }},
        "sl": {"method": "atr", "params": {"atr_k": 2.5}},
        "tp": {"method": "r", "params": {"r": 2.0},
                "multi": {"enable": True, "tp1_r": 1.5, "tp2_r": 3.0, "trail_be": True}},
        "filters": {"min_adv_inr": 5e7, "max_spread_pct": 0.35, "band_guard": True, "min_rr_net": 1.3},
        "costs": {
            "brokerage_model": "flat",
            "brokerage_flat_inr": 20,
            "brokerage_pct": 0.0003,
            "stt_pct": 0.001,
            "exchange_txn_pct": 0.0000325,
            "sebi_fee_pct": 0.000001,
            "stamp_duty_pct": 0.00015,
            "gst_pct": 0.18,
            "slippage_bps": 5
        },
        "tick_size": 0.05,
        "valid_minutes": 180,
        "aggressive": False
    },
    "schedule": {
        "$schema": "indiquant.schedule.v4",
        "market_window": {"start": "09:15", "end": "15:30"},
        "timeframes": ["1h", "D"],
        "autoscan": False,
        "quiet_windows": [{"start": "09:15", "end": "09:30"}],
        "digest": {"enabled": True, "interval_minutes": 60, "eod": True},
        "throttle": {"max_per_min": 8, "max_per_symbol": 2},
        "live_ticks": False
    },
    "runtime": {
        "$schema": "indiquant.runtime.v4",
        "ist_ts": None,
        "autoscan": "off",
        "scan_status": "idle",
        "last_bar_close": {"1m": None, "5m": None, "15m": None, "1h": None, "D": None},
        "counters": {"alerts_sent": 0, "suppressed": 0, "deduped": 0, "skipped": 0},
        "chat_id": ADMIN_CHAT_ID or None,
        "nse_holidays": []
    },
    "portfolio": {
        "active_trades": {},  # trade_id -> trade dict
        "closed_trades": []   # list of trade dicts
    },
    "dedupe": {},             # (symbol|tf) -> valid_until ISO
}

STATE: Dict = {}
DATA_CACHE: Dict[Tuple[str,str,str], Tuple[pd.DataFrame, float]] = {}
NSE_CACHE_MEM: Dict[str, Tuple[List[str], float]] = {}

# throttle counters
CURRENT_MINUTE: Optional[str] = None
SENT_THIS_MINUTE: int = 0
SYMBOL_COUNT: Dict[Tuple[str,str], int] = {}

# ============================== UTILITIES ==================================

def now_ist() -> datetime:
    return datetime.now(tz=IST)

def ist_iso(dt: Optional[datetime] = None) -> str:
    return (dt or now_ist()).astimezone(IST).isoformat(timespec="seconds")

def parse_hhmm(s: str) -> dtime:
    h, m = map(int, s.split(":"))
    return dtime(hour=h, minute=m, tzinfo=IST)

def in_window(ts: datetime, start_hhmm: str, end_hhmm: str) -> bool:
    t = ts.timetz()
    return parse_hhmm(start_hhmm) <= t <= parse_hhmm(end_hhmm)

def in_any_quiet(ts: datetime) -> bool:
    for w in STATE["schedule"].get("quiet_windows", []):
        if in_window(ts, w["start"], w["end"]):
            return True
    return False

def reset_throttle(ts: datetime):
    global CURRENT_MINUTE, SENT_THIS_MINUTE
    key = ts.strftime("%Y-%m-%d %H:%M")
    if key != CURRENT_MINUTE:
        CURRENT_MINUTE = key
        SENT_THIS_MINUTE = 0

def symbol_session_key(sym: str, ts: datetime) -> Tuple[str,str]:
    return (sym.upper(), ts.strftime("%Y-%m-%d"))

# ============================ PERSISTENCE ==================================

def load_state() -> None:
    global STATE
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r") as f:
                STATE = json.load(f)
            # merge with defaults to add new keys safely
            STATE = _deep_merge(DEFAULT_STATE, STATE)
        else:
            STATE = json.loads(json.dumps(DEFAULT_STATE))
        log.info("State loaded.")
    except Exception as e:
        log.warning(f"State load failed, using defaults: {e}")
        STATE = json.loads(json.dumps(DEFAULT_STATE))

def save_state() -> None:
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        with open(STATE_PATH, "w") as f:
            json.dump(STATE, f)
    except Exception as e:
        log.warning(f"State save failed: {e}")

def _deep_merge(base: Dict, override: Dict) -> Dict:
    out = json.loads(json.dumps(base))
    def rec(dst, src):
        for k, v in src.items():
            if isinstance(v, dict) and isinstance(dst.get(k), dict):
                rec(dst[k], v)
            else:
                dst[k] = v
    rec(out, override)
    return out

# =============================== NSE HELPERS ===============================

def _nse_session() -> requests.Session:
    s = requests.Session(); s.headers.update(NSE_HEADERS)
    try:
        s.get(NSE_BASE, timeout=10)
    except Exception:
        pass
    return s

def fetch_nse_index_symbols(index_name: str, ttl_sec: int = NSE_INDEX_TTL) -> List[str]:
    now = time.time()
    key = index_name.strip().upper()
    # memory cache
    if key in NSE_CACHE_MEM and now < NSE_CACHE_MEM[key][1]:
        return NSE_CACHE_MEM[key][0]
    # disk cache
    try:
        if os.path.exists(NSE_CACHE_PATH):
            d = json.load(open(NSE_CACHE_PATH))
            if key in d and now < d[key]["exp"]:
                NSE_CACHE_MEM[key] = (d[key]["symbols"], d[key]["exp"])
                return d[key]["symbols"]
    except Exception:
        pass
    # fetch
    out: List[str] = []
    try:
        ses = _nse_session()
        for attempt in range(3):
            r = ses.get(NSE_EQ_INDEX_URL, params={"index": index_name}, timeout=12)
            if r.status_code == 200:
                j = r.json(); data = j.get("data", j)
                out = [ (row.get("symbol") or "").strip().upper() for row in data if row.get("symbol") ]
                break
            if r.status_code in (403, 429):
                time.sleep(1.2*(attempt+1)); continue
            r.raise_for_status()
    except Exception as e:
        log.warning(f"NSE index fetch failed for {index_name}: {e}")
    # update caches
    exp = now + ttl_sec
    NSE_CACHE_MEM[key] = (out, exp)
    try:
        disk = json.load(open(NSE_CACHE_PATH)) if os.path.exists(NSE_CACHE_PATH) else {}
        disk[key] = {"symbols": out, "exp": exp}
        json.dump(disk, open(NSE_CACHE_PATH, "w"))
    except Exception:
        pass
    return out

async def fetch_nse_holidays() -> List[str]:
    # return list of ISO dates "YYYY-MM-DD" for trading holidays
    try:
        if os.path.exists(HOLIDAY_CACHE_PATH):
            d = json.load(open(HOLIDAY_CACHE_PATH))
            # refresh daily at 06:00 IST in job
            return d.get("holidays", [])
    except Exception:
        pass
    try:
        ses = _nse_session()
        r = ses.get(NSE_HOLIDAY_URL, timeout=12)
        r.raise_for_status()
        j = r.json(); arr = j.get("CBM", []) or j.get("trading", []) or []
        hol = []
        for it in arr:
            dt_str = it.get("tradingDate") or it.get("holidayDate")
            if not dt_str: continue
            try:
                # formats like "02-Oct-2025"
                dte = datetime.strptime(dt_str, "%d-%b-%Y").date()
                hol.append(str(dte))
            except Exception:
                pass
        json.dump({"holidays": hol, "fetched": ist_iso()}, open(HOLIDAY_CACHE_PATH, "w"))
        return hol
    except Exception as e:
        log.warning(f"Holiday fetch failed: {e}")
        return []

# ============================ DATA LAYER (YF) ==============================

def yahoo_symbol(sym: str) -> str:
    s = sym.strip().upper()
    if s.endswith(".NS") or s.endswith(".BO"): return s
    return s + ".NS"

def _yf_download(tickers: str, interval: Optional[str]=None, period: Optional[str]=None,
                 start: Optional[str]=None, end: Optional[str]=None) -> pd.DataFrame:
    # retry wrapper to reduce transient failures
    last_exc: Optional[Exception] = None
    for _ in range(3):
        try:
            df = yf.download(tickers=tickers, interval=interval, period=period, start=start, end=end,
                             auto_adjust=True, progress=False)
            if df is not None and not df.empty:
                return df.rename(columns=str.title).dropna()
        except Exception as e:
            last_exc = e
        time.sleep(0.8)
    if last_exc: raise last_exc
    raise RuntimeError("No data from yfinance")

async def fetch_ohlcv(symbol: str, tf: str, period: Optional[str]=None,
                      start: Optional[str]=None, end: Optional[str]=None) -> pd.DataFrame:
    cache_key = (symbol, tf, period or start or "", end or "")
    now_ts = time.time()
    if cache_key in DATA_CACHE and now_ts - DATA_CACHE[cache_key][1] < DATA_CACHE_TTL:
        return DATA_CACHE[cache_key][0]
    loop = asyncio.get_event_loop()
    if tf.upper() == "D":
        df = await loop.run_in_executor(None, lambda: _yf_download(yahoo_symbol(symbol), start=start, end=end, period=period or "5y"))
    else:
        tf_map = {"1m": ("1m","7d"), "5m": ("5m","30d"), "15m": ("15m","60d"), "1h": ("60m","730d")}
        interval, per = tf_map.get(tf, ("15m","60d"))
        df = await loop.run_in_executor(None, lambda: _yf_download(yahoo_symbol(symbol), interval=interval, period=period or per))
    DATA_CACHE[cache_key] = (df, now_ts)
    return df

# =============================== INDICATORS ================================

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window).mean()

def rsi(series: pd.Series, period: int=14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    gain = up.rolling(period).mean()
    loss = down.rolling(period).mean()
    rs = gain / (loss.replace(0, np.nan))
    return 100 - (100 / (1 + rs))

def rsi2(series: pd.Series) -> pd.Series:
    return rsi(series, 2)

def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int=14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def donchian(high: pd.Series, low: pd.Series, n: int=20) -> Tuple[pd.Series, pd.Series]:
    return high.rolling(n).max(), low.rolling(n).min()

# ============================ STRATEGY ENGINE ==============================

def adv_inr(df: pd.DataFrame, window: int=20) -> Optional[float]:
    if "Close" not in df or "Volume" not in df: return None
    dv = (df["Close"] * df["Volume"]).rolling(window).mean().dropna()
    return float(dv.iloc[-1]) if not dv.empty else None

async def get_benchmark_df(cfg: Dict, tf: str) -> Optional[pd.DataFrame]:
    b = cfg["signal"]["params"]["rs"].get("benchmark", "NIFTYBEES")
    try:
        return await fetch_ohlcv(b, tf if tf.upper()!="1H" else "1h")
    except Exception:
        # fallback to NIFTYBEES if provided benchmark fails
        if b.upper() != "NIFTYBEES":
            try:
                return await fetch_ohlcv("NIFTYBEES", tf if tf.upper()!="1H" else "1h")
            except Exception:
                return None
        return None

def rs_rating_from(df: pd.DataFrame, bench: Optional[pd.DataFrame], lookback: int=55) -> pd.Series:
    if bench is None or bench.empty: return pd.Series(50.0, index=df.index)
    j = df[["Close"]].join(bench[["Close"]].rename(columns={"Close":"Bench"}), how="inner").dropna()
    if j.empty: return pd.Series(50.0, index=df.index)
    stock_norm = j["Close"] / j["Close"].iloc[0]
    bench_norm = j["Bench"] / j["Bench"].iloc[0]
    rs = stock_norm / bench_norm
    # percentile rank of the latest vs trailing lookback
    out = rs.rolling(lookback).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1]*100, raw=False)
    out = out.reindex(df.index).ffill().fillna(50.0)
    return out

async def pick_setup(symbol: str, tf: str, cfg: Dict) -> Optional[Dict]:
    df = await fetch_ohlcv(symbol, tf)
    if df is None or len(df) < 200: return None
    close, high, low, vol = df["Close"], df["High"], df["Low"], df["Volume"]
    ema_fast_n = cfg["signal"]["params"].get("ema_fast", 20)
    ema_slow_n = cfg["signal"]["params"].get("ema_slow", 50)
    don_n = cfg["signal"]["params"].get("donchian_n", 20)
    rsi2_th = cfg["signal"]["params"].get("rsi2_th", 10)

    ema_f = ema(close, ema_fast_n); ema_s = ema(close, ema_slow_n)
    sma200 = sma(close, 200)
    a14 = atr(high, low, close, 14)
    don_hi, don_lo = donchian(high, low, don_n)
    vol_ma = vol.rolling(20).mean()
    atr_pctile = a14.rolling(100).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1]*100, raw=False)
    rsi2_now = rsi2(close)

    # Relative Strength rating vs benchmark
    bench = await get_benchmark_df(cfg, tf)
    rs_rate = rs_rating_from(df, bench)

    last = pd.DataFrame({
        "close": close, "high": high, "low": low, "ema_f": ema_f, "ema_s": ema_s,
        "sma200": sma200, "atr": a14, "don_hi": don_hi.shift(1), "vol": vol, "vol_ma": vol_ma,
        "rsi2": rsi2_now, "atr_pctile": atr_pctile, "rs_rating": rs_rate
    }).dropna().iloc[-1]

    # Liquidity gate
    adv = adv_inr(df)
    if adv is None or adv < cfg["filters"]["min_adv_inr"]: return None

    # MTF filter (higher TF trend up)
    if cfg["signal"]["params"].get("mtf", {}).get("enable", True):
        htf = cfg["signal"]["params"]["mtf"].get("htf", "D")
        try:
            df_htf = await fetch_ohlcv(symbol, htf)
            if df_htf is not None and len(df_htf) > ema_slow_n+5:
                e_f = ema(df_htf["Close"], ema_fast_n).iloc[-1]
                e_s = ema(df_htf["Close"], ema_slow_n).iloc[-1]
                if not (e_f > e_s):
                    return None
        except Exception:
            pass

    # Trend health & RS gate
    if not (last["close"] >= last["sma200"] and last["ema_f"] >= last["ema_s"]):
        return None
    if cfg["signal"]["params"]["rs"].get("enable", True) and last["rs_rating"] < cfg["signal"]["params"]["rs"].get("min_rating", 70):
        return None

    # Volatility regime
    vol_ok = cfg["signal"]["params"].get("vol_pctile_low", 40) <= last["atr_pctile"] <= cfg["signal"]["params"].get("vol_pctile_high", 70)

    # Volume confirmation
    vol_conf = last["vol"] > (last["vol_ma"] or 0)

    # Candidate signals
    signals = []
    if last["close"] <= last["ema_f"] and vol_conf:
        signals.append(("EMA Pullback", 65))
    if last["rsi2"] <= rsi2_th and vol_ok and vol_conf:
        signals.append(("RSI2 Dip", 75))
    if last["close"] >= last["don_hi"] and vol_ok and vol_conf:
        signals.append(("Donchian Breakout", 70))
    if not signals:
        return None

    best = max(signals, key=lambda x: x[1])
    conf = best[1] + (10 if vol_ok else 0) + (10 if len(signals) > 1 else 0) + (10 if last["rs_rating"] > 85 else 0)

    tick = cfg.get("tick_size", 0.05)
    bump = tick if tick >= 0.05 else 0.01
    raw_entry = max(float(last["high"])+bump, float(last["don_hi"]) + bump if not math.isnan(last["don_hi"]) else 0)
    entry = round(raw_entry / tick) * tick

    # SL = max(ATR stop, structure stop)
    swing_low = float(df["Low"].rolling(10).min().iloc[-2])
    sl_atr = entry - cfg["sl"]["params"].get("atr_k", 2.5) * float(last["atr"])
    sl_struct = swing_low * 0.99
    sl_raw = min(sl_atr, sl_struct)
    sl = round(sl_raw / tick) * tick

    # Multi-TP
    tp_cfg = cfg["tp"]
    if tp_cfg.get("multi", {}).get("enable", True):
        tp1 = entry + tp_cfg["multi"].get("tp1_r", 1.5) * (entry - sl)
        tp2 = entry + tp_cfg["multi"].get("tp2_r", 3.0) * (entry - sl)
        tp = tp2
    else:
        tp = entry + tp_cfg["params"].get("r", 2.0) * (entry - sl)
        tp1, tp2 = None, None
    tp = round(tp / tick) * tick
    tp1 = round(tp1 / tick) * tick if tp1 else None
    tp2 = round(tp2 / tick) * tick if tp2 else None

    if not (sl < entry < tp):
        return None

    gross, net, cps = net_rr(entry, sl, tp, cfg)
    if not cfg.get("aggressive", False) and net < cfg["filters"].get("min_rr_net", 1.3):
        return None

    return {
        "symbol": symbol.upper(),
        "timeframe": tf,
        "session": STATE["config"]["session"],
        "idea": best[0] + " + MTF + RS + VolConfirm",
        "confidence": int(max(0, min(100, conf))),
        "entry": {"type": "stop", "price": float(entry)},
        "sl": {"method": f"ATR/Struct", "price": float(sl)},
        "tp": {"method": "multi" if tp1 else "R", "price": float(tp), "tp1": tp1, "tp2": tp2},
        "assumptions": {"data_src": "yfinance", "bar_size": tf, "tick_size": cfg.get("tick_size", 0.05), "costs_placeholders": True},
        "filters": {"adv_ok": True, "spread_ok": None, "band_guard_ok": None},
        "last": {"adv_inr": adv, "rs_rating": float(last["rs_rating"]), "atr_pctile": float(last["atr_pctile"])},
        "rr": {"gross": round(gross,2), "net": round(net,2)}
    }

# ============================= COSTS / R:R ================================

def cost_per_share(entry: float, exit_px: float, C: Dict) -> float:
    c = C["costs"]
    turn = entry + exit_px
    brokerage = (c["brokerage_flat_inr"]*2) if c["brokerage_model"] == "flat" else (turn * c["brokerage_pct"])
    stt   = turn * c["stt_pct"]
    exch  = turn * c["exchange_txn_pct"]
    sebi  = turn * c["sebi_fee_pct"]
    stamp = turn * c["stamp_duty_pct"]
    gst   = (brokerage + exch + sebi) * c["gst_pct"]
    slip  = (turn) * (c.get("slippage_bps",0)/10000.0)
    return brokerage + stt + exch + sebi + stamp + gst + slip

def rr_multiple(entry: float, sl: float, tp: float) -> float:
    risk = max(1e-9, entry - sl)
    return (tp - entry) / risk

def net_rr(entry: float, sl: float, tp: float, cfg: Dict) -> Tuple[float,float,float]:
    gross = rr_multiple(entry, sl, tp)
    cps = cost_per_share(entry, tp, cfg)
    reward_net = max(0.0, (tp - entry) - cps)
    risk = max(1e-9, entry - sl)
    return gross, reward_net / risk, cps

# =============================== MESSAGING ================================

def tip_keyboard() -> Optional[InlineKeyboardMarkup]:
    rows = []
    if TIP_UPI_ID:
        rows.append([InlineKeyboardButton("Tip via UPI", url=f"upi://pay?pa={TIP_UPI_ID}&pn=IndiQuant&cu=INR")])
    for label, url in [("Buy Me a Coffee", TIP_BMC), ("Ko-fi", TIP_KOFI), ("Stripe", TIP_STRIPE), ("Razorpay", TIP_RAZORPAY)]:
        if url: rows.append([InlineKeyboardButton(label, url=url)])
    return InlineKeyboardMarkup(rows) if rows else None

async def send_html(context: ContextTypes.DEFAULT_TYPE, chat_id: int, html: str):
    await context.bot.send_message(chat_id=chat_id, text=html, parse_mode="HTML", reply_markup=tip_keyboard(), disable_web_page_preview=True)

def build_alert_html(payload: Dict, cfg: Dict) -> str:
    ts = ist_iso()
    entry = payload["entry"]["price"]; sl = payload["sl"]["price"]; tp = payload["tp"]["price"]
    rr_g = payload["rr"]["gross"]; rr_n = payload["rr"]["net"]
    valid_until = (now_ist() + timedelta(minutes=cfg.get("valid_minutes",180))).astimezone(IST).strftime("%H:%M")
    sqoff = cfg["square_off_time"] if payload["session"].lower()=="intraday" else "N/A"
    costs = cfg["costs"]
    tp_line = f"TP: ₹{tp:.2f} (final)" if payload["tp"].get("tp1") is None else (
        f"TP1: ₹{payload['tp']['tp1']:.2f} • TP2: ₹{payload['tp']['tp2']:.2f}")
    dec = " → ".join([
        "Objective: Buy setup",
        f"Data: yfinance closed bars (tf={payload['timeframe']})",
        f"Method: {payload['idea']}",
        "Controls: ADV gate; spread/bands n/a",
        "Levels: tick-rounded Entry/SL/TP",
        f"Net R:R {rr_n:.2f}:1; Valid {valid_until} IST"
    ])
    html = (
        f"<b>{ts}</b>\n"
        f"<b>ALERT • {payload['symbol']} • {payload['timeframe']} • {payload['session'].capitalize()} (IST)</b>\n"
        f"Idea: {payload['idea']} • Confidence {payload['confidence']}%\n"
        f"Entry: <b>stop ₹{entry:.2f}</b> • SL: ₹{sl:.2f} ({payload['sl']['method']}) • {tp_line} → RR(gross) <b>{rr_g:.2f}:1</b>\n"
        f"Costs: brokerage {costs['brokerage_model']}, GST {costs['gst_pct']*100:.0f}%, STT {costs['stt_pct']*100:.2f}%, slippage {costs['slippage_bps']} bps → RR(net) <b>{rr_n:.2f}:1</b>\n"
        f"Timing: valid till {valid_until} • Session {payload['session']} • Square-off {sqoff} IST • Filters: ADV OK\n"
        f"Decision Trace: {dec}\n"
        f"<i>{COMPLIANCE}</i>\n"
        f"<pre><code>{json.dumps(payload, indent=2)}</code></pre>"
    )
    return html

# =========================== AUTOSCAN & ALERTS ============================

LAST_ALERT_VALID: Dict[str, str] = {}  # key = f"{sym}|{tf}" -> ISO time

def bar_close_now(ts: datetime, tf: str, schedule: dict) -> bool:
    m, s = ts.minute, ts.second
    if s > 5: return False
    if tf == "1m": return True
    if tf == "5m": return m % 5 == 0
    if tf == "15m": return m % 15 == 0
    if tf == "1h": return m == 0
    if tf.upper() == "D": return ts.strftime("%H:%M") == schedule["market_window"]["end"]
    return False

async def scan_once_for_tf(context: ContextTypes.DEFAULT_TYPE, tf: str):
    global SENT_THIS_MINUTE
    ts = now_ist()
    STATE["runtime"]["ist_ts"] = ist_iso(ts)
    if not is_market_open():
        STATE["runtime"]["scan_status"] = "idle"
        return
    STATE["runtime"]["scan_status"] = "running"
    reset_throttle(ts)

    suppressed = in_any_quiet(ts)
    max_per_min = STATE["schedule"]["throttle"]["max_per_min"]
    max_per_symbol = STATE["schedule"]["throttle"]["max_per_symbol"]

    for sym in STATE["config"]["universe"]:
        key = f"{sym.upper()}|{tf}"
        vu = LAST_ALERT_VALID.get(key)
        if vu and ts < datetime.fromisoformat(vu):
            STATE["runtime"]["counters"]["deduped"] += 1
            continue
        if SENT_THIS_MINUTE >= max_per_min:
            STATE["runtime"]["counters"]["suppressed"] += 1
            break
        if SYMBOL_COUNT.get(symbol_session_key(sym, ts), 0) >= max_per_symbol:
            STATE["runtime"]["counters"]["skipped"] += 1
            continue
        try:
            setup = await pick_setup(sym, tf, STATE["config"])
            if not setup: 
                STATE["runtime"]["counters"]["skipped"] += 1
                continue
            payload = build_payload_from_setup(setup)
            html = build_alert_html(payload, STATE["config"])
            if suppressed:
                STATE["runtime"]["counters"]["suppressed"] += 1
            else:
                chat_id = int(STATE["runtime"].get("chat_id") or (ADMIN_CHAT_ID or 0))
                if chat_id:
                    await send_html(context, chat_id, html)
                    STATE["runtime"]["counters"]["alerts_sent"] += 1
                    SENT_THIS_MINUTE += 1
                    SYMBOL_COUNT[symbol_session_key(sym, ts)] = SYMBOL_COUNT.get(symbol_session_key(sym, ts),0) + 1
                    # record dedupe horizon
                    LAST_ALERT_VALID[key] = ist_iso(ts + timedelta(minutes=STATE["config"].get("valid_minutes",180)))
                    # arm a pending trade to monitor entry trigger
                    arm_pending_trade_from_alert(payload)
        except Exception as e:
            log.warning(f"scan error {sym}/{tf}: {e}")
            STATE["runtime"]["counters"]["skipped"] += 1
            continue
    save_state()

async def scheduler_job(context: ContextTypes.DEFAULT_TYPE):
    if not STATE["schedule"]["autoscan"]: return
    ts = now_ist()
    if not is_market_open(): return
    # run on bar-close boundaries (tolerant window)
    for tf in STATE["schedule"]["timeframes"]:
        if bar_close_now(ts, tf, STATE["schedule"]):
            await scan_once_for_tf(context, tf)
            STATE["runtime"]["last_bar_close"][tf] = ts.strftime("%H:%M")

async def digest_job(context: ContextTypes.DEFAULT_TYPE):
    if not (STATE["schedule"]["autoscan"] and STATE["schedule"]["digest"]["enabled"]): return
    ts = now_ist()
    if not is_market_open(): return
    chat_id = STATE["runtime"].get("chat_id") or ADMIN_CHAT_ID
    if not chat_id: return
    html = (
        f"<b>{ist_iso(ts)}</b>\n"
        f"<b>NO SETUPS</b> — Window {STATE['schedule']['digest']['interval_minutes']}m across {', '.join(STATE['schedule']['timeframes'])}. "
        f"Suppressed {STATE['runtime']['counters']['suppressed']}, Deduped {STATE['runtime']['counters']['deduped']}, Skipped {STATE['runtime']['counters']['skipped']}.\n"
        f"Next scan: on bar closes.\n"
        f"<pre><code>{json.dumps(STATE['runtime'], indent=2)}</code></pre>\n"
        f"<i>{COMPLIANCE}</i>"
    )
    await send_html(context, int(chat_id), html)

async def eod_job(context: ContextTypes.DEFAULT_TYPE):
    ts = now_ist()
    end = parse_hhmm(STATE["schedule"]["market_window"]["end"])
    if ts.hour == end.hour and ts.minute == end.minute:
        # auto square-off note
        chat_id = STATE["runtime"].get("chat_id") or ADMIN_CHAT_ID
        if chat_id:
            html = (
                f"<b>{ist_iso(ts)}</b>\n"
                f"<b>EOD DIGEST</b> — Alerts {STATE['runtime']['counters']['alerts_sent']}, Suppressed {STATE['runtime']['counters']['suppressed']}, Deduped {STATE['runtime']['counters']['deduped']}.\n"
                f"Auto square-off {STATE['config']['square_off_time']} IST (intraday rule).\n"
                f"<pre><code>{json.dumps(STATE['runtime'], indent=2)}</code></pre>\n"
                f"<i>{COMPLIANCE}</i>"
            )
            await send_html(context, int(chat_id), html)
        # reset counters daily
        STATE["runtime"]["counters"] = {"alerts_sent":0,"suppressed":0,"deduped":0,"skipped":0}
        SYMBOL_COUNT.clear()
        save_state()

# =========================== TRADE STATE & MONITOR ========================

def build_payload_from_setup(setup: Dict) -> Dict:
    payload = {
        "$schema": "indiquant.alert.v4",
        "alert_id": str(uuid.uuid4()),
        "ts_ist": ist_iso(),
        "symbol": setup["symbol"],
        "timeframe": setup["timeframe"],
        "session": setup["session"],
        "idea": setup["idea"],
        "entry": {"type": setup["entry"]["type"], "price": setup["entry"]["price"], "valid_until_ist": (now_ist()+timedelta(minutes=STATE['config']['valid_minutes'])).strftime('%H:%M')},
        "sl": {"method": setup["sl"]["method"], "price": setup["sl"]["price"]},
        "tp": {"method": setup["tp"]["method"], "price": setup["tp"]["price"], "tp1": setup["tp"].get("tp1"), "tp2": setup["tp"].get("tp2")},
        "confidence": setup["confidence"],
        "assumptions": setup["assumptions"],
        "filters": setup["filters"],
        "costs": {"model": STATE["config"]["costs"]["brokerage_model"], "slippage_bps": STATE["config"]["costs"]["slippage_bps"]},
        "risk_reward": setup["rr"],
        "notes": ["educational only", "square-off "+STATE["config"]["square_off_time"] if setup["session"].lower()=="intraday" else "swing"]
    }
    return payload

def arm_pending_trade_from_alert(payload: Dict) -> None:
    # Create a pending trade to be triggered when price >= entry
    trade_id = payload["alert_id"]
    sym = payload["symbol"].upper()
    STATE["portfolio"]["active_trades"][trade_id] = {
        "trade_id": trade_id,
        "symbol": sym,
        "state": "PENDING",  # PENDING -> OPEN -> TP1 -> CLOSED / STOP
        "entry_px": payload["entry"]["price"],
        "sl_px": payload["sl"]["price"],
        "tp1_px": payload["tp"].get("tp1"),
        "tp2_px": payload["tp"].get("tp2") or payload["tp"]["price"],
        "qty": None,  # we don't size positions; optional for calcs
        "opened_at": None,
        "closed_at": None,
        "session": payload["session"],
        "timeframe": payload["timeframe"],
        "idea": payload["idea"],
        "confidence": payload["confidence"],
        "log": [],
    }
    save_state()

async def latest_price(symbol: str) -> Optional[float]:
    try:
        df = await fetch_ohlcv(symbol, "1m", period="7d")
        if df is None or df.empty: return None
        return float(df["Close"].iloc[-1])
    except Exception:
        return None

async def monitor_trades_job(context: ContextTypes.DEFAULT_TYPE):
    if not STATE["portfolio"]["active_trades"]: return
    ts = ist_iso()
    chat_id = STATE["runtime"].get("chat_id") or ADMIN_CHAT_ID
    to_close = []
    for tid, tr in list(STATE["portfolio"]["active_trades"].items()):
        sym = tr["symbol"]
        px = await latest_price(sym)
        if px is None: continue
        # PENDING -> entry trigger
        if tr["state"] == "PENDING" and px >= tr["entry_px"]:
            tr["state"] = "OPEN"; tr["opened_at"] = ts
            tr["log"].append({"ts": ts, "event": "ENTRY", "px": px})
            # notify
            if chat_id:
                html = (
                    f"<b>{ts}</b>\n<b>ENTRY FILLED • {sym}</b> at ₹{px:.2f} • SL ₹{tr['sl_px']:.2f} • TP1 {fmt_px(tr['tp1_px'])} • TP2 {fmt_px(tr['tp2_px'])}\n"
                    f"Auto trail to BE after TP1.\n<i>{COMPLIANCE}</i>"
                ); await send_html(context, int(chat_id), html)
        # OPEN -> TP1/TP2/STOP
        elif tr["state"] == "OPEN":
            if tr["tp1_px"] and px >= tr["tp1_px"]:
                tr["state"] = "TP1"; tr["log"].append({"ts": ts, "event": "TP1", "px": px})
                # move SL to breakeven
                tr["sl_px"] = tr["entry_px"]
                if chat_id:
                    html = (
                        f"<b>{ts}</b>\n<b>UPDATE • {sym}</b> TP1 hit at ₹{px:.2f} • SL moved to BE (₹{tr['sl_px']:.2f}) • TP2 {fmt_px(tr['tp2_px'])}\n"
                        f"<i>{COMPLIANCE}</i>"
                    ); await send_html(context, int(chat_id), html)
            elif px <= tr["sl_px"]:
                tr["state"] = "STOP"; tr["closed_at"] = ts; tr["log"].append({"ts": ts, "event": "SL", "px": px}); to_close.append(tid)
                if chat_id:
                    pnl = pnl_net_trade(tr, px)
                    html = (
                        f"<b>{ts}</b>\n<b>STOP LOSS • {sym}</b> at ₹{px:.2f} • Net P&L est ₹{pnl:.2f}\n<i>{COMPLIANCE}</i>"
                    ); await send_html(context, int(chat_id), html)
            elif px >= tr["tp2_px"]:
                tr["state"] = "CLOSED"; tr["closed_at"] = ts; tr["log"].append({"ts": ts, "event": "TP2", "px": px}); to_close.append(tid)
                if chat_id:
                    pnl = pnl_net_trade(tr, px)
                    html = (
                        f"<b>{ts}</b>\n<b>TARGET HIT • {sym}</b> at ₹{px:.2f} • Net P&L est ₹{pnl:.2f}\n<i>{COMPLIANCE}</i>"
                    ); await send_html(context, int(chat_id), html)
        elif tr["state"] == "TP1":
            if px <= tr["sl_px"]:
                tr["state"] = "CLOSED"; tr["closed_at"] = ts; tr["log"].append({"ts": ts, "event": "BE_EXIT", "px": px}); to_close.append(tid)
                if chat_id:
                    html = (
                        f"<b>{ts}</b>\n<b>EXIT @ BE • {sym}</b> at ₹{px:.2f} after TP1 • Risk-free outcome\n<i>{COMPLIANCE}</i>"
                    ); await send_html(context, int(chat_id), html)
            elif px >= tr["tp2_px"]:
                tr["state"] = "CLOSED"; tr["closed_at"] = ts; tr["log"].append({"ts": ts, "event": "TP2", "px": px}); to_close.append(tid)
                if chat_id:
                    pnl = pnl_net_trade(tr, px)
                    html = (
                        f"<b>{ts}</b>\n<b>TARGET HIT • {sym}</b> at ₹{px:.2f} • Net P&L est ₹{pnl:.2f}\n<i>{COMPLIANCE}</i>"
                    ); await send_html(context, int(chat_id), html)
    # close moved trades
    for tid in to_close:
        STATE["portfolio"]["closed_trades"].append(STATE["portfolio"]["active_trades"].pop(tid))
    if to_close:
        save_state()

def fmt_px(v):
    return "—" if v is None else f"₹{float(v):.2f}"

def pnl_net_trade(tr: Dict, exit_px: float) -> float:
    # purely illustrative — assumes 1 share for net estimate
    c = STATE["config"]
    cps = cost_per_share(tr["entry_px"], exit_px, c)
    return (exit_px - tr["entry_px"]) - cps

# ============================ MARKET CALENDAR =============================

def is_market_open() -> bool:
    ts = now_ist()
    if ts.weekday() >= 5:  # Sat/Sun
        return False
    try:
        holidays = json.load(open(HOLIDAY_CACHE_PATH)).get("holidays", []) if os.path.exists(HOLIDAY_CACHE_PATH) else []
        if str(ts.date()) in holidays:
            return False
    except Exception:
        pass
    return in_window(ts, STATE["schedule"]["market_window"]["start"], STATE["schedule"]["market_window"]["end"])

# ================================ COMMANDS ================================

HELP = textwrap.dedent(f"""
{BOT_NAME} ({VERSION}) — NSE/BSE • IST • Hands‑Free Scanner
<i>{COMPLIANCE}</i>

Core:
/start, /help — this help
/configure key=value ... — update costs/tick/session/square_off (enables AutoScan)
/universe TCS,RELIANCE,... — set symbols
/universe_nse "NIFTY 50" — set from NSE index
/indices — list available NSE indices (preview)
/timeframe 1m|5m|15m|1h|D — default tf for /alert
/schedule tf=1h,D window=09:15-15:30 digest=60m — cadence & window
/autoscan on|off — toggle autonomous scans
/strategy auto|ema|rsi2|donchian|hybrid — selection bias
/sl atr:2.5 | structure | trail:3
/tp r:2 | multi:on tp1=1.5 tp2=3.0
/filters min_adv=5e7 max_spread=0.35 band_guard=on min_rr_net=1.3
/session intraday|swing square_off=15:20
/aggressive on|off — allow RR(net)<min
/alert SYMBOL [tf] [session] [strategy] — manual proposal
/backtest SYMBOL start=YYYY-MM-DD end=YYYY-MM-DD [strategy=auto] [capital=100000]
/portfolio — open/closed trade snapshot
/status — full state snapshot
/reset — factory defaults
""")

async def bind_chat(update: Update):
    cid = update.effective_chat.id if update and update.effective_chat else None
    if cid:
        STATE["runtime"]["chat_id"] = cid
        save_state()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    await update.message.reply_text(HELP, parse_mode="HTML")

def parse_kv(args: List[str]) -> Dict[str,str]:
    kv = {}
    for a in args:
        if "=" in a:
            k,v = a.split("=",1); kv[k.strip()] = v.strip()
    return kv

async def cmd_configure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    kv = parse_kv(context.args)
    # costs
    for k in list(kv.keys()):
        if k in STATE["config"]["costs"]:
            try: STATE["config"]["costs"][k] = float(kv.pop(k))
            except: STATE["config"]["costs"][k] = kv.pop(k)
    if "tick_size" in kv: STATE["config"]["tick_size"] = float(kv.pop("tick_size"))
    if "valid_minutes" in kv: STATE["config"]["valid_minutes"] = int(kv.pop("valid_minutes"))
    if "session" in kv: STATE["config"]["session"] = kv.pop("session")
    if "square_off" in kv: STATE["config"]["square_off_time"] = kv.pop("square_off")
    # filters
    if "min_adv" in kv: STATE["config"]["filters"]["min_adv_inr"] = float(kv.pop("min_adv"))
    if "max_spread" in kv: STATE["config"]["filters"]["max_spread_pct"] = float(kv.pop("max_spread"))
    if "band_guard" in kv: STATE["config"]["filters"]["band_guard"] = kv.pop("band_guard").lower() in ["on","true","1","yes"]
    if "min_rr_net" in kv: STATE["config"]["filters"]["min_rr_net"] = float(kv.pop("min_rr_net"))
    # TP multi toggles
    if "multi" in kv:
        STATE["config"]["tp"]["multi"]["enable"] = kv.pop("multi").lower() in ["on","true","1","yes"]
    if "tp1" in kv: STATE["config"]["tp"]["multi"]["tp1_r"] = float(kv.pop("tp1"))
    if "tp2" in kv: STATE["config"]["tp"]["multi"]["tp2_r"] = float(kv.pop("tp2"))
    # turn on autoscan
    STATE["schedule"]["autoscan"] = True
    STATE["runtime"]["autoscan"] = "on"
    save_state()
    await update.message.reply_text(
        f"<b>{ist_iso()}</b>\n<b>AUTOSCAN ON</b> — {len(STATE['config']['universe'])} symbols • TF {', '.join(STATE['schedule']['timeframes'])}\n"
        f"Window {STATE['schedule']['market_window']['start']}-{STATE['schedule']['market_window']['end']} IST\n"
        f"<i>{COMPLIANCE}</i>\n<pre><code>{json.dumps(STATE['schedule'], indent=2)}</code></pre>", parse_mode="HTML")

async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text("Usage: /universe TCS,RELIANCE,..."); return
    syms = [s.strip().upper().replace(".NS", "").replace(".BO", "") for s in " ".join(context.args).split(",") if s.strip()]
    STATE["config"]["universe"] = syms; save_state()
    await update.message.reply_text(f"Universe set: {', '.join(STATE['config']['universe'])}")

async def cmd_universe_nse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text('Usage: /universe_nse "NIFTY 50"'); return
    idx = " ".join(context.args).strip().strip('"').strip("'")
    syms = fetch_nse_index_symbols(idx)
    if not syms:
        await update.message.reply_text(f"Could not fetch symbols for {idx}."); return
    STATE["config"]["universe"] = syms; save_state()
    preview = ", ".join(syms[:20]) + (" …" if len(syms)>20 else "")
    await update.message.reply_text(f"Universe set from NSE index {idx}: {len(syms)} symbols.\n{preview}")

async def cmd_indices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        ses = _nse_session(); r = ses.get(NSE_INDEX_LIST_URL, timeout=12); r.raise_for_status(); j = r.json()
        names = sorted({it.get("index") for it in j.get("data", []) if it.get("index")})
    except Exception:
        names = []
    if not names:
        await update.message.reply_text("Could not fetch index list from NSE now."); return
    await update.message.reply_text(
        f"Known indices (partial): {', '.join(names[:60])}\n\nExample: /universe_nse \"NIFTY 50\""
    )

async def cmd_timeframe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: await update.message.reply_text("Usage: /timeframe 1m|5m|15m|1h|D"); return
    STATE["config"]["timeframe"] = context.args[0]
    save_state(); await update.message.reply_text(f"Default timeframe set: {STATE['config']['timeframe']}")

async def cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kv = parse_kv(context.args)
    if "tf" in kv: STATE["schedule"]["timeframes"] = [t.strip() for t in kv["tf"].split(",") if t.strip()]
    if "window" in kv and "-" in kv["window"]:
        st,en = kv["window"].split("-"); STATE["schedule"]["market_window"] = {"start":st, "end":en}
    if "digest" in kv:
        STATE["schedule"]["digest"]["enabled"] = True
        STATE["schedule"]["digest"]["interval_minutes"] = int(kv["digest"].rstrip("m"))
    save_state(); await update.message.reply_text(f"Schedule updated.\n<pre><code>{json.dumps(STATE['schedule'], indent=2)}</code></pre>", parse_mode="HTML")

async def cmd_autoscan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = context.args[0].lower() if context.args else "on"
    on = mode == "on"
    STATE["schedule"]["autoscan"] = on
    STATE["runtime"]["autoscan"] = "on" if on else "off"
    save_state(); await update.message.reply_text(f"AUTOSCAN {'ON' if on else 'OFF'}")

async def cmd_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: await update.message.reply_text("Usage: /strategy auto|ema|rsi2|donchian|hybrid"); return
    STATE["config"]["signal"]["name"] = context.args[0].lower(); save_state()
    await update.message.reply_text(f"Strategy set: {STATE['config']['signal']['name']}")

async def cmd_sl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: await update.message.reply_text("Usage: /sl atr:2.5 | structure | trail:3"); return
    a = context.args[0].lower()
    if a.startswith("atr:"):
        STATE["config"]["sl"]["method"] = "atr"; STATE["config"]["sl"]["params"]["atr_k"] = float(a.split(":")[1])
    elif a.startswith("trail:"):
        STATE["config"]["sl"]["method"] = "trail"; STATE["config"]["sl"]["params"]["atr_k"] = float(a.split(":")[1])
    elif a == "structure":
        STATE["config"]["sl"]["method"] = "structure"
    save_state(); await update.message.reply_text(f"SL updated: {STATE['config']['sl']}")

async def cmd_tp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: await update.message.reply_text("Usage: /tp r:2 | multi:on tp1=1.5 tp2=3.0"); return
    kv = parse_kv(context.args)
    if any(s.startswith("r:") for s in context.args):
        r = float([s for s in context.args if s.startswith("r:")][0].split(":")[1])
        STATE["config"]["tp"]["method"] = "r"; STATE["config"]["tp"]["params"]["r"] = r
    if "multi" in kv:
        STATE["config"]["tp"]["multi"]["enable"] = kv["multi"].lower() in ["on","true","1","yes"]
    if "tp1" in kv: STATE["config"]["tp"]["multi"]["tp1_r"] = float(kv["tp1"])
    if "tp2" in kv: STATE["config"]["tp"]["multi"]["tp2_r"] = float(kv["tp2"])
    save_state(); await update.message.reply_text(f"TP updated: {STATE['config']['tp']}")

async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kv = parse_kv(context.args)
    if "min_adv" in kv: STATE["config"]["filters"]["min_adv_inr"] = float(kv["min_adv"])
    if "max_spread" in kv: STATE["config"]["filters"]["max_spread_pct"] = float(kv["max_spread"])
    if "band_guard" in kv: STATE["config"]["filters"]["band_guard"] = kv["band_guard"].lower() in ["on","true","1","yes"]
    if "min_rr_net" in kv: STATE["config"]["filters"]["min_rr_net"] = float(kv["min_rr_net"])
    save_state(); await update.message.reply_text(f"Filters updated: {STATE['config']['filters']}")

async def cmd_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kv = parse_kv(context.args)
    if context.args and context.args[0] in ["intraday","swing"]:
        STATE["config"]["session"] = context.args[0]
    if "square_off" in kv: STATE["config"]["square_off_time"] = kv["square_off"]
    save_state(); await update.message.reply_text(f"Session: {STATE['config']['session']} • Square-off: {STATE['config']['square_off_time']}")

async def cmd_aggressive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: await update.message.reply_text("Usage: /aggressive on|off"); return
    STATE["config"]["aggressive"] = context.args[0].lower() == "on"; save_state()
    await update.message.reply_text(f"Aggressive mode: {'ON' if STATE['config']['aggressive'] else 'OFF'}")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    snap = {"config": STATE["config"], "schedule": STATE["schedule"], "runtime": {**STATE["runtime"], "ist_ts": ist_iso()}}
    await update.message.reply_text(f"<pre><code>{json.dumps(snap, indent=2)}</code></pre>", parse_mode="HTML")

async def cmd_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text("Usage: /alert SYMBOL [tf] [session] [strategy]"); return
    sym = context.args[0].upper()
    tf = context.args[1] if len(context.args)>=2 else STATE["config"]["timeframe"]
    sess = context.args[2] if len(context.args)>=3 else STATE["config"]["session"]
    strat = context.args[3] if len(context.args)>=4 else STATE["config"]["signal"]["name"]
    # temporarily force strategy in config for this call
    cfg = json.loads(json.dumps(STATE["config"]))
    cfg["signal"]["name"] = strat
    try:
        setup = await pick_setup(sym, tf, cfg)
        if not setup:
            await update.message.reply_text("No qualifying setup right now."); return
        payload = build_payload_from_setup(setup)
        html = build_alert_html(payload, cfg)
        await update.message.reply_text(html, parse_mode="HTML", reply_markup=tip_keyboard(), disable_web_page_preview=True)
        arm_pending_trade_from_alert(payload)
        save_state()
    except Exception as e:
        await update.message.reply_text(f"Cannot compute alert: {e}")

async def cmd_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    act = STATE["portfolio"]["active_trades"]
    closed_recent = STATE["portfolio"]["closed_trades"][-10:]
    def fmt_trade(t):
        return f"{t['symbol']} • {t['state']} • entry ₹{t['entry_px']:.2f} • SL ₹{t['sl_px']:.2f} • TP2 {fmt_px(t['tp2_px'])}"
    lines = ["<b>OPEN TRADES</b>" if act else "<b>OPEN TRADES</b> — none"]
    for t in act.values(): lines.append(fmt_trade(t))
    lines.append("\n<b>RECENTLY CLOSED (last 10)</b>")
    for t in closed_recent: lines.append(f"{t['symbol']} • {t['state']} • closed {t.get('closed_at','')} at events={len(t.get('log',[]))}")
    await update.message.reply_text("\n".join(lines) + f"\n\n<i>{COMPLIANCE}</i>", parse_mode="HTML")

# -------- Improved /backtest (daily, next-open fills, SL priority if both hit) --------

async def cmd_backtest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /backtest SYMBOL start=YYYY-MM-DD end=YYYY-MM-DD [strategy=auto] [capital=100000]"); return
    sym = context.args[0].upper(); kv = parse_kv(context.args[1:])
    start = kv.get("start"); end = kv.get("end"); strat = kv.get("strategy", STATE["config"]["signal"]["name"]); capital = float(kv.get("capital", 100000))
    if not start or not end: await update.message.reply_text("Provide start and end (YYYY-MM-DD)"); return
    try:
        df = await fetch_ohlcv(sym, "D", start=start, end=end)
        if df is None or len(df) < 260: raise RuntimeError("Need ≥ ~1y daily data.")
        # Precompute indicators once for whole period
        close, high, low, vol = df["Close"], df["High"], df["Low"], df["Volume"]
        cfg = json.loads(json.dumps(STATE["config"]))
        ema_fast_n = cfg["signal"]["params"].get("ema_fast", 20)
        ema_slow_n = cfg["signal"]["params"].get("ema_slow", 50)
        don_n = cfg["signal"]["params"].get("donchian_n", 20)
        rsi2_th = cfg["signal"]["params"].get("rsi2_th", 10)
        ema_f = ema(close, ema_fast_n); ema_s = ema(close, ema_slow_n)
        sma200 = sma(close, 200)
        a14 = atr(high, low, close, 14)
        don_hi, _ = donchian(high, low, don_n)
        vol_ma = vol.rolling(20).mean()
        atr_pctile = a14.rolling(100).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1]*100, raw=False)
        rsi2_now = rsi2(close)
        # Benchmark RS (daily)
        bench = await get_benchmark_df(cfg, "D")
        rs_rate = rs_rating_from(df, bench)
        indic = pd.DataFrame({
            "Close": close, "High": high, "Low": low, "Volume": vol,
            "ema_f": ema_f, "ema_s": ema_s, "sma200": sma200, "atr": a14,
            "don_hi": don_hi.shift(1), "vol_ma": vol_ma, "rsi2": rsi2_now,
            "atr_pctile": atr_pctile, "rs_rating": rs_rate
        }).dropna()
        tick = cfg.get("tick_size", 0.05)
        cash = capital; pos = 0; entry_px = sl_px = tp_px = None
        trades = []
        i_start = max(200, indic.index.get_indexer([indic.index[0]])[0])  # ensure enough history
        for i in range(200, len(indic)-1):
            today = indic.iloc[i]
            nxt = df.iloc[i+1]
            n_date = str(df.index[i+1].date())
            # liquidity gate
            dv = (df["Close"]*df["Volume"]).rolling(20).mean().iloc[i]
            if dv < cfg["filters"]["min_adv_inr"]:
                continue
            if pos == 0:
                # gates: trend + RS
                if not (today["Close"] >= today["sma200"] and today["ema_f"] >= today["ema_s"]): 
                    continue
                if today["rs_rating"] < cfg["signal"]["params"]["rs"].get("min_rating", 70):
                    continue
                vol_ok = cfg["signal"]["params"].get("vol_pctile_low", 40) <= today["atr_pctile"] <= cfg["signal"]["params"].get("vol_pctile_high", 70)
                vol_conf = today["Volume"] > (today["vol_ma"] or 0)
                signals = []
                if today["Close"] <= today["ema_f"] and vol_conf:
                    signals.append(("EMA Pullback", 65))
                if today["rsi2"] <= rsi2_th and vol_ok and vol_conf:
                    signals.append(("RSI2 Dip", 75))
                if today["Close"] >= today["don_hi"] and vol_ok and vol_conf:
                    signals.append(("Donchian Breakout", 70))
                if not signals: 
                    continue
                best = max(signals, key=lambda x: x[1])[0]
                bump = tick if tick >= 0.05 else 0.01
                raw_entry = max(float(today["High"])+bump, float(today["don_hi"])+bump if not math.isnan(today["don_hi"]) else 0)
                entry = round(raw_entry/tick)*tick
                swing_low = float(df["Low"].rolling(10).min().iloc[i-1])
                sl_atr = entry - cfg["sl"]["params"].get("atr_k", 2.5)*float(today["atr"])
                sl_struct = swing_low*0.99
                sl = round(min(sl_atr, sl_struct)/tick)*tick
                # Multi-TP: final tp for RR
                tp = entry + (cfg["tp"]["multi"]["tp2_r"] if cfg["tp"]["multi"]["enable"] else cfg["tp"]["params"]["r"]) * (entry - sl)
                tp = round(tp/tick)*tick
                if not (sl < entry < tp): 
                    continue
                # next day open entry (gap logic)
                px_open = float(nxt["Open"])
                fill = max(entry, px_open); fill = round(fill/tick)*tick
                cps = cost_per_share(fill, fill, cfg)
                qty = int(cash // (fill + cps))
                if qty>0:
                    pos = qty; entry_px = fill; sl_px = sl; tp_px = tp; cash -= qty*fill + cps*qty/2
                    trades.append({"date": n_date, "action":"BUY", "price": fill, "qty": qty, "cash": cash})
            else:
                day_high = float(nxt["High"]); day_low = float(nxt["Low"])
                exit_px = None; reason = None
                if day_low <= sl_px: exit_px = round(sl_px/tick)*tick; reason = "SL"
                elif day_high >= tp_px: exit_px = round(tp_px/tick)*tick; reason = "TP"
                if exit_px is not None:
                    cps = cost_per_share(entry_px, exit_px, cfg)
                    cash += pos*exit_px - cps*pos/2
                    trades.append({"date": n_date, "action":reason, "price": exit_px, "qty": -pos, "cash": cash})
                    pos = 0; entry_px = sl_px = tp_px = None
        if pos>0:
            last_close = float(df.iloc[-1]["Close"]); exit_px = round(last_close/tick)*tick
            cps = cost_per_share(entry_px, exit_px, cfg)
            cash += pos*exit_px - cps*pos/2
            trades.append({"date": str(df.index[-1].date()), "action":"EOD", "price": exit_px, "qty": -pos, "cash": cash})
        # metrics
        eq = pd.Series([capital] + [t["cash"] for t in trades])
        total_ret = (eq.iloc[-1] - capital) / capital
        days = max(1, (df.index[-1].date() - df.index[200].date()).days)
        cagr = (1+total_ret)**(365.25/days) - 1
        if len(eq) > 1:
            pnls = pd.Series(np.diff(eq)) / capital
            vol = float(np.std(pnls, ddof=1))*math.sqrt(252) if len(pnls)>1 else 0.0
            sharpe = float(np.mean(pnls)/(np.std(pnls,ddof=1)+1e-9))*math.sqrt(252) if len(pnls)>1 else 0.0
        else:
            vol = sharpe = 0.0
        vals = eq.values
        peak = np.maximum.accumulate(vals)
        dd = (vals - peak)/peak
        maxdd = float(dd.min()) if len(dd)>0 else 0.0
        html = (
            f"<b>{ist_iso()}</b>\n<b>BACKTEST • {sym} • D</b>\n"
            f"Period: {start}..{end} • Start ₹{capital:.2f} → End ₹{float(eq.iloc[-1]):.2f}\n"
            f"TotalReturn {total_ret*100:.2f}% • CAGR {cagr*100:.2f}% • Vol {vol*100:.2f}% • Sharpe {sharpe:.2f} • MaxDD {maxdd*100:.2f}%\n"
            f"Trades {sum(1 for t in trades if t['action'] in ['TP','SL','EOD'])}\n<i>{COMPLIANCE}</i>"
        )
        await update.message.reply_text(html, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"Backtest failed: {e}")

async def cmd_portfolio_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    STATE["portfolio"]["active_trades"].clear(); save_state(); await update.message.reply_text("Active trades cleared (monitor reset).")

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global STATE, LAST_ALERT_VALID, SYMBOL_COUNT
    STATE = json.loads(json.dumps(DEFAULT_STATE)); LAST_ALERT_VALID = {}; SYMBOL_COUNT = {}
    save_state(); await update.message.reply_text("Reset to factory defaults.")

async def guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Use commands only. See /help.")

# ================================ JOB WIRING ===============================

def setup_jobs(app: Application):
    jq: JobQueue = app.job_queue
    jq.run_repeating(scheduler_job, interval=1.0, first=1.5, name="scheduler")
    jq.run_repeating(digest_job, interval=60*max(1, STATE["schedule"]["digest"]["interval_minutes"]), first=60, name="digest")
    jq.run_repeating(eod_job, interval=60, first=15, name="eod")
    jq.run_repeating(monitor_trades_job, interval=20, first=10, name="monitor")
    # holiday refresh daily 06:00 IST
    jq.run_daily(lambda ctx: asyncio.create_task(fetch_nse_holidays()), time=dtime(hour=6, minute=0, tzinfo=IST), name="holiday_refresh")

# ================================ BUILD/RUN ===============================

def build_app() -> Application:
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN secret or .env.")
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("configure", cmd_configure))
    app.add_handler(CommandHandler("universe", cmd_universe))
    app.add_handler(CommandHandler("universe_nse", cmd_universe_nse))
    app.add_handler(CommandHandler("indices", cmd_indices))
    app.add_handler(CommandHandler("timeframe", cmd_timeframe))
    app.add_handler(CommandHandler("schedule", cmd_schedule))
    app.add_handler(CommandHandler("autoscan", cmd_autoscan))
    app.add_handler(CommandHandler("strategy", cmd_strategy))
    app.add_handler(CommandHandler("sl", cmd_sl))
    app.add_handler(CommandHandler("tp", cmd_tp))
    app.add_handler(CommandHandler("filters", cmd_filters))
    app.add_handler(CommandHandler("session", cmd_session))
    app.add_handler(CommandHandler("aggressive", cmd_aggressive))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("alert", cmd_alert))
    app.add_handler(CommandHandler("backtest", cmd_backtest))
    app.add_handler(CommandHandler("portfolio", cmd_portfolio))
    app.add_handler(CommandHandler("portfolio_clear", cmd_portfolio_clear))
    app.add_handler(CommandHandler("reset", cmd_reset))

    app.add_handler(MessageHandler(~filters.COMMAND, guard))

    setup_jobs(app)
    return app

if __name__ == "__main__":
    load_state()
    # prime holidays cache on boot (non-blocking)
    try:
        asyncio.run(fetch_nse_holidays())
    except RuntimeError:
        # if event loop already running (rare on certain hosts), ignore — jobs will refresh later
        pass
    app = build_app()
    log.info(f"Starting {BOT_NAME} ({VERSION}) …")
    app.run_polling()  # synchronous run (no asyncio.run to avoid loop conflicts on Replit)
