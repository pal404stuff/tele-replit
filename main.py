# IndiQuant Apex AutoScan — Ultimate Long-Term Edition (v5.0.0)
# =====================================================================
# NSE/BSE • IST • Hands-Free • Universe up to ~all-listable symbols
# Long-term swing alerts (Daily/Weekly), position-sizing hints,
# multi-TP with breakeven trail, persistent state, holiday-aware,
# async scanning with concurrency & caching, daily backtests.
#
# COMPLIANCE: Educational only — not investment advice. Not SEBI-registered. Markets carry risk.
# =====================================================================
# Replit Setup
# 1) Secrets (.env): TELEGRAM_BOT_TOKEN="your_bot_token"  (optional) ADMIN_CHAT_ID="123456789"
# 2) requirements.txt:
#    python-telegram-bot==21.4
#    yfinance==0.2.43
#    pandas==2.2.2
#    numpy==1.26.4
#    requests==2.32.3
#
# Quickstart in Telegram:
# /reset
# /configure session=swing valid_minutes=2880 aggressive=true risk_pct=2 capital=1000000
# /universe_all  (or /universe_nse "NIFTY 500" or /universe RELIANCE,TCS,...)
# /schedule tf=D window=09:15-15:30 digest=EOD
# /autoscan on
# /status
#
# Backtest example:
# /backtest RELIANCE start=2015-01-01 end=2025-09-30

from __future__ import annotations
import os, json, uuid, logging, math, asyncio, time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf

from telegram import Update
from telegram.ext import (
    ApplicationBuilder, Application, CommandHandler, MessageHandler,
    ContextTypes, JobQueue, filters
)

# ============================== CONSTANTS ==================================
IST = ZoneInfo("Asia/Kolkata")
BOT = "IndiQuant Apex Ultimate (LT)"
VER = "v5.0.0"
COMPLIANCE = "Educational only — not investment advice. Not SEBI-registered. Markets carry risk."

STATE_PATH = "/mnt/data/indiquant_state_v5.json"
NSE_CACHE_PATH = "/mnt/data/nse_index_cache_v5.json"
HOLIDAY_CACHE_PATH = "/mnt/data/nse_holidays_v5.json"
BACKTEST_DIR = "/mnt/data/backtests"

DATA_CACHE_TTL = 90  # seconds
NSE_INDEX_TTL = 12*3600
MAX_CONCURRENCY = 16  # async fetch/scan concurrency

HTTP_HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}

NSE_BASE = "https://www.nseindia.com"
NSE_EQ_INDEX_URL = f"{NSE_BASE}/api/equity-stockIndices"
NSE_INDICES_LIST_URL = f"{NSE_BASE}/api/allIndices"
NSE_HOLIDAY_URL = f"{NSE_BASE}/api/holiday-master?type=trading"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("indiquant.v5")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")

# ============================== DEFAULT STATE ===============================
DEFAULT_STATE: Dict = {
    "config": {
        "timezone": "Asia/Kolkata",
        "universe": ["RELIANCE", "TCS", "HDFCBANK", "NIFTYBEES"],
        "exchange": "NSE",                 # NSE | BSE | MIX (suffix mapping)
        "timeframe": "D",                  # default for manual /alert, autoscan uses schedule
        "session": "swing",                # swing for >=1h/D (no intraday)
        "square_off_time": "15:20",
        "data": {"source": "yfinance", "adjusted": True},
        "signal": {"name": "apex_long", "params": {
            "ema_fast": 20, "ema_slow": 50, "rsi2_th": 10,
            "donchian_n": 20, "atr_k_sl": 2.5,
            "vol_pctile_low": 30, "vol_pctile_high": 85,
            "mtf": {"enable": True, "htf": "1wk"},  # MTF: weekly filter
            "rs": {"enable": True, "benchmark": "NIFTYBEES", "min_rating": 70}
        }},
        "sl": {"method": "atr_or_structure"},
        "tp": {"method": "multi", "params": {"r": 2.0},
               "multi": {"enable": True, "tp1_r": 1.5, "tp2_r": 3.0, "trail_be": True, "trail_atr": True, "trail_atr_k": 3.0}},
        "filters": {"min_adv_inr": 7.5e7, "max_spread_pct": 0.75, "band_guard": True, "min_rr_net": 1.4},
        "costs": {
            "brokerage_model": "flat",       # or "percent"
            "brokerage_flat_inr": 20.0,      # per side
            "brokerage_pct": 0.0003,
            "stt_pct": 0.001,
            "exchange_txn_pct": 0.0000325,
            "sebi_fee_pct": 0.000001,
            "stamp_duty_pct": 0.00015,
            "gst_pct": 0.18,
            "slippage_bps": 5
        },
        "tick_size": 0.05,
        "valid_minutes": 2880,  # 48 hours by default for daily bars
        "aggressive": True,
        "capital": 1_000_000.0,  # for sizing hints
        "risk_pct": 2.0,         # % risk per trade (hint only)
        "max_open_trades": 12    # soft cap (for hints/digest)
    },
    "schedule": {
        "market_window": {"start": "09:15", "end": "15:30"},
        "timeframes": ["D"],                # Long-term scans only by default
        "autoscan": False,
        "quiet_windows": [],
        "digest": {"enabled": True, "interval_minutes": 120, "eod": True},  # EOD digest
        "throttle": {"max_per_min": 15, "max_per_symbol": 1},
        "live_ticks": False
    },
    "runtime": {
        "ist_ts": None,
        "autoscan": "off",
        "scan_status": "idle",
        "last_bar_close": {"D": None},
        "counters": {"alerts_sent": 0, "suppressed": 0, "deduped": 0, "skipped": 0},
        "chat_id": ADMIN_CHAT_ID or None,
        "nse_holidays": []
    },
    "portfolio": {
        "active": {},    # trade_id -> trade dict
        "closed": []     # list of trade dicts
    },
    "dedupe": {}         # (symbol|tf) -> valid_until ISO
}
STATE: Dict = {}

# Caches & throttles
DATA_CACHE: Dict[Tuple[str,str,str,str], Tuple[pd.DataFrame, float]] = {}
NSE_CACHE_MEM: Dict[str, Tuple[List[str], float]] = {}
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

def reset_throttle(ts: datetime):
    global CURRENT_MINUTE, SENT_THIS_MINUTE
    key = ts.strftime("%Y-%m-%d %H:%M")
    if key != CURRENT_MINUTE:
        CURRENT_MINUTE = key
        SENT_THIS_MINUTE = 0

def symbol_session_key(sym: str, ts: datetime) -> Tuple[str,str]:
    return (sym.upper(), ts.strftime("%Y-%m-%d"))

def deep_merge(base: Dict, overlay: Dict) -> Dict:
    out = json.loads(json.dumps(base))
    def rec(dst, src):
        for k,v in src.items():
            if isinstance(dst.get(k), dict) and isinstance(v, dict):
                rec(dst[k], v)
            else:
                dst[k] = v
    rec(out, overlay)
    return out

def ensure_dirs():
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        os.makedirs(os.path.dirname(NSE_CACHE_PATH), exist_ok=True)
        os.makedirs(os.path.dirname(HOLIDAY_CACHE_PATH), exist_ok=True)
        os.makedirs(BACKTEST_DIR, exist_ok=True)
    except Exception:
        pass

# ============================ PERSISTENCE ==================================

def load_state() -> None:
    global STATE
    try:
        ensure_dirs()
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r") as f:
                disk = json.load(f)
            STATE = deep_merge(DEFAULT_STATE, disk)
            log.info("State loaded from disk.")
        else:
            STATE = json.loads(json.dumps(DEFAULT_STATE))
            log.info("State initialized with defaults.")
    except Exception as e:
        log.warning(f"State load failed, using defaults: {e}")
        STATE = json.loads(json.dumps(DEFAULT_STATE))

def save_state() -> None:
    try:
        ensure_dirs()
        with open(STATE_PATH, "w") as f:
            json.dump(STATE, f)
    except Exception as e:
        log.warning(f"State save failed: {e}")

# =============================== NSE HELPERS ===============================

def _nse_session() -> requests.Session:
    s = requests.Session(); s.headers.update(HTTP_HEADERS)
    try: s.get(NSE_BASE, timeout=10)
    except Exception: pass
    return s

def fetch_nse_index_symbols(index_name: str, ttl_sec: int = NSE_INDEX_TTL) -> List[str]:
    now = time.time()
    key = index_name.strip().upper()
    if key in NSE_CACHE_MEM and now < NSE_CACHE_MEM[key][1]:
        return NSE_CACHE_MEM[key][0]
    try:
        if os.path.exists(NSE_CACHE_PATH):
            disk = json.load(open(NSE_CACHE_PATH))
            if key in disk and now < disk[key]["exp"]:
                NSE_CACHE_MEM[key] = (disk[key]["symbols"], disk[key]["exp"])
                return disk[key]["symbols"]
    except Exception:
        pass
    out: List[str] = []
    try:
        ses = _nse_session()
        for attempt in range(3):
            r = ses.get(NSE_EQ_INDEX_URL, params={"index": index_name}, timeout=12)
            if r.status_code == 200:
                data = r.json().get("data", [])
                out = [ (row.get("symbol") or "").strip().upper() for row in data if row.get("symbol") ]
                break
            if r.status_code in (429, 403):
                time.sleep(1.2*(attempt+1)); continue
            r.raise_for_status()
    except Exception as e:
        log.warning(f"NSE index fetch failed for {index_name}: {e}")
    exp = now + ttl_sec
    NSE_CACHE_MEM[key] = (out, exp)
    try:
        disk = json.load(open(NSE_CACHE_PATH)) if os.path.exists(NSE_CACHE_PATH) else {}
        disk[key] = {"symbols": out, "exp": exp}
        json.dump(disk, open(NSE_CACHE_PATH, "w"))
    except Exception:
        pass
    return out

def fetch_all_indices_names() -> List[str]:
    try:
        ses = _nse_session()
        r = ses.get(NSE_INDICES_LIST_URL, timeout=12); r.raise_for_status()
        j = r.json()
        names = sorted({it.get("index") for it in j.get("data", []) if it.get("index")})
        return names
    except Exception as e:
        log.warning(f"Index list fetch failed: {e}")
        return []

def build_all_symbols_nse(max_indices: int = 60) -> List[str]:
    names = fetch_all_indices_names()
    if not names: return []
    # Prefer broad/sectoral indices first
    priority = [
        "NIFTY 50", "NIFTY NEXT 50", "NIFTY 100", "NIFTY 200", "NIFTY 500",
        "NIFTY MIDCAP 50", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
        "NIFTY SMALLCAP 50", "NIFTY SMALLCAP 100", "NIFTY SMALLCAP 250",
        "NIFTY MICROCAP 250"
    ]
    ordered = priority + [n for n in names if n not in priority]
    universe: List[str] = []
    for idx_name in ordered[:max_indices]:
        comps = fetch_nse_index_symbols(idx_name)
        universe.extend(comps)
    # Unique and cleaned
    uni = sorted({s for s in universe if s})
    return uni

# ============================ DATA LAYER (YF) ==============================

def yahoo_symbol(sym: str, exchange: str="NSE") -> str:
    s = sym.strip().upper()
    if s.endswith(".NS") or s.endswith(".BO"): return s
    if exchange.upper() == "BSE": return s + ".BO"
    return s + ".NS"

def _yf_download(tickers: str, interval: Optional[str]=None, period: Optional[str]=None,
                 start: Optional[str]=None, end: Optional[str]=None) -> pd.DataFrame:
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
    if tf.lower() in ("d", "1d", "day"):
        df = await loop.run_in_executor(None, lambda: _yf_download(yahoo_symbol(symbol, STATE["config"]["exchange"]), start=start, end=end, period=period or "10y"))
    elif tf.lower() in ("1wk", "w", "week"):
        df = await loop.run_in_executor(None, lambda: _yf_download(yahoo_symbol(symbol, STATE["config"]["exchange"]), interval="1wk", period=period or "20y"))
    else:
        raise ValueError("This LT bot supports D and 1wk only.")
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

def donchian_high(high: pd.Series, n: int=20) -> pd.Series:
    return high.rolling(n).max()

# ============================ STRATEGY ENGINE ==============================

def adv_inr(df: pd.DataFrame, window: int=20) -> Optional[float]:
    if "Close" not in df or "Volume" not in df: return None
    dv = (df["Close"] * df["Volume"]).rolling(window).mean().dropna()
    return float(dv.iloc[-1]) if not dv.empty else None

async def get_benchmark_df(cfg: Dict, tf: str) -> Optional[pd.DataFrame]:
    b = cfg["signal"]["params"]["rs"].get("benchmark", "NIFTYBEES")
    try:
        return await fetch_ohlcv(b, "1wk" if tf.lower().startswith("1wk") else "D")
    except Exception:
        return None

def rs_rating_from(df: pd.DataFrame, bench: Optional[pd.DataFrame], lookback: int=55) -> pd.Series:
    if bench is None or bench.empty: return pd.Series(50.0, index=df.index)
    j = df[["Close"]].join(bench[["Close"]].rename(columns={"Close":"Bench"}), how="inner").dropna()
    if j.empty: return pd.Series(50.0, index=df.index)
    stock_norm = j["Close"] / j["Close"].iloc[0]
    bench_norm = j["Bench"] / j["Bench"].iloc[0]
    rs = stock_norm / bench_norm
    out = rs.rolling(lookback).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1]*100, raw=False)
    out = out.reindex(df.index).ffill().fillna(50.0)
    return out

def round_tick(p: float, tick: float) -> float:
    return round(p / tick) * tick

async def pick_setup(symbol: str, tf: str, cfg: Dict) -> Optional[Dict]:
    df = await fetch_ohlcv(symbol, tf)
    if df is None or len(df) < 220: return None
    close, high, low, vol = df["Close"], df["High"], df["Low"], df["Volume"]
    ema_fast_n = cfg["signal"]["params"].get("ema_fast", 20)
    ema_slow_n = cfg["signal"]["params"].get("ema_slow", 50)
    don_n = cfg["signal"]["params"].get("donchian_n", 20)
    atr_k_sl = cfg["signal"]["params"].get("atr_k_sl", 2.5)

    ema_f = ema(close, ema_fast_n); ema_s = ema(close, ema_slow_n)
    sma200 = sma(close, 200)
    a14 = atr(high, low, close, 14)
    don_hi = donchian_high(high, don_n)
    vol_ma = vol.rolling(20).mean()
    atr_pctile = a14.rolling(150).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1]*100, raw=False)
    rsi2_now = rsi2(close)
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

    # MTF filter, weekly uptrend if enabled
    if cfg["signal"]["params"].get("mtf", {}).get("enable", True):
        try:
            df_w = await fetch_ohlcv(symbol, cfg["signal"]["params"]["mtf"].get("htf", "1wk"))
            if df_w is not None and len(df_w) > ema_slow_n+5:
                e_f = ema(df_w["Close"], ema_fast_n).iloc[-1]
                e_s = ema(df_w["Close"], ema_slow_n).iloc[-1]
                if not (e_f > e_s): return None
        except Exception:
            pass

    # Trend health & RS gate
    if not (last["close"] >= last["sma200"] and last["ema_f"] >= last["ema_s"]):
        return None
    if cfg["signal"]["params"]["rs"].get("enable", True) and last["rs_rating"] < cfg["signal"]["params"]["rs"].get("min_rating", 70):
        return None

    # Volatility regime & Volume confirmation
    vol_ok = cfg["signal"]["params"].get("vol_pctile_low", 30) <= last["atr_pctile"] <= cfg["signal"]["params"].get("vol_pctile_high", 85)
    vol_conf = last["vol"] > (last["vol_ma"] or 0)

    # Candidate signals (long-only)
    signals = []
    if (last["close"] <= last["ema_f"]) and vol_conf:
        signals.append(("EMA Pullback", 65))
    if (last["rsi2"] <= cfg["signal"]["params"].get("rsi2_th", 10)) and vol_ok and vol_conf:
        signals.append(("RSI2 Dip", 75))
    if (last["close"] >= last["don_hi"]) and vol_ok and vol_conf:
        signals.append(("Donchian Breakout", 70))
    if not signals: return None

    best = max(signals, key=lambda x: x[1])
    conf = best[1] + (10 if vol_ok else 0) + (10 if len(signals) > 1 else 0) + (10 if last["rs_rating"] > 85 else 0)

    tick = cfg.get("tick_size", 0.05)
    raw_entry = max(float(last["high"])+tick, float(last["don_hi"])+tick if not math.isnan(last["don_hi"]) else 0)
    entry = round_tick(raw_entry, tick)

    # SL = min(ATR stop, structure stop)
    swing_low = float(df["Low"].rolling(10).min().iloc[-2])
    sl_atr = entry - atr_k_sl * float(last["atr"])
    sl_struct = swing_low * 0.99
    sl_raw = min(sl_atr, sl_struct)
    sl = round_tick(sl_raw, tick)

    # Multi-TP
    tp_cfg = cfg["tp"]
    if tp_cfg.get("multi", {}).get("enable", True):
        tp1 = entry + tp_cfg["multi"].get("tp1_r", 1.5) * (entry - sl)
        tp2 = entry + tp_cfg["multi"].get("tp2_r", 3.0) * (entry - sl)
        tp = tp2
    else:
        tp = entry + tp_cfg["params"].get("r", 2.0) * (entry - sl)
        tp1, tp2 = None, None
    tp = round_tick(tp / tick, 1) * tick  # keep rounding chain stable
    tp1 = round_tick(tp1 / tick, 1) * tick if tp1 else None
    tp2 = round_tick(tp2 / tick, 1) * tick if tp2 else None

    if not (sl < entry < tp):
        return None

    gross, net, cps = net_rr(entry, sl, tp, cfg)
    if not cfg.get("aggressive", False) and net < cfg["filters"].get("min_rr_net", 1.4):
        return None

    # Sizing hint
    risk_amt = cfg.get("capital", 0) * (cfg.get("risk_pct", 0)/100.0)
    qty_hint = int(max(0, math.floor(risk_amt / max(1e-8, entry - sl))))

    return {
        "symbol": symbol.upper(),
        "timeframe": tf,
        "session": STATE["config"]["session"],
        "idea": best[0] + " + MTF + RS + VolConfirm",
        "confidence": int(max(0, min(100, conf))),
        "entry": {"type": "stop", "price": float(entry)},
        "sl": {"method": "ATR/Structure", "price": float(sl)},
        "tp": {"method": "multi" if tp1 else "R", "price": float(tp), "tp1": tp1, "tp2": tp2},
        "assumptions": {"data_src": "yfinance", "bar_size": tf, "tick_size": cfg.get("tick_size", 0.05), "costs_placeholders": True},
        "filters": {"adv_ok": True, "spread_ok": None, "band_guard_ok": None},
        "last": {"adv_inr": adv, "rs_rating": float(last["rs_rating"]), "atr_pctile": float(last["atr_pctile"])},
        "rr": {"gross": round(gross,2), "net": round(net,2)},
        "sizing_hint": {"risk_amt": round(risk_amt,2), "qty": qty_hint}
    }

# ============================= COSTS / R:R ================================

def cost_per_share(entry: float, exit_px: float, C: Dict) -> float:
    c = C["costs"]
    turn = entry + exit_px
    brokerage = (c["brokerage_flat_inr"]*2) if c["brokerage_model"] == "flat" else (turn * c.get("brokerage_pct", 0.0))
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

# =============================== ALERTS ====================================

def build_alert_html(payload: Dict, cfg: Dict) -> str:
    ts = ist_iso()
    entry = payload["entry"]["price"]; sl = payload["sl"]["price"]
    tp = payload["tp"]["price"]; rr_g = payload["rr"]["gross"]; rr_n = payload["rr"]["net"]
    s_hint = payload.get("sizing_hint", {})
    qty = s_hint.get("qty"); risk_amt = s_hint.get("risk_amt")
    valid_until = (now_ist() + timedelta(minutes=cfg.get("valid_minutes",1440))).astimezone(IST).strftime("%H:%M")
    sqoff = cfg["square_off_time"] if payload["session"].lower()=="intraday" else "N/A"
    tp_line = f"TP: ₹{tp:.2f} (final)" if payload["tp"].get("tp1") is None else (
        f"TP1 ₹{payload['tp']['tp1']:.2f} • TP2 ₹{payload['tp']['tp2']:.2f}")
    costs = cfg["costs"]
    dec = " → ".join([
        "Objective: Buy setup (LT)",
        f"Data: yfinance D/1wk closed bars",
        f"Method: {payload['idea']}",
        "Controls: ADV gate; spread/bands n/a",
        "Levels: tick-rounded Entry/SL/TP",
        f"Net R:R {rr_n:.2f}:1; Valid {valid_until} IST"
    ])
    sizing = f"Hint (risk {cfg['risk_pct']}% of ₹{cfg['capital']:.0f}): qty ≈ {qty} (risk ≈ ₹{risk_amt:.0f})" if qty else "Sizing hint unavailable"
    html = (
        f"<b>{ts}</b>\n"
        f"<b>ALERT • {payload['symbol']} • {payload['timeframe']} • {payload['session'].capitalize()} (IST)</b>\n"
        f"Idea: {payload['idea']} • Confidence {payload['confidence']}%\n"
        f"Entry: <b>stop ₹{entry:.2f}</b> • SL: ₹{sl:.2f} ({payload['sl']['method']}) • {tp_line} → RR(gross) <b>{rr_g:.2f}:1</b>\n"
        f"Costs: brokerage {costs['brokerage_model']}, GST {costs['gst_pct']*100:.0f}%, STT {costs['stt_pct']*100:.2f}%, slippage {costs['slippage_bps']} bps → RR(net) <b>{rr_n:.2f}:1</b>\n"
        f"{sizing}\n"
        f"Timing: valid till {valid_until} • Session {payload['session']} • Square-off {sqoff} IST • Filters: ADV OK\n"
        f"Decision Trace: {dec}\n"
        f"<i>{COMPLIANCE}</i>\n"
        f"<pre><code>{json.dumps(payload, indent=2)}</code></pre>"
    )
    return html

async def send_html(context: ContextTypes.DEFAULT_TYPE, chat_id: int, html: str):
    await context.bot.send_message(chat_id=chat_id, text=html, parse_mode="HTML", disable_web_page_preview=True)

# =========================== AUTOSCAN & SCHEDULER ==========================

LAST_ALERT_VALID: Dict[str, str] = {}  # key = f"{sym}|{tf}" -> ISO time

def bar_close_now(ts: datetime, tf: str, schedule: dict) -> bool:
    # For long-term use D only; trigger at market end
    if tf.upper() == "D":
        return ts.strftime("%H:%M") == schedule["market_window"]["end"]
    return False

def is_market_open() -> bool:
    ts = now_ist()
    if ts.weekday() >= 5: return False
    try:
        hol = json.load(open(HOLIDAY_CACHE_PATH)).get("holidays", []) if os.path.exists(HOLIDAY_CACHE_PATH) else []
        if str(ts.date()) in hol: return False
    except Exception:
        pass
    mw = STATE["schedule"]["market_window"]
    return in_window(ts, mw["start"], mw["end"])

def build_payload_from_setup(setup: Dict) -> Dict:
    return {
        "$schema": "indiquant.alert.v5",
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
        "sizing_hint": setup.get("sizing_hint", {}),
        "notes": ["educational only", "LT daily closed-bar"]
    }

async def scan_symbol(sym: str, tf: str, cfg: Dict, context: ContextTypes.DEFAULT_TYPE, ts: datetime, sem: asyncio.Semaphore):
    global SENT_THIS_MINUTE
    async with sem:
        key = f"{sym.upper()}|{tf}"
        vu = LAST_ALERT_VALID.get(key)
        if vu and ts < datetime.fromisoformat(vu):
            STATE["runtime"]["counters"]["deduped"] += 1
            return
        if SENT_THIS_MINUTE >= STATE["schedule"]["throttle"]["max_per_min"]:
            STATE["runtime"]["counters"]["suppressed"] += 1
            return
        try:
            setup = await pick_setup(sym, tf, cfg)
            if not setup:
                STATE["runtime"]["counters"]["skipped"] += 1
                return
            payload = build_payload_from_setup(setup)
            html = build_alert_html(payload, cfg)
            chat_id = int(STATE["runtime"].get("chat_id") or (ADMIN_CHAT_ID or 0))
            if chat_id:
                await send_html(context, chat_id, html)
                STATE["runtime"]["counters"]["alerts_sent"] += 1
                SENT_THIS_MINUTE += 1
                LAST_ALERT_VALID[key] = ist_iso(ts + timedelta(minutes=cfg.get("valid_minutes", 1440)))
        except Exception as e:
            log.warning(f"scan error {sym}/{tf}: {e}")
            STATE["runtime"]["counters"]["skipped"] += 1

async def scheduler_job(context: ContextTypes.DEFAULT_TYPE):
    if not STATE["schedule"]["autoscan"]: return
    ts = now_ist()
    if not is_market_open(): return
    reset_throttle(ts)
    STATE["runtime"]["ist_ts"] = ist_iso(ts)
    STATE["runtime"]["scan_status"] = "running"
    # Only daily in LT mode
    if bar_close_now(ts, "D", STATE["schedule"]):
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        tasks = [scan_symbol(sym, "D", STATE["config"], context, ts, sem) for sym in STATE["config"]["universe"]]
        # chunk tasks to avoid burst
        for i in range(0, len(tasks), MAX_CONCURRENCY*2):
            await asyncio.gather(*tasks[i:i+MAX_CONCURRENCY*2])
        STATE["runtime"]["last_bar_close"]["D"] = ts.strftime("%H:%M")
    STATE["runtime"]["scan_status"] = "idle"
    save_state()

async def digest_job(context: ContextTypes.DEFAULT_TYPE):
    if not (STATE["schedule"]["autoscan"] and STATE["schedule"]["digest"]["enabled"]): return
    ts = now_ist()
    if not is_market_open() and not STATE["schedule"]["digest"]["eod"]: return
    chat_id = STATE["runtime"].get("chat_id") or ADMIN_CHAT_ID
    if not chat_id: return
    html = (
        f"<b>{ist_iso(ts)}</b>\n"
        f"<b>SESSION DIGEST</b> — Alerts {STATE['runtime']['counters']['alerts_sent']}, "
        f"Suppressed {STATE['runtime']['counters']['suppressed']}, Deduped {STATE['runtime']['counters']['deduped']}, Skipped {STATE['runtime']['counters']['skipped']}.\n"
        f"Universe: {len(STATE['config']['universe'])} symbols • TF D\n"
        f"<pre><code>{json.dumps(STATE['runtime'], indent=2)}</code></pre>\n"
        f"<i>{COMPLIANCE}</i>"
    )
    await send_html(context, int(chat_id), html)
    # Reset counters after EOD digest
    if STATE["schedule"]["digest"]["eod"] and not is_market_open():
        STATE["runtime"]["counters"] = {"alerts_sent":0,"suppressed":0,"deduped":0,"skipped":0}
        save_state()

async def refresh_holidays_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        ses = _nse_session()
        r = ses.get(NSE_HOLIDAY_URL, timeout=12)
        r.raise_for_status()
        j = r.json()
        arr = j.get("CBM", []) or j.get("trading", []) or []
        hol = []
        for it in arr:
            dt_str = it.get("tradingDate") or it.get("holidayDate")
            if not dt_str: continue
            try:
                dte = datetime.strptime(dt_str, "%d-%b-%Y").date()
                hol.append(str(dte))
            except Exception:
                pass
        json.dump({"holidays": hol, "fetched": ist_iso()}, open(HOLIDAY_CACHE_PATH, "w"))
        STATE["runtime"]["nse_holidays"] = hol
        save_state()
    except Exception as e:
        log.warning(f"Holiday refresh failed: {e}")

# ================================ BACKTEST =================================

def backtest_daily_next_open(df: pd.DataFrame, cfg: Dict) -> Tuple[pd.DataFrame, Dict]:
    """
    Deterministic daily backtest:
    - Compute signal on t close; enter on t+1 open via stop trigger (if Open >= Entry)
    - SL priority if both SL/TP touched within a day (assume worst-case)
    - Costs + slippage included in net P&L per share
    """
    if df is None or len(df) < 260:
        return pd.DataFrame(), {}

    # Precompute indicators
    close, high, low, vol = df["Close"], df["High"], df["Low"], df["Volume"]
    ema_f = ema(close, cfg["signal"]["params"].get("ema_fast",20))
    ema_s = ema(close, cfg["signal"]["params"].get("ema_slow",50))
    sma200 = sma(close, 200)
    a14 = atr(high, low, close, 14)
    don_hi = donchian_high(high, cfg["signal"]["params"].get("donchian_n",20))
    vol_ma = vol.rolling(20).mean()
    atr_pctile = a14.rolling(150).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1]*100, raw=False)
    rsi2_now = rsi2(close)

    # Iterate
    tick = cfg.get("tick_size",0.05)
    rows = []
    in_pos = False
    entry=None; sl=None; tp=None; tp1=None; trail_be = cfg["tp"]["multi"].get("trail_be",True); trail_atr = cfg["tp"]["multi"].get("trail_atr",True); trail_k = cfg["tp"]["multi"].get("trail_atr_k",3.0)
    for i in range(210, len(df)-1):
        # t close, t+1 open
        today = df.index[i]
        tom = df.index[i+1]
        last = pd.Series({
            "close": close.iloc[i], "high": high.iloc[i], "low": low.iloc[i],
            "ema_f": ema_f.iloc[i], "ema_s": ema_s.iloc[i], "sma200": sma200.iloc[i],
            "atr": a14.iloc[i], "don_hi": don_hi.shift(1).iloc[i], "vol": vol.iloc[i],
            "vol_ma": vol_ma.iloc[i], "rsi2": rsi2_now.iloc[i], "atr_pctile": atr_pctile.iloc[i]
        })
        adv = float((close*vol).rolling(20).mean().iloc[i] or 0)
        # If in position, evaluate SL/TP at next day's OHLC
        if in_pos:
            o,h,l,c = df.loc[tom, ["Open","High","Low","Close"]]
            # SL priority if both touched within the bar
            exit_px=None; exit_reason=None
            # Trail to BE after TP1
            if trail_be and tp1 and h >= tp1 and sl < entry:
                sl = entry
            # ATR trailing after TP1?
            if trail_atr and h >= tp1:
                new_sl = c - (trail_k * a14.iloc[i+1])
                sl = max(sl, round_tick(float(new_sl), tick))

            if l <= sl and h >= tp:   # both touched — assume SL first (conservative)
                exit_px = sl; exit_reason="SL"
            elif l <= sl:
                exit_px = sl; exit_reason="SL"
            elif h >= tp:
                exit_px = tp; exit_reason="TP"
            # gap above/below
            elif o <= sl:
                exit_px = o; exit_reason="SL_gap"
            elif o >= tp:
                exit_px = o; exit_reason="TP_gap"

            if exit_px is not None:
                cps = cost_per_share(entry, exit_px, cfg)
                pnl_net = (exit_px - entry) - cps
                rows.append({"date": tom.strftime("%Y-%m-%d"), "action": exit_reason, "price": float(exit_px), "pnl_net": float(pnl_net)})
                in_pos=False; entry=sl=tp=tp1=None
                continue

        # If flat, evaluate signal at today's close; place stop at tomorrow's open if triggered
        if not in_pos:
            # gates
            if not (last["close"] >= last["sma200"] and last["ema_f"] >= last["ema_s"]): continue
            if adv < cfg["filters"]["min_adv_inr"]: continue
            vol_ok = cfg["signal"]["params"].get("vol_pctile_low",30) <= last["atr_pctile"] <= cfg["signal"]["params"].get("vol_pctile_high",85)
            vol_conf = last["vol"] > (last["vol_ma"] or 0)

            signals=[]
            if (last["close"] <= last["ema_f"]) and vol_conf: signals.append(("EMA Pullback",65))
            if (last["rsi2"] <= cfg["signal"]["params"].get("rsi2_th",10)) and vol_ok and vol_conf: signals.append(("RSI2 Dip",75))
            if (last["close"] >= last["don_hi"]) and vol_ok and vol_conf: signals.append(("Donchian Breakout",70))
            if not signals: continue
            sig = max(signals, key=lambda x:x[1])

            raw_entry = max(float(last["high"])+tick, float(last["don_hi"])+tick if not math.isnan(last["don_hi"]) else 0)
            entry = round_tick(raw_entry, tick)
            sl_atr = entry - cfg["signal"]["params"].get("atr_k_sl",2.5)*float(last["atr"])
            sl_struct = float(df["Low"].rolling(10).min().iloc[i-1])*0.99
            sl = round_tick(min(sl_atr, sl_struct), tick)
            tp1 = round_tick(entry + cfg["tp"]["multi"].get("tp1_r",1.5)*(entry-sl), tick)
            tp  = round_tick(entry + cfg["tp"]["multi"].get("tp2_r",3.0)*(entry-sl), tick)
            # enter next bar if open >= entry (stop)
            o_next = float(df.loc[tom, "Open"])
            if o_next >= entry:
                rows.append({"date": tom.strftime("%Y-%m-%d"), "action": "ENTRY", "price": float(o_next), "pnl_net": 0.0})
                entry = o_next  # assume slip at open
                in_pos=True
            else:
                # keep order for the day: if high >= entry, fill at entry
                h_next = float(df.loc[tom, "High"])
                if h_next >= entry:
                    rows.append({"date": tom.strftime("%Y-%m-%d"), "action": "ENTRY", "price": float(entry), "pnl_net": 0.0})
                    in_pos=True
                else:
                    entry=sl=tp=tp1=None  # no fill
                    continue

    trades = pd.DataFrame(rows)
    if trades.empty:
        return trades, {}

    # Equity curve (per share, position sizing shown separately by user)
    pnl_series = trades["pnl_net"].astype(float)
    equity = pnl_series.cumsum()
    # Metrics
    if not pnl_series.empty:
        # approximate daily returns assuming one share for simplicity
        rets = pnl_series.replace(0, np.nan).dropna()  # realized only
        total_net = float(equity.iloc[-1])
        trades_n = int((trades["action"]=="TP").sum() + (trades["action"].str.startswith("SL")).sum())
        wins = int((trades["action"].str.contains("TP")).sum())
        losses = int((trades["action"].str.startswith("SL")).sum())
        winrate = (wins / max(1, wins+losses))*100
        avg = float(pnl_series.mean())
        pf = (pnl_series[pnl_series>0].sum() / abs(pnl_series[pnl_series<0].sum())) if (pnl_series[pnl_series<0].sum()!=0) else np.inf
        # rolling max drawdown on cumulative equity
        cum = equity
        peak = cum.cummax()
        dd = (cum-peak)
        maxdd = float(dd.min())
        metrics = {
            "TotalNet(₹/share)": round(total_net,2),
            "Trades": trades_n,
            "WinRate%": round(winrate,2),
            "AvgTrade(₹/share)": round(avg,2),
            "ProfitFactor": round(float(pf),2) if np.isfinite(pf) else float('inf'),
            "MaxDD(₹/share)": round(maxdd,2)
        }
    else:
        metrics = {}

    return trades, metrics

# ================================ COMMANDS =================================

HELP = f"""{BOT} {VER} — Long-Term Daily Scanner (NSE/BSE)
<i>{COMPLIANCE}</i>

Core:
/start • /help — this help
/reset — factory defaults
/configure key=value … — update costs/tick/session/capital/risk_pct/valid_minutes/aggressive
/universe RELIANCE,TCS,... — set explicit symbols
/universe_nse "NIFTY 500" — from NSE index
/universe_all — union of many NSE indices (broadest; deduped)
/exchange NSE|BSE|MIX — choose suffix .NS / .BO mapping for yfinance
/timeframe D — default TF for manual /alert (autoscan uses schedule)
/schedule tf=D window=09:15-15:30 digest=EOD — daily scans & digest timing
/autoscan on|off — toggle autonomous daily scans at market close
/alert SYMBOL [D] — immediate proposal for one symbol
/portfolio — snapshot open/closed trades & sizing settings
/backtest SYMBOL start=YYYY-MM-DD end=YYYY-MM-DD — daily next-open backtest (LT only)
/status — state snapshot
"""

async def bind_chat(update: Update):
    cid = update.effective_chat.id if update and update.effective_chat else None
    if cid:
        STATE["runtime"]["chat_id"] = cid
        save_state()

def parse_kv(args: List[str]) -> Dict[str,str]:
    kv = {}
    for a in args:
        if "=" in a:
            k,v = a.split("=",1); kv[k.strip()] = v.strip()
    return kv

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    await update.message.reply_text(HELP, parse_mode="HTML")

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global STATE, LAST_ALERT_VALID, SYMBOL_COUNT, CURRENT_MINUTE, SENT_THIS_MINUTE
    STATE = json.loads(json.dumps(DEFAULT_STATE))
    LAST_ALERT_VALID = {}
    SYMBOL_COUNT = {}
    CURRENT_MINUTE = None
    SENT_THIS_MINUTE = 0
    save_state()
    await update.message.reply_text("Reset complete. Use /configure and /universe… then /autoscan on")

async def cmd_configure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    kv = parse_kv(context.args)
    # config surface
    cfg = STATE["config"]
    # numeric or bools
    def setf(key, cast):
        if key in kv:
            try: cfg[key] = cast(kv.pop(key))
            except: pass
    setf("tick_size", float)
    setf("valid_minutes", int)
    setf("capital", float)
    setf("risk_pct", float)
    setf("max_open_trades", int)

    if "session" in kv: cfg["session"] = kv.pop("session")
    if "aggressive" in kv: cfg["aggressive"] = (kv.pop("aggressive").lower() in ("1","true","yes","on"))
    # costs
    for k in list(kv.keys()):
        if k in cfg["costs"]:
            try: cfg["costs"][k] = float(kv.pop(k))
            except: cfg["costs"][k] = kv.pop(k)
    save_state()
    await update.message.reply_text(f"Configured.\n<pre><code>{json.dumps(cfg, indent=2)}</code></pre>", parse_mode="HTML")

async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text("Usage: /universe RELIANCE,TCS,HDFCBANK,NIFTYBEES"); return
    syms = [s.strip().upper().replace(".NS","").replace(".BO","") for s in " ".join(context.args).split(",") if s.strip()]
    STATE["config"]["universe"] = sorted(list(set(syms))); save_state()
    await update.message.reply_text(f"Universe set: {len(STATE['config']['universe'])} symbols")

async def cmd_indices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    names = fetch_all_indices_names()
    if not names:
        await update.message.reply_text("Could not fetch NSE indices right now."); return
    await update.message.reply_text("Some indices:\n" + ", ".join(names[:80]) + "\n\nTry: /universe_nse \"NIFTY 500\"")

async def cmd_universe_nse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text('Usage: /universe_nse "NIFTY 500"'); return
    idx = " ".join(context.args).strip().strip('"').strip("'")
    syms = fetch_nse_index_symbols(idx)
    if not syms:
        await update.message.reply_text(f"No symbols fetched for {idx}."); return
    STATE["config"]["universe"] = sorted(list(set(syms))); save_state()
    await update.message.reply_text(f"Universe set from NSE index {idx}: {len(STATE['config']['universe'])} symbols")

async def cmd_universe_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    syms = build_all_symbols_nse()
    if not syms:
        await update.message.reply_text("Could not build broad NSE universe now."); return
    STATE["config"]["universe"] = syms
    save_state()
    await update.message.reply_text(f"Universe set to broad NSE: {len(syms)} symbols (deduped)")

async def cmd_exchange(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: await update.message.reply_text("Usage: /exchange NSE|BSE|MIX"); return
    val = context.args[0].upper()
    if val not in ("NSE","BSE","MIX"): await update.message.reply_text("Choose one of: NSE, BSE, MIX"); return
    STATE["config"]["exchange"] = val; save_state()
    await update.message.reply_text(f"Exchange mapping set: {val}")

async def cmd_timeframe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: await update.message.reply_text("Usage: /timeframe D"); return
    STATE["config"]["timeframe"] = context.args[0]
    save_state(); await update.message.reply_text(f"Default timeframe set: {STATE['config']['timeframe']}")

async def cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kv = parse_kv(context.args)
    if "tf" in kv: STATE["schedule"]["timeframes"] = [t.strip() for t in kv["tf"].split(",") if t.strip()]
    if "window" in kv and "-" in kv["window"]:
        st,en = kv["window"].split("-"); STATE["schedule"]["market_window"] = {"start":st, "end":en}
    if "digest" in kv:
        v = kv["digest"].upper()
        if v == "EOD":
            STATE["schedule"]["digest"]["enabled"] = True
            STATE["schedule"]["digest"]["eod"] = True
        else:
            STATE["schedule"]["digest"]["enabled"] = True
            STATE["schedule"]["digest"]["eod"] = False
            STATE["schedule"]["digest"]["interval_minutes"] = int(v.rstrip("M").rstrip("m"))
    save_state(); await update.message.reply_text(f"Schedule updated.\n<pre><code>{json.dumps(STATE['schedule'], indent=2)}</code></pre>", parse_mode="HTML")

async def cmd_autoscan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = context.args[0].lower() if context.args else "on"
    on = mode == "on"
    STATE["schedule"]["autoscan"] = on
    STATE["runtime"]["autoscan"] = "on" if on else "off"
    save_state(); await update.message.reply_text(f"AUTOSCAN {'ON' if on else 'OFF'}")

async def cmd_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text("Usage: /alert SYMBOL [D]"); return
    sym = context.args[0].upper()
    tf = context.args[1] if len(context.args)>=2 else STATE["config"]["timeframe"]
    try:
        setup = await pick_setup(sym, tf, STATE["config"])
        if not setup:
            await update.message.reply_text("No qualifying setup right now."); return
        payload = build_payload_from_setup(setup)
        html = build_alert_html(payload, STATE["config"])
        await update.message.reply_text(html, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        await update.message.reply_text(f"Cannot compute alert: {e}")

async def cmd_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg = STATE["config"]
    lines = [f"<b>Capital</b> ₹{cfg['capital']:.0f} • Risk/trade {cfg['risk_pct']}% • Max Open {cfg['max_open_trades']}"]
    lines.append(f"Universe: {len(cfg['universe'])} symbols")
    lines.append("\n<i>Note: This LT bot provides sizing hints only; it does not route orders.</i>")
    await update.message.reply_text("\n".join(lines) + f"\n\n<i>{COMPLIANCE}</i>", parse_mode="HTML")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    snap = {"config": STATE["config"], "schedule": STATE["schedule"], "runtime": {**STATE["runtime"], "ist_ts": ist_iso()}}
    await update.message.reply_text(f"<pre><code>{json.dumps(snap, indent=2)}</code></pre>", parse_mode="HTML")

async def cmd_backtest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /backtest SYMBOL start=YYYY-MM-DD end=YYYY-MM-DD"); return
    sym = context.args[0].upper()
    kv = parse_kv(context.args[1:])
    start = kv.get("start"); end = kv.get("end")
    if not start or not end:
        await update.message.reply_text("Provide start=YYYY-MM-DD end=YYYY-MM-DD"); return
    try:
        df = await fetch_ohlcv(sym, "D", start=start, end=end)
        trades, metrics = backtest_daily_next_open(df, STATE["config"])
        if trades.empty:
            await update.message.reply_text("No trades in backtest period."); return
        # Save CSVs
        ensure_dirs()
        tpath = os.path.join(BACKTEST_DIR, f"{sym}_trades.csv")
        trades.to_csv(tpath, index=False)
        mpath = os.path.join(BACKTEST_DIR, f"{sym}_metrics.json")
        with open(mpath, "w") as f: json.dump(metrics, f)
        await update.message.reply_text(
            f"<b>Backtest • {sym}</b>\nPeriod: {start}..{end}\n"
            f"Metrics: <pre><code>{json.dumps(metrics, indent=2)}</code></pre>\n"
            f"Files:\n- trades: {tpath}\n- metrics: {mpath}\n<i>{COMPLIANCE}</i>",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"Backtest failed: {e}")

async def guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Use commands only. See /help.")

# ================================ JOB WIRING ===============================

def setup_jobs(app: Application):
    jq: JobQueue = app.job_queue
    jq.run_repeating(scheduler_job, interval=5.0, first=5.0, name="scheduler")  # check every 5s for D bar close time
    jq.run_repeating(digest_job, interval=60*max(1, STATE["schedule"]["digest"]["interval_minutes"]), first=120, name="digest")
    jq.run_daily(refresh_holidays_job, time=dtime(hour=6, minute=0, tzinfo=IST), name="holiday_refresh")

def build_app() -> Application:
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN secret or .env.")
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("configure", cmd_configure))
    app.add_handler(CommandHandler("universe", cmd_universe))
    app.add_handler(CommandHandler("indices", cmd_indices))
    app.add_handler(CommandHandler("universe_nse", cmd_universe_nse))
    app.add_handler(CommandHandler("universe_all", cmd_universe_all))
    app.add_handler(CommandHandler("exchange", cmd_exchange))
    app.add_handler(CommandHandler("timeframe", cmd_timeframe))
    app.add_handler(CommandHandler("schedule", cmd_schedule))
    app.add_handler(CommandHandler("autoscan", cmd_autoscan))
    app.add_handler(CommandHandler("alert", cmd_alert))
    app.add_handler(CommandHandler("portfolio", cmd_portfolio))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("backtest", cmd_backtest))
    app.add_handler(MessageHandler(~filters.COMMAND, guard))
    setup_jobs(app)
    return app

# ================================ MAIN =====================================

if __name__ == "__main__":
    load_state()
    # Prime holiday cache once (best-effort)
    try:
        asyncio.run(refresh_holidays_job(None))  # type: ignore
    except Exception:
        pass
    app = build_app()
    log.info(f"Starting {BOT} {VER} …")
    try:
        asyncio.run(app.run_polling())
    except RuntimeError:
        # Replit-safe fallback loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(app.run_polling())
