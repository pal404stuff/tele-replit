# IndiQuant Apex AutoScan — Long-Term Wealth Edition (v5.0.0)
# NSE/BSE • Daily (D) • Hands-Free • Wide Universe (1000+ stocks via NSE indices union)
# Signals: Trend/Momentum (12-1), 52-Week Breakout, RS vs Benchmark, Volume Confirmation
# Risk: ATR-based initial SL, optional trailing ATR, Multi-TP (TP1/TP2) with breakeven
# Backtest: Daily next-open execution, SL priority, India cost model, CSV export
# Persistence: JSON state on disk (/mnt/data), holiday aware, resilient runtime
# -----------------------------------------------------------------------------
# Educational only — not investment advice. Not SEBI-registered. Markets carry risk.
# -----------------------------------------------------------------------------
# Replit Setup
# 1) Secrets (.env or Replit Secrets):
#    TELEGRAM_BOT_TOKEN="<your_bot_token>"
#    ADMIN_CHAT_ID="<your_chat_id>"   (optional; will bind to /start chat if omitted)
#
# 2) requirements.txt:
#    python-telegram-bot==21.4
#    yfinance==0.2.43
#    pandas==2.2.2
#    numpy==1.26.4
#    requests==2.32.3
#
# 3) Run, then in Telegram:
#    /start
#    /configure session=swing valid_minutes=2880 aggressive=true
#    /universe_wide   (unions major NSE indices → 1000+ stocks)
#    /schedule tf=D
#    /autoscan on
#
# Notes
# - This build avoids pandas_ta and implements indicators directly (EMA, SMA, RSI, ATR, Donchian, 12-1 momentum).
# - “All 3000” symbols from NSE+BSE is impractical under free data & Telegram limits.
#   Instead, /universe_wide merges many NSE indices (NIFTY 50/100/200/500, Mid/Small) to approximate the broad market
#   while staying performant. You can append your own CSV too (/universe_csv).
# - Daily scan runs at bar close (15:30 IST). To force immediately: /scan
# - Backtest: /backtest RELIANCE start=2015-01-01 end=2025-10-01
# -----------------------------------------------------------------------------


from __future__ import annotations
import os, json, uuid, logging, math, asyncio, time, io, traceback
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, time as dtime, date
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf

from telegram import Update, InputFile
from telegram.ext import (
    ApplicationBuilder, Application, CommandHandler, MessageHandler,
    ContextTypes, JobQueue, filters
)

# ============================== CONSTANTS ==================================
IST = ZoneInfo("Asia/Kolkata")
BOT_NAME = "IndiQuant Apex Ultimate"
VERSION = "v5.0.0"
COMPLIANCE = "Educational only — not investment advice. Not SEBI-registered. Markets carry risk."

STATE_PATH = "/mnt/data/indiquant_state.json"      # persistent state
SIGNALS_CSV_PATH = "/mnt/data/signals_daily.csv"   # last scan export
BACKTEST_CSV_PATH = "/mnt/data/backtest_trades.csv"

DATA_CACHE_TTL = 120  # seconds
HTTP_HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}

NSE_BASE = "https://www.nseindia.com"
NSE_EQ_INDEX_URL = f"{NSE_BASE}/api/equity-stockIndices"
NSE_ALL_INDICES_URL = f"{NSE_BASE}/api/allIndices"
NSE_HOLIDAY_URL = f"{NSE_BASE}/api/holiday-master?type=trading"

WIDE_INDICES = [
    "NIFTY 50", "NIFTY NEXT 50", "NIFTY 100", "NIFTY 200", "NIFTY 500",
    "NIFTY MIDCAP 50", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
    "NIFTY SMALLCAP 50", "NIFTY SMALLCAP 100", "NIFTY SMALLCAP 250",
    "NIFTY MICROCAP 250"
]

# ============================== LOGGING ====================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("indiquant.v5")

# ============================== SECRETS ====================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")

# ============================== DEFAULT STATE ===============================
DEFAULT_STATE: Dict = {
    "config": {
        "timezone": "Asia/Kolkata",
        "universe": ["RELIANCE", "TCS", "HDFCBANK", "NIFTYBEES"],
        "timeframe": "D",                 # default for /alert, /scan
        "session": "swing",               # swing for >=1h/D only
        "square_off_time": "15:20",       # informational for swing
        "data": {"source": "yfinance", "adjusted": True},
        # Strategy knobs
        "signal": {"name": "hybrid", "params": {
            "ema_fast": 20, "ema_slow": 50,
            "rsi2_th": 10,
            "donchian_n": 200,           # long-term breakout (≈52w)
            "mom_lb": 252, "mom_skip": 21,   # 12-1 momentum
            "vol_pctile_low": 20, "vol_pctile_high": 90,
            "mtf": {"enable": True, "htf": "D"},  # HTF=D (self) for daily regime
            "rs": {"enable": True, "benchmark": "NIFTYBEES", "min_rating": 60}  # RS threshold slightly looser for coverage
        }},
        # Risk/Targets
        "sl": {"method": "atr", "params": {"atr_k": 3.0}},  # slightly wider for long-term
        "tp": {
            "method": "trend",             # "r" or "trend"
            "params": {"r": 3.0},          # used if method="r"
            "multi": {"enable": True, "tp1_r": 1.5, "tp2_r": 3.0, "trail_be": True, "trail_atr_k": 3.0}
        },
        # Filters / Liquidity
        "filters": {"min_adv_inr": 1.0e8, "max_spread_pct": 1.0, "band_guard": True, "min_rr_net": 1.3, "min_price": 25.0},
        # India costs
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
        "valid_minutes": 2880,   # 2 days
        "aggressive": True       # earlier entries allowed
    },
    "schedule": {
        "market_window": {"start": "09:15", "end": "15:30"},
        "timeframes": ["D"],
        "autoscan": False,
        "digest": {"enabled": False, "interval_minutes": 60, "eod": True}
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
        "active": {},    # trade_id -> trade dict (for long-term we send signals; no auto fills)
        "closed": []
    },
    "dedupe": {}         # (symbol|tf) -> valid_until ISO
}
STATE: Dict = {}

# in-memory caches
DATA_CACHE: Dict[Tuple[str,str,str], Tuple[pd.DataFrame, float]] = {}
NSE_CACHE_MEM: Dict[str, Tuple[List[str], float]] = {}

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

def round_tick(p: float, tick: float) -> float:
    # robust rounding to the nearest tick
    return float(np.round(p / tick) * tick)

# ============================ PERSISTENCE ==================================

def load_state() -> None:
    global STATE
    try:
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
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        with open(STATE_PATH, "w") as f:
            json.dump(STATE, f)
    except Exception as e:
        log.warning(f"State save failed: {e}")

# =============================== NSE HELPERS ===============================

def _nse_session() -> requests.Session:
    s = requests.Session(); s.headers.update(HTTP_HEADERS)
    try:
        s.get(NSE_BASE, timeout=10)
    except Exception:
        pass
    return s

def fetch_nse_index_symbols(index_name: str, ttl_sec: int = 12*3600) -> List[str]:
    # memoized in memory only (simple)
    now = time.time()
    key = index_name.strip().upper()
    if key in NSE_CACHE_MEM and now < NSE_CACHE_MEM[key][1]:
        return NSE_CACHE_MEM[key][0]
    out: List[str] = []
    try:
        ses = _nse_session()
        for attempt in range(3):
            r = ses.get(NSE_EQ_INDEX_URL, params={"index": index_name}, timeout=12)
            if r.status_code == 200:
                data = r.json().get("data", [])
                out = sorted({ (row.get("symbol") or "").strip().upper() for row in data if row.get("symbol") })
                break
            if r.status_code in (429, 403):
                time.sleep(1.2*(attempt+1)); continue
            r.raise_for_status()
    except Exception as e:
        log.warning(f"NSE index fetch failed for {index_name}: {e}")
    NSE_CACHE_MEM[key] = (out, now + ttl_sec)
    return out

def wide_universe() -> List[str]:
    symbols: set[str] = set()
    for idx in WIDE_INDICES:
        syms = fetch_nse_index_symbols(idx)
        symbols.update(syms)
    # Filter commonsense garbage & sort
    syms = [s for s in sorted(symbols) if s and s.isalnum()]
    return syms

async def fetch_nse_holidays() -> List[str]:
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
        STATE["runtime"]["nse_holidays"] = hol
        save_state()
        return hol
    except Exception as e:
        log.warning(f"Holiday fetch failed: {e}")
        return STATE["runtime"].get("nse_holidays", [])

def is_market_open_today() -> bool:
    ts = now_ist()
    if ts.weekday() >= 5: return False
    hol = STATE["runtime"].get("nse_holidays", [])
    return str(ts.date()) not in hol

# ============================ DATA LAYER (YF) ==============================

def yahoo_symbol(sym: str) -> str:
    s = sym.strip().upper()
    if s.endswith(".NS") or s.endswith(".BO"): return s
    return s + ".NS"  # prefer NSE by default

def _yf_download(ticker: str, interval: Optional[str]=None, period: Optional[str]=None,
                 start: Optional[str]=None, end: Optional[str]=None) -> pd.DataFrame:
    last_exc: Optional[Exception] = None
    for _ in range(3):
        try:
            df = yf.download(tickers=ticker, interval=interval, period=period, start=start, end=end,
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
        raise ValueError("This build is long-term D only. Use timeframe=D.")
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

def donchian_high(high: pd.Series, n: int=200) -> pd.Series:
    return high.rolling(n).max()

def momentum_12_1(close: pd.Series, lb: int=252, skip: int=21) -> pd.Series:
    # total return over last 12 months excluding last 1 month
    return close.pct_change(lb - skip) - close.pct_change(skip)

def adv_inr_df(df: pd.DataFrame, window: int=20) -> Optional[float]:
    if "Close" not in df or "Volume" not in df: return None
    dv = (df["Close"] * df["Volume"]).rolling(window).mean().dropna()
    return float(dv.iloc[-1]) if not dv.empty else None

# ============================ STRATEGY ENGINE ==============================

async def compute_setup(symbol: str, cfg: Dict) -> Optional[Dict]:
    # Fetch daily OHLCV
    df = await fetch_ohlcv(symbol, "D")
    if df is None or len(df) < 260:  # ~1y
        return None

    # Indicators
    close, high, low, vol = df["Close"], df["High"], df["Low"], df["Volume"]
    ema_f = ema(close, cfg["signal"]["params"].get("ema_fast", 20))
    ema_s = ema(close, cfg["signal"]["params"].get("ema_slow", 50))
    sma200 = sma(close, 200)
    a14 = atr(high, low, close, 14)
    don_hi = donchian_high(high, cfg["signal"]["params"].get("donchian_n", 200))
    rsi2_now = rsi2(close)
    mom = momentum_12_1(close, cfg["signal"]["params"].get("mom_lb",252), cfg["signal"]["params"].get("mom_skip",21))
    vol_ma = vol.rolling(20).mean()

    # RS vs benchmark (NIFTYBEES by default)
    bench_sym = cfg["signal"]["params"]["rs"].get("benchmark", "NIFTYBEES")
    bench = await fetch_ohlcv(bench_sym, "D")
    if bench is not None and not bench.empty:
        j = df[["Close"]].join(bench[["Close"]].rename(columns={"Close":"B"}), how="inner").dropna()
        stock_norm = j["Close"] / j["Close"].iloc[0]; bench_norm = j["B"] / j["B"].iloc[0]
        rs = (stock_norm / bench_norm).rolling(55).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1]*100, raw=False)
    else:
        rs = pd.Series(50.0, index=df.index)

    last = pd.DataFrame({
        "close": close, "high": high, "low": low, "ema_f": ema_f, "ema_s": ema_s,
        "sma200": sma200, "atr": a14, "don_hi": don_hi.shift(1), "vol": vol,
        "vol_ma": vol_ma, "rsi2": rsi2_now, "mom": mom, "rs": rs
    }).dropna().iloc[-1]

    # Liquidity & price gates
    adv = adv_inr_df(df)
    if adv is None or adv < cfg["filters"]["min_adv_inr"]: return None
    if float(last["close"]) < cfg["filters"].get("min_price", 25.0): return None

    # Regime & RS gates
    if not (last["close"] >= last["sma200"] and last["ema_f"] >= last["ema_s"]):
        return None
    if cfg["signal"]["params"]["rs"].get("enable", True) and float(last["rs"]) < cfg["signal"]["params"]["rs"].get("min_rating", 60):
        return None

    # Signal set: hybrid of Donchian breakout + momentum + volume confirmation
    vol_conf = float(last["vol"]) > float(last["vol_ma"] or 0)
    breakout_ok = float(last["close"]) >= float(last["don_hi"])
    momentum_ok = float(last["mom"]) > 0
    rsi2_dip = float(last["rsi2"]) <= cfg["signal"]["params"].get("rsi2_th", 10)

    # Accept if long-term breakout + momentum + volume OR (aggressive) dip buy in uptrend
    good = (breakout_ok and momentum_ok and vol_conf) or (cfg.get("aggressive", False) and rsi2_dip and vol_conf)
    if not good: return None

    tick = cfg.get("tick_size", 0.05)
    entry = round_tick(float(last["high"]) + tick, tick)

    # Initial SL = min(ATR-based, structure swing low 10)
    swing_low = float(df["Low"].rolling(10).min().iloc[-2])
    sl_atr = entry - cfg["sl"]["params"].get("atr_k", 3.0) * float(last["atr"])
    sl_struct = swing_low * 0.99
    sl = round_tick(min(sl_atr, sl_struct), tick)

    # Targets
    if cfg["tp"]["method"] == "r":
        tp = entry + cfg["tp"]["params"].get("r", 3.0) * (entry - sl)
        tp1 = entry + cfg["tp"]["multi"].get("tp1_r", 1.5) * (entry - sl) if cfg["tp"]["multi"].get("enable", True) else None
        tp2 = entry + cfg["tp"]["multi"].get("tp2_r", 3.0) * (entry - sl) if cfg["tp"]["multi"].get("enable", True) else None
    else:
        # trend method: compute a “soft” TP2 via 52-w extension (heuristic), TP1 at 1.5R
        tp1 = entry + cfg["tp"]["multi"].get("tp1_r", 1.5) * (entry - sl)
        tp2 = entry + cfg["tp"]["multi"].get("tp2_r", 3.0) * (entry - sl)
        tp = tp2
    tp = round_tick(tp, tick)
    if tp1: tp1 = round_tick(tp1, tick)
    if tp2: tp2 = round_tick(tp2, tick)

    if not (sl < entry < tp):
        return None

    rr_g, rr_n, cps = net_rr(entry, sl, tp, cfg)
    if not cfg.get("aggressive", False) and rr_n < cfg["filters"].get("min_rr_net", 1.3):
        return None

    confidence = 60
    if breakout_ok: confidence += 10
    if momentum_ok: confidence += 10
    if vol_conf:    confidence += 10
    if float(last["rs"]) >= 80: confidence += 5
    confidence = int(max(0, min(100, confidence)))

    return {
        "symbol": symbol.upper(),
        "timeframe": "D",
        "session": "swing",
        "idea": "LT Breakout + Mom + RS + VolConfirm" if breakout_ok else "Uptrend Dip (Aggressive) + RS + VolConfirm",
        "confidence": confidence,
        "entry": {"type": "stop", "price": float(entry)},
        "sl": {"method": "ATR/Struct", "price": float(sl)},
        "tp": {"method": cfg["tp"]["method"], "price": float(tp), "tp1": tp1, "tp2": tp2},
        "assumptions": {"data_src": "yfinance", "bar_size": "D", "tick_size": cfg.get("tick_size", 0.05), "costs_placeholders": True},
        "filters": {"adv_ok": True, "spread_ok": None, "band_guard_ok": None},
        "rr": {"gross": round(rr_g,2), "net": round(rr_n,2)},
        "extras": {"mom12_1": float(last["mom"]), "rs_rating": float(last["rs"]), "adv_inr": float(adv)}
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
    slip  = turn * (c.get("slippage_bps",0)/10000.0)
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

def build_alert_html(signal: Dict, cfg: Dict) -> str:
    ts = ist_iso()
    entry = signal["entry"]["price"]; sl = signal["sl"]["price"]; tp = signal["tp"]["price"]
    rr_g = signal["rr"]["gross"]; rr_n = signal["rr"]["net"]
    valid_until = (now_ist() + timedelta(minutes=cfg.get("valid_minutes",2880))).astimezone(IST).strftime("%H:%M")
    tp_line = f"TP: ₹{tp:.2f}" if signal["tp"].get("tp1") is None else (f"TP1 ₹{signal['tp']['tp1']:.2f} • TP2 ₹{signal['tp']['tp2']:.2f}")
    dec = " → ".join([
        "Objective: Long-term buy",
        "Data: D/closed bars (yfinance)",
        f"Method: {signal['idea']}",
        "Controls: ADV, SMA200/EMA trend, RS",
        f"Levels tick-rounded; Net R:R {rr_n:.2f}:1; Valid {valid_until} IST"
    ])
    costs = cfg["costs"]
    return (
        f"<b>{ts}</b>\n"
        f"<b>ALERT • {signal['symbol']} • D • Swing (IST)</b>\n"
        f"Idea: {signal['idea']} • Confidence {signal['confidence']}%\n"
        f"Entry: <b>stop ₹{entry:.2f}</b> • SL ₹{sl:.2f} • {tp_line} → RR(gross) <b>{rr_g:.2f}:1</b>\n"
        f"Costs: model {costs['brokerage_model']}, GST {costs['gst_pct']*100:.0f}%, STT {costs['stt_pct']*100:.2f}%, slippage {costs['slippage_bps']} bps → RR(net) <b>{rr_n:.2f}:1</b>\n"
        f"Decision Trace: {dec}\n"
        f"<i>{COMPLIANCE}</i>\n"
        f"<pre><code>{json.dumps(signal, indent=2)}</code></pre>"
    )

# =========================== AUTOSCAN & EXPORT ============================

async def scan_universe_once(context: ContextTypes.DEFAULT_TYPE, universe: List[str]) -> List[Dict]:
    results: List[Dict] = []
    sem = asyncio.Semaphore(8)  # limit concurrency for stability
    async def _one(sym: str):
        async with sem:
            try:
                s = await compute_setup(sym, STATE["config"])
                if s: results.append(s)
            except Exception as e:
                log.debug(f"scan fail {sym}: {e}")
    await asyncio.gather(*[_one(s) for s in universe])
    # Sort by composite quality: RS, momentum, confidence, RR(net)
    results.sort(key=lambda r: (r["extras"]["rs_rating"], r["extras"]["mom12_1"], r["confidence"], r["rr"]["net"]), reverse=True)
    return results

def to_csv_bytes(rows: List[Dict]) -> bytes:
    if not rows:
        df = pd.DataFrame(columns=["symbol","entry","sl","tp","rr_net","confidence","idea"])
    else:
        df = pd.DataFrame([{
            "symbol": r["symbol"], "entry": r["entry"]["price"], "sl": r["sl"]["price"],
            "tp": r["tp"]["price"], "rr_net": r["rr"]["net"], "confidence": r["confidence"],
            "idea": r["idea"], "rs_rating": r["extras"].get("rs_rating"), "mom12_1": r["extras"].get("mom12_1"),
            "adv_inr": r["extras"].get("adv_inr")
        } for r in rows])
    bio = io.StringIO(); df.to_csv(bio, index=False)
    return bio.getvalue().encode()

async def do_daily_scan(context: ContextTypes.DEFAULT_TYPE):
    if not (STATE["schedule"]["autoscan"] and STATE["schedule"]["timeframes"] == ["D"]):
        return
    ts = now_ist()
    if ts.strftime("%H:%M") != STATE["schedule"]["market_window"]["end"]:  # run at 15:30 only
        return
    if not is_market_open_today():  # holiday/weekend guard
        return
    chat_id = STATE["runtime"].get("chat_id") or ADMIN_CHAT_ID
    if not chat_id:
        return
    # Scan
    uni = STATE["config"]["universe"]
    await context.bot.send_message(chat_id=int(chat_id), text=f"⏱️ Scanning {len(uni)} symbols (Daily)…", disable_web_page_preview=True)
    results = await scan_universe_once(context, uni)
    # Export CSV
    csv_bytes = to_csv_bytes(results)
    os.makedirs(os.path.dirname(SIGNALS_CSV_PATH), exist_ok=True)
    with open(SIGNALS_CSV_PATH, "wb") as f: f.write(csv_bytes)
    # Send top N alerts inline, rest as CSV
    topN = min(20, len(results))
    for r in results[:topN]:
        await context.bot.send_message(chat_id=int(chat_id), text=build_alert_html(r, STATE["config"]), parse_mode="HTML", disable_web_page_preview=True)
    await context.bot.send_document(chat_id=int(chat_id), document=InputFile(SIGNALS_CSV_PATH), caption=f"Daily signals ({len(results)} rows). {COMPLIANCE}")

# ================================ BACKTEST ================================

def backtest_daily(df: pd.DataFrame, cfg: Dict) -> Tuple[pd.DataFrame, Dict]:
    """
    Daily backtest for long-only breakout+trend logic (simplified):
    - Signal on close(t) ⇒ enter next open(t+1) if Entry(stop) <= next open? We assume stop @ prior high + tick;
      execution: buy at next open if next open >= stop, else wait until price crosses stop within the day (approx: use High).
    - SL priority: if Low(t+1) <= SL before reaching Entry, skip entry; if in trade, if Low <= SL before High >= TP, exit SL.
    - TP: 2-stage or trend; for backtest, apply TP2 and ignore trailing intricacies (keeps speed & determinism).
    """
    if df is None or df.empty
