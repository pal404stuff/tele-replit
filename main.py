# main.py
# IndiQuant Apex AutoScan — NSE/BSE • IST • Bloomberg-grade (v3.1.0)
# Educational only — not investment advice. Not SEBI-registered. Markets carry risk.

import os, json, uuid, logging, math, textwrap
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, time as dtime, date
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters, JobQueue
)

# =========================== GLOBALS / CONSTANTS ===========================
IST = ZoneInfo("Asia/Kolkata")
BOT_NAME = "IndiQuant Apex AutoScan"
VERSION = "v3.1.0"
COMPLIANCE = "Educational only — not investment advice. Not SEBI-registered. Markets carry risk."

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("indiquant.apex")

# --- Secrets (set in Replit: Tools → Secrets) ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")  # optional default chat

# --- Optional tipping links ---
TIP_UPI_ID   = os.getenv("TIP_UPI_ID", "")      # e.g. yourname@okhdfcbank
TIP_BMC      = os.getenv("TIP_BMC", "")
TIP_KOFI     = os.getenv("TIP_KOFI", "")
TIP_STRIPE   = os.getenv("TIP_STRIPE", "")
TIP_RAZORPAY = os.getenv("TIP_RAZORPAY", "")

# ================================ CONFIG ===================================
DEFAULT_CONFIG = {
    "$schema":"indiquant.config.v3",
    "timezone":"Asia/Kolkata",
    "universe":["NIFTYBEES","BANKBEES","RELIANCE","TCS","HDFCBANK","ICICIBANK","SBIN","INFY"],
    "timeframe":"D",
    "session":"swing",
    "square_off_time":"15:20",
    "data":{"source":"yfinance","bar_size":"D","adjusted":True},
    "signal":{"name":"auto","params":{
        "ema_fast":20,"ema_slow":50,"rsi2_th":10,"donchian_n":20,
        "vol_pctile_low":40,"vol_pctile_high":70,
        "mtf":{"enable":True,"ltf":"1h","htf":"D"}
    }},
    "sl":{"method":"atr","params":{"atr_k":2.5}},
    "tp":{"method":"r","params":{"r":2.0},"multi":{"enable":False,"tp1_r":1.5,"tp2_r":2.5,"trail_atr_k":3}},
    "filters":{"min_adv_inr":5e7,"max_spread_pct":0.35,"band_guard":True},
    "costs":{
        "brokerage_model":"flat",
        "brokerage_flat_inr":20,
        "brokerage_pct":0.0003,
        "stt_pct":0.001,
        "exchange_txn_pct":0.0000325,
        "sebi_fee_pct":0.000001,
        "stamp_duty_pct":0.00015,
        "gst_pct":0.18,
        "slippage_bps":5
    },
    "tick_size":0.05,
    "valid_minutes":180,
    "aggressive": False
}

DEFAULT_SCHEDULE = {
    "$schema":"indiquant.schedule.v3",
    "market_window":{"start":"09:15","end":"15:30"},
    "timeframes":["1h","D"],
    "autoscan":False,
    "quiet_windows":[{"start":"09:15","end":"09:30"}],
    "digest":{"enabled":True,"interval_minutes":30,"eod":True},
    "throttle":{"max_per_min":6,"max_per_symbol":2},
    "live_ticks":False
}

CONFIG  = json.loads(json.dumps(DEFAULT_CONFIG))
SCHEDULE = json.loads(json.dumps(DEFAULT_SCHEDULE))

RUNTIME = {
    "$schema":"indiquant.runtime.v3",
    "ist_ts":None,
    "autoscan":"off",
    "scan_status":"idle",
    "last_bar_close":{"1m":None,"5m":None,"15m":None,"1h":None,"D":None},
    "counters":{"alerts_sent":0,"suppressed":0,"deduped":0,"skipped":0},
    "universe":CONFIG["universe"],
    "filters":{"min_adv_ok":None,"spread_ok":None,"band_guard_ok":None},
    "chat_id": ADMIN_CHAT_ID or None
}

# de-dupe: (symbol,timeframe) -> valid_until_ist
LAST_ALERT_VALID: Dict[Tuple[str,str], datetime] = {}
# per-minute throttle
CURRENT_MINUTE = None
SENT_THIS_MINUTE = 0
# per-symbol/session count: (symbol, YYYY-MM-DD) -> count
SYMBOL_COUNT: Dict[Tuple[str,str], int] = {}

# ================================ HELPERS ==================================
def now_ist() -> datetime:
    return datetime.now(tz=IST)

def ist_iso(dt: Optional[datetime]=None) -> str:
    return (dt or now_ist()).astimezone(IST).isoformat(timespec="seconds")

def parse_hhmm(s: str) -> dtime:
    h, m = map(int, s.split(":"))
    return dtime(hour=h, minute=m, tzinfo=IST)

def in_window(ts: datetime, start_hhmm: str, end_hhmm: str) -> bool:
    t = ts.timetz()
    return parse_hhmm(start_hhmm) <= t <= parse_hhmm(end_hhmm)

def in_any_quiet(ts: datetime) -> bool:
    for w in SCHEDULE.get("quiet_windows", []):
        if in_window(ts, w["start"], w["end"]):
            return True
    return False

def is_market_open(ts: datetime) -> bool:
    if ts.weekday() >= 5:  # Sat/Sun
        return False
    return in_window(ts, SCHEDULE["market_window"]["start"], SCHEDULE["market_window"]["end"])

def bar_close_now(ts: datetime, tf: str) -> bool:
    m, s = ts.minute, ts.second
    if not is_market_open(ts): return False
    if tf == "1m":  return s == 0
    if tf == "5m":  return (m % 5 == 0) and s == 0
    if tf == "15m": return (m % 15 == 0) and s == 0
    if tf == "1h":  return (m == 0) and (s == 0)
    if tf.upper() == "D":
        # fire once ~ after session end (15:31:00)
        end = SCHEDULE["market_window"]["end"]
        eh, em = map(int, end.split(":"))
        return (ts.hour == eh and ts.minute == em + 1 and s == 0)
    return False

def yahoo_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if s.endswith(".NS") or s.endswith(".BO"): return s
    return s + ".NS"

def round_tick(p: float, tick: float) -> float:
    return round(p / tick) * tick

def symbol_session_key(sym: str, ts: datetime) -> Tuple[str,str]:
    return (sym.upper(), ts.strftime("%Y-%m-%d"))

def reset_throttle_if_new_minute(ts: datetime):
    global CURRENT_MINUTE, SENT_THIS_MINUTE
    key = ts.strftime("%Y-%m-%d %H:%M")
    if key != CURRENT_MINUTE:
        CURRENT_MINUTE = key
        SENT_THIS_MINUTE = 0

# ============================= INDICATORS ==================================
def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    gain = up.rolling(period).mean()
    loss = down.rolling(period).mean()
    rs = gain / (loss.replace(0, np.nan))
    return 100 - (100 / (1 + rs))

def rsi2(series: pd.Series) -> pd.Series:
    return rsi(series, 2)

def atr(high: pd.Series, low: pd.Series, close: pd.Series, period=14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def donchian(high: pd.Series, low: pd.Series, n: int=20) -> Tuple[pd.Series, pd.Series]:
    return high.rolling(n).max(), low.rolling(n).min()

def is_inside_bar(h: pd.Series, l: pd.Series) -> pd.Series:
    return (h <= h.shift(1)) & (l >= l.shift(1))

def nr_n(h: pd.Series, l: pd.Series, n:int) -> pd.Series:
    rng = (h - l)
    return rng == rng.rolling(n).min()

def vol_pctile(close: pd.Series, high: pd.Series, low: pd.Series, lookback: int = 90) -> float:
    # ATR/Close percentile
    a = atr(high, low, close, 14)
    vol = (a / close).tail(lookback).dropna()
    if vol.empty: return 50.0
    return float((vol.rank(pct=True).iloc[-1] * 100.0))

def adv_inr(df: pd.DataFrame, window: int = 20) -> Optional[float]:
    if "Close" not in df or "Volume" not in df: return None
    dv = (df["Close"] * df["Volume"]).rolling(window).mean().dropna()
    return float(dv.iloc[-1]) if not dv.empty else None

# =============================== DATA ======================================
def fetch_intraday(symbol: str, tf: str) -> pd.DataFrame:
    tf = tf.lower()
    tf_map = {"1m":("1m","5d"), "5m":("5m","30d"), "15m":("15m","60d"), "1h":("60m","730d")}
    if tf not in tf_map: raise RuntimeError(f"Unsupported intraday tf: {tf}")
    interval, period = tf_map[tf]
    df = yf.download(tickers=yahoo_symbol(symbol), interval=interval, period=period, auto_adjust=True, progress=False)
    if df is None or df.empty: raise RuntimeError("No data")
    df = df.rename(columns=str.title).dropna()
    return df

def fetch_daily(symbol: str, start: Optional[str]=None, end: Optional[str]=None) -> pd.DataFrame:
    df = yf.download(tickers=yahoo_symbol(symbol), start=start, end=end, auto_adjust=True, progress=False)
    if df is None or df.empty: raise RuntimeError("No data")
    df = df.rename(columns=str.title).dropna()
    return df

def fetch_ohlcv(symbol: str, tf: str) -> pd.DataFrame:
    if tf.upper() == "D":
        return fetch_daily(symbol)
    return fetch_intraday(symbol, tf)

# =============================== COSTS =====================================
def cost_per_share(entry: float, exit_px: float, C: dict) -> float:
    c = C["costs"]
    # turnover per side approximated by price; sum buy+sell
    turn = entry + exit_px
    if c["brokerage_model"] == "flat":
        brokerage = c["brokerage_flat_inr"] * 2
    else:
        brokerage = turn * c["brokerage_pct"]
    stt   = turn * c["stt_pct"]
    exch  = turn * c["exchange_txn_pct"]
    sebi  = turn * c["sebi_fee_pct"]
    stamp = turn * c["stamp_duty_pct"]
    gst   = (brokerage + exch + sebi) * c["gst_pct"]
    slip  = (entry + exit_px) * (c.get("slippage_bps",0)/10000.0)
    return brokerage + stt + exch + sebi + stamp + gst + slip

def rr_multiple(entry: float, sl: float, tp: float) -> float:
    risk = max(1e-9, entry - sl)
    return (tp - entry) / risk

def net_rr(entry: float, sl: float, tp: float, C: dict) -> Tuple[float,float,float]:
    gross = rr_multiple(entry, sl, tp)
    cps = cost_per_share(entry, tp, C)
    reward_net = max(0.0, (tp - entry) - cps)
    risk = max(1e-9, entry - sl)
    net = reward_net / risk
    return gross, net, cps

# ============================== STRATEGY ===================================
def mtf_ok(symbol: str, ltf_tf: str, htf_tf: str, ema_fast_n:int, ema_slow_n:int) -> bool:
    try:
        if htf_tf.upper() == "D":
            d = fetch_daily(symbol)
        else:
            d = fetch_ohlcv(symbol, htf_tf)
        c = d["Close"]
        if len(c) < max(ema_fast_n, ema_slow_n) + 5: return True  # don't block if insufficient
        ema_f, ema_s = ema(c, ema_fast_n).iloc[-1], ema(c, ema_slow_n).iloc[-1]
        return bool(ema_f > ema_s)
    except Exception:
        return True  # do not block if htf unavailable

def pick_strategy(symbol: str, tf: str, C: dict, df: pd.DataFrame) -> Tuple[str, Dict]:
    """
    Returns (strategy_name, details_dict) — details holds computed fields for reuse.
    """
    close, high, low = df["Close"], df["High"], df["Low"]
    ema_fast_n = C["signal"]["params"].get("ema_fast", 20)
    ema_slow_n = C["signal"]["params"].get("ema_slow", 50)
    don_n = C["signal"]["params"].get("donchian_n", 20)
    rsi2_th = C["signal"]["params"].get("rsi2_th", 10)

    ema_f = ema(close, ema_fast_n)
    ema_s = ema(close, ema_slow_n)
    atr14 = atr(high, low, close, 14)
    sma200 = sma(close, 200)
    rsi2_now = float(rsi2(close).iloc[-1]) if len(close) > 5 else 50.0
    don_hi, don_lo = donchian(high, low, don_n)
    inside = bool(is_inside_bar(high, low).iloc[-1]) if len(high) > 2 else False
    nr4 = bool(nr_n(high, low, 4).iloc[-1]) if len(high) > 5 else False
    nr7 = bool(nr_n(high, low, 7).iloc[-1]) if len(high) > 8 else False
    volp = vol_pctile(close, high, low, 90)

    last = {
        "close": float(close.iloc[-1]),
        "high": float(high.iloc[-1]),
        "low": float(low.iloc[-1]),
        "ema_fast": float(ema_f.iloc[-1]),
        "ema_slow": float(ema_s.iloc[-1]),
        "atr": float(atr14.iloc[-1]),
        "sma200": float(sma200.iloc[-1]) if not np.isnan(sma200.iloc[-1]) else None,
        "don_hi": float(don_hi.iloc[-1]) if not np.isnan(don_hi.iloc[-1]) else None,
        "don_lo": float(don_lo.iloc[-1]) if not np.isnan(don_lo.iloc[-1]) else None,
        "inside": inside, "nr4": nr4, "nr7": nr7, "vol_pctile": volp
    }

    # MTF alignment (optional)
    mtf = C["signal"]["params"].get("mtf", {"enable":False})
    htf_ok = True
    if mtf.get("enable", False):
        htf_ok = mtf_ok(symbol, tf, mtf.get("htf","D"), ema_fast_n, ema_slow_n)

    # Candidate ideas (evaluate RR later)
    ideas = []
    # 1) Trend + Pullback
    if last["ema_fast"] > last["ema_slow"] and last["sma200"] and last["close"] >= last["sma200"] and last["close"] <= last["ema_fast"]:
        ideas.append(("ema", "EMA trend + pullback (EMA20/50, SMA200 health)"))
    # 2) RSI2 MR (quality)
    if last["sma200"] and last["close"] > last["sma200"] and rsi2_now <= rsi2_th:
        ideas.append(("rsi2", f"RSI2<={rsi2_th} with SMA200 up"))
    # 3) Donchian breakout / NR / InsideBar expansion
    if last["don_hi"] is not None:
        ideas.append(("donchian", f"Donchian breakout N={don_n}"))
    if inside or nr4 or nr7:
        ideas.append(("expansion", "Inside/NR expansion"))

    # Default fallback
    if not ideas:
        ideas.append(("donchian", f"Donchian breakout N={don_n}"))

    details = {"last": last, "ema_fast_n": ema_fast_n, "ema_slow_n": ema_slow_n, "don_n": don_n,
               "rsi2_th": rsi2_th, "htf_ok": htf_ok}
    return ideas[0][0], {**details, "rsi2": rsi2_now, "inside": inside, "nr4": nr4, "nr7": nr7}

def compute_levels(symbol: str, tf: str, session: str, C: dict, strategy: str="auto") -> dict:
    df = fetch_ohlcv(symbol, tf)
    if len(df) < 70: raise RuntimeError("Insufficient bars")
    close, high, low = df["Close"], df["High"], df["Low"]
    tick = C.get("tick_size", 0.05)

    # Liquidity
    adv = adv_inr(df)
    adv_ok = adv is not None and adv >= C["filters"]["min_adv_inr"]
    spread_ok = None  # not available from yfinance
    band_ok = None    # not available — mark unknown

    # Decide strategy
    if strategy.lower() == "auto" or strategy.lower() == "hybrid":
        st, det = pick_strategy(symbol, tf, C, df)
    else:
        st = strategy.lower()
        det = pick_strategy(symbol, tf, C, df)[1]

    last = det["last"]
    ema_fast_n = det["ema_fast_n"]; ema_slow_n = det["ema_slow_n"]; don_n = det["don_n"]; rsi2_th = det["rsi2_th"]

    k = float(C["sl"]["params"].get("atr_k", 2.5))
    R = float(C["tp"]["params"].get("r", 2.0))
    entry_type = "stop"

    # Compute raw entry based on strategy
    if st == "ema":
        raw_entry = max(last["high"], last["close"]) * 1.001
        idea = f"EMA({ema_fast_n}/{ema_slow_n}) trend + pullback + ATR stop"
        conf = 50 + 10 + (10 if adv_ok else 0) + (10 if det["htf_ok"] else 0) + (10 if last["close"] <= last["ema_fast"] else 0)
    elif st == "rsi2":
        if not (last["sma200"] and last["close"] > last["sma200"]):
            raise RuntimeError("RSI2 blocked by SMA200 filter")
        raw_entry = last["close"] * 1.001
        idea = f"RSI2<={rsi2_th} + SMA200 up + ATR stop"
        conf = 50 + 10 + (10 if adv_ok else 0) + (10 if det["htf_ok"] else 0) + 10
    elif st == "donchian" or st == "expansion":
        if last["don_hi"] is None:
            raise RuntimeError("Donchian bands unavailable")
        bump = 0.01 if tick < 0.1 else tick
        raw_entry = last["don_hi"] + bump
        exp_note = " + NR/Inside" if (det["inside"] or det["nr4"] or det["nr7"]) else ""
        idea = f"Donchian breakout N={don_n}{exp_note} + ATR stop"
        conf = 50 + (10 if adv_ok else 0) + (10 if det["htf_ok"] else 0)
    else:
        raise RuntimeError(f"Unknown strategy: {st}")

    # Stops/Targets
    raw_sl = raw_entry - k * last["atr"]
    raw_tp = raw_entry + R * (raw_entry - raw_sl)

    entry = round_tick(raw_entry, tick)
    sl    = round_tick(raw_sl, tick)
    tp    = round_tick(raw_tp, tick)
    if not (sl < entry < tp): raise RuntimeError("Invalid levels after rounding")

    # Confidence: volatility fit bonus
    volp = last["vol_pctile"]
    vp_lo = C["signal"]["params"].get("vol_pctile_low", 40)
    vp_hi = C["signal"]["params"].get("vol_pctile_high", 70)
    if vp_lo <= volp <= vp_hi: conf += 10
    conf = max(0, min(100, int(conf)))

    return {
        "symbol": symbol.upper(),
        "timeframe": tf,
        "session": session,
        "idea": idea,
        "entry":{"type":entry_type,"price": entry},
        "sl":{"method": f"ATR*{k}", "price": sl},
        "tp":{"method": f"R={R}", "price": tp, "r_multiple": R},
        "confidence": conf,
        "assumptions":{"data_src":"yfinance","bar_size":tf,"tick_size":tick,"costs_placeholders":True,"strategy":st},
        "filters":{"adv_ok": bool(adv_ok), "spread_ok": spread_ok, "band_guard_ok": band_ok},
        "last": {**last, "adv_inr": adv}
    }

# ============================== UI / MESSAGES ===============================
def tip_keyboard() -> Optional[InlineKeyboardMarkup]:
    rows = []
    if TIP_UPI_ID:
        rows.append([InlineKeyboardButton("Tip via UPI", url=f"upi://pay?pa={TIP_UPI_ID}&pn=IndiQuant&cu=INR")])
    for label, url in [("Buy Me a Coffee", TIP_BMC), ("Ko-fi", TIP_KOFI), ("Stripe", TIP_STRIPE), ("Razorpay", TIP_RAZORPAY)]:
        if url: rows.append([InlineKeyboardButton(label, url=url)])
    return InlineKeyboardMarkup(rows) if rows else None

def build_alert_html(levels: dict, C: dict) -> Tuple[str, dict]:
    ts = now_ist()
    valid_until = ts + timedelta(minutes=C.get("valid_minutes", 180))
    entry, sl, tp = levels["entry"]["price"], levels["sl"]["price"], levels["tp"]["price"]
    gross, net, cps = net_rr(entry, sl, tp, C)
    # Aggressive filter
    if not C.get("aggressive", False) and net < 1.30:
        raise RuntimeError("Net RR below minimum 1.30:1")

    sqoff = C["square_off_time"] if levels["session"].lower()=="intraday" else "N/A"

    payload = {
      "$schema":"indiquant.alert.v3",
      "alert_id": str(uuid.uuid4()),
      "ts_ist": ist_iso(ts),
      "symbol": levels["symbol"],
      "timeframe": levels["timeframe"],
      "session": levels["session"],
      "idea": levels["idea"],
      "entry":{"type":levels["entry"]["type"],"price":entry,"valid_until_ist": valid_until.astimezone(IST).strftime("%H:%M")},
      "sl":{"method":levels["sl"]["method"],"price":sl},
      "tp":{"method":levels["tp"]["method"],"price":tp,"r_multiple":levels["tp"]["r_multiple"]},
      "tp_multi":{"enabled": C["tp"].get("multi",{}).get("enable",False),"tp1":None,"tp2":None,"trail_to_be":False},
      "confidence": levels["confidence"],
      "assumptions": {**levels["assumptions"]},
      "filters": levels["filters"],
      "costs":{"model":C["costs"]["brokerage_model"],"slippage_bps":C["costs"]["slippage_bps"]},
      "risk_reward":{"gross":round(gross,2),"net":round(net,2)},
      "notes":["educational only","square-off "+sqoff if levels["session"].lower()=="intraday" else "swing"]
    }

    dec = [
        "Objective: Buy setup (long-only)",
        "Data: yfinance, closed bars, IST",
        f"Method: {levels['idea']}",
        "Controls: ADV gate; spread/bands unknown",
        "Levels: tick-rounded Entry/SL/TP",
        f"Net R:R {payload['risk_reward']['net']:.2f}:1; Valid {valid_until.astimezone(IST).strftime('%H:%M')} IST"
    ]
    html = (
      f"<b>{payload['ts_ist']}</b>\n"
      f"<b>ALERT • {payload['symbol']} • {payload['timeframe']} • {payload['session'].capitalize()} (IST)</b>\n"
      f"Idea: {levels['idea']} • Confidence {levels['confidence']}%\n"
      f"Entry: <b>{payload['entry']['type']} ₹{entry:.2f}</b> • Valid till <b>{payload['entry']['valid_until_ist']}</b>\n"
      f"SL: ₹{sl:.2f} ({levels['sl']['method']}) • TP: ₹{tp:.2f} ({levels['tp']['method']}) → RR(gross) <b>{payload['risk_reward']['gross']:.2f}:1</b>\n"
      f"Costs: brokerage {C['costs']['brokerage_model']}, GST {C['costs']['gst_pct']*100:.0f}%, STT {C['costs']['stt_pct']*100:.2f}%, slippage {C['costs']['slippage_bps']} bps → RR(net) <b>{payload['risk_reward']['net']:.2f}:1</b>\n"
      f"Timing: session {payload['session']} • square-off {sqoff} IST • Filters: ADV {'OK' if levels['filters']['adv_ok'] else 'FAIL'}; spread/bands unknown\n"
      f"Decision Trace: " + " → ".join(dec) + "\n"
      f"<i>{COMPLIANCE}</i>\n"
      f"<pre><code>{json.dumps(payload, indent=2)}</code></pre>"
    )
    return html, payload

async def push_html(context: ContextTypes.DEFAULT_TYPE, chat_id: int, html: str):
    kb = tip_keyboard()
    await context.bot.send_message(chat_id=chat_id, text=html, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)

# ============================== AUTOSCAN CORE ===============================
async def scan_once_for_tf(context: ContextTypes.DEFAULT_TYPE, tf: str):
    global SENT_THIS_MINUTE
    ts = now_ist()
    RUNTIME["ist_ts"] = ist_iso(ts)
    RUNTIME["scan_status"] = "running" if is_market_open(ts) else "idle"
    if not is_market_open(ts): return

    suppressed_mode = in_any_quiet(ts)
    reset_throttle_if_new_minute(ts)
    max_per_min = SCHEDULE["throttle"]["max_per_min"]
    max_per_symbol = SCHEDULE["throttle"]["max_per_symbol"]

    shipped = skipped = deduped = 0

    for sym in CONFIG["universe"]:
        key = (sym.upper(), tf)
        vu = LAST_ALERT_VALID.get(key)
        if vu and ts < vu:
            deduped += 1
            continue

        # per symbol/session gate
        skey = symbol_session_key(sym, ts)
        if SYMBOL_COUNT.get(skey, 0) >= max_per_symbol:
            skipped += 1
            continue

        # per minute throttle
        if SENT_THIS_MINUTE >= max_per_min:
            RUNTIME["counters"]["suppressed"] += 1
            continue

        # compute & send
        try:
            lv = compute_levels(sym, tf, CONFIG["session"], CONFIG, CONFIG["signal"]["name"])
            html, payload = build_alert_html(lv, CONFIG)
        except Exception as e:
            skipped += 1
            continue

        if suppressed_mode:
            RUNTIME["counters"]["suppressed"] += 1
        else:
            chat_id = RUNTIME["chat_id"] or ADMIN_CHAT_ID
            if not chat_id:
                continue
            try:
                await push_html(context, int(chat_id), html)
                shipped += 1
                SENT_THIS_MINUTE += 1
                SYMBOL_COUNT[skey] = SYMBOL_COUNT.get(skey, 0) + 1
                valid_until = ts + timedelta(minutes=CONFIG["valid_minutes"])
                LAST_ALERT_VALID[key] = valid_until
            except Exception:
                log.exception("send failed")

    RUNTIME["counters"]["alerts_sent"] += shipped
    RUNTIME["counters"]["skipped"] += skipped
    RUNTIME["counters"]["deduped"] += deduped
    RUNTIME["last_bar_close"][tf] = ts.strftime("%H:%M")

async def scheduler_job(context: ContextTypes.DEFAULT_TYPE):
    if not SCHEDULE["autoscan"]: return
    ts = now_ist()
    if not is_market_open(ts): return
    for tf in SCHEDULE["timeframes"]:
        if bar_close_now(ts, tf):
            await scan_once_for_tf(context, tf)

async def digest_job(context: ContextTypes.DEFAULT_TYPE):
    if not (SCHEDULE["autoscan"] and SCHEDULE["digest"]["enabled"]): return
    ts = now_ist()
    if not is_market_open(ts): return
    chat_id = RUNTIME["chat_id"] or ADMIN_CHAT_ID
    if not chat_id: return
    html = (
      f"<b>{ist_iso(ts)}</b>\n"
      f"<b>NO SETUPS</b> — Last {SCHEDULE['digest']['interval_minutes']}m across {', '.join(SCHEDULE['timeframes'])}; "
      f"{RUNTIME['counters']['skipped']} skipped (filters/throttle), {RUNTIME['counters']['deduped']} deduped.\n"
      f"Next scan: on bar closes\n"
      f"<pre><code>{json.dumps(RUNTIME, indent=2)}</code></pre>\n"
      f"<i>{COMPLIANCE}</i>"
    )
    await push_html(context, int(chat_id), html)

async def eod_digest_job(context: ContextTypes.DEFAULT_TYPE):
    ts = now_ist()
    if ts.weekday() >= 5: return
    end = parse_hhmm(SCHEDULE["market_window"]["end"])
    if ts.hour != end.hour or ts.minute != end.minute: return
    chat_id = RUNTIME["chat_id"] or ADMIN_CHAT_ID
    if not chat_id: return
    html = (
      f"<b>{ist_iso(ts)}</b>\n"
      f"<b>EOD DIGEST</b> — Alerts sent: {RUNTIME['counters']['alerts_sent']}, "
      f"Suppressed: {RUNTIME['counters']['suppressed']}, Deduped: {RUNTIME['counters']['deduped']}.\n"
      f"Auto square-off: {CONFIG['square_off_time']} IST (intraday rule)\n"
      f"<pre><code>{json.dumps(RUNTIME, indent=2)}</code></pre>\n"
      f"<i>{COMPLIANCE}</i>"
    )
    await push_html(context, int(chat_id), html)
    SYMBOL_COUNT.clear()
    RUNTIME["counters"] = {"alerts_sent":0,"suppressed":0,"deduped":0,"skipped":0}

# ================================ COMMANDS =================================
HELP_TEXT = textwrap.dedent(f"""
{BOT_NAME} ({VERSION})
IST • NSE/BSE cash & ETFs • BUY-side alerts • closed bars
<i>{COMPLIANCE}</i>

Commands:
/configure key=value ...             — costs/tick/session/square_off (AutoScan ON)
/universe TCS,RELIANCE,...           — set symbols
/timeframe 1m|5m|15m|1h|D            — default D (swing)
/schedule tf=1h,D window=09:15-15:30 digest=30m
/autoscan on|off
/strategy auto|ema|rsi2|donchian|hybrid
/sl atr:2.5 | structure | trail:3
/tp r:2 | atrband:3 | level:prev_high
/filters min_adv=5e7 max_spread=0.35 band_guard=on
/session intraday|swing square_off=15:20
/aggressive on|off
/alert SYMBOL [tf] [session] [strategy]
/backtest SYMBOL start=YYYY-MM-DD end=YYYY-MM-DD [strategy=auto] [capital=100000]
/status
/reset
""")

async def bind_chat(update: Update):
    cid = update.effective_chat.id if update and update.effective_chat else None
    if cid: RUNTIME["chat_id"] = cid

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    await update.message.reply_text(HELP_TEXT, parse_mode="HTML")

def parse_kv(args: List[str]) -> Dict[str,str]:
    kv = {}
    for a in args:
        if "=" in a:
            k,v = a.split("=",1)
            kv[k.strip()] = v.strip()
    return kv

async def cmd_configure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text("Usage: /configure key=value ...")
        return
    kv = parse_kv(context.args)
    # costs
    for k in list(kv.keys()):
        if k in CONFIG["costs"]:
            v = kv.pop(k)
            try: CONFIG["costs"][k] = float(v)
            except: CONFIG["costs"][k] = v
    # filters/session/misc
    if "min_adv" in kv: CONFIG["filters"]["min_adv_inr"] = float(kv.pop("min_adv"))
    if "max_spread" in kv: CONFIG["filters"]["max_spread_pct"] = float(kv.pop("max_spread"))
    if "band_guard" in kv: CONFIG["filters"]["band_guard"] = kv.pop("band_guard").lower() in ["1","on","true","yes"]
    if "tick_size" in kv: CONFIG["tick_size"] = float(kv.pop("tick_size"))
    if "valid_minutes" in kv: CONFIG["valid_minutes"] = int(kv.pop("valid_minutes"))
    if "session" in kv: CONFIG["session"] = kv.pop("session")
    if "square_off" in kv: CONFIG["square_off_time"] = kv.pop("square_off")
    # flags
    SCHEDULE["autoscan"] = True
    RUNTIME["autoscan"] = "on"
    await update.message.reply_text(
        f"<b>{ist_iso()}</b>\n<b>AUTOSCAN ON</b> — Symbols {len(CONFIG['universe'])} • TF {', '.join(SCHEDULE['timeframes'])}\n"
        f"Window {SCHEDULE['market_window']['start']}-{SCHEDULE['market_window']['end']} IST\n"
        f"<i>{COMPLIANCE}</i>\n<pre><code>{json.dumps(SCHEDULE, indent=2)}</code></pre>", parse_mode="HTML"
    )

async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text("Usage: /universe TCS,RELIANCE,..."); return
    syms = " ".join(context.args).replace(" ","")
    CONFIG["universe"] = [s for s in syms.split(",") if s]
    RUNTIME["universe"] = CONFIG["universe"]
    await update.message.reply_text(f"Universe set: {', '.join(CONFIG['universe'])}")

async def cmd_timeframe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text("Usage: /timeframe 1m|5m|15m|1h|D"); return
    CONFIG["timeframe"] = context.args[0]
    await update.message.reply_text(f"Default timeframe set: {CONFIG['timeframe']}")

async def cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    kv = parse_kv(context.args)
    if "tf" in kv: SCHEDULE["timeframes"] = [t.strip() for t in kv["tf"].split(",") if t.strip()]
    if "window" in kv and "-" in kv["window"]:
        st, en = kv["window"].split("-")
        SCHEDULE["market_window"]["start"] = st
        SCHEDULE["market_window"]["end"] = en
    if "digest" in kv:
        SCHEDULE["digest"]["enabled"] = True
        SCHEDULE["digest"]["interval_minutes"] = int(kv["digest"].replace("m","")) if "m" in kv["digest"] else int(kv["digest"])
    await update.message.reply_text(f"Schedule updated.\n<pre><code>{json.dumps(SCHEDULE, indent=2)}</code></pre>", parse_mode="HTML")

async def cmd_autoscan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    mode = context.args[0].lower() if context.args else "on"
    on = mode == "on"
    SCHEDULE["autoscan"] = on
    RUNTIME["autoscan"] = "on" if on else "off"
    await update.message.reply_text(f"AUTOSCAN {'ON' if on else 'OFF'}")

async def cmd_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /strategy auto|ema|rsi2|donchian|hybrid"); return
    choice = context.args[0].lower()
    if choice not in ["auto","ema","rsi2","donchian","hybrid"]:
        await update.message.reply_text("Invalid. Use auto|ema|rsi2|donchian|hybrid"); return
    CONFIG["signal"]["name"] = choice
    await update.message.reply_text(f"Strategy set: {CONFIG['signal']['name']}")

async def cmd_sl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /sl atr:2.5 | structure | trail:3"); return
    arg = context.args[0].lower()
    if arg.startswith("atr:"):
        CONFIG["sl"]["method"] = "atr"; CONFIG["sl"]["params"]["atr_k"] = float(arg.split(":")[1])
    elif arg.startswith("trail:"):
        CONFIG["sl"]["method"] = "trail"; CONFIG["sl"]["params"]["atr_k"] = float(arg.split(":")[1])
    elif arg == "structure":
        CONFIG["sl"]["method"] = "structure"
    await update.message.reply_text(f"SL updated: {CONFIG['sl']}")

async def cmd_tp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /tp r:2 | atrband:3 | level:prev_high"); return
    arg = context.args[0].lower()
    if arg.startswith("r:"):
        CONFIG["tp"]["method"] = "r"; CONFIG["tp"]["params"]["r"] = float(arg.split(":")[1])
    elif arg.startswith("atrband:"):
        CONFIG["tp"]["method"] = "atrband"; CONFIG["tp"]["params"]["k"] = float(arg.split(":")[1])
    elif arg.startswith("level"):
        CONFIG["tp"]["method"] = "level"; CONFIG["tp"]["params"]["level"] = "prev_high"
    await update.message.reply_text(f"TP updated: {CONFIG['tp']}")

async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kv = parse_kv(context.args)
    if "min_adv" in kv: CONFIG["filters"]["min_adv_inr"] = float(kv["min_adv"])
    if "max_spread" in kv: CONFIG["filters"]["max_spread_pct"] = float(kv["max_spread"])
    if "band_guard" in kv: CONFIG["filters"]["band_guard"] = kv["band_guard"].lower() in ["on","true","1","yes"]
    await update.message.reply_text(f"Filters updated: {CONFIG['filters']}")

async def cmd_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kv = parse_kv(context.args)
    if context.args and context.args[0] in ["intraday","swing"]:
        CONFIG["session"] = context.args[0]
    if "square_off" in kv:
        CONFIG["square_off_time"] = kv["square_off"]
    await update.message.reply_text(f"Session: {CONFIG['session']} • Square-off: {CONFIG['square_off_time']}")

async def cmd_aggressive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /aggressive on|off"); return
    CONFIG["aggressive"] = context.args[0].lower() == "on"
    await update.message.reply_text(f"Aggressive mode: {'ON' if CONFIG['aggressive'] else 'OFF'}")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    snap = {"config": CONFIG, "schedule": SCHEDULE, "runtime": {**RUNTIME, "ist_ts": ist_iso()}}
    await update.message.reply_text(f"<pre><code>{json.dumps(snap, indent=2)}</code></pre>", parse_mode="HTML")

async def cmd_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text("Usage: /alert SYMBOL [tf] [session] [strategy]"); return
    sym = context.args[0].upper()
    tf = (context.args[1] if len(context.args)>=2 else CONFIG["timeframe"])
    sess = (context.args[2] if len(context.args)>=3 else CONFIG["session"]).lower()
    strat = (context.args[3] if len(context.args)>=4 else CONFIG["signal"]["name"]).lower()
    try:
        lv = compute_levels(sym, tf, sess, CONFIG, strat)
        html, payload = build_alert_html(lv, CONFIG)
    except Exception as e:
        await update.message.reply_text(f"Cannot compute alert: {e}"); return
    await update.message.reply_text(html, parse_mode="HTML", reply_markup=tip_keyboard(), disable_web_page_preview=True)

# ============================ BACKTEST (Daily) ==============================
class BTResult:
    def __init__(self, metrics: dict, trades: pd.DataFrame): self.metrics, self.trades = metrics, trades

def compute_levels_daily_row(i: int, df: pd.DataFrame, C: dict, strategy: str) -> Optional[Tuple[float,float,float,str]]:
    close = df["Close"].iloc[:i+1]; high = df["High"].iloc[:i+1]; low = df["Low"].iloc[:i+1]
    ema_fast_n = C["signal"]["params"].get("ema_fast",20)
    ema_slow_n = C["signal"]["params"].get("ema_slow",50)
    don_n = C["signal"]["params"].get("donchian_n",20)
    rsi2_th = C["signal"]["params"].get("rsi2_th",10)
    tick = C.get("tick_size", 0.05)
    ema_f = ema(close, ema_fast_n); ema_s = ema(close, ema_slow_n); a = atr(high, low, close, 14)
    sma200 = sma(close, 200)
    last_close, last_high = float(close.iloc[-1]), float(high.iloc[-1])
    last_ema_f, last_ema_s, last_atr = float(ema_f.iloc[-1]), float(ema_s.iloc[-1]), float(a.iloc[-1])
    st = strategy.lower()
    if st == "auto" or st == "hybrid":
        # mini-auto for daily
        if last_ema_f > last_ema_s and last_close <= last_ema_f and (not np.isnan(sma200.iloc[-1]) and last_close >= float(sma200.iloc[-1])):
            st = "ema"
        elif float(rsi2(close).iloc[-1]) <= rsi2_th and (not np.isnan(sma200.iloc[-1]) and last_close > float(sma200.iloc[-1])):
            st = "rsi2"
        else:
            st = "donchian"
    if st == "ema":
        raw_entry = max(last_high, last_close) * 1.001
    elif st == "rsi2":
        if np.isnan(sma200.iloc[-1]) or last_close <= float(sma200.iloc[-1]): return None
        raw_entry = last_close * 1.001
    elif st == "donchian":
        don_hi = high.rolling(don_n).max().iloc[-1]
        if np.isnan(don_hi): return None
        bump = 0.01 if tick < 0.1 else tick
        raw_entry = float(don_hi) + bump
    else:
        return None
    raw_sl = raw_entry - C["sl"]["params"].get("atr_k",2.5) * last_atr
    R = C["tp"]["params"].get("r", 2.0)
    raw_tp = raw_entry + R * (raw_entry - raw_sl)
    entry, sl, tp = round_tick(raw_entry,tick), round_tick(raw_sl,tick), round_tick(raw_tp,tick)
    if not (sl < entry < tp): return None
    return (entry, sl, tp, st)

def backtest_daily(symbol: str, start: str, end: str, C: dict, strategy: str="auto", capital_start: float=100000.0) -> BTResult:
    df = fetch_daily(symbol, start, end).dropna().copy()
    if len(df) < 260: raise RuntimeError("Need ≥ ~1y daily data.")
    tick = C.get("tick_size",0.05)
    cash = capital_start; pos_qty = 0; pos_entry = None; pos_sl = None; pos_tp = None
    trades = []
    for i in range(70, len(df)-1):
        nxt = df.iloc[i+1]; n_dt = df.index[i+1].date()
        if pos_qty == 0:
            lv = compute_levels_daily_row(i, df, C, strategy)
            if lv:
                entry_px, sl_px, tp_px, st = lv
                px_open = float(nxt["Open"])
                entry_fill = round_tick(max(entry_px, px_open), tick)
                per_share_cost = cost_per_share(entry_fill, entry_fill, C)
                qty = int(cash // entry_fill)
                if qty > 0:
                    pos_qty = qty; pos_entry = entry_fill; pos_sl = sl_px; pos_tp = tp_px
                    cash -= qty * entry_fill + per_share_cost * qty / 2.0  # approx half on entry
                    trades.append({"date": str(n_dt), "action":"BUY", "price": entry_fill, "qty": qty, "cash": cash, "strategy": st})
        else:
            day_high = float(nxt["High"]); day_low = float(nxt["Low"])
            exit_px = None; reason = None
            if day_low <= pos_sl:
                exit_px = round_tick(pos_sl, tick); reason = "SL"
            elif day_high >= pos_tp:
                exit_px = round_tick(pos_tp, tick); reason = "TP"
            if exit_px is not None:
                per_share_cost = cost_per_share(pos_entry, exit_px, C)
                pnl_gross = (exit_px - pos_entry) * pos_qty
                pnl_net = pnl_gross - per_share_cost * pos_qty
                cash += pos_qty * exit_px - per_share_cost * pos_qty / 2.0
                trades.append({"date": str(n_dt), "action":reason, "price": exit_px, "qty": -pos_qty, "pnl_net": pnl_net, "cash": cash})
                pos_qty = 0; pos_entry = pos_sl = pos_tp = None
    if pos_qty > 0:
        last_close = float(df.iloc[-1]["Close"])
        exit_px = round_tick(last_close, tick)
        per_share_cost = cost_per_share(pos_entry, exit_px, C)
        pnl_gross = (exit_px - pos_entry) * pos_qty
        pnl_net = pnl_gross - per_share_cost * pos_qty
        cash += pos_qty * exit_px - per_share_cost * pos_qty / 2.0
        trades.append({"date": str(df.index[-1].date()), "action":"EOD", "price": exit_px, "qty": -pos_qty, "pnl_net": pnl_net, "cash": cash})
    trades_df = pd.DataFrame(trades)
    if trades_df.empty: raise RuntimeError("No trades generated.")
    equity = trades_df["cash"].astype(float)
    total_ret = (equity.iloc[-1] - capital_start) / capital_start
    days = max(1, (df.index[-1].date() - df.index[70].date()).days)
    cagr = (1 + total_ret) ** (365.25/days) - 1 if days > 0 else 0.0
    pnl_series = trades_df.get("pnl_net", pd.Series(dtype=float)).fillna(0.0)
    daily_ret = pnl_series / capital_start
    if len(daily_ret) < 2:
        vol = sharpe = sortino = 0.0
    else:
        vol = float(np.std(daily_ret, ddof=1)) * math.sqrt(252)
        sharpe = float(np.mean(daily_ret) / (np.std(daily_ret, ddof=1) + 1e-9)) * math.sqrt(252)
        downside = np.std(np.minimum(daily_ret, 0), ddof=1) * math.sqrt(252)
        sortino = float(np.mean(daily_ret) / (downside + 1e-9))
    eq_vals = equity.values.astype(float)
    peaks = np.maximum.accumulate(eq_vals)
    dds = (eq_vals - peaks) / peaks
    maxdd = float(np.min(dds)) if len(dds) else 0.0
    trade_pnls = trades_df["pnl_net"].dropna()
    wins = trade_pnls[trade_pnls > 0].sum()
    losses = -trade_pnls[trade_pnls < 0].sum()
    profit_factor = float(wins / (losses + 1e-9)) if losses > 0 else float("inf")
    win_rate = float((trade_pnls > 0).mean()) if len(trade_pnls) else 0.0
    metrics = {
        "symbol": symbol.upper(),
        "period": f"{start}..{end}",
        "capital_start": round(capital_start,2),
        "capital_end": round(float(equity.iloc[-1]),2),
        "TotalReturn_pct": round(total_ret*100,2),
        "CAGR_pct": round(cagr*100,2),
        "Vol_ann_pct": round(vol*100,2),
        "Sharpe": round(sharpe,2),
        "Sortino": round(sortino,2),
        "MaxDD_pct": round(maxdd*100,2),
        "Trades": int((trades_df["action"].isin(["TP","SL","EOD"])).sum()),
        "WinRate_pct": round(win_rate*100,2),
        "ProfitFactor": round(profit_factor,2)
    }
    return BTResult(metrics, trades_df)

def format_bt_html(res: BTResult) -> str:
    m = res.metrics
    return (
        f"<b>{ist_iso()}</b>\n"
        f"<b>BACKTEST • {m['symbol']} • D</b>\n"
        f"Period: {m['period']} • Start ₹{m['capital_start']:.2f} → End ₹{m['capital_end']:.2f}\n"
        f"TotalReturn {m['TotalReturn_pct']}% • CAGR {m['CAGR_pct']}% • Vol {m['Vol_ann_pct']}% • Sharpe {m['Sharpe']} • Sortino {m['Sortino']}\n"
        f"MaxDD {m['MaxDD_pct']}% • Trades {m['Trades']} • WinRate {m['WinRate_pct']}% • PF {m['ProfitFactor']}\n"
        f"<i>{COMPLIANCE}</i>\n"
        f"<pre><code>{json.dumps(m, indent=2)}</code></pre>"
    )

async def cmd_backtest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /backtest SYMBOL start=YYYY-MM-DD end=YYYY-MM-DD [strategy=auto] [capital=100000]"); return
    sym = context.args[0].upper()
    kv = parse_kv(context.args[1:])
    start = kv.get("start"); end = kv.get("end")
    strat = kv.get("strategy", CONFIG["signal"]["name"])
    capital = float(kv.get("capital", 100000))
    if not start or not end:
        await update.message.reply_text("Provide start and end (YYYY-MM-DD)"); return
    try:
        res = backtest_daily(sym, start, end, CONFIG, strat, capital)
    except Exception as e:
        await update.message.reply_text(f"Backtest failed: {e}"); return
    await update.message.reply_text(format_bt_html(res), parse_mode="HTML")

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CONFIG, SCHEDULE, RUNTIME, LAST_ALERT_VALID, SYMBOL_COUNT
    CONFIG = json.loads(json.dumps(DEFAULT_CONFIG))
    SCHEDULE = json.loads(json.dumps(DEFAULT_SCHEDULE))
    LAST_ALERT_VALID.clear(); SYMBOL_COUNT.clear()
    RUNTIME.update({"autoscan":"off","scan_status":"idle",
                    "counters":{"alerts_sent":0,"suppressed":0,"deduped":0,"skipped":0},
                    "universe":CONFIG["universe"]})
    await update.message.reply_text("Reset to factory defaults.")

async def non_command_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Enforce “commands only”
    await update.message.reply_text("Use commands only. See /help.")

# ================================ APP / JOBS ================================
def setup_jobs(app):
    jq: JobQueue = app.job_queue
    jq.run_repeating(scheduler_job, interval=1.0, first=1.5, name="scheduler")
    jq.run_repeating(digest_job, interval=60*max(1, SCHEDULE["digest"]["interval_minutes"]),
                     first=60*SCHEDULE["digest"]["interval_minutes"], name="digest")
    jq.run_repeating(eod_digest_job, interval=60, first=15, name="eod")

def build_app():
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN secret.")
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("configure", cmd_configure))
    app.add_handler(CommandHandler("universe", cmd_universe))
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
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(MessageHandler(~filters.COMMAND, non_command_guard))
    setup_jobs(app)
    return app

if __name__ == "__main__":
    app = build_app()
    log.info("Starting IndiQuant Apex AutoScan (polling)…")
    app.run_polling()  # synchronous; safe on Replit
