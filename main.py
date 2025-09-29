# IndiQuant AutoScan AlertBot — Hands-Free (IST, NSE/BSE, cost-aware, tips, backtesting)
# Educational only — not investment advice. Not SEBI-registered. Markets carry risk.

import os, json, uuid, logging, textwrap, math
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta, time as dtime, date
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, JobQueue
)

# ----------------------- CONSTANTS / GLOBALS -----------------------
IST = ZoneInfo("Asia/Kolkata")
BOT_NAME = "IndiQuant AutoScan"
VERSION = "v2.0.1"
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("indiquant.autoscan")

# --- SECRETS (set in Replit → Tools → Secrets) ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")  # optional default push target

# Tip links (set any you like; UPI strongly recommended)
TIP_UPI_ID   = os.getenv("TIP_UPI_ID", "")   # e.g., yourname@okhdfcbank
TIP_BMC      = os.getenv("TIP_BMC", "")      # https://www.buymeacoffee.com/yourid
TIP_KOFI     = os.getenv("TIP_KOFI", "")     # https://ko-fi.com/yourid
TIP_STRIPE   = os.getenv("TIP_STRIPE", "")   # Stripe Payment Link
TIP_RAZORPAY = os.getenv("TIP_RAZORPAY", "") # Razorpay Payment Link

COMPLIANCE = "Educational only — not investment advice. Not SEBI-registered. Markets carry risk."

# ----------------------- DEFAULT CONFIG ----------------------------
DEFAULT_CONFIG = {
    "$schema":"indiquant.config.v1",
    "timezone":"Asia/Kolkata",
    "universe":["TCS","RELIANCE","HDFCBANK","NIFTYBEES"],
    "timeframe":"15m",
    "session":"intraday",
    "square_off_time":"15:20",
    "data":{"source":"yfinance","bar_size":"15m","adjusted":True,"start":None,"end":None},
    "signal":{"name":"auto", "params":{
        "ema_fast":20, "ema_slow":50,       # EMA-trend
        "rsi2_th":10,                       # RSI2 oversold threshold
        "donchian_n":20                     # Donchian breakout length
    }},
    "sl":{"method":"atr","params":{"atr_k":2.5}},
    "tp":{"method":"r","params":{"r":2}},
    "filters":{"min_adv_inr":5e7,"max_spread_pct":0.35,"band_guard":True},
    "costs":{
        # PLACEHOLDER rates — replace via /configure
        "brokerage_model":"flat",      # flat|percent
        "brokerage_flat_inr":20,       # per order
        "brokerage_pct":0.0003,        # if percent
        "stt_pct":0.001,               # 0.10%
        "exchange_txn_pct":0.0000325,
        "sebi_fee_pct":0.000001,
        "stamp_duty_pct":0.00015,
        "gst_pct":0.18,
        "slippage_bps":5
    },
    "tick_size":0.05,
    "valid_minutes":45
}

DEFAULT_SCHEDULE = {
    "$schema":"indiquant.schedule.v1",
    "timezone":"Asia/Kolkata",
    "market_window":{"start":"09:15","end":"15:30"},
    "timeframes":["5m","15m"],
    "autoscan":False,  # flips to True after /configure unless user turns off
    "quiet_windows":[{"start":"09:15","end":"09:30"}],
    "digest":{"enabled":True,"interval_minutes":30,"eod":True},
    "throttle":{"max_per_min":10,"max_per_symbol":3},
    "live_ticks":False
}

RUNTIME = {
    "$schema":"indiquant.runtime.v1",
    "ist_ts":None,
    "autoscan":"off",
    "scan_status":"idle",
    "last_bar_close":{"1m":None,"5m":None,"15m":None,"1h":None,"D":None},
    "counters":{"alerts_sent":0,"suppressed":0,"deduped":0,"skipped":0},
    "universe":DEFAULT_CONFIG["universe"],
    "filters":{"min_adv_ok":True,"spread_ok":None,"band_guard_ok":True},
    "chat_id": ADMIN_CHAT_ID or None
}

# Dedupe store: (symbol, timeframe) -> valid_until_ist (datetime)
LAST_ALERT_VALID: Dict[Tuple[str,str], datetime] = {}
# Per-minute throttle counters
CURRENT_MINUTE = None
SENT_THIS_MINUTE = 0
# Per-symbol per-session counters
SYMBOL_COUNT: Dict[Tuple[str,str], int] = {}  # (symbol, YYYY-MM-DD) -> count

CONFIG = json.loads(json.dumps(DEFAULT_CONFIG))
SCHEDULE = json.loads(json.dumps(DEFAULT_SCHEDULE))
DEFAULT_STRATEGY = "auto"  # auto|ema|rsi2|donchian

# ----------------------- TIME / CALENDAR HELPERS -------------------
def now_ist() -> datetime:
    return datetime.now(tz=IST)

def ist_iso(dt: Optional[datetime]=None) -> str:
    return (dt or now_ist()).astimezone(IST).isoformat()

def parse_hhmm(s: str) -> dtime:
    hh, mm = s.split(":")
    return dtime(hour=int(hh), minute=int(mm), tzinfo=IST)

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
    m = ts.minute
    s = ts.second
    if not is_market_open(ts): return False
    if tf == "1m":   return s == 0
    if tf == "5m":   return (m % 5 == 0) and s == 0
    if tf == "15m":  return (m % 15 == 0) and s == 0
    if tf == "1h":   return m == 0 and s == 0
    if tf == "D":
        end = SCHEDULE["market_window"]["end"]  # "15:30"
        eh, em = map(int, end.split(":"))
        return ts.hour == eh and ts.minute == em + 1 and s == 0
    return False

# ----------------------- DATA / INDICATORS -------------------------
def yahoo_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if s.endswith(".NS") or s.endswith(".BO"): return s
    return s + ".NS"  # default to NSE

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window).mean()

def rsi2(series: pd.Series, period: int = 2) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    ru = up.rolling(period).mean()
    rd = down.rolling(period).mean().replace(0, np.nan)
    rs = ru / rd
    return 100 - (100 / (1 + rs))

def atr(high: pd.Series, low: pd.Series, close: pd.Series, period=14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def donchian(high: pd.Series, low: pd.Series, n: int=20) -> Tuple[pd.Series,pd.Series]:
    upper = high.rolling(n).max()
    lower = low.rolling(n).min()
    return upper, lower

def adv_inr(df: pd.DataFrame) -> Optional[float]:
    if "Close" not in df or "Volume" not in df: return None
    dv = (df["Close"] * df["Volume"]).rolling(20).mean()
    dv = dv.dropna()
    return float(dv.iloc[-1]) if not dv.empty else None

def round_tick(p: float, tick: float) -> float:
    return round(p / tick) * tick

# ----------------------- COST MODEL & RR ---------------------------
def cost_per_share(entry: float, exit_px: float, C: dict) -> float:
    c = C["costs"]
    buy_turn, sell_turn = entry, exit_px
    turn = buy_turn + sell_turn
    if c["brokerage_model"] == "flat":
        brokerage = c["brokerage_flat_inr"] * 2
    else:
        brokerage = turn * c["brokerage_pct"]
    stt  = turn * c["stt_pct"]
    exch = turn * c["exchange_txn_pct"]
    sebi = turn * c["sebi_fee_pct"]
    stamp = turn * c["stamp_duty_pct"]
    gst = (brokerage + exch + sebi) * c["gst_pct"]
    slip_inr = (entry + exit_px) * (c.get("slippage_bps", 0) / 10000.0)
    return brokerage + stt + exch + sebi + stamp + gst + slip_inr

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

# ----------------------- DATA FETCHERS -----------------------------
def fetch_intraday(symbol: str, tf: str) -> pd.DataFrame:
    tf_map = {"1m":("1m","5d"), "5m":("5m","30d"), "15m":("15m","60d"),
              "1h":("60m","730d")}
    interval, period = tf_map[tf]
    df = yf.download(tickers=yahoo_symbol(symbol), interval=interval, period=period,
                     auto_adjust=True, progress=False)
    if df is None or df.empty: raise RuntimeError("No data")
    df = df.rename(columns=str.title).dropna()
    return df

def fetch_daily(symbol: str, start: Optional[str]=None, end: Optional[str]=None) -> pd.DataFrame:
    df = yf.download(tickers=yahoo_symbol(symbol), start=start, end=end,
                     auto_adjust=True, progress=False)
    if df is None or df.empty: raise RuntimeError("No data")
    df = df.rename(columns=str.title).dropna()
    return df

def fetch_ohlcv(symbol: str, tf: str) -> pd.DataFrame:
    if tf == "D":
        return fetch_daily(symbol)
    return fetch_intraday(symbol, tf)

# ----------------------- STRATEGY ENGINES --------------------------
def compute_levels(symbol: str, tf: str, session: str, C: dict, strategy: str="auto") -> dict:
    df = fetch_ohlcv(symbol, tf)
    if len(df) < 60: raise RuntimeError("Insufficient bars")
    close, high, low = df["Close"], df["High"], df["Low"]
    tick = C.get("tick_size", 0.05)

    # Indicators
    ema_fast = ema(close, C["signal"]["params"].get("ema_fast",20))
    ema_slow = ema(close, C["signal"]["params"].get("ema_slow",50))
    atr14 = atr(high, low, close, 14)
    rsi2_now = float(rsi2(close).iloc[-1])
    don_n = int(C["signal"]["params"].get("donchian_n", 20))
    don_hi, don_lo = donchian(high, low, don_n)

    last = {
        "close": float(close.iloc[-1]),
        "high": float(high.iloc[-1]),
        "low": float(low.iloc[-1]),
        "ema_fast": float(ema_fast.iloc[-1]),
        "ema_slow": float(ema_slow.iloc[-1]),
        "atr": float(atr14.iloc[-1]),
        "don_hi": float(don_hi.iloc[-1]) if not np.isnan(don_hi.iloc[-1]) else None,
        "don_lo": float(don_lo.iloc[-1]) if not np.isnan(don_lo.iloc[-1]) else None
    }
    adv = adv_inr(df)
    adv_ok = (adv is not None) and (adv >= C["filters"]["min_adv_inr"])
    spread_ok = None  # not available from yfinance
    band_ok = True

    # Strategy selection
    st = strategy.lower()
    if st == "auto":
        if last["ema_fast"] > last["ema_slow"] and last["close"] <= last["ema_fast"]:
            st = "ema"
        elif rsi2_now <= C["signal"]["params"].get("rsi2_th", 10):
            st = "rsi2"
        else:
            st = "donchian"

    # Compute entry/SL/TP
    k = C["sl"]["params"].get("atr_k", 2.5)
    R = C["tp"]["params"].get("r", 2.0)
    entry_type = "stop_trigger"

    if st == "ema":
        raw_entry = max(last["high"], last["close"]) * 1.001
        idea = f"EMA({C['signal']['params'].get('ema_fast',20)}/{C['signal']['params'].get('ema_slow',50)}) trend + pullback"
        conf = 50 + 10 + (10 if adv_ok else 0) + (10 if last["close"] <= last["ema_fast"] else 0)
    elif st == "rsi2":
        sma200 = float(sma(close, 200).iloc[-1])
        if np.isnan(sma200): raise RuntimeError("SMA200 unavailable")
        if last["close"] < sma200:
            raise RuntimeError("RSI2 setup blocked by SMA200 filter (price < SMA200)")
        raw_entry = last["close"] * 1.001
        idea = f"RSI2<{C['signal']['params'].get('rsi2_th',10)} + SMA200 trend filter"
        conf = 50 + 10 + (10 if adv_ok else 0) + (10 if rsi2_now <= C['signal']['params'].get('rsi2_th',10) else 0)
    elif st == "donchian":
        if last["don_hi"] is None:
            raise RuntimeError("Donchian bands unavailable")
        raw_entry = (last["don_hi"] + 0.01)
        idea = f"Donchian breakout N={don_n}"
        conf = 50 + 10 + (10 if adv_ok else 0)
    else:
        raise RuntimeError(f"Unknown strategy: {strategy}")

    raw_sl = raw_entry - k * last["atr"]
    raw_tp = raw_entry + R * (raw_entry - raw_sl)

    entry = round_tick(raw_entry, tick)
    sl    = round_tick(raw_sl, tick)
    tp    = round_tick(raw_tp, tick)
    if not (sl < entry < tp): raise RuntimeError("Invalid levels after rounding")

    conf = max(0, min(100, conf))
    return {
        "symbol": symbol.upper(),
        "timeframe": tf,
        "session": session,
        "entry":{"type":entry_type,"price": entry},
        "sl":{"method": f"ATR*{k}", "price": sl},
        "tp":{"method": f"R={R}", "price": tp, "r_multiple": R},
        "confidence": int(conf),
        "assumptions":{"data_src":"yfinance","bar_size":tf,"tick_size":tick,"strategy":st},
        "filters":{"adv_ok": bool(adv_ok), "spread_ok": spread_ok, "band_guard_ok": band_ok},
        "last":{"close": last["close"], "ema_fast": last["ema_fast"], "ema_slow": last["ema_slow"],
                "atr": last["atr"], "adv_inr": adv, "don_hi": last["don_hi"], "don_lo": last["don_lo"]}
    }

# ----------------------- UI HELPERS --------------------------------
def tip_keyboard() -> Optional[InlineKeyboardMarkup]:
    rows = []
    if TIP_UPI_ID:
        upi = f"upi://pay?pa={TIP_UPI_ID}&pn=IndiQuant&cu=INR"
        rows.append([InlineKeyboardButton("Tip via UPI", url=upi)])
    for title, url in [("Buy Me a Coffee", TIP_BMC), ("Ko-fi", TIP_KOFI),
                       ("Stripe", TIP_STRIPE), ("Razorpay", TIP_RAZORPAY)]:
        if url: rows.append([InlineKeyboardButton(title, url=url)])
    return InlineKeyboardMarkup(rows) if rows else None

def costs_placeholder_note(C: dict) -> bool:
    # Mark placeholders true until user sets their real costs via /configure.
    return True

def build_alert_html(levels: dict, C: dict) -> Tuple[str, dict]:
    ts = now_ist()
    valid_until = ts + timedelta(minutes=C["valid_minutes"])
    entry, sl, tp = levels["entry"]["price"], levels["sl"]["price"], levels["tp"]["price"]
    gross, net, cps = net_rr(entry, sl, tp, C)
    sqoff = C["square_off_time"] if levels["session"].lower()=="intraday" else "N/A"

    payload = {
      "$schema":"indiquant.alert.v1",
      "alert_id": str(uuid.uuid4()),
      "ts_ist": ist_iso(ts),
      "symbol": levels["symbol"],
      "timeframe": levels["timeframe"],
      "session": levels["session"],
      "entry":{"type":"stop","price":entry,"valid_until_ist": valid_until.astimezone(IST).strftime("%H:%M")},
      "sl":{"method":levels["sl"]["method"],"price":sl},
      "tp":{"method":levels["tp"]["method"],"price":tp,"r_multiple":levels["tp"]["r_multiple"]},
      "confidence": levels["confidence"],
      "assumptions": {**levels["assumptions"], "costs_placeholders": costs_placeholder_note(C)},
      "filters": levels["filters"],
      "costs":{"model":C["costs"]["brokerage_model"],"slippage_bps":C["costs"]["slippage_bps"]},
      "risk_reward":{"gross":round(gross,2),"net":round(net,2)},
      "notes":["educational only","square-off "+sqoff if levels["session"].lower()=="intraday" else "swing"]
    }

    idea_line = {
        "ema": f"EMA({C['signal']['params'].get('ema_fast',20)}/{C['signal']['params'].get('ema_slow',50)}) trend + pullback + ATR stop",
        "rsi2": f"RSI2<{C['signal']['params'].get('rsi2_th',10)} + SMA200 filter + ATR stop",
        "donchian": f"Donchian breakout N={C['signal']['params'].get('donchian_n',20)} + ATR stop",
        "auto": "Auto select (EMA / RSI2 / Donchian)"
    }.get(levels["assumptions"]["strategy"], "Strategy")

    dec_trace = [
      "Objective: cost-aware long setup",
      "Data scope: yfinance, closed bars",
      f"Method: {idea_line}",
      "Controls: ADV filter; spread/band unknown",
      "Levels: tick-rounded Entry/SL/TP",
      f"Net R:R: {net:.2f}:1; Valid: {valid_until.astimezone(IST).strftime('%H:%M')} IST"
    ]

    html = (
      f"<b>{payload['ts_ist']}</b>\n"
      f"<b>ALERT • {levels['symbol']} • {levels['timeframe']} • {levels['session'].capitalize()} (IST)</b>\n"
      f"Idea: {idea_line} • Confidence {levels['confidence']}%\n"
      f"Entry: <b>stop ₹{entry:.2f}</b> • Valid till <b>{valid_until.astimezone(IST).strftime('%H:%M')}</b>\n"
      f"SL: ₹{sl:.2f} ({levels['sl']['method']}) • TP: ₹{tp:.2f} ({levels['tp']['method']}) → "
      f"RR(gross) <b>{gross:.2f}:1</b>\n"
      f"Costs: brokerage {C['costs']['brokerage_model']}, GST {C['costs']['gst_pct']*100:.0f}%, "
      f"STT {C['costs']['stt_pct']*100:.2f}% , slippage {C['costs']['slippage_bps']} bps → "
      f"RR(net) <b>{net:.2f}:1</b>\n"
      f"Timing: session {levels['session']} • square-off {sqoff} IST • "
      f"Filters: ADV {'OK' if levels['filters']['adv_ok'] else 'FAIL'}; spread/bands unknown\n"
      f"Decision Trace: " + " → ".join(dec_trace) + "\n"
      f"<i>{COMPLIANCE}</i>\n"
      f"<pre><code>{json.dumps(payload, indent=2)}</code></pre>"
    )
    return html, payload

# ----------------------- AUTOSCAN CORE -----------------------------
def reset_throttle_if_new_minute(ts: datetime):
    global CURRENT_MINUTE, SENT_THIS_MINUTE
    key = ts.strftime("%Y-%m-%d %H:%M")
    if key != CURRENT_MINUTE:
        CURRENT_MINUTE = key
        SENT_THIS_MINUTE = 0

def symbol_session_key(sym: str, ts: datetime) -> Tuple[str,str]:
    return (sym.upper(), ts.strftime("%Y-%m-%d"))

async def push_html(context: ContextTypes.DEFAULT_TYPE, chat_id: int, html: str):
    kb = tip_keyboard()
    await context.bot.send_message(chat_id=chat_id, text=html, parse_mode="HTML",
                                   reply_markup=kb, disable_web_page_preview=True)

async def scan_once_for_tf(context: ContextTypes.DEFAULT_TYPE, tf: str):
    global SENT_THIS_MINUTE  # <-- FIX: declare at top since we increment it below
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
        # Dedupe window
        key = (sym.upper(), tf)
        vu = LAST_ALERT_VALID.get(key)
        if vu and ts < vu:
            deduped += 1
            continue

        # Per-symbol/session throttle
        skey = symbol_session_key(sym, ts)
        if SYMBOL_COUNT.get(skey, 0) >= max_per_symbol:
            skipped += 1
            continue

        # Throttle per minute
        if SENT_THIS_MINUTE >= max_per_min:
            RUNTIME["counters"]["suppressed"] += 1
            continue

        # Compute setup
        try:
            levels = compute_levels(sym, tf, CONFIG["session"], CONFIG, DEFAULT_STRATEGY)
        except Exception as e:
            skipped += 1
            continue

        html, payload = build_alert_html(levels, CONFIG)

        # Apply quiet suppression OR send
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
                # Validity window for dedupe
                valid_until = ts + timedelta(minutes=CONFIG["valid_minutes"])
                LAST_ALERT_VALID[key] = valid_until
            except Exception:
                log.exception("send failed")

    RUNTIME["counters"]["alerts_sent"] += shipped
    RUNTIME["counters"]["skipped"] += skipped
    RUNTIME["counters"]["deduped"] += deduped
    RUNTIME["last_bar_close"][tf] = ts.strftime("%H:%M")

async def scheduler_job(context: ContextTypes.DEFAULT_TYPE):
    """Runs every second; triggers scans on bar closes for configured timeframes."""
    if not SCHEDULE["autoscan"]:
        return
    ts = now_ist()
    if not is_market_open(ts):
        return
    for tf in SCHEDULE["timeframes"]:
        if bar_close_now(ts, tf):
            await scan_once_for_tf(context, tf)

async def digest_job(context: ContextTypes.DEFAULT_TYPE):
    """Periodic 'no setups' digest."""
    if not SCHEDULE["autoscan"] or not SCHEDULE["digest"]["enabled"]:
        return
    ts = now_ist()
    if not is_market_open(ts): return
    chat_id = RUNTIME["chat_id"] or ADMIN_CHAT_ID
    if not chat_id: return
    html = (
      f"<b>{ist_iso(ts)}</b>\n"
      f"<b>NO SETUPS</b> — Last {SCHEDULE['digest']['interval_minutes']}m across {','.join(SCHEDULE['timeframes'])}; "
      f"{RUNTIME['counters']['skipped']} skipped (filters/throttle), {RUNTIME['counters']['deduped']} deduped.\n"
      f"Next scan: on bar closes.\n"
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
      f"Auto square-off time: {CONFIG['square_off_time']} IST (intraday rule).\n"
      f"<pre><code>{json.dumps(RUNTIME, indent=2)}</code></pre>\n"
      f"<i>{COMPLIANCE}</i>"
    )
    await push_html(context, int(chat_id), html)
    # Reset per-session counters
    SYMBOL_COUNT.clear()
    RUNTIME["counters"] = {"alerts_sent":0,"suppressed":0,"deduped":0,"skipped":0}

# ----------------------- COMMANDS ---------------------------------
HELP = textwrap.dedent(f"""
{BOT_NAME} ({VERSION}) — Hands-Free AutoScan
IST • NSE/BSE cash & ETFs • cost-aware • closed bars
<i>{COMPLIANCE}</i>

/configure key=value ...     — costs/filters/session (AutoScan turns ON)
/universe TCS,RELIANCE,...   — set scan symbols
/schedule tf=5m,15m window=09:15-15:30 digest=30m
/autoscan on|off             — toggle autonomous scanning
/strategy auto|ema|rsi2|donchian
/throttle max_per_min=10 max_per_symbol=3
/quiet on|off [09:15-09:45]
/status                      — snapshot (config/schedule/runtime)
/alert SYMBOL [tf] [session] [strategy] — manual one-off
/backtest SYMBOL start=YYYY-MM-DD end=YYYY-MM-DD [strategy=auto]
/reset                       — factory defaults
""")

async def bind_chat(update: Update):
    cid = update.effective_chat.id if update and update.effective_chat else None
    if cid:
        RUNTIME["chat_id"] = cid

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    await update.message.reply_text(HELP, parse_mode="HTML")

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
        await update.message.reply_text("Usage: /configure key=value ...\n"
                                        "Example: /configure brokerage_model=flat brokerage_flat_inr=20 gst_pct=0.18 stt_pct=0.001 slippage_bps=5")
        return
    kv = parse_kv(context.args)
    # Costs
    for k in list(kv.keys()):
        if k in CONFIG["costs"]:
            v = kv.pop(k)
            try:
                CONFIG["costs"][k] = float(v)
            except:
                CONFIG["costs"][k] = v
    # Filters/session/misc
    if "min_adv_inr" in kv: CONFIG["filters"]["min_adv_inr"] = float(kv.pop("min_adv_inr"))
    if "max_spread_pct" in kv: CONFIG["filters"]["max_spread_pct"] = float(kv.pop("max_spread_pct"))
    if "band_guard" in kv: CONFIG["filters"]["band_guard"] = kv.pop("band_guard").lower() in ["1","true","on","yes"]
    if "tick_size" in kv: CONFIG["tick_size"] = float(kv.pop("tick_size"))
    if "valid_minutes" in kv: CONFIG["valid_minutes"] = int(kv.pop("valid_minutes"))
    if "session" in kv: CONFIG["session"] = kv.pop("session")
    if "square_off_time" in kv: CONFIG["square_off_time"] = kv.pop("square_off_time")

    SCHEDULE["autoscan"] = True
    RUNTIME["autoscan"] = "on"
    sched_json = json.dumps(SCHEDULE, indent=2)
    msg = (f"<b>{ist_iso()}</b>\n<b>AUTOSCAN ON</b> — Scanning {len(CONFIG['universe'])} symbols on {','.join(SCHEDULE['timeframes'])}. "
           f"Window {SCHEDULE['market_window']['start']}-{SCHEDULE['market_window']['end']} IST.\n"
           f"Digest: {SCHEDULE['digest']['interval_minutes']}m; Throttle: {SCHEDULE['throttle']['max_per_min']}/min, "
           f"{SCHEDULE['throttle']['max_per_symbol']}/symbol.\n"
           f"<i>{COMPLIANCE}</i>\n"
           f"<pre><code>{sched_json}</code></pre>")
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text("Usage: /universe TCS,RELIANCE,HDFCBANK,NIFTYBEES")
        return
    syms = " ".join(context.args).replace(" ","")
    CONFIG["universe"] = [s for s in syms.split(",") if s]
    RUNTIME["universe"] = CONFIG["universe"]
    await update.message.reply_text(f"Universe set: {', '.join(CONFIG['universe'])}")

async def cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    kv = parse_kv(context.args)
    if "tf" in kv: SCHEDULE["timeframes"] = [t.strip() for t in kv["tf"].split(",") if t.strip()]
    if "window" in kv:
        try:
            start,end = kv["window"].split("-")
            SCHEDULE["market_window"]["start"] = start
            SCHEDULE["market_window"]["end"] = end
        except: pass
    if "digest" in kv:
        try:
            SCHEDULE["digest"]["interval_minutes"] = int(kv["digest"].replace("m",""))
            SCHEDULE["digest"]["enabled"] = True
        except: pass
    await update.message.reply_text(f"Schedule updated.\n<pre><code>{json.dumps(SCHEDULE, indent=2)}</code></pre>", parse_mode="HTML")

async def cmd_autoscan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    mode = context.args[0].lower() if context.args else "on"
    on = mode == "on"
    SCHEDULE["autoscan"] = on
    RUNTIME["autoscan"] = "on" if on else "off"
    await update.message.reply_text(f"AUTOSCAN {'ON' if on else 'OFF'}", parse_mode="HTML")

async def cmd_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global DEFAULT_STRATEGY
    if not context.args:
        await update.message.reply_text(f"Usage: /strategy auto|ema|rsi2|donchian\nCurrent: {DEFAULT_STRATEGY}")
        return
    choice = context.args[0].lower()
    if choice not in ["auto","ema","rsi2","donchian"]:
        await update.message.reply_text("Invalid. Use auto|ema|rsi2|donchian")
        return
    DEFAULT_STRATEGY = choice
    await update.message.reply_text(f"Strategy set: {DEFAULT_STRATEGY}")

async def cmd_throttle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kv = parse_kv(context.args)
    if "max_per_min" in kv: SCHEDULE["throttle"]["max_per_min"] = int(kv["max_per_min"])
    if "max_per_symbol" in kv: SCHEDULE["throttle"]["max_per_symbol"] = int(kv["max_per_symbol"])
    await update.message.reply_text(f"Throttle updated: {SCHEDULE['throttle']}", parse_mode="HTML")

async def cmd_quiet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /quiet on|off [09:15-09:45]")
        return
    on = context.args[0].lower() == "on"
    if on and len(context.args) > 1 and "-" in context.args[1]:
        st, en = context.args[1].split("-")
        SCHEDULE["quiet_windows"] = [{"start":st, "end":en}]
    else:
        SCHEDULE["quiet_windows"] = [] if not on else SCHEDULE["quiet_windows"]
    await update.message.reply_text(f"Quiet windows: {SCHEDULE['quiet_windows']}", parse_mode="HTML")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    snap = {
        "config": CONFIG,
        "schedule": SCHEDULE,
        "runtime": {**RUNTIME, "ist_ts": ist_iso()}
    }
    await update.message.reply_text(f"<pre><code>{json.dumps(snap, indent=2)}</code></pre>", parse_mode="HTML")

async def cmd_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await bind_chat(update)
    if not context.args:
        await update.message.reply_text("Usage: /alert SYMBOL [tf] [session] [strategy]\nExample: /alert TCS 15m intraday ema")
        return
    sym = context.args[0].upper()
    tf = (context.args[1] if len(context.args)>=2 else CONFIG["timeframe"]).upper()
    sess = (context.args[2] if len(context.args)>=3 else CONFIG["session"]).lower()
    strat = (context.args[3] if len(context.args)>=4 else DEFAULT_STRATEGY).lower()
    try:
        lv = compute_levels(sym, tf, sess, CONFIG, strat)
    except Exception as e:
        await update.message.reply_text(f"Cannot compute alert: {e}")
        return
    html, payload = build_alert_html(lv, CONFIG)
    await update.message.reply_text(html, parse_mode="HTML", reply_markup=tip_keyboard(), disable_web_page_preview=True)

# ----------------------- BACKTEST ENGINE (D only) ------------------
class BTResult:
    def __init__(self, metrics: dict, trades: pd.DataFrame):
        self.metrics = metrics
        self.trades = trades

def compute_levels_daily_row(row_idx: int, df: pd.DataFrame, C: dict, strategy: str) -> Optional[Tuple[float,float,float]]:
    close = df["Close"].iloc[:row_idx+1]
    high = df["High"].iloc[:row_idx+1]
    low  = df["Low"].iloc[:row_idx+1]

    ema_fast = ema(close, C["signal"]["params"].get("ema_fast",20))
    ema_slow = ema(close, C["signal"]["params"].get("ema_slow",50))
    atr14 = atr(high, low, close, 14)
    tick = C.get("tick_size", 0.05)

    last_close = float(close.iloc[-1])
    last_high  = float(high.iloc[-1])
    last_ema_f = float(ema_fast.iloc[-1])
    last_ema_s = float(ema_slow.iloc[-1])
    last_atr   = float(atr14.iloc[-1])

    st = strategy
    if st == "auto":
        if last_ema_f > last_ema_s and last_close <= last_ema_f:
            st = "ema"
        elif float(rsi2(close).iloc[-1]) <= C["signal"]["params"].get("rsi2_th",10):
            st = "rsi2"
        else:
            st = "donchian"

    k = C["sl"]["params"].get("atr_k", 2.5)
    R = C["tp"]["params"].get("r", 2.0)

    if st == "ema":
        raw_entry = max(last_high, last_close) * 1.001
    elif st == "rsi2":
        sma200 = float(sma(close, 200).iloc[-1])
        if np.isnan(sma200) or last_close < sma200:
            return None
        raw_entry = last_close * 1.001
    elif st == "donchian":
        n = int(C["signal"]["params"].get("donchian_n", 20))
        don_hi = high.rolling(n).max().iloc[-1]
        if np.isnan(don_hi): return None
        raw_entry = float(don_hi) + 0.01
    else:
        return None

    raw_sl = raw_entry - k * last_atr
    raw_tp = raw_entry + R * (raw_entry - raw_sl)

    entry = round_tick(raw_entry, tick)
    sl    = round_tick(raw_sl, tick)
    tp    = round_tick(raw_tp, tick)
    if not (sl < entry < tp): return None
    return (entry, sl, tp)

def backtest_daily(symbol: str, start: str, end: str, C: dict, strategy: str="auto", capital_start: float=100000.0):
    df = fetch_daily(symbol, start, end)
    df = df.dropna().copy()
    if len(df) < 260:
        raise RuntimeError("Need at least ~1y of daily data for robust backtest.")

    tick = C.get("tick_size", 0.05)
    cash = capital_start
    pos_qty = 0
    pos_entry = None
    pos_sl = None
    pos_tp = None
    trades = []

    for i in range(60, len(df)-1):  # start when indicators stable
        nextbar = df.iloc[i+1]
        n_dt = df.index[i+1].date()

        if pos_qty == 0:
            lv = compute_levels_daily_row(i, df, C, strategy)
            if lv:
                entry_px, sl_px, tp_px = lv
                px_open = float(nextbar["Open"])
                entry_fill = round_tick(max(entry_px, px_open), tick)  # conservative: stop or better
                per_share_cost = cost_per_share(entry_fill, entry_fill, C)
                qty = int(cash // entry_fill)
                if qty > 0:
                    pos_qty = qty
                    pos_entry = entry_fill
                    pos_sl = sl_px
                    pos_tp = tp_px
                    cost_entry_total = per_share_cost * qty / 2.0  # approx half on entry
                    cash -= qty * entry_fill + cost_entry_total
                    trades.append({"date": str(n_dt), "action":"BUY", "price": entry_fill, "qty": qty, "cash": cash})
        else:
            day_high = float(nextbar["High"])
            day_low  = float(nextbar["Low"])
            exit_px = None
            reason = None
            if day_low <= pos_sl:
                exit_px = round_tick(pos_sl, tick)
                reason = "SL"
            elif day_high >= pos_tp:
                exit_px = round_tick(pos_tp, tick)
                reason = "TP"
            if exit_px is not None:
                per_share_cost = cost_per_share(pos_entry, exit_px, C)
                pnl_gross = (exit_px - pos_entry) * pos_qty
                pnl_net = pnl_gross - per_share_cost * pos_qty
                cash += pos_qty * exit_px - (per_share_cost * pos_qty / 2.0)  # approx half on exit
                trades.append({"date": str(n_dt), "action":reason, "price": exit_px, "qty": -pos_qty,
                               "pnl_net": pnl_net, "cash": cash})
                pos_qty = 0
                pos_entry = pos_sl = pos_tp = None

    if pos_qty > 0:
        last_close = float(df.iloc[-1]["Close"])
        exit_px = round_tick(last_close, tick)
        per_share_cost = cost_per_share(pos_entry, exit_px, C)
        pnl_gross = (exit_px - pos_entry) * pos_qty
        pnl_net = pnl_gross - per_share_cost * pos_qty
        cash += pos_qty * exit_px - (per_share_cost * pos_qty / 2.0)
        trades.append({"date": str(df.index[-1].date()), "action":"EOD", "price": exit_px, "qty": -pos_qty,
                       "pnl_net": pnl_net, "cash": cash})

    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        raise RuntimeError("No trades generated in backtest window.")

    equity = trades_df["cash"]
    total_ret = (equity.iloc[-1] - capital_start) / capital_start
    days = max(1, (df.index[-1].date() - df.index[60].date()).days)
    cagr = (1 + total_ret) ** (365.25 / days) - 1 if days > 0 else 0.0

    pnl_series = trades_df.get("pnl_net", pd.Series(dtype=float)).fillna(0.0)
    daily_ret = pnl_series / capital_start
    if len(daily_ret) < 2:
        sharpe = sortino = vol = 0.0
    else:
        vol = float(np.std(daily_ret, ddof=1)) * math.sqrt(252)
        downside = np.std(np.minimum(daily_ret, 0), ddof=1) * math.sqrt(252)
        sharpe = float(np.mean(daily_ret) / (np.std(daily_ret, ddof=1) + 1e-9)) * math.sqrt(252)
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
        "capital_start": round(capital_start, 2),
        "capital_end": round(float(equity.iloc[-1]), 2),
        "TotalReturn_pct": round(total_ret * 100, 2),
        "CAGR_pct": round(cagr * 100, 2),
        "Vol_ann_pct": round(vol * 100, 2),
        "Sharpe": round(sharpe, 2),
        "Sortino": round(sortino, 2),
        "MaxDD_pct": round(maxdd * 100, 2),
        "Trades": int((trades_df["action"].isin(["TP","SL","EOD"])).sum()),
        "WinRate_pct": round(win_rate * 100, 2),
        "ProfitFactor": round(profit_factor, 2)
    }
    return BTResult(metrics, trades_df)

def format_bt_html(res: BTResult) -> str:
    ts = ist_iso()
    m = res.metrics
    tbl = (
        f"<b>{ts}</b>\n"
        f"<b>BACKTEST • {m['symbol']} • D</b>\n"
        f"Period: {m['period']} • Start ₹{m['capital_start']:.2f} → End ₹{m['capital_end']:.2f}\n"
        f"TotalReturn {m['TotalReturn_pct']}% • CAGR {m['CAGR_pct']}% • Vol {m['Vol_ann_pct']}% • Sharpe {m['Sharpe']} • Sortino {m['Sortino']}\n"
        f"MaxDD {m['MaxDD_pct']}% • Trades {m['Trades']} • WinRate {m['WinRate_pct']}% • PF {m['ProfitFactor']}\n"
        f"<i>{COMPLIANCE}</i>\n"
        f"<pre><code>{json.dumps(m, indent=2)}</code></pre>"
    )
    return tbl

async def cmd_backtest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /backtest SYMBOL start=YYYY-MM-DD end=YYYY-MM-DD [strategy=auto] [capital=100000]")
        return
    sym = context.args[0].upper()
    kv = parse_kv(context.args[1:])
    start = kv.get("start")
    end = kv.get("end")
    strat = kv.get("strategy", DEFAULT_STRATEGY)
    capital = float(kv.get("capital", 100000))
    if not start or not end:
        await update.message.reply_text("Provide start and end. Example: /backtest TCS start=2018-01-01 end=2025-01-01 strategy=ema")
        return
    try:
        res = backtest_daily(sym, start, end, CONFIG, strat, capital_start=capital)
    except Exception as e:
        await update.message.reply_text(f"Backtest failed: {e}")
        return
    await update.message.reply_text(format_bt_html(res), parse_mode="HTML")

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CONFIG, SCHEDULE, RUNTIME, LAST_ALERT_VALID, SYMBOL_COUNT, DEFAULT_STRATEGY
    CONFIG = json.loads(json.dumps(DEFAULT_CONFIG))
    SCHEDULE = json.loads(json.dumps(DEFAULT_SCHEDULE))
    RUNTIME.update({
        "autoscan":"off","scan_status":"idle","counters":{"alerts_sent":0,"suppressed":0,"deduped":0,"skipped":0},
        "universe":CONFIG["universe"],"chat_id": ADMIN_CHAT_ID or RUNTIME.get("chat_id")
    })
    LAST_ALERT_VALID.clear(); SYMBOL_COUNT.clear()
    DEFAULT_STRATEGY = "auto"
    await update.message.reply_text("Reset to factory defaults.")

# ----------------------- APP / JOBS (SYNC RUN) ---------------------
def setup_jobs(app):
    jq: JobQueue = app.job_queue
    jq.run_repeating(scheduler_job, interval=1.0, first=1.0, name="scheduler")
    jq.run_repeating(
        digest_job,
        interval=60 * max(1, SCHEDULE["digest"]["interval_minutes"]),
        first=60 * SCHEDULE["digest"]["interval_minutes"],
        name="digest",
    )
    jq.run_repeating(eod_digest_job, interval=60, first=10, name="eod")

def build_app():
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN secret.")
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("configure", cmd_configure))
    app.add_handler(CommandHandler("universe", cmd_universe))
    app.add_handler(CommandHandler("schedule", cmd_schedule))
    app.add_handler(CommandHandler("autoscan", cmd_autoscan))
    app.add_handler(CommandHandler("strategy", cmd_strategy))
    app.add_handler(CommandHandler("throttle", cmd_throttle))
    app.add_handler(CommandHandler("quiet", cmd_quiet))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("alert", cmd_alert))
    app.add_handler(CommandHandler("backtest", cmd_backtest))
    app.add_handler(CommandHandler("reset", cmd_reset))
    setup_jobs(app)
    return app

if __name__ == "__main__":
    app = build_app()
    log.info("Starting IndiQuant AutoScan (polling)…")
    app.run_polling()  # synchronous; safe on Replit
