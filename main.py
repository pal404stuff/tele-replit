# Setup:
# 1. In Replit Secrets: TELEGRAM_BOT_TOKEN, ADMIN_CHAT_ID
# 2. requirements.txt:
#    python-telegram-bot==21.4
#    pandas==2.2.2
#    numpy==1.26.4
#    jugaad-data==0.41
#    requests==2.32.3
#    python-dotenv==1.0.1

import os, uuid, pickle, logging, asyncio
from datetime import datetime, timedelta, date, time as dtime
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
from jugaad_data.nse import stock_df, NSELive
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, PicklePersistence
)

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
IST = ZoneInfo("Asia/Kolkata")
BOT_NAME = "IndiQuant Apex Ultimate"
VERSION = "v5.0.0"
COMPLIANCE = "Educational only — not investment advice. Not SEBI-registered. Markets carry risk."
STATE_FILE = "bot_state.pkl"

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("indiquant.ultimate")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", None)

DEFAULT_STATE = {
    "config": {
        "universe": ["RELIANCE", "TCS", "INFY", "NIFTYBEES"],
        "timeframe": "D",
        "session": "swing",
        "square_off_time": "15:20",
        "sl_atr_k": 2.5,
        "tp_multi": {"enable": True, "tp1_r": 1.5, "tp2_r": 3.0},
        "filters": {"min_adv_inr": 5e7, "min_rr": 1.5}
    },
    "portfolio": {"active": {}, "closed": []},
    "runtime": {"nse_holidays": []}
}

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def now_ist(): return datetime.now(tz=IST)
def ist_iso(): return now_ist().isoformat()

def round_tick(p, tick=0.05):
    return round(p / tick) * tick

# -----------------------------------------------------------------------------
# DATA FETCH (jugaad-data)
# -----------------------------------------------------------------------------
async def fetch_history(symbol, tf="D", days=365):
    try:
        if tf.upper() == "D":
            df = stock_df(symbol=symbol, from_date=date.today()-timedelta(days=days),
                          to_date=date.today(), series="EQ")
            if df is None or df.empty: return None
            df = df.rename(columns=str.title)
            return df
        else:
            return None
    except Exception as e:
        log.error(f"Data fetch failed {symbol}: {e}")
        return None

def live_quote(symbol):
    try:
        n = NSELive()
        q = n.stock_quote(symbol)
        return q.get("priceInfo", {})
    except Exception as e:
        log.error(f"Live quote failed {symbol}: {e}")
        return {}

# -----------------------------------------------------------------------------
# STRATEGY ENGINE
# -----------------------------------------------------------------------------
def evaluate_signal(df: pd.DataFrame, benchmark_df: pd.DataFrame, cfg: dict):
    if df is None or len(df) < 100: return None
    last = df.iloc[-1]
    prev = df.iloc[-2]

    # Relative Strength filter
    if benchmark_df is not None and not benchmark_df.empty:
        stock_rel = (df["Close"] / df["Close"].iloc[0])
        bench_rel = (benchmark_df["Close"] / benchmark_df["Close"].iloc[0])
        rs = (stock_rel / bench_rel).iloc[-55:].rank(pct=True).iloc[-1] * 100
        if rs < 70: return None

    # Volume confirmation
    vol_ma = df["Volume"].rolling(20).mean().iloc[-1]
    if last["Volume"] < vol_ma: return None

    # Example breakout: close > prev high
    if last["Close"] <= prev["High"]: return None

    entry = round_tick(last["Close"] * 1.001)
    atr = (df["High"]-df["Low"]).rolling(14).mean().iloc[-1]
    sl = round_tick(entry - cfg["sl_atr_k"]*atr)
    tp1 = round_tick(entry + cfg["tp_multi"]["tp1_r"]*(entry-sl))
    tp2 = round_tick(entry + cfg["tp_multi"]["tp2_r"]*(entry-sl))
    rr = (tp2-entry)/(entry-sl)
    if rr < cfg["filters"]["min_rr"]: return None

    return {"symbol": df.name if hasattr(df, "name") else "?", "entry": entry,
            "sl": sl, "tp1": tp1, "tp2": tp2, "rr": rr}

# -----------------------------------------------------------------------------
# COMMAND HANDLERS
# -----------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"{BOT_NAME} {VERSION}\n{COMPLIANCE}")

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = context.bot_data
    pf = state.get("portfolio", {})
    active = pf.get("active", {})
    closed = pf.get("closed", [])
    txt = f"<b>Portfolio</b>\nActive: {len(active)}\nClosed: {len(closed)}"
    await update.message.reply_html(txt)

# -----------------------------------------------------------------------------
# APP
# -----------------------------------------------------------------------------
def build_app():
    persistence = PicklePersistence(filepath=STATE_FILE)
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).persistence(persistence).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("portfolio", portfolio))
    return app

if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN")
    app = build_app()
    log.info(f"Starting {BOT_NAME} ({VERSION}) …")
    try:
        asyncio.run(app.run_polling())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(app.run_polling())
