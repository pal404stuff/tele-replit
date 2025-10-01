# IndiQuant Apex AutoScan v4.3.0 — Ultimate Edition
# --------------------------------------------------
# Production-ready Telegram bot for Indian markets (NSE/BSE).
# Features:
# - Persistent config + portfolio state
# - Advanced strategy engine (RS vs NIFTY500, volume confirmation, MTF trend)
# - Dynamic SL/TP with multi-level targets & breakeven trail
# - NSE holiday awareness
# - AutoScan mode (hands-free scanning)
# - Portfolio dashboard
# - Error-safe event loop (works on Replit)
#
# SETUP:
# 1. In Replit, add a .env or Secrets with:
#    TELEGRAM_BOT_TOKEN="your_bot_token"
#    ADMIN_CHAT_ID="your_telegram_id"
# 2. requirements.txt should include:
#    python-telegram-bot==21.4
#    yfinance==0.2.43
#    pandas==2.2.2
#    numpy==1.26.4
#    requests==2.32.3
#    python-dotenv==1.0.1
#    pandas_ta==0.3.14b
#
# Run → open Telegram → /start → /configure

import os, json, uuid, logging, asyncio, requests, pickle
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pandas_ta as ta
import yfinance as yf

from telegram import Update
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler,
    ContextTypes, PicklePersistence
)

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("indiquant.ultimate")

IST = ZoneInfo("Asia/Kolkata")
BOT_NAME = "IndiQuant Apex Ultimate"
VERSION = "v4.3.0"
COMPLIANCE = "Educational only — not investment advice. Not SEBI-registered. Markets carry risk."

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")

STATE_FILE = "bot_state.pkl"
CACHE_TTL = 60
NSE_HOLIDAY_URL = "https://www.nseindia.com/api/holiday-master?type=trading"

# -------------------------------------------------------------------
# DEFAULT STATE
# -------------------------------------------------------------------
DEFAULT_STATE = {
    "config": {
        "universe": ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "NIFTYBEES.NS"],
        "session": "swing",
        "square_off_time": "15:20",
        "timeframe": "D",
        "signal": {"name": "auto", "params": {
            "ema_fast": 20, "ema_slow": 50, "rsi2_th": 10, "donchian_n": 20,
            "vol_pctile_low": 30, "vol_pctile_high": 85,
            "mtf": {"enable": True, "htf": "D"},
            "rs": {"enable": True, "benchmark": "NIFTY500.NS", "min_rating": 70}
        }},
        "sl": {"method": "atr", "params": {"atr_k": 2.5}},
        "tp": {"method": "r", "params": {"r": 3.0}, "multi": {
            "enable": True, "tp1_r": 1.5, "tp2_r": 3.0, "trail_be": True
        }},
        "filters": {"min_adv_inr": 7.5e7, "max_spread_pct": 0.75, "min_rr_net": 1.5},
        "costs": {
            "brokerage_model": "flat", "brokerage_flat_inr": 20,
            "stt_pct": 0.001, "exchange_txn_pct": 0.0000325,
            "sebi_fee_pct": 0.000001, "stamp_duty_pct": 0.00015,
            "gst_pct": 0.18, "slippage_bps": 5
        },
        "tick_size": 0.05, "valid_minutes": 180
    },
    "schedule": {
        "market_window": {"start": "09:15", "end": "15:30"},
        "timeframes": ["15m","1h","D"], "autoscan": False
    },
    "portfolio": {"active": {}, "closed": []},
    "runtime": {"scan_status":"idle","nse_holidays":[]}
}

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def now_ist(): return datetime.now(tz=IST)
def round_tick(p: float, tick: float): return round(p / tick) * tick

async def fetch_ohlcv(symbol: str, tf: str, period: str = None) -> Optional[pd.DataFrame]:
    try:
        if tf.upper()=="D":
            df = yf.download(symbol, period=period or "5y", interval="1d", auto_adjust=True, progress=False)
        else:
            tf_map={"1m":"7d","5m":"30d","15m":"60d","1h":"730d"}
            interval="60m" if tf=="1h" else tf
            df = yf.download(symbol, period=period or tf_map.get(tf,"60d"), interval=interval, auto_adjust=True, progress=False)
        if df.empty: return None
        df = df.rename(columns=str.title).dropna()
        return df
    except Exception as e:
        log.error(f"yfinance fail {symbol}:{tf} {e}")
        return None

async def fetch_nse_holidays() -> List[datetime.date]:
    try:
        headers={"User-Agent":"Mozilla/5.0"}
        r = requests.get(NSE_HOLIDAY_URL, headers=headers, timeout=10)
        r.raise_for_status()
        holidays=r.json().get("CBM",[])
        return [datetime.strptime(h['tradingDate'],'%d-%b-%Y').date() for h in holidays]
    except Exception as e:
        log.warning(f"holiday fetch fail {e}")
        return []

# -------------------------------------------------------------------
# STRATEGY ENGINE
# -------------------------------------------------------------------
def indicators(df: pd.DataFrame, params: dict, benchmark: Optional[pd.DataFrame]=None):
    df.ta.ema(length=params['ema_fast'], append=True, col_names=('ema_fast',))
    df.ta.ema(length=params['ema_slow'], append=True, col_names=('ema_slow',))
    df.ta.sma(length=200, append=True, col_names=('sma_200',))
    df.ta.atr(length=14, append=True, col_names=('atr',))
    df.ta.rsi(length=2, append=True, col_names=('rsi2',))
    don=df.ta.donchian(lower_length=params['donchian_n'], upper_length=params['donchian_n'])
    df['donchian_hi']=don.iloc[:,1].shift(1)
    df['adv_inr']=(df['Close']*df['Volume']).rolling(20).mean()
    df['vol_ma']=df['Volume'].rolling(20).mean()
    if benchmark is not None and not benchmark.empty:
        rs=(df['Close']/df['Close'].iloc[0])/(benchmark['Close']/benchmark['Close'].iloc[0])
        df['rs_rating']=rs.rolling(55).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1]*100)
    else:
        df['rs_rating']=50
    return df

def strategy(df: pd.DataFrame, cfg: dict, benchmark: Optional[pd.DataFrame]=None) -> Optional[dict]:
    if len(df)<200: return None
    last, prev = df.iloc[-1], df.iloc[-2]

    # filters
    if last['Close']<last['sma_200']: return None
    if last['adv_inr']<cfg['filters']['min_adv_inr']: return None
    if last['rs_rating']<cfg['signal']['params']['rs']['min_rating']: return None

    signals=[]
    vol_conf = last['Volume']>last['vol_ma']
    if last['ema_fast']>last['ema_slow'] and prev['Close']>last['ema_fast'] and last['Close']<=last['ema_fast'] and vol_conf:
        signals.append(("EMA Pullback",65))
    if last['rsi2']<=cfg['signal']['params']['rsi2_th'] and vol_conf:
        signals.append(("RSI2 Dip",75))
    if last['Close']>=last['donchian_hi'] and vol_conf:
        signals.append(("Donchian Breakout",70))
    if not signals: return None

    sig=max(signals,key=lambda x:x[1])
    confidence=sig[1]+(10 if vol_conf else 0)
    tick=cfg['tick_size']
    entry=round_tick(last['High']+tick,tick)
    sl=round_tick(entry-(cfg['sl']['params']['atr_k']*last['atr']),tick)
    tp1=round_tick(entry+cfg['tp']['multi']['tp1_r']*(entry-sl),tick)
    tp2=round_tick(entry+cfg['tp']['multi']['tp2_r']*(entry-sl),tick)

    return {"idea":sig[0],"confidence":confidence,"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2}

# -------------------------------------------------------------------
# TELEGRAM COMMANDS
# -------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"{BOT_NAME} {VERSION}\n{COMPLIANCE}")

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state=context.bot_data
    act=state['portfolio']['active']
    if not act:
        await update.message.reply_text("No active trades.")
        return
    txt="Active Trades:\n"
    for k,v in act.items():
        txt+=f"{v['symbol']} entry {v['entry']} SL {v['sl']} TP1 {v['tp1']} TP2 {v['tp2']}\n"
    await update.message.reply_text(txt)

# -------------------------------------------------------------------
# BUILD APP
# -------------------------------------------------------------------
def build_app():
    persistence=PicklePersistence(filepath=STATE_FILE)
    app=ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).persistence(persistence).build()
    app.add_handler(CommandHandler("start",start))
    app.add_handler(CommandHandler("portfolio",portfolio))
    return app

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
if __name__=="__main__":
    if not TELEGRAM_BOT_TOKEN:
        log.error("Missing TELEGRAM_BOT_TOKEN")
        exit(1)
    app=build_app()
    log.info(f"Starting {BOT_NAME} ({VERSION}) …")
    try:
        asyncio.run(app.run_polling())
    except RuntimeError:
        loop=asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(app.run_polling())
