# IndiQuant AutoScan AlertBot

## Overview
This is a Python Telegram bot application for automated stock market scanning and alerting for Indian markets (NSE/BSE). The bot provides real-time alerts for trading setups based on technical analysis.

## Project Architecture
- **Language**: Python 3.11
- **Main Framework**: python-telegram-bot with JobQueue
- **Data Sources**: yfinance for stock data
- **Analysis**: pandas, numpy for technical analysis
- **Scheduling**: APScheduler for automated scanning

## Current State
✅ Python 3.11 installed and configured
✅ All dependencies installed (python-telegram-bot with job-queue, yfinance, pandas, numpy)
✅ Workflow configured to run the bot application
⚠️ **Requires Telegram Bot Token Configuration** - Bot cannot start without proper secrets

## Required Configuration

### Essential Secrets (Required)
To run this bot, you MUST configure the following secret in Replit:

1. **TELEGRAM_BOT_TOKEN** - Create a bot via @BotFather on Telegram and get the token

### Optional Secrets (For Enhanced Features)
- **ADMIN_CHAT_ID** - Chat ID where alerts will be sent by default
- **TIP_UPI_ID** - UPI ID for Indian payment tips (e.g., yourname@okhdfcbank)
- **TIP_BMC** - Buy Me a Coffee URL
- **TIP_KOFI** - Ko-fi URL  
- **TIP_STRIPE** - Stripe Payment Link
- **TIP_RAZORPAY** - Razorpay Payment Link

### How to Configure Secrets
1. Go to Replit Tools → Secrets
2. Add key: `TELEGRAM_BOT_TOKEN`, value: `your_bot_token_from_botfather`
3. Restart the bot workflow

## Features
- **Automated Scanning**: Real-time scanning of configured stocks during market hours
- **Technical Analysis**: EMA crossovers, RSI2, ATR-based stops
- **Cost-Aware Alerts**: Includes brokerage, taxes, and slippage in calculations
- **Risk Management**: Configurable stop-loss and take-profit levels
- **Market Hours**: IST timezone, NSE/BSE market hours (9:15 AM - 3:30 PM)
- **Throttling**: Configurable limits to prevent spam

## Bot Commands
- `/start` - Show help and bind chat for alerts
- `/configure key=value` - Set costs, filters, session parameters
- `/universe SYMBOL1,SYMBOL2` - Set stocks to scan
- `/schedule` - Configure timeframes and market window
- `/autoscan on|off` - Toggle automated scanning
- `/status` - Show current configuration and runtime status
- `/alert SYMBOL` - Manual one-off alert for a symbol
- `/reset` - Reset to factory defaults

## Default Configuration
- **Universe**: TCS, RELIANCE, HDFCBANK, NIFTYBEES
- **Timeframe**: 15-minute bars
- **Session**: Intraday (square-off at 15:20)
- **Market Window**: 09:15 - 15:30 IST
- **Costs**: Placeholder values (must be configured)

## Recent Changes
- 2025-09-29: Initial import and setup in Replit environment
- Fixed Python syntax error in global variable declaration
- Installed job-queue support for scheduling
- Configured workflow for console output

## User Preferences
- This is a fresh import, no specific user preferences documented yet

## Next Steps
1. Configure TELEGRAM_BOT_TOKEN in Replit secrets
2. Test bot functionality with `/start` command
3. Configure trading costs via `/configure` command
4. Set up stock universe and enable autoscan