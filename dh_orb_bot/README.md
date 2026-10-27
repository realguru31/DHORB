# DH ORB-H bot

SPX 0DTE breakout credit-spread signals with Telegram alerts and a Streamlit dashboard.
It runs the same rules as the DH ORB-H TradingView indicator and the backtest, but on live tastytrade data
(SPX, VIX1D and VIX 1-minute bars plus the 0DTE option chain).

## What it sends to Telegram
| When | Message |
|---|---|
| First check after 9:00 ET | 🤖 bot running, days of history loaded |
| 10:31 | 📏 range, triggers, ratio threshold for the day |
| Signal (first break by 13:30) | 🟢 TRADE or ⚪ SKIP: strikes, live mid credit, delta, ratio checks, QuantGlide-style strike, size |
| 13:30, no break | ⏹ no trade today |
| 16:00 | 🏁 win or loss (or what a skipped trade would have done) |

## Files
- `engine.py`: strategy rules (pure functions, checked against the backtest: same signals and ratios on every 2026 day)
- `feed.py`: tastytrade OAuth, option chain, DXLink websocket
- `worker.py`: background bot (can also run on its own: `python worker.py`)
- `app.py`: Streamlit dashboard; starts the bot once per server process

## Deploy on Streamlit Community Cloud
1. Push this folder to a **private** GitHub repo (`.gitignore` already keeps secrets and state out).
2. On share.streamlit.io, create an app from the repo with main file `app.py`.
3. Under App settings → Secrets, paste your keys in the format of `.streamlit/secrets.toml.example`.
4. Open the app once each morning before 9:30 ET so it is awake. The bot keeps running in the background
   while you're away; if the morning 🤖 message doesn't arrive, open the page.

## Run it anywhere else (Colab test, VPS)
Set the environment variables `TT_CLIENT_SECRET TT_REFRESH_TOKEN TG_BOT_TOKEN TG_CHAT_ID` (optionally `ACCOUNT_SIZE`)
and run `python worker.py`.

## Rules
61-bar opening range · trigger 0.055% of the 9:30 open · first 1-min candle beyond it, closing by 13:30 ·
up = bull put, down = bear call · ratio VIX1D / forecast RV > 1.2 (1.0 if VIX opened < 16) · VIX1D rise < 2 ·
VIX open ≤ 35 · no FOMC · short strike 1.16 × remaining expected move, rounded away to 5, 15 wide ·
skip if mid credit < $0.30 (bear call: try one strike closer first) · limit at mid · no stop · hold to close.
