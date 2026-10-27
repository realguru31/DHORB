# Changelog – DH ORB-H bot

## 1.2.1 – 2026-10-27
- Fix: dashboard crashed after a code update because the running bot was still the old version
  (now shows a "reboot to switch versions" banner instead)
- Replaced deprecated use_container_width

## 1.2.0 – 2026-10-27
- TradingView Lightweight Charts day chart (candles, range, triggers, signal, strikes, result) with day picker
- Dark theme across the app
- Version shown on the dashboard and in the morning Telegram message

## 1.1.0 – 2026-10-27
- Warning in the signal alert when the VIX1D value is more than 2 minutes old
- Loads 60 calendar days of 1-minute history (for the full 20-day ratio)

## 1.0.0 – 2026-10-27
- First release: tastytrade live feed, DH ORB-H rules, live mid credit check ($0.30 minimum, one strike closer on thin calls),
  QuantGlide-style delta strike, Telegram alerts (start, 10:31 levels, signal, 13:30 no trade, 16:00 result), Streamlit dashboard
