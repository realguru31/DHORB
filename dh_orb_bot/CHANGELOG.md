# Changelog – DH ORB-H bot

## 1.3.0 – 2026-10-27
- After a trigger: our spread is sampled every 5 s for 1 minute; entry credit = most common reading (to $0.05),
  with min/max shown
- P&L of our spread every 5 minutes until 16:00 (dashboard), per contract, % of max profit, × contracts
- QuantGlide comparison (display only): spreads with short delta in his quoted range (default 0.08–0.12, editable),
  sampled in the same minute; likely pick = delta nearest 0.10, its P&L tracked next to ours
- Login form with a Log in button
- Today's trade is saved, so P&L tracking resumes after a reboot
- Our trade decision and strikes are unchanged

## 1.2.3 – 2026-10-27
- Recent days table: skip decisions now show the actual reason (e.g. "skip – VIX1D +2.3", "skip – ratio 0.87 ≤ 1.2")
  instead of only the ratio

## 1.2.2 – 2026-10-27
- QuantGlide-style comparison strike: delta limit 0.095 → 0.105 (his Oct 27 trade 7680/7665 was at 0.101 delta).
  Display only – the traded strike is unchanged (1.16 × expected move).

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
