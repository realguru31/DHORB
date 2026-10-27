"""
DH ORB-H – Streamlit dashboard + background Telegram bot.
The bot runs in a background thread started once per server process (st.cache_resource),
so it keeps working even if this browser tab disconnects. Keep the app awake: open it once each morning.
Secrets (Streamlit → App settings → Secrets): TT_CLIENT_SECRET, TT_REFRESH_TOKEN, TG_BOT_TOKEN, TG_CHAT_ID,
optional ACCOUNT_SIZE (for contract sizing) and APP_PASSWORD (protects this page).
"""
import math

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import engine as E
from chart import chart_html
from version import __version__
from worker import Worker

st.set_page_config(page_title="DH ORB-H", page_icon="📈", layout="wide")

# ---------- optional password ----------
pw = st.secrets.get("APP_PASSWORD")
if pw and not st.session_state.get("ok"):
    p = st.text_input("Password", type="password")
    if p == pw:
        st.session_state.ok = True
        st.rerun()
    st.stop()


@st.cache_resource
def get_worker():
    w = Worker(dict(st.secrets))
    w.start()
    return w


w = get_worker()
STALE = getattr(w, "version", None) != __version__
if STALE:
    st.error(f"New code v{__version__} is deployed but the running bot is v{getattr(w, 'version', 'old')}. "
             "Click **Manage app → Reboot app** (bottom right) to switch the bot to the new version.")


def f(x, nd=2):
    return "–" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:,.{nd}f}"


st.title("DH ORB-H · SPX 0DTE breakout spreads")
st.caption(f"v{__version__}")

c1, c2 = st.columns([3, 1])
with c2:
    if st.button("Send test alert"):
        ok = w.telegram("🧪 DH ORB-H test alert from the dashboard")
        st.toast("sent" if ok else "Telegram failed – check secrets")


@st.fragment(run_every="3s")
def live():
    s = w.snapshot()
    r = s.get("today") or {}
    tr = s.get("trade") or {}
    st.caption(f"Feed: {s.get('feed')} · updated {s.get('updated')} ET · history {r.get('hist_days', 0)}/{E.CFG['look_n']} days")

    status = r.get("status", s.get("status"))
    if status == "signal":
        go = tr.get("go") if tr.get("date") == r.get("date") else r.get("gate")
        side = "BULL PUT" if r.get("side") == "put" else "BEAR CALL"
        k = f"{f(tr.get('short') or r.get('short'),0)} / {f(tr.get('long') or r.get('long'),0)}"
        msg = f"{'TRADE' if go else 'SKIP'} · {side} {k} · signal {r.get('sig_time')} · credit ${f(tr.get('credit'))}"
        (st.success if go else st.warning)(msg + ("" if go else f" · {r.get('why') or 'credit below $0.30'}"))
    elif status in ("watching", "building range"):
        st.info(f"{status} …")
    else:
        st.info(status or "–")

    a, b, c, d = st.columns(4)
    a.metric("Range high / low", f"{f(r.get('orH'))} / {f(r.get('orL'))}")
    b.metric("Triggers ▲ / ▼", f"{f(r.get('up'))} / {f(r.get('dn'))}")
    ratio = r.get("ratio") if status == "signal" else r.get("live_ratio")
    c.metric(f"VIX1D / RV (> {r.get('thr', '–')})", f(ratio), help="at the signal minute once a signal fired, live before")
    d.metric("VIX open · VIX1D open", f"{f(r.get('vix_open'),1)} · {f(r.get('v1d_open'))}")

    if status == "watching" and r.get("if_up"):
        st.write(f"If ▲ now: bull put **{f(r['if_up'][0],0)} / {f(r['if_up'][1],0)}** · "
                 f"If ▼ now: bear call **{f(r['if_dn'][0],0)} / {f(r['if_dn'][1],0)}**")
    if status == "signal":
        st.write(f"SPX at signal **{f(r.get('S'))}** · EM {f(r.get('em'),0)} pts · VIX1D rise {f(r.get('jump'),1)} · "
                 f"{'close-confirmed' if r.get('confirmed') else 'wick-only break'}")
        if tr.get("qg_short"):
            st.write(f"QuantGlide-style (Δ ≤ {E.CFG['max_delta']}): **{f(tr['qg_short'],0)} / {f(tr['qg_long'],0)}** · "
                     f"${f(tr.get('qg_credit'))} · Δ {f(tr.get('qg_delta'),3)}")



live()


@st.fragment(run_every="20s")
def day_chart():
    if STALE or not hasattr(w, "available_days"):
        st.info("chart available after reboot")
        return
    days = w.available_days()
    if not days:
        st.info("chart: waiting for data…")
        return
    sel = st.selectbox("Chart", days, index=0, key="chart_day", label_visibility="collapsed")
    data = w.chart_day(sel)
    if data and data["candles"]:
        components.html(chart_html(data, 520), height=530)
    else:
        st.info(f"no bars yet for {sel}")


st.subheader("Chart")
day_chart()


@st.fragment(run_every="30s")
def history_log():
    s = w.snapshot()
    h = s.get("history") or []
    if h:
        st.subheader("Recent days")
        st.dataframe(pd.DataFrame(h)[["date", "signal", "strikes", "decision", "result"]], hide_index=True, width="stretch")
    with st.expander("Bot log"):
        st.code("\n".join(s.get("log", [])[:60]) or "–")


history_log()
st.caption("Rules: 61-bar opening range · 0.055% trigger · first break by 13:30 · ratio > 1.2 (1.0 if VIX < 16) · "
           "VIX1D rise < 2 · VIX ≤ 35 · no FOMC · 1.16× EM strike, 15 wide · min credit $0.30 · hold to close")
