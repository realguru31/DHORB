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
    with st.form("login"):
        p = st.text_input("Password", type="password")
        go_in = st.form_submit_button("Log in")
    if go_in:
        if p == pw:
            st.session_state.ok = True
            st.rerun()
        st.error("Wrong password")
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

    # ---------- entry credit sampling + P&L ----------
    if tr.get("date") == r.get("date") and tr.get("short") is not None:
        n = E.contracts(float(st.secrets.get("ACCOUNT_SIZE", 0) or 0), tr.get("entry") or tr.get("credit") or 0.4)
        label = "Open trade" if tr.get("go") else "Skipped signal (tracked for comparison only)"
        st.subheader(label)
        if tr.get("samples") is None:
            st.write(f"Sampling the spread every 5 s for 1 minute after {tr.get('sig_time')} …")
        else:
            st.write(f"Entry credit **${f(tr.get('entry'))}** (most common of {len(tr['samples'])} readings, "
                     f"range ${f(tr.get('smin'))}–${f(tr.get('smax'))}, sampled to {tr.get('sampled_at')}) · "
                     f"{f(tr['short'],0)} / {f(tr['long'],0)} {'P' if tr['side']=='put' else 'C'}")
            rows = tr.get("pnl") or []
            if rows:
                last = rows[-1]
                a, b, c = st.columns(3)
                a.metric(f"Ours · P&L per contract ({last['t']})", f"${f(last.get('ours_pnl'),0)}",
                         help=f"spread now ${f(last.get('ours'))} vs entry ${f(tr.get('entry'))}")
                mx = (tr.get("entry") or 0) * 100
                b.metric("% of max profit", f"{f(100 * last['ours_pnl'] / mx, 0)}%" if mx and last.get("ours_pnl") is not None else "–")
                c.metric(f"× {n} contracts" if n else "SPX − short strike",
                         f"${f(last['ours_pnl'] * n, 0)}" if n and last.get("ours_pnl") is not None else
                         f(abs((last.get('spx') or 0) - tr['short']), 1))
            else:
                st.caption("first P&L reading 5 minutes after sampling")
        # QuantGlide comparison (does not affect our trade)
        if tr.get("qg_cands") is not None:
            with st.expander(f"QuantGlide comparison – delta {w.cfg['qg_lo']:.2f}–{w.cfg['qg_hi']:.2f} (does not change our trade)", expanded=True):
                lo, hi = st.columns(2)
                nlo = lo.number_input("his delta low", 0.01, 0.50, float(w.cfg["qg_lo"]), 0.01, key="qglo")
                nhi = hi.number_input("his delta high", 0.01, 0.50, float(w.cfg["qg_hi"]), 0.01, key="qghi")
                if (nlo, nhi) != (w.cfg["qg_lo"], w.cfg["qg_hi"]):
                    w.set_qg_range(nlo, nhi)
                cands = tr.get("qg_cands") or []
                if cands:
                    st.dataframe(pd.DataFrame(cands).rename(columns=dict(short="short", long="long", delta="Δ (median)", credit="credit (most common)", cmin="min", cmax="max")),
                                 hide_index=True, width="stretch")
                    st.write(f"Likely pick (Δ nearest {w.cfg['qg_target']}): **{f(tr.get('qg_track_short'),0)} / {f(tr.get('qg_track_long'),0)}** · "
                             f"entry ${f(tr.get('qg_track_entry'))} · Δ {f(tr.get('qg_track_delta'),3)}")
                else:
                    st.write("no spread in that delta range")
        rows = tr.get("pnl") or []
        if rows:
            st.dataframe(pd.DataFrame(rows).rename(columns=dict(t="time", spx="SPX", ours="ours mid", ours_pnl="ours P&L $", qg="QG mid", qg_pnl="QG P&L $")),
                         hide_index=True, width="stretch")



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

# ---------- research: 0.08-0.12 delta spreads by minute (from 10:15 ET) + day ZIP ----------
from datetime import datetime
from zoneinfo import ZoneInfo
today_et = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
with st.expander("Spreads by minute (0.08–0.12 Δ, from 10:15 ET) – research", expanded=False):
    rows = w.rec_rows(today_et) if hasattr(w, "rec_rows") else []
    if rows:
        df = pd.DataFrame(rows)
        c1, c2 = st.columns(2)
        side = c1.selectbox("side", ["all", "put", "call"], key="rec_side")
        ks = sorted(df.short.unique())
        k = c2.selectbox("short strike", ["all"] + [f"{x:.0f}" for x in ks], key="rec_k")
        if side != "all":
            df = df[df.side == side]
        if k != "all":
            df = df[df.short == float(k)]
        st.dataframe(df, hide_index=True, width="stretch", height=320)
    else:
        st.write("no rows yet (recording starts 10:15 ET and runs until 30 min after our signal, or 13:30)")
if hasattr(w, "day_zip"):
    st.download_button(f"⬇ Download today's data (ZIP) – {today_et}", data=w.day_zip(today_et),
                       file_name=f"dh_orb_{today_et}.zip", mime="application/zip")


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
