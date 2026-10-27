"""
Background worker: streams SPX / VIX1D / VIX 1-min bars from tastytrade, runs the DH ORB-H rules every second,
checks live option prices at the signal, and posts to Telegram. Runs in its own thread (Streamlit) or standalone:
    python worker.py        (reads secrets from environment variables or .streamlit/secrets.toml)
"""
import asyncio
import json
import math
import os
import threading
import time
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import requests

import engine as E
from feed import DXLink, TastyAuth, spxw_chain
from version import __version__

ET = ZoneInfo("America/New_York")
STATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state")
os.makedirs(STATE_DIR, exist_ok=True)
SENT_FILE = os.path.join(STATE_DIR, "sent.json")
JOURNAL = os.path.join(STATE_DIR, "journal.jsonl")
HIST_CAL_DAYS = 60          # calendar days of 1-min history to load (20 trading days + spare)
FRESH_MIN = 10              # only alert events younger than this (minutes) – avoids stale alerts after a restart


def f2(x, nd=2):
    return "–" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:.{nd}f}"


class Worker:
    def __init__(self, secrets, cfg=None):
        self.s = secrets
        self.version = __version__
        self.cfg = dict(E.CFG, **(cfg or {}))
        self.auth = TastyAuth(secrets["TT_CLIENT_SECRET"], secrets["TT_REFRESH_TOKEN"])
        self.account = float(secrets.get("ACCOUNT_SIZE", 0) or 0)
        self.lock = threading.Lock()
        self.spx, self.v1d, self.vix = {}, {}, {}      # date -> arrays
        self.quotes, self.greeks = {}, {}
        self.sq_cache = {}
        self.chain, self.chain_date = {}, None
        self.sent = self._load_sent()
        self.state = dict(status="starting", feed="connecting", log=[], today={}, trade={}, history=[], updated=None)
        self.dx = None
        self.loop = None
        self._hist_cache = {}
        self.track_task = None
        self.ladder = {}                 # strike -> [(credit, |delta|), ...] sampled in the minute after the trigger

    # ---------------- persistence / telegram ----------------
    def _load_sent(self):
        try:
            with open(SENT_FILE) as f:
                return set(json.load(f))
        except Exception:
            return set()

    def _mark(self, key):
        self.sent.add(key)
        try:
            with open(SENT_FILE, "w") as f:
                json.dump(sorted(self.sent)[-400:], f)
        except Exception:
            pass

    def log(self, msg):
        line = f"{datetime.now(ET):%m-%d %H:%M:%S}  {msg}"
        with self.lock:
            self.state["log"] = ([line] + self.state["log"])[:200]

    def telegram(self, text):
        try:
            r = requests.post(f"https://api.telegram.org/bot{self.s['TG_BOT_TOKEN']}/sendMessage", timeout=15,
                              data={"chat_id": self.s["TG_CHAT_ID"], "text": text, "parse_mode": "HTML",
                                    "disable_web_page_preview": True})
            ok = r.ok and r.json().get("ok")
            self.log(("telegram sent: " if ok else f"telegram FAILED {r.status_code}: ") + text.split("\n")[0])
            return ok
        except Exception as e:
            self.log(f"telegram error: {e}")
            return False

    async def send(self, key, text):
        if key in self.sent:
            return
        self._mark(key)
        await asyncio.get_running_loop().run_in_executor(None, self.telegram, text)

    def journal(self, rec):
        try:
            with open(JOURNAL, "a") as f:
                f.write(json.dumps(rec, default=str) + "\n")
        except Exception:
            pass

    # ---------------- market data ----------------
    def on_event(self, typ, row):
        sym = str(row.get("eventSymbol", ""))
        if typ == "Candle":
            base = sym.split("{")[0]
            store = {"SPX": self.spx, "VIX1D": self.v1d, "VIX": self.vix}.get(base)
            if store is None or row.get("time") is None:
                return
            t = datetime.fromtimestamp(float(row["time"]) / 1000, ET)
            idx = t.hour * 60 + t.minute - 570
            if not 0 <= idx < 390:
                return
            d = t.date().isoformat()
            vals = [_n(row.get(k)) for k in ("open", "high", "low", "close")]
            if vals[3] is None:
                return
            if base == "SPX":
                a = store.get(d)
                if a is None:
                    a = store[d] = np.full((390, 4), np.nan)
                a[idx] = [v if v is not None else np.nan for v in vals]
                self.sq_cache.pop(d, None)
            else:
                a = store.get(d)
                if a is None:
                    a = store[d] = np.full((390, 2), np.nan)   # open, close
                a[idx] = [vals[0] if vals[0] is not None else np.nan, vals[3]]
        elif typ == "Quote":
            self.quotes[sym] = (_n(row.get("bidPrice")), _n(row.get("askPrice")), time.time())
        elif typ == "Greeks":
            self.greeks[sym] = (_n(row.get("delta")), _n(row.get("volatility")), time.time())

    def hist_sqs(self, before, n):
        days = sorted(d for d, a in self.spx.items() if d < before and np.sum(~np.isnan(a[:, 3])) >= 300)[-n:]
        out = []
        for d in days:
            if d not in self.sq_cache:
                self.sq_cache[d] = E.returns_sq(self.spx[d][:, 3])
            out.append(self.sq_cache[d])
        return out

    def day_arrays(self, d):
        bars = self.spx.get(d, np.full((390, 4), np.nan))
        v1d = self.v1d.get(d, np.full((390, 2), np.nan))[:, 1]
        vx = self.vix.get(d, np.full((390, 2), np.nan))
        vix = vx[:, 1].copy()
        if not np.isnan(vx[0, 0]):
            vix[0] = vx[0, 0]           # VIX open = open of the 9:30 bar
        return bars, v1d, vix

    # ---------------- option prices at the signal ----------------
    async def option_check(self, r):
        side, S, em = r["side"], r["S"], r.get("em") or 30
        d = r["date"]
        if self.chain_date != d:
            self.chain = await asyncio.get_running_loop().run_in_executor(None, spxw_chain, self.auth, d)
            self.chain_date = d
        if not self.chain:
            return dict(err="no 0DTE chain")
        strikes = sorted(self.chain)
        if side == "put":
            ladder = [k for k in strikes if S - 3.2 * em - 20 <= k <= S]
        else:
            ladder = [k for k in strikes if S <= k <= S + 3.2 * em + 20]
        pc = 0 if side == "put" else 1
        syms = {k: self.chain[k][pc] for k in ladder}
        subs = [{"type": t, "symbol": s} for s in syms.values() for t in ("Quote", "Greeks")]
        t0 = time.time()
        await self.dx.subscribe(subs)
        need = [syms.get(float(r["short"])), syms.get(float(r["long"]))]
        while time.time() - t0 < 8:
            await asyncio.sleep(0.5)
            if all(s and s in self.quotes and self.quotes[s][2] >= t0 for s in need) and time.time() - t0 > 2.5:
                break
        await self.dx.unsubscribe(subs)

        def mid(k):
            q = self.quotes.get(syms.get(float(k)))
            if not q or q[0] is None or q[1] is None:
                return None
            return (q[0] + q[1]) / 2

        def spread(k):
            w = self.cfg["width"]
            kl = k - w if side == "put" else k + w
            a, b = mid(k), mid(kl)
            return (round(a - b, 2), kl) if a is not None and b is not None else (None, kl)

        out = {}
        ks = float(r["short"])
        out["our_credit"], _ = spread(ks)
        g = self.greeks.get(syms.get(ks))
        out["our_delta"] = abs(g[0]) if g and g[0] is not None else None
        # bear call too thin -> one strike closer
        if side == "call" and (out["our_credit"] is None or out["our_credit"] < self.cfg["min_credit"]):
            k2 = ks - self.cfg["step"]
            c2, kl2 = spread(k2)
            out.update(alt_short=k2, alt_long=kl2, alt_credit=c2)
        deltas = {k: (abs(self.greeks[s][0]) if s in self.greeks and self.greeks[s][0] is not None else None) for k, s in syms.items()}
        kq = E.pick_qg_strike(side, S, deltas, self.cfg["max_delta"])
        if kq is not None:
            cq, klq = spread(kq)
            out.update(qg_short=kq, qg_long=klq, qg_credit=cq, qg_delta=deltas.get(kq))
        return out

    # ---------------- after the trigger: 1-minute credit sampling + P&L every 5 minutes ----------------
    def _mid(self, sym, since=0):
        q = self.quotes.get(sym)
        if not q or q[0] is None or q[1] is None or q[2] < since:
            return None
        return (q[0] + q[1]) / 2

    def _legs(self, side, k):
        w, pc = self.cfg["width"], (0 if side == "put" else 1)
        kl = k - w if side == "put" else k + w
        a, b = self.chain.get(float(k)), self.chain.get(float(kl))
        return (a[pc] if a else None), (b[pc] if b else None), kl

    def _spread_mid(self, side, k, since=0):
        s1, s2, _ = self._legs(side, k)
        a, b = self._mid(s1, since), self._mid(s2, since)
        return round(a - b, 2) if a is not None and b is not None else None

    @staticmethod
    def _mode(vals):
        """most common value after rounding to $0.05 (ties -> closest to the median)"""
        if not vals:
            return None
        from collections import Counter
        r = [round(v * 20) / 20 for v in vals]
        c = Counter(r)
        top = max(c.values())
        tied = [v for v, n in c.items() if n == top]
        med = float(np.median(r))
        return round(min(tied, key=lambda v: abs(v - med)), 2)

    def _save_trade(self, tr):
        try:
            with open(os.path.join(STATE_DIR, f"trade_{tr['date']}.json"), "w") as f:
                json.dump(dict(tr, ladder={str(k): v for k, v in self.ladder.items()}), f, default=_json)
        except Exception:
            pass

    def qg_pick(self, tr=None):
        """QuantGlide comparison: spreads whose short delta is in [qg_lo, qg_hi]; likely pick = nearest qg_target"""
        tr = tr if tr is not None else self.state.get("trade") or {}
        lo, hi, tgt = self.cfg["qg_lo"], self.cfg["qg_hi"], self.cfg["qg_target"]
        cands = []
        for k, v in self.ladder.items():
            if not v:
                continue
            dl = float(np.median([x[1] for x in v]))
            if lo <= dl <= hi:
                cr = [x[0] for x in v]
                cands.append(dict(short=k, long=self._legs(tr.get("side", "put"), k)[2], delta=round(dl, 3),
                                  credit=self._mode(cr), cmin=min(cr), cmax=max(cr)))
        cands.sort(key=lambda c: c["short"], reverse=(tr.get("side") == "put"))
        pick = min(cands, key=lambda c: abs(c["delta"] - tgt)) if cands else None
        tr["qg_cands"] = cands
        if pick:
            tr.update(qg_track_short=pick["short"], qg_track_long=pick["long"], qg_track_entry=pick["credit"], qg_track_delta=pick["delta"])
        return tr

    def set_qg_range(self, lo, hi):
        self.cfg["qg_lo"], self.cfg["qg_hi"] = float(lo), float(hi)
        with self.lock:
            tr = self.state.get("trade") or {}
            if tr:
                self.qg_pick(tr)
                self._save_trade(tr)

    async def post_trigger(self, d, r, tr, sample=True):
        """1) every 5 s for 60 s: our spread + ladder credits/deltas; 2) every 5 min until 16:00: P&L"""
        try:
            side = r["side"]
            if self.chain_date != d:
                self.chain = await asyncio.get_running_loop().run_in_executor(None, spxw_chain, self.auth, d)
                self.chain_date = d
            if sample and tr.get("short") is not None:
                S, em = r["S"], r.get("em") or 30
                ks = sorted(self.chain)
                lad = [k for k in ks if S - 3.2 * em - 20 <= k <= S] if side == "put" else [k for k in ks if S <= k <= S + 3.2 * em + 20]
                pc = 0 if side == "put" else 1
                syms = {k: self.chain[k][pc] for k in lad}
                subs = [{"type": t, "symbol": x} for x in syms.values() for t in ("Quote", "Greeks")]
                await self.dx.subscribe(subs)
                ours, self.ladder = [], {k: [] for k in lad}
                for i in range(12):
                    await asyncio.sleep(5)
                    c = self._spread_mid(side, tr["short"])
                    if c is not None:
                        ours.append(c)
                    for k, x in syms.items():
                        cr, g = self._spread_mid(side, k), self.greeks.get(x)
                        if cr is not None and g and g[0] is not None:
                            self.ladder[k].append((cr, abs(g[0])))
                await self.dx.unsubscribe(subs)
                with self.lock:
                    tr.update(samples=ours, entry=self._mode(ours), smin=min(ours) if ours else None, smax=max(ours) if ours else None,
                              sampled_at=datetime.now(ET).strftime("%H:%M"), pnl=[])
                    self.qg_pick(tr)
                    self.state["trade"] = tr
                self._save_trade(tr)
                self.log(f"entry credit sampled: {len(ours)} readings, most common ${tr.get('entry')}")
            # ---- P&L every 5 minutes until the close ----
            start = datetime.now(ET)
            while True:
                now = datetime.now(ET)
                close_t = now.replace(hour=16, minute=0, second=0, microsecond=0)
                if now.date().isoformat() != d or now >= close_t:
                    break
                nxt = start + timedelta(minutes=5 * (int((now - start).total_seconds() // 300) + 1))
                await asyncio.sleep(max(1, min((nxt - now).total_seconds(), (close_t - now).total_seconds())))
                if datetime.now(ET) >= close_t:
                    break
                legs = list(self._legs(side, tr["short"])[:2])
                if tr.get("qg_track_short") is not None:
                    legs += list(self._legs(side, tr["qg_track_short"])[:2])
                legs = [x for x in legs if x]
                t0 = time.time()
                await self.dx.subscribe([{"type": "Quote", "symbol": x} for x in legs])
                await asyncio.sleep(4)
                await self.dx.unsubscribe([{"type": "Quote", "symbol": x} for x in legs])
                ours_now = self._spread_mid(side, tr["short"], t0)
                qg_now = self._spread_mid(side, tr["qg_track_short"], t0) if tr.get("qg_track_short") is not None else None
                spx = (self.state.get("today") or {})
                bars = self.spx.get(d)
                last = float(bars[~np.isnan(bars[:, 3]), 3][-1]) if bars is not None and np.any(~np.isnan(bars[:, 3])) else None
                row = dict(t=datetime.now(ET).strftime("%H:%M"), spx=last, ours=ours_now,
                           ours_pnl=round((tr["entry"] - ours_now) * 100, 0) if ours_now is not None and tr.get("entry") is not None else None,
                           qg=qg_now,
                           qg_pnl=round((tr["qg_track_entry"] - qg_now) * 100, 0) if qg_now is not None and tr.get("qg_track_entry") is not None else None)
                with self.lock:
                    tr.setdefault("pnl", []).append(row)
                self._save_trade(tr)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log(f"P&L tracker error: {e!r}")

    def resume_trade(self):
        """after a restart: reload today's trade and keep tracking P&L"""
        d = datetime.now(ET).date().isoformat()
        p = os.path.join(STATE_DIR, f"trade_{d}.json")
        if not os.path.exists(p) or (self.track_task and not self.track_task.done()):
            return
        try:
            with open(p) as f:
                tr = json.load(f)
            self.ladder = {float(k): [tuple(x) for x in v] for k, v in (tr.pop("ladder", {}) or {}).items()}
            with self.lock:
                self.state["trade"] = tr
            if tr.get("entry") is not None:
                r = dict(side=tr["side"], S=tr.get("S"), em=tr.get("em"))
                self.track_task = asyncio.get_running_loop().create_task(self.post_trigger(d, r, tr, sample=False))
                self.log("resumed P&L tracking after restart")
        except Exception as e:
            self.log(f"resume failed: {e!r}")

    # ---------------- messages ----------------
    def msg_signal(self, r, oc, late=False):
        cfg = self.cfg
        side_txt = "BULL PUT" if r["side"] == "put" else "BEAR CALL"
        arrow = "▲" if r["side"] == "put" else "▼"
        pc = "P" if r["side"] == "put" else "C"
        short, long_ = r.get("short"), r.get("long")
        credit = oc.get("our_credit")
        trade_k = (short, long_, credit)
        note = ""
        if r["side"] == "call" and (credit is None or credit < cfg["min_credit"]) and oc.get("alt_credit") is not None \
                and oc["alt_credit"] >= cfg["min_credit"]:
            trade_k = (oc["alt_short"], oc["alt_long"], oc["alt_credit"])
            note = f"\n(our {f2(short,0)} paid only ${f2(credit)} → moved one strike closer)"
        credit_ok = trade_k[2] is not None and trade_k[2] >= cfg["min_credit"]
        go = r["gate"] and credit_ok
        head = ("🟢 <b>TRADE</b>" if go else "⚪ <b>SKIP</b>") + f" – {arrow} {side_txt}  {r['sig_time']} ET" + ("  (late alert)" if late else "")
        lines = [head]
        lines.append(f"{'Sell' if go else 'Spread'} <b>{f2(trade_k[0],0)} / {f2(trade_k[1],0)} {pc}</b> 0DTE · mid <b>${f2(trade_k[2])}</b>"
                     + (" ✓" if credit_ok else f" ✗ (< ${cfg['min_credit']:.2f})") + note)
        lines.append(f"SPX {f2(r['S'])} · EM {f2(r.get('em'),0)} pts · short Δ {f2(oc.get('our_delta'),3)}"
                     + ("" if r.get("confirmed") else " · wick-only break"))
        lines.append(f"Ratio {f2(r['ratio'])} {'✓' if r['okR'] else '✗'} (> {r['thr']}) · VIX1D {r['jump']:+.1f} {'✓' if r['okJ'] else '✗'} · VIX {f2(r['vix_open'],1)} {'✓' if r['okV'] else '✗'}")
        if not r["gate"]:
            lines.append(f"Skip reason: {r['why']}")
        if oc.get("qg_short") is not None:
            lines.append(f"QG-style (Δ≤{cfg['max_delta']}): {f2(oc['qg_short'],0)}/{f2(oc['qg_long'],0)} · ${f2(oc.get('qg_credit'))} · Δ {f2(oc.get('qg_delta'),3)}")
        if go:
            n = E.contracts(self.account, trade_k[2], cfg)
            if n is not None:
                lines.append(f"Size @{cfg['risk_pct']:.0f}%: <b>{n}</b> contracts (max loss ${(cfg['width']-trade_k[2])*100:,.0f} each)")
            lines.append("Limit at mid · lower $0.05 once if no fill · no stop · hold to close")
        lag = r.get("v1d_lag", 0)
        if lag and lag > 2:
            lines.append(f"⚠ VIX1D data {lag} min old at the signal – ratio may be off, check VIX1D before trading")
        if oc.get("err"):
            lines.append(f"⚠ option prices unavailable: {oc['err']} – check credit manually")
        return "\n".join(lines), go, trade_k

    # ---------------- main tick ----------------
    async def tick(self):
        now = datetime.now(ET)
        d = now.date().isoformat()
        if now.weekday() >= 5:
            with self.lock:
                self.state.update(status="weekend – no session", updated=now)
            return
        open_t = now.replace(hour=9, minute=30, second=0, microsecond=0)
        secs = (now - open_t).total_seconds()
        n_done = int(max(0, min(390, (secs - 2) // 60)))
        bars, v1d, vix = self.day_arrays(d)
        while n_done > 0 and np.isnan(bars[n_done - 1, 3]) and secs - (n_done * 60) < 20:
            n_done -= 1                      # wait up to 20 s for the last bar to arrive
        hist = self.hist_sqs(d, self.cfg["look_n"])
        r = E.evaluate_day(d, bars, v1d, vix, hist, n_done, self.cfg) if secs > 0 else dict(date=d, status="pre-market", hist_days=len(hist))
        with self.lock:
            self.state.update(today=r, updated=now, status=r.get("status"))

        mins_since = lambda hh, mm: (now - now.replace(hour=hh, minute=mm, second=0, microsecond=0)).total_seconds() / 60

        # heartbeat (first tick of the day between 9:00 and 16:00)
        if 9 * 60 <= now.hour * 60 + now.minute < 16 * 60:
            await self.send(f"{d}:hb", f"🤖 <b>DH ORB-H bot v{__version__} running</b> – {d}\nHistory {len(hist)}/{self.cfg['look_n']} days · "
                                       f"feed OK{' · FOMC day: no trade' if d in E.FOMC else ''}")
        if secs <= 0:
            return
        if r.get("fomc") and n_done >= 1:
            await self.send(f"{d}:fomc", "🏛 FOMC day – no trade today")
        if "up" in r and f"{d}:levels" not in self.sent:
            if mins_since(10, 31) < FRESH_MIN:
                await self.send(f"{d}:levels", f"📏 <b>Range set</b> {f2(r['orL'])} – {f2(r['orH'])}\n"
                                               f"Triggers ▲ {f2(r['up'])}  ▼ {f2(r['dn'])}\n"
                                               f"VIX open {f2(r['vix_open'],1)} → ratio must be > {r['thr']} · VIX1D open {f2(r['v1d_open'])}")
            else:
                self._mark(f"{d}:levels")
        if r.get("status") == "signal" and f"{d}:signal" not in self.sent:
            age = mins_since(*map(int, r["sig_time"].split(":")))
            if age < 30:
                self._mark(f"{d}:signal")
                vv = ~np.isnan(v1d[: r["sig_idx"] + 1])
                r["v1d_lag"] = int(r["sig_idx"] - np.flatnonzero(vv)[-1]) if vv.any() else 999
                try:
                    oc = await self.option_check(r) if r.get("short") is not None else dict(err="no VIX1D")
                except Exception as e:
                    oc = dict(err=str(e)[:80])
                text, go, tk = self.msg_signal(r, oc, late=age > FRESH_MIN)
                tr = dict(date=d, go=go, side=r["side"], short=tk[0], long=tk[1], credit=tk[2], S=r["S"], em=r.get("em"),
                          sig_time=r["sig_time"], **oc)
                with self.lock:
                    self.state["trade"] = tr
                self.journal(dict(r, **oc, go=go, traded=tk))
                await self.send(f"{d}:signal_msg", text)
                if tk[0] is not None:
                    self.track_task = asyncio.get_running_loop().create_task(self.post_trigger(d, r, tr))
            else:
                self._mark(f"{d}:signal")
        if r.get("status", "").startswith("no breakout") and f"{d}:nobreak" not in self.sent:
            if mins_since(13, 30) < FRESH_MIN:
                await self.send(f"{d}:nobreak", f"⏹ No breakout by 13:30 – <b>no trade today</b>\nRange {f2(r['orL'])} – {f2(r['orH'])}")
            else:
                self._mark(f"{d}:nobreak")
        if n_done >= 390 and r.get("status") == "signal" and r.get("short") is not None and f"{d}:result" not in self.sent:
            tr = self.state.get("trade") or {}
            short = tr.get("short") if tr.get("date") == d and tr.get("short") else r["short"]
            go = tr.get("go") if tr.get("date") == d else r["gate"]
            intr, cat = E.settle(r["side"], short, r["close"], self.cfg["width"])
            res = "✅ WIN (full credit)" if cat == 0 else ("❌ MAX LOSS" if cat == 2 else f"❌ LOSS {intr:.1f} pts minus credit")
            pre = "" if go else "skipped – would have been: "
            await self.send(f"{d}:result", f"🏁 {pre}{res}\n{'Bull put' if r['side']=='put' else 'Bear call'} short {f2(short,0)} · SPX close {f2(r['close'])}")

    # ---------------- history for the dashboard ----------------
    def history(self, n=15):
        days = sorted(d for d, a in self.spx.items() if np.sum(~np.isnan(a[:, 3])) >= 300)
        today = datetime.now(ET).date().isoformat()
        out = []
        for j, d in enumerate(days):
            if d >= today or j < 5:
                continue
            if d in self._hist_cache:
                out.append(self._hist_cache[d]); continue
            bars, v1d, vix = self.day_arrays(d)
            hist = [self.sq_cache.setdefault(q, E.returns_sq(self.spx[q][:, 3])) for q in days[max(0, j - self.cfg["look_n"]):j]]
            r = E.evaluate_day(d, bars, v1d, vix, hist, 390, self.cfg)
            row = dict(date=d, hist=len(hist))
            if r.get("status") == "signal":
                row.update(signal=f"{'▲ put' if r['side']=='put' else '▼ call'} {r['sig_time']}", strikes=f"{f2(r.get('short'),0)}/{f2(r.get('long'),0)}",
                           decision="TRADE" if r["gate"] else f"skip – {r['why'] or 'ratio ' + f2(r['ratio'])}",
                           result={0: "WIN", 1: "LOSS", 2: "MAX LOSS"}.get(r.get("cat"), "–") if r["gate"] else
                           ("(would win)" if r.get("cat") == 0 else "(would lose)"))
            else:
                row.update(signal=r.get("status"), strikes="–", decision="NO TRADE", result="–")
            self._hist_cache[d] = row
            out.append(row)
        return out[-n:][::-1]

    # ---------------- chart data for the dashboard ----------------
    def available_days(self, n=15):
        days = sorted((d for d, a in self.spx.items() if np.sum(~np.isnan(a[:, 3])) > 0), reverse=True)
        return days[:n]

    def chart_day(self, d):
        """candles (ET wall-clock as epoch seconds) + levels + signal for one day"""
        if d not in self.spx:
            return None
        bars, v1d, vix = self.day_arrays(d)
        days = sorted(x for x, a in self.spx.items() if x < d and np.sum(~np.isnan(a[:, 3])) >= 300)[-self.cfg["look_n"]:]
        hist = [self.sq_cache.setdefault(q, E.returns_sq(self.spx[q][:, 3])) for q in days]
        today = datetime.now(ET).date().isoformat()
        n_done = (self.state.get("today") or {}).get("n_done", 390) if d == today else 390
        r = E.evaluate_day(d, bars, v1d, vix, hist, n_done, self.cfg)
        y, m, dd = map(int, d.split("-"))
        base = int(datetime(y, m, dd, 9, 30).replace(tzinfo=ZoneInfo("UTC")).timestamp())   # ET wall time shown as UTC
        candles = [dict(time=base + i * 60, open=float(b[0]) if not np.isnan(b[0]) else float(b[3]), high=float(b[1]),
                        low=float(b[2]), close=float(b[3])) for i, b in enumerate(bars[:n_done]) if not np.isnan(b[3])]
        tr = self.state.get("trade") or {}
        out = dict(date=d, candles=candles, r=r, base=base)
        if d == today and tr.get("date") == d:
            out["trade"] = tr
        return json.loads(json.dumps(out, default=_json))

    # ---------------- run ----------------
    async def session(self):
        self.dx = DXLink(self.auth, self.on_event)
        await self.dx.connect()
        ft = int((time.time() - HIST_CAL_DAYS * 86400) * 1000)
        await self.dx.subscribe([{"type": "Candle", "symbol": s, "fromTime": ft} for s in ("SPX{=m}", "VIX1D{=m}", "VIX{=m}")])
        with self.lock:
            self.state["feed"] = "connected"
        self.log("feed connected, loading history…")
        self.resume_trade()

        async def ticker():
            last_hist = 0
            while True:
                await asyncio.sleep(1)
                await self.tick()
                if time.time() - last_hist > 60:
                    h = self.history()
                    with self.lock:
                        self.state["history"] = h
                    last_hist = time.time()

        await asyncio.gather(self.dx.run(), ticker())

    async def main(self):
        backoff = 5
        while True:
            try:
                await self.session()
            except Exception as e:
                with self.lock:
                    self.state["feed"] = f"reconnecting ({type(e).__name__}: {str(e)[:60]})"
                self.log(f"feed error: {e!r}")
                traceback.print_exc()
            finally:
                if self.dx:
                    await self.dx.close()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    def start(self):
        def run():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.main())
        t = threading.Thread(target=run, daemon=True, name="dh-orb-worker")
        t.start()
        return t

    def snapshot(self):
        with self.lock:
            return json.loads(json.dumps(self.state, default=_json))


def _n(x):
    try:
        v = float(x)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _json(o):
    if isinstance(o, (np.floating,)):
        return None if np.isnan(o) else float(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, datetime):
        return o.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(o, tuple):
        return list(o)
    return str(o)


def load_secrets():
    keys = ["TT_CLIENT_SECRET", "TT_REFRESH_TOKEN", "TG_BOT_TOKEN", "TG_CHAT_ID", "ACCOUNT_SIZE"]
    s = {k: os.environ[k] for k in keys if k in os.environ}
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".streamlit", "secrets.toml")
    if len(s) < 4 and os.path.exists(p):
        import tomllib
        with open(p, "rb") as f:
            s = {**tomllib.load(f), **s}
    return s


if __name__ == "__main__":
    w = Worker(load_secrets())
    asyncio.run(w.main())
