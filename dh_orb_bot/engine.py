"""
DH ORB-H strategy logic – pure functions, no I/O.
Same rules as the TradingView indicator and the backtest:
  opening range 9:30-10:31 (61 one-minute bars), trigger = 0.055% of the 9:30 open beyond it,
  first break by 13:30 decides the side (up -> bull put, down -> bear call),
  gate: VIX1D / forecast RV > 1.2 (1.0 if VIX opened < 16), VIX1D rise < 2, VIX open <= 35, not FOMC,
  short strike = 1.16 x remaining-day expected move, rounded away to 5, 15 wide, hold to close.
Arrays are indexed by minute of the session: 0 = 9:30 bar ... 389 = 15:59 bar.
"""
import math
import numpy as np

CFG = dict(
    pct=0.055,          # trigger buffer, % of 9:30 open
    or_bars=61,         # opening-range length (1-min bars)
    cut=13 * 60 + 30,   # last signal: bar must CLOSE by 13:30
    ratio=1.2,          # VIX1D / forecast RV must be >
    low_vix=16.0,       # calm market: VIX open below this ...
    low_ratio=1.0,      # ... then ratio only needs to be >
    jump_max=2.0,       # VIX1D rise since open must be <
    vix_max=35.0,       # VIX open must be <=
    look_n=20,          # days in the rest-of-day / first-hour ratio
    kmult=1.16,         # short strike = kmult x remaining expected move
    width=15,
    step=5,
    min_credit=0.30,    # skip below this mid credit
    max_delta=0.105,    # QuantGlide-style pick: closest strike with |delta| <= this (matched his Oct 27 strike at 0.101)
    risk_pct=5.0,       # sizing: max loss per trade as % of account
    qg_lo=0.08,         # QuantGlide comparison: delta range he quotes ...
    qg_hi=0.12,
    qg_target=0.10,     # ... and his likely pick = strike nearest this delta
)

FOMC = {
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18", "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-10",
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17", "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
}
ANN = 390 * 252


def hhmm(minute_of_day):
    return f"{minute_of_day // 60}:{minute_of_day % 60:02d}"


def returns_sq(closes):
    """squared 1-min log returns, sq[i] = log(c[i]/previous available close)^2"""
    sq = np.full(390, np.nan)
    prev = np.nan
    for i in range(390):
        x = closes[i]
        if not np.isnan(x):
            if not np.isnan(prev):
                sq[i] = math.log(x / prev) ** 2
            prev = x
    return sq


def first_mean(sq):
    v = sq[1:60]
    v = v[~np.isnan(v)]
    return float(v.mean()) if len(v) else np.nan


def rest_mean(sq, frm):
    if frm >= 390:
        return np.nan
    v = sq[frm:]
    v = v[~np.isnan(v)]
    return float(v.mean()) if len(v) else np.nan


def forecast_rv(today_sq, hist_sqs, idx):
    """today's first-hour vol x sqrt(20-day avg rest-of-day var / avg first-hour var), annualised %"""
    frm = min(idx + 2, 390)
    sR = sF = 0.0
    used = 0
    for h in hist_sqs:
        fm, rm = first_mean(h), rest_mean(h, frm)
        if not np.isnan(fm) and not np.isnan(rm):
            sR += rm
            sF += fm
            used += 1
    fm_t = first_mean(today_sq)
    if used < 5 or sF <= 0 or np.isnan(fm_t):
        return np.nan
    return math.sqrt(fm_t * sR / sF) * math.sqrt(ANN) * 100


def expected_move(S, iv, t_end_min):
    return S * iv / 100 * math.sqrt(max(960 - t_end_min, 0) / ANN)


def our_strikes(S, iv, t_end_min, side, cfg=CFG):
    u = expected_move(S, iv, t_end_min)
    st, k = cfg["step"], cfg["kmult"]
    if side == "put":
        ks = math.floor((S - k * u) / st) * st
        return ks, ks - cfg["width"], u
    ks = math.ceil((S + k * u) / st) * st
    return ks, ks + cfg["width"], u


def last_valid(a, upto):
    v = a[: upto + 1]
    v = v[~np.isnan(v)]
    return float(v[-1]) if len(v) else np.nan


def settle(side, short_k, close, width=15):
    intr = max(min(short_k - close, width), 0) if side == "put" else max(min(close - short_k, width), 0)
    cat = 0 if intr <= 0 else (2 if intr >= width else 1)
    return intr, cat


def evaluate_day(date, bars, v1d, vix, hist_sqs, n_done=390, cfg=CFG):
    """
    bars: (390,4) o/h/l/c of SPX, nan where missing; v1d, vix: (390,) closes (vix may be all nan)
    hist_sqs: list of returns_sq() arrays for the previous look_n days (oldest first)
    n_done: number of completed 1-min bars available today (390 = full day)
    returns a dict describing the day so far
    """
    r = dict(date=date, n_done=n_done, fomc=date in cfg.get("fomc", FOMC), hist_days=len(hist_sqs))
    nb = cfg["or_bars"]
    open0 = bars[0, 0] if not np.isnan(bars[0, 0]) else last_valid(bars[:, 3], 0)
    r["open"] = open0
    vix_open = vix[0] if len(vix) and not np.isnan(vix[0]) else last_valid(vix, min(5, len(vix) - 1)) if len(vix) else np.nan
    r["vix_open"] = vix_open
    r["v1d_open"] = last_valid(v1d, 0) if not np.isnan(v1d[0]) else (last_valid(v1d, 5))
    r["thr"] = cfg["low_ratio"] if (not np.isnan(vix_open) and vix_open < cfg["low_vix"]) else cfg["ratio"]
    sq = returns_sq(np.where(np.arange(390) < n_done, bars[:, 3], np.nan))
    if n_done < nb:
        r["status"] = "building range"
        r["orH"] = np.nanmax(bars[:n_done, 1]) if n_done else np.nan
        r["orL"] = np.nanmin(bars[:n_done, 2]) if n_done else np.nan
        return r
    H, L = float(np.nanmax(bars[:nb, 1])), float(np.nanmin(bars[:nb, 2]))
    buf = open0 * cfg["pct"] / 100
    r.update(orH=H, orL=L, up=H + buf, dn=L - buf)
    sig = None
    for i in range(nb, n_done):
        t_end = 570 + i + 1
        if t_end > cfg["cut"]:
            break
        h, l = bars[i, 1], bars[i, 2]
        if np.isnan(h):
            continue
        up, dn = h > H + buf, l < L - buf
        if up or dn:
            sig = (i, "put" if (up and not dn) else "call")
            break
    if sig is None:
        cut_idx = cfg["cut"] - 570 - 1
        if n_done - 1 >= cut_idx:
            r["status"] = "FOMC – no trade" if r["fomc"] else "no breakout – no trade"
        else:
            r["status"] = "watching"
            i = n_done - 1
            S, iv = bars[i, 3], last_valid(v1d, i)
            fc = forecast_rv(sq, hist_sqs, i)
            r["live_ratio"] = iv / fc if (fc and not np.isnan(fc)) else np.nan
            r["live_fc"] = fc
            r["live_iv"] = iv
            if not np.isnan(iv):
                r["if_up"] = our_strikes(S, iv, 570 + i + 1, "put", cfg)[:2]
                r["if_dn"] = our_strikes(S, iv, 570 + i + 1, "call", cfg)[:2]
        return r
    i, side = sig
    t_end = 570 + i + 1
    S = float(bars[i, 3])
    iv = last_valid(v1d, i)
    fc = forecast_rv(sq, hist_sqs, i)
    ratio = iv / fc if (fc and not np.isnan(fc) and not np.isnan(iv)) else np.nan
    jump = iv - r["v1d_open"] if not (np.isnan(iv) or np.isnan(r["v1d_open"])) else 0.0
    okR = not np.isnan(ratio) and ratio > r["thr"]
    okJ = jump < cfg["jump_max"]
    okV = np.isnan(vix_open) or vix_open <= cfg["vix_max"]
    gate = okR and okJ and okV and not r["fomc"]
    why = []
    if r["fomc"]: why.append("FOMC")
    if np.isnan(ratio): why.append("ratio n/a")
    elif not okR: why.append(f"ratio {ratio:.2f} ≤ {r['thr']}")
    if not okJ: why.append(f"VIX1D +{jump:.1f}")
    if not okV: why.append(f"VIX {vix_open:.1f}")
    confirmed = (bars[i, 3] > H + buf) if side == "put" else (bars[i, 3] < L - buf)
    r.update(status="signal", sig_idx=i, sig_time=hhmm(t_end), side=side, S=S, iv=iv, fc=fc, ratio=ratio, jump=jump,
             okR=okR, okJ=okJ, okV=okV, gate=gate, why=", ".join(why), confirmed=bool(confirmed))
    if not np.isnan(iv):
        ks, kl, u = our_strikes(S, iv, t_end, side, cfg)
        r.update(short=ks, long=kl, em=u)
        if n_done >= 390:
            close = last_valid(bars[:, 3], 389)
            intr, cat = settle(side, ks, close, cfg["width"])
            r.update(close=close, intr=intr, cat=cat)
    return r


def pick_qg_strike(side, S, deltas, max_delta):
    """QuantGlide-style: strike closest to the money whose |delta| <= max_delta. deltas: {strike: |delta|}"""
    ks = sorted(deltas, reverse=(side == "put"))   # puts: from high (near money) down; calls: from low up
    cand = [k for k in ks if (k < S if side == "put" else k > S) and deltas[k] is not None and abs(deltas[k]) <= max_delta]
    return cand[0] if cand else None


def contracts(account, credit, cfg=CFG):
    if not account:
        return None
    loss = (cfg["width"] - credit) * 100
    return max(int(account * cfg["risk_pct"] / 100 // loss), 0)
