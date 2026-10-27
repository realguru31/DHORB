"""
tastytrade connectivity: OAuth refresh, option chain (REST) and DXLink market-data websocket (raw, no SDK).
"""
import asyncio
import json
import time

import requests
import websockets

API = "https://api.tastyworks.com"


class TastyAuth:
    def __init__(self, client_secret, refresh_token):
        self.cs, self.rt = client_secret, refresh_token
        self._access, self._access_exp = None, 0
        self._qtok, self._qurl, self._q_exp = None, None, 0

    def access(self):
        if time.time() > self._access_exp:
            r = requests.post(f"{API}/oauth/token", timeout=20,
                              data={"grant_type": "refresh_token", "refresh_token": self.rt, "client_secret": self.cs})
            r.raise_for_status()
            j = r.json()
            self._access = j["access_token"]
            self._access_exp = time.time() + int(j.get("expires_in", 900)) - 60
        return self._access

    def headers(self):
        return {"Authorization": f"Bearer {self.access()}"}

    def quote_token(self):
        if time.time() > self._q_exp:
            r = requests.get(f"{API}/api-quote-tokens", headers=self.headers(), timeout=20)
            r.raise_for_status()
            d = r.json()["data"]
            self._qtok, self._qurl = d["token"], d["dxlink-url"]
            self._q_exp = time.time() + 12 * 3600
        return self._qtok, self._qurl


def spxw_chain(auth, expiry):
    """{strike(float): (put_streamer_symbol, call_streamer_symbol)} for one SPXW expiry (YYYY-MM-DD)"""
    r = requests.get(f"{API}/option-chains/SPX/nested", headers=auth.headers(), timeout=30)
    r.raise_for_status()
    out = {}
    for it in r.json()["data"]["items"]:
        if it.get("root-symbol") != "SPXW":
            continue
        for e in it["expirations"]:
            if e["expiration-date"] == expiry:
                for k in e["strikes"]:
                    out[float(k["strike-price"])] = (k["put-streamer-symbol"], k["call-streamer-symbol"])
    return out


FIELDS = {
    "Candle": ["eventSymbol", "time", "open", "high", "low", "close"],
    "Trade": ["eventSymbol", "price", "time"],
    "Quote": ["eventSymbol", "bidPrice", "askPrice"],
    "Greeks": ["eventSymbol", "delta", "volatility"],
}


def _num(x):
    try:
        v = float(x)
        return v if v == v else None   # NaN -> None
    except (TypeError, ValueError):
        return None


class DXLink:
    """Minimal DXLink client. on_event(type, row_dict) is called for every event."""

    def __init__(self, auth, on_event):
        self.auth, self.on_event = auth, on_event
        self.ws, self.fields = None, dict(FIELDS)
        self.last_msg = 0

    async def _send(self, m):
        await self.ws.send(json.dumps(m))

    async def _wait(self, cond, name, secs=20):
        t0 = time.time()
        while time.time() - t0 < secs:
            try:
                m = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=2))
            except asyncio.TimeoutError:
                continue
            if m.get("type") == "ERROR":
                raise RuntimeError(f"DXLink error: {m}")
            if cond(m):
                return m
        raise TimeoutError(f"DXLink: no {name}")

    async def connect(self):
        token, url = self.auth.quote_token()
        self.ws = await websockets.connect(url, max_size=2 ** 26, ping_interval=None)
        await self._send({"type": "SETUP", "channel": 0, "version": "0.1-DXF-JS/0.3.0", "keepaliveTimeout": 60, "acceptKeepaliveTimeout": 60})
        await self._wait(lambda m: m.get("type") == "AUTH_STATE", "AUTH_STATE")
        await self._send({"type": "AUTH", "channel": 0, "token": token})
        await self._wait(lambda m: m.get("type") == "AUTH_STATE" and m.get("state") == "AUTHORIZED", "AUTHORIZED")
        await self._send({"type": "CHANNEL_REQUEST", "channel": 1, "service": "FEED", "parameters": {"contract": "AUTO"}})
        await self._wait(lambda m: m.get("type") == "CHANNEL_OPENED", "CHANNEL_OPENED")
        await self._send({"type": "FEED_SETUP", "channel": 1, "acceptAggregationPeriod": 0.2, "acceptDataFormat": "COMPACT",
                          "acceptEventFields": FIELDS})
        m = await self._wait(lambda m: m.get("type") == "FEED_CONFIG", "FEED_CONFIG")
        if m.get("eventFields"):
            self.fields.update(m["eventFields"])
        self.last_msg = time.time()

    async def subscribe(self, subs):
        await self._send({"type": "FEED_SUBSCRIPTION", "channel": 1, "add": subs})

    async def unsubscribe(self, subs):
        await self._send({"type": "FEED_SUBSCRIPTION", "channel": 1, "remove": subs})

    async def run(self):
        """read forever; raises on disconnect (caller reconnects)"""
        ka = time.time()
        while True:
            if time.time() - ka > 25:
                await self._send({"type": "KEEPALIVE", "channel": 0})
                ka = time.time()
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                if time.time() - self.last_msg > 120:
                    raise TimeoutError("DXLink silent for 2 minutes")
                continue
            self.last_msg = time.time()
            m = json.loads(raw)
            if m.get("type") == "FEED_DATA":
                d = m["data"]
                if not d:
                    continue
                typ, flat = d[0], d[1]
                f = self.fields.get(typ)
                if not f:
                    continue
                n = len(f)
                for i in range(0, len(flat) - n + 1, n):
                    self.on_event(typ, dict(zip(f, flat[i:i + n])))
            elif m.get("type") == "ERROR":
                raise RuntimeError(f"DXLink error: {m}")

    async def close(self):
        try:
            await self.ws.close()
        except Exception:
            pass
