"""TradingView Lightweight Charts rendering of one day: candles, range, triggers, signal and strikes."""
import json

LWC = "https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"


def chart_html(data, height=520):
    r = data["r"]
    tr = data.get("trade") or {}
    lines = []

    def add(price, color, title, style=0, width=1):
        if price is not None:
            lines.append(dict(price=price, color=color, title=title, lineStyle=style, lineWidth=width))

    add(r.get("orH"), "#8a8f98", "range H")
    add(r.get("orL"), "#8a8f98", "range L")
    add(r.get("up"), "#26a69a", "▲ trigger", 2)
    add(r.get("dn"), "#ef5350", "▼ trigger", 2)
    markers = []
    if r.get("status") == "signal" and r.get("sig_idx") is not None:
        go = tr.get("go") if tr else r.get("gate")
        short = tr.get("short") or r.get("short")
        long_ = tr.get("long") or r.get("long")
        col = ("#26a69a" if r["side"] == "put" else "#ef5350") if go else "#9e9e9e"
        add(short, col, f"short {short:.0f}" if short else "", 0, 2)
        add(long_, col, f"long {long_:.0f}" if long_ else "", 1, 1)
        side = "BULL PUT" if r["side"] == "put" else "BEAR CALL"
        markers.append(dict(time=data["base"] + r["sig_idx"] * 60,
                            position="belowBar" if r["side"] == "put" else "aboveBar",
                            color=col, shape="arrowUp" if r["side"] == "put" else "arrowDown",
                            text=f"{'TRADE' if go else 'SKIP'} {side} {short:.0f}/{long_:.0f}" if short else side))
        if r.get("cat") is not None and data["candles"]:
            res = {0: "WIN", 1: "LOSS", 2: "MAX LOSS"}[r["cat"]]
            markers.append(dict(time=data["candles"][-1]["time"], position="belowBar",
                                color="#26a69a" if r["cat"] == 0 else "#ef5350", shape="circle",
                                text=res if go else f"(would {'win' if r['cat'] == 0 else 'lose'})"))
    payload = json.dumps(dict(candles=data["candles"], lines=lines, markers=markers))
    return f"""
<div id="c" style="width:100%;height:{height}px"></div>
<script src="{LWC}"></script>
<script>
const d = {payload};
const el = document.getElementById('c');
const chart = LightweightCharts.createChart(el, {{
  width: el.clientWidth, height: {height},
  layout: {{ background: {{ type: 'solid', color: '#0e1117' }}, textColor: '#c9ccd1', fontSize: 12 }},
  grid: {{ vertLines: {{ color: '#1c2030' }}, horzLines: {{ color: '#1c2030' }} }},
  rightPriceScale: {{ borderColor: '#2a2e39' }},
  timeScale: {{ borderColor: '#2a2e39', timeVisible: true, secondsVisible: false, rightOffset: 12 }},
  crosshair: {{ mode: 0 }},
  localization: {{ locale: 'en-US' }}
}});
const s = chart.addCandlestickSeries({{ upColor: '#26a69a', downColor: '#ef5350', borderVisible: false,
  wickUpColor: '#26a69a', wickDownColor: '#ef5350', priceLineVisible: false }});
s.setData(d.candles);
d.lines.forEach(l => s.createPriceLine({{ price: l.price, color: l.color, lineWidth: l.lineWidth,
  lineStyle: l.lineStyle, axisLabelVisible: true, title: l.title }}));
s.setMarkers(d.markers);
chart.timeScale().fitContent();
new ResizeObserver(() => chart.applyOptions({{ width: el.clientWidth }})).observe(el);
</script>"""
