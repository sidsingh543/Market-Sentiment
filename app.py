import json
import time
import redis
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objs as go

from config import REDIS_HOST, REDIS_PORT, REDIS_DB

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

app = Dash(__name__)
app.title = "Real-Time Market Sim"

def load_snapshot():
    raw = r.get("market:latest")
    if not raw:
        return {"symbols": {}}
    return json.loads(raw)

def layout_symbol(symbol, s):
    if not s:
        return html.Div(f"No data for {symbol}")

    bands = s["bands"]
    p05, p50, p95 = bands["p05"], bands["p50"], bands["p95"]
    s0 = s["s0"]

    fig = go.Figure()
    fig.add_trace(go.Indicator(
        mode="number+delta",
        value=s0,
        title={"text": f"{symbol} last"},
        delta={"reference": p50, "relative": False},
        domain={"x": [0, 1], "y": [0, 1]},
    ))

    fig.add_hline(y=p05, line_dash="dot", annotation_text="p05", annotation_position="right")
    fig.add_hline(y=p50, line_dash="dash", annotation_text="median", annotation_position="right")
    fig.add_hline(y=p95, line_dash="dot", annotation_text="p95", annotation_position="right")

    fig.update_layout(height=260, margin=dict(l=20, r=20, t=40, b=20))
    return dcc.Graph(figure=fig)

app.layout = html.Div([
    html.H2("Real-Time Sentiment-Driven Market Simulation"),
    html.Div(id="last-updated", style={"marginBottom": "8px", "fontSize": "14px", "color": "#555"}),
    html.Div(id="cards"),
    dcc.Interval(id="tick", interval=1000, n_intervals=0)
], style={"maxWidth": "1100px", "margin": "40px auto", "fontFamily": "system-ui, -apple-system"})

@app.callback(
    Output("cards", "children"),
    Output("last-updated", "children"),
    Input("tick", "n_intervals")
)
def refresh(_):
    snap = load_snapshot()
    syms = snap.get("symbols", {})
    updated = snap.get("updated")
    when = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(updated)) if updated else "—"

    cards = []
    for symbol in sorted(syms.keys()):
        s = syms[symbol]
        cards.append(html.Div([
            html.H4(symbol, style={"margin": "0 0 6px 0"}),
            layout_symbol(symbol, s)
        ], style={
            "border": "1px solid #eee",
            "borderRadius": "12px",
            "padding": "16px",
            "marginBottom": "16px",
            "boxShadow": "0 1px 6px rgba(0,0,0,0.04)"
        }))

    return cards, f"Last update: {when}"

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)
