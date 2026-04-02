# dashboard/app.py
from dash import Dash, html, dcc, dash_table, ctx
from dash.dependencies import Input, Output, State
from strategies.pmcc.signals_india import IndiaPMCC
from strategies.credit_spread.signals_india import IndiaCreditSpreads
from dashboard.portfolio import init_db, log_trade, get_trades, start_ltp_polling, clear_all_trades, get_total_pnl
import pandas as pd

app = Dash(__name__)

# ── shared objects ────────────────────────────────────────────────────────────
obj = IndiaPMCC()
obj.get_long_short_df()
obj.start_live_feed()
obj.start_greeks_refresh(interval_seconds=15)
obj.iv_stats = obj.get_iv_stats()

cs = IndiaCreditSpreads(broker=obj.broker, connection=obj.connection)
cs.load_from_pmcc(obj.raw_calls)
cs.spread_latest = obj.latest

_orig_refresh = obj._refresh_greeks_cache
def _patched_refresh():
    try:
        _orig_refresh()
    except Exception as e:
        print(f"[greeks refresh] failed: {e}")
        return  # bail early, don't touch cs at all
    if obj.greeks_cache is not None:
        cs.get_filtered_dfs()
        cs.apply_greeks_filters(obj.greeks_cache)
obj._refresh_greeks_cache = _patched_refresh

# ── portfolio db + polling ────────────────────────────────────────────────────
init_db()
start_ltp_polling(obj.connection, interval=2)

# ── shared helpers ────────────────────────────────────────────────────────────
COL_WIDTHS = {
    "token": 108, "strike": 96, "ltp": 96, "bid": 96, "ask": 96, "spread": 90,
    "day_volume": 120, "total_buy": 108, "total_sell": 108, "oi": 108,
    "iv": 84, "delta": 84, "gamma": 84, "theta": 90, "vega": 84,
}

def make_columns(df):
    return [{"name": c, "id": c} for c in df.columns]

def table_style(header_color):
    return dict(
        style_table={"overflowX": "auto", "marginBottom": "10px"},
        style_header={"backgroundColor": "#222", "color": header_color, "fontWeight": "bold", "border": "1px solid #333", "whiteSpace": "nowrap"},
        style_cell={"backgroundColor": "#111", "color": "white", "border": "1px solid #222", "fontFamily": "Courier", "fontSize": "13px", "padding": "6px 10px", "whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"},
        style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#1a1a1a"}],
        style_cell_conditional=[{"if": {"column_id": col}, "width": f"{w}px", "minWidth": f"{w}px", "maxWidth": f"{w}px"} for col, w in COL_WIDTHS.items()],
        page_size=20,
        sort_action="native",
    )

def make_expiry_tables(df, label, header_color):
    if df.empty:
        return [html.P("No liquid candidates found.", style={"color": "#aaa"})]
    expiries = df['expiry'].unique()
    blocks, row = [], []
    for i, expiry in enumerate(expiries):
        subset = df[df['expiry'] == expiry].drop(columns=['expiry']).reset_index(drop=True)
        for col in ['iv', 'delta', 'gamma', 'theta', 'vega']:
            if col in subset.columns:
                subset[col] = subset[col].round(4)
        card = html.Div(
            style={"flex": "1 1 90%", "minWidth": "280px", "maxWidth": "98%", "backgroundColor": "#1a1a1a", "borderRadius": "8px", "border": "1px solid #333", "padding": "12px", "boxSizing": "border-box"},
            children=[
                html.H4(f"{label} — {expiry}", style={"color": header_color, "margin": "0 0 8px 0", "fontSize": "13px"}),
                dash_table.DataTable(columns=make_columns(subset), data=subset.to_dict("records"), **table_style(header_color))
            ]
        )
        row.append(card)
        if len(row) == 1 or i == len(expiries) - 1:
            blocks.append(html.Div(row, style={"display": "flex", "flexWrap": "wrap", "gap": "12px", "marginBottom": "12px"}))
            row = []
    return blocks

def format_expiry(df):
    if not df.empty and 'expiry' in df.columns:
        df['expiry'] = pd.to_datetime(df['expiry']).dt.strftime('%d %b %Y')
    return df

def input_box(label, id, placeholder, width="120px"):
    return html.Div([
        html.Label(label, style={"color": "#aaa", "fontSize": "12px", "marginBottom": "4px", "display": "block"}),
        dcc.Input(id=id, placeholder=placeholder, debounce=False, style={"backgroundColor": "#111", "color": "white", "border": "1px solid #444", "padding": "6px 8px", "fontFamily": "Courier", "fontSize": "13px", "width": width, "display": "block"}),
    ])

def trade_entry_panel():
    return html.Div(
        style={"backgroundColor": "#1a1a1a", "padding": "16px", "borderRadius": "8px", "border": "1px solid #333", "marginBottom": "24px", "display": "flex", "gap": "12px", "alignItems": "flex-end", "flexWrap": "wrap"},
        children=[
            input_box("Exchange",       "p-exchange", "NFO",     width="80px"),
            input_box("Trading Symbol", "p-symbol",   "NIFTY28APR2026C26000", width="200px"),
            input_box("Symbol Token",   "p-token",    "7000",    width="100px"),
            html.Button("BUY",  id="btn-buy",  n_clicks=0, style={"backgroundColor": "#1a3a1a", "color": "#00e676", "border": "1px solid #00e676", "padding": "8px 20px", "fontFamily": "Courier", "cursor": "pointer", "fontWeight": "bold", "fontSize": "13px"}),
            html.Button("SELL", id="btn-sell", n_clicks=0, style={"backgroundColor": "#3a1a1a", "color": "#ff5252", "border": "1px solid #ff5252", "padding": "8px 20px", "fontFamily": "Courier", "cursor": "pointer", "fontWeight": "bold", "fontSize": "13px"}),
            html.Button("CLEAR ALL", id="btn-clear", n_clicks=0, style={"backgroundColor": "#2a1a00", "color": "#ffab40", "border": "1px solid #ffab40", "padding": "8px 20px", "fontFamily": "Courier", "cursor": "pointer", "fontWeight": "bold", "fontSize": "13px"}),
            html.Div(id="clear-confirm", style={"display": "none"}, children=[
                html.Span("Sure? ", style={"color": "#aaa", "fontFamily": "Courier", "fontSize": "13px"}),
                html.Button("YES, CLEAR", id="btn-clear-confirm", n_clicks=0, style={"backgroundColor": "#3a0000", "color": "#ff5252", "border": "1px solid #ff5252", "padding": "4px 12px", "fontFamily": "Courier", "cursor": "pointer", "fontSize": "12px"}),
            ]),
            html.Div(id="trade-status", style={"color": "#aaa", "fontFamily": "Courier", "fontSize": "13px", "alignSelf": "center"}),
        ]
    )

# ── layout ────────────────────────────────────────────────────────────────────
TAB_STYLE    = {"backgroundColor": "#111", "color": "#aaa",   "border": "1px solid #333"}
TAB_SELECTED = {"backgroundColor": "#1a1a1a", "color": "white", "border": "1px solid #555"}

app.layout = html.Div(
    style={"backgroundColor": "#111", "padding": "20px", "fontFamily": "Courier"},
    children=[
        dcc.Tabs(
            id="tabs", value="pmcc",
            style={"marginBottom": "20px"},
            colors={"background": "#111", "primary": "#00e5ff", "border": "#333"},
            children=[
                dcc.Tab(label="PMCC", value="pmcc", style=TAB_STYLE, selected_style=TAB_SELECTED,
                        children=[
                            html.Div(
                                style={"display": "inline-block", "backgroundColor": "#1a1a1a", "padding": "12px 18px", "borderRadius": "8px", "border": "1px solid #333", "marginBottom": "24px"},
                                children=[
                                    html.H4("IV Stats", style={"color": "#aaa", "margin": "0 0 8px 0"}),
                                    html.P(id="current-iv", style={"color": "white", "margin": "4px 0"}),
                                    html.P(id="iv-rank",    style={"color": "white", "margin": "4px 0"}),
                                    html.P(id="iv-pct",     style={"color": "white", "margin": "4px 0"}),
                                ]
                            ),
                            html.H2("Candidate Long Calls",  style={"color": "white", "marginBottom": "12px"}),
                            html.Div(id="long-call-tables"),
                            html.Hr(style={"borderColor": "#333", "margin": "24px 0"}),
                            html.H2("Candidate Short Calls", style={"color": "white", "marginBottom": "12px"}),
                            html.Div(id="short-call-tables"),
                        ]),
                dcc.Tab(label="Credit Spreads", value="credit-spreads", style=TAB_STYLE, selected_style=TAB_SELECTED,
                        children=[
                            html.H2("Candidate Credit Spread Legs", style={"color": "white", "marginBottom": "12px"}),
                            html.Div(id="cs-tables"),
                        ]),
                dcc.Tab(label="Spread Pairs", value="spread-pairs", style=TAB_STYLE, selected_style=TAB_SELECTED,
                        children=[
                            html.H2("Bear Call Spread Candidates", style={"color": "white", "marginBottom": "12px"}),
                            html.Div(id="spread-pairs-tables"),
                        ]),
                dcc.Tab(label="Portfolio", value="portfolio", style=TAB_STYLE, selected_style=TAB_SELECTED,
                        children=[
                            html.H2("Paper Trade Portfolio", style={"color": "white", "marginBottom": "16px"}),
                            trade_entry_panel(),
                            html.Div(id="portfolio-table"),
                            html.Div(id="portfolio-pnl", style={"marginTop": "20px", "padding": "16px 24px", "backgroundColor": "#1a1a1a", "borderRadius": "8px", "border": "1px solid #333", "display": "inline-block"}),
                        ]),
            ]
        ),
        dcc.Interval(id="interval", interval=500, n_intervals=0),
    ]
)

# ── callbacks ─────────────────────────────────────────────────────────────────
@app.callback(
    Output("current-iv", "children"), Output("iv-rank", "children"), Output("iv-pct", "children"),
    Output("long-call-tables", "children"), Output("short-call-tables", "children"),
    Input("interval", "n_intervals"))
def refresh_pmcc(_):
    iv, iv_rank, iv_pct = obj.iv_stats
    long_call, short_call = obj.get_final_dfs()
    format_expiry(long_call)
    format_expiry(short_call)
    return (f"Current IV:    {iv:.2f}", f"IV Rank:       {iv_rank:.1f}%", f"IV Percentile: {iv_pct:.1f}%",
            make_expiry_tables(long_call, "Long", "#00e5ff"), make_expiry_tables(short_call, "Short", "#ff6b6b"))

@app.callback(Output("cs-tables", "children"), Input("interval", "n_intervals"))
def refresh_cs(_):
    df = cs.get_tick_data() if hasattr(cs, 'get_tick_data') else (cs.raw_calls.copy() if cs.raw_calls is not None else pd.DataFrame())
    if df.empty:
        return [html.P("Waiting for data...", style={"color": "#aaa"})]
    desired = ['token', 'strike', 'expiry', 'ltp', 'bid', 'ask', 'spread', 'day_volume', 'oi', 'iv', 'delta', 'gamma', 'theta', 'vega']
    df = df[[c for c in desired if c in df.columns]]
    format_expiry(df)
    return make_expiry_tables(df, "Credit Spread", "#a0e080")

@app.callback(
    Output("trade-status", "children"),
    Input("btn-buy", "n_clicks"), Input("btn-sell", "n_clicks"),
    State("p-exchange", "value"), State("p-symbol", "value"), State("p-token", "value"),
    prevent_initial_call=True)
def log_trade_cb(buy_clicks, sell_clicks, exchange, symbol, token):
    if not exchange or not symbol or not token:
        return "⚠ Fill all fields"
    try:
        ltp = obj.connection.ltpData(exchange=exchange, tradingsymbol=symbol, symboltoken=token)['data']['ltp']
        side = "BUY" if ctx.triggered_id == "btn-buy" else "SELL"
        log_trade(exchange, symbol, token, side, ltp)
        return f"✓ {side} {symbol} @ {ltp}"
    except Exception as e:
        return f"✗ {e}"

@app.callback(
    Output("portfolio-table", "children"), Output("portfolio-pnl", "children"),
    Input("interval", "n_intervals"))
def refresh_portfolio(_):
    df = get_trades()
    raw_pnl = get_total_pnl()
    total_pnl = raw_pnl * 65
    pnl_color = "#00e676" if total_pnl >= 0 else "#ff5252"
    pnl_sign  = "+" if total_pnl >= 0 else ""
    pnl_block = html.Div([
        html.Span("Total Portfolio PnL  ", style={"color": "#aaa", "fontFamily": "Courier", "fontSize": "14px"}),
        html.Span(f"{pnl_sign}₹{total_pnl:,.2f}", style={"color": pnl_color, "fontFamily": "Courier", "fontSize": "22px", "fontWeight": "bold"}),
        html.Span("  (65× lot)", style={"color": "#555", "fontFamily": "Courier", "fontSize": "12px"}),
    ])
    if df.empty:
        return html.P("No trades yet.", style={"color": "#aaa", "fontFamily": "Courier"}), pnl_block
    style_data_conditional = [
        {"if": {"row_index": "odd"}, "backgroundColor": "#1a1a1a"},
        *[{"if": {"filter_query": f"{{id}} = {row['id']}", "column_id": "pnl"}, "color": "#00e676" if row['pnl'] >= 0 else "#ff5252"} for _, row in df.iterrows()]
    ]
    table = dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in df.columns],
        data=df.to_dict("records"),
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#222", "color": "#a0e080", "fontWeight": "bold", "border": "1px solid #333", "whiteSpace": "nowrap"},
        style_cell={"backgroundColor": "#111", "color": "white", "border": "1px solid #222", "fontFamily": "Courier", "fontSize": "13px", "padding": "6px 10px", "whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"},
        style_data_conditional=style_data_conditional,
        sort_action="native", page_size=20,
    )
    return table, pnl_block

@app.callback(Output("clear-confirm", "style"), Input("btn-clear", "n_clicks"), prevent_initial_call=True)
def show_confirm(_):
    return {"display": "flex", "alignItems": "center", "gap": "8px"}

@app.callback(
    Output("trade-status", "children"), Output("clear-confirm", "style"),
    Input("btn-clear-confirm", "n_clicks"), prevent_initial_call=True)
def do_clear(_):
    clear_all_trades()
    return "✓ All trades cleared", {"display": "none"}

FILTERS = {
    'pop'         : (80, None),
    'max_loss'    : (None, -20000),
    'max_profit'  : (500, None),
    'delta'       : (-15, 0),
    'theta'       : (10, None),
    'vega'        : (None, 0),
    'reward_risk' : (0.08, None),
}

@app.callback(Output("spread-pairs-tables", "children"), Input("interval", "n_intervals"))
def refresh_spread_pairs(_):
    if not cs.greeks_ready:
        return [html.P("⏳ Waiting for Greeks...", style={"color": "#aaa", "fontFamily": "Courier"})]
    if cs.raw_calls is None or cs.raw_calls.empty:
        return [html.P("Waiting for data...", style={"color": "#aaa"})]
    try:
        candidates = cs.get_spread_candidates(FILTERS, max_margin=70000)
    except Exception as e:
        return [html.P(f"Error: {e}", style={"color": "#ff5252", "fontFamily": "Courier"})]
    if not candidates:
        return [html.P("No pairs passed filters.", style={"color": "#aaa"})]

    blocks = []
    for expiry, df in candidates.items():
        df = df.copy()
        for col in ['delta', 'gamma', 'theta', 'vega', 'pop', 'reward_risk']:
            if col in df.columns:
                df[col] = df[col].round(4)
        card = html.Div(
            style={"backgroundColor": "#1a1a1a", "borderRadius": "8px", "border": "1px solid #333", "padding": "12px", "marginBottom": "12px"},
            children=[
                html.H4(f"Bear Call Spread — {expiry}", style={"color": "#f0c040", "margin": "0 0 8px 0", "fontSize": "13px"}),
                dash_table.DataTable(
                    columns=[{"name": c, "id": c} for c in df.columns],
                    data=df.to_dict("records"),
                    **table_style("#f0c040"),
                    style_data_conditional=[
                        {"if": {"row_index": "odd"}, "backgroundColor": "#1a1a1a"},
                        {"if": {"column_id": "max_profit"}, "color": "#00e676"},
                        {"if": {"column_id": "max_loss"},   "color": "#ff5252"},
                        {"if": {"column_id": "pop"},        "color": "#00e5ff"},
                    ],
                )
            ]
        )
        blocks.append(card)
    return blocks

if __name__ == "__main__":
    app.run(debug=False)