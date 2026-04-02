from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output
from strategies.pmcc.signals_india import IndiaPMCC
from strategies.credit_spread.signals_india import IndiaCreditSpreads
import pandas as pd

app = Dash(__name__)

# ── shared objects ────────────────────────────────────────────────────────────
obj  = IndiaPMCC()
obj.get_long_short_df()
obj.start_live_feed()
obj.start_greeks_refresh(interval_seconds=15)
obj.iv_stats = obj.get_iv_stats()

cs = IndiaCreditSpreads(broker=obj.broker, connection=obj.connection)
cs.load_from_pmcc(obj.raw_calls)
cs.start_spread_feed()

# hook credit-spreads greek filter into PMCC's refresh loop
_orig_refresh = obj._refresh_greeks_cache
def _patched_refresh():
    _orig_refresh()
    if obj.greeks_cache is not None:
        cs.get_filtered_dfs()
        cs.apply_greeks_filters(obj.greeks_cache)
obj._refresh_greeks_cache = _patched_refresh

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
        style_header={"backgroundColor": "#222", "color": header_color, "fontWeight": "bold",
                      "border": "1px solid #333", "whiteSpace": "nowrap"},
        style_cell={"backgroundColor": "#111", "color": "white", "border": "1px solid #222",
                    "fontFamily": "Courier", "fontSize": "13px", "padding": "6px 10px",
                    "whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"},
        style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#1a1a1a"}],
        style_cell_conditional=[
            {"if": {"column_id": col}, "width": f"{w}px", "minWidth": f"{w}px", "maxWidth": f"{w}px"}
            for col, w in COL_WIDTHS.items()
        ],
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
            style={"flex": "1 1 90%", "minWidth": "280px", "maxWidth": "98%",
                   "backgroundColor": "#1a1a1a", "borderRadius": "8px",
                   "border": "1px solid #333", "padding": "12px", "boxSizing": "border-box"},
            children=[
                html.H4(f"{label} — {expiry}",
                        style={"color": header_color, "margin": "0 0 8px 0", "fontSize": "13px"}),
                dash_table.DataTable(
                    columns=make_columns(subset),
                    data=subset.to_dict("records"),
                    **table_style(header_color),
                )
            ]
        )
        row.append(card)
        if len(row) == 1 or i == len(expiries) - 1:
            blocks.append(html.Div(row, style={"display": "flex", "flexWrap": "wrap",
                                               "gap": "12px", "marginBottom": "12px"}))
            row = []
    return blocks

def format_expiry(df):
    if not df.empty and 'expiry' in df.columns:
        df['expiry'] = pd.to_datetime(df['expiry']).dt.strftime('%d %b %Y')
    return df

# ── layout ────────────────────────────────────────────────────────────────────
TAB_STYLE        = {"backgroundColor": "#111", "color": "#aaa",  "border": "1px solid #333"}
TAB_SELECTED     = {"backgroundColor": "#1a1a1a", "color": "white", "border": "1px solid #555"}

app.layout = html.Div(
    style={"backgroundColor": "#111", "padding": "20px", "fontFamily": "Courier"},
    children=[
        dcc.Tabs(
            id="tabs",
            value="pmcc",
            style={"marginBottom": "20px"},
            colors={"background": "#111", "primary": "#00e5ff", "border": "#333"},
            children=[
                # ── PMCC tab ──────────────────────────────────────────────────
                dcc.Tab(label="PMCC", value="pmcc",
                        style=TAB_STYLE, selected_style=TAB_SELECTED,
                        children=[
                            html.Div(
                                style={"display": "inline-block", "backgroundColor": "#1a1a1a",
                                       "padding": "12px 18px", "borderRadius": "8px",
                                       "border": "1px solid #333", "marginBottom": "24px"},
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

                # ── Credit Spreads tab ────────────────────────────────────────
                dcc.Tab(label="Credit Spreads", value="credit-spreads",
                        style=TAB_STYLE, selected_style=TAB_SELECTED,
                        children=[
                            html.H2("Candidate Credit Spread Legs",
                                    style={"color": "white", "marginBottom": "12px"}),
                            html.Div(id="cs-tables"),
                        ]),
            ]
        ),

        dcc.Interval(id="interval", interval=500, n_intervals=0),
    ]
)

# ── callbacks ─────────────────────────────────────────────────────────────────
@app.callback(
    Output("current-iv",       "children"),
    Output("iv-rank",          "children"),
    Output("iv-pct",           "children"),
    Output("long-call-tables", "children"),
    Output("short-call-tables","children"),
    Input("interval", "n_intervals"))
def refresh_pmcc(_):
    iv, iv_rank, iv_pct = obj.iv_stats
    long_call, short_call = obj.get_final_dfs()
    format_expiry(long_call)
    format_expiry(short_call)
    return (
        f"Current IV:    {iv:.2f}",
        f"IV Rank:       {iv_rank:.1f}%",
        f"IV Percentile: {iv_pct:.1f}%",
        make_expiry_tables(long_call,  "Long",  "#00e5ff"),
        make_expiry_tables(short_call, "Short", "#ff6b6b"),
    )

@app.callback(
    Output("cs-tables", "children"),
    Input("interval", "n_intervals"))
def refresh_cs(_):
    df = cs.raw_calls.copy() if cs.raw_calls is not None else pd.DataFrame()
    if df.empty:
        return [html.P("Waiting for data...", style={"color": "#aaa"})]
    desired = ['token', 'strike', 'expiry', 'ltp', 'bid', 'ask', 'spread',
               'day_volume', 'oi', 'iv', 'delta', 'gamma', 'theta', 'vega']
    cols = [c for c in desired if c in df.columns]
    df = df[cols]
    format_expiry(df)
    return make_expiry_tables(df, "Credit Spread", "#a0e080")

if __name__ == "__main__":
    app.run(debug=False)