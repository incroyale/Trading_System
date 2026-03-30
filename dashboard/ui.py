from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output
from strategies.pmcc.signals_india import IndiaPMCC
import pandas as pd

app = Dash(__name__)
obj = IndiaPMCC()
obj.get_long_short_df()

def ticks_to_df(ticks_dict, calls_df):
    rows = []
    calls_df = calls_df.copy()
    calls_df['token'] = calls_df['token'].astype(str)
    for token, tick in ticks_dict.items():
        match = calls_df[calls_df['token'] == token]
        if match.empty:
            continue
        rows.append({
            'token':      token,
            'strike':     match['strike'].values[0],
            'expiry':     match['expiry'].values[0],
            'ltp':        tick.get('last_traded_price', 0) / 100,
            'day_volume': tick.get('volume_trade_for_the_day', 0),
            'total_buy':  tick.get('total_buy_quantity', 0),
            'total_sell': tick.get('total_sell_quantity', 0),
            'oi':         tick.get('open_interest', 0),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df['expiry'] = pd.to_datetime(df['expiry']).dt.strftime('%d %b %Y')
        df = df.sort_values(['expiry', 'strike']).reset_index(drop=True)
    return df

COL_WIDTHS = {"token": 90, "strike": 80, "ltp": 80, "day_volume": 100, "total_buy": 90, "total_sell": 90, "oi": 90}

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
        card = html.Div(
            style={"flex": "1 1 30%", "minWidth": "280px", "maxWidth": "33%",
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
        if len(row) == 3 or i == len(expiries) - 1:
            blocks.append(html.Div(row, style={"display": "flex", "flexWrap": "wrap",
                                               "gap": "12px", "marginBottom": "12px"}))
            row = []
    return blocks

app.layout = html.Div(
    style={"backgroundColor": "#111", "padding": "20px", "fontFamily": "Courier"},
    children=[
        html.Div(
            style={"display": "inline-block", "backgroundColor": "#1a1a1a", "padding": "12px 18px",
                   "borderRadius": "8px", "border": "1px solid #333", "marginBottom": "24px"},
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

        dcc.Interval(id="interval", interval=5000, n_intervals=0),
    ]
)

@app.callback(
    Output("current-iv",     "children"),
    Output("iv-rank",        "children"),
    Output("iv-pct",         "children"),
    Output("long-call-tables",  "children"),
    Output("short-call-tables", "children"),
    Input("interval", "n_intervals"))
def refresh(_):
    iv, iv_rank, iv_pct = obj.get_iv_stats()

    long_df, short_df = obj.filter_long_short_calls()
    long_call  = ticks_to_df(long_df,  obj.long_calls)
    short_call = ticks_to_df(short_df, obj.short_calls)

    return (
        f"Current IV:    {iv:.2f}",
        f"IV Rank:       {iv_rank:.1f}%",
        f"IV Percentile: {iv_pct:.1f}%",
        make_expiry_tables(long_call,  "Long",  "#00e5ff"),
        make_expiry_tables(short_call, "Short", "#ff6b6b"),
    )

if __name__ == "__main__":
    app.run(debug=False)