from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output
from strategies.pmcc.signals_india import IndiaPMCC
import pandas as pd

app = Dash(__name__)
obj = IndiaPMCC()

spot, short_call, long_call = obj.get_long_short_df()

for df in [long_call, short_call]:
    df.drop(columns=['name'], inplace=True, errors='ignore')
    df['expiry'] = pd.to_datetime(df['expiry']).dt.strftime('%d %b %Y')

COL_WIDTHS = {"token": 90, "symbol": 160, "expiry": 100, "strike": 80}

def make_columns(df):
    return [{"name": c, "id": c} for c in df.columns]

def table_style(header_color):
    return dict(
        style_table={"overflowX": "auto", "marginBottom": "10px"},
        style_header={"backgroundColor": "#222", "color": header_color, "fontWeight": "bold", "border": "1px solid #333", "whiteSpace": "nowrap"},
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
    expiries = df['expiry'].unique()
    blocks   = []
    row      = []

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
            blocks.append(
                html.Div(row, style={"display": "flex", "flexWrap": "wrap", "gap": "12px", "marginBottom": "12px"})
            )
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

        html.H2("Candidate Long Calls", style={"color": "white", "marginBottom": "12px"}),
        *make_expiry_tables(long_call, "Long", "#00e5ff"),

        html.Hr(style={"borderColor": "#333", "margin": "24px 0"}),

        html.H2("Candidate Short Calls", style={"color": "white", "marginBottom": "12px"}),
        *make_expiry_tables(short_call, "Short", "#ff6b6b"),

        dcc.Interval(id="interval", interval=5000, n_intervals=0),
    ]
)

@app.callback(
    Output("current-iv", "children"),
    Output("iv-rank",    "children"),
    Output("iv-pct",     "children"),
    Input("interval",    "n_intervals"))
def refresh(_):
    iv, iv_rank, iv_pct = obj.get_iv_stats()
    return f"Current IV:    {iv:.2f}", f"IV Rank:       {iv_rank:.1f}%", f"IV Percentile: {iv_pct:.1f}%"

if __name__ == "__main__":
    app.run(debug=False)