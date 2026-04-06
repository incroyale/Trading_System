# credit_spread/spread_builder.py
import pandas as pd
import numpy as np
from itertools import combinations
from scipy.stats import norm
import time
import yfinance as yf

LOT_SIZE = 65
_spot_cache = {"value": None, "ts": 0}

def get_spot(ttl=5):
    if time.time() - _spot_cache["ts"] > ttl:
        _spot_cache["value"] = yf.Ticker("^NSEI").fast_info['last_price']
        _spot_cache["ts"] = time.time()
    return _spot_cache["value"]

def _d2(S, K, r, sigma, T):
    if sigma <= 0 or T <= 0:
        return np.nan
    return (np.log(S / K) + (r - 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))

def prob_of_profit_bear_call(S, breakeven, sigma, T, r=0.065):
    """P(spot < breakeven at expiry) using risk-neutral N(d2)."""
    d2 = _d2(S, breakeven, r, sigma, T)
    if np.isnan(d2):
        return np.nan
    return 1 - norm.cdf(d2)

def generate_pairs(df, spot, T, r=0.065):
    """
    df     : candidate legs df for ONE expiry, must have columns:
             strike, ltp, iv, delta, gamma, theta, vega, token
    spot   : current Nifty spot
    T      : time to expiry in years
    returns: dataframe of all valid bear call spread pairs with position stats
    """
    records = []
    rows = df.reset_index(drop=True)

    for i, j in combinations(range(len(rows)), 2):
        # bear call spread: sell lower strike, buy higher strike
        low, high = (rows.iloc[i], rows.iloc[j]) if rows.iloc[i]['strike'] < rows.iloc[j]['strike'] else (rows.iloc[j], rows.iloc[i])

        sell = low   # short leg (lower strike, higher premium)
        buy  = high  # long leg  (higher strike, lower premium)

        net_credit   = sell['ltp'] - buy['ltp']
        if net_credit <= 0:
            continue   # skip if not a credit

        width        = buy['strike'] - sell['strike']
        max_profit   = net_credit * LOT_SIZE
        max_loss     = (width - net_credit) * LOT_SIZE
        breakeven    = sell['strike'] + net_credit

        # portfolio greeks (sell = -1, buy = +1)
        delta  = -sell['delta'] + buy['delta']
        gamma  = -sell['gamma'] + buy['gamma']
        theta  = -sell['theta'] + buy['theta']
        vega   = -sell['vega']  + buy['vega']

        # use avg IV of the two legs for PoP
        avg_iv = (sell['iv'] + buy['iv']) / 2
        pop    = prob_of_profit_bear_call(spot, breakeven, avg_iv, T, r)

        records.append({
            'sell_strike'  : sell['strike'],
            'buy_strike'   : buy['strike'],
            'sell_token'   : sell['token'],
            'buy_token'    : buy['token'],
            'sell_ltp'     : sell['ltp'],
            'buy_ltp'      : buy['ltp'],
            'net_credit'   : round(net_credit, 2),
            'width'        : width,
            'max_profit'   : round(max_profit, 2),
            'max_loss'     : round(-abs(max_loss), 2),   # always negative
            'breakeven'    : round(breakeven, 2),
            'pop'          : round(pop * 100, 2),         # as %
            'delta'        : round(delta, 4),
            'gamma'        : round(gamma, 6),
            'theta'        : round(theta, 4),
            'vega'         : round(vega, 4),
            'reward_risk'  : round(max_profit / abs(max_loss), 4) if max_loss != 0 else np.nan,
        })

    return pd.DataFrame(records)


def filter_pairs(pairs_df, filters: dict):
    """
    filters is a dict of column -> (min, max), use None to skip a bound.
    Example:
    {
        'pop'         : (70, None),
        'max_loss'    : (None, -3000),   # max_loss <= -3000 means at most 3k loss
        'max_profit'  : (500, None),
        'delta'       : (-10, 0),
        'theta'       : (20, None),
        'vega'        : (None, 0),
        'reward_risk' : (0.1, None),
        'net_credit'  : (15, None),
        'width'       : (None, 300),
    }
    """
    df = pairs_df.copy()
    for col, (lo, hi) in filters.items():
        if col not in df.columns:
            continue
        if lo is not None:
            df = df[df[col] >= lo]
        if hi is not None:
            df = df[df[col] <= hi]
    return df.reset_index(drop=True)


def build_spread_candidates(candidates_by_expiry, days_to_expiry, filters, connection=None, max_margin=None, r=0.065):
    spot = get_spot()
    result = {}
    for expiry, df in candidates_by_expiry.items():
        if df.empty or len(df) < 2:
            continue
        T = days_to_expiry[expiry] / 365
        pairs = generate_pairs(df, spot, T, r)
        if pairs.empty:
            continue
        filtered = filter_pairs(pairs, filters)
        if filtered.empty:
            continue

        # margin filter — only on pairs that passed everything else
        if connection is not None and max_margin is not None:
            margins = []
            for _, row in filtered.iterrows():
                m = get_margin(connection, row['sell_token'], row['buy_token'])
                margins.append(m)
                time.sleep(0.12)   # stay under 10 req/s
            filtered['margin'] = margins
            filtered = filtered[filtered['margin'].notna() & (filtered['margin'] <= max_margin)]
        if not filtered.empty:
            result[expiry] = filtered.reset_index(drop=True)
    return result

def get_margin(connection, sell_token, buy_token, qty=65):
    try:
        resp = connection.getMarginApi({"positions":[
            {"exchange": "NFO", "qty": qty, "price": 0, "productType": "INTRADAY",
             "token": str(sell_token), "tradeType": "SELL", "orderType": "MARKET"},
            {"exchange": "NFO", "qty": qty, "price": 0, "productType": "INTRADAY",
             "token": str(buy_token), "tradeType": "BUY", "orderType": "MARKET"},
        ]})
        if resp.get("status"):
            return resp["data"]["totalMarginRequired"]
    except Exception as e:
        print(f"[margin] {e}")
    return None