# strategies/credit_spread/spread_builder.py
import pandas as pd
from itertools import combinations
import os

_REQUIRED_COLS = {'delta', 'gamma', 'theta', 'vega', 'iv', 'bid', 'ask'}


def _load_csv(filepath: str) -> pd.DataFrame | None:
    """
    Read a spread leg CSV and return it only if greeks are present and valid.
    Returns None if the file should be skipped (greeks not ready yet).
    """
    df = pd.read_csv(filepath)
    if not _REQUIRED_COLS.issubset(df.columns):
        print(f"[spread_builder] skipping {os.path.basename(filepath)} — greeks columns missing")
        return None
    df = df.dropna(subset=list(_REQUIRED_COLS))
    if df.empty:
        return None
    return df


def get_call_spreads(
    dir_path: str,
    min_pop: float       = 0.75,
    max_net_delta: float = 2.5,
    min_net_theta: float = 25.0,
    max_net_vega: float  = 0.0,
    max_net_gamma: float = 0.0,
    min_credit: float    = 25.0,
    max_width: float     = 200,
    min_rr: float        = 0.25,
    min_max_profit: float = 1500.0,
    max_max_profit: float = 10000.0,
    min_max_loss: float   = 2000.0,
    max_max_loss: float   = 10000.0,
    lot_size: int         = 65,
) -> pd.DataFrame:
    """
    Process all CE CSVs in dir_path, generate bear call spreads,
    apply filters, and return a sorted DataFrame.

    Bear call spread mechanics
    ──────────────────────────
    • Sell the LOWER  strike call  (short leg, closer to spot)
    • Buy  the HIGHER strike call  (long leg,  further OTM)
    • Net credit  = short_mid - long_mid
    • Max loss    = (strike_width - net_credit_per_unit) × lot_size
    • POP         = 1 - short_delta
    """
    all_results = []

    for filename in sorted(os.listdir(dir_path)):
        if not filename.endswith('_CE.csv') or filename.endswith('_PE.csv'):
            continue

        filepath = os.path.join(dir_path, filename)
        df = _load_csv(filepath)
        if df is None:
            continue

        expiry = (
            df['expiry'].iloc[0]
            if 'expiry' in df.columns
            else filename.replace('_CE.csv', '')
        )

        for (_, short), (_, long) in combinations(df.iterrows(), 2):
            # Bear call: short the lower strike
            if short['strike'] > long['strike']:
                short, long = long, short

            strike_width = long['strike'] - short['strike']
            if strike_width <= 0 or strike_width > max_width:
                continue

            short_mid   = (short['bid'] + short['ask']) / 2
            long_mid    = (long['bid']  + long['ask'])  / 2
            net_credit  = (short_mid - long_mid) * lot_size
            max_loss    = (strike_width - (short_mid - long_mid)) * lot_size
            reward_risk = net_credit / max_loss if max_loss > 0 else 0

            net_delta = (-short['delta'] + long['delta']) * lot_size
            net_theta = (-short['theta'] + long['theta']) * lot_size
            net_vega  = (-short['vega']  + long['vega'])  * lot_size
            net_gamma = (-short['gamma'] + long['gamma']) * lot_size
            pop       = 1 - short['delta']

            # ── Filters ──────────────────────────────────────────────────────
            if max_loss <= 0:
                continue
            if net_credit <= 0:
                continue
            if not (min_max_profit <= net_credit <= max_max_profit):
                continue
            if not (min_max_loss <= max_loss <= max_max_loss):
                continue
            if net_credit < min_credit:
                continue
            if reward_risk < min_rr:
                continue
            if abs(net_delta) > max_net_delta:
                continue
            if net_theta < min_net_theta:
                continue
            if net_vega > max_net_vega:
                continue
            if net_gamma > max_net_gamma:
                continue
            if pop < min_pop:
                continue

            all_results.append({
                'expiry':       expiry,
                'short_strike': short['strike'],
                'long_strike':  long['strike'],
                'short_token':  short['token'],
                'long_token':   long['token'],
                'width':        round(strike_width, 2),
                'net_credit':   round(net_credit, 2),
                'max_profit':   round(net_credit, 2),
                'max_loss':     round(max_loss, 2),
                'reward_risk':  round(reward_risk, 4),
                'net_delta':    round(net_delta, 4),
                'net_theta':    round(net_theta, 4),
                'net_vega':     round(net_vega, 4),
                'net_gamma':    round(net_gamma, 6),
                'pop':          round(pop, 4),
            })

    if not all_results:
        return pd.DataFrame()
    return (
        pd.DataFrame(all_results)
        .sort_values('reward_risk', ascending=False)
        .reset_index(drop=True)
    )


def get_put_spreads(
    dir_path: str,
    min_pop: float       = 0.75,
    max_net_delta: float = 2.5,
    min_net_theta: float = 25.0,
    max_net_vega: float  = 0.0,
    max_net_gamma: float = 0.0,
    min_credit: float    = 25.0,
    max_width: float     = 200,
    min_rr: float        = 0.25,
    min_max_profit: float = 1500.0,
    max_max_profit: float = 10000.0,
    min_max_loss: float   = 2000.0,
    max_max_loss: float   = 10000.0,
    lot_size: int         = 65,
) -> pd.DataFrame:
    """
    Process all *_PE.csv files in dir_path, generate bull put spreads,
    apply filters, and return a sorted DataFrame.

    Bull put spread mechanics
    ─────────────────────────
    • Sell the HIGHER strike put  (short leg, delta closer to 0, e.g. -0.15)
    • Buy  the LOWER  strike put  (long leg,  delta more negative, e.g. -0.05)
    • Net credit  = short_mid - long_mid
    • Max loss    = (strike_width - net_credit_per_unit) × lot_size
    • POP         = 1 - |short_delta|

    Greeks (position-aware)
    ───────────────────────
    net_delta = (-short_delta + long_delta) × lot   → positive (bullish)
    net_theta = (-short_theta + long_theta) × lot   → positive (time decay earns)
    net_vega  = (-short_vega  + long_vega)  × lot   → negative (short vega)
    net_gamma = (-short_gamma + long_gamma) × lot   → negative (short gamma)
    """
    all_results = []

    for filename in sorted(os.listdir(dir_path)):
        if not filename.endswith('_PE.csv'):
            continue

        filepath = os.path.join(dir_path, filename)
        df = _load_csv(filepath)
        if df is None:
            continue

        expiry = (
            df['expiry'].iloc[0]
            if 'expiry' in df.columns
            else filename.replace('_PE.csv', '')
        )

        # Sort descending so higher strike (short leg) comes first — clearer intent
        df = df.sort_values('strike', ascending=False).reset_index(drop=True)

        for (_, row_a), (_, row_b) in combinations(df.iterrows(), 2):
            # Bull put: short the higher strike
            short, long = (row_a, row_b) if row_a['strike'] > row_b['strike'] else (row_b, row_a)

            strike_width = short['strike'] - long['strike']
            if strike_width <= 0 or strike_width > max_width:
                continue

            short_mid   = (short['bid'] + short['ask']) / 2
            long_mid    = (long['bid']  + long['ask'])  / 2
            net_credit  = (short_mid - long_mid) * lot_size
            max_loss    = (strike_width - (short_mid - long_mid)) * lot_size
            reward_risk = net_credit / max_loss if max_loss > 0 else 0

            # Put deltas are negative; position delta ends up positive (bullish)
            net_delta = (-short['delta'] + long['delta']) * lot_size
            net_theta = (-short['theta'] + long['theta']) * lot_size
            net_vega  = (-short['vega']  + long['vega'])  * lot_size
            net_gamma = (-short['gamma'] + long['gamma']) * lot_size
            pop       = 1 - abs(short['delta'])

            # ── Filters ──────────────────────────────────────────────────────
            if max_loss <= 0:
                continue
            if net_credit <= 0:
                continue
            if not (min_max_profit <= net_credit <= max_max_profit):
                continue
            if not (min_max_loss <= max_loss <= max_max_loss):
                continue
            if net_credit < min_credit:
                continue
            if reward_risk < min_rr:
                continue
            if abs(net_delta) > max_net_delta:
                continue
            if net_theta < min_net_theta:
                continue
            if net_vega > max_net_vega:
                continue
            if net_gamma > max_net_gamma:
                continue
            if pop < min_pop:
                continue
            # Short-leg delta must be in the target OTM range [-0.30, -0.08]
            if not (-0.30 <= short['delta'] <= -0.08):
                continue

            all_results.append({
                'expiry':       expiry,
                'short_strike': short['strike'],   # higher strike (sold)
                'long_strike':  long['strike'],    # lower  strike (bought)
                'short_token':  short['token'],
                'long_token':   long['token'],
                'width':        round(strike_width, 2),
                'net_credit':   round(net_credit, 2),
                'max_profit':   round(net_credit, 2),
                'max_loss':     round(max_loss, 2),
                'reward_risk':  round(reward_risk, 4),
                'net_delta':    round(net_delta, 4),
                'net_theta':    round(net_theta, 4),
                'net_vega':     round(net_vega, 4),
                'net_gamma':    round(net_gamma, 6),
                'pop':          round(pop, 4),
            })

    if not all_results:
        return pd.DataFrame()
    return (
        pd.DataFrame(all_results)
        .sort_values('reward_risk', ascending=False)
        .reset_index(drop=True)
    )