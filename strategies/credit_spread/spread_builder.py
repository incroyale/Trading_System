import pandas as pd
from itertools import combinations
import os

_REQUIRED_COLS = {'delta', 'gamma', 'theta', 'vega', 'iv', 'bid', 'ask'}


# ─────────────────────────────────────────────
# LOAD CSV
# ─────────────────────────────────────────────
def _load_csv(filepath: str) -> pd.DataFrame | None:
    df = pd.read_csv(filepath)

    if not _REQUIRED_COLS.issubset(df.columns):
        print(f"[spread_builder] skipping {os.path.basename(filepath)} — greeks missing")
        return None

    df = df.dropna(subset=list(_REQUIRED_COLS))
    if df.empty:
        return None

    return df


# ─────────────────────────────────────────────
# INTERNAL SAFE CALCULATOR 
# ─────────────────────────────────────────────
def _calc_spread(short, long, lot_size: int):

    strike_width = abs(long['strike'] - short['strike'])
    if strike_width <= 0:
        return None

    short_mid = (short['bid'] + short['ask']) / 2
    long_mid  = (long['bid'] + long['ask']) / 2

    # per-unit credit (internal only)
    credit_u = short_mid - long_mid

    if credit_u <= 0:
        return None

    # INR value
    net_credit = credit_u * lot_size
    max_loss   = (strike_width - credit_u) * lot_size

    if max_loss <= 0:
        return None

    reward_risk = net_credit / max_loss

    return {
        "strike_width": strike_width,
        "net_credit": net_credit,
        "max_loss": max_loss,
        "reward_risk": reward_risk,
        "credit_u": credit_u,
    }


# ─────────────────────────────────────────────
# CALL SPREADS
# ─────────────────────────────────────────────
def get_call_spreads(
    dir_path: str,
    min_pop: float       = 0.7,
    max_net_delta: float = 2.5,
    min_net_theta: float = 25.0,
    max_net_vega: float  = 0.0,
    max_net_gamma: float = 0.0,
    min_credit: float    = 25.0,
    max_width: float     = 200,
    min_rr: float        = 0.2,
    min_max_profit: float = 1000.0,
    max_max_profit: float = 10000.0,
    min_max_loss: float   = 1000.0,
    max_max_loss: float   = 10000.0,
    lot_size: int         = 65,
) -> pd.DataFrame:

    results = []

    for filename in sorted(os.listdir(dir_path)):
        if not filename.endswith('_CE.csv'):
            continue

        df = _load_csv(os.path.join(dir_path, filename))
        if df is None:
            continue

        expiry = df['expiry'].iloc[0] if 'expiry' in df.columns else filename

        for (_, short), (_, long) in combinations(df.iterrows(), 2):

            if short['strike'] > long['strike']:
                short, long = long, short

            calc = _calc_spread(short, long, lot_size)
            if calc is None:
                continue

            strike_width = calc["strike_width"]

            # ── ORIGINAL FILTER ──
            if strike_width > max_width:
                continue

            if not (min_max_profit <= calc["net_credit"] <= max_max_profit):
                continue

            if not (min_max_loss <= calc["max_loss"] <= max_max_loss):
                continue

            if calc["net_credit"] < min_credit:
                continue

            if calc["reward_risk"] < min_rr:
                continue

            # Greeks 
            net_delta = (-short['delta'] + long['delta']) * lot_size
            net_theta = (-short['theta'] + long['theta']) * lot_size
            net_vega  = (-short['vega'] + long['vega']) * lot_size
            net_gamma = (-short['gamma'] + long['gamma']) * lot_size
            pop = 1 - short['delta']

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

            results.append({
                "expiry": expiry,
                "short_strike": short["strike"],
                "long_strike": long["strike"],
                "short_token": short["token"],
                "long_token": long["token"],
                "width": strike_width,
                "net_credit": calc["net_credit"],
                "max_profit": calc["net_credit"],   
                "max_loss": calc["max_loss"],
                "reward_risk": calc["reward_risk"],

                "net_delta": net_delta,
                "net_theta": net_theta,
                "net_vega": net_vega,
                "net_gamma": net_gamma,
                "pop": pop,
            })

    if not results:
        return pd.DataFrame()

    return (
        pd.DataFrame(results)
        .sort_values("reward_risk", ascending=False)
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────
# PUT SPREADS 
# ─────────────────────────────────────────────
def get_put_spreads(
    dir_path: str,
    min_pop: float       = 0.7,
    max_net_delta: float = 2.5,
    min_net_theta: float = 25.0,
    max_net_vega: float  = 0.0,
    max_net_gamma: float = 0.0,
    min_credit: float    = 25.0,
    max_width: float     = 200,
    min_rr: float        = 0.2,
    min_max_profit: float = 1000.0,
    max_max_profit: float = 10000.0,
    min_max_loss: float   = 1000.0,
    max_max_loss: float   = 10000.0,
    lot_size: int         = 65,
) -> pd.DataFrame:

    results = []

    for filename in sorted(os.listdir(dir_path)):
        if not filename.endswith('_PE.csv'):
            continue

        df = _load_csv(os.path.join(dir_path, filename))
        if df is None:
            continue

        expiry = df['expiry'].iloc[0] if 'expiry' in df.columns else filename

        for (_, a), (_, b) in combinations(df.iterrows(), 2):

            short, long = (a, b) if a['strike'] > b['strike'] else (b, a)

            calc = _calc_spread(short, long, lot_size)
            if calc is None:
                continue

            strike_width = calc["strike_width"]

            if strike_width > max_width:
                continue

            if not (min_max_profit <= calc["net_credit"] <= max_max_profit):
                continue

            if not (min_max_loss <= calc["max_loss"] <= max_max_loss):
                continue

            if calc["net_credit"] < min_credit:
                continue

            if calc["reward_risk"] < min_rr:
                continue

            net_delta = (-short['delta'] + long['delta']) * lot_size
            net_theta = (-short['theta'] + long['theta']) * lot_size
            net_vega  = (-short['vega'] + long['vega']) * lot_size
            net_gamma = (-short['gamma'] + long['gamma']) * lot_size
            pop = 1 - abs(short['delta'])

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

            if not (-0.30 <= short['delta'] <= -0.08):
                continue

            results.append({
                "expiry": expiry,
                "short_strike": short["strike"],
                "long_strike": long["strike"],
                "short_token": short["token"],
                "long_token": long["token"],

                "width": strike_width,

                "net_credit": calc["net_credit"],
                "max_profit": calc["net_credit"],
                "max_loss": calc["max_loss"],
                "reward_risk": calc["reward_risk"],

                "net_delta": net_delta,
                "net_theta": net_theta,
                "net_vega": net_vega,
                "net_gamma": net_gamma,
                "pop": pop,
            })

    if not results:
        return pd.DataFrame()

    return (
        pd.DataFrame(results)
        .sort_values("reward_risk", ascending=False)
        .reset_index(drop=True)
    )
