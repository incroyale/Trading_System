# credit_spread/spread_builder.py
import pandas as pd
from itertools import combinations
import os


def get_spreads(dir_path, min_pop=0.75, max_net_delta=2, min_net_theta=25.0, max_net_vega=0.0, max_net_gamma=0.0,min_credit=25.0, max_width=200,
                min_rr=0.25, min_max_profit=1500.0, max_max_profit=10000, min_max_loss=2000.0, max_max_loss=10000.0, lot_size=65):
    """
    Process all CSV files in a directory, generate bear call spreads,
    filter with strict criteria, and return a single concatenated DataFrame.

    dir_path: path to directory containing CSVs
    lot_size: number of contracts per spread
    """

    all_results = []

    # loop through all CSV files
    for filename in os.listdir(dir_path):
        if filename.endswith(".csv"):
            filepath = os.path.join(dir_path, filename)
            df = pd.read_csv(filepath)

            # attempt to infer expiry from filename if present, else None
            expiry = None
            if "expiry" in df.columns:
                expiry = df['expiry'].iloc[0]
            else:
                # optional: parse expiry from filename like "calls_2026-04-28.csv"
                try:
                    expiry = filename.split("_")[-1].replace(".csv", "")
                except:
                    expiry = None

            # loop through combinations of options
            for (i, short), (j, long) in combinations(df.iterrows(), 2):
                if short['strike'] >= long['strike']:
                    short, long = long, short

                strike_width = long['strike'] - short['strike']
                if strike_width > max_width:
                    continue

                short_mid = (short['bid'] + short['ask']) / 2
                long_mid = (long['bid'] + long['ask']) / 2
                net_credit = (short_mid - long_mid) * lot_size
                max_loss = (strike_width - (short_mid - long_mid)) * lot_size
                reward_risk = net_credit / max_loss if max_loss != 0 else 0

                # Position-aware Greeks scaled by lot size
                net_delta = ((-short['delta'] + long['delta']) * lot_size)
                net_theta = ((-short['theta'] + long['theta']) * lot_size)
                net_vega = ((-short['vega'] + long['vega']) * lot_size)
                net_gamma = ((-short['gamma'] + long['gamma']) * lot_size)

                # Filters
                if max_loss <= 0:
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

                pop = 1 - short['delta']
                if pop < min_pop:
                    continue

                all_results.append({
                    'expiry': expiry,
                    'short_strike': short['strike'],
                    'long_strike': long['strike'],
                    'short_token': short['token'],
                    'long_token': long['token'],
                    'width': strike_width,
                    'net_credit': round(net_credit, 2),
                    'max_profit': round(net_credit, 2),
                    'max_loss': round(max_loss, 2),
                    'reward_risk': round(reward_risk, 4),
                    'net_delta': round(net_delta, 4),
                    'net_theta': round(net_theta, 4),
                    'net_vega': round(net_vega, 4),
                    'net_gamma': round(net_gamma, 6),
                    'pop': round(pop, 4),
                })
    if not all_results:
        return pd.DataFrame()
    # return concatenated dataframe sorted by reward/risk
    return pd.DataFrame(all_results).sort_values('reward_risk', ascending=False).reset_index(drop=True)





