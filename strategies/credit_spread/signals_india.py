import pandas as pd
import time


class IndiaCreditSpreads:

    def __init__(self, broker, connection):
        self.broker = broker
        self.connection = connection
        self.raw_calls = None
        self._base_calls = None
        self.greeks_cache = None
        self.spread_tokens = []
        self.spread_latest = {}  # will be pointed at obj.latest from ui.py

    def load_from_pmcc(self, pmcc_raw_calls, min_dte=15, max_dte=45):
        today = pd.to_datetime('today').normalize()
        ex_min = today + pd.Timedelta(days=min_dte)
        ex_max = today + pd.Timedelta(days=max_dte)
        df = pmcc_raw_calls.copy()
        self._base_calls = df[(df['expiry'] >= ex_min) & (df['expiry'] <= ex_max)]
        self.raw_calls = self._base_calls.copy()
        self.spread_tokens = list(self.raw_calls['token'].astype(str))

    def get_filtered_dfs(self):
        LIQUIDITY_KEYS = ['last_traded_quantity', 'average_traded_price', 'volume_trade_for_the_day',
                          'total_buy_quantity', 'total_sell_quantity', 'open_price_of_the_day',
                          'high_price_of_the_day', 'low_price_of_the_day', 'last_traded_timestamp',
                          'open_interest']
        def is_valid_tick(tick):
            if not all(tick.get(k, 0) != 0 for k in LIQUIDITY_KEYS):
                return False
            if sum(1 for d in tick.get('best_5_sell_data', []) if d['price'] != 0 and d['quantity'] != 0) < 3:
                return False
            if sum(1 for d in tick.get('best_5_buy_data', []) if d['price'] != 0 and d['quantity'] != 0) < 3:
                return False
            return True
        valid_tokens = {t for t, tick in self.spread_latest.items() if is_valid_tick(tick)}
        self.raw_calls = self._base_calls[self._base_calls['token'].astype(str).isin(valid_tokens)]

    def apply_greeks_filters(self, pmcc_greeks_cache):
        self.greeks_cache = pmcc_greeks_cache
        df = self.raw_calls.copy()
        df['strike']     = df['strike'].round(2)
        df['expiry_str'] = df['expiry'].dt.strftime('%d%b%Y').str.upper()
        df = df.merge(self.greeks_cache, on=['strike', 'expiry_str'], how='left').drop(columns=['expiry_str'])
        rows = []
        for _, row in df.iterrows():
            tick = self.spread_latest.get(str(row['token']))
            if tick is None:
                continue
            bid = tick['best_5_buy_data'][0]['price'] / 100 if tick.get('best_5_buy_data') else None
            ask = tick['best_5_sell_data'][0]['price'] / 100 if tick.get('best_5_sell_data') else None
            row['ltp']        = tick.get('last_traded_price', 0) / 100
            row['bid']        = bid
            row['ask']        = ask
            row['spread']     = round(ask - bid, 4) if bid and ask else None
            row['day_volume'] = tick.get('volume_trade_for_the_day', 0)
            row['oi']         = tick.get('open_interest', 0)
            rows.append(row)
        df = pd.DataFrame(rows)
        df = df[(df['delta'] >= 0.08) & (df['delta'] <= 0.30)]
        df = df[df['spread'] <= 4]
        self.raw_calls = df

    def get_tick_data(self):
        """Attach live tick data to raw_calls without requiring greeks."""
        df = self.raw_calls.copy()
        if df.empty:
            return df
        rows = []
        for _, row in df.iterrows():
            tick = self.spread_latest.get(str(row['token']))
            if tick is None:
                continue
            bid = tick['best_5_buy_data'][0]['price'] / 100 if tick.get('best_5_buy_data') else None
            ask = tick['best_5_sell_data'][0]['price'] / 100 if tick.get('best_5_sell_data') else None
            row['ltp']        = tick.get('last_traded_price', 0) / 100
            row['bid']        = bid
            row['ask']        = ask
            row['spread']     = round(ask - bid, 2) if bid and ask else None
            row['day_volume'] = tick.get('volume_trade_for_the_day', 0)
            row['oi']         = tick.get('open_interest', 0)
            rows.append(row)
        return pd.DataFrame(rows)