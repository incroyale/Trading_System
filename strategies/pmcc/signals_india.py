# pmcc/signals_india.py
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
import yfinance as yf
from broker.connection import BrokerConnection
import requests
import threading, time


class IndiaPMCC:
    def __init__(self):
        self.iv_stats = None
        self.greeks_cache = None
        self.spot = None
        self.broker = BrokerConnection()
        self.connection = self.broker.get_client()
        self.long_calls = None
        self.short_calls = None
        self.tokens = None
        self.latest = {}
        self._base_long_calls = None
        self._base_short_calls = None
        self.raw_calls = None

    def get_iv_stats(self):
        """
        Get Current IV, IV Rank and Percentile from yfinance (rolling 1 year)
        IV Percentile = % of days IV was below current IV
        IV Rank = (Current IV - Min IV) / (Max IV - Min IV) * 100
        """
        start_date = (datetime.today() - timedelta(days=365)).date()
        df = yf.download("^INDIAVIX", start=start_date)
        iv = df['Close'].to_numpy().flatten()  # replaces the loop + temp list
        current_iv = iv[-1]
        iv_min = iv.min()
        iv_max = iv.max()
        iv_rank = (current_iv - iv_min) / (iv_max - iv_min) * 100
        iv_percentile = np.mean(iv <= current_iv) * 100
        self.iv_stats = (current_iv, iv_rank, iv_percentile)
        return current_iv, iv_rank, iv_percentile

    def _get_tokens_by_delta(self, calls_df, is_long, min_delta=None, max_delta=None):
        if self.greeks_cache is None:
            return set()
        calls_df = calls_df.copy()
        calls_df['strike'] = calls_df['strike'].round(2)
        calls_df['expiry_str'] = calls_df['expiry'].dt.strftime('%d%b%Y').str.upper()
        merged = calls_df.merge(self.greeks_cache, on=['strike', 'expiry_str'], how='left')
        if is_long:
            filtered = merged[(merged['delta'] >= min_delta) & (merged['iv'] <= 20)]
        else:
            filtered = merged[(merged['delta'] <= max_delta) & (merged['theta'] < -3) & (merged['iv'] >= 20)]
        return set(filtered['token'].astype(str))

    def get_long_short_df(self, short_min_dte=10, short_max_dte=30, long_min_dte=90, long_max_dte=150):
        """Get Long-Short Dataframes for candidates of Long and Short leg (calls)
        Rules for choosing short and long calls:
        Long calls: strike lies in [-40% of spot, spot], expiry lies in [90, 150] days
        Short calls:strike lies in [spot, +50% of spot], expiry lies in [10, 30] days"""

        url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
        d = requests.get(url).json()
        token_df = pd.DataFrame(d)
        token_df['expiry'] = pd.to_datetime(token_df['expiry'], format='mixed').dt.date
        token_df['strike'] = pd.to_numeric(token_df['strike'], errors='coerce')
        nifty_calls = token_df[(token_df['name'] == 'NIFTY') & (token_df['instrumenttype'] == 'OPTIDX')].copy()

        # Separate CE and PE, sort by expiry and strike
        nifty_calls['option_type'] = nifty_calls['symbol'].str[-2:]
        nifty_calls = nifty_calls.sort_values(['expiry', 'strike']).reset_index(drop=True)
        nifty_calls = nifty_calls[nifty_calls['option_type'] == 'CE']
        nifty = nifty_calls[['token', 'symbol', 'name', 'expiry', 'strike']]
        nifty = nifty.copy()
        nifty['expiry'] = pd.to_datetime(nifty['expiry'])
        nifty['strike'] = nifty['strike'] / 100
        self.spot = self.connection.ltpData(exchange="NSE", tradingsymbol="NIFTY", symboltoken="99926000")['data']['ltp']
        today = pd.to_datetime('today').normalize()
        self.raw_calls = nifty.copy() # in use for credit spreads
        long_calls = nifty[(nifty['strike'] >= self.spot * 0.60) & (nifty['strike'] <= self.spot) &
                    (nifty['expiry'] >= today + pd.Timedelta(days=90)) & (nifty['expiry'] <= today + pd.Timedelta(days=540))].copy()

        short_calls = nifty[(nifty['strike'] >= self.spot) & (nifty['strike'] <= self.spot * 1.3) &
                    (nifty['expiry'] >= today + pd.Timedelta(days=10)) & (nifty['expiry'] <= today + pd.Timedelta(days=30))].copy()
        self.long_calls = long_calls
        self.short_calls = short_calls
        temp = pd.concat([long_calls, short_calls])
        self.tokens = list(temp['token'])
        self._base_long_calls = self.long_calls.copy()
        self._base_short_calls = self.short_calls.copy()

    def start_live_feed(self):
        latest = {}
        token_list = [{"exchangeType": 2, "tokens": self.tokens}]

        def on_data(wsapp, message):
            token = message['token']
            latest[token] = message

        self.broker.start_ws(token_list=token_list, mode=3, on_data=on_data)
        self.latest = latest  # shared state, updates in background

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
        latest = {t: tick for t, tick in self.latest.items() if is_valid_tick(tick)}
        long_tokens = set(self.long_calls['token'].astype(str).tolist())
        short_tokens = set(self.short_calls['token'].astype(str).tolist())
        long_df = {t: tick for t, tick in latest.items() if t in long_tokens}
        short_df = {t: tick for t, tick in latest.items() if t in short_tokens}
        return long_df, short_df

    def start_greeks_refresh(self, interval_seconds=10):
        def _loop():
            while True:
                try:
                    self._refresh_greeks_cache()
                except Exception as e:
                    print(f"[greeks thread] {e}")
                threading.Event().wait(interval_seconds)
        threading.Thread(target=_loop, daemon=True).start()

    def _refresh_greeks_cache(self):
        if self._base_long_calls is None or self._base_short_calls is None:
            return
        combined = pd.concat([self._base_long_calls, self._base_short_calls])
        unique_expiries = combined['expiry'].dt.strftime('%d%b%Y').str.upper().unique()
        all_strikes = set(combined['strike'].round(2))
        greek_rows = []
        for expiry_str in unique_expiries:
            try:
                result = self.connection.optionGreek({"name": "NIFTY", "expirydate": expiry_str})
                if not result.get('status') or not result.get('data'):
                    continue
                for entry in result['data']:
                    if entry.get('optionType') != 'CE':
                        continue
                    strike = round(float(entry['strikePrice']), 2)
                    if strike not in all_strikes:
                        continue
                    greek_rows.append({'strike': strike, 'expiry_str': expiry_str, 'iv': float(entry['impliedVolatility']),'delta': float(entry['delta']),
                        'gamma': float(entry['gamma']), 'theta': float(entry['theta']), 'vega': float(entry['vega']),})
                time.sleep(2)
            except Exception as e:
                print(f"[greeks] {expiry_str}: {e}")
        if greek_rows:
            self.greeks_cache = pd.DataFrame(greek_rows)
            self.long_calls = self._base_long_calls[self._base_long_calls['token'].astype(str).isin(self._get_tokens_by_delta(min_delta=0.70, calls_df=self._base_long_calls, is_long=True))]
            self.short_calls = self._base_short_calls[self._base_short_calls['token'].astype(str).isin(self._get_tokens_by_delta(max_delta=0.20, calls_df=self._base_short_calls, is_long=False))]

    def get_final_dfs(self):
        long_ticks, short_ticks = self.get_filtered_dfs()

        def build(ticks_dict, calls_df, is_long):
            calls_df = calls_df.copy()
            calls_df['token'] = calls_df['token'].astype(str)
            rows = []
            for token, tick in ticks_dict.items():
                match = calls_df[calls_df['token'] == token]
                if match.empty:
                    continue
                bid = tick['best_5_buy_data'][0]['price'] / 100 if tick.get('best_5_buy_data') else None
                ask = tick['best_5_sell_data'][0]['price'] / 100 if tick.get('best_5_sell_data') else None
                rows.append({'token': token, 'strike': match['strike'].values[0], 'expiry': match['expiry'].values[0],
                    'ltp': tick.get('last_traded_price', 0) / 100, 'bid': bid, 'ask': ask, 'spread': round(ask - bid, 2) if bid and ask else None,
                    'day_volume': tick.get('volume_trade_for_the_day', 0), 'oi': tick.get('open_interest', 0),})
            df = pd.DataFrame(rows)
            if df.empty or self.greeks_cache is None:
                return df
            df['strike'] = df['strike'].round(2)
            df['expiry_str'] = pd.to_datetime(df['expiry']).dt.strftime('%d%b%Y').str.upper()
            df = df.merge(self.greeks_cache, on=['strike', 'expiry_str'], how='left').drop(columns=['expiry_str'])
            if not df.empty:
                if is_long:
                    df = df[df['spread'] <= 50]
                else:
                    df = df[df['spread'] <= 2]
            return df
        return build(long_ticks, self.long_calls, is_long=True), build(short_ticks, self.short_calls, is_long=False)