from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
import yfinance as yf
from broker.connection import BrokerConnection
import requests


class IndiaPMCC:
    def __init__(self):
        self.iv_rank = None
        self.iv_pct = None
        self.greeks = None
        self.spot = None
        self.connection = BrokerConnection().get_client()
        self.spot = None

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
        self.iv_rank = iv_rank
        self.iv_percentile = iv_percentile
        return current_iv, iv_rank, iv_percentile

    def get_greeks_df(self):
        params = {"name": "NIFTY", "expirydate": "30JUN2026"}
        temp = self.connection.optionGreek(params)
        greeks_df = pd.DataFrame(temp['data'])
        return greeks_df

    def get_pcr_df(self):
        """ Get Put-Call Ratio """
        pcr_df = self.connection.putCallRatio()
        return pcr_df

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
        long_calls = nifty[(nifty['strike'] >= self.spot * 0.60) & (nifty['strike'] <= self.spot) &
                    (nifty['expiry'] >= today + pd.Timedelta(days=90)) & (nifty['expiry'] <= today + pd.Timedelta(days=150))].copy()

        short_calls = nifty[(nifty['strike'] >= self.spot) & (nifty['strike'] <= self.spot * 1.3) &
                    (nifty['expiry'] >= today + pd.Timedelta(days=10)) & (nifty['expiry'] <= today + pd.Timedelta(days=30))].copy()

        return self.spot, short_calls, long_calls


if __name__ == "__main__":
    obj = IndiaPMCC()
    a, b, c = obj.get_iv_stats()
    print(a, b, c)
    print(obj.get_greeks_df())
    print(obj.get_pcr_df())



