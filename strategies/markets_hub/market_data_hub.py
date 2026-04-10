# strategies/markets_hub/market_data_hub.py
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import yfinance as yf
from broker.connection import BrokerConnection
import requests
import threading
import time


class IndiaMarketHub:
    """
    Central market data hub for credit spread strategies.
    Provides:
      - Spot price
      - IV stats (rank + percentile)
      - CE universe  → raw_calls  (bear call spreads)
      - PE universe  → raw_puts   (bull put spreads)
      - Unified greeks cache for both CE and PE
      - Live websocket feed shared across all strategies
    """

    def __init__(self):
        self.broker      = BrokerConnection()
        self.connection  = self.broker.get_client()

        self.spot        = None
        self.iv_stats    = None
        self.latest      = {}        # shared live tick dict {token: tick}
        self.tokens      = []        # all subscribed tokens

        # Universe DataFrames
        self.raw_calls   = None      # CE options for bear call spreads
        self.raw_puts    = None      # PE options for bull put spreads

        # Greeks caches — populated by _refresh_greeks_cache()
        self.ce_greeks_cache = None  # columns: strike, expiry_str, iv, delta, gamma, theta, vega
        self.pe_greeks_cache = None  # same, but put greeks (delta is negative)

    # ─────────────────────────────────────────────────────────────────────────
    # IV Stats
    # ─────────────────────────────────────────────────────────────────────────

    def get_iv_stats(self):
        """
        Fetch India VIX from yfinance (rolling 1 year).
        Returns (current_iv, iv_rank, iv_percentile).
        IV Rank       = (current - min) / (max - min) * 100
        IV Percentile = % of days IV was below current
        """
        start_date = (datetime.today() - timedelta(days=365)).date()
        df         = yf.download("^INDIAVIX", start=start_date)
        iv         = df['Close'].to_numpy().flatten()
        current_iv = float(iv[-1])
        iv_rank    = float((current_iv - iv.min()) / (iv.max() - iv.min()) * 100)
        iv_pct     = float(np.mean(iv <= current_iv) * 100)
        self.iv_stats = (current_iv, iv_rank, iv_pct)
        return current_iv, iv_rank, iv_pct

    # ─────────────────────────────────────────────────────────────────────────
    # Universe Builder
    # ─────────────────────────────────────────────────────────────────────────

    def build_universe(self, min_dte: int = 20, max_dte: int = 50):
        """
        Download the Angel Broking scrip master and build:
          raw_calls — CE options for bear call spreads
          raw_puts  — PE options for bull put spreads

        Strike ranges (centred around spot):
          CE: [spot * 0.90, spot * 1.15]
          PE: [spot * 0.70, spot * 1.10]

        DTE window: [min_dte, max_dte] days from today (default 20–45).
        """
        url      = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
        d        = requests.get(url).json()
        token_df = pd.DataFrame(d)
        token_df['expiry'] = pd.to_datetime(token_df['expiry'], format='mixed').dt.date
        token_df['strike'] = pd.to_numeric(token_df['strike'], errors='coerce')

        nifty_opts = token_df[
            (token_df['name'] == 'NIFTY') &
            (token_df['instrumenttype'] == 'OPTIDX')
        ].copy()
        nifty_opts['option_type'] = nifty_opts['symbol'].str[-2:]
        nifty_opts = nifty_opts.sort_values(['expiry', 'strike']).reset_index(drop=True)

        # Current spot
        self.spot = self.connection.ltpData(
            exchange="NSE", tradingsymbol="NIFTY", symboltoken="99926000"
        )['data']['ltp']

        today  = pd.to_datetime('today').normalize()
        ex_min = today + pd.Timedelta(days=min_dte)
        ex_max = today + pd.Timedelta(days=max_dte)

        # ── CE universe ───────────────────────────────────────────────────────
        ce_raw = nifty_opts[nifty_opts['option_type'] == 'CE'][
            ['token', 'symbol', 'name', 'expiry', 'strike']
        ].copy()
        ce_raw['expiry'] = pd.to_datetime(ce_raw['expiry'])
        ce_raw['strike'] = ce_raw['strike'] / 100

        self.raw_calls = ce_raw[
            (ce_raw['strike'] >= self.spot * 1.0) &
            (ce_raw['strike'] <= self.spot * 1.2) &
            (ce_raw['expiry'] >= ex_min) &
            (ce_raw['expiry'] <= ex_max)
        ].copy()

        # ── PE universe ───────────────────────────────────────────────────────
        pe_raw = nifty_opts[nifty_opts['option_type'] == 'PE'][
            ['token', 'symbol', 'name', 'expiry', 'strike']
        ].copy()
        pe_raw['expiry'] = pd.to_datetime(pe_raw['expiry'])
        pe_raw['strike'] = pe_raw['strike'] / 100

        self.raw_puts = pe_raw[
            (pe_raw['strike'] >= self.spot * 0.8) &
            (pe_raw['strike'] <= self.spot * 1.0) &
            (pe_raw['expiry'] >= ex_min) &
            (pe_raw['expiry'] <= ex_max)
        ].copy()

        # Combined token list for websocket subscription
        self.tokens = list(
            set(self.raw_calls['token'].tolist()) |
            set(self.raw_puts['token'].tolist())
        )

        print(f"[universe] spot={self.spot}  CE={len(self.raw_calls)}  PE={len(self.raw_puts)}")

    # ─────────────────────────────────────────────────────────────────────────
    # Websocket Live Feed
    # ─────────────────────────────────────────────────────────────────────────

    def start_live_feed(self):
        """Start websocket subscription for all tokens in self.tokens."""
        latest     = {}
        token_list = [{"exchangeType": 2, "tokens": self.tokens}]

        def on_data(wsapp, message):
            latest[message['token']] = message

        self.broker.start_ws(token_list=token_list, mode=3, on_data=on_data)
        self.latest = latest

    # ─────────────────────────────────────────────────────────────────────────
    # Greeks Cache
    # ─────────────────────────────────────────────────────────────────────────

    def _refresh_greeks_cache(self):
        """
        Fetch CE and PE greeks from the broker for all expiries in the universe.
        Populates self.ce_greeks_cache and self.pe_greeks_cache.

        Both caches share the schema:
            strike (float), expiry_str (str), iv, delta, gamma, theta, vega

        Put deltas are negative as returned by the API.
        """
        if self.raw_calls is None or self.raw_puts is None:
            return

        # ── CE greeks ─────────────────────────────────────────────────────────
        ce_expiries = self.raw_calls['expiry'].dt.strftime('%d%b%Y').str.upper().unique()
        ce_strikes  = set(self.raw_calls['strike'].round(2))
        ce_rows     = []

        for expiry_str in ce_expiries:
            try:
                result = self.connection.optionGreek({"name": "NIFTY", "expirydate": expiry_str})
                if not result.get('status') or not result.get('data'):
                    continue
                for entry in result['data']:
                    if entry.get('optionType') != 'CE':
                        continue
                    strike = round(float(entry['strikePrice']), 2)
                    if strike not in ce_strikes:
                        continue
                    ce_rows.append({
                        'strike':     strike,
                        'expiry_str': expiry_str,
                        'iv':         float(entry['impliedVolatility']),
                        'delta':      float(entry['delta']),
                        'gamma':      float(entry['gamma']),
                        'theta':      float(entry['theta']),
                        'vega':       float(entry['vega']),
                    })
                time.sleep(2)
            except Exception as e:
                print(f"[greeks CE] {expiry_str}: {e}")

        if ce_rows:
            self.ce_greeks_cache = pd.DataFrame(ce_rows)
            print(f"[greeks CE] cached {len(ce_rows)} rows across {len(ce_expiries)} expiries")

        # ── PE greeks ─────────────────────────────────────────────────────────
        pe_expiries = self.raw_puts['expiry'].dt.strftime('%d%b%Y').str.upper().unique()
        pe_strikes  = set(self.raw_puts['strike'].round(2))
        pe_rows     = []

        for expiry_str in pe_expiries:
            try:
                result = self.connection.optionGreek({"name": "NIFTY", "expirydate": expiry_str})
                if not result.get('status') or not result.get('data'):
                    continue
                for entry in result['data']:
                    if entry.get('optionType') != 'PE':
                        continue
                    strike = round(float(entry['strikePrice']), 2)
                    if strike not in pe_strikes:
                        continue
                    pe_rows.append({
                        'strike':     strike,
                        'expiry_str': expiry_str,
                        'iv':         float(entry['impliedVolatility']),
                        'delta':      float(entry['delta']),   # negative for puts
                        'gamma':      float(entry['gamma']),
                        'theta':      float(entry['theta']),
                        'vega':       float(entry['vega']),
                    })
                time.sleep(2)
            except Exception as e:
                print(f"[greeks PE] {expiry_str}: {e}")

        if pe_rows:
            self.pe_greeks_cache = pd.DataFrame(pe_rows)
            print(f"[greeks PE] cached {len(pe_rows)} rows across {len(pe_expiries)} expiries")

    def start_greeks_refresh(self, interval_seconds: int = 15):
        """Spawn a daemon thread that refreshes CE+PE greeks every interval_seconds."""
        def _loop():
            while True:
                try:
                    self._refresh_greeks_cache()
                except Exception as e:
                    print(f"[greeks thread] {e}")
                threading.Event().wait(interval_seconds)

        threading.Thread(target=_loop, daemon=True, name="greeks-refresh").start()