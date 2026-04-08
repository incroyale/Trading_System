# strategies/credit_spread/signals_india.py
import pandas as pd


# Bear Call Spread

class IndiaCreditSpreads:
    """
    Manages the CE leg universe for bear call spreads.

    Data flow:
      1. load_universe(obj.raw_calls)     — called once at startup
      2. get_filtered_dfs()               — drops illiquid tokens each cycle
      3. apply_greeks_filters(obj.ce_greeks_cache) — merges greeks, delta filter
      4. get_tick_data()                  — returns display-ready DataFrame
    """

    def __init__(self, broker, connection):
        self.broker        = broker
        self.connection    = connection
        self.raw_calls     = None       # working filtered DataFrame
        self._base_calls   = None       # snapshot after DTE filter
        self.spread_tokens = []
        self.spread_latest = {}         # pointed at obj.latest in app.py
        self.greeks_ready  = False

    # ── Universe loading ──────────────────────────────────────────────────────

    def load_universe(self, raw_calls: pd.DataFrame):
        """
        Load the CE universe directly from IndiaMarketHub.raw_calls.
        The hub has already applied the DTE window (20–45 days) and
        strike range filter, so we just snapshot it here.
        """
        self._base_calls   = raw_calls.copy()
        self.raw_calls     = self._base_calls.copy()
        self.spread_tokens = list(self.raw_calls['token'].astype(str))

    # ── Liquidity gate ────────────────────────────────────────────────────────

    def get_filtered_dfs(self):
        """Remove illiquid tokens from raw_calls based on live tick quality."""
        LIQUIDITY_KEYS = [
            'last_traded_quantity', 'average_traded_price', 'volume_trade_for_the_day',
            'total_buy_quantity', 'total_sell_quantity', 'open_price_of_the_day',
            'high_price_of_the_day', 'low_price_of_the_day', 'last_traded_timestamp',
            'open_interest',
        ]

        def is_valid_tick(tick):
            if not all(tick.get(k, 0) != 0 for k in LIQUIDITY_KEYS):
                return False
            sell_ok = sum(
                1 for d in tick.get('best_5_sell_data', [])
                if d['price'] != 0 and d['quantity'] != 0
            ) >= 3
            buy_ok = sum(
                1 for d in tick.get('best_5_buy_data', [])
                if d['price'] != 0 and d['quantity'] != 0
            ) >= 3
            return sell_ok and buy_ok

        valid_tokens  = {t for t, tick in self.spread_latest.items() if is_valid_tick(tick)}
        self.raw_calls = self._base_calls[
            self._base_calls['token'].astype(str).isin(valid_tokens)
        ].copy()

    # ── Greeks + tick enrichment ──────────────────────────────────────────────

    def apply_greeks_filters(self, ce_greeks_cache: pd.DataFrame):
        """
        Merge hub-provided CE greeks into raw_calls, attach live bid/ask/ltp,
        then filter:
          - delta in [0.08, 0.30]   (short-leg OTM calls)
          - bid-ask spread <= 4
        """
        try:
            if ce_greeks_cache is None or ce_greeks_cache.empty:
                return

            df = self.raw_calls.copy()
            df['strike']     = df['strike'].round(2)
            df['expiry_str'] = df['expiry'].dt.strftime('%d%b%Y').str.upper()
            df = df.merge(
                ce_greeks_cache, on=['strike', 'expiry_str'], how='left'
            ).drop(columns=['expiry_str'])

            rows = []
            for _, row in df.iterrows():
                tick = self.spread_latest.get(str(row['token']))
                if tick is None:
                    continue
                bid = tick['best_5_buy_data'][0]['price']  / 100 if tick.get('best_5_buy_data')  else None
                ask = tick['best_5_sell_data'][0]['price'] / 100 if tick.get('best_5_sell_data') else None
                row          = row.copy()
                row['ltp']   = tick.get('last_traded_price', 0) / 100
                row['bid']   = bid
                row['ask']   = ask
                row['spread']     = round(ask - bid, 4) if bid and ask else None
                row['day_volume'] = tick.get('volume_trade_for_the_day', 0)
                row['oi']         = tick.get('open_interest', 0)
                rows.append(row)

            df = pd.DataFrame(rows)
            if df.empty:
                return

            df = df[(df['delta'] >= 0.08) & (df['delta'] <= 0.30)]
            df = df[df['spread'] <= 4]

            if df.empty:
                return

            self.raw_calls    = df
            self.greeks_ready = True

        except Exception as e:
            print(f"[CE apply_greeks_filters] keeping last good data: {e}")

    # ── Tick snapshot for CSV / display ──────────────────────────────────────

    def get_tick_data(self) -> pd.DataFrame:
        """Return raw_calls with live bid/ask/ltp attached."""
        df = self.raw_calls.copy()
        if df.empty:
            return df
        rows = []
        for _, row in df.iterrows():
            tick = self.spread_latest.get(str(row['token']))
            if tick is None:
                continue
            bid = tick['best_5_buy_data'][0]['price']  / 100 if tick.get('best_5_buy_data')  else None
            ask = tick['best_5_sell_data'][0]['price'] / 100 if tick.get('best_5_sell_data') else None
            row              = row.copy()
            row['ltp']       = tick.get('last_traded_price', 0) / 100
            row['bid']       = bid
            row['ask']       = ask
            row['spread']    = round(ask - bid, 2) if bid and ask else None
            row['day_volume'] = tick.get('volume_trade_for_the_day', 0)
            row['oi']        = tick.get('open_interest', 0)
            rows.append(row)
        return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Bull Put Spread legs
# ─────────────────────────────────────────────────────────────────────────────

class IndiaCreditSpreadsPut:
    """
    Manages the PE leg universe for bull put spreads.

    Data flow (mirrors IndiaCreditSpreads exactly):
      1. load_universe(obj.raw_puts)              — called once at startup
      2. get_filtered_dfs()                       — liquidity gate each cycle
      3. apply_greeks_filters(obj.pe_greeks_cache)— merges greeks, delta filter
      4. get_tick_data()                          — display-ready DataFrame

    Greeks are owned by IndiaMarketHub and passed in — no internal fetching.
    Put deltas are negative; short-leg filter: delta in [-0.30, -0.08].
    """

    def __init__(self, broker, connection):
        self.broker        = broker
        self.connection    = connection
        self.raw_puts      = None       # working filtered DataFrame
        self._base_puts    = None       # snapshot after DTE filter
        self.spread_tokens = []
        self.spread_latest = {}         # pointed at obj.latest in app.py
        self.greeks_ready  = False

    # ── Universe loading ──────────────────────────────────────────────────────

    def load_universe(self, raw_puts: pd.DataFrame):
        """
        Load the PE universe directly from IndiaMarketHub.raw_puts.
        The hub has already applied the DTE window and strike range filter.
        """
        self._base_puts    = raw_puts.copy()
        self.raw_puts      = self._base_puts.copy()
        self.spread_tokens = list(self.raw_puts['token'].astype(str))

    # ── Liquidity gate ────────────────────────────────────────────────────────

    def get_filtered_dfs(self):
        """Remove illiquid tokens from raw_puts based on live tick quality."""
        LIQUIDITY_KEYS = [
            'last_traded_quantity', 'average_traded_price', 'volume_trade_for_the_day',
            'total_buy_quantity', 'total_sell_quantity', 'open_price_of_the_day',
            'high_price_of_the_day', 'low_price_of_the_day', 'last_traded_timestamp',
            'open_interest',
        ]

        def is_valid_tick(tick):
            if not all(tick.get(k, 0) != 0 for k in LIQUIDITY_KEYS):
                return False
            sell_ok = sum(
                1 for d in tick.get('best_5_sell_data', [])
                if d['price'] != 0 and d['quantity'] != 0
            ) >= 3
            buy_ok = sum(
                1 for d in tick.get('best_5_buy_data', [])
                if d['price'] != 0 and d['quantity'] != 0
            ) >= 3
            return sell_ok and buy_ok

        valid_tokens  = {t for t, tick in self.spread_latest.items() if is_valid_tick(tick)}
        self.raw_puts = self._base_puts[
            self._base_puts['token'].astype(str).isin(valid_tokens)
        ].copy()

    # ── Greeks + tick enrichment ──────────────────────────────────────────────

    def apply_greeks_filters(self, pe_greeks_cache: pd.DataFrame):
        """
        Merge hub-provided PE greeks into raw_puts, attach live bid/ask/ltp,
        then filter:
          - delta in [-0.30, -0.05]  (keeps short + long leg candidates)
          - bid-ask spread <= 4
        spread_builder handles the short/long pairing and the tighter
        short-leg delta gate [-0.30, -0.08].
        """
        try:
            if pe_greeks_cache is None or pe_greeks_cache.empty:
                return

            df = self.raw_puts.copy()
            df['strike']     = df['strike'].round(2)
            df['expiry_str'] = df['expiry'].dt.strftime('%d%b%Y').str.upper()
            df = df.merge(
                pe_greeks_cache, on=['strike', 'expiry_str'], how='left'
            ).drop(columns=['expiry_str'])

            rows = []
            for _, row in df.iterrows():
                tick = self.spread_latest.get(str(row['token']))
                if tick is None:
                    continue
                bid = tick['best_5_buy_data'][0]['price']  / 100 if tick.get('best_5_buy_data')  else None
                ask = tick['best_5_sell_data'][0]['price'] / 100 if tick.get('best_5_sell_data') else None
                row               = row.copy()
                row['ltp']        = tick.get('last_traded_price', 0) / 100
                row['bid']        = bid
                row['ask']        = ask
                row['spread']     = round(ask - bid, 4) if bid and ask else None
                row['day_volume'] = tick.get('volume_trade_for_the_day', 0)
                row['oi']         = tick.get('open_interest', 0)
                rows.append(row)

            df = pd.DataFrame(rows)
            if df.empty:
                return

            # Keep all rows with delta <= -0.05 so spread_builder can form pairs
            df = df[df['delta'] <= -0.05]
            df = df[df['spread'] <= 4]

            if df.empty:
                return

            self.raw_puts     = df
            self.greeks_ready = True

        except Exception as e:
            print(f"[PE apply_greeks_filters] keeping last good data: {e}")

    # ── Tick snapshot for CSV / display ──────────────────────────────────────

    def get_tick_data(self) -> pd.DataFrame:
        """Return raw_puts with live bid/ask/ltp attached."""
        df = self.raw_puts.copy()
        if df.empty:
            return df
        rows = []
        for _, row in df.iterrows():
            tick = self.spread_latest.get(str(row['token']))
            if tick is None:
                continue
            bid = tick['best_5_buy_data'][0]['price']  / 100 if tick.get('best_5_buy_data')  else None
            ask = tick['best_5_sell_data'][0]['price'] / 100 if tick.get('best_5_sell_data') else None
            row               = row.copy()
            row['ltp']        = tick.get('last_traded_price', 0) / 100
            row['bid']        = bid
            row['ask']        = ask
            row['spread']     = round(ask - bid, 2) if bid and ask else None
            row['day_volume'] = tick.get('volume_trade_for_the_day', 0)
            row['oi']         = tick.get('open_interest', 0)
            rows.append(row)
        return pd.DataFrame(rows)

# ── Bull Put Spreads ──────────────────────────────────────────────────────────

class IndiaCreditSpreadsPut:
    """
    Mirrors IndiaCreditSpreads but operates on the PE universe from
    IndiaPMCC.raw_puts.

    Greeks are fetched fresh per expiry via optionGreek (PE optionType).
    Delta for puts is negative; the short leg filter is -0.08 to -0.30.
    Bid/ask/ltp are shared from obj.latest (same websocket feed).
    CSV output uses a _PE suffix so spread_builder can distinguish them.
    """

    def __init__(self, broker, connection):
        self.broker = broker
        self.connection = connection
        self.raw_puts = None          # working filtered df
        self._base_puts = None        # snapshot after DTE filter
        self.greeks_cache = None      # DataFrame with put greeks
        self.spread_tokens = []
        self.spread_latest = {}       # pointed at obj.latest in app.py
        self.greeks_ready = False

    # ── Initialisation ────────────────────────────────────────────────────────

    def load_universe(self, raw_puts):
        self._base_puts = raw_puts.copy()
        self.raw_puts = self._base_puts.copy()
        self.spread_tokens = list(self.raw_puts['token'].astype(str))

    # ── Liquidity gate ────────────────────────────────────────────────────────

    def get_filtered_dfs(self):
        """Remove illiquid tokens from raw_puts."""
        LIQUIDITY_KEYS = [
            'last_traded_quantity', 'average_traded_price', 'volume_trade_for_the_day',
            'total_buy_quantity', 'total_sell_quantity', 'open_price_of_the_day',
            'high_price_of_the_day', 'low_price_of_the_day', 'last_traded_timestamp',
            'open_interest',
        ]
        def is_valid_tick(tick):
            if not all(tick.get(k, 0) != 0 for k in LIQUIDITY_KEYS):
                return False
            if sum(1 for d in tick.get('best_5_sell_data', []) if d['price'] != 0 and d['quantity'] != 0) < 3:
                return False
            if sum(1 for d in tick.get('best_5_buy_data', []) if d['price'] != 0 and d['quantity'] != 0) < 3:
                return False
            return True
        valid_tokens = {t for t, tick in self.spread_latest.items() if is_valid_tick(tick)}
        self.raw_puts = self._base_puts[self._base_puts['token'].astype(str).isin(valid_tokens)].copy()

    # ── Greeks ────────────────────────────────────────────────────────────────

    def _fetch_greeks_cache(self):
        """
        Fetch PE greeks from the broker for every expiry in _base_puts.
        Stores a DataFrame with columns:
            strike, expiry_str, iv, delta, gamma, theta, vega
        Put deltas are negative from the API.
        """
        if self._base_puts is None or self._base_puts.empty:
            return

        unique_expiries = self._base_puts['expiry'].dt.strftime('%d%b%Y').str.upper().unique()
        all_strikes = set(self._base_puts['strike'].round(2))
        greek_rows = []

        for expiry_str in unique_expiries:
            try:
                result = self.connection.optionGreek({"name": "NIFTY", "expirydate": expiry_str})
                if not result.get('status') or not result.get('data'):
                    continue
                for entry in result['data']:
                    if entry.get('optionType') != 'PE':
                        continue
                    strike = round(float(entry['strikePrice']), 2)
                    if strike not in all_strikes:
                        continue
                    greek_rows.append({
                        'strike':     strike,
                        'expiry_str': expiry_str,
                        'iv':         float(entry['impliedVolatility']),
                        'delta':      float(entry['delta']),   # negative for puts
                        'gamma':      float(entry['gamma']),
                        'theta':      float(entry['theta']),
                        'vega':       float(entry['vega']),
                    })
                import time; time.sleep(2)
            except Exception as e:
                print(f"[put greeks] {expiry_str}: {e}")

        if greek_rows:
            self.greeks_cache = pd.DataFrame(greek_rows)

    def apply_greeks_filters(self, _ignored_cache=None):
        """
        Fetch PE greeks independently and filter:
          - Short-leg candidates: delta in [-0.30, -0.08]
          - bid-ask spread <= 4
        Keeps all strikes per expiry so spread_builder can pair them.
        """
        try:
            self._fetch_greeks_cache()
            if self.greeks_cache is None or self.greeks_cache.empty:
                return

            df = self.raw_puts.copy()
            df['strike'] = df['strike'].round(2)
            df['expiry_str'] = df['expiry'].dt.strftime('%d%b%Y').str.upper()
            df = df.merge(self.greeks_cache, on=['strike', 'expiry_str'], how='left').drop(columns=['expiry_str'])

            rows = []
            for _, row in df.iterrows():
                tick = self.spread_latest.get(str(row['token']))
                if tick is None:
                    continue
                bid = tick['best_5_buy_data'][0]['price'] / 100 if tick.get('best_5_buy_data') else None
                ask = tick['best_5_sell_data'][0]['price'] / 100 if tick.get('best_5_sell_data') else None
                row = row.copy()
                row['ltp']       = tick.get('last_traded_price', 0) / 100
                row['bid']       = bid
                row['ask']       = ask
                row['spread']    = round(ask - bid, 4) if bid and ask else None
                row['day_volume'] = tick.get('volume_trade_for_the_day', 0)
                row['oi']        = tick.get('open_interest', 0)
                rows.append(row)

            df = pd.DataFrame(rows)
            if df.empty:
                return

            # Delta filter: short leg is -0.08 to -0.30; keep all rows in that
            # neighbourhood so spread_builder can form pairs (long leg will be
            # more negative, i.e. delta < -0.30).  We keep delta <= -0.05 to
            # include long legs too, spread_builder does the pairing logic.
            df = df[df['delta'] <= -0.05]
            df = df[df['spread'] <= 4]

            if df.empty:
                return

            self.raw_puts = df
            self.greeks_ready = True
        except Exception as e:
            print(f"[put apply_greeks_filters] failed, keeping last good data: {e}")

    # ── Live tick snapshot ────────────────────────────────────────────────────

    def get_tick_data(self):
        """Return raw_puts with live bid/ask/ltp attached."""
        df = self.raw_puts.copy()
        if df.empty:
            return df
        rows = []
        for _, row in df.iterrows():
            tick = self.spread_latest.get(str(row['token']))
            if tick is None:
                continue
            bid = tick['best_5_buy_data'][0]['price'] / 100 if tick.get('best_5_buy_data') else None
            ask = tick['best_5_sell_data'][0]['price'] / 100 if tick.get('best_5_sell_data') else None
            row = row.copy()
            row['ltp']        = tick.get('last_traded_price', 0) / 100
            row['bid']        = bid
            row['ask']        = ask
            row['spread']     = round(ask - bid, 2) if bid and ask else None
            row['day_volume'] = tick.get('volume_trade_for_the_day', 0)
            row['oi']         = tick.get('open_interest', 0)
            rows.append(row)
        return pd.DataFrame(rows)