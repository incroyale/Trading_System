# dashboard/portfolio.py
import sqlite3, threading, time
import pandas as pd

DB_PATH = "data/portfolio.db"
_lock = threading.Lock()

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                spread_id   INTEGER,
                exchange    TEXT,
                symbol      TEXT,
                token       TEXT,
                side        TEXT,
                entry_price REAL,
                close_price REAL,
                ltp         REAL,
                pnl         REAL,
                max_profit  REAL,
                status      TEXT DEFAULT 'OPEN',
                timestamp   TEXT DEFAULT (datetime('now','localtime'))
            )
        """)
        for col, defn in [
            ("close_price", "REAL"),
            ("status",      "TEXT DEFAULT 'OPEN'"),
            ("max_profit",  "REAL"),
            ("spread_id",   "INTEGER"),
        ]:
            try:
                conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {defn}")
            except Exception:
                pass

        # ── Trade params table for research/backtesting ───────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trade_params (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                spread_id     INTEGER,
                side          TEXT,
                iv_rank       REAL,
                reward_risk   REAL,
                short_delta   REAL,
                dte           INTEGER,
                short_strike  REAL,
                long_strike   REAL,
                width         REAL,
                net_credit    REAL,
                max_loss      REAL,
                pop           REAL,
                net_theta     REAL,
                net_vega      REAL,
                net_gamma     REAL,
                timestamp     TEXT DEFAULT (datetime('now','localtime'))
            )
        """)


def log_trade(exchange, symbol, token, side, ltp, spread_id=None):
    with _lock:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.execute(
                "INSERT INTO trades "
                "(spread_id, exchange, symbol, token, side, entry_price, ltp, pnl, max_profit, status) "
                "VALUES (?,?,?,?,?,?,?,0,NULL,'OPEN')",
                (spread_id, exchange, symbol, token, side, ltp, ltp)
            )
            new_id = cur.lastrowid

            if spread_id is not None:
                partner = conn.execute(
                    "SELECT id, side, ltp FROM trades "
                    "WHERE spread_id=? AND id!=? AND status='OPEN'",
                    (spread_id, new_id)
                ).fetchone()

                if partner is not None:
                    partner_id, partner_side, partner_ltp = partner

                    if side == "SELL":
                        sell_ltp = ltp
                        buy_ltp  = partner_ltp
                    else:
                        sell_ltp = partner_ltp
                        buy_ltp  = ltp

                    max_profit = round(sell_ltp - buy_ltp, 4)
                    conn.execute(
                        "UPDATE trades SET max_profit=? WHERE id IN (?,?)",
                        (max_profit, new_id, partner_id)
                    )

        return new_id


def log_trade_params(spread_id, side, params: dict):
    """
    Store filter parameters at the time of trade entry for research analysis.
    params dict keys: iv_rank, reward_risk, short_delta, dte, short_strike,
                      long_strike, width, net_credit, max_loss, pop,
                      net_theta, net_vega, net_gamma
    """
    with _lock:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO trade_params
                (spread_id, side, iv_rank, reward_risk, short_delta, dte,
                 short_strike, long_strike, width, net_credit, max_loss,
                 pop, net_theta, net_vega, net_gamma)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                spread_id,
                side,
                params.get('iv_rank'),
                params.get('reward_risk'),
                params.get('short_delta'),
                params.get('dte'),
                params.get('short_strike'),
                params.get('long_strike'),
                params.get('width'),
                params.get('net_credit'),
                params.get('max_loss'),
                params.get('pop'),
                params.get('net_theta'),
                params.get('net_vega'),
                params.get('net_gamma'),
            ))


def get_open_tokens() -> set:
    """Return set of tokens currently in OPEN trades — used for duplicate prevention."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT token FROM trades WHERE status='OPEN'"
        ).fetchall()
    return {r[0] for r in rows}


def get_next_spread_id() -> int:
    """Return max existing spread_id + 1, or 1 if no trades exist."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(spread_id), 0) FROM trades"
        ).fetchone()
    return row[0] + 1


def close_trade(trade_id, close_price):
    """Square off an open trade — locks in PnL permanently."""
    with _lock:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT side, entry_price FROM trades WHERE id=? AND status='OPEN'",
                (trade_id,)
            ).fetchone()
            if row is None:
                return
            side, entry_price = row
            pnl = (close_price - entry_price) if side == "BUY" else (entry_price - close_price)
            conn.execute(
                "UPDATE trades SET close_price=?, ltp=?, pnl=?, status='CLOSED' WHERE id=?",
                (close_price, close_price, pnl, trade_id)
            )


def _close_trade_unlocked(conn, trade_id, close_price):
    """Same as close_trade but assumes lock is already held and conn is provided."""
    row = conn.execute(
        "SELECT side, entry_price FROM trades WHERE id=? AND status='OPEN'",
        (trade_id,)
    ).fetchone()
    if row is None:
        return
    side, entry_price = row
    pnl = (close_price - entry_price) if side == "BUY" else (entry_price - close_price)
    conn.execute(
        "UPDATE trades SET close_price=?, ltp=?, pnl=?, status='CLOSED' WHERE id=?",
        (close_price, close_price, pnl, trade_id)
    )


def _check_exit_rules(conn, ltp_map):
    """
    For every OPEN spread with both legs filled (max_profit not NULL),
    check combined PnL against exit rules:
      - Take profit : combined_pnl >= max_profit * 0.70
      - Stop loss   : combined_pnl <= -(max_profit * 2.00)
    Closes both legs at current LTP if triggered.
    """
    rows = conn.execute("""
        SELECT spread_id, id, token, pnl, max_profit
        FROM trades
        WHERE status='OPEN' AND spread_id IS NOT NULL AND max_profit IS NOT NULL
    """).fetchall()

    if not rows:
        return

    spreads = {}
    for spread_id, trade_id, token, pnl, max_profit in rows:
        spreads.setdefault(spread_id, []).append({
            "id": trade_id, "token": token, "pnl": pnl, "max_profit": max_profit
        })

    for spread_id, legs in spreads.items():
        if len(legs) != 2:
            continue

        combined_pnl      = sum(leg["pnl"] for leg in legs)
        max_profit        = legs[0]["max_profit"]
        take_profit_level = max_profit * 0.70
        stop_loss_level   = -(max_profit * 2.0)

        triggered = None
        if combined_pnl >= take_profit_level:
            triggered = f"TP +{combined_pnl:.2f} >= {take_profit_level:.2f}"
        elif combined_pnl <= stop_loss_level:
            triggered = f"SL {combined_pnl:.2f} <= {stop_loss_level:.2f}"

        if triggered:
            print(f"[exit rules] Spread {spread_id} triggered — {triggered}. Closing both legs.")
            for leg in legs:
                close_ltp = ltp_map.get(leg["token"], leg["pnl"])
                _close_trade_unlocked(conn, leg["id"], close_ltp)


def update_open_ltps(ltp_map: dict):
    """Update ltp + unrealised PnL for OPEN trades, then check exit rules."""
    with _lock:
        with sqlite3.connect(DB_PATH) as conn:
            for token, ltp in ltp_map.items():
                rows = conn.execute(
                    "SELECT id, side, entry_price FROM trades WHERE token=? AND status='OPEN'",
                    (token,)
                ).fetchall()
                for trade_id, side, entry_price in rows:
                    pnl = (ltp - entry_price) if side == "BUY" else (entry_price - ltp)
                    conn.execute(
                        "UPDATE trades SET ltp=?, pnl=? WHERE id=?",
                        (ltp, pnl, trade_id)
                    )
            # Check exit rules after all LTPs are updated
            _check_exit_rules(conn, ltp_map)


def get_trades():
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM trades ORDER BY id DESC", conn)
    return df


def get_total_pnl():
    """Sum of locked PnL (CLOSED) + unrealised PnL (OPEN)."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT COALESCE(SUM(pnl),0) FROM trades").fetchone()
    return row[0]


def clear_all_trades():
    with _lock:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("DELETE FROM trades")


# ── LTP polling (open trades only) ───────────────────────────────────────────
_poll_connection = None

def start_ltp_polling(connection, interval=2, time_gate=None):
    """
    Start background LTP polling.
    time_gate: optional callable() -> bool. If provided, polling only runs when it returns True.
    """
    global _poll_connection
    _poll_connection = connection

    def _loop():
        while True:
            if time_gate is not None and not time_gate():
                time.sleep(interval)
                continue
            try:
                with sqlite3.connect(DB_PATH) as conn:
                    tokens = [
                        r[0] for r in conn.execute(
                            "SELECT DISTINCT token FROM trades WHERE status='OPEN'"
                        ).fetchall()
                    ]
                    ltp_map = {}
                    for token in tokens:
                        try:
                            row = conn.execute(
                                "SELECT exchange, symbol FROM trades "
                                "WHERE token=? AND status='OPEN' LIMIT 1",
                                (token,)
                            ).fetchone()
                            if row and _poll_connection:
                                data = _poll_connection.ltpData(
                                    exchange=row[0],
                                    tradingsymbol=row[1],
                                    symboltoken=token
                                )
                                ltp_map[token] = data['data']['ltp']
                        except Exception as e:
                            print(f"[ltp poll token {token}] {e}")
                    if ltp_map:
                        update_open_ltps(ltp_map)
            except Exception as e:
                print(f"[ltp poll] {e}")
            time.sleep(interval)

    threading.Thread(target=_loop, daemon=True).start()
