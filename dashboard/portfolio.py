# dashboard/portfolio.py
import sqlite3, threading, time
import pandas as pd

DB_PATH = "data\portfolio.db"
_lock = threading.Lock()

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
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
        # migrate existing DBs that lack columns
        for col, defn in [
            ("close_price", "REAL"),
            ("status",      "TEXT DEFAULT 'OPEN'"),
            ("max_profit",  "REAL"),
        ]:
            try:
                conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {defn}")
            except Exception:
                pass


def log_trade(exchange, symbol, token, side, ltp):
    """
    Insert a new trade row.

    Pairing rule:
      - Odd ID  → first leg of a spread; max_profit stays NULL until the counter leg arrives.
      - Even ID → second leg of a spread; compute max_profit from this pair and
                  write it back to BOTH rows immediately.

    max_profit = SELL-leg LTP − BUY-leg LTP  (net credit received per point)
    Either leg can be entered first (BUY or SELL), the code detects which is which.
    """
    with _lock:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.execute(
                "INSERT INTO trades "
                "(exchange, symbol, token, side, entry_price, ltp, pnl, max_profit, status) "
                "VALUES (?,?,?,?,?,?,0,NULL,'OPEN')",
                (exchange, symbol, token, side, ltp, ltp)
            )
            new_id = cur.lastrowid

            # ── Even ID → pair is complete, calculate max_profit ──────────────
            if new_id % 2 == 0:
                partner_id = new_id - 1
                partner = conn.execute(
                    "SELECT side, ltp FROM trades WHERE id=?", (partner_id,)
                ).fetchone()

                if partner is not None:
                    partner_side, partner_ltp = partner

                    # Identify which leg is SELL and which is BUY
                    if side == "SELL":
                        sell_ltp = ltp
                        buy_ltp  = partner_ltp
                    else:
                        sell_ltp = partner_ltp
                        buy_ltp  = ltp

                    max_profit = round(sell_ltp - buy_ltp, 4)

                    # Write max_profit to both legs
                    conn.execute(
                        "UPDATE trades SET max_profit=? WHERE id IN (?,?)",
                        (max_profit, new_id, partner_id)
                    )

        return new_id


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


def update_open_ltps(ltp_map: dict):
    """Update ltp + unrealised PnL for OPEN trades only."""
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

def start_ltp_polling(connection, interval=2):
    global _poll_connection
    _poll_connection = connection

    def _loop():
        while True:
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