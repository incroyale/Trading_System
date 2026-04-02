import sqlite3
import threading
import time
import pandas as pd

DB_PATH = "portfolio.db"

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, exchange TEXT, tradingsymbol TEXT, 
        symboltoken TEXT, side TEXT, entry_ltp REAL, current_ltp REAL, pnl REAL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)""")

def log_trade(exchange, tradingsymbol, symboltoken, side, ltp):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""INSERT INTO trades (exchange, tradingsymbol, symboltoken, side, entry_ltp, current_ltp, pnl)
            VALUES (?, ?, ?, ?, ?, ?, 0) """, (exchange, tradingsymbol, symboltoken, side, ltp, ltp))

def get_trades():
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql("SELECT * FROM trades ORDER BY timestamp DESC", conn)

def update_ltp(connection, trade_id, symboltoken, exchange, tradingsymbol, side, entry_ltp):
    try:
        result = connection.ltpData(exchange=exchange, tradingsymbol=tradingsymbol, symboltoken=symboltoken)
        ltp = result['data']['ltp']
        pnl = (ltp - entry_ltp) if side == "BUY" else (entry_ltp - ltp)
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("UPDATE trades SET current_ltp=?, pnl=? WHERE id=?", (ltp, pnl, trade_id))
    except Exception as e:
        print(f"[ltp poll] {e}")

def start_ltp_polling(connection, interval=2):
    def _loop():
        while True:
            try:
                trades = get_trades()
                for _, row in trades.iterrows():
                    update_ltp(connection, row['id'], row['symboltoken'],
                               row['exchange'], row['tradingsymbol'],
                               row['side'], row['entry_ltp'])
                    time.sleep(0.15)  # ~6-7 rows before hitting 10 req/s limit
            except Exception as e:
                print(f"[poll loop] {e}")
            time.sleep(interval)
    threading.Thread(target=_loop, daemon=True).start()