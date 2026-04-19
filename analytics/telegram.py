# analytics/telegram.py
import requests
from dotenv import load_dotenv
import os
from dashboard.portfolio import get_trades, get_total_pnl
import pandas as pd

load_dotenv()

# ---- TELEGRAM CONFIG ----
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
GET_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
SEND_MSG_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

def notify(message: str):
    try:
        requests.post(SEND_MSG_URL, data={"chat_id": CHAT_ID, "text": message}, timeout=5)
    except Exception as e:
        print(f"[telegram] failed to send: {e}")

def send_eod_summary():
    try:
        df = get_trades()
        total_pnl = get_total_pnl() * 65

        open_trades = df[df['status'] == 'OPEN']
        closed_trades = df[df['status'] == 'CLOSED']

        # Build open spreads table
        if open_trades.empty:
            open_msg = "No open spreads."
        else:
            lines = []
            for _, row in open_trades.iterrows():
                lines.append(
                    f"  Spread {int(row['spread_id']) if pd.notna(row['spread_id']) else '?'} | "
                    f"{row['side']} {row['symbol']} | "
                    f"Entry: {row['entry_price']} | "
                    f"LTP: {row['ltp']} | "
                    f"PnL: {row['pnl']:.2f}pts"
                )
            open_msg = "\n".join(lines)

        sign = "+" if total_pnl >= 0 else ""
        message = (f"📋 EOD Summary\n"
            f"{'─' * 30}\n"
            f"💰 Total PnL: {sign}₹{total_pnl:,.2f} (65× lot)\n"
            f"✅ Closed trades: {len(closed_trades)}\n"
            f"⚠️ Still open: {len(open_trades)}\n"
            f"{'─' * 30}\n"
            f"Open Positions:\n{open_msg}")
        notify(message)
    except Exception as e:
        notify(f"⚠️ EOD summary failed: {e}")

