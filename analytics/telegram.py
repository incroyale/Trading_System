# analytics/telegram.py
import requests
from dotenv import load_dotenv
import os

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

