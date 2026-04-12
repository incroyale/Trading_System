# main.py
import os
import signal
import datetime
from analytics.graphs import vol_cone, iv_vs_rv
from analytics.telegram import notify, send_eod_summary
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")

def on_stop(signum, frame):
    notify(f"🔴 Trading system stopped — {datetime.datetime.now(tz=IST).strftime('%H:%M:%S')} IST")
    send_eod_summary()
    exit(0)

signal.signal(signal.SIGTERM, on_stop)
signal.signal(signal.SIGINT, on_stop)

# Write PID
with open("trading.pid", "w") as f:
    f.write(str(os.getpid()))

# ── Pre-market ────────────────────────────────────────────────────────────────
iv_vs_rv()
vol_cone()

# ── Start dashboard ───────────────────────────────────────────────────────────
notify(f"🟢 Trading system started — {datetime.datetime.now(tz=IST).strftime('%H:%M:%S')} IST")
from dashboard.app import app
app.run(debug=False)

# ── Post-market (runs after app is stopped) ───────────────────────────────────
notify(f"🔴 Trading system stopped — {datetime.datetime.now().strftime('%H:%M:%S')} IST")