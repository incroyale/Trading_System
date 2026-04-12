# Trading System

A live options credit spread trading dashboard for Indian markets (NSE/NFO), built on top of the AngelOne API. Trades Bear Call Spreads and Bull Put Spreads on Nifty with automated entry screening, real-time Greeks monitoring, and rule-based exit management.

---

## Strategies

### Bear Call Spread (CE)
Sell an OTM call, buy a further OTM call at the same expiry. Profits when Nifty stays below the short strike. Benefits from time decay and IV contraction.

### Bull Put Spread (PE)
Sell an OTM put, buy a further OTM put at the same expiry. Profits when Nifty stays above the short strike. Benefits from time decay and IV contraction.

Both strategies target **30–45 DTE** with short strike deltas in the **0.15–0.20** range.

---

## Project Structure

```
Trading_System/
├── analytics/
│   ├── graphs.py           # Pre-market vol charts (RV vs IV, vol cone)
│   └── telegram.py         # Telegram notifications + EOD summary
├── broker/
│   └── connection.py       # AngelOne API connection (singleton)
├── dashboard/
│   ├── app.py              # Dash UI — 3 tabs: Live Spreads, Opportunities, Portfolio
│   └── portfolio.py        # SQLite trade log, LTP polling, exit rules
├── data/                   # CSV snapshots, vol charts, equity curve, portfolio DB
├── strategies/
│   ├── markets_hub/
│   │   └── market_data_hub.py   # Universe builder, live WebSocket feed, Greeks cache
│   └── credit_spread/
│       ├── signals_india.py     # CE/PE leg filtering and Greeks-based screening
│       └── spread_builder.py    # Spread construction and reward/risk ranking
├── notebooks/              # Research and backtesting notebooks
├── config.py               # Broker credentials (not committed)
├── main.py                 # Entry point — pre-market → dashboard → post-market
└── .env.example            # Environment variable template
```

---

## Dashboard Tabs

**Live Credit Spread** — Real-time CE and PE leg data (LTP, bid, ask, IV, delta, gamma, theta, vega) grouped by expiry. Updates every 500ms. Active 10:00–14:30.

**Opportunities** — Ranked bear call and bull put spread candidates with reward/risk ratios. Includes pre-market volatility charts (RV vs IV, vol cone). Active 10:00–14:30.

**Portfolio** — Paper trade log with spread-level PnL, max profit tracking, and manual trade entry/close. Active always.

---

## Exit Rules

Automated exits run every 2 seconds during market hours (9:15–15:30) via the LTP polling loop:

| Rule | Trigger |
|------|---------|
| Take Profit | Combined spread PnL ≥ 70% of max profit |
| Stop Loss | Combined spread PnL ≤ −250% of max profit |

Both legs of a spread are closed atomically when triggered.

---

## Trade Entry

Trades are logged manually via the Portfolio tab UI. Each spread requires:
- **Spread ID** — groups both legs together for PnL and exit rule calculation
- **Exchange** — NFO
- **Trading Symbol** — e.g. NIFTY25APR24000CE
- **Symbol Token** — AngelOne token for the instrument

Max profit is automatically calculated when the second leg of a spread is entered (SELL LTP − BUY LTP).

---

## Market Hours Gating

| Window | What runs |
|--------|-----------|
| 9:15 – 15:30 | LTP polling, exit rules, portfolio updates |
| 10:00 – 14:30 | Greeks refresh, CSV writer, spread builder, signal tabs |
| Outside hours | UI only, no broker calls |

---

## Notifications

Telegram notifications are sent for:
- System start
- System stop
- EOD summary — total PnL (₹, 65× lot size), closed trade count, open positions table

---

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/incroyale/Trading_System.git
cd Trading_System
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure credentials
Copy `.env.example` to `config.py` and fill in your AngelOne API credentials:
```python
BROKER_USERNAME  = "your_client_code"
BROKER_PASSWORD  = "your_4_digit_mpin"
BROKER_TOTP_KEY  = "your_totp_secret"
BROKER_API_KEY   = "your_api_key"
```

### 4. Add your VPS IP to AngelOne API whitelist
Log in to the AngelOne developer portal and whitelist your server's public IP.

### 5. Run
```bash
python main.py
```

Dashboard accessible at `http://localhost:8050` or `http://<your-server-ip>:8050`.

---

## VPS Deployment (Ubuntu 22.04)

### Copy project to VPS
```bash
scp -r Trading_System root@<your-vps-ip>:/root/
```

### Install dependencies
```bash
pip3 install -r requirements.txt --break-system-packages
```

### Open port
```bash
ufw allow 8050
ufw allow ssh
ufw enable
```

### Schedule with cron (times in UTC, IST = UTC+5:30)
```bash
crontab -e
```
```
0 3 * * 1-5 cd /root/Trading_System && python3 main.py >> /root/trading.log 2>&1
30 11 * * 1-5 kill $(cat /root/Trading_System/trading.pid)
```

---

## Requirements

- Python 3.9+
- AngelOne trading account with API access
- Telegram bot token and chat ID for notifications

---