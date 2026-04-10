import matplotlib.dates as mdates
import yfinance as yf
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os

def iv_vs_rv(window=20):
    END_DATE   = datetime.today()
    START_DATE = END_DATE - timedelta(days=180)
    WINDOW = window
    nifty = yf.download("^NSEI",  start=START_DATE, end=END_DATE, auto_adjust=True)
    vix = yf.download("^INDIAVIX", start=START_DATE, end=END_DATE, auto_adjust=True)
    log_ret = np.log(nifty["Close"] / nifty["Close"].shift(1))
    nifty_vol = log_ret.rolling(WINDOW).std() * np.sqrt(252) * 100   # → %
    vix_close = vix["Close"].reindex(nifty_vol.index, method="ffill")
    df = pd.DataFrame({"NIFTY 30d Realised Vol (%)": nifty_vol.squeeze(), "India VIX (%)": vix_close.squeeze(),}).dropna()
    fig, ax = plt.subplots(figsize=(13, 5))
    ax.plot(df.index, df["NIFTY 30d Realised Vol (%)"], label="NIFTY 30d Realised Vol", color="#1f77b4", linewidth=1.8)
    ax.plot(df.index, df["India VIX (%)"],label="India VIX (Implied)", color="#d62728",linewidth=1.8, linestyle="--")
    ax.fill_between(df.index, df["NIFTY 30d Realised Vol (%)"], df["India VIX (%)"], alpha=0.12, color="purple",label="VIX − Realised spread")
    ax.set_title("NIFTY 30-Day Realised Volatility vs India VIX", fontsize=14, pad=12)
    ax.set_ylabel("Volatility (%)", fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.xticks(rotation=25)
    ax.legend(fontsize=10)
    ax.grid(axis="y", linewidth=0.4, alpha=0.6)
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.savefig(r"C:\Users\Lenovo\PycharmProjects\Trading_System\dashboard\data\nifty_vol_vs_vix.png", dpi=150)


def vol_cone(lookback=3):
    TENORS = [5, 10, 20, 30, 45, 63, 126, 252]  # trading days
    TENOR_LBLS = ['5d', '10d', '20d', '30d', '45d', '63d', '126d', '252d']
    PERCENTILES = [10, 25, 50, 75, 90]
    LOOKBACK_YRS = lookback
    END = datetime.today()
    START = END - timedelta(days=365 * LOOKBACK_YRS + 60)
    nifty = yf.download("^NSEI", start=START, end=END, auto_adjust=True, progress=False)
    vix = yf.download("^INDIAVIX", start=START, end=END, auto_adjust=True, progress=False)
    nifty_close = nifty["Close"].squeeze()
    vix_close = vix["Close"].squeeze().reindex(nifty_close.index, method="ffill")
    log_ret = np.log(nifty_close / nifty_close.shift(1))
    cone = {}
    for t in TENORS:
        rv = log_ret.rolling(t).std() * np.sqrt(252) * 100  # annualised %
        rv = rv.dropna()
        cone[t] = {p: np.percentile(rv, p) for p in PERCENTILES}
        cone[t]["current_rv"] = rv.iloc[-1]  # most recent realised
    vix_current = vix_close.iloc[-1]

    def scale_vix(vix_30d, target_tenor, base_tenor=30):
        """Rough scaling: IV ~ VIX * sqrt(target/base) — a first-order approximation."""
        return vix_30d * np.sqrt(target_tenor / base_tenor)

    iv_curve = {t: scale_vix(vix_current, t) for t in TENORS}
    fig, ax = plt.subplots(figsize=(12, 6))
    x = list(range(len(TENORS)))
    p10 = [cone[t][10] for t in TENORS]
    p25 = [cone[t][25] for t in TENORS]
    p50 = [cone[t][50] for t in TENORS]
    p75 = [cone[t][75] for t in TENORS]
    p90 = [cone[t][90] for t in TENORS]
    crv = [cone[t]["current_rv"] for t in TENORS]
    iv = [iv_curve[t] for t in TENORS]
    ax.fill_between(x, p10, p90, alpha=0.18, color="#1f77b4", label="10–90th pct")
    ax.fill_between(x, p25, p75, alpha=0.30, color="#1f77b4", label="25–75th pct")
    ax.plot(x, p50, color="#1f77b4", linewidth=1.8, linestyle="--",marker="o", markersize=4, label="Median realised vol")
    ax.plot(x, crv, color="#2ca02c", linewidth=2, linestyle="-.",marker="s", markersize=5, label="Current realised vol")
    ax.plot(x, iv, color="#d62728", linewidth=2.2,marker="^", markersize=6, label=f"India VIX implied ({vix_current:.1f}% @ 30d, scaled)")

    for p, vals, va in [(10, p10, "top"), (25, p25, "top"), (75, p75, "bottom"), (90, p90, "bottom")]:
        ax.annotate(f"{p}th", xy=(x[-1], vals[-1]),xytext=(8, 0), textcoords="offset points", fontsize=8, color="#555", va=va)

    # Where does current IV sit in the cone?
    iv_30d_pct = sum(1 for v in log_ret.rolling(30).std().dropna() * np.sqrt(252) * 100 if v <= vix_current) / len(log_ret.rolling(30).std().dropna()) * 100
    regime_color = "#d62728" if iv_30d_pct > 75 else ("#2ca02c" if iv_30d_pct < 25 else "#ff7f0e")
    regime_label = "EXPENSIVE" if iv_30d_pct > 75 else ("CHEAP" if iv_30d_pct < 25 else "FAIR VALUE")
    ax.set_title(f"NIFTY 50 Vol Cone  |  India VIX = {vix_current:.1f}%  "f"→  {regime_label} (≈{iv_30d_pct:.0f}th pct)",fontsize=13, color=regime_color, pad=14)
    ax.set_xticks(x)
    ax.set_xticklabels(TENOR_LBLS)
    ax.set_xlabel("Measurement tenor", fontsize=11)
    ax.set_ylabel("Annualised volatility (%)", fontsize=11)
    ax.legend(fontsize=9, loc="upper right")
    ax.grid(axis="y", linewidth=0.4, alpha=0.5)
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.savefig(r"C:\Users\Lenovo\PycharmProjects\Trading_System\dashboard\data\nifty_vol_cone.png", dpi=150)


if __name__ == "__main__":
    vol_cone()
    iv_vs_rv()


