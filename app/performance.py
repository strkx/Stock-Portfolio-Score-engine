import numpy as np
import pandas as pd
from typing import Dict, List, Union

# -------------------------------------------------------------------------
# 🔹 Equity curve per holding
# -------------------------------------------------------------------------
def equity_curve_for_holding(bars, quantity, symbol):
    df = pd.DataFrame(bars)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    return quantity * df["close"].rename(symbol)

# -------------------------------------------------------------------------
# 🔹 Aggregate portfolio time-series
# -------------------------------------------------------------------------
def portfolio_timeseries(holdings, ohlc_map):
    series_list = []
    for sym, row in holdings.items():
        series_list.append(equity_curve_for_holding(ohlc_map[sym], row["quantity"], sym))
    df = pd.concat(series_list, axis=1).ffill()
    return df.sum(axis=1)

# -------------------------------------------------------------------------
# 🔹 Invested value
# -------------------------------------------------------------------------
def invested_value(holdings):
    return float(sum(row["quantity"] * row["buy_price"] for row in holdings.values()))

# -------------------------------------------------------------------------
# 🔹 Daily returns
# -------------------------------------------------------------------------
def daily_returns(curve):
    return curve.pct_change().dropna()

# -------------------------------------------------------------------------
# 🔹 Max drawdown (%)
# -------------------------------------------------------------------------
def max_drawdown(curve):
    cummax = curve.cummax()
    dd = (curve - cummax) / cummax
    return float(dd.min() * 100)

# -------------------------------------------------------------------------
# 🔹 Smoothed Sharpe ratio (120-day window + 30-day rolling std)
# -------------------------------------------------------------------------
def sharpe_ratio(returns, risk_free_annual=0.07, periods_per_year=252):
    """
    Computes annualized Sharpe ratio with smoothed volatility.
    - Uses 30-day rolling std to stabilize volatility.
    - Annualizes both mean and std (×√252).
    """
    if returns.empty:
        return 0.0

    # Convert risk-free rate to daily
    rf_daily = (1 + risk_free_annual) ** (1 / periods_per_year) - 1

    # Excess daily returns
    excess = returns - rf_daily

    # Rolling 30-day volatility smoothing
    rolling_std = excess.rolling(window=30).std().dropna()
    if rolling_std.empty:
        return 0.0

    avg_volatility = rolling_std.mean() * np.sqrt(periods_per_year)
    mean_return = excess.mean() * periods_per_year

    if avg_volatility == 0 or np.isnan(avg_volatility):
        return 0.0

    sharpe = mean_return / avg_volatility
    return float(round(sharpe, 3))

# -------------------------------------------------------------------------
# 🔹 Run performance module
# -------------------------------------------------------------------------
def run_performance_module(holdings, ohlc_map):
    """
    Main performance computation entrypoint.
    Returns a dictionary with portfolio metrics:
      - portfolio_value
      - invested_value
      - return_pct
      - sharpe_ratio (smoothed)
      - max_drawdown
    """
    curve = portfolio_timeseries(holdings, ohlc_map)
    rets = daily_returns(curve)

    inv_val = invested_value(holdings)
    port_val = float(curve.iloc[-1]) if not curve.empty else 0.0
    ret_pct = (port_val / inv_val - 1) * 100 if inv_val else 0.0

    return {
        "portfolio_value": round(port_val, 2),
        "invested_value": round(inv_val, 2),
        "return_pct": round(ret_pct, 3),
        "sharpe_ratio": sharpe_ratio(rets),
        "max_drawdown": round(max_drawdown(curve), 3),
    }