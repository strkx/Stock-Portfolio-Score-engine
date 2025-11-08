import numpy as np
import pandas as pd
from typing import Dict, List, Union

# -------------------------------------------------------------------------
# 🔹 Equity curve per holding
# -------------------------------------------------------------------------
def equity_curve_for_holding(bars, quantity, symbol):
    """
    bars: list[dict] or DataFrame with at least ['date','close']
    quantity: float
    returns: pd.Series indexed by datetime, values = position value in currency
    """
    if bars is None:
        return pd.Series(dtype=float, name=symbol)

    df = pd.DataFrame(bars)
    if df.empty or "close" not in df or "date" not in df:
        return pd.Series(dtype=float, name=symbol)

    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    s = (float(quantity) * df["close"]).rename(symbol)
    return s

# -------------------------------------------------------------------------
# 🔹 Aggregate portfolio time-series
# -------------------------------------------------------------------------
def portfolio_timeseries(holdings, ohlc_map):
    """
    holdings: dict like { 'SYM': {'quantity': q, 'buy_price': p}, ... }
    ohlc_map: dict like { 'SYM': [ {date, open, high, low, close, volume}, ... ], ... }
    returns: pd.Series (portfolio value over time)
    """
    series_list = []
    for sym, row in holdings.items():
        if sym not in ohlc_map:
            continue
        s = equity_curve_for_holding(ohlc_map[sym], row.get("quantity", 0.0), sym)
        if not s.empty:
            series_list.append(s)

    if not series_list:
        return pd.Series(dtype=float)

    df = pd.concat(series_list, axis=1).sort_index()

    # Forward-fill missing prices after each series' first valid point
    df = df.ffill()

    # Sum with min_count=1 so all-NaN rows DO NOT become zero (avoids fake zeros)
    curve = df.sum(axis=1, min_count=1).dropna()

    return curve

# -------------------------------------------------------------------------
# 🔹 Invested value (cost basis)
# -------------------------------------------------------------------------
def invested_value(holdings):
    """
    Sum of quantity * buy_price across positions.
    """
    return float(sum(float(row.get("quantity", 0.0)) * float(row.get("buy_price", 0.0))
                     for row in holdings.values()))

# -------------------------------------------------------------------------
# 🔹 Daily returns
# -------------------------------------------------------------------------
def daily_returns(curve):
    """
    Percentage returns from a currency-valued equity curve.
    """
    if curve is None or curve.empty:
        return pd.Series(dtype=float)
    return curve.pct_change().dropna()

# -------------------------------------------------------------------------
# 🔹 Max drawdown (%; negative number)
# -------------------------------------------------------------------------
def max_drawdown(curve):
    """
    Compute max drawdown from the equity curve.
    Returns a negative percentage (e.g., -35.2 for -35.2%).
    """
    if curve is None or curve.empty:
        return 0.0
    rolling_peak = curve.cummax()
    dd = (curve - rolling_peak) / rolling_peak
    return float(dd.min() * 100.0)

# -------------------------------------------------------------------------
# 🔹 Smoothed Sharpe ratio (120-day window + 30-day rolling std)
# -------------------------------------------------------------------------
def sharpe_ratio(returns, risk_free_annual=0.07, periods_per_year=252):
    """
    Computes annualized Sharpe ratio with smoothed volatility.
    - Uses 30-day rolling std to stabilize volatility.
    - Annualizes both mean and std (×√periods_per_year).
    """
    if returns is None or returns.empty:
        return 0.0

    # Convert risk-free rate to per-period (daily)
    rf_daily = (1 + float(risk_free_annual)) ** (1 / float(periods_per_year)) - 1

    # Excess daily returns
    excess = returns - rf_daily

    # Rolling 30-day volatility smoothing
    rolling_std = excess.rolling(window=30, min_periods=20).std().dropna()
    if rolling_std.empty:
        return 0.0

    avg_volatility = rolling_std.mean() * np.sqrt(periods_per_year)
    mean_return = excess.mean() * periods_per_year

    if not np.isfinite(avg_volatility) or avg_volatility == 0:
        return 0.0

    sharpe = mean_return / avg_volatility
    return float(np.round(sharpe, 3))

# -------------------------------------------------------------------------
# 🔹 Run performance module
# -------------------------------------------------------------------------
def run_performance_module(holdings, ohlc_map):
    """
    Main performance computation entrypoint.
    Returns a dictionary with portfolio metrics:
      - portfolio_value
      - invested_value
      - return_pct           (based on rebased curve → cost-basis aligned)
      - sharpe_ratio         (from the same rebased curve returns)
      - max_drawdown         (from the same rebased curve; % negative)
    Notes:
      • We rebase the OHLC-derived equity curve so its first point equals the
        cost-basis invested_value. This aligns all metrics to the same baseline,
        even if the OHLC window is shorter than holding history.
      • If invested_value is 0 (degenerate input), we fall back gracefully.
    """
    curve_raw = portfolio_timeseries(holdings, ohlc_map)

    inv_val = invested_value(holdings)
    if curve_raw is None or curve_raw.empty:
        return {
            "portfolio_value": 0.0,
            "invested_value": round(inv_val, 2),
            "return_pct": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
        }

    # --- Rebase curve so that first point equals invested_value (cost-basis alignment)
    first_val = float(curve_raw.iloc[0])
    if first_val > 0 and inv_val > 0:
        scale = inv_val / first_val
        curve = curve_raw * scale
    else:
        # If either is non-positive, use raw curve (and derived metrics still work)
        curve = curve_raw.copy()

    # Totals & return from the aligned curve
    port_val = float(curve.iloc[-1])
    ret_pct = ((port_val / inv_val) - 1) * 100.0 if inv_val > 0 else 0.0

    # Returns & risk metrics from the same aligned curve
    rets = daily_returns(curve)

    return {
        "portfolio_value": round(port_val, 2),
        "invested_value": round(inv_val, 2),
        "return_pct": float(np.round(ret_pct, 3)),
        "sharpe_ratio": sharpe_ratio(rets),
        "max_drawdown": float(np.round(max_drawdown(curve), 3)),
    }
