"""
Diversification Module
----------------------
Computes portfolio diversification metrics including:
- HHI Index
- Gini Coefficient
- Top 5 Concentration
- Sector Allocation
- Market Cap Split
- Diversification Score (0–100)
"""

from typing import Dict
import numpy as np


def run_diversification_module(
    holdings: Dict[str, dict],
    meta_map: Dict[str, dict],
    prices: Dict[str, float],
) -> dict:
    """
    Calculates diversification metrics for a given portfolio.

    Parameters
    ----------
    holdings : dict
        Example:
        {
          "RELIANCE": {"quantity": 10, "buy_price": 2500},
          "TCS": {"quantity": 5, "buy_price": 3400}
        }

    meta_map : dict
        Example:
        {
          "RELIANCE": {"sector": "Energy", "mcap": "Large"},
          "TCS": {"sector": "IT", "mcap": "Large"}
        }

    prices : dict
        Example:
        { "RELIANCE": 2800, "TCS": 3600 }

    Returns
    -------
    dict
        {
          "hhi_index": float,
          "gini_coefficient": float,
          "top_5_concentration": float,
          "sector_allocation": dict,
          "market_cap_split": dict,
          "score": float
        }
    """

    try:
        # --- Step 1: Compute portfolio weights ---
        portfolio_values = {
            stock: holdings[stock]["quantity"] * prices.get(stock, 0)
            for stock in holdings
        }
        total_value = sum(portfolio_values.values())

        if total_value == 0:
            raise ValueError("Total portfolio value is zero. Cannot compute weights.")

        weights = {s: v / total_value for s, v in portfolio_values.items()}

        # --- Step 2: HHI Index (Herfindahl–Hirschman Index) ---
        hhi = sum(w ** 2 for w in weights.values())

        # --- Step 3: Gini Coefficient ---
        weight_list = np.array(list(weights.values()))
        sorted_weights = np.sort(weight_list)
        n = len(sorted_weights)
        cumulative = np.cumsum(sorted_weights)
        gini = (n + 1 - 2 * np.sum(cumulative) / cumulative[-1]) / n if n > 1 else 0

        # --- Step 4: Top 5 Concentration ---
        top_5_concentration = sum(sorted_weights[::-1][:5])

        # --- Step 5: Sector Allocation ---
        sector_allocation = {}
        for stock, w in weights.items():
            sector = meta_map.get(stock, {}).get("sector", "Unknown")
            sector_allocation[sector] = sector_allocation.get(sector, 0) + w
        sector_allocation = {k: round(v * 100, 2) for k, v in sector_allocation.items()}

        # --- Step 6: Market Cap Split ---
        market_cap_split = {}
        for stock, w in weights.items():
            mcap = meta_map.get(stock, {}).get("mcap", "Unknown")
            market_cap_split[mcap] = market_cap_split.get(mcap, 0) + w
        market_cap_split = {k: round(v * 100, 2) for k, v in market_cap_split.items()}

        # --- Step 7: Diversification Score (0–100) ---
        # Lower HHI → better diversification; lower Gini → more equality
        hhi_score = max(0, 100 * (1 - (hhi - 1 / n) / (1 - 1 / n)))
        gini_score = 100 * (1 - gini)
        conc_score = max(0, 100 * (1 - top_5_concentration))
        score = round((0.4 * hhi_score + 0.3 * gini_score + 0.3 * conc_score), 2)

        return {
            "hhi_index": round(hhi, 4),
            "gini_coefficient": round(gini, 4),
            "top_5_concentration": round(top_5_concentration * 100, 2),
            "sector_allocation": sector_allocation,
            "market_cap_split": market_cap_split,
            "score": score,
        }

    except Exception as e:
        return {"error": str(e)}
