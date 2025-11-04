# app/main.py
from fastapi import FastAPI
import pandas as pd

from app.dhan_client import DhanClient
from app.perplexity_client import PerplexityClient
from app.performance import run_performance_module
from app.diversification import run_diversification_module
from app.dhan_instrument_lookup import DhanInstrumentLookup

app = FastAPI()

# 🔹 Load CSV lookup once (for symbol-to-security_id mapping)
lookup = DhanInstrumentLookup()

# 🔹 Create reusable Dhan client
client = DhanClient()


@app.get("/health")
def health():
    """Simple healthcheck."""
    return {"status": "ok"}


@app.post("/portfolio/score")
def score_portfolio(payload: dict):
    """
    Input can use either SYMBOL or SECURITY_ID as keys.
    Example:
    {
      "goal": "Retirement in 20 years",
      "holdings": {
        "RELIANCE INDUSTRIES LTD": {"quantity": 10, "buy_price": 2200},
        "13188": {"quantity": 5, "buy_price": 3200}   # direct security_id (TCS)
      }
    }
    """
    holdings = payload.get("holdings", {})
    goal = payload.get("goal", "General growth")

    if not holdings:
        return {"error": "No holdings provided"}

    security_map = {}
    missing = []

    # 🟢 Resolve each key — numeric = already security_id, text = lookup from CSV
    for key in holdings.keys():
        if key.isdigit():
            security_map[key] = key  # already security_id
        else:
            sid = lookup.get_security_id(key)
            if sid:
                security_map[key] = sid
            else:
                missing.append(key)

    if missing:
        return {"error": "Could not find security ids", "missing": missing}

    # 🟢 Fetch OHLC data for each resolved ID
    ohlc_map = {}
    for label, sid in security_map.items():
        try:
            ohlc_data = client.get_ohlc(sid, days=60)
        except Exception as e:
            print(f"⚠️ Error fetching OHLC for {label}: {e}")
            ohlc_data = []

        if not ohlc_data:
            df = pd.DataFrame(columns=["date", "close"])
        else:
            df = pd.DataFrame(ohlc_data)
            # Ensure correct columns
            if "date" not in df.columns:
                df["date"] = pd.NaT
            if "close" not in df.columns:
                df["close"] = 0.0

        ohlc_map[label] = df

    # 🟢 Filter valid holdings (those with OHLC data)
    valid_holdings = {s: holdings[s] for s, df in ohlc_map.items() if not df.empty}
    if not valid_holdings:
        return {"error": "No OHLC data available for any holding"}

    # 🟢 Run Performance metrics
    perf = run_performance_module(valid_holdings, ohlc_map)

    # 🟢 Prepare latest prices (for diversification)
    latest_prices = {}
    for s, df in ohlc_map.items():
        if not df.empty:
            latest_prices[s] = float(df["close"].iloc[-1])

    # 🟢 Build meta_map using real data from Dhan master CSV
    meta_map = {}
    for label, sid in security_map.items():
        meta_map[label] = lookup.get_meta(sid)

    # 🟢 Run Diversification metrics
    div = run_diversification_module(valid_holdings, meta_map, latest_prices)

    # 🟢 Goal alignment via Perplexity
    pplx = PerplexityClient()
    try:
        goal_align = pplx.analyze_goal(perf, goal) or {
            "explanation": "no data",
            "score": 0.5,
        }
    except Exception as e:
        print(f"⚠️ Goal alignment failed: {e}")
        goal_align = {"explanation": "analysis failed", "score": 0.5}

    # 🟢 Combine all into final portfolio score
    return_pct = perf.get("return_pct", 0.0)
    diversification_score = div.get("score", 50)
    goal_score = goal_align.get("score", 0.5) * 100

    final_score = round(
        (0.5 * return_pct) + (0.3 * diversification_score) + (0.2 * goal_score), 2
    )

    return {
        "performance": perf,
        "diversification": div,
        "goal_alignment": goal_align,
        "final_score": final_score,
    }
