import pandas as pd
import yfinance as yf
from typing import Optional, List, Dict
from app.dhan_client import DhanClient
from app.perplexity_client import PerplexityClient


class DhanInstrumentLookup:
    """
    DhanInstrumentLookup:
    ----------------------
    - Loads Dhan master CSV once
    - get_security_id(symbol): finds security_id from symbol
    - get_symbol_from_id(security_id): reverse lookup
    - get_meta(security_id): sector, industry, mcap (via yfinance or Perplexity)
    - get_meta_ai_bulk(symbols): batch sector lookup via AI
    """

    def __init__(self, csv_url: str = "https://images.dhan.co/api-data/api-scrip-master.csv"):
        print("📥 Loading Dhan Instrument Master...")
        df = pd.read_csv(csv_url, low_memory=False)

        # ✅ Keep only NSE/BSE equities
        df = df[
            (df["SEM_SEGMENT"].astype(str).str.upper().isin(["NSE_EQ", "BSE_EQ"])) &
            (df["SEM_INSTRUMENT_NAME"].astype(str).str.upper() == "EQUITY")
        ].copy()

        # Normalize & clean
        df["SM_SYMBOL_NAME"] = df["SM_SYMBOL_NAME"].astype(str).str.upper().str.strip()
        df["SEM_SMST_SECURITY_ID"] = pd.to_numeric(df["SEM_SMST_SECURITY_ID"], errors="coerce")
        df["exchangeSegment"] = df["SEM_SEGMENT"].astype(str).str.upper()

        self.df = df.dropna(subset=["SEM_SMST_SECURITY_ID"]).copy()
        self._client = DhanClient()
        self._meta_cache = {}
        self._pplx = PerplexityClient()

        print(f"✅ Dhan master loaded: {len(self.df)} equities")

    # ----------------------------------------------------------------------
    # 🔹 SYMBOL → SECURITY ID
    # ----------------------------------------------------------------------
    def get_security_id(self, symbol: str) -> Optional[str]:
        sym = symbol.upper().strip()
        matches = self.df[self.df["SM_SYMBOL_NAME"] == sym]

        # Try variants if direct match fails
        if matches.empty:
            for variant in (
                sym + ".",
                sym + " LTD",
                sym + " LTD.",
                sym.replace(" LIMITED", " LTD.").replace("LIMITED", "LTD.")
            ):
                matches = self.df[self.df["SM_SYMBOL_NAME"] == variant]
                if not matches.empty:
                    break

        if matches.empty:
            matches = self.df[self.df["SM_SYMBOL_NAME"].str.contains(sym, case=False, regex=False)]

        if matches.empty:
            print(f"⚠️ No match found for {symbol}")
            return None

        sid = str(int(matches.iloc[0]["SEM_SMST_SECURITY_ID"]))
        print(f"✅ Matched {symbol} → {sid}")
        return sid

    # ----------------------------------------------------------------------
    # 🔹 SECURITY ID → SYMBOL
    # ----------------------------------------------------------------------
    def get_symbol_from_id(self, security_id: str) -> Optional[str]:
        row = self.df[self.df["SEM_SMST_SECURITY_ID"].astype(str) == str(security_id)]
        if row.empty:
            return None
        return row.iloc[0]["SM_SYMBOL_NAME"]

    # ----------------------------------------------------------------------
    # 🔹 SECURITY ID → META (Sector, Industry, MarketCap)
    # ----------------------------------------------------------------------
    def get_meta(self, security_id: str) -> dict:
        if security_id in self._meta_cache:
            return self._meta_cache[security_id]

        try:
            row = self.df[self.df["SEM_SMST_SECURITY_ID"].astype(str) == str(security_id)]
            if row.empty:
                return {"sector": "Unknown", "industry": "Unknown", "mcap": "Unknown"}

            raw_name = row.iloc[0]["SM_SYMBOL_NAME"].upper().strip()
            clean = raw_name.replace("LIMITED", "").replace("LTD.", "").strip()
            symbol = clean.split()[0]

            # Try Yahoo Finance
            info = None
            for suffix in [".NS", ".BO"]:
                try:
                    ticker = yf.Ticker(symbol + suffix)
                    info = ticker.info
                    if info and info.get("sector"):
                        break
                except Exception as e:
                    print(f"⚠️ yfinance error for {symbol+suffix}: {e}")
                    continue

            if info and info.get("sector"):
                sector = info.get("sector", "Unknown")
                industry = info.get("industry", "Unknown")
                mcap_val = info.get("marketCap", None)

                if not mcap_val:
                    mcap_cat = "Unknown"
                elif mcap_val >= 5e12:
                    mcap_cat = "Large"
                elif mcap_val >= 5e11:
                    mcap_cat = "Mid"
                else:
                    mcap_cat = "Small"

                result = {"sector": sector, "industry": industry, "mcap": mcap_cat}
                self._meta_cache[security_id] = result
                print(f"✅ [yfinance] {symbol}: {result}")
                return result

            # Fallback → Perplexity
            print(f"⚙️ Falling back to Perplexity AI for {raw_name}...")
            ai_result = self._fetch_from_ai(raw_name)
            if ai_result:
                self._meta_cache[security_id] = ai_result
                print(f"✅ [AI lookup] {raw_name}: {ai_result}")
                return ai_result
            else:
                return {"sector": "Unknown", "industry": "Unknown", "mcap": "Unknown"}

        except Exception as e:
            print(f"⚠️ Meta lookup failed for {security_id}: {e}")
            return {"sector": "Unknown", "industry": "Unknown", "mcap": "Unknown"}

    # ----------------------------------------------------------------------
    # 🔹 AI-based fallback
    # ----------------------------------------------------------------------
    def _fetch_from_ai(self, company_name: str) -> Optional[dict]:
        try:
            query = f"""
            You are a financial market analyst.
            For the Indian listed company "{company_name}", identify:
              1. Its primary SECTOR (Energy, IT, Banking, FMCG, etc.)
              2. Its MARKET CAP category: Large / Mid / Small.
            Respond only as JSON:
            {{"sector": "...", "mcap": "..."}}
            """
            response = self._pplx.ask_json(query)
            if isinstance(response, dict) and "sector" in response:
                response.setdefault("industry", "Unknown")
                return response
            return None
        except Exception as e:
            print(f"⚠️ AI meta fetch failed for {company_name}: {e}")
            return None

    # ----------------------------------------------------------------------
    # 🔹 Bulk AI Meta Fetch
    # ----------------------------------------------------------------------
    def get_meta_ai_bulk(self, symbols: List[str]) -> Dict[str, dict]:
        try:
            joined = "\n".join(symbols)
            query = f"""
            You are a financial market analyst.
            For each of the following Indian listed companies, identify their
            primary sector and market cap classification (Large/Mid/Small).
            Respond strictly as JSON list:
            [{{"symbol": "RELIANCE INDUSTRIES", "sector": "Energy", "mcap": "Large"}}, ...]
            Companies:
            {joined}
            """
            response = self._pplx.ask_json(query)
            meta_map = {}
            if isinstance(response, list):
                for item in response:
                    name = item.get("symbol", "").upper().strip()
                    meta_map[name] = {
                        "sector": item.get("sector", "Unknown"),
                        "industry": "Unknown",
                        "mcap": item.get("mcap", "Unknown")
                    }
            return meta_map
        except Exception as e:
            print(f"⚠️ AI bulk meta fetch failed: {e}")
            return {}
