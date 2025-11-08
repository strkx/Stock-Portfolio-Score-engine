import pandas as pd
from typing import Optional, List, Dict
from pathlib import Path

# Optional deps: keep your imports but guard them
try:
    import yfinance as yf
except Exception:  # pragma: no cover
    yf = None

try:
    from app.dhan_client import DhanClient
except Exception:
    DhanClient = None  # lazy / optional

try:
    from app.perplexity_client import PerplexityClient
except Exception:
    PerplexityClient = None


class DhanInstrumentLookup:
    """
    DhanInstrumentLookup:
    ----------------------
    - Loads & normalizes Dhan master CSV once
    - get_security_id(symbol): finds security_id from symbol (robust matching)
    - get_symbol_from_id(security_id): reverse lookup
    - get_meta(security_id): sector/industry/mcap (yfinance first; AI fallback optional)
    - get_meta_ai_bulk(symbols): batch AI lookup
    """

    MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
    PREFERRED_EQ_SEGMENTS = {"NSE_EQ", "BSE_EQ"}

    def __init__(self, csv_url: str = None):
        csv_url = csv_url or self.MASTER_URL

        print("📥 Loading Dhan instrument master...")
        raw_df = pd.read_csv(csv_url, low_memory=False)
        # Strip column whitespace
        raw_df.columns = [c.strip() for c in raw_df.columns]

        # ---- Column aliases (handle schema drift) ----
        col_symbol_name = (
            "SM_SYMBOL_NAME" if "SM_SYMBOL_NAME" in raw_df.columns
            else ("SEM_SYMBOL_NAME" if "SEM_SYMBOL_NAME" in raw_df.columns else None)
        )
        col_trading_symbol = (
            "TRADING_SYMBOL" if "TRADING_SYMBOL" in raw_df.columns
            else ("SEM_TRADING_SYMBOL" if "SEM_TRADING_SYMBOL" in raw_df.columns else None)
        )
        col_security_id = (
            "SEM_SMST_SECURITY_ID" if "SEM_SMST_SECURITY_ID" in raw_df.columns
            else ("SECURITY_ID" if "SECURITY_ID" in raw_df.columns else None)
        )
        col_instr = (
            "SEM_INSTRUMENT_NAME" if "SEM_INSTRUMENT_NAME" in raw_df.columns
            else ("INSTRUMENT_NAME" if "INSTRUMENT_NAME" in raw_df.columns else None)
        )
        # Full combined segment often lives here:
        col_exseg = (
            "SEM_EXM_EXCHANGE_SEGMENT" if "SEM_EXM_EXCHANGE_SEGMENT" in raw_df.columns
            else ("EXCHANGE_SEGMENT" if "EXCHANGE_SEGMENT" in raw_df.columns else None)
        )
        # Else: exchange + segment we can combine
        col_exchange = "SEM_EXCHANGE" if "SEM_EXCHANGE" in raw_df.columns else "EXCHANGE" if "EXCHANGE" in raw_df.columns else None
        col_segment = "SEM_SEGMENT" if "SEM_SEGMENT" in raw_df.columns else "SEGMENT" if "SEGMENT" in raw_df.columns else None

        # Validate required minimum columns
        if not col_security_id:
            raise RuntimeError("Dhan CSV missing SECURITY ID column (SEM_SMST_SECURITY_ID/SECURITY_ID).")
        if not col_instr:
            raise RuntimeError("Dhan CSV missing instrument name column (SEM_INSTRUMENT_NAME/INSTRUMENT_NAME).")
        if not (col_symbol_name or col_trading_symbol):
            raise RuntimeError("Dhan CSV missing symbol columns (SM_SYMBOL_NAME/SEM_SYMBOL_NAME/TRADING_SYMBOL).")

        # ---- Build normalized dataframe ----
        df = pd.DataFrame()
        # Symbol columns (upper-trim)
        if col_symbol_name:
            df["SM_SYMBOL_NAME"] = raw_df[col_symbol_name].astype(str).str.upper().str.strip()
        else:
            df["SM_SYMBOL_NAME"] = ""

        if col_trading_symbol:
            df["TRADING_SYMBOL"] = raw_df[col_trading_symbol].astype(str).str.upper().str.strip()
        else:
            df["TRADING_SYMBOL"] = ""

        # Security ID: KEEP AS STRING
        df["SECURITY_ID"] = raw_df[col_security_id].astype(str).str.strip()

        # Instrument
        df["INSTRUMENT_NAME"] = raw_df[col_instr].astype(str).str.upper().str.strip()

        # Exchange segment (prefer combined; else combine exchange + segment)
        if col_exseg:
            df["exchangeSegment"] = raw_df[col_exseg].astype(str).str.upper().str.strip()
        else:
            ex = raw_df[col_exchange].astype(str).str.upper().str.strip() if col_exchange else ""
            seg = raw_df[col_segment].astype(str).str.upper().str.strip() if col_segment else ""
            if isinstance(ex, str) or isinstance(seg, str):
                # if missing, default empty
                df["exchangeSegment"] = (ex if isinstance(ex, pd.Series) else pd.Series([ex] * len(raw_df))).astype(str) + "_" + \
                                        (seg if isinstance(seg, pd.Series) else pd.Series([seg] * len(raw_df))).astype(str)
            else:
                df["exchangeSegment"] = ""

        # ---- Filter: keep only NSE/BSE equities ----
        eq_mask = df["INSTRUMENT_NAME"].fillna("").str.upper().eq("EQUITY")
        seg_mask = df["exchangeSegment"].fillna("").isin(self.PREFERRED_EQ_SEGMENTS)
        filtered = df[eq_mask & seg_mask].copy()

        # If nothing matched, relax the segment check but keep EQUITY (to avoid “0 equities” trap)
        if filtered.empty:
            filtered = df[eq_mask].copy()
            print("⚠️ No rows matched preferred segments; keeping all EQUITY rows for now.")

        # Index for faster lookup
        filtered["SM_SYMBOL_NAME"] = filtered["SM_SYMBOL_NAME"].fillna("")
        filtered["TRADING_SYMBOL"] = filtered["TRADING_SYMBOL"].fillna("")
        filtered["SECURITY_ID"] = filtered["SECURITY_ID"].fillna("")
        filtered["exchangeSegment"] = filtered["exchangeSegment"].fillna("")

        self.df = filtered.reset_index(drop=True)
        self._meta_cache: Dict[str, dict] = {}

        # Lazy init clients (don’t explode if missing)
        self._client = DhanClient() if DhanClient else None
        self._pplx = PerplexityClient() if PerplexityClient else None

        print(f"✅ Dhan master loaded: {len(self.df)} equities")

    # ------------------------------ helpers ------------------------------

    @staticmethod
    def _normalize_company_name(name: str) -> str:
        n = name.upper().strip()
        n = n.replace(" LIMITED", "").replace(" LTD.", "").replace(" LTD", "")
        return " ".join(n.split())

    def _candidate_names(self, sym: str) -> List[str]:
        # Try common variants for Indian names
        sym = sym.upper().strip()
        base = self._normalize_company_name(sym)
        return list(dict.fromkeys([
            sym,
            base,
            base + " LTD",
            base + " LTD.",
            base + " LIMITED",
        ]))

    # --------------------------- public methods --------------------------

    def get_security_id(self, symbol: str) -> Optional[str]:
        """
        SYMBOL → SECURITY ID
        Tries exact SM_SYMBOL_NAME, then TRADING_SYMBOL, then contains-search on both.
        """
        if not symbol:
            return None
        candidates = self._candidate_names(symbol)

        # Exact: SM_SYMBOL_NAME or TRADING_SYMBOL
        for col in ("SM_SYMBOL_NAME", "TRADING_SYMBOL"):
            for cand in candidates:
                hits = self.df[self.df[col] == cand]
                if not hits.empty:
                    sid = hits.iloc[0]["SECURITY_ID"]
                    print(f"✅ Matched {symbol} [{col}] → {sid}")
                    return sid

        # Contains (fallback)
        for col in ("SM_SYMBOL_NAME", "TRADING_SYMBOL"):
            hits = self.df[self.df[col].str.contains(candidates[0], case=False, regex=False)]
            if not hits.empty:
                sid = hits.iloc[0]["SECURITY_ID"]
                print(f"✅ Fuzzy match {symbol} [{col}] → {sid}")
                return sid

        print(f"⚠️ No match found for {symbol}")
        return None

    def get_symbol_from_id(self, security_id: str) -> Optional[str]:
        """
        SECURITY ID → SYMBOL (prefer SM_SYMBOL_NAME; fallback TRADING_SYMBOL)
        """
        if not security_id:
            return None
        sid = str(security_id).strip()
        row = self.df[self.df["SECURITY_ID"] == sid]
        if row.empty:
            return None
        # Prefer company-style name if present
        name = row.iloc[0]["SM_SYMBOL_NAME"] or row.iloc[0]["TRADING_SYMBOL"]
        return name

    def get_meta(self, security_id: str) -> dict:
        """
        META lookup from Yahoo Finance; fallback to Perplexity if available.
        Returns {sector, industry, mcap} (mcap bucket: Large/Mid/Small/Unknown)
        """
        sid = str(security_id).strip()
        if not sid:
            return {"sector": "Unknown", "industry": "Unknown", "mcap": "Unknown"}

        if sid in self._meta_cache:
            return self._meta_cache[sid]

        try:
            row = self.df[self.df["SECURITY_ID"] == sid]
            if row.empty:
                return {"sector": "Unknown", "industry": "Unknown", "mcap": "Unknown"}

            # Prefer trading symbol for exchange tickers
            raw_company = (row.iloc[0]["SM_SYMBOL_NAME"] or row.iloc[0]["TRADING_SYMBOL"]).strip()
            trading = row.iloc[0]["TRADING_SYMBOL"].strip()
            base_for_yf = trading or self._normalize_company_name(raw_company)
            # Try Yahoo as <symbol>.NS then <symbol>.BO
            if yf is not None and base_for_yf:
                for suffix in (".NS", ".BO"):
                    try:
                        tkr = yf.Ticker(base_for_yf + suffix)
                        # yfinance .info can be slow; still the most compatible
                        info = tkr.info
                        if info and info.get("sector"):
                            sector = info.get("sector") or "Unknown"
                            industry = info.get("industry") or "Unknown"
                            mcap_val = info.get("marketCap")
                            if not mcap_val:
                                mcap_bucket = "Unknown"
                            elif mcap_val >= 5e12:
                                mcap_bucket = "Large"
                            elif mcap_val >= 5e11:
                                mcap_bucket = "Mid"
                            else:
                                mcap_bucket = "Small"
                            out = {"sector": sector, "industry": industry, "mcap": mcap_bucket}
                            self._meta_cache[sid] = out
                            print(f"✅ [yfinance] {base_for_yf+suffix}: {out}")
                            return out
                    except Exception as e:
                        # Keep trying next suffix
                        print(f"⚠️ yfinance error for {base_for_yf+suffix}: {e}")
                        continue

            # Fallback to Perplexity (optional)
            if self._pplx is not None:
                try:
                    print(f"⚙️ Falling back to Perplexity AI for {raw_company}…")
                    query = (
                        'You are a financial market analyst. '
                        f'For the Indian listed company "{raw_company}", identify: '
                        '1) primary sector, and 2) market cap bucket: Large/Mid/Small. '
                        'Respond only as JSON: {"sector":"...","mcap":"..."}'
                    )
                    resp = self._pplx.ask_json(query)
                    if isinstance(resp, dict) and "sector" in resp:
                        resp.setdefault("industry", "Unknown")
                        self._meta_cache[sid] = resp
                        print(f"✅ [AI lookup] {raw_company}: {resp}")
                        return resp
                except Exception as e:
                    print(f"⚠️ AI meta fetch failed for {raw_company}: {e}")

            return {"sector": "Unknown", "industry": "Unknown", "mcap": "Unknown"}

        except Exception as e:
            print(f"⚠️ Meta lookup failed for {security_id}: {e}")
            return {"sector": "Unknown", "industry": "Unknown", "mcap": "Unknown"}

    def get_meta_ai_bulk(self, symbols: List[str]) -> Dict[str, dict]:
        """
        Bulk meta via Perplexity. Safe no-op if PerplexityClient not available.
        """
        if self._pplx is None or not symbols:
            return {}

        try:
            joined = "\n".join([s.strip() for s in symbols if s and s.strip()])
            if not joined:
                return {}
            query = (
                "You are a financial market analyst.\n"
                "For each of the following Indian listed companies, identify their "
                "primary sector and market cap classification (Large/Mid/Small).\n"
                "Respond strictly as JSON list like: "
                '[{"symbol":"RELIANCE INDUSTRIES","sector":"Energy","mcap":"Large"}, ...]\n'
                f"Companies:\n{joined}\n"
            )
            response = self._pplx.ask_json(query)
            meta_map: Dict[str, dict] = {}
            if isinstance(response, list):
                for item in response:
                    name = (item.get("symbol") or "").upper().strip()
                    if not name:
                        continue
                    meta_map[name] = {
                        "sector": item.get("sector", "Unknown"),
                        "industry": "Unknown",
                        "mcap": item.get("mcap", "Unknown"),
                    }
            return meta_map
        except Exception as e:
            print(f"⚠️ AI bulk meta fetch failed: {e}")
            return {}
