import os
import time
import json
import requests
import pandas as pd
from datetime import date, timedelta, datetime
from typing import List, Dict, Optional, Tuple, Any
from dotenv import load_dotenv

PREFERRED_EQ_SEGMENTS = ("NSE_EQ", "BSE_EQ")
PREFERRED_EQ_INSTR    = ("EQUITY",)

def _coerce_str(x) -> str:
    return "" if x is None else str(x).strip()

def _get(row: dict, *keys, default=None):
    for k in keys:
        if k in row and pd.notna(row[k]):
            return row[k]
    return default

def _has_err(body: str, code: str) -> bool:
    try:
        j = json.loads(body)
        return j.get("errorCode") == code
    except Exception:
        return code in body

class DhanClient:
    
    MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
    BASE_URL   = "https://api.dhan.co"

    def __init__(self):
        load_dotenv()
        self.api_key: str = os.getenv("DHAN_API_KEY") or ""
        self.client_id: Optional[str] = os.getenv("DHAN_CLIENT_ID") or None

        if not self.api_key:
            raise ValueError("❌ Missing DHAN_API_KEY in .env")

        self.headers = {
            "access-token": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.client_id:
            self.headers["client-id"] = self.client_id

        print("📥 Loading Dhan instrument master...")
        self._master_df = pd.read_csv(self.MASTER_URL, low_memory=False)
        self._master_df.columns = [c.strip() for c in self._master_df.columns]
        if "SEM_SMST_SECURITY_ID" in self._master_df.columns:
            self._master_df["SEM_SMST_SECURITY_ID"] = self._master_df["SEM_SMST_SECURITY_ID"].astype(str)
        print(f"✅ Dhan CSV loaded: {len(self._master_df)} instruments")

    # -------------------------- Search --------------------------
    def instrument_search(self, query: str) -> List[dict]:
        q = query.lower().strip()
        id_col = "SEM_SMST_SECURITY_ID"
        name_cols = [c for c in self._master_df.columns if "SM_SYMBOL_NAME" in c or "TRADING_SYMBOL" in c]

        mask_name = pd.Series([False] * len(self._master_df))
        for c in name_cols:
            mask_name = mask_name | self._master_df[c].astype(str).str.lower().str.contains(q, na=False)
        mask_id = self._master_df[id_col].astype(str).str.contains(q, na=False) if id_col in self._master_df else False
        df = self._master_df[mask_name | mask_id]
        return df.to_dict(orient="records")

    # ----------------------- Resolver ---------------------------
    def _resolve_contract(
        self,
        security_id: str,
        preferred_segments: Tuple[str, ...] = PREFERRED_EQ_SEGMENTS,
        preferred_instr: Tuple[str, ...] = PREFERRED_EQ_INSTR,
        desired_segment: Optional[str] = None,
        desired_instrument: Optional[str] = None,
        desired_expiry_code: Optional[int] = None,
    ) -> Tuple[str, str, int]:
        """Pick the 'best' row for this securityId (cash equity by default)."""
        sid = str(security_id).strip()
        if "SEM_SMST_SECURITY_ID" not in self._master_df.columns:
            raise RuntimeError("Instrument master missing SEM_SMST_SECURITY_ID")

        rows = self._master_df[self._master_df["SEM_SMST_SECURITY_ID"] == sid]
        if rows.empty:
            raise ValueError(f"securityId {sid} not found in instrument master")

        # If user passed an explicit desired segment/instrument, try to match exactly first
        if desired_segment or desired_instrument or desired_expiry_code is not None:
            df = rows.copy()
            if desired_segment:
                df = df[df["SEM_EXM_EXCHANGE_SEGMENT"].astype(str) == str(desired_segment)]
            if desired_instrument:
                # CSV sometimes has SEM_INSTRUMENT_NAME / INSTRUMENT_NAME
                df = df[
                    df.get("SEM_INSTRUMENT_NAME", df.get("INSTRUMENT_NAME")).astype(str)
                    == str(desired_instrument)
                ]
            if desired_expiry_code is not None and "SEM_EXPIRY_CODE" in df.columns:
                df = df[df["SEM_EXPIRY_CODE"].fillna(0).astype(int) == int(desired_expiry_code)]
            if not df.empty:
                row = df.iloc[0].to_dict()
                exseg = _get(row, "SEM_EXM_EXCHANGE_SEGMENT", "EXCHANGE_SEGMENT", default="NSE_EQ")
                instr = _get(row, "SEM_INSTRUMENT_NAME", "INSTRUMENT_NAME", default="EQUITY")
                exp   = int(_get(row, "SEM_EXPIRY_CODE", "EXPIRY_CODE", default=0) or 0)
                return str(exseg), str(instr), exp

        # Otherwise: score-based pick favoring cash equity
        best = None
        best_score = -1
        for _, r in rows.iterrows():
            row = r.to_dict()
            exseg = _get(row, "SEM_EXM_EXCHANGE_SEGMENT", "EXCHANGE_SEGMENT", default="")
            instr = _get(row, "SEM_INSTRUMENT_NAME", "INSTRUMENT_NAME", default="")
            exp   = _get(row, "SEM_EXPIRY_CODE", "EXPIRY_CODE", default=0)
            try:
                exp = int(exp or 0)
            except Exception:
                exp = 0

            score = 0
            if str(exseg) in preferred_segments:
                score += 3
            if str(instr) in preferred_instr:
                score += 3
            if exp == 0:
                score += 1
            # Prefer active instruments if column exists
            active = _get(row, "SEM_ACTIVE", "ACTIVE", default="Y")
            if str(active).upper().startswith("Y"):
                score += 1

            if score > best_score:
                best_score = score
                best = (str(exseg or "NSE_EQ"), str(instr or "EQUITY"), int(exp))

        # Fallback if nothing scored (shouldn’t happen)
        if not best:
            first = rows.iloc[0].to_dict()
            return (
                str(_get(first, "SEM_EXM_EXCHANGE_SEGMENT", "EXCHANGE_SEGMENT", default="NSE_EQ")),
                str(_get(first, "SEM_INSTRUMENT_NAME", "INSTRUMENT_NAME", default="EQUITY")),
                int(_get(first, "SEM_EXPIRY_CODE", "EXPIRY_CODE", default=0) or 0),
            )
        return best

    # ---------------------- LTP snapshot ------------------------
    def get_current_prices(self, security_ids: List[str], exchange_segment: str = "NSE_EQ") -> Dict[str, Optional[float]]:
        if not security_ids:
            return {}
        payload = {exchange_segment: [str(s) for s in security_ids]}
        url = f"{self.BASE_URL}/v2/marketfeed/ltp"
        for attempt in range(3):
            try:
                r = requests.post(url, headers=self.headers, json=payload, timeout=10)
                print(f"📡 LTP req → {r.status_code}")
                body = r.text
                if _has_err(body, "DH-906"):
                    raise RuntimeError("❌ Invalid Token (DH-906). Fix DHAN_API_KEY in your .env")
                if r.status_code == 429:
                    time.sleep(2); continue
                r.raise_for_status()
                data = r.json().get("data", {}).get(exchange_segment, {})
                out = {}
                for sid, info in data.items():
                    try:
                        out[str(sid)] = float(info.get("last_price"))
                    except Exception:
                        out[str(sid)] = None
                return out
            except Exception as e:
                print(f"⚠️ LTP fetch failed (attempt {attempt+1}/3): {e}")
                time.sleep(1)
        return {str(sid): None for sid in security_ids}

    # ---------------------- Daily OHLC --------------------------
    def get_ohlc(
        self,
        security_id: str,
        days: int = 60,
        desired_segment: Optional[str] = None,
        desired_instrument: Optional[str] = None,
        desired_expiry_code: Optional[int] = None,
    ) -> List[dict]:
        # Resolve contract preferring cash equity unless overridden
        try:
            exchange_segment, instrument, expiry_code = self._resolve_contract(
                security_id,
                desired_segment=desired_segment,
                desired_instrument=desired_instrument,
                desired_expiry_code=desired_expiry_code,
            )
        except Exception as e:
            print(f"❌ Contract resolve failed for {security_id}: {e}")
            return []

        end_date = date.today() + timedelta(days=1)   # include up to 'today'
        start_date = end_date - timedelta(days=days)

        payload = {
            "securityId": str(security_id),
            "exchangeSegment": exchange_segment,
            "instrument": instrument,
            "expiryCode": int(expiry_code),
            "fromDate": str(start_date),
            "toDate": str(end_date),
            "oi": instrument.upper().startswith(("FUT", "OPT")),
        }

        url = f"{self.BASE_URL}/v2/charts/historical"
        for attempt in range(3):
            try:
                r = requests.post(url, headers=self.headers, json=payload, timeout=30)
                print(f"📡 OHLC req {security_id} → {r.status_code} ({exchange_segment}/{instrument}, expiry={expiry_code})")
                body = r.text
                if _has_err(body, "DH-906"):
                    raise RuntimeError("❌ Invalid Token (DH-906). Fix DHAN_API_KEY in your .env")
                if r.status_code == 429:
                    print("⏳ Rate limited — waiting 3s..."); time.sleep(3); continue
                if r.status_code >= 400:
                    print("🧾 Server says:", body[:500])
                    print("🧪 Payload used:", json.dumps(payload, indent=2))
                r.raise_for_status()
                data = r.json()
                break
            except Exception as e:
                print(f"⚠️ OHLC fetch failed for {security_id} (attempt {attempt+1}/3): {e}")
                if attempt == 2:
                    return []
                time.sleep(2)

        if not isinstance(data, dict) or "open" not in data or "timestamp" not in data:
            print(f"⚠️ Empty/invalid response for {security_id}: {data}")
            return []

        n = min(*(len(data.get(k, [])) for k in ("open","high","low","close","volume","timestamp")))
        out = []
        for i in range(n):
            try:
                out.append({
                    "date": pd.to_datetime(int(data["timestamp"][i]), unit="s", utc=True),
                    "open": float(data["open"][i]),
                    "high": float(data["high"][i]),
                    "low": float(data["low"][i]),
                    "close": float(data["close"][i]),
                    "volume": int(data["volume"][i]),
                })
            except Exception:
                continue
        print(f"✅ {len(out)} OHLC candles fetched for {security_id}")
        return out

    # ---------------------- Intraday OHLC -----------------------
    def get_intraday(
        self,
        security_id: str,
        interval: int = 15,
        lookback_days: int = 1,
        desired_segment: Optional[str] = None,
        desired_instrument: Optional[str] = None,
        desired_expiry_code: Optional[int] = None,
    ) -> List[dict]:
        if str(interval) not in {"1","5","15","25","60"}:
            raise ValueError("interval must be one of 1,5,15,25,60")

        try:
            exchange_segment, instrument, expiry_code = self._resolve_contract(
                security_id,
                desired_segment=desired_segment,
                desired_instrument=desired_instrument,
                desired_expiry_code=desired_expiry_code,
            )
        except Exception as e:
            print(f"❌ Contract resolve failed for {security_id}: {e}")
            return []

        # Use IST to avoid market-day boundary issues
        try:
            import pytz
            ist = pytz.timezone("Asia/Kolkata")
            end_time = datetime.now(ist)
            start_time = end_time - timedelta(days=int(lookback_days))
        except Exception:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=int(lookback_days))

        payload = {
            "securityId": str(security_id),
            "exchangeSegment": exchange_segment,
            "instrument": instrument,
            "interval": str(interval),
            "oi": instrument.upper().startswith(("FUT", "OPT")),
            "fromDate": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "toDate": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        url = f"{self.BASE_URL}/v2/charts/intraday"
        for attempt in range(3):
            try:
                r = requests.post(url, headers=self.headers, json=payload, timeout=30)
                print(f"📡 Intraday req {security_id} → {r.status_code} ({exchange_segment}/{instrument}, {interval}m)")
                body = r.text
                if _has_err(body, "DH-906"):
                    raise RuntimeError("❌ Invalid Token (DH-906). Fix DHAN_API_KEY in your .env")
                if r.status_code == 429:
                    print("⏳ Rate limited — retrying in 2s..."); time.sleep(2); continue
                if r.status_code >= 400:
                    print("🧾 Server says:", body[:500])
                    print("🧪 Payload used:", json.dumps(payload, indent=2))
                r.raise_for_status()
                data = r.json()
                break
            except Exception as e:
                print(f"⚠️ Intraday fetch failed for {security_id} (attempt {attempt+1}/3): {e}")
                if attempt == 2:
                    return []
                time.sleep(2)

        if not isinstance(data, dict) or "open" not in data or "timestamp" not in data:
            print(f"⚠️ Empty intraday response for {security_id}: {data}")
            return []

        n = min(*(len(data.get(k, [])) for k in ("open","high","low","close","volume","timestamp")))
        out = []
        for i in range(n):
            try:
                out.append({
                    "date": pd.to_datetime(int(data["timestamp"][i]), unit="s", utc=True),
                    "open": float(data["open"][i]),
                    "high": float(data["high"][i]),
                    "low": float(data["low"][i]),
                    "close": float(data["close"][i]),
                    "volume": int(data["volume"][i]),
                })
            except Exception:
                continue
        print(f"✅ {len(out)} intraday candles fetched for {security_id}")
        return out


# ---------------------- Quick manual test ----------------------
if __name__ == "__main__":
    c = DhanClient()
    test_ids = ["1333", "4963", "10604", "236"]

    print("\n=== Daily OHLC (prefer cash equity) ===")
    for sid in test_ids:
        rows = c.get_ohlc(sid, days=60)  # prefers NSE_EQ/BSE_EQ + EQUITY
        print(sid, "→", len(rows), "rows")

    print("\n=== Intraday 15-min (prefer cash equity) ===")
    for sid in test_ids:
        rows = c.get_intraday(sid, interval=15, lookback_days=1)
        print(sid, "→", len(rows), "rows")
