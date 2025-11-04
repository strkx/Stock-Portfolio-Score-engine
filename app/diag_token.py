from pathlib import Path
from dotenv import load_dotenv, dotenv_values
import os, json, time, requests, pandas as pd
from datetime import date, datetime, timedelta

MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
BASE_URL   = "https://api.dhan.co"

PREFERRED_EQ_SEGMENTS = ("NSE_EQ", "BSE_EQ")
PREFERRED_EQ_INSTR    = ("EQUITY",)

def load_env_here():
    here = Path(__file__).resolve().parent
    load_dotenv(here / ".env", override=True)
    key = (os.getenv("DHAN_API_KEY") or "").strip()
    cid = (os.getenv("DHAN_CLIENT_ID") or "").strip()
    print("🔑 Has key?", bool(key), "len:", len(key))
    print("🆔 Has client-id?", bool(cid), "value:", cid if cid else "<EMPTY>")
    if not key:
        raise SystemExit("❌ DHAN_API_KEY missing in .env next to this script")
    if not cid:
        print("⚠️  DHAN_CLIENT_ID empty — some endpoints may reject; set it in .env")
    return key, cid

def headers(key, cid):
    h = {
        "access-token": key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if cid:
        h["client-id"] = cid
    return h

def fetch_master():
    print("📥 Downloading Dhan instrument master…")
    df = pd.read_csv(MASTER_URL, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    if "SEM_SMST_SECURITY_ID" in df.columns:
        df["SEM_SMST_SECURITY_ID"] = df["SEM_SMST_SECURITY_ID"].astype(str)
    print(f"✅ Master loaded: {len(df)} rows")
    return df

def pick_contract(df: pd.DataFrame, security_id: str):
    sid = str(security_id).strip()
    rows = df[df["SEM_SMST_SECURITY_ID"] == sid]
    if rows.empty:
        raise ValueError(f"securityId {sid} not found in instrument master")
    best, score_best = None, -1
    for _, r in rows.iterrows():
        row = r.to_dict()
        exseg = str(row.get("SEM_EXM_EXCHANGE_SEGMENT") or row.get("EXCHANGE_SEGMENT") or "")
        instr = str(row.get("SEM_INSTRUMENT_NAME") or row.get("INSTRUMENT_NAME") or "")
        try:
            exp = int(row.get("SEM_EXPIRY_CODE") or row.get("EXPIRY_CODE") or 0)
        except Exception:
            exp = 0
        score = 0
        if exseg in PREFERRED_EQ_SEGMENTS: score += 3
        if instr in PREFERRED_EQ_INSTR:    score += 3
        if exp == 0:                        score += 1
        if score > score_best:
            score_best = score
            best = (exseg or "NSE_EQ", instr or "EQUITY", exp)
    return best  # (exchangeSegment, instrument, expiryCode)

def pretty_json(text, limit=400):
    try:
        return json.dumps(json.loads(text), indent=2)[:limit]
    except Exception:
        return text[:limit]

def test_ltp(key, cid, exchange_segment, ids):
    print("\n=== 🔎 LTP test ===")
    url = f"{BASE_URL}/v2/marketfeed/ltp"
    payload = {exchange_segment: [str(x) for x in ids]}
    r = requests.post(url, headers=headers(key, cid), json=payload, timeout=15)
    print("➡️  POST", url)
    print("📦 Payload:", json.dumps(payload))
    print("🟡 Status:", r.status_code)
    print("📝 Body  :", pretty_json(r.text))
    if r.status_code == 200:
        data = r.json().get("data", {}).get(exchange_segment, {})
        # print first couple
        for k in list(data.keys())[:3]:
            print(f"   • {k} → {data[k]}")
    else:
        print("⚠️  LTP failed; fix credentials/client-id if 401/invalid.")

def test_daily_ohlc(key, cid, sec_tuple, days=10):
    sec_id, exseg, instr, exp = sec_tuple
    print("\n=== 📈 Daily OHLC test ===")
    url = f"{BASE_URL}/v2/charts/historical"
    end_date = date.today() + timedelta(days=1)   # include today (non-inclusive toDate)
    start_date = end_date - timedelta(days=days)
    payload = {
        "securityId": str(sec_id),
        "exchangeSegment": exseg,
        "instrument": instr,
        "expiryCode": int(exp),
        "fromDate": str(start_date),
        "toDate": str(end_date),
        "oi": instr.upper().startswith(("FUT","OPT"))
    }
    r = requests.post(url, headers=headers(key, cid), json=payload, timeout=30)
    print("➡️  POST", url)
    print("📦 Payload:", json.dumps(payload, indent=2))
    print("🟡 Status:", r.status_code)
    print("📝 Body  :", pretty_json(r.text, 600))
    if r.status_code == 200:
        data = r.json()
        n = min(*(len(data.get(k,[])) for k in ("open","high","low","close","volume","timestamp")))
        print(f"✅ candles: {n}")
        if n:
            i0, iL = 0, n-1
            print("   • first:", {
                "ts": pd.to_datetime(int(data["timestamp"][i0]), unit="s", utc=True).isoformat(),
                "o": data["open"][i0], "h": data["high"][i0], "l": data["low"][i0], "c": data["close"][i0]
            })
            print("   • last :", {
                "ts": pd.to_datetime(int(data["timestamp"][iL]), unit="s", utc=True).isoformat(),
                "o": data["open"][iL], "h": data["high"][iL], "l": data["low"][iL], "c": data["close"][iL]
            })

def test_intraday(key, cid, sec_tuple, interval=15, lookback_days=1):
    sec_id, exseg, instr, exp = sec_tuple
    print("\n=== 🕒 Intraday test ===")
    url = f"{BASE_URL}/v2/charts/intraday"
    if str(interval) not in {"1","5","15","25","60"}:
        raise SystemExit("interval must be one of 1,5,15,25,60")
    try:
        import pytz
        ist = pytz.timezone("Asia/Kolkata")
        end_time = datetime.now(ist)
        start_time = end_time - timedelta(days=lookback_days)
    except Exception:
        end_time = datetime.now()
        start_time = end_time - timedelta(days=lookback_days)
    payload = {
        "securityId": str(sec_id),
        "exchangeSegment": exseg,
        "instrument": instr,
        "interval": str(interval),
        "oi": instr.upper().startswith(("FUT","OPT")),
        "fromDate": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "toDate":   end_time.strftime("%Y-%m-%d %H:%M:%S")
    }
    r = requests.post(url, headers=headers(key, cid), json=payload, timeout=30)
    print("➡️  POST", url)
    print("📦 Payload:", json.dumps(payload, indent=2))
    print("🟡 Status:", r.status_code)
    print("📝 Body  :", pretty_json(r.text, 600))
    if r.status_code == 200:
        data = r.json()
        n = min(*(len(data.get(k,[])) for k in ("open","high","low","close","volume","timestamp")))
        print(f"✅ candles: {n}")
        if n:
            i0, iL = 0, n-1
            print("   • first:", {
                "ts": pd.to_datetime(int(data["timestamp"][i0]), unit="s", utc=True).isoformat(),
                "o": data["open"][i0], "h": data["high"][i0], "l": data["low"][i0], "c": data["close"][i0]
            })
            print("   • last :", {
                "ts": pd.to_datetime(int(data["timestamp"][iL]), unit="s", utc=True).isoformat(),
                "o": data["open"][iL], "h": data["high"][iL], "l": data["low"][iL], "c": data["close"][iL]
            })

if __name__ == "__main__":
    # 1) Env + headers
    key, cid = load_env_here()

    # 2) Choose securityIds to test (you can edit this list)
    security_ids = ["236", "1333", "4963", "10604"]  # 236=RELIANCE (example)

    # 3) LTP test on NSE_EQ bucket (uses same header set)
    test_ltp(key, cid, "NSE_EQ", security_ids[:1])  # start simple with 236

    # 4) Resolve contracts for each id (preferring NSE_EQ/EQUITY)
    df = fetch_master()
    resolved = {}
    for sid in security_ids:
        try:
            exseg, instr, exp = pick_contract(df, sid)
            print(f"🔧 {sid}: {exseg}/{instr}, expiry={exp}")
            resolved[sid] = (sid, exseg, instr, exp)
        except Exception as e:
            print(f"❌ resolve failed for {sid}: {e}")

    # 5) Daily OHLC + Intraday tests
    for sid, tup in resolved.items():
        test_daily_ohlc(key, cid, tup, days=10)
        time.sleep(1)
        test_intraday(key, cid, tup, interval=15, lookback_days=1)
        time.sleep(1)

    print("\n🚩 If any call shows 401/`ClientId is invalid` → fix DHAN_CLIENT_ID.")
    print("🚩 If `DH-906 Invalid Token` → regenerate DHAN_API_KEY (JWT) and update .env.")
    print("🚩 If `DH-905` → check payload printed above (segment/instrument/expiryCode).")
