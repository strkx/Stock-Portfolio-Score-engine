import requests, os
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("DHAN_API_KEY")
cid   = os.getenv("DHAN_CLIENT_ID")

url = "https://api.dhan.co/v2/marketfeed/ltp"

headers = {
    "access-token": token,
    "client-id": cid,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

payload = {
    "NSE_EQ": [2885]   # 👈 Correct new format
}

r = requests.post(url, headers=headers, json=payload)

print("Status:", r.status_code)
print(r.text[:500])
