import os
import requests
from dotenv import load_dotenv

# Load API key and model
load_dotenv()
PPLX_API_KEY = os.getenv("PPLX_API_KEY")
PPLX_MODEL = os.getenv("PPLX_MODEL", "sonar-pro")
PPLX_BASE_URL = os.getenv("PPLX_BASE_URL", "https://api.perplexity.ai")

# Define prompt
prompt = "Summarize the NIFTY 50 trend this week in one line."

# Prepare request
url = f"{PPLX_BASE_URL}/chat/completions"
headers = {
    "Authorization": f"Bearer {PPLX_API_KEY}",
    "Content-Type": "application/json"
}
payload = {
    "model": PPLX_MODEL,
    "messages": [{"role": "user", "content": prompt}],
}

# Send request
r = requests.post(url, headers=headers, json=payload)
print("Status:", r.status_code)
print("Response:", r.text[:600])
