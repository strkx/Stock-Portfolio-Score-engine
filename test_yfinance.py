import yfinance as yf

for s in ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"]:
    info = yf.Ticker(s).info
    print(f"\n{s}")
    print("Sector:", info.get("sector"))
    print("Industry:", info.get("industry"))
    print("Market Cap:", info.get("marketCap"))
