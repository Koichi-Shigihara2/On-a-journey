import os
import requests

def get_market_context(ticker):
    av_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    fred_key = os.environ.get("FRED_API_KEY")
    
    # 株価取得 (Alpha Vantage)
    price_url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={av_key}"
    price_data = requests.get(price_url).json()
    price = float(price_data.get("Global Quote", {}).get("05. price", 0))
    
    # 米10年債利回り取得 (FRED)
    fred_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key={fred_key}&file_type=json&sort_order=desc&limit=1"
    fred_data = requests.get(fred_url).json()
    yield_10y = float(fred_data['observations'][0]['value'])
    
    return {"price": price, "yield_10y": yield_10y}
