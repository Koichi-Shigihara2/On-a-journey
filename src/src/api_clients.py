import os
import requests

def get_current_price(ticker):
    # Alpha Vantageを使用して最新株価と分割修正係数を取得
    key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={key}"
    r = requests.get(url).json()
    return float(r.get("Global Quote", {}).get("05. price", 0))

def get_press_release(ticker):
    # FMPを使用して最新の決算プレスリリースを取得（調整理由の特定用）
    key = os.environ.get("FMP_API_KEY")
    url = f"https://financialmodelingprep.com/api/v3/press-releases/{ticker}?limit=1&apikey={key}"
    return requests.get(url).json()
