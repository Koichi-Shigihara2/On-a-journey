"""
共通ティッカー設定
status: "holding"（保有）/ "watching"（監視）
"""

TICKERS = {
    # 保有銘柄
    "TSLA": {"status": "holding", "name": "Tesla Inc"},
    "PLTR": {"status": "holding", "name": "Palantir Technologies"},
    "SOFI": {"status": "holding", "name": "SoFi Technologies"},
    
    # 監視銘柄
    "CELH": {"status": "watching", "name": "Celsius Holdings"},
    "NVDA": {"status": "watching", "name": "NVIDIA Corporation"},
    "AMD":  {"status": "watching", "name": "Advanced Micro Devices"},
    "APP":  {"status": "watching", "name": "AppLovin Corporation"},
    "SOUN": {"status": "watching", "name": "SoundHound AI"},
    "RKLB": {"status": "watching", "name": "Rocket Lab USA"},
    "ONDS": {"status": "watching", "name": "Ondas Holdings"},
    "MSFT": {"status": "watching", "name": "Microsoft Corporation"},
    "AMZN": {"status": "watching", "name": "Amazon.com"},
    "FIG":  {"status": "watching", "name": "Simplify Asset Management"},
}


def get_holdings():
    """保有銘柄のみ取得"""
    return [t for t, v in TICKERS.items() if v["status"] == "holding"]


def get_watchlist():
    """監視銘柄のみ取得"""
    return [t for t, v in TICKERS.items() if v["status"] == "watching"]


def get_all():
    """全銘柄取得"""
    return list(TICKERS.keys())


def get_ticker_info(ticker: str) -> dict:
    """個別ティッカー情報取得"""
    return TICKERS.get(ticker, {"status": "unknown", "name": ticker})
