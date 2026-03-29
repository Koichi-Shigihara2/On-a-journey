# src/value/tanuki_valuation/data_fetcher.py
import os
import json
import numpy as np
from typing import Dict, Any
import requests

class TanukiDataFetcher:
    def __init__(self):
        self.fmp_key = os.getenv("FMP_API_KEY")
        self.fred_key = os.getenv("FRED_API_KEY")
        self.alpha_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    def get_financials(self, ticker: str) -> Dict[str, Any]:
        base = "https://financialmodelingprep.com/api/v3"
        
        # キャッシュフロー
        cf_url = f"{base}/cash-flow-statement/{ticker}?period=annual&limit=20&apikey={self.fmp_key}"
        cf_resp = requests.get(cf_url)
        print(f"DEBUG {ticker} CF status: {cf_resp.status_code} | length: {len(cf_resp.text)}")
        cf_data = cf_resp.json()
        fcf_list = [item.get("freeCashFlow", 0) for item in cf_data if isinstance(item, dict)]
        
        # キー指標
        metrics_url = f"{base}/key-metrics/{ticker}?limit=20&apikey={self.fmp_key}"
        metrics_resp = requests.get(metrics_url)
        print(f"DEBUG {ticker} Metrics status: {metrics_resp.status_code}")
        metrics = metrics_resp.json()
        roe_values = [m.get("returnOnEquity", m.get("roe", 0)) for m in metrics if isinstance(m, dict)]  # 両キー対応
        
        # EPSアナライザー連携（緩やか連携）
        eps_path = f"docs/value-monitor/adjusted_eps_analyzer/data/{ticker}/annual.json"
        eps_data = {}
        if os.path.exists(eps_path):
            with open(eps_path, "r", encoding="utf-8") as f:
                eps_data = json.load(f)
        
        fcf_5yr = self._normalize_fcf(fcf_list[-5:]) if fcf_list else 0.0
        
        return {
            "fcf_5yr_avg": fcf_5yr,
            "roe_10yr_avg": float(np.mean(roe_values)) if roe_values else 0.0,
            "current_price": self._get_current_price(ticker),
            "fcf_list_raw": fcf_list,
            "eps_data": eps_data
        }

    def _normalize_fcf(self, fcf_list: list) -> float:
        if not fcf_list:
            return 0.0
        mean = np.mean(fcf_list)
        std = np.std(fcf_list) if len(fcf_list) > 1 else 0
        clipped = np.clip(fcf_list, mean - 2*std, mean + 2*std)
        return float(np.mean(clipped))

    def _get_current_price(self, ticker: str) -> float:
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={self.alpha_key}"
        data = requests.get(url).json()
        price = data.get("Global Quote", {}).get("05. price", 0)
        return float(price) if price else 0.0
