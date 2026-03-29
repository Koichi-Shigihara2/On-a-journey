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
        base = f"https://financialmodelingprep.com/api/v3"
        
        # キャッシュフロー
        cf_url = f"{base}/cash-flow-statement/{ticker}?period=annual&limit=10&apikey={self.fmp_key}"
        cf_data = requests.get(cf_url).json()
        fcf_list = [item.get("freeCashFlow", 0) for item in cf_data if isinstance(item, dict)]
        
        # キー指標（FMPの実際のキー名に修正）
        metrics_url = f"{base}/key-metrics/{ticker}?limit=10&apikey={self.fmp_key}"
        metrics = requests.get(metrics_url).json()
        roe_values = [m.get("returnOnEquity", 0) for m in metrics if isinstance(m, dict)]  # ← ここを修正
        
        # EPSアナライザー連携（緩やか連携）
        eps_path = f"docs/value-monitor/adjusted_eps_analyzer/data/{ticker}/annual.json"
        eps_data = {}
        if os.path.exists(eps_path):
            with open(eps_path, "r", encoding="utf-8") as f:
                eps_data = json.load(f)
        
        print(f"DEBUG {ticker}: FCF count={len(fcf_list)}, ROE values={roe_values[:3]}...")  # デバッグ出力
        
        return {
            "fcf_5yr_avg": self._normalize_fcf(fcf_list[-5:]),
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
        return float(data.get("Global Quote", {}).get("05. price", 0) or 0)
