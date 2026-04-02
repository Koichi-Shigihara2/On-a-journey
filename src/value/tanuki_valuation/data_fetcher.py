import requests
import pandas as pd
import numpy as np
from typing import Dict, Any, List
import json
import os
from datetime import datetime

class TanukiDataFetcher:
    def __init__(self):
        self.fmp_key = os.getenv("FMP_API_KEY")
        if not self.fmp_key:
            print("❌ FMP_API_KEY が環境変数にありません！")
            raise ValueError("FMP_API_KEY missing")

    def get_financials(self, ticker: str) -> Dict[str, Any]:
        print(f"   [DEBUG {ticker}] FMP API 取得開始")

        cf_url = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}?period=quarter&apikey={self.fmp_key}"
        cf_data = self._fetch_fmp(cf_url)

        key_url = f"https://financialmodelingprep.com/api/v3/key-metrics/{ticker}?period=quarter&apikey={self.fmp_key}"
        key_data = self._fetch_fmp(key_url)

        quote_url = f"https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={self.fmp_key}"
        quote = self._fetch_fmp(quote_url)
        current_price = quote[0]["price"] if quote else 0.0

        # FCF計算
        fcf_list = []
        for cf in cf_data[:20]:
            ocf = cf.get("operatingCashFlow", 0)
            capex = abs(cf.get("capitalExpenditures", 0))
            fcf_list.append(ocf - capex)

        fcf_5yr_avg = np.mean(fcf_list) if fcf_list else 0.0
        diluted_shares = key_data[0].get("weightedAverageShsOutDil", 0) if key_data else 0
        roe_10yr_avg = np.mean([k.get("roe", 0) for k in key_data[:40] if k.get("roe")]) if key_data else 0.0

        print(f"   [DEBUG {ticker}] FCF 5yr Avg = {fcf_5yr_avg:,.0f} | diluted_shares = {diluted_shares:,.0f}")

        return {
            "fcf_5yr_avg": float(fcf_5yr_avg),
            "diluted_shares": float(diluted_shares),
            "roe_10yr_avg": float(roe_10yr_avg),
            "current_price": float(current_price),
            "fcf_list_raw": fcf_list,
            "eps_data": {"ticker": ticker},
            "fcf_calc_method": "FMP"
        }

    def _fetch_fmp(self, url: str) -> List[Dict]:
        try:
            resp = requests.get(url, timeout=15)
            print(f"   [FMP STATUS {ticker}] {resp.status_code}")
            if resp.status_code != 200:
                print(f"   [FMP ERROR BODY] {resp.text[:300]}")   # ← エラー本文を表示
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"   [FMP EXCEPTION] {e}")
            return []
