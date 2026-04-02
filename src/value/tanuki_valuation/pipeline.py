import requests
import pandas as pd
import numpy as np
from typing import Dict, Any, List
import os
from datetime import datetime

class TanukiDataFetcher:
    def __init__(self):
        self.av_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        if not self.av_key:
            print("❌ ALPHA_VANTAGE_API_KEY が環境変数に設定されていません！")
            raise ValueError("ALPHA_VANTAGE_API_KEY missing")

    def get_financials(self, ticker: str) -> Dict[str, Any]:
        print(f"   [DEBUG {ticker}] Alpha Vantage API 取得開始")

        # 1. Cash Flow (FCF計算用)
        cf_url = f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={ticker}&apikey={self.av_key}"
        cf_data = self._fetch_av(cf_url, ticker)

        # 2. Overview (diluted shares, ROEなど)
        overview_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={self.av_key}"
        overview = self._fetch_av(overview_url, ticker)

        # 3. Global Quote (現在株価)
        quote_url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={self.av_key}"
        quote = self._fetch_av(quote_url, ticker)
        current_price = float(quote.get("Global Quote", {}).get("05. price", 0)) if quote else 0.0

        # FCF計算
        fcf_list = []
        for cf in cf_data.get("annualReports", [])[:5]:  # 直近5年
            ocf = float(cf.get("operatingCashflow", 0))
            capex = abs(float(cf.get("capitalExpenditures", 0)))
            fcf_list.append(ocf - capex)

        fcf_5yr_avg = np.mean(fcf_list) if fcf_list else 0.0

        # diluted shares & ROE
        diluted_shares = float(overview.get("DilutedEPS", 0)) * 1000000 if overview else 0  # 概算
        roe_10yr_avg = float(overview.get("ReturnOnEquityTTM", 0)) if overview else 0.0

        print(f"   [DEBUG {ticker}] FCF 5yr Avg = {fcf_5yr_avg:,.0f} | diluted_shares = {diluted_shares:,.0f}")

        return {
            "fcf_5yr_avg": float(fcf_5yr_avg),
            "diluted_shares": float(diluted_shares),
            "roe_10yr_avg": float(roe_10yr_avg),
            "current_price": float(current_price),
            "fcf_list_raw": fcf_list,
            "eps_data": {"ticker": ticker},
            "fcf_calc_method": "Alpha Vantage"
        }

    def _fetch_av(self, url: str, ticker: str) -> Dict:
        try:
            resp = requests.get(url, timeout=15)
            print(f"   [AV STATUS {ticker}] {resp.status_code}")
            if resp.status_code != 200:
                print(f"   [AV ERROR BODY {ticker}] {resp.text[:300]}")
            data = resp.json()
            if "Error Message" in data or "Note" in data:
                print(f"   [AV WARNING {ticker}] {data.get('Error Message') or data.get('Note')}")
            return data
        except Exception as e:
            print(f"   [AV EXCEPTION {ticker}] {e}")
            return {}