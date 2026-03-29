# src/value/tanuki_valuation/data_fetcher.py
import os
import json
import numpy as np
from typing import Dict, Any
from edgartools import Company
from datetime import datetime

class TanukiDataFetcher:
    """edgartoolsを使ってSEC EDGARから直接キャッシュフロー・財務データを取得"""
    
    def __init__(self):
        self.alpha_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    
    def get_financials(self, ticker: str) -> Dict[str, Any]:
        print(f"🔍 edgartools + SEC EDGARから {ticker} の財務データを取得中...")
        
        try:
            company = Company(ticker)
            # 最新5年分のキャッシュフロー計算書を取得
            cf = company.get_financials().cash_flow_statement(period="annual", limit=5)
            
            # Free Cash Flow（FCF）抽出
            fcf_list = cf.get("Free Cash Flow", pd.Series([0])).tolist()
            
            # ROE（簡易）
            income = company.get_financials().income_statement(period="annual", limit=10)
            roe_values = income.get("Return on Equity", pd.Series([0])).tolist()
            
            # 最新株価
            current_price = self._get_current_price(ticker)
            
            return {
                "fcf_5yr_avg": self._normalize_fcf(fcf_list),
                "roe_10yr_avg": float(np.mean(roe_values)) if roe_values else 0.0,
                "current_price": current_price,
                "fcf_list_raw": fcf_list,
                "eps_data": {"ticker": ticker}  # 将来的に拡張
            }
        except Exception as e:
            print(f"⚠️ {ticker} 取得失敗: {e} → フォールバック値で続行")
            return {
                "fcf_5yr_avg": 0.0,
                "roe_10yr_avg": 0.0,
                "current_price": self._get_current_price(ticker),
                "fcf_list_raw": [],
                "eps_data": {"ticker": ticker}
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
