# src/value/tanuki_valuation/data_fetcher.py
import os
import numpy as np
from typing import Dict, Any
import requests

# 既存のextract_key_facts.pyを使用
from ..adjusted_eps_analyzer.extract_key_facts import extract_quarterly_facts

class TanukiDataFetcher:
    """FCF計算をより正確に（OCF優先 + CapEx直接取得）"""
    
    def __init__(self):
        self.alpha_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    
    def get_financials(self, ticker: str) -> Dict[str, Any]:
        print(f"🔍 SEC EDGARから {ticker} の財務データを取得中...")
        
        try:
            quarterly_data = extract_quarterly_facts(ticker, years=5)
        except Exception as e:
            print(f"⚠️ {ticker} データ取得失敗: {e}")
            quarterly_data = []

        # === FCF計算（OCF優先の正確版）===
        fcf_list = []
        method_used = "未計算"
        
        for q in quarterly_data:
            # 優先1: OCF（Operating Cash Flow） - CapEx
            ocf_tag = 'us-gaap:NetCashProvidedByUsedInOperatingActivities'
            ocf = q.get(ocf_tag, {}).get('value', 0)
            
            capex_tag = 'us-gaap:PaymentsForPropertyPlantAndEquipment'
            capex = q.get(capex_tag, {}).get('value', 0)
            
            if ocf != 0:  # OCFが取れたら最優先
                fcf = ocf - abs(capex)
                method_used = "OCF - CapEx（最正確）"
            else:
                # フォールバック: 従来の簡易計算
                net_income = q.get('net_income', {}).get('value', 0)
                sbc = q.get('us-gaap:ShareBasedCompensation', {}).get('value', 0) or 0
                amort = q.get('us-gaap:AmortizationOfIntangibleAssets', {}).get('value', 0) or 0
                fcf = net_income + sbc + amort - abs(capex)
                method_used = "簡易計算 (フォールバック)"
            
            fcf_list.append(fcf)

        fcf_5yr_avg = self._normalize_fcf(fcf_list[-5:]) if fcf_list else 0.0

        print(f"DEBUG {ticker}: FCF_5yr_avg = {fcf_5yr_avg:,.0f} | 方法: {method_used} | データ件数: {len(fcf_list)}")

        # ROE（簡易）
        roe_values = [q.get('adjusted_eps', 0) * 100 for q in quarterly_data if 'adjusted_eps' in q]

        return {
            "fcf_5yr_avg": fcf_5yr_avg,
            "roe_10yr_avg": float(np.mean(roe_values)) if roe_values else 0.0,
            "current_price": self._get_current_price(ticker),
            "fcf_list_raw": fcf_list,
            "eps_data": {"ticker": ticker, "quarters": quarterly_data},
            "diluted_shares": quarterly_data[0].get('diluted_shares', {}).get('value', 0) if quarterly_data else 0,
            "fcf_calc_method": method_used
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
        try:
            data = requests.get(url, timeout=10).json()
            price = data.get("Global Quote", {}).get("05. price", 0)
            return float(price) if price else 0.0
        except:
            return 0.0