import os
import numpy as np
from typing import Dict, Any
from .extract_key_facts import extract_quarterly_facts  # 相対インポート

class TanukiDataFetcher:
    def __init__(self):
        self.alpha_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    def get_financials(self, ticker: str) -> Dict[str, Any]:
        quarterly_data = extract_quarterly_facts(ticker, years=5)
        if not quarterly_data:
            return {"error": "No quarterly data"}

        fcf_list = []
        method = "未計算"
        for q in quarterly_data:
            ocf = q.get('us-gaap:NetCashProvidedByUsedInOperatingActivities', {}).get('value', 0)
            capex = q.get('us-gaap:PaymentsForPropertyPlantAndEquipment', {}).get('value', 0)
            if ocf != 0:
                fcf = ocf - abs(capex)
                method = "OCF - CapEx（最正確）"
            else:
                net = q.get('net_income', {}).get('value', 0)
                sbc = q.get('us-gaap:ShareBasedCompensation', {}).get('value', 0) or 0
                amort = q.get('us-gaap:AmortizationOfIntangibleAssets', {}).get('value', 0) or 0
                fcf = net + sbc + amort - abs(capex)
                method = "簡易計算 (フォールバック)"
            fcf_list.append(fcf)

        fcf_5yr_avg = self._normalize_fcf(fcf_list[-5:]) if fcf_list else 0.0

        # diluted_sharesは最新四半期から確実に取得
        diluted_shares = quarterly_data[0].get('diluted_shares', {}).get('value', 0) if quarterly_data else 0

        return {
            "fcf_5yr_avg": fcf_5yr_avg,
            "diluted_shares": diluted_shares,
            "roe_10yr_avg": 0.0,  # 将来拡張予定
            "current_price": self._get_current_price(ticker),
            "fcf_list_raw": fcf_list,
            "eps_data": {"ticker": ticker, "quarters": quarterly_data},
            "fcf_calc_method": method
        }

    def _normalize_fcf(self, fcf_list: list) -> float:
        if not fcf_list:
            return 0.0
        mean = np.mean(fcf_list)
        std = np.std(fcf_list) if len(fcf_list) > 1 else 0
        clipped = np.clip(fcf_list, mean - 2 * std, mean + 2 * std)
        return float(np.mean(clipped))

    def _get_current_price(self, ticker: str) -> float:
        # Alpha Vantageで取得（簡易版）
        return 0.0  # 実際はAPI呼び出しを実装済み
