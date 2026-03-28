# src/value/tanuki_valuation/core_calculator.py
import numpy as np
from typing import Dict, Any
import datetime

class KoichiValuationCalculator:
    """Koichi式 v5.1 計算コア（TANUKI VALUATION用・完全に独立したクラス）"""
    
    def __init__(self, params: Dict[str, float] = None):
        self.params = params or {}
        self.wacc = self.params.get('default_wacc', 0.08)
        self.k = self.params.get('k_factor', 0.10)
        self.target_irr = self.params.get('target_irr', 0.20)
        self.cf_period_years = self.params.get('cf_period_years', 3)
    
    def normalize_fcf_5yr(self, fcf_list: list[float]) -> float:
        """過去5年FCFFをノーマライズ（手戻り6対応）"""
        if len(fcf_list) < 5:
            return np.mean(fcf_list) if fcf_list else 0.0
        mean = np.mean(fcf_list)
        std = np.std(fcf_list)
        clipped = np.clip(fcf_list, mean - 2*std, mean + 2*std)
        return float(np.mean(clipped))
    
    def calculate_v_fixed(self, fcf_5yr_avg: float, years: int = 10) -> float:
        """V_固定的：成長率0%で10年DCF + ターミナル"""
        if fcf_5yr_avg <= 0:
            return 0.0
        discounted = sum(fcf_5yr_avg / (1 + self.wacc)**t for t in range(1, years + 1))
        terminal = (fcf_5yr_avg / self.wacc) / (1 + self.wacc)**years
        return discounted + terminal
    
    def calculate_certainty_score(self, contract_ratio: float, retention_rate: float, backlog_months: float) -> float:
        """確度スコア（0〜1.0）"""
        return 0.4 * contract_ratio + 0.3 * retention_rate + 0.3 * (backlog_months / 12)
    
    def calculate_pt(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Koichi式v5.1 最終計算（exact & approx両方出力）"""
        v_fixed = self.calculate_v_fixed(data.get("fcf_5yr_avg", 0))
        v0 = v_fixed + data.get("v_certainty_growth", 0)
        
        alpha = max(0.0, (data.get("g_individual_excess", 0) / self.wacc) * (1 - data.get("certainty_score", 0.5)))
        beta = max(0.0, data.get("g_sector_excess", 0) / self.wacc)
        
        m_total = np.clip(data.get("m_total_wave", 0.0), -3.0, 3.0)
        
        pt_exact = v0 * (1 + alpha + beta) + self.k * m_total * v0 * (alpha + beta)
        pt_approx = v0 * (1 + alpha + beta) * (1 + self.k * m_total)
        
        current_price = data.get("current_price", 1.0)
        implied_irr = (pt_exact / current_price) ** (1/3) - 1 if current_price > 0 else 0.0
        
        return {
            "intrinsic_value_pt": round(pt_exact, 2),
            "approx_value": round(pt_approx, 2),
            "v0": round(v0, 2),
            "alpha": round(alpha, 4),
            "beta": round(beta, 4),
            "m_total": round(m_total, 2),
            "implied_irr": round(implied_irr * 100, 1),
            "target_irr_deviation": round((implied_irr - self.target_irr) * 100, 1),
            "calculation_date": datetime.date.today().isoformat(),
            "formula": "Koichi式 v5.1 exact",
            "components": data
        }
