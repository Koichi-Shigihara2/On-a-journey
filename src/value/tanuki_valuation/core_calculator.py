import numpy as np
from typing import Dict, Any

class KoichiValuationCalculator:
    def __init__(self):
        self.wacc_default = 0.085  # 8.5%（中長期成長株向け）
        self.k = 0.10              # モメンタム係数（0.08〜0.15推奨）

    def calculate_pt(self, financials: Dict[str, Any]) -> Dict[str, Any]:
        fcf_avg = financials.get("fcf_5yr_avg", 0.0)
        diluted_shares = financials.get("diluted_shares", 0)
        roe_avg = financials.get("roe_10yr_avg", 0.0)
        current_price = financials.get("current_price", 0.0)

        if fcf_avg <= 0 or diluted_shares <= 0:
            return {"error": "FCF or shares data missing"}

        # V_固定的（10年DCF + 終値0成長）
        wacc = self.wacc_default
        v_fixed = sum(fcf_avg / (1 + wacc) ** t for t in range(1, 11))
        terminal = fcf_avg / wacc / (1 + wacc) ** 10
        v0 = v_fixed + terminal

        # α個別・βセクター（簡易版：ROEベース成長期待）
        g_individual = max(0.0, roe_avg * 0.6)
        alpha = max(0.0, (g_individual / wacc) * 0.7)
        beta = 0.0

        # モメンタム（現在0）
        m_total = 0.0

        # 完全形
        intrinsic_value_pt = v0 * (1 + alpha + beta) + self.k * m_total * v0 * (alpha + beta)

        # 1株あたり
        intrinsic_value_per_share = intrinsic_value_pt / diluted_shares if diluted_shares > 0 else 0.0

        # 簡略近似形
        approx_value = intrinsic_value_pt * (1 + self.k * m_total)

        return {
            "intrinsic_value_pt": intrinsic_value_pt,
            "intrinsic_value_per_share": intrinsic_value_per_share,
            "approx_value": approx_value,
            "v0": v0,
            "alpha": alpha,
            "beta": beta,
            "m_total": m_total,
            "implied_irr": (intrinsic_value_per_share / current_price - 1) * 100 if current_price > 0 else 0,
            "calculation_date": "2026-03-30",
            "formula": "Koichi式 v5.1 exact",
            "components": financials
        }
