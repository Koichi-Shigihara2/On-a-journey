import numpy as np
from typing import Dict, Any

class KoichiValuationCalculator:
    def __init__(self):
        self.wacc = 0.085
        self.high_growth_years = 3
        self.retention_rate = 0.60

    def calculate_pt(self, financials: Dict[str, Any]) -> Dict[str, Any]:
        fcf_avg = financials.get("fcf_5yr_avg", 0.0)
        diluted_shares = financials.get("diluted_shares", 0)
        roe_avg = financials.get("roe_10yr_avg", 0.0)
        latest_revenue = financials.get("latest_revenue", 0.0)
        fcf_list_raw = financials.get("fcf_list_raw", [])
        ticker = financials.get("eps_data", {}).get("ticker", "Unknown")

        if diluted_shares <= 100_000:
            return {"error": "diluted_shares missing"}

        # 企業別高成長率
        high_growth_rate = 0.25
        if len(fcf_list_raw) >= 3:
            recent_fcfs = [f for f in fcf_list_raw[-5:] if f > 0]
            if len(recent_fcfs) >= 2:
                cagr = (recent_fcfs[-1] / recent_fcfs[0]) ** (1 / (len(recent_fcfs) - 1)) - 1
                high_growth_rate = max(0.15, min(0.50, cagr))

        print(f"   [{ticker}] 企業別高成長率（CAGR）: {high_growth_rate:.1%}")

        # ★★★ FCF現実的補正（Phase 1） ★★★
        original_fcf = fcf_avg
        if fcf_avg <= 0 and latest_revenue > 0:
            fcf_floor = - (latest_revenue * 0.08)   # 売上高の8%を下限（5〜10%の中央値）
            fcf_avg = max(fcf_avg, fcf_floor)
            print(f"   [{ticker}] FCFが{original_fcf:,.0f}のため補正 → ${fcf_avg:,.0f} (売上高×8%)")

        # 2段階DCF
        high_growth_fcf = fcf_avg * (1 + high_growth_rate)
        pv_high = sum(high_growth_fcf * ((1 + high_growth_rate) ** t) / (1 + self.wacc) ** (t + 1) 
                     for t in range(self.high_growth_years))
        terminal_fcf = high_growth_fcf * ((1 + high_growth_rate) ** self.high_growth_years) * 1.03
        terminal_value = terminal_fcf / (self.wacc - 0.03)
        pv_terminal = terminal_value / (1 + self.wacc) ** self.high_growth_years
        v0 = pv_high + pv_terminal

        # α（成長期待プレミアム） - 現在はROEベース（Phase 2で拡張）
        g_individual = max(0.0, roe_avg * self.retention_rate)
        alpha = max(0.0, (g_individual / self.wacc) * 0.7)

        print(f"   [{ticker}] ROE_10yr = {roe_avg:.1%} → α = {alpha:.3f}")

        intrinsic_value_pt = v0 * (1 + alpha)
        intrinsic_value_per_share = intrinsic_value_pt / diluted_shares

        print(f"   [{ticker}] 理論株価（本質的価値） = ${intrinsic_value_per_share:.2f}")

        return {
            "intrinsic_value_pt": float(intrinsic_value_pt),
            "intrinsic_value_per_share": float(intrinsic_value_per_share),
            "v0": float(v0),
            "alpha": float(alpha),
            "calculation_date": "2026-04-03",
            "formula": "Koichi式 v5.1 Phase 1（FCF補正版）",
            "components": {
                **financials,
                "high_growth_rate_used": float(high_growth_rate),
                "pv_high": float(pv_high),
                "pv_terminal": float(pv_terminal),
                "roe_used": float(roe_avg),
                "fcf_floor_applied": float(fcf_avg - original_fcf) if 'original_fcf' in locals() else 0
            }
        }
