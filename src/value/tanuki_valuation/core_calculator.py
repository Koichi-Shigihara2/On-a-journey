import numpy as np
from typing import Dict, Any

class KoichiValuationCalculator:
    def __init__(self):
        self.wacc = 0.085
        self.k = 0.10
        self.high_growth_years = 3
        self.retention_rate = 0.60

    def calculate_pt(self, financials: Dict[str, Any]) -> Dict[str, Any]:
        fcf_avg = financials.get("fcf_5yr_avg", 0.0)
        diluted_shares = financials.get("diluted_shares", 0)
        roe_avg = financials.get("roe_10yr_avg", 0.0)
        current_price = financials.get("current_price", 0.0)
        fcf_list_raw = financials.get("fcf_list_raw", [])
        ticker = financials.get("eps_data", {}).get("ticker", "Unknown")

        if diluted_shares <= 100_000:  # 実用的な下限
            print(f"   [{ticker}] diluted_shares が不十分 ({diluted_shares:,.0f}) → スキップ")
            return {"error": "diluted_shares missing"}

        # 企業別高成長率（CAGR）
        high_growth_rate = 0.25
        if len(fcf_list_raw) >= 3:
            recent_fcfs = [f for f in fcf_list_raw[-5:] if f > 0]
            if len(recent_fcfs) >= 2:
                cagr = (recent_fcfs[-1] / recent_fcfs[0]) ** (1 / (len(recent_fcfs) - 1)) - 1
                high_growth_rate = max(0.15, min(0.50, cagr))

        print(f"   [{ticker}] 企業別高成長率（CAGR）: {high_growth_rate:.1%}")

        # FCFフロア
        if fcf_avg < 100_000:
            fcf_avg = max(100_000, abs(fcf_avg) * 0.1)

        # 2段階DCF + α自動計算
        high_growth_fcf = fcf_avg * (1 + high_growth_rate)
        pv_high = sum(high_growth_fcf * ((1 + high_growth_rate) ** t) / (1 + self.wacc) ** (t + 1) for t in range(self.high_growth_years))
        terminal_fcf = high_growth_fcf * ((1 + high_growth_rate) ** self.high_growth_years) * 1.03
        terminal_value = terminal_fcf / (self.wacc - 0.03)
        pv_terminal = terminal_value / (1 + self.wacc) ** self.high_growth_years
        v0 = pv_high + pv_terminal

        g_individual = max(0.0, roe_avg * self.retention_rate)
        alpha = max(0.0, (g_individual / self.wacc) * 0.7)

        print(f"   [{ticker}] ROE_10yr = {roe_avg:.1%} → α = {alpha:.3f}")

        intrinsic_value_pt = v0 * (1 + alpha)
        intrinsic_value_per_share = intrinsic_value_pt / diluted_shares

        return {
            "intrinsic_value_pt": float(intrinsic_value_pt),
            "intrinsic_value_per_share": float(intrinsic_value_per_share),
            "v0": float(v0),
            "alpha": float(alpha),
            "calculation_date": "2026-04-03",
            "formula": "Koichi式 v5.1 α自動計算版",
            "components": {**financials, "high_growth_rate_used": float(high_growth_rate)}
        }
