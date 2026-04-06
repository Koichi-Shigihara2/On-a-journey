import numpy as np
from typing import Dict, Any

class KoichiValuationCalculator:
    def __init__(self):
        self.wacc = 0.085
        self.high_growth_years = 3
        self.retention_rate = 0.60
        self.terminal_growth = 0.03

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

        # FCF現実的補正
        original_fcf = fcf_avg
        if fcf_avg <= 0 and latest_revenue > 0:
            fcf_floor = latest_revenue * 0.08
            fcf_avg = max(fcf_avg, fcf_floor)
            print(f"   [{ticker}] FCFが{original_fcf:,.0f}のため補正 → ${fcf_avg:,.0f} (売上高×8%)")

        # 2段階DCF（成長減衰カーブ適用）
        current_fcf = fcf_avg
        pv_high = 0.0
        for t in range(self.high_growth_years):
            current_fcf *= (1 + high_growth_rate)
            pv_high += current_fcf / (1 + self.wacc) ** (t + 1)

        terminal_fcf = current_fcf * (1 + self.terminal_growth)
        terminal_value = terminal_fcf / (self.wacc - self.terminal_growth)
        pv_terminal = terminal_value / (1 + self.wacc) ** self.high_growth_years
        v0 = pv_high + pv_terminal

        # α（成長期待プレミアム）
        g_individual = max(0.0, roe_avg * self.retention_rate)
        alpha = max(0.0, (g_individual / self.wacc) * 0.7)

        print(f"   [{ticker}] ROE_10yr = {roe_avg:.1%} → α = {alpha:.3f}")

        intrinsic_value_pt = v0 * (1 + alpha)
        intrinsic_value_per_share = intrinsic_value_pt / diluted_shares if diluted_shares > 0 else 0.0

        # ★★★ Phase 4：1〜3年後価値予測 ★★★
        future_values = {}
        current_value = intrinsic_value_per_share
        for year in range(1, 4):
            # 高成長期中はCAGR適用、以降はterminal成長
            if year <= self.high_growth_years:
                future_value = current_value * (1 + high_growth_rate)
            else:
                future_value = current_value * (1 + self.terminal_growth)
            future_values[f"{year}年後"] = round(future_value, 2)
            current_value = future_value

        print(f"   [{ticker}] 1〜3年後理論株価: {future_values}")

        return {
            "intrinsic_value_pt": float(intrinsic_value_pt),
            "intrinsic_value_per_share": float(intrinsic_value_per_share),
            "v0": float(v0),
            "alpha": float(alpha),
            "future_values": future_values,
            "calculation_date": "2026-04-06",
            "formula": "Koichi式 v5.1 Phase 4（将来価値予測版）",
            "components": {
                **financials,
                "high_growth_rate_used": float(high_growth_rate),
                "pv_high": float(pv_high),
                "pv_terminal": float(pv_terminal),
                "roe_used": float(roe_avg),
                "fcf_floor_applied": float(fcf_avg - original_fcf) if 'original_fcf' in locals() else 0
            }
        }