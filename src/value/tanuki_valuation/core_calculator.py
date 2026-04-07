"""
TANUKI VALUATION - Core Calculator v5.1
Koichi式株価評価モデル

P_t = V_0 × (1 + α)
V_0 = 2段階DCF（高成長期3年 + ターミナル）
α = max(0, (ROE_10yr × retention_rate / WACC) × 0.7)

パラメータ:
- WACC: 8.5%（成長期待を含まない固定値）
- terminal_growth: 3%
- retention_rate: 60%
- high_growth_range: 15%〜50%
- FCF floor: revenue × 8%（FCFがマイナスの場合）
"""

import numpy as np
from typing import Dict, Any, List
from datetime import datetime


class KoichiValuationCalculator:
    """Koichi式 v5.1 バリュエーション計算エンジン"""

    def __init__(self):
        # 固定パラメータ
        self.wacc = 0.085           # 割引率（成長期待を含まない）
        self.high_growth_years = 3   # 高成長期間（年）
        self.retention_rate = 0.60   # 内部留保率
        self.terminal_growth = 0.03  # 永続成長率

    def calculate_pt(self, financials: Dict[str, Any]) -> Dict[str, Any]:
        """
        メイン計算関数
        
        Args:
            financials: {
                "fcf_5yr_avg": float,
                "diluted_shares": int,
                "roe_10yr_avg": float,
                "current_price": float,
                "fcf_list_raw": list,
                "latest_revenue": float,
                "eps_data": {"ticker": str}
            }
        
        Returns:
            完全なバリュエーション結果（calculation_stepsを含む）
        """
        # データ抽出
        fcf_avg = financials.get("fcf_5yr_avg", 0.0)
        diluted_shares = financials.get("diluted_shares", 0)
        roe_avg = financials.get("roe_10yr_avg", 0.0)
        latest_revenue = financials.get("latest_revenue", 0.0)
        fcf_list_raw = financials.get("fcf_list_raw", [])
        current_price = financials.get("current_price", 0.0)
        ticker = financials.get("eps_data", {}).get("ticker", "Unknown")

        # バリデーション
        if diluted_shares <= 100_000:
            return {"error": "diluted_shares missing or invalid"}

        # ========================================
        # STEP 1: FCF 5年平均算出
        # ========================================
        step1_description = "過去5年のFCF（営業CF - 設備投資）を平均化"
        fcf_calculation = {
            "input": fcf_list_raw,
            "sum": sum(fcf_list_raw) if fcf_list_raw else 0,
            "count": len(fcf_list_raw),
            "result": fcf_avg
        }

        # ========================================
        # STEP 2: 企業別高成長率（CAGR）算出
        # ========================================
        step2_description = "直近5年FCFのCAGRを算出し、15%〜50%にクリップ"
        high_growth_rate = 0.25  # デフォルト
        cagr_calculation = {"method": "default", "result": high_growth_rate}

        if len(fcf_list_raw) >= 3:
            recent_fcfs = [f for f in fcf_list_raw[-5:] if f > 0]
            if len(recent_fcfs) >= 2:
                raw_cagr = (recent_fcfs[-1] / recent_fcfs[0]) ** (1 / (len(recent_fcfs) - 1)) - 1
                high_growth_rate = max(0.15, min(0.50, raw_cagr))
                cagr_calculation = {
                    "method": "cagr",
                    "start_value": recent_fcfs[0],
                    "end_value": recent_fcfs[-1],
                    "periods": len(recent_fcfs) - 1,
                    "raw_cagr": raw_cagr,
                    "clipped_result": high_growth_rate
                }

        print(f"   [{ticker}] 企業別高成長率（CAGR）: {high_growth_rate:.1%}")

        # ========================================
        # FCF現実的補正（マイナスFCF対応）
        # ========================================
        original_fcf = fcf_avg
        fcf_floor_applied = 0.0
        fcf_correction = {"applied": False}

        if fcf_avg <= 0 and latest_revenue > 0:
            fcf_floor = latest_revenue * 0.08
            fcf_avg = max(fcf_avg, fcf_floor)
            fcf_floor_applied = fcf_avg - original_fcf
            fcf_correction = {
                "applied": True,
                "original_fcf": original_fcf,
                "revenue": latest_revenue,
                "floor_rate": 0.08,
                "floor_value": fcf_floor,
                "adjusted_fcf": fcf_avg
            }
            print(f"   [{ticker}] FCFが{original_fcf:,.0f}のため補正 → ${fcf_avg:,.0f} (売上高×8%)")

        # ========================================
        # STEP 3: 2段階DCF計算
        # ========================================
        step3_description = "高成長期3年 + ターミナル価値のDCF"

        # 高成長期 PV計算
        current_fcf = fcf_avg
        pv_high = 0.0
        high_growth_detail = []

        for t in range(self.high_growth_years):
            current_fcf *= (1 + high_growth_rate)
            discount_factor = (1 + self.wacc) ** (t + 1)
            pv_year = current_fcf / discount_factor
            pv_high += pv_year
            high_growth_detail.append({
                "year": t + 1,
                "fcf": current_fcf,
                "discount_factor": discount_factor,
                "pv": pv_year
            })

        # ターミナル価値計算
        terminal_fcf = current_fcf * (1 + self.terminal_growth)
        terminal_value = terminal_fcf / (self.wacc - self.terminal_growth)
        pv_terminal = terminal_value / (1 + self.wacc) ** self.high_growth_years

        dcf_calculation = {
            "wacc": self.wacc,
            "high_growth_years": self.high_growth_years,
            "terminal_growth": self.terminal_growth,
            "high_growth_detail": high_growth_detail,
            "pv_high": pv_high,
            "terminal_fcf": terminal_fcf,
            "terminal_value": terminal_value,
            "pv_terminal": pv_terminal
        }

        # V_0（本質的価値ベース）
        v0 = pv_high + pv_terminal

        # ========================================
        # STEP 4: α（成長期待プレミアム）算出
        # ========================================
        step4_description = "α = max(0, (g_individual / WACC) × 0.7)"
        g_individual = max(0.0, roe_avg * self.retention_rate)
        alpha_raw = (g_individual / self.wacc) * 0.7
        alpha = max(0.0, alpha_raw)

        alpha_calculation = {
            "roe_10yr_avg": roe_avg,
            "retention_rate": self.retention_rate,
            "g_individual": g_individual,
            "wacc": self.wacc,
            "alpha_raw": alpha_raw,
            "alpha_clipped": alpha
        }

        print(f"   [{ticker}] ROE_10yr = {roe_avg:.1%} → α = {alpha:.3f}")

        # ========================================
        # STEP 5: 本質的価値（P_t）算出
        # ========================================
        step5_description = "P_t = V_0 × (1 + α)"
        intrinsic_value_pt = v0 * (1 + alpha)
        intrinsic_value_per_share = intrinsic_value_pt / diluted_shares if diluted_shares > 0 else 0.0

        pt_calculation = {
            "v0": v0,
            "alpha": alpha,
            "formula": "V_0 × (1 + α)",
            "intrinsic_value_pt": intrinsic_value_pt,
            "diluted_shares": diluted_shares,
            "intrinsic_value_per_share": intrinsic_value_per_share
        }

        # ========================================
        # 1〜3年後価値予測
        # ========================================
        future_values = {}
        current_value = intrinsic_value_per_share
        future_detail = []

        for year in range(1, 4):
            if year <= self.high_growth_years:
                growth_rate = high_growth_rate
            else:
                growth_rate = self.terminal_growth
            
            future_value = current_value * (1 + growth_rate)
            future_values[f"{year}年後"] = round(future_value, 2)
            future_detail.append({
                "year": year,
                "growth_rate": growth_rate,
                "value": round(future_value, 2)
            })
            current_value = future_value

        print(f"   [{ticker}] 1〜3年後理論株価: {future_values}")

        # 乖離率計算
        upside_percent = ((intrinsic_value_per_share / current_price) - 1) * 100 if current_price > 0 else 0

        # ========================================
        # 結果返却
        # ========================================
        return {
            "intrinsic_value_pt": float(intrinsic_value_pt),
            "intrinsic_value_per_share": float(intrinsic_value_per_share),
            "v0": float(v0),
            "alpha": float(alpha),
            "future_values": future_values,
            "upside_percent": round(upside_percent, 1),
            "calculation_date": datetime.now().strftime("%Y-%m-%d"),
            "formula": "Koichi式 v5.1 Phase 4（将来価値予測版）",
            
            # 計算ステップ詳細（フロントエンド表示用）
            "calculation_steps": {
                "step1": {
                    "title": "FCF 5年平均算出",
                    "description": step1_description,
                    "formula": "Σ(FCF_t) / n",
                    "inputs": {
                        "fcf_list": fcf_list_raw,
                        "years": len(fcf_list_raw)
                    },
                    "result": fcf_avg,
                    "detail": fcf_calculation
                },
                "step2": {
                    "title": "企業別高成長率（CAGR）",
                    "description": step2_description,
                    "formula": "(FCF_end / FCF_start)^(1/n) - 1",
                    "inputs": cagr_calculation,
                    "result": high_growth_rate,
                    "constraints": {"min": 0.15, "max": 0.50}
                },
                "step3": {
                    "title": "2段階DCF計算",
                    "description": step3_description,
                    "formula": "PV_high + PV_terminal",
                    "inputs": {
                        "fcf_base": fcf_avg,
                        "wacc": self.wacc,
                        "high_growth_rate": high_growth_rate,
                        "terminal_growth": self.terminal_growth
                    },
                    "result": {
                        "pv_high": pv_high,
                        "pv_terminal": pv_terminal,
                        "v0": v0
                    },
                    "detail": dcf_calculation
                },
                "step4": {
                    "title": "α 成長期待プレミアム",
                    "description": step4_description,
                    "formula": "max(0, (ROE × 内部留保率 / WACC) × 0.7)",
                    "inputs": alpha_calculation,
                    "result": alpha
                },
                "step5": {
                    "title": "本質的価値（P_t）算出",
                    "description": step5_description,
                    "formula": "V_0 × (1 + α)",
                    "inputs": pt_calculation,
                    "result": intrinsic_value_per_share
                }
            },
            
            # 従来形式のcomponents（互換性維持）
            "components": {
                **financials,
                "high_growth_rate_used": float(high_growth_rate),
                "pv_high": float(pv_high),
                "pv_terminal": float(pv_terminal),
                "roe_used": float(roe_avg),
                "fcf_floor_applied": float(fcf_floor_applied)
            }
        }


if __name__ == "__main__":
    # テスト実行
    calculator = KoichiValuationCalculator()
    
    test_data = {
        "fcf_5yr_avg": 4850000000,
        "diluted_shares": 3180000000,
        "roe_10yr_avg": 0.148,
        "current_price": 248.50,
        "fcf_list_raw": [2800000000, 3500000000, 4200000000, 5800000000, 7950000000],
        "latest_revenue": 96773000000,
        "eps_data": {"ticker": "TSLA"}
    }
    
    result = calculator.calculate_pt(test_data)
    
    import json
    print("\n=== Calculation Result ===")
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
