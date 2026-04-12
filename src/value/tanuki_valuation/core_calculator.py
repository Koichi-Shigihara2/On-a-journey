"""
TANUKI VALUATION - Core Calculator v6.0
Koichi式株価評価モデル（モジュール分割版）

P_t = (V_0 + RPO調整) × (1 + α)
V_0 = 2段階DCF（高成長期5年 + ターミナル）

新機能 (v6.0):
- モジュール分割によるコード整理
- 明確な責務分離
- テスト容易性の向上

計算フロー:
1. WACC計算（CAPM）: wacc.py
2. 成長率決定: growth.py
3. FCF補正: adjustments.py
4. 2段階DCF: dcf.py
5. RPO補正 + α計算: adjustments.py
6. 感度分析: sensitivity.py
7. シナリオ分析: scenarios.py
8. 将来価値予測: future_values.py
"""

from typing import Dict, Any, Optional
from datetime import datetime

# Calculator modules
from .calculator import (
    # WACC
    calculate_wacc,
    WACCResult,
    
    # Growth
    determine_growth_rate,
    GrowthResult,
    
    # DCF
    calculate_two_stage_dcf,
    DCFResult,
    DEFAULT_HIGH_GROWTH_YEARS,
    DEFAULT_TERMINAL_GROWTH,
    
    # Adjustments
    adjust_fcf,
    adjust_rpo,
    calculate_alpha,
    calculate_intrinsic_value,
    calculate_per_share_value,
    calculate_upside,
    DEFAULT_RETENTION_RATE,
    DEFAULT_ALPHA_CAP,
    
    # Sensitivity
    calculate_sensitivity_matrix,
    create_sensitivity_calc_func,
    SensitivityResult,
    
    # Scenarios
    calculate_scenario_valuations,
    create_scenario_calc_func as create_scenario_func,
    ScenarioResult,
    
    # Future Values
    calculate_future_values,
)


class KoichiValuationCalculator:
    """
    Koichi式 v6.0 バリュエーション計算エンジン
    
    各計算ステップを独立モジュールに委譲するオーケストレーター
    """
    
    VERSION = "6.0.0"
    
    def __init__(
        self,
        high_growth_years: int = DEFAULT_HIGH_GROWTH_YEARS,
        terminal_growth: float = DEFAULT_TERMINAL_GROWTH,
        retention_rate: float = DEFAULT_RETENTION_RATE,
        alpha_cap: float = DEFAULT_ALPHA_CAP,
        min_fcf_years: int = 3,
    ):
        """
        Args:
            high_growth_years: 高成長期間（年）
            terminal_growth: 永続成長率
            retention_rate: 内部留保率
            alpha_cap: αの上限
            min_fcf_years: 最低FCFデータ年数
        """
        self.high_growth_years = high_growth_years
        self.terminal_growth = terminal_growth
        self.retention_rate = retention_rate
        self.alpha_cap = alpha_cap
        self.min_fcf_years = min_fcf_years
    
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
                "eps_data": {"ticker": str},
                "rpo": float (optional),
                "beta": float (optional),
                "sector": str (optional)
            }
        
        Returns:
            完全なバリュエーション結果
        """
        # ========================================
        # データ抽出
        # ========================================
        fcf_avg = financials.get("fcf_5yr_avg", 0.0)
        diluted_shares = financials.get("diluted_shares", 0)
        roe_avg = financials.get("roe_10yr_avg", 0.0)
        latest_revenue = financials.get("latest_revenue", 0.0)
        fcf_list_raw = financials.get("fcf_list_raw", [])
        current_price = financials.get("current_price", 0.0)
        ticker = financials.get("eps_data", {}).get("ticker", "Unknown")
        rpo = financials.get("rpo", 0.0)
        beta = financials.get("beta")
        sector = financials.get("sector")
        
        # ========================================
        # バリデーション
        # ========================================
        if diluted_shares <= 100_000:
            return {"error": "diluted_shares missing or invalid", "ticker": ticker}
        
        if len(fcf_list_raw) < self.min_fcf_years:
            print(f"   [{ticker}] ⚠️ FCFデータ不足 ({len(fcf_list_raw)}年 < {self.min_fcf_years}年)")
            return {
                "error": f"FCFデータ不足 ({len(fcf_list_raw)}年)",
                "ticker": ticker,
                "fcf_years_available": len(fcf_list_raw),
                "min_required": self.min_fcf_years
            }
        
        # ========================================
        # STEP 1: WACC計算（CAPM）
        # ========================================
        wacc_result: WACCResult = calculate_wacc(
            beta=beta,
            sector=sector
        )
        wacc = wacc_result.value
        print(f"   [{ticker}] WACC (CAPM): {wacc:.1%} (β={wacc_result.beta:.2f})")
        
        # ========================================
        # STEP 2: 成長率決定
        # ========================================
        growth_result: GrowthResult = determine_growth_rate(
            ticker=ticker,
            fcf_list=fcf_list_raw
        )
        high_growth_rate = growth_result.rate
        print(f"   [{ticker}] 成長率: {high_growth_rate:.1%} (source: {growth_result.source})")
        
        # ========================================
        # STEP 3: FCF補正
        # ========================================
        fcf_adjustment = adjust_fcf(
            fcf_avg=fcf_avg,
            latest_revenue=latest_revenue
        )
        adjusted_fcf = fcf_adjustment.adjusted_fcf
        
        if fcf_adjustment.method != "none":
            print(f"   [{ticker}] FCF補正: ${fcf_adjustment.original_fcf:,.0f} → ${adjusted_fcf:,.0f}")
        
        # ========================================
        # STEP 4: 2段階DCF計算
        # ========================================
        dcf_result: DCFResult = calculate_two_stage_dcf(
            base_fcf=adjusted_fcf,
            high_growth_rate=high_growth_rate,
            wacc=wacc,
            high_growth_years=self.high_growth_years,
            terminal_growth=self.terminal_growth
        )
        v0 = dcf_result.v0
        
        # ========================================
        # STEP 5: RPO補正
        # ========================================
        rpo_adjustment = adjust_rpo(rpo=rpo)
        rpo_pv = rpo_adjustment.rpo_pv
        
        if rpo_adjustment.applied:
            print(f"   [{ticker}] RPO補正: ${rpo:,.0f} → PV ${rpo_pv:,.0f}")
        
        # ========================================
        # STEP 6: α計算
        # ========================================
        alpha_result = calculate_alpha(
            roe=roe_avg,
            wacc=wacc,
            retention_rate=self.retention_rate,
            alpha_cap=self.alpha_cap
        )
        alpha = alpha_result.alpha
        
        if alpha_result.was_capped:
            print(f"   [{ticker}] α: {alpha_result.alpha_uncapped:.3f} → キャップ適用 → {alpha:.3f}")
        else:
            print(f"   [{ticker}] α: {alpha:.3f}")
        
        # ========================================
        # STEP 7: 本質的価値（P_t）算出
        # ========================================
        v0_adjusted, intrinsic_value_pt = calculate_intrinsic_value(
            v0=v0,
            rpo_pv=rpo_pv,
            alpha=alpha
        )
        
        intrinsic_value_per_share = calculate_per_share_value(
            intrinsic_value_pt=intrinsic_value_pt,
            diluted_shares=diluted_shares
        )
        
        upside_percent = calculate_upside(
            intrinsic_value_per_share=intrinsic_value_per_share,
            current_price=current_price
        )
        
        # ========================================
        # STEP 8: 感度分析マトリクス
        # ========================================
        sensitivity_calc_func = create_sensitivity_calc_func(
            base_fcf=adjusted_fcf,
            high_growth_rate=high_growth_rate,
            diluted_shares=diluted_shares,
            rpo_pv=rpo_pv,
            alpha=alpha,
            terminal_growth=self.terminal_growth
        )
        
        sensitivity_result: SensitivityResult = calculate_sensitivity_matrix(
            calc_func=sensitivity_calc_func,
            base_wacc=wacc,
            base_years=self.high_growth_years
        )
        
        # ========================================
        # STEP 9: シナリオ分析（セグメント設定がある場合のみ）
        # ========================================
        scenario_result: Optional[ScenarioResult] = None
        
        if growth_result.source == "segment_weighted":
            scenario_calc_func = create_scenario_func(
                base_fcf=adjusted_fcf,
                wacc=wacc,
                high_growth_years=self.high_growth_years,
                diluted_shares=diluted_shares,
                rpo_pv=rpo_pv,
                alpha=alpha,
                terminal_growth=self.terminal_growth
            )
            
            scenario_result = calculate_scenario_valuations(
                calc_func=scenario_calc_func,
                base_growth_rate=high_growth_rate
            )
        
        # ========================================
        # STEP 10: 将来価値予測
        # ========================================
        future_values = calculate_future_values(
            current_value=intrinsic_value_per_share,
            high_growth_rate=high_growth_rate,
            high_growth_years=self.high_growth_years,
            terminal_growth=self.terminal_growth
        )
        
        print(f"   [{ticker}] 1〜3年後理論株価: {future_values}")
        
        # ========================================
        # 結果返却
        # ========================================
        result = {
            "intrinsic_value_pt": float(intrinsic_value_pt),
            "intrinsic_value_per_share": float(intrinsic_value_per_share),
            "v0": float(v0),
            "v0_adjusted": float(v0_adjusted),
            "alpha": float(alpha),
            "alpha_was_capped": alpha_result.was_capped,
            "future_values": future_values,
            "upside_percent": round(upside_percent, 1),
            "calculation_date": datetime.now().strftime("%Y-%m-%d"),
            "formula": f"Koichi式 v{self.VERSION}（動的WACC＋感度分析＋セグメント成長率）",
            
            # WACC詳細
            "wacc": wacc_result.to_dict(),
            
            # 感度分析
            "sensitivity": sensitivity_result.to_dict(),
            
            # 成長シナリオ
            "growth_scenarios": {
                "primary": {
                    "rate": high_growth_rate,
                    "source": growth_result.source
                },
                "segment": growth_result.segment_detail
            },
            
            # シナリオ別理論株価
            "scenario_valuations": scenario_result.to_dict() if scenario_result else None,
            
            # 計算コンポーネント
            "components": {
                "fcf_5yr_avg": financials.get("fcf_5yr_avg"),
                "fcf_list_raw": fcf_list_raw,
                "diluted_shares": diluted_shares,
                "roe_10yr_avg": roe_avg,
                "current_price": current_price,
                "latest_revenue": latest_revenue,
                "rpo": rpo,
                "beta": wacc_result.beta,
                "sector": sector,
                "eps_data": financials.get("eps_data"),
                "_shares_source": financials.get("_shares_source"),
                "_beta_source": financials.get("_beta_source"),
                "high_growth_rate_used": high_growth_rate,
                "high_growth_years": self.high_growth_years,
                "pv_high": dcf_result.pv_high_growth,
                "pv_terminal": dcf_result.pv_terminal,
                "roe_used": roe_avg,
                "fcf_floor_applied": fcf_adjustment.floor_applied,
                "rpo_adjustment": rpo_pv,
                "alpha_uncapped": alpha_result.alpha_uncapped,
            }
        }
        
        return result


# ========================================
# エントリーポイント
# ========================================
def create_calculator(**kwargs) -> KoichiValuationCalculator:
    """計算エンジンを生成"""
    return KoichiValuationCalculator(**kwargs)


if __name__ == "__main__":
    # 簡易テスト
    calculator = KoichiValuationCalculator()
    
    test_data = {
        "fcf_5yr_avg": 5_000_000_000,
        "diluted_shares": 3_000_000_000,
        "roe_10yr_avg": 0.15,
        "current_price": 100.0,
        "fcf_list_raw": [3e9, 4e9, 5e9, 6e9, 7e9],
        "latest_revenue": 50_000_000_000,
        "eps_data": {"ticker": "TEST"},
        "rpo": 5_000_000_000,
        "beta": 1.5,
        "sector": "Technology"
    }
    
    result = calculator.calculate_pt(test_data)
    
    if "error" not in result:
        print(f"\n=== 結果 ===")
        print(f"理論株価: ${result['intrinsic_value_per_share']:.2f}")
        print(f"乖離率: {result['upside_percent']:.1f}%")
        print(f"WACC: {result['wacc']['value']:.1%}")
    else:
        print(f"エラー: {result['error']}")
