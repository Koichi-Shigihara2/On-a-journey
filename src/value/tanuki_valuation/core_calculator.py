"""
TANUKI VALUATION - Core Calculator v6.2
Koichi式株価評価モデル（FCFベース自動判定対応）

P_t = (V_0 + RPO調整 + GrowthOption_PV) × (1 + α)
V_0 = 3段階DCF or 2段階DCF（maturity_config参照）

v6.2 追加:
  - FCFベース自動判定（determine_fcf_base）
    直近2年 / 5年平均 の比率が閾値超 → recent_2yr を自動選択
    安定・成熟企業 → avg_5yr（既存動作）

計算フロー:
  1. WACC計算（CAPM）
  2. 成長率決定
  3. FCF補正（マイナスFCF対応）
  4. FCFベース自動判定（v6.2追加）← NEW
  5. DCF計算（2段階 or 3段階）
  6. RPO補正
  7. 成長オプションPV計算
  8. α計算
  9. 本質的価値（P_t）算出
 10. 感度分析
 11. シナリオ分析
 12. 将来価値予測
"""

import os
import sys

_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from typing import Dict, Any, Optional
from datetime import datetime

from calculator import (
    calculate_wacc, WACCResult,
    determine_growth_rate, GrowthResult,
    calculate_two_stage_dcf, DCFResult,
    DEFAULT_HIGH_GROWTH_YEARS, DEFAULT_TERMINAL_GROWTH,
    adjust_fcf, adjust_rpo, calculate_alpha,
    calculate_intrinsic_value, calculate_per_share_value, calculate_upside,
    DEFAULT_RETENTION_RATE, DEFAULT_ALPHA_CAP,
    calculate_sensitivity_matrix, create_sensitivity_calc_func, SensitivityResult,
    calculate_scenario_valuations, create_scenario_calc_func as create_scenario_func, ScenarioResult,
    calculate_future_values,
)

from calculator.dcf import calculate_three_stage_dcf, ThreeStageDCFResult
from calculator.adjustments import (
    calculate_growth_option_pv, GrowthOptionResult,
    determine_fcf_base, FCFBaseResult,          # v6.2追加
    DEFAULT_FCF_CV_THRESHOLD,
)

try:
    from maturity_config import get_maturity_profile, is_three_stage, get_terminal_growth
    HAS_MATURITY_CONFIG = True
except ImportError:
    HAS_MATURITY_CONFIG = False


class KoichiValuationCalculator:
    """
    Koichi式 v6.2 バリュエーション計算エンジン

    v6.2変更点:
      - FCFベース自動判定（急拡大銘柄は直近2年平均を使用）
    """

    VERSION = "6.2.0"

    def __init__(
        self,
        high_growth_years: int = DEFAULT_HIGH_GROWTH_YEARS,
        terminal_growth: float = DEFAULT_TERMINAL_GROWTH,
        retention_rate: float = DEFAULT_RETENTION_RATE,
        alpha_cap: float = DEFAULT_ALPHA_CAP,
        min_fcf_years: int = 3,
        fcf_base_threshold: float = DEFAULT_FCF_CV_THRESHOLD,
    ):
        self.high_growth_years = high_growth_years
        self.terminal_growth = terminal_growth
        self.retention_rate = retention_rate
        self.alpha_cap = alpha_cap
        self.min_fcf_years = min_fcf_years
        self.fcf_base_threshold = fcf_base_threshold

    def calculate_pt(self, financials: Dict[str, Any]) -> Dict[str, Any]:
        """メイン計算関数"""

        # ── データ抽出 ──
        fcf_avg        = financials.get("fcf_5yr_avg", 0.0)
        fcf_2yr_avg    = financials.get("fcf_2yr_avg", 0.0)   # v6.2追加
        diluted_shares = financials.get("diluted_shares", 0)
        roe_avg        = financials.get("roe_10yr_avg", 0.0)
        latest_revenue = financials.get("latest_revenue", 0.0)
        fcf_list_raw   = financials.get("fcf_list_raw", [])
        current_price  = financials.get("current_price", 0.0)
        ticker         = financials.get("eps_data", {}).get("ticker", "Unknown")
        rpo            = financials.get("rpo", 0.0)
        beta           = financials.get("beta")
        sector         = financials.get("sector")

        # ── バリデーション ──
        if diluted_shares <= 100_000:
            return {"error": "diluted_shares missing or invalid", "ticker": ticker}

        if len(fcf_list_raw) < self.min_fcf_years:
            return {
                "error": f"FCFデータ不足 ({len(fcf_list_raw)}年)",
                "ticker": ticker,
                "fcf_years_available": len(fcf_list_raw),
                "min_required": self.min_fcf_years
            }

        # ── STEP 1: WACC計算 ──
        wacc_result: WACCResult = calculate_wacc(beta=beta, sector=sector)
        wacc = wacc_result.value
        print(f"   [{ticker}] WACC (CAPM): {wacc:.1%} (β={wacc_result.beta:.2f})")

        # ── STEP 2: 成長率決定 ──
        growth_result: GrowthResult = determine_growth_rate(
            ticker=ticker, fcf_list=fcf_list_raw
        )
        high_growth_rate = growth_result.rate
        print(f"   [{ticker}] 成長率: {high_growth_rate:.1%} (source: {growth_result.source})")

        # ── STEP 3: FCF補正（マイナスFCF対応） ──
        fcf_adjustment = adjust_fcf(fcf_avg=fcf_avg, latest_revenue=latest_revenue)
        adjusted_fcf_5yr = fcf_adjustment.adjusted_fcf
        if fcf_adjustment.method != "none":
            print(f"   [{ticker}] FCF補正: ${fcf_adjustment.original_fcf:,.0f} → ${adjusted_fcf_5yr:,.0f}")

        # ── STEP 4: FCFベース自動判定（v6.3 CV方式） ──
        # 元のfcf_2yr_avg（マイナス含む）をそのまま渡す。
        # フォールバック処理はdetermine_fcf_base内で行う。
        fcf_base_result: FCFBaseResult = determine_fcf_base(
            fcf_5yr_avg=adjusted_fcf_5yr,
            fcf_2yr_avg=fcf_2yr_avg,
            fcf_list=fcf_list_raw,
            threshold=self.fcf_base_threshold
        )
        adjusted_fcf_2yr = fcf_2yr_avg  # ログ表示用
        base_fcf = fcf_base_result.base_fcf
        print(f"   [{ticker}] FCFベース: {fcf_base_result.method}  "
              f"5yr=${adjusted_fcf_5yr/1e9:.2f}B  "
              f"2yr=${adjusted_fcf_2yr/1e9:.2f}B  "
              f"→ 採用=${base_fcf/1e9:.2f}B"
              + (f"  (CV={fcf_base_result.cv:.2f})" if fcf_base_result.cv < 999 else "  (CV=データ不足)"))

        # ── STEP 5: DCF計算（2段階 or 3段階） ──
        dcf_type = "two_stage"
        dcf_result = None
        three_stage_result: Optional[ThreeStageDCFResult] = None
        maturity_profile = None

        terminal_growth = self.terminal_growth
        if HAS_MATURITY_CONFIG:
            terminal_growth = get_terminal_growth(ticker)

        if HAS_MATURITY_CONFIG and is_three_stage(ticker):
            maturity_profile = get_maturity_profile(ticker)
            p1 = maturity_profile["phase1"]
            p2 = maturity_profile["phase2"]

            phase1_growth = p1["growth"] if p1["growth"] is not None else high_growth_rate
            phase2_growth = p2["growth"]
            phase1_years  = p1["years"]
            phase2_years  = p2["years"]

            print(f"   [{ticker}] DCF: 3段階  P1={phase1_years}yr@{phase1_growth:.1%}  P2={phase2_years}yr@{phase2_growth:.1%}  TV={terminal_growth:.1%}")

            three_stage_result = calculate_three_stage_dcf(
                base_fcf=base_fcf,
                phase1_growth_rate=phase1_growth,
                phase2_growth_rate=phase2_growth,
                wacc=wacc,
                phase1_years=phase1_years,
                phase2_years=phase2_years,
                terminal_growth=terminal_growth
            )
            v0 = three_stage_result.v0
            dcf_type = "three_stage"
        else:
            print(f"   [{ticker}] DCF: 2段階  g={high_growth_rate:.1%}  TV={terminal_growth:.1%}")
            dcf_result = calculate_two_stage_dcf(
                base_fcf=base_fcf,
                high_growth_rate=high_growth_rate,
                wacc=wacc,
                high_growth_years=self.high_growth_years,
                terminal_growth=terminal_growth
            )
            v0 = dcf_result.v0

        # ── STEP 6: RPO補正 ──
        rpo_adjustment = adjust_rpo(rpo=rpo)
        rpo_pv = rpo_adjustment.rpo_pv
        if rpo_adjustment.applied:
            print(f"   [{ticker}] RPO補正: ${rpo:,.0f} → PV ${rpo_pv:,.0f}")

        # ── STEP 7: 成長オプションPV計算 ──
        go_result: GrowthOptionResult = calculate_growth_option_pv(ticker)
        growth_option_pv = go_result.total_pv
        if go_result.applied:
            print(f"   [{ticker}] 成長オプション: {go_result.count}件  PV=${growth_option_pv/1e9:.2f}B")

        # ── STEP 8: α計算 ──
        alpha_result = calculate_alpha(
            roe=roe_avg, wacc=wacc,
            retention_rate=self.retention_rate, alpha_cap=self.alpha_cap
        )
        alpha = alpha_result.alpha
        if alpha_result.was_capped:
            print(f"   [{ticker}] α: {alpha_result.alpha_uncapped:.3f} → cap → {alpha:.3f}")
        else:
            print(f"   [{ticker}] α: {alpha:.3f}")

        # ── STEP 9: 本質的価値（P_t）算出 ──
        v0_adjusted, intrinsic_value_pt = calculate_intrinsic_value(
            v0=v0, rpo_pv=rpo_pv, alpha=alpha,
            growth_option_pv=growth_option_pv
        )

        intrinsic_value_per_share = calculate_per_share_value(
            intrinsic_value_pt=intrinsic_value_pt,
            diluted_shares=diluted_shares
        )

        upside_percent = calculate_upside(
            intrinsic_value_per_share=intrinsic_value_per_share,
            current_price=current_price
        )

        # ── STEP 10: 感度分析 ──
        sensitivity_calc_func = create_sensitivity_calc_func(
            base_fcf=base_fcf,
            high_growth_rate=high_growth_rate,
            diluted_shares=diluted_shares,
            rpo_pv=rpo_pv + growth_option_pv,
            alpha=alpha,
            terminal_growth=terminal_growth
        )
        sensitivity_result: SensitivityResult = calculate_sensitivity_matrix(
            calc_func=sensitivity_calc_func,
            base_wacc=wacc,
            base_years=self.high_growth_years
        )

        # ── STEP 11: シナリオ分析 ──
        scenario_result: Optional[ScenarioResult] = None
        if growth_result.source == "segment_weighted":
            scenario_calc_func = create_scenario_func(
                base_fcf=base_fcf,
                wacc=wacc,
                high_growth_years=self.high_growth_years,
                diluted_shares=diluted_shares,
                rpo_pv=rpo_pv + growth_option_pv,
                alpha=alpha,
                terminal_growth=terminal_growth
            )
            scenario_result = calculate_scenario_valuations(
                calc_func=scenario_calc_func,
                base_growth_rate=high_growth_rate
            )

        # ── STEP 12: 将来価値予測 ──
        future_values = calculate_future_values(
            current_value=intrinsic_value_per_share,
            high_growth_rate=high_growth_rate,
            high_growth_years=self.high_growth_years,
            terminal_growth=terminal_growth
        )
        print(f"   [{ticker}] 1〜3年後理論株価: {future_values}")

        # ── DCF詳細（共通フォーマット） ──
        if dcf_type == "three_stage" and three_stage_result:
            dcf_components = three_stage_result.to_dict()
            pv_high = three_stage_result.pv_high_growth
            pv_terminal = three_stage_result.pv_terminal
        else:
            dcf_components = dcf_result.to_dict() if dcf_result else {}
            pv_high = dcf_result.pv_high_growth if dcf_result else 0.0
            pv_terminal = dcf_result.pv_terminal if dcf_result else 0.0

        # ── 結果返却 ──
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
            "formula": f"Koichi式 v{self.VERSION}（動的WACC + {dcf_type} DCF + FCFベース自動判定 + 成長オプション）",
            "dcf_type": dcf_type,

            "wacc": wacc_result.to_dict(),
            "sensitivity": sensitivity_result.to_dict(),
            "growth_scenarios": {
                "primary": {"rate": high_growth_rate, "source": growth_result.source},
                "segment": growth_result.segment_detail
            },
            "scenario_valuations": scenario_result.to_dict() if scenario_result else None,
            "growth_options": go_result.to_dict(),
            "maturity_profile": maturity_profile,
            "dcf_components": dcf_components,

            # FCFベース判定結果（v6.2追加）
            "fcf_base": fcf_base_result.to_dict(),

            "components": {
                "fcf_5yr_avg": financials.get("fcf_5yr_avg"),
                "fcf_2yr_avg": fcf_2yr_avg,
                "fcf_base_used": base_fcf,
                "fcf_base_method": fcf_base_result.method,
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
                "terminal_growth_used": terminal_growth,
                "pv_high": pv_high,
                "pv_terminal": pv_terminal,
                "roe_used": roe_avg,
                "fcf_floor_applied": fcf_adjustment.floor_applied,
                "rpo_pv": rpo_pv,
                "growth_option_pv": growth_option_pv,
                "alpha_uncapped": alpha_result.alpha_uncapped,
            }
        }

        return result


def create_calculator(**kwargs) -> KoichiValuationCalculator:
    return KoichiValuationCalculator(**kwargs)


if __name__ == "__main__":
    calculator = KoichiValuationCalculator()

    test_data = {
        "fcf_5yr_avg": 8_234_200_000,
        "fcf_2yr_avg": 50_000_000_000,
        "diluted_shares": 10_754_251_799,
        "roe_10yr_avg": 0.182,
        "current_price": 254.17,
        "fcf_list_raw": [2e9, 3e9, 5e9, 25e9, 75e9],
        "latest_revenue": 716_924_000_000,
        "eps_data": {"ticker": "AMZN"},
        "rpo": 25_000_000_000,
        "beta": 1.38,
        "sector": "Consumer Cyclical"
    }

    result = calculator.calculate_pt(test_data)

    if "error" not in result:
        print(f"\n=== 結果 ===")
        print(f"DCFタイプ      : {result['dcf_type']}")
        print(f"FCFベース      : {result['fcf_base']['method']}  (CV={result['fcf_base']['cv']})")
        print(f"理論株価       : ${result['intrinsic_value_per_share']:.2f}")
        print(f"乖離率         : {result['upside_percent']:.1f}%")
    else:
        print(f"エラー: {result['error']}")
