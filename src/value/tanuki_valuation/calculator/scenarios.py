"""
TANUKI VALUATION - Scenario Analysis
シナリオ別理論株価計算

責務: Bear/Base/Bull の3シナリオでの理論株価算出
"""

from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass


@dataclass
class ScenarioValuation:
    """単一シナリオの評価結果"""
    growth_rate: float
    intrinsic_value_per_share: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "growth_rate": self.growth_rate,
            "intrinsic_value_per_share": self.intrinsic_value_per_share
        }


@dataclass
class ScenarioResult:
    """シナリオ分析結果"""
    bear: ScenarioValuation
    base: ScenarioValuation
    bull: ScenarioValuation
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "bear": self.bear.to_dict(),
            "base": self.base.to_dict(),
            "bull": self.bull.to_dict()
        }


def calculate_scenario_valuations(
    calc_func: Callable[[float], float],
    base_growth_rate: float,
    bear_multiplier: float = 0.7,
    bull_multiplier: float = 1.2
) -> ScenarioResult:
    """
    シナリオ別理論株価を計算
    
    Args:
        calc_func: 計算関数 (growth_rate) -> per_share_value
        base_growth_rate: ベース成長率
        bear_multiplier: Bear乗数（成長率に適用）
        bull_multiplier: Bull乗数（成長率に適用）
    
    Returns:
        ScenarioResult: シナリオ分析結果
    
    シナリオ設定:
        Bear: 成長率 × 0.7（悲観）
        Base: 成長率 × 1.0（基準）
        Bull: 成長率 × 1.2（楽観）

    [[SCENARIO-BEARBULL-SIGN-FLIP-1]]: base_growth_rateが負の場合、
    単純乗算（bear=base×0.7・bull=base×1.2）だと下落幅の大小関係が
    ラベルと逆転する（例: base=-10%のとき、単純乗算ではbear=-7%
    〈下落が緩い＝実態は楽観〉・bull=-12%〈下落が急＝実態は悲観〉と
    なりBear/Bullの意味が入れ替わる）。負の場合のみ乗数を反転させ
    （bear=×1.3・bull=×0.8）、下落幅で見てBearが最も悲観的
    （bear_rate < base_rate < bull_rate）となるよう補正する。
    正の場合は既存の挙動（呼び出し元が渡すbear_multiplier/
    bull_multiplier、デフォルト0.7/1.2）を一切変更しない。
    """
    # 成長率計算
    if base_growth_rate < 0:
        # 負の成長率: 乗数を反転させ、下落幅がBear>Base>Bullになるよう補正
        bear_rate = base_growth_rate * 1.3
        base_rate = base_growth_rate
        bull_rate = base_growth_rate * 0.8
    else:
        bear_rate = base_growth_rate * bear_multiplier
        base_rate = base_growth_rate
        bull_rate = base_growth_rate * bull_multiplier
    
    # 各シナリオの理論株価計算
    bear_value = calc_func(bear_rate)
    base_value = calc_func(base_rate)
    bull_value = calc_func(bull_rate)
    
    return ScenarioResult(
        bear=ScenarioValuation(
            growth_rate=round(bear_rate, 3),
            intrinsic_value_per_share=round(bear_value, 2)
        ),
        base=ScenarioValuation(
            growth_rate=round(base_rate, 3),
            intrinsic_value_per_share=round(base_value, 2)
        ),
        bull=ScenarioValuation(
            growth_rate=round(bull_rate, 3),
            intrinsic_value_per_share=round(bull_value, 2)
        )
    )


def create_scenario_calc_func(
    base_fcf: float,
    wacc: float,
    high_growth_years: int,
    diluted_shares: int,
    rpo_pv: float,
    alpha: float,
    terminal_growth: float = 0.03,
    net_cash_per_share: float = 0.0,
    phase2_growth: float = None,
    phase2_years: int = 0,
    tapering_g_end: float | None = None,
) -> Callable[[float], float]:
    """
    シナリオ分析用の計算関数を生成（v7.1: BS補正・3段階DCF対応）

    Args:
        base_fcf: ベースFCF
        wacc: WACC
        high_growth_years: Phase1高成長期間
        diluted_shares: 希薄化後株式数
        rpo_pv: RPO + 成長オプション現在価値の合計
        alpha: α
        terminal_growth: 永続成長率
        net_cash_per_share: BS補正（ネットキャッシュ/株）v7.1追加
        phase2_growth: Phase2成長率（Noneなら2段階DCF）
        phase2_years: Phase2年数（0なら2段階DCF）
        tapering_g_end: Phase1終了成長率（設定時は線形逓減DCFを使用）v9.0追加

    Returns:
        calc_func: (growth_rate) -> per_share_value（BS補正込み）
    """
    def calc_func(growth_rate: float) -> float:
        # 精密計算のためdcfモジュールを使用
        try:
            if tapering_g_end is not None and growth_rate > tapering_g_end:
                # 線形逓減DCF（DCF-1）: シナリオ成長率がg_endを超える場合のみ適用
                from .dcf import calculate_tapering_dcf
                result = calculate_tapering_dcf(
                    base_fcf=base_fcf,
                    g_start=growth_rate,
                    g_end=tapering_g_end,
                    high_growth_years=high_growth_years,
                    terminal_growth=terminal_growth,
                    wacc=wacc,
                )
                v0 = result.v0
            elif phase2_growth is not None and phase2_years > 0:
                from .dcf import calculate_three_stage_dcf
                result = calculate_three_stage_dcf(
                    base_fcf=base_fcf,
                    phase1_growth_rate=growth_rate,
                    phase1_years=high_growth_years,
                    phase2_growth_rate=phase2_growth,
                    phase2_years=phase2_years,
                    terminal_growth=terminal_growth,
                    wacc=wacc,
                )
                v0 = result.v0
            else:
                from .dcf import calculate_two_stage_dcf
                result = calculate_two_stage_dcf(
                    base_fcf=base_fcf,
                    high_growth_rate=growth_rate,
                    high_growth_years=high_growth_years,
                    terminal_growth=terminal_growth,
                    wacc=wacc,
                )
                v0 = result.v0
        except Exception:
            # フォールバック: シンプル計算
            current_fcf = base_fcf
            pv = 0.0
            t = 0
            for _ in range(high_growth_years):
                t += 1
                current_fcf *= (1 + growth_rate)
                pv += current_fcf / (1 + wacc) ** t
            if phase2_growth is not None and phase2_years > 0:
                for _ in range(phase2_years):
                    t += 1
                    current_fcf *= (1 + phase2_growth)
                    pv += current_fcf / (1 + wacc) ** t
            terminal_fcf = current_fcf * (1 + terminal_growth)
            tv = terminal_fcf / (wacc - terminal_growth) if wacc > terminal_growth else terminal_fcf * 20
            pv += tv / (1 + wacc) ** t
            v0 = pv

        # P_t = V0 × (1 + α) + rpo_pv（αの外出し v9.0修正）
        pt = v0 * (1 + alpha) + rpo_pv

        # 1株あたり + BS補正
        if diluted_shares > 0:
            return pt / diluted_shares + net_cash_per_share
        return 0.0

    return calc_func


def format_scenario_for_display(result: ScenarioResult) -> str:
    """
    シナリオ結果を表示用文字列に変換
    """
    lines = [
        "シナリオ\t成長率\t理論株価",
        f"Bear\t{result.bear.growth_rate:.1%}\t${result.bear.intrinsic_value_per_share:.2f}",
        f"Base\t{result.base.growth_rate:.1%}\t${result.base.intrinsic_value_per_share:.2f}",
        f"Bull\t{result.bull.growth_rate:.1%}\t${result.bull.intrinsic_value_per_share:.2f}"
    ]
    return "\n".join(lines)


# デフォルトパラメータ
DEFAULT_BEAR_MULTIPLIER = 0.7
DEFAULT_BULL_MULTIPLIER = 1.2


if __name__ == "__main__":
    print("=== Scenario Analysis テスト ===\n")
    
    # テスト用計算関数
    calc_func = create_scenario_calc_func(
        base_fcf=1_000_000_000,       # $1B
        wacc=0.12,                    # 12%
        high_growth_years=5,
        diluted_shares=500_000_000,   # 500M
        rpo_pv=500_000_000,           # $500M
        alpha=0.5
    )
    
    # シナリオ分析実行
    result = calculate_scenario_valuations(
        calc_func=calc_func,
        base_growth_rate=0.30
    )
    
    print(format_scenario_for_display(result))
    
    print(f"\n詳細:")
    print(f"  Bear差分: ${result.base.intrinsic_value_per_share - result.bear.intrinsic_value_per_share:.2f}")
    print(f"  Bull差分: ${result.bull.intrinsic_value_per_share - result.base.intrinsic_value_per_share:.2f}")
