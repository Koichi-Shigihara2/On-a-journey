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
    """
    # 成長率計算
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
    terminal_growth: float = 0.03
) -> Callable[[float], float]:
    """
    シナリオ分析用の計算関数を生成
    
    Args:
        base_fcf: ベースFCF
        wacc: WACC
        high_growth_years: 高成長期間
        diluted_shares: 希薄化後株式数
        rpo_pv: RPO現在価値
        alpha: α
        terminal_growth: 永続成長率
    
    Returns:
        calc_func: (growth_rate) -> per_share_value
    """
    def calc_func(growth_rate: float) -> float:
        # DCF計算
        current_fcf = base_fcf
        pv_high = 0.0
        
        for t in range(high_growth_years):
            current_fcf *= (1 + growth_rate)
            pv_high += current_fcf / (1 + wacc) ** (t + 1)
        
        # ターミナル価値
        terminal_fcf = current_fcf * (1 + terminal_growth)
        if wacc <= terminal_growth:
            terminal_value = terminal_fcf * 20
        else:
            terminal_value = terminal_fcf / (wacc - terminal_growth)
        pv_terminal = terminal_value / (1 + wacc) ** high_growth_years
        
        # V_0 + RPO + α
        v0 = pv_high + pv_terminal
        v0_adjusted = v0 + rpo_pv
        pt = v0_adjusted * (1 + alpha)
        
        # Per share
        if diluted_shares > 0:
            return pt / diluted_shares
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
