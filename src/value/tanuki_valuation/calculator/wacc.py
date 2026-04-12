"""
TANUKI VALUATION - Sensitivity Analysis
感度分析マトリクス

責務: WACC ± 1% × 高成長期間 3/5/7年 の9パターン計算
"""

from typing import Dict, Any, List, Callable
from dataclasses import dataclass


@dataclass
class SensitivityResult:
    """感度分析結果"""
    matrix: List[List[float]]     # 3×3マトリクス（1株あたり価格）
    wacc_values: List[float]      # WACCの値（3つ）
    growth_years: List[int]       # 高成長期間の値（3つ）
    base_wacc: float
    base_years: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "matrix": self.matrix,
            "wacc_values": self.wacc_values,
            "growth_years": self.growth_years,
            "base_wacc": self.base_wacc,
            "base_years": self.base_years
        }
    
    def get_value(self, wacc_idx: int, years_idx: int) -> float:
        """マトリクスから値を取得"""
        return self.matrix[wacc_idx][years_idx]


def calculate_sensitivity_matrix(
    calc_func: Callable[[float, int], float],
    base_wacc: float,
    base_years: int = 5,
    wacc_delta: float = 0.01,
    years_options: List[int] = None
) -> SensitivityResult:
    """
    感度分析マトリクスを計算
    
    Args:
        calc_func: 計算関数 (wacc, years) -> per_share_value
        base_wacc: ベースWACC
        base_years: ベース高成長期間
        wacc_delta: WACCの変動幅
        years_options: 高成長期間オプション
    
    Returns:
        SensitivityResult: 感度分析結果
    
    マトリクス構造:
                    3年    5年    7年
        WACC-1%   [ ][0,0] [0,1] [0,2]
        WACC      [ ][1,0] [1,1] [1,2]
        WACC+1%   [ ][2,0] [2,1] [2,2]
    """
    if years_options is None:
        years_options = [3, 5, 7]
    
    # WACCの3つの値
    wacc_values = [
        round(base_wacc - wacc_delta, 3),
        round(base_wacc, 3),
        round(base_wacc + wacc_delta, 3)
    ]
    
    # マトリクス計算
    matrix = []
    for wacc in wacc_values:
        row = []
        for years in years_options:
            value = calc_func(wacc, years)
            row.append(round(value, 2))
        matrix.append(row)
    
    return SensitivityResult(
        matrix=matrix,
        wacc_values=wacc_values,
        growth_years=years_options,
        base_wacc=base_wacc,
        base_years=base_years
    )


def create_sensitivity_calc_func(
    base_fcf: float,
    high_growth_rate: float,
    diluted_shares: int,
    rpo_pv: float,
    alpha: float,
    terminal_growth: float = 0.03
) -> Callable[[float, int], float]:
    """
    感度分析用の計算関数を生成
    
    Args:
        base_fcf: ベースFCF
        high_growth_rate: 高成長率
        diluted_shares: 希薄化後株式数
        rpo_pv: RPO現在価値
        alpha: α（成長期待プレミアム）
        terminal_growth: 永続成長率
    
    Returns:
        calc_func: (wacc, years) -> per_share_value
    """
    def calc_func(wacc: float, years: int) -> float:
        # DCF計算（簡易版）
        current_fcf = base_fcf
        pv_high = 0.0
        
        for t in range(years):
            current_fcf *= (1 + high_growth_rate)
            pv_high += current_fcf / (1 + wacc) ** (t + 1)
        
        # ターミナル価値
        terminal_fcf = current_fcf * (1 + terminal_growth)
        if wacc <= terminal_growth:
            terminal_value = terminal_fcf * 20
        else:
            terminal_value = terminal_fcf / (wacc - terminal_growth)
        pv_terminal = terminal_value / (1 + wacc) ** years
        
        # V_0
        v0 = pv_high + pv_terminal
        
        # P_t
        v0_adjusted = v0 + rpo_pv
        pt = v0_adjusted * (1 + alpha)
        
        # Per share
        if diluted_shares > 0:
            return pt / diluted_shares
        return 0.0
    
    return calc_func


def format_matrix_for_display(
    result: SensitivityResult,
    currency: str = "$"
) -> str:
    """
    マトリクスを表示用文字列に変換
    
    Args:
        result: SensitivityResult
        currency: 通貨記号
    
    Returns:
        フォーマット済み文字列
    """
    lines = []
    
    # ヘッダー
    header = "WACC \\ Years"
    for years in result.growth_years:
        header += f"\t{years}年"
    lines.append(header)
    
    # データ行
    for i, wacc in enumerate(result.wacc_values):
        row = f"{wacc:.1%}"
        for value in result.matrix[i]:
            row += f"\t{currency}{value:.2f}"
        lines.append(row)
    
    return "\n".join(lines)


if __name__ == "__main__":
    print("=== Sensitivity Analysis テスト ===\n")
    
    # テスト用計算関数
    calc_func = create_sensitivity_calc_func(
        base_fcf=1_000_000_000,       # $1B
        high_growth_rate=0.30,        # 30%
        diluted_shares=500_000_000,   # 500M
        rpo_pv=500_000_000,           # $500M
        alpha=0.5
    )
    
    # 感度分析実行
    result = calculate_sensitivity_matrix(
        calc_func=calc_func,
        base_wacc=0.12,
        base_years=5
    )
    
    print(format_matrix_for_display(result))
    
    print(f"\n基準値: WACC={result.base_wacc:.1%}, Years={result.base_years}")
    print(f"基準価格: ${result.get_value(1, 1):.2f}")
