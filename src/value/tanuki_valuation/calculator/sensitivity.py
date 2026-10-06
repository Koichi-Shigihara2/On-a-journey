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
    感度分析マトリクスを計算（v7.1: base_years中心の可変軸）

    Args:
        calc_func: 計算関数 (wacc, years) -> per_share_value
        base_wacc: ベースWACC
        base_years: Phase1の実際の年数（中央列に配置）
        wacc_delta: WACCの変動幅（±1%）
        years_options: 年数軸を明示指定する場合（Noneで自動計算）

    Returns:
        SensitivityResult: 感度分析結果

    マトリクス構造（base_years=4の例）:
                    2年    4年    6年
        WACC-1%   [ ][0,0] [0,1] [0,2]
        WACC      [ ][1,0] [1,1] [1,2]  ← 基準値 = 理論株価と一致
        WACC+1%   [ ][2,0] [2,1] [2,2]
    """
    if years_options is None:
        # base_yearsを中央に、±(base_years//2または2)を両側に配置
        # base_years=1の場合は [1, 2, 3]、base_years=2なら [1, 2, 4] など
        step = max(1, base_years // 2)
        left  = max(1, base_years - step)
        right = base_years + step
        years_options = [left, base_years, right]

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
    terminal_growth: float = 0.03,
    net_cash_per_share: float = 0.0,
    phase2_growth: float = None,
    phase2_years: int = 0,
    tapering_g_end: float | None = None,
) -> Callable[[float, int], float]:
    """
    感度分析用の計算関数を生成（v7.1: BS補正・3段階DCF対応）

    Args:
        base_fcf: ベースFCF
        high_growth_rate: 高成長率（Phase1）
        diluted_shares: 希薄化後株式数
        rpo_pv: RPO + 成長オプション現在価値の合計
        alpha: α（成長期待プレミアム）
        terminal_growth: 永続成長率
        net_cash_per_share: BS補正（ネットキャッシュ/株）v7.0追加
        phase2_growth: Phase2成長率（Noneなら2段階DCF）
        phase2_years: Phase2年数（0なら2段階DCF）
        tapering_g_end: Phase1終了成長率（設定時は線形逓減DCFを使用。
            scenarios.py::create_scenario_calc_func()と同じ分岐。2026-10-06）

    Returns:
        calc_func: (wacc, years) -> per_share_value（BS補正込み）
    """
    def calc_func(wacc: float, years: int) -> float:
        # 精密計算のためdcfモジュールを使用
        try:
            if tapering_g_end is not None and high_growth_rate > tapering_g_end:
                # 線形逓減DCF（DCF-1）: メイン理論株価が逓減型の銘柄で中央セルを一致させる
                from .dcf import calculate_tapering_dcf
                result = calculate_tapering_dcf(
                    base_fcf=base_fcf,
                    g_start=high_growth_rate,
                    g_end=tapering_g_end,
                    high_growth_years=years,
                    terminal_growth=terminal_growth,
                    wacc=wacc,
                )
                v0 = result.v0
            elif phase2_growth is not None and phase2_years > 0:
                from .dcf import calculate_three_stage_dcf
                result = calculate_three_stage_dcf(
                    base_fcf=base_fcf,
                    phase1_growth_rate=high_growth_rate,
                    phase1_years=years,
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
                    high_growth_rate=high_growth_rate,
                    high_growth_years=years,
                    terminal_growth=terminal_growth,
                    wacc=wacc,
                )
                v0 = result.v0
        except Exception:
            # フォールバック: シンプル計算
            current_fcf = base_fcf
            pv = 0.0
            t = 0
            for _ in range(years):
                t += 1
                current_fcf *= (1 + high_growth_rate)
                pv += current_fcf / (1 + wacc) ** t
            if phase2_growth is not None and phase2_years > 0:
                for _ in range(phase2_years):
                    t += 1
                    current_fcf *= (1 + phase2_growth)
                    pv += current_fcf / (1 + wacc) ** t
            terminal_fcf = current_fcf * (1 + terminal_growth)
            terminal_value = terminal_fcf / (wacc - terminal_growth) if wacc > terminal_growth else terminal_fcf * 20
            pv += terminal_value / (1 + wacc) ** t
            v0 = pv

        # P_t = V0 × (1 + α) + rpo_pv（αの外出し v9.0修正）
        pt = v0 * (1 + alpha) + rpo_pv

        # 1株あたり + BS補正
        if diluted_shares > 0:
            return pt / diluted_shares + net_cash_per_share
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
