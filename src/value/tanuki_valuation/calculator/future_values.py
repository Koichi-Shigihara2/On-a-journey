"""
TANUKI VALUATION - DCF Calculator
2段階DCFモデル

責務: 高成長期 + ターミナル価値の現在価値計算
"""

from typing import Dict, Any, List
from dataclasses import dataclass


@dataclass
class DCFResult:
    """DCF計算結果"""
    v0: float                    # 本質的価値（総額）
    pv_high_growth: float        # 高成長期PV
    pv_terminal: float           # ターミナル価値PV
    high_growth_detail: List[Dict[str, float]]  # 年別詳細
    terminal_fcf: float          # ターミナルFCF
    terminal_value: float        # ターミナル価値（割引前）
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "v0": self.v0,
            "pv_high_growth": self.pv_high_growth,
            "pv_terminal": self.pv_terminal,
            "high_growth_detail": self.high_growth_detail,
            "terminal_fcf": self.terminal_fcf,
            "terminal_value": self.terminal_value
        }


def calculate_two_stage_dcf(
    base_fcf: float,
    high_growth_rate: float,
    wacc: float,
    high_growth_years: int = 5,
    terminal_growth: float = 0.03
) -> DCFResult:
    """
    2段階DCF計算
    
    Args:
        base_fcf: ベースFCF（5年平均など）
        high_growth_rate: 高成長期の成長率
        wacc: 割引率
        high_growth_years: 高成長期間（年）
        terminal_growth: 永続成長率
    
    Returns:
        DCFResult: DCF計算結果
    
    計算式:
        V_0 = Σ(FCF_t / (1+WACC)^t) + TV / (1+WACC)^n
        TV = FCF_n+1 / (WACC - g_terminal)
    """
    # ========================================
    # Phase 1: 高成長期のPV計算
    # ========================================
    current_fcf = base_fcf
    pv_high = 0.0
    high_growth_detail = []
    
    for t in range(high_growth_years):
        # 成長後FCF
        current_fcf *= (1 + high_growth_rate)
        
        # 割引係数
        discount_factor = (1 + wacc) ** (t + 1)
        
        # 現在価値
        pv_year = current_fcf / discount_factor
        pv_high += pv_year
        
        high_growth_detail.append({
            "year": t + 1,
            "fcf": current_fcf,
            "discount_factor": discount_factor,
            "pv": pv_year
        })
    
    # ========================================
    # Phase 2: ターミナル価値計算
    # ========================================
    # ターミナル年のFCF（高成長期最終年から永続成長）
    terminal_fcf = current_fcf * (1 + terminal_growth)
    
    # ターミナル価値（ゴードン成長モデル）
    # TV = FCF_terminal / (WACC - g_terminal)
    if wacc <= terminal_growth:
        # 安全ガード：WACCが永続成長率以下の場合
        terminal_value = terminal_fcf * 20  # 20倍をキャップ
    else:
        terminal_value = terminal_fcf / (wacc - terminal_growth)
    
    # ターミナル価値の現在価値
    pv_terminal = terminal_value / (1 + wacc) ** high_growth_years
    
    # ========================================
    # V_0（本質的価値）
    # ========================================
    v0 = pv_high + pv_terminal
    
    return DCFResult(
        v0=v0,
        pv_high_growth=pv_high,
        pv_terminal=pv_terminal,
        high_growth_detail=high_growth_detail,
        terminal_fcf=terminal_fcf,
        terminal_value=terminal_value
    )


def calculate_dcf_with_varying_wacc(
    base_fcf: float,
    high_growth_rate: float,
    wacc_values: List[float],
    high_growth_years: int = 5,
    terminal_growth: float = 0.03
) -> Dict[float, DCFResult]:
    """
    複数のWACCでDCF計算（感度分析用）
    
    Args:
        base_fcf: ベースFCF
        high_growth_rate: 高成長率
        wacc_values: WACCのリスト
        high_growth_years: 高成長期間
        terminal_growth: 永続成長率
    
    Returns:
        {wacc: DCFResult} の辞書
    """
    results = {}
    for wacc in wacc_values:
        results[wacc] = calculate_two_stage_dcf(
            base_fcf=base_fcf,
            high_growth_rate=high_growth_rate,
            wacc=wacc,
            high_growth_years=high_growth_years,
            terminal_growth=terminal_growth
        )
    return results


def calculate_dcf_with_varying_years(
    base_fcf: float,
    high_growth_rate: float,
    wacc: float,
    years_list: List[int],
    terminal_growth: float = 0.03
) -> Dict[int, DCFResult]:
    """
    複数の高成長期間でDCF計算（感度分析用）
    
    Args:
        base_fcf: ベースFCF
        high_growth_rate: 高成長率
        wacc: WACC
        years_list: 高成長期間のリスト
        terminal_growth: 永続成長率
    
    Returns:
        {years: DCFResult} の辞書
    """
    results = {}
    for years in years_list:
        results[years] = calculate_two_stage_dcf(
            base_fcf=base_fcf,
            high_growth_rate=high_growth_rate,
            wacc=wacc,
            high_growth_years=years,
            terminal_growth=terminal_growth
        )
    return results


# デフォルトパラメータ
DEFAULT_HIGH_GROWTH_YEARS = 5
DEFAULT_TERMINAL_GROWTH = 0.03


if __name__ == "__main__":
    print("=== DCF Calculator テスト ===\n")
    
    # 基本テスト
    result = calculate_two_stage_dcf(
        base_fcf=5_000_000_000,  # $5B
        high_growth_rate=0.25,   # 25%
        wacc=0.10,               # 10%
        high_growth_years=5,
        terminal_growth=0.03
    )
    
    print(f"Base FCF: $5B, Growth: 25%, WACC: 10%")
    print(f"  V_0: ${result.v0 / 1e9:.2f}B")
    print(f"  PV (High Growth): ${result.pv_high_growth / 1e9:.2f}B")
    print(f"  PV (Terminal): ${result.pv_terminal / 1e9:.2f}B")
    print(f"\n  年別詳細:")
    for detail in result.high_growth_detail:
        print(f"    Year {detail['year']}: FCF ${detail['fcf']/1e9:.2f}B → PV ${detail['pv']/1e9:.2f}B")
    
    # WACC感度分析
    print(f"\n=== WACC感度分析 ===")
    wacc_results = calculate_dcf_with_varying_wacc(
        base_fcf=5_000_000_000,
        high_growth_rate=0.25,
        wacc_values=[0.08, 0.10, 0.12]
    )
    for wacc, res in wacc_results.items():
        print(f"  WACC {wacc:.0%}: V_0 = ${res.v0 / 1e9:.2f}B")
