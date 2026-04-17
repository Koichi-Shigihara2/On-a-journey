"""
TANUKI VALUATION - DCF Calculator
2段階 / 3段階 DCFモデル

責務: 高成長期 + ターミナル価値の現在価値計算

v6.1 追加:
  - calculate_three_stage_dcf(): 3段階DCF（高成長→移行→ターミナル）
  - ThreeStageDCFResult: 3段階結果データクラス
  既存の calculate_two_stage_dcf() は完全に維持（変更なし）
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field


@dataclass
class DCFResult:
    """DCF計算結果（2段階）"""
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


@dataclass
class ThreeStageDCFResult:
    """DCF計算結果（3段階）"""
    v0: float                          # 本質的価値（総額）
    pv_phase1: float                   # Phase1（高成長）PV
    pv_phase2: float                   # Phase2（移行）PV
    pv_terminal: float                 # ターミナル価値PV
    phase1_detail: List[Dict[str, float]]  # Phase1 年別詳細
    phase2_detail: List[Dict[str, float]]  # Phase2 年別詳細
    terminal_fcf: float
    terminal_value: float
    # 2段階との比較用（後方互換）
    pv_high_growth: float = field(init=False)

    def __post_init__(self):
        # 2段階との互換性のため pv_phase1 + pv_phase2 を pv_high_growth として提供
        self.pv_high_growth = self.pv_phase1 + self.pv_phase2

    def to_dict(self) -> Dict[str, Any]:
        return {
            "v0": self.v0,
            "pv_phase1": self.pv_phase1,
            "pv_phase2": self.pv_phase2,
            "pv_high_growth": self.pv_high_growth,
            "pv_terminal": self.pv_terminal,
            "phase1_detail": self.phase1_detail,
            "phase2_detail": self.phase2_detail,
            "terminal_fcf": self.terminal_fcf,
            "terminal_value": self.terminal_value,
            "dcf_type": "three_stage"
        }


# ========================================
# 既存2段階DCF（変更なし）
# ========================================

def calculate_two_stage_dcf(
    base_fcf: float,
    high_growth_rate: float,
    wacc: float,
    high_growth_years: int = 5,
    terminal_growth: float = 0.03
) -> DCFResult:
    """
    2段階DCF計算（既存モデル・変更なし）

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
    # Phase 1: 高成長期のPV計算
    current_fcf = base_fcf
    pv_high = 0.0
    high_growth_detail = []

    for t in range(high_growth_years):
        current_fcf *= (1 + high_growth_rate)
        discount_factor = (1 + wacc) ** (t + 1)
        pv_year = current_fcf / discount_factor
        pv_high += pv_year

        high_growth_detail.append({
            "year": t + 1,
            "fcf": current_fcf,
            "discount_factor": discount_factor,
            "pv": pv_year
        })

    # Phase 2: ターミナル価値計算
    terminal_fcf = current_fcf * (1 + terminal_growth)

    if wacc <= terminal_growth:
        terminal_value = terminal_fcf * 20
    else:
        terminal_value = terminal_fcf / (wacc - terminal_growth)

    pv_terminal = terminal_value / (1 + wacc) ** high_growth_years

    v0 = pv_high + pv_terminal

    return DCFResult(
        v0=v0,
        pv_high_growth=pv_high,
        pv_terminal=pv_terminal,
        high_growth_detail=high_growth_detail,
        terminal_fcf=terminal_fcf,
        terminal_value=terminal_value
    )


# ========================================
# 3段階DCF（v6.1 新規追加）
# ========================================

def calculate_three_stage_dcf(
    base_fcf: float,
    phase1_growth_rate: float,
    phase2_growth_rate: float,
    wacc: float,
    phase1_years: int = 5,
    phase2_years: int = 5,
    terminal_growth: float = 0.03
) -> ThreeStageDCFResult:
    """
    3段階DCF計算

    高成長期（Phase1）→ 移行期（Phase2）→ ターミナル（永続）の
    3段階で成長鈍化を段階的に表現する。

    Args:
        base_fcf: ベースFCF
        phase1_growth_rate: Phase1（高成長）成長率
        phase2_growth_rate: Phase2（移行）成長率
        wacc: 割引率
        phase1_years: Phase1の年数
        phase2_years: Phase2の年数
        terminal_growth: 永続成長率

    Returns:
        ThreeStageDCFResult

    計算式:
        Phase1: t=1..n1  FCF × (1+g1)^t / (1+WACC)^t
        Phase2: t=n1+1..n1+n2  FCF × (1+g2)^(t-n1) / (1+WACC)^t
        Terminal: FCF_last × (1+g_t) / (WACC - g_t) / (1+WACC)^(n1+n2)
    """
    # ── Phase1: 高成長期 ──
    current_fcf = base_fcf
    pv_phase1 = 0.0
    phase1_detail = []

    for t in range(phase1_years):
        current_fcf *= (1 + phase1_growth_rate)
        discount_factor = (1 + wacc) ** (t + 1)
        pv_year = current_fcf / discount_factor
        pv_phase1 += pv_year

        phase1_detail.append({
            "year": t + 1,
            "phase": "phase1",
            "growth_rate": phase1_growth_rate,
            "fcf": current_fcf,
            "discount_factor": discount_factor,
            "pv": pv_year
        })

    # ── Phase2: 移行期 ──
    pv_phase2 = 0.0
    phase2_detail = []
    total_years_so_far = phase1_years

    for t in range(phase2_years):
        current_fcf *= (1 + phase2_growth_rate)
        abs_year = total_years_so_far + t + 1
        discount_factor = (1 + wacc) ** abs_year
        pv_year = current_fcf / discount_factor
        pv_phase2 += pv_year

        phase2_detail.append({
            "year": abs_year,
            "phase": "phase2",
            "growth_rate": phase2_growth_rate,
            "fcf": current_fcf,
            "discount_factor": discount_factor,
            "pv": pv_year
        })

    # ── Terminal: 永続成長 ──
    total_years = phase1_years + phase2_years
    terminal_fcf = current_fcf * (1 + terminal_growth)

    if wacc <= terminal_growth:
        terminal_value = terminal_fcf * 20
    else:
        terminal_value = terminal_fcf / (wacc - terminal_growth)

    pv_terminal = terminal_value / (1 + wacc) ** total_years

    v0 = pv_phase1 + pv_phase2 + pv_terminal

    return ThreeStageDCFResult(
        v0=v0,
        pv_phase1=pv_phase1,
        pv_phase2=pv_phase2,
        pv_terminal=pv_terminal,
        phase1_detail=phase1_detail,
        phase2_detail=phase2_detail,
        terminal_fcf=terminal_fcf,
        terminal_value=terminal_value
    )


# ========================================
# 感度分析用（既存・変更なし）
# ========================================

def calculate_dcf_with_varying_wacc(
    base_fcf: float,
    high_growth_rate: float,
    wacc_values: List[float],
    high_growth_years: int = 5,
    terminal_growth: float = 0.03
) -> Dict[float, DCFResult]:
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

    # 2段階（既存）
    r2 = calculate_two_stage_dcf(
        base_fcf=5_000_000_000,
        high_growth_rate=0.40,
        wacc=0.152,
        high_growth_years=5,
        terminal_growth=0.03
    )
    print(f"[2段階] V_0: ${r2.v0/1e9:.2f}B  PV_high: ${r2.pv_high_growth/1e9:.2f}B  PV_tv: ${r2.pv_terminal/1e9:.2f}B")

    # 3段階（新規）
    r3 = calculate_three_stage_dcf(
        base_fcf=5_000_000_000,
        phase1_growth_rate=0.40,
        phase2_growth_rate=0.15,
        wacc=0.152,
        phase1_years=5,
        phase2_years=5,
        terminal_growth=0.03
    )
    print(f"[3段階] V_0: ${r3.v0/1e9:.2f}B  PV_p1: ${r3.pv_phase1/1e9:.2f}B  PV_p2: ${r3.pv_phase2/1e9:.2f}B  PV_tv: ${r3.pv_terminal/1e9:.2f}B")
    print(f"  → 2段階比: +${(r3.v0 - r2.v0)/1e9:.2f}B ({(r3.v0/r2.v0-1)*100:+.1f}%)")
