"""
TANUKI VALUATION - DCF Calculator
2段階 / 3段階 / 線形逓減 DCFモデル

責務: 高成長期 + ターミナル価値の現在価値計算

v6.1 追加:
  - calculate_three_stage_dcf(): 3段階DCF（高成長→移行→ターミナル）
  - ThreeStageDCFResult: 3段階結果データクラス
  既存の calculate_two_stage_dcf() は完全に維持（変更なし）

v9.0 追加（DCF-1）:
  - calculate_tapering_dcf(): Phase1内で成長率を線形逓減させるDCF
  - TaperingDCFResult: 逓減型結果データクラス
  高成長銘柄で「成長の減速」を年次単位で織り込む
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
    # [[DCF-1b]]（2026-09-25）: Phase1をPhase1初年度g→Phase2のgへ線形逓減
    # させたか（provenance）。phase1_growth_pathはPhase1各年の成長率
    phase1_tapered: bool = False
    phase1_growth_path: List[float] = field(default_factory=list)
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
            "phase1_tapered": self.phase1_tapered,
            "phase1_growth_path": self.phase1_growth_path,
            "dcf_type": "three_stage"
        }


def _linear_taper_rate(g_start: float, g_end: float, t: int, n_years: int) -> float:
    """Phase内の線形逓減成長率（t=0でg_start、t=n_years-1でg_end）。
    calculate_tapering_dcf()（DCF-1）とcalculate_three_stage_dcf()の
    Phase1（DCF-1b）で共有する単一の補間式。n_years==1ならg_start。"""
    if n_years == 1:
        return g_start
    return g_start + (g_end - g_start) * t / (n_years - 1)


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
        Phase1: t=1..n1  g(t) = g1 + (g2 - g1) × (t-1)/(n1-1)（線形逓減、[[DCF-1b]]）
                         FCF_t = FCF_{t-1} × (1+g(t)),  PV = FCF_t / (1+WACC)^t
        Phase2: t=n1+1..n1+n2  FCF × (1+g2)^(t-n1) / (1+WACC)^t
        Terminal: FCF_last × (1+g_t) / (WACC - g_t) / (1+WACC)^(n1+n2)

    [[DCF-1b]]（2026-09-25）: Phase1はg1固定ではなく、Phase1初年度g1から
    Phase2のg2へ線形逓減させる（calculate_tapering_dcf()と同一の補間式
    _linear_taper_rate()を共有）。[[DCF-1]]（2026-05-31）は3段階DCFを
    「Phase2で成長減速を既に表現済み」として逓減の適用外としたが、当時の
    Phase1は5年固定だった。ALPHA-REDESIGN-1（2026-06-26）でPhase1年数が
    Moat Score連動（3+round(moat×7)、最大10年）となり、segment_xbrl
    （2026-09-18）の成長率が上限50%に張り付く銘柄では「50%×9年」の複利で
    Phase1終了時FCFが基準の約38倍（NVDA・APP、IV/株÷株価 9.5倍・11.7倍）に
    達し、Phase2だけでは減速を表現できなくなっていたため逓減を導入した。
    n1=1の場合はg1のみ（逓減なし）。
    """
    # ── Phase1: 高成長期（g1→g2へ線形逓減、DCF-1b）──
    current_fcf = base_fcf
    pv_phase1 = 0.0
    phase1_detail = []
    phase1_growth_path = []

    for t in range(phase1_years):
        g_t = _linear_taper_rate(phase1_growth_rate, phase2_growth_rate, t, phase1_years)
        phase1_growth_path.append(g_t)
        current_fcf *= (1 + g_t)
        discount_factor = (1 + wacc) ** (t + 1)
        pv_year = current_fcf / discount_factor
        pv_phase1 += pv_year

        phase1_detail.append({
            "year": t + 1,
            "phase": "phase1",
            "growth_rate": g_t,
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
        terminal_value=terminal_value,
        phase1_tapered=phase1_years > 1 and phase1_growth_rate != phase2_growth_rate,
        phase1_growth_path=phase1_growth_path,
    )


# ========================================
# 線形逓減DCF（v9.0 DCF-1）
# ========================================

@dataclass
class TaperingDCFResult:
    """DCF計算結果（線形逓減型）"""
    v0: float
    pv_high_growth: float
    pv_terminal: float
    high_growth_detail: List[Dict[str, float]]  # 年別詳細（年別成長率含む）
    terminal_fcf: float
    terminal_value: float
    g_start: float   # Phase1開始成長率
    g_end: float     # Phase1終了成長率（業界ベンチマーク）

    def to_dict(self) -> Dict[str, Any]:
        return {
            "v0": self.v0,
            "pv_high_growth": self.pv_high_growth,
            "pv_terminal": self.pv_terminal,
            "high_growth_detail": self.high_growth_detail,
            "terminal_fcf": self.terminal_fcf,
            "terminal_value": self.terminal_value,
            "g_start": round(self.g_start, 4),
            "g_end": round(self.g_end, 4),
            "dcf_type": "tapering",
        }


def calculate_tapering_dcf(
    base_fcf: float,
    g_start: float,
    g_end: float,
    wacc: float,
    high_growth_years: int = 5,
    terminal_growth: float = 0.03,
) -> TaperingDCFResult:
    """
    線形逓減DCF計算（DCF-1）

    Phase1内でg_startからg_endへ年次線形逓減させる。
      Year 1: g_start
      Year N: g_end
      中間: 線形補間

    設計意図:
      高成長率（例: 93%）を5年間固定で使うのではなく、
      Year1=93% → Year5=10%（業界平均）へ逓減させることで
      「成長の減速」を現実的に織り込む。

    Args:
        base_fcf: ベースFCF
        g_start: Phase1開始成長率（推奨成長率 or Phase1成長率）
        g_end: Phase1終了成長率（業界ベンチマーク等）
        wacc: 割引率
        high_growth_years: Phase1年数（デフォルト5年）
        terminal_growth: 永続成長率

    Returns:
        TaperingDCFResult
    """
    current_fcf = base_fcf
    pv_high = 0.0
    high_growth_detail = []

    for t in range(high_growth_years):
        # 線形補間: t=0でg_start、t=N-1でg_end
        g_t = _linear_taper_rate(g_start, g_end, t, high_growth_years)

        current_fcf *= (1 + g_t)
        discount_factor = (1 + wacc) ** (t + 1)
        pv_year = current_fcf / discount_factor
        pv_high += pv_year

        high_growth_detail.append({
            "year": t + 1,
            "growth_rate": round(g_t, 4),
            "fcf": current_fcf,
            "discount_factor": round(discount_factor, 6),
            "pv": pv_year,
        })

    # ターミナル価値（Phase1最終FCF × TV成長率 → 割引）
    terminal_fcf = current_fcf * (1 + terminal_growth)
    if wacc <= terminal_growth:
        terminal_value = terminal_fcf * 20
    else:
        terminal_value = terminal_fcf / (wacc - terminal_growth)
    pv_terminal = terminal_value / (1 + wacc) ** high_growth_years

    v0 = pv_high + pv_terminal

    return TaperingDCFResult(
        v0=v0,
        pv_high_growth=pv_high,
        pv_terminal=pv_terminal,
        high_growth_detail=high_growth_detail,
        terminal_fcf=terminal_fcf,
        terminal_value=terminal_value,
        g_start=g_start,
        g_end=g_end,
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
