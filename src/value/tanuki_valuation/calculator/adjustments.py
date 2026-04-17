"""
TANUKI VALUATION - Adjustments
FCF補正、RPO補正、α計算、成長オプション補正

責務: 各種調整・補正ロジック

v6.1 追加:
  - GrowthOptionResult: 成長オプションPV結果
  - calculate_growth_option_pv(): 仮説セグメント合計PV計算
  既存関数はすべて変更なし
"""

from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass


@dataclass
class FCFAdjustmentResult:
    """FCF補正結果"""
    adjusted_fcf: float
    original_fcf: float
    floor_applied: float
    method: str  # "none" | "revenue_floor"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "adjusted_fcf": self.adjusted_fcf,
            "original_fcf": self.original_fcf,
            "floor_applied": self.floor_applied,
            "method": self.method
        }


@dataclass
class RPOAdjustmentResult:
    """RPO補正結果"""
    rpo_pv: float
    rpo_raw: float
    discount_rate: float
    assumed_years: float
    applied: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rpo_pv": self.rpo_pv,
            "rpo_raw": self.rpo_raw,
            "discount_rate": self.discount_rate,
            "assumed_realization_years": self.assumed_years,
            "applied": self.applied
        }


@dataclass
class AlphaResult:
    """α計算結果"""
    alpha: float
    alpha_uncapped: float
    was_capped: bool
    roe: float
    retention_rate: float
    wacc: float
    g_individual: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alpha": self.alpha,
            "alpha_uncapped": self.alpha_uncapped,
            "was_capped": self.was_capped,
            "roe": self.roe,
            "retention_rate": self.retention_rate,
            "wacc": self.wacc,
            "g_individual": self.g_individual
        }


@dataclass
class GrowthOptionResult:
    """
    成長オプション（仮説セグメント）PV計算結果 v6.1 新規

    案B: V₀への独立加算項として扱う
    """
    total_pv: float                  # 全仮説セグメントの合計PV
    options: List[Dict[str, Any]]    # 各仮説の詳細（pv, expected_fcf付き）
    count: int                       # 仮説セグメント数
    applied: bool                    # 補正が適用されたか

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_pv": self.total_pv,
            "count": self.count,
            "applied": self.applied,
            "options": [
                {
                    "name": o["name"],
                    "tam": o["tam"],
                    "penetration": o["penetration"],
                    "fcf_margin": o["fcf_margin"],
                    "probability": o["probability"],
                    "delay_years": o["delay_years"],
                    "expected_fcf": o["expected_fcf"],
                    "pv": o["pv"],
                    "note": o.get("note", "")
                }
                for o in self.options
            ]
        }


# ========================================
# 既存関数（変更なし）
# ========================================

def adjust_fcf(
    fcf_avg: float,
    latest_revenue: float,
    revenue_floor_ratio: float = 0.08
) -> FCFAdjustmentResult:
    """
    FCF補正（マイナスFCF対応）

    ロジック:
        FCFがマイナスの場合、売上高 × 8% をフロアとして適用
    """
    if fcf_avg > 0:
        return FCFAdjustmentResult(
            adjusted_fcf=fcf_avg,
            original_fcf=fcf_avg,
            floor_applied=0.0,
            method="none"
        )

    if latest_revenue <= 0:
        return FCFAdjustmentResult(
            adjusted_fcf=fcf_avg,
            original_fcf=fcf_avg,
            floor_applied=0.0,
            method="none"
        )

    fcf_floor = latest_revenue * revenue_floor_ratio
    adjusted_fcf = max(fcf_avg, fcf_floor)
    floor_applied = adjusted_fcf - fcf_avg

    return FCFAdjustmentResult(
        adjusted_fcf=adjusted_fcf,
        original_fcf=fcf_avg,
        floor_applied=floor_applied,
        method="revenue_floor"
    )


def adjust_rpo(
    rpo: float,
    discount_rate: float = 0.15,
    assumed_realization_years: float = 1.5
) -> RPOAdjustmentResult:
    """
    RPO補正（残存履行義務の現在価値化）

    ロジック:
        RPO_PV = RPO / (1 + discount_rate)^assumed_years
    """
    if rpo <= 0:
        return RPOAdjustmentResult(
            rpo_pv=0.0,
            rpo_raw=0.0,
            discount_rate=discount_rate,
            assumed_years=assumed_realization_years,
            applied=False
        )

    rpo_pv = rpo / (1 + discount_rate) ** assumed_realization_years

    return RPOAdjustmentResult(
        rpo_pv=rpo_pv,
        rpo_raw=rpo,
        discount_rate=discount_rate,
        assumed_years=assumed_realization_years,
        applied=True
    )


def calculate_alpha(
    roe: float,
    wacc: float,
    retention_rate: float = 0.60,
    alpha_cap: float = 1.0,
    discount_factor: float = 0.7
) -> AlphaResult:
    """
    α（成長期待プレミアム）計算

    計算式:
        g_individual = ROE × retention_rate
        α = min(alpha_cap, max(0, (g_individual / WACC) × discount_factor))
    """
    g_individual = max(0.0, roe * retention_rate)

    if wacc <= 0:
        alpha_raw = 0.0
    else:
        alpha_raw = (g_individual / wacc) * discount_factor

    alpha_uncapped = max(0.0, alpha_raw)
    alpha = min(alpha_cap, alpha_uncapped)

    return AlphaResult(
        alpha=alpha,
        alpha_uncapped=alpha_uncapped,
        was_capped=alpha_uncapped > alpha_cap,
        roe=roe,
        retention_rate=retention_rate,
        wacc=wacc,
        g_individual=g_individual
    )


def calculate_intrinsic_value(
    v0: float,
    rpo_pv: float,
    alpha: float,
    growth_option_pv: float = 0.0
) -> Tuple[float, float]:
    """
    本質的価値（P_t）計算

    v6.1: growth_option_pv を加算項として追加（案B）

    計算式:
        V_0_adjusted = V_0 + RPO_PV + GrowthOption_PV
        P_t = V_0_adjusted × (1 + α)

    Args:
        v0: DCFによる本質的価値
        rpo_pv: RPOの現在価値
        alpha: 成長期待プレミアム
        growth_option_pv: 成長オプション合計PV（デフォルト0: 既存動作と同一）

    Returns:
        (v0_adjusted, intrinsic_value_pt)
    """
    v0_adjusted = v0 + rpo_pv + growth_option_pv
    intrinsic_value_pt = v0_adjusted * (1 + alpha)

    return v0_adjusted, intrinsic_value_pt


def calculate_per_share_value(
    intrinsic_value_pt: float,
    diluted_shares: int
) -> float:
    """1株あたり本質的価値計算"""
    if diluted_shares <= 0:
        return 0.0
    return intrinsic_value_pt / diluted_shares


def calculate_upside(
    intrinsic_value_per_share: float,
    current_price: float
) -> float:
    """乖離率計算"""
    if current_price <= 0:
        return 0.0
    return ((intrinsic_value_per_share / current_price) - 1) * 100


# ========================================
# 成長オプション補正 v6.1 新規
# ========================================

def calculate_growth_option_pv(ticker: str) -> GrowthOptionResult:
    """
    仮説セグメント（成長オプション）の合計PVを計算

    segment_config.py の GROWTH_OPTIONS を参照する。
    計算式（各仮説セグメント）:
        期待FCF = TAM × 侵入率 × FCFマージン × 確率
        仮説PV  = 期待FCF / (1 + discount_rate)^delay_years

    案B: V₀への独立加算項として core_calculator.py が利用する

    Args:
        ticker: 銘柄コード

    Returns:
        GrowthOptionResult
    """
    try:
        from segment_config import calculate_growth_option_total_pv
        result = calculate_growth_option_total_pv(ticker)
        return GrowthOptionResult(
            total_pv=result["total_pv"],
            options=result["options"],
            count=result["count"],
            applied=result["count"] > 0
        )
    except ImportError:
        return GrowthOptionResult(
            total_pv=0.0,
            options=[],
            count=0,
            applied=False
        )


# デフォルトパラメータ
DEFAULT_RETENTION_RATE = 0.60
DEFAULT_ALPHA_CAP = 1.0
DEFAULT_RPO_DISCOUNT_RATE = 0.15
DEFAULT_FCF_FLOOR_RATIO = 0.08


if __name__ == "__main__":
    print("=== Adjustments テスト ===\n")

    print("1. FCF補正:")
    fcf_result = adjust_fcf(fcf_avg=-1_000_000_000, latest_revenue=5_000_000_000)
    print(f"   Original: ${fcf_result.original_fcf/1e9:.2f}B → Adjusted: ${fcf_result.adjusted_fcf/1e9:.2f}B")

    print("\n2. RPO補正:")
    rpo_result = adjust_rpo(rpo=10_000_000_000)
    print(f"   RPO: ${rpo_result.rpo_raw/1e9:.2f}B → PV: ${rpo_result.rpo_pv/1e9:.2f}B")

    print("\n3. α計算:")
    alpha_result = calculate_alpha(roe=0.45, wacc=0.15)
    print(f"   ROE=45%, WACC=15%: α = {alpha_result.alpha:.3f}")

    print("\n4. 成長オプション補正 (NVDA):")
    go_result = calculate_growth_option_pv("NVDA")
    if go_result.applied:
        print(f"   合計PV: ${go_result.total_pv/1e9:.2f}B  ({go_result.count}件)")
        for opt in go_result.options:
            print(f"   [{opt['name']}] PV=${opt['pv']/1e9:.2f}B")
    else:
        print("   仮説セグメントなし")

    print("\n5. 本質的価値（成長オプション込み）:")
    v0_adj, pt = calculate_intrinsic_value(
        v0=64_700_000_000,
        rpo_pv=8_450_000_000,
        alpha=0.4044,
        growth_option_pv=go_result.total_pv
    )
    print(f"   V0=$64.7B + RPO=$8.45B + GO=${go_result.total_pv/1e9:.2f}B, α=0.4044: P_t = ${pt/1e9:.2f}B")
