"""
TANUKI VALUATION - Adjustments
FCF補正、RPO補正、α計算

責務: 各種調整・補正ロジック
"""

from typing import Dict, Any, Optional, Tuple
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


def adjust_fcf(
    fcf_avg: float,
    latest_revenue: float,
    revenue_floor_ratio: float = 0.08
) -> FCFAdjustmentResult:
    """
    FCF補正（マイナスFCF対応）
    
    Args:
        fcf_avg: 平均FCF
        latest_revenue: 直近売上高
        revenue_floor_ratio: 売上高に対するFCFフロア比率
    
    Returns:
        FCFAdjustmentResult: 補正結果
    
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
        # 売上高もない場合は補正不可
        return FCFAdjustmentResult(
            adjusted_fcf=fcf_avg,
            original_fcf=fcf_avg,
            floor_applied=0.0,
            method="none"
        )
    
    # フロア適用
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
    
    Args:
        rpo: 残存履行義務（生値）
        discount_rate: 割引率
        assumed_realization_years: 想定実現期間
    
    Returns:
        RPOAdjustmentResult: 補正結果
    
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
    
    Args:
        roe: ROE（10年平均など）
        wacc: WACC
        retention_rate: 内部留保率
        alpha_cap: αの上限
        discount_factor: 割引係数（保守性調整）
    
    Returns:
        AlphaResult: α計算結果
    
    計算式:
        g_individual = ROE × retention_rate
        α = min(alpha_cap, max(0, (g_individual / WACC) × discount_factor))
    """
    # 個別成長率
    g_individual = max(0.0, roe * retention_rate)
    
    # α計算（WACCが0の場合の安全ガード）
    if wacc <= 0:
        alpha_raw = 0.0
    else:
        alpha_raw = (g_individual / wacc) * discount_factor
    
    # 範囲制限
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
    alpha: float
) -> Tuple[float, float]:
    """
    本質的価値（P_t）計算
    
    Args:
        v0: DCFによる本質的価値
        rpo_pv: RPOの現在価値
        alpha: 成長期待プレミアム
    
    Returns:
        (v0_adjusted, intrinsic_value_pt)
    
    計算式:
        V_0_adjusted = V_0 + RPO_PV
        P_t = V_0_adjusted × (1 + α)
    """
    v0_adjusted = v0 + rpo_pv
    intrinsic_value_pt = v0_adjusted * (1 + alpha)
    
    return v0_adjusted, intrinsic_value_pt


def calculate_per_share_value(
    intrinsic_value_pt: float,
    diluted_shares: int
) -> float:
    """
    1株あたり本質的価値計算
    
    Args:
        intrinsic_value_pt: 本質的価値（総額）
        diluted_shares: 希薄化後株式数
    
    Returns:
        1株あたり本質的価値
    """
    if diluted_shares <= 0:
        return 0.0
    return intrinsic_value_pt / diluted_shares


def calculate_upside(
    intrinsic_value_per_share: float,
    current_price: float
) -> float:
    """
    乖離率計算
    
    Args:
        intrinsic_value_per_share: 1株あたり本質的価値
        current_price: 現在株価
    
    Returns:
        乖離率（%）
    """
    if current_price <= 0:
        return 0.0
    return ((intrinsic_value_per_share / current_price) - 1) * 100


# デフォルトパラメータ
DEFAULT_RETENTION_RATE = 0.60
DEFAULT_ALPHA_CAP = 1.0
DEFAULT_RPO_DISCOUNT_RATE = 0.15
DEFAULT_FCF_FLOOR_RATIO = 0.08


if __name__ == "__main__":
    print("=== Adjustments テスト ===\n")
    
    # FCF補正テスト
    print("1. FCF補正:")
    fcf_result = adjust_fcf(fcf_avg=-1_000_000_000, latest_revenue=5_000_000_000)
    print(f"   Original: ${fcf_result.original_fcf/1e9:.2f}B → Adjusted: ${fcf_result.adjusted_fcf/1e9:.2f}B")
    
    # RPO補正テスト
    print("\n2. RPO補正:")
    rpo_result = adjust_rpo(rpo=10_000_000_000)
    print(f"   RPO: ${rpo_result.rpo_raw/1e9:.2f}B → PV: ${rpo_result.rpo_pv/1e9:.2f}B")
    
    # α計算テスト
    print("\n3. α計算:")
    alpha_result = calculate_alpha(roe=0.45, wacc=0.15)
    print(f"   ROE=45%, WACC=15%: α = {alpha_result.alpha:.3f} (capped: {alpha_result.was_capped})")
    
    alpha_result2 = calculate_alpha(roe=0.15, wacc=0.12)
    print(f"   ROE=15%, WACC=12%: α = {alpha_result2.alpha:.3f} (capped: {alpha_result2.was_capped})")
    
    # 本質的価値計算
    print("\n4. 本質的価値:")
    v0_adj, pt = calculate_intrinsic_value(v0=100_000_000_000, rpo_pv=10_000_000_000, alpha=0.5)
    print(f"   V0=$100B + RPO=$10B, α=0.5: P_t = ${pt/1e9:.2f}B")
