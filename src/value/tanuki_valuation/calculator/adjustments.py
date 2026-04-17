"""
TANUKI VALUATION - Adjustments
FCF補正、RPO補正、α計算、成長オプション補正、FCFベース判定

責務: 各種調整・補正ロジック

v6.1 追加:
  - GrowthOptionResult / calculate_growth_option_pv()
  - calculate_intrinsic_value() に growth_option_pv 引数追加

v6.2 追加:
  - FCFBaseResult / determine_fcf_base()
    FCFリストのトレンドから5年平均 or 直近2年平均を自動判定
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
    """成長オプション（仮説セグメント）PV計算結果"""
    total_pv: float
    options: List[Dict[str, Any]]
    count: int
    applied: bool

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


@dataclass
class FCFBaseResult:
    """
    FCFベース判定結果 v6.2 新規

    DCFの出発点となるベースFCFと、その選択根拠を保持する。
    """
    base_fcf: float       # 採用したベースFCF
    method: str           # "avg_5yr" | "recent_2yr"
    fcf_5yr_avg: float    # 5年平均（参考値）
    fcf_2yr_avg: float    # 直近2年平均（参考値）
    ratio: float          # recent_2yr / avg_5yr（判定根拠）
    threshold: float      # 判定閾値

    def to_dict(self) -> Dict[str, Any]:
        return {
            "base_fcf": self.base_fcf,
            "method": self.method,
            "fcf_5yr_avg": self.fcf_5yr_avg,
            "fcf_2yr_avg": self.fcf_2yr_avg,
            "ratio": round(self.ratio, 2),
            "threshold": self.threshold
        }


# ========================================
# FCFベース自動判定 v6.2 新規
# ========================================

def determine_fcf_base(
    fcf_5yr_avg: float,
    fcf_2yr_avg: float,
    fcf_list: List[float],
    threshold: float = 1.5
) -> FCFBaseResult:
    """
    FCFトレンドから5年平均 or 直近2年平均を自動判定

    判定ロジック:
        ratio = fcf_2yr_avg / fcf_5yr_avg
        ratio > threshold → recent_2yr（急拡大中）
        ratio ≤ threshold → avg_5yr（安定・成熟）

    特殊ケース:
        fcf_5yr_avg ≤ 0（赤字期間含む） → recent_2yr
        fcf_2yr_avg ≤ 0（直近も赤字）   → avg_5yr（FCF補正に委ねる）
        データ不足（< 2年）              → avg_5yr

    Args:
        fcf_5yr_avg : 5年平均FCF
        fcf_2yr_avg : 直近2年平均FCF
        fcf_list    : FCFリスト（件数チェック用）
        threshold   : 切り替え閾値（デフォルト1.5）

    Returns:
        FCFBaseResult
    """
    # データ不足
    if len(fcf_list) < 2:
        return FCFBaseResult(
            base_fcf=fcf_5yr_avg,
            method="avg_5yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            ratio=0.0,
            threshold=threshold
        )

    # 直近2年も赤字 → avg_5yr（FCF補正ロジックに委ねる）
    if fcf_2yr_avg <= 0:
        return FCFBaseResult(
            base_fcf=fcf_5yr_avg,
            method="avg_5yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            ratio=0.0,
            threshold=threshold
        )

    # 5年平均が0以下（赤字期間含む）→ recent_2yr
    if fcf_5yr_avg <= 0:
        return FCFBaseResult(
            base_fcf=fcf_2yr_avg,
            method="recent_2yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            ratio=0.0,
            threshold=threshold
        )

    # 通常判定
    ratio = fcf_2yr_avg / fcf_5yr_avg

    if ratio > threshold:
        return FCFBaseResult(
            base_fcf=fcf_2yr_avg,
            method="recent_2yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            ratio=ratio,
            threshold=threshold
        )
    else:
        return FCFBaseResult(
            base_fcf=fcf_5yr_avg,
            method="avg_5yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            ratio=ratio,
            threshold=threshold
        )


# ========================================
# 既存関数（変更なし）
# ========================================

def adjust_fcf(
    fcf_avg: float,
    latest_revenue: float,
    revenue_floor_ratio: float = 0.08
) -> FCFAdjustmentResult:
    """FCF補正（マイナスFCF対応）"""
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
    """RPO補正（残存履行義務の現在価値化）"""
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
    """α（成長期待プレミアム）計算"""
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
    """本質的価値（P_t）計算"""
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


def calculate_growth_option_pv(ticker: str) -> GrowthOptionResult:
    """仮説セグメント（成長オプション）の合計PVを計算"""
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
DEFAULT_FCF_BASE_THRESHOLD = 1.5


if __name__ == "__main__":
    print("=== FCFベース自動判定テスト ===\n")

    # AMZN相当（急拡大 → recent_2yr期待）
    r1 = determine_fcf_base(
        fcf_5yr_avg=8_234_200_000,
        fcf_2yr_avg=50_000_000_000,
        fcf_list=[2e9, 3e9, 5e9, 25e9, 75e9]
    )
    print(f"AMZN相当: method={r1.method}  ratio={r1.ratio:.2f}  base=${r1.base_fcf/1e9:.1f}B")

    # MSFT相当（安定 → avg_5yr期待）
    r2 = determine_fcf_base(
        fcf_5yr_avg=65_284_800_000,
        fcf_2yr_avg=70_000_000_000,
        fcf_list=[55e9, 60e9, 65e9, 68e9, 72e9]
    )
    print(f"MSFT相当: method={r2.method}  ratio={r2.ratio:.2f}  base=${r2.base_fcf/1e9:.1f}B")

    # SOFI相当（5年平均マイナス → recent_2yr）
    r3 = determine_fcf_base(
        fcf_5yr_avg=-4_269_811_800,
        fcf_2yr_avg=500_000_000,
        fcf_list=[-3e9, -2e9, -1e9, 0.2e9, 0.8e9]
    )
    print(f"SOFI相当: method={r3.method}  ratio={r3.ratio:.2f}  base=${r3.base_fcf/1e9:.2f}B")
