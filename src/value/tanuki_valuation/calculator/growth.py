"""
TANUKI VALUATION - Growth Rate Calculator
成長率決定ロジック

責務: 高成長期間の成長率を決定
優先順位: セグメント加重成長率 > FCF CAGR > デフォルト
"""

from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass


@dataclass
class GrowthResult:
    """成長率計算結果"""
    rate: float
    source: str  # "segment_weighted" | "fcf_cagr" | "default"
    segment_detail: Optional[Dict[str, Any]] = None
    cagr_detail: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "rate": self.rate,
            "source": self.source
        }
        if self.segment_detail:
            result["segment_detail"] = self.segment_detail
        if self.cagr_detail:
            result["cagr_detail"] = self.cagr_detail
        return result


# セグメント設定（segment_config.pyから移行予定）
SEGMENT_CONFIG: Dict[str, Dict[str, Any]] = {
    "NVDA": {
        "enabled": True,
        "fiscal_year": "FY2025",
        "segments": [
            {"name": "Data Center", "weight": 0.88, "growth": 0.40},
            {"name": "Gaming", "weight": 0.10, "growth": 0.05},
            {"name": "Other", "weight": 0.02, "growth": 0.10},
        ]
    },
    "TSLA": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": [
            {"name": "Automotive", "weight": 0.82, "growth": 0.10},
            {"name": "Energy", "weight": 0.10, "growth": 0.35},
            {"name": "Services", "weight": 0.08, "growth": 0.25},
        ]
    },
    "PLTR": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": [
            {"name": "Commercial US", "weight": 0.38, "growth": 0.50},
            {"name": "Commercial International", "weight": 0.18, "growth": 0.30},
            {"name": "Government US", "weight": 0.32, "growth": 0.20},
            {"name": "Government International", "weight": 0.12, "growth": 0.15},
        ]
    },
    "MSFT": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": [
            {"name": "Intelligent Cloud", "weight": 0.44, "growth": 0.22},
            {"name": "Productivity", "weight": 0.32, "growth": 0.12},
            {"name": "More Personal Computing", "weight": 0.24, "growth": 0.05},
        ]
    },
    "AMZN": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": [
            {"name": "AWS", "weight": 0.17, "growth": 0.18},
            {"name": "Online Stores", "weight": 0.40, "growth": 0.08},
            {"name": "Third-Party Seller", "weight": 0.24, "growth": 0.12},
            {"name": "Advertising", "weight": 0.08, "growth": 0.25},
            {"name": "Other", "weight": 0.11, "growth": 0.10},
        ]
    },
    "AMD": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": [
            {"name": "Data Center", "weight": 0.50, "growth": 0.35},
            {"name": "Client", "weight": 0.28, "growth": 0.10},
            {"name": "Gaming", "weight": 0.12, "growth": -0.05},
            {"name": "Embedded", "weight": 0.10, "growth": 0.05},
        ]
    },
    "APP": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": [
            {"name": "Software Platform", "weight": 0.70, "growth": 0.45},
            {"name": "Apps", "weight": 0.30, "growth": 0.05},
        ]
    },
    "CELH": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": [
            {"name": "North America", "weight": 0.95, "growth": 0.25},
            {"name": "International", "weight": 0.05, "growth": 0.50},
        ]
    },
}


def get_segment_growth(ticker: str) -> Optional[GrowthResult]:
    """
    セグメント加重平均成長率を取得
    
    Args:
        ticker: 銘柄コード
    
    Returns:
        GrowthResult or None (設定なし/無効の場合)
    """
    config = SEGMENT_CONFIG.get(ticker)
    if not config or not config.get("enabled"):
        return None
    
    segments = config.get("segments", [])
    if not segments:
        return None
    
    # 加重平均計算
    weighted_growth = sum(
        seg["weight"] * seg["growth"]
        for seg in segments
    )
    
    return GrowthResult(
        rate=weighted_growth,
        source="segment_weighted",
        segment_detail={
            "enabled": True,
            "weighted_growth": weighted_growth,
            "fiscal_year": config.get("fiscal_year"),
            "source": "segment_config",
            "segments": segments
        }
    )


def calculate_fcf_cagr(
    fcf_list: List[float],
    min_periods: int = 2,
    growth_floor: float = 0.15,
    growth_cap: float = 0.50
) -> Optional[GrowthResult]:
    """
    FCF CAGRを計算
    
    Args:
        fcf_list: FCFリスト（時系列順）
        min_periods: 最低期間
        growth_floor: 成長率下限
        growth_cap: 成長率上限
    
    Returns:
        GrowthResult or None (計算不可の場合)
    """
    if len(fcf_list) < 3:
        return None
    
    # 正のFCFのみを対象
    recent_fcfs = [f for f in fcf_list[-5:] if f > 0]
    if len(recent_fcfs) < min_periods:
        return None
    
    # CAGR計算
    start_value = recent_fcfs[0]
    end_value = recent_fcfs[-1]
    periods = len(recent_fcfs) - 1
    
    if start_value <= 0:
        return None
    
    raw_cagr = (end_value / start_value) ** (1 / periods) - 1
    
    # 範囲クリッピング
    clipped_cagr = max(growth_floor, min(growth_cap, raw_cagr))
    
    return GrowthResult(
        rate=clipped_cagr,
        source="fcf_cagr",
        cagr_detail={
            "start_value": start_value,
            "end_value": end_value,
            "periods": periods,
            "raw_cagr": raw_cagr,
            "clipped_cagr": clipped_cagr,
            "floor": growth_floor,
            "cap": growth_cap
        }
    )


def determine_growth_rate(
    ticker: str,
    fcf_list: Optional[List[float]] = None,
    default_rate: float = 0.25
) -> GrowthResult:
    """
    成長率を決定（優先順位付き）
    
    優先順位:
    1. セグメント加重成長率（設定がある場合）
    2. FCF CAGR（計算可能な場合）
    3. デフォルト値
    
    Args:
        ticker: 銘柄コード
        fcf_list: FCFリスト（オプション）
        default_rate: デフォルト成長率
    
    Returns:
        GrowthResult: 決定された成長率
    """
    # 1. セグメント成長率を試行
    segment_result = get_segment_growth(ticker)
    if segment_result:
        return segment_result
    
    # 2. FCF CAGRを試行
    if fcf_list:
        cagr_result = calculate_fcf_cagr(fcf_list)
        if cagr_result:
            return cagr_result
    
    # 3. デフォルト
    return GrowthResult(
        rate=default_rate,
        source="default"
    )


def get_scenario_growth_rates(
    base_rate: float,
    bear_multiplier: float = 0.7,
    bull_multiplier: float = 1.2
) -> Dict[str, float]:
    """
    シナリオ別成長率を計算
    
    Args:
        base_rate: ベース成長率
        bear_multiplier: Bear乗数
        bull_multiplier: Bull乗数
    
    Returns:
        {"bear": float, "base": float, "bull": float}
    """
    return {
        "bear": base_rate * bear_multiplier,
        "base": base_rate,
        "bull": base_rate * bull_multiplier
    }


if __name__ == "__main__":
    print("=== Growth Rate Calculator テスト ===\n")
    
    # ケース1: セグメント設定あり
    result1 = determine_growth_rate("NVDA")
    print(f"NVDA: {result1.rate:.1%} (source: {result1.source})")
    
    # ケース2: FCF CAGRフォールバック
    fcf_list = [100, 150, 200, 280, 350]
    result2 = determine_growth_rate("UNKNOWN", fcf_list)
    print(f"UNKNOWN (with FCF): {result2.rate:.1%} (source: {result2.source})")
    
    # ケース3: デフォルト
    result3 = determine_growth_rate("UNKNOWN")
    print(f"UNKNOWN (no FCF): {result3.rate:.1%} (source: {result3.source})")
    
    # シナリオ成長率
    scenarios = get_scenario_growth_rates(0.324)
    print(f"\nシナリオ成長率 (base=32.4%):")
    print(f"  Bear: {scenarios['bear']:.1%}")
    print(f"  Base: {scenarios['base']:.1%}")
    print(f"  Bull: {scenarios['bull']:.1%}")
