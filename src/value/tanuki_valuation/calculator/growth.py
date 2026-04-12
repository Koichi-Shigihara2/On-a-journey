"""
TANUKI VALUATION - Growth Rate Calculator
成長率決定ロジック

責務: 高成長期間の成長率を決定
優先順位: セグメント加重成長率 > FCF CAGR > デフォルト
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass

# 外部のsegment_config.pyからインポート
try:
    from segment_config import get_segment_growth as _get_segment_growth_from_config
    HAS_SEGMENT_CONFIG = True
except ImportError:
    HAS_SEGMENT_CONFIG = False
    _get_segment_growth_from_config = None


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


def get_segment_growth(ticker: str) -> Optional[GrowthResult]:
    """
    セグメント加重平均成長率を取得
    
    Args:
        ticker: 銘柄コード
    
    Returns:
        GrowthResult or None (設定なし/無効の場合)
    """
    if not HAS_SEGMENT_CONFIG:
        return None
    
    # segment_config.pyのget_segment_growth()を呼び出す
    config = _get_segment_growth_from_config(ticker)
    
    if config is None:
        return None
    
    # segment_config.pyがdictを返す場合の処理
    if isinstance(config, dict):
        if not config.get("enabled"):
            return None
        
        weighted_growth = config.get("weighted_growth")
        if weighted_growth is None:
            return None
        
        return GrowthResult(
            rate=weighted_growth,
            source="segment_weighted",
            segment_detail={
                "enabled": True,
                "weighted_growth": weighted_growth,
                "fiscal_year": config.get("fiscal_year"),
                "source": "segment_config"
            }
        )
    
    # segment_config.pyがfloatを返す場合の処理
    if isinstance(config, (int, float)):
        return GrowthResult(
            rate=float(config),
            source="segment_weighted",
            segment_detail={
                "enabled": True,
                "weighted_growth": float(config),
                "source": "segment_config"
            }
        )
    
    return None


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
    
    print(f"segment_config.py available: {HAS_SEGMENT_CONFIG}")
    
    # ケース1: セグメント設定あり（segment_config.pyが存在する場合）
    result1 = determine_growth_rate("NVDA")
    print(f"NVDA: {result1.rate:.1%} (source: {result1.source})")
    
    # ケース2: FCF CAGRフォールバック
    fcf_list = [100, 150, 200, 280, 350]
    result2 = determine_growth_rate("UNKNOWN", fcf_list)
    print(f"UNKNOWN (with FCF): {result2.rate:.1%} (source: {result2.source})")
    
    # ケース3: デフォルト
    result3 = determine_growth_rate("UNKNOWN")
    print(f"UNKNOWN (no FCF): {result3.rate:.1%} (source: {result3.source})")
