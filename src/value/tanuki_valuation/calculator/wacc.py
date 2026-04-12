"""
TANUKI VALUATION - WACC Calculator
CAPM: WACC = Rf + β × (Rm - Rf)

責務: 動的WACC計算（CAPM）
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class WACCResult:
    """WACC計算結果"""
    value: float
    beta: float
    risk_free_rate: float
    market_return: float
    method: str = "CAPM"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "beta": self.beta,
            "risk_free_rate": self.risk_free_rate,
            "market_return": self.market_return,
            "method": self.method
        }


# セクター別デフォルトβ
SECTOR_DEFAULT_BETA: Dict[str, float] = {
    "Technology": 1.20,
    "Consumer Cyclical": 1.10,
    "Consumer Defensive": 0.80,
    "Financial Services": 1.30,
    "Healthcare": 0.90,
    "Communication Services": 1.15,
    "Industrials": 1.05,
    "Energy": 1.10,
    "Utilities": 0.60,
    "Real Estate": 0.85,
    "Basic Materials": 1.00,
}


def get_default_beta(sector: Optional[str]) -> float:
    """セクターに基づくデフォルトβを取得"""
    if sector and sector in SECTOR_DEFAULT_BETA:
        return SECTOR_DEFAULT_BETA[sector]
    return 1.00  # 汎用デフォルト


def calculate_wacc(
    beta: Optional[float] = None,
    sector: Optional[str] = None,
    risk_free_rate: float = 0.043,
    market_return: float = 0.10,
) -> WACCResult:
    """
    CAPMに基づくWACC計算
    
    Args:
        beta: yfinanceから取得したβ（None可）
        sector: セクター名（βがNoneの場合のフォールバック用）
        risk_free_rate: リスクフリーレート（デフォルト: 4.3% = 10年国債利回り）
        market_return: 市場期待リターン（デフォルト: 10%）
    
    Returns:
        WACCResult: WACC計算結果
    
    計算式:
        WACC = Rf + β × (Rm - Rf)
        
    例:
        β = 1.5, Rf = 4.3%, Rm = 10%
        WACC = 4.3% + 1.5 × (10% - 4.3%) = 4.3% + 8.55% = 12.85%
    """
    # βの決定
    if beta is not None and beta > 0:
        used_beta = beta
        beta_source = "provided"
    else:
        used_beta = get_default_beta(sector)
        beta_source = f"sector_default ({sector or 'unknown'})"
    
    # CAPM計算
    equity_risk_premium = market_return - risk_free_rate
    wacc = risk_free_rate + used_beta * equity_risk_premium
    
    # 下限・上限（現実的範囲）
    wacc = max(0.06, min(0.25, wacc))  # 6% - 25%
    
    return WACCResult(
        value=wacc,
        beta=used_beta,
        risk_free_rate=risk_free_rate,
        market_return=market_return,
        method="CAPM"
    )


# 定数（互換性維持）
DEFAULT_RISK_FREE_RATE = 0.043
DEFAULT_MARKET_RETURN = 0.10


if __name__ == "__main__":
    # テスト
    print("=== WACC Calculator テスト ===\n")
    
    # ケース1: βあり
    result1 = calculate_wacc(beta=1.92, sector="Technology")
    print(f"TSLA (β=1.92): WACC = {result1.value:.2%}")
    
    # ケース2: βなし、セクターあり
    result2 = calculate_wacc(beta=None, sector="Technology")
    print(f"Unknown Tech (β=None): WACC = {result2.value:.2%} (default β={result2.beta})")
    
    # ケース3: βなし、セクターなし
    result3 = calculate_wacc(beta=None, sector=None)
    print(f"Unknown (β=None, sector=None): WACC = {result3.value:.2%} (default β={result3.beta})")
    
    # ケース4: 低β
    result4 = calculate_wacc(beta=0.60, sector="Utilities")
    print(f"Utility (β=0.60): WACC = {result4.value:.2%}")
