"""
TANUKI VALUATION - Future Values Calculator
将来価値予測

責務: 1〜3年後の理論株価予測
"""

from typing import Dict


def calculate_future_values(
    current_value: float,
    high_growth_rate: float,
    high_growth_years: int,
    terminal_growth: float = 0.03,
    projection_years: int = 3
) -> Dict[str, float]:
    """
    将来価値予測を計算
    
    Args:
        current_value: 現在の理論株価
        high_growth_rate: 高成長期の成長率
        high_growth_years: 高成長期間（年）
        terminal_growth: 永続成長率
        projection_years: 予測年数
    
    Returns:
        {"1年後": float, "2年後": float, "3年後": float, ...}
    
    ロジック:
        - 高成長期間内: high_growth_rateで成長
        - 高成長期間後: terminal_growthで成長
    """
    future_values = {}
    value = current_value
    
    for year in range(1, projection_years + 1):
        # 成長率決定
        if year <= high_growth_years:
            growth_rate = high_growth_rate
        else:
            growth_rate = terminal_growth
        
        # 将来価値計算
        value = value * (1 + growth_rate)
        future_values[f"{year}年後"] = round(value, 2)
    
    return future_values


def calculate_return_metrics(
    current_value: float,
    current_price: float,
    future_values: Dict[str, float]
) -> Dict[str, Dict[str, float]]:
    """
    リターン指標を計算
    
    Args:
        current_value: 現在の理論株価
        current_price: 現在の市場株価
        future_values: 将来価値の辞書
    
    Returns:
        各年の期待リターン情報
    """
    metrics = {}
    
    for period, future_value in future_values.items():
        # 理論価値からの成長率
        value_growth = (future_value / current_value - 1) * 100
        
        # 市場価格からの期待リターン
        if current_price > 0:
            expected_return = (future_value / current_price - 1) * 100
        else:
            expected_return = 0.0
        
        metrics[period] = {
            "future_value": future_value,
            "value_growth_pct": round(value_growth, 1),
            "expected_return_pct": round(expected_return, 1)
        }
    
    return metrics


if __name__ == "__main__":
    print("=== Future Values Calculator テスト ===\n")
    
    # 基本テスト
    future = calculate_future_values(
        current_value=100.0,
        high_growth_rate=0.25,
        high_growth_years=5
    )
    
    print("高成長25%, 5年間:")
    for period, value in future.items():
        print(f"  {period}: ${value:.2f}")
    
    # リターン指標
    metrics = calculate_return_metrics(
        current_value=100.0,
        current_price=80.0,  # 20%割安
        future_values=future
    )
    
    print("\n期待リターン (現在株価$80):")
    for period, m in metrics.items():
        print(f"  {period}: 価値成長{m['value_growth_pct']:.1f}%, 期待リターン{m['expected_return_pct']:.1f}%")
