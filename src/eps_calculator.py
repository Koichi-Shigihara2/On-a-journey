"""
eps_calculator.py
EPS計算モジュール
- GAAP EPS と Adjusted EPS を計算
- 調整後純利益 = GAAP純利益 + 税効果適用後の調整額合計
- 分母には希薄化後株式数を使用
"""
from typing import Dict, Any, List
from extract_key_facts import normalize_value

def calculate_eps(period_data: Dict[str, Any], net_adjustment: float, adjustments_detail: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    EPSを計算する
    Args:
        period_data: 当該四半期のデータ（normalize_value で各値を取得）
        net_adjustment: 税効果適用後の調整額合計（純利益への加算額）
        adjustments_detail: 税効果適用後の詳細リスト（各項目に net_amount を含む）
    Returns:
        Dict: 以下のキーを含む辞書
            - gaap_eps: float
            - adjusted_eps: float
            - gaap_net_income: float (正規化済み)
            - adjusted_net_income: float
            - diluted_shares_used: float
            - adjustments: List[Dict] (adjustments_detail をそのまま)
            - net_adjustment_total: float (net_adjustment と同じ)
    """
    # 正規化された値を取得
    gaap_net_income = normalize_value(period_data.get('net_income'))
    diluted_shares = normalize_value(period_data.get('diluted_shares'))
    
    if diluted_shares == 0:
        # エラー処理：株式数が0の場合は0を返す
        print(f"Warning: diluted_shares is 0 for {period_data.get('filing_date', 'unknown')}")
        gaap_eps = 0.0
        adjusted_eps = 0.0
    else:
        gaap_eps = gaap_net_income / diluted_shares
        adjusted_net_income = gaap_net_income + net_adjustment
        adjusted_eps = adjusted_net_income / diluted_shares
    
    result = {
        "gaap_eps": gaap_eps,
        "adjusted_eps": adjusted_eps,
        "gaap_net_income": gaap_net_income,
        "adjusted_net_income": gaap_net_income + net_adjustment,
        "diluted_shares_used": diluted_shares,
        "adjustments": adjustments_detail,
        "net_adjustment_total": net_adjustment
    }
    return result

# テスト用
if __name__ == "__main__":
    # 簡易テスト
    sample_period = {
        "filing_date": "2025-03-31",
        "net_income": {"value": 214031000, "unit": "USD"},
        "diluted_shares": {"value": 2552818000, "unit": "shares"}
    }
    sample_adjustments = [
        {
            "item_name": "株式報酬費用",
            "amount": 155339000,
            "unit": "USD",
            "net_amount": 122717810,  # 税効果適用後
            "category": "株式報酬 (SBC)"
        }
    ]
    net_adj = 122717810
    
    result = calculate_eps(sample_period, net_adj, sample_adjustments)
    print(f"GAAP EPS: {result['gaap_eps']:.4f}")
    print(f"Adjusted EPS: {result['adjusted_eps']:.4f}")
    print(f"Adjustments count: {len(result['adjustments'])}")
