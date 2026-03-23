"""
tax_adjuster.py
税効果適用モジュール
- 検出された調整項目（税前/税後）に実効税率を適用
- 各項目に net_amount を追加
- 純額合計と詳細リストを返す
"""
from typing import Dict, List, Any, Tuple

def apply_tax_adjustments(adjustments: List[Dict[str, Any]], period_data: Dict[str, Any]) -> Tuple[float, List[Dict[str, Any]]]:
    """
    調整項目に税効果を適用する
    Args:
        adjustments: detect_adjustments で得られた調整項目リスト
        period_data: 当該四半期のデータ（実効税率を取得するために使用）
    Returns:
        Tuple[float, List[Dict]]:
            - net_adjustment_total: 税効果適用後の調整額合計（純利益への加算額）
            - detailed: 税効果適用後の詳細リスト（各項目に net_amount を追加）
    """
    tax_rate = 0.21  # デフォルト税率
    
    pretax_val = period_data.get('pretax_income', 0.0)
    tax_val = period_data.get('tax_expense', 0.0)
    
    print(f"      DEBUG: pretax={pretax_val:,.0f}, tax={tax_val:,.0f}")
    
    if pretax_val != 0:
        computed_rate = abs(tax_val / pretax_val)
        if 0.0 <= computed_rate <= 0.5:
            tax_rate = computed_rate
            print(f"      Using computed tax rate: {tax_rate:.2%}")
        else:
            print(f"      Computed tax rate {computed_rate:.2%} out of range, using default 21%")
    else:
        print(f"      Pretax income zero, using default tax rate 21%")
    
    detailed = []
    net_total = 0.0
    
    for adj in adjustments:
        new_adj = adj.copy()
        
        amount = adj['amount']
        unit = adj.get('unit', 'USD')
        pre_tax = adj['pre_tax']
        
        if pre_tax:
            net_amount = amount * (1 - tax_rate)
        else:
            net_amount = amount
        
        new_adj['net_amount'] = net_amount
        new_adj['tax_rate_applied'] = tax_rate if pre_tax else 0.0
        
        detailed.append(new_adj)
        net_total += net_amount
    
    return net_total, detailed