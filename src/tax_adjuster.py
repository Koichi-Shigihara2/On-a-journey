"""
tax_adjuster.py
税効果適用モジュール
- 検出された調整項目（税前/税後）に実効税率を適用
- 各項目に net_amount を追加
- 純額合計と詳細リストを返す
"""
from typing import Dict, List, Any, Tuple
#　from extract_key_facts import normalize_value

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
    # period_data から税引前利益と税費用を取得して実効税率を計算
    tax_rate = 0.21  # デフォルト税率
    
    pretax_val = normalize_value(period_data.get('pretax_income'))
    tax_val = normalize_value(period_data.get('tax_expense'))
    
    print(f"      DEBUG: pretax={pretax_val:,.0f}, tax={tax_val:,.0f}")
    
    if pretax_val != 0:
        # 実効税率 = 税費用 / 税引前利益（絶対値で計算、赤字の場合は便宜上絶対値で割る）
        computed_rate = abs(tax_val / pretax_val)
        # 常識的な範囲内かチェック（0%〜50%）
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
        # 単位情報を保持したままコピー
        new_adj = adj.copy()
        
        amount = adj['amount']
        unit = adj.get('unit', 'USD')
        pre_tax = adj['pre_tax']
        
        if pre_tax:
            # 税前項目 → 税効果適用
            net_amount = amount * (1 - tax_rate)
        else:
            # 税後項目 → そのまま
            net_amount = amount
        
        new_adj['net_amount'] = net_amount
        new_adj['tax_rate_applied'] = tax_rate if pre_tax else 0.0
        
        detailed.append(new_adj)
        net_total += net_amount
    
    return net_total, detailed
