import json
from typing import Dict, List, Any, Optional

def get_value_from_period_data(period_data: Dict, tag_key: str) -> Optional[float]:
    """
    期間データ（period_data）から特定のタグの値を取得する（単位正規化付き）
    Args:
        period_data: extract_key_facts で抽出された1四半期分のデータ
        tag_key: タグのキー（例: 'sbc', 'restructuring'）
    Returns:
        Optional[float]: 見つかった場合はUSD換算された値、なければNone
    """
    if tag_key not in period_data:
        return None
    
    value_info = period_data[tag_key]
    if not isinstance(value_info, dict) or 'value' not in value_info:
        return None
    
    value = float(value_info['value'])
    unit = value_info.get('unit', 'USD').lower()
    
    # 単位正規化
    if unit in ['thousands', 'thousand']:
        return value * 1_000
    elif unit in ['millions', 'million']:
        return value * 1_000_000
    elif unit in ['billions', 'billion']:
        return value * 1_000_000_000
    else:
        return value

def detect_adjustments(period_data: Dict, adjustment_config: Dict) -> List[Dict[str, Any]]:
    """
    一つの四半期データに対して調整項目を検出する
    Args:
        period_data: extract_key_facts で抽出された1四半期分のデータ
        adjustment_config: config/adjustment_items.json の内容
    Returns:
        List[Dict]: 検出された調整項目のリスト
    """
    adjustments = []
    
    # period_data から 'net_income' や 'pretax_income' などの基本情報も取得しておく（税効果計算用）
    net_income = get_value_from_period_data(period_data, 'net_income')
    pretax_income = get_value_from_period_data(period_data, 'pretax_income')
    tax_expense = get_value_from_period_data(period_data, 'tax_expense')
    
    # 実効税率の計算（後で使う）
    effective_tax_rate = 0.21  # デフォルト
    if pretax_income and pretax_income != 0 and tax_expense is not None:
        effective_tax_rate = tax_expense / pretax_income
    
    for category in adjustment_config["categories"]:
        for item in category["sub_items"]:
            amount = None
            
            # item に定義された xbrl_tags に対応するキーを period_data から探す
            # 例: item["xbrl_tags"] に ["us-gaap:ShareBasedCompensation"] があれば、キー "sbc" を探す
            # ここでは簡易的に、設定ファイルのタグ名からキーを推測するか、マッピングを設ける
            # 例: "us-gaap:ShareBasedCompensation" -> "sbc"
            
            # 簡易的なマッピング（実際のキー名に合わせて修正が必要）
            key_mapping = {
                "us-gaap:ShareBasedCompensation": "sbc",
                "us-gaap:RestructuringCharges": "restructuring",
                # 他のタグも必要に応じて追加
            }
            
            for tag in item["xbrl_tags"]:
                # タグからキーを取得
                found_key = None
                if tag in key_mapping:
                    found_key = key_mapping[tag]
                else:
                    # タグ名の最後の部分を小文字にしてキーとして使う（例: "RestructuringCharges" -> "restructuringcharges"）
                    # ただし、これは暫定的な対応。実際のキー名と一致するとは限らない。
                    last_part = tag.split(':')[-1].lower()
                    if last_part in period_data:
                        found_key = last_part
                
                if found_key:
                    amount = get_value_from_period_data(period_data, found_key)
                    if amount is not None:
                        # ソースタグを記録
                        source_tag = tag
                        break
            
            if amount is not None and abs(amount) > 1000:  # 小額は無視
                adjustments.append({
                    "item_name": item["item_name"],
                    "amount": abs(amount),  # 絶対値で保存
                    "direction": item["direction"],
                    "pre_tax": item["pre_tax"],
                    "reason": item["reason"],
                    "extracted_from": source_tag,
                    "category": category["category_name"]
                })
    
    return adjustments
