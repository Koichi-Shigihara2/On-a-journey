"""
調整項目検出モジュール
- 期間データから調整項目を検出
- セクター別デフォルト設定の適用
"""
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
    
    if unit in ['thousands', 'thousand']:
        return value * 1_000
    elif unit in ['millions', 'million']:
        return value * 1_000_000
    elif unit in ['billions', 'billion']:
        return value * 1_000_000_000
    else:
        return value

def detect_adjustments(period_data: Dict, adjustment_config: Dict, 
                       sector: Optional[str] = None, 
                       sector_exclusions: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
    """
    一つの四半期データに対して調整項目を検出する
    Args:
        period_data: extract_key_facts で抽出された1四半期分のデータ
        adjustment_config: config/adjustment_items_v2.json の内容
        sector: セクター名（任意）
        sector_exclusions: セクター別デフォルト除外項目リスト
    Returns:
        List[Dict]: 検出された調整項目のリスト
    """
    adjustments = []
    sector_exclusions = sector_exclusions or []
    
    # タグとキーのマッピング（設定ファイルのxbrl_tagsとperiod_dataのキーを紐付け）
    tag_to_key = {
        "us-gaap:ShareBasedCompensation": "sbc",
        "us-gaap:RestructuringCharges": "restructuring",
        "us-gaap:AmortizationOfIntangibleAssets": "amortization",
        "us-gaap:BusinessCombinationIntegrationCosts": "ma_integration",
        "us-gaap:InventoryWriteDown": "inventory_writeoff",
        "us-gaap:ProvisionForLoanLosses": "loan_loss_provision",
        "us-gaap:OtherComprehensiveIncome": "crypto_fair_value",
        "us-gaap:ImpairmentOfIntangibleAssets": "impairment"
    }
    
    for category in adjustment_config["categories"]:
        for item in category["sub_items"]:
            item_id = item.get('item_id')
            amount = None
            source_tag = None
            
            # xbrl_tags を順に試す
            for tag in item.get("xbrl_tags", []):
                key = tag_to_key.get(tag, tag.split(':')[-1].lower())
                if key in period_data:
                    amount = get_value_from_period_data(period_data, key)
                    if amount is not None:
                        source_tag = tag
                        break
            
            if amount is not None and abs(amount) > 1000:
                adj_item = {
                    "item_id": item_id,
                    "item_name": item["item_name"],
                    "amount": abs(amount),
                    "direction": item["direction"],
                    "pre_tax": item["pre_tax"],
                    "reason": item.get("reason_default", ""),
                    "criteria": item.get("criteria", []),
                    "extracted_from": source_tag,
                    "category": category["category_name"]
                }
                
                # セクターデフォルトかどうかをマーク
                if sector and any(ex.get('item_id') == item_id for ex in sector_exclusions):
                    adj_item["sector_default"] = True
                    # セクター固有の理由があれば上書き
                    for ex in sector_exclusions:
                        if ex.get('item_id') == item_id and ex.get('reason'):
                            adj_item["reason"] = ex['reason']
                            break
                
                # 3軸判定の充足数を計算
                criteria_met = len(adj_item["criteria"])
                adj_item["criteria_score"] = criteria_met
                adj_item["exclude_priority"] = "high" if criteria_met >= 2 else "medium" if criteria_met == 1 else "low"
                
                adjustments.append(adj_item)
    
    return adjustments
