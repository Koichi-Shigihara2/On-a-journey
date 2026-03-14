import json
from typing import Dict, List, Any

def detect_adjustments(raw_facts: Dict[str, float], config: Dict) -> List[Dict[str, Any]]:
    """
    Configに基づいて調整項目を検出
    """
    adjustments = []
    
    for category in config["categories"]:
        for item in category["sub_items"]:
            amount = 0
            source_tag = None
            
            # XBRLタグで検索
            for tag in item["xbrl_tags"]:
                # タグ名をシンプルなキーに変換（us-gaap:RestructuringCharges → restructuring）
                simple_key = tag.split(":")[-1].lower()
                if simple_key in raw_facts:
                    amount = raw_facts[simple_key]
                    source_tag = tag
                    break
                
                # 完全一致で検索
                if tag in raw_facts:
                    amount = raw_facts[tag]
                    source_tag = tag
                    break
            
            # 金額があれば調整項目として追加
            if amount and abs(amount) > 1000:  # 小額は無視
                adjustments.append({
                    "item_name": item["item_name"],
                    "amount": abs(amount),  # 絶対値で保存
                    "direction": item["direction"],
                    "pre_tax": item["pre_tax"],
                    "reason": item["reason"],
                    "extracted_from": source_tag or "manual",
                    "category": category["category_name"]
                })
    
    return adjustments
