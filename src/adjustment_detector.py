"""
adjustment_detector.py
調整項目検出モジュール
- adjustment_items.json（カテゴリ構造）を読み込み、フラットな項目リストに変換
- period_data から各項目の xbrl_tags に該当するタグを探し、値があれば調整項目として抽出
"""
import json
from pathlib import Path
from typing import Dict, List, Any, Optional

# 設定ファイルのパス
CONFIG_DIR = Path(__file__).parent.parent / "config"
ADJUSTMENT_ITEMS_PATH = CONFIG_DIR / "adjustment_items.json"

# キャッシュ用（複数回呼ばれることを考慮）
_items_config_cache = None

def load_adjustment_items() -> List[Dict[str, Any]]:
    """
    adjustment_items.json を読み込み、フラットな項目リストを返す
    各項目には category フィールドが追加される
    """
    global _items_config_cache
    if _items_config_cache is not None:
        return _items_config_cache

    try:
        with open(ADJUSTMENT_ITEMS_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Warning: {ADJUSTMENT_ITEMS_PATH} not found. Using empty list.")
        return []
    
    items = []
    categories = config.get("categories", [])
    for cat in categories:
        category_name = cat.get("category_name", "その他")
        for sub in cat.get("sub_items", []):
            item = sub.copy()
            item["category"] = category_name
            items.append(item)
    
    _items_config_cache = items
    return items


# sector item_id → adjustment_items.json の item_name へのマッピング
SECTOR_ITEM_ID_TO_NAME = {
    'sbc':                        '株式報酬費用',
    'amortization_intangibles':   '買収無形資産償却',
    'inventory_writeoff':         '在庫評価損',
    'ma_integration':             '買収関連費用',
    'iprd_amortization':          '買収無形資産償却',
    # 以下はadjustment_items.jsonに未定義（将来追加時に対応）
    'depreciation':               None,
    'asset_sale_gain':            None,
    'logistics_one_time':         None,
    'gov_subsidy_timing':         None,
    'loan_fair_value':            None,
    'loan_loss_provision_abnormal': None,
    'milestone_rd':               None,
    'crypto_fair_value':          None,
    'impairment_legacy':          None,
}

def detect_adjustments(
    period_data: Dict[str, Any],
    adjustment_config: Optional[Dict] = None,
    sector: Optional[str] = None,
    sector_exclusions: Optional[List[Dict]] = None
) -> List[Dict[str, Any]]:
    """
    period_data から調整項目を検出する
    Args:
        period_data: 1四半期分のデータ（extract_quarterly_facts の戻り値の要素）
        adjustment_config: 互換性のため残しているが、使用しない
        sector: セクター名（pipeline.py から渡される）
        sector_exclusions: セクター別除外項目リスト（各要素は item_id キーを持つ dict）
    Returns:
        List[Dict]: 検出された調整項目のリスト
        各要素は以下の形式：
        {
            "item_name": str,
            "amount": float,          # 生の値（単位未正規化）
            "unit": str,               # 単位
            "direction": str,          # "add_back" など
            "pre_tax": bool,
            "reason": str,
            "extracted_from": str,     # 実際に使われたXBRLタグ
            "category": str
        }
    """
    items_config = load_adjustment_items()
    detected = []

    # セクター除外対象の item_name セットを構築
    excluded_item_names: set = set()
    if sector_exclusions:
        for ex in sector_exclusions:
            item_id = ex.get('item_id') if isinstance(ex, dict) else ex
            mapped_name = SECTOR_ITEM_ID_TO_NAME.get(item_id)
            if mapped_name:
                excluded_item_names.add(mapped_name)

    for item in items_config:
        # セクター除外対象はスキップ
        if item.get('item_name', '') in excluded_item_names:
            continue
        xbrl_tags = item.get('xbrl_tags', [])
        for tag in xbrl_tags:
            if tag in period_data:
                value_dict = period_data[tag]
                # value_dict は {"value": ..., "unit": ...} 形式を想定
                amount = value_dict.get('value')
                unit = value_dict.get('unit', 'USD')
                if amount is None or amount == 0:
                    continue
                detected.append({
                    "item_name": item.get('item_name', ''),
                    "amount": amount,
                    "unit": unit,
                    "direction": item.get('direction', 'add_back'),
                    "pre_tax": item.get('pre_tax', True),
                    "reason": item.get('reason', ''),        # ★ KeyError修正
                    "extracted_from": tag,
                    "category": item.get('category', 'その他')
                })
                break  # 最初に見つかったタグで採用（優先順位が必要なら変更）
    
    return detected

# テスト用
if __name__ == "__main__":
    # 簡易テスト
    sample_period = {
        "filing_date": "2025-03-31",
        "form": "10-Q",
        "net_income": {"value": 214031000, "unit": "USD"},
        "diluted_shares": {"value": 2552818000, "unit": "shares"},
        "us-gaap:ShareBasedCompensation": {"value": 155339000, "unit": "USD"},
        "us-gaap:RestructuringCharges": {"value": 5000000, "unit": "USD"}
    }
    adjustments = detect_adjustments(sample_period)
    print("Detected adjustments:")
    for adj in adjustments:
        print(f"  {adj['item_name']}: {adj['amount']} {adj['unit']} (category: {adj['category']})")
