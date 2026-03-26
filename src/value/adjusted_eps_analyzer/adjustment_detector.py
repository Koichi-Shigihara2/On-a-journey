"""
adjustment_detector.py
調整項目検出モジュール
- adjustment_items.json（カテゴリ構造）を読み込み、フラットな項目リストに変換
- period_data から各項目の xbrl_tags に該当するタグを探し、値があれば調整項目として抽出
- sector_exclusions に基づき、セクター別除外項目をスキップ
"""
import json
from pathlib import Path
from typing import Dict, List, Any, Optional

# プロジェクトルートを取得（adjustment_detector.py の場所から3階層上）
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
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

# ★ SBC 関連タグを取得する関数（YTD変換用）
def get_sbc_xbrl_tags() -> List[str]:
    """
    adjustment_items.json から SBC（株式報酬費用）項目の xbrl_tags を返す
    """
    items = load_adjustment_items()
    sbc_tags = set()
    for item in items:
        if item.get('item_id') == 'sbc':
            sbc_tags.update(item.get('xbrl_tags', []))
    return list(sbc_tags)

# sectors.yaml の item_id → adjustment_items.json の item_name へのマッピング
SECTOR_ITEM_ID_TO_NAME = {
    'sbc':                          '株式報酬費用',
    'amortization_intangibles':     '無形資産償却費',
    'inventory_writeoff':           '在庫評価損・減損',
    'ma_integration':               'M&A統合費用',
    'iprd_amortization':            'IPR&D償却',
    'goodwill_impairment':          'のれん減損',
    'loan_fair_value':              'ローン公正価値評価損益',
    'loan_loss_provision_abnormal': '貸倒引当金繰入（異常変動分）',
    'investment_gains':             '投資有価証券評価損益',
    'logistics_one_time':           '物流・関税一時的コスト',
    'milestone_rd':                 'マイルストーン型研究開発費',
    'depreciation':                 '減価償却費',
    'asset_sale_gain':              '資産売却損益',
    'crypto_fair_value':            '暗号資産公正価値変動損益',
    'impairment_legacy':            '暗号資産減損',
    'restructuring':                'リストラ費用',
    'litigation_settlement':        '訴訟和解金・罰金',
    'fx_gains_losses':              '為替差損益',
    'derivative_gains_losses':      'デリバティブ評価損益',
    'tax_one_time':                 '一過性税効果',
    'gov_subsidy_timing':           '政府補助金（一時的影響）',
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
        period_data: 1四半期分のデータ
        adjustment_config: 互換性のため残しているが、使用しない
        sector: セクター名
        sector_exclusions: セクター別除外項目リスト
    Returns:
        List[Dict]: 検出された調整項目のリスト
    """
    items_config = load_adjustment_items()
    detected = []

    excluded_item_names: set = set()
    if sector_exclusions:
        for ex in sector_exclusions:
            item_id = ex.get('item_id') if isinstance(ex, dict) else str(ex)
            mapped_name = SECTOR_ITEM_ID_TO_NAME.get(item_id)
            if mapped_name:
                excluded_item_names.add(mapped_name)

    REIT_ONLY_ITEM_IDS = {'depreciation', 'asset_sale_gain'}
    is_reit = sector and '不動産' in sector

    for item in items_config:
        item_name = item.get('item_name', '')
        item_id   = item.get('item_id', '')

        if item_id in REIT_ONLY_ITEM_IDS and not is_reit:
            continue

        if item_name in excluded_item_names:
            continue

        xbrl_tags = item.get('xbrl_tags', [])
        for tag in xbrl_tags:
            if tag in period_data:
                value_dict = period_data[tag]
                amount = value_dict.get('value')
                unit = value_dict.get('unit', 'USD')
                if amount is None or amount == 0:
                    continue
                detected.append({
                    "item_id": item.get('item_id', ''),
                    "item_name": item_name,
                    "amount": amount,
                    "unit": unit,
                    "direction": item.get('direction', 'add_back'),
                    "pre_tax": item.get('pre_tax', True),
                    "reason": item.get('reason_default', item.get('reason', '')),
                    "extracted_from": tag,
                    "category": item.get('category', 'その他')
                })
                break

    return detected


if __name__ == "__main__":
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
        print(f"  {adj['item_name']}: {adj['amount']} {adj['unit']} (reason: {adj['reason']})")
    
    # SBC タグ取得のテスト
    sbc_tags = get_sbc_xbrl_tags()
    print(f"\nSBC XBRL tags: {sbc_tags}")