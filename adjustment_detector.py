import json

with open("config/adjustment_items.json") as f:
    config = json.load(f)

def detect_adjustments(xbrl_data):
    adjustments = []
    for cat in config["categories"]:
        for item in cat["sub_items"]:
            amount = 0
            source_tag = None
            snippet = None

            # XBRLタグ優先
            for tag in item["xbrl_tags"]:
                if tag in xbrl_data:
                    amount = xbrl_data[tag]
                    source_tag = tag
                    snippet = xbrl_data.get(f"{tag}_snippet", "N/A")  # xbrl_parserでsnippet保存前提
                    break

            # タグなしならkeywords検索（簡易）
            if not amount:
                for kw in item["keywords"]:
                    if kw.lower() in str(xbrl_data).lower():  # 粗い検索
                        amount = 1000000  # 仮; 実際は抽出ロジック強化
                        snippet = "keyword match: " + kw

            if amount:
                adjustments.append({
                    "item_name": item["item_name"],
                    "amount": amount,
                    "direction": item["direction"],
                    "pre_tax": item["pre_tax"],
                    "reason": item["reason"],
                    "extracted_from": source_tag or "keyword",
                    "context_snippet": snippet
                })
    return adjustments
