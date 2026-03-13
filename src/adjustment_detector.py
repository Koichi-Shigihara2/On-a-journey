import json

with open("config/adjustment_items.json") as f:
    rules = json.load(f)

def detect_adjustments(data):
    adjustments = []
    for name, rule in rules.items():
        for tag in rule["xbrl_tags"]:
            if tag in data:
                adjustments.append({
                    "category": name,
                    "amount": data[tag],
                    "pretax": True
                })
    return adjustments
