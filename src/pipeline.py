import yaml
import json
import os
from datetime import datetime

# モジュールインポート（実際のファイル名に合わせて調整済み）
from edgar_fetcher import fetch_filings
from xbrl_parser import parse_xbrl
from adjustment_detector import detect_adjustments
from tax_adjuster import apply_tax_adjustments   # ← ここを関数名に合わせ修正
from eps_calculator import calculate_eps
from ai_analyzer import analyze_adjustments      # ← Grok/xAI版を使う想定

def save_result(ticker, accession_no, result):
    """結果をJSONとして保存"""
    os.makedirs(f"data/{ticker}", exist_ok=True)
    filepath = f"data/{ticker}/{accession_no}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"Saved: {filepath}")

# ... 他のimportはそのまま ...

from extract_key_facts import extract_key_facts  # 新ファイル名に変更推奨

def run():
    with open("config/monitor_tickers.yaml") as f:
        tickers = yaml.safe_load(f)["tickers"]

    for ticker in tickers:
        print(f"Processing {ticker}...")
        extracted = extract_key_facts(ticker)
        if not extracted:
            continue

        # 調整検知（raw_factsを使う）
        adjustments_raw = detect_adjustments(extracted.get("raw_facts", {}))
        net_adjustment, detailed_adjustments = apply_tax_adjustments(adjustments_raw, extracted)
        result = calculate_eps(extracted, net_adjustment, detailed_adjustments)

        # AI分析
        ai_result = analyze_adjustments(ticker, result, detailed_adjustments)
        try:
            result["ai_analysis"] = json.loads(ai_result)
        except:
            result["ai_analysis"] = {"health": "Error", "comment": ai_result}

        result["processed_at"] = datetime.now().isoformat()

        # 保存（filing accession_noがないので "latest" で）
        os.makedirs(f"data/{ticker}", exist_ok=True)
        with open(f"data/{ticker}/latest.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        print(f"Saved latest for {ticker}")

if __name__ == "__main__":
    run()

if __name__ == "__main__":
    run()
