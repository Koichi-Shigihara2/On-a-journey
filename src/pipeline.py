import yaml, json, os
from edgar_fetcher import fetch_filings
from xbrl_parser import parse_xbrl
from adjustment_detector import detect_adjustments
from tax_adjuster import apply_tax
from eps_calculator import calculate_eps

def save_result(ticker, period, result):
    os.makedirs(f"data/{ticker}", exist_ok=True)
    # アクセッション番号（期間）ごとの保存
    with open(f"data/{ticker}/{period}.json", "w") as f:
        json.dump(result, f, indent=2)
    # サイトが読み込むための「最新版」としても保存
    with open(f"data/{ticker}/latest.json", "w") as f:
        json.dump(result, f, indent=2)

def run():
    with open("config/monitor_tickers.yaml") as f:
        tickers = yaml.safe_load(f)["tickers"]

    for ticker in tickers:
        # 10年分をカバーするため多めに取得 (10Q/Kを40件以上)
        filings = fetch_filings(ticker, count=45) 

        results_history = []
        for filing in filings:
            raw_data = parse_xbrl(filing)
            if not raw_data: continue
            
            adjustments = detect_adjustments(raw_data)
            net_adjustments = apply_tax(adjustments, raw_data)
            
            # EPS計算ロジック（株式分割等は別途考慮が必要）
            result = calculate_eps(raw_data, net_adjustments)
            result["period"] = filing.period_end_date
            
            save_result(ticker, filing.accession_no, result)
            results_history.append(result)
        
        # ここでYoY成長率やCAGRの「推移データ」をまとめて生成するロジックへ
        generate_trend_data(ticker, results_history)

if __name__ == "__main__":
    run()
