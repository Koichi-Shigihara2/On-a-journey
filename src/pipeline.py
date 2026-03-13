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
        filings = fetch_filings(ticker)

        for filing in filings:
            data = parse_xbrl(filing)
            adjustments = detect_adjustments(data)
            net_adjustments = apply_tax(adjustments, data)
            result = calculate_eps(data, net_adjustments)
            save_result(ticker, filing, result)

if __name__ == "__main__":
    run()
