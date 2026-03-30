from .data_fetcher import TanukiDataFetcher
from .core_calculator import KoichiValuationCalculator
import json, os
from datetime import datetime

def run_update():
    fetcher = TanukiDataFetcher()
    calculator = KoichiValuationCalculator()
    tickers = ["MSFT", "AMZN", "SOFI", "TSLA", "PLTR", "CELH", "NVDA", "AMD", "APP", "SOUN", "RKLB", "ONDS", "FIG"]

    results = {}
    for ticker in tickers:
        print(f"🔄 Updating {ticker}...")
        financials = fetcher.get_financials(ticker)
        if "error" in financials:
            print(f"❌ {ticker} skipped")
            continue
        calc = calculator.calculate_pt(financials)
        results[ticker] = calc
        print(f"✅ {ticker} 更新完了")

    # 保存
    data_dir = "docs/value-monitor/tanuki_valuation/data"
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(f"{data_dir}/history", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"{data_dir}/history/{timestamp}.json", "w") as f:
        json.dump(results, f, indent=2)
    with open(f"{data_dir}/latest.json", "w") as f:
        json.dump(results, f, indent=2)

    print("🎉 TANUKI VALUATION 全銘柄更新完了！")

if __name__ == "__main__":
    run_update()
