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
        
        # 詳細表示を強化
        per_share = calc.get("intrinsic_value_per_share", 0)
        total_value = calc.get("intrinsic_value_pt", 0)
        fcf_avg = financials.get("fcf_5yr_avg", 0)
        method = financials.get("fcf_calc_method", "N/A")
        diluted = financials.get("diluted_shares", 0)
        
        print(f"   → FCF 5yr Avg: ${fcf_avg:,.0f} | Method: {method}")
        print(f"   → Diluted Shares: {diluted:,.0f}")
        print(f"   → Intrinsic Value (Total): ${total_value:,.0f}")
        print(f"   → Intrinsic Value (Per Share): ${per_share:.2f}")
        print(f"✅ {ticker} 更新完了\n")

    # 保存
    data_dir = "docs/value-monitor/tanuki_valuation/data"
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(f"{data_dir}/history", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"{data_dir}/history/{timestamp}.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    with open(f"{data_dir}/latest.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    print("🎉 TANUKI VALUATION 全銘柄更新完了！")

if __name__ == "__main__":
    run_update()
