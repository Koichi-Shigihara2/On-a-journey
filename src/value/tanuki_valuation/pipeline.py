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
            
        # ここで計算を実行
        calc = calculator.calculate_pt(financials)
        results[ticker] = calc
        
        # 重要な結果を明確に表示
        per_share = calc.get("intrinsic_value_per_share", 0)
        pt = calc.get("intrinsic_value_pt", 0)
        print(f"   → Intrinsic Value (Total): ${pt:,.0f}")
        print(f"   → Intrinsic Value (Per Share): ${per_share:.2f}")
        print(f"   → FCF 5yr Avg: ${financials.get('fcf_5yr_avg', 0):,.0f} | Method: {financials.get('fcf_calc_method', 'N/A')}")
        print(f"✅ {ticker} 更新完了")

    # 保存
    data_dir = "docs/value-monitor/tanuki_valuation/data"
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(f"{data_dir}/history", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"{data_dir}/history/{timestamp}.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    with open(f"{data_dir}/latest.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    print("\n🎉 TANUKI VALUATION 全銘柄更新完了！")

if __name__ == "__main__":
    run_update()
