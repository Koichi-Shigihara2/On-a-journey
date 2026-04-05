from .data_fetcher import TanukiDataFetcher
from .core_calculator import KoichiValuationCalculator
import json, os
from datetime import datetime

def run_update():
    fetcher = TanukiDataFetcher()
    calculator = KoichiValuationCalculator()
    
    tickers = ["TSLA", "PLTR", "SOFI", "CELH", "NVDA", "AMD", "APP", "SOUN", "RKLB", "ONDS", "FIG"]

    print("=== TANUKI VALUATION Phase 3 実行開始（成長率減衰カーブ＋RPO補正）===\n")
    results = {}
    for ticker in tickers:
        print(f"🔄 Updating {ticker}...")
        try:
            financials = fetcher.get_financials(ticker)
            calc = calculator.calculate_pt(financials)
            
            if "error" in calc:
                print(f"❌ {ticker} skipped - {calc.get('error')}")
                continue

            results[ticker] = calc

            print(f"   → FCF 5yr Avg      : ${financials.get('fcf_5yr_avg', 0):,.0f}")
            print(f"   → Diluted Shares   : {financials.get('diluted_shares', 0):,.0f}")
            print(f"   → ROE_10yr_avg     : {financials.get('roe_10yr_avg', 0):.1%}")
            print(f"   → α (個別成長期待): {calc.get('alpha', 0):.3f}")
            print(f"   → 理論株価         : ${calc.get('intrinsic_value_per_share', 0):.2f}")
            print(f"✅ {ticker} 更新完了\n")

        except Exception as e:
            print(f"❌ {ticker} エラー: {e}")
            continue

    data_dir = "docs/value-monitor/tanuki_valuation/data"
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(f"{data_dir}/history", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"{data_dir}/history/{timestamp}.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    with open(f"{data_dir}/latest.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    print("🎉 TANUKI VALUATION Phase 3 更新完了！")

if __name__ == "__main__":
    run_update()
