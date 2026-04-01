from .data_fetcher import TanukiDataFetcher
from .core_calculator import KoichiValuationCalculator
import json, os
from datetime import datetime

def run_update():
    fetcher = TanukiDataFetcher()
    calculator = KoichiValuationCalculator()
    tickers = ["MSFT", "AMZN", "TSLA", "NVDA", "PLTR", "CELH", "APP", "AMD", "SOFI", "SOUN", "RKLB", "ONDS", "FIG"]

    print("=== TANUKI VALUATION 全銘柄実行開始（企業別成長率自動計算）===\n")
    results = {}
    for ticker in tickers:
        print(f"🔄 Updating {ticker}...")
        try:
            financials = fetcher.get_financials(ticker)
            
            if "error" in financials:
                print(f"❌ {ticker} skipped - {financials.get('error')}")
                continue
                
            calc = calculator.calculate_pt(financials)
            
            if "error" in calc:
                print(f"❌ {ticker} skipped - {calc.get('error')}")
                continue
            
            results[ticker] = calc
            
            print(f"   → FCF 5yr Avg          : ${financials.get('fcf_5yr_avg', 0):,.0f}")
            print(f"   → Diluted Shares       : {financials.get('diluted_shares', 0):,.0f}")
            print(f"   → 企業別高成長率（CAGR）: {calc['components'].get('high_growth_rate_used', 0):.1%}")
            print(f"   → V0 (本質的価値ベース) : ${calc.get('v0', 0):,.0f}")
            print(f"   → α (個別成長期待)    : {calc.get('alpha', 0):.3f}")
            print(f"   → 2段階DCF内訳        : 高成長期PV + 永続期PV")
            print(f"   → Intrinsic Value (Per Share) : ${calc.get('intrinsic_value_per_share', 0):.2f}")
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

    print("🎉 TANUKI VALUATION 全銘柄更新完了！（計算過程はlatest.jsonで照会可能）")

if __name__ == "__main__":
    run_update()