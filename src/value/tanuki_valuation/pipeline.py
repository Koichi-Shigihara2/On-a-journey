# src/value/tanuki_valuation/pipeline.py
import os
import json
from datetime import datetime
from .core_calculator import KoichiValuationCalculator
from .data_fetcher import TanukiDataFetcher
from .segment_kpi_ai import SegmentKPIAI

def run_update():
    fetcher = TanukiDataFetcher()
    ai = SegmentKPIAI()
    calc = KoichiValuationCalculator()
    
    tickers = ["MSFT", "AMZN"] + ["SOFI","TSLA","PLTR","CELH","NVDA","AMD","APP","SOUN","RKLB","ONDS","FIG"]
    
    for ticker in tickers:
        print(f"🔄 Updating {ticker}...")
        data = fetcher.get_financials(ticker)
        
        sec_text = "SECデータ取得中（FMP連携）"
        scenarios = ai.generate_scenarios(ticker, sec_text)
        
        result = calc.calculate_pt(data)
        
        # tickerごとのフォルダを確実に作成（重要修正）
        base_dir = f"docs/value-monitor/tanuki_valuation/data/{ticker}"
        os.makedirs(base_dir, exist_ok=True)
        history_dir = f"{base_dir}/history"
        os.makedirs(history_dir, exist_ok=True)
        
        # 履歴保存
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(f"{history_dir}/{timestamp}.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        # 最新結果保存
        with open(f"{base_dir}/latest.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"✅ {ticker} 更新完了")
    
    print("🎉 TANUKI VALUATION 全銘柄更新完了！")

if __name__ == "__main__":
    run_update()
