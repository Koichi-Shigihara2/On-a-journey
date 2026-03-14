import yaml
import json
import os
from datetime import datetime
from extract_key_facts import extract_quarterly_facts, normalize_value
from adjustment_detector import detect_adjustments
from tax_adjuster import apply_tax_adjustments
from eps_calculator import calculate_eps
from ai_analyzer import analyze_adjustments

def calculate_ttm(quarterly_data, end_idx):
    """TTM（直近4四半期）を計算"""
    if end_idx < 3:
        return None
    
    ttm_data = quarterly_data[end_idx-3:end_idx+1]
    if len(ttm_data) < 4:
        return None
    
    # TTM集計
    total_net_income = sum(q["net_income"] for q in ttm_data)
    total_adjustments = sum(q.get("total_adjustments", 0) for q in ttm_data)
    
    # 希薄化後株式数は平均を使用
    avg_shares = sum(q["diluted_shares"] for q in ttm_data) / 4
    
    return {
        "period": f"{ttm_data[0]['filing_date']} to {ttm_data[-1]['filing_date']}",
        "net_income": total_net_income,
        "adjusted_income": total_net_income + total_adjustments,
        "diluted_shares": avg_shares,
        "eps": total_net_income / avg_shares if avg_shares else 0,
        "adjusted_eps": (total_net_income + total_adjustments) / avg_shares if avg_shares else 0
    }

def run():
    # 設定読み込み
    with open("config/monitor_tickers.yaml") as f:
        tickers = yaml.safe_load(f)["tickers"]
    
    with open("config/adjustment_items.json") as f:
        adjustment_config = json.load(f)
    
    for ticker in tickers:
        print(f"\n=== Processing {ticker} ===")
        
        # 1. 過去10年分の四半期データを取得
        quarterly_raw = extract_quarterly_facts(ticker, years=10)
        if not quarterly_raw:
            print(f"{ticker}: データなし")
            continue
        
        # 2. 各四半期ごとに調整後EPSを計算
        quarterly_results = []
        for i, q in enumerate(quarterly_raw):
            # 値の正規化
            data = {
                "net_income": normalize_value(q.get("net_income", {})),
                "diluted_shares": normalize_value(q.get("diluted_shares", {})),
                "tax_expense": normalize_value(q.get("tax_expense", {})),
                "pretax_income": normalize_value(q.get("pretax_income", {})),
                "raw_facts": {k: normalize_value(v) for k, v in q.items() 
                            if isinstance(v, dict) and k not in ["net_income", "diluted_shares"]},
                "filing_date": q.get("filing_date"),
                "form": q.get("form")
            }
            
            # 調整項目検出
            adjustments_raw = detect_adjustments(data["raw_facts"], adjustment_config)
            
            # 税効果適用
            net_adjustment, detailed = apply_tax_adjustments(adjustments_raw, data)
            data["total_adjustments"] = net_adjustment
            
            # EPS計算
            result = calculate_eps(data, net_adjustment, detailed)
            result["filing_date"] = data["filing_date"]
            result["form"] = data["form"]
            
            quarterly_results.append(result)
            
            print(f"  {data['filing_date']} ({data['form']}): "
                  f"GAAP EPS=${result['gaap_eps']:.4f} → "
                  f"Adj EPS=${result['adjusted_eps']:.4f}")
        
        # 3. TTM計算
        ttm_results = []
        for i in range(3, len(quarterly_results)):
            ttm = calculate_ttm(quarterly_results, i)
            if ttm:
                ttm_results.append(ttm)
        
        # 4. AI分析（最新四半期）
        if quarterly_results:
            latest = quarterly_results[-1]
            ai_result = analyze_adjustments(
                ticker, 
                latest, 
                latest.get("adjustments", [])
            )
            try:
                latest["ai_analysis"] = json.loads(ai_result)
            except:
                latest["ai_analysis"] = {"health": "Error", "comment": ai_result}
        
        # 5. 保存
        ticker_dir = f"data/{ticker}"
        os.makedirs(ticker_dir, exist_ok=True)
        
        # 四半期データ
        with open(f"{ticker_dir}/quarterly.json", "w", encoding="utf-8") as f:
            json.dump({
                "ticker": ticker,
                "last_updated": datetime.now().isoformat(),
                "quarters": quarterly_results
            }, f, indent=2, ensure_ascii=False)
        
        # TTMデータ
        if ttm_results:
            with open(f"{ticker_dir}/ttm.json", "w", encoding="utf-8") as f:
                json.dump({
                    "ticker": ticker,
                    "last_updated": datetime.now().isoformat(),
                    "ttm": ttm_results
                }, f, indent=2, ensure_ascii=False)
        
        # 年次データ（四半期を年度で集計）
        annual_data = {}
        for q in quarterly_results:
            year = q["filing_date"][:4]
            if year not in annual_data:
                annual_data[year] = []
            annual_data[year].append(q)
        
        annual_results = []
        for year, quarters in annual_data.items():
            if len(quarters) >= 4:  # 完全な年のみ
                ttm = calculate_ttm(quarters, len(quarters)-1)
                if ttm:
                    ttm["year"] = year
                    annual_results.append(ttm)
        
        with open(f"{ticker_dir}/annual.json", "w", encoding="utf-8") as f:
            json.dump({
                "ticker": ticker,
                "last_updated": datetime.now().isoformat(),
                "years": annual_results
            }, f, indent=2, ensure_ascii=False)
        
        print(f"✓ {ticker} 保存完了: {ticker_dir}/")

if __name__ == "__main__":
    run()
