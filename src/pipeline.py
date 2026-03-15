"""
pipeline.py
メインパイプライン
- 設定ファイル読み込み
- 銘柄ごとに四半期データ取得
- 調整項目検出、税効果適用、EPS計算
- TTM計算、年次集計
- AI分析（全四半期）
- JSON保存（docs/data/配下）
"""
import yaml
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

# 各モジュールのインポート
from extract_key_facts import extract_quarterly_facts, normalize_value
from adjustment_detector import detect_adjustments
from tax_adjuster import apply_tax_adjustments
from eps_calculator import calculate_eps
from ai_analyzer import analyze_adjustments

# ============================================
# 定数
# ============================================
CONFIG_DIR = "config"
OUTPUT_BASE = "docs/data"  # GitHub Pages用

# ============================================
# TTM計算関数
# ============================================
def calculate_ttm(quarterly_results: List[Dict], end_idx: int) -> Optional[Dict]:
    """
    TTM（直近4四半期）を計算
    Args:
        quarterly_results: 四半期結果のリスト（新しい順）
        end_idx: 現在のインデックス（このインデックスを含む過去4四半期）
    Returns:
        Optional[Dict]: TTMデータ
    """
    if end_idx < 3:
        return None
    
    ttm_data = quarterly_results[end_idx-3:end_idx+1]
    if len(ttm_data) < 4:
        return None
    
    total_net_income = sum(q["gaap_net_income"] for q in ttm_data)
    total_adjustments = sum(q.get("net_adjustment_total", 0) for q in ttm_data)
    avg_shares = sum(q["diluted_shares_used"] for q in ttm_data) / 4

    return {
        "period": f"{ttm_data[0]['filing_date']} to {ttm_data[-1]['filing_date']}",
        "net_income": total_net_income,
        "adjusted_income": total_net_income + total_adjustments,
        "diluted_shares": avg_shares,
        "eps": total_net_income / avg_shares if avg_shares else 0,
        "adjusted_eps": (total_net_income + total_adjustments) / avg_shares if avg_shares else 0
    }

# ============================================
# 年次集計関数
# ============================================
def aggregate_annual(quarterly_results: List[Dict]) -> List[Dict]:
    """
    四半期データを年次に集計（暦年ベース）
    """
    annual_map = {}
    for q in quarterly_results:
        year = q["filing_date"][:4]
        if year not in annual_map:
            annual_map[year] = []
        annual_map[year].append(q)
    
    annual_results = []
    for year, quarters in annual_map.items():
        if len(quarters) < 4:
            continue
        
        latest_q = max(quarters, key=lambda x: x["filing_date"])
        total_net_income = sum(q["gaap_net_income"] for q in quarters)
        total_adjustments = sum(q.get("net_adjustment_total", 0) for q in quarters)
        avg_shares = sum(q["diluted_shares_used"] for q in quarters) / 4
        
        annual_results.append({
            "year": year,
            "filing_date": latest_q["filing_date"],
            "gaap_net_income": total_net_income,
            "adjusted_net_income": total_net_income + total_adjustments,
            "diluted_shares_used": avg_shares,
            "gaap_eps": total_net_income / avg_shares if avg_shares else 0,
            "adjusted_eps": (total_net_income + total_adjustments) / avg_shares if avg_shares else 0,
            "adjustments": [adj for q in quarters for adj in q.get("adjustments", [])],
            "net_adjustment_total": total_adjustments
        })
    
    annual_results.sort(key=lambda x: x["year"], reverse=True)
    return annual_results

# ============================================
# メイン実行関数
# ============================================
def run():
    # 設定読み込み
    with open(os.path.join(CONFIG_DIR, "monitor_tickers.yaml"), 'r', encoding='utf-8') as f:
        tickers = yaml.safe_load(f)["tickers"]
    
    for ticker in tickers:
        print(f"\n=== Processing {ticker} ===")
        
        # 1. 過去N年分の四半期データを取得
        quarterly_raw = extract_quarterly_facts(ticker, years=10)
        if not quarterly_raw:
            print(f"{ticker}: データなし")
            continue
        
        # 2. 各四半期ごとに調整後EPSを計算
        quarterly_results = []
        for i, period_data in enumerate(quarterly_raw):
            print(f"\nProcessing quarter {i+1}/{len(quarterly_raw)}: {period_data.get('filing_date', 'unknown')} ({period_data.get('form', '10-Q')})")
            
            data_for_eps = {
                "net_income": period_data.get('net_income'),
                "diluted_shares": period_data.get('diluted_shares'),
                "pretax_income": period_data.get('pretax_income'),
                "tax_expense": period_data.get('tax_expense'),
                "filing_date": period_data.get('filing_date'),
                "form": period_data.get('form')
            }
            
            adjustments_raw = detect_adjustments(period_data)
            net_adjustment, detailed = apply_tax_adjustments(adjustments_raw, data_for_eps)
            result = calculate_eps(data_for_eps, net_adjustment, detailed)
            result["filing_date"] = data_for_eps["filing_date"]
            result["form"] = data_for_eps["form"]
            
            # ★★★ AI分析（各四半期ごとに実行）★★★
            ai_result_str = analyze_adjustments(ticker, result, result.get("adjustments", []))
            try:
                result["ai_analysis"] = json.loads(ai_result_str)
            except Exception as e:
                print(f"AI analysis parse error for {result['filing_date']}: {e}")
                result["ai_analysis"] = {
                    "health": "Error",
                    "comment": f"AI分析エラー: {str(e)}",
                    "sources": []
                }
            
            quarterly_results.append(result)
            print(f"  {result['filing_date']}: GAAP EPS=${result['gaap_eps']:.4f} → Adj EPS=${result['adjusted_eps']:.4f}")
        
        # 3. TTM計算
        ttm_results = []
        for i in range(3, len(quarterly_results)):
            ttm = calculate_ttm(quarterly_results, i)
            if ttm:
                ttm_results.append(ttm)
        
        # 4. 年次集計
        annual_results = aggregate_annual(quarterly_results)
        
        # 5. 保存（docs/data/ 配下）
        ticker_dir = os.path.join(OUTPUT_BASE, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        
        # 四半期データ
        with open(os.path.join(ticker_dir, "quarterly.json"), "w", encoding="utf-8") as f:
            json.dump({
                "ticker": ticker,
                "last_updated": datetime.now().isoformat(),
                "quarters": quarterly_results
            }, f, indent=2, ensure_ascii=False)
        
        # TTMデータ
        if ttm_results:
            with open(os.path.join(ticker_dir, "ttm.json"), "w", encoding="utf-8") as f:
                json.dump({
                    "ticker": ticker,
                    "last_updated": datetime.now().isoformat(),
                    "ttm": ttm_results
                }, f, indent=2, ensure_ascii=False)
        
        # 年次データ
        if annual_results:
            with open(os.path.join(ticker_dir, "annual.json"), "w", encoding="utf-8") as f:
                json.dump({
                    "ticker": ticker,
                    "last_updated": datetime.now().isoformat(),
                    "years": annual_results
                }, f, indent=2, ensure_ascii=False)
        else:
            with open(os.path.join(ticker_dir, "annual.json"), "w", encoding="utf-8") as f:
                json.dump({
                    "ticker": ticker,
                    "last_updated": datetime.now().isoformat(),
                    "years": []
                }, f, indent=2, ensure_ascii=False)
        
        print(f"✓ {ticker} 保存完了: {ticker_dir}/")

# ============================================
# エントリーポイント
# ============================================
if __name__ == "__main__":
    run()
