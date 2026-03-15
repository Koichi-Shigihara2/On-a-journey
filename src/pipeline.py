#!/usr/bin/env python3
"""
Adjusted EPS Analyzer Pipeline
メイン実行スクリプト：全監視銘柄のデータを取得・計算し、docs/data/配下にJSON保存
"""
import json
from pathlib import Path
import yaml
import sys
from datetime import datetime

# 自作モジュール
from extract_key_facts import extract_quarterly_data
from adjustment_detector import detect_adjustments
from tax_adjuster.apply_tax_effect import apply_tax_effect_to_adjustments
from eps_calculator.calculate import calculate_eps
from ai_analyzer import analyze_adjustments

# 設定ファイルのパス
CONFIG_DIR = Path(__file__).parent.parent / "config"
MONITOR_TICKERS_PATH = CONFIG_DIR / "monitor_tickers.yaml"
ADJUSTMENT_ITEMS_PATH = CONFIG_DIR / "adjustment_items.json"
CIK_LOOKUP_PATH = CONFIG_DIR / "cik_lookup.csv"

# 出力先（docs/data/）
OUTPUT_BASE = Path(__file__).parent.parent / "docs" / "data"

def load_config():
    with open(MONITOR_TICKERS_PATH) as f:
        tickers_config = yaml.safe_load(f)
    with open(ADJUSTMENT_ITEMS_PATH) as f:
        items_config = json.load(f)
    return tickers_config, items_config

def get_cik(ticker):
    """cik_lookup.csvからCIKを取得（文字列、先頭ゼロ補完なし）"""
    import csv
    with open(CIK_LOOKUP_PATH) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['ticker'].upper() == ticker.upper():
                return row['cik']
    raise ValueError(f"CIK not found for ticker {ticker}")

def save_json(data, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2, default=str)

def process_ticker(ticker):
    print(f"\n=== Processing {ticker} ===")
    cik = get_cik(ticker)
    
    # 1. SECから四半期データを取得
    print("  Fetching quarterly data from SEC...")
    quarters_raw = extract_quarterly_data(cik, ticker)
    if not quarters_raw:
        print(f"  No data for {ticker}, skipping.")
        return
    
    # 2. 各四半期ごとに調整項目検出・税効果・EPS計算・AI分析
    quarters_processed = []
    for i, q in enumerate(quarters_raw):
        filing_date = q.get('filing_date', 'unknown')
        form = q.get('form', '10-Q')
        print(f"  Processing quarter {i+1}/{len(quarters_raw)}: {filing_date} ({form})")
        
        gaap_net_income = q['net_income']  # 親会社株主帰属
        diluted_shares = q['weighted_average_shares_diluted']
        tax_rate = q.get('effective_tax_rate', 0.21)  # 実効税率（後で改善）
        
        # 調整項目検出（period_dataから該当項目を抽出）
        adjustments_raw = detect_adjustments(q, ticker, filing_date)
        
        # 税効果適用
        adjustments_with_tax = apply_tax_effect_to_adjustments(adjustments_raw, tax_rate)
        net_adjustment_total = sum(item['net_amount'] for item in adjustments_with_tax)
        
        # EPS計算
        gaap_eps, adjusted_eps = calculate_eps(gaap_net_income, 
                                                gaap_net_income + net_adjustment_total,
                                                diluted_shares)
        
        # AI分析（adjustmentsが空なら早期リターン）
        ai_analysis = analyze_adjustments(ticker, filing_date, adjustments_with_tax, 
                                           gaap_eps, adjusted_eps)
        
        # 出力用に整形
        quarter_record = {
            "gaap_net_income": gaap_net_income,
            "gaap_eps": gaap_eps,
            "adjusted_net_income": gaap_net_income + net_adjustment_total,
            "adjusted_eps": adjusted_eps,
            "diluted_shares_used": diluted_shares,
            "adjustments": adjustments_with_tax,
            "net_adjustment_total": net_adjustment_total,
            "effective_tax_rate": tax_rate,
            "yoy_growth": None,  # 後で計算する場合はここで
            "filing_date": filing_date,
            "form": form,
            "ai_analysis": ai_analysis
        }
        quarters_processed.append(quarter_record)
        
        print(f"    GAAP EPS=${gaap_eps:.4f} → Adj EPS=${adjusted_eps:.4f}")
    
    # 3. TTMデータの計算（簡易版：直近4四半期の和）
    ttm_data = []
    if len(quarters_processed) >= 4:
        for i in range(len(quarters_processed) - 3):
            ttm_quarters = quarters_processed[i:i+4]
            ttm_net_income = sum(q['gaap_net_income'] for q in ttm_quarters)
            ttm_adjusted_income = sum(q['adjusted_net_income'] for q in ttm_quarters)
            # 加重平均株式数は単純平均では正確でないが、簡易的に平均
            avg_shares = sum(q['diluted_shares_used'] for q in ttm_quarters) / 4
            ttm_eps = ttm_net_income / avg_shares
            ttm_adj_eps = ttm_adjusted_income / avg_shares
            period_str = f"{ttm_quarters[-1]['filing_date']} to {ttm_quarters[0]['filing_date']}"
            ttm_data.append({
                "period": period_str,
                "net_income": ttm_net_income,
                "adjusted_income": ttm_adjusted_income,
                "diluted_shares": avg_shares,
                "eps": ttm_eps,
                "adjusted_eps": ttm_adj_eps
            })
    
    # 4. 年次データ（四半期から集計）※今回は空のままにするか、必要なら実装
    # ここでは空リストを出力（後で必要なら追加）
    years_data = []
    
    # 5. 保存
    ticker_dir = OUTPUT_BASE / ticker
    save_json({
        "ticker": ticker,
        "last_updated": datetime.now().isoformat(),
        "quarters": quarters_processed
    }, ticker_dir / "quarterly.json")
    
    save_json({
        "ticker": ticker,
        "last_updated": datetime.now().isoformat(),
        "ttm": ttm_data
    }, ticker_dir / "ttm.json")
    
    save_json({
        "ticker": ticker,
        "last_updated": datetime.now().isoformat(),
        "years": years_data
    }, ticker_dir / "annual.json")   # 空のファイルでも残す（削除してもOK）
    
    print(f"✓ {ticker} 保存完了: {ticker_dir}")

def main():
    tickers_config, _ = load_config()
    tickers = tickers_config.get('tickers', [])
    if not tickers:
        print("No tickers to process.")
        return
    
    for ticker in tickers:
        try:
            process_ticker(ticker)
        except Exception as e:
            print(f"Error processing {ticker}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
