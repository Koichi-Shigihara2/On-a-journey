"""
メインパイプライン（セクター別除外対応版）
"""
import yaml
import json
import os
import csv
from datetime import datetime
from typing import Dict, List, Any, Optional

from extract_key_facts import extract_quarterly_facts, normalize_value
from adjustment_detector import detect_adjustments
from tax_adjuster import apply_tax_adjustments
from eps_calculator import calculate_eps
from ai_analyzer import analyze_adjustments
from sector_classifier_v2 import SectorClassifierV2
from company_metadata import get_company_metadata
from maturity_monitor import MaturityMonitor

def load_cik_data() -> List[Dict]:
    """cik_lookup.csv から全データを読み込む（セクター情報含む）"""
    cik_file = os.path.join("config", "cik_lookup.csv")
    data = []
    try:
        with open(cik_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
    except Exception as e:
        print(f"Warning: Could not load cik_lookup.csv: {e}")
    return data

def calculate_ttm(quarterly_results: List[Dict], end_idx: int) -> Optional[Dict]:
    """TTM（直近4四半期）を計算"""
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

def aggregate_annual(quarterly_results: List[Dict]) -> List[Dict]:
    """四半期データを年次に集計（暦年ベース）"""
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

def generate_summary(tickers_data: Dict[str, Dict]) -> Dict:
    """全銘柄のサマリー情報を生成"""
    summary = {
        "last_updated": datetime.now().isoformat(),
        "tickers": []
    }
    for ticker, data in tickers_data.items():
        if data.get("quarters") and len(data["quarters"]) > 0:
            # 最新四半期を取得（新しい順にソート済みと仮定）
            latest = data["quarters"][0]
            # YoY成長率を計算（4四半期前と比較）
            yoy_growth = None
            if len(data["quarters"]) >= 5:
                prev = data["quarters"][4]  # 4つ前の四半期
                if prev["adjusted_eps"] != 0:
                    yoy_growth = (latest["adjusted_eps"] - prev["adjusted_eps"]) / abs(prev["adjusted_eps"])
            # 健全性は ai_analysis から取得、なければデフォルト "Caution"
            health = "Caution"
            if "ai_analysis" in latest:
                health = latest["ai_analysis"].get("health", "Caution")
            summary["tickers"].append({
                "ticker": ticker,
                "company_name": data.get("company_name", ""),
                "latest_filing_date": latest["filing_date"],
                "gaap_eps": latest["gaap_eps"],
                "adjusted_eps": latest["adjusted_eps"],
                "yoy_growth": yoy_growth,
                "health": health
            })
    return summary

def run():
    config_base = "config"
    with open(os.path.join(config_base, "monitor_tickers.yaml"), 'r', encoding='utf-8') as f:
        tickers = yaml.safe_load(f)["tickers"]
    
    with open(os.path.join(config_base, "adjustment_items.json"), 'r', encoding='utf-8') as f:
        adjustment_config = json.load(f)
    
    # セクター分類器の初期化
    classifier = SectorClassifierV2(os.path.join(config_base, "sectors.yaml"))
    
    # 銘柄マスタ（セクター情報）を読み込み
    cik_data = load_cik_data()
    ticker_to_sector = {row['ticker']: row.get('sector') for row in cik_data if row.get('sector')}
    # 会社名マップも作成
    ticker_to_name = {row['ticker']: row.get('name', '') for row in cik_data}
    
    # 成熟度監視の設定（調整項目設定から取得）
    maturity_config = adjustment_config.get('maturity_defaults', {})
    
    # 全銘柄のデータを一時保存（summary用）
    all_tickers_data = {}

    for ticker in tickers:
        print(f"\n=== Processing {ticker} ===")
        
        # データ取得
        quarterly_raw = extract_quarterly_facts(ticker, years=10)
        if not quarterly_raw:
            print(f"{ticker}: データなし")
            continue
        
        # CIK取得（cik_lookup.csvにあればそれを使う）
        from extract_key_facts import get_cik as get_cik_func
        try:
            cik = get_cik_func(ticker)
        except:
            cik = None
        
        # 企業メタデータ取得（SICコードなど）
        metadata = {}
        if cik:
            metadata = get_company_metadata(cik)
        
        # セクター判定（優先順位: 銘柄マスタ > SICコード > キーワード）
        sector = ticker_to_sector.get(ticker)
        if not sector:
            sector = classifier.classify_by_sic(metadata.get('sic', ''))
        if not sector:
            sector = classifier.classify_by_keywords(metadata.get('name', ''))
        
        print(f"  Sector: {sector or 'Unknown'}")
        
        # セクター別デフォルト除外項目を取得
        sector_exclusions = classifier.get_exclusions_for_sector(sector) if sector else []
        exclusion_item_ids = [ex['item_id'] for ex in sector_exclusions]
        
        quarterly_results = []
        for i, period_data in enumerate(quarterly_raw):
            print(f"\nProcessing quarter {i+1}/{len(quarterly_raw)}: {period_data['filing_date']} ({period_data['form']})")
            
            data = {
                "net_income": normalize_value(period_data.get("net_income")),
                "diluted_shares": normalize_value(period_data.get("diluted_shares")),
                "tax_expense": normalize_value(period_data.get("tax_expense")),
                "pretax_income": normalize_value(period_data.get("pretax_income")),
                "filing_date": period_data["filing_date"],
                "form": period_data["form"],
                "raw_facts": {k: v for k, v in period_data.items() 
                            if k not in ["net_income", "diluted_shares", "tax_expense", "pretax_income", "filing_date", "form"]}
            }
            
            # 調整項目検出（セクター情報を渡す）
            adjustments_raw = detect_adjustments(period_data, adjustment_config, sector, sector_exclusions)
            
            # 税効果適用
            net_adjustment, detailed = apply_tax_adjustments(adjustments_raw, data)
            data["total_adjustments"] = net_adjustment
            
            # EPS計算
            result = calculate_eps(data, net_adjustment, detailed)
            result["filing_date"] = data["filing_date"]
            result["form"] = data["form"]
            result["net_adjustment_total"] = net_adjustment
            result["sector"] = sector
            result["sector_exclusions"] = exclusion_item_ids
            
            quarterly_results.append(result)
            
            print(f"  {result['filing_date']} ({result['form']}): "
                  f"GAAP EPS=${result['gaap_eps']:.4f} → "
                  f"Adj EPS=${result['adjusted_eps']:.4f}")
        
        # 成熟度監視（SBCが含まれるセクターのみ）
        if sector in ['ハイパーグロース / SaaS', 'テクノロジー']:
            monitor = MaturityMonitor(maturity_config)
            maturity_status = monitor.monitor(quarterly_results)
            if maturity_status.get('alert'):
                print(f"  ⚠ Maturity Alert: {maturity_status['alert']}")
            # 最新四半期に結果を保存
            if quarterly_results:
                quarterly_results[-1]['maturity_monitor'] = maturity_status
        
        # TTM計算
        ttm_results = []
        for i in range(3, len(quarterly_results)):
            ttm = calculate_ttm(quarterly_results, i)
            if ttm:
                ttm_results.append(ttm)
        
        # 年次集計
        annual_results = aggregate_annual(quarterly_results)
        
        # ★★★ 修正点：最新四半期を確実に特定してAI分析を実行 ★★★
        if quarterly_results:
            # 新しい順にソート
            quarterly_results.sort(key=lambda x: x["filing_date"], reverse=True)
            latest = quarterly_results[0]  # 最新
            print(f"  [AI] Running analysis for latest quarter: {latest['filing_date']}")
            ai_result = analyze_adjustments(
                ticker, 
                latest, 
                latest.get("adjustments", [])
            )
            try:
                latest["ai_analysis"] = json.loads(ai_result)
                print(f"  [AI] Analysis stored for {latest['filing_date']}")
            except Exception as e:
                print(f"  [AI] Failed to parse AI result: {e}")
                latest["ai_analysis"] = {"health": "Error", "comment": str(ai_result), "sources": []}
        
        # 保存
        ticker_dir = f"docs/data/{ticker}"
        os.makedirs(ticker_dir, exist_ok=True)
        
        with open(f"{ticker_dir}/quarterly.json", "w", encoding="utf-8") as f:
            json.dump({
                "ticker": ticker,
                "last_updated": datetime.now().isoformat(),
                "quarters": quarterly_results
            }, f, indent=2, ensure_ascii=False)
        
        if ttm_results:
            with open(f"{ticker_dir}/ttm.json", "w", encoding="utf-8") as f:
                json.dump({
                    "ticker": ticker,
                    "last_updated": datetime.now().isoformat(),
                    "ttm": ttm_results
                }, f, indent=2, ensure_ascii=False)
        
        if annual_results:
            with open(f"{ticker_dir}/annual.json", "w", encoding="utf-8") as f:
                json.dump({
                    "ticker": ticker,
                    "last_updated": datetime.now().isoformat(),
                    "years": annual_results
                }, f, indent=2, ensure_ascii=False)
        
        # サマリー用にデータを保持
        all_tickers_data[ticker] = {
            "quarters": quarterly_results,
            "company_name": ticker_to_name.get(ticker, metadata.get('name', ''))
        }
        
        print(f"✓ {ticker} 保存完了: {ticker_dir}/")
    
    # サマリーファイル生成
    if all_tickers_data:
        summary = generate_summary(all_tickers_data)
        with open("docs/data/summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        print("✓ summary.json 生成完了")

if __name__ == "__main__":
    run()
