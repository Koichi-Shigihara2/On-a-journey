"""
pipeline.py
メインパイプライン
- 設定ファイル読み込み
- 銘柄ごとに四半期データ取得
- 調整項目検出、税効果適用、EPS計算
- TTM計算、年次集計（加重平均税率適用）
- AI分析（全四半期）
- JSON保存（docs/data/配下）
- サマリJSONを生成（銘柄一覧用）
"""
import yaml
import json
import os
import csv
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
# サマリ生成用関数
# ============================================
def load_company_names() -> Dict[str, str]:
    """cik_lookup.csv からティッカー→会社名のマップを作成"""
    name_map = {}
    csv_path = os.path.join(CONFIG_DIR, "cik_lookup.csv")
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticker = row['ticker'].strip().upper()
                name_map[ticker] = row['name']
    except Exception as e:
        print(f"Warning: Cannot load company names: {e}")
    return name_map

def calculate_yoy(latest_quarter: Dict, all_quarters: List[Dict]) -> Optional[float]:
    """最新四半期のYoY成長率を計算（前年同期比）"""
    if not all_quarters or len(all_quarters) < 5:
        return None
    # 日付でソート
    sorted_q = sorted(all_quarters, key=lambda x: x['filing_date'])
    latest = sorted_q[-1]
    # 4四半期前のデータ（同じ四半期であることを簡易チェック：月が一致）
    if len(sorted_q) >= 5:
        candidate = sorted_q[-5]
        if latest['filing_date'][5:7] == candidate['filing_date'][5:7]:
            prev_eps = candidate['adjusted_eps']
            if abs(prev_eps) > 1e-6:
                return (latest['adjusted_eps'] - prev_eps) / abs(prev_eps)
    # フォールバック：単純に4四半期前
    idx = sorted_q.index(latest)
    if idx >= 4:
        prev_eps = sorted_q[idx-4]['adjusted_eps']
        if abs(prev_eps) > 1e-6:
            return (latest['adjusted_eps'] - prev_eps) / abs(prev_eps)
    return None

def generate_summary(tickers: List[str], quarterly_results_map: Dict[str, List[Dict]], company_names: Dict[str, str]):
    """全銘柄のサマリJSONを生成"""
    summary = []
    for ticker in tickers:
        quarters = quarterly_results_map.get(ticker, [])
        if not quarters:
            continue
        latest = max(quarters, key=lambda x: x['filing_date'])
        yoy = calculate_yoy(latest, quarters)
        summary.append({
            "ticker": ticker,
            "company_name": company_names.get(ticker, ""),
            "latest_filing_date": latest['filing_date'],
            "gaap_eps": latest['gaap_eps'],
            "adjusted_eps": latest['adjusted_eps'],
            "health": latest.get('ai_analysis', {}).get('health', 'Good'),
            "yoy_growth": yoy if yoy is not None else None
        })
    # 保存
    summary_path = os.path.join(OUTPUT_BASE, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump({
            "last_updated": datetime.now().isoformat(),
            "tickers": summary
        }, f, indent=2, ensure_ascii=False)
    print(f"Summary saved to {summary_path} ({len(summary)} tickers)")

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
# 年次集計関数（加重平均税率適用版）
# ============================================
def aggregate_annual(quarterly_results: List[Dict]) -> List[Dict]:
    """
    四半期データを年次に集計（会計年度ベース）
    各四半期データには 'fiscal_year' フィールドが含まれていることを前提とする。
    年次調整額は加重平均税率を用いて再計算する。
    """
    # 年度ごとに四半期をグループ化
    annual_map = {}
    for q in quarterly_results:
        fiscal_year = q.get("fiscal_year")
        if fiscal_year is None:
            # 後方互換：fiscal_yearがなければ filing_date の年を使う（警告）
            print(f"Warning: 'fiscal_year' missing in {q.get('filing_date')}, using filing_date year")
            fiscal_year = int(q["filing_date"][:4])
        if fiscal_year not in annual_map:
            annual_map[fiscal_year] = []
        annual_map[fiscal_year].append(q)
    
    annual_results = []
    for year, quarters in annual_map.items():
        if len(quarters) < 4:
            print(f"Warning: Fiscal year {year} has only {len(quarters)} quarters")
        
        # 最新のfiling_dateを取得（その年の最大日付）
        latest_q = max(quarters, key=lambda x: x["filing_date"])
        
        # 年度のGAAP純利益、希薄化後株式数（平均）を計算
        total_gaap_net_income = sum(q["gaap_net_income"] for q in quarters)
        avg_shares = sum(q["diluted_shares_used"] for q in quarters) / len(quarters)
        
        # ---------- 加重平均税率の計算 ----------
        total_pretax = 0.0
        total_tax = 0.0
        weighted_tax_rate = 0.21  # デフォルト
        valid_quarters = []
        
        for q in quarters:
            # 税引前利益を取得（正規化）
            pretax_obj = q.get("pretax_income")
            pretax = normalize_value(pretax_obj) if pretax_obj is not None else 0.0
            
            # 税費用を取得（正規化）
            tax_obj = q.get("tax_expense")
            tax_exp = normalize_value(tax_obj) if tax_obj is not None else 0.0
            
            # 実効税率を取得（調整項目のtax_rate_appliedから採取）
            tax_rate = None
            adjustments = q.get("adjustments", [])
            if adjustments and len(adjustments) > 0:
                # 最初の調整項目から税率を取得（全項目同じ税率のはず）
                first_adj = adjustments[0]
                tax_rate = first_adj.get("tax_rate_applied")
            
            # 取得できなければ税引前利益と税費用から再計算
            if tax_rate is None and pretax != 0:
                computed = abs(tax_exp / pretax) if pretax != 0 else None
                if computed is not None and 0.0 <= computed <= 0.5:
                    tax_rate = computed
            
            if tax_rate is not None and pretax != 0:
                total_pretax += pretax
                total_tax += pretax * tax_rate
                valid_quarters.append(q)
        
        if total_pretax != 0 and valid_quarters:
            weighted_tax_rate = total_tax / total_pretax
        else:
            # 有効な四半期がなければデフォルト
            weighted_tax_rate = 0.21
        
        # ---------- 税前調整額合計を計算 ----------
        total_pretax_adjustments = 0.0
        all_adjustments = []  # 年次用にフラットに結合するリスト
        
        for q in quarters:
            for adj in q.get("adjustments", []):
                # 税前額（amount）を合計
                amount = adj.get("amount", 0.0)
                total_pretax_adjustments += amount
                
                # 年次調整項目としてコピーを作成（後でnet_amountを再計算）
                adj_copy = adj.copy()
                # 一旦税前額を保持（オプション）
                adj_copy["pre_tax_amount"] = amount
                all_adjustments.append(adj_copy)
        
        # 加重平均税率を適用して税効果後調整額合計を計算
        net_adjustment_total = total_pretax_adjustments * (1 - weighted_tax_rate)
        
        # 各調整項目のnet_amountを再計算
        for adj in all_adjustments:
            pre_tax = adj.get("pre_tax", True)
            amount = adj.get("amount", 0.0)
            if pre_tax:
                adj["net_amount"] = amount * (1 - weighted_tax_rate)
                adj["tax_rate_applied"] = weighted_tax_rate
            else:
                # 税後項目はそのまま
                adj["net_amount"] = amount
                adj["tax_rate_applied"] = 0.0
        
        # 年次オブジェクトの構築
        annual_results.append({
            "year": str(year),
            "filing_date": latest_q["filing_date"],
            "gaap_net_income": total_gaap_net_income,
            "adjusted_net_income": total_gaap_net_income + net_adjustment_total,
            "diluted_shares_used": avg_shares,
            "gaap_eps": total_gaap_net_income / avg_shares if avg_shares else 0,
            "adjusted_eps": (total_gaap_net_income + net_adjustment_total) / avg_shares if avg_shares else 0,
            "adjustments": all_adjustments,
            "net_adjustment_total": net_adjustment_total,
            "weighted_tax_rate": weighted_tax_rate  # 使用した加重平均税率を記録
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
    
    quarterly_results_map = {}  # サマリ生成用に保持
    
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
            result["fiscal_year"] = period_data.get("fiscal_year")
            # ★★★ pretax_income と tax_expense を保存（年次集計で使用）★★★
            result["pretax_income"] = data_for_eps["pretax_income"]
            result["tax_expense"] = data_for_eps["tax_expense"]
            
            # AI分析（各四半期ごとに実行）
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
        
        quarterly_results_map[ticker] = quarterly_results
        
        # 3. TTM計算
        ttm_results = []
        for i in range(3, len(quarterly_results)):
            ttm = calculate_ttm(quarterly_results, i)
            if ttm:
                ttm_results.append(ttm)
        
        # 4. 年次集計（加重平均税率適用）
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
    
    # 全銘柄処理後、サマリJSONを生成
    company_names = load_company_names()
    generate_summary(tickers, quarterly_results_map, company_names)

# ============================================
# エントリーポイント
# ============================================
if __name__ == "__main__":
    run()
