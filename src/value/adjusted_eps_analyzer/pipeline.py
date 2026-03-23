"""
メインパイプライン（セクター別除外対応版）
"""
import yaml
import json
import os
import csv
from datetime import datetime
from typing import Dict, List, Any, Optional

from .extract_key_facts import extract_quarterly_facts, normalize_value
from .adjustment_detector import detect_adjustments
from .tax_adjuster import apply_tax_adjustments
from .eps_calculator import calculate_eps
from .ai_analyzer import analyze_adjustments
from .sector_classifier_v2 import SectorClassifierV2
from .company_metadata import get_company_metadata
from .maturity_monitor import MaturityMonitor

# プロジェクトルートを取得（pipeline.py の場所から4階層上）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def load_cik_data() -> List[Dict]:
    cik_file = os.path.join(PROJECT_ROOT, "config", "cik_lookup.csv")
    data = []
    try:
        with open(cik_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
    except Exception as e:
        print(f"Warning: Could not load cik_lookup.csv: {e}")
    return data

def calculate_ttm(quarterly_results: List[Dict], end_idx: int) -> Optional[Dict]:
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
    summary = {
        "last_updated": datetime.now().isoformat(),
        "tickers": []
    }
    for ticker, data in tickers_data.items():
        if data.get("quarters") and len(data["quarters"]) > 0:
            latest = data["quarters"][0]
            yoy_growth = None
            if len(data["quarters"]) >= 5:
                prev = data["quarters"][4]
                if prev["adjusted_eps"] != 0:
                    yoy_growth = (latest["adjusted_eps"] - prev["adjusted_eps"]) / abs(prev["adjusted_eps"])
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

def get_revenue(period_data: Dict) -> float:
    """売上高を取得（一般企業：Revenues、銀行：RevenuesNetOfInterestExpense優先）"""
    # ★ RevenuesNetOfInterestExpense を最優先（銀行・金融機関の総収益）
    rev_net = normalize_value(period_data.get("us-gaap:RevenuesNetOfInterestExpense"))
    if rev_net and rev_net > 0:
        return rev_net
    # 一般企業系タグ
    for tag in [
        "us-gaap:Revenues",
        "us-gaap:RevenueFromContractWithCustomer",
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        "us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",
        "us-gaap:NetSales",
        "us-gaap:TotalRevenue",
        "us-gaap:SalesRevenueNet",
    ]:
        val = normalize_value(period_data.get(tag))
        if val and val > 0:
            return val
    # 銀行系フォールバック：純金利収益 ＋ 非金利収益 を合算
    net_interest = normalize_value(
        period_data.get("us-gaap:NetInterestIncome") or
        period_data.get("us-gaap:InterestIncomeExpenseNet") or
        period_data.get("us-gaap:InterestAndDividendIncomeOperating")
    ) or 0.0
    non_interest = normalize_value(period_data.get("us-gaap:NoninterestIncome")) or 0.0
    if net_interest or non_interest:
        return net_interest + non_interest
    return 0.0

def run(ticker_filter: str = None):
    config_base = os.path.join(PROJECT_ROOT, "config")
    with open(os.path.join(config_base, "monitor_tickers.yaml"), 'r', encoding='utf-8') as f:
        tickers = yaml.safe_load(f)["tickers"]
    
    if ticker_filter:
        requested = [t.strip().upper() for t in ticker_filter.split(',') if t.strip()]
        for t in requested:
            if t not in tickers:
                print(f"Warning: {t} は monitor_tickers.yaml に未登録ですが処理を続行します")
        tickers = requested
    
    with open(os.path.join(config_base, "adjustment_items.json"), 'r', encoding='utf-8') as f:
        adjustment_config = json.load(f)
    
    classifier = SectorClassifierV2(os.path.join(PROJECT_ROOT, "config", "sectors.yaml"))
    
    cik_data = load_cik_data()
    ticker_to_sector = {row['ticker']: row.get('sector') for row in cik_data if row.get('sector')}
    ticker_to_name = {row['ticker']: row.get('name', '') for row in cik_data}
    
    maturity_config = adjustment_config.get('maturity_defaults', {})
    
    all_tickers_data = {}
    DATA_ROOT = os.path.join(PROJECT_ROOT, "docs", "value-monitor", "adjusted_eps_analyzer", "data")

    for ticker in tickers:
        print(f"\n=== Processing {ticker} ===")
        
        quarterly_raw = extract_quarterly_facts(ticker, years=10)
        if not quarterly_raw:
            print(f"{ticker}: データなし")
            continue
        
        from .extract_key_facts import get_cik as get_cik_func
        try:
            cik = get_cik_func(ticker)
        except:
            cik = None
        
        metadata = {}
        if cik:
            metadata = get_company_metadata(cik)
        
        sector = ticker_to_sector.get(ticker)
        if not sector:
            sector = classifier.classify_by_sic(metadata.get('sic', ''))
        if not sector:
            sector = classifier.classify_by_keywords(metadata.get('name', ''))
        if not sector:
            sector = classifier.classify_by_keywords(ticker)
        
        print(f"  Sector: {sector or 'Unknown（除外項目なしで処理）'}")
        
        sector_exclusions = classifier.get_exclusions_for_sector(sector) if sector else []
        exclusion_item_ids = [ex['item_id'] for ex in sector_exclusions]
        
        # YTD累計SBCタグを四半期差分に変換
        SBC_YTD_TAGS = [
            'us-gaap:ShareBasedCompensation',
            'us-gaap:AllocatedShareBasedCompensationExpense',
            'us-gaap:EmployeeBenefitsAndShareBasedCompensation',
            'us-gaap:StockBasedCompensation',
        ]
        raw_sorted = sorted(quarterly_raw, key=lambda x: x['filing_date'])
        for sbc_tag in SBC_YTD_TAGS:
            ytd_key = f'_ytd_{sbc_tag}'
            ytd_by_fq = {}
            for pd in raw_sorted:
                ytd_dict = pd.get(ytd_key)
                if not ytd_dict:
                    continue
                ytd_val = ytd_dict.get('value', 0) if isinstance(ytd_dict, dict) else 0
                if ytd_val <= 0:
                    continue
                fy = pd.get('fiscal_year', int(pd['filing_date'][:4]))
                qn = pd.get('quarter', 0)
                if (fy, qn) not in ytd_by_fq or ytd_val > ytd_by_fq[(fy, qn)]:
                    ytd_by_fq[(fy, qn)] = ytd_val
            if not ytd_by_fq:
                continue
            applied = 0
            for pd in raw_sorted:
                fy = pd.get('fiscal_year', int(pd['filing_date'][:4]))
                qn = pd.get('quarter', 0)
                ytd_val = ytd_by_fq.get((fy, qn), 0)
                if ytd_val <= 0:
                    continue
                if qn <= 1:
                    qval = ytd_val
                else:
                    prev_ytd = ytd_by_fq.get((fy, qn - 1), 0)
                    qval = ytd_val - prev_ytd if prev_ytd > 0 else ytd_val
                if qval > 0:
                    pd[sbc_tag] = {'value': qval, 'unit': 'USD'}
                    applied += 1
            if applied:
                print(f"  [pipeline YTD→Q diff] {sbc_tag}: {applied}四半期に注入")

        quarterly_results = []
        for i, period_data in enumerate(quarterly_raw):
            print(f"\nProcessing quarter {i+1}/{len(quarterly_raw)}: {period_data['filing_date']} ({period_data['form']})")
            
            data = {
                "net_income": normalize_value(period_data.get("net_income")),
                "diluted_shares": normalize_value(period_data.get("diluted_shares")),
                "tax_expense": normalize_value(period_data.get("tax_expense")),
                "pretax_income": normalize_value(period_data.get("pretax_income")),
                "revenue": get_revenue(period_data),
                "filing_date": period_data["filing_date"],
                "form": period_data["form"],
                "raw_facts": {k: v for k, v in period_data.items() 
                            if k not in ["net_income", "diluted_shares", "tax_expense", "pretax_income", "filing_date", "form"]}
            }
            
            adjustments_raw = detect_adjustments(period_data, adjustment_config, sector, sector_exclusions)
            net_adjustment, detailed = apply_tax_adjustments(adjustments_raw, data)
            data["total_adjustments"] = net_adjustment
            
            result = calculate_eps(data, net_adjustment, detailed)
            result["filing_date"] = data["filing_date"]
            result["form"] = data["form"]
            result["net_adjustment_total"] = net_adjustment
            result["sector"] = sector
            result["sector_exclusions"] = exclusion_item_ids
            result["revenue"] = data.get("revenue", 0)
            result["diluted_shares"] = data.get("diluted_shares", 0)
            result["period_end"] = period_data.get("end", period_data["filing_date"])
            result["fiscal_year"] = period_data.get("fiscal_year")
            result["quarter"] = period_data.get("quarter")
            
            quarterly_results.append(result)
            
            print(f"  {result['filing_date']} ({result['form']}): "
                  f"GAAP EPS=${result['gaap_eps']:.4f} → "
                  f"Adj EPS=${result['adjusted_eps']:.4f}")
        
        # 成熟度監視
        if sector and quarterly_results:
            latest_for_monitor = max(quarterly_results, key=lambda x: x["filing_date"])
            _latest_raw = max(quarterly_raw, key=lambda x: x["filing_date"])
            _sbc_raw = (
                _latest_raw.get("us-gaap:ShareBasedCompensation") or
                _latest_raw.get("us-gaap:AllocatedShareBasedCompensationExpense") or
                {}
            )
            _sbc_val = _sbc_raw.get("value", 0) if isinstance(_sbc_raw, dict) else 0
            monitor = MaturityMonitor(maturity_config)
            maturity_status = monitor.monitor(quarterly_results, sector=sector,
                                              latest_override=latest_for_monitor,
                                              sbc_override=_sbc_val)
            if maturity_status.get('alert'):
                print(f"  ⚠ Maturity Alert for {ticker} ({sector}): {maturity_status['alert']}")
            _pending_maturity = maturity_status
        else:
            _pending_maturity = None
        
        # TTM・年次集計・AI分析
        ttm_results = []
        for i in range(3, len(quarterly_results)):
            ttm = calculate_ttm(quarterly_results, i)
            if ttm:
                ttm_results.append(ttm)
        
        annual_results = aggregate_annual(quarterly_results)
        
        if quarterly_results:
            quarterly_results.sort(key=lambda x: x["filing_date"], reverse=True)
            latest = quarterly_results[0]
            if _pending_maturity is not None:
                latest['maturity_monitor'] = _pending_maturity
            print(f"  [AI] Running analysis for latest quarter: {latest['filing_date']}")
            ai_result = analyze_adjustments(ticker, latest, latest.get("adjustments", []))
            try:
                latest["ai_analysis"] = json.loads(ai_result)
            except Exception as e:
                print(f"  [AI] Failed to parse AI result: {e}")
                latest["ai_analysis"] = {"health": "Error", "comment": str(ai_result), "sources": []}
        
        # 保存
        ticker_dir = os.path.join(DATA_ROOT, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        
        with open(os.path.join(ticker_dir, "quarterly.json"), "w", encoding="utf-8") as f:
            json.dump({
                "ticker": ticker,
                "last_updated": datetime.now().isoformat(),
                "quarters": quarterly_results
            }, f, indent=2, ensure_ascii=False)
        
        if ttm_results:
            with open(os.path.join(ticker_dir, "ttm.json"), "w", encoding="utf-8") as f:
                json.dump({"ticker": ticker, "last_updated": datetime.now().isoformat(), "ttm": ttm_results}, f, indent=2, ensure_ascii=False)
        
        if annual_results:
            with open(os.path.join(ticker_dir, "annual.json"), "w", encoding="utf-8") as f:
                json.dump({"ticker": ticker, "last_updated": datetime.now().isoformat(), "years": annual_results}, f, indent=2, ensure_ascii=False)
        
        all_tickers_data[ticker] = {
            "quarters": quarterly_results,
            "company_name": ticker_to_name.get(ticker, metadata.get('name', ''))
        }
        
        print(f"✓ {ticker} 保存完了: {ticker_dir}/")
    
    if all_tickers_data:
        summary = generate_summary(all_tickers_data)
        with open(os.path.join(DATA_ROOT, "summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        print("✓ summary.json 生成完了")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="AEA Pipeline")
    parser.add_argument("--ticker", type=str, default=None, help="更新する銘柄ティッカー（省略時は全銘柄）")
    args = parser.parse_args()
    run(ticker_filter=args.ticker)