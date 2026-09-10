"""
メインパイプライン（セクター別除外対応版）
"""
import yaml
import json
import os
import sys
import csv
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from .extract_key_facts import extract_quarterly_facts, normalize_value, get_cik as get_cik_func
from .adjustment_detector import detect_adjustments, get_sbc_xbrl_tags
from .tax_adjuster import apply_tax_adjustments
from .eps_calculator import calculate_eps
from .ai_analyzer import analyze_adjustments
from .sector_classifier_v2 import SectorClassifierV2
from .company_metadata import get_company_metadata
from .maturity_monitor import MaturityMonitor
from .fair_value_detector import apply_fair_value_detection

# プロジェクトルートを取得（pipeline.py の場所から3階層上）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
print("DEBUG: PROJECT_ROOT =", PROJECT_ROOT)

# common.sec_data.tickers を importできるようにプロジェクトルートをパスに追加
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from common.sec_data.tickers import get_eps_tickers, get_registrable_tickers

# ============================================
# Alpha Vantage API 差分検知機能
# ============================================
ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
EPS_DISCREPANCY_THRESHOLD = 0.20  # 20%以上の差異で警告
EPS_CHECK_TICKERS = ['SOUN', 'CELH']

def fetch_alpha_vantage_earnings(ticker: str) -> List[Dict]:
    """Alpha Vantage APIから四半期EPS情報を取得"""
    if not ALPHA_VANTAGE_API_KEY:
        print("  [AV] Warning: ALPHA_VANTAGE_API_KEY not set, skipping EPS discrepancy check")
        return []

    url = f"https://www.alphavantage.co/query?function=EARNINGS&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}"
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            # エラーレスポンスのチェック
            if "Error Message" in data or "Note" in data:
                error_msg = data.get("Error Message") or data.get("Note", "")
                print(f"  [AV] API error: {error_msg[:100]}...")
                return []
            # quarterlyEarningsを返す
            return data.get("quarterlyEarnings", [])
        else:
            print(f"  [AV] API error: {response.status_code}")
            return []
    except Exception as e:
        print(f"  [AV] Request failed: {e}")
        return []

def check_eps_discrepancy(ticker: str, quarterly_results: List[Dict]) -> Dict[str, Dict]:
    """
    XBRLから計算したEPSとAlpha Vantage APIの公式EPSを比較し、差異が大きい四半期を検出

    Returns:
        Dict[period_end, special_note_dict]
    """
    if not ALPHA_VANTAGE_API_KEY:
        return {}

    print(f"  [AV] Checking EPS discrepancy for {ticker}...")
    av_data = fetch_alpha_vantage_earnings(ticker)
    if not av_data:
        return {}

    # Alpha VantageデータをfiscalDateEndingでインデックス化
    av_by_date = {}
    for item in av_data:
        date_str = item.get('fiscalDateEnding', '')
        if date_str:
            av_by_date[date_str] = item

    discrepancies = {}

    for q in quarterly_results:
        period_end = q.get('period_end', q.get('filing_date', ''))
        if not period_end:
            continue

        av_item = av_by_date.get(period_end)
        if not av_item:
            continue

        # Alpha Vantageの公式値（reportedEPS）
        try:
            av_eps = float(av_item.get('reportedEPS', 0) or 0)
        except (ValueError, TypeError):
            av_eps = 0

        # XBRLから計算した値
        xbrl_eps = q.get('gaap_eps', 0)

        # 差異を計算（EPSベース）
        if av_eps and abs(av_eps) > 0.001:
            eps_diff_ratio = abs(xbrl_eps - av_eps) / abs(av_eps)
        else:
            eps_diff_ratio = 0

        # 閾値を超えたら警告
        if eps_diff_ratio > EPS_DISCREPANCY_THRESHOLD:
            print(f"    [AV] Discrepancy detected for {period_end}:")
            print(f"          XBRL EPS: ${xbrl_eps:.4f}, Official EPS: ${av_eps:.4f} (diff: {eps_diff_ratio*100:.1f}%)")

            discrepancies[period_end] = {
                'flag': 'EPS_DISCREPANCY',
                'xbrl_eps': xbrl_eps,
                'official_eps': av_eps,
                'eps_diff_pct': round(eps_diff_ratio * 100, 1),
                'note': (
                    f"XBRLと公式発表のGAAP EPSに{eps_diff_ratio*100:.0f}%の差異があります。"
                    f"公式EPS: ${av_eps:.2f}, XBRL計算EPS: ${xbrl_eps:.2f}。"
                    f"買収関連負債の公正価値変動など、特殊な会計処理が影響している可能性があります。"
                    f"当ツールのAdj EPSはこれらの一過性項目を除外した実力ベースの値です。"
                )
            }

    if discrepancies:
        print(f"  [AV] Found {len(discrepancies)} quarters with EPS discrepancy")
        print(f"  [AV] DEBUG: discrepancies keys = {list(discrepancies.keys())}")
    else:
        print(f"  [AV] No significant EPS discrepancy found")

    return discrepancies

def resolve_split_history_path() -> Optional[str]:
    """split_history.yamlのパス解決ロジック（report_consistency_check.pyの
    設定ファイル読み込み横断チェックと共用するため、2026-08-16に
    load_split_history()から切り出した。[[CONFIG-LOAD-SILENT-FALLBACK-1]]）。

    Returns:
        解決できたパス（存在確認済み）、解決できなければNone
    """
    path = os.path.join(PROJECT_ROOT, "config", "split_history.yaml")
    return path if os.path.exists(path) else None


def load_split_history() -> Dict[str, List[Dict]]:
    """config/split_history.yaml を読み込む。ファイルがなければ {} を返す。"""
    path = resolve_split_history_path()
    if path is None:
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return data.get("splits", {})
    except Exception as e:
        print(f"Warning: split_history.yaml 読み込みエラー: {e}")
        return {}


def apply_split_adjustments(ticker: str, quarterly_results: List[Dict], split_history: Dict) -> List[Dict]:
    """
    株式分割の遡及補正を quarterly_results に適用する。
    分割前の期間（period_end < split_date）で pre-split 株数のままの四半期を補正。
    SEC XBRL は旧ファイリングを遡及修正しないため、分割前提出の10-Qでは
    diluted_shares が分割前のままになる。この関数で一括補正する。
    """
    splits = split_history.get(ticker, [])
    if not splits:
        return quarterly_results

    for split in splits:
        split_date = split['date']
        ratio = float(split['ratio'])

        # 分割後四半期の平均株数（閾値計算に使用）
        post_split_shares = [
            q.get('diluted_shares_used', 0)
            for q in quarterly_results
            if q.get('period_end', q.get('filing_date', '')) >= split_date
            and q.get('diluted_shares_used', 0) > 0
        ]
        if not post_split_shares:
            print(f"  [SPLIT] {ticker}: 分割後データなし, スキップ (split_date={split_date})")
            continue

        post_split_avg = sum(post_split_shares) / len(post_split_shares)
        # 1.5倍の余裕: 比較期間から取得済みで既に補正されている四半期を誤補正しない
        pre_split_threshold = post_split_avg / ratio * 1.5

        print(f"  [SPLIT] {ticker}: date={split_date}, ratio={ratio}x, "
              f"post_avg={post_split_avg:,.0f}, threshold<{pre_split_threshold:,.0f}")

        adjusted_count = 0
        for q in quarterly_results:
            period_end = q.get('period_end', q.get('filing_date', ''))
            if period_end >= split_date:
                continue
            shares = q.get('diluted_shares_used', 0)
            if shares <= 0 or shares > pre_split_threshold:
                continue  # すでに分割後株数に補正済みの四半期はスキップ

            q['diluted_shares_used'] = shares * ratio
            q['diluted_shares'] = q.get('diluted_shares', shares) * ratio
            q['gaap_eps'] = q.get('gaap_eps', 0) / ratio
            q['adjusted_eps'] = q.get('adjusted_eps', 0) / ratio
            q['split_adjusted'] = True
            adjusted_count += 1

        print(f"  [SPLIT] {ticker}: {adjusted_count}四半期を {ratio}x 分割補正済み")

    return quarterly_results


def apply_dta_adjustments(ticker: str, quarterly_results: List[Dict]) -> List[Dict]:
    """
    DTA（繰延税金資産）認識による adj_eps 異常高値を検出・補正する。

    検出条件（すべて満たす場合）:
      1. tax_expense < 0  （大規模税メリット）
      2. |tax_expense| > pretax_income * 0.5  （税メリットが税引前利益の50%超）
      3. pretax_income > 0  （黒字四半期）
      4. net_income > pretax_income * 3  （NI が税引前利益の3倍超 = DTA膨張）

    補正方法:
      - 他の正常四半期の tax_expense 中央値を代替税費用として使用
      - adjusted_net_income = pretax_income - median_normal_tax
      - adjusted_eps を更新（gaap_eps は変更しない）
    """
    import statistics

    for i, result in enumerate(quarterly_results):
        tax_expense  = result.get("tax_expense", 0) or 0
        net_income   = result.get("gaap_net_income", 0) or 0
        pretax       = result.get("pretax_income", 0) or 0
        diluted      = result.get("diluted_shares_used", 0) or 0

        # pretax_income がない場合は推定
        if not pretax and net_income and tax_expense:
            pretax = net_income + tax_expense

        # 検出条件:
        #   (A) pretax <= 0 なのに net_income > 0  (LYFT型: 税引前損失→DTA還付で黒字化)
        #   (B) pretax > 0  かつ net_income > pretax * 3  (税引前黒字→DTA膨張)
        #   + tax_expense < 0 かつ |tax_expense| > |net_income| * 0.5  (大規模DTA)
        is_type_a = (pretax <= 0 and net_income > 0)
        is_type_b = (pretax > 0 and net_income > pretax * 3)
        if not (tax_expense < 0
                and abs(tax_expense) > abs(net_income) * 0.5
                and (is_type_a or is_type_b)):
            continue

        # 正常四半期（tax_expense > 0）の中央値
        normal_tax = [q.get("tax_expense", 0) for j, q in enumerate(quarterly_results)
                      if j != i and (q.get("tax_expense", 0) or 0) > 0]
        if not normal_tax:
            print(f"  [DTA] {result['filing_date']}: DTA検出したが正常四半期の税データなし → スキップ")
            continue

        median_tax = statistics.median(normal_tax)
        adj_net    = pretax - median_tax
        adj_eps    = adj_net / diluted if diluted else 0

        print(f"  [DTA detected] {ticker} {result['filing_date']}: "
              f"tax={tax_expense:,.0f}, pretax={pretax:,.0f}, net={net_income:,.0f} "
              f"→ median_tax={median_tax:,.0f}, adj_net={adj_net:,.0f}, adj_eps={adj_eps:.4f}")

        result["adjusted_net_income"] = adj_net
        result["adjusted_eps"]        = adj_eps
        result["net_adjustment_total"] = adj_net - net_income
        result["dta_detected"]        = True
        result["adjustments"] = result.get("adjustments", []) + [{
            "item_id":   "tax_one_time_dta",
            "item_name": "DTA認識（繰延税金資産）除外",
            "amount":    adj_net - net_income,
            "net_amount": adj_net - net_income,
            "note":      f"tax_expense={tax_expense:,.0f}, median_normal_tax={median_tax:,.0f}",
        }]

    return quarterly_results


# [[EPS-LOAR-1]]: 直近（最新）四半期のdiluted_sharesに対する比率の閾値。
# IPO前・株式併合前等で株式数の構造自体が別物になっている四半期を検知する
# （|EPS|等の絶対値閾値では一部の異常値〈adjusted_eps=3.32等〉を見逃すため、
# 根本原因〈株式数が別物〉に直結する株式数基準を採用）。
SHARE_STRUCTURE_MISMATCH_RATIO = 0.01


def apply_share_structure_filter(ticker: str, quarterly_results: List[Dict]) -> List[Dict]:
    """
    [[EPS-LOAR-1]]: 直近四半期のdiluted_sharesと比較して
    SHARE_STRUCTURE_MISMATCH_RATIO（1%）未満の四半期を「株式数構造が
    別物」としてフラグする（LOARのIPO前204,000株等）。

    削除はせず special_flags=["SHARE_STRUCTURE_MISMATCH"] を付与するのみ
    （quarterly.jsonには残し監査可能性を維持、表示側・集計側でスキップする
    設計）。calculate_ttm()・aggregate_annual()はこのフラグを見て、
    フラグ付き四半期を含むTTM窓・年度を除外する。
    """
    valid = [q for q in quarterly_results if (q.get('diluted_shares', 0) or 0) > 0]
    if not valid:
        return quarterly_results

    latest = max(valid, key=lambda x: x["filing_date"])
    latest_shares = latest.get('diluted_shares', 0) or 0
    if latest_shares <= 0:
        return quarterly_results

    threshold = latest_shares * SHARE_STRUCTURE_MISMATCH_RATIO
    excluded = []
    for q in quarterly_results:
        shares = q.get('diluted_shares', 0) or 0
        if 0 < shares < threshold:
            q['special_flags'] = q.get('special_flags', []) + ['SHARE_STRUCTURE_MISMATCH']
            q['special_notes'] = q.get('special_notes', {})
            q['special_notes']['share_structure_mismatch'] = (
                f"diluted_shares={shares:,.0f} は直近四半期"
                f"({latest['filing_date']}, {latest_shares:,.0f}株)の"
                f"{SHARE_STRUCTURE_MISMATCH_RATIO:.0%}未満のため、株式数構造が"
                f"別物（IPO前/株式併合前等）と判定し表示・TTM・年次集計から除外"
            )
            excluded.append(q['filing_date'])

    if excluded:
        print(f"  [Share Structure Filter] {ticker}: {len(excluded)}四半期を株式数基準で除外フラグ "
              f"(閾値: latest={latest_shares:,.0f}×{SHARE_STRUCTURE_MISMATCH_RATIO:.0%}={threshold:,.0f}) "
              f"→ {', '.join(excluded)}")

    return quarterly_results


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
    # [[EPS-LOAR-1]]: 株式数構造が別物の四半期（SHARE_STRUCTURE_MISMATCH）を
    # 1件でも含む窓はTTMとして無意味なため丸ごと除外する（部分的な差し替えは
    # しない。IPO前後で株式数の意味自体が異なり平均しても実態を表さないため）。
    if any('SHARE_STRUCTURE_MISMATCH' in (q.get('special_flags') or []) for q in ttm_data):
        return None
    if len(ttm_data) < 4:
        return None
    total_net_income = sum(q["gaap_net_income"] for q in ttm_data)
    total_adjustments = sum(q.get("net_adjustment_total", 0) for q in ttm_data)
    avg_shares = sum(q["diluted_shares_used"] for q in ttm_data) / 4
    # [[NETINCOME-DUAL-PIPELINE-1]]: periodに"TTM "を明示的に前置し、STONKS SILO
    # の単年度net_income_fyとの混同を防ぐ（NAMING_CONVENTIONS.md規則2）。
    # netincomeキー自体は改名しない（stock.htmlのupdateChart()等にttm[].
    # net_incomeへの直接参照が存在しないことを確認済み、2026-08-13）。
    # 既存の"{start} to {end}"形式・末尾日付を`.split(' to ')[1]`で抽出する
    # フロントエンド側の解析（stock.html）はプレフィックス追加後も影響を
    # 受けない（' to '区切り自体は変わらないため）。
    return {
        "period": f"TTM {ttm_data[0]['filing_date']} to {ttm_data[-1]['filing_date']}",
        "net_income": total_net_income,
        "adjusted_income": total_net_income + total_adjustments,
        "diluted_shares": avg_shares,
        "eps": total_net_income / avg_shares if avg_shares else 0,
        "adjusted_eps": (total_net_income + total_adjustments) / avg_shares if avg_shares else 0
    }

def aggregate_annual(quarterly_results: List[Dict]) -> List[Dict]:
    # fiscal_year フィールドを使ってグループ化（extract_key_facts.py が常に設定する）
    # fiscal_year が None の場合は filing_date[:4] にフォールバックするが、
    # 非12月決算企業では Q1 等で誤グループ化が起きる可能性があるため警告を出す
    #
    # [[EPS-LOAR-1]]: 株式数構造が別物の四半期（SHARE_STRUCTURE_MISMATCH）は
    # 年度集計の対象から除外する。除外後4四半期に満たない年度（LOAR FY2023の
    # 全4四半期・FY2024の1四半期が該当）は下の len(quarters) < 4 で自然に
    # スキップされ、意味のある年度のみannual.jsonに出力される。
    annual_map = {}
    for q in quarterly_results:
        if 'SHARE_STRUCTURE_MISMATCH' in (q.get('special_flags') or []):
            continue
        fy = q.get("fiscal_year")
        if fy is not None:
            year = str(fy)
        else:
            year = q["filing_date"][:4]
            print(
                f"[WARN] aggregate_annual: fiscal_year 未設定 filing_date={q.get('filing_date', '?')}"
                f" → {year} にフォールバック（非12月決算企業では誤集計の可能性あり）"
            )
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

def generate_summary(tickers_data: Dict[str, Dict], existing_summary_path: str = None) -> Dict:
    """
    サマリーを生成。既存のsummary.jsonがあればマージする。
    """
    # 既存のサマリーを読み込み
    existing_tickers = {}
    if existing_summary_path and os.path.exists(existing_summary_path):
        try:
            with open(existing_summary_path, 'r', encoding='utf-8') as f:
                existing = json.load(f)
                for t in existing.get("tickers", []):
                    existing_tickers[t["ticker"]] = t
            print(f"  [Summary] Loaded {len(existing_tickers)} existing tickers from summary.json")
        except Exception as e:
            print(f"  [Summary] Warning: Could not load existing summary.json: {e}")

    # 新しいデータで更新
    for ticker, data in tickers_data.items():
        # [[EPS-LOAR-1]]: 株式数構造が別物の四半期はsummary.jsonの
        # latest/YoY計算からも除外する（quarters[0]は常に真の最新四半期の
        # ため通常は影響しないが、IPO直後で該当四半期が直近5件以内に
        # 入るティッカーへの副作用を防ぐための防御的措置）
        _quarters = [
            q for q in data.get("quarters", [])
            if 'SHARE_STRUCTURE_MISMATCH' not in (q.get('special_flags') or [])
        ]
        if _quarters:
            latest = _quarters[0]
            yoy_growth = None
            if len(_quarters) >= 5:
                prev = _quarters[4]
                if prev["adjusted_eps"] != 0:
                    yoy_growth = (latest["adjusted_eps"] - prev["adjusted_eps"]) / abs(prev["adjusted_eps"])

            # EPS差分比率からhealthを自動計算
            gaap_eps = latest["gaap_eps"]
            adj_eps  = latest["adjusted_eps"]
            ratio = (adj_eps - gaap_eps) / abs(gaap_eps) * 100 if gaap_eps != 0 else 0
            if ratio == 0:
                health = "調整なし"
            elif ratio > 0:
                if ratio <= 20:
                    health = "調整小"
                elif ratio <= 80:
                    health = "調整中"
                else:
                    health = "調整大"
            else:
                if ratio >= -20:
                    health = "調整小（マイナス）"
                else:
                    health = "過大調整"
            # GAAPで赤字→Adjで黒字は強制的に「調整大」
            if gaap_eps < 0 and adj_eps > 0:
                health = "調整大"

            existing_tickers[ticker] = {
                "ticker": ticker,
                "company_name": data.get("company_name", ""),
                "latest_filing_date": latest["filing_date"],
                "gaap_eps": gaap_eps,
                "adjusted_eps": adj_eps,
                "eps_diff": round(adj_eps - gaap_eps, 4),
                "eps_ratio": round(ratio, 1),
                "gaap_to_adj_positive": gaap_eps < 0 and adj_eps > 0,
                "yoy_growth": yoy_growth,
                "health": health
            }

    summary = {
        "last_updated": datetime.now().isoformat(),
        "tickers": list(existing_tickers.values())
    }
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


def process_one_ticker(ticker, adjustment_config, classifier, ticker_to_name,
                       DATA_ROOT, SBC_YTD_TAGS, maturity_config, split_history=None):
    """1銘柄を処理して (ticker, data_dict) を返す。エラー時は (ticker, None)。"""
    try:
        print(f"\n=== Processing {ticker} ===")

        quarterly_raw = extract_quarterly_facts(ticker, years=10)
        if not quarterly_raw:
            print(f"{ticker}: データなし")
            return (ticker, None)

        try:
            cik = get_cik_func(ticker)
        except:
            cik = None

        metadata = {}
        if cik:
            metadata = get_company_metadata(cik)

        sector = classifier.classify(
            ticker,
            sic_code=metadata.get('sic', ''),
            company_name=metadata.get('name', ''),
        )

        print(f"  Sector: {sector or 'Unknown（除外項目なしで処理）'}")

        sector_exclusions = classifier.get_exclusions_for_sector(sector) if sector else []
        exclusion_item_ids = [ex['item_id'] for ex in sector_exclusions]

        # YTD累計SBCタグを四半期差分に変換（動的タグリスト使用）
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
            result["tax_expense"] = data.get("tax_expense", 0) or 0
            result["pretax_income"] = data.get("pretax_income", 0) or 0

            quarterly_results.append(result)

            print(f"  {result['filing_date']} ({result['form']}): "
                  f"GAAP EPS=${result['gaap_eps']:.4f} → "
                  f"Adj EPS=${result['adjusted_eps']:.4f}")

        # 株式分割遡及補正
        if split_history:
            quarterly_results = apply_split_adjustments(ticker, quarterly_results, split_history)

        # DTA（繰延税金資産）認識による異常 adj_eps の検出・補正
        quarterly_results = apply_dta_adjustments(ticker, quarterly_results)

        # 株式数構造の別物検知（IPO前/株式併合前等、[[EPS-LOAR-1]]）
        quarterly_results = apply_share_structure_filter(ticker, quarterly_results)

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

        # EPS差分検知（Alpha Vantage API vs XBRL）
        if ticker in EPS_CHECK_TICKERS:
            eps_discrepancies = check_eps_discrepancy(ticker, quarterly_results)
            if eps_discrepancies:
                print(f"  [AV] Applying discrepancies to {len(eps_discrepancies)} quarters...")
                matched_count = 0
                for q in quarterly_results:
                    period_end = q.get('period_end', q.get('filing_date', ''))
                    if period_end in eps_discrepancies:
                        q['special_flags'] = q.get('special_flags', []) + ['EPS_DISCREPANCY']
                        q['special_notes'] = q.get('special_notes', {})
                        q['special_notes']['eps_discrepancy'] = eps_discrepancies[period_end]
                        matched_count += 1
                print(f"  [AV] Applied special_flags to {matched_count} quarters")
        else:
            print(f"  [AV] Skipping EPS discrepancy check for {ticker} (not in EPS_CHECK_TICKERS)")

        # 公正価値変動の自動検出・調整項目化
        print(f"  [FV Auto] Running fair value auto-detection for {ticker}...")
        quarterly_results = apply_fair_value_detection(quarterly_raw, quarterly_results)

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

        print(f"✓ {ticker} 保存完了: {ticker_dir}/")

        return (ticker, {
            "quarters": quarterly_results,
            "company_name": ticker_to_name.get(ticker, metadata.get('name', ''))
        })

    except Exception as e:
        import traceback
        print(f"✗ {ticker} 処理エラー: {e}")
        traceback.print_exc()
        return (ticker, None)


def _filter_eps_tickers(target, eps_tickers):
    """CLI引数等でticker明示指定時にeps=trueのみへ絞り込む。

    FLAG-CONSUMER-AUDIT-3: --ticker明示指定時にeps=trueフラグの検証を
    一切行っていなかった（tanuki_valuation/pipeline.pyのCLI引数パスと
    同型のギャップ）。範囲外のticker指定は警告し無条件実行しない。
    """
    eps_set = set(eps_tickers)
    excluded = [t for t in target if t not in eps_set]
    if excluded:
        print(
            f"警告: eps=false のため除外: {', '.join(excluded)}"
            f"（cik_lookup.csvでeps=trueに変更しない限り処理されません）"
        )
    return [t for t in target if t in eps_set]


# [[WORKFLOW-FALLBACK-CRON-DUPLICATE-1]]対応（案2: 冪等性ガード）:
# Adjusted_Eps_Analyzer_update.ymlはworkflow_run連鎖成功後も週次
# フォールバックcronが無条件に追加実行され、実行履歴で直近3週中2週
# （08-30/31・09-06/07）で約19時間差の無条件重複を実測確認した。
# summary.jsonのlast_updatedが本閾値未満なら「直近で既に完了済み」と
# 判断しGrok呼び出しを含む本処理をスキップする。SEC Data Update失敗週
# （workflow_run側skipped）はsummary.jsonが更新されないため、フォール
# バックcronは従来通り正常に「真の安全網」として機能する。
_RECENT_UPDATE_THRESHOLD_HOURS = 24


def _recently_completed_at(data_root: str, threshold_hours: float = _RECENT_UPDATE_THRESHOLD_HOURS) -> Optional[str]:
    """summary.jsonのlast_updatedが閾値時間以内なら、そのISO文字列を返す
    （＝直近で完了済みと判定）。閾値超過・ファイル不在・パース失敗時は
    None（＝通常通り処理継続すべき）を返す。"""
    summary_path = os.path.join(data_root, "summary.json")
    if not os.path.exists(summary_path):
        return None
    try:
        with open(summary_path, 'r', encoding='utf-8') as f:
            last_updated_str = json.load(f).get("last_updated")
        if not last_updated_str:
            return None
        last_updated = datetime.fromisoformat(last_updated_str)
        if datetime.now() - last_updated < timedelta(hours=threshold_hours):
            return last_updated_str
    except (ValueError, TypeError, json.JSONDecodeError, OSError):
        pass
    return None


def run(ticker_filter: str = None, force: bool = False):
    config_base = os.path.join(PROJECT_ROOT, "config")
    DATA_ROOT = os.path.join(PROJECT_ROOT, "docs", "value-monitor", "adjusted_eps_analyzer", "data")

    # [[WORKFLOW-FALLBACK-CRON-DUPLICATE-1]]対応（案2: 冪等性ガード）。
    # 全銘柄バッチ経路（ticker_filter未指定）のみ対象とする。--ticker
    # 明示指定時（新規銘柄登録オーケストレーション等）はスキップ対象外。
    if not ticker_filter and not force:
        recent = _recently_completed_at(DATA_ROOT)
        if recent:
            print(
                f"[Idempotency Guard] summary.jsonの最終更新は{recent}"
                f"（{_RECENT_UPDATE_THRESHOLD_HOURS}時間以内）のため、"
                f"意図的にスキップします（[[WORKFLOW-FALLBACK-CRON-"
                f"DUPLICATE-1]]対応: 週次フォールバックcronによる無条件"
                f"重複実行の防止。強制実行するには --force を指定）"
            )
            return

    tickers = get_eps_tickers()

    if ticker_filter:
        with open(os.path.join(config_base, "monitor_tickers.yaml"), 'r', encoding='utf-8') as f:
            monitor_tickers = yaml.safe_load(f)["tickers"]
        requested = [t.strip().upper() for t in ticker_filter.split(',') if t.strip()]
        for t in requested:
            if t not in monitor_tickers:
                print(f"Warning: {t} は monitor_tickers.yaml に未登録ですが処理を続行します")
        # get_eps_tickers()ではなくget_registrable_tickers()を使う:
        # 前者はstatus=provisioning（登録処理中、[[REGISTER-FLOW-
        # REDESIGN-1]]方針2）を除外するため、新規銘柄登録
        # オーケストレーション（common/registration/register_ticker.py
        # Step 5b）がprovisioning中のティッカーを明示指定して実行
        # できなくなってしまう（2026-09-03）。デフォルト・バッチ経路
        # （ticker_filter未指定）は上のtickers=get_eps_tickers()の
        # ままprovisioningを除外する。
        tickers = _filter_eps_tickers(requested, get_registrable_tickers("eps"))

    with open(os.path.join(config_base, "adjustment_items.json"), 'r', encoding='utf-8') as f:
        adjustment_config = json.load(f)

    cik_lookup_path = os.path.join(PROJECT_ROOT, "config", "cik_lookup.csv")
    classifier = SectorClassifierV2(
        os.path.join(PROJECT_ROOT, "config", "sectors.yaml"),
        cik_lookup_path=cik_lookup_path,
    )

    cik_data = load_cik_data()
    ticker_to_name = {row['ticker']: row.get('name', '') for row in cik_data}

    maturity_config = adjustment_config.get('maturity_defaults', {})
    split_history = load_split_history()

    all_tickers_data = {}
    lock = threading.Lock()

    # ★ YTD 変換用の基本タグ（固定）
    BASE_SBC_YTD_TAGS = [
        'us-gaap:ShareBasedCompensation',
        'us-gaap:AllocatedShareBasedCompensationExpense',
        'us-gaap:EmployeeBenefitsAndShareBasedCompensation',
        'us-gaap:StockBasedCompensation',
        'us-gaap:ShareBasedCompensationExpense',
        'us-gaap:RestrictedStockExpense',
    ]
    # adjustment_items.json から SBC 関連タグを動的に取得しマージ
    dynamic_sbc_tags = get_sbc_xbrl_tags()
    SBC_YTD_TAGS = list(set(BASE_SBC_YTD_TAGS + dynamic_sbc_tags))
    print(f"  [pipeline] SBC YTD tags: {SBC_YTD_TAGS}")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(
                process_one_ticker, ticker, adjustment_config, classifier,
                ticker_to_name, DATA_ROOT, SBC_YTD_TAGS, maturity_config, split_history
            ): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            _ticker = futures[future]
            try:
                result_ticker, result_data = future.result()
                if result_data is not None:
                    with lock:
                        all_tickers_data[result_ticker] = result_data
            except Exception as e:
                print(f"✗ {_ticker} 未捕捉エラー: {e}")

    if all_tickers_data:
        summary_path = os.path.join(DATA_ROOT, "summary.json")
        summary = generate_summary(all_tickers_data, existing_summary_path=summary_path)
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        print(f"✓ summary.json 生成完了 (全{len(summary['tickers'])}銘柄)")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="AEA Pipeline")
    parser.add_argument("--ticker", type=str, default=None, help="更新する銘柄ティッカー（省略時は全銘柄）")
    parser.add_argument("--force", action="store_true",
                         help="[[WORKFLOW-FALLBACK-CRON-DUPLICATE-1]]対応の冪等性ガードを"
                              "無視して強制実行する（手動re-run等の正当な再実行用）")
    args = parser.parse_args()
    run(ticker_filter=args.ticker, force=args.force)
