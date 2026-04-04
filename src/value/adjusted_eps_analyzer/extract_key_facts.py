"""
extract_key_facts.py
SEC EDGAR CompanyFacts API からXBRLデータを取得し、四半期ごとに集約して返す

pipeline.py が期待する戻り値フォーマット:
[
  {
    "filing_date": "2024-12-31",
    "form": "10-K",
    "fiscal_year": 2024,
    "quarter": 4,
    "end": "2024-12-31",
    "net_income": {"value": 123456000, "unit": "USD"},
    "diluted_shares": {"value": 1500000000, "unit": "shares"},
    "pretax_income": {"value": 160000000, "unit": "USD"},
    "tax_expense": {"value": 36544000, "unit": "USD"},
    "us-gaap:ShareBasedCompensation": {"value": 55000000, "unit": "USD"},
    "_ytd_us-gaap:ShareBasedCompensation": {"value": 220000000, "unit": "USD"},
    ... (他のXBRLタグも同様)
  },
  ...
]
"""
import requests
import json
import os
import time
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# ====================== 設定 ======================
# プロジェクトルートを取得（extract_key_facts.py → adjusted_eps_analyzer → value → src → ROOT）
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_SCRIPT_DIR)))

CIK_FILE = os.path.join(_PROJECT_ROOT, "config", "cik_lookup.csv")
ADJUSTMENT_ITEMS_FILE = os.path.join(_PROJECT_ROOT, "config", "adjustment_items.json")

# ★ 以前の動作版と同じシンプルなヘッダー（SECが要求する形式）
HEADERS = {
    'User-Agent': 'jamablue01@gmail.com',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'data.sec.gov'
}


# ====================== ヘルパー ======================

def normalize_value(value: Any) -> float:
    """
    スカラー値 または {"value": ..., "unit": ...} dict を float に変換する。
    pipeline.py から normalize_value(period_data.get("net_income")) のように呼ばれるため、
    両方のフォーマットに対応する必要がある。
    """
    if value is None:
        return 0.0
    # dict形式 {"value": ..., "unit": ...}
    if isinstance(value, dict):
        v = value.get('value')
        if v is None:
            return 0.0
        return normalize_value(v)  # 再帰で数値変換
    # 数値
    if isinstance(value, (int, float)):
        return float(value)
    # 文字列
    if isinstance(value, str):
        try:
            return float(value.replace(',', ''))
        except (ValueError, TypeError):
            return 0.0
    return 0.0


def get_cik(ticker: str) -> str:
    """cik_lookup.csv からティッカーに対応するCIKを取得（10桁ゼロ埋め）"""
    try:
        with open(CIK_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('ticker', '').strip().upper() == ticker.upper():
                    return str(row['cik']).strip().zfill(10)
    except Exception as e:
        print(f"Error reading CIK file: {e}")
    raise Exception(f"CIK not found for {ticker}. Please add to {CIK_FILE}")


def load_required_xbrl_tags() -> List[str]:
    """
    adjustment_items.json から取得すべきXBRLタグの一覧を返す。
    基本タグ（net_income, diluted_shares等）+ 調整項目タグ + 売上高系タグ
    """
    tags = set()

    # adjustment_items.json から動的に取得
    try:
        with open(ADJUSTMENT_ITEMS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for cat in data.get("categories", []):
                for sub in cat.get("sub_items", []):
                    # ★ 複数形 xbrl_tags に対応
                    for tag_name in sub.get('xbrl_tags', []):
                        # "us-gaap:" プレフィックスを除去したタグ名を登録
                        clean = tag_name.replace('us-gaap:', '')
                        tags.add(clean)
    except FileNotFoundError:
        print(f"Warning: {ADJUSTMENT_ITEMS_FILE} not found")
    except Exception as e:
        print(f"Warning: Error loading adjustment_items.json: {e}")

    # === 基本タグ（必須） ===
    # 純利益
    tags.update([
        'NetIncomeLoss',
        'NetIncomeLossAvailableToCommonStockholdersBasic',
        'ProfitLoss',
    ])
    # 希薄化後株式数
    tags.update([
        'WeightedAverageNumberOfDilutedSharesOutstanding',
        'WeightedAverageNumberOfSharesOutstandingBasic',
        'CommonStockSharesOutstanding',
    ])
    # 税前利益・税額
    tags.update([
        'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
        'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
        'IncomeLossBeforeIncomeTaxExpenseBenefit',
        'IncomeTaxExpenseBenefit',
    ])
    # 売上高系
    tags.update([
        'Revenues',
        'RevenueFromContractWithCustomer',
        'RevenueFromContractWithCustomerExcludingAssessedTax',
        'RevenueFromContractWithCustomerIncludingAssessedTax',
        'NetSales',
        'TotalRevenue',
        'SalesRevenueNet',
        'RevenuesNetOfInterestExpense',
        'NetInterestIncome',
        'InterestIncomeExpenseNet',
        'InterestAndDividendIncomeOperating',
        'NoninterestIncome',
    ])
    # 営業CF・CAPEX（参考用）
    tags.update([
        'NetCashProvidedByUsedInOperatingActivities',
        'PaymentsForPropertyPlantAndEquipment',
    ])

    return list(tags)


# ====================== 期間判定ヘルパー ======================

def _is_quarterly_duration(start: str, end: str) -> bool:
    """期間が1四半期（約90日）かどうか"""
    try:
        d_start = datetime.strptime(start, '%Y-%m-%d')
        d_end = datetime.strptime(end, '%Y-%m-%d')
        days = (d_end - d_start).days
        return 60 <= days <= 115
    except:
        return False


def _is_annual_duration(start: str, end: str) -> bool:
    """期間が1年間（約365日）かどうか"""
    try:
        d_start = datetime.strptime(start, '%Y-%m-%d')
        d_end = datetime.strptime(end, '%Y-%m-%d')
        days = (d_end - d_start).days
        return 340 <= days <= 400
    except:
        return False


# ====================== メイン関数 ======================

def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict]:
    """
    SEC CompanyFacts APIから四半期別にデータを集約して返す。

    Returns:
        List[Dict]: 四半期ごとのdict。各dictは以下のキーを持つ:
            - filing_date: str (end date, e.g. "2024-12-31")
            - form: str ("10-Q" or "10-K")
            - fiscal_year: int
            - quarter: int (1-4)
            - end: str (end date)
            - net_income: {"value": float, "unit": "USD"}
            - diluted_shares: {"value": float, "unit": "shares"}
            - pretax_income: {"value": float, "unit": "USD"}
            - tax_expense: {"value": float, "unit": "USD"}
            - us-gaap:TagName: {"value": float, "unit": str} (各XBRLタグ)
            - _ytd_us-gaap:TagName: {"value": float, "unit": str} (YTD累計値)
    """
    cik = get_cik(ticker)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    print(f"   [DEBUG {ticker}] extract_quarterly_facts 開始")
    print(f"   CIK: {cik}")
    print(f"   Fetching from {url}")

    # --- APIリクエスト（以前の動作版と同じシンプルな形式） ---
    try:
        print(f"   [DEBUG {ticker}] APIリクエスト送信中...")
        resp = requests.get(url, headers=HEADERS, timeout=30)
        print(f"   [DEBUG {ticker}] レスポンス: {resp.status_code}")
        
        if resp.status_code != 200:
            print(f"   Error fetching company facts: {resp.status_code} {resp.reason}")
            return []
        
        print(f"   [DEBUG {ticker}] データ取得成功")
    except requests.exceptions.Timeout:
        print(f"   [DEBUG {ticker}] タイムアウト")
        return []
    except requests.exceptions.ConnectionError as e:
        print(f"   [DEBUG {ticker}] 接続エラー: {e}")
        return []
    except Exception as e:
        print(f"   [DEBUG {ticker}] リクエスト例外: {e}")
        return []

    print(f"   [DEBUG {ticker}] JSONパース開始...")
    all_facts = resp.json().get('facts', {})
    required_tags = load_required_xbrl_tags()
    cutoff = datetime.now() - timedelta(days=365 * years + 180)

    # --- Step 1: 全レコードを収集 ---
    records = []

    for namespace in ['us-gaap', 'ifrs-full', 'dei']:
        ns_facts = all_facts.get(namespace, {})
        for tag_name in required_tags:
            if tag_name not in ns_facts:
                continue
            tag_data = ns_facts[tag_name]
            units = tag_data.get('units', {})
            for unit_type, values in units.items():
                for v in values:
                    form = v.get('form', '')
                    if form not in ('10-Q', '10-K'):
                        continue
                    try:
                        end_str = v.get('end', '')
                        start_str = v.get('start', '')
                        end_date = datetime.strptime(end_str, '%Y-%m-%d')
                        if end_date < cutoff:
                            continue
                        val = v.get('val', 0)
                        if val is None:
                            continue
                        val = float(val)

                        tag_full = f"us-gaap:{tag_name}"
                        records.append({
                            'tag': tag_full,
                            'end': end_str,
                            'start': start_str,
                            'value': val,
                            'unit': unit_type,
                            'form': form,
                            'fy': v.get('fy'),
                            'fp': v.get('fp', ''),
                            'filed': v.get('filed', ''),
                        })
                    except (ValueError, TypeError):
                        continue

    print(f"   {ticker}: {len(records)}件のレコードを取得")

    if not records:
        return []

    # --- Step 2: (end, fy, quarter) をキーとして四半期を集約 ---
    quarterly_map: Dict[tuple, Dict[str, Any]] = {}

    for rec in records:
        end_str = rec['end']
        start_str = rec['start']
        form = rec['form']
        fy = rec['fy']
        fp = rec['fp']
        tag = rec['tag']
        val = rec['value']
        unit = rec['unit']

        fp_upper = (fp or '').upper().strip()

        if fp_upper in ('Q1', 'Q2', 'Q3'):
            q_num = {'Q1': 1, 'Q2': 2, 'Q3': 3}[fp_upper]

            key = (end_str, fy, q_num)
            if key not in quarterly_map:
                quarterly_map[key] = {
                    'filing_date': end_str,
                    'form': form,
                    'fiscal_year': fy,
                    'quarter': q_num,
                    'end': end_str,
                }

            # 期間が四半期でない場合（YTD累計）
            if start_str and not _is_quarterly_duration(start_str, end_str):
                ytd_tag = f"_ytd_{tag}"
                existing_ytd = quarterly_map[key].get(ytd_tag)
                if existing_ytd is None or (isinstance(existing_ytd, dict) and abs(val) > abs(existing_ytd.get('value', 0))):
                    quarterly_map[key][ytd_tag] = {'value': val, 'unit': unit}
                continue

            # 四半期の値として登録（ゼロでない値を優先）
            existing = quarterly_map[key].get(tag)
            if existing is None or (isinstance(existing, dict) and existing.get('value', 0) == 0 and val != 0):
                quarterly_map[key][tag] = {'value': val, 'unit': unit}

        elif fp_upper in ('FY', 'Q4'):
            q_num = 4
            key = (end_str, fy, q_num)

            if key not in quarterly_map:
                quarterly_map[key] = {
                    'filing_date': end_str,
                    'form': form,
                    'fiscal_year': fy,
                    'quarter': q_num,
                    'end': end_str,
                }

            if form == '10-K':
                if start_str and _is_quarterly_duration(start_str, end_str):
                    # 四半期相当の期間 → Q4値
                    existing = quarterly_map[key].get(tag)
                    if existing is None or (isinstance(existing, dict) and existing.get('value', 0) == 0 and val != 0):
                        quarterly_map[key][tag] = {'value': val, 'unit': unit}
                elif start_str and _is_annual_duration(start_str, end_str):
                    # 年間累計 → _ytd_
                    ytd_tag = f"_ytd_{tag}"
                    existing_ytd = quarterly_map[key].get(ytd_tag)
                    if existing_ytd is None or (isinstance(existing_ytd, dict) and abs(val) > abs(existing_ytd.get('value', 0))):
                        quarterly_map[key][ytd_tag] = {'value': val, 'unit': unit}
                elif not start_str:
                    # instant 値（株式数など）
                    existing = quarterly_map[key].get(tag)
                    if existing is None or (isinstance(existing, dict) and existing.get('value', 0) == 0 and val != 0):
                        quarterly_map[key][tag] = {'value': val, 'unit': unit}
                else:
                    # 期間不明 → YTD扱い
                    ytd_tag = f"_ytd_{tag}"
                    existing_ytd = quarterly_map[key].get(ytd_tag)
                    if existing_ytd is None or (isinstance(existing_ytd, dict) and abs(val) > abs(existing_ytd.get('value', 0))):
                        quarterly_map[key][ytd_tag] = {'value': val, 'unit': unit}
            else:
                # 10-Q で fp='Q4' は稀 → 四半期値として扱う
                existing = quarterly_map[key].get(tag)
                if existing is None or (isinstance(existing, dict) and existing.get('value', 0) == 0 and val != 0):
                    quarterly_map[key][tag] = {'value': val, 'unit': unit}

    # --- Step 3: Q4 の YTD → 四半期差分変換 ---
    INCOME_STMT_TAGS = [
        'us-gaap:NetIncomeLoss',
        'us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic',
        'us-gaap:ProfitLoss',
        'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
        'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
        'us-gaap:IncomeLossBeforeIncomeTaxExpenseBenefit',
        'us-gaap:IncomeTaxExpenseBenefit',
        'us-gaap:Revenues',
        'us-gaap:RevenueFromContractWithCustomer',
        'us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax',
        'us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax',
        'us-gaap:NetSales',
        'us-gaap:TotalRevenue',
        'us-gaap:SalesRevenueNet',
        'us-gaap:RevenuesNetOfInterestExpense',
        'us-gaap:NetInterestIncome',
        'us-gaap:NoninterestIncome',
        'us-gaap:NetCashProvidedByUsedInOperatingActivities',
        'us-gaap:PaymentsForPropertyPlantAndEquipment',
    ]

    for key, qdata in quarterly_map.items():
        end_str, fy, q_num = key
        if q_num != 4:
            continue

        for tag in INCOME_STMT_TAGS:
            # Q4 に四半期値がすでにある場合はスキップ
            if tag in qdata and isinstance(qdata[tag], dict) and qdata[tag].get('value', 0) != 0:
                continue

            ytd_tag = f"_ytd_{tag}"
            ytd_val_dict = qdata.get(ytd_tag)
            if not ytd_val_dict or not isinstance(ytd_val_dict, dict):
                continue
            annual_val = ytd_val_dict.get('value', 0)
            if annual_val == 0:
                continue

            # Q1+Q2+Q3 の合計を計算
            q123_sum = 0.0
            q123_count = 0
            for prev_q in (1, 2, 3):
                for pk, pd in quarterly_map.items():
                    if pk[1] == fy and pk[2] == prev_q:
                        v = pd.get(tag)
                        if v and isinstance(v, dict) and v.get('value', 0) != 0:
                            q123_sum += v['value']
                            q123_count += 1
                        break

            if q123_count == 3:
                q4_val = annual_val - q123_sum
                unit = ytd_val_dict.get('unit', 'USD')
                qdata[tag] = {'value': q4_val, 'unit': unit}

    # --- Step 4: 正規化タグへのマッピング ---
    NET_INCOME_TAGS = [
        'us-gaap:NetIncomeLoss',
        'us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic',
        'us-gaap:ProfitLoss',
    ]
    DILUTED_SHARES_TAGS = [
        'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding',
        'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
        'us-gaap:CommonStockSharesOutstanding',
    ]
    PRETAX_INCOME_TAGS = [
        'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
        'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
        'us-gaap:IncomeLossBeforeIncomeTaxExpenseBenefit',
    ]
    TAX_EXPENSE_TAGS = [
        'us-gaap:IncomeTaxExpenseBenefit',
    ]

    def _pick_best(qdata: Dict, tag_list: List[str], allow_ytd: bool = True) -> Optional[Dict]:
        """tag_list から最初にゼロでない値を持つタグを返す"""
        for tag in tag_list:
            v = qdata.get(tag)
            if v and isinstance(v, dict) and v.get('value', 0) != 0:
                return v
        if allow_ytd:
            for tag in tag_list:
                ytd = qdata.get(f'_ytd_{tag}')
                if ytd and isinstance(ytd, dict) and ytd.get('value', 0) != 0:
                    return ytd
        return None

    quarterly_list = []
    for key in sorted(quarterly_map.keys(), key=lambda k: k[0]):
        qdata = quarterly_map[key]

        # 基本フィールドのマッピング
        if 'net_income' not in qdata:
            v = _pick_best(qdata, NET_INCOME_TAGS)
            if v:
                qdata['net_income'] = v

        if 'diluted_shares' not in qdata:
            v = _pick_best(qdata, DILUTED_SHARES_TAGS, allow_ytd=False)
            if v:
                qdata['diluted_shares'] = v

        if 'pretax_income' not in qdata:
            v = _pick_best(qdata, PRETAX_INCOME_TAGS)
            if v:
                qdata['pretax_income'] = v

        if 'tax_expense' not in qdata:
            v = _pick_best(qdata, TAX_EXPENSE_TAGS)
            if v:
                qdata['tax_expense'] = v

        # net_income と diluted_shares がなければスキップ
        ni = qdata.get('net_income')
        ds = qdata.get('diluted_shares')
        if not ni or not isinstance(ni, dict) or ni.get('value', 0) == 0:
            continue
        if not ds or not isinstance(ds, dict) or ds.get('value', 0) == 0:
            continue

        quarterly_list.append(qdata)

    # --- Step 5: diluted_shares の補完（前方値の引き継ぎ） ---
    last_shares = None
    for qdata in quarterly_list:
        ds = qdata.get('diluted_shares')
        if ds and isinstance(ds, dict) and ds.get('value', 0) > 0:
            last_shares = ds
        elif last_shares:
            qdata['diluted_shares'] = last_shares

    print(f"   {ticker}: {len(quarterly_list)}四半期を集約完了")
    return quarterly_list


if __name__ == "__main__":
    import sys
    ticker = sys.argv[1] if len(sys.argv) > 1 else "NVDA"
    results = extract_quarterly_facts(ticker, years=3)
    print(f"\n=== {ticker}: {len(results)} quarters ===")
    for q in results[-4:]:
        ni = q.get('net_income', {}).get('value', 0)
        ds = q.get('diluted_shares', {}).get('value', 0)
        eps = ni / ds if ds else 0
        print(f"  {q['filing_date']} (Q{q['quarter']} FY{q['fiscal_year']}) "
              f"form={q['form']} NI={ni:,.0f} shares={ds:,.0f} EPS={eps:.4f}")
