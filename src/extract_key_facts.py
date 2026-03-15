"""
SEC EDGARから企業の財務データを抽出するモジュール（会計年度対応・四半期分類改善版・複数期間対応）
- CIKマップファイルから銘柄のCIKを取得
- SECのCompany Facts APIから直接XBRLデータを取得
- 10-Qから四半期データを取得し、期間から正しい四半期番号（Q1, Q2, Q3）を割り当て
- 10-Kから通期データを取得し、Q4を計算（通期 - Q1~Q3合計）
- 会計年度が暦年と異なる場合にも対応（例：NVDAの1月決算）
- 複数クラス株式の希薄化後株式数を合算（ただし、同じend, startの期間ごとに合算）
- 調整項目は元のXBRLタグ名で保存
- adjustment_items.json から必要なXBRLタグを動的に取得し、全て抽出する
- 詳細なデバッグ出力とエラーハンドリング
"""
import os
import csv
import json
import requests
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta

# ============================================
# 定数設定
# ============================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
CIK_FILE = os.path.join(CONFIG_DIR, "cik_lookup.csv")
ADJUSTMENT_ITEMS_FILE = os.path.join(CONFIG_DIR, "adjustment_items.json")

HEADERS = {
    'User-Agent': 'jamablue01@gmail.com',  # 必須：連絡先メールアドレス
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'data.sec.gov'
}

# 四半期とみなす期間の範囲（日数）- 少し余裕を持たせる
QUARTER_DAYS_MIN = 70
QUARTER_DAYS_MAX = 120
# 年次とみなす期間の最小日数（10-Kの場合）
ANNUAL_DAYS_MIN = 300

# ============================================
# 調整項目から必要なXBRLタグを動的に収集
# ============================================
def load_required_xbrl_tags() -> List[str]:
    """adjustment_items.json から全ての xbrl_tags を収集し、重複を排除して返す"""
    try:
        with open(ADJUSTMENT_ITEMS_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Warning: {ADJUSTMENT_ITEMS_FILE} not found. Using empty list.")
        return []
    
    tags = set()
    categories = config.get("categories", [])
    for cat in categories:
        for sub in cat.get("sub_items", []):
            xbrl_tags = sub.get("xbrl_tags", [])
            for tag in xbrl_tags:
                tags.add(tag)
    # 基本的な必須タグも追加
    tags.add("us-gaap:NetIncomeLoss")
    tags.add("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding")
    tags.add("us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit")  # 税引前利益
    tags.add("us-gaap:IncomeTaxExpenseBenefit")  # 法人税等
    tags.add("us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic")  # 親会社株主帰属当期利益（あれば）
    return list(tags)

# ============================================
# CIKマップ管理
# ============================================
def load_cik_map() -> Dict[str, str]:
    """CIKマップをCSVから読み込む"""
    cik_map = {}
    try:
        if not os.path.exists(CIK_FILE):
            print(f"Warning: {CIK_FILE} not found. Creating empty mapping.")
            os.makedirs(CONFIG_DIR, exist_ok=True)
            with open(CIK_FILE, 'w', encoding='utf-8') as f:
                f.write("ticker,cik,name\n")
            return cik_map
        
        with open(CIK_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['ticker'] and row['cik']:
                    cik = row['cik'].strip().zfill(10)
                    cik_map[row['ticker'].strip().upper()] = cik
        print(f"Loaded {len(cik_map)} CIK mappings from {CIK_FILE}")
        return cik_map
    except Exception as e:
        print(f"Error loading CIK map: {e}")
        return {}

def save_cik_map(cik_map: Dict[str, str]) -> bool:
    """CIKマップをCSVに保存"""
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(CIK_FILE, 'w', encoding='utf-8') as f:
            f.write("ticker,cik,name\n")
            for ticker, cik in sorted(cik_map.items()):
                f.write(f"{ticker},{cik},\n")
        print(f"Saved {len(cik_map)} CIK mappings to {CIK_FILE}")
        return True
    except Exception as e:
        print(f"Error saving CIK map: {e}")
        return False

def get_cik(ticker: str) -> str:
    """ティッカーからCIKを取得"""
    ticker = ticker.strip().upper()
    cik_map = load_cik_map()
    
    if ticker in cik_map:
        return cik_map[ticker]
    
    # SEC APIから直接取得
    print(f"CIK not found for {ticker} in local file. Trying SEC API...")
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            data = response.json()
            for item in data.values():
                if item['ticker'] and item['ticker'].upper() == ticker:
                    cik = str(item['cik_str']).zfill(10)
                    cik_map[ticker] = cik
                    save_cik_map(cik_map)
                    return cik
    except Exception as e:
        print(f"SEC API lookup failed: {e}")
    
    raise Exception(f"CIK not found for {ticker}. Please add to {CIK_FILE}")

# ============================================
# SEC Company Facts APIからデータ取得
# ============================================
def fetch_company_facts(cik: str) -> Dict:
    """
    SEC Company Facts APIから企業の全XBRLファクトを取得
    Args:
        cik: 10桁のCIK番号
    Returns:
        Dict: 企業ファクトデータ
    """
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    print(f"Fetching company facts from {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching company facts: {e}")
        return {}

def extract_value_from_facts(facts_data: Dict, us_gaap_tag: str, form_type: Optional[str] = None, limit: int = 40) -> List[Dict]:
    """
    Company Factsから特定タグの時系列データを抽出
    Args:
        facts_data: Company Facts APIのレスポンス
        us_gaap_tag: タグ名（例: 'NetIncomeLoss'）
        form_type: フォーム種類（'10-Q', '10-K'）でフィルタする場合は指定
        limit: 取得する最大件数
    Returns:
        List[Dict]: 各期のデータ
    """
    results = []
    try:
        if 'facts' not in facts_data or 'us-gaap' not in facts_data['facts']:
            return results
        
        if us_gaap_tag not in facts_data['facts']['us-gaap']:
            return results
        
        units_data = facts_data['facts']['us-gaap'][us_gaap_tag]['units']
        for unit_key in units_data:
            if 'USD' in unit_key or 'shares' in unit_key:
                for item in units_data[unit_key]:
                    if form_type and not item.get('form', '').startswith(form_type):
                        continue
                    # 期間情報があるものだけ採用（instantではなくduration）
                    if 'start' in item and 'end' in item:
                        results.append({
                            'end': item.get('end'),
                            'val': item.get('val'),
                            'filed': item.get('filed'),
                            'form': item.get('form'),
                            'unit': unit_key,
                            'start': item.get('start')
                        })
                break
    except Exception as e:
        print(f"Error extracting {us_gaap_tag}: {e}")
    
    # 日付でソート（新しい順）
    results.sort(key=lambda x: x['end'], reverse=True)
    return results[:limit]

def get_diluted_shares_from_facts(facts_data: Dict, form_type: Optional[str] = None, limit: int = 40) -> List[Dict]:
    """
    希薄化後株式数を取得（複数クラスがある場合は合算）
    戻り値の各要素は {'end': str, 'val': float, 'filed': str, 'form': str, 'unit': str, 'start': str} の形式
    修正点: グループ化キーを (end, start) に変更し、同じ期間内のクラス株を合算する。
    """
    tag = "WeightedAverageNumberOfDilutedSharesOutstanding"
    try:
        if 'facts' not in facts_data or 'us-gaap' not in facts_data['facts']:
            return []
        if tag not in facts_data['facts']['us-gaap']:
            return []
        
        units_data = facts_data['facts']['us-gaap'][tag]['units']
        # 通常は 'shares' 単位
        for unit_key in units_data:
            if 'shares' in unit_key:
                # 同じ (end, start) 日付のものをグループ化して合算
                period_map = {}
                for item in units_data[unit_key]:
                    if form_type and not item.get('form', '').startswith(form_type):
                        continue
                    if 'start' in item and 'end' in item:
                        key = (item['end'], item['start'])  # ★修正ポイント
                        if key not in period_map:
                            period_map[key] = {
                                'end': item['end'],
                                'start': item['start'],
                                'val': 0,
                                'filed': item.get('filed'),
                                'form': item.get('form'),
                                'unit': unit_key,
                            }
                        period_map[key]['val'] += item.get('val', 0)
                
                # マップをリストに変換
                results = list(period_map.values())
                results.sort(key=lambda x: x['end'], reverse=True)
                return results[:limit]
    except Exception as e:
        print(f"Error getting diluted shares: {e}")
    return []

# ============================================
# 会計年度判定と四半期分類
# ============================================
def determine_fiscal_year_end(annual_data: List[Dict]) -> int:
    """
    10-Kのデータから、会計年度終了月を特定する
    最も頻出する月を返す（デフォルト12）
    """
    month_counts = {}
    for item in annual_data:
        if 'end' in item:
            end_date = datetime.strptime(item['end'], '%Y-%m-%d')
            month = end_date.month
            month_counts[month] = month_counts.get(month, 0) + 1
    
    if not month_counts:
        return 12
    
    # 最も多い月を返す
    fiscal_end_month = max(month_counts.items(), key=lambda x: x[1])[0]
    return fiscal_end_month

def get_quarter_number(end_date: datetime, fiscal_end_month: int) -> int:
    """
    終了日と会計年度終了月から四半期番号（1-4）を決定する
    """
    end_month = end_date.month
    
    # 会計年度終了月からのオフセットを計算
    # 例： fiscal_end_month=1（1月決算）の場合
    #   end_month=1 → Q4 (offset 0)
    #   end_month=10 → Q1 (offset 3)
    #   end_month=7  → Q2 (offset 6)
    #   end_month=4  → Q3 (offset 9)
    if end_month <= fiscal_end_month:
        offset = fiscal_end_month - end_month
    else:
        offset = fiscal_end_month + 12 - end_month
    
    # offset から四半期をマッピング
    if offset <= 1:
        return 4
    elif offset <= 4:
        return 3
    elif offset <= 7:
        return 2
    else:
        return 1

# ============================================
# メイン抽出関数（改善版）
# ============================================
def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict[str, Any]]:
    """
    四半期データを取得（SEC API直アクセス＋会計年度対応＋10-KからのQ4補完）
    Args:
        ticker: 銘柄ティッカー
        years: 取得する年数
    Returns:
        List[Dict]: 四半期データのリスト（Q1～Q4が含まれる）
    """
    try:
        # CIK取得
        cik = get_cik(ticker)
        print(f"CIK: {cik}")
        
        # Company Facts取得
        facts = fetch_company_facts(cik)
        if not facts:
            print(f"No facts data for {ticker}")
            return []
        
        # 必要なXBRLタグを動的に収集
        required_tags = load_required_xbrl_tags()
        print(f"Required XBRL tags: {required_tags}")
        
        # タグごとにデータを抽出し、マップに保存
        tag_data_map = {}  # tag -> list of items
        for tag in required_tags:
            # 10-Qと10-Kの両方を取得（後でフィルタする）
            items = extract_value_from_facts(facts, tag, form_type=None, limit=years*6)  # 多めに取得
            tag_data_map[tag] = items
            print(f"Extracted {len(items)} items for {tag}")
        
        # ---------- 10-Qデータを期間フィルタリング ----------
        # タグ別に期間フィルタを適用するのは後で行う。まずは10-Kから年次データを抽出。
        
        # 年次データ（10-K）のみを期間フィルタ（300日以上）で抽出
        annual_data_by_tag = {}
        for tag, items in tag_data_map.items():
            annual_items = []
            for item in items:
                if item.get('form', '').startswith('10-K') and 'start' in item and 'end' in item:
                    start = datetime.strptime(item['start'], '%Y-%m-%d')
                    end = datetime.strptime(item['end'], '%Y-%m-%d')
                    days_diff = (end - start).days
                    if days_diff >= ANNUAL_DAYS_MIN:
                        annual_items.append(item)
            annual_data_by_tag[tag] = annual_items
        
        # 会計年度終了月を特定（NetIncomeLossの年次データを使用）
        net_income_annual = annual_data_by_tag.get('us-gaap:NetIncomeLoss', [])
        fiscal_end_month = determine_fiscal_year_end(net_income_annual)
        print(f"Detected fiscal year end month: {fiscal_end_month}")
        
        # 希薄化後株式数のマップ（end, start -> value）を作成（Q4計算用に年次も必要）
        diluted_shares_all = tag_data_map.get('us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding', [])
        diluted_map = {}  # key: (end, start) -> value
        for item in diluted_shares_all:
            if 'start' in item and 'end' in item:
                key = (item['end'], item['start'])
                diluted_map[key] = item['val']
        
        # ---------- 10-Qデータを四半期ごとに分類 ----------
        # まず、すべての10-Qデータを期間でフィルタ（70～120日）し、さらに同じend日付で複数ある場合は最も短い期間（四半期）を優先
        # ここでは各タグごとに候補を作るが、四半期データは各タグで同じ期間セットが存在するため、代表としてNetIncomeLossで期間を決める。
        net_income_10q = tag_data_map.get('us-gaap:NetIncomeLoss', [])
        quarterly_candidates = []  # 四半期の期間情報を保持
        for q_item in net_income_10q:
            if not q_item.get('form', '').startswith('10-Q'):
                continue
            if 'start' not in q_item or 'end' not in q_item:
                continue
            start = datetime.strptime(q_item['start'], '%Y-%m-%d')
            end = datetime.strptime(q_item['end'], '%Y-%m-%d')
            days_diff = (end - start).days
            if QUARTER_DAYS_MIN <= days_diff <= QUARTER_DAYS_MAX:
                quarterly_candidates.append({
                    'start': start,
                    'end': end,
                    'end_str': q_item['end'],
                    'start_str': q_item['start'],
                    'val': q_item['val'],
                    'unit': q_item['unit'],
                    'filed': q_item.get('filed', q_item['end']),
                    'days': days_diff
                })
        
        # 同じ終了日(end)のデータをグループ化し、最も期間の短いものを採用（四半期データとして適切）
        best_quarterly = {}
        for cand in quarterly_candidates:
            end_str = cand['end_str']
            if end_str not in best_quarterly or cand['days'] < best_quarterly[end_str]['days']:
                best_quarterly[end_str] = cand
        
        # quarters_map の構築
        quarters_map = {}  # key: (fiscal_year, quarter) -> data
        
        for end_str, cand in best_quarterly.items():
            # 会計年度を決定
            # 終了日が fiscal_end_month より後なら翌年度、そうでなければ当年
            if cand['end'].month > fiscal_end_month:
                fiscal_year = cand['end'].year + 1
            else:
                fiscal_year = cand['end'].year
            
            quarter_num = get_quarter_number(cand['end'], fiscal_end_month)
            key = (fiscal_year, quarter_num)
            
            # 基本データを作成
            if key not in quarters_map:
                quarters_map[key] = {
                    'filing_date': end_str,
                    'form': '10-Q',
                    'start': cand['start_str'],
                    'end': end_str,
                    'filed': cand['filed'],
                    'quarter': quarter_num,
                    'fiscal_year': fiscal_year
                }
            else:
                # 既存のデータより提出日が新しければ更新
                existing_filed = quarters_map[key].get('filed', '')
                if cand['filed'] > existing_filed:
                    quarters_map[key]['filed'] = cand['filed']
            
            # NetIncomeLossをセット
            quarters_map[key]['net_income'] = {'value': cand['val'], 'unit': cand['unit']}
        
        # 他のタグのデータを追加
        for tag in required_tags:
            if tag == 'us-gaap:NetIncomeLoss' or tag == 'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding':
                continue  # 特別扱い済み
            tag_items = tag_data_map.get(tag, [])
            for item in tag_items:
                if not item.get('form', '').startswith('10-Q'):
                    continue
                if 'start' not in item or 'end' not in item:
                    continue
                start = datetime.strptime(item['start'], '%Y-%m-%d')
                end = datetime.strptime(item['end'], '%Y-%m-%d')
                days_diff = (end - start).days
                if not (QUARTER_DAYS_MIN <= days_diff <= QUARTER_DAYS_MAX):
                    continue
                
                if end.month > fiscal_end_month:
                    fiscal_year = end.year + 1
                else:
                    fiscal_year = end.year
                
                quarter_num = get_quarter_number(end, fiscal_end_month)
                key = (fiscal_year, quarter_num)
                if key in quarters_map:
                    quarters_map[key][tag] = {'value': item['val'], 'unit': item['unit']}
        
        # 希薄化後株式数を追加（四半期）
        for item in diluted_shares_all:
            if not item.get('form', '').startswith('10-Q'):
                continue
            if 'start' not in item or 'end' not in item:
                continue
            start = datetime.strptime(item['start'], '%Y-%m-%d')
            end = datetime.strptime(item['end'], '%Y-%m-%d')
            days_diff = (end - start).days
            if not (QUARTER_DAYS_MIN <= days_diff <= QUARTER_DAYS_MAX):
                continue
            
            if end.month > fiscal_end_month:
                fiscal_year = end.year + 1
            else:
                fiscal_year = end.year
            
            quarter_num = get_quarter_number(end, fiscal_end_month)
            key = (fiscal_year, quarter_num)
            if key in quarters_map:
                quarters_map[key]['diluted_shares'] = {'value': item['val'], 'unit': item['unit']}
        
        # ---------- 10-KからQ4を計算 ----------
        # 各タグの年次データを使ってQ4を計算する
        # まず、各 fiscal_year の Q1, Q2, Q3 が揃っているかを確認
        fiscal_years = set(k[0] for k in quarters_map.keys() if k[1] in (1,2,3))
        for fiscal_year in fiscal_years:
            q1_key = (fiscal_year, 1)
            q2_key = (fiscal_year, 2)
            q3_key = (fiscal_year, 3)
            q4_key = (fiscal_year, 4)
            
            if q1_key in quarters_map and q2_key in quarters_map and q3_key in quarters_map:
                # Q1-Q3 のデータがある場合、年次データからQ4を計算
                # 年次データは NetIncomeLoss の10-Kを使用
                net_income_annual_items = annual_data_by_tag.get('us-gaap:NetIncomeLoss', [])
                # この fiscal_year に対応する10-Kを探す（endが fiscal_year の終了日と一致するもの）
                # fiscal_year の終了日は、fiscal_end_month から計算できるが、正確には10-Kのendから会計年度を判断。
                # 10-Kのendから fiscal_year を決定するロジックと同じ
                target_k_item = None
                for item in net_income_annual_items:
                    item_end = datetime.strptime(item['end'], '%Y-%m-%d')
                    # 会計年度の決定: endの年が fiscal_end_month によって決まる
                    if item_end.month > fiscal_end_month:
                        item_fiscal_year = item_end.year + 1
                    else:
                        item_fiscal_year = item_end.year
                    if item_fiscal_year == fiscal_year:
                        target_k_item = item
                        break
                
                if target_k_item:
                    q1_income = normalize_value(quarters_map[q1_key].get('net_income', {'value':0}))
                    q2_income = normalize_value(quarters_map[q2_key].get('net_income', {'value':0}))
                    q3_income = normalize_value(quarters_map[q3_key].get('net_income', {'value':0}))
                    q1q3_sum = q1_income + q2_income + q3_income
                    
                    annual_net = target_k_item['val']
                    q4_net = annual_net - q1q3_sum
                    
                    # 希薄化後株式数（年次）を取得
                    diluted_val = 0
                    # 年次希薄化後株式数を探す
                    for d_item in diluted_shares_all:
                        if d_item.get('form', '').startswith('10-K') and d_item.get('end') == target_k_item['end']:
                            diluted_val = d_item['val']
                            break
                    if diluted_val == 0 and q3_key in quarters_map:
                        diluted_val = normalize_value(quarters_map[q3_key].get('diluted_shares', {'value':0}))
                    
                    # Q4データを作成
                    q4_data = {
                        'filing_date': target_k_item['end'],
                        'form': '10-K',
                        'net_income': {'value': q4_net, 'unit': 'USD'},
                        'diluted_shares': {'value': diluted_val, 'unit': 'shares'},
                        'start': quarters_map[q3_key]['end'] if q3_key in quarters_map else target_k_item['start'],
                        'end': target_k_item['end'],
                        'filed': target_k_item.get('filed', target_k_item['end']),
                        'quarter': 4,
                        'fiscal_year': fiscal_year
                    }
                    quarters_map[q4_key] = q4_data
                    print(f"  Calculated Q4 for fiscal year {fiscal_year} (end {target_k_item['end']}): net_income={q4_net:,.0f}, diluted_shares={diluted_val:,.0f}")
                else:
                    print(f"  Warning: No 10-K found for fiscal year {fiscal_year}")
            else:
                print(f"  Warning: Missing Q1-Q3 for fiscal year {fiscal_year} (have: {q1_key in quarters_map}, {q2_key in quarters_map}, {q3_key in quarters_map})")
        
        # ---------- 最終的なリストに変換 ----------
        quarterly_list = []
        for (fiscal_year, quarter), data in sorted(quarters_map.items(), reverse=True):
            # 必須データの確認
            if 'net_income' in data and 'diluted_shares' in data:
                quarterly_list.append(data)
                net_val = normalize_value(data['net_income'])
                shr_val = normalize_value(data['diluted_shares'])
                print(f"  ✓ {data['filing_date']} (FY{fiscal_year} Q{quarter}): net_income={net_val:,.0f}, diluted_shares={shr_val:,.0f}")
            else:
                missing = []
                if 'net_income' not in data:
                    missing.append('net_income')
                if 'diluted_shares' not in data:
                    missing.append('diluted_shares')
                print(f"  ✗ {data.get('filing_date', 'unknown')} (FY{fiscal_year} Q{quarter}): missing {', '.join(missing)}")
        
        print(f"\n{ticker}: {len(quarterly_list)}件の四半期データを取得")
        return quarterly_list
        
    except Exception as e:
        print(f"{ticker} データ取得エラー: {e}")
        import traceback
        traceback.print_exc()
        return []

def normalize_value(value_dict: Optional[Dict]) -> float:
    """
    単位正規化（すべてUSD absolute valueに統一）
    Args:
        value_dict: {"value": 数値, "unit": "USD"|"shares"|"thousands"|...}
    Returns:
        float: 正規化された値
    """
    if not value_dict:
        return 0.0
    
    value = float(value_dict.get("value", 0))
    unit = value_dict.get("unit", "USD").lower()
    
    if unit in ["thousands", "thousand"]:
        return value * 1_000
    elif unit in ["millions", "million"]:
        return value * 1_000_000
    elif unit in ["billions", "billion"]:
        return value * 1_000_000_000
    else:
        return value

# ============================================
# テスト用メイン関数
# ============================================
def main():
    """テスト実行用"""
    ticker = "TSLA"  # テストしたい銘柄
    print(f"Testing data extraction for {ticker}...")
    
    data = extract_quarterly_facts(ticker, years=5)
    
    if data:
        print(f"\nSuccessfully extracted {len(data)} quarters:")
        for i, quarter in enumerate(data[:15]):
            print(f"\nQuarter {i+1}: {quarter['filing_date']} ({quarter.get('form', 'unknown')})")
            net = normalize_value(quarter.get('net_income'))
            shares = normalize_value(quarter.get('diluted_shares'))
            print(f"  Net Income: {net:,.0f} USD")
            print(f"  Diluted Shares: {shares:,.0f}")
            if shares > 0:
                eps = net / shares
                print(f"  Implied EPS: {eps:.4f} USD")
            
            # 調整項目の例
            sbc = quarter.get('us-gaap:ShareBasedCompensation')
            if sbc:
                sbc_val = normalize_value(sbc)
                print(f"  SBC: {sbc_val:,.0f} USD")
    else:
        print("No data extracted")

if __name__ == "__main__":
    main()
