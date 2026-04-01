import requests
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import json
import os
import time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ====================== 設定 ======================
CIK_FILE = "config/cik_lookup.csv"
ADJUSTMENT_ITEMS_FILE = "config/adjustment_items.json"

# ★★★ SEC公式必須：明確なUser-Agent ★★★
USER_AGENT = "Koichi Shigihara (koichi.shigihara2@gmail.com) - TanukiValuation/1.0 (+https://github.com/koichi-shigihara2/On-a-journey)"

# リトライ設定（403/429対応）
def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,           # 指数バックオフ
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    return session

# ====================== ヘルパー ======================
def normalize_value(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(',', ''))
        except:
            return 0.0
    return 0.0

def load_required_xbrl_tags() -> List[str]:
    tags = set()
    try:
        with open(ADJUSTMENT_ITEMS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for item in data.get("adjustment_items", []):
                if "xbrl_tag" in item:
                    tags.add(item["xbrl_tag"])
    except:
        pass

    tags.update([
        'us-gaap:NetIncomeLoss', 'us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic',
        'us-gaap:NetCashProvidedByUsedInOperatingActivities', 'us-gaap:PaymentsForPropertyPlantAndEquipment',
        'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding',
        'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
        'us-gaap:CommonStockSharesOutstanding',
        'us-gaap:WeightedAverageNumberOfDilutedShares',
        'us-gaap:SharesOutstanding',
    ])
    return list(tags)

def get_cik(ticker: str) -> str:
    try:
        df = pd.read_csv(CIK_FILE)
        row = df[df['ticker'].str.upper() == ticker.upper()]
        if not row.empty:
            return str(row.iloc[0]['cik']).zfill(10)
    except:
        pass
    raise Exception(f"CIK not found for {ticker}. Please add to {CIK_FILE}")

def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict]:
    cik = get_cik(ticker)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    session = create_session()

    print(f"   [DEBUG {ticker}] extract_quarterly_facts 開始")
    print(f"CIK: {cik}")
    print(f"Fetching from {url}")

    quarterly_data = []
    diluted_shares = 0.0

    for attempt in range(5):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                break
            if resp.status_code == 403:
                wait = 2 ** attempt
                print(f"   [DEBUG {ticker}] 403 Forbidden → {wait}秒待機 ({attempt+1}/5)")
                time.sleep(wait)
                continue
            print(f"Error fetching company facts: {resp.status_code} {resp.reason}")
            return []
        except Exception as e:
            print(f"   [DEBUG {ticker}] リクエスト例外: {e}")
            time.sleep(2 ** attempt)

    else:
        print(f"   [DEBUG {ticker}] 最大リトライ失敗")
        return []

    facts = resp.json().get('facts', {}).get('us-gaap', {})
    required_tags = load_required_xbrl_tags()

    for tag in required_tags:
        if tag not in facts:
            continue
        units = facts[tag].get('units', {})
        for unit_type, values in units.items():
            for v in values:
                if v.get('form') not in ['10-Q', '10-K']:
                    continue
                try:
                    end = datetime.strptime(v['end'], '%Y-%m-%d')
                    val = normalize_value(v.get('val', 0))
                    if val == 0:
                        continue
                    quarterly_data.append({
                        'tag': tag,
                        'end': end,
                        'value': val,
                        'form': v['form'],
                        'fy': v.get('fy'),
                        'fp': v.get('fp')
                    })
                    if any(k in tag.lower() for k in ['diluted', 'sharesoutstanding', 'weightedaveragenumberof']):
                        diluted_shares = max(diluted_shares, val)
                except:
                    continue

    quarterly_data = sorted(set((d['end'], d['tag'], d['value']) for d in quarterly_data), key=lambda x: x[0], reverse=True)

    print(f"   {ticker}: {len(quarterly_data)}件の四半期データを取得")
    print(f"   [DEBUG {ticker}] diluted_shares = {diluted_shares:,.0f}")

    result = []
    for end, tag, value in quarterly_data[:years*4]:
        result.append({
            tag: {'value': value, 'end': end.strftime('%Y-%m-%d')},
            'diluted_shares': diluted_shares
        })

    return result