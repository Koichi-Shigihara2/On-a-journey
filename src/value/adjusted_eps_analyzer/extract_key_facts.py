import requests
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import json
import os
from datetime import datetime, timedelta

# ====================== 設定 ======================
CIK_FILE = "config/cik_lookup.csv"
ADJUSTMENT_ITEMS_FILE = "config/adjustment_items.json"
USER_AGENT = "Mozilla/5.0 (compatible; TanukiValuation/1.0; +https://github.com/koichi-shigihara2/On-a-journey)"

# 緩和された閾値（PLTR/AMD対応）
QUARTER_DAYS_MIN = 60
QUARTER_DAYS_MAX = 140
ANNUAL_DAYS_MIN = 280

# ====================== ヘルパー関数 ======================
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
    """動的タグロード + diluted_shares用追加タグ（強化版）"""
    tags = set()
    try:
        with open(ADJUSTMENT_ITEMS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for item in data.get("adjustment_items", []):
                if "xbrl_tag" in item:
                    tags.add(item["xbrl_tag"])
    except:
        pass

    # 基本タグ（既存）
    tags.update([
        'us-gaap:NetIncomeLoss', 'us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic',
        'us-gaap:NetCashProvidedByUsedInOperatingActivities', 'us-gaap:PaymentsForPropertyPlantAndEquipment',
        'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding',
    ])

    # ★★★ PLTR/AMD対応：diluted_shares追加タグ ★★★
    tags.update([
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
    # SEC API fallback
    try:
        resp = requests.get(f"https://api.sec.gov/search?q={ticker}&category=company", headers={"User-Agent": USER_AGENT})
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get('hits', {}).get('hits', []):
                if item['_source']['ticker'] == ticker.upper():
                    return item['_source']['cik'].zfill(10)
    except:
        pass
    raise Exception(f"CIK not found for {ticker}. Please add to {CIK_FILE}")

def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict]:
    cik = get_cik(ticker)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    headers = {"User-Agent": USER_AGENT}

    print(f"   [DEBUG {ticker}] extract_quarterly_facts 開始")
    print(f"Loaded {len(pd.read_csv(CIK_FILE)) if os.path.exists(CIK_FILE) else 0} CIK mappings from {CIK_FILE}")
    print(f"CIK: {cik}")
    print(f"Fetching company facts from {url}")

    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"Error fetching company facts: {resp.status_code} {resp.reason}")
        return []

    facts = resp.json().get('facts', {}).get('us-gaap', {})
    required_tags = load_required_xbrl_tags()
    print(f"Required XBRL tags: {required_tags[:50]}...")  # 省略表示

    quarterly_data = []
    for tag in required_tags:
        if tag not in facts:
            continue
        units = facts[tag].get('units', {})
        for unit_type, values in units.items():
            for v in values:
                if v.get('form') not in ['10-Q', '10-K']:
                    continue
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

    # 重複除去＆ソート
    quarterly_data = sorted(set((d['end'], d['tag'], d['value']) for d in quarterly_data),
                            key=lambda x: x[0], reverse=True)

    # diluted_shares専用抽出（PLTR/AMD対応）
    diluted_shares_list = []
    for item in quarterly_data:
        if any(k in item[1] for k in ['DilutedSharesOutstanding', 'SharesOutstanding', 'WeightedAverageNumberOfDilutedShares']):
            diluted_shares_list.append((item[0], item[2]))

    print(f"   {ticker}: {len(quarterly_data)}件の四半期データを取得")

    if diluted_shares_list:
        latest_shares = diluted_shares_list[0][1]
        print(f"   [DEBUG {ticker}] diluted_shares = {latest_shares:,.0f} (最新値)")
    else:
        latest_shares = 0.0
        print(f"   [DEBUG {ticker}] diluted_shares が0です！ スキップの可能性あり")

    # 結果にsharesを付与
    result = []
    for end, tag, value in quarterly_data[:years*4]:
        result.append({
            tag: {'value': value, 'end': end.strftime('%Y-%m-%d')},
            'diluted_shares': latest_shares
        })

    return result