# src/value/tanuki_valuation/pipeline.py
import argparse
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

# ====================== 設定 ======================
CACHE_DIR = Path("data/sec_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_EXPIRE_HOURS = 24

USER_AGENT = "TanukiValuation/1.0 (your-email@example.com)"  # ← 自分のメールに変えてね！

# SEC API用共通ヘッダー
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
}

# ====================== ヘルパー関数 ======================
def get_cache_path(cik: str) -> Path:
    return CACHE_DIR / f"CIK{cik.zfill(10)}.json"

def load_from_cache(cik: str) -> dict | None:
    cache_path = get_cache_path(cik)
    if not cache_path.exists():
        return None
    # 期限切れチェック
    if datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime) > timedelta(hours=CACHE_EXPIRE_HOURS):
        return None
    with open(cache_path, encoding="utf-8") as f:
        return json.load(f)

def save_to_cache(cik: str, data: dict):
    cache_path = get_cache_path(cik)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_company_facts(cik: str) -> dict:
    """SECからcompanyfactsを取得（キャッシュ＋リトライ＋User-Agent）"""
    cache_data = load_from_cache(cik)
    if cache_data:
        print(f"    [CACHE HIT] CIK{cik} （キャッシュ使用）")
        return cache_data

    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik.zfill(10)}.json"
    print(f"    Fetching from {url}")

    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()  # 4xx/5xxで例外

    data = response.json()
    save_to_cache(cik, data)
    print(f"    [CACHE SAVED] CIK{cik}")
    return data


# ====================== 元の関数を置き換え ======================
def extract_quarterly_facts(cik: str, ticker: str):
    """修正済み：キャッシュ＋User-Agent＋リトライ"""
    print(f"    [DEBUG {ticker}] extract_quarterly_facts 開始")
    try:
        facts = fetch_company_facts(cik)
        # ここ以降はあなたが元々書いていたquarterly_data処理をそのまま貼ってOK
        # （例: facts["facts"]["us-gaap"] から revenue, eps などを抜き出す部分）
        quarterly_data = {}  # ← ここにあなたの元のロジックを入れる
        # ... あなたのコード ...

        return quarterly_data

    except Exception as e:
        print(f"    [ERROR {ticker}] {e}")
        return None


# ====================== メイン処理 ======================
def update_ticker(ticker: str):
    """1銘柄更新（成長率自動計算もここで）"""
    print(f"🔄 Updating {ticker}...")
    # CIK取得（あなたの既存ロジックをそのまま使う）
    cik = get_cik_from_ticker(ticker)  # ← あなたが既に持ってる関数があればそのまま
    if not cik:
        print(f"❌ {ticker} skipped - CIK not found")
        return

    quarterly_data = extract_quarterly_facts(cik, ticker)
    if not quarterly_data:
        print(f"❌ {ticker} skipped - No quarterly data")
        return

    # ここに成長率計算などのあなたのロジックを入れる
    # valuation = calculate_valuation(quarterly_data, ticker)
    print(f"✅ {ticker} updated successfully")


def main():
    parser = argparse.ArgumentParser(description="TANUKI VALUATION パイプライン")
    parser.add_argument("--tickers", type=str, help="カンマ区切りで指定（例: MSFT,NVDA,PLTR）")
    parser.add_argument("--all", action="store_true", help="全銘柄実行（定期更新用）")
    args = parser.parse_args()

    print("=== TANUKI VALUATION 全銘柄実行開始（企業別成長率自動計算）===")

    if args.all:
        tickers = ["MSFT", "AMZN", "TSLA", "NVDA", "PLTR", "CELH", "APP", "AMD", "SOFI", "SOUN", "RKLB", "ONDS", "FIG"]  # ← あなたの銘柄リスト
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    else:
        # 引数なし → デフォルトで個別テスト用（1銘柄だけ）
        tickers = ["MSFT"]

    for ticker in tickers:
        update_ticker(ticker)
        time.sleep(0.5)  # SECの負荷軽減

    print("🎉 TANUKI VALUATION 全銘柄更新完了！（計算過程はlatest.jsonで照会可能）")


if __name__ == "__main__":
    main()