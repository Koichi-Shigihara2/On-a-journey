#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
breadth_calculator.py — S&P500 Market Breadth Calculator

S&P500構成銘柄を対象に、以下のブレッスデータを日次で算出し保存する:
  - Advance / Decline 数 (日次)
  - AD Ratio (5日移動平均)
  - 52週新高値 / 新安値 数
  - NH-NL差分

使い方:
  python breadth_calculator.py

出力:
  docs/market-monitor/market-pulse/data/breadth_data.json
"""

import os
import sys
import json
import time
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone, timedelta

# ── パス設定 ──────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_SCRIPT_DIR)))
DATA_DIR = os.path.join(_REPO_ROOT, "docs", "market-monitor", "market-pulse", "data")
BREADTH_JSON = os.path.join(DATA_DIR, "breadth_data.json")
TICKERS_CACHE = os.path.join(DATA_DIR, "sp500_tickers.json")

JST = timezone(timedelta(hours=9))


def get_sp500_tickers():
    """
    S&P500構成銘柄リストを取得。
    まずキャッシュ（7日以内）を確認し、なければWikipediaから取得。
    """
    # キャッシュ確認
    if os.path.exists(TICKERS_CACHE):
        try:
            with open(TICKERS_CACHE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            cached_date = datetime.fromisoformat(cache["fetched_at"])
            if (datetime.now(JST) - cached_date).days < 7:
                print(f"[INFO] S&P500銘柄リスト: キャッシュ使用 ({len(cache['tickers'])}銘柄, {cache['fetched_at']})")
                return cache["tickers"]
        except Exception as e:
            print(f"[WARN] キャッシュ読み込み失敗: {e}")

    # WikipediaからS&P500構成銘柄を取得
    print("[INFO] WikipediaからS&P500構成銘柄を取得中...")
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        df = tables[0]
        tickers = df["Symbol"].str.replace(".", "-", regex=False).tolist()
        tickers = [t.strip() for t in tickers if t.strip()]
        print(f"[INFO] S&P500銘柄リスト取得完了: {len(tickers)}銘柄")

        # キャッシュ保存
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(TICKERS_CACHE, 'w', encoding='utf-8') as f:
            json.dump({
                "fetched_at": datetime.now(JST).isoformat(),
                "count": len(tickers),
                "tickers": tickers
            }, f, ensure_ascii=False, indent=2)
        return tickers
    except Exception as e:
        print(f"[ERROR] S&P500銘柄リスト取得失敗: {e}")
        # フォールバック: キャッシュがあれば期限切れでも使う
        if os.path.exists(TICKERS_CACHE):
            with open(TICKERS_CACHE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            print(f"[WARN] 期限切れキャッシュを使用 ({len(cache['tickers'])}銘柄)")
            return cache["tickers"]
        raise


def compute_breadth(tickers):
    """
    yfinanceで一括ダウンロードし、ブレッスデータを算出する。

    Returns:
        dict: {
            "date": "2026-04-03",
            "advances": 280,
            "declines": 210,
            "unchanged": 10,
            "ad_ratio_1d": 1.33,
            "ad_ratio_5d": 1.15,
            "new_highs_52w": 45,
            "new_lows_52w": 12,
            "nh_nl_diff": 33,
            "total_stocks": 500,
            "pct_above_50ma": 62.5,
            "pct_above_200ma": 55.2
        }
    """
    print(f"[INFO] {len(tickers)}銘柄の株価データを一括ダウンロード中 (period=1y)...")
    start_time = time.time()

    # 1年分のデータを一括ダウンロード（52週高値/安値の算出に必要）
    try:
        data = yf.download(
            tickers,
            period="1y",
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=True
        )
    except Exception as e:
        print(f"[ERROR] 一括ダウンロード失敗: {e}")
        return None

    elapsed = time.time() - start_time
    print(f"[INFO] ダウンロード完了 ({elapsed:.1f}秒)")

    if data.empty:
        print("[ERROR] ダウンロードデータが空です")
        return None

    close = data["Close"]

    # NaNが多すぎる銘柄を除外（直近5日でNaNが3日以上）
    recent_nan = close.iloc[-5:].isna().sum()
    valid_tickers = recent_nan[recent_nan < 3].index.tolist()
    close = close[valid_tickers]
    print(f"[INFO] 有効銘柄数: {len(valid_tickers)} / {len(tickers)}")

    if len(valid_tickers) < 100:
        print("[ERROR] 有効銘柄が100未満です。データ品質に問題があります。")
        return None

    # ── 日次 Advance / Decline ──
    daily_returns = close.pct_change()
    latest_returns = daily_returns.iloc[-1].dropna()

    advances = int((latest_returns > 0.0001).sum())
    declines = int((latest_returns < -0.0001).sum())
    unchanged = int(len(latest_returns) - advances - declines)
    ad_ratio_1d = round(advances / max(declines, 1), 2)

    # ── 5日 AD Ratio (5日間の累積Advance / 累積Decline) ──
    last5_returns = daily_returns.iloc[-5:]
    adv_5d = int((last5_returns > 0.0001).sum().sum())
    dec_5d = int((last5_returns < -0.0001).sum().sum())
    ad_ratio_5d = round(adv_5d / max(dec_5d, 1), 2)

    # ── 52週新高値 / 新安値 ──
    # 直近の終値 vs 過去252営業日（≒52週）の高値/安値
    lookback = min(252, len(close) - 1)
    if lookback < 50:
        print("[WARN] データ期間が短すぎます（52週分に満たない）")
        high_52w = close.iloc[-lookback:].max()
        low_52w = close.iloc[-lookback:].min()
    else:
        high_52w = close.iloc[-lookback:].max()
        low_52w = close.iloc[-lookback:].min()

    latest_close = close.iloc[-1].dropna()

    # 新高値: 直近終値が52週高値の99%以上（ほぼ等しいか超えている）
    new_highs = int((latest_close >= high_52w[latest_close.index] * 0.99).sum())
    # 新安値: 直近終値が52週安値の101%以下
    new_lows = int((latest_close <= low_52w[latest_close.index] * 1.01).sum())
    nh_nl_diff = new_highs - new_lows

    # ── 移動平均上回り率 ──
    pct_above_50ma = None
    pct_above_200ma = None
    if len(close) >= 50:
        ma50 = close.iloc[-50:].mean()
        above_50 = (latest_close > ma50[latest_close.index]).sum()
        pct_above_50ma = round(above_50 / len(latest_close) * 100, 1)
    if len(close) >= 200:
        ma200 = close.iloc[-200:].mean()
        above_200 = (latest_close > ma200[latest_close.index]).sum()
        pct_above_200ma = round(above_200 / len(latest_close) * 100, 1)

    last_date = close.index[-1].strftime('%Y-%m-%d')

    result = {
        "date": last_date,
        "advances": advances,
        "declines": declines,
        "unchanged": unchanged,
        "ad_ratio_1d": ad_ratio_1d,
        "ad_ratio_5d": ad_ratio_5d,
        "new_highs_52w": new_highs,
        "new_lows_52w": new_lows,
        "nh_nl_diff": nh_nl_diff,
        "total_stocks": len(valid_tickers),
        "pct_above_50ma": pct_above_50ma,
        "pct_above_200ma": pct_above_200ma,
    }

    print(f"[INFO] ブレッスデータ算出完了: ADV={advances} DEC={declines} "
          f"AD(1d)={ad_ratio_1d} AD(5d)={ad_ratio_5d} "
          f"NH={new_highs} NL={new_lows} NH-NL={nh_nl_diff} "
          f"50MA%={pct_above_50ma} 200MA%={pct_above_200ma}")

    return result


def save_breadth(data):
    """ブレッスデータをJSONに追記保存"""
    os.makedirs(DATA_DIR, exist_ok=True)

    all_data = []
    if os.path.exists(BREADTH_JSON):
        try:
            with open(BREADTH_JSON, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    all_data = json.loads(content)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"[WARN] breadth_data.json破損。新規作成します: {e}")
            all_data = []

    # 同じ日付のデータがあれば上書き
    all_data = [d for d in all_data if d.get("date") != data["date"]]
    all_data.append(data)

    # 日付順にソート、最大365日分保持
    all_data.sort(key=lambda x: x["date"])
    if len(all_data) > 365:
        all_data = all_data[-365:]

    with open(BREADTH_JSON, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"[INFO] breadth_data.json保存完了 (全{len(all_data)}件)")


if __name__ == "__main__":
    tickers = get_sp500_tickers()
    breadth = compute_breadth(tickers)
    if breadth:
        save_breadth(breadth)
        print("[OK] ブレッスデータ処理完了")
    else:
        print("[ERROR] ブレッスデータ算出に失敗しました")
        sys.exit(1)
