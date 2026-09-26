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

import functools
import os
import sys
import json
import time
import io
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta

# ── パス設定 ──────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_SCRIPT_DIR)))
DATA_DIR = os.path.join(_REPO_ROOT, "docs", "market-monitor", "market-pulse", "data")
BREADTH_JSON = os.path.join(DATA_DIR, "breadth_data.json")
TICKERS_CACHE = os.path.join(DATA_DIR, "sp500_tickers.json")

JST = timezone(timedelta(hours=9))

# common/market_data - [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-7:
# S&P500構成銘柄の株価データ一括取得（yf.download()）をcommon.market_data.
# reader経由に切替。get_sp500_tickers()（銘柄リスト取得）は意図的独立
# 実装のため対象外、compute_breadth()・fetch_rsp_spy_divergence()の価格
# データ取得部分のみが対象。他の切替済みファイルと同じHAS_MARKET_DATA
# ガード・二段構えsys.path解決パターンを踏襲する（本ファイルは`python
# src/market/market_pulse/breadth_calculator.py`で直接実行されるため
# リポジトリルートがsys.pathに含まれない。_REPO_ROOTは上で既に計算
# 済みのため流用する）。
HAS_MARKET_DATA = False
_md_get_price_series = None

try:
    if _REPO_ROOT not in sys.path:
        sys.path.insert(0, _REPO_ROOT)
    from common.market_data.reader import get_price_series as _md_get_price_series
    HAS_MARKET_DATA = True
except Exception:
    pass

if not HAS_MARKET_DATA:
    try:
        _github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if _github_workspace and _github_workspace not in sys.path:
            sys.path.insert(0, _github_workspace)
        from common.market_data.reader import get_price_series as _md_get_price_series
        HAS_MARKET_DATA = True
    except Exception:
        pass


def get_sp500_tickers():
    """
    S&P500構成銘柄リストを取得。
    優先順: キャッシュ(7日) → Wikipedia(requests) → GitHub CSV → 期限切れキャッシュ
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

    tickers = None

    # ソース1: Wikipedia（requests で User-Agent を付けて取得）
    print("[INFO] WikipediaからS&P500構成銘柄を取得中...")
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; MarketPulseBot/1.0)"}
        resp = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=headers,
            timeout=30
        )
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text))
        df = tables[0]
        tickers = df["Symbol"].str.replace(".", "-", regex=False).tolist()
        tickers = [t.strip() for t in tickers if t.strip()]
        print(f"[INFO] Wikipedia取得成功: {len(tickers)}銘柄")
    except Exception as e:
        print(f"[WARN] Wikipedia取得失敗: {e}")

    # ソース2: GitHub datasets/s-and-p-500-companies（フォールバック）
    if not tickers:
        print("[INFO] GitHub CSVからS&P500構成銘柄を取得中...")
        try:
            csv_url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
            resp = requests.get(csv_url, timeout=30)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            tickers = df["Symbol"].str.replace(".", "-", regex=False).tolist()
            tickers = [t.strip() for t in tickers if t.strip()]
            print(f"[INFO] GitHub CSV取得成功: {len(tickers)}銘柄")
        except Exception as e:
            print(f"[WARN] GitHub CSV取得失敗: {e}")

    # 取得成功 → キャッシュ保存
    if tickers and len(tickers) >= 400:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(TICKERS_CACHE, 'w', encoding='utf-8') as f:
            json.dump({
                "fetched_at": datetime.now(JST).isoformat(),
                "count": len(tickers),
                "tickers": tickers
            }, f, ensure_ascii=False, indent=2)
        return tickers

    # すべて失敗 → 期限切れキャッシュを使う
    if os.path.exists(TICKERS_CACHE):
        with open(TICKERS_CACHE, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        print(f"[WARN] 期限切れキャッシュを使用 ({len(cache['tickers'])}銘柄)")
        return cache["tickers"]

    print("[ERROR] S&P500銘柄リストの取得手段がすべて失敗しました")
    sys.exit(1)


def compute_breadth(tickers):
    """
    common/market_data/のdaily/層から銘柄別に取得し、ブレッスデータを
    算出する。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-7: yfinance一括ダウン
    ロード（yf.download()）をcommon.market_data.reader経由に切替
    （2026-08-11）。全銘柄をループしreader.get_price_series(ticker,
    days=260)で生系列を取得し、daily/層内で自前rolling計算する
    （hypecore.py・_get_sp500_ma_deviation()調査で確立した「daily/層内の
    自前計算はreader.pyの層またぎ禁止ルールの例外規定に整合する」という
    設計方針を踏襲）。pct_above_Nmaはreader.get_ma_deviation()と数学的に
    同じ計算だが、52週高安値の算出に既に取得済みの生系列から自前計算する
    ことで銘柄あたりのファイル読み込みを1回に抑える（get_ma_deviation()を
    別途呼ぶと同一銘柄を二重読み込みすることになるため）。

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
    if not HAS_MARKET_DATA:
        print("[ERROR] common.market_data未import。ブレッス算出をスキップします。")
        return None

    print(f"[INFO] {len(tickers)}銘柄をmarket_data daily/層から取得中...")
    start_time = time.time()

    advances = declines = unchanged = 0
    adv_5d = dec_5d = 0
    new_highs = new_lows = 0
    above_50ma = above_200ma = 0
    n_has_50ma = n_has_200ma = 0
    valid_count = 0
    last_date = None
    excluded_date = 0
    n_5d = 0

    # [[MARKETPULSE-BREADTH-MIXED-DATES-1]]（2026-09-26）: 以前は銘柄ごとに「直近2つの
    # 有効終値」で上昇/下落を判定し、日付は全銘柄の最大値をラベルにしていたため、
    # 終値の欠損がある日は異なる日付の前日比が混ざった（2026-09-26のエントリは133銘柄が
    # 09-25、370銘柄が09-24の比較）。まず全銘柄の最新の有効終値日のうち最大の日を
    # 基準日とし、基準日の終値があり、かつ直前の有効終値が基準日の前営業日である
    # 銘柄だけを集計する。5日騰落比は直近6営業日がそろった銘柄だけで数える。
    loaded = []
    for ticker in tickers:
        try:
            series = _md_get_price_series(ticker, days=260)
        except Exception as e:
            print(f"[WARN] {ticker}: 取得失敗 - {e}")
            continue
        real = [r for r in series if not r.get("_gap") and r.get("close") is not None]
        if real:
            loaded.append((ticker, series))
            if last_date is None or real[-1]["date"] > last_date:
                last_date = real[-1]["date"]
    if last_date is None:
        print("[ERROR] 有効な日付が取得できませんでした")
        return None
    last6 = _recent_trading_days(last_date, 6)
    prev_day = last6[-2]

    for ticker, series in loaded:

        # NaN(欠損)が多すぎる銘柄を除外（直近5営業日で実データが3日未満、
        # 旧ロジックのrecent_nan<3条件と同義。_gapは営業日欠損プレース
        # ホルダーのため実データとしてカウントしない）
        recent_real = [r for r in series[-5:] if not r.get("_gap") and r.get("close") is not None]
        if len(recent_real) < 3:
            continue

        vals = [r["close"] for r in series if not r.get("_gap") and r.get("close") is not None]
        dates = [r["date"] for r in series if not r.get("_gap") and r.get("close") is not None]
        if len(vals) < 2:
            continue

        if dates[-1] != last_date or dates[-2] != prev_day:
            excluded_date += 1
            continue

        valid_count += 1
        latest = vals[-1]
        prev = vals[-2]

        # ── 日次 Advance / Decline ──
        ret1d = (latest - prev) / prev if prev else 0.0
        if ret1d > 0.0001:
            advances += 1
        elif ret1d < -0.0001:
            declines += 1
        else:
            unchanged += 1

        # ── 5日 AD Ratio (5日間の累積Advance / 累積Decline) ──
        if len(vals) >= 6 and tuple(dates[-6:]) == last6:
            n_5d += 1
            last6_vals = vals[-6:]
            for i in range(1, 6):
                r = (last6_vals[i] - last6_vals[i - 1]) / last6_vals[i - 1] if last6_vals[i - 1] else 0.0
                if r > 0.0001:
                    adv_5d += 1
                elif r < -0.0001:
                    dec_5d += 1

        # ── 52週新高値 / 新安値 ──
        # 直近の終値 vs 過去252営業日（≒52週）の高値/安値
        lookback = min(252, len(vals) - 1)
        if lookback >= 1:
            window = vals[-lookback:]
            hi = max(window)
            lo = min(window)
            # 新高値: 直近終値が52週高値の99%以上（ほぼ等しいか超えている）
            if latest >= hi * 0.99:
                new_highs += 1
            # 新安値: 直近終値が52週安値の101%以下
            if latest <= lo * 1.01:
                new_lows += 1

        # ── 移動平均上回り率 ──
        if len(vals) >= 50:
            ma50 = sum(vals[-50:]) / 50
            n_has_50ma += 1
            if latest > ma50:
                above_50ma += 1
        if len(vals) >= 200:
            ma200 = sum(vals[-200:]) / 200
            n_has_200ma += 1
            if latest > ma200:
                above_200ma += 1

    elapsed = time.time() - start_time
    print(f"[INFO] 取得完了 ({elapsed:.1f}秒)")
    print(f"[INFO] 有効銘柄数: {valid_count} / {len(tickers)}")

    print(f"[INFO] 基準日{last_date}（前営業日{prev_day}）: 集計{valid_count}銘柄 / "
          f"日付不一致で除外{excluded_date}銘柄 / 5日騰落比の対象{n_5d}銘柄")
    if valid_count < 100:
        print("[ERROR] 有効銘柄が100未満です。データ品質に問題があります。")
        return None

    ad_ratio_1d = round(advances / max(declines, 1), 2)
    ad_ratio_5d = round(adv_5d / max(dec_5d, 1), 2)
    nh_nl_diff = new_highs - new_lows
    pct_above_50ma = round(above_50ma / n_has_50ma * 100, 1) if n_has_50ma else None
    pct_above_200ma = round(above_200ma / n_has_200ma * 100, 1) if n_has_200ma else None

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
        "total_stocks": valid_count,
        "stocks_excluded_date_mismatch": excluded_date,
        "stocks_counted_5d": n_5d,
        "pct_above_50ma": pct_above_50ma,
        "pct_above_200ma": pct_above_200ma,
    }

    print(f"[INFO] ブレッスデータ算出完了: ADV={advances} DEC={declines} "
          f"AD(1d)={ad_ratio_1d} AD(5d)={ad_ratio_5d} "
          f"NH={new_highs} NL={new_lows} NH-NL={nh_nl_diff} "
          f"50MA%={pct_above_50ma} 200MA%={pct_above_200ma}")

    return result


@functools.lru_cache(maxsize=64)
def _recent_trading_days(anchor_date: str, n: int) -> list:
    """anchor_date（'YYYY-MM-DD'、当日を含む）までの直近n営業日（NYSE）を古い順に返す。"""
    import pandas_market_calendars as _mcal
    anchor = datetime.strptime(anchor_date, "%Y-%m-%d").date()
    days = _mcal.get_calendar("NYSE").valid_days(
        start_date=(anchor - timedelta(days=n * 3 + 15)).isoformat(), end_date=anchor.isoformat())
    return tuple(d.strftime("%Y-%m-%d") for d in days[-n:])


def fetch_rsp_spy_divergence():
    """
    RSP（Equal Weight S&P500 ETF）とSPY（Cap Weight S&P500 ETF）の
    騰落率差分から「二極化スコア」を算出する（MP-BREADTH-2）。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-7: yfinance直接呼び出し
    （yf.download(["RSP","SPY"], period="2mo")）をcommon.market_data.
    reader経由に切替（2026-08-11）。RSP・SPYそれぞれget_price_series()で
    生系列を取得し、daily/層内で自前rolling計算する（RSPは同事前調査で
    判明した未収録ETFのため、INDEX_ETF_COMMODITY_SYMBOLSへ2026-08-11に
    追加済み）。

    Returns:
        dict | None: {
            "rsp_return_1d": 0.42,
            "spy_return_1d": 0.31,
            "rsp_spy_divergence_1d": 0.11,
            "rsp_spy_divergence_20d_avg": -0.05
        }
        マイナス = SPY（時価総額加重）のみが上昇 = 二極化（メガキャップ集中）
        プラス   = RSP（均等加重）が優勢 = 広範な上昇（健全な広がり）
    """
    if not HAS_MARKET_DATA:
        print("[WARN] common.market_data未import。RSP/SPY取得スキップ。")
        return None
    try:
        rsp_series = _md_get_price_series("RSP", days=25)
        spy_series = _md_get_price_series("SPY", days=25)
        # 2026-09-26（MARKETPULSE-BREADTH-MIXED-DATES-1）: RSPとSPYを日付でそろえ、
        # 隣り合う営業日同士の騰落率だけを使う（以前は各系列の末尾をそのまま
        # 対応させており、片方の終値が欠けた日は別の日同士を比べていた）
        rsp_map = {r["date"]: r["close"] for r in rsp_series if not r.get("_gap") and r.get("close") is not None}
        spy_map = {r["date"]: r["close"] for r in spy_series if not r.get("_gap") and r.get("close") is not None}
        common = sorted(set(rsp_map) & set(spy_map))
        if len(common) < 2:
            print("[WARN] RSP/SPYの有効データが不足しています")
            return None
        trading = _recent_trading_days(common[-1], 40)
        pos = {d: i for i, d in enumerate(trading)}
        rsp_returns, spy_returns = [], []
        for a, b in zip(common, common[1:]):
            if a not in pos or b not in pos or pos[b] - pos[a] != 1:
                continue
            rsp_returns.append((rsp_map[b] - rsp_map[a]) / rsp_map[a] * 100 if rsp_map[a] else 0.0)
            spy_returns.append((spy_map[b] - spy_map[a]) / spy_map[a] * 100 if spy_map[a] else 0.0)
        if not rsp_returns or not spy_returns:
            print("[WARN] RSP/SPYの有効データが不足しています")
            return None
        divergence_series = [r - s for r, s in zip(rsp_returns, spy_returns)]

        rsp_return_1d = round(rsp_returns[-1], 3)
        spy_return_1d = round(spy_returns[-1], 3)
        divergence_1d = round(divergence_series[-1], 3)

        lookback = min(20, len(divergence_series))
        divergence_20d_avg = round(sum(divergence_series[-lookback:]) / lookback, 3)

        print(f"[INFO] RSP/SPY乖離: 1d={divergence_1d:+.3f}pt 20d平均={divergence_20d_avg:+.3f}pt")

        return {
            "rsp_return_1d": rsp_return_1d,
            "spy_return_1d": spy_return_1d,
            "rsp_spy_divergence_1d": divergence_1d,
            "rsp_spy_divergence_20d_avg": divergence_20d_avg,
        }
    except Exception as e:
        print(f"[WARN] RSP/SPY取得失敗: {e}")
        return None


def _ema(values, span):
    """単純なEMA計算（pandas非依存・Noneをスキップしない前提でNaN除外済みリストを渡す）"""
    if not values:
        return []
    alpha = 2 / (span + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(alpha * v + (1 - alpha) * result[-1])
    return result


def backfill_ad_line_and_mcclellan(all_data):
    """
    蓄積済みbreadth_data.json全件から累積A-DラインとS&P500ベース近似
    マクラレンオシレーターを再計算し、各エントリにフィールドを追加する（MP-BREADTH-2）。

    - ad_line: advances - declines の累積値（市場内部の累積勢い）
    - mcclellan_oscillator: (advances-declines)の19日EMA − 39日EMA
      ※本来のマクラレンオシレーターはNYSE全銘柄ベースだが、本システムは
        S&P500構成銘柄ベースの近似値（画面上にもその旨を明記する）

    全件を毎回再計算する設計（日付抜け・遡及修正があっても整合性が取れるよう、
    差分更新ではなく毎回フルリビルドする）。
    """
    if not all_data:
        return all_data

    sorted_data = sorted(all_data, key=lambda x: x["date"])
    net_advances = [
        (d.get("advances") or 0) - (d.get("declines") or 0)
        for d in sorted_data
    ]

    # 累積A-Dライン
    cumulative = 0
    ad_lines = []
    for v in net_advances:
        cumulative += v
        ad_lines.append(cumulative)

    # マクラレンオシレーター（19日EMA - 39日EMA）
    ema19 = _ema(net_advances, 19)
    ema39 = _ema(net_advances, 39)
    mcclellan = [round(a - b, 1) for a, b in zip(ema19, ema39)]

    for d, ad_line, mc in zip(sorted_data, ad_lines, mcclellan):
        d["ad_line"] = ad_line
        d["mcclellan_oscillator"] = mc

    return sorted_data


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

    # A-Dライン・マクラレンオシレーターを全件分バックフィル（MP-BREADTH-2）
    all_data = backfill_ad_line_and_mcclellan(all_data)

    with open(BREADTH_JSON, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"[INFO] breadth_data.json保存完了 (全{len(all_data)}件)")


if __name__ == "__main__":
    tickers = get_sp500_tickers()
    breadth = compute_breadth(tickers)
    if breadth:
        rsp_spy = fetch_rsp_spy_divergence()
        if rsp_spy:
            breadth.update(rsp_spy)
        save_breadth(breadth)
        print("[OK] ブレッスデータ処理完了")
    else:
        print("[ERROR] ブレッスデータ算出に失敗しました")
        sys.exit(1)
