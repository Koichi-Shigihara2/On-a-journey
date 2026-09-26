"""
common/market_data/reader.py

yfinance統合層の読み取りAPI（BACKLOG [[MARKETDATA-LAYER-CONSTRUCTION-1]]、
着手順序2）。fetcher.pyが書き込むdaily/・attributes/・analyst_history/を
読み取る唯一のモジュールとして新設する。

【層またぎ再計算の禁止（BACKLOG確定事項4）】
daily/（日次OHLCV）・attributes/（週次.infoスナップショット）・
analyst_history/（イベント履歴）は取得時点が異なる独立したサブレイヤーで
あり、本モジュールの各APIはそれぞれ自分のレイヤーのみを参照する。
特に get_attributes() が返すPER/PEG/PSR/EV_EBITDA等はすべて同一.info
呼び出し由来の自己完結した値であり、get_price_series()等の別時点データ
（daily/の株価）と組み合わせて再計算してはならない（例:
attributes/のtrailing_peとdaily/の別日の株価を組み合わせて実質PERを
再計算する、等）。get_ma_deviation()のみ例外的にdaily/内部（同一レイヤー）
で完結する計算を行う。

現時点でこのモジュールを参照する本番消費者は存在しない（グリーンフィールド
実装）。既存の8本番消費者・2診断ツール・2周辺ツールの切替は別タスク
（BACKLOG着手順序3・4）で行う。データが一切存在しない銘柄（fetcher.py
未実行）に対する呼び出しは、例外ではなくNone/空リスト/空dictを返す
（エラー耐性）。
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .fetcher import MARKET_DATA_DIR, get_nyse_calendar, get_sp500_constituents

__all__ = [
    "get_latest_price",
    "get_price_series",
    "get_price_series_as_of",
    "get_price_on_or_after",
    "get_ma_deviation",
    "get_attributes",
    "get_analyst_events",
    "get_earnings_history",
    "get_recommendations_history",
    "get_calendar",
    "get_index_series",
    "get_sp500_constituents_prices",
]


def _resolve_base_dir(base_dir: Optional[str]) -> str:
    return base_dir if base_dir is not None else MARKET_DATA_DIR


def _load_json(path: str, default: Any) -> Any:
    """JSON読み込み。ファイル不在・パース失敗時は例外を投げずdefaultを返す
    （エラー耐性。fetcher.py未実行の銘柄への呼び出しがここで吸収される）。
    """
    if not os.path.exists(path):
        return json.loads(json.dumps(default))
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"   [WARN] {path} 読み込み失敗（初期値で継続）: {e}")
        return json.loads(json.dumps(default))


def _load_daily_payload(symbol: str, base_dir: str) -> Dict[str, Any]:
    path = os.path.join(base_dir, "daily", f"{symbol}.json")
    return _load_json(path, default={"symbol": symbol, "records": []})


# ── 日次価格層 ────────────────────────────────────────────

def _has_valid_close(record: Dict[str, Any]) -> bool:
    close = record.get("close")
    return isinstance(close, (int, float)) and close == close and close > 0


def get_latest_price(symbol: str, base_dir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """daily/{SYMBOL}.jsonのうち、有効な終値を持つ最新の1件（date/open/high/low/
    close/volume/_validation_warnings）を返す。`date`がその終値の日付。
    有効な終値を持つ行が無い場合はNoneを返す。

    [[MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1]]（2026-09-26）: 以前は終値の無い
    行でも最新であれば返していたため、TANUKI VALUATIONがcurrent_price=0.0で
    計算を続け（2026-09-22に99銘柄・09-26に78銘柄）、Stonks Siloもcurrent_price=
    Noneになっていた。消費者は返り値の`date`で基準日を確認できる。
    """
    symbol = symbol.upper()
    base = _resolve_base_dir(base_dir)
    records = [r for r in _load_daily_payload(symbol, base).get("records", []) if _has_valid_close(r)]
    if not records:
        return None
    latest = dict(sorted(records, key=lambda r: r.get("date", ""))[-1])
    latest["symbol"] = symbol
    return latest


def _expected_trading_days(anchor_date: str, days: int, calendar=None) -> List[str]:
    """anchor_date（'YYYY-MM-DD'）を最終日とする、直近days営業日分の日付
    文字列リストを返す（NYSEカレンダー基準、BACKLOG確定事項1）。
    """
    if days <= 0:
        return []
    cal = calendar or get_nyse_calendar()
    anchor = datetime.strptime(anchor_date, "%Y-%m-%d").date()
    # 長期休場クラスタでも確実にdays件確保できるよう余裕を持って遡る
    lookback_start = anchor - timedelta(days=days * 3 + 15)
    valid_days = cal.valid_days(start_date=lookback_start.isoformat(), end_date=anchor.isoformat())
    tail = valid_days[-days:]
    return [d.strftime("%Y-%m-%d") for d in tail]


def _price_series_ending_at(symbol: str, anchor_date: str, days: int,
                             base_dir: str) -> List[Dict[str, Any]]:
    """anchor_date（'YYYY-MM-DD'）を終点として、そこから遡ったdays営業日分の
    日次レコードを返す共通実装（MARKETDATA-LAYER-CONSTRUCTION-1着手順序6-2）。
    get_price_series()・get_price_series_as_of()の双方から呼ばれる
    （終点日のみが異なる）。

    NYSE営業日カレンダー（pandas_market_calendars）で「本来存在すべき
    営業日集合」を計算し、実際に保存されているレコード日付集合と突合する
    （BACKLOG確定事項1）。データが1件でも存在する銘柄については、返り値は
    常にちょうどdays件になる（欠損している営業日は`{"date": ..., "_gap":
    True}`のプレースホルダーとして埋め込まれる。実データがある日は
    `_gap: False`）。データが一切存在しない銘柄・days<=0の場合は空リストを
    返す（例外は発生させない）。
    """
    records_list = _load_daily_payload(symbol, base_dir).get("records", [])
    records = {r["date"]: r for r in records_list if r.get("date")}
    if not records or days <= 0:
        return []

    expected_dates = _expected_trading_days(anchor_date, days)

    series: List[Dict[str, Any]] = []
    for d in expected_dates:
        if d in records:
            row = dict(records[d])
            row["symbol"] = symbol
            row["_gap"] = False
            series.append(row)
        else:
            series.append({"date": d, "symbol": symbol, "_gap": True})
    return series


def get_price_series(symbol: str, days: int = 30, base_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """直近days営業日分の日次レコードを返す（終点＝daily/の最新保存日）。

    層またぎ再計算禁止（BACKLOG確定事項4）: 本APIが返すOHLCV系列は
    daily/{SYMBOL}.jsonの生データそのものであり、attributes/{SYMBOL}.jsonの
    PER/PEG等（別時点の.infoスナップショット）と組み合わせて再計算しては
    ならない。
    """
    symbol = symbol.upper()
    base = _resolve_base_dir(base_dir)
    records_list = _load_daily_payload(symbol, base).get("records", [])
    records = {r["date"]: r for r in records_list if r.get("date")}
    if not records or days <= 0:
        return []

    anchor_date = max(records.keys())
    return _price_series_ending_at(symbol, anchor_date, days, base)


def get_price_series_as_of(symbol: str, as_of_date: Any, days: int = 30,
                            base_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """as_of_date（'YYYY-MM-DD'文字列またはdate/datetime）を終点として、
    そこから遡ったdays営業日分の日次レコードを返す。get_price_series()の
    「終点＝現在（daily/の最新保存日）」を任意の過去日に一般化した版
    （MARKETDATA-LAYER-CONSTRUCTION-1着手順序6-2、backfill_tech_pulse.py型の
    一過性バックフィルツールが「過去の欠落エントリを埋める際、その基準日
    時点でのトレイリングウィンドウが必要」というクエリ形状に対応するため
    新規追加）。

    daily/の最新保存日より未来のas_of_dateを渡した場合、未保存分の営業日は
    すべて`_gap: True`として返る（例外は発生させない、既存のエラー耐性
    方針を維持）。データが一切存在しない銘柄・days<=0の場合は空リストを
    返す。

    層またぎ再計算禁止（BACKLOG確定事項4）: 本APIが返すOHLCV系列は
    daily/{SYMBOL}.jsonの生データそのものである。
    """
    symbol = symbol.upper()
    base = _resolve_base_dir(base_dir)
    date_str = as_of_date.isoformat()[:10] if hasattr(as_of_date, "isoformat") else str(as_of_date)[:10]
    return _price_series_ending_at(symbol, date_str, days, base)


def get_price_on_or_after(symbol: str, date: Any, base_dir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """指定日付（date、'YYYY-MM-DD'文字列またはdate/datetime）以降で最初に
    見つかる取引日のレコード（date/open/high/low/close/volume等）を返す。

    daily/{SYMBOL}.jsonの全期間データから、date <= record.date <=
    date+5日の範囲で最も早い日付のレコードを検索する（
    score_verifier.py::fetch_price_after()と同じクエリ形状: start=target,
    end=target+5日ウィンドウで先頭値を採用するロジックをdaily/層のデータに
    対して再現、BACKLOG [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序5-2）。
    5営業日以内に該当データが見つからない場合、またはシンボル自体の
    データが存在しない場合はNoneを返す（例外は発生させない）。

    層またぎ再計算禁止（BACKLOG確定事項4）: 本APIはdaily/のみを参照する。
    """
    symbol = symbol.upper()
    base = _resolve_base_dir(base_dir)
    date_str = date.isoformat()[:10] if hasattr(date, "isoformat") else str(date)[:10]
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    target_str = target.isoformat()
    window_end_str = (target + timedelta(days=5)).isoformat()

    records = _load_daily_payload(symbol, base).get("records", [])
    candidates = [
        r for r in records
        if r.get("date") and target_str <= r["date"] <= window_end_str and _has_valid_close(r)
    ]  # 終値の無い行は飛ばして次の取引日を採る（MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1）
    if not candidates:
        return None

    chosen = dict(min(candidates, key=lambda r: r["date"]))
    chosen["symbol"] = symbol
    return chosen


def get_ma_deviation(symbol: str, window: int = 50, base_dir: Optional[str] = None) -> Optional[float]:
    """直近window日移動平均に対する最新終値の乖離率（%）を返す。

    get_price_series(symbol, days=window)から都度計算する
    （twoHundredDayAverage等のyfinance事前計算値は保存しない設計、
    BACKLOG確定事項7）。window日分の実データ（欠損営業日を除く）が
    揃っていない場合はNoneを返す。

    層またぎ再計算禁止（BACKLOG確定事項4）: 本関数はdaily/のみを参照する
    （同一レイヤー内の計算のみ許容される例外）。attributes/由来の値と
    混在させてはならない。
    """
    series = get_price_series(symbol, days=window, base_dir=base_dir)
    if not series:
        return None

    closes = [r.get("close") for r in series if not r.get("_gap") and r.get("close") is not None]
    if len(closes) < window:
        return None

    latest_close = series[-1].get("close")
    if series[-1].get("_gap") or latest_close is None:
        return None

    ma = sum(closes) / window
    if ma == 0:
        return None
    return (latest_close - ma) / ma * 100.0


# ── 週次準静的属性層 ──────────────────────────────────────

def get_attributes(symbol: str, base_dir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """attributes/{SYMBOL}.jsonの最新レコード（fetched_at含む）を返す。
    データが存在しない場合はNoneを返す。

    層またぎ再計算禁止（BACKLOG確定事項4）: 返り値のPER/PEG/PSR/EV_EBITDA/
    β等はすべて同一.info呼び出し由来の自己完結した値であり、
    get_price_series()等の別時点データと組み合わせて再計算してはならない。
    """
    symbol = symbol.upper()
    base = _resolve_base_dir(base_dir)
    path = os.path.join(base, "attributes", f"{symbol}.json")
    if not os.path.exists(path):
        return None
    record = _load_json(path, default=None)
    if not record:
        return None
    record = dict(record)
    record["symbol"] = symbol
    return record


def get_calendar(symbol: str, base_dir: Optional[str] = None) -> Dict[str, Any]:
    """次回決算日等のカレンダー情報を返す。

    fetcher.py（fetch_weekly_attributes）は現時点では次回決算日等の
    カレンダー情報を取得・保存していないため、常に空dictを返す
    （BACKLOG設計: 「fetcher.py側で取得済みの場合、なければ空dict」）。
    attributes/{SYMBOL}.jsonに将来`calendar`キーが追加された場合はそれを
    そのまま返す設計とし、reader.py側の追加実装なしに対応できるようにする。
    """
    attrs = get_attributes(symbol, base_dir=base_dir)
    if not attrs:
        return {}
    calendar = attrs.get("calendar")
    return calendar if isinstance(calendar, dict) else {}


# ── イベント履歴層 ────────────────────────────────────────

def get_analyst_events(symbol: str, since: Optional[Any] = None,
                        base_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """analyst_history/{SYMBOL}.jsonのイベント一覧（新しい順）を返す。

    since: 'YYYY-MM-DD'文字列 または date/datetime。指定した場合、
    date >= since のイベントのみに絞り込む。データが存在しない場合は
    空リストを返す。
    """
    symbol = symbol.upper()
    base = _resolve_base_dir(base_dir)
    path = os.path.join(base, "analyst_history", f"{symbol}.json")
    events = _load_json(path, default={"symbol": symbol, "events": []}).get("events", [])
    if since is None:
        return events
    since_str = since.isoformat()[:10] if hasattr(since, "isoformat") else str(since)[:10]
    return [e for e in events if (e.get("date") or "") >= since_str]


def get_earnings_history(symbol: str, base_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """analyst_history/{SYMBOL}.jsonのearnings_history一覧（新しい順、
    四半期末日quarterで降順ソート済み）を返す。データが存在しない場合は
    空リストを返す。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8前提作業3で
    fetcher.py::fetch_analyst_events()に追加されたearnings_history
    （直近3〜4四半期の確定済み実績、quarter一意キーで追記型保存）を
    そのまま返す。surprise_percentはYahoo生値（小数）のままで%変換は
    行われていない（fetcher.py側の方針、消費側で必要に応じ変換する）。
    """
    symbol = symbol.upper()
    base = _resolve_base_dir(base_dir)
    path = os.path.join(base, "analyst_history", f"{symbol}.json")
    return _load_json(path, default={"symbol": symbol, "earnings_history": []}).get("earnings_history", [])


def get_recommendations_history(symbol: str, latest_only: bool = True,
                                 base_dir: Optional[str] = None) -> Any:
    """analyst_history/{SYMBOL}.jsonのrecommendations_history
    （週次スナップショットの蓄積、取得日で降順ソート済み）を返す。

    latest_only=True（既定）: 最新1件のスナップショット（dict）を返す。
    データが存在しない場合は空dict{}を返す。
    latest_only=False: 全履歴（リスト、新しい順）を返す。データが存在
    しない場合は空リスト[]を返す。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8前提作業3で
    fetcher.py::fetch_analyst_events()に追加されたrecommendations_
    history（"0m"時点のstrongBuy/buy/hold/sell/strongSell生カウント・
    buy_hold_ratio、取得日をキーに週次蓄積）をそのまま返す。
    upgrades_downgrades等の不変イベントとは異なり「取得時点の
    コンセンサススナップショット」という性質を持つ点に注意
    （fetcher.py::fetch_analyst_events()のdocstring参照）。
    """
    symbol = symbol.upper()
    base = _resolve_base_dir(base_dir)
    path = os.path.join(base, "analyst_history", f"{symbol}.json")
    history = _load_json(
        path, default={"symbol": symbol, "recommendations_history": []}
    ).get("recommendations_history", [])
    if not latest_only:
        return history
    return history[0] if history else {}


# ── 指数/ETF/商品（daily/と同一スキーマ） ───────────────────

def get_index_series(symbol: str, days: int = 30, base_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """指数/ETF/商品の日次系列を返す。daily/{SYMBOL}.jsonは銘柄種別
    （個別株/指数/ETF/商品）を区別せず同一形式で保存されるため、
    実装はget_price_series()への単純なエイリアス。
    """
    return get_price_series(symbol, days=days, base_dir=base_dir)


# ── 銘柄横断アクセサ ──────────────────────────────────────

def get_sp500_constituents_prices(symbols: Optional[List[str]] = None,
                                   base_dir: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """S&P500構成銘柄（省略時はfetcher.get_sp500_constituents()）の
    daily/{SYMBOL}.json最新レコードを一括取得する（breadth計算用）。

    daily/にデータが存在しない銘柄（fetcher.py未実行分）は結果dictに
    含めない。呼び出し側は`len(result)`と対象銘柄数の差でカバレッジを
    確認できる。symbolsを明示指定した場合はS&P500構成銘柄取得（ネットワーク
    アクセスを伴いうる）を省略できる。
    """
    base = _resolve_base_dir(base_dir)
    target_symbols = symbols if symbols is not None else get_sp500_constituents(base_dir=base)
    result: Dict[str, Dict[str, Any]] = {}
    for symbol in target_symbols:
        price = get_latest_price(symbol, base_dir=base)
        if price is not None:
            result[symbol.upper()] = price
    return result
