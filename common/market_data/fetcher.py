"""
common/market_data/fetcher.py

yfinance統合層のデータ取得モジュール（BACKLOG [[MARKETDATA-LAYER-CONSTRUCTION-1]]、
2026-08-08確定の設計に基づく実装）。3つの独立したサブレイヤーごとに
取得関数を提供する:

    fetch_daily_prices(symbols)      日次価格層   → daily/{SYMBOL}.json
    fetch_weekly_attributes(symbols) 週次準静的属性層 → attributes/{SYMBOL}.json
    fetch_analyst_events(symbols)    イベント履歴層  → analyst_history/{SYMBOL}.json

backfill_daily_prices(symbols, period="1y") は一過性ツール（定期cronには
組み込まない、backfill_tech_pulse.py型）。日次収集開始前の過去分を一括
取得し、200日移動平均（get_ma_deviation(window=200)）等が即座に計算可能な
状態までdaily/を埋める。手動実行専用（CLIの--backfillフラグ経由）。

いずれも保存前に恒等式検証（validate_price_record / validate_attributes_record）
を行うが、検証失敗時も保存は拒否しない（_validation_warningsフィールドに
記録した上で保存継続。common/sec_data/parser.py の fy_collision_log.json と
同じ「検知のみ・自動修正なし」方針を踏襲、日次連続性に穴を作らないため）。
検証結果は毎回 {SYMBOL}/market_data_violations_log.json に書き込む（0件でも
書き込む、fy_collision_log.json型の化石ファイル対策）。

daily/・attributes/・analyst_history/・violations_logの書き込みは全て
tempfile→os.replace()でアトミック化する（BACKLOG確定事項3）。

本番消費者切替（BACKLOG着手順序4）が開始しており、
src/value/tanuki_valuation/beta_fetcher.py がcommon.market_data.reader
経由で本モジュールの生成データ（attributes/）に依存する最初の消費者と
なった（2026-08-11）。残り7本番消費者・2診断ツール・2周辺ツールの切替は
順次対応する。
"""

import io
import json
import math
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import yaml
import yfinance as yf
import pandas_market_calendars as mcal

# ── パス設定 ──────────────────────────────────────────────
MARKET_DATA_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(MARKET_DATA_DIR))  # common/market_data -> common -> リポジトリルート
_MONITOR_TICKERS_YAML = os.path.join(_REPO_ROOT, "config", "monitor_tickers.yaml")
_SP500_CACHE_FILENAME = "_sp500_constituents_cache.json"

# common.yfinance_utils.safe_yf_ticker()（リトライ2回・待機3秒）経由で
# .info を呼ぶための準備（src/value/tanuki_valuation/beta_fetcher.pyと同型
# パターン）。本ファイルはGitHub Actions上で`python common/market_data/
# fetcher.py ...`のようにスクリプトとして直接実行されるため、リポジトリ
# ルートがsys.pathに乗っていないと`from common.yfinance_utils import ...`
# が失敗する（相対importは`python -m`経由でない直接実行では使えないため
# 採用しない。実測確認済み）。
_repo_str = str(_REPO_ROOT)
if _repo_str not in sys.path:
    sys.path.insert(0, _repo_str)

try:
    from common.yfinance_utils import safe_yf_ticker as _safe_yf_ticker
    _USE_SAFE_YF = True
except ImportError:
    _USE_SAFE_YF = False

# 指数/ETF/商品ユニバース（既存消費者12ファイルの実使用実態から抽出、
# BACKLOG [[MARKETDATA-LAYER-CONSTRUCTION-1]]投資調査参照）。網羅リストでは
# なく、本番消費者切替時（着手順序3・4）に個別ファイルの参照銘柄と突合して
# 追加検討する前提の初期セット。
INDEX_ETF_COMMODITY_SYMBOLS: List[str] = [
    # 指数（Market Pulse: collect_and_send.py）
    "^GSPC", "^IXIC", "^DJI", "^NYA", "^RUT", "^VIX", "^VIX9D", "^TNX", "^N225",
    # ETF（Market Pulse: collect_and_send.py・breadth_calculator.py）
    "SPY", "QQQ", "GLD", "TLT", "HYG",
    # ETF（Market Pulse: collect_and_send.py、[[MARKETDATA-LAYER-
    # CONSTRUCTION-1]]着手順序4-6事前調査で判明した未収録分。IVW/IVEは
    # sentiment_scoreのgrowth_value〈9.0%〉、LQDはhyg_lqd_dir〈10.8%〉の
    # 算出に必須、2026-08-11追加）
    "IVW", "IVE", "LQD",
    # 商品先物（Market Pulse: collect_and_send.py）
    "CL=F", "GC=F",
    # 為替（Market Pulse: collect_and_send.py、ドル円表示専用。同事前調査で
    # 発見、2026-08-11追加）
    "JPY=X",
    # ETF（Market Pulse: breadth_calculator.py、[[MARKETDATA-LAYER-
    # CONSTRUCTION-1]]着手順序4-7事前調査で判明した未収録分。RSPは
    # fetch_rsp_spy_divergence()〈sentiment_scoreのrsp_spy_divergence
    # 10.0%〉の算出に必須、2026-08-11追加。SPYは既収録）
    "RSP",
    # ETF（Market Pulse: collect_and_send.py::collect_asset_flow()、
    # [[MARKETDATA-COLLECT-ASSET-FLOW-UNTRACKED-1]]で判明した未収録分。
    # SHV（超短期国債ETF）は資産フロー可視化7資産中の1つで唯一未収録
    # だったため_fetch_hist_legacy()の対象として残存していた、2026-08-13追加）
    "SHV",
]


# ── 汎用ユーティリティ ────────────────────────────────────

def _resolve_base_dir(base_dir: Optional[str]) -> str:
    return base_dir if base_dir is not None else MARKET_DATA_DIR


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _numeric_or_none(value: Any) -> Optional[float]:
    """yfinance .infoの数値フィールドをNoneへ正規化する型ガード。

    ZETAの`trailingPE`が文字列"Infinity"として返るケースを実測確認済み
    （[[MARKETDATA-TRAILING-PE-STRING-INFINITY-1]]）。boolはPythonの
    isinstance(x, int)がTrueになる罠があるためint判定より先に除外する。
    """
    if isinstance(value, bool):
        return None
    if not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _dedupe_symbols(symbols: Optional[List[str]]) -> List[str]:
    seen = set()
    result: List[str] = []
    for s in symbols or []:
        s = str(s).strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        result.append(s)
    return result


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


def _to_int(value: Any) -> Optional[int]:
    f = _to_float(value)
    return int(f) if f is not None else None


def _to_str_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def _atomic_write_json(path: str, payload: Any) -> None:
    """tempfile→os.replace()でアトミック書き込みする（BACKLOG確定事項3）。

    書き込み中にプロセスが中断しても、os.replace()は同一ファイルシステム内
    でアトミックなrenameのため、既存ファイルは最後まで完了した版のまま残る
    （中断時に破損した中間状態が観測されることはない）。
    """
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def _load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return json.loads(json.dumps(default))
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"   [WARN] {path} 読み込み失敗（初期値で継続）: {e}")
        return json.loads(json.dumps(default))


# ── NYSE営業日カレンダー（BACKLOG確定事項1） ──────────────

_NYSE_CALENDAR = None


def get_nyse_calendar():
    """pandas_market_calendarsのNYSEカレンダーを返す（プロセス内でキャッシュ）。"""
    global _NYSE_CALENDAR
    if _NYSE_CALENDAR is None:
        _NYSE_CALENDAR = mcal.get_calendar("NYSE")
    return _NYSE_CALENDAR


def is_trading_day(day: Any, calendar=None) -> bool:
    """指定日がNYSE営業日かを判定する。

    day: date/datetime または 'YYYY-MM-DD' 文字列。
    層またぎ再計算の禁止（BACKLOG確定事項4）によりreader.py側の連続性
    保証（get_price_series()）とは別の軽量な単日判定ユーティリティ。
    """
    cal = calendar or get_nyse_calendar()
    day_str = day.isoformat() if hasattr(day, "isoformat") else str(day)
    valid_days = cal.valid_days(start_date=day_str, end_date=day_str)
    return len(valid_days) > 0


# ── 保存前検証（BACKLOG確定事項5） ────────────────────────

def validate_price_record(record: Dict[str, Any]) -> List[str]:
    """日次価格レコードの恒等式検証。検証失敗時も保存は拒否しない
    （呼び出し側が_validation_warningsとして記録した上で保存する）。

    検証項目:
        - open/high/low/close（存在するもののみ） > 0
        - volume > 0
        - high >= close >= low
        - fifty_two_week_high >= fifty_two_week_low（両方存在する場合のみ。
          fetch_daily_prices()は.history()/yf.download()のみを使うため
          通常はこのフィールド自体が存在しない。将来的にyfinance側の値を
          付与するケースに備えた任意項目）
    """
    warnings: List[str] = []

    for field in ("open", "high", "low", "close"):
        if field not in record:
            continue
        value = record.get(field)
        if value is None or not (isinstance(value, (int, float)) and value > 0):
            warnings.append(f"{field} must be > 0 (got {value!r})")

    if "volume" in record:
        volume = record.get("volume")
        if volume is None or not (isinstance(volume, (int, float)) and volume > 0):
            warnings.append(f"volume must be > 0 (got {volume!r})")

    high, close, low = record.get("high"), record.get("close"), record.get("low")
    if high is not None and close is not None and low is not None:
        if not (high >= close >= low):
            warnings.append(
                f"expected high >= close >= low (got high={high}, close={close}, low={low})"
            )

    wk_high = record.get("fifty_two_week_high")
    wk_low = record.get("fifty_two_week_low")
    if wk_high is not None and wk_low is not None:
        if not (wk_high >= wk_low):
            warnings.append(
                f"expected fifty_two_week_high >= fifty_two_week_low "
                f"(got high={wk_high}, low={wk_low})"
            )

    return warnings


def validate_attributes_record(record: Dict[str, Any]) -> List[str]:
    """週次属性レコードの恒等式検証（時価総額 ≈ 株価×発行済株式数）。

    許容範囲: 相対2%または絶対$1,000,000のいずれか大きい方
    （common/sec_data/parser.py の _BS_IDENTITY_TOL_REL=0.02 を踏襲し、
    小型株向けに絶対フロアを追加。BACKLOG確定事項5）。
    current_price/shares_outstanding/market_cap のいずれかが欠落している
    場合（指数等）は検証をスキップする。
    """
    warnings: List[str] = []
    price = record.get("current_price")
    shares = record.get("shares_outstanding")
    reported_mcap = record.get("market_cap")

    if price is not None and shares is not None and reported_mcap is not None:
        computed_mcap = price * shares
        tolerance = max(reported_mcap * 0.02, 1_000_000)
        if abs(computed_mcap - reported_mcap) > tolerance:
            warnings.append(
                f"market_cap mismatch: reported={reported_mcap}, "
                f"computed(price*shares)={computed_mcap}, tolerance={tolerance}"
            )

    return warnings


# ── 検証結果ログ（fy_collision_log.json型） ────────────────

def _violations_log_path(symbol: str, base_dir: str) -> str:
    return os.path.join(base_dir, symbol, "market_data_violations_log.json")


def _write_violations_section(symbol: str, section_key: str, section_value: Dict[str, Any],
                               base_dir: str) -> None:
    """market_data_violations_log.json の指定セクションのみを更新する。

    daily_price_validation（日次）とattributes_validation（週次）は独立した
    頻度で実行されるため、片方の実行が他方の最新記録を上書きしないよう
    セクション単位でread-modify-writeする。0件でも毎回書き込む
    （fy_collision_log.json型の化石ファイル対策）。
    """
    path = _violations_log_path(symbol, base_dir)
    payload = _load_json(path, default={"ticker": symbol})
    payload["ticker"] = symbol
    payload[section_key] = section_value
    _atomic_write_json(path, payload)


# ── 日次価格層 ────────────────────────────────────────────

def _download_historical_bars(symbols: List[str], period: str = "5d",
                               start: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
    """yf.download()で複数銘柄の日足を一括取得し、銘柄ごとのOHLCVレコード
    のリスト（日付昇順）を返す（取得できなかった銘柄はキーごと欠落する）。
    日次バッチ（直近1件のみ使用）とバックフィル（全件使用）の共通実装。

    start（'YYYY-MM-DD'形式、省略可）を指定した場合はperiodを無視し
    yf.download(symbols, start=start, ...)で日付範囲を直接指定する
    （[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8前提作業1、
    2026-08-11追加）。`period="5y"`は「本日から遡って5年」を意味し
    時間経過で開始日がスライドするため、hypecore.py切替が要求する固定
    開始日（2021-01-01）を正確に満たせない（事前調査で実測: period="5y"
    は2021-08-11開始と約7ヶ月不足、period="max"は1980年からと約8.2倍の
    過剰取得と判明）。start指定はこの両方の問題を解消する。
    """
    if not symbols:
        return {}
    try:
        if start is not None:
            raw = yf.download(symbols, start=start, group_by="ticker",
                               auto_adjust=False, progress=False, threads=True)
        else:
            raw = yf.download(symbols, period=period, group_by="ticker",
                               auto_adjust=False, progress=False, threads=True)
    except Exception as e:
        print(f"   [WARN] yf.download失敗: {e}")
        return {}
    if raw is None or raw.empty:
        return {}

    results: Dict[str, List[Dict[str, Any]]] = {}
    for symbol in symbols:
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                if symbol not in raw.columns.get_level_values(0):
                    print(f"   [{symbol}] データが取得結果に含まれない（スキップ）")
                    continue
                sub = raw[symbol]
            else:
                sub = raw
            sub = sub.dropna(how="all")
            if sub.empty:
                print(f"   [{symbol}] データが空（スキップ）")
                continue
            bars: List[Dict[str, Any]] = []
            for idx, row in sub.iterrows():
                bars.append({
                    "date": idx.strftime("%Y-%m-%d"),
                    "open": _to_float(row.get("Open")),
                    "high": _to_float(row.get("High")),
                    "low": _to_float(row.get("Low")),
                    "close": _to_float(row.get("Close")),
                    "volume": _to_int(row.get("Volume")),
                })
            results[symbol] = bars
        except Exception as e:
            print(f"   [{symbol}] データ解析エラー: {e}")
    return results


def _merge_daily_records(existing_records: List[Dict[str, Any]],
                          new_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """日付をキーに新レコードで置き換え（同一日付は新しい方で上書き）、
    日付昇順でソートして返す。fetch_daily_prices()の_append_daily_record()
    と同じ「同一date上書き」冪等性設計をリスト全体のマージに一般化した版
    （バックフィルで既存の単発日次取得分を消さずに過去分を統合するため）。
    """
    merged = {r["date"]: r for r in existing_records if r.get("date")}
    for r in new_records:
        if r.get("date"):
            merged[r["date"]] = r
    return sorted(merged.values(), key=lambda r: r.get("date", ""))


def _append_daily_record(symbol: str, record: Dict[str, Any], base_dir: str) -> None:
    """daily/{symbol}.json に1営業日分のレコードを追記する（同一dateは上書き、
    記録キーは日付ベースで重複排除するリポジトリ規約に準拠）。
    """
    path = os.path.join(base_dir, "daily", f"{symbol}.json")
    payload = _load_json(path, default={"symbol": symbol, "records": []})
    records = [r for r in payload.get("records", []) if r.get("date") != record["date"]]
    records.append(record)
    records.sort(key=lambda r: r.get("date", ""))
    payload["symbol"] = symbol
    payload["records"] = records
    _atomic_write_json(path, payload)


def _has_valid_close(bar: Dict[str, Any]) -> bool:
    """終値が数値で0より大きい足か（終値の無い未確定の足を確定値として扱わない）。"""
    close = bar.get("close")
    return isinstance(close, (int, float)) and not math.isnan(close) and close > 0


def _repair_close_missing_records(symbol: str, valid_bars: List[Dict[str, Any]], base_dir: str) -> int:
    """daily/{symbol}.jsonのうち終値の無い行を、同じ日付の終値つきの足で置き換える。
    置き換えた件数を返す（[[MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1]]の自己修復）。"""
    path = os.path.join(base_dir, "daily", f"{symbol}.json")
    payload = _load_json(path, default=None)
    if not payload or not valid_bars:
        return 0
    by_date = {b["date"]: b for b in valid_bars}
    repaired = 0
    records = []
    for r in payload.get("records", []):
        if not _has_valid_close(r) and r.get("date") in by_date:
            fixed = dict(by_date[r["date"]])
            fixed["_validation_warnings"] = validate_price_record(fixed)
            records.append(fixed)
            repaired += 1
        else:
            records.append(r)
    if repaired:
        payload["records"] = records
        _atomic_write_json(path, payload)
        print(f"   [{symbol}] 終値の無い行を{repaired}件取り直した")
    return repaired


def fetch_daily_prices(symbols: List[str], base_dir: Optional[str] = None) -> None:
    """日次価格層を取得・保存する。

    yf.download()で対象銘柄を一括取得し、銘柄ごとに直近1営業日分のOHLCVを
    daily/{SYMBOL}.jsonへ追記する。保存前にvalidate_price_record()で検証し、
    失敗しても保存は拒否しない（_validation_warningsに記録の上で保存継続）。
    検証結果は{SYMBOL}/market_data_violations_log.jsonへ毎回書き込む。

    本番バッチではsymbolsにget_default_symbol_universe()（S&P500構成銘柄＋
    監視銘柄＋指数/ETF/商品の和集合）を渡す想定。
    """
    base = _resolve_base_dir(base_dir)
    target_symbols = _dedupe_symbols(symbols)
    if not target_symbols:
        return

    # [[MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1]]（2026-09-26）: 直近1件だけを
    # 無条件に保存していたため、yfinanceが終値の無い足（始値・高値・安値・途中の
    # 出来高のみ）を返した日にそれが確定値として保存され、以後二度と取り直されな
    # かった（2026-09-21に510銘柄・09-25に379銘柄）。5日分の足のうち終値のある
    # 足だけを扱い、終値の無い足は保存しない（次回実行時の5日窓で取り直される）。
    # あわせて、既存の終値の無い行が5日窓内で終値つきで取れた場合は置き換える
    # （自己修復）。既に終値のある過去の行は上書きしない（他システムの入力を
    # 動かさないため）。
    history = _download_historical_bars(target_symbols, period="5d")
    for symbol in target_symbols:
        symbol_bars = history.get(symbol) or []
        if not symbol_bars:
            print(f"   [{symbol}] 日次データ取得失敗（スキップ）")
            continue
        valid_bars = [b for b in symbol_bars if _has_valid_close(b)]
        if not _has_valid_close(symbol_bars[-1]):
            print(f"   [{symbol}] {symbol_bars[-1]['date']}の足に終値が無い（未確定のため保存せず、次回取り直す）")
            _write_violations_section(
                symbol, "daily_price_unconfirmed",
                {"checked_at": _now_iso(), "date": symbol_bars[-1]["date"],
                 "note": "終値が無い足のため保存しなかった（次回実行時に取り直す）"},
                base_dir=base,
            )
        _repair_close_missing_records(symbol, valid_bars, base_dir=base)
        if not valid_bars:
            continue
        bar = valid_bars[-1]

        record = dict(bar)
        record_warnings = validate_price_record(record)
        record["_validation_warnings"] = record_warnings

        _append_daily_record(symbol, record, base_dir=base)
        _write_violations_section(
            symbol, "daily_price_validation",
            {"checked_at": _now_iso(), "date": record["date"], "warnings": record_warnings},
            base_dir=base,
        )
        if record_warnings:
            print(f"   [{symbol}] 保存前検証で{len(record_warnings)}件の警告を検知（保存は継続）")


def backfill_daily_prices(symbols: List[str], period: str = "1y",
                           start: Optional[str] = None,
                           base_dir: Optional[str] = None) -> None:
    """日次価格層の過去分を一括バックフィルする（一過性ツール、
    backfill_tech_pulse.py型。定期cronには組み込まない、手動実行専用）。

    daily/{SYMBOL}.jsonの日次収集は2026-08-10開始のため、200営業日分の
    移動平均（reader.get_ma_deviation(window=200)）が自然蓄積で計算可能に
    なるまで約10ヶ月かかる。yf.download(period=period)で過去分を一括取得し
    即座にこのブートストラップ欠落を解消する。

    start（'YYYY-MM-DD'形式、省略可）を指定した場合はperiodを無視し、
    _download_historical_bars()側でyf.download(symbols, start=start, ...)
    により日付範囲を直接指定する（[[MARKETDATA-LAYER-CONSTRUCTION-1]]
    着手順序4-8前提作業1、2026-08-11追加。hypecore.py切替が要求する
    start="2021-01-01"のような固定開始日には`period`文字列では正確に
    対応できないため）。

    fetch_daily_prices()と同じ保存前検証（validate_price_record()）・
    アトミック書き込みを適用する。同一日付が既存レコードにある場合は
    新しい取得値で上書きする（fetch_daily_prices()の「同一date上書き」
    冪等性設計と一貫。日次cronが既に取得済みの日付を再取得しても、
    同じ取引日の確定値であれば内容は変わらない）。過去分の検証結果は
    {SYMBOL}/market_data_violations_log.jsonの`backfill_price_validation`
    セクションに要約を記録する（`daily_price_validation`＝直近の日次チェック
    セクションとは独立させ、日次バッチの「最新1件」という意味を壊さない）。
    """
    base = _resolve_base_dir(base_dir)
    target_symbols = _dedupe_symbols(symbols)
    if not target_symbols:
        return

    history = _download_historical_bars(target_symbols, period=period, start=start)
    for symbol in target_symbols:
        bars = history.get(symbol)
        if not bars:
            print(f"   [{symbol}] バックフィルデータ取得失敗（スキップ）")
            continue

        new_records: List[Dict[str, Any]] = []
        dates_with_warnings: List[str] = []
        skipped_no_close = 0
        for bar in bars:
            if not _has_valid_close(bar):
                # 終値の無い足は確定値として保存しない（MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1）
                skipped_no_close += 1
                continue
            record = dict(bar)
            record_warnings = validate_price_record(record)
            record["_validation_warnings"] = record_warnings
            if record_warnings:
                dates_with_warnings.append(record["date"])
            new_records.append(record)

        path = os.path.join(base, "daily", f"{symbol}.json")
        payload = _load_json(path, default={"symbol": symbol, "records": []})
        merged = _merge_daily_records(payload.get("records", []), new_records)
        payload["symbol"] = symbol
        payload["records"] = merged
        _atomic_write_json(path, payload)

        _write_violations_section(
            symbol, "backfill_price_validation",
            {
                "checked_at": _now_iso(), "period": start or period,
                "dates_fetched": len(new_records),
                "dates_with_warnings": dates_with_warnings,
                "skipped_no_close": skipped_no_close,
            },
            base_dir=base,
        )
        print(f"   [{symbol}] バックフィル完了: {len(new_records)}日分取得 → "
              f"累計{len(merged)}日分保存"
              + (f"（検証警告{len(dates_with_warnings)}日分）" if dates_with_warnings else ""))


def repair_close_missing_records(symbols: List[str], base_dir: Optional[str] = None) -> Dict[str, int]:
    """daily/の終値の無い行だけを取り直す（[[MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1]]の
    既存行のバックフィル、手動実行専用。CLIの--repair-missing-closeフラグ経由）。

    backfill_daily_prices()は取得期間の全日付を上書きするため、既に終値のある行の
    出来高等も取得時点の値に置き換わる。本関数は終値の無い行の日付だけを対象に、
    その最古日からyf.download(start=...)で取り直し、同じ日付の終値つきの足で
    置き換える（終値のある行は変更しない）。取り直せなかった行はそのまま残す。
    戻り値は{symbol: 置き換えた件数}。
    """
    base = _resolve_base_dir(base_dir)
    targets: Dict[str, str] = {}
    for symbol in _dedupe_symbols(symbols):
        path = os.path.join(base, "daily", f"{symbol}.json")
        payload = _load_json(path, default=None)
        if not payload:
            continue
        missing = [r["date"] for r in payload.get("records", []) if r.get("date") and not _has_valid_close(r)]
        if missing:
            targets[symbol] = min(missing)
    result: Dict[str, int] = {}
    # 開始日ごとにまとめて一括取得する（銘柄ごとに取得するより呼び出し回数が少ない）
    by_start: Dict[str, List[str]] = {}
    for symbol, start in targets.items():
        by_start.setdefault(start, []).append(symbol)
    for start, group in sorted(by_start.items()):
        history = _download_historical_bars(group, start=start)
        for symbol in group:
            valid = [b for b in (history.get(symbol) or []) if _has_valid_close(b)]
            result[symbol] = _repair_close_missing_records(symbol, valid, base_dir=base)
    return result


# 取引所カレンダーがNYSEと異なる銘柄（休場日の行が無いのは正常）
_NON_NYSE_CALENDARS = {"^N225": "JPX"}


def find_missing_trading_days(symbol: str, base_dir: Optional[str] = None) -> List[str]:
    """daily/{symbol}.jsonの最古〜最新の範囲で、取引日なのに行ごと無い日付を返す
    （[[MARKETPULSE-TECHPULSE-QQQ-NULL-1]]、2026-09-26。QQQの2026-08-25のように行が
    抜けると、reader.get_ma_deviation()が窓内の欠損でNoneを返し続ける）。
    取引日はNYSEカレンダー（^N225はJPX）。"""
    base = _resolve_base_dir(base_dir)
    payload = _load_json(os.path.join(base, "daily", f"{symbol}.json"), default=None)
    records = (payload or {}).get("records", [])
    have = {r["date"] for r in records if r.get("date")}
    if not have:
        return []
    lo, hi = min(have), max(have)
    days = _calendar_trading_days(_NON_NYSE_CALENDARS.get(symbol, "NYSE"))
    return [d for d in days if lo <= d <= hi and d not in have]


_TRADING_DAYS_CACHE: Dict[str, List[str]] = {}


def _calendar_trading_days(name: str) -> List[str]:
    """取引所カレンダーの2000-01-01〜本日+10日の取引日（'YYYY-MM-DD'、昇順）。プロセス内でキャッシュ
    （daily/全銘柄を走査するCHECK-57で銘柄ごとにカレンダーを計算すると遅いため）。"""
    if name not in _TRADING_DAYS_CACHE:
        end = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
        _TRADING_DAYS_CACHE[name] = [d.strftime("%Y-%m-%d")
                                     for d in mcal.get_calendar(name).valid_days(start_date="2000-01-01", end_date=end)]
    return _TRADING_DAYS_CACHE[name]


def repair_missing_trading_days(symbols: List[str], base_dir: Optional[str] = None) -> Dict[str, Dict[str, List[str]]]:
    """行ごと抜けている取引日を実データで取り直す（推測で埋めない。手動実行専用、
    CLIの--repair-missing-daysフラグ経由）。

    抜けている日付の最古日からyf.download(start=...)で取得し、抜けている日付の
    足のうち終値のあるものだけを追加する（既存の行は変更しない）。
    戻り値は{symbol: {"filled": [...], "unresolved": [...]}}。
    """
    base = _resolve_base_dir(base_dir)
    result: Dict[str, Dict[str, List[str]]] = {}
    for symbol in _dedupe_symbols(symbols):
        missing = find_missing_trading_days(symbol, base_dir=base)
        if not missing:
            continue
        history = _download_historical_bars([symbol], start=min(missing))
        by_date = {b["date"]: b for b in (history.get(symbol) or []) if _has_valid_close(b)}
        filled = [d for d in missing if d in by_date]
        if filled:
            path = os.path.join(base, "daily", f"{symbol}.json")
            payload = _load_json(path, default={"symbol": symbol, "records": []})
            new_records = []
            for d in filled:
                rec = dict(by_date[d])
                rec["_validation_warnings"] = validate_price_record(rec)
                new_records.append(rec)
            payload["records"] = sorted(payload.get("records", []) + new_records, key=lambda r: r.get("date", ""))
            _atomic_write_json(path, payload)
        result[symbol] = {"filled": filled, "unresolved": [d for d in missing if d not in by_date]}
        print(f"   [{symbol}] 抜けていた取引日{len(missing)}日: 取得{len(filled)}日・取得できず{len(missing) - len(filled)}日")
    return result


# ── 週次準静的属性層 ──────────────────────────────────────

def fetch_weekly_attributes(symbols: List[str], base_dir: Optional[str] = None) -> None:
    """週次準静的属性層を取得・保存する。

    銘柄ごとに.infoを1回呼び出し、PER（trailing/forward）/PEG/PSR/EV_EBITDA/
    β/セクター/業種/配当/株式数（sharesOutstanding/impliedSharesOutstanding）/
    Forward EPS/配当性向/アナリスト目標株価コンセンサス（mean/median/low/high/
    件数/推奨度）/totalDebtを抽出し、fetched_at（ISO8601 UTC）を付与して
    attributes/{SYMBOL}.jsonへ保存する。保存前にvalidate_attributes_record()で
    時価総額恒等式を検証するが、失敗しても保存は拒否しない。
    twoHundredDayAverageは保存しない（BACKLOG確定事項7、get_ma_deviation()が
    price_seriesから都度計算する設計のため）。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-2（data_fetcher.py切替）の
    事前調査で判明した必須フィールドを2026-08-11に追加（実装依頼「attributes/
    スキーマ拡張」）。追加時の判断根拠:
    - `previousClose`は追加しない: 株価フォールバック第3段はdaily/層
      （reader.get_latest_price()のclose）の責務であり、attributes/に
      重複保存すると「どちらが正か」の層またぎ問題を生む（BACKLOG確定事項4
      「層またぎ再計算の禁止」、twoHundredDayAverage非保存と同じ理由）。
    - `dividend_yield`のソースを`dividendYield`から`trailingAnnualDividendYield`
      に変更した: 実測（AAPL/KO/MSFT）でdividendYieldは百分率表記
      （例: AAPL=0.34≒0.34%）、trailingAnnualDividendYieldは小数表記
      （例: AAPL=0.00335≒0.335%）と**スケールが100倍異なる**ことを発見。
      data_fetcher.py（ディビデンドトラップ判定でtrailingAnnualDividendYield
      使用）との単位統一、および小数表記の方が他の比率フィールド
      （beta/per等）との一貫性が高いためtrailingAnnualDividendYieldに統一。
    - `peg_ratio`（trailingPegRatio優先・pegRatio フォールバック）は変更なし:
      実測で両者はほぼ同値（精度差のみ、単位不一致なし）であり、既存の
      フォールバック設計が既にdata_fetcher.py単体（pegRatioのみ参照）より
      堅牢なため、data_fetcher.py側が本フィールドへ合わせる想定。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-3（valuation_fetcher.py
    切替）の事前調査で判明した不足フィールドを2026-08-11に追加:
    - `enterprise_value`（`enterpriseValue`）: STONKS SILOのEV/Sales算出に
      必須。既存フィールドからの合成（market_cap+total_debt-cash等）は
      Yahoo側の実際のenterpriseValue計算式（優先株・少数株主持分等を含む
      可能性）と乖離するリスクがあるため、独自再計算はせず生の値をそのまま
      保存する。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-4（pipeline.py .calendar
    切替）で2026-08-11に`calendar`フィールドを追加:
    - `.info`とは別のyfinance API（`Ticker.calendar`、次回決算日等）を
      追加で1回呼び出し、`{"earnings_date": [ISO8601日付文字列, ...]}`の
      形で保存する。`.info`取得成功後の独立した呼び出しとして扱い、
      calendar取得の失敗は`.info`由来の他フィールドの保存を妨げない
      （空dictのまま保存継続）。pipeline.py側が使うのは次回決算日の
      配列のみのため、Dividend Date等の他フィールドは保存しない
      （実際に使用するフィールドのみを保存する既存方針を踏襲）。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8前提作業2（hypecore.py
    切替の前提、attributes/スキーマ拡張）で2026-08-11に7フィールドを追加:
    - `revenue_growth`（`revenueGrowth`）・`earnings_growth`
      （`earningsGrowth`）・`gross_margins`（`grossMargins`）:
      hypecore.pyの.info参照フィールドをそのまま追加。
    - `recommendation_mean`（`recommendationMean`）: 数値のアナリスト
      推奨度（1=Strong Buy〜5=Sell）。既存の`analyst_recommendation_key`
      （`recommendationKey`由来の文字列、例:"buy"）とは別物のフィールド
      であり、混同を避けるため両方を保持する（片方をもう片方から導出
      しない、Yahoo側の生値をそのまま両方保存する既存方針を踏襲）。
    - `short_pct_float`（`shortPercentOfFloat`）・`short_ratio`
      （`shortRatio`）: hypecore.pyの.info参照フィールドをそのまま追加。
    - `average_volume`（`averageVolume`優先・`averageVolume10days`
      フォールバック）: hypecore.py::fetch_info_snapshot()の
      `avg_vol = info.get("averageVolume") or info.get("averageVolume10days")
      or 1`と同じフォールバック順序をそのまま踏襲する（既存の`peg_ratio`
      フィールドが`trailingPegRatio`優先・`pegRatio`フォールバックという
      同型のフォールバック設計を採用済み）。「現在の出来高」
      （hypecore.pyの`cur_vol = info.get("volume") or avg_vol`）は
      attributes/には保存しない: 出来高はdaily/層のOHLCVレコード
      （`reader.get_latest_price()["volume"]`）が正であり、attributes/に
      重複保存すると層またぎ問題を生む（BACKLOG確定事項4、
      previousClose非保存と同じ理由）。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序5（診断ツール2ファイル
    切替、audit.py::audit_ticker()のカナダ企業判定の前提）で2026-08-11に
    1フィールドを追加:
    - `country`（`info.get("country")`）: 生値をそのまま保存する単純な
      直接フィールド（フォールバックなし）。audit.py側が
      `country == "Canada"`判定にのみ使用する。

    .info呼び出しはcommon.yfinance_utils.safe_yf_ticker()（リトライ2回・
    待機3秒、beta_fetcher.py::fetch_yfinance_beta()と同型）経由で行う
    （import失敗時は無リトライの直接呼び出しにフォールバック）。全銘柄
    バッチ実行時の一時的なネットワーク不調による取りこぼしを減らすため。
    """
    base = _resolve_base_dir(base_dir)
    target_symbols = _dedupe_symbols(symbols)

    for symbol in target_symbols:
        if _USE_SAFE_YF:
            t = _safe_yf_ticker(symbol)
            if t is None:
                print(f"   [{symbol}] .info取得失敗（リトライ後もNone、スキップ）")
                continue
            try:
                info = t.info
            except Exception as e:
                print(f"   [{symbol}] .info取得失敗（スキップ）: {e}")
                continue
        else:
            t = yf.Ticker(symbol)
            try:
                info = t.info
            except Exception as e:
                print(f"   [{symbol}] .info取得失敗（スキップ）: {e}")
                continue
        if not info:
            print(f"   [{symbol}] .info が空（スキップ）")
            continue

        # [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-4（pipeline.py
        # .calendar切替）の実装依頼で追加（2026-08-11）。次回決算日
        # （Earnings Date）のみを対象とし、.info単発呼び出しの失敗とは
        # 独立した失敗として扱う（calendar取得失敗が.info取得済みの
        # 他フィールド保存を妨げない）。calendar_dictの構造は{"earnings_
        # date": [ISO8601日付文字列, ...]}。取得失敗・空の場合は空dictの
        # まま保存し、reader.get_calendar()が空dictを返す（中立デフォルト）。
        calendar_dict: Dict[str, Any] = {}
        try:
            cal = t.calendar
            earnings_dates = cal.get("Earnings Date") if isinstance(cal, dict) else None
            if earnings_dates:
                calendar_dict["earnings_date"] = [
                    (d.isoformat() if hasattr(d, "isoformat") else str(d)[:10])
                    for d in earnings_dates
                ]
        except Exception as e:
            print(f"   [{symbol}] calendar取得失敗（次回決算日なしで継続）: {e}")

        # [[EPS-1]]（Next_Quarter_EPS N/A問題）で追加（2026-09-16）。
        # Ticker.quarterly_earnings（Ticker.earningsと同型）はyfinance側で
        # 常にNoneを返す廃止済みAPIと事前確認済み（着手順序4-8前提作業2、
        # DeprecationWarning: 'Ticker.earnings' is deprecated as not
        # available via API）。代わりにTicker.earnings_dates（カレンダー
        # ベースのDataFrame、未発表＝Reported EPSがNaNの行にもEPS Estimate
        # 列がアナリストコンセンサスとして入る）を使用する。calendarと同じく
        # .info取得の失敗とは独立した失敗として扱う（99銘柄実測で97/99が
        # 取得成功、欠損2件はいずれもEPS Estimate自体がアナリスト網羅性
        # 不足でNaN）。
        #
        # 「Reported EPSがNaN」だけでは不十分（ZETA実データで発見）:
        # 2022-05-10行はSurprise(%)=-111.11という値が入っているにも
        # 関わらずReported EPSはNaNという、Yahoo側の過去データ欠損行が
        # 存在する。単純に最初のNaN行を採用すると、この4年以上前の
        # 過去欠損行を「次回決算」と誤認する。日付が現在時刻より未来
        # であることも条件に加えて除外する。
        next_quarter_eps_estimate: Optional[float] = None
        next_quarter_eps_date: Optional[str] = None
        try:
            edates = t.earnings_dates
            if edates is not None and not edates.empty and "Reported EPS" in edates.columns:
                now = pd.Timestamp.now(tz=edates.index.tz) if edates.index.tz is not None else pd.Timestamp.now()
                future = edates[edates["Reported EPS"].isna() & (edates.index > now)].sort_index()
                if not future.empty:
                    next_idx = future.index[0]
                    est = future.iloc[0].get("EPS Estimate")
                    if est is not None and not pd.isna(est):
                        next_quarter_eps_estimate = float(est)
                        next_quarter_eps_date = (
                            next_idx.isoformat()[:10] if hasattr(next_idx, "isoformat") else str(next_idx)[:10]
                        )
        except Exception as e:
            print(f"   [{symbol}] earnings_dates取得失敗（次期EPS推定なしで継続）: {e}")

        fetched_at = _now_iso()
        record: Dict[str, Any] = {
            "symbol": symbol,
            "fetched_at": fetched_at,
            "current_price": _numeric_or_none(info.get("currentPrice") or info.get("regularMarketPrice")),
            "market_cap": _numeric_or_none(info.get("marketCap")),
            "enterprise_value": _numeric_or_none(info.get("enterpriseValue")),
            "trailing_pe": _numeric_or_none(info.get("trailingPE")),
            "forward_pe": _numeric_or_none(info.get("forwardPE")),
            "peg_ratio": _numeric_or_none(info.get("trailingPegRatio") or info.get("pegRatio")),
            "price_to_sales": _numeric_or_none(info.get("priceToSalesTrailing12Months")),
            "ev_to_ebitda": _numeric_or_none(info.get("enterpriseToEbitda")),
            "beta": _numeric_or_none(info.get("beta")),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "country": info.get("country"),
            "dividend_yield": _numeric_or_none(info.get("trailingAnnualDividendYield")),
            "payout_ratio": _numeric_or_none(info.get("payoutRatio")),
            "shares_outstanding": _numeric_or_none(info.get("sharesOutstanding")),
            "implied_shares_outstanding": _numeric_or_none(info.get("impliedSharesOutstanding")),
            "forward_eps": _numeric_or_none(info.get("forwardEps")),
            "target_mean_price": _numeric_or_none(info.get("targetMeanPrice")),
            "target_median_price": _numeric_or_none(info.get("targetMedianPrice")),
            "target_low_price": _numeric_or_none(info.get("targetLowPrice")),
            "target_high_price": _numeric_or_none(info.get("targetHighPrice")),
            "analyst_count": _numeric_or_none(info.get("numberOfAnalystOpinions")),
            "analyst_recommendation_key": info.get("recommendationKey"),
            "recommendation_mean": _numeric_or_none(info.get("recommendationMean")),
            "total_debt": _numeric_or_none(info.get("totalDebt")),
            "revenue_growth": _numeric_or_none(info.get("revenueGrowth")),
            "earnings_growth": _numeric_or_none(info.get("earningsGrowth")),
            "gross_margins": _numeric_or_none(info.get("grossMargins")),
            "short_pct_float": _numeric_or_none(info.get("shortPercentOfFloat")),
            "short_ratio": _numeric_or_none(info.get("shortRatio")),
            "average_volume": _numeric_or_none(info.get("averageVolume") or info.get("averageVolume10days")),
            "calendar": calendar_dict,
            "next_quarter_eps_estimate": next_quarter_eps_estimate,
            "next_quarter_eps_date": next_quarter_eps_date,
        }

        record_warnings = validate_attributes_record(record)
        record["_validation_warnings"] = record_warnings

        path = os.path.join(base, "attributes", f"{symbol}.json")
        _atomic_write_json(path, record)
        _write_violations_section(
            symbol, "attributes_validation",
            {"checked_at": fetched_at, "warnings": record_warnings},
            base_dir=base,
        )
        if record_warnings:
            print(f"   [{symbol}] 保存前検証で{len(record_warnings)}件の警告を検知（保存は継続）")


# ── イベント履歴層 ────────────────────────────────────────

def _event_dedup_key(event: Dict[str, Any]):
    return (
        event.get("date"), event.get("firm"),
        event.get("to_grade"), event.get("from_grade"), event.get("action"),
    )


def fetch_analyst_events(symbols: List[str], base_dir: Optional[str] = None) -> None:
    """イベント履歴層を取得・保存する。

    銘柄ごとにupgrades_downgrades・earnings_history・recommendationsの
    3系統を取得し、analyst_history/{SYMBOL}.jsonへ追記型で保存する。
    (date, firm, to_grade, from_grade, action)をキーに重複イベントを
    排除する（同一イベントの再取得で無限増殖しない）。

    3系統は互いに独立した失敗として扱う（1系統の取得失敗が他系統の
    保存を妨げない、fetch_weekly_attributes()のcalendar追加と同型の
    設計。旧実装ではupgrades_downgrades取得失敗時に銘柄全体をスキップ
    していたが、3系統に拡張したことで挙動を改善した）。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8前提作業3
    （hypecore.py切替の前提、analyst_history/スキーマ拡張）で
    2026-08-11に2系統を追加:
    - `earnings_history`: `Ticker.earnings_history`（直近3〜4四半期の
      ローリングウィンドウ、確定済みの不変の事実）を`quarter`
      （四半期末日）をキーに追記型で保存する。`surprise_percent`は
      Yahoo生値（小数）をそのまま保存し%変換はしない（既存の
      `dividend_yield`等と同じ「生値保存・変換は消費側」方針）。
      `Ticker.quarterly_earnings`（hypecore.pyのフォールバック①）は
      事前調査で現行yfinanceにおいて常にNoneを返す（DeprecationWarning:
      'Ticker.earnings' is deprecated as not available via API）ことを
      実測確認したため実装対象外とした。フォールバック②
      （`info['earningsGrowth']`）は着手順序4-8前提作業2で既に
      `attributes/{SYMBOL}.json`の`earnings_growth`として保存済みの
      ため重複実装しない（hypecore.py切替の実装時に消費側で参照する）。
    - `recommendations_history`: `Ticker.recommendations`
      （strongBuy/buy/hold/sell/strongSellの生カウント、"0m"=現時点の
      行のみ使用）を取得日（`date`、本関数の実行日。Weekly Updateで
      週次実行される）をキーに追記型で保存する。事前調査で本データは
      upgrades_downgrades・earnings_historyと異なり「不変の確定済み
      イベント」ではなく「取得時点のコンセンサススナップショット」
      という性質を持つ（同じ"-1m"ラベルでも取得時期により指す暦月・
      値が変わりうる）と判明したため、hypecore.py本体のコード内コメント
      （「現時点値を全期間に設定（将来は月次記録で上書き）」）が示す
      意図に沿い、週次スナップショットを日付キーで蓄積する設計とした
      （各エントリは「取得した日にはこう見えた」という事実として扱う、
      遡及改定があっても過去エントリは書き換えない）。
    """
    base = _resolve_base_dir(base_dir)
    target_symbols = _dedupe_symbols(symbols)
    fetched_date = _now_iso()[:10]

    for symbol in target_symbols:
        t = yf.Ticker(symbol)

        new_events: List[Dict[str, Any]] = []
        try:
            df = t.upgrades_downgrades
            if df is not None and not df.empty:
                for grade_date, row in df.iterrows():
                    date_str = grade_date.strftime("%Y-%m-%d") if hasattr(grade_date, "strftime") else str(grade_date)
                    new_events.append({
                        "date": date_str,
                        "firm": _to_str_or_none(row.get("Firm")),
                        "to_grade": _to_str_or_none(row.get("ToGrade")),
                        "from_grade": _to_str_or_none(row.get("FromGrade")),
                        "action": _to_str_or_none(row.get("Action")),
                        "price_target_action": _to_str_or_none(row.get("priceTargetAction")),
                        "current_price_target": _to_float(row.get("currentPriceTarget")),
                        "prior_price_target": _to_float(row.get("priorPriceTarget")),
                    })
        except Exception as e:
            print(f"   [{symbol}] upgrades_downgrades取得失敗（スキップ）: {e}")

        new_earnings: List[Dict[str, Any]] = []
        try:
            eh = t.earnings_history
            if eh is not None and not eh.empty:
                for quarter_date, row in eh.iterrows():
                    quarter_str = (
                        quarter_date.strftime("%Y-%m-%d") if hasattr(quarter_date, "strftime")
                        else str(quarter_date)
                    )
                    new_earnings.append({
                        "quarter": quarter_str,
                        "eps_actual": _to_float(row.get("epsActual")),
                        "eps_estimate": _to_float(row.get("epsEstimate")),
                        "eps_difference": _to_float(row.get("epsDifference")),
                        "surprise_percent": _to_float(row.get("surprisePercent")),
                    })
        except Exception as e:
            print(f"   [{symbol}] earnings_history取得失敗（スキップ）: {e}")

        new_recommendation: Optional[Dict[str, Any]] = None
        try:
            rec = t.recommendations
            if rec is not None and not rec.empty:
                cur_rows = rec[rec["period"] == "0m"]
                cur = cur_rows.iloc[0] if len(cur_rows) > 0 else rec.iloc[0]
                strong_buy = _to_int(cur.get("strongBuy")) or 0
                buy = _to_int(cur.get("buy")) or 0
                hold = _to_int(cur.get("hold")) or 0
                sell = _to_int(cur.get("sell")) or 0
                strong_sell = _to_int(cur.get("strongSell")) or 0
                total = strong_buy + buy + hold + sell + strong_sell
                new_recommendation = {
                    "date": fetched_date,
                    "strong_buy": strong_buy, "buy": buy, "hold": hold,
                    "sell": sell, "strong_sell": strong_sell,
                    "buy_hold_ratio": round((strong_buy + buy) / total, 4) if total > 0 else None,
                }
        except Exception as e:
            print(f"   [{symbol}] recommendations取得失敗（スキップ）: {e}")

        path = os.path.join(base, "analyst_history", f"{symbol}.json")
        payload = _load_json(path, default={
            "symbol": symbol, "events": [], "earnings_history": [], "recommendations_history": [],
        })

        merged_events = {_event_dedup_key(e): e for e in payload.get("events", [])}
        for e in new_events:
            merged_events[_event_dedup_key(e)] = e
        events = sorted(merged_events.values(), key=lambda e: e.get("date", ""), reverse=True)

        merged_earnings = {e.get("quarter"): e for e in payload.get("earnings_history", [])}
        for e in new_earnings:
            merged_earnings[e.get("quarter")] = e
        earnings_history = sorted(merged_earnings.values(), key=lambda e: e.get("quarter", ""), reverse=True)

        merged_recs = {r.get("date"): r for r in payload.get("recommendations_history", [])}
        if new_recommendation is not None:
            merged_recs[new_recommendation["date"]] = new_recommendation
        recommendations_history = sorted(merged_recs.values(), key=lambda r: r.get("date", ""), reverse=True)

        payload["symbol"] = symbol
        payload["events"] = events
        payload["earnings_history"] = earnings_history
        payload["recommendations_history"] = recommendations_history
        _atomic_write_json(path, payload)


# ── 銘柄ユニバース構築（S&P500 + 監視銘柄 + 指数/ETF/商品） ──

def get_monitor_tickers(config_path: Optional[str] = None) -> List[str]:
    """config/monitor_tickers.yaml の監視銘柄リストを返す。"""
    path = config_path or _MONITOR_TICKERS_YAML
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return _dedupe_symbols(data.get("tickers", []))
    except Exception as e:
        print(f"   [WARN] monitor_tickers.yaml 読み込み失敗: {e}")
        return []


def get_sp500_constituents(base_dir: Optional[str] = None, max_age_days: int = 7) -> List[str]:
    """S&P500構成銘柄リストを取得する（Wikipedia→GitHub CSVフォールバック、
    {max_age_days}日キャッシュ）。

    src/market/market_pulse/breadth_calculator.py::get_sp500_tickers() と
    同じ取得ロジックだが、market_dataは一次データ層として他レイヤーの
    consumerスクリプトに依存すべきではないため独立実装として保持する
    （意図的な重複であり、DRY違反の見落としではない）。
    """
    base = _resolve_base_dir(base_dir)
    cache_path = os.path.join(base, _SP500_CACHE_FILENAME)

    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                cache = json.load(f)
            fetched_at = datetime.fromisoformat(cache["fetched_at"])
            if fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=timezone.utc)
            age_days = (datetime.now(timezone.utc) - fetched_at).days
            if age_days < max_age_days and cache.get("tickers"):
                return cache["tickers"]
        except Exception:
            pass

    tickers: Optional[List[str]] = None
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; MarketDataFetcher/1.0)"}
        resp = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=headers, timeout=30,
        )
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text))
        df = tables[0]
        tickers = [t.strip() for t in df["Symbol"].str.replace(".", "-", regex=False).tolist() if t.strip()]
    except Exception as e:
        print(f"   [WARN] S&P500構成銘柄のWikipedia取得失敗: {e}")

    if not tickers:
        try:
            csv_url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
            resp = requests.get(csv_url, timeout=30)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            tickers = [t.strip() for t in df["Symbol"].str.replace(".", "-", regex=False).tolist() if t.strip()]
        except Exception as e:
            print(f"   [WARN] S&P500構成銘柄のGitHub CSV取得失敗: {e}")

    if tickers and len(tickers) >= 400:
        _atomic_write_json(cache_path, {
            "fetched_at": _now_iso(), "count": len(tickers), "tickers": tickers,
        })
        return tickers

    # 両ソースとも失敗した場合、期限切れキャッシュがあればそれを返す
    # （完全な空リストで日次バッチを回すよりは安全）
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f).get("tickers", [])
        except Exception:
            pass
    return []


def get_default_symbol_universe(base_dir: Optional[str] = None) -> List[str]:
    """S&P500構成銘柄＋監視銘柄＋指数/ETF/商品の和集合を返す
    （BACKLOG [[MARKETDATA-LAYER-CONSTRUCTION-1]]設計。fetch_daily_prices()の
    本番バッチ実行が対象とすべき銘柄ユニバース）。
    """
    universe: List[str] = []
    universe.extend(get_sp500_constituents(base_dir=base_dir))
    universe.extend(get_monitor_tickers())
    universe.extend(INDEX_ETF_COMMODITY_SYMBOLS)
    return _dedupe_symbols(universe)


if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(description="common/market_data 取得CLI")
    arg_parser.add_argument("symbols", nargs="*", help="対象銘柄（省略時はデフォルトユニバース）")
    arg_parser.add_argument("--layer", choices=["daily", "attributes", "analyst", "all"], default="all")
    arg_parser.add_argument(
        "--backfill", action="store_true",
        help="一過性: daily/の過去分を一括取得する（backfill_tech_pulse.py型、定期cronには組み込まない）",
    )
    arg_parser.add_argument(
        "--period", default="1y",
        help="--backfill時の取得期間（yfinance期間指定形式、デフォルト1y。--start指定時は無視される）",
    )
    arg_parser.add_argument(
        "--start", default=None,
        help="--backfill時の取得開始日（'YYYY-MM-DD'形式、指定時は--periodより優先される。"
             "periodは相対期間〈本日から遡ってN年等〉のため固定開始日を厳密に指定できない場合に使う）",
    )
    arg_parser.add_argument(
        "--repair-missing-close", action="store_true",
        help="一過性: daily/の終値の無い行だけを取り直す（銘柄省略時はdaily/の全ファイルが対象）",
    )
    arg_parser.add_argument(
        "--repair-missing-days", action="store_true",
        help="一過性: daily/で行ごと抜けている取引日を実データで取り直す（銘柄省略時はdaily/の全ファイルが対象）",
    )
    args = arg_parser.parse_args()

    if args.repair_missing_days:
        base_for_repair = _resolve_base_dir(None)
        repair_symbols = (_dedupe_symbols(args.symbols) if args.symbols else sorted(
            os.path.splitext(n)[0] for n in os.listdir(os.path.join(base_for_repair, "daily")) if n.endswith(".json")))
        res = repair_missing_trading_days(repair_symbols)
        print(json.dumps(res, ensure_ascii=False))
        sys.exit(0)

    if args.repair_missing_close:
        base_for_repair = _resolve_base_dir(None)
        repair_symbols = (_dedupe_symbols(args.symbols) if args.symbols else sorted(
            os.path.splitext(n)[0] for n in os.listdir(os.path.join(base_for_repair, "daily")) if n.endswith(".json")))
        repaired = repair_close_missing_records(repair_symbols)
        print(f"終値の無い行の取り直し: {sum(repaired.values())}行（{sum(1 for v in repaired.values() if v)}銘柄）")
        sys.exit(0)

    symbols_arg = _dedupe_symbols(args.symbols) if args.symbols else get_default_symbol_universe()
    print(f"対象銘柄数: {len(symbols_arg)}")

    if args.backfill:
        backfill_daily_prices(symbols_arg, period=args.period, start=args.start)
    else:
        if args.layer in ("daily", "all"):
            fetch_daily_prices(symbols_arg)
        if args.layer in ("attributes", "all"):
            fetch_weekly_attributes(symbols_arg)
        if args.layer in ("analyst", "all"):
            fetch_analyst_events(symbols_arg)
