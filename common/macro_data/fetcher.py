"""
common/macro_data/fetcher.py

FRED統合層のネットワーク取得モジュール（新DB構築プロジェクト フェーズ1、
BACKLOG [[MACRODATA-LAYER-CONSTRUCTION-1]]）。common/market_data/と同型
構成（fetcher.py がネットワーク取得・保存前検証・保存を担当、reader.py
が読み取りAPIを提供）。

【スコープ】
2026-08-12: fetcher.py/reader.py本体を新規構築。続けて同日中に
`.github/workflows/Macro_Data_Update.yml`（定期実行cron・
workflow_dispatch）を新設し、本ファイル末尾の`__main__`ブロック
（`common/market_data/fetcher.py`と同じCLIパターン）から
`fetch_all_series()`を呼び出す構成にした。

以下は引き続き次段階に分離し、今回は一切変更していない:
- `05_main.py`（MACRO PULSE）・`collect_and_send.py`（Market Pulse）側の
  本番消費者切替（重複3系列 BAMLH0A0HYM2/T10Y2Y/VIXCLS の解消を含む）
- 過去データの一括投入（フェーズ2、過去データ移管）

現時点でこのモジュールを参照する本番消費者は存在しない
（グリーンフィールド実装、定期実行のみ稼働）。

保存構造:
    common/macro_data/
        series/{SERIES_ID}.json          系列単位の時系列ストア
        series_meta.json                  系列単位のメタ情報（fred_release_id等、静的ファイル）
        macro_data_violations_log.json    保存前検証の結果ログ（0件でも毎回書き込む）

FRED_API_KEY環境変数名・fredapiクライアントの初期化方法（`Fred(api_key=...)`）は
`05_main.py::get_fred()`・`collect_and_send.py`側の既存3関数と同一のものを
実コード確認の上で踏襲している（新規の変数名は導入していない）。
"""

import json
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

MACRO_DATA_DIR = os.path.dirname(os.path.abspath(__file__))

JST = timezone(timedelta(hours=9))

# 05_main.py::get_fred() / collect_and_send.py側3関数と同一の環境変数名
# （実コード確認済み、新規の変数名は導入しない）
FRED_API_KEY_ENV = "FRED_API_KEY"

_FRED_CLIENT = None


# ── FREDクライアント（モジュールレベルで1つだけ生成し使い回す） ──────

def _get_fred_client():
    """fredapiクライアントをモジュールレベルで1つだけ生成し使い回す。
    現状の各ファイル（05_main.py::get_fred()・collect_and_send.py側3関数）が
    呼び出しの都度Fred()を個別生成する設計は踏襲しない。

    APIキー未設定・fredapi未インストールの場合は例外を送出する
    （呼び出し元のfetch_series()がtry/exceptで捕捉しNoneを返す）。
    """
    global _FRED_CLIENT
    if _FRED_CLIENT is None:
        from fredapi import Fred
        api_key = os.environ.get(FRED_API_KEY_ENV, "")
        if not api_key:
            raise RuntimeError(f"{FRED_API_KEY_ENV} not set.")
        _FRED_CLIENT = Fred(api_key=api_key)
    return _FRED_CLIENT


def _now_jst_iso() -> str:
    return datetime.now(JST).isoformat()


# ── 共通ユーティリティ（common/market_data/fetcher.pyと同型） ────────

def _atomic_write_json(path: str, payload: Any) -> None:
    """tempfile→os.replace()でアトミック書き込みする
    （common/market_data/fetcher.py::_atomic_write_jsonと同じ実装）。"""
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


def _series_path(series_id: str, base_dir: str) -> str:
    return os.path.join(base_dir, "series", f"{series_id}.json")


def _series_meta_path(base_dir: str) -> str:
    return os.path.join(base_dir, "series_meta.json")


def _violations_log_path(base_dir: str) -> str:
    return os.path.join(base_dir, "macro_data_violations_log.json")


# ── 外部取得（唯一の窓口） ──────────────────────────────────

def _fetch_series_raw(series_id: str, start: Optional[str] = None):
    """fetch_series()の実処理本体。[[MACRODATA-FETCH-FAILURE-VISIBILITY-
    GAP-1]]対応（2026-09-19）: update_series()がfetch_status（"ok"/
    "failed"）とfailure_reasonを区別できるよう、(series_or_None, status,
    reason)の3値を返す内部関数として分離した。fetch_series()自体は
    既存の公開API（呼び出し元へ与える型を変えない）として維持する。

    Returns:
        (pandas.Series | None, "ok" | "failed", str | None)
        status=="ok"のときraw(pandas.Series)は必ず非None（空Seriesの
        可能性はある＝FREDが応答したが新規データなし、これはfetch自体の
        失敗ではない）。status=="failed"のときrawは必ずNone。
    """
    try:
        fred = _get_fred_client()
    except Exception as e:
        reason = f"FREDクライアント初期化失敗: {e}"
        print(f"   [{series_id}] {reason}（スキップ）")
        return None, "failed", reason

    kwargs = {}
    if start:
        kwargs["observation_start"] = start

    last_err = None
    for attempt in range(3):
        try:
            return fred.get_series(series_id, **kwargs), "ok", None
        except Exception as e:
            last_err = e
            print(f"   [{series_id}] FRED attempt {attempt + 1}: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)

    reason = f"FRED取得失敗（3回リトライ後も失敗）: {last_err}"
    print(f"   [{series_id}] {reason}（スキップ）")
    return None, "failed", reason


def fetch_series(series_id: str, start: Optional[str] = None):
    """FREDから単一系列を取得する唯一の外部アクセス関数。

    リトライ3回＋指数バックオフ（`05_main.py::fetch_event_row()`の
    リトライ実装パターン: `for attempt in range(3): ... time.sleep(2**attempt)`
    を踏襲）。APIキー未設定等でクライアント自体が初期化できない場合は
    リトライせず即座にNoneを返す（他のFRED消費者と同じ「キー未設定なら
    即スキップ」パターンを踏襲）。

    Args:
        series_id: FRED系列コード（例: "BAMLH0A0HYM2"）
        start: 'YYYY-MM-DD'形式の取得開始日（省略時はFREDのデフォルト全期間）

    Returns:
        pandas.Series（インデックス=観測日のTimestamp、値=観測値）。
        取得失敗時はNone（例外は投げない、既存のエラー耐性方針を踏襲）。
    """
    raw, _status, _reason = _fetch_series_raw(series_id, start=start)
    return raw


# ── 保存前検証 ────────────────────────────────────────────

def _validate_incoming_batch(incoming: List[Dict[str, Any]],
                              prior_last_value: Optional[float]) -> List[str]:
    """今回の取得バッチに対する保存前検証。検証失敗時も保存は拒否しない
    （呼び出し元が警告として記録した上で保存を継続する、
    common/market_data/と同じ設計方針）。

    ①同一as_ofの重複が生じていないか（FRED側の異常データ検知）
    ②直前値（バッチ内の直前観測日、先頭要素についてはprior_last_value=
      既存ストアの直近値）からの変化が1桁（10倍または1/10）以上か
    """
    warnings: List[str] = []

    seen = set()
    for item in incoming:
        as_of = item["as_of"]
        if as_of in seen:
            warnings.append(f"duplicate as_of in fetched batch: {as_of}")
        seen.add(as_of)

    prev_val = prior_last_value
    for item in incoming:
        val = item["value"]
        if prev_val is not None and prev_val != 0 and val != 0:
            ratio = abs(val / prev_val)
            if ratio >= 10 or ratio <= 0.1:
                warnings.append(
                    f"order-of-magnitude jump at {item['as_of']}: "
                    f"prev={prev_val}, current={val} (ratio={ratio:.4f})"
                )
        prev_val = val

    return warnings


def _write_violations_section(series_id: str, warnings: List[str], base_dir: str,
                               fetch_status: str = "ok",
                               failure_reason: Optional[str] = None) -> None:
    """macro_data_violations_log.jsonの指定系列セクションのみを更新する。
    0件でも毎回書き込む（fy_collision_log.json型の化石ファイル対策、
    common/market_data/fetcher.py::_write_violations_sectionと同じ設計。
    ただしmarket_dataは銘柄ごとに別ファイルだが、macro_dataは単一の
    共有ログファイル内を系列IDキーでセクション分割する）。

    [[MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1]]対応（2026-09-19）:
    fetch_status（"ok"/"failed"）を追加。既存フィールド（checked_at/
    warnings）は維持。failure_reasonはfetch_status=="failed"のときのみ
    書き込む（成功時にキー自体を増やさず、既存の正常系エントリの形を
    変えない）。
    """
    path = _violations_log_path(base_dir)
    payload = _load_json(path, default={})
    entry: Dict[str, Any] = {
        "checked_at": _now_jst_iso(),
        "warnings": warnings,
        "fetch_status": fetch_status,
    }
    if fetch_status == "failed" and failure_reason:
        entry["failure_reason"] = failure_reason
    payload[series_id] = entry
    _atomic_write_json(path, payload)


# ── 更新（fetch + upsert + 検証 + 保存） ──────────────────────

def update_series(series_id: str, start: Optional[str] = None,
                   base_dir: Optional[str] = None) -> Dict[str, Any]:
    """fetch_series()の結果を既存series/{SERIES_ID}.jsonとマージ（日付単位で
    upsert、重複日付は新しい取得値で上書き）し、各エントリにvalue/as_of
    （観測日）/fetched_at（取得日時、ISO8601・JST）/source（"FRED"固定）/
    source_detail（"series={series_id}"）を付与して保存する。

    保存前に_validate_incoming_batch()で検証し、結果を
    macro_data_violations_log.jsonへ記録する（違反0件でも毎回書き込む）。

    [[MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1]]対応（2026-09-19）:
    戻り値・violations_logのいずれにもfetch_status（"ok"/"failed"）と
    failure_reason（失敗時のみ）を追加した。"failed"はFRED呼び出し自体が
    失敗した場合（APIキー未設定・3回リトライ後も失敗等）のみを指し、
    「FREDは正常応答したが新規データが0件／全件NaN」は技術的には成功の
    ためfetch_status="ok"のまま扱う（両者を区別しないことが本バグの
    原因だったため、この区別自体が対応の核心）。

    Returns:
        {"series_id": str, "updated": int, "warnings": List[str],
         "fetch_status": "ok" | "failed", "failure_reason": str | None}
    """
    base = base_dir if base_dir is not None else MACRO_DATA_DIR

    raw, fetch_status, failure_reason = _fetch_series_raw(series_id, start=start)
    if fetch_status == "failed":
        print(f"   [{series_id}] 更新対象データなし（取得失敗、スキップ）")
        _write_violations_section(series_id, [], base, fetch_status="failed",
                                   failure_reason=failure_reason)
        return {"series_id": series_id, "updated": 0, "warnings": [],
                "fetch_status": "failed", "failure_reason": failure_reason}

    if len(raw) == 0:
        print(f"   [{series_id}] 更新対象データなし（スキップ）")
        _write_violations_section(series_id, [], base, fetch_status="ok")
        return {"series_id": series_id, "updated": 0, "warnings": [],
                "fetch_status": "ok", "failure_reason": None}

    raw = raw.dropna()
    if len(raw) == 0:
        print(f"   [{series_id}] 更新対象データなし（全件NaN、スキップ）")
        _write_violations_section(series_id, [], base, fetch_status="ok")
        return {"series_id": series_id, "updated": 0, "warnings": [],
                "fetch_status": "ok", "failure_reason": None}

    incoming: List[Dict[str, Any]] = []
    for obs_date, val in raw.items():
        as_of = obs_date.strftime("%Y-%m-%d") if hasattr(obs_date, "strftime") else str(obs_date)[:10]
        incoming.append({"as_of": as_of, "value": float(val)})
    incoming.sort(key=lambda r: r["as_of"])

    path = _series_path(series_id, base)
    payload = _load_json(path, default={"series_id": series_id, "records": []})
    existing_records = payload.get("records", [])
    existing_map = {r["as_of"]: r for r in existing_records if r.get("as_of")}

    # 直前値: 既存ストア内で今回バッチの最古日より前にある直近レコードの値
    # （バッチ先頭要素の変化率検証に使う。バッチ内2件目以降はバッチ内の
    # 直前要素を使う）
    prior_dates = sorted(d for d in existing_map if d < incoming[0]["as_of"])
    prior_last_value = existing_map[prior_dates[-1]]["value"] if prior_dates else None

    validation_warnings = _validate_incoming_batch(incoming, prior_last_value)

    fetched_at = _now_jst_iso()
    updated_count = 0
    for item in incoming:
        existing_map[item["as_of"]] = {
            "value": item["value"],
            "as_of": item["as_of"],
            "fetched_at": fetched_at,
            "source": "FRED",
            "source_detail": f"series={series_id}",
        }
        updated_count += 1

    records = sorted(existing_map.values(), key=lambda r: r["as_of"])
    payload["series_id"] = series_id
    payload["records"] = records
    _atomic_write_json(path, payload)

    _write_violations_section(series_id, validation_warnings, base, fetch_status="ok")

    print(f"   [{series_id}] 更新完了: {updated_count}件（全{len(records)}件）"
          + (f" [WARN {len(validation_warnings)}件]" if validation_warnings else ""))

    return {"series_id": series_id, "updated": updated_count, "warnings": validation_warnings,
            "fetch_status": "ok", "failure_reason": None}


def fetch_all_series(series_ids: Optional[List[str]] = None,
                      base_dir: Optional[str] = None,
                      start: Optional[str] = None) -> List[Dict[str, Any]]:
    """series_meta.jsonの全系列（またはseries_ids指定分）に対し
    update_series()を順次実行するバッチ関数。`.github/workflows/
    Macro_Data_Update.yml`（定期実行cron・workflow_dispatch）から、
    本ファイル末尾の`__main__`ブロック経由で呼び出される。

    Args:
        start: 'YYYY-MM-DD'形式の取得開始日（省略時はFREDのデフォルト
            全期間、update_series()経由でfetch_series()へそのまま伝播）。
            [[MACRODATA-FULL-HISTORY-DAILY-REFETCH-1]]: 日次cronが
            毎回FRED観測開始日（系列によっては1940〜1970年代）からの
            全期間を取得してしまう問題への対応で追加した。

    Returns:
        各系列のupdate_series()結果のリスト。

    [[MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1]]対応（2026-09-19）:
    ループへ系列単位のtry/exceptを追加した。update_series()内部で
    捕捉されない予期しない例外（ディスクエラー等）が1系列で発生しても、
    その系列をfetch_status="failed"として記録するのみで残りの系列の
    処理を継続する（登録時に確認された「1系列の例外でバッチ全体が
    中断する構造的リスク」への対応）。ジョブ自体（本関数の戻り値・
    `__main__`の終了コード）は従来通り失敗させない（挙動を変えない）。
    """
    base = base_dir if base_dir is not None else MACRO_DATA_DIR
    meta = _load_json(_series_meta_path(base), default={})
    targets = series_ids if series_ids is not None else list(meta.keys())

    results = []
    for series_id in targets:
        try:
            result = update_series(series_id, start=start, base_dir=base)
        except Exception as e:
            reason = f"update_series内で予期しない例外: {type(e).__name__}: {e}"
            print(f"   [{series_id}] {reason}（この系列のみスキップし、残りは続行）")
            try:
                _write_violations_section(series_id, [], base, fetch_status="failed",
                                           failure_reason=reason)
            except Exception as log_e:
                print(f"   [{series_id}] violations_log書き込みにも失敗（続行）: {log_e}")
            result = {"series_id": series_id, "updated": 0, "warnings": [],
                      "fetch_status": "failed", "failure_reason": reason}
        results.append(result)
    return results


if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(description="common/macro_data 取得CLI")
    arg_parser.add_argument(
        "series_ids", nargs="*",
        help="対象FRED系列コード（省略時はseries_meta.json全系列、"
             "common/market_data/fetcher.pyのsymbols引数と同じパターン）",
    )
    arg_parser.add_argument(
        "--start", default=None,
        help="取得開始日（YYYY-MM-DD形式、省略時はFREDのデフォルト全期間）。"
             "[[MACRODATA-FULL-HISTORY-DAILY-REFETCH-1]]: 日次cronから"
             "直近日数分のみに絞って呼び出す用途を想定。",
    )
    args = arg_parser.parse_args()

    target_series = args.series_ids if args.series_ids else None
    if target_series:
        print(f"対象系列数: {len(target_series)}")
    else:
        print("対象系列数: series_meta.json全系列")
    if args.start:
        print(f"取得開始日: {args.start}")

    all_results = fetch_all_series(series_ids=target_series, start=args.start)
    total_warnings = sum(len(r["warnings"]) for r in all_results)
    total_updated = sum(r["updated"] for r in all_results)
    failed_results = [r for r in all_results if r.get("fetch_status") == "failed"]
    ok_count = len(all_results) - len(failed_results)
    print(
        f"\n完了: {len(all_results)}系列処理、"
        f"成功{ok_count}件/失敗{len(failed_results)}件、"
        f"更新レコード合計{total_updated}件、警告合計{total_warnings}件"
    )
    if failed_results:
        print("失敗系列:")
        for r in failed_results:
            print(f"  - {r['series_id']}: {r.get('failure_reason')}")

    # GitHub Actions Step Summaryへ成功N/失敗Mと失敗系列一覧を出力する
    # （[[MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1]]対応、2026-09-19。
    # common/sec_data/update.py::main()の既存GITHUB_STEP_SUMMARY書き込み
    # パターンを踏襲）。
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as sf:
            sf.write("\n## Macro Data Update\n\n")
            sf.write(f"成功: {ok_count}件 / 失敗: {len(failed_results)}件\n\n")
            if failed_results:
                sf.write("| 系列ID | 失敗理由 |\n|---|---|\n")
                for r in failed_results:
                    reason = (r.get("failure_reason") or "")[:200]
                    sf.write(f"| {r['series_id']} | {reason} |\n")
