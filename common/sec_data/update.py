"""
SEC データ一括更新スクリプト
GitHub Actions から実行される
使用方法:
    python common/sec_data/update.py              # 全ティッカー
    python common/sec_data/update.py TSLA PLTR    # 特定ティッカーのみ
    python common/sec_data/update.py --dry-run    # submissionsだけ取得し、company_factsを取り直す銘柄を表示（書き込みなし）

company_factsの取り直しの判定（[[SEC-FETCH-CACHE-MTIME-CI-1]]、2026-10-10）:
submissionsを毎回取り直し、手元のcompany_facts.jsonに無い10-K/10-Q(/A)の提出がある銘柄だけ
company_factsを取り直す。判定にファイルのmtimeは使わない（CIではチェックアウトの時刻になり、
24時間キャッシュが常に有効と判定されてSEC APIを一度も呼んでいなかったため）。
結果はcommon/sec_data/data/_freshness.jsonに記録する（freshness.py）。
"""
import argparse
import json
import sys
import os
import time
import traceback
from typing import Optional

# common/sec_data/ から実行される前提でパス設定
script_dir = os.path.dirname(os.path.abspath(__file__))
common_dir = os.path.dirname(script_dir)
repo_root = os.path.dirname(common_dir)
sys.path.insert(0, repo_root)

from common.sec_data.config import get_all
from common.sec_data.fetcher import SECFetcher, load_company_facts
from common.sec_data import freshness as fr
from common.sec_data.parser import SECParser
from common.sec_data.quarterly import build_raw_table, check_revenue_quality
from common.sec_data.normalizer import normalize, save_normalized
from common.sec_data.layer3_builder import build_ticker_store
from common.sec_data.ttm_calculator import calc_ttm_series, save_ttm_series
from common.sec_data.revenue_tag_conflict_check import check_revenue_tag_conflict, _format_conflict
# 失敗の一覧（JSON）の書き出し先。リポジトリの外（環境変数SEC_UPDATE_RESULT → RUNNER_TEMP → 一時ディレクトリ）。
# 巻き戻しの段（rollback_failed.py）と同じ場所を使う
from common.sec_data.rollback_failed import default_result_path as result_path


def plan_company_facts(fetcher: SECFetcher, ticker: str, data_dir: str, dry_run: bool = False) -> dict:
    """submissionsを取り直し、company_factsをどう扱うかを決める（company_factsのAPIは呼ばない）。

    case:
      a: company_facts.jsonが無い → 取得する
      b: 手元の最大filed（L）以降に、company_factsに無い10-K/10-Q(/A)の提出がある → 取り直す
      c: 新しい提出なし → 手元のcompany_facts.jsonをそのまま使う
      submissions_failed: submissionsを取得できない → 手元のファイルで続行（FETCH_FAILED）
    """
    ticker = ticker.upper()
    local = load_company_facts(ticker, data_dir=data_dir)
    try:
        payload = fetcher.fetch_submissions(ticker, force_refresh=True, save=not dry_run, return_payload=True)
    except Exception as e:
        print(f"   [WARN] submissions取得エラー: {e}")
        payload = None
    filing_dates = (payload or {}).get("accn_to_filingdate") or {}
    latest_filed, accns = fr.facts_index(local)
    if local is None:
        case, pending = "a", []
    elif payload is None:
        case, pending = "submissions_failed", []
    else:
        pending = fr.pending_filings(filing_dates, latest_filed, accns)
        case = "b" if pending else "c"
    return {
        "ticker": ticker, "case": case, "local": local, "payload": payload,
        "filing_dates": filing_dates, "latest_filed": latest_filed,
        "latest_submission": max(filing_dates.values()) if filing_dates else None,
        "pending": pending,
    }


def execute_company_facts(fetcher: SECFetcher, plan: dict):
    """plan_company_facts()の結果に従ってcompany_factsを取得し、(_freshness.jsonの1銘柄分, company_facts)を返す。

    company_factsがNoneなのは、company_facts.jsonが無く取得にも失敗したときだけ（従来どおり失敗扱い）。
    取り直しに失敗しても、手元のファイルがあればそれで続行する（FETCH_FAILED）。
    """
    ticker, case = plan["ticker"], plan["case"]
    latest_sub, local = plan["latest_submission"], plan["local"]

    if case == "c":
        return fr.entry(fr.OK_NO_NEW, case, plan["latest_filed"], latest_sub, []), local
    if case == "submissions_failed":
        return fr.entry(fr.FETCH_FAILED, case, plan["latest_filed"], None, [],
                        "submissionsを取得できない（手元のファイルで続行）"), local

    raw = fetcher.fetch_company_facts(ticker, force_refresh=True)
    if raw is None:
        if case == "a":
            return fr.entry(fr.FETCH_FAILED, case, "", latest_sub, [],
                            "company_facts.jsonが無く、取得にも失敗"), None
        return fr.entry(fr.FETCH_FAILED, case, plan["latest_filed"], latest_sub, plan["pending"],
                        "company_factsの取り直しに失敗（手元のファイルで続行）"), local

    new_latest, new_accns = fr.facts_index(raw)
    if case == "a":
        still_missing = fr.pending_filings(plan["filing_dates"], new_latest, new_accns)
    else:
        # 取り直す前に見つけた未反映の提出そのものを照らす（新しいLで数え直すと、
        # 後の提出だけ入った場合に先の提出の未反映を見落とすため）
        still_missing = [p for p in plan["pending"] if p["accn"] not in new_accns]
    if still_missing:
        return fr.entry(fr.SEC_LAG, case, new_latest, latest_sub, still_missing,
                        "提出はあるがcompanyfacts APIに入っていない（SEC側の未反映）"), raw
    return fr.entry(fr.REFRESHED, case, new_latest, latest_sub, []), raw


def dry_run(tickers: list) -> None:
    data_dir = os.path.join(script_dir, "data")
    fetcher = SECFetcher(data_dir=data_dir)
    started = time.time()
    rows = []
    for ticker in tickers:
        p = plan_company_facts(fetcher, ticker, data_dir, dry_run=True)
        rows.append(p)
    print("\n" + "=" * 60)
    print("dry-run（ファイルは書かない。company_factsのAPIは呼ばない）")
    for case in ("a", "b", "submissions_failed", "c"):
        hit = [p for p in rows if p["case"] == case]
        print(f"\n({case}) {len(hit)}銘柄")
        if case == "c":
            print("   " + " ".join(p["ticker"] for p in hit))
            continue
        for p in hit:
            pend = ", ".join(f"{x['accn']}（{x['filing_date']}）" for x in p["pending"])
            print(f"   {p['ticker']}: 手元の最大filed {p['latest_filed'] or '—'}・submissionsの最新 "
                  f"{p['latest_submission'] or '—'}" + (f"・未反映 {pend}" if pend else ""))
    print(f"\nリクエスト数: {fetcher.request_count}・実行時間: {time.time() - started:.1f}秒")
    print("=" * 60)


# 1銘柄の失敗の扱い（[[SEC-UPDATE-EXIT1-BLOCKS-ALL-1]]、2026-10-15）:
# 取得からTTMまでのどの段で失敗しても（例外・Noneの戻り値）、その銘柄をfailedに段階と理由つきで入れて
# 次の銘柄へ進む。失敗した銘柄の出力は、SEC_Data_Update.ymlの次の段（rollback_failed.py）が前回の状態に戻す。
# 終了コード: 失敗0件・失敗が対象の10%以下は0、10%を超えるか銘柄のループの外で例外が出たら1。
FAILED_EXIT_RATIO = fr.FAILED_EXIT_RATIO
DATA_DIR = os.path.join(script_dir, "data")

# 段階の名前（Step Summary・Discord・_freshness.jsonのfailed_stageに出る）
STAGE_FETCH = "fetch"          # submissions・company_factsの取得
STAGE_PARSE = "parse"          # parser.parse_and_save()（annual・quarterly）
STAGE_RAW = "raw"              # 四半期Raw Table
STAGE_NORMALIZE = "normalize"  # 正規化と保存
STAGE_TTM = "ttm"              # Layer3・TTMの計算と保存


class TickerFailed(Exception):
    def __init__(self, stage: str, reason: str):
        super().__init__(reason)
        self.stage = stage
        self.reason = reason


def exit_code(n_targets: int, n_failed: int, aborted: bool) -> int:
    if aborted:
        return 1
    if n_failed == 0:
        return 0
    return 0 if n_failed <= n_targets * FAILED_EXIT_RATIO else 1


def process_ticker(ticker: str, fetcher: SECFetcher, parser: SECParser, data_dir: str,
                   ctx: dict, revenue_rows: list) -> dict:
    """1銘柄を取得からTTMまで処理し、_freshness.jsonの1銘柄分を返す。失敗はTickerFailedを送出する。
    ctxには途中の状態（段階・plan・取得の結果のentry）を入れる（失敗の記録に使う）。"""
    # 1. submissionsを取り直し、company_factsを取得・取り直すかを決める
    #    （submissionsは本人データ判定用のreportDateにも使う。取得に失敗しても
    #    手元のsubmissions.json・determine_fiscal_year()フォールバックで継続できる）
    plan = plan_company_facts(fetcher, ticker, data_dir)
    ctx["plan"] = plan
    entry, raw = execute_company_facts(fetcher, plan)
    ctx["entry"] = entry
    print(f"   鮮度: {entry['status']}（{entry['case']}）" + (f" {entry['note']}" if entry["note"] else ""))
    if not raw:
        raise TickerFailed(STAGE_FETCH, entry["note"] or "company_factsを取得できない")

    # 2. 従来パース＆保存（annual等）
    ctx["stage"] = STAGE_PARSE
    parsed = parser.parse_and_save(ticker)
    if not parsed:
        raise TickerFailed(STAGE_PARSE, "parse_and_save()が空の結果を返した（company_facts.jsonが無い・読めない）")
    annual_years = list(parsed.get("annual", {}).keys())[:3]
    print(f"   年次: {annual_years}")

    # 3. 四半期Raw Table生成
    ctx["stage"] = STAGE_RAW
    company_facts = load_company_facts(ticker, data_dir=data_dir)
    if company_facts is None:
        raise TickerFailed(STAGE_RAW, "company_facts.jsonを読み込めない")
    raw_table = build_raw_table(ticker, company_facts)
    print(f"   Raw Table: {len(raw_table.get('fields', {}))} fields")

    # 4. 正規化（YTD→Q差分変換）
    ctx["stage"] = STAGE_NORMALIZE
    normalized = normalize(ticker, raw_table)
    save_normalized(ticker, normalized)
    print(f"   Normalized: OK")

    # 4b. Revenue品質チェック（検査だけで出力を書かないため、例外は銘柄の失敗にしない）
    try:
        rq = check_revenue_quality(ticker, normalized)
        status = rq["status"]
        yoy = f"{rq['latest_rev_yoy']:+.1f}%" if rq["latest_rev_yoy"] is not None else "N/A"
        if status == "ISSUE":
            print(f"   [Revenue ❌ ISSUE] yoy={yoy}")
            for iss in rq["issues"]:
                print(f"     ❌ {iss}")
        elif status == "WARN":
            print(f"   [Revenue ⚠️  WARN] yoy={yoy}")
            for w in rq["warnings"]:
                print(f"     ⚠️  {w}")
        else:
            print(f"   Revenue: OK  yoy={yoy}")
        # Step Summaryの行はメモリに貯め、最後に見出しと一緒に書く
        if status in ("ISSUE", "WARN"):
            icon = "🔴" if status == "ISSUE" else "🟡"
            revenue_rows.append(f"| {ticker} | {icon} {status} | {yoy} | "
                                f"{'; '.join(rq['issues'] + rq['warnings'])[:120]} |")
    except Exception as e:
        print(f"   [WARN] Revenue品質チェックエラー: {e}")

    # 4c. Revenue系タグ競合チェック（ARCH-DATA-1残課題③）
    # SEC-REV-FINTECH-1/BUG-REV-SPAC-1型の「複数候補タグが同一年度で
    # 大きく食い違う」ケースを検知する。自動修正は行わずWARN表示のみ。
    try:
        tc = check_revenue_tag_conflict(ticker, data_dir=data_dir)
        if tc["status"] == "WARN":
            print(f"   [TagConflict ⚠️  WARN] {len(tc['conflicts'])}件")
            for c in tc["conflicts"]:
                print(f"    {_format_conflict(c)}")
    except Exception as e:
        print(f"   [WARN] タグ競合チェックエラー: {e}")

    # 5. TTMシリーズ生成（フェーズC対応: 入力元をnormalize()の戻り値
    #    からbuild_ticker_store()の戻り値〈Layer3、インメモリ〉に切替）
    ctx["stage"] = STAGE_TTM
    store = build_ticker_store(ticker)
    if store is None:
        raise TickerFailed(STAGE_TTM, "Layer3ストアを構築できない")
    ttm_series = calc_ttm_series(ticker, store)
    save_ttm_series(ticker, ttm_series)
    n = len(ttm_series)
    fields_sample = list(ttm_series[0].get("flow", {}).keys()) if ttm_series else []
    print(f"   TTM: {n} periods → {fields_sample[:5]}...")
    return entry


def failed_entry(stage: str, reason: str, ctx: dict) -> dict:
    """失敗した銘柄の_freshness.jsonの1銘柄分（status=FAILED）。出力は前回の状態に戻すので、
    手元の最大filed・未反映の提出は取り直す前の値で書く。fetch_statusは取得の段の分類。"""
    plan = ctx.get("plan") or {}
    fetched = ctx.get("entry") or {}
    e = fr.entry(fr.FAILED, plan.get("case") or "unknown", plan.get("latest_filed") or "",
                 plan.get("latest_submission"), plan.get("pending") or [],
                 f"[{stage}] {reason}（この銘柄の出力は前回の状態に戻す）")
    e["failed_stage"] = stage
    e["failed_reason"] = reason
    e["fetch_status"] = fetched.get("status")
    return e


def step_summary_failures(failures: list, n_targets: int, code: int, aborted: Optional[str]) -> str:
    """Step Summaryの先頭の節（失敗した銘柄の表・ループの外の例外）。失敗が無ければ空。"""
    lines = []
    pct = int(FAILED_EXIT_RATIO * 100)
    if aborted:
        lines += ["## ❌ SEC Data Update: 銘柄のループの外で例外（終了コード1、commitしない）", "",
                  f"`{aborted}`", ""]
    if failures:
        if aborted:
            icon, rule = "❌", "ループの外の例外のため終了コード1（commitしない）"
        elif code:
            icon, rule = "❌", f"失敗が対象の{pct}%を超えたため終了コード1（commitしない）"
        else:
            icon, rule = "⚠", (f"失敗が対象の{pct}%以下のため終了コード0"
                               "（失敗した銘柄だけ前回の状態に戻し、他の銘柄はcommitする）")
        lines += [f"## {icon} 失敗した銘柄 {len(failures)} / 対象 {n_targets}", "", rule, "",
                  "| 銘柄 | 段階 | 理由 |", "|:---|:---|:---|"]
        lines += [f"| {f['ticker']} | {f['stage']} | {f['reason'].replace('|', '/')[:200]} |" for f in failures]
        lines.append("")
    return "\n".join(lines) + ("\n" if lines else "")


def step_summary_revenue(revenue_rows: list) -> str:
    """Revenue品質チェックの表（見出しを行より先に書く。[[SEC-UPDATE-SUMMARY-HEADER-ORDER-1]]）。"""
    if not revenue_rows:
        return ""
    head = ["", "## Revenue品質チェック（WARN/ISSUE銘柄のみ）", "",
            "| Ticker | Status | YoY | 内容 |", "|:---|:---|---:|:---|"]
    return "\n".join(head + revenue_rows) + "\n"


def run(tickers: list, full_run: bool, data_dir: Optional[str] = None) -> int:
    """全銘柄（またはtickers）を処理し、終了コードを返す。"""
    data_dir = data_dir or DATA_DIR
    started = time.time()
    started_at = fr.utc_now_iso()

    print("=" * 60)
    print("SEC EDGAR データ更新")
    print(f"対象: {len(tickers)} 銘柄")
    print(f"保存先: {data_dir}")
    print("=" * 60)

    success = 0
    failures = []          # [{"ticker", "stage", "reason"}]
    freshness_entries = {}
    revenue_rows = []
    aborted = None         # 銘柄のループの外の例外
    fetcher = None

    try:
        fetcher = SECFetcher(data_dir=data_dir)
        parser = SECParser(data_dir=data_dir)
        for ticker in tickers:
            print(f"\n--- {ticker} ---")
            ctx = {"stage": STAGE_FETCH}
            try:
                freshness_entries[ticker] = process_ticker(ticker, fetcher, parser, data_dir, ctx, revenue_rows)
                success += 1
            except Exception as e:
                if isinstance(e, TickerFailed):
                    stage, reason = e.stage, e.reason
                else:
                    stage, reason = ctx["stage"], f"{type(e).__name__}: {e}"
                    traceback.print_exc()
                print(f"   [FAILED] {stage}: {reason} → 次の銘柄へ（この銘柄の出力は前回の状態に戻す）")
                failures.append({"ticker": ticker, "stage": stage, "reason": reason})
                freshness_entries[ticker] = failed_entry(stage, reason, ctx)
    except Exception as e:
        aborted = f"{type(e).__name__}: {e}"
        traceback.print_exc()
    finally:
        aborted = _finalize(tickers, full_run, data_dir, started, started_at, success, failures,
                            freshness_entries, revenue_rows, aborted, fetcher)

    return exit_code(len(tickers), len(failures), aborted is not None)


def _finalize(tickers, full_run, data_dir, started, started_at, success, failures,
              freshness_entries, revenue_rows, aborted, fetcher) -> Optional[str]:
    """_freshness.json・失敗の一覧・Step Summaryを書く（銘柄の処理が途中で止まっても必ず通る）。
    書き込みの例外は、銘柄のループの外の例外として返す（終了コード1）。"""
    errors = []

    # 鮮度の記録（銘柄を絞った実行ではgenerated_atを変えない。freshness.write()参照）。
    # ループの外の例外で途中までの実行は、全銘柄の実行として数えない（generated_atを進めない）
    try:
        fr.write(freshness_entries, full_run=full_run and aborted is None, path=fr.default_path(data_dir))
    except Exception as e:
        errors.append(f"_freshness.jsonを書けない: {type(e).__name__}: {e}")
        traceback.print_exc()

    # サマリー
    print("\n" + "=" * 60)
    print(f"完了: {success}/{len(tickers)}")
    if failures:
        print("失敗: " + ", ".join(f"{f['ticker']}({f['stage']})" for f in failures))
    run_counts = fr.count_statuses(freshness_entries)
    print("鮮度: " + " / ".join(f"{s} {run_counts[s]}" for s in fr.STATUSES))
    for s in (fr.REFRESHED, fr.SEC_LAG, fr.FETCH_FAILED, fr.FAILED):
        hit = [t for t, e in freshness_entries.items() if e["status"] == s]
        if hit:
            print(f"   {s}: {', '.join(hit)}")
    if fetcher is not None:
        print(f"SECへのリクエスト数: {fetcher.request_count}・実行時間: {time.time() - started:.1f}秒")
    print("=" * 60)

    # 失敗の一覧（リポジトリの外。SEC_Data_Update.ymlの巻き戻しとDiscordの段が読む）
    rp = result_path()
    try:
        os.makedirs(os.path.dirname(rp) or ".", exist_ok=True)
        aborted_now = aborted or ("; ".join(errors) if errors else None)
        result = {
            "started_at": started_at,
            "finished_at": fr.utc_now_iso(),
            "full_run": full_run,
            "targets": len(tickers),
            "succeeded": success,
            "failed": failures,
            "failed_exit_ratio": FAILED_EXIT_RATIO,
            "aborted": aborted_now,
            "exit_code": exit_code(len(tickers), len(failures), aborted_now is not None),
        }
        with open(rp, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"失敗の一覧: {rp}")
    except Exception as e:
        errors.append(f"失敗の一覧を書けない: {type(e).__name__}: {e}")
        traceback.print_exc()

    if errors and aborted is None:
        aborted = "; ".join(errors)
    code = exit_code(len(tickers), len(failures), aborted is not None)

    # GitHub Actions Step Summary（先頭に失敗した銘柄、次にRevenue品質、最後に鮮度）
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        try:
            text = step_summary_failures(failures, len(tickers), code, aborted)
            text += step_summary_revenue(revenue_rows)
            # SECデータの鮮度（分類ごとの件数と、REFRESHED・SEC_LAG・FETCH_FAILED・FAILEDの銘柄）
            text += fr.step_summary_markdown({"tickers": freshness_entries})
            with open(summary_path, "a", encoding="utf-8") as sf:
                sf.write(text)
        except Exception as e:
            print(f"[ERROR] Step Summaryを書けない: {type(e).__name__}: {e}")
    return aborted


def main():
    ap = argparse.ArgumentParser(description="SEC EDGAR データ更新")
    ap.add_argument("tickers", nargs="*", help="対象ティッカー（省略時は全銘柄）")
    ap.add_argument("--dry-run", action="store_true",
                    help="submissionsだけ取得し、company_factsを取得・取り直しする銘柄を表示する（ファイルは書かない）")
    args = ap.parse_args()
    full_run = not args.tickers
    tickers = [t.upper() for t in args.tickers] if args.tickers else get_all()

    if args.dry_run:
        dry_run(tickers)
        return

    sys.exit(run(tickers, full_run))


if __name__ == "__main__":
    main()
