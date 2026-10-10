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
import sys
import os
import time

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

    # データ保存先をcommon/sec_data/data/に設定
    data_dir = os.path.join(script_dir, "data")
    fetcher = SECFetcher(data_dir=data_dir)
    parser = SECParser(data_dir=data_dir)
    started = time.time()

    print("=" * 60)
    print("SEC EDGAR データ更新")
    print(f"対象: {len(tickers)} 銘柄")
    print(f"保存先: {data_dir}")
    print("=" * 60)

    success = 0
    failed = []
    freshness_entries = {}

    for ticker in tickers:
        print(f"\n--- {ticker} ---")

        # 1. submissionsを取り直し、company_factsを取得・取り直すかを決める
        #    （submissionsは本人データ判定用のreportDateにも使う。取得に失敗しても
        #    手元のsubmissions.json・determine_fiscal_year()フォールバックで継続できる）
        plan = plan_company_facts(fetcher, ticker, data_dir)
        entry, raw = execute_company_facts(fetcher, plan)
        freshness_entries[ticker] = entry
        print(f"   鮮度: {entry['status']}（{entry['case']}）" + (f" {entry['note']}" if entry["note"] else ""))
        if not raw:
            failed.append(ticker)
            continue

        # 2. 従来パース＆保存（annual等）
        parsed = parser.parse_and_save(ticker)
        if parsed:
            annual_years = list(parsed.get("annual", {}).keys())[:3]
            print(f"   年次: {annual_years}")
        else:
            failed.append(ticker)
            continue

        # 3. 四半期Raw Table生成
        try:
            company_facts = load_company_facts(ticker, data_dir=data_dir)
            if company_facts is None:
                print(f"   [WARN] company_facts 読み込み失敗 → TTMスキップ")
                success += 1
                continue

            raw_table = build_raw_table(ticker, company_facts)
            print(f"   Raw Table: {len(raw_table.get('fields', {}))} fields")
        except Exception as e:
            print(f"   [WARN] Raw Table生成エラー: {e} → TTMスキップ")
            success += 1
            continue

        # 4. 正規化（YTD→Q差分変換）
        try:
            normalized = normalize(ticker, raw_table)
            save_normalized(ticker, normalized)
            print(f"   Normalized: OK")
        except Exception as e:
            print(f"   [WARN] Normalize エラー: {e} → TTMスキップ")
            success += 1
            continue

        # 4b. Revenue品質チェック
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
            # GitHub Actions Step Summary に書き込み
            summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
            if summary_path and status in ("ISSUE", "WARN"):
                with open(summary_path, "a", encoding="utf-8") as sf:
                    icon = "🔴" if status == "ISSUE" else "🟡"
                    sf.write(f"| {ticker} | {icon} {status} | {yoy} | "
                             f"{'; '.join(rq['issues'] + rq['warnings'])[:120]} |\n")
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
        try:
            store = build_ticker_store(ticker)
            if store is None:
                print(f"   [WARN] Layer3ストア構築失敗 → TTMスキップ")
            else:
                ttm_series = calc_ttm_series(ticker, store)
                save_ttm_series(ticker, ttm_series)
                n = len(ttm_series)
                fields_sample = list(ttm_series[0].get("flow", {}).keys()) if ttm_series else []
                print(f"   TTM: {n} periods → {fields_sample[:5]}...")
        except Exception as e:
            print(f"   [WARN] TTM計算エラー: {e}")

        success += 1

    # 鮮度の記録（銘柄を絞った実行ではgenerated_atを変えない。freshness.write()参照）
    doc = fr.write(freshness_entries, full_run=full_run, path=fr.default_path(data_dir))

    # サマリー
    print("\n" + "=" * 60)
    print(f"完了: {success}/{len(tickers)}")
    if failed:
        print(f"失敗: {', '.join(failed)}")
    run_counts = fr.count_statuses(freshness_entries)
    print("鮮度: " + " / ".join(f"{s} {run_counts[s]}" for s in fr.STATUSES))
    for s in (fr.REFRESHED, fr.SEC_LAG, fr.FETCH_FAILED):
        hit = [t for t, e in freshness_entries.items() if e["status"] == s]
        if hit:
            print(f"   {s}: {', '.join(hit)}")
    print(f"SECへのリクエスト数: {fetcher.request_count}・実行時間: {time.time() - started:.1f}秒")
    print("=" * 60)

    # GitHub Actions Step Summary にRevenue品質チェック結果のヘッダーを追記
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        # ヘッダーが未出力の場合のみ追記（先頭に書く）
        header = "\n## Revenue品質チェック（WARN/ISSUE銘柄のみ）\n\n| Ticker | Status | YoY | 内容 |\n|:---|:---|---:|:---|\n"
        try:
            with open(summary_path, "r", encoding="utf-8") as sf:
                existing = sf.read()
        except FileNotFoundError:
            existing = ""
        if "Revenue品質チェック" not in existing:
            with open(summary_path, "a", encoding="utf-8") as sf:
                sf.write(header)
        # SECデータの鮮度（分類ごとの件数と、REFRESHED・SEC_LAG・FETCH_FAILEDの銘柄）
        with open(summary_path, "a", encoding="utf-8") as sf:
            sf.write(fr.step_summary_markdown({"tickers": freshness_entries}))

    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
