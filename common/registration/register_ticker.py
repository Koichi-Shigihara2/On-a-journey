#!/usr/bin/env python3
"""
common/registration/register_ticker.py
新規銘柄登録オーケストレーションスクリプト
-----------------------------------------------------------------------
[[REGISTER-FLOW-REDESIGN-1]]方針3（2026-09-03新設）。CLAUDE_CODE_START.md
「新規銘柄登録時の必須手順」のStep 1〜8を自動連続実行する。

前提（Step 0.5、本スクリプトの対象外・事前に手動で完了させること）:
    - config/cik_lookup.csv に対象ティッカーの行が既に存在し、
      status=provisioning（方針2、common/sec_data/tickers.pyが
      パイプライン対象外として扱う）・cik・各フラグ
      （tanuki/stonks_silo/eps/hypecore）・registered_date・
      registration_source・registration_note が記録済みであること
    - Step 0（カナダ企業チェック）が完了していること

Step 0.5直後・Step 1実行前（[[QUALITY-GATES-EPIC-1]]ゲート0、
[[PREFLIGHT-CHECK-1]]、2026-09-05新設）:
    - common/registration/preflight_check.pyによるプリフライトチェック
      （①上場後3年未満 ②直近提出書類が20-F等 ③収益系XBRLタグ不在）を
      自動実行する。フラグが立っても処理は自動停止せず、警告表示のみ
      で続行する（判断材料の提示に留め、Koichiさんの最終判断を妨げない）。

Usage:
    python common/registration/register_ticker.py TICKER --target-status active
    python common/registration/register_ticker.py TICKER1 TICKER2 --target-status candidate
        # 複数指定時も内部的に1銘柄ずつStep 1〜8をフル実行する
        # （[[REGISTER-FLOW-REDESIGN-1]]方針5、「手動一括登録」の
        # 抜け道を構造的に塞ぐ設計）。

    --target-status {active,candidate}
        Step 8でNG=0だった場合の昇格先。Step 0.5で決めた本来の意図を
        CLIで明示指定させる（cik_lookup.csvのregistration_note等からの
        自動判定は曖昧になりうるため、[[REGISTER-FLOW-REDESIGN-1]]方針2
        着手時に明示指定方式を採用した）。

    --dry-run
        本スクリプト自身が行う書き込み（Step 6 discover_config.json・
        Step 7 monitor_tickers.yaml・Step 8 promote）のみをスキップし、
        「実行されたであろう内容」を表示する。Step 1/3/5/5b（update.py・
        pipeline.py・hypecore.py・adjusted_eps_analyzer/pipeline.py）は
        ネイティブなdry-runモードを持たないサブプロセス呼び出しのため、
        本フラグでは抑止できない（実際にデータファイルへ書き込む）。
        Step 2（beta_fetcher.py）のみネイティブ--dry-runへ引き継ぐ。
        本番銘柄リストに影響させずに全体のフローを確認したい場合は、
        検証手順にある通りテスト用の一時ブランチ・worktreeを使うこと。

各ステップはべき等に設計されている（Step 1/2/3/4/5/5bはいずれも
「取得・再生成して上書き」する既存スクリプトの挙動に依存、Step 6/7は
CLAUDE_CODE_START.mdの既存インラインコードと同じ「既に存在すればスキップ」
パターンを踏襲）。Step 2.5・3.5（下記）で一時停止した場合、Claude Codeが
必要な確認・書き込みを行った後に同じコマンドを再実行すれば、完了済みの
ステップは再実行されるだけで安全に続きから進められる（ロールバックは
実装しない設計、[[REGISTER-FLOW-REDESIGN-1]]方針3依頼書の通り）。

Step 2.5・3.5はいずれも本スクリプトが判定を代行しない（risk_fetcher.py・
Discoverサブシステム撤去と同じ「根拠不明の生成をそのまま採用しない」
方針）。一時停止し、Claude Code自身が10-K本文を読んで判断してから
書き込み、再実行することを前提とする。

common/sec_data/update.py（SEC取得本体）はconfig.py::get_all()という
別経路でティッカー一覧を取得しており、statusを一切見ない（全銘柄を対象に
する）。本スクリプトのStep 1もこの既存経路をそのまま呼ぶため、
provisioning状態のティッカーもSEC取得自体は通常通り行われる
（SEC取得は計算・表示に影響しないため実害小、既知のギャップとして
[[REGISTER-FLOW-REDESIGN-1]]に記録済み）。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import textwrap
from datetime import datetime

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.normpath(os.path.join(_SCRIPT_DIR, "..", ".."))
sys.path.insert(0, _REPO_ROOT)

from common.registration.preflight_check import (  # noqa: E402
    run_preflight_check, print_preflight_report,
)

# 標準出力がパイプ/リダイレクト先の場合、Pythonのprint()はデフォルトで
# フルバッファリングされる。一方、subprocess.run()の子プロセス出力は
# 共有fdへ直接書き込まれるため即時表示される。これにより、本スクリプト
# 自身のprint()（Stepヘッダー・一時停止メッセージ等）が子プロセスの出力
# より大幅に遅れて表示される（実行順序と表示順序が食い違う）事故が
# 実地検証（2026-09-03、HIMSでのテスト登録）で発覚した。行バッファ化して
# 子プロセス出力との時系列整合性を保つ。
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

CIK_CSV      = os.path.join(_REPO_ROOT, "config", "cik_lookup.csv")
BETA_CFG     = os.path.join(_REPO_ROOT, "config", "beta_config.json")
DISCOVER_CFG = os.path.join(_REPO_ROOT, "config", "discover_config.json")
MONITOR_YAML = os.path.join(_REPO_ROOT, "config", "monitor_tickers.yaml")
SEC_DATA_DIR = os.path.join(_REPO_ROOT, "common", "sec_data", "data")

PYTHON = sys.executable
TARGET_STATUSES = ("active", "candidate")


class PausedForReview(Exception):
    """Step 3.5でClaude Codeの判断待ちのため一時停止する場合に送出する。"""


# ─── cik_lookup.csv 読み取りヘルパー ────────────────────────────────────

def _load_cik_row(ticker: str) -> dict | None:
    with open(CIK_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("ticker", "").strip().upper() == ticker.upper():
                return row
    return None


def _flag(row: dict, name: str) -> bool:
    return row.get(name, "").strip().lower() == "true"


# ─── サブプロセス実行ヘルパー ────────────────────────────────────────────

def _run(cmd: list[str], label: str) -> int:
    print(f"\n--- {label} ---")
    print("  $ " + " ".join(cmd))
    result = subprocess.run(cmd, cwd=_REPO_ROOT)
    return result.returncode


# ─── Step 1: SEC データ取得 ──────────────────────────────────────────────

def step1_sec_data(ticker: str) -> bool:
    rc = _run([PYTHON, "common/sec_data/update.py", ticker], "Step 1: SEC データ取得")
    if rc != 0:
        print(f"  ❌ Step 1 失敗（exit={rc}）。SEC取得に失敗したため以降を中断します。")
        return False
    return True


# ─── Step 2: β取得 ───────────────────────────────────────────────────────

def step2_beta(ticker: str, dry_run: bool) -> None:
    cmd = [PYTHON, "src/value/tanuki_valuation/beta_fetcher.py", ticker]
    if dry_run:
        cmd.append("--dry-run")
    rc = _run(cmd, "Step 2: β取得")
    if rc != 0:
        print("  ⚠️  Step 2 は非ブロッキング（raw yfinance値のまま続行、"
              "market_data未生成の新規銘柄では正常にスキップされることがある）")


# ─── Step 3: TANUKI VALUATION パイプライン実行 ───────────────────────────

def step3_pipeline(ticker: str) -> bool:
    rc = _run([PYTHON, "src/value/tanuki_valuation/pipeline.py", ticker],
              "Step 3: TANUKI VALUATION パイプライン実行")
    if rc != 0:
        print(f"  ❌ Step 3 失敗（exit={rc}）。latest.json が生成されなかった"
              "可能性があるため以降を中断します。")
        return False
    return True


def _segment_review_path(ticker: str) -> str:
    return os.path.join(SEC_DATA_DIR, ticker, "segment_review.json")


def step3_5_segment_config_gate(ticker: str) -> None:
    """ASC 280の正式セグメント数はXBRLタグから機械的に判定できないため、
    common/sec_data/data/{ticker}/segment_review.json に
    {"reviewed": true, ...} が書き込まれるまで常に一時停止する。

    Claude Codeが10-Kの"Segment Information"セクションを確認し、以下
    いずれかを行った上でこのファイルを書き込む:
    - LLY型（formal segmentが1つ）: 設定不要と判断し、その旨を記録するのみ
    - LMT型（formal segmentが2つ以上）: config/segment_config.jsonに
      比率・成長率・根拠コメントを書き込んだ上で、その旨を記録する
    """
    path = _segment_review_path(ticker)
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                review = json.load(f)
        except Exception:
            review = {}
        if review.get("reviewed"):
            print(f"  ✅ Step 3.5: セグメント確認済み（{path}）"
                  f" — result={review.get('result', '?')}")
            return

    raise PausedForReview(textwrap.dedent(f"""\
        ⏸️  Step 3.5: セグメント設定の確認が必要です（一時停止）
        {ticker} のASC 280正式セグメント数はXBRLタグから機械的に判定
        できないため、Claude Codeが10-Kの"Segment Information"
        セクションを直接確認する必要があります（LLY型/LMT型判定ルールは
        CLAUDE_CODE_START.md「新規銘柄のセグメント設定判断ルール」参照）。

        確認後、{path} に以下いずれかの内容を書き込んでから
        このコマンドを再実行してください（"note"には10-Kの該当箇所の
        引用・確認内容を記録すること）:

        - formal segmentが1つ（LLY型・設定不要）:
          {{"reviewed": true, "formal_segments": 1,
            "result": "no_segment_config_needed", "note": "<引用・根拠>"}}

        - formal segmentが2つ以上（LMT型・要設定）:
          config/segment_config.json に比率・成長率・根拠コメントを
          設定した上で、
          {{"reviewed": true, "formal_segments": <N>,
            "result": "segment_config_written", "note": "<引用・根拠>"}}
    """))


# ─── Step 4: データ品質確認（β設定含む、非ブロッキング） ────────────────

def step4_audit(ticker: str) -> None:
    rc = _run([PYTHON, "common/sec_data/audit.py", ticker, "--check-beta"],
              "Step 4: データ品質確認（β設定含む）")
    if rc != 0:
        print("  ⚠️  Step 4 は非ブロッキング（重大問題が出力された場合は"
              "内容を確認すること。ゲートはStep 8のNG=0判定）")


# ─── Step 5: HypeCore 実行（hypecore=true のみ） ─────────────────────────

def step5_hypecore(ticker: str) -> None:
    _run([PYTHON, "src/value/hypecore/hypecore.py", "--batch", ticker],
         "Step 5: HypeCore 実行")
    # hypecore.py の __main__ は個別ティッカーの失敗を捕捉して継続する
    # 設計のため、プロセス自体のexit codeは常に0になる（例外なしで完走
    # すれば）。実際の成否は生成物の有無で判定する（registration_
    # validator.pyのP1-Step5-HypeCoreと同じ判定基準）。
    docs_poc = os.path.join(
        _REPO_ROOT, "docs", "value-monitor", "hypecore", "data", f"{ticker}_poc.json"
    )
    if not os.path.exists(docs_poc):
        print(f"  ⚠️  Step 5 は非ブロッキング（{ticker}_poc.json 未生成、"
              "yfinance依存のためデータ不足銘柄は失敗することがある）")


# ─── Step 5b: EPS Analyzer 実行（eps=true のみ） ─────────────────────────

def step5b_eps_analyzer(ticker: str) -> None:
    _run([PYTHON, "-m", "src.value.adjusted_eps_analyzer.pipeline", "--ticker", ticker],
         "Step 5b: EPS Analyzer 実行")
    eps_dir = os.path.join(
        _REPO_ROOT, "docs", "value-monitor", "adjusted_eps_analyzer", "data", ticker
    )
    if not os.path.exists(eps_dir):
        print(f"  ⚠️  Step 5b は非ブロッキング（{ticker}のEPS Analyzerデータ"
              "未生成。非US GAAP・NetIncomeLoss四半期データ欠損等の場合は"
              "cik_lookup.csvのeps列をfalseに設定することを検討）")


# ─── Step 6: Discover 監視リストに追加 ───────────────────────────────────

def step6_discover_register(ticker: str, dry_run: bool) -> None:
    print("\n--- Step 6: Discover 監視リストに追加 ---")
    with open(DISCOVER_CFG, encoding="utf-8") as f:
        config = json.load(f)
    if ticker in config.get("tickers", {}):
        print(f"  {ticker} はすでに登録済みです")
        return
    if dry_run:
        print(f"  [dry-run] {ticker} をDiscover監視リストに追加します（実際には書き込みません）")
        return
    config.setdefault("tickers", {})[ticker] = {"category": "監視中", "memo": "", "themes": []}
    config["last_updated"] = datetime.now().date().isoformat()
    with open(DISCOVER_CFG, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"  {ticker} をDiscover監視リストに追加しました")
    print("  （docs/portfolio/data/discover_config.jsonへの同期はDiscover_Config_Sync.ymlが自動実行、"
          "push後数分内に反映）")


# ─── Step 7: monitor_tickers.yaml に追加 ─────────────────────────────────

def step7_monitor_register(ticker: str, dry_run: bool) -> None:
    print("\n--- Step 7: monitor_tickers.yaml に追加 ---")
    with open(MONITOR_YAML, encoding="utf-8") as f:
        content = f.read()
    existing = {l.strip().lstrip("- ") for l in content.splitlines() if l.strip().startswith("- ")}
    if ticker in existing:
        print(f"  {ticker} はすでに登録済みです")
        return
    if dry_run:
        print(f"  [dry-run] {ticker} を monitor_tickers.yaml に追加します（実際には書き込みません）")
        return
    with open(MONITOR_YAML, "a", encoding="utf-8") as f:
        f.write(f"  - {ticker}\n")
    print(f"  {ticker} を monitor_tickers.yaml に追加しました")


# ─── Step 7.5: 同一期間の整合性チェック（昇格前ゲート） ─────────────────
# [[REGISTRATION-VALIDATOR-P2A-PERIOD-MISMATCH-1]]（2026-09-24）:
# registration_validator.pyのP2-A（年次売上とTTM売上の比較）は期末の異なる
# 期間を比べていたため廃止した。代わりに、同一期間どうしを突合する
# report_consistency_check.pyのCHECK-35（operating_income再構成×yfinance）・
# CHECK-41（revenue/net_income×yfinance、年次期末日が一致する列）・
# CHECK-47（parser系⇔Layer3系のTTM、連続した同一4四半期）等を、
# provisioning中の当該銘柄に対して昇格判定の前に実行する。
#
# 判定: report_consistency_checkのNG（--fail-on-ng、exit 1）または対象0件
# （exit 2）なら昇格を止める。CHECK-35/47はWARN専用。CHECK-41のrevenueは
# --include-provisioning指定時（本ステップ）のみ、yfinanceとの|乖離|が
# REGISTRATION_REVENUE_NG_THRESHOLD（30%）を超えるとNG（NG-41）になり昇格を
# 止める（単位誤り・別年度混入等。日次・週次のCIではWARNのまま）。
# yfinance取得失敗で CHECK-41 revenue突合が実行できなかった場合も昇格は
# 止めず（外部サービスの一時障害で登録を止めないため。CHECK-47〈SEC内部の
# 2系統突合〉・CHECK-31等はyfinanceに依存せず実行される）、その旨を
# 明示的に表示する。
# WARN行の先頭には台帳の注記（"🆕未確認 "等）が入りうるため、括弧を含めず照合する
_CHECK41_REVENUE_MARK = "WARN-41 revenue yfinance突合"


def step7_5_consistency_check(ticker: str, row: dict) -> bool:
    label = "Step 7.5: 同一期間の整合性チェック（CHECK-35/41/47等）"
    if not _flag(row, "tanuki"):
        print(f"\n--- {label} ---")
        print("  スキップ（tanuki=false: report_consistency_check.pyの対象外）")
        return True
    cmd = [PYTHON, "common/sec_data/report_consistency_check.py",
           "--ticker", ticker, "--include-provisioning",
           "--include-yfinance-checks", "--fail-on-ng"]
    print(f"\n--- {label} ---")
    print("  $ " + " ".join(cmd))
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    result = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True,
                            text=True, encoding="utf-8", errors="replace", env=env)
    out = (result.stdout or "") + (result.stderr or "")
    print(out, end="" if out.endswith("\n") else "\n")
    if result.returncode == 2:
        print(f"  ❌ Step 7.5: {ticker}がチェック対象にならなかったため昇格を止めます"
              "（report.txt未生成等。上記の理由を確認すること）")
        return False
    if result.returncode != 0:
        print(f"  ❌ Step 7.5: 整合性チェックでNGが検出されたため昇格を止めます（exit={result.returncode}）")
        return False
    if _CHECK41_REVENUE_MARK not in out:
        print("  ⚠️  Step 7.5: CHECK-41のrevenue yfinance突合が実行されませんでした"
              "（yfinance取得失敗、または年次期末日と一致する列なし）。"
              "昇格は止めずに続行します（CHECK-47等のSEC内部突合は実行済み）")
    return True


# ─── Step 8: 登録パイプライン健全性チェック＋昇格 ────────────────────────

def step8_validate_and_promote(ticker: str, target_status: str, dry_run: bool) -> bool:
    cmd = [PYTHON, "common/sec_data/registration_validator.py", ticker]
    if not dry_run:
        cmd += ["--promote", target_status]
    rc = _run(cmd, "Step 8: 登録パイプライン健全性チェック" + ("" if dry_run else "＋昇格"))
    if dry_run:
        print(f"  [dry-run] NG=0であれば status を '{target_status}' へ昇格します（実際には昇格しません）")
        return rc == 0
    return rc == 0


# ─── メイン ──────────────────────────────────────────────────────────────

def register_one(ticker: str, target_status: str, dry_run: bool) -> bool:
    print(f"\n{'=' * 60}")
    print(f"  {ticker} の登録処理を開始")
    print(f"{'=' * 60}")

    row = _load_cik_row(ticker)
    if row is None:
        print(f"❌ {ticker} は config/cik_lookup.csv に見つかりません。"
              "Step 0.5（登録メタデータの記録）を先に実施してください。")
        return False

    print(f"現在のstatus: {row.get('status', '(不明)')}"
          f" / tanuki={row.get('tanuki')} stonks_silo={row.get('stonks_silo')}"
          f" eps={row.get('eps')} hypecore={row.get('hypecore')}")

    # Step 0.5直後・Step 1実行前のプリフライトチェック（[[QUALITY-GATES-
    # EPIC-1]]ゲート0、[[PREFLIGHT-CHECK-1]]想定機能①〜④）。フラグが
    # 立っても自動停止しない（判断材料の提示のみ、想定機能④）。
    preflight_result = run_preflight_check(ticker)
    print_preflight_report(preflight_result)

    tanuki_enabled = _flag(row, "tanuki")
    hypecore_enabled = _flag(row, "hypecore")
    eps_enabled = _flag(row, "eps")

    if not step1_sec_data(ticker):
        return False

    step2_beta(ticker, dry_run)

    if tanuki_enabled:
        if not step3_pipeline(ticker):
            return False

        try:
            step3_5_segment_config_gate(ticker)
        except PausedForReview as e:
            print(str(e))
            return False
    else:
        print("\n--- Step 3/2.5/3.5: スキップ（tanuki=false） ---")

    step4_audit(ticker)

    if hypecore_enabled:
        step5_hypecore(ticker)
    else:
        print("\n--- Step 5: スキップ（hypecore=false） ---")

    if eps_enabled:
        step5b_eps_analyzer(ticker)
    else:
        print("\n--- Step 5b: スキップ（eps=false） ---")

    step6_discover_register(ticker, dry_run)
    step7_monitor_register(ticker, dry_run)

    ok = step7_5_consistency_check(ticker, row)
    if ok:
        ok = step8_validate_and_promote(ticker, target_status, dry_run)
    else:
        print("\n--- Step 8: スキップ（Step 7.5で昇格を止めたため） ---")

    print(f"\n{'─' * 60}")
    if ok:
        print(f"✅ {ticker}: 登録処理が完了しました"
              + ("（dry-run、実際の昇格なし）" if dry_run else f"（status → {target_status}）"))
    else:
        print(f"⏸️  {ticker}: NGが残っているため昇格せず終了しました。"
              "内容を確認し、対処後に再実行してください。")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(
        description="新規銘柄登録オーケストレーションスクリプト（Step 1〜8を自動連続実行）"
    )
    parser.add_argument("tickers", nargs="+", help="登録対象ティッカー（複数指定可、1銘柄ずつフル実行）")
    parser.add_argument(
        "--target-status", required=True, choices=TARGET_STATUSES,
        help="Step 8でNG=0だった場合の昇格先（Step 0.5で決めた本来の意図をここで明示指定する）",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="本スクリプト自身の書き込み（Step 6/7/8）のみ抑止する（Step 1/3/5/5bは対象外、docstring参照）",
    )
    args = parser.parse_args()

    results = {t.upper(): register_one(t.upper(), args.target_status, args.dry_run) for t in args.tickers}

    print(f"\n{'=' * 60}")
    print("登録処理サマリー")
    for t, ok in results.items():
        print(f"  {'✅' if ok else '⏸️ '} {t}")

    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
