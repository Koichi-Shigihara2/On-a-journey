"""
update.pyで失敗した銘柄の出力を前回の状態（HEAD）に戻す（[[SEC-UPDATE-EXIT1-BLOCKS-ALL-1]]、2026-10-15）。

SEC_Data_Update.ymlでupdate.pyの後・Consistency Check Gateの前に実行する。
update.pyが書いた失敗の一覧（sec_update_result.json、リポジトリの外）を読み、失敗した銘柄ごとに
次のパスを戻す（1銘柄が書き換えるファイルのすべて）:
  common/sec_data/data/{T}/                         company_facts.json・company_facts_legacy_*.json・
                                                    submissions*.json・annual_*.json・quarterly_*.json・
                                                    parserの検知ログ（fy_collision_log.json等）
  common/sec_data/normalized/{T}_quarterly_normalized.json
  common/sec_data/ttm/{T}_ttm_series.json
- HEADにあるファイル（既存の銘柄）: git checkout HEAD -- で戻す
- HEADに無いファイル（新規の銘柄の作りかけ、既存の銘柄で今回新しくできたファイル）: git clean で消す
共有のファイル（common/sec_data/data/_freshness.json・_cik_cache.json）は戻さない
（_freshness.jsonには失敗した銘柄がFAILEDとして記録されている）。

使用方法:
    python common/sec_data/rollback_failed.py [sec_update_result.json のパス]
    （省略時はupdate.result_path()と同じ場所: 環境変数SEC_UPDATE_RESULT → RUNNER_TEMP → 一時ディレクトリ）
終了コード: 戻せた（失敗した銘柄が無い場合を含む）なら0、gitの操作に失敗したら1。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

RESULT_FILENAME = "sec_update_result.json"


def default_result_path() -> str:
    p = os.environ.get("SEC_UPDATE_RESULT")
    if p:
        return p
    return os.path.join(os.environ.get("RUNNER_TEMP") or tempfile.gettempdir(), RESULT_FILENAME)


def ticker_paths(ticker: str) -> list[str]:
    """1銘柄の出力のパス（リポジトリのルートからの相対、/区切り）。"""
    t = ticker.upper()
    return [
        f"common/sec_data/data/{t}",
        f"common/sec_data/normalized/{t}_quarterly_normalized.json",
        f"common/sec_data/ttm/{t}_ttm_series.json",
    ]


def _git(args: list[str], cwd: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, encoding="utf-8")


def rollback_ticker(ticker: str, repo: str) -> list[str]:
    """1銘柄の出力をHEADの状態に戻し、行った操作の説明の一覧を返す。gitの失敗はRuntimeError。"""
    done = []
    for path in ticker_paths(ticker):
        tracked = _git(["ls-files", "--", path], repo)
        if tracked.returncode != 0:
            raise RuntimeError(f"git ls-files {path}: {tracked.stderr.strip()}")
        if tracked.stdout.strip():
            r = _git(["checkout", "HEAD", "--", path], repo)
            if r.returncode != 0:
                raise RuntimeError(f"git checkout HEAD -- {path}: {r.stderr.strip()}")
            done.append(f"checkout {path}")
        # HEADに無いファイル（.gitignoreの対象は残す）
        r = _git(["clean", "-fdq", "--", path], repo)
        if r.returncode != 0:
            raise RuntimeError(f"git clean {path}: {r.stderr.strip()}")
        if os.path.exists(os.path.join(repo, path)) and not tracked.stdout.strip():
            raise RuntimeError(f"{path}: 消せなかった")
        if not tracked.stdout.strip():
            done.append(f"clean {path}")
    return done


def main(argv: list[str] | None = None, repo: str | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    path = argv[0] if argv else default_result_path()
    repo = repo or os.getcwd()
    try:
        with open(path, encoding="utf-8") as f:
            result = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"❌ 失敗の一覧を読めない（{path}）: {e}")
        return 1
    failed = result.get("failed") or []
    if not failed:
        print("失敗した銘柄なし（戻すものはない）")
        return 0
    rc = 0
    for f in failed:
        t = f.get("ticker")
        try:
            ops = rollback_ticker(t, repo)
            print(f"{t}（{f.get('stage')}）: 前回の状態に戻した — " + ("; ".join(ops) or "変更なし"))
        except RuntimeError as e:
            print(f"❌ {t}: 戻せない — {e}")
            rc = 1
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        try:
            with open(summary, "a", encoding="utf-8") as sf:
                sf.write(f"\n失敗した銘柄の出力を前回の状態に戻した: {', '.join(f.get('ticker') for f in failed)}"
                         + ("（一部失敗、ログを参照）" if rc else "") + "\n")
        except OSError:
            pass
    return rc


if __name__ == "__main__":
    sys.exit(main())
