"""
common/system_health.py
全システム健全性チェック + 毎朝Discord通知

使用方法:
    python common/system_health.py         # 全チェック実行
    python common/system_health.py --quiet # Discord通知のみ（コンソール省略）

終了コード:
    0 = HEALTHY（問題なし）
    1 = WARNING（軽微な問題あり）
    2 = CRITICAL（重大問題あり）
"""

import argparse
import glob
import json
import math
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from typing import Optional

# repo root を sys.path に追加（common/ から見て1段上）
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data.audit import (
    audit_beta_drift,
    get_registered_tickers,
    post_discord,
    run_audit,
)
from common.sec_data import tickers as _tickers_mod
# [[QUALITY-GATES-EPIC-1]]ゲート4（旧TICKER-AUDIT-1想定機能⑤）: P4-CIKOrphan
# チェック・monitor_tickers.yaml読み込みロジックを再利用する（2026-09-05）。
# 独立に再実装せず、既存の唯一の実装を再利用することでロジックの重複・
# 乖離を防ぐ。
from common.sec_data.registration_validator import (
    Issues as _RegIssues,
    check_p4_orphan_configs as _check_p4_orphan_configs,
    _load_monitor_tickers,
)

_TANUKI_DATA = os.path.join(_REPO_ROOT, "docs", "value-monitor", "tanuki_valuation", "data")
_DOCS_ROOT   = os.path.join(_REPO_ROOT, "docs")
_SS_RESULTS  = os.path.join(_DOCS_ROOT, "value-monitor", "stonks-silo", "data", "results.json")
_WORKFLOWS_DIR = os.path.join(_REPO_ROOT, ".github", "workflows")
_PORTFOLIO_JSON = os.path.join(_DOCS_ROOT, "portfolio", "data", "portfolio.json")
_MACRO_DATA_VIOLATIONS_LOG = os.path.join(_REPO_ROOT, "common", "macro_data", "macro_data_violations_log.json")

_STALE_DAYS  = 7   # この日数以上古いデータを「stale」と判定
_GH_API_BASE = "https://api.github.com"

# ── check_k_ticker_audit()用の定数 ────────────────────────────────────
# ①: status=candidateのまま放置とみなす閾値（日）。旧TICKER-AUDIT-1原案の
# 「status=test」は現行cik_lookup.csvに存在せず、candidateが唯一の
# test相当の値（tickers.py _INVALID_STATUSESのコメント参照）。2026-09-05確認
_STALE_CANDIDATE_DAYS = 30
# ②: 検証由来のregistration_source値。旧TICKER-AUDIT-1原案の
# 「moomoo_screening」は現行データに存在しない列挙値のため、実際に
# 使われている値（technical_screening）に合わせた。2026-09-05確認
_SCREENING_REGISTRATION_SOURCES = {"technical_screening"}


# ── git log でファイルの最終コミット日時を取得 ──────────────────────
def _git_last_commit_date(rel_path: str) -> Optional[date]:
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%ci", "--", rel_path],
            capture_output=True, text=True, cwd=_REPO_ROOT, timeout=10,
        )
        line = result.stdout.strip()
        if not line:
            return None
        return datetime.fromisoformat(line[:19]).date()
    except Exception:
        return None


# ── A. SEC データ健全性 ──────────────────────────────────────────────
def check_a_sec(tickers: list[str]) -> tuple[str, bool, str]:
    critical, warning = run_audit(tickers)
    n_crit   = len(critical)
    n_warn   = len(warning)
    ok       = n_crit == 0
    icon     = "✅" if ok else "⚠️ " if n_warn and not n_crit else "🔴"
    detail   = f"警告{n_warn}件 / 重大{n_crit}件"
    label    = f"{icon} {detail}"
    return label, ok, detail


# ── B. score_history.json 整合性 ─────────────────────────────────────
def check_b_score_history(tickers: list[str]) -> tuple[str, bool, str]:
    missing, stale = [], []
    today = date.today()
    for t in tickers:
        sh_path = os.path.join(_TANUKI_DATA, t, "score_history.json")
        if not os.path.exists(sh_path):
            missing.append(t)
            continue
        try:
            entries = json.load(open(sh_path, encoding="utf-8"))
        except Exception:
            missing.append(t)
            continue
        if not entries:
            stale.append(t)
            continue
        latest = max(e.get("date", "2000-01-01") for e in entries)
        if (today - date.fromisoformat(latest)).days > _STALE_DAYS:
            stale.append(t)

    total  = len(tickers)
    n_miss = len(missing)
    n_stale= len(stale)
    ok     = n_miss == 0 and n_stale == 0
    icon   = "✅" if ok else "⚠️ "
    detail = f"{total - n_miss}/{total}件存在 / 古いデータ{n_stale}件"
    if missing:
        detail += f" (未作成: {', '.join(missing[:5])}{'…' if len(missing) > 5 else ''})"
    return f"{icon} {detail}", ok, detail


# ── C. latest.json 必須フィールド欠損 ───────────────────────────────
def check_c_latest(tickers: list[str]) -> tuple[str, bool, str]:
    broken = []
    for t in tickers:
        lp = os.path.join(_TANUKI_DATA, t, "latest.json")
        if not os.path.exists(lp):
            broken.append(t)
            continue
        try:
            d = json.load(open(lp, encoding="utf-8"))
        except Exception:
            broken.append(t)
            continue
        price = (d.get("components") or {}).get("current_price")
        if (d.get("tanuki_score") is None
                or d.get("upside_percent") is None
                or price is None):
            broken.append(t)

    n   = len(broken)
    ok  = n == 0
    icon = "✅" if ok else "⚠️ "
    detail = f"欠損{n}件" + (f"（{', '.join(broken[:5])}{'…' if n > 5 else ''}）" if broken else "")
    return f"{icon} {detail}", ok, detail


# ── D. ワークフロー出力データの鮮度チェック ─────────────────────────
def check_d_actions() -> tuple[str, bool, str]:
    # ワークフロー別にチェックするデータ出力ファイル（repo_root基準の相対パス）
    checks = [
        ("MarketPulse", "docs/market-monitor/market-pulse/data/market_data.json"),
        ("TANUKI",      "docs/value-monitor/tanuki_valuation/data/tickers.json"),
        ("StonksSilo",  "docs/value-monitor/stonks-silo/data/results.json"),
        ("MacroPulse",  "docs/market-monitor/macro-pulse/data/fed_context.csv"),
    ]
    stale = []
    today = date.today()
    for name, rel_path in checks:
        last = _git_last_commit_date(rel_path)
        if last is None:
            continue
        if (today - last).days > _STALE_DAYS:
            stale.append(f"{name}({(today - last).days}日)")

    ok     = len(stale) == 0
    icon   = "✅" if ok else "⚠️ "
    detail = "全ワークフロー正常" if ok else f"7日超 {len(stale)}件: {', '.join(stale)}"
    return f"{icon} {detail}", ok, detail


# ── E. Stonks Silo 健全性 ────────────────────────────────────────────
def check_e_silo() -> tuple[str, bool, str]:
    if not os.path.exists(_SS_RESULTS):
        return "⚠️  results.json未作成", False, "results.json未作成"
    try:
        data = json.load(open(_SS_RESULTS, encoding="utf-8"))
    except Exception as e:
        return f"🔴 読み込みエラー: {e}", False, str(e)

    ss_date  = (data.get("generated_at") or "")[:10]
    tickers  = data.get("tickers") or {}
    null_ue  = sum(
        1 for td in tickers.values()
        if (td.get("deficit_quality") or {}).get("unit_economics_score") is None
    )
    ok      = True
    icon    = "✅"
    detail  = f"更新{ss_date or '不明'} / UEスコアnull {null_ue}件"
    return f"{icon} {detail}", ok, detail


_HYPE_STALE_DAYS = 14  # HypeCore 鮮度閾値（日）
_EPS_STALE_DAYS  = 14  # EPS ANALYZER 鮮度閾値（日）
_TAIL_CTRL       = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "ctrl")
_TAIL_POSITIONS  = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "positions")
_HYPE_DATA       = os.path.join(_REPO_ROOT, "docs", "value-monitor", "hypecore", "data")
_EPS_SUMMARY     = os.path.join(_REPO_ROOT, "docs", "value-monitor", "adjusted_eps_analyzer", "data", "summary.json")
_BETA_CONFIG     = os.path.join(_REPO_ROOT, "config", "beta_config.json")
_SEGMENT_CONFIG  = os.path.join(_REPO_ROOT, "config", "segment_config.json")
_MATURITY_CONFIG = os.path.join(_REPO_ROOT, "config", "maturity_config.json")


# ── F. TANUKI TAIL ctrl データ存在確認 ──────────────────────────────
def check_f_tail() -> tuple[str, bool, str]:
    if not os.path.exists(_TAIL_POSITIONS):
        return "⚠️  positions ディレクトリ未作成", False, "positions dir missing"

    thesis_tickers = [
        f.replace("_thesis.json", "")
        for f in os.listdir(_TAIL_POSITIONS)
        if f.endswith("_thesis.json")
    ]

    missing_ctrl = [
        t for t in thesis_tickers
        if not os.path.exists(os.path.join(_TAIL_CTRL, t, "latest.json"))
    ]

    total  = len(thesis_tickers)
    n_miss = len(missing_ctrl)
    ok     = n_miss == 0
    icon   = "✅" if ok else "⚠️ "
    detail = f"{total - n_miss}/{total}件 ctrl/latest.json 存在"
    if missing_ctrl:
        detail += f" (不足: {', '.join(missing_ctrl)})"
    return f"{icon} {detail}", ok, detail


# ── G. HypeCore poc.json 健全性・鮮度 ────────────────────────────────
def check_g_hypecore() -> tuple[str, bool, str]:
    poc_files = glob.glob(os.path.join(_HYPE_DATA, "*_poc.json"))
    if not poc_files:
        return "⚠️  poc.jsonなし", False, "poc.json not found"

    inf_tickers: list[str] = []
    newest_date: date | None = None
    today = date.today()

    for f in poc_files:
        ticker = os.path.basename(f).replace("_poc.json", "")
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:
            continue

        gen_at = (d.get("generated_at") or "")[:10]
        if gen_at:
            try:
                gd = date.fromisoformat(gen_at)
                if newest_date is None or gd > newest_date:
                    newest_date = gd
            except ValueError:
                pass

        for entry in d.get("monthly", []):
            if ticker in inf_tickers:
                break
            for v in entry.values():
                if isinstance(v, float) and (math.isinf(v) or math.isnan(v)):
                    inf_tickers.append(ticker)
                    break

    stale_days = (today - newest_date).days if newest_date else None
    stale = stale_days is not None and stale_days > _HYPE_STALE_DAYS
    n_inf = len(inf_tickers)

    ok   = n_inf == 0 and not stale
    icon = "✅" if ok else "⚠️ "
    parts = [f"最終更新: {newest_date or '不明'}({stale_days if stale_days is not None else '?'}日経過)"]
    if n_inf:
        parts.append(f"Inf混入: {', '.join(inf_tickers[:3])}{'…' if n_inf > 3 else ''}")
    if stale:
        parts.append(f"{stale_days}日更新なし(閾値{_HYPE_STALE_DAYS}日)")
    detail = " / ".join(parts)
    return f"{icon} {detail}", ok, detail


# ── H. config 整合性チェック ──────────────────────────────────────────
def check_h_config() -> tuple[str, bool, str]:
    # all_tickersはsegment/maturity configの孤立エントリ検出のため意図的に
    # フラグ無視の全登録銘柄（監査目的）。tanuki_tickersはtickers.py経由に統一
    # （ZS-TICKERS-LEAK-1: 独自にCSVを読んでフラグ判定する重複実装を解消）。
    # all_tickers取得も[[TICKER-LOADING-UNIFICATION-1]]でtickers.py経由に
    # 統一（2026-09-05、旧csv.DictReader直接パースを置換。意味は完全に
    # 同一＝フラグ無視の全登録銘柄）。
    all_tickers    = set(_tickers_mod.get_all_tickers())
    tanuki_tickers = _tickers_mod.get_tanuki_tickers()

    beta_data      = json.load(open(_BETA_CONFIG, encoding="utf-8"))
    beta_overrides = set(beta_data.get("overrides", {}).keys())

    segment_data  = json.load(open(_SEGMENT_CONFIG, encoding="utf-8"))
    segment_ticks = [k for k in segment_data if not k.startswith("_")]

    maturity_data  = json.load(open(_MATURITY_CONFIG, encoding="utf-8"))
    maturity_ticks = [k for k in maturity_data if not k.startswith("_")]

    missing_beta  = [t for t in tanuki_tickers if t not in beta_overrides]
    orphaned_seg  = [t for t in segment_ticks if t not in all_tickers]
    orphaned_mat  = [t for t in maturity_ticks if t not in all_tickers]

    ok   = not missing_beta and not orphaned_seg and not orphaned_mat
    icon = "✅" if ok else "⚠️ "
    parts: list[str] = []
    if missing_beta:
        parts.append(f"beta未登録({len(missing_beta)}件): {', '.join(missing_beta[:3])}{'…' if len(missing_beta) > 3 else ''}")
    if orphaned_seg:
        parts.append(f"segment孤立: {', '.join(orphaned_seg)}")
    if orphaned_mat:
        parts.append(f"maturity孤立: {', '.join(orphaned_mat)}")
    detail = "整合OK" if ok else " / ".join(parts)
    return f"{icon} {detail}", ok, detail


# ── I. EPS ANALYZER summary.json 鮮度・カバレッジ ────────────────────
def check_i_eps() -> tuple[str, bool, str]:
    if not os.path.exists(_EPS_SUMMARY):
        return "⚠️  summary.json未作成", False, "summary.json not found"

    try:
        data = json.load(open(_EPS_SUMMARY, encoding="utf-8"))
    except Exception as e:
        return f"🔴 読み込みエラー: {e}", False, str(e)

    last_updated = (data.get("last_updated") or "")[:10]
    tickers_list = data.get("tickers", [])
    today = date.today()

    stale_days: int | None = None
    stale = False
    if last_updated:
        try:
            upd_date = date.fromisoformat(last_updated)
            stale_days = (today - upd_date).days
            stale = stale_days > _EPS_STALE_DAYS
        except ValueError:
            pass

    # eps=true のティッカーが summary.json に含まれているかカバレッジ確認
    # （ZS-TICKERS-LEAK-1: tickers.py経由に統一）
    eps_tickers    = set(_tickers_mod.get_eps_tickers())
    summary_tickers= {e["ticker"] for e in tickers_list if isinstance(e, dict)}
    missing_eps    = sorted(eps_tickers - summary_tickers)

    ok   = not stale and not missing_eps
    icon = "✅" if ok else "⚠️ "
    parts = [f"更新{last_updated or '不明'}({stale_days if stale_days is not None else '?'}日経過)"]
    if stale:
        parts.append(f"{stale_days}日更新なし(閾値{_EPS_STALE_DAYS}日)")
    if missing_eps:
        parts.append(f"未収録({len(missing_eps)}件): {', '.join(missing_eps[:3])}{'…' if len(missing_eps) > 3 else ''}")
    detail = " / ".join(parts)
    return f"{icon} {detail}", ok, detail


# ── J. cron定義ワークフローの実行状況チェック ────────────────────────
# [[DATA-FRESHNESS-MONITORING-FUTURE-IDEA-1]]対応（2026-08-30）。
# SEC_Data_Updateが2週連続でConsistency Check Gateに失敗し、約3週間
# データ更新が誰にも気づかれず滞留した実例を受けて追加。
#
# .github/workflows/配下のcron定義済みワークフローすべてについて、
# GitHub Actions REST APIで直近の完了済み実行を取得し、
# (1) 失敗（conclusion != success）していないか
# (2) cronの想定間隔を大きく超えて未実行になっていないか
# を確認する。外部通知サービスは使わず、既存のDiscord Webhook
# （このSystem_Health.yml自体が元々使っている通知経路）とワークフロー
# 自体の終了コード（異常時は非ゼロ→GitHub Actions上でRED表示）のみで
# 完結させる。
#
# 頻度推定はcron式の day-of-month / day-of-week フィールドを見る簡易
# ヒューリスティックであり、厳密なcronパーサではない（本用途では
# 「毎日/週数回/毎週/毎月」の粗い分類で十分なため、新規ライブラリは
# 追加していない）。
def _parse_cron_threshold_days(cron_expr: str) -> tuple[str, int]:
    """cron式から (頻度ラベル, 許容日数（この日数を超えたら stale）) を推定する。"""
    fields = cron_expr.split()
    if len(fields) != 5:
        return "不明", 3
    _, _, dom, _, dow = fields
    if dom != "*":
        return "月次", 40
    if dow == "*":
        return "毎日", 3
    days: set[int] = set()
    for part in dow.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-")
            days.update(range(int(a), int(b) + 1))
        elif part.isdigit():
            days.add(int(part))
    if len(days) >= 2:
        return "週数回", 4
    return "週次", 10


def _discover_cron_workflows() -> list[tuple[str, str]]:
    """.github/workflows/配下からcron定義済みワークフロー(ファイル名, cron式)を列挙する。"""
    found: list[tuple[str, str]] = []
    if not os.path.isdir(_WORKFLOWS_DIR):
        return found
    for fname in sorted(os.listdir(_WORKFLOWS_DIR)):
        if not fname.endswith((".yml", ".yaml")):
            continue
        path = os.path.join(_WORKFLOWS_DIR, fname)
        try:
            with open(path, encoding="utf-8") as f:
                content = f.read()
        except OSError:
            continue
        m = re.search(r"cron:\s*['\"]([^'\"]+)['\"]", content)
        if m:
            found.append((fname, m.group(1)))
    return found


def _get_repo_slug() -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True, text=True, cwd=_REPO_ROOT, timeout=10,
        )
        url = result.stdout.strip()
        m = re.search(r"github\.com[:/]([^/]+/[^/.]+?)(?:\.git)?$", url)
        return m.group(1) if m else None
    except Exception:
        return None


def _fetch_latest_run(repo_slug: str, workflow_file: str) -> Optional[dict]:
    """指定ワークフローの直近の完了済み実行1件をGitHub Actions REST APIから取得する。
    GITHUB_TOKEN環境変数があれば認証付きで（レート制限緩和）、なければ匿名で呼ぶ。"""
    import urllib.request

    url = (f"{_GH_API_BASE}/repos/{repo_slug}/actions/workflows/"
           f"{workflow_file}/runs?per_page=1&status=completed")
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "On-a-journey-system-health",
    }
    token = os.environ.get("GITHUB_TOKEN", "")
    if token:
        headers["Authorization"] = f"token {token}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    runs = data.get("workflow_runs") or []
    return runs[0] if runs else None


_SELF_WORKFLOW_FILE = "System_Health.yml"  # 実行環境調査で判明: 本ワークフローは
# check F/G等のWARNINGでもexit 1になり毎日のようにrun自体がfailure表示に
# なる設計（意図的な「REDで通知」挙動）のため、これを他ワークフローと
# 同列の「失敗」として監視対象に含めるとcheck Jが常時RED化し信号として
# 機能しなくなる。自己を監視対象から除外する（自身が完全に動かなく
# なった場合の検知はこの仕組みの原理的な限界として残る）。


def check_j_workflow_runs() -> tuple[str, bool, str]:
    cron_workflows = [
        (f, c) for f, c in _discover_cron_workflows() if f != _SELF_WORKFLOW_FILE
    ]
    if not cron_workflows:
        return "⚠️  cronワークフロー未検出", True, "no cron workflows found"

    repo_slug = _get_repo_slug()
    if not repo_slug:
        return "⚠️  リポジトリ特定不可（スキップ）", True, "repo slug not found"

    failed: list[str] = []
    stale: list[str] = []
    unchecked: list[str] = []
    today = date.today()

    for fname, cron_expr in cron_workflows:
        freq_label, threshold_days = _parse_cron_threshold_days(cron_expr)
        try:
            run = _fetch_latest_run(repo_slug, fname)
        except Exception:
            unchecked.append(fname)
            continue
        if run is None:
            unchecked.append(fname)
            continue

        conclusion = run.get("conclusion")
        if conclusion not in ("success", "skipped"):
            failed.append(f"{fname}({conclusion})")
            continue

        created_at = (run.get("created_at") or "")[:10]
        try:
            run_date = date.fromisoformat(created_at)
        except ValueError:
            unchecked.append(fname)
            continue
        age = (today - run_date).days
        if age > threshold_days:
            stale.append(f"{fname}({age}日/{freq_label}閾値{threshold_days}日)")

    ok   = not failed and not stale
    icon = "🔴" if failed else ("⚠️ " if stale else "✅")
    parts = [f"{len(cron_workflows)}件監視"]
    if failed:
        parts.append(f"失敗{len(failed)}件: {', '.join(failed[:3])}{'…' if len(failed) > 3 else ''}")
    if stale:
        parts.append(f"未実行超過{len(stale)}件: {', '.join(stale[:3])}{'…' if len(stale) > 3 else ''}")
    if unchecked:
        parts.append(f"確認不可{len(unchecked)}件（API到達不可等、異常扱いしない）")
    if not failed and not stale and not unchecked:
        parts.append("すべて正常")
    detail = " / ".join(parts)
    return f"{icon} {detail}", ok, detail


# ── K. 銘柄棚卸しレポート（[[QUALITY-GATES-EPIC-1]]ゲート4） ───────────
def _portfolio_tickers() -> set[str]:
    """docs/portfolio/data/portfolio.jsonの全ブローカー・全ポジションから
    保有中の銘柄集合を返す（ファイル不在・パースエラー時は空集合）。"""
    try:
        with open(_PORTFOLIO_JSON, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return set()
    result: set[str] = set()
    for broker in (data or {}).get("brokers", {}).values():
        result.update(broker.get("positions", {}).keys())
    return result


def check_k_ticker_audit() -> tuple[str, bool, str]:
    """[[QUALITY-GATES-EPIC-1]]ゲート4（付録「元[TICKER-AUDIT-1]」の
    想定機能①②⑤⑥）: 銘柄棚卸しレポート。判断は自動化せず、候補一覧の
    可視化のみを行う（想定機能④: status変更・retired化等は一切行わない）。

    ①status=candidate（現行cik_lookup.csvにおける唯一のtest相当値）かつ
      登録から`_STALE_CANDIDATE_DAYS`超の銘柄を「見直し候補」として一覧化
    ②registration_source=technical_screening（現行データでの検証由来値）
      かつportfolio.jsonに保有記載のない銘柄を抽出
    ⑤registration_validator.py::check_p4_orphan_configs()のP4-CIKOrphan
      チェック結果を集約する（WARNレベルのため運用上見落とされてきた
      経緯があるため、他のチェックと同じ目立つ形式で出力する）
    ⑥monitor_tickers.yamlとcik_lookup.csvの件数差・銘柄差分を検出する
      （[[TICKER-SOURCE-UNIFY-1]]で根本原因は解消済みだが、安全網として残す）

    想定機能③（実装場所）は本関数自体がその決定の実装。
    """
    rows  = _tickers_mod.get_all_rows()
    today = date.today()

    # ①
    stale_candidates: list[str] = []
    for r in rows:
        if r.get("status", "").strip().lower() != "candidate":
            continue
        reg_date_str = r.get("registered_date", "").strip()
        if not reg_date_str:
            continue
        try:
            reg_date = date.fromisoformat(reg_date_str)
        except ValueError:
            continue
        if (today - reg_date).days > _STALE_CANDIDATE_DAYS:
            stale_candidates.append(r["ticker"])
    stale_candidates.sort()

    # ②
    held = _portfolio_tickers()
    screening_no_position: list[str] = []
    for r in rows:
        if r.get("registration_source", "").strip() not in _SCREENING_REGISTRATION_SOURCES:
            continue
        if r["ticker"] not in held:
            screening_no_position.append(r["ticker"])
    screening_no_position.sort()

    # ⑤
    reg_issues  = _RegIssues()
    monitor_set = set(_load_monitor_tickers())
    _check_p4_orphan_configs(reg_issues, monitor_set)
    orphan_warnings = [msg for sev, cat, msg in reg_issues.all() if cat == "P4-CIKOrphan"]

    # ⑥
    cik_set       = {r["ticker"] for r in rows}
    only_in_cik     = sorted(cik_set - monitor_set)
    only_in_monitor = sorted(monitor_set - cik_set)

    ok = not (stale_candidates or screening_no_position or orphan_warnings
              or only_in_cik or only_in_monitor)
    icon = "✅" if ok else "⚠️ "
    parts: list[str] = []
    if stale_candidates:
        parts.append(
            f"①見直し候補(candidate>{_STALE_CANDIDATE_DAYS}日){len(stale_candidates)}件: "
            f"{', '.join(stale_candidates[:5])}{'…' if len(stale_candidates) > 5 else ''}"
        )
    if screening_no_position:
        parts.append(
            f"②検証由来・無保有{len(screening_no_position)}件: "
            f"{', '.join(screening_no_position[:5])}{'…' if len(screening_no_position) > 5 else ''}"
        )
    if orphan_warnings:
        parts.append(
            f"⑤P4-CIKOrphan{len(orphan_warnings)}件: "
            f"{'; '.join(orphan_warnings[:2])}{'…' if len(orphan_warnings) > 2 else ''}"
        )
    if only_in_cik or only_in_monitor:
        diff_bits = []
        if only_in_cik:
            diff_bits.append(f"cik_lookupのみ{len(only_in_cik)}件({', '.join(only_in_cik[:3])})")
        if only_in_monitor:
            diff_bits.append(f"monitor_tickersのみ{len(only_in_monitor)}件({', '.join(only_in_monitor[:3])})")
        parts.append("⑥同期漏れ: " + " / ".join(diff_bits))
    detail = "該当なし（棚卸し正常）" if ok else " / ".join(parts)
    return f"{icon} {detail}", ok, detail


# ── L. common/macro_data/ 系列単位のfetch失敗検知 ───────────────────
# [[MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1]]対応（2026-09-19）。
# fetch_all_series()は系列単位の例外・取得失敗を内部で捕捉するため
# ジョブ自体は常にsuccessで終わり、Check J（ワークフロー実行状況）では
# 検知できない。common/macro_data/fetcher.py側で追加したfetch_status
# （"ok"/"failed"）をmacro_data_violations_log.jsonから読むだけの軽量
# チェック（新規ロジックなし）。FTSDのような「常に失敗し続ける系列」を
# 放置しないための可視化が目的。WARNレベル（icon "⚠️"）に留め、
# check_j_workflow_runsの判定（CRITICAL対象）には含めない。
def check_l_macro_data() -> tuple[str, bool, str]:
    if not os.path.exists(_MACRO_DATA_VIOLATIONS_LOG):
        return "⚠️  macro_data_violations_log.json未作成（スキップ）", True, "log not found (skip)"

    try:
        log = json.load(open(_MACRO_DATA_VIOLATIONS_LOG, encoding="utf-8"))
    except Exception as e:
        return f"🔴 読み込みエラー: {e}", False, str(e)

    failed = sorted(
        series_id for series_id, entry in log.items()
        if isinstance(entry, dict) and entry.get("fetch_status") == "failed"
    )

    ok   = not failed
    icon = "✅" if ok else "⚠️ "
    detail = ("全系列取得成功" if ok else
              f"取得失敗({len(failed)}件): {', '.join(failed[:3])}{'…' if len(failed) > 3 else ''}")
    return f"{icon} {detail}", ok, detail


# ── Discord 1行サマリー ───────────────────────────────────────────────
def build_one_line(run_date: str, results: dict) -> str:
    overall_ok = all(r["ok"] for r in results.values())
    globe      = "🟢" if overall_ok else "🔴"
    status     = "HEALTHY" if overall_ok else "WARNING"
    parts = []
    labels = {
        "A": "SEC", "B": "Score", "C": "Latest", "D": "Actions", "E": "Silo",
        "F": "Tail", "G": "Hype", "H": "Config", "I": "EPS", "J": "CronRuns",
        "K": "TickerAudit", "L": "MacroData",
    }
    for key, label in labels.items():
        r = results.get(key, {})
        icon = "✅" if r.get("ok") else "⚠️"
        extra = f"{r.get('short','')}" if not r.get("ok") else ""
        parts.append(f"{label}{icon}{extra}")

    return f"{globe} System Health {run_date}: {status} | {' '.join(parts)}"


# ── メイン ───────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(description="全システム健全性チェック")
    parser.add_argument("--quiet", action="store_true", help="コンソール出力を省略")
    args = parser.parse_args()

    today    = date.today().isoformat()
    tickers  = get_registered_tickers()

    if not args.quiet:
        print(f"\n=== System Health Check {today} ===")

    # 各チェック実行
    label_a, ok_a, det_a = check_a_sec(tickers)
    label_b, ok_b, det_b = check_b_score_history(tickers)
    label_c, ok_c, det_c = check_c_latest(tickers)
    label_d, ok_d, det_d = check_d_actions()
    label_e, ok_e, det_e = check_e_silo()
    label_f, ok_f, det_f = check_f_tail()
    label_g, ok_g, det_g = check_g_hypecore()
    label_h, ok_h, det_h = check_h_config()
    label_i, ok_i, det_i = check_i_eps()
    label_j, ok_j, det_j = check_j_workflow_runs()
    label_k, ok_k, det_k = check_k_ticker_audit()
    label_l, ok_l, det_l = check_l_macro_data()

    results = {
        "A": {"ok": ok_a, "short": det_a.split("件")[0] + "件" if "件" in det_a else det_a[:10]},
        "B": {"ok": ok_b, "short": det_b.split("件")[0] + "件" if "件" in det_b else det_b[:10]},
        "C": {"ok": ok_c, "short": det_c.split("）")[0] + "）" if "）" in det_c else det_c[:10]},
        "D": {"ok": ok_d, "short": det_d[:20]},
        "E": {"ok": ok_e, "short": det_e[:20]},
        "F": {"ok": ok_f, "short": det_f[:20]},
        "G": {"ok": ok_g, "short": det_g[:20]},
        "H": {"ok": ok_h, "short": det_h[:20]},
        "I": {"ok": ok_i, "short": det_i[:20]},
        "J": {"ok": ok_j, "short": det_j[:20]},
        "K": {"ok": ok_k, "short": det_k[:20]},
        "L": {"ok": ok_l, "short": det_l[:20]},
    }

    overall_ok = all(r["ok"] for r in results.values())
    n_warn     = sum(1 for r in results.values() if not r["ok"])

    if not args.quiet:
        print(f"[A] SEC Data:      {label_a}")
        print(f"[B] ScoreHistory:  {label_b}")
        print(f"[C] Latest JSON:   {label_c}")
        print(f"[D] Actions:       {label_d}")
        print(f"[E] StonksSilo:    {label_e}")
        print(f"[F] TailCtrl:      {label_f}")
        print(f"[G] HypeCore:      {label_g}")
        print(f"[H] Config:        {label_h}")
        print(f"[I] EPS Analyzer:  {label_i}")
        print(f"[J] CronRuns:      {label_j}")
        print(f"[K] TickerAudit:   {label_k}")
        print(f"[L] MacroData:     {label_l}")
        status_str = "✅ HEALTHY" if overall_ok else f"⚠️  WARNING（問題{n_warn}件）"
        print(f"Overall: {status_str}\n")

    # Discord 通知
    one_line = build_one_line(today, results)
    sent = post_discord(one_line)
    if not args.quiet:
        if sent:
            print("Discord通知: 送信完了")
        else:
            print("Discord通知: DISCORD_WEB_HOOK 未設定またはスキップ")

    if not overall_ok:
        # SEC content異常（A）またはcronワークフロー異常（J、実行失敗・
        # 長期未実行）はデータパイプライン停止の実害に直結するためCRITICAL、
        # それ以外はWARNING
        return 2 if (not ok_a or not ok_j) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
