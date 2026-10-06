"""
tests/test_system_health_workflow_monitor.py

common/system_health.py の check J（ワークフローの実行状況チェック）の回帰テスト。

- [[DATA-FRESHNESS-MONITORING-FUTURE-IDEA-1]]（2026-08-30）: 失敗・長期未実行を検知する
  （SEC_Data_Updateの週次実行が2回連続失敗し3週間誰も気づかなかった事態の再発防止）
- 2026-10-07 書き直し:
  - [[SYSHEALTH-CRONRUNS-GUARD-CANCELLED-1]]: Market Data Dailyのガードによる正常な取り消し（cancelled）を失敗と数えない。
    想定間隔の期間に成功が1本以上あれば正常。一覧は`created>=`で取り、`status=completed`で絞らない
    （絞り込みの結果が古く、MACRO_PULSEで09-24の実行が返った。実際の最新は10-06）
  - [[EXTERNAL-TRIGGER-DOWNSTREAM-UNCHECKED-1]]: workflow_runで起動する下流（cronの無いMarket Pulse等）も監視し、
    10-05のMarket Pulseの失敗（同じ期間に10-03の成功あり）を🔴にする
  - 失敗の前の12時間以内に成功があれば⚠️（取得済みの後の重複起動・保険の起動の失敗）。CRITICALは🔴だけ
  - Discordの1行の通知でJ・Kを切らない

実行方法:
    python -m pytest tests/test_system_health_workflow_monitor.py -v
"""

import os
import sys
from datetime import date, datetime, timedelta, timezone

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common import system_health as sh  # noqa: E402


# ── cron頻度推定 ──────────────────────────────────────────────────────
def test_parse_cron_threshold_daily():
    assert sh._parse_cron_threshold_days("15 22 * * *") == ("毎日", 3)


def test_parse_cron_threshold_weekdays():
    assert sh._parse_cron_threshold_days("25 21 * * 1-5") == ("週数回", 4)


def test_parse_cron_threshold_weekly_single_day():
    # SEC_Data_Update.yml実物のcron式（日曜のみ）
    assert sh._parse_cron_threshold_days("0 12 * * 0") == ("週次", 10)


def test_parse_cron_threshold_monthly_dom_restricted():
    # Beta_Config_Update.yml実物のcron式（1〜7日、日曜だけ続ける）
    assert sh._parse_cron_threshold_days("0 23 1-7 * *") == ("月次", 40)


# ── 監視するワークフロー（実際のYAMLから） ───────────────────────────
@pytest.fixture(scope="module")
def monitored():
    return {f: (label, days) for f, label, days in sh._discover_monitored_workflows(sh._load_workflow_defs())}


def test_discover_includes_cron_workflows(monitored):
    assert monitored["SEC_Data_Update.yml"] == ("週次", 10)
    assert monitored["Market_Data_Daily_Update.yml"][1] == 4
    assert monitored["MACRO_PULSE_Update.yml"][1] == 3     # 4本のcronのうち最短（毎日22:15）
    assert monitored["Beta_Config_Update.yml"][1] == 40


def test_discover_includes_workflow_run_downstream_with_inherited_threshold(monitored):
    """cronの無い下流（2026-10-03に金曜のcronを削除した3本）も対象で、起動元の閾値を継ぐ。"""
    assert monitored["Market_Pulse_Update.yml"] == ("連鎖（Market_Data_Daily_Update.yml）", 4)
    assert monitored["Stonks_Silo_Update.yml"][1] == 4          # SEC（10日）とMarket Data Daily（4日）の短い方
    assert monitored["TANUKI_VALUATION_Update.yml"][1] == 4     # Stonks Silo経由で4日
    assert monitored["SEC_Data_Audit.yml"] == ("連鎖（SEC_Data_Update.yml）", 10)


def test_discover_excludes_self_and_manual_only(monitored):
    assert "System_Health.yml" not in monitored
    for f in ("TANUKI_CIK_Lookup.yml", "TANUKI_Segment_AI.yml", "TANUKI_TAIL_Position_Write.yml", "Discover_Config_Sync.yml"):
        assert f not in monitored


# ── 実行の一覧からの判定 ─────────────────────────────────────────────
def _run(conclusion, when, status="completed"):
    return {"conclusion": conclusion, "status": status, "created_at": when}


def test_guard_cancelled_runs_are_not_failures():
    """2026-10-06（米国10-06の足）: 20:25の取得が成功、20:55・21:25はガードで取り消し。以前は直近1件のcancelledで🔴だった。"""
    runs = [_run("cancelled", "2026-10-06T21:25:42Z"), _run("cancelled", "2026-10-06T20:55:39Z"),
            _run("success", "2026-10-06T20:25:44Z")]
    assert sh._judge_runs(runs) == ("ok", "")


def test_only_cancelled_runs_means_no_success():
    runs = [_run("cancelled", "2026-10-06T21:25:42Z"), _run("cancelled", "2026-10-05T21:25:42Z")]
    assert sh._judge_runs(runs)[0] == "no_success"


def test_only_skipped_downstream_runs_means_no_success():
    runs = [_run("skipped", "2026-10-06T21:28:35Z"), _run("skipped", "2026-10-06T20:58:35Z")]
    assert sh._judge_runs(runs)[0] == "no_success"


def test_market_pulse_failure_on_10_05_is_red_despite_earlier_success():
    """10-05のMarket Pulse（ランナー未割り当てでfailure）。期間内に10-03の成功はあるが、失敗の前12時間に成功は無い。"""
    runs = [_run("skipped", "2026-10-05T21:28:35Z"), _run("failure", "2026-10-05T20:28:54Z"),
            _run("success", "2026-10-03T02:09:03Z")]
    state, info = sh._judge_runs(runs)
    assert state == "failed"
    assert "failure 10-05 20:28 UTC" in info


def test_failure_after_same_evening_success_is_warning():
    """10-05のMarket Data Daily: 20:25の取得が成功、20:55の外部起動がランナー未割り当てでfailure、21:25は取り消し。"""
    runs = [_run("cancelled", "2026-10-05T21:25:40Z"), _run("failure", "2026-10-05T20:55:40Z"),
            _run("success", "2026-10-05T20:25:47Z")]
    state, info = sh._judge_runs(runs)
    assert state == "covered"
    assert "直前の成功 10-05 20:25" in info


def test_insurance_run_failure_next_day_after_success_is_warning():
    """20:25 UTCの成功の後、翌日01:47 UTCの保険の起動が失敗（UTCの日付は違うが5時間22分前に成功）→ ⚠️。"""
    runs = [_run("failure", "2026-10-07T01:47:10Z"), _run("cancelled", "2026-10-06T21:25:42Z"),
            _run("success", "2026-10-06T20:25:44Z")]
    state, info = sh._judge_runs(runs)
    assert state == "covered"
    assert "failure 10-07 01:47 UTC" in info and "直前の成功 10-06 20:25" in info


def test_failure_with_success_more_than_12_hours_before_is_red():
    runs = [_run("failure", "2026-10-07T09:30:00Z"), _run("success", "2026-10-06T20:25:44Z")]
    assert sh._judge_runs(runs)[0] == "failed"


def test_failure_followed_by_success_is_ok():
    runs = [_run("success", "2026-10-06T20:25:44Z"), _run("failure", "2026-10-05T20:55:40Z")]
    assert sh._judge_runs(runs) == ("ok", "")


def test_in_progress_runs_are_ignored():
    runs = [_run(None, "2026-10-06T22:00:00Z", status="in_progress"), _run("success", "2026-10-06T20:25:44Z")]
    assert sh._judge_runs(runs) == ("ok", "")


# ── check_j_workflow_runs ────────────────────────────────────────────
def _patch_monitored(monkeypatch, items):
    monkeypatch.setattr(sh, "_get_repo_slug", lambda: "Koichi-Shigihara2/On-a-journey")
    monkeypatch.setattr(sh, "_load_workflow_defs", lambda: [])
    monkeypatch.setattr(sh, "_discover_monitored_workflows", lambda wfs: items)


def test_check_j_red_and_critical_on_failure(monkeypatch):
    _patch_monitored(monkeypatch, [("Market_Pulse_Update.yml", "連鎖（Market_Data_Daily_Update.yml）", 4),
                                   ("Market_Data_Daily_Update.yml", "週数回", 4)])
    runs = {"Market_Pulse_Update.yml": [_run("failure", "2026-10-05T20:28:54Z"), _run("success", "2026-10-03T02:09:03Z")],
            "Market_Data_Daily_Update.yml": [_run("cancelled", "2026-10-05T21:25:40Z"), _run("success", "2026-10-05T20:25:47Z")]}
    monkeypatch.setattr(sh, "_fetch_runs_since", lambda slug, f, since: runs[f])
    r = sh._check_j()
    assert r["critical"] is True and r["ok"] is False
    assert r["label"].startswith("🔴")
    assert "失敗1件: Market_Pulse_Update.yml(failure 10-05 20:28 UTC)" in r["detail"]
    assert "Market_Data_Daily_Update.yml" not in r["detail"]


def test_check_j_warning_only_is_not_critical(monkeypatch):
    _patch_monitored(monkeypatch, [("Market_Data_Daily_Update.yml", "週数回", 4)])
    monkeypatch.setattr(sh, "_fetch_runs_since", lambda slug, f, since: [
        _run("failure", "2026-10-07T01:47:10Z"), _run("success", "2026-10-06T20:25:44Z")])
    r = sh._check_j()
    assert r["critical"] is False and r["ok"] is False
    assert r["label"].startswith("⚠️")
    assert "失敗（直前に成功あり）1件: Market_Data_Daily_Update.yml" in r["detail"]


def test_check_j_no_success_in_window_is_critical(monkeypatch):
    """SEC_Data_Updateの3週間の滞留を模擬（期間10日に成功なし）。"""
    _patch_monitored(monkeypatch, [("SEC_Data_Update.yml", "週次", 10)])
    monkeypatch.setattr(sh, "_fetch_runs_since", lambda slug, f, since: [])
    r = sh._check_j()
    assert r["critical"] is True
    assert "成功なし1件: SEC_Data_Update.yml(10日間に成功なし/週次)" in r["detail"]


def test_check_j_healthy(monkeypatch):
    _patch_monitored(monkeypatch, [("Score_Verifier.yml", "毎日", 3)])
    monkeypatch.setattr(sh, "_fetch_runs_since", lambda slug, f, since: [_run("success", "2026-10-06T03:26:08Z")])
    label, ok, detail = sh.check_j_workflow_runs()
    assert ok is True and label.startswith("✅")
    assert detail == "1件監視 / すべて正常"


def test_check_j_window_starts_threshold_days_before_today_utc(monkeypatch):
    _patch_monitored(monkeypatch, [("SEC_Data_Update.yml", "週次", 10)])
    seen = []
    monkeypatch.setattr(sh, "_fetch_runs_since", lambda slug, f, since: seen.append(since) or [_run("success", "2026-10-04T16:00:32Z")])
    sh._check_j()
    assert seen == [datetime.now(timezone.utc).date() - timedelta(days=10)]


def test_check_j_api_unreachable_is_not_treated_as_failure(monkeypatch):
    """API到達不可（レート制限・ネットワーク不調等）は異常扱いしない設計。"""
    _patch_monitored(monkeypatch, [("SEC_Data_Update.yml", "週次", 10)])

    def raise_error(slug, f, since):
        raise OSError("network unreachable")

    monkeypatch.setattr(sh, "_fetch_runs_since", raise_error)
    label, ok, detail = sh.check_j_workflow_runs()
    assert ok is True
    assert "確認不可" in detail


def test_check_j_no_repo_slug_skips_gracefully(monkeypatch):
    monkeypatch.setattr(sh, "_get_repo_slug", lambda: None)
    label, ok, detail = sh.check_j_workflow_runs()
    assert ok is True


# ── 一覧の取り方（status=completedで絞らない） ───────────────────────
class _Resp:
    def __init__(self, body):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_fetch_runs_uses_created_filter_not_status(monkeypatch):
    import json
    import urllib.request
    urls = []

    def fake_urlopen(req, timeout=0):
        urls.append(req.full_url)
        return _Resp(json.dumps({"total_count": 1, "workflow_runs": [_run("success", "2026-10-06T18:52:05Z")]}).encode())

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    runs = sh._fetch_runs_since("o/r", "MACRO_PULSE_Update.yml", date(2026, 10, 4))
    assert len(runs) == 1
    assert len(urls) == 1
    assert "status=" not in urls[0]
    assert "created=%3E%3D2026-10-04" in urls[0]


def test_fetch_runs_follows_pages(monkeypatch):
    import json
    import urllib.request
    pages = [[_run("cancelled", "2026-10-06T21:25:42Z")] * 100, [_run("success", "2026-10-03T02:09:03Z")]]

    def fake_urlopen(req, timeout=0):
        page = int(req.full_url.rsplit("page=", 1)[1])
        return _Resp(json.dumps({"total_count": 101, "workflow_runs": pages[page - 1]}).encode())

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    runs = sh._fetch_runs_since("o/r", "Market_Data_Daily_Update.yml", date(2026, 10, 3))
    assert len(runs) == 101
    assert sh._judge_runs(runs) == ("ok", "")


# ── main(): 終了コードと1行の通知 ───────────────────────────────────
def _patch_main(monkeypatch, j, k_ok=True, k_detail="すべて正常"):
    ok3 = lambda *a: ("✅", True, "OK")  # noqa: E731
    for name in ("check_a_sec", "check_b_score_history", "check_c_latest"):
        monkeypatch.setattr(sh, name, lambda tickers: ("✅", True, "0件"))
    for name in ("check_d_actions", "check_e_silo", "check_f_tail", "check_g_hypecore", "check_h_config",
                 "check_i_eps", "check_l_macro_data"):
        monkeypatch.setattr(sh, name, ok3)
    monkeypatch.setattr(sh, "check_k_ticker_audit", lambda: ("⚠️", k_ok, k_detail))
    monkeypatch.setattr(sh, "_check_j", lambda: j)
    monkeypatch.setattr(sh, "get_registered_tickers", lambda: [])
    sent = []
    monkeypatch.setattr(sh, "post_discord", lambda text: sent.append(text) or False)
    monkeypatch.setattr(sys, "argv", ["system_health.py", "--quiet"])
    return sent


def test_main_one_line_shows_j_and_k_in_full(monkeypatch):
    j_detail = ("17件監視 / 失敗1件: Market_Pulse_Update.yml(failure 10-05 20:28 UTC) / "
                "失敗（直前に成功あり）1件: Market_Data_Daily_Update.yml(failure 10-05 20:55 UTC、直前の成功 10-05 20:25)")
    k_detail = "①見直し候補(candidate>30日)4件: APGE, CON, SN, WST / ②検証由来・無保有4件: APGE, CON, SN, WST"
    sent = _patch_main(monkeypatch, {"label": "🔴 " + j_detail, "ok": False, "detail": j_detail, "critical": True},
                       k_ok=False, k_detail=k_detail)
    assert sh.main() == 2
    assert j_detail in sent[0]
    assert k_detail in sent[0]


def test_main_warning_only_j_exits_one(monkeypatch):
    d = "17件監視 / 失敗（直前に成功あり）1件: Market_Data_Daily_Update.yml(failure 10-07 01:47 UTC、直前の成功 10-06 20:25)"
    _patch_main(monkeypatch, {"label": "⚠️  " + d, "ok": False, "detail": d, "critical": False})
    assert sh.main() == 1
