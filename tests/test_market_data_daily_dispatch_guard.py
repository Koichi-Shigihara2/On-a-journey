"""
tests/test_market_data_daily_dispatch_guard.py

2026-10-03（外部起動の準備）: Market_Data_Daily_Update.ymlのworkflow_dispatchに入力guard（既定false）を追加した。
- guard=true（tools/external_trigger/のCloudflare Workerが使う）: scheduleと同じくdaily_guard.pyを通す
- guard=false（人の手動実行）: 従来どおりガードを通さず取得する
- reset_windowで終わった実行はcommitしない（下流は動かさない＝自分を取り消す）
- GitHubのscheduleは外部起動の保険として残す

ガードの段のシェルを、${{ }}の式を値に置き換えてbashで実際に動かして確かめる。
"""

import os
import subprocess

import pytest
import yaml

from tests.test_push_with_retry import _git_bash

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_WF = os.path.join(_ROOT, ".github", "workflows", "Market_Data_Daily_Update.yml")


def _wf():
    with open(_WF, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _steps():
    return _wf()["jobs"]["update-market-data-daily"]["steps"]


def _step(name_prefix):
    return next(s for s in _steps() if str(s.get("name", "")).startswith(name_prefix))


def test_dispatch_has_guard_input_default_false():
    guard = _wf()[True]["workflow_dispatch"]["inputs"]["guard"]   # PyYAMLは`on:`をTrueとして読む
    assert guard["type"] == "boolean"
    assert guard["default"] is False


def test_schedule_kept_as_insurance():
    crons = [c["cron"] for c in _wf()[True]["schedule"]]
    assert crons == ["47 20 * * 1-5", "17,47 21-22 * * 1-5", "17 23 * * 1-5", "47 1 * * 2-6", "17 2 * * 2-6"]


@pytest.fixture(scope="module")
def bash():
    b = _git_bash()
    if not b:
        pytest.skip("bashが無い")
    return b


def _run_guard_step(bash, tmp_path, event_name, guard_input):
    script = _step("Guard")["run"]
    assert "python common/market_data/daily_guard.py" in script
    script = (script.replace("${{ github.event_name }}", event_name)
                    .replace("${{ inputs.guard }}", guard_input)
                    .replace("python common/market_data/daily_guard.py",
                             "printf 'run=false\\nreason=stub-guard\\n'"))
    out = tmp_path / "github_output"
    out.write_text("", encoding="utf-8")
    r = subprocess.run([bash, "-c", script], env={**os.environ, "GITHUB_OUTPUT": str(out)},
                       capture_output=True, text=True, encoding="utf-8", timeout=30)
    assert r.returncode == 0, r.stderr
    return dict(line.split("=", 1) for line in out.read_text(encoding="utf-8").splitlines() if "=" in line)


def test_manual_dispatch_skips_guard(bash, tmp_path):
    assert _run_guard_step(bash, tmp_path, "workflow_dispatch", "false") == {"run": "true", "reason": "手動実行"}


def test_dispatch_with_guard_true_runs_guard(bash, tmp_path):
    assert _run_guard_step(bash, tmp_path, "workflow_dispatch", "true") == {"run": "false", "reason": "stub-guard"}


def test_schedule_runs_guard(bash, tmp_path):
    # scheduleの起動ではinputsが無く、${{ inputs.guard }}は空文字になる
    assert _run_guard_step(bash, tmp_path, "schedule", "") == {"run": "false", "reason": "stub-guard"}


def test_reset_window_does_not_commit_and_cancels():
    commit_if = _step("Commit and push changes")["if"]
    assert "steps.guard.outputs.run == 'true'" in commit_if
    assert "steps.fetch.outputs.status != 'reset_window'" in commit_if
    # commitの段が飛ばされるとsavedが空になり、取り消しの段の条件に当たる（下流は動かない）
    cancel_if = _step("Cancel this run")["if"]
    assert "steps.fetch.outputs.status != 'fetched'" in cancel_if
    assert "steps.commit.outputs.saved != 'true'" in cancel_if


def test_downstream_have_no_friday_fallback_schedule():
    # 2026-10-03: 金曜の保険のcronを削除。Market Data Dailyの取得より前に遅れて起動し、前日のデータで動いていた
    for name in ("Market_Pulse_Update", "Stonks_Silo_Update", "TANUKI_VALUATION_Update"):
        with open(os.path.join(_ROOT, ".github", "workflows", f"{name}.yml"), encoding="utf-8") as f:
            triggers = yaml.safe_load(f)[True]
        assert "schedule" not in triggers, name
        assert "workflow_run" in triggers, name
