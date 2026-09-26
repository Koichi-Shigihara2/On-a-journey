"""
tests/test_workflow_notify_after_gate.py

2026-09-26（指示書㉑ STEP A）: ゲート（report_consistency_check.py --fail-on-ng）を持つ
ワークフローで、通知（Discord・メール）の段がゲートより前に無いことを確認する。
TANUKI_VALUATION_Update.ymlではscore_watcher.py（Discord通知）がゲートより前にあり、
ゲートが公開を止めた日も誤った判定変化が通知されていた。
"""

import glob
import os
import re

import yaml

_WF_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".github", "workflows")
_NOTIFY = re.compile(r"discord|webhook|score_watcher|send_email|gmail|slack", re.I)


def _step_text(step):
    return " ".join(str(step.get(k, "")) for k in ("name", "run", "env"))


def test_notify_steps_come_after_gate_in_every_workflow():
    problems = []
    for path in sorted(glob.glob(os.path.join(_WF_DIR, "*.yml"))):
        with open(path, encoding="utf-8") as f:
            wf = yaml.safe_load(f)
        for job_name, job in (wf.get("jobs") or {}).items():
            steps = job.get("steps") or []
            gates = [i for i, s in enumerate(steps) if "report_consistency_check" in str(s.get("run", ""))]
            if not gates:
                continue
            for i, s in enumerate(steps):
                if _NOTIFY.search(_step_text(s)) and i < gates[0]:
                    problems.append(f"{os.path.basename(path)}:{job_name}: 「{s.get('name')}」がゲートより前")
    assert problems == []


def test_tanuki_score_watcher_runs_between_gate_and_commit():
    with open(os.path.join(_WF_DIR, "TANUKI_VALUATION_Update.yml"), encoding="utf-8") as f:
        steps = yaml.safe_load(f)["jobs"]["update-valuations"]["steps"]
    names = [s.get("name") for s in steps]
    gate = names.index("Consistency Check Gate")
    watcher = names.index("Detect TANUKI SCORE changes and notify Discord")
    commit = names.index("Commit and push changes")
    assert gate < watcher < commit
