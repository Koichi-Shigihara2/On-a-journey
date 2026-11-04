"""[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] A1（2026-11-04）: ワークフローのランナーの版を明示の版に固定し、`*-latest` を禁止する。

`ubuntu-latest` は GitHub の都合で中身の版が変わる（2026-10-25頃に Ubuntu 24.04 → 26.04。FINDINGS 2026-10-18）。
版の変化を知らないうちに受けないよう、使ってよい版は下の RUNNER の1か所に書き、全ワークフローの全ジョブがそれを使う。
版を上げるときは RUNNER とワークフローを同じcommitで変える（GitHubの廃止の予告は監視しない。固定で十分）。
"""
import glob
import os

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNNER = "ubuntu-26.04"   # 2026-11-02以降の全ワークフローの実行がこの版で成功（FINDINGS 2026-10-18）


def _jobs():
    for wf in sorted(glob.glob(os.path.join(REPO, ".github", "workflows", "*.yml"))):
        with open(wf, encoding="utf-8") as f:
            d = yaml.safe_load(f)
        for name, job in (d.get("jobs") or {}).items():
            yield os.path.basename(wf), name, job.get("runs-on")


def test_all_jobs_use_the_pinned_runner():
    jobs = list(_jobs())
    assert len(jobs) >= 21
    bad = [(wf, name, r) for wf, name, r in jobs if r != RUNNER]
    assert not bad, f"runs-on は {RUNNER} だけ（*-latest は使わない）: {bad}"


def test_no_latest_label_anywhere_in_workflows():
    hits = []
    for wf in glob.glob(os.path.join(REPO, ".github", "workflows", "*.yml")):
        with open(wf, encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                if "-latest" in line and not line.lstrip().startswith("#"):
                    hits.append(f"{os.path.basename(wf)}:{i}")
    assert not hits, hits
