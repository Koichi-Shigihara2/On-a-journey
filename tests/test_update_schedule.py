"""更新スケジュールの自動生成（scripts/gen_update_schedule.py）とCHECK-58（指示書㉘、2026-09-30）のテスト。"""
import json
import os
import shutil
import sys

import pytest

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_REPO, "scripts"))
import gen_update_schedule as gus  # noqa: E402
from common.sec_data import report_consistency_check as rcc  # noqa: E402


@pytest.mark.parametrize("expr,utc,jst", [
    ("17,47 20-22 * * 1-5", "月〜金 20:17・20:47・21:17・21:47・22:17・22:47", "火〜土 05:17・05:47・06:17・06:47・07:17・07:47"),
    ("47 1 * * 2-6", "火〜土 01:47", "火〜土 10:47"),
    ("15 22 * * *", "毎日 22:15", "毎日（翌日） 07:15"),
    ("0 23 1-7 * 0", "日（1〜7日） 23:00", "月（1〜7日） 08:00"),
])
def test_cron_text(expr, utc, jst):
    assert gus.cron_text(expr) == (utc, jst)


def test_repo_is_in_sync():
    """現在のリポジトリ: 一覧・JSONとYAMLが一致している（CHECK-58が出ない）"""
    assert gus.check_drift(_REPO) == []
    assert rcc._check_update_schedule_drift(_REPO) == []


def _copy_repo(tmp_path):
    for rel in (".github/workflows", "docs/architecture", "config"):
        os.makedirs(tmp_path / rel, exist_ok=True)
    for f in os.listdir(os.path.join(_REPO, ".github", "workflows")):
        shutil.copy(os.path.join(_REPO, ".github", "workflows", f), tmp_path / ".github" / "workflows" / f)
    shutil.copy(os.path.join(_REPO, "docs", "architecture", "UPDATE_SCHEDULE.md"), tmp_path / "docs" / "architecture")
    shutil.copy(os.path.join(_REPO, "config", "workflow_dependencies.json"), tmp_path / "config")
    return tmp_path


def test_drift_detected_when_yaml_changes(tmp_path):
    root = _copy_repo(tmp_path)
    p = root / ".github" / "workflows" / "Market_Pulse_Update.yml"
    p.write_text(p.read_text(encoding="utf-8").replace('cron: "50 22 * * 5"', 'cron: "55 22 * * 5"'), encoding="utf-8")
    msgs = gus.check_drift(str(root))
    assert msgs == ["UPDATE_SCHEDULE.mdのワークフロー一覧が.github/workflows/*.ymlと一致しない"]
    warn = rcc._check_update_schedule_drift(str(root))
    assert len(warn) == 1 and "WARN-58" in warn[0]


def test_drift_detected_when_doc_edited_by_hand(tmp_path):
    root = _copy_repo(tmp_path)
    p = root / "docs" / "architecture" / "UPDATE_SCHEDULE.md"
    p.write_text(p.read_text(encoding="utf-8").replace("全22本", "全21本"), encoding="utf-8")
    assert gus.check_drift(str(root)) == ["UPDATE_SCHEDULE.mdのワークフロー一覧が.github/workflows/*.ymlと一致しない"]


def test_deps_generated_fields_and_curated_kept(tmp_path):
    root = _copy_repo(tmp_path)
    dp = root / "config" / "workflow_dependencies.json"
    d = json.loads(dp.read_text(encoding="utf-8"))
    d["workflows"]["TANUKI_VALUATION_Update"]["depends_on"].append("Market_Data_Daily_Update")   # 古い依存を残した状態
    dp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    assert any("depends_on" in m for m in gus.check_drift(str(root)))
    wfs = gus.load_workflows(str(root / ".github" / "workflows"))
    g = gus.generated_deps(wfs, d)
    assert g["workflows"]["TANUKI_VALUATION_Update"]["depends_on"] == ["HypeCore_Update", "Adjusted_EPS_Update", "Stonks_Silo_Update"]
    # admin.htmlが使う手作業の定義はそのまま
    for k, v in d["workflows"].items():
        for f in ("label", "yml", "accepts_tickers", "input_param"):
            assert g["workflows"][k].get(f) == v.get(f)
    for f in ("bulk_update_order", "bulk_update_phases", "new_ticker_order"):
        assert g[f] == d[f]


def test_curated_input_param_checked():
    wfs = gus.load_workflows()
    d = json.load(open(os.path.join(_REPO, "config", "workflow_dependencies.json"), encoding="utf-8"))
    assert gus.curated_input_problems(wfs, d) == []
    d["workflows"]["Adjusted_EPS_Update"]["input_param"] = "tickers"   # 実際の入力はticker
    assert gus.curated_input_problems(wfs, d) == ["Adjusted_EPS_Update: input_param tickers がAdjusted_Eps_Analyzer_update.ymlの手動実行の入力に無い"]
