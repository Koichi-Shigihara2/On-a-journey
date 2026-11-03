"""common/reference_check.py（rcc CHECK-68、[[CHECK-REFERENCE-DEFINITION-1]]、2026-11-03）。

文書・説明文に書いたパスの存在、説明文の「消費者なし」と出力の一覧の読み手、ワークフローの表示名と動かすスクリプト。
小さな入力での判定を確かめる（今のリポジトリの WARN は rcc で見る。pytest で止めると WARN が NG になるため）。
"""
import os
import sys

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from common import reference_check as rc  # noqa: E402


def _repo(files):
    r = rc._Repo(files=files)
    r.ignored = lambda p: False
    return r


def test_path_existence_rules():
    r = _repo(["common/sec_data/fetcher.py", "common/sec_data/data/AAPL/annual_2025.json",
               "config/sec_concept_definitions.json", "docs/portfolio/tail/data/mda/AAPL/2026Q3.json"])
    assert r.exists("common/sec_data/fetcher.py") and r.exists("common/sec_data/")       # ファイル・フォルダ
    assert r.exists("common/sec_data/data/{TICKER}/annual_YYYY.json")                     # 型は何にでも当たる
    assert r.exists("config/sec_concept_definitions.")                                    # 行末で折れた書きかけ
    assert r.exists("docs/portfolio/tail/data/{risk_factors,legal_proceedings,mda}/{TICKER}/")
    assert not r.exists("common/sec_data/store_v2/")


def test_scan_skips_deleted_planned_and_struck_lines():
    r = _repo(["common/sec_data/fetcher.py"])
    lines = [(1, "書き手は `common/sec_data/gone.py`"),
             (2, "`common/sec_data/gone.py`（`abc1234`〈2026-10-20〉で削除）"),
             (3, "`config/ledger.json`（予定）"),
             (4, "~~`common/sec_data/gone.py`~~ は使わない"),
             (5, "`data/annual_*.json`（各フォルダからの相対）")]
    assert rc._scan_lines("X.md", lines, r) == [("X.md", 1, "common/sec_data/gone.py")]


def test_workflow_scripts_resolve_working_directory_and_module():
    wf = {"jobs": {"a": {"defaults": {"run": {"working-directory": "src/value/stonks_silo"}},
                         "steps": [{"run": "python pipeline.py --all\n# python old.py"},
                                   {"run": "python -m src.value.adjusted_eps_analyzer.pipeline", "working-directory": "."},
                                   {"run": "python -c 'print(1)'"}]}}}
    assert rc.workflow_scripts(wf) == {"src/value/stonks_silo/pipeline.py", "src/value/adjusted_eps_analyzer/pipeline.py"}


def test_workflow_name_and_scripts_mismatch_is_reported(tmp_path, monkeypatch):
    import json
    (tmp_path / "config").mkdir()
    (tmp_path / ".github" / "workflows").mkdir(parents=True)
    (tmp_path / ".github" / "workflows" / "X.yml").write_text(
        "name: TANUKI_Score_Update\njobs:\n  u:\n    steps:\n      - run: python src/portfolio/snapshot.py\n", encoding="utf-8")
    (tmp_path / "config" / "workflow_dependencies.json").write_text(json.dumps({"workflows": {
        "Portfolio_Snapshot_Update": {"yml": "X.yml", "name": "Portfolio Snapshot Update", "scripts": ["src/portfolio/snapshot.py"]}}}),
        encoding="utf-8")
    monkeypatch.setattr(rc, "REPO_ROOT", str(tmp_path))
    msgs = rc.workflow_name_mismatch()
    assert len(msgs) == 1 and "表示名が違う" in msgs[0]


def test_consumer_claim_against_inventory(monkeypatch):
    monkeypatch.setattr(rc, "_inventory_readers_by_package", lambda: {"common/macro_data": {"src/market/macro_pulse/05_main.py"}})
    monkeypatch.setattr(rc, "module_docstring", lambda rel: ("現時点でこのモジュールを参照する本番消費者は存在しない", 1)
                        if rel == "common/macro_data/reader.py" else (None, 0))
    r = _repo(["common/macro_data/reader.py", "common/market_data/reader.py"])
    assert rc.consumer_claims(r) == [("common/macro_data/reader.py", 1, 1)]


def test_rcc_calls_check_68():
    with open(os.path.join(_REPO, "common", "sec_data", "report_consistency_check.py"), encoding="utf-8") as f:
        src = f.read()
    assert "_ref_check.check()" in src and "CHECK-68" in src
