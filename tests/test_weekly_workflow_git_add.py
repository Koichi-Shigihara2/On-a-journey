"""Market Data Weekly のコミットの段: 無いことがあるファイル（[SKIP] の記録）は、`git add` の単独の行にする。
`git add a b` は b が無いと、a も追加せずに終わる（fatal: pathspec ... did not match）ため、同じ行に置くと daily/ などが
コミットされなくなる（2026-11-28 に気づいた）"""
import os
import re

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WF = os.path.join(REPO, ".github", "workflows", "Market_Data_Weekly_Update.yml")


def test_optional_split_skips_file_is_added_on_its_own_line():
    lines = [ln.strip() for ln in open(WF, encoding="utf-8").read().splitlines()]
    adds = [ln for ln in lines if ln.startswith("git add ") and "_split_reconcile_skips.json" in ln]
    assert adds == ["git add common/market_data/_split_reconcile_skips.json || true"]


def test_split_reconcile_scan_and_its_record_are_committed_in_the_same_workflow():
    text = open(WF, encoding="utf-8").read()
    assert "split_reconcile.py --apply" in text and "_split_reconcile_skips.json" in text
    other = [f for f in os.listdir(os.path.dirname(WF)) if f.endswith(".yml") and f != "Market_Data_Weekly_Update.yml"]
    for f in other:
        assert "split_reconcile.py" not in open(os.path.join(os.path.dirname(WF), f), encoding="utf-8").read(), f   # 走査が動くのはここだけ
