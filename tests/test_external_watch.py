"""[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] B1（2026-11-04）: 外の変化の検知を System Health の [P] に集め、台帳に無い新しい件だけ⚠️。
B1 CIKの照合・[P]の枠、B2 タグの途切れ・切り替え・候補外・期末の数日違い、B3 先物の限月（欄の異常と切り替えの印）"""
import json
import os
import sys
from datetime import date, datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common import external_watch as ew
from common import fetch_failure
from common import system_health as sh
from common.macro_data import fomc_calendar as fc
from common.sec_data import source_watch as sw


# ── B1 ───────────────────────────────────────────────────────────────
def test_cik_items(tmp_path):
    p = tmp_path / "cik.csv"
    p.write_text("ticker,cik,status\nAAA,0000000001,active\nBBB,0000000002,active\nCCC,0000000003,retired\nDDD,0000000004,dormant\n",
                 encoding="utf-8")
    ct = {"0": {"ticker": "AAA", "cik_str": 1}, "1": {"ticker": "BBB", "cik_str": 9}}
    got = {(i["ticker"], i["match"]) for i in sw.cik_items(ct, str(p))}
    assert got == {("BBB", "0000000002→0000000009"), ("DDD", "0000000004→なし")}   # retired は見ない


def test_ledger_split_and_judge():
    items = [{"kind": "cik", "ticker": "A", "match": "1→2", "detail": ""}, {"kind": "tag_gap", "ticker": "B", "match": "rpo", "detail": ""}]
    new, ack = ew.split(items, [{"kind": "tag_gap", "ticker": "B", "match": "rpo"}])
    assert [i["ticker"] for i in new] == ["A"] and [i["ticker"] for i in ack] == ["B"]
    assert fetch_failure.judge_external(1) == fetch_failure.EXTERNAL_NEW_ROW and fetch_failure.judge_external(0)[0] == 0
    assert "CIKの切り替え 1件（A 1→2）" in ew.summarize(new)


def test_write_partial_run_keeps_other_tickers(tmp_path):
    p = str(tmp_path / "w.json")
    sw.write({"A": [{"kind": "tag_gap", "ticker": "A", "match": "rpo", "detail": ""}], "B": []}, [], full_run=True, path=p)
    sw.write({"C": [{"kind": "tag_gap", "ticker": "C", "match": "rpo", "detail": ""}]}, None, full_run=False, path=p)
    assert {i["ticker"] for i in sw.all_items(p)} == {"A", "C"}
    sw.write({"A": []}, None, full_run=False, path=p)                                 # 直った銘柄は消える
    assert {i["ticker"] for i in sw.all_items(p)} == {"C"}


def _cache(tmp_path):
    p = str(tmp_path / "fomc.json")
    page = ('<h4>2026 FOMC Meetings</h4><div class="fomc-meeting__month"><strong>December</strong></div>'
            '<div class="fomc-meeting__date">8-9*</div><a href="/newsevents/pressreleases/monetary20261028a.htm">s</a>')
    fc.save_cache(page, now=datetime(2026, 11, 4, tzinfo=timezone.utc), path=p)
    return p


def test_check_p_new_items_warn_and_ledger_clears(tmp_path):
    swp = str(tmp_path / "w.json")
    sw.write({"A": [{"kind": "tag_gap", "ticker": "A", "match": "rpo", "detail": "x"}]}, [], full_run=True, path=swp)
    daily = tmp_path / "daily"
    daily.mkdir()
    led = tmp_path / "led.json"
    led.write_text(json.dumps({"acknowledged": []}), encoding="utf-8")
    r = sh._check_p(date(2026, 11, 5), _cache(tmp_path), swp, str(daily), str(led), str(tmp_path / "sm.json"))
    assert r["severity"] == 1 and not r["ok"] and fetch_failure.EXTERNAL_NEW_ROW[1] in r["label"] and "タグの途切れ 1件" in r["label"]
    led.write_text(json.dumps({"acknowledged": [{"kind": "tag_gap", "ticker": "A", "match": "rpo"}]}), encoding="utf-8")
    r = sh._check_p(date(2026, 11, 5), _cache(tmp_path), swp, str(daily), str(led), str(tmp_path / "sm.json"))
    assert r["ok"] and r["severity"] == 0 and "確認済み1・新しい件0" in r["label"]
