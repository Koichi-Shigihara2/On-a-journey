"""[[PARSER-LAYER3-SINGLE-SOURCE-1]] 段1: parser の四半期BS（quarterly_*.json の bs）を Layer3 の val から作る1か所の回帰テスト。

- 違う値は Layer3 の val で置き換わる。parser にだけある値・Layer3にだけある値・符号だけの違い・期末日が決まらない期は触らない
- CAKE型（古い期の非流動の借入を残して二重に数える）が起きない: 置き換えた後の値は Layer3 の val と一致
- 書き手は quarterly_bs.py の1か所（parse_and_save が呼ぶ）
"""
import os

from common.sec_data import quarterly_bs as QB

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _entry(end, val, category_flow=False):
    return {"end": end, "val": val, "is_ytd": False, "is_annual": False, "form": "10-Q"}


def _store(per_field):
    """per_field: {Layer3の項目: [(期末, 値)]}"""
    fields = {}
    for f, pts in per_field.items():
        fields[f] = {"category": "stock", "entries": [_entry(e, v) for e, v in pts]}
    return {"fields": fields}


BASE = {
    "cash_and_equivalents": [("2026-06-30", 100.0), ("2026-09-30", 120.0)],
    "current_assets": [("2026-06-30", 300.0), ("2026-09-30", 330.0)],
    "current_liabilities": [("2026-06-30", 200.0), ("2026-09-30", 210.0)],
    "total_assets": [("2026-06-30", 900.0), ("2026-09-30", 950.0)],
    "stockholders_equity": [("2026-06-30", 400.0), ("2026-09-30", 420.0)],
    "long_term_debt": [("2026-06-30", 560.0), ("2026-09-30", 0.0)],      # 2026-09-30は流動分だけ（非流動は0）
    "short_term_debt": [("2026-06-30", 0.0), ("2026-09-30", 563.0)],
}


def _record(**bs):
    b = {"cash_and_equivalents": 120.0, "current_assets": 330.0, "current_liabilities": 210.0, "total_assets": 950.0,
         "stockholders_equity": 420.0, "long_term_debt": 0.0, "short_term_debt": 563.0}
    b.update(bs)
    return {"period": "2026Q3", "bs": b}


def test_differing_value_is_replaced_by_layer3_val():
    """CAKE型: parser が前の期の非流動の借入 560 を残していた → Layer3 の val 0 に置き換わる（二重に数えない）"""
    rec = _record(long_term_debt=560.0)
    changed = QB.replace_record(rec, _store(BASE))
    assert changed == [("long_term_debt", 560.0, 0.0)] and rec["bs"]["long_term_debt"] == 0.0
    assert rec["bs"]["short_term_debt"] == 563.0


def test_equal_sign_only_parser_only_and_layer3_only_cells_are_not_touched():
    st = _store({**BASE, "goodwill": [("2026-09-30", 77.0)]})                # Layer3にだけある値（goodwill）
    rec = _record(stockholders_equity=-420.0)                                 # 符号だけの違い
    rec["bs"]["insurance_reserves"] = 5.0                                     # Layer3に対応の無い項目
    before = dict(rec["bs"])
    assert QB.replace_record(rec, st) == [] and rec["bs"] == before and "goodwill" not in rec["bs"]


def test_unresolved_period_is_left_alone():
    """値が Layer3 のどの期とも合わない（票が少ない）期は置き換えない"""
    rec = {"period": "2026Q3", "bs": {"cash_and_equivalents": 1.0, "total_assets": 2.0, "long_term_debt": 3.0}}
    assert QB.replace_record(rec, _store(BASE)) == [] and rec["bs"]["long_term_debt"] == 3.0


def test_replace_from_layer3_counts_and_is_idempotent():
    parsed = {"quarterly": {"2026Q3": _record(long_term_debt=560.0)}}
    assert QB.replace_from_layer3("ZZZ", parsed, store=_store(BASE)) == 1
    assert QB.replace_from_layer3("ZZZ", parsed, store=_store(BASE)) == 0          # 2回目は0
    assert QB.replace_from_layer3("ZZZ", {"quarterly": {}}, store=_store(BASE)) == 0


def test_committed_files_match_layer3_for_cake():
    """手元のデータ: CAKE の最新の四半期の長期借入金が Layer3 の val（流動分だけの行＝非流動 0）と一致している"""
    import glob
    import json
    store = QB._build_store("CAKE")
    assert store
    for p in sorted(glob.glob(os.path.join(QB.DATA_DIR, "CAKE", "quarterly_*.json")))[-3:]:
        with open(p, encoding="utf-8") as f:
            rec = json.load(f)
        assert QB.replace_record(rec, store) == []        # 置き換え済み（当てても変わらない）


def _src(rel):
    with open(os.path.join(REPO, rel), encoding="utf-8") as f:
        return f.read()


def test_quarterly_bs_has_one_writer():
    """四半期BSを Layer3 の val で置き換えるのは quarterly_bs.py の1か所で、parse_and_save だけが呼ぶ"""
    p = _src("common/sec_data/parser.py")
    assert p.count("quarterly_bs.replace_from_layer3(") == 1
    assert p.index("quarterly_bs.replace_from_layer3(") < p.index("self.save_parsed_data(ticker, parsed)")
    assert "save_parsed_data(" not in _src("common/sec_data/update.py")        # 保存の入口は parse_and_save だけ
    assert "parse_and_save(" in _src("common/sec_data/update.py")
