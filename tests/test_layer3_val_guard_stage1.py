"""[[PARSER-LAYER3-SINGLE-SOURCE-1]] 段1（2026-11-11）: Layer3 の val の守り・借入金の貸借対照表の行・非支配持分込みの純資産。

1. val の守り(i): 単位の誤り（10の累乗、|k|≥3）は first_val に戻す。first_val 側が誤り・持ち株数の分割・隣の期が無いときは触らない
2. 守り(iii): 値は変えず val_suspect の印だけ。期全体の組み替え（5項目以上）は対象外
3. cf_lines(ii): 内訳の行が最新の申告で消え、額がどの行にも残っていないときだけ、内訳の揃った申告の値
4. bs_lines の借入金: 完全に取れた申告の行を正にする。short_term_debt は行が大きいときだけ
5. 純資産が非支配持分込みのタグなら minority_interest を引く。値が無い期は値を残して印
"""
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common.sec_data import bs_lines as B  # noqa: E402
from common.sec_data import cf_lines as C  # noqa: E402
from common.sec_data import layer3_builder as L  # noqa: E402
from common.sec_data import val_guard as G  # noqa: E402


def _e(end, val, first=None, ann=False, **kw):
    return dict({"end": end, "val": val, "first_val": val if first is None else first, "is_annual": ann, "is_ytd": False,
                 "source_tag": "x"}, **kw)


def _series(vals, last_val, last_first, ann=True):
    es = [_e(f"20{19 + i}-12-31", v, ann=ann) for i, v in enumerate(vals)]
    es.append(_e("2023-12-31", last_val, last_first, ann=ann))
    es += [_e("2024-12-31", vals[-1], ann=ann), _e("2025-12-31", vals[-1], ann=ann)]
    return es


def test_unit_error_val_replaced_by_first_val():
    es = _series([400, 410, 420, 430], 430_000, 430)          # 1,000倍
    done = G.guard_unit_errors(es)
    bad = next(e for e in es if e["end"] == "2023-12-31")
    assert len(done) == 1 and bad["val"] == 430 and bad["val_latest"] == 430_000 and bad["val_guard"] == "unit_pow10"


def test_unit_error_in_first_val_is_left_alone():
    es = _series([0.07, 0.08, 0.07, 0.06], 0.07, 70)          # first_val 側が誤り（COHR 2020 EPS型）
    assert G.guard_unit_errors(es) == []
    assert next(e for e in es if e["end"] == "2023-12-31")["val"] == 0.07


def test_split_ratio_10x_not_touched_and_no_neighbors_not_touched():
    es = _series([13, 13, 13, 13], 130, 13)                   # 10倍（株式分割の組み替えは val が正しい）
    assert G.guard_unit_errors(es) == []
    lone = [_e("2023-12-31", 430_000, 430, ann=True)]
    assert G.guard_unit_errors(lone) == []


def test_mark_suspect_keeps_value_and_skips_whole_period_restatement():
    es = _series([100, 105, 110, 108], 1_000, 109)            # 隣から外れるのに first_val は外れない
    fields = {"f": {"entries": es}}
    assert G.mark_suspect(fields) == 1
    bad = next(e for e in es if e["end"] == "2023-12-31")
    assert bad["val"] == 1_000 and bad["val_suspect"] == "neighbor_outlier"
    # 同じ期末で5項目以上が first_val から動く＝期全体の組み替え → 印を付けない
    fields2 = {f"f{i}": {"entries": _series([100, 105, 110, 108], 1_000, 109)} for i in range(5)}
    assert G.mark_suspect(fields2) == 0
    # 持ち株数・1株は対象外
    assert G.mark_suspect({"eps_diluted": {"entries": _series([100, 105, 110, 108], 1_000, 109)}}) == 0


def test_marks_from_store_window_and_text():
    store = {"latest_period_end": "2026-09-30", "fields": {"x": {"entries": [
        _e("2024-06-30", 3_600, 13_048, val_suspect="neighbor_outlier"),
        _e("2019-06-30", 1, 2, val_guard="unit_pow10"),                                  # 3年より前は渡さない
        _e("2023-12-31", 5, 6, ann=True, val_guard="components_dropped:software", val_latest=4)]}}}
    marks = G.marks_from_store(store)
    assert [(m["end"], m["rule"]) for m in marks] == [("2023-12-31", "components_dropped:software"), ("2024-06-30", "suspect:neighbor_outlier")]
    assert G.rule_text("components_dropped:software") and G.rule_text("suspect:neighbor_outlier")


def test_equity_ex_nci_subtracts_and_marks_missing():
    f = {"stockholders_equity": {"entries": [
        _e("2026-06-30", 2_398, 2_390, source_tag=L.SE_NCI_TAG),
        _e("2026-03-31", 2_000, source_tag=L.SE_NCI_TAG),                                # minority_interest の値なし
        _e("2025-12-31", 1_500, source_tag="StockholdersEquity")]},
        "minority_interest": {"entries": [_e("2026-06-30", 504, 500)]}}
    L._equity_ex_nci(f)
    a, b, c = f["stockholders_equity"]["entries"]
    assert (a["val"], a["first_val"], a["val_incl_nci"], a["source_tag"]) == (1_894, 1_890, 2_398, L.EQUITY_EX_NCI_TAG)
    assert b["val"] == 2_000 and b["mark"] == L.EQUITY_INCL_NCI_MARK and b["source_tag"] == L.SE_NCI_TAG   # 0で埋めず、値を残して印
    assert c["val"] == 1_500 and "mark" not in c


def _rec(accn, filed, lines, concepts, form="10-Q"):
    return {"accn": accn, "filed": filed, "form": form, "status": "ok", "role": "r", "source_files": [],
            "lines": [{"concept": "us-gaap:" + k, "values": v} for k, v in lines.items()],
            "concepts": ["us-gaap:" + c for c in concepts]}


def test_prefer_face_debt_complete_record_only(tmp_path):
    d = str(tmp_path)
    cash = {"2026-07-31": 4_098}
    ok = _rec("a1", "2026-08-28", {"CashAndCashEquivalentsAtCarryingValue": cash, "NotesPayableCurrent": {"2026-07-31": 499},
                                   "ShortTermBorrowings": {"2026-07-31": 994}, "LongTermNotesPayable": {"2026-07-31": 1_985}},
              ["NotesPayableCurrent", "ShortTermBorrowings", "LongTermNotesPayable"])
    # 借入金と分類される名前（1年内返済の長期債務）が concepts にあるのに lines に無い記録 → 使わない
    bad = _rec("a2", "2026-09-01", {"CashAndCashEquivalentsAtCarryingValue": {"2026-06-30": 1}, "ShortTermBorrowings": {"2026-06-30": 34}},
               ["ShortTermBorrowings", "LongTermDebtAndCapitalLeaseObligationsCurrent"])
    assert B.complete_debt_record(ok) and not B.complete_debt_record(bad)
    B.save_record("ZZZ", "a1", ok, d)
    B.save_record("ZZZ", "a2", bad, d)
    f = {"short_term_debt": {"entries": [_e("2026-07-31", 994, 994), _e("2026-06-30", 90, 90)]},
         "long_term_debt": {"entries": [_e("2026-07-31", 2_500, 2_480)]}}
    n = B.prefer_face_debt("ZZZ", f, d)
    s, l = f["short_term_debt"]["entries"], f["long_term_debt"]["entries"]
    assert n == {"short_term_debt": 1, "long_term_debt": 1}
    assert s[0]["val"] == 1_493 and s[0]["debt_basis"] == "bs_lines" and s[0]["val_tag_val"] == 994
    assert s[1]["val"] == 90 and "debt_basis" not in s[1]                       # 不完全な記録の期は触らない
    assert l[0]["val"] == 1_985 and l[0]["first_val"] == 1_965                 # first_val も同じ差だけずらす
    # short_term_debt は行の値がタグより小さいときは使わない
    f2 = {"short_term_debt": {"entries": [_e("2026-07-31", 2_000, 2_000)]}, "long_term_debt": {"entries": []}}
    assert B.prefer_face_debt("ZZZ", f2, d) == {} and f2["short_term_debt"]["entries"][0]["val"] == 2_000


def _cf_rec(accn, filed, form, per, ppe=None, sw=None, other=None, total=-100.0):
    lines = []
    if ppe is not None:
        lines.append({"concept": "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment", "values": {per: ppe}})
    if sw is not None:
        lines.append({"concept": "us-gaap:PaymentsToDevelopSoftware", "values": {per: sw}})
    if other is not None:
        lines.append({"concept": "us-gaap:PaymentsToAcquireIntangibleAssets", "values": {per: other}})
    lines.append({"concept": "us-gaap:NetCashProvidedByUsedInInvestingActivities", "values": {per: total}})
    return {"accn": accn, "filed": filed, "form": form, "status": "ok", "lines": lines}


def test_cf_lines_components_dropped_only_when_amount_vanished(tmp_path, monkeypatch):
    p = "2021-01-01~2021-12-31"
    early = _cf_rec("e", "2022-03-11", "10-K", p, ppe=1_390.0, sw=4_067.0)
    late_vanished = _cf_rec("l", "2024-02-26", "10-K", p, ppe=1_390.0)                       # ソフトの行が消え、額はどの行にも無い
    late_retagged = _cf_rec("l", "2024-02-26", "10-K", p, ppe=1_390.0, other=4_067.0)        # 同額が別の行にある＝付け替え
    for k in ("recs",):
        pass
    monkeypatch.setattr(C, "line_kinds", lambda rec: _kinds(rec))
    out = C.period_values({"e": early, "l": late_vanished})[p]
    assert out["guard"] == "components_dropped:software" and out["ppe"] + out["software"] == 5_457.0 and out["accn"] == "e"
    out2 = C.period_values({"e": early, "l": late_retagged})[p]
    assert "guard" not in out2 and out2["accn"] == "l" and out2["software"] == 0.0


def _kinds(rec):
    kmap = {"PaymentsToAcquirePropertyPlantAndEquipment": "ppe", "PaymentsToDevelopSoftware": "software",
            "PaymentsToAcquireIntangibleAssets": "other_acq", "NetCashProvidedByUsedInInvestingActivities": "total"}
    return [(kmap[x["concept"].split(":")[1]], {"concept": x["concept"], "values": x["values"]}) for x in rec["lines"]]


def test_definitions_debt_current_first():
    d = json.load(open(os.path.join(REPO, "config", "sec_concept_definitions.json"), encoding="utf-8"))
    assert d["fields"]["short_term_debt"]["candidates"][0] == "DebtCurrent"            # 合計のタグが先（値0の内訳が先に勝たない）
