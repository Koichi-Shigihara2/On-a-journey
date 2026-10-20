"""[[LAYER3-FIRST-FILED-DATE-1]]（R-C、2026-10-21）: Layer3・normalized の first_filed・first_val と、Q4 の逆算（案1）。

- filed・val は今の意味（最新の申告）のまま。first_filed は候補のタグ全体で最初に申告された書類の提出日、
  first_val は採用したタグで最初に申告された値
- 派生の値（YTD からの差し引き・Q4 の逆算・合算）は first_filed を元の値の最大、first_val を同じ計算で作る
- Q4 = 最初の 10-K の年次 − その 10-K の提出日以前に申告された Q1〜Q3 の最も新しい値（年次と同じ時点の値どうし）。
  その時点の値が無ければ最新どうしの差＋印
"""
import json

from common.sec_data import layer3_builder as l3
from common.sec_data import normalizer as nz
from common.sec_data.contracts import FinancialEntry
from common.sec_data.fact_selection import ZERO_TERM, combine_first, derive, strip_asof, value_asof
from common.sec_data.q4_implied import build_q4_implied_entries
from common.sec_data.quarterly import _process_entries, earliest_filed_by_period
from common.sec_data.report_consistency_check import _check_restated_annual


def _fact(start, end, val, filed, form="10-Q", fp="Q1"):
    return {"start": start, "end": end, "val": val, "filed": filed, "form": form, "fp": fp, "fy": 2025, "accn": f"a-{filed}"}


# APP 型: 2025 の Q1〜Q3 は 10-Q の値のまま、年次は後の 10-K で組み替え（事業の売却）
_Q1 = ("2025-01-01", "2025-03-31")
_Q2 = ("2025-04-01", "2025-06-30")
_Q3 = ("2025-07-01", "2025-09-30")
_FY = ("2025-01-01", "2025-12-31")


def _app_like_raw():
    return [
        _fact(*_Q1, 100, "2025-05-01"),
        _fact(*_Q2, 110, "2025-08-01", fp="Q2"),
        _fact(*_Q3, 120, "2025-11-01", fp="Q3"),
        _fact(*_FY, 460, "2026-02-20", form="10-K", fp="FY"),        # 最初の10-K: Q4 = 130
        _fact(*_FY, 300, "2027-02-20", form="10-K", fp="FY"),        # 後の10-Kで組み替え（売却した事業を除く）
    ]


def test_process_entries_keeps_latest_and_adds_first():
    out = {(e["start"], e["end"]): e for e in _process_entries(_app_like_raw(), 99, 99)}
    fy = out[_FY]
    assert fy["val"] == 300 and fy["filed"] == "2027-02-20"
    assert fy["first_val"] == 460 and fy["first_filed"] == "2026-02-20"
    q1 = out[_Q1]
    assert q1["first_val"] == q1["val"] == 100 and q1["first_filed"] == q1["filed"] == "2025-05-01"


def test_first_filed_floor_takes_minimum_across_tags():
    other_tag = [_fact(*_Q1, 999, "2025-04-20")]               # 別のタグで先に申告されていた
    floor = earliest_filed_by_period([_app_like_raw(), other_tag])
    q1 = next(e for e in _process_entries(_app_like_raw(), 99, 99, first_filed_floor=floor) if (e["start"], e["end"]) == _Q1)
    assert q1["first_filed"] == "2025-04-20"
    assert q1["first_val"] == 100                               # first_val は採用したタグの値のまま


def test_q4_uses_first_values_and_gets_positive():
    entries = _process_entries(_app_like_raw(), 99, 99)
    annual = [e for e in entries if e["is_annual"]]
    quarters = [e for e in entries if not e["is_annual"]]
    q4 = build_q4_implied_entries(annual, quarters, "revenue")[0]
    assert q4["val"] == 130                                     # 最新どうしなら 300 − 330 = −30
    assert q4["first_val"] == 130 and q4["first_filed"] == "2026-02-20"
    assert q4["filed"] == "2027-02-20"                          # filed は今の意味（年次の最新の申告）のまま
    assert "q4_basis" not in q4


def test_q4_without_first_values_falls_back_to_latest_with_mark():
    annual = [{"start": _FY[0], "end": _FY[1], "val": 300, "filed": "2027-02-20", "is_annual": True}]
    quarters = [{"start": s, "end": e, "val": v, "filed": "2025-11-01", "is_annual": False}
                for (s, e), v in ((_Q1, 100), (_Q2, 110), (_Q3, 120))]
    q4 = build_q4_implied_entries(annual, quarters, "revenue")[0]
    assert q4["val"] == -30
    assert q4["q4_basis"] == "latest"
    assert "first_val" not in q4


def test_q4_capex_abs_applies_to_first_val():
    entries = _process_entries([dict(f, val=-f["val"]) for f in _app_like_raw()], 99, 99)
    q4 = build_q4_implied_entries([e for e in entries if e["is_annual"]],
                                  [e for e in entries if not e["is_annual"]], "capital_expenditure")[0]
    assert q4["val"] == 130 and q4["first_val"] == 130


def test_ytd_difference_carries_first_val():
    """Q2 = H1 の YTD − Q1。H1 が後で修正されても first_val は最初の申告どうしの差。"""
    raw = [
        _fact(*_Q1, 100, "2025-05-01"),
        _fact("2025-01-01", "2025-06-30", 210, "2025-08-01", fp="Q2"),
        _fact("2025-01-01", "2025-06-30", 250, "2026-08-01", fp="Q2"),   # 翌年の10-Qで修正
    ]
    entries = _process_entries(raw, 99, 99)
    for norm in (l3._normalize_field_entries(entries), nz._normalize_field(entries)):
        q2 = next(e for e in norm if e["end"] == "2025-06-30" and not e["is_ytd"])
        assert q2["val"] == 150 and q2["first_val"] == 110 and q2["first_filed"] == "2025-08-01"


def test_normalizer_tail_missing_quarter_from_annual_uses_first_basis():
    """normalized の Q4 は欠落四半期の逆算（年次から末尾の四半期）でも作られる。q4_implied と同じ基準にする。"""
    raw = {"fields": {"Revenue": _process_entries(_app_like_raw(), 99, 99)}}
    out = nz.normalize("TEST", raw)["fields"]["Revenue"]
    q4 = next(e for e in out if e["end"] == "2025-12-31" and not e.get("is_annual"))
    assert q4["val"] == 130 and q4["first_val"] == 130


def test_combine_first_needs_every_term():
    a = {"first_filed": "2025-01-01", "first_val": 5}
    assert combine_first([(1, a), (-1, {"val": 1})]) == {}
    assert combine_first([(1, a), (-1, {"first_filed": "2025-02-01", "first_val": 2})]) == {"first_filed": "2025-02-01", "first_val": 3}
    assert combine_first([(1, ZERO_TERM), (1, a)]) == {"first_filed": "2025-01-01", "first_val": 5.0}


def test_derive_asof_history():
    a = {"_asof": [["2025-05-01", 100], ["2026-05-01", 90]]}
    b = {"_asof": [["2025-08-01", 30]]}
    d = derive([(1, a), (-1, b)])
    assert d["_asof"] == [["2025-08-01", 70], ["2026-05-01", 60]]
    assert value_asof(d, "2025-12-31") == 70 and value_asof(d, "2025-07-01") is None


# AVAV 型: Q1 が年内の後の 10-Q（10-K の前）で載せ直された。年次の 10-K はその後の値と同じ基準
def _avav_like_raw():
    return [
        _fact(*_Q1, 100, "2025-05-01"),
        _fact(*_Q1, 80, "2025-08-01"),                               # Q2 の 10-Q で Q1 を修正（10-K の前）
        _fact(*_Q2, 110, "2025-08-01", fp="Q2"),
        _fact(*_Q3, 120, "2025-11-01", fp="Q3"),
        _fact(*_FY, 440, "2026-02-20", form="10-K", fp="FY"),        # 修正後の Q1 を含む年次: Q4 = 130
        _fact(*_Q1, 75, "2026-05-01"),                               # 翌年の 10-Q で再び載せ直し（10-K の後）
    ]


def test_q4_uses_quarters_as_of_first_10k():
    entries = _process_entries(_avav_like_raw(), 99, 99)
    q4 = build_q4_implied_entries([e for e in entries if e["is_annual"]],
                                  [e for e in entries if not e["is_annual"]], "net_income")[0]
    assert q4["val"] == 130                                     # 10-K の時点の Q1（80）を使う。最初の申告（100）なら 110、最新（75）なら 135
    assert q4["first_val"] == 110                               # first_val は最初の申告どうし（STAGE-V2 で使う）
    assert "q4_basis" not in q4


def test_asof_is_not_saved(tmp_path, monkeypatch):
    raw = {"fields": {"Revenue": _process_entries(_app_like_raw(), 99, 99)}}
    norm = nz.normalize("TEST", raw)
    assert any("_asof" in e for e in norm["fields"]["Revenue"])
    monkeypatch.setattr(nz, "NORMALIZED_DIR", str(tmp_path))
    nz.save_normalized("TEST", norm)
    saved = json.loads((tmp_path / "TEST_quarterly_normalized.json").read_text(encoding="utf-8"))
    assert not any("_asof" in e for e in saved["fields"]["Revenue"])
    strip_asof(norm["fields"]["Revenue"])


def test_contract_accepts_and_round_trips_optional_keys():
    d = {"end": "2025-12-31", "start": "2025-10-01", "val": 1, "accn": "", "fp": "Q4", "fy": 2025, "form": "10-K",
         "filed": "2027-02-20", "period_days": 91, "is_ytd": False, "is_annual": False, "is_implied": True,
         "first_filed": "2026-02-20", "first_val": 1, "q4_basis": "latest"}
    assert FinancialEntry.from_dict(d).to_dict() == d


def test_check_restated_annual(tmp_path):
    raw = {"fields": {"Revenue": _process_entries(_app_like_raw(), 99, 99)}}
    norm = nz.normalize("APPX", raw)
    (tmp_path / "APPX_quarterly_normalized.json").write_text(json.dumps(norm), encoding="utf-8")
    out = _check_restated_annual(str(tmp_path))
    assert len(out) == 1
    t, msg = out[0]
    assert t == "APPX" and "WARN-64" in msg and "Revenue 2025-12-31" in msg and "ずれ-53.3%" in msg
    # 最初の申告の値が無い（作り直す前の）ファイルは対象にしない
    for e in norm["fields"]["Revenue"]:
        e.pop("first_val", None)
    (tmp_path / "APPX_quarterly_normalized.json").write_text(json.dumps(norm), encoding="utf-8")
    assert _check_restated_annual(str(tmp_path)) == []


def test_merged_tags_keep_first_and_history_within_chosen_tag():
    """normalized の売上は複数のタグを混ぜる。first_val・申告の履歴は採用したタグの中だけで取る（総額と純額の混在を防ぐ）。"""
    gross = [{**_fact(*_Q1, 300, "2025-05-01"), "_tag": "Revenues"}]
    net = [{**_fact(*_Q1, 100, "2025-05-01"), "_tag": "RevenueFromContract"},
           {**_fact(*_Q1, 105, "2026-05-01"), "_tag": "RevenueFromContract"}]
    q1 = _process_entries(gross + net, 99, 99)[0]
    assert q1["val"] == 105 and "_tag" not in q1
    assert q1["first_val"] == 100 and q1["_asof"] == [["2025-05-01", 100], ["2026-05-01", 105]]
