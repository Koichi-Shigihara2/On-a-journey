"""設備投資の符号（[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段2、2026-11-03）。

- 符号の正規化（abs）は差し引きの前の申告の値（タグごとのYTD・年次）で行い、単独の四半期・Q4は符号つきで保存する
  （YTDが前の四半期より減った期は負。以前は差し引きの後に abs で正の値になっていた: ALABの2024-06-30・ONDS等）
- TTMの設備投資（4四半期の合計）が負の点は欠け（None）にして印（capex_ttm_negative）。FCFも作らない（fcf.ttm_capex の1か所）
"""
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from common.sec_data import layer3_builder as l3  # noqa: E402
from common.sec_data.fcf import CAPEX_TTM_NEGATIVE_MARK, ttm_capex  # noqa: E402
from common.sec_data.normalizer import normalize  # noqa: E402
from common.sec_data.quarterly import build_raw_table  # noqa: E402
from common.sec_data.ttm_calculator import calc_ttm_series  # noqa: E402

TAG = "PaymentsToAcquirePropertyPlantAndEquipment"


def _fact(start, end, val, form, fp, fy, filed):
    return {"start": start, "end": end, "val": val, "form": form, "fp": fp, "fy": fy, "filed": filed,
            "accn": f"0000-{filed}-{fp}", "frame": None}


def _company_facts(sign=1):
    """Q1 単独 100、半期のYTD 80（Q2は −20）、9か月のYTD 150（Q3は 70）、年次 260（Q4は 110）"""
    facts = [
        _fact("2024-01-01", "2024-03-31", 100 * sign, "10-Q", "Q1", 2024, "2024-05-01"),
        _fact("2024-01-01", "2024-06-30", 80 * sign, "10-Q", "Q2", 2024, "2024-08-01"),
        _fact("2024-01-01", "2024-09-30", 150 * sign, "10-Q", "Q3", 2024, "2024-11-01"),
        _fact("2024-01-01", "2024-12-31", 260 * sign, "10-K", "FY", 2024, "2025-02-15"),
    ]
    return {"facts": {"us-gaap": {TAG: {"units": {"USD": facts}}}}}


def _q(entries):
    return {e["end"]: e["val"] for e in entries if not e.get("is_annual")}


def test_layer3_quarter_after_ytd_decrease_is_negative_regardless_of_reported_sign():
    fd = {"unit": "USD", "candidates": [TAG], "sign_normalize": "abs"}
    for sign in (1, -1):   # 正で申告する会社・負で申告する会社のどちらでも同じ
        entries, tag = l3.extract_field_raw_entries(_company_facts(sign), fd, "capital_expenditure", "TEST")
        q = _q(entries)
        assert tag == TAG and q["2024-03-31"] == 100 and q["2024-06-30"] == -20 and q["2024-09-30"] == 70, (sign, q)
        assert [e["val"] for e in entries if e.get("is_annual")] == [260]


def test_layer2_normalizer_does_not_write_capex():
    """normalized は CapEx を書かない（[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段5d、2026-11-04。符号つきの四半期は Layer3 の側で確かめる）"""
    nz = normalize("TEST", build_raw_table("TEST", _company_facts(-1)))
    assert "CapEx" not in nz["fields"]


def _store(capex):
    ocf = [{"end": e, "start": s, "val": 50.0, "is_annual": False} for e, s, _ in capex]
    return {"fields": {"operating_cash_flow": {"entries": ocf},
                       "capital_expenditure": {"entries": [{"end": e, "start": s, "val": v, "is_annual": False}
                                                           for e, s, v in capex]}}}


def test_negative_ttm_capex_is_missing_with_mark_and_no_fcf():
    capex = [("2025-06-30", "2025-04-01", 1.0), ("2025-03-31", "2025-01-01", -9.0),
             ("2024-12-31", "2024-10-01", 2.0), ("2024-09-30", "2024-07-01", 1.0)]
    flow = calc_ttm_series("TEST", _store(capex), n_periods=1)[0]["flow"]
    c = flow["capital_expenditure"]
    assert c["val"] is None and c[CAPEX_TTM_NEGATIVE_MARK] is True and "negative_sum" not in c and c["quarters_used"] == 4
    assert "FCF" not in flow
    assert ttm_capex(0.0) == 0.0 and ttm_capex(3.0) == 3.0 and ttm_capex(-0.1) is None and ttm_capex(None) is None


def test_tanuki_ttm_reader_skips_negative_point_and_guard_reports_latest(tmp_path):
    sys.path.insert(0, os.path.join(REPO, "src", "value", "tanuki_valuation"))
    import data_fetcher as df
    ttm_dir = tmp_path / "common" / "sec_data" / "ttm"
    ttm_dir.mkdir(parents=True)

    def pt(end, fcf, neg=False):
        cap = {"val": None, "quarters_used": 4, "missing": 0, CAPEX_TTM_NEGATIVE_MARK: True} if neg else \
            {"val": 10.0, "quarters_used": 4, "missing": 0}
        flow = {"operating_cash_flow": {"val": 100.0, "quarters_used": 4, "missing": 0}, "capital_expenditure": cap}
        if fcf is not None:
            flow["FCF"] = {"val": fcf}
        return {"ttm_end": end, "flow": flow}

    from datetime import date, timedelta
    d0 = date.today() - timedelta(days=40)
    ends = [(d0 - timedelta(days=365 * i)).isoformat() for i in range(4)]
    series = [pt(ends[0], None, neg=True), pt(ends[1], 90.0), pt(ends[2], 80.0), pt(ends[3], 70.0)]
    (ttm_dir / "TEST_ttm_series.json").write_text(json.dumps({"series": series}), encoding="utf-8")
    r = df.TTMReader("TEST", str(tmp_path))
    assert r.get_fcf_series() == [90.0, 80.0, 70.0] and r.get_fcf_dates() == ends[1:]
    assert r.latest_point_missing_fields() == ["capital_expenditure"]
