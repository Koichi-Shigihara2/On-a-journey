"""
tests/test_layer3_shares_non_additive.py

[[LAYER2-SHARES-UNIT-THOUSANDS-1]]（2026-10-10）: Layer3の株式数（足し算できない値）に
YTD→単四半期変換（累計−前四半期）をかけない。

ESTC 2022-10-31: 上半期の加重平均94,964,423 − 第1四半期の加重平均94,621,365 = 343,058
という意味のない値が四半期の株式数として入っていた。

実行方法:
    python -m pytest tests/test_layer3_shares_non_additive.py -v
"""

from common.sec_data import layer3_builder as lb

W = "WeightedAverageNumberOfDilutedSharesOutstanding"


def _f(start, end, val, fp, form="10-Q", filed=None, accn="0001-22-1"):
    return {"start": start, "end": end, "val": val, "fp": fp, "fy": 2023, "form": form,
            "filed": filed or end, "accn": accn}


def _facts(entries):
    return {"facts": {"us-gaap": {W: {"units": {"shares": entries}}}}}


def _quarterly(entries, field="shares_diluted"):
    field_def = lb.load_concept_definitions()["fields"][field]
    out, _ = lb.extract_field_raw_entries(_facts(entries), field_def, field, "TEST")
    if field in lb.NO_CANDIDATE_MERGE_FIELDS:
        out = lb._normalize_field_entries(out, additive=field not in lb.NON_ADDITIVE_FIELDS)
    return {e["end"]: e["val"] for e in out if not e.get("is_annual") and not e.get("is_ytd")}


def test_ytd_not_converted_for_shares():
    # ESTC 2022年度第2四半期の型: 単独四半期の値がなく、上半期累計（6か月の加重平均）だけある
    entries = [
        _f("2022-05-01", "2022-07-31", 94_621_365, "Q1"),
        _f("2022-05-01", "2022-10-31", 94_964_423, "Q2", filed="2022-12-02"),
    ]
    q = _quarterly(entries)
    assert q.get("2022-07-31") == 94_621_365
    assert "2022-10-31" not in q          # 343,058（累計−第1四半期）を作らない
    assert 343_058 not in q.values()


def test_nine_month_ytd_not_converted():
    # ESTC 2022-01-31の型: 9か月累計だけ（単独四半期なし）
    entries = [
        _f("2021-05-01", "2021-07-31", 91_201_372, "Q1"),
        _f("2021-08-01", "2021-10-31", 92_206_199, "Q2"),
        _f("2021-05-01", "2021-10-31", 91_703_786, "Q2"),
        _f("2021-05-01", "2022-01-31", 92_140_919, "Q3", filed="2022-03-10"),
    ]
    q = _quarterly(entries)
    assert "2022-01-31" not in q
    assert q["2021-10-31"] == 92_206_199    # 単独四半期の値はそのまま


def test_additive_fields_still_converted():
    # 金額（足し算できる値）は従来どおりYTD→単四半期変換する
    raw = [
        {"start": "2022-05-01", "end": "2022-07-31", "val": 100, "is_annual": False, "is_ytd": False,
         "filed": "a", "fp": "Q1", "form": "10-Q", "accn": "x", "period_days": 91},
        {"start": "2022-05-01", "end": "2022-10-31", "val": 250, "is_annual": False, "is_ytd": True,
         "filed": "b", "fp": "Q2", "form": "10-Q", "accn": "y", "period_days": 183},
    ]
    out = {e["end"]: e["val"] for e in lb._normalize_field_entries(raw) if not e.get("is_ytd")}
    assert out["2022-10-31"] == 150
    out_na = {e["end"]: e["val"] for e in lb._normalize_field_entries(raw, additive=False)}
    assert "2022-10-31" not in out_na


def test_non_additive_fields_match_shares_category():
    fields = lb.load_concept_definitions()["fields"]
    assert lb.NON_ADDITIVE_FIELDS == {k for k, v in fields.items() if v.get("category") == "shares"}
