"""
tests/test_share_unit_fix.py

[[LAYER2-SHARES-UNIT-THOUSANDS-1]]（2026-10-10）: 同じ期間の株式数が書類によって1,000倍・100万倍
ずれていたら、その概念の中央値に近い方（ほとんどは大きい方）を採る（common/sec_data/share_unit_fix.py）。

実行方法:
    python -m pytest tests/test_share_unit_fix.py -v
"""

from common.sec_data.share_unit_fix import resolve_share_unit_conflicts, with_share_unit_fix

W = "WeightedAverageNumberOfDilutedSharesOutstanding"
CS = "CommonStockSharesOutstanding"


def _f(end, val, filed, accn, start="2025-04-01"):
    d = {"end": end, "val": val, "filed": filed, "accn": accn, "form": "10-Q"}
    if start:
        d["start"] = start
    return d


def _history(level, n=8, tag_start=True):
    """前後の期（中央値を決める正しい値の並び）"""
    return [_f(f"20{20 + i // 4}-{3 * (i % 4) + 3:02d}-28", level + i, f"20{20 + i // 4}-12-01", f"h{i}",
               start=("2020-01-01" if tag_start else None)) for i in range(n)]


def _vals(us_gaap, tag, end):
    return sorted(f["val"] for f in us_gaap[tag]["units"]["shares"] if f["end"] == end)


def test_restated_comparative_wrong():
    # CIX 2025-06-30型: 元の10-Qは12,321,000、翌年の10-Q（比較期間）が12,321
    facts = _history(12_300_000) + [
        _f("2025-06-30", 12_321_000, "2025-08-05", "orig"),
        _f("2025-06-30", 12_321, "2026-08-04", "later"),
    ]
    out, log = resolve_share_unit_conflicts({W: {"units": {"shares": facts}}})
    assert _vals(out, W, "2025-06-30") == [12_321_000, 12_321_000]
    assert len(log) == 1
    assert log[0]["rejected_val"] == 12_321 and log[0]["rejected_accn"] == "later"
    assert log[0]["adopted_val"] == 12_321_000 and log[0]["adopted_accn"] == "orig"


def test_original_filing_wrong():
    # TER 2023Q3型: 元の10-Qが164,050、翌年の10-Qが164,050,000
    facts = _history(160_000_000) + [
        _f("2023-10-01", 164_050, "2023-11-03", "orig"),
        _f("2023-10-01", 164_050_000, "2024-11-01", "later"),
    ]
    out, log = resolve_share_unit_conflicts({W: {"units": {"shares": facts}}})
    assert _vals(out, W, "2023-10-01") == [164_050_000, 164_050_000]
    fixed = [f for f in out[W]["units"]["shares"] if f.get("share_unit_fix")]
    assert fixed[0]["accn"] == "orig"             # 書類の情報は残し、値だけ置き換える
    assert fixed[0]["share_unit_fix"]["adopted_accn"] == "later"


def test_million_ratio():
    # FCX 2009型: 百万株単位（426）と株数（426,000,000）
    facts = _history(420_000_000) + [
        _f("2009-06-30", 426, "2010-08-06", "a"), _f("2009-06-30", 426_000_000, "2009-08-07", "b")]
    out, _ = resolve_share_unit_conflicts({W: {"units": {"shares": facts}}})
    assert _vals(out, W, "2009-06-30") == [426_000_000, 426_000_000]


def test_not_applied_to_split_ratios():
    # 分割の前後（25倍・10倍）や、単位と分割が重なった比（2,002,114倍）には適用しない
    for small, big in ((32_558_000, 813_950_000), (131_750_000, 1_317_500_000), (473, 947_000_000)):
        facts = _history(small) + [_f("2025-06-30", small, "2025-08-01", "a"),
                                   _f("2025-06-30", big, "2026-08-01", "b")]
        out, log = resolve_share_unit_conflicts({W: {"units": {"shares": facts}}})
        assert log == []
        assert _vals(out, W, "2025-06-30") == sorted([small, big])


def test_larger_value_can_be_the_error():
    # MSCI 2014-12-31型: 1つの10-Qだけが1,000倍（112,072,469,000）→ 中央値に近い小さい方を採る
    facts = _history(110_000_000, tag_start=False) + [
        _f("2014-12-31", 112_072_469, "2015-02-27", "k", start=None),
        _f("2014-12-31", 112_072_469_000, "2015-05-01", "q", start=None),
    ]
    out, log = resolve_share_unit_conflicts({CS: {"units": {"shares": facts}}})
    assert _vals(out, CS, "2014-12-31") == [112_072_469, 112_072_469]
    assert log[0]["rejected_val"] == 112_072_469_000


def test_different_periods_not_mixed():
    # 期間（start）が違う値どうしは比べない
    facts = _history(12_000_000) + [
        _f("2025-06-30", 12_321_000, "2025-08-05", "a", start="2025-04-01"),
        _f("2025-06-30", 12_320, "2026-08-04", "b", start="2025-01-01"),
    ]
    _, log = resolve_share_unit_conflicts({W: {"units": {"shares": facts}}})
    assert log == []


def test_amount_tags_untouched():
    cf = {"facts": {"us-gaap": {"NetIncomeLoss": {"units": {"USD": [
        {"start": "2025-04-01", "end": "2025-06-30", "val": 1_000, "filed": "a", "accn": "a"},
        {"start": "2025-04-01", "end": "2025-06-30", "val": 1_000_000, "filed": "b", "accn": "b"}]}}}}}
    out, log = with_share_unit_fix(cf)
    assert log == [] and out is cf
