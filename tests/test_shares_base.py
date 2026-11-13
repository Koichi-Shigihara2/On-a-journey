"""[[TANUKI-SHARES-SINGLE-SOURCE-1]]（2026-11-13の決定）の回帰テスト。

- 分母の株数 = 加重平均の希薄化後 Wd × (1 + 実際の発行の分 ÷ Wb)。発行が無ければ補正は0、買い戻しは下がる、閾値なし
- 二重に数えない（|発行の分| ≤ |c1−c0|）。TSLA型（表紙と加重平均の差が発行でない）は入らない
- 分母の書き手は1か所（data_fetcher._determine_diluted_shares）で、倍率の部品（時価総額と同じ値）を読む
"""
import itertools
import os

from common import multiples as M
from common import shares_base as SB

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_no_issuance_no_correction():
    r = SB.corrected_shares(wd=1000.0, wb=950.0, c0=1100.0, c1=1100.0)
    assert r["issued"] == 0.0 and r["shares"] == 1000.0


def test_buyback_lowers_shares():
    r = SB.corrected_shares(wd=1000.0, wb=1000.0, c0=1000.0, c1=980.0)
    assert r["issued"] == -20.0 and abs(r["shares"] - 980.0) < 1e-9


def test_issuance_already_in_weighted_average_is_not_added_again():
    # c0=900 → c1=1000。加重平均の基本 Wb=960 は増加100のうち60を既に含む → 残り40だけ
    r = SB.corrected_shares(wd=1010.0, wb=960.0, c0=900.0, c1=1000.0)
    assert abs(r["issued"] - 40.0) < 1e-9 and abs(r["shares"] - 1010.0 * (1 + 40 / 960)) < 1e-9


def test_tsla_type_gap_is_not_issuance():
    """表紙が加重平均より大きく離れていても（未確定の株等）、表紙の増え方（c1−c0）が小さければ補正は小さい"""
    r = SB.corrected_shares(wd=3541.0, wb=3240.0, c0=3949.5, c1=3952.8)
    assert abs(r["issued"] - 3.3) < 1e-9 and r["shares"] < 3541.0 * 1.002   # 412相当の差は入らない


def test_never_double_counts_over_grid():
    """発行の分は表紙の変化を超えない（|issued| ≤ |c1−c0|）。符号は変化と同じか0"""
    vals = [800.0, 900.0, 950.0, 1000.0, 1050.0, 1100.0, 1300.0]
    for wb, c0, c1 in itertools.product(vals, repeat=3):
        r = SB.corrected_shares(wd=wb * 1.02, wb=wb, c0=c0, c1=c1)
        assert abs(r["issued"]) <= abs(c1 - c0) + 1e-9
        assert r["issued"] * (c1 - c0) >= -1e-9


def _cf(dei, wb=None, wd=None, end="2026-09-30"):
    def row(v, **kw):
        return {"val": v, "form": "10-Q", "filed": "2026-11-01", "start": "2026-07-01", "end": end, **kw}
    g = {}
    if wb:
        g["WeightedAverageNumberOfSharesOutstandingBasic"] = {"units": {"shares": [row(wb)]}}
    if wd:
        g["WeightedAverageNumberOfDilutedSharesOutstanding"] = {"units": {"shares": [row(wd)]}}
    cover = [{"val": v, "form": "10-Q", "filed": f, "end": e} for e, f, v in dei]
    return {"facts": {"us-gaap": g, "dei": {SB.COVER_KEY: {"units": {"shares": cover}}}}}


DEI = [("2026-07-20", "2026-07-25", 900.0), ("2026-10-25", "2026-10-30", 1000.0)]


def test_base_shares_applies_and_marks():
    out = SB.base_shares(1010.0, "2026-09-30", "quarterly_weighted", _cf(DEI, wb=960.0, wd=1010.0))
    assert out["shares_basis"] == SB.SOURCE_ADJUSTED and abs(out["shares_issued"] - 40.0) < 1e-9
    assert abs(out["outstanding_gap_pct"] - (1000.0 / out["shares"] - 1) * 100) < 1e-9


def test_base_shares_marks_only_cases():
    cf = _cf(DEI, wb=960.0, wd=1010.0)
    assert SB.base_shares(1010.0, "2026-09-30", "annual_weighted", cf)["shares_note"].startswith("wd_not_quarterly")
    assert SB.base_shares(1010.0, "2026-09-30", "quarterly_weighted", _cf([]))["shares_note"] == "no_cover"
    multi = _cf(DEI + [("2026-10-25", "2026-10-30", 55.0)], wb=960.0, wd=1010.0)      # 同じ申告に2値＝種類株
    assert SB.base_shares(1010.0, "2026-09-30", "quarterly_weighted", multi)["shares_note"] == "cover_multi_class"
    sp = [{"split_date": "2026-10-01"}]
    assert SB.base_shares(1010.0, "2026-09-30", "quarterly_weighted", cf, sp)["shares_note"] == "split_after_period_end"
    for out in (SB.base_shares(1010.0, "2026-09-30", "annual_weighted", cf),):
        assert out["shares"] == 1010.0 and out["shares_issued"] == 0.0     # 補正できない時はWdのまま


def test_basic_ratio_handles_thousand_unit_basic():
    """LOAR型: 基本だけ千株単位でも、同じ期の Wb÷Wd の比で補う"""
    assert abs(SB.basic_in_units_of_wd(_cf([], wb=93.688, wd=100.0), "2026-09-30") - 0.93688) < 1e-9


def test_multiples_current_row_uses_the_same_function():
    """倍率の部品の current の株数（時価総額の株数）は shares_base.base_shares と同じ値"""
    store = {"fields": {"shares_diluted": {"entries": [{"end": "2026-09-30", "val": 1010.0, "filed": "2026-10-30",
                                                        "first_filed": "2026-10-30", "form": "10-Q",
                                                        "source_tag": "WeightedAverageNumberOfDilutedSharesOutstanding"}]}}}
    cf = _cf(DEI, wb=960.0, wd=1010.0)
    rows = [{"date": "2026-11-12", "close": 10.0}]
    out = M.compute("ZZZ", store, rows, [], [], lambda d: {}, [], current="2026-11-12", company_facts=cf)
    cur = out["current"]
    base = SB.base_shares(cur["shares_wd"], "2026-09-30", cur["shares_source"], cf)
    assert cur["shares"] == base["shares"] and cur["market_cap"] == 10.0 * base["shares"]
    assert M.at_price(cur, 20.0)["market_cap"] == 20.0 * base["shares"]
    assert M.at_price(cur, 20.0)["shares_basis"] == SB.SOURCE_ADJUSTED


def _src(rel):
    with open(os.path.join(REPO, rel), encoding="utf-8") as f:
        return f.read()


def test_denominator_has_one_writer():
    """理論株価の分母 diluted_shares を決めるのは data_fetcher._determine_diluted_shares の1か所で、部品の株数を読む。
    補正の計算（corrected_shares）は common/shares_base.py の1か所だけ"""
    s = _src("src/value/tanuki_valuation/data_fetcher.py")
    assert s.count("self._determine_diluted_shares(") == 1 and "part=mult" in s
    assert '"diluted_shares": final_shares' in s
    assert 'part["shares"]' in s and "yf_implied_fallback" in s
    for rel in ("src/value/tanuki_valuation/data_fetcher.py", "src/value/tanuki_valuation/core_calculator.py",
                "src/value/tanuki_valuation/pipeline.py", "src/value/stonks_silo/valuation_fetcher.py",
                "src/value/hypecore/hypecore.py", "common/multiples.py"):
        assert "corrected_shares(" not in _src(rel)
    assert "corrected_shares(" in _src("common/shares_base.py")


def test_determine_diluted_shares_prefers_part_and_marks_fallback():
    import sys
    d = os.path.join(REPO, "src", "value", "tanuki_valuation")
    if d not in sys.path:
        sys.path.insert(0, d)
    import data_fetcher as DF
    f = DF.TanukiDataFetcher.__new__(DF.TanukiDataFetcher)
    assert f._determine_diluted_shares("X", 999_999_999, 0, 0, part={"shares": 5e8, "shares_basis": SB.SOURCE_ADJUSTED}) \
        == (500_000_000, "sec_weighted_plus_issuance")
    assert f._determine_diluted_shares("V", 2_000_000_000, 0, 0, part={"shares": None}) == (2_000_000_000, "yf_implied_fallback")
