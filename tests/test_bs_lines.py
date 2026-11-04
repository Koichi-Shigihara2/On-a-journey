"""申告の貸借対照表の行（bs_lines）と欠けの補い（[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] ②、2026-11-04）"""
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common.sec_data import bs_lines as bl

LB = 'xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink"'


@pytest.mark.parametrize("concept,kind", [
    ("crwv:RecourseDebtCurrent", "short_term_debt"),
    ("crwv:NonRecourseDebtNonCurrent", "long_term_debt"),
    ("us-gaap:LongTermDebtCurrent", "short_term_debt"),
    ("us-gaap:LongTermDebtNoncurrent", "long_term_debt"),
    ("us-gaap:OperatingLeaseLiabilityCurrent", None),
    ("us-gaap:FinanceLeaseLiabilityNoncurrent", None),
    ("us-gaap:InterestPayableCurrent", None),
    ("us-gaap:IntangibleAssetsNetIncludingGoodwill", None),
    ("us-gaap:IndefiniteLivedTrademarks", "intangible_assets_excl_goodwill"),
    ("us-gaap:ConvertiblePreferredStockNonredeemableOrRedeemableIssuerOptionValue", "preferred_stock"),
    ("us-gaap:RedeemableNoncontrollingInterestEquityCarryingAmount", "minority_interest"),
    ("us-gaap:TemporaryEquityCarryingAmountAttributableToParent", None),
    ("us-gaap:CashAndCashEquivalentsAtCarryingValue", "cash_and_equivalents"),
])
def test_classify(concept, kind):
    assert bl.classify(concept) == kind


def _pre(concepts):
    locs = "".join(f'<link:loc xlink:type="locator" xlink:href="x.xsd#{c}" xlink:label="L{i}"/>' for i, c in enumerate(concepts))
    return f'<link:linkbase {LB}><link:presentationLink xlink:type="extended" xlink:role="r">{locs}</link:presentationLink></link:linkbase>'.encode()


def _inst(facts):
    ctxs, els = {}, []
    for pfx, local, d, v in facts:
        ctxs[d] = (f'<xbrli:context id="c{d}"><xbrli:entity><xbrli:identifier scheme="x">1</xbrli:identifier></xbrli:entity>'
                   f'<xbrli:period><xbrli:instant>{d}</xbrli:instant></xbrli:period></xbrli:context>')
        els.append(f'<{pfx}:{local} contextRef="c{d}" unitRef="usd" decimals="-6">{v}</{pfx}:{local}>')
    return (f'<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:us-gaap="http://fasb.org/us-gaap/2024" '
            f'xmlns:crwv="http://crwv.com/2026">{"".join(ctxs.values())}{"".join(els)}</xbrli:xbrl>').encode()


def test_extract_sums_custom_debt_lines_and_zero_rule():
    pre = _pre(["us-gaap_Assets", "us-gaap_LiabilitiesAndStockholdersEquity", "us-gaap_CashAndCashEquivalentsAtCarryingValue",
                "crwv_RecourseDebtCurrent", "crwv_NonRecourseDebtCurrent", "crwv_RecourseDebtNonCurrent", "us-gaap_Goodwill"])
    inst = _inst([("us-gaap", "CashAndCashEquivalentsAtCarryingValue", "2026-06-30", 5524), ("crwv", "RecourseDebtCurrent", "2026-06-30", 1000),
                  ("crwv", "NonRecourseDebtCurrent", "2026-06-30", 6513), ("crwv", "RecourseDebtNonCurrent", "2026-06-30", 27555),
                  ("us-gaap", "Goodwill", "2026-06-30", 1101)])
    rec = {"accn": "a", "form": "10-Q", "filed": "2026-08-12", **bl.extract(pre, inst)}
    v = bl.record_values(rec)
    assert v[("short_term_debt", "2026-06-30")] == 7513 and v[("long_term_debt", "2026-06-30")] == 27555
    z = bl.zero_fields(rec)
    assert "preferred_stock" in z and "intangible_assets_excl_goodwill" in z and "short_term_debt" not in z


def test_zero_rule_blocked_by_near_name_line():
    rec = {"status": "ok", "accn": "a", "form": "10-Q", "filed": "2026-08-01",
           "lines": [{"concept": "us-gaap:CashAndCashEquivalentsAtCarryingValue", "values": {"2026-06-30": 1.0}}],
           "concepts": ["us-gaap:CashAndCashEquivalentsAtCarryingValue", "abc:TermLoanFacilityOutstanding",
                        "us-gaap:ConvertiblePreferredStockMember"]}
    z = bl.zero_fields(rec)
    assert "long_term_debt" not in z and "short_term_debt" not in z     # 借入金に近い名前の独自の行があれば0にしない
    assert "preferred_stock" in z                                      # 軸の要素（…Member）は行として数えない


def _save(tmp_path, t, recs):
    for r in recs:
        bl.save_record(t, r["accn"], r, data_dir=str(tmp_path))


def test_fill_layer3_bs_only_missing_ends(tmp_path):
    _save(tmp_path, "T", [{"status": "ok", "accn": "q2", "form": "10-Q", "filed": "2026-08-12", "concepts": ["x:Cash"],
                           "lines": [{"concept": "us-gaap:CashAndCashEquivalentsAtCarryingValue", "values": {"2026-06-30": 5.0}},
                                     {"concept": "crwv:RecourseDebtNonCurrent", "values": {"2026-06-30": 100.0, "2025-12-31": 90.0}}]}])
    fields = {"stockholders_equity": {"entries": [{"end": "2025-12-31", "val": 1.0, "fy": 2025, "fp": "FY", "is_annual": True},
                                                  {"end": "2026-06-30", "val": 1.0, "fy": 2026, "fp": "Q2", "is_annual": False}]},
              "long_term_debt": {"entries": [{"end": "2025-12-31", "val": 80.0, "source_tag": "LongTermDebtNoncurrent"}]},
              "minority_interest": {"entries": [{"end": "2025-12-31", "val": 3.0}]}}
    added = bl.fill_layer3_bs("T", fields, data_dir=str(tmp_path))
    ents = {e["end"]: e for e in fields["long_term_debt"]["entries"]}
    assert ents["2025-12-31"]["val"] == 80.0                            # companyfactsの値はそのまま
    assert ents["2026-06-30"]["val"] == 100 and ents["2026-06-30"]["source_tag"] == "bs_lines"
    mi = {e["end"]: e for e in fields["minority_interest"]["entries"]}
    assert mi["2026-06-30"]["val"] == 0 and mi["2026-06-30"]["mark"] == bl.NO_BS_LINE_MARK   # 行なし＝0（履歴のある項目）
    assert not fields["preferred_stock"]["entries"]       # 一度も申告の無い項目は足さない（ROTCEは0扱い。XOMの優先株）
    assert added["long_term_debt"] == 1


def test_latest_balance_requires_same_cash(tmp_path):
    _save(tmp_path, "T", [{"status": "ok", "accn": "q2", "form": "10-Q", "filed": "2026-08-12", "concepts": [],
                           "lines": [{"concept": "us-gaap:CashAndCashEquivalentsAtCarryingValue", "values": {"2026-06-30": 5.0}},
                                     {"concept": "crwv:RecourseDebtCurrent", "values": {"2026-06-30": 7.0}}]}])
    assert bl.latest_balance("T", 5.0, data_dir=str(tmp_path))["short_term_debt"] == 7
    assert bl.latest_balance("T", 6.0, data_dir=str(tmp_path)) == {}    # 現金が違う＝別の時点


def test_saved_records_crwv():
    lb = bl.latest_balance("CRWV", 5524000000)
    assert lb["end"] == "2026-06-30" and lb["short_term_debt"] == 7513000000 and lb["long_term_debt"] == 27555000000
    st = {}
    for t in os.listdir(bl.DATA_DIR):
        for r in bl.load_records(t).values():
            st[r["status"]] = st.get(r["status"], 0) + 1
            if r["status"] != "ok":
                assert r.get("reason")
    assert set(st) <= {"ok", "no_bs_statement", "not_found"} and st["ok"] > 2000


def _rec(accn, form, filed, inst, concepts, lines=()):
    return {"status": "ok", "accn": accn, "form": form, "filed": filed, "concepts": concepts,
            "lines": [{"concept": "us-gaap:CashAndCashEquivalentsAtCarryingValue", "values": {inst: 1.0}}] + list(lines)}


def test_zero_rule_by_category_wmt_and_pltr_types(tmp_path):
    """借入金・優先株・非支配持分は10-Qに行が無ければ0。無形資産・のれん・短期投資は直近の10-Kにも行が無く、
    10-Kで載った値（注記）も400日以内に無いときだけ0（チャット側の決定、2026-11-04 夜）"""
    cash = ["us-gaap:CashAndCashEquivalentsAtCarryingValue"]
    _save(tmp_path, "W", [_rec("k", "10-K", "2026-03-01", "2026-01-31", cash), _rec("q", "10-Q", "2026-09-01", "2026-07-31", cash)])
    eq = {"stockholders_equity": {"entries": [{"end": "2026-07-31", "val": 1.0}]},
          "long_term_debt": {"entries": [{"end": "2026-04-30", "val": 9.0}]}}
    wmt = {**eq, "intangible_assets_excl_goodwill": {"entries": [{"end": "2026-01-31", "val": 4.3e9, "form": "10-Q", "fp": "FY"}]}}
    bl.fill_layer3_bs("W", wmt, data_dir=str(tmp_path))
    assert [e["end"] for e in wmt["intangible_assets_excl_goodwill"]["entries"]] == ["2026-01-31"]   # WMT型: 0にしない
    assert {e["end"]: e["val"] for e in wmt["long_term_debt"]["entries"]}["2026-07-31"] == 0      # 借入金は0
    pltr = {**eq, "long_term_debt": {"entries": [{"end": "2026-04-30", "val": 9.0}]},
            "intangible_assets_excl_goodwill": {"entries": [{"end": "2025-09-30", "val": 1.4e7, "form": "10-Q", "fp": "Q3"}]}}
    bl.fill_layer3_bs("W", pltr, data_dir=str(tmp_path))
    got = {e["end"]: e["val"] for e in pltr["intangible_assets_excl_goodwill"]["entries"]}
    assert got["2026-07-31"] == 0                                                                  # PLTR型: 0


def test_zero_if_10k_needs_10k_without_line(tmp_path):
    cash = ["us-gaap:CashAndCashEquivalentsAtCarryingValue"]
    _save(tmp_path, "X", [_rec("k", "10-K", "2026-03-01", "2025-12-31", cash + ["us-gaap:Goodwill"],
                               [{"concept": "us-gaap:Goodwill", "values": {"2025-12-31": 5.0}}]),
                          _rec("q", "10-Q", "2026-08-01", "2026-06-30", cash)])
    recs = bl.load_records("X", data_dir=str(tmp_path))
    z = bl.zero_fields_with_10k(recs["q"], recs)
    assert "goodwill" not in z and "long_term_debt" in z     # 10-Kにのれんの行がある → 10-Qで行が無くても0にしない


def test_near_names_exclude_other_term(tmp_path):
    """短期の分類の「近い名前」に長期の行を数えない（LLY: 短期投資の行が無く長期投資の行だけ → 0）。借入金も同じ形（2026-11-04 夜）"""
    cash = "us-gaap:CashAndCashEquivalentsAtCarryingValue"
    _save(tmp_path, "L", [
        _rec("k", "10-K", "2026-02-12", "2025-12-31", [cash, "us-gaap:LongTermInvestments", "abc:ConvertibleNotesNoncurrent"]),
        _rec("q", "10-Q", "2026-10-29", "2026-09-30", [cash, "us-gaap:LongTermInvestments", "abc:ConvertibleNotesNoncurrent"])])
    recs = bl.load_records("L", data_dir=str(tmp_path))
    z = bl.zero_fields_with_10k(recs["q"], recs)
    assert "short_term_investments" in z                       # 長期投資の行は短期投資の近い名前ではない
    assert "short_term_debt" in z and "long_term_debt" not in z   # 長期の転換社債の行は、長期の借入金だけを止める
