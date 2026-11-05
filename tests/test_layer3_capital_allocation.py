"""Layer3の資本の使い方の5項目（[[HYPECORE-STAGE-TIMESCALE-1]] 段B-3、2026-11-05）

- 定義は config/sec_concept_definitions.json の1か所（parserのタグの表には書かない）。タグの候補の順
- フローの項目は TTM の FLOW_FIELDS と Q4 の逆算（q4_implied の写像）に入る。性質上負にならない項目（non_negative）は、
  逆算したQ4が負なら欠けにして印（q4_implied_negative）。10-Kの年間の欄に1四半期分の額を申告する会社がある（CEG FY2025）
テストは作った company_facts だけを使う（本物のファイル・外部の取得に触れない）
"""
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common.sec_data import layer3_builder as lb
from common.sec_data import q4_implied
from common.sec_data import tag_definitions
from common.sec_data.ttm_calculator import FLOW_FIELDS

FIELDS = {
    "dividends_paid": ["PaymentsOfDividendsCommonStock", "PaymentsOfDividends", "PaymentsOfOrdinaryDividends"],
    "dividends_per_share": ["CommonStockDividendsPerShareDeclared", "CommonStockDividendsPerShareCashPaid"],
    "equity_issuance_proceeds": ["ProceedsFromIssuanceOfCommonStock", "ProceedsFromIssuanceOrSaleOfEquity",
                                 "ProceedsFromIssuanceOfCommonStockNetOfIssuanceCosts", "ProceedsFromIssuanceInitialPublicOffering"],
    "option_exercise_proceeds": ["ProceedsFromStockOptionsExercised",
                                 "ProceedsFromIssuanceOfSharesUnderIncentiveAndShareBasedCompensationPlans"],
    "stock_issued_acquisitions": ["StockIssuedDuringPeriodValueAcquisitions"],
}


def test_definitions_in_one_place():
    defs = lb.load_concept_definitions()["fields"]
    for f, tags in FIELDS.items():
        assert defs[f]["category"] == "flow" and defs[f]["candidates"] == tags
        assert f in FLOW_FIELDS
    # parser のタグの表（tag_definitions.TAG_CANDIDATES）には書かない（二重にしない）
    parser_tags = {t for v in tag_definitions.TAG_CANDIDATES.values() for t in v}
    assert not parser_tags & {t for tags in FIELDS.values() for t in tags}


def test_q4_implied_fields_and_non_negative():
    for f in FIELDS:
        assert q4_implied._SNAKE_TO_PASCAL[f] in q4_implied.Q4_IMPLIED_FIELDS
    # 非負の欄は今は段B-3の5項目だけ（既存の項目に当てると公開の値が変わるため当てていない）。CapExは対象外
    assert q4_implied.non_negative_fields() == frozenset(q4_implied._SNAKE_TO_PASCAL[f] for f in FIELDS)


def test_negative_q4_of_non_negative_field_is_missing_with_mark():
    def e(start, end, val, annual=False):
        return {"start": start, "end": end, "val": val, "is_annual": annual, "accn": "A", "filed": "2026-02-20"}
    annual = [e("2025-01-01", "2025-12-31", 0.5, annual=True)]
    qs = [e("2025-01-01", "2025-03-31", 0.5), e("2025-04-01", "2025-06-30", 0.5), e("2025-07-01", "2025-09-30", 0.5)]
    marks = []
    assert q4_implied.build_q4_implied_entries(annual, qs, "dividends_per_share", marks_out=marks) == []
    assert marks and marks[0]["mark"] == q4_implied.Q4_NEGATIVE_MARK and marks[0]["implied_val"] == -1.0
    # 非負でない項目（buyback）は今までどおり負のQ4も作る
    assert q4_implied.build_q4_implied_entries(annual, qs, "buyback")[0]["val"] == -1.0


ACC = {"Q1": "0000000001-25-000001", "Q2": "0000000001-25-000002", "Q3": "0000000001-25-000003", "FY": "0000000001-26-000001"}
FILED = {"Q1": "2025-05-01", "Q2": "2025-08-01", "Q3": "2025-11-01", "FY": "2026-02-20"}


def _dur(start, end, val, fp, form):
    return {"start": start, "end": end, "val": val, "accn": ACC[fp], "fy": 2025, "fp": fp, "form": form, "filed": FILED[fp]}


def _ytd(tag_vals, unit="USD"):
    """Q1・上半期・9か月の累計（10-Q）と年次（10-K）"""
    q1, h1, m9, fy = tag_vals
    return {"units": {unit: [
        _dur("2025-01-01", "2025-03-31", q1, "Q1", "10-Q"),
        _dur("2025-01-01", "2025-06-30", h1, "Q2", "10-Q"),
        _dur("2025-01-01", "2025-09-30", m9, "Q3", "10-Q"),
        _dur("2025-01-01", "2025-12-31", fy, "FY", "10-K"),
    ]}}


def _store(monkeypatch, us_gaap):
    facts = {"cik": 1, "entityName": "TEST", "facts": {"us-gaap": us_gaap, "dei": {}}}
    monkeypatch.setattr(lb, "load_company_facts", lambda t: facts)
    return lb.build_ticker_store("ZZTEST")


def _quarters(store, field):
    return {e["end"]: e["val"] for e in lb.get_field_entries(store, field) if not e.get("is_annual")}


def test_dividends_quarters_from_ytd_and_q4_implied(monkeypatch):
    s = _store(monkeypatch, {"PaymentsOfDividends": _ytd((100, 210, 330, 460)),
                             "CommonStockDividendsPerShareDeclared": _ytd((0.5, 1.0, 1.5, 0.5), unit="USD/shares")})
    q = _quarters(s, "dividends_paid")
    assert q == {"2025-03-31": 100, "2025-06-30": 110, "2025-09-30": 120, "2025-12-31": 130}   # Q4＝年次−9か月
    dps = _quarters(s, "dividends_per_share")
    assert "2025-12-31" not in dps          # 年間の欄に1四半期分（0.5）の会社でも負のQ4を作らない（欠け）
    marks = s["fields"]["dividends_per_share"]["q4_implied_negative"]
    assert [(m["end"], m["implied_val"]) for m in marks] == [("2025-12-31", -1.0)]
    assert "q4_implied_negative" not in s["fields"]["dividends_paid"]


def test_positive_q4_of_dividends_per_share_is_made(monkeypatch):
    s = _store(monkeypatch, {"CommonStockDividendsPerShareDeclared": _ytd((0.5, 1.0, 1.5, 2.1), unit="USD/shares")})
    assert abs(_quarters(s, "dividends_per_share")["2025-12-31"] - 0.6) < 1e-9


def test_candidate_order_common_stock_first(monkeypatch):
    s = _store(monkeypatch, {"PaymentsOfDividendsCommonStock": _ytd((10, 20, 30, 40)),
                             "PaymentsOfDividends": _ytd((15, 30, 45, 60))})          # 優先株の配当を含む合計
    assert _quarters(s, "dividends_paid")["2025-03-31"] == 10
