"""
tests/test_sec_retag_reconcile.py

[[SEC-RETAG-RECONCILE-1]]（2026-11-24）:
  1. 再タグの突き合わせ（layer3_builder.retag_alias_rows）: 最新の申告で候補のタグが別のタグに付け替わったとき、同じ申告の前年同期の
     比較値が前の申告の候補のタグの値と一致する場合だけ、別のタグの値を採る（COHR型）。作りのデータ（銘柄名は使わない）
  2. 合計のタグが部分の値（減価償却だけ）のとき、無形資産の償却が別にあれば和を採る（ENTG型）。合計が本物の期は合計のまま。
     部分の1つしか無い期は値を変えず印（partial_concept_only）
  3. bs_lines: MarketableSecurities（Current無し）を短期投資に分類する。保存済みの申告の行はcompanyfactsから埋め戻す（SECを呼ばない）
  4. quarterly_bs: parser の四半期のD&Aが部分のタグの値のとき、Layer3の値で置き換える

実行方法:
    python -m pytest tests/test_sec_retag_reconcile.py -v
"""
import contextlib
import io

import pytest

from common.sec_data import bs_lines as bl
from common.sec_data import layer3_builder as lb
from common.sec_data import quarterly_bs as qb

AMORT = "AmortizationOfIntangibleAssets"
DEP = "Depreciation"
OTHER_AMORT = "OtherAmortizationOfDeferredCharges"
DDA = "DepreciationDepletionAndAmortization"


def _r(start, end, val, accn, filed, form="10-Q", fp="Q3"):
    return {"start": start, "end": end, "val": val, "accn": accn, "filed": filed, "form": form, "fp": fp, "fy": int(end[:4])}


def _facts(tags):
    return {"facts": {"us-gaap": {k: {"units": {"USD": v}} for k, v in tags.items()}}}


def _store(monkeypatch, facts):
    monkeypatch.setattr(lb, "load_company_facts", lambda t: facts)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return lb.build_ticker_store("TEST")


def _da(store):
    return {e["end"]: e for e in lb.get_field_entries(store, "depreciation_and_amortization")
            if not e.get("is_annual") and not e.get("is_ytd")}


# ─── 1. 再タグの突き合わせ ───
def _retag_facts(new_val_prior=100.0, with_second_alias=False):
    """前の申告（a1: 2026-Q2の10-Q）が AMORT を報告し、最新の申告（a2: 2026-Q3の10-Q）では AMORT が無く OTHER_AMORT に付け替わる。
    a2 の前年同期（2025-07-01〜09-30）の比較値 = a1 が報告していた AMORT の同じ期の値"""
    amort = [_r("2025-07-01", "2025-09-30", 100.0, "a0", "2025-11-01"),
             _r("2026-04-01", "2026-06-30", 110.0, "a1", "2026-08-01", fp="Q2")]
    other = [_r("2025-07-01", "2025-09-30", new_val_prior, "a2", "2026-11-01"),
             _r("2026-07-01", "2026-09-30", 120.0, "a2", "2026-11-01")]
    dep = [_r("2025-07-01", "2025-09-30", 50.0, "a0", "2025-11-01"), _r("2026-04-01", "2026-06-30", 55.0, "a1", "2026-08-01", fp="Q2"),
           _r("2026-07-01", "2026-09-30", 58.0, "a2", "2026-11-01")]
    tags = {AMORT: amort, OTHER_AMORT: other, DEP: dep}
    if with_second_alias:
        tags["OtherDepreciationAndAmortization"] = list(other)
    return _facts(tags)


def _alias(facts, concept=AMORT):
    cands = lb.load_concept_definitions()["fields"]["depreciation_and_amortization"]["candidates"]
    raw = facts["facts"]["us-gaap"][concept]["units"]["USD"]
    out = lb.retag_alias_rows(facts, concept, raw, "USD", cands, "depreciation_and_amortization")
    return out[len(raw):]


def test_retag_adopted_when_comparative_ties_out():
    extra = _alias(_retag_facts())
    assert [(e["end"], e["val"], e["retag_alias"]) for e in extra] == [("2026-09-30", 120.0, OTHER_AMORT)]


def test_retag_not_adopted_when_comparative_differs():
    assert _alias(_retag_facts(new_val_prior=130.0)) == []


def test_retag_not_adopted_when_two_aliases_tie():
    assert _alias(_retag_facts(with_second_alias=True)) == []


def test_retag_ignores_name_outside_the_field_hint():
    f = _retag_facts()
    f["facts"]["us-gaap"]["SomethingElse"] = f["facts"]["us-gaap"].pop(OTHER_AMORT)
    assert _alias(f) == []


def test_retag_only_looks_at_recent_filings():
    f = _retag_facts()
    # 付け替えの申告の後に、同じタグを持たない申告が3本続くと、付け替えの申告は「最新の3本」から外れる
    pad = [_r("2026-10-01", "2026-12-31", 1.0, f"p{i}", f"2027-0{i + 1}-01") for i in range(3)]
    f["facts"]["us-gaap"]["Revenues"] = {"units": {"USD": pad}}
    assert _alias(f) == []


def test_retag_fills_layer3_latest_quarter(monkeypatch):
    st = _store(monkeypatch, _retag_facts())
    e = _da(st)["2026-09-30"]
    assert e["val"] == 120.0 + 58.0                       # 減価償却 58 + 付け替えた償却 120
    assert e["retag_alias"] == OTHER_AMORT and e["partial_sum"]


def test_layer3_unchanged_without_retag(monkeypatch):
    f = _retag_facts()
    del f["facts"]["us-gaap"][OTHER_AMORT]
    st = _store(monkeypatch, f)
    e = _da(st).get("2026-09-30")
    assert e is None or not e.get("retag_alias")


# ─── 2. 合計のタグが部分（ENTG型） ───
def _entg_facts(total_val):
    mk = lambda v: [_r("2025-07-01", "2025-09-30", v, "a1", "2025-11-01"), _r("2026-04-01", "2026-06-30", v, "a2", "2026-08-01", fp="Q2")]  # noqa: E731
    return _facts({DDA: mk(total_val), DEP: mk(100.0), AMORT: mk(60.0)})


def test_total_tag_equal_to_a_part_is_replaced_by_the_sum(monkeypatch):
    e = _da(_store(monkeypatch, _entg_facts(100.0)))["2026-06-30"]
    assert e["val"] == 160.0 and e["partial_sum"] and e["total_tag_was_partial"] == DDA


def test_real_total_tag_is_kept(monkeypatch):
    e = _da(_store(monkeypatch, _entg_facts(160.0)))["2026-06-30"]
    assert e["val"] == 160.0 and e["source_tag"] == DDA and not e.get("partial_sum")


def test_single_part_keeps_value_and_is_marked(monkeypatch):
    f = _facts({DEP: [_r("2025-07-01", "2025-09-30", 100.0, "a1", "2025-11-01")]})
    e = _da(_store(monkeypatch, f))["2025-09-30"]
    assert e["val"] == 100.0 and e["partial_concept_only"] is True


# ─── 3. bs_lines ───
def test_marketable_securities_without_current_is_short_term_investments():
    assert bl.classify("us-gaap:MarketableSecurities") == "short_term_investments"
    assert bl.classify("us-gaap:MarketableSecuritiesCurrent") == "short_term_investments"
    # 注記の合計（現金同等物を含む）は貸借対照表の行ではないので足さない
    assert bl.classify("us-gaap:AvailableForSaleSecuritiesDebtSecurities") is None


def test_backfill_adds_only_the_new_line_from_company_facts():
    rec = {"status": "ok", "accn": "a1", "concepts": ["us-gaap:MarketableSecurities", "us-gaap:Goodwill"],
           "lines": [{"concept": "us-gaap:CashAndCashEquivalentsAtCarryingValue", "values": {"2026-09-30": 5.0}}]}
    facts = _facts({"MarketableSecurities": [{"end": "2026-09-30", "val": 178.0, "accn": "a1"},
                                             {"end": "2026-09-30", "val": 1.0, "accn": "other"}],
                    "Goodwill": [{"end": "2026-09-30", "val": 9.0, "accn": "a1"}]})
    assert bl.backfill_lines_from_company_facts(rec, facts) == 1                  # Goodwillは対象の名前でないので触らない
    assert {x["concept"]: x["values"] for x in rec["lines"]}["us-gaap:MarketableSecurities"] == {"2026-09-30": 178.0}
    assert bl.backfill_lines_from_company_facts(rec, facts) == 0                  # 2回目は足さない


def test_backfill_skips_when_no_value():
    rec = {"status": "ok", "accn": "a1", "concepts": ["us-gaap:MarketableSecurities"], "lines": []}
    assert bl.backfill_lines_from_company_facts(rec, _facts({})) == 0
    assert rec["lines"] == []


# ─── 4. quarterly_bs（parser の四半期のD&A） ───
def test_parser_quarterly_da_is_replaced_by_layer3_value():
    store = {"fields": {k: {"entries": [{"end": "2026-06-30", "val": v, "is_annual": False, "is_ytd": False}]}
                        for k, v in {"depreciation_and_amortization": 160.0, "operating_cash_flow": 1000.0,
                                     "stock_based_compensation": 30.0, "buyback": 7.0,
                                     "cash_and_equivalents": 500.0}.items()}}
    rec = {"period": "2026Q2", "bs": {"cash_and_equivalents": 500.0},
           "cf": {"depreciation_and_amortization": 100.0, "operating_cash_flow": 1000.0, "stock_based_compensation": 30.0, "buyback": 7.0}}
    changed = qb.replace_record(rec, store)
    assert rec["cf"]["depreciation_and_amortization"] == 160.0
    assert changed == [("depreciation_and_amortization", 100.0, 160.0)]
    assert rec["cf"]["operating_cash_flow"] == 1000.0 and rec["cf"]["buyback"] == 7.0       # D&A以外のCFは置き換えない
