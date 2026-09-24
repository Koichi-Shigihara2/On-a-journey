"""
tests/test_net_income_parent_attribution.py

[[NET-INCOME-NCI-PARENT-ATTRIBUTION-1]]の回帰テスト。net_incomeは3系統
（parser.py・quarterly.py〈normalized〉・layer3_builder.py〈Layer3/TTM〉）とも
「親会社帰属」を正とする共通の候補定義（tag_definitions.py::
NET_INCOME_CANDIDATES、連結系タグは同じ期間・同じaccnのNCIを差し引いた
派生概念）で決まることを検証する。

3パターン（修正前から存在する各系統の入口関数だけを使う）:
  (1) FCX型: 親会社帰属タグ（NetIncomeLossAvailableToCommonStockholdersBasic）と
      NCI込みのProfitLossが並存 → 親会社帰属を採用（修正前はProfitLossを採用）
  (2) AVAV型: NCIなし、直近四半期はProfitLossのみ申告 → ProfitLossをそのまま採用し、
      直近四半期が欠落しない（単純な候補順入れ替えでは欠落する回帰があった）
  (3) 親会社帰属タグなし・NCIあり型: 同じ期間・同じaccnのNCIを差し引く。
      NCIの申告がある提出書類で当該期間のNCIが無ければ推測せず採用しない
  (4) NCI符号逆型（FCXのFY2021 10-K実例）: NCIが負値でタグ付けされていても、
      同じ期間・同じaccnの親会社帰属タグの値を優先する（機械的な控除では
      5,365-(-1,059)=6,424と誤る）

実行方法:
    python -m pytest tests/test_net_income_parent_attribution.py -v
"""

import contextlib
import io

import pytest

from common.sec_data import layer3_builder
from common.sec_data.layer3_builder import build_ticker_store, get_quarterly_series
from common.sec_data.parser import SECParser
from common.sec_data.quarterly import build_raw_table

_TICKER = "ZZNI"


def _f(start, end, val, accn, form, fp, fy, filed):
    return {"start": start, "end": end, "val": val, "accn": accn,
            "form": form, "fp": fp, "fy": fy, "filed": filed}


def _facts(tags: dict) -> dict:
    return {"cik": 1, "entityName": "Test Co",
            "facts": {"us-gaap": {t: {"units": {"USD": v}} for t, v in tags.items()}}}


# 10-K（FY2025）と10-Q（2025Q3・2026Q1）の最小構成。Q3はstandalone＋YTD、
# FYはQ4 implied算出用。
_K, _Q3, _Q1 = "0000000001-26-000012", "0000000001-25-000050", "0000000001-26-000025"


def _period_facts(fy, q3, ytd9, q1, accns=(_K, _Q3, _Q1)):
    """fy/q3/ytd9/q1の値（Noneなら出さない）から1タグ分のファクト列を作る"""
    k, a3, a1 = accns
    out = []
    if fy is not None:
        out.append(_f("2025-01-01", "2025-12-31", fy, k, "10-K", "FY", 2025, "2026-02-13"))
    if q3 is not None:
        out.append(_f("2025-07-01", "2025-09-30", q3, a3, "10-Q", "Q3", 2025, "2025-11-06"))
    if ytd9 is not None:
        out.append(_f("2025-01-01", "2025-09-30", ytd9, a3, "10-Q", "Q3", 2025, "2025-11-06"))
    if q1 is not None:
        out.append(_f("2026-01-01", "2026-03-31", q1, a1, "10-Q", "Q1", 2026, "2026-05-08"))
    return out


_PATTERNS = {
    # (1) FCX型（値はFCXの10-K/10-Qの実数、百万ドル）
    "fcx": _facts({
        "ProfitLoss": _period_facts(4152, 1247, 3587, 1387),
        "NetIncomeLossAttributableToNoncontrollingInterest": _period_facts(1948, 573, 1789, 506),
        "NetIncomeLossAvailableToCommonStockholdersBasic": _period_facts(2204, 674, 1798, 881),
    }),
    # (2) AVAV型: NCIなし。親会社帰属タグはFYのみ、四半期はProfitLossのみ
    "avav": _facts({
        "NetIncomeLossAvailableToCommonStockholdersBasic": _period_facts(-265, None, None, None),
        "ProfitLoss": _period_facts(-265, -40, -120, -30),
    }),
    # (3) 親会社帰属タグなし・NCIあり。Q1（accn _Q1）はNCIの申告自体はあるが
    #     Q1期間の値が無い（別期間のみ）→ 推測で埋めず採用しない
    "nci_only": _facts({
        "ProfitLoss": _period_facts(1000, 300, 800, 250),
        "NetIncomeLossAttributableToNoncontrollingInterest": (
            _period_facts(100, 30, 80, None)
            + [_f("2025-01-01", "2025-03-31", 20, _Q1, "10-Q", "Q1", 2026, "2026-05-08")]
        ),
    }),
    # (4) NCI符号逆型: FYのみNCIが負値（FCX FY2021 10-Kの実数を流用）
    "nci_sign_flipped": _facts({
        "ProfitLoss": _period_facts(5365, 1247, 3587, 1387),
        "NetIncomeLossAttributableToNoncontrollingInterest": _period_facts(-1059, 573, 1789, 506),
        "NetIncomeLossAvailableToCommonStockholdersBasic": _period_facts(4306, 674, 1798, 881),
    }),
}

# 期待値: (FY2025, 2025Q3 standalone, 2026Q1 standalone) の親会社帰属純利益
_EXPECTED = {
    "fcx": (2204, 674, 881),
    "avav": (-265, -40, -30),
    "nci_only": (900, 270, None),
    "nci_sign_flipped": (4306, 674, 881),
}


def _quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a, **k)


def _by_end(entries, key_start="start"):
    return {(e.get(key_start), e.get("end")): e.get("val") for e in entries}


@pytest.mark.parametrize("pattern", sorted(_PATTERNS))
def test_parser_annual_is_parent_attributable(pattern, tmp_path):
    parser = SECParser(data_dir=str(tmp_path))
    parsed = _quiet(parser._parse_raw_data, _TICKER, _PATTERNS[pattern])
    annual = parsed["annual"]
    assert 2025 in annual, sorted(annual)
    assert annual[2025]["pl"].get("net_income") == _EXPECTED[pattern][0]


@pytest.mark.parametrize("pattern", sorted(_PATTERNS))
def test_normalized_quarters_are_parent_attributable(pattern):
    raw = _quiet(build_raw_table, _TICKER, _PATTERNS[pattern])
    got = _by_end(raw["fields"].get("NetIncome", []))
    fy, q3, q1 = _EXPECTED[pattern]
    assert got.get(("2025-01-01", "2025-12-31")) == fy
    assert got.get(("2025-07-01", "2025-09-30")) == q3
    # (2)では直近四半期（2026Q1）が欠落しないこと、(3)では推測で埋めないこと
    assert got.get(("2026-01-01", "2026-03-31")) == q1


@pytest.mark.parametrize("pattern", sorted(_PATTERNS))
def test_layer3_quarters_are_parent_attributable(pattern, monkeypatch):
    monkeypatch.setattr(layer3_builder, "load_company_facts", lambda t: _PATTERNS[pattern])
    store = _quiet(build_ticker_store, _TICKER)
    got = _by_end(get_quarterly_series(store, "net_income"))
    _, q3, q1 = _EXPECTED[pattern]
    assert got.get(("2025-07-01", "2025-09-30")) == q3
    assert got.get(("2026-01-01", "2026-03-31")) == q1


def test_layer3_json_has_no_net_income_candidate_list():
    """JSON側の候補リストは廃止され、3系統が同じ共通定義を参照する"""
    import json
    from common.sec_data.parser import SECParser as _P
    from common.sec_data.quarterly import FIELD_CONCEPTS, _FIELD_FALLBACKS
    from common.sec_data.tag_definitions import NET_INCOME_CANDIDATES
    with open(layer3_builder.CONFIG_PATH, encoding="utf-8") as f:
        raw = json.load(f)
    assert "candidates" not in raw["fields"]["net_income"]
    defs = layer3_builder.load_concept_definitions()
    # 3系統とも同一の候補定義（順序込み）を参照していること
    assert defs["fields"]["net_income"]["candidates"] == list(NET_INCOME_CANDIDATES)
    assert _P.XBRL_MAPPING["net_income"] == list(NET_INCOME_CANDIDATES)
    assert (FIELD_CONCEPTS["NetIncome"][0],) + tuple(_FIELD_FALLBACKS["NetIncome"]) == NET_INCOME_CANDIDATES


def test_layer3_json_candidates_match_tag_definitions():
    """net_income以外でLayer3のJSONとtag_definitions.pyの両方に定義がある
    フィールドは、候補リスト（順序込み）が一致していること（二重管理の
    ずれを検知する）"""
    import json
    from common.sec_data.tag_definitions import TAG_CANDIDATES
    with open(layer3_builder.CONFIG_PATH, encoding="utf-8") as f:
        fields = json.load(f)["fields"]
    pairs = {
        "gross_profit": "GROSS_PROFIT",
        "operating_cash_flow": "OPERATING_CASH_FLOW",
        "stock_based_compensation": "STOCK_BASED_COMPENSATION",
        "capital_expenditure": "CAPITAL_EXPENDITURE",
        "finance_lease_payments": "FINANCE_LEASE_PAYMENTS",
        "buyback": "BUYBACK",
    }
    mismatches = {f: (fields[f].get("candidates"), list(TAG_CANDIDATES[k]))
                  for f, k in pairs.items()
                  if fields[f].get("candidates") != list(TAG_CANDIDATES[k])}
    assert mismatches == {}
