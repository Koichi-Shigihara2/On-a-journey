"""
tests/test_layer3_ticker_overrides_single_source.py

[[TICKER-OVERRIDES-SINGLE-SOURCE-1]]の回帰テスト。銘柄別の上書き設定は
quarterly.py::TICKER_RESTRICTIONSが唯一の正で、layer3_builder.pyも同じ定義を
直接読む（JSON側の写しticker_overridesは廃止）。2026-07-24の移行以降に
TICKER_RESTRICTIONSへ追加されたCPRT/HEI/CEG/JOBY/LYFTの設定がLayer3に
反映されていなかった問題の再発防止。

実行方法:
    python -m pytest tests/test_layer3_ticker_overrides_single_source.py -v
"""

import json

import pytest

from common.sec_data import layer3_builder as lb
from common.sec_data.quarterly import TICKER_RESTRICTIONS


def test_json_has_no_ticker_overrides_copy():
    with open(lb.CONFIG_PATH, encoding="utf-8") as f:
        raw = json.load(f)
    assert "ticker_overrides" not in raw


@pytest.mark.parametrize("ticker,field,expected", [
    # 2026-07-24移行以降に追加され、Layer3に反映されていなかった設定
    ("LYFT", "capital_expenditure", ("override_concept", "CapitalizedComputerSoftwareAdditions")),
    ("CPRT", "cash_and_equivalents", ("override_concept", "CashAndCashEquivalentsAtCarryingValue")),
    ("HEI", "cash_and_equivalents", ("override_concept", "CashAndCashEquivalentsAtCarryingValue")),
    ("CPRT", "cost_of_revenue", ("append_concept", "CostDirectMaterial")),
    ("CEG", "cost_of_revenue", ("append_concept", "CostDirectMaterial")),
    ("JOBY", "cost_of_revenue", ("append_concept", "OtherCostAndExpenseOperating")),
    # 従来からJSONに移行済みだった設定（挙動が変わらないこと）
    ("MSFT", "depreciation_and_amortization", ("exclude", None)),
    ("APP", "capital_expenditure", ("exclude", None)),
    ("SOFI", "revenue", ("override_concept", "RevenuesNetOfInterestExpense")),
    ("SOFI", "long_term_debt", ("override_concept", "DebtLongtermAndShorttermCombinedAmount")),
    ("IONQ", "revenue", ("override_concept", "RevenueFromContractWithCustomerExcludingAssessedTax")),
    ("KLAC", "short_term_investments", ("override_concept", "AvailableForSaleSecuritiesDebtSecurities")),
])
def test_override_derived_from_ticker_restrictions(ticker, field, expected):
    ov = lb._get_ticker_field_override({}, ticker, field)
    assert ov is not None
    assert (ov["action"], ov.get("override_concept")) == expected


def test_every_concept_key_in_ticker_restrictions_reaches_layer3():
    """TICKER_RESTRICTIONSの*_concept・excludeは全件Layer3の上書きに反映される
    （新しいキーを追加したのにLayer3側の対応表を更新し忘れると失敗する）"""
    missing = []
    for ticker, r in TICKER_RESTRICTIONS.items():
        for key, val in r.items():
            if key.endswith("_concept"):
                assert key in lb._TICKER_CONCEPT_OVERRIDE_KEYS, f"未対応キー: {ticker}.{key}"
                field = lb._TICKER_CONCEPT_OVERRIDE_KEYS[key][0]
                if lb._get_ticker_field_override({}, ticker, field) is None:
                    missing.append((ticker, key))
            elif key == "exclude":
                for pascal in val:
                    field = lb._PASCAL_TO_SNAKE.get(pascal, pascal)
                    ov = lb._get_ticker_field_override({}, ticker, field)
                    if not ov or ov["action"] != "exclude":
                        missing.append((ticker, pascal))
    assert missing == []


def test_no_override_for_unlisted_ticker():
    assert lb._get_ticker_field_override({}, "AAPL", "revenue") is None
