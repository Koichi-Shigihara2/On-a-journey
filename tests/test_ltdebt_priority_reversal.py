"""
tests/test_ltdebt_priority_reversal.py

[[SCHEMA-NORMALIZED-ISSUES-1]]③の回帰テスト。

quarterly.py::FIELD_CONCEPTS/_FIELD_FALLBACKSのLTDebt優先順序が、
parser.py::XBRL_MAPPING（LongTermDebtNoncurrentを優先、
LongTermDebtCurrentとの二重計上防止というBUG-NETDEBT-2の明示的な
設計意図）と逆転していた。normalized/側はLongTermDebtを先に試す設定
になっており、この配慮が反映されていなかった。

修正後は`FIELD_CONCEPTS["LTDebt"]`をLongTermDebtNoncurrentに、
`_FIELD_FALLBACKS["LTDebt"]`をLongTermDebtに変更し、parser.py側と
優先順序を統一した。

実行方法:
    python -m pytest tests/test_ltdebt_priority_reversal.py -v
"""

from common.sec_data.quarterly import build_raw_table, FIELD_CONCEPTS, _FIELD_FALLBACKS


def _entry(start, end, val, fp, form="10-Q", filed=None):
    return {
        "start": start, "end": end, "val": val, "accn": "0000000000-00-000000",
        "fp": fp, "fy": int(end[:4]), "form": form, "filed": filed or (end + "T00:00:00"),
    }


def test_field_concepts_ltdebt_primary_is_noncurrent():
    """FIELD_CONCEPTSのLTDebt primaryがLongTermDebtNoncurrentであること"""
    concept, unit = FIELD_CONCEPTS["LTDebt"]
    assert concept == "LongTermDebtNoncurrent"
    assert unit == "USD"


def test_field_fallbacks_ltdebt_fallback_is_longtermdebt():
    """_FIELD_FALLBACKSのLTDebtフォールバックがLongTermDebtであること"""
    assert _FIELD_FALLBACKS["LTDebt"] == ("LongTermDebt",)


def _minimal_us_gaap():
    """build_raw_table実行に必要な最小限の他フィールドを用意する
    （KeyError回避、CEG型の日付を使用）"""
    us_gaap = {}
    for concept in ["NetCashProvidedByUsedInOperatingActivities", "NetIncomeLoss",
                     "Revenues", "GrossProfit", "StockholdersEquity", "Assets"]:
        us_gaap[concept] = {"units": {"USD": [
            _entry("2025-01-01", "2025-12-31", 1000, "FY", form="10-K"),
        ]}}
    return us_gaap


class TestBuildRawTableAdoptsNoncurrentOnTie:
    """CEG型: LongTermDebt(合算値)・LongTermDebtNoncurrent(non-current
    単体)の両方が同数・同一最新end日で申告されている場合（同着）、
    優先順位（primary→fallback順）によりLongTermDebtNoncurrentが
    採用されること"""

    def test_noncurrent_wins_tie(self):
        us_gaap = _minimal_us_gaap()
        # LongTermDebt: current+non-current合算値（二重計上リスクのある方）
        us_gaap["LongTermDebt"] = {"units": {"USD": [
            _entry("2025-01-01", "2025-03-31", 9_000_000_000, "Q1"),
            _entry("2025-01-01", "2025-06-30", 9_100_000_000, "Q2"),
            _entry("2025-01-01", "2025-09-30", 9_200_000_000, "Q3"),
            _entry("2025-01-01", "2025-12-31", 9_300_000_000, "FY", form="10-K"),
        ]}}
        # LongTermDebtNoncurrent: non-current単体（parser.pyが優先する方）
        us_gaap["LongTermDebtNoncurrent"] = {"units": {"USD": [
            _entry("2025-01-01", "2025-03-31", 7_000_000_000, "Q1"),
            _entry("2025-01-01", "2025-06-30", 7_100_000_000, "Q2"),
            _entry("2025-01-01", "2025-09-30", 7_200_000_000, "Q3"),
            _entry("2025-01-01", "2025-12-31", 7_300_000_000, "FY", form="10-K"),
        ]}}
        company_facts = {"facts": {"us-gaap": us_gaap}}

        result = build_raw_table("TESTCO", company_facts)
        ltdebt = result["fields"]["LTDebt"]
        q3 = next(e for e in ltdebt if e["end"] == "2025-09-30")
        assert q3["val"] == 7_200_000_000, (
            "同着の場合、primaryであるLongTermDebtNoncurrent"
            "（non-current単体、二重計上なし）を採用すべき"
        )

    def test_longterm_debt_used_when_noncurrent_absent(self):
        """CEGの逆パターン: LongTermDebtNoncurrentが皆無で
        LongTermDebt(合算値)のみ申告されている場合は、フォールバックで
        LongTermDebtを採用する（回帰なしの確認）"""
        us_gaap = _minimal_us_gaap()
        us_gaap["LongTermDebt"] = {"units": {"USD": [
            _entry("2025-01-01", "2025-03-31", 9_000_000_000, "Q1"),
            _entry("2025-01-01", "2025-06-30", 9_100_000_000, "Q2"),
            _entry("2025-01-01", "2025-09-30", 9_200_000_000, "Q3"),
            _entry("2025-01-01", "2025-12-31", 9_300_000_000, "FY", form="10-K"),
        ]}}
        company_facts = {"facts": {"us-gaap": us_gaap}}

        result = build_raw_table("TESTCO", company_facts)
        ltdebt = result["fields"]["LTDebt"]
        q3 = next(e for e in ltdebt if e["end"] == "2025-09-30")
        assert q3["val"] == 9_200_000_000, (
            "LongTermDebtNoncurrentが皆無の場合はLongTermDebtへ"
            "フォールバックすべき"
        )
