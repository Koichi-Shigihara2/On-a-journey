"""
tests/test_layer3_annual_misclassification.py

[[LAYER3-ANNUAL-MISCLASSIFICATION-NOW-RMBS-1]]・
[[LAYER3-ANNUAL-MISCLASSIFICATION-MINOR-5TICKERS-1]]の回帰テスト。

layer3_builder.py::_reclassify_misannotated_fy_entries()が、
_ANNUAL_MISCLASSIFICATION_FIX_TICKERSに含まれるticker（BBAI・DELL・
SPIR、2026-08-30にDELL・SPIRを追加）に対して、同一accn・同一start日内
に複数end日付を持つfp=='FY'・130<period_days<=300のis_annual=True
エントリ（YTD比較開示の混入）を正しく除外することを確認する。

修正前（DELL・SPIRが_ANNUAL_MISCLASSIFICATION_FIX_TICKERSに未登録
だった状態）は、DELL/SPIRのケースで対象外tickerとして入力がそのまま
返され、重複エントリが除外されなかった。本テストはその状態から
「除外される」状態への回帰を検証する。

実行方法:
    python -m pytest tests/test_layer3_annual_misclassification.py -v
"""

from common.sec_data.layer3_builder import (
    _reclassify_misannotated_fy_entries,
    _ANNUAL_MISCLASSIFICATION_FIX_TICKERS,
)


def _entry(end, start, val, accn, fp="FY", period_days=181, is_annual=True, form="10-K", **overrides):
    e = {
        "end": end, "start": start, "fp": fp, "fy": 2024, "form": form,
        "filed": "2025-03-25", "period_days": period_days, "is_ytd": False,
        "is_annual": is_annual, "val": val, "accn": accn,
    }
    e.update(overrides)
    return e


class TestFrozensetMembership:
    def test_dell_is_in_fix_set_and_deleted_tickers_are_not(self):
        """DELLが対象ticker。BBAI・SPIRは2026-11-23に登録を抹消したので入っていない（UNIVERSE-REBALANCE-2026Q4-1）"""
        assert "DELL" in _ANNUAL_MISCLASSIFICATION_FIX_TICKERS
        assert "SPIR" not in _ANNUAL_MISCLASSIFICATION_FIX_TICKERS
        assert "BBAI" not in _ANNUAL_MISCLASSIFICATION_FIX_TICKERS

    def test_rmbs_asts_vrt_now_meta_not_added(self):
        """単一end日パターン（別種の症状）・自然解消した銘柄は対象外のまま
        （誤って対象に加えると同一end日1件のみのグループは判定基準
        〈複数end日〉に合致せず何も除外されない＝無効果な変更になる
        ため、実際に判定ロジックが効くtickerのみを対象にする）"""
        for t in ("RMBS", "ASTS", "VRT", "NOW", "META"):
            assert t not in _ANNUAL_MISCLASSIFICATION_FIX_TICKERS


class TestDellNetIncomeReclassification:
    """DELLのnet_income: 実データ（2026-08-30実測）を模した2グループ、
    各グループ2件（181日・272日、同一accn・同一start）が競合するケース"""

    def _dell_entries(self):
        return [
            _entry("2023-08-04", "2023-02-04", 1114000000, "0001571996-25-000034", period_days=181),
            _entry("2023-11-03", "2023-02-04", 2164000000, "0001571996-25-000034", period_days=272),
            _entry("2024-08-02", "2024-02-03", 1874000000, "0001571996-25-000034", period_days=181),
            _entry("2024-11-01", "2024-02-03", 3044000000, "0001571996-25-000034", period_days=272),
            # 正常な単独年次エントリ（除外対象外）
            _entry("2024-02-02", "2023-02-04", 3200000000, "0001571996-25-000034", period_days=364),
        ]

    def test_dell_duplicate_end_dates_excluded(self):
        result = _reclassify_misannotated_fy_entries(self._dell_entries(), "DELL")
        remaining_ends = {e["end"] for e in result}
        assert "2023-08-04" not in remaining_ends
        assert "2023-11-03" not in remaining_ends
        assert "2024-08-02" not in remaining_ends
        assert "2024-11-01" not in remaining_ends
        # 正常な単独年次エントリは保持される
        assert "2024-02-02" in remaining_ends
        assert len(result) == 1

    def test_dell_lowercase_ticker_also_matched(self):
        """ticker.upper()で比較するため小文字入力でも一致すること"""
        result = _reclassify_misannotated_fy_entries(self._dell_entries(), "dell")
        assert len(result) == 1


class TestSpirRevenueReclassification:
    """SPIRのrevenue: 実データ（2026-08-30実測）を模した1グループ2件
    （180日・272日、同一accn・同一start、10-K/A由来）"""

    def _spir_entries(self):
        return [
            _entry("2023-06-30", "2023-01-01", 621000, "0000950170-25-030856",
                   period_days=180, form="10-K/A"),
            _entry("2023-09-30", "2023-01-01", 4570000, "0000950170-25-030856",
                   period_days=272, form="10-K/A"),
        ]

    def test_spir_duplicate_end_dates_excluded(self):
        """SPIR型（同一accn・同一startで複数end日が競合）の入力を、今の対象ticker（DELL）で確かめる"""
        result = _reclassify_misannotated_fy_entries(self._spir_entries(), "DELL")
        assert result == []


class TestNonTargetTickerUnaffected:
    """対象外tickerでは、同型の重複エントリがあっても一切変更されない
    こと（frozenset外への意図しない波及がないことの確認）"""

    def test_unrelated_ticker_passthrough(self):
        entries = [
            _entry("2023-08-04", "2023-02-04", 1114000000, "0001-25-000001", period_days=181),
            _entry("2023-11-03", "2023-02-04", 2164000000, "0001-25-000001", period_days=272),
        ]
        result = _reclassify_misannotated_fy_entries(entries, "AAPL")
        assert result == entries
        assert len(result) == 2


class TestSingleEndDatePatternNotCaught:
    """RMBS/ASTS/VRT型（同一accn・同一start内でend日が1種類のみ）は、
    対象tickerであっても判定基準（複数end日の競合）に合致しないため
    除外されないことを明示する（意図した仕様上の限界の固定化）"""

    def test_single_end_date_group_is_not_excluded_even_for_target_ticker(self):
        entries = [
            _entry("2020-09-30", "2020-01-01", -28416000, "0000917273-21-000010", period_days=273),
            _entry("2020-12-31", "2020-01-01", -40471000, "0000917273-21-000010", period_days=365),
        ]
        # 2020-12-31の365日エントリはこの関数のフィルタ条件
        # （130<period_days<=300）の対象外のため無条件で通過する
        result = _reclassify_misannotated_fy_entries(entries, "DELL")
        # 単一end日（2020-09-30）の重複がないため、この関数単体では
        # 除外されない（別種の検知ロジックが必要という既知の限界）
        assert len(result) == 2
