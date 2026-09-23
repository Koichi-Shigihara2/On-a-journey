"""
tests/test_roic_shared_module.py

[[HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1]]④の回帰テスト。
`_calc_roic_wacc_ratio()`（元pipeline.py内実装）を`common.sec_data.roic`
へ切り出したことを検証する。共有モジュール自体が存在しない状態では
このテストファイルはimport自体が失敗する（fail-before）。

実行方法:
    python -m pytest tests/test_roic_shared_module.py -v
"""
import json
import os

from common.sec_data.roic import calc_roic_wacc_ratio


def _write_annual(sec_dir: str, ticker: str, year: int, pl: dict, bs: dict) -> None:
    ticker_dir = os.path.join(sec_dir, ticker)
    os.makedirs(ticker_dir, exist_ok=True)
    with open(os.path.join(ticker_dir, f"annual_{year}.json"), "w", encoding="utf-8") as f:
        json.dump({"pl": pl, "bs": bs}, f)


def _repo_root(tmp_path) -> str:
    root = str(tmp_path)
    os.makedirs(os.path.join(root, "common", "sec_data", "data"), exist_ok=True)
    return root


class TestCalcRoicWaccRatioBasic:
    def test_ok(self, tmp_path):
        root = _repo_root(tmp_path)
        _write_annual(
            os.path.join(root, "common", "sec_data", "data"), "TESTCO", 2025,
            pl={"operating_income": 1_000_000},
            bs={"stockholders_equity": 5_000_000, "cash_and_equivalents": 1_000_000,
                "long_term_debt": 2_000_000, "short_term_debt": 0},
        )
        val, reason = calc_roic_wacc_ratio("TESTCO", root, wacc_rm=0.10)
        assert reason == "ok"
        # NOPAT = 1,000,000 * 0.79 = 790,000
        # Invested_Capital = 5,000,000 + 2,000,000 + 0 - 1,000,000 = 6,000,000
        # ROIC = 790,000 / 6,000,000 = 0.131667
        assert val is not None
        assert abs(val - (0.790000 / 6.0) / 0.10) < 1e-6

    def test_no_sec_dir(self, tmp_path):
        root = _repo_root(tmp_path)
        val, reason = calc_roic_wacc_ratio("NOPE", root)
        assert val is None and reason == "no_sec_dir"

    def test_no_annual_data(self, tmp_path):
        root = _repo_root(tmp_path)
        os.makedirs(os.path.join(root, "common", "sec_data", "data", "EMPTYCO"))
        val, reason = calc_roic_wacc_ratio("EMPTYCO", root)
        assert val is None and reason == "no_annual_data"

    def test_no_operating_income_without_fallback(self, tmp_path):
        root = _repo_root(tmp_path)
        _write_annual(
            os.path.join(root, "common", "sec_data", "data"), "NOOI", 2025,
            pl={"operating_income": None}, bs={},
        )
        val, reason = calc_roic_wacc_ratio("NOOI", root)
        assert val is None and reason == "no_operating_income"

    def test_no_operating_income_with_fallback(self, tmp_path):
        root = _repo_root(tmp_path)
        _write_annual(
            os.path.join(root, "common", "sec_data", "data"), "FALLBACK", 2025,
            pl={"operating_income": None},
            bs={"stockholders_equity": 5_000_000, "cash_and_equivalents": 1_000_000,
                "long_term_debt": 2_000_000, "short_term_debt": 0},
        )
        val, reason = calc_roic_wacc_ratio(
            "FALLBACK", root, estimate_ttm_operating_income_fn=lambda t: 1_000_000,
        )
        assert reason == "ok"
        assert val is not None

    def test_reported_negative_oi(self, tmp_path):
        root = _repo_root(tmp_path)
        _write_annual(
            os.path.join(root, "common", "sec_data", "data"), "LOSSCO", 2025,
            pl={"operating_income": -500_000}, bs={},
        )
        val, reason = calc_roic_wacc_ratio("LOSSCO", root)
        assert val is None and reason == "reported_negative_oi"

    def test_missing_equity_data(self, tmp_path):
        root = _repo_root(tmp_path)
        _write_annual(
            os.path.join(root, "common", "sec_data", "data"), "NOEQ", 2025,
            pl={"operating_income": 1_000_000}, bs={},
        )
        val, reason = calc_roic_wacc_ratio("NOEQ", root)
        assert val is None and reason == "missing_equity_data"

    def test_missing_cash_data(self, tmp_path):
        root = _repo_root(tmp_path)
        _write_annual(
            os.path.join(root, "common", "sec_data", "data"), "NOCASH", 2025,
            pl={"operating_income": 1_000_000},
            bs={"stockholders_equity": 5_000_000},
        )
        val, reason = calc_roic_wacc_ratio("NOCASH", root)
        assert val is None and reason == "missing_cash_data"

    def test_negative_invested_capital(self, tmp_path):
        root = _repo_root(tmp_path)
        _write_annual(
            os.path.join(root, "common", "sec_data", "data"), "NEGIC", 2025,
            pl={"operating_income": 1_000_000},
            bs={"stockholders_equity": -10_000_000, "cash_and_equivalents": 1_000_000,
                "long_term_debt": 0, "short_term_debt": 0},
        )
        val, reason = calc_roic_wacc_ratio("NEGIC", root)
        assert val is None and reason == "negative_invested_capital"

    def test_lt_debt_fallback_used_when_missing(self, tmp_path):
        root = _repo_root(tmp_path)
        _write_annual(
            os.path.join(root, "common", "sec_data", "data"), "LTFB", 2025,
            pl={"operating_income": 1_000_000},
            bs={"stockholders_equity": 5_000_000, "cash_and_equivalents": 1_000_000,
                "short_term_debt": 0},
        )
        val, reason = calc_roic_wacc_ratio(
            "LTFB", root, get_lt_debt_fallback_fn=lambda t: 2_000_000,
        )
        assert reason == "ok"
        # Invested_Capital = 5,000,000 + 2,000,000(fallback) + 0 - 1,000,000 = 6,000,000
        assert val is not None
        assert abs(val - (0.790000 / 6.0) / 0.10) < 1e-6

    def test_year_param_selects_specific_year(self, tmp_path):
        root = _repo_root(tmp_path)
        data_dir = os.path.join(root, "common", "sec_data", "data")
        _write_annual(data_dir, "MULTIYR", 2023,
                       pl={"operating_income": 1_000_000},
                       bs={"stockholders_equity": 5_000_000, "cash_and_equivalents": 1_000_000,
                           "long_term_debt": 0, "short_term_debt": 0})
        _write_annual(data_dir, "MULTIYR", 2024,
                       pl={"operating_income": 2_000_000},
                       bs={"stockholders_equity": 5_000_000, "cash_and_equivalents": 1_000_000,
                           "long_term_debt": 0, "short_term_debt": 0})
        val_2023, reason_2023 = calc_roic_wacc_ratio("MULTIYR", root, year=2023)
        val_2024, reason_2024 = calc_roic_wacc_ratio("MULTIYR", root, year=2024)
        val_latest, reason_latest = calc_roic_wacc_ratio("MULTIYR", root)
        assert reason_2023 == reason_2024 == reason_latest == "ok"
        assert val_2023 != val_2024
        assert val_latest == val_2024  # yearを省略すると最新年度（従来動作）

    def test_year_not_found(self, tmp_path):
        root = _repo_root(tmp_path)
        _write_annual(
            os.path.join(root, "common", "sec_data", "data"), "ONEYR", 2025,
            pl={"operating_income": 1_000_000}, bs={},
        )
        val, reason = calc_roic_wacc_ratio("ONEYR", root, year=2020)
        assert val is None and reason == "year_not_found"
