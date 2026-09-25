"""
tests/test_stonks_flag_rule.py

[[FLAG-THRESHOLD-DESIGN-1]]案C（2026-09-25採用）: stonks_siloフラグの機械判定
（TTM営業利益<0 または TTM売上=0 → true）の回帰テスト。
common/sec_data/stonks_flag_rule.py::judge_stonks_silo()、
report_consistency_check.py CHECK-51、registration_validator.py P7。

実行方法:
    python -m pytest tests/test_stonks_flag_rule.py -v
"""

import json
import os
import sys

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data import stonks_flag_rule as sfr  # noqa: E402
import common.sec_data.report_consistency_check as rcc  # noqa: E402
import common.sec_data.registration_validator as rv  # noqa: E402

_CFG = {"excluded": {"SOFI": "金融機関"}, "known_undeterminable": {"XOM": "pretax法未実装"}}


def _write(tmp_path, ticker, oi, rev, annual_revs=(1_000_000_000,)):
    ttm_dir = tmp_path / "ttm"
    ttm_dir.mkdir(exist_ok=True)
    flow = {}
    if oi is not None:
        flow["operating_income"] = {"val": oi}
    if rev is not None:
        flow["revenue"] = {"val": rev}
    (ttm_dir / f"{ticker}_ttm_series.json").write_text(json.dumps(
        {"series": [{"ttm_end": "2026-06-30", "flow": flow}]}), encoding="utf-8")
    d = tmp_path / "data" / ticker
    d.mkdir(parents=True, exist_ok=True)
    for i, r in enumerate(annual_revs):
        (d / f"annual_{2023 + i}.json").write_text(json.dumps({"pl": {"revenue": r}}), encoding="utf-8")
    return str(ttm_dir), str(tmp_path / "data")


def _judge(tmp_path, ticker, oi, rev, annual_revs=(1_000_000_000,)):
    ttm_dir, data_dir = _write(tmp_path, ticker, oi, rev, annual_revs)
    return sfr.judge_stonks_silo(ticker, ttm_dir=ttm_dir, data_dir=data_dir, config=_CFG)


class TestJudgeStonksSilo:
    def test_operating_loss_is_true(self, tmp_path):
        """GTLB型: TTM営業利益<0（純利益ではなく営業利益で判定）"""
        j = _judge(tmp_path, "GTLB", -51_620_000, 1_005_000_000)
        assert j["state"] == "judged" and j["expected"] is True

    def test_operating_profit_is_false(self, tmp_path):
        """LITE型: 純利益は大幅赤字でも営業利益>0ならfalse"""
        j = _judge(tmp_path, "LITE", 524_800_000, 3_014_000_000)
        assert j["expected"] is False

    def test_pre_revenue_is_true(self, tmp_path):
        """APGE型: TTM売上なし・年次売上も全年なし → プレレベニューでtrue"""
        j = _judge(tmp_path, "APGE", -328_110_000, None, annual_revs=(None, None, None))
        assert j["expected"] is True
        assert "プレレベニュー" in j["reason"]

    def test_revenue_missing_with_annual_history_is_undeterminable(self, tmp_path):
        """SN型: TTM売上欠損でも年次売上に実績があればデータ欠損（判定不能）"""
        j = _judge(tmp_path, "SN", None, None, annual_revs=(5_528_639_000,))
        assert j["state"] == "undeterminable" and j["expected"] is None

    def test_operating_income_missing_is_undeterminable_with_note(self, tmp_path):
        """XOM型: 営業利益が取れない → 判定不能（known_undeterminableの注記を表示）"""
        j = _judge(tmp_path, "XOM", None, 330_000_000_000)
        assert j["state"] == "undeterminable"
        assert j["reason"] == "pretax法未実装"

    def test_excluded_financial_institution(self, tmp_path):
        """SOFI: 案Cの適用外（営業利益の符号に関わらず判定しない）"""
        j = _judge(tmp_path, "SOFI", -1, 4_310_000_000)
        assert j["state"] == "excluded" and j["expected"] is None

    def test_production_config_loads(self):
        cfg = sfr.load_rule_config()
        assert "SOFI" in cfg["excluded"]
        assert {"ASTS", "XOM", "SN"} <= set(cfg["known_undeterminable"])


class TestCheck51:
    def test_warns_on_mismatch_only(self, monkeypatch):
        rows = [
            {"ticker": "AAA", "stonks_silo": "false", "status": "active"},   # 期待true → WARN
            {"ticker": "BBB", "stonks_silo": "true", "status": "active"},    # 期待true → 一致
            {"ticker": "CCC", "stonks_silo": "true", "status": "active"},    # 判定不能 → 対象外
            {"ticker": "DDD", "stonks_silo": "false", "status": "retired"},  # retired → 対象外
        ]
        verdict = {"AAA": (True, "judged"), "BBB": (True, "judged"), "CCC": (None, "undeterminable"),
                   "DDD": (True, "judged")}
        monkeypatch.setattr(rcc._tickers_mod, "get_all_rows", lambda: rows)
        monkeypatch.setattr(sfr, "judge_stonks_silo", lambda t, config=None: {
            "ticker": t, "expected": verdict[t][0], "state": verdict[t][1], "reason": "r",
            "ttm_end": "2026-06-30", "operating_income": -1.0, "revenue": 1.0})
        w = rcc._check_stonks_silo_flag_rule()
        assert len(w) == 1 and "WARN-51" in w[0] and "AAA" in w[0]

    def test_production_flags_match_plan_c(self):
        """2026-09-25のフラグ是正（APGE・GTLB・LYFT・SPIR→true、LITE・ZETA→false）
        後、本番のcik_lookup.csvと案C判定が全銘柄で一致している"""
        assert rcc._check_stonks_silo_flag_rule() == []


class TestRegistrationValidatorP7:
    def test_p7_warns_when_flag_mismatches(self, monkeypatch):
        monkeypatch.setattr(rv, "judge_stonks_silo", lambda t, config=None: {
            "ticker": t, "expected": True, "state": "judged", "reason": "TTM営業利益<0",
            "ttm_end": "2026-06-30", "operating_income": -1.0, "revenue": 1.0})
        issues = rv.Issues()
        rv.check_p7_stonks_silo_flag("NEWT", issues, {"NEWT": {"stonks_silo": "false"}}, _CFG)
        assert [(s, c) for s, c, _ in issues.all()] == [("WARN", "P7-StonksFlag")]

    def test_p7_info_only_when_undeterminable(self, monkeypatch):
        monkeypatch.setattr(rv, "judge_stonks_silo", lambda t, config=None: {
            "ticker": t, "expected": None, "state": "undeterminable", "reason": "TTM系列なし",
            "ttm_end": None, "operating_income": None, "revenue": None})
        issues = rv.Issues()
        rv.check_p7_stonks_silo_flag("NEWT", issues, {"NEWT": {"stonks_silo": "false"}}, _CFG)
        assert issues.count_warn() == 0 and issues.count_ng() == 0
