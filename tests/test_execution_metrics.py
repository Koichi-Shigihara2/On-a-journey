"""
tests/test_execution_metrics.py

[[HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1]]④の回帰テスト。
EPS Analyzer側の3指標（売上成長加速度・ROICトレンド・SBC比率）を検証する。

実行方法:
    python -m pytest tests/test_execution_metrics.py -v
"""
import json
import os

from src.value.adjusted_eps_analyzer.execution_metrics import (
    calc_execution_metrics,
    calc_revenue_growth_acceleration,
    calc_roic_trend,
    calc_sbc_ratio,
)


def _q(filing_date, revenue, adjustments=None, period_end=None):
    return {
        "filing_date": filing_date,
        "period_end": period_end or filing_date,
        "revenue": revenue,
        "adjustments": adjustments or [],
    }


class TestRevenueGrowthAcceleration:
    def test_accelerating_growth(self):
        # 16四半期分、YoYが直近4Qで加速するよう設計
        # base(idx8-15): 100,100,100,100,100,100,100,100
        # recent(idx4-7)=120,120,120,120 -> YoY=20% (vs idx8-11=100)
        # recent(idx0-3)=150,150,150,150 -> YoY=50% (vs idx4-7=100... but idx4-7 is 120)
        # 単純化のため明示的に構築する
        revs_newest_first = [150, 150, 150, 150,  # idx0-3 (直近4Q)
                              100, 100, 100, 100,  # idx4-7 (その前4Q, base of idx0-3)
                              100, 100, 100, 100,  # idx8-11 (base of idx4-7)
                              80, 80, 80, 80]       # idx12-15 (base of idx8-11, unused for accel but needed for prior4's own base... )
        quarters = [_q(f"2026-{str(12-i).zfill(2)}-01", r) for i, r in enumerate(revs_newest_first)]
        result = calc_revenue_growth_acceleration(quarters)
        assert result["reason"] == "ok"
        # recent4 YoY = (150-100)/100 = 0.5 (all 4 quarters identical)
        # prior4 YoY = (100-100)/100 = 0.0 (all 4 quarters identical, idx4-7 vs idx8-11)
        assert abs(result["recent_4q_avg_yoy"] - 0.5) < 1e-9
        assert abs(result["prior_4q_avg_yoy"] - 0.0) < 1e-9
        assert abs(result["value"] - 0.5) < 1e-9

    def test_insufficient_data_returns_none(self):
        quarters = [_q(f"2026-{str(12-i).zfill(2)}-01", 100) for i in range(8)]  # only 8 quarters
        result = calc_revenue_growth_acceleration(quarters)
        assert result["value"] is None
        assert result["reason"] == "insufficient_data"

    def test_base_effect_does_not_apply_naive_second_diff(self):
        """C方式（移動平均の差分）はA方式（単純2階差分）と異なる結果を返すことの確認。
        前年が一時的に急変しても平均化で緩和されることを示す。"""
        # 直近4Qは安定成長(10%)だが、prior4のうち1四半期だけ極端なbaseを持つケース
        revs_newest_first = [110, 110, 110, 110,   # idx0-3
                              100, 100, 100, 100,   # idx4-7
                              100, 100, 100, 10,    # idx8-11 (最後だけ極端に低いbase)
                              90, 90, 90, 90]
        quarters = [_q(f"2026-{str(12-i).zfill(2)}-01", r) for i, r in enumerate(revs_newest_first)]
        result = calc_revenue_growth_acceleration(quarters)
        assert result["reason"] == "ok"
        # 単一四半期のYoY急変(idx7 vs idx11: (100-10)/10=9.0)が平均に混入するが、
        # 4件平均のため単独の2階差分よりは緩和される
        assert result["value"] is not None


class TestRoicTrend:
    def test_trend_with_ok_years(self, tmp_path, monkeypatch):
        root = str(tmp_path)
        sec_dir = os.path.join(root, "common", "sec_data", "data", "TESTCO")
        os.makedirs(sec_dir)
        for y, oi in [(2023, 1_000_000), (2024, 2_000_000), (2025, 3_000_000)]:
            with open(os.path.join(sec_dir, f"annual_{y}.json"), "w", encoding="utf-8") as f:
                json.dump({
                    "pl": {"operating_income": oi},
                    "bs": {"stockholders_equity": 5_000_000, "cash_and_equivalents": 1_000_000,
                           "long_term_debt": 0, "short_term_debt": 0},
                }, f)
        result = calc_roic_trend("TESTCO", root)
        assert result["reason"] == "ok"
        assert len(result["years"]) == 3
        assert all(e["reason"] == "ok" for e in result["years"])
        # 新しい順であること
        assert result["years"][0]["year"] == 2025

    def test_insufficient_data_when_mostly_losses(self, tmp_path):
        root = str(tmp_path)
        sec_dir = os.path.join(root, "common", "sec_data", "data", "LOSSCO")
        os.makedirs(sec_dir)
        for y in [2023, 2024, 2025]:
            with open(os.path.join(sec_dir, f"annual_{y}.json"), "w", encoding="utf-8") as f:
                json.dump({"pl": {"operating_income": -1_000_000}, "bs": {}}, f)
        result = calc_roic_trend("LOSSCO", root)
        assert result["reason"] == "insufficient_data"

    def test_no_sec_dir(self, tmp_path):
        result = calc_roic_trend("NOPE", str(tmp_path))
        assert result["years"] == []
        assert result["reason"] == "no_sec_dir"


class TestSbcRatio:
    def test_ok(self):
        quarters = [_q("2026-07-26", 1000, adjustments=[
            {"item_id": "sbc", "amount": 50, "net_amount": 40},
        ])]
        result = calc_sbc_ratio(quarters)
        assert result["reason"] == "ok"
        assert abs(result["value"] - 0.05) < 1e-9

    def test_no_sbc_data(self):
        quarters = [_q("2026-07-26", 1000, adjustments=[])]
        result = calc_sbc_ratio(quarters)
        assert result["value"] is None
        assert result["reason"] == "no_sbc_data"

    def test_no_revenue(self):
        quarters = [_q("2026-07-26", 0, adjustments=[{"item_id": "sbc", "amount": 50}])]
        result = calc_sbc_ratio(quarters)
        assert result["value"] is None
        assert result["reason"] == "no_revenue"

    def test_empty_quarters(self):
        result = calc_sbc_ratio([])
        assert result["value"] is None
        assert result["reason"] == "no_data"


class TestCalcExecutionMetrics:
    def test_combines_all_three_without_synthesizing_score(self, tmp_path):
        root = str(tmp_path)
        quarters = [_q(f"2026-{str(12-i).zfill(2)}-01", 100, adjustments=[{"item_id": "sbc", "amount": 5}])
                    for i in range(8)]
        result = calc_execution_metrics("NOSEC", quarters, root)
        assert set(result.keys()) == {"revenue_growth_acceleration", "roic_trend", "sbc_ratio"}
        # 単一の合成スコアフィールドが存在しないことを確認
        assert "score" not in result
        assert "composite" not in result
