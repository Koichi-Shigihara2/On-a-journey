"""
tests/test_split_adjust.py

[[SPLIT-REALTIME-GAP-REVERSE-1]]（2026-09-25）: 株式分割の遡及補正の方向非依存化
（common/sec_data/split_adjust.py）と、その利用箇所
（src/value/adjusted_eps_analyzer/pipeline.py::apply_split_adjustments()、
TANUKI VALUATIONの3年希薄化率）の回帰テスト。

実データ（KULR 1-for-8・2025-06-23、SPIR 1-for-8・2023-08-31、HON 1-for-2・
2026-06-29）を縮約した系列を使う。

実行方法:
    python -m pytest tests/test_split_adjust.py -v
"""

import copy
import os
import sys

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data.split_adjust import (  # noqa: E402
    adjust_share_points,
    load_split_history,
    unadjusted_indices,
)

M = 1_000_000

# KULR（EPS ANALYZERの四半期株数、2026-09-25時点の実値）。2024-06-30以降の
# 分割前の期は分割後の10-Qで遡及修正済み、2024-03-31以前は分割前のまま
_KULR = [
    ("2021-06-30", 92.51 * M), ("2021-09-30", 99.02 * M), ("2022-06-30", 104.55 * M),
    ("2023-12-31", 117.82 * M), ("2024-03-31", 142.36 * M),
    ("2024-06-30", 22.68 * M), ("2024-09-30", 24.31 * M), ("2025-03-31", 34.92 * M),
    ("2025-06-30", 37.59 * M), ("2025-09-30", 41.14 * M), ("2026-03-31", 46.25 * M),
    ("2026-06-30", 46.30 * M),
]


class TestUnadjustedIndices:
    def test_kulr_reverse_only_stuck_quarters(self):
        idx = unadjusted_indices(_KULR, "2025-06-23", 0.125)
        assert sorted(_KULR[i][0] for i in idx) == [
            "2021-06-30", "2021-09-30", "2022-06-30", "2023-12-31", "2024-03-31"]

    def test_kulr_already_adjusted_quarter_is_not_double_adjusted(self):
        """旧実装は2024-06-30の22.68Mを2.84Mへ二重補正していた"""
        vals = adjust_share_points(_KULR, [{"date": "2025-06-23", "ratio": 0.125}])
        by_end = {e: v for (e, _), v in zip(_KULR, vals)}
        assert by_end["2024-06-30"] == pytest.approx(22.68 * M)
        assert by_end["2024-03-31"] == pytest.approx(142.36 * M * 0.125)
        assert by_end["2021-06-30"] == pytest.approx(92.51 * M * 0.125)
        assert by_end["2026-06-30"] == pytest.approx(46.30 * M)

    def test_spir_reverse(self):
        pts = [("2021-06-30", 18.64 * M), ("2021-09-30", 67.35 * M), ("2022-03-31", 139.27 * M),
               ("2022-06-30", 139.69 * M), ("2022-09-30", 17.49 * M), ("2023-06-30", 18.47 * M),
               ("2023-09-30", 20.76 * M), ("2024-03-31", 21.81 * M)]
        idx = unadjusted_indices(pts, "2023-08-31", 0.125)
        assert sorted(pts[i][0] for i in idx) == ["2021-09-30", "2022-03-31", "2022-06-30"]

    def test_hon_restated_comparative_quarter_is_kept(self):
        """HON 2025-06-30は分割後の10-Qが比較期間として修正済み（320.5M）"""
        pts = [("2025-03-31", 651.7 * M), ("2025-06-30", 320.5 * M), ("2025-09-30", 638.8 * M),
               ("2026-03-31", 638.4 * M), ("2026-06-30", 318.6 * M)]
        idx = unadjusted_indices(pts, "2026-06-29", 0.5)
        assert sorted(pts[i][0] for i in idx) == ["2025-03-31", "2025-09-30", "2026-03-31"]

    def test_forward_with_unregistered_earlier_split(self):
        """NVDA型: 2021年4:1が未登録で2020-01-26だけ別基準（4:1反映済み）でも、
        分割後平均を基準にした判定で未調整（10:1未反映）と判定される"""
        pts = [("2019-10-27", 618 * M), ("2020-01-26", 2472 * M), ("2020-04-26", 622 * M),
               ("2023-04-30", 2490 * M), ("2023-07-30", 24940 * M),
               ("2024-07-28", 24530 * M), ("2024-10-27", 24500 * M)]
        idx = unadjusted_indices(pts, "2024-06-10", 10)
        assert sorted(pts[i][0] for i in idx) == ["2019-10-27", "2020-01-26", "2020-04-26", "2023-04-30"]

    def test_no_post_split_data_is_noop(self):
        assert unadjusted_indices([("2020-03-31", 100.0)], "2026-06-12", 10) == []

    def test_production_split_history_has_reverse_entries(self):
        sh = load_split_history()
        assert sh["KULR"][0]["ratio"] == pytest.approx(0.125)
        assert sh["SPIR"][0]["ratio"] == pytest.approx(0.125)
        assert sh["HON"][0]["ratio"] == pytest.approx(0.5)


class TestEpsApplySplitAdjustments:
    def test_kulr_eps_rows(self):
        from src.value.adjusted_eps_analyzer.pipeline import apply_split_adjustments
        rows = [{"period_end": e, "filing_date": e, "diluted_shares_used": s, "diluted_shares": s,
                 "gaap_eps": -1.0, "adjusted_eps": -1.0} for e, s in _KULR]
        out = apply_split_adjustments("KULR", copy.deepcopy(rows),
                                      {"KULR": [{"date": "2025-06-23", "ratio": 0.125}]})
        by_end = {r["period_end"]: r for r in out}
        assert by_end["2024-06-30"]["diluted_shares_used"] == pytest.approx(22.68 * M)
        assert not by_end["2024-06-30"].get("split_adjusted")
        assert by_end["2024-03-31"]["diluted_shares_used"] == pytest.approx(142.36 * M * 0.125)
        assert by_end["2024-03-31"]["gaap_eps"] == pytest.approx(-8.0)


class TestTanukiDilutionUsesRegisteredSplits:
    """TANUKI VALUATIONの希薄化率（[[DILUTION-DEFINITION-UNIFY-1]]: 直近の四半期÷12四半期前の加重平均の希薄化後株数）:
    KULR型（四半期株数が2025-03までは分割前、2025-06以降は分割後）で、登録済みの1-for-8を「株数減少」と扱わない"""

    @staticmethod
    def _store():
        tag = "WeightedAverageNumberOfDilutedSharesOutstanding"

        def q(end, v):
            return {"end": end, "val": v, "fp": "Q2", "is_annual": False, "is_ytd": False, "source_tag": tag}

        ends = ["2023-06-30", "2023-09-30", "2023-12-31", "2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31",
                "2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31", "2026-06-30"]
        vals = [115.38, 117.14, 125.0, 142.36, 150.0, 155.0, 160.0, 165.0,      # 分割前（1-for-8の前）
                37.59, 41.14, 43.0, 45.0, 46.28]                                  # 分割後
        entries = [q(e, v * M) for e, v in zip(ends, vals)]
        return {"fields": {"shares_diluted": {"entries": entries}}}

    def _run(self, tmp_path, monkeypatch, splits):
        import test_pipeline_logic as tpl
        import pipeline as tanuki_pipeline
        pipe = tpl._make_pipe(tmp_path)
        monkeypatch.setattr(pipe, "_get_layer3_store", lambda t: self._store())
        monkeypatch.setattr(tanuki_pipeline, "load_split_history", lambda: splits)
        valuation = {"components": {"latest_revenue": 10 * M, "diluted_shares": 46.28 * M}}
        return pipe._load_extra_data("KULR", valuation)

    def test_kulr_dilution_is_positive_with_registered_split(self, tmp_path, monkeypatch):
        res = self._run(tmp_path, monkeypatch, {"KULR": [{"date": "2025-06-23", "ratio": 0.125}]})
        dil = res["financial_health"]["dilution_3yr_annual_pct"]
        # 2023-06 115.38M（×1/8 = 14.42M）→ 2026-06 46.28M: 年率約+47%
        assert dil == pytest.approx(((46.28 / (115.38 / 8)) ** (1 / 3) - 1) * 100, abs=0.05)
        assert dil > 40  # 希薄化ペナルティ（>40%で funda −25）の対象
        assert "split_suspect" not in res["financial_health"]

    def test_without_registration_the_old_misjudgment_remains(self, tmp_path, monkeypatch):
        """未登録なら割り戻さず、未登録の分割の疑いの印を付ける（登録が効いていることの対照）。印の付いた銘柄は減点に使わない"""
        res = self._run(tmp_path, monkeypatch, {})
        assert res["financial_health"]["dilution_3yr_annual_pct"] < 0
        assert res["financial_health"]["split_suspect"] == ["2025-06-30"]
