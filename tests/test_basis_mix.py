"""
tests/test_basis_mix.py

ベースが混在（[[QUARTERLY-BASIS-MIX-1]]、2026-10-26）:
- 案1: common/sec_data/basis_mix.py の判定（HONの2026Q3の10-Q型は混在、APPの続く組み替え型は混在でない、
  残りの四半期がそろえば外れる、20%未満は対象外）と、TANUKI VALUATIONのDCF信頼性LOW・WATCH・fundaの部品・report.txt
- 案4: layer3_builder.py の YTD の差し引き（前年同期を組み替えた申告の累計から、前の申告の累計を引かない）

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_basis_mix.py -v
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), "..")))
from test_pipeline_logic import _make_pipe, _minimal_extra, _minimal_score_data, _minimal_valuation, _write_stonks_json  # noqa: E402

from common.sec_data import basis_mix as bm  # noqa: E402
from common.sec_data.layer3_builder import _restating_filings, _ytd_to_quarterly  # noqa: E402


def _q(end, val, filed, first_val=None, first_filed=None, start=None, form="10-Q", **kw):
    e = {"end": end, "val": val, "filed": filed, "form": form, "is_annual": False, "is_ytd": False,
         "first_val": val if first_val is None else first_val, "first_filed": first_filed or filed}
    if start:
        e["start"] = start
    e.update(kw)
    return e


def _hon_revenue(extra=()):
    """HONの2026Q3の10-Q（2026-10-22）型: 前年同期（2025Q3）を継続事業ベースに組み替え、2025Q4〜2026Q2は旧ベースのまま"""
    return [
        _q("2025-09-30", 4929, "2026-10-22", 10408, "2025-10-23"),
        _q("2025-12-31", 9758, "2026-02-17", form="10-K", is_implied=True),
        _q("2026-03-31", 9143, "2026-04-23"),
        _q("2026-06-30", 9719, "2026-07-23"),
        _q("2026-09-30", 4994, "2026-10-22"),
        *extra,
    ]


class TestDetect:
    def test_hon_type_is_mixed(self):
        r = bm.detect({"Revenue": _hon_revenue()})
        assert r["mixed"] is True
        assert r["recast_filed"] == "2026-10-22" and r["trigger_filed"] == "2026-10-22"
        assert r["old_basis_quarters"] == {"Revenue": ["2025-12-31", "2026-03-31", "2026-06-30"]}
        assert "2026-09-30" in r["mixed_ttm_ends"]
        assert r["note"].startswith(bm.NOTE)
        assert r["restated"][0]["change_pct"] == -52.6

    def test_app_type_chain_is_not_mixed(self):
        """APP型: 2025-08-06から毎回の申告が前年同期を組み替えている（同じ売却）。直近4四半期は組み替えの後のベース"""
        rev = [
            _q("2024-06-30", 711, "2025-08-06", 1080, "2024-08-07"),
            _q("2024-09-30", 835, "2025-11-05", 1198, "2024-11-06"),
            {"end": "2024-12-31", "val": 3000, "filed": "2026-02-19", "form": "10-K", "is_annual": True,
             "first_val": 4380, "first_filed": "2025-02-26"},
            _q("2025-03-31", 1159, "2026-05-06", 1484, "2025-05-07"),
            _q("2025-06-30", 1259, "2025-08-06"),
            _q("2025-09-30", 1405, "2025-11-05"),
            _q("2025-12-31", 1658, "2026-02-19", form="10-K", is_implied=True),
            _q("2026-03-31", 1750, "2026-05-06"),
            _q("2026-06-30", 1900, "2026-08-05"),
        ]
        assert bm.detect({"Revenue": rev}) is None

    def test_clears_when_remaining_quarters_are_new_basis(self):
        still = _hon_revenue([_q("2026-12-31", 5100, "2027-02-15", form="10-K"), _q("2027-03-31", 5000, "2027-04-25")])
        assert bm.detect({"Revenue": still})["old_basis_quarters"] == {"Revenue": ["2026-06-30"]}
        done = _hon_revenue([_q("2026-12-31", 5100, "2027-02-15", form="10-K"), _q("2027-03-31", 5000, "2027-04-25"),
                             _q("2027-06-30", 5200, "2027-07-25")])
        assert bm.detect({"Revenue": done}) is None

    def test_small_restatement_is_ignored(self):
        rev = _hon_revenue()
        rev[0] = _q("2025-09-30", 9500, "2026-10-22", 10408, "2025-10-23")   # −8.7%
        assert bm.detect({"Revenue": rev}) is None

    def test_restated_without_later_quarter_is_not_trigger(self):
        """組み替えた申告が約1年後の四半期を申告していない（前年同期の比較ではない）なら、きっかけにしない"""
        rev = _hon_revenue()
        rev[0] = _q("2025-09-30", 4929, "2026-10-23", 10408, "2025-10-23")   # 2026-09-30 を申告した 10-22 と別の申告
        assert bm.detect({"Revenue": rev}) is None

    def test_is_mixed(self):
        assert bm.is_mixed({"mixed": True}) and not bm.is_mixed(None) and not bm.is_mixed({})


class TestLoad:
    def test_reads_saved_mark_in_ttm(self, tmp_path):
        saved = {"mixed": True, "note": "x", "mixed_ttm_ends": ["2026-09-30"]}
        (tmp_path / "HON_ttm_series.json").write_text(json.dumps({"series": [], "basis_mix": saved}), encoding="utf-8")
        assert bm.load("HON", ttm_dir=str(tmp_path)) == saved


class TestYtdBreak:
    def _chain(self, accn_q3="F"):
        return [
            {"start": "2026-01-01", "end": "2026-03-31", "val": 223, "is_ytd": False, "filed": "2026-04-23", "accn": "Q1"},
            {"start": "2026-01-01", "end": "2026-06-30", "val": 538, "is_ytd": True, "filed": "2026-07-23", "accn": "Q2"},
            {"start": "2026-01-01", "end": "2026-09-30", "val": 369, "is_ytd": True, "filed": "2026-10-22", "accn": accn_q3},
        ]

    def test_restating_filing_leaves_quarter_missing(self):
        converted, _ = _ytd_to_quarterly(self._chain(), frozenset({"F"}))
        assert [(e["end"], e["val"]) for e in converted] == [("2026-03-31", 223), ("2026-06-30", 315)]   # Q3は欠け

    def test_without_restating_filing_quarter_is_derived(self):
        converted, _ = _ytd_to_quarterly(self._chain(), frozenset())
        assert [(e["end"], e["val"]) for e in converted] == [("2026-03-31", 223), ("2026-06-30", 315), ("2026-09-30", -169)]

    def test_restating_filings(self):
        raw = [
            # 2026Q3の10-Q（F）: 9か月の累計2025（比較期間、最初は928）と2026
            {"start": "2025-01-01", "end": "2025-09-30", "val": 375, "first_val": 928, "first_filed": "2025-10-23",
             "filed": "2026-10-22", "accn": "F"},
            {"start": "2026-01-01", "end": "2026-09-30", "val": 369, "first_val": 369, "first_filed": "2026-10-22",
             "filed": "2026-10-22", "accn": "F"},
            # 組み替えたが約1年後の同じ長さの期を申告していない（G）
            {"start": "2025-01-01", "end": "2025-06-30", "val": 300, "first_val": 554, "first_filed": "2025-07-24",
             "filed": "2026-07-23", "accn": "G"},
        ]
        assert _restating_filings(raw) == frozenset({"F"})


_BM = {"mixed": True, "note": bm.NOTE + ": 10-Q（2026-10-22提出）が前年同期を組み替え、直近の4四半期に組み替えの前の申告の四半期が残る",
       "mixed_ttm_ends": ["2026-09-30"]}


def _valuation(basis_mix=None):
    v = {
        "upside_percent": -5.0,
        "components": {"current_price": 100.0, "diluted_shares": None},
        "fcf_base": {"base_fcf": 500_000_000},
        "financial_health": {},
        "fcf_estimation": {"applied": False},
        "fcf_outlier": {"detected": False},
    }
    if basis_mix is not None:
        v["basis_mix"] = basis_mix
    return v


def _write_poc(tmp_path, ticker):
    d = tmp_path / "docs" / "value-monitor" / "hypecore" / "data"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{ticker}_poc.json").write_text(json.dumps({"monthly": [
        {"rev_yoy": 30.0, "rule40_yoy_netmargin": 50.0, "stage": 2, "ma200_dev_local": 0.0}]}), encoding="utf-8")


class TestTanuki:
    def test_policy_b_low(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        assert pipe._calc_dcf_reliability_policy_b(_valuation(_BM)) == "LOW"
        assert pipe._calc_dcf_reliability_policy_b(_valuation()) == "NORMAL"
        assert pipe._calc_dcf_reliability_policy_b(_valuation({"mixed": False})) == "NORMAL"

    def test_score_rounds_to_watch_and_drops_yoy_parts(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        for t in ("BM_BASE", "BM_MIX"):
            _write_poc(tmp_path, t)
        base = pipe._compute_tanuki_score("BM_BASE", _valuation())
        assert base["funda_score"] == 75          # 売上の伸び25・Rule40 25・FCF 25（EPSの伸びなし）
        mixed = pipe._compute_tanuki_score("BM_MIX", _valuation(_BM))
        assert mixed["funda_score"] == 25         # 売上の伸び・Rule40・EPSの伸びを使わない
        assert mixed["score"] == "WATCH" and mixed["rounded_by_policy"] == "B"
        assert bm.NOTE in mixed["score_comment"]

    def test_report_line(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        v = _minimal_valuation()
        v["basis_mix"] = _BM
        report = pipe._generate_report("BM", v, _minimal_score_data(), _minimal_extra())
        assert f"DCF_Reliability: LOW ⚠️ ({bm.NOTE}, IV参考値)" in report
        assert "10-Q（2026-10-22提出）が前年同期を組み替え" in report
