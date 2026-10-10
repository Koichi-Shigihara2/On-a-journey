"""
tests/test_share_count_outlier_check.py

[[LAYER2-SHARES-UNIT-THOUSANDS-1]]（2026-10-10）: report_consistency_check.pyのCHECK-59
（株式数の単位誤りの疑い、WARN）。

実行方法:
    python -m pytest tests/test_share_count_outlier_check.py -v
"""
import json
from datetime import datetime

from common.sec_data import report_consistency_check as rcc

TODAY = datetime(2026, 10, 10)


def _write(tmp_path, ticker, quarters, annual=None, dei=None, wavg_filed="2026-08-04", dei_filed="2026-08-04"):
    d = tmp_path / ticker
    d.mkdir()
    for name, v in quarters.items():
        (d / f"quarterly_{name}.json").write_text(json.dumps({"shares": {"shares_diluted": v}}), encoding="utf-8")
    for name, v in (annual or {}).items():
        (d / f"annual_{name}.json").write_text(json.dumps({"shares": {"shares_diluted": v}}), encoding="utf-8")
    facts = {"us-gaap": {"WeightedAverageNumberOfDilutedSharesOutstanding": {"units": {"shares": [
        {"start": "2026-04-01", "end": "2026-06-30", "val": 1, "filed": wavg_filed}]}}}}
    if dei:
        facts["dei"] = {"EntityCommonStockSharesOutstanding": {"units": {"shares": [
            {"end": dei[0], "val": dei[1], "filed": dei_filed}]}}}
    (d / "company_facts.json").write_text(json.dumps({"facts": facts}), encoding="utf-8")
    return str(tmp_path)


Q = {"2025Q3": 12_319_000, "2025Q4": 12_321_000, "2026Q1": 12_323_000}


def test_thousand_unit_quarter_flagged(tmp_path):
    # CIX 2026Q2型: 12,329（前後は約1,232万株）
    sec = _write(tmp_path, "XXX", {**Q, "2026Q2": 12_329})
    out = rcc._check_share_count_outliers("XXX", sec_dir=sec, today=TODAY)
    assert len(out) == 1 and "[WARN-59" in out[0] and "quarterly_2026Q2.json" in out[0]


def test_clean_series_not_flagged(tmp_path):
    sec = _write(tmp_path, "XXX", {**Q, "2026Q2": 12_329_000}, dei=("2026-07-30", 12_336_657))
    assert rcc._check_share_count_outliers("XXX", sec_dir=sec, today=TODAY) == []


def test_old_periods_ignored(tmp_path):
    # 直近5年より前の期（2018年）の申告誤りは対象外
    sec = _write(tmp_path, "XXX", {"2017Q4": 4_300_000_000, "2018Q1": 4_306, "2018Q2": 4_290_000_000,
                                   "2018Q3": 4_295_000_000})
    assert rcc._check_share_count_outliers("XXX", sec_dir=sec, today=TODAY) == []


def test_registered_split_not_flagged(tmp_path, monkeypatch):
    # BKNG型: 25対1分割の前後（登録済み）
    import common.sec_data.split_adjust as sa
    monkeypatch.setattr(sa, "load_split_history", lambda *a, **k: {"XXX": [{"date": "2026-04-06", "ratio": 25}]})
    sec = _write(tmp_path, "XXX", {"2025Q3": 32_558_000, "2025Q4": 32_400_000, "2026Q1": 794_000_000,
                                   "2026Q2": 770_000_000})
    assert rcc._check_share_count_outliers("XXX", sec_dir=sec, today=TODAY) == []


def test_cover_ratio_flagged_and_stale_dei_skipped(tmp_path):
    # LOAR型: 全期が千株単位で前後比較では見えない → 表紙の発行済株式数との比で検知
    q = {"2025Q3": 95_875, "2025Q4": 95_893, "2026Q1": 95_651, "2026Q2": 95_521}
    sec = _write(tmp_path, "AAA", q, dei=("2026-07-31", 93_688_471))
    out = rcc._check_share_count_outliers("AAA", sec_dir=sec, today=TODAY)
    assert len(out) == 1 and "表紙の発行済株式数" in out[0]
    # deiの最新の提出が古い（SOUN・HEI型）→ 比較しない
    (tmp_path / "s").mkdir()
    sec2 = _write(tmp_path / "s", "BBB", q, dei=("2022-03-09", 17_461_000), dei_filed="2022-03-10")
    assert rcc._check_share_count_outliers("BBB", sec_dir=sec2, today=TODAY) == []
