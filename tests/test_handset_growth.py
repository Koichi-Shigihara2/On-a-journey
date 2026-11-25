"""手書きの成長率の古さの印（[[TANUKI-UPSIDE-MAGNITUDE-INFLATED-1]] 6-1、common/handset_growth.py）。表示だけ"""
import json
import os
from datetime import date, timedelta

from common import handset_growth as hg

TODAY = date(2026, 11, 25)


def test_stale_after_180_days_not_before():
    ok = hg.evaluate("segment_weighted", True, (TODAY - timedelta(days=180)).isoformat(), TODAY)
    assert ok["status"] == hg.FRESH
    old = hg.evaluate("segment_weighted", True, (TODAY - timedelta(days=181)).isoformat(), TODAY)
    assert old["status"] == hg.STALE and old["age_days"] == 181 and "181日前" in old["label"]
    assert "Handset_Growth_Age" in hg.report_line(old)
    assert hg.report_line(ok) is None


def test_only_handset_configured_growth_is_checked():
    assert hg.evaluate("segment_xbrl", True, "2020-01-01", TODAY)["status"] == hg.NOT_HANDSET
    assert hg.evaluate("segment_weighted", False, "2020-01-01", TODAY)["status"] == hg.NOT_HANDSET   # General 1行（過去の数字の平均に置き換わる）
    assert hg.evaluate("growth_override", True, "2020-01-01", TODAY)["status"] == hg.NOT_HANDSET


def test_unknown_when_no_updated_date():
    r = hg.evaluate("segment_weighted", True, None, TODAY)
    assert r["status"] == hg.UNKNOWN and "記録されていない" in hg.report_line(r)


def test_every_enabled_ticker_in_config_has_updated_date():
    """設定のブロックを手で更新したら updated も更新する（無いと古さが分からない）"""
    p = os.path.join(os.path.dirname(__file__), "..", "config", "segment_config.json")
    cfg = json.load(open(p, encoding="utf-8"))
    missing = [t for t, c in cfg.items() if not t.startswith("_") and c.get("enabled") and not hg._d(c.get("updated"))]
    assert not missing, missing
