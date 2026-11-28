"""古さの判定（期末からの日数）は common/sec_data/freshness.py の1か所（[[DUPLICATE-RULES-ZERO-EPIC-1]] 段3、2026-11-28）。"""
from datetime import date, datetime, timezone

import pytest

from common.sec_data import freshness as fr
from src.value.stonks_silo import financial_as_of as fa

TODAY = date(2026, 11, 28)


def test_is_stale_period_boundary_and_unreadable():
    assert fr.is_stale_period("2026-07-16", TODAY, 135) is False   # ちょうど135日
    assert fr.is_stale_period("2026-07-15", TODAY, 135) is True    # 136日
    assert fr.is_stale_period(None, TODAY, 135) is None
    assert fr.is_stale_period("not-a-date", TODAY, 135) is None


@pytest.mark.parametrize("q,limit", [(1, 145), (2, 145), (4, 145), (3, 190), (None, 190)])
def test_eps_limit_depends_on_next_filing(q, limit):
    assert fr.eps_stale_limit_days(q) == limit


def test_eps_stale_fields_match_the_old_screen_rule():
    # CEG（Q2、期末 2026-06-30）: 151日 > 145日
    f = fr.eps_stale_fields("2026-06-30", 2, TODAY)
    assert f == {"stale": True, "stale_days": 151, "stale_limit_days": 145}
    # Q3 の後は 190日まで待つ（150日では印を付けない）
    assert fr.eps_stale_fields("2026-06-30", 3, TODAY)["stale"] is False
    # 読めない期末は印なし・日数なし
    assert fr.eps_stale_fields("", 1, TODAY) == {"stale": None, "stale_days": None, "stale_limit_days": 145}


def test_stonks_is_stale_uses_the_shared_judgement():
    gen = datetime(2026, 11, 28, tzinfo=timezone.utc)
    assert fa.is_stale("2026-07-16", gen) is fr.is_stale_period("2026-07-16", TODAY, fa.STALE_DAYS_AFTER_QUARTER_END)
    assert fa.is_stale("2026-07-15", gen) is True
