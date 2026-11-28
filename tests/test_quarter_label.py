"""quarter_label は common/sec_data/utils.py の1か所（[[DUPLICATE-RULES-ZERO-EPIC-1]] 段3）。"""
import pytest

from common.sec_data.utils import quarter_label


@pytest.mark.parametrize("end,expected", [
    ("2026-03-31", "2026Q1"), ("2026-06-30", "2026Q2"), ("2026-07-26", "2026Q3"),
    ("2025-12-31", "2025Q4"), ("2026-03-31T00:00:00", "2026Q1"),
])
def test_calendar_quarter(end, expected):
    assert quarter_label(end) == expected
    assert quarter_label(end, strict=True) == expected


def test_invalid_input_lenient_returns_input_strict_raises():
    assert quarter_label("abc") == "abc"
    with pytest.raises(ValueError):
        quarter_label("abc", strict=True)
