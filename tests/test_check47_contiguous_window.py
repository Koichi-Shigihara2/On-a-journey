"""
tests/test_check47_contiguous_window.py

[[CHECK47-NONCONTIGUOUS-WINDOW-1]]の回帰テスト。CHECK-47（parser⇔Layer3 TTM乖離）は
normalized/側の末尾4件を単純合計していたため、途中の四半期（10-K由来のQ4等）が
normalized/に無い銘柄では1年分にならない4件を合計してTTMと比較し、誤検知していた
（BKNG stock_based_compensation 2.2%・RCAT SBC 8.2%、2026-09-24確認）。
- 末尾4件が連続しない場合はスキップしてWARNを出さない
- 連続した4四半期で値が食い違う場合は従来どおりWARNを出す（検知力を落とさない）

実行方法:
    python -m pytest tests/test_check47_contiguous_window.py -v
"""

import json

import pytest

from common.sec_data import report_consistency_check as rcc

_TICKER = "ZZ47"

# BKNG型: 2025Q4（10-K由来）がnormalized/に無い
_NONCONTIGUOUS = [
    ("2025-01-01", "2025-03-31", 143), ("2025-04-01", "2025-06-30", 154),
    ("2025-07-01", "2025-09-30", 153),
    ("2026-01-01", "2026-03-31", 141), ("2026-04-01", "2026-06-30", 140),
]
_CONTIGUOUS = [
    ("2025-07-01", "2025-09-30", 153), ("2025-10-01", "2025-12-31", 167),
    ("2026-01-01", "2026-03-31", 141), ("2026-04-01", "2026-06-30", 140),
]


def _run(tmp_path, monkeypatch, quarters, ttm_val):
    norm = {"fields": {"SBC": [
        {"start": s, "end": e, "val": v * 10**6, "fp": "Q", "form": "10-Q"} for s, e, v in quarters
    ]}}
    ttm = {"series": [{"ttm_end": "2026-06-30", "flow": {
        "stock_based_compensation": {"val": ttm_val * 10**6, "quarters_used": 4, "missing": 0},
    }}]}
    (tmp_path / "norm").mkdir()
    (tmp_path / "ttm").mkdir()
    (tmp_path / "norm" / f"{_TICKER}_quarterly_normalized.json").write_text(json.dumps(norm), encoding="utf-8")
    (tmp_path / "ttm" / f"{_TICKER}_ttm_series.json").write_text(json.dumps(ttm), encoding="utf-8")
    monkeypatch.setattr(rcc, "NORMALIZED_DIR", str(tmp_path / "norm"))
    monkeypatch.setattr(rcc, "TTM_DIR", str(tmp_path / "ttm"))
    monkeypatch.setattr(rcc, "_WARN47_FIELD_MAP", {"stock_based_compensation": ("SBC", "stock_based_compensation")})
    return rcc._check_ttm_parser_layer3_reconciliation(_TICKER)


def test_noncontiguous_last4_is_skipped(tmp_path, monkeypatch):
    # 末尾4件（Q2'25・Q3'25・Q1'26・Q2'26）の合計588はTTM 601と2.2%乖離するが、
    # 同一期間の比較ではないためWARNを出さない
    assert _run(tmp_path, monkeypatch, _NONCONTIGUOUS, 601) == []


def test_contiguous_match_has_no_warn(tmp_path, monkeypatch):
    assert _run(tmp_path, monkeypatch, _CONTIGUOUS, 601) == []


def test_contiguous_mismatch_still_warns(tmp_path, monkeypatch):
    warn = _run(tmp_path, monkeypatch, _CONTIGUOUS, 650)
    assert len(warn) == 1 and "WARN-47" in warn[0]


@pytest.mark.parametrize("quarters,expected", [
    (_CONTIGUOUS, True),
    (_NONCONTIGUOUS[-4:], False),
    # 52/53週決算（数日のずれ）は連続とみなす
    ([("2024-07-28", "2024-10-27", 1), ("2024-10-28", "2025-01-26", 1),
      ("2025-01-27", "2025-04-27", 1), ("2025-04-28", "2025-07-27", 1)], True),
])
def test_is_contiguous_four_quarters(quarters, expected):
    entries = [{"start": s, "end": e} for s, e, _ in quarters]
    assert rcc._is_contiguous_four_quarters(entries) is expected
