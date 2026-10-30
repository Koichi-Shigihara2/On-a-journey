"""
TANUKI TAIL のレビューを対象の期のデータで作る・後から作ったレビューに印（[[TAIL-REVIEW-PERIOD-FILTER-1]]、2026-10-30）。

SEC EDGAR Layer3は対象の期末日以前・申告日以前に申告された値（first_val）、Layer2は対象の期以前の期、TAIL側のLayer3
（本文のKPI）は期が同じときだけ。backfilledは「対象の期の次の四半期の申告日 < 作った日」で決め、比較・集計から外す期の
判定（superseded・skipped_old・backfilled・対象より後の期）は review_status.is_comparable() の1か所。
storeは手で作り、Grok・SECには触れない。
"""
import json
import os
import sys

import pytest

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.tail import review_status as rs  # noqa: E402
from src.tail import quarterly_review_generator as qrg  # noqa: E402
from src.tail import prediction_tracker as pt  # noqa: E402


def _e(end, val, filed, accn, first_filed=None, first_val=None, form="10-Q", **extra):
    return {"end": end, "val": val, "filed": filed, "accn": accn, "form": form,
            "first_filed": first_filed or filed, "first_val": val if first_val is None else first_val,
            "is_annual": False, "is_ytd": False, **extra}


def _store():
    # 2025Q3（11-05申告）・2025Q4（10-K、2026-02-20申告）・2026Q1（05-06申告）。2025Q3の売上は2026-05-06の申告で組み替え
    rev = [
        _e("2025-06-30", 90.0, "2025-08-05", "A-Q2"),
        _e("2025-09-30", 120.0, "2026-05-06", "A-26Q1", first_filed="2025-11-05", first_val=100.0),
        _e("2025-12-31", 130.0, "2026-02-20", "A-K", form="10-K"),
        _e("2026-03-31", 150.0, "2026-05-06", "A-26Q1"),
    ]
    oi = [
        _e("2025-06-30", 9.0, "2025-08-05", "A-Q2"),
        _e("2025-09-30", 10.0, "2025-11-05", "A-Q3"),
        _e("2025-12-31", 13.0, "2026-02-20", "A-K", form="10-K"),
        _e("2026-03-31", 30.0, "2026-05-06", "A-26Q1"),
    ]
    return {"fields": {"revenue": {"entries": rev}, "operating_income": {"entries": oi}}}


# ── 期末日・次の申告日・backfilled ───────────────────────────

def test_target_period_end_by_accn_or_quarter_name():
    st = _store()
    assert qrg._target_period_end(st, "A-K", "2025Q4") == "2025-12-31"
    # accnがLayer3に無い（古いレビュー）なら期の名前（暦の四半期）で
    assert qrg._target_period_end(st, "", "2025Q3") == "2025-09-30"
    assert qrg._target_period_end(st, "", "2024Q1") is None


def test_next_filing_date_is_first_filed_after_period_end():
    st = _store()
    assert qrg._next_filing_date(st, "2025-09-30") == "2026-02-20"
    assert qrg._next_filing_date(st, "2026-03-31") is None


@pytest.mark.parametrize("next_filed,generated_at,expected", [
    ("2026-05-06", "2026-09-03T10:00:00+09:00", True),   # 次の申告の後に作った
    ("2026-08-26", "2026-08-20T19:48:00+09:00", False),  # 次の申告の前（NVDA_2026Q2の形）
    ("2026-08-20", "2026-08-20T19:48:00+09:00", False),  # 同じ日は後付けにしない
    (None, "2026-09-03T10:00:00+09:00", False),          # 次の申告がまだ無い
])
def test_is_backfilled(next_filed, generated_at, expected):
    assert rs.is_backfilled(next_filed, generated_at) is expected


# ── SEC EDGAR Layer3の絞り込み ────────────────────────────────

def test_layer1_financials_filtered_to_period_and_first_val():
    st = _store()
    r = qrg.load_layer1_financials("T", period_end="2025-09-30", as_of="2025-11-05", store=st)
    # 2025Q3の売上は組み替え前の100（first_val）、2025Q4・2026Q1は入らない
    assert r["operating_margin"] == round(10.0 / 100.0, 4)
    assert [h["quarter"] for h in r["operating_margin_history"]] == ["2025Q2", "2025Q3"]


def test_layer1_financials_without_period_is_latest_as_before():
    st = _store()
    st["latest_period_end"] = "2026-03-31"
    r = qrg.load_layer1_financials("T", store=st)
    assert r["operating_margin"] == round(30.0 / 150.0, 4)
    assert r["operating_margin_history"][-1]["quarter"] == "2026Q1"


def test_layer1_drops_values_first_filed_after_as_of():
    st = _store()
    # 申告日の前の時点では、2025Q3はまだ申告されていない → 直近は2025Q2
    r = qrg.load_layer1_financials("T", period_end="2025-09-30", as_of="2025-11-04", store=st)
    assert r["operating_margin"] == round(9.0 / 90.0, 4)


# ── Layer2・TAIL側のLayer3 ───────────────────────────────────

def test_layer2_filtered_by_quarter_not_filed():
    l2 = {"kpis": {"売上": {"unit": "USD", "data": [
        {"quarter": "2026Q1", "value": 3, "filed": "2026-05-06"},
        {"quarter": "2025Q3", "value": 2, "filed": "2026-11-04"},   # 後の申告の比較欄から入った前年同期
        {"quarter": "2025Q2", "value": 1, "filed": "2025-08-05"},
    ]}}}
    out = qrg.filter_layer2_to_quarter(l2, "2025Q3")
    assert [d["quarter"] for d in out["kpis"]["売上"]["data"]] == ["2025Q3", "2025Q2"]
    assert len(l2["kpis"]["売上"]["data"]) == 3                       # 元は変えない
    assert list(qrg.build_kpi_snapshot(out)["売上"]) == ["2025Q3", "2025Q2"]


def test_tail_layer3_only_for_same_quarter():
    l3 = {"quarter": "2026Q2", "kpis": {"会員数": {"value": 1}}}
    assert qrg.layer3_kpi_for_quarter(l3, "2026Q2") is l3
    assert qrg.layer3_kpi_for_quarter(l3, "2025Q3") is None


# ── 比較・集計から外す期（1か所の判定） ───────────────────────

def _q(*entries):
    return {"queue": [{"ticker": t, "quarter": q, "status": s} for t, q, s in entries]}


def _write_review(d, ticker, quarter, **extra):
    rv = {"ticker": ticker, "quarter": quarter, "stage1": {"health_score": 50, "recommendation": "WATCH"},
          "call2": {"x": quarter}, **extra}
    with open(os.path.join(d, f"{ticker}_{quarter}_review.json"), "w", encoding="utf-8") as f:
        json.dump(rv, f)


def test_is_comparable_rules():
    assert rs.is_comparable({"quarter": "2026Q1"}, set())
    assert not rs.is_comparable({"quarter": "2025Q4"}, {"2025Q4"})
    assert not rs.is_comparable({"quarter": "2025Q3", "backfilled": True}, set())
    assert rs.is_comparable({"quarter": "2025Q3", "backfilled": False}, set())
    assert not rs.is_comparable({"quarter": "2026Q2"}, set(), before_quarter="2026Q2")
    assert not rs.is_comparable({"quarter": "2026Q3"}, set(), before_quarter="2026Q2")
    assert rs.is_comparable({"quarter": "2026Q1"}, set(), before_quarter="2026Q2")


def _setup(tmp_path, monkeypatch):
    for q, extra in [("2025Q3", {"backfilled": True}), ("2025Q4", {}), ("2026Q1", {}), ("2026Q2", {}), ("2026Q3", {})]:
        _write_review(tmp_path, "SOFI", q, **extra)
    queue = _q(("SOFI", "2025Q3", "completed"), ("SOFI", "2025Q4", "superseded"), ("SOFI", "2026Q1", "completed"),
               ("SOFI", "2026Q2", "completed"), ("SOFI", "2026Q3", "completed"))
    monkeypatch.setattr(qrg, "REVIEWS_DIR", str(tmp_path))
    monkeypatch.setattr(pt, "REVIEWS_DIR", str(tmp_path))
    monkeypatch.setattr(rs, "load_queue", lambda path=None: queue)
    return queue


def test_generator_past_scores_and_call2_use_comparable(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    # 2026Q2を作るとき: 後付け（2025Q3）・superseded（2025Q4）・同じ期と後の期（2026Q2・Q3）を外す
    assert qrg._load_past_health_scores("SOFI", "2026Q2") == "2026Q1:50(WATCH)"
    assert [x["quarter"] for x in qrg._load_past_call2("SOFI", "2026Q3")] == ["2026Q2", "2026Q1"]


def test_prediction_tracker_excludes_backfilled(tmp_path, monkeypatch):
    queue = _setup(tmp_path, monkeypatch)
    assert [r["quarter"] for r in pt._load_reviews_for_ticker("SOFI", queue=queue)] == ["2026Q1", "2026Q2", "2026Q3"]


# ── 公開データ（2026-10-30の印） ──────────────────────────────

_REVIEWS_DIR = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "reviews")
_BACKFILLED_2026_10_30 = (
    ["APP_2025Q4", "CELH_2025Q4", "NVDA_2026Q1", "SOUN_2025Q4"]
    + [f"{t}_{q}" for t in ("PLTR", "SOFI", "TSLA")
       for q in ("2023Q3", "2024Q1", "2024Q2", "2024Q3", "2025Q1", "2025Q2", "2025Q3", "2025Q4")]
)


def test_published_backfilled_marks():
    """2026-10-30に印を付けた28本は backfilled=true・理由・データの時点がある。NVDA_2026Q2は後付けではない。"""
    assert len(_BACKFILLED_2026_10_30) == 28
    for name in _BACKFILLED_2026_10_30:
        with open(os.path.join(_REVIEWS_DIR, f"{name}_review.json"), encoding="utf-8") as f:
            rv = json.load(f)
        assert rv["backfilled"] is True, name
        assert rv["backfilled_note"] and rv["data_as_of"], name
    with open(os.path.join(_REVIEWS_DIR, "NVDA_2026Q2_review.json"), encoding="utf-8") as f:
        assert json.load(f).get("backfilled") is not True
