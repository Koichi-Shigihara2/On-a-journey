"""
TANUKI TAIL のレビューとDCFシナリオの「最新」の決め方（[[TAIL-REVIEW-LATEST-MISMATCH-1]]、2026-10-16）。

後から作られた古い期（2025Q4）のレビューにis_latestが移り、tail_dcf_bridge.pyがそれでシナリオを作っていた
（APP・CELH・SOUN・PLTR・SOFI・TSLA）。最新 = キューのcompletedのうちquarterが最大（画面と同じ規則）。
ファイルは一時ディレクトリに作り、Grok・SEC・TANUKIのデータには触れない。
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
from src.tail import tail_dcf_bridge as bridge  # noqa: E402
from src.tail import prediction_tracker as pt  # noqa: E402


def _q(*entries):
    return {"queue": [{"ticker": t, "quarter": q, "status": s, "accn": f"{t}-{q}"} for t, q, s in entries]}


def _write_review(d, ticker, quarter, is_latest, **extra):
    rv = {"ticker": ticker, "quarter": quarter, "is_latest": is_latest, **extra}
    with open(os.path.join(d, f"{ticker}_{quarter}_review.json"), "w", encoding="utf-8") as f:
        json.dump(rv, f)


def _read(d, ticker, quarter):
    with open(os.path.join(d, f"{ticker}_{quarter}_review.json"), encoding="utf-8") as f:
        return json.load(f)


# ── review_status ─────────────────────────────────────────────

def test_quarter_key_and_latest_completed():
    assert rs.quarter_key("2026Q2") == (2026, 2)
    assert rs.quarter_key("不明") is None
    q = _q(("APP", "2026Q2", "completed"), ("APP", "2025Q4", "superseded"),
           ("APP", "2026Q3", "pending"), ("APP", "不明", "completed"), ("CELH", "2026Q3", "completed"))
    assert rs.latest_completed_quarter(q, "APP") == "2026Q2"
    assert rs.excluded_quarters(q, "APP") == {"2025Q4"}
    assert rs.latest_completed_quarter(q, "SOUN") is None


# ── quarterly_review_generator: is_latest ────────────────────

def test_saving_older_quarter_later_does_not_move_is_latest(tmp_path, monkeypatch):
    """APPの形: 2026Q2がcompletedのあとに2025Q4を保存しても、is_latestは2026Q2のまま。"""
    monkeypatch.setattr(qrg, "REVIEWS_DIR", str(tmp_path))
    _write_review(tmp_path, "APP", "2026Q2", True)
    q = _q(("APP", "2026Q2", "completed"), ("APP", "2025Q4", "pending"))
    assert qrg.is_latest_quarter(q, "APP", "2025Q4") is False
    # 新しい期は印を取り、既存の印を外す（保存する期のファイルは外さない）
    assert qrg.is_latest_quarter(q, "APP", "2026Q3") is True
    assert qrg.is_latest_quarter(q, "APP", "2026Q2") is True   # 同じ期の作り直し
    _write_review(tmp_path, "APP", "2026Q3", True)
    qrg._mark_older_reviews_not_latest("APP", keep_quarter="2026Q3")
    assert _read(tmp_path, "APP", "2026Q2")["is_latest"] is False
    assert _read(tmp_path, "APP", "2026Q3")["is_latest"] is True


def test_is_latest_for_first_review_and_unknown_quarter():
    assert qrg.is_latest_quarter(_q(), "NEW", "2026Q2") is True
    assert qrg.is_latest_quarter(_q(), "NEW", "不明") is True
    assert qrg.is_latest_quarter(_q(("NEW", "2026Q2", "completed")), "NEW", "不明") is False


# ── quarterly_review_generator: skipped_old ──────────────────

def test_older_pending_is_skipped_without_generating(monkeypatch):
    queue = _q(("CELH", "2026Q2", "completed"), ("CELH", "2025Q4", "pending"), ("CELH", "2026Q3", "pending"))
    saved = {}
    called = []
    monkeypatch.setattr(qrg, "load_queue", lambda: queue)
    monkeypatch.setattr(qrg, "save_queue", lambda q: saved.setdefault("q", q))
    monkeypatch.setattr(qrg, "generate_review",
                        lambda entry, dry_run=False, queue=None: called.append(entry["quarter"]) or None)
    monkeypatch.setattr(sys, "argv", ["quarterly_review_generator.py"])
    qrg.main()
    assert called == ["2026Q3"]                      # 古い期はGrokを呼ばない
    old = next(e for e in saved["q"]["queue"] if e["quarter"] == "2025Q4")
    assert old["status"] == rs.STATUS_SKIPPED_OLD
    assert "2026Q2" in old["skipped_reason"] and old["skipped_at"]


def test_skipped_old_not_applied_when_no_completed_or_unknown_quarter():
    q = _q(("SOUN", "2025Q4", "pending"))
    assert qrg.is_older_than_completed(q, "SOUN", "2025Q4") is None
    q = _q(("SOUN", "2026Q2", "completed"), ("SOUN", "不明", "pending"))
    assert qrg.is_older_than_completed(q, "SOUN", "不明") is None
    assert qrg.is_older_than_completed(q, "SOUN", "2025Q4") == "2026Q2"


# ── tail_dcf_bridge ───────────────────────────────────────────

def test_bridge_picks_max_completed_quarter_ignoring_is_latest(tmp_path, monkeypatch):
    """データの形（2025Q4にis_latest=True、2026Q2がFalse）でも2026Q2を選ぶ。"""
    monkeypatch.setattr(bridge, "REVIEWS_DIR", str(tmp_path))
    _write_review(tmp_path, "PLTR", "2025Q4", True)
    _write_review(tmp_path, "PLTR", "2026Q2", False)
    _write_review(tmp_path, "PLTR", "2026Q1", False)
    q = _q(("PLTR", "2026Q1", "completed"), ("PLTR", "2026Q2", "completed"), ("PLTR", "2025Q4", "completed"))
    assert bridge._load_latest_review("PLTR", queue=q)["quarter"] == "2026Q2"
    # supersededの期・pendingの期は選ばない
    q2 = _q(("PLTR", "2026Q1", "completed"), ("PLTR", "2026Q2", "pending"), ("PLTR", "2025Q4", "superseded"))
    assert bridge._load_latest_review("PLTR", queue=q2)["quarter"] == "2026Q1"


def test_bridge_falls_back_when_file_missing_and_none_without_completed(tmp_path, monkeypatch):
    monkeypatch.setattr(bridge, "REVIEWS_DIR", str(tmp_path))
    _write_review(tmp_path, "SOFI", "2026Q1", False)
    q = _q(("SOFI", "2026Q2", "completed"), ("SOFI", "2026Q1", "completed"))
    assert bridge._load_latest_review("SOFI", queue=q)["quarter"] == "2026Q1"
    assert bridge._load_latest_review("SOFI", queue=_q()) is None


# ── prediction_tracker ────────────────────────────────────────

def test_prediction_tracker_excludes_superseded_reviews(tmp_path, monkeypatch):
    monkeypatch.setattr(pt, "REVIEWS_DIR", str(tmp_path))
    for quarter in ("2025Q4", "2026Q2"):
        _write_review(tmp_path, "TSLA", quarter, False)
    q = _q(("TSLA", "2026Q2", "completed"), ("TSLA", "2025Q4", "superseded"))
    assert [r["quarter"] for r in pt._load_reviews_for_ticker("TSLA", queue=q)] == ["2026Q2"]
    q = _q(("TSLA", "2026Q2", "completed"), ("TSLA", "2025Q4", "completed"))
    assert [r["quarter"] for r in pt._load_reviews_for_ticker("TSLA", queue=q)] == ["2025Q4", "2026Q2"]


# ── 公開データ（2026-10-16の修正の後） ─────────────────────────

@pytest.mark.parametrize("ticker", ["APP", "CELH", "SOUN", "PLTR", "SOFI", "TSLA"])
def test_published_queue_and_flags_for_six_tickers(ticker):
    q = rs.load_queue()
    assert rs.latest_completed_quarter(q, ticker) == "2026Q2"
    assert "2025Q4" in rs.excluded_quarters(q, ticker)
    reviews_dir = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "reviews")
    assert _read(reviews_dir, ticker, "2026Q2")["is_latest"] is True
    assert _read(reviews_dir, ticker, "2025Q4")["is_latest"] is False
    assert bridge._load_latest_review(ticker, queue=q)["quarter"] == "2026Q2"
