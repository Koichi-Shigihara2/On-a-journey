"""
TANUKI TAIL のレビューとDCFシナリオの「最新」の決め方（[[TAIL-REVIEW-LATEST-MISMATCH-1]]、2026-10-16）。

後から作られた古い期（2025Q4）のレビューにファイルの「最新」の印が移り、tail_dcf_bridge.pyがそれでシナリオを作っていた
（APP・CELH・SOUN・PLTR・SOFI・TSLA）。最新 = キューのcompletedのうちquarterが最大（画面と同じ規則）。
ファイルの印は読み手が0件になったため、2026-10-19に書くのをやめ、既存のファイルからも消した（FINDINGS.md 2026-10-16）。
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


def _write_review(d, ticker, quarter, **extra):
    rv = {"ticker": ticker, "quarter": quarter, **extra}
    with open(os.path.join(d, f"{ticker}_{quarter}_review.json"), "w", encoding="utf-8") as f:
        json.dump(rv, f)


# ── review_status ─────────────────────────────────────────────

def test_quarter_key_and_latest_completed():
    assert rs.quarter_key("2026Q2") == (2026, 2)
    assert rs.quarter_key("不明") is None
    q = _q(("APP", "2026Q2", "completed"), ("APP", "2025Q4", "superseded"),
           ("APP", "2026Q3", "pending"), ("APP", "不明", "completed"), ("CELH", "2026Q3", "completed"))
    assert rs.latest_completed_quarter(q, "APP") == "2026Q2"
    assert rs.excluded_quarters(q, "APP") == {"2025Q4"}
    assert rs.latest_completed_quarter(q, "SOUN") is None


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

def test_bridge_picks_max_completed_quarter(tmp_path, monkeypatch):
    """後から作られた2025Q4があっても、completedの最大の2026Q2を選ぶ。"""
    monkeypatch.setattr(bridge, "REVIEWS_DIR", str(tmp_path))
    _write_review(tmp_path, "PLTR", "2025Q4")
    _write_review(tmp_path, "PLTR", "2026Q2")
    _write_review(tmp_path, "PLTR", "2026Q1")
    q = _q(("PLTR", "2026Q1", "completed"), ("PLTR", "2026Q2", "completed"), ("PLTR", "2025Q4", "completed"))
    assert bridge._load_latest_review("PLTR", queue=q)["quarter"] == "2026Q2"
    # supersededの期・pendingの期は選ばない
    q2 = _q(("PLTR", "2026Q1", "completed"), ("PLTR", "2026Q2", "pending"), ("PLTR", "2025Q4", "superseded"))
    assert bridge._load_latest_review("PLTR", queue=q2)["quarter"] == "2026Q1"


def test_bridge_falls_back_when_file_missing_and_none_without_completed(tmp_path, monkeypatch):
    monkeypatch.setattr(bridge, "REVIEWS_DIR", str(tmp_path))
    _write_review(tmp_path, "SOFI", "2026Q1")
    q = _q(("SOFI", "2026Q2", "completed"), ("SOFI", "2026Q1", "completed"))
    assert bridge._load_latest_review("SOFI", queue=q)["quarter"] == "2026Q1"
    assert bridge._load_latest_review("SOFI", queue=_q()) is None


# ── prediction_tracker ────────────────────────────────────────

def test_prediction_tracker_excludes_superseded_reviews(tmp_path, monkeypatch):
    monkeypatch.setattr(pt, "REVIEWS_DIR", str(tmp_path))
    for quarter in ("2025Q4", "2026Q2"):
        _write_review(tmp_path, "TSLA", quarter)
    q = _q(("TSLA", "2026Q2", "completed"), ("TSLA", "2025Q4", "superseded"))
    assert [r["quarter"] for r in pt._load_reviews_for_ticker("TSLA", queue=q)] == ["2026Q2"]
    q = _q(("TSLA", "2026Q2", "completed"), ("TSLA", "2025Q4", "completed"))
    assert [r["quarter"] for r in pt._load_reviews_for_ticker("TSLA", queue=q)] == ["2025Q4", "2026Q2"]


# ── 公開データ（2026-10-16の修正の後） ─────────────────────────

@pytest.mark.parametrize("ticker", ["APP", "CELH", "SOUN", "PLTR", "SOFI", "TSLA"])
def test_published_queue_and_flags_for_six_tickers(ticker):
    # 新しい10-Qのレビューができると最新の期は進む（2026-10-22にTSLAの2026Q3）。期の値は決め打ちせず、
    # 2026Q2以降であること・後から作った2025Q4を除くこと・DCFのシナリオの元が最新の期とそろうことを確かめる
    q = rs.load_queue()
    latest = rs.latest_completed_quarter(q, ticker)
    assert latest >= "2026Q2"
    assert "2025Q4" in rs.excluded_quarters(q, ticker)
    assert bridge._load_latest_review(ticker, queue=q)["quarter"] == latest


def test_published_reviews_have_no_latest_flag():
    """読み手0件のファイルの印（is_latest）は、公開のレビューのどのファイルにも無い（2026-10-19に削除）。"""
    reviews_dir = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "reviews")
    names = [n for n in os.listdir(reviews_dir) if n.endswith("_review.json")]
    assert names
    for n in names:
        with open(os.path.join(reviews_dir, n), encoding="utf-8") as f:
            assert "is_latest" not in json.load(f), n
