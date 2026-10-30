"""
TANUKI TAIL — review_status.py

四半期レビューの「最新」の決め方とキュー（review_queue.json）の状態の共通処理
（[[TAIL-REVIEW-LATEST-MISMATCH-1]]、2026-10-16）。

最新のレビュー = キューでstatusがcompletedのうち、対象期間（quarter、"2026Q2"の形）が最大のもの。
画面（docs/portfolio/tail/index.html・detail.html）の規則（completedだけを読み、quarterの降順の先頭）と同じ。
レビューのファイルのis_latestは使わない（後から古い期を作ると印が移り、DCFシナリオが古いレビューから作られていた）。

キューのstatus:
  pending      生成待ち（quarterly_review_generator.pyが処理する）
  completed    生成済み（画面・DCFシナリオ・予測の振り返りの対象）
  error        生成に失敗
  superseded   生成済みだが、後から作られた古い期のレビューで中身がその期の決算ではないため外した
               （レビューのファイルは残す）
  skipped_old  対象の期がその銘柄のcompletedの最大より古いため生成しなかった
"""

import json
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

_THIS_DIR  = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))
REVIEW_QUEUE_PATH = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "review_queue.json")

STATUS_PENDING     = "pending"
STATUS_COMPLETED   = "completed"
STATUS_SUPERSEDED  = "superseded"
STATUS_SKIPPED_OLD = "skipped_old"
# 生成済み・生成待ちとして扱わない（予測の振り返り等から外す）status
EXCLUDED_STATUSES = frozenset({STATUS_SUPERSEDED, STATUS_SKIPPED_OLD})

_QUARTER_RE = re.compile(r"^(\d{4})Q([1-4])$")


def quarter_key(quarter: Any) -> Optional[Tuple[int, int]]:
    """"2026Q2" → (2026, 2)。形が違う（"不明"等）ならNone。"""
    m = _QUARTER_RE.match(str(quarter or ""))
    return (int(m.group(1)), int(m.group(2))) if m else None


def load_queue(path: Optional[str] = None) -> Dict[str, Any]:
    p = path or REVIEW_QUEUE_PATH
    if not os.path.exists(p):
        return {"queue": []}
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def completed_quarters(queue: Dict[str, Any], ticker: str) -> List[str]:
    """その銘柄のcompletedの期（形の正しいものだけ）を新しい順に返す。"""
    qs = {e.get("quarter") for e in queue.get("queue", [])
          if e.get("ticker") == ticker and e.get("status") == STATUS_COMPLETED}
    return sorted((q for q in qs if quarter_key(q)), key=quarter_key, reverse=True)


def latest_completed_quarter(queue: Dict[str, Any], ticker: str) -> Optional[str]:
    qs = completed_quarters(queue, ticker)
    return qs[0] if qs else None


def excluded_quarters(queue: Dict[str, Any], ticker: str) -> Set[str]:
    """superseded・skipped_oldの期（同じ期にcompletedがあるものは除く）。"""
    done = {e.get("quarter") for e in queue.get("queue", [])
            if e.get("ticker") == ticker and e.get("status") == STATUS_COMPLETED}
    return {e.get("quarter") for e in queue.get("queue", [])
            if e.get("ticker") == ticker and e.get("status") in EXCLUDED_STATUSES
            and e.get("quarter") not in done}


# ── 比較・集計から外すレビュー（[[TAIL-REVIEW-PERIOD-FILTER-1]]、2026-10-30） ──
# 過去のスコアの推移・過去の定性分析の引き継ぎ・予測の振り返りは、この判定を通ったレビューだけを使う。
# 画面（detail.html・index.html）は同じ条件をレビューのbackfilledとキューのstatusで読む（規則はここだけ）。

def is_backfilled(next_filed: Optional[str], generated_at: Optional[str]) -> bool:
    """対象の期の次の四半期の申告日（"YYYY-MM-DD"）がレビューを作った日より前なら、後から作ったレビュー。"""
    if not next_filed or not generated_at:
        return False
    return next_filed < str(generated_at)[:10]


def is_comparable(review: Dict[str, Any], excluded: Set[str], before_quarter: Optional[str] = None) -> bool:
    """
    比較・集計に使えるレビューか。外すのは次の4つ:
      - キューでsuperseded・skipped_oldの期（excluded_quarters()）
      - 後から作ったレビュー（backfilled: true）
      - before_quarterを渡したとき、その期と、それより後の期
    """
    q = review.get("quarter")
    if q in excluded or review.get("backfilled") is True:
        return False
    if before_quarter is not None:
        k, b = quarter_key(q), quarter_key(before_quarter)
        if k is None or b is None or k >= b:
            return False
    return True


def comparable_reviews(reviews_dir: str, ticker: str, queue: Optional[Dict[str, Any]] = None,
                       before_quarter: Optional[str] = None) -> List[Dict[str, Any]]:
    """reviews_dirのその銘柄のレビューのうち、is_comparable()を通るものを期の昇順で返す。"""
    if not os.path.isdir(reviews_dir):
        return []
    excluded = excluded_quarters(load_queue() if queue is None else queue, ticker)
    out: List[Dict[str, Any]] = []
    for fname in os.listdir(reviews_dir):
        if not (fname.startswith(f"{ticker}_") and fname.endswith("_review.json")):
            continue
        try:
            with open(os.path.join(reviews_dir, fname), encoding="utf-8") as f:
                rv = json.load(f)
        except (OSError, ValueError):
            continue
        rv.setdefault("quarter", fname[len(ticker) + 1: -len("_review.json")])
        if is_comparable(rv, excluded, before_quarter):
            out.append(rv)
    out.sort(key=lambda r: quarter_key(r.get("quarter")) or (0, 0))
    return out
