"""株数の変化（希薄化率）の共通の部品（[[DILUTION-DEFINITION-UNIFY-1]]、2026-11-12。docs/architecture/DILUTION_UNIFY_STEP0.md）。

「希薄化」という名前の3つの定義のうち、**株数の変化**だけをここに寄せる。ほかの2つは概念が違うので名前を分けて残す:
  - SBC負担（STONKSの`sbc_burden`、SBC÷売上）= 株数ではなく費用の比率
  - 増資による資金調達（定義v2の`equity_raise_ratio`）= まだ作らない

share_count_change:
  直近の四半期の加重平均の希薄化後株数 ÷ 約3年前（1,096日前±50日の四半期）の年率。Layer3の四半期の株数はQ4が無い（Q1〜Q3だけ）ので、
  「12個前の点」ではなく**日付**で3年前の点を探す（12個前は約4年前になる）。年率は実際の日数で割る。
  - 分割は config/split_history.yaml（株数の比）で調整する（株価の調整の比は split_price_ledger.json の price_factor で別。
    POINT-IN-TIME-HISTORY-INTEGRITY-1 (b)）
  - 未登録の分割の安全網: 隣り合う四半期の比が2.5倍超・0.4倍未満なら、割り戻さずに印（split_suspect）を付ける。
    印が付いた銘柄は funda点の減点に使わない（分割か本当の希薄化かをここで決めない）
  - 3年前の点が無いときは欠け（年次に落とさない＝窓の境目で値が飛ばない）
  - 発行済株数（期末、Layer3）との差（TSLA型: 制限付株式などで加重平均に入らない株）は表示だけの補助の印 outstanding_gap_pct

dilution_penalty: funda点の減点（傾斜）。20%以下は0、20〜40%で0→25点の直線、40%以上は25点（段の分岐は無い。2026-11-12のチャット側の決定）。

上場（IPO・分社・SPAC合併）の前後で株数が跳ぶ銘柄（ALAB・CART・RBRK・LOAR・GEV・CRWV）:
  3年前の点が上場前（最初の定期報告の期末以前）なら、上場後の最初の四半期を基準にする（上場後の四半期が4四半期以上あるときだけ。
  年率は実際の日数）。上場の時点はSECの申告（submissions.json の最初の定期報告の期末）から機械的に決める。銘柄名は手で書かない。
  4四半期に満たなければ欠け。上場の前後の跳びが窓に入らないので、split_suspect は残らない
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

WINDOW_DAYS = 1096                # 3年前（直近の期末からの日数）
WINDOW_TOLERANCE_DAYS = 50        # 3年前の点を探す幅。Q4が無いので同じ四半期の3年前が入る。無ければ欠け
SPLIT_RATIO_UP = 2.5
SPLIT_RATIO_DOWN = 0.4
OUTSTANDING_GAP_MARK = 10.0       # 発行済株数との差がこれ（%）を超えたら表示の印

SLOPE_LOW, SLOPE_HIGH = 20.0, 40.0   # 減点の傾斜の両端（%）
PENALTY_MAX = 25                      # 40%以上の減点（点）
MIN_SINCE_LISTING_DAYS = 270          # 上場後の基準点から直近まで。4四半期（3つの間隔≒273日）に満たなければ欠け


def _days(a: str, b: str) -> int:
    return (date.fromisoformat(b[:10]) - date.fromisoformat(a[:10])).days


def first_report_end(ticker: str, data_dir: Optional[str] = None) -> Optional[str]:
    """SECの申告（submissions.json）で最初に提出された定期報告（10-Q・10-K）の期末。上場（IPO・分社・SPAC合併）の後の最初の報告の期。
    申告の一覧（accn_to_filingdate）の最も古い提出の期末（accn_to_reportdate）。無ければ None"""
    import json
    import os
    base = data_dir or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "common", "sec_data", "data")
    try:
        with open(os.path.join(base, ticker, "submissions.json"), encoding="utf-8") as f:
            sub = json.load(f)
    except (OSError, ValueError):
        return None
    filed = sub.get("accn_to_filingdate") or {}
    if not filed:
        return None
    first = min(filed, key=lambda a: filed[a])
    return (sub.get("accn_to_reportdate") or {}).get(first)


def share_count_change(quarters: Sequence[Tuple[str, float]], splits: Optional[List[dict]] = None,
                       outstanding: Optional[Tuple[str, float]] = None,
                       first_report_period_end: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """quarters: [(期末, 加重平均の希薄化後株数)]（四半期。順不同でよい）。splits: split_history.yaml のその銘柄の行。
    outstanding: (期末, 発行済株数) 直近の期末の値（あれば補助の印を作る）。
    戻り値: {"cagr_pct", "shares_now", "shares_start", "end", "start_end", "n", "split_suspect", "outstanding_gap_pct"?}。出せなければ None"""
    from common.sec_data.split_adjust import adjust_share_points
    pts = sorted({(e, float(v)) for e, v in quarters if e and v and v > 0}, key=lambda x: x[0])
    if len(pts) < 2:
        return None
    vals = adjust_share_points(pts, splits) if splits else [v for _, v in pts]
    now_i = len(pts) - 1
    cand = [(abs(_days(pts[i][0], pts[now_i][0]) - WINDOW_DAYS), i) for i in range(now_i)]
    diff, start_i = min(cand)
    basis = "3y"
    listing = first_report_period_end
    point_ok = diff <= WINDOW_TOLERANCE_DAYS and not (listing and pts[start_i][0] <= listing)
    if not point_ok:
        # 3年前の点が無い、または上場の前（最初の定期報告の期末以前）。上場が窓の中（3年以内）にあるときだけ、
        # 上場後の最初の四半期を基準にする。上場が3年より前なのに点が無いのは単なる欠けなので、欠けのまま
        if not listing or _days(listing, pts[now_i][0]) >= WINDOW_DAYS + WINDOW_TOLERANCE_DAYS:
            return None
        post = [i for i in range(now_i) if pts[i][0] > listing]
        if not post or _days(pts[post[0]][0], pts[now_i][0]) < MIN_SINCE_LISTING_DAYS:
            return None
        start_i, basis = post[0], "since_listing"
    span = _days(pts[start_i][0], pts[now_i][0])
    # 未登録の分割の安全網（窓の中だけ）: 割り戻さずに印
    suspect = [pts[i][0] for i in range(start_i + 1, now_i + 1)
               if vals[i - 1] > 0 and not (SPLIT_RATIO_DOWN <= vals[i] / vals[i - 1] <= SPLIT_RATIO_UP)]
    cagr = ((vals[now_i] / vals[start_i]) ** (365.25 / span) - 1) * 100
    out: Dict[str, Any] = {"cagr_pct": cagr, "shares_now": vals[now_i], "shares_start": vals[start_i],
                           "end": pts[now_i][0], "start_end": pts[start_i][0], "n": len(pts),
                           "split_suspect": suspect, "basis": basis}
    if outstanding and outstanding[1] and outstanding[1] > 0:
        out["outstanding_gap_pct"] = (outstanding[1] / vals[now_i] - 1) * 100
        out["outstanding_end"] = outstanding[0]
    return out


def dilution_penalty(pct: Optional[float]) -> float:
    """funda点の減点（正の値＝引く点）。pct は株数の変化の年率（%）。20%以下は0、20〜40%で0→25の直線、40%以上は25"""
    if pct is None or pct <= SLOPE_LOW:
        return 0.0
    if pct >= SLOPE_HIGH:
        return float(PENALTY_MAX)
    return PENALTY_MAX * (pct - SLOPE_LOW) / (SLOPE_HIGH - SLOPE_LOW)
