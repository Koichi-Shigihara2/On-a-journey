"""株数の変化（希薄化率）の共通の部品（[[DILUTION-DEFINITION-UNIFY-1]]、2026-11-12。docs/architecture/DILUTION_UNIFY_STEP0.md）。

「希薄化」という名前の3つの定義のうち、**株数の変化**だけをここに寄せる。ほかの2つは概念が違うので名前を分けて残す:
  - SBC負担（STONKSの`sbc_burden`、SBC÷売上）= 株数ではなく費用の比率
  - 増資による資金調達（定義v2の`equity_raise_ratio`）= まだ作らない

share_count_change:
  直近の四半期の加重平均の希薄化後株数 ÷ 12四半期前（約3年前）の年率。
  - 分割は config/split_history.yaml（株数の比）で調整する（株価の調整の比は split_price_ledger.json の price_factor で別。
    POINT-IN-TIME-HISTORY-INTEGRITY-1 (b)）
  - 未登録の分割の安全網: 隣り合う四半期の比が2.5倍超・0.4倍未満なら、割り戻さずに印（split_suspect）を付ける。
    印が付いた銘柄は funda点の減点に使わない（分割か本当の希薄化かをここで決めない）
  - 13点（12四半期前から直近まで）が無い、または窓が3年から外れるときは欠け（年次に落とさない＝12四半期の境目で値が飛ばない）
  - 発行済株数（期末、Layer3）との差（TSLA型: 制限付株式などで加重平均に入らない株）は表示だけの補助の印 outstanding_gap_pct

dilution_penalty: funda点の減点。mode "step"（今: >20%で−15・>40%で−25）か "slope"（20〜40%で0→−25の直線）。
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

QUARTERS_BACK = 12
WINDOW_DAYS = (1000, 1200)        # 12四半期前〜直近の日数（約3年。決算期の変更・欠けで外れたら欠け）
SPLIT_RATIO_UP = 2.5
SPLIT_RATIO_DOWN = 0.4
OUTSTANDING_GAP_MARK = 10.0       # 発行済株数との差がこれ（%）を超えたら表示の印

PENALTY_MODE = "step"             # "step" | "slope"（採否はチャット側。試算は両方）
STEP_LOW, STEP_HIGH = 20.0, 40.0
PENALTY_LOW, PENALTY_HIGH = 15, 25


def _days(a: str, b: str) -> int:
    return (date.fromisoformat(b[:10]) - date.fromisoformat(a[:10])).days


def share_count_change(quarters: Sequence[Tuple[str, float]], splits: Optional[List[dict]] = None,
                       outstanding: Optional[Tuple[str, float]] = None) -> Optional[Dict[str, Any]]:
    """quarters: [(期末, 加重平均の希薄化後株数)]（四半期。順不同でよい）。splits: split_history.yaml のその銘柄の行。
    outstanding: (期末, 発行済株数) 直近の期末の値（あれば補助の印を作る）。
    戻り値: {"cagr_pct", "shares_now", "shares_start", "end", "start_end", "n", "split_suspect", "outstanding_gap_pct"?}。出せなければ None"""
    from common.sec_data.split_adjust import adjust_share_points
    pts = sorted({(e, float(v)) for e, v in quarters if e and v and v > 0}, key=lambda x: x[0])
    if len(pts) < QUARTERS_BACK + 1:
        return None
    vals = adjust_share_points(pts, splits) if splits else [v for _, v in pts]
    now_i, start_i = len(pts) - 1, len(pts) - 1 - QUARTERS_BACK
    span = _days(pts[start_i][0], pts[now_i][0])
    if not (WINDOW_DAYS[0] <= span <= WINDOW_DAYS[1]):
        return None
    # 未登録の分割の安全網（窓の中だけ）: 割り戻さずに印
    suspect = [pts[i][0] for i in range(start_i + 1, now_i + 1)
               if vals[i - 1] > 0 and not (SPLIT_RATIO_DOWN <= vals[i] / vals[i - 1] <= SPLIT_RATIO_UP)]
    cagr = ((vals[now_i] / vals[start_i]) ** (1 / 3) - 1) * 100
    out: Dict[str, Any] = {"cagr_pct": cagr, "shares_now": vals[now_i], "shares_start": vals[start_i],
                           "end": pts[now_i][0], "start_end": pts[start_i][0], "n": len(pts),
                           "split_suspect": suspect}
    if outstanding and outstanding[1] and outstanding[1] > 0:
        out["outstanding_gap_pct"] = (outstanding[1] / vals[now_i] - 1) * 100
        out["outstanding_end"] = outstanding[0]
    return out


def dilution_penalty(pct: Optional[float], mode: Optional[str] = None) -> float:
    """funda点の減点（正の値＝引く点）。pct は株数の変化の年率（%）"""
    if pct is None:
        return 0.0
    mode = mode or PENALTY_MODE
    if mode == "slope":
        if pct <= STEP_LOW:
            return 0.0
        if pct >= STEP_HIGH:
            return float(PENALTY_HIGH)
        return PENALTY_HIGH * (pct - STEP_LOW) / (STEP_HIGH - STEP_LOW)
    if mode != "step":
        raise ValueError(f"減点の方式が決まりに無い: {mode}")
    if pct > STEP_HIGH:
        return float(PENALTY_HIGH)
    if pct > STEP_LOW:
        return float(PENALTY_LOW)
    return 0.0
