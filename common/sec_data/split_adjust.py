"""
株式分割（forward・reverse）の遡及補正の共通ロジック

config/split_history.yaml（正の情報源）に登録された分割について、分割日より
前の株数のうち「分割前基準のまま残っている（未調整の）値」を判定し、分割後
基準へ換算するための係数を返す。SEC XBRLは分割前に提出済みの10-Qを遡及修正
しないため、分割後の10-Qが比較期間として再掲しなかった四半期は分割前の株数の
まま残る（[[SPLIT-REALTIME-GAP-1]]・[[SPLIT-REALTIME-GAP-REVERSE-1]]）。

利用箇所:
  - src/value/adjusted_eps_analyzer/pipeline.py::apply_split_adjustments()
    （EPS ANALYZERの四半期株数・EPS）
  - src/value/tanuki_valuation/pipeline.py（3年希薄化率の計算前に、Layer3の
    年次・四半期のshares_dilutedへ適用）

判定（2026-09-25、方向非依存）:
  未調整の値は基準株数に対する比 r が 1/ratio 付近、調整済みの値は 1 付近に
  なるため、対数上で 1/ratio に近い方を未調整とする（境界は 基準 ÷ √ratio）。
  forward（ratio>1）は境界を下回る値、reverse（ratio<1）は上回る値が未調整。
  基準は次の2種類を併用し、どちらか一方で未調整と判定されれば補正する:
    (1) 分割後の値の平均（旧実装と同じ基準）
    (2) 隣接する値: 分割日に近い順（過去へ）にたどり、直前に判定した値の
        分割後基準の株数
  (2)だけでは、未登録の過去の分割で基準の異なる値が並ぶ場合（NVDA 2021年4:1・
  TSLA 2020年5:1が未登録）に判定を誤る。(1)だけでは、分割前後の実際の増資で
  未調整の値を取りこぼす（KULRは分割後平均が約42Mで、未調整の2021〜2023年の
  92〜118Mが境界119Mを下回る）。旧実装の固定閾値（分割後平均 ÷ ratio × 1.5）は
  reverseで調整済みの値まで二重補正していた（KULR 2024-06-30: 22.68M→2.84M）。
"""

import math
import os
from typing import Dict, List, Optional, Sequence, Tuple

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
_SPLIT_HISTORY_PATH = os.path.join(_REPO_ROOT, "config", "split_history.yaml")


def load_split_history(path: Optional[str] = None) -> Dict[str, List[Dict]]:
    """config/split_history.yamlの`splits`（{ticker: [{date, ratio, notes}]}）を返す。
    ファイルがない・読めない場合は{}。"""
    import yaml
    p = path or _SPLIT_HISTORY_PATH
    if not os.path.exists(p):
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            return (yaml.safe_load(f) or {}).get("splits", {}) or {}
    except Exception:
        return {}


def unadjusted_indices(points: Sequence[Tuple[str, float]], split_date: str, ratio: float) -> List[int]:
    """分割前基準のまま残っている（未調整の）値のインデックスを返す。

    points: [(期末日 'YYYY-MM-DD', 株数), ...]（順不同。株数<=0は判定対象外）
    split_date: 分割の効力発生日。期末日がこれ以降の値は分割後基準とみなす
    ratio: 新株/旧株（forward 10:1なら10、reverse 1-for-8なら0.125）
    分割後の値が1件もない場合は判定できないため空リスト。
    """
    if ratio <= 0 or ratio == 1.0:
        return []
    post = sorted(((e, s) for e, s in points if e >= split_date and s > 0), key=lambda x: x[0])
    if not post:
        return []
    log_inv_ratio = math.log(1.0 / ratio)

    def _closer_to_unadjusted(shares: float, base: float) -> bool:
        log_r = math.log(shares / base)
        return abs(log_r - log_inv_ratio) < abs(log_r)

    post_avg = sum(s for _, s in post) / len(post)
    ref = post[0][1]
    pre = sorted(
        ((i, e, s) for i, (e, s) in enumerate(points) if e < split_date and s > 0),
        key=lambda x: x[1], reverse=True,
    )
    result: List[int] = []
    for i, _e, s in pre:
        if _closer_to_unadjusted(s, post_avg) or _closer_to_unadjusted(s, ref):
            result.append(i)
            ref = s * ratio
        else:
            ref = s
    return result


def adjust_share_points(points: Sequence[Tuple[str, float]], splits: Sequence[Dict]) -> List[float]:
    """登録済みの分割（古い順に適用）で未調整の値を分割後基準へ換算した株数の
    リストを返す（入力と同じ順序）。"""
    values = [float(s) for _, s in points]
    for sp in sorted(splits, key=lambda x: str(x.get("date", ""))):
        ratio = float(sp["ratio"])
        cur = [(points[i][0], values[i]) for i in range(len(points))]
        for i in unadjusted_indices(cur, str(sp["date"]), ratio):
            values[i] = values[i] * ratio
    return values
