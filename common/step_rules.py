"""段・閾値の切り替わりの共通の部品（[[MARKETPULSE-REVIEW-EPIC-1]] 順4・型P、2026-11-27）。Market Pulse と MACRO PULSE が呼ぶ。

役割を分ける（STEP 0 §9-3・§9-7の決定）:
  - 点数の段 → ramp_score（直線補間。状態を持たない。MACRO PULSE の SIGNAL_STEPS の22境目、幅は L=3）
  - ラベル・発動 → hysteresis（境目の幅。前のラベルを受け取る。MACRO の局面 2pt・セクターの象限 0.5・BUY の発動の出 30）
  - その日の測定の分類（段階1〜5・天気・クレジットの日次の向き）は対象外

連続性の物差しは tests/test_growth_sanity_continuity.py・tests/test_tanuki_calc_continuity.py と同じ（LIPSCHITZ_MAX = 10: 1刻みあたりの
出力の動きが、理論上の傾き〈点数の全幅 ÷ 入力の幅 X〉の10倍以下）。補間の幅の下限は同じ式から出る:
  幅 ≥ X × (その境目の点数差 ÷ 点数の全幅) ÷ L        （min_ramp_width。X＝入力の10〜90%点の幅）
実際の幅は L=3（RAMP_L）で作り、SIGNAL_STEPS の各段の "w" に書く。画面の JS の SIGNAL_STEPS（写し）も同じ "w" を持ち、
tests/test_macro_pulse_accuracy.py が一致を検査する。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

LIPSCHITZ_MAX = 10.0      # 連続性のテストの上限（既存のテストと同じ）
RAMP_L = 3.0              # 補間の幅を作るときの L（STEP 0 §9-7 (b) の決定）
BASIS_ID = "STEP-RULES-1"  # 判定の基準の版（Market Pulse の各エントリの basis.step_rules・MACRO の週次の行の basis）。画面のグラフに、基準を変えた日の線を引く


# MACRO PULSE の8指標の入力の10〜90%点の幅 X（直近1200日、2023-08〜2026-11。STEP 0 §9-4）。補間の幅（SIGNAL_STEPS の "w"）はこれから作った。
# tests/test_step_rules.py が、幅が連続性の物差し（L=LIPSCHITZ_MAX）の下限以上で、L=RAMP_L の式と一致することを検査する
MACRO_INPUT_RANGE_X: Dict[str, float] = {"yc": 0.99, "hy": 1.11, "cbcc2": 175.0, "philly": 48.4, "cfnai": 0.25, "claims": 34750.0, "cbcc": 24.2, "sahm": 0.36}


def min_ramp_width(x_range: float, dy: float, y_total: float, L: float = LIPSCHITZ_MAX) -> float:
    """補間の幅の下限。x_range＝入力の10〜90%点の幅、dy＝その境目の点数差、y_total＝点数の全幅、L＝理論上の傾きの何倍まで許すか"""
    return x_range * (abs(dy) / y_total) / L


def _hit(t: Dict[str, Any], val: float) -> bool:
    return val < t["x"] if t["cmp"] == "<" else val > t["x"] if t["cmp"] == ">" else val >= t["x"]


def hard_score(table: Dict[str, Any], val: float, trend_dir: int = 0) -> float:
    """今までの段の点数（補間なし）。トレンドの加点つき"""
    for t in table["tiers"]:
        if _hit(t, val):
            tr = t.get("trend")
            return t["score"] + (tr["add"] if tr and trend_dir == tr["dir"] else 0)
    return table["else"]["score"]


def ramp_score(table: Dict[str, Any], val: float, trend_dir: int = 0) -> float:
    """段の表（SIGNAL_STEPS の1指標）の点数を、境目の幅の中で直線補間する。各段の "w"（境目 x を中心とした幅）が無い・0 なら段のまま。
    トレンドの加点（philly・claims の±10）は、値の段で決めて二値のまま足す（直前の3点の向きという別の入力）。
    境目ごとに「悪い側の点数 → 良い側の点数」を結ぶ。幅は境目どうしの間隔より狭い前提（表で検査）"""
    tiers = table["tiers"]
    regs = [t["score"] for t in tiers] + [table["else"]["score"]]
    lt = tiers[0]["cmp"] == "<"
    y: Optional[float] = None
    for i, t in enumerate(tiers):
        w = float(t.get("w") or 0.0)
        x = t["x"]
        lo, hi = x - w / 2, x + w / 2
        if lt:
            if val < lo:
                y = float(regs[i]); break
            if val < hi and w > 0:
                y = regs[i] + (regs[i + 1] - regs[i]) * (val - lo) / (hi - lo); break
        else:
            if val > hi:
                y = float(regs[i]); break
            if val > lo and w > 0:
                y = regs[i] + (regs[i + 1] - regs[i]) * (hi - val) / (hi - lo); break
    if y is None:
        y = float(regs[-1])
    # トレンドの加点: 値の段（段のまま）が持つ加点
    hard = hard_score(table, val, trend_dir)
    plain = hard_score(table, val, 0)
    return y + (hard - plain)


def check_table(table: Dict[str, Any]) -> List[str]:
    """表の検査: 幅が負でない・隣り合う境目の幅が重ならない。問題の文の一覧（無ければ空）"""
    out: List[str] = []
    ts = table["tiers"]
    for a, b in zip(ts, ts[1:]):
        wa, wb = float(a.get("w") or 0.0), float(b.get("w") or 0.0)
        if abs(a["x"] - b["x"]) < (wa + wb) / 2:
            out.append(f"境目 {a['x']} と {b['x']} の補間の幅が重なる（{wa}・{wb}）")
    if any(float(t.get("w") or 0.0) < 0 for t in ts):
        out.append("幅が負")
    return out


def hysteresis(prev: Optional[str], value: float, bounds: Sequence[float], labels: Sequence[str], band: float) -> str:
    """境目 bounds（昇順、len(labels)-1 個）で分かれるラベルを、前のラベル prev から、境目を band だけ越えたときだけ切り替える。
    value ≥ 境目 のとき上側（今までの規則: スコアが境目ちょうどなら上のラベル）。前のラベルが無い（None・知らない名前）ときは、
    その日の段の判定から始める（初日・基準を切り替えた日。切り替えでラベルが変わらないよう、呼び出し側は前の公開のラベルを渡す）"""
    def hard() -> int:
        return sum(1 for b in bounds if value >= b)
    if prev not in labels:
        return labels[hard()]
    i = list(labels).index(prev)
    while i < len(bounds) and value >= bounds[i] + band:
        i += 1
    while i > 0 and value < bounds[i - 1] - band:
        i -= 1
    return labels[i]


def hysteresis_gate(prev_on: Optional[bool], value: float, enter: float, exit_: float, below_is_on: bool = True) -> bool:
    """発動の入り切り（二値）。below_is_on=True なら value ≤ enter で入り、前が入りなら value ≤ exit_ の間は入りのまま（exit_ ≥ enter）。
    前が無い（None）なら、その日の段の判定（≤ enter）"""
    if below_is_on:
        return value <= enter or bool(prev_on and value <= exit_)
    return value >= enter or bool(prev_on and value >= exit_)


def quadrant_hysteresis(prev_quadrant: Optional[str], ratio: float, mom: float, band: float) -> str:
    """セクターの象限（RS-Ratio・RS-Momentum の境目100）。前の象限から、各軸を band だけ越えたときだけ切り替える。
    Strong＝比≥100・勢い≥100、Weakening＝比≥100・勢い<100、Improving＝比<100・勢い≥100、Weak＝比<100・勢い<100"""
    hi_r = hysteresis({"Strong": "hi", "Weakening": "hi", "Improving": "lo", "Weak": "lo"}.get(prev_quadrant or ""), ratio, [100.0], ["lo", "hi"], band) == "hi"
    hi_m = hysteresis({"Strong": "hi", "Improving": "hi", "Weakening": "lo", "Weak": "lo"}.get(prev_quadrant or ""), mom, [100.0], ["lo", "hi"], band) == "hi"
    if hi_r:
        return "Strong" if hi_m else "Weakening"
    return "Improving" if hi_m else "Weak"
