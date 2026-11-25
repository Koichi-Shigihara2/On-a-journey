"""市場が織り込んだ成長率の逆算（[[TANUKI-UPSIDE-MAGNITUDE-INFLATED-1]] 6-2、2026-11-25。EXPECTATIONS-INVESTING-EPIC-1 の最初の一歩）。

今の株価に、理論株価（メインの計算。割引率 Rm＝10%固定）が一致する phase1 の成長率を、二分法で探す。他の前提（phase1の年数・
ターミナル・基準のFCF・正味現金）は今のまま。理論株価の式は `calculator/sensitivity.py::create_sensitivity_calc_func`
（中央のセルがメインの理論株価と一致する設計）で、`tools/sim` の hook（calculate_pt を成長率だけ替えて回す二分法）と同じ式。

解けない銘柄は status="unsolved" にして理由を書く。逆算した成長率は参考の値で、理論株価・判定には入れない。
"""
from __future__ import annotations

from typing import Callable, Optional

G_LO = -0.30
G_HI = 2.0
ITERS = 40

OK = "ok"
UNSOLVED = "unsolved"


def solve(iv_at: Callable[[float], Optional[float]], price: Optional[float], lo: float = G_LO, hi: float = G_HI,
          iters: int = ITERS) -> dict:
    """iv_at(g) = 成長率 g のときの理論株価（成長率について単調増加）。price に一致する g を返す"""
    if price is None or price <= 0:
        return {"status": UNSOLVED, "reverse_g": None, "reason": "株価が取れていない"}
    try:
        f_lo, f_hi = iv_at(lo), iv_at(hi)
    except Exception as e:  # noqa: BLE001（計算の失敗は理由にして続ける）
        return {"status": UNSOLVED, "reverse_g": None, "reason": f"理論株価の計算に失敗（{str(e)[:40]}）"}
    if f_lo is None or f_hi is None:
        return {"status": UNSOLVED, "reverse_g": None, "reason": "理論株価が計算できない"}
    if f_hi == f_lo:
        return {"status": UNSOLVED, "reverse_g": None, "reason": "成長率が理論株価に効かない設定（phase1の成長率が固定）"}
    if price < f_lo:
        return {"status": UNSOLVED, "reverse_g": None, "reason": f"株価が成長率{lo*100:.0f}%の理論株価より低い（解が範囲の外）"}
    if price > f_hi:
        return {"status": UNSOLVED, "reverse_g": None, "reason": f"株価が成長率{hi*100:.0f}%の理論株価より高い（解が範囲の外）"}
    a, b = lo, hi
    for _ in range(iters):
        mid = (a + b) / 2.0
        fm = iv_at(mid)
        if fm is None:
            return {"status": UNSOLVED, "reverse_g": None, "reason": "理論株価が計算できない"}
        if fm < price:
            a = mid
        else:
            b = mid
    return {"status": OK, "reverse_g": (a + b) / 2.0, "reason": None}


def context(upside_percent_beta: Optional[float], tanuki_growth: Optional[float], analyst_growth: Optional[float],
            rev: Optional[dict]) -> dict:
    """上昇余地の横に出す値: β込みWACCの上昇余地（既存の upside_percent_beta）・逆算した成長率・TANUKIとアナリストとの差（pt）"""
    rev = rev or {}
    out = {"upside_percent_beta": upside_percent_beta, "reverse_g": rev.get("reverse_g"), "tanuki_growth": tanuki_growth,
           "analyst_revenue_growth": analyst_growth, "gap_tanuki_pt": None, "gap_analyst_pt": None,
           "status": rev.get("status") or UNSOLVED, "reason": rev.get("reason") or ("逆算していない" if not rev else None)}
    g = rev.get("reverse_g")
    if g is not None:
        if tanuki_growth is not None:
            out["gap_tanuki_pt"] = round((tanuki_growth - g) * 100.0, 1)
        if analyst_growth is not None:
            out["gap_analyst_pt"] = round((analyst_growth - g) * 100.0, 1)
    return out


def report_line(c: Optional[dict]) -> Optional[str]:
    if not c:
        return None
    beta = c.get("upside_percent_beta")
    b = f"β込みWACCの上昇余地 {beta:+.0f}%" if beta is not None else "β込みWACCの上昇余地は空欄"
    if c.get("reverse_g") is None:
        return f"Upside_Context: {b}／逆算した成長率は空欄（{c.get('reason') or '解けていない'}）"
    tg = c.get("tanuki_growth")
    ag = c.get("analyst_revenue_growth")
    parts = [f"逆算した成長率 {c['reverse_g']*100:.1f}%"]
    if c.get("gap_tanuki_pt") is not None:
        parts.append(f"TANUKI {tg*100:.1f}%との差 {c['gap_tanuki_pt']:+.1f}pt")
    if c.get("gap_analyst_pt") is not None:
        parts.append(f"アナリストの売上予想 {ag*100:.1f}%との差 {c['gap_analyst_pt']:+.1f}pt")
    return f"Upside_Context: {b}／" + "・".join(parts)
