"""TANUKIの成長率とアナリストの予想の差（[[TANUKI-GUIDANCE-REVISION-BLIND-1]] 案1、2026-11-25）。

背景: 理論株価の成長率には、会社の見通しもアナリストの予想も入らない（docs/architecture/GUIDANCE_REVISION_STEP0.md）。
そこで、差の値（growth_gap_pt）と予想の改定の向き（直近30日のEPS予想の引き上げ・引き下げの件数）を**表示だけ**で出す
（閾値の印は付けない。理論株価・判定・stageは変えない。method_changeに登録しない）。

  growth_gap_pt = (TANUKIの成長率 − アナリストの売上予想の伸び〈次の会計年度＝yfinanceの +1y〉) × 100
  予想の改定: 次の会計年度のEPS予想の、直近30日の引き上げ件数・引き下げ件数

予想は common/market_data の attributes/{T}.json の `estimates`（Market Data Weekly で週1回、監視銘柄だけ）。
取れない銘柄は status="no_estimate" にして値を空にし、理由を残す（report.txt に書く）。差の計算はこの関数の1か所だけ。
"""
from __future__ import annotations

from typing import Any, Optional

NEXT_FY = "+1y"        # yfinanceの「来年度」＝次の会計年度
NO_ESTIMATE = "no_estimate"
OK = "ok"


def evaluate(growth_rate: Optional[float], estimates: Optional[dict]) -> dict:
    out: dict = {"status": OK, "growth_gap_pt": None, "tanuki_growth": growth_rate, "analyst_revenue_growth": None,
                 "analyst_count": None, "revisions_up_30d": None, "revisions_down_30d": None, "reason": None}
    est = estimates or {}
    rg = (est.get("revenue_growth") or {}).get(NEXT_FY)
    rev = (est.get("eps_revisions") or {}).get(NEXT_FY) or {}
    if rg is None:
        out["status"] = NO_ESTIMATE
        out["reason"] = "アナリストの売上予想（次の会計年度）が取れていない" if not est else "アナリストの売上予想の伸び（次の会計年度）が空"
    else:
        out["analyst_revenue_growth"] = rg
        out["analyst_count"] = (est.get("revenue_analysts") or {}).get(NEXT_FY)
        if growth_rate is None:
            out["status"] = NO_ESTIMATE
            out["reason"] = "TANUKIの成長率が無い"
        else:
            out["growth_gap_pt"] = round((growth_rate - rg) * 100.0, 1)
    out["revisions_up_30d"] = rev.get("up_30d")
    out["revisions_down_30d"] = rev.get("down_30d")
    if est.get("fetched_at"):
        out["estimates_fetched_at"] = est["fetched_at"]
    return out


def build(ticker: str, growth_rate: Optional[float]) -> dict:
    try:
        from common.market_data.reader import get_attributes
        est = (get_attributes(ticker) or {}).get("estimates")
    except Exception:
        est = None
    return evaluate(growth_rate, est)


def report_line(g: Optional[dict]) -> Optional[str]:
    if not g:
        return None
    if g.get("status") != OK or g.get("growth_gap_pt") is None:
        return f"Guidance_Gap: 空欄（{g.get('reason') or '予想が取れていない'}）"
    up, dn = g.get("revisions_up_30d"), g.get("revisions_down_30d")
    rv = f"／EPS予想の改定（次の会計年度・30日）引き上げ{up}・引き下げ{dn}" if up is not None or dn is not None else "／EPS予想の改定は取れていない"
    n = f"・{g['analyst_count']}人" if g.get("analyst_count") else ""
    return (f"Guidance_Gap: 成長率 {g['tanuki_growth']*100:.1f}% − アナリストの売上予想の伸び（次の会計年度{n}）"
            f"{g['analyst_revenue_growth']*100:.1f}% = {g['growth_gap_pt']:+.1f}pt{rv}")
