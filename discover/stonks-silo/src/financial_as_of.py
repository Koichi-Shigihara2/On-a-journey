# discover/stonks-silo/src/financial_as_of.py
"""
財務データの基準日（表示用。verdict・score・overallには使わない）
[[STONKS-DATA-ASOF-MISSING-1]]（2026-10-10）

背景: 画面のヒーローの「UPDATED」はresults.jsonのgenerated_at（株価の更新時刻）だけで、
財務が何期末までかが分からなかった。SECの取得が2026-08-30から止まっていた間も
（[[SEC-FETCH-CACHE-MTIME-CI-1]]）、画面からは気づけなかった。

- latest_quarter_end: 最新の四半期末。Layer3ストアからcalc_ttm_series()で計算した最新の
  ttm_end（get_ttm_revenue()と同じ計算。売上が4四半期そろわない銘柄でも取れる）。
  2026-10-10に、get_ttm_revenue()のttm_end・ttm/ファイル・financial_vectorsのseries_qの
  最新endと全24銘柄で一致することを確認
- scoring_fy / scoring_fy_end: スコア（赤字品質・生存能力・黒字化パス）に使った会計年度
  （load_annual_data()のyears[-1]）と、その期末日。期末日は年次ファイルの本人データの
  accn（*_provenance）をsubmissions.jsonのaccn_to_reportdateで引いたもの
- stale: generated_atがlatest_quarter_end＋STALE_DAYS_AFTER_QUARTER_ENDを超えていればtrue
  （次の四半期の10-Qが出ているはずなのに取り込めていない）
取れない値はNone（画面は「不明」）。
"""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Optional

# 四半期91日＋10-Qの提出期限45日（大規模早期提出会社は40日。遅い方に合わせる）
STALE_DAYS_AFTER_QUARTER_END = 135

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SEC_DATA_DIR = _REPO_ROOT / "common" / "sec_data" / "data"


def latest_quarter_end(ticker: str, store: Optional[dict]) -> Optional[str]:
    if store is None:
        return None
    from common.sec_data.ttm_calculator import calc_ttm_series
    try:
        series = calc_ttm_series(ticker, store, n_periods=1)
    except Exception:
        return None
    return (series[0].get("ttm_end") if series else None) or None


def scoring_fy_end(ticker: str, fy: Optional[int], data_dir: Optional[Path] = None) -> Optional[str]:
    """スコアに使った年次ファイル（annual_{fy}.json）の期末日（10-KのreportDate）。"""
    if fy is None:
        return None
    from common.sec_data.fetcher import load_submissions
    base = Path(data_dir) if data_dir else _SEC_DATA_DIR
    path = base / ticker.upper() / f"annual_{fy}.json"
    try:
        with open(path, encoding="utf-8") as f:
            annual = json.load(f)
    except Exception:
        return None
    accns = {
        v.get("accn")
        for sec in ("pl_provenance", "cf_provenance", "bs_provenance")
        for v in (annual.get(sec) or {}).values()
        if isinstance(v, dict) and v.get("is_own_data") and v.get("accn")
    }
    reportdates = load_submissions(ticker, data_dir=str(base))
    ends = sorted({reportdates[a] for a in accns if reportdates.get(a)})
    # 本人データのaccnは年次の10-K（と訂正の10-K/A）なので通常は1つ。複数なら最も新しい期末
    return ends[-1] if ends else None


def is_stale(quarter_end: Optional[str], generated_at: datetime) -> Optional[bool]:
    if not quarter_end:
        return None
    try:
        q = date.fromisoformat(quarter_end[:10])
    except ValueError:
        return None
    return (generated_at.date() - q).days > STALE_DAYS_AFTER_QUARTER_END


def build(ticker: str, years: list, store: Optional[dict], generated_at: datetime,
          data_dir: Optional[Path] = None) -> dict:
    fy = years[-1] if years else None
    lqe = latest_quarter_end(ticker, store)
    return {
        "latest_quarter_end": lqe,
        "scoring_fy": fy,
        "scoring_fy_end": scoring_fy_end(ticker, fy, data_dir),
        "stale": is_stale(lqe, generated_at),
        "stale_threshold_days": STALE_DAYS_AFTER_QUARTER_END,
    }


def restamp(results: dict, generated_at: datetime) -> int:
    """全銘柄のstaleを出力のgenerated_atで判定し直す（銘柄を絞った実行でマージした古い行もそろえる）。
    staleの銘柄数を返す。"""
    n = 0
    for r in results.values():
        fa = r.get("financial_as_of")
        if not isinstance(fa, dict):
            continue
        fa["stale"] = is_stale(fa.get("latest_quarter_end"), generated_at)
        fa["stale_threshold_days"] = STALE_DAYS_AFTER_QUARTER_END
        n += fa["stale"] is True
    return n
