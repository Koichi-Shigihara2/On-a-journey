"""
common/sec_data/conditional_tags.py
条件付きの候補タグ（[[LAYER3-LATEST-FILING-TAG-DROP-1]]、2026-10-14新設）

セグメント開示の新基準（ASU 2023-07）への対応で、設備投資の全体額を従来のタグ
（PaymentsToAcquireProductiveAssets等）ではなく、注記の
SegmentExpenditureAdditionToLongLivedAssets（セグメントの軸なしの全体値）だけで
申告する会社が出た（FCXの2026年第2四半期の10-Q。キャッシュフロー計算書の設備投資は
事業区分の軸つきの値だけで、合計の行が無い）。

ただし同じタグを申告する26銘柄のうち、従来の設備投資と値が一致するのは16銘柄で、
AMZN・CAT・XOM・HON・TER等では意味の違う値（会計上の追加額など）になる。
そこで、次の両方を満たす銘柄だけ、候補の最後（既存の候補に値がある期は上書きしない）に使う:
  (a) セグメントの軸なしの値であること
      company_facts（SECのcompanyfacts API）は軸なしの値だけを載せるため、
      company_factsから読む限り常に満たす（軸つきの値は取り込まれない）
  (b) 過去の重なる期間（startとendが一致する期間）で、既存の候補タグの値と
      ±0.5%で2期間以上一致し、不一致が0

判定はこのモジュールの conditional_candidate() 1つにまとめ、Layer3
（layer3_builder.py）とparser.py（年次・四半期）の両方から呼ぶ。
採用した期のエントリには fallback_reason（"segment_total"）を付ける。
"""

from __future__ import annotations

SEGMENT_TOTAL_CAPEX_TAG = "SegmentExpenditureAdditionToLongLivedAssets"

# 項目 → (条件付きの候補タグ, fallback_reason)
CONDITIONAL_CANDIDATES: dict[str, tuple[str, str]] = {
    "capital_expenditure": (SEGMENT_TOTAL_CAPEX_TAG, "segment_total"),
}

MATCH_TOLERANCE = 0.005
MIN_MATCHED_PERIODS = 2
_FORMS = ("10-Q", "10-K", "10-Q/A", "10-K/A")


def _facts_by_period(us_gaap: dict, tag: str, unit: str) -> dict[tuple[str, str], float]:
    out: dict[tuple[str, str], tuple[float, str]] = {}
    for e in ((us_gaap.get(tag) or {}).get("units") or {}).get(unit) or []:
        if e.get("form") not in _FORMS or e.get("val") is None:
            continue
        k = (e.get("start") or "", e["end"])
        if k not in out or (e.get("filed") or "") > out[k][1]:
            out[k] = (e["val"], e.get("filed") or "")
    return {k: v[0] for k, v in out.items()}


def match_counts(us_gaap: dict, base_tags: list[str], new_tag: str, unit: str = "USD") -> tuple[int, int]:
    """new_tagと既存の候補タグ（base_tags）の、同じ期間の値の一致・不一致の数（絶対値で比べる）。"""
    new = _facts_by_period(us_gaap, new_tag, unit)
    base: dict[tuple[str, str], float] = {}
    for tag in base_tags:
        for k, v in _facts_by_period(us_gaap, tag, unit).items():
            base.setdefault(k, v)
    matched = mismatched = 0
    for k, v in new.items():
        b = base.get(k)
        if not b:
            continue
        if abs(abs(v) - abs(b)) <= abs(b) * MATCH_TOLERANCE:
            matched += 1
        else:
            mismatched += 1
    return matched, mismatched


def conditional_candidate(field_name: str, company_facts: dict | None, base_tags: list[str],
                          unit: str = "USD") -> tuple[str, str] | None:
    """field_nameに条件付きの候補タグがあり、この銘柄で採用条件を満たすなら (タグ, fallback_reason)。"""
    spec = CONDITIONAL_CANDIDATES.get(field_name)
    if not spec or not company_facts:
        return None
    tag, reason = spec
    if tag in base_tags:
        return None
    us_gaap = (company_facts.get("facts") or {}).get("us-gaap") or {}
    if tag not in us_gaap:
        return None
    matched, mismatched = match_counts(us_gaap, base_tags, tag, unit)
    if matched >= MIN_MATCHED_PERIODS and mismatched == 0:
        return tag, reason
    return None
