"""
common/sec_data/share_unit_fix.py

株式数の単位誤り（千株・百万株単位での申告）の解消（[[LAYER2-SHARES-UNIT-THOUSANDS-1]]、2026-10-10）。

同じ概念・同じ期間（start・end）について、複数の提出書類が比 1,000 または 1,000,000（±1%）の
2つの値を申告していたら、その銘柄・その概念の全期間の値の中央値に（対数で）近い方を正しい値とみなし、
もう一方のファクトの値を置き換える。ほとんどは大きい方が正しい（千株単位で申告した側が小さい）が、
MSCI 2014-12-31の発行済株式数は1つの10-Q（2015-05-01提出）だけが1,000倍の112,072,469,000を
申告しており、単純に大きい方を採ると誤るため、中央値で判定する。
提出者がHTML上の「千株単位」の表をXBRLで株数としてタグ付けした誤りで、同じ期の別の書類が
正しい株数を申告している場合に限る（推測による補正はしない）。

  - 後の書類の比較期間（再掲）が誤り: CIX 2025-06-30（元の10-Q 12,321,000／2026年の10-Q 12,321）
  - 元の書類が誤り・翌年の書類で正しい: TER 2023-10-01（元の10-Q 164,050／翌年の10-Q 164,050,000）

値だけを置き換え、ファクトのaccn・filed・form等は残す。parser.py（本人のその期の書類を優先）と
layer3_builder.py（最新の提出を優先）のどちらの選び方でも、同じ期間は同じ値になる。
置き換えたファクトには"share_unit_fix"（退けた値・採った値の書類）を付け、一覧を返す。

対象は株式数のタグだけ（金額には適用しない）。比がちょうど1,000倍・100万倍でない2つの値
（株式分割の前後など）には適用しない。書類が1つしかない期（CIX 2026-06-30・ONDS 2025年度・LOAR）は
直らないため、fact_overrides.jsonの銘柄ごとの"share_unit"に根拠つきで登録し、同じ前処理の中で
個別に補正する（apply_share_unit_overrides()）。fact_overrides.jsonの年度単位の上書き
（parser.py::_apply_fact_overrides()、年次のLayer2だけに効く）と違い、四半期とLayer3にも効く。
年度単位の上書きは年度のキーを整数として読むため、"share_unit"のキーは読み飛ばされる。
"""
import json
import math
import os
import statistics
from typing import Any, Dict, List, Optional, Tuple

FACT_OVERRIDES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fact_overrides.json")

SHARE_TAGS: Tuple[str, ...] = (
    "WeightedAverageNumberOfDilutedSharesOutstanding",
    "WeightedAverageNumberOfSharesOutstandingBasic",
    "CommonStockSharesOutstanding",
)
UNIT_RATIOS: Tuple[float, ...] = (1_000.0, 1_000_000.0)
RATIO_TOLERANCE = 0.01


def _unit_ratio(a: float, b: float) -> float | None:
    """aとbの比（大÷小）が1,000倍・100万倍（±1%）ならその倍率、違えばNone"""
    big, small = max(a, b), min(a, b)
    if small <= 0 or big == small:
        return None
    r = big / small
    for u in UNIT_RATIOS:
        if abs(r / u - 1) <= RATIO_TOLERANCE:
            return u
    return None


def resolve_share_unit_conflicts(us_gaap: dict) -> Tuple[dict, List[Dict[str, Any]]]:
    """us-gaapの写しと、置き換えの一覧を返す（元のdictは変更しない）。

    一覧の各要素: {tag, start, end, rejected_val, rejected_accn, adopted_val, adopted_accn, ratio}
    """
    log: List[Dict[str, Any]] = []
    if not isinstance(us_gaap, dict):
        return us_gaap, log
    out = dict(us_gaap)
    for tag in SHARE_TAGS:
        node = us_gaap.get(tag)
        if not node:
            continue
        units = node.get("units") or {}
        new_units = {}
        changed = False
        for unit, facts in units.items():
            vals = [float(f["val"]) for f in facts if f.get("val") and f["val"] > 0]
            if not vals:
                new_units[unit] = facts
                continue
            med = statistics.median(vals)

            def _dist(v: float) -> float:
                return abs(math.log(v / med)) if v > 0 else float("inf")

            by_period: Dict[tuple, List[int]] = {}
            for i, f in enumerate(facts):
                if f.get("val") is None or f.get("end") is None:
                    continue
                by_period.setdefault((f.get("start"), f["end"]), []).append(i)
            new_facts = list(facts)
            for (start, end), idxs in by_period.items():
                if len(idxs) < 2:
                    continue
                for i in idxs:
                    cur = facts[i]
                    # 比がちょうど1,000倍・100万倍の相手（同じ期間の別の書類）
                    partners = [facts[j] for j in idxs
                                if _unit_ratio(float(facts[j]["val"]), float(cur["val"])) is not None]
                    if not partners:
                        continue
                    # 中央値に近い方の値が正しい。自分の値の方が近ければ置き換えない
                    best = min(partners, key=lambda f: _dist(float(f["val"])))
                    if _dist(float(best["val"])) >= _dist(float(cur["val"])):
                        continue
                    same = [f for f in partners if f["val"] == best["val"]]
                    adopted = max(same, key=lambda f: (f.get("filed") or "", f.get("accn") or ""))
                    ratio = _unit_ratio(float(adopted["val"]), float(cur["val"]))
                    new_facts[i] = {**cur, "val": adopted["val"], "share_unit_fix": {
                        "rejected_val": cur["val"], "adopted_accn": adopted.get("accn"), "ratio": ratio}}
                    log.append({"tag": tag, "start": start, "end": end,
                                "rejected_val": cur["val"], "rejected_accn": cur.get("accn"),
                                "rejected_filed": cur.get("filed"), "rejected_form": cur.get("form"),
                                "adopted_val": adopted["val"], "adopted_accn": adopted.get("accn"),
                                "adopted_filed": adopted.get("filed"), "ratio": ratio})
                    changed = True
            new_units[unit] = new_facts
        if changed:
            out[tag] = {**node, "units": new_units}
    log.sort(key=lambda x: (x["tag"], x["end"], x["start"] or "", x["rejected_accn"] or ""))
    return out, log


def load_share_unit_overrides(ticker: str, path: Optional[str] = None) -> List[Dict[str, Any]]:
    """fact_overrides.jsonの{ticker: {"share_unit": [...]}}を返す（無ければ空リスト）"""
    p = path or FACT_OVERRIDES_PATH
    if not ticker or not os.path.exists(p):
        return []
    try:
        with open(p, encoding="utf-8") as f:
            return ((json.load(f).get(ticker.upper()) or {}).get("share_unit")) or []
    except Exception:
        return []


def _rule_matches(rule: Dict[str, Any], tag: str, f: Dict[str, Any]) -> bool:
    if tag not in rule.get("tags", []):
        return False
    if rule.get("accn") and f.get("accn") != rule["accn"]:
        return False
    if rule.get("end") and f.get("end") != rule["end"]:
        return False
    v = f.get("val")
    if v is None:
        return False
    if "match_vals" in rule:
        return v in rule["match_vals"]
    return rule.get("min_val", 0) <= v < rule.get("max_val", float("inf"))


def apply_share_unit_overrides(us_gaap: dict, rules: List[Dict[str, Any]]) -> Tuple[dict, List[Dict[str, Any]]]:
    """fact_overrides.jsonの"share_unit"の規則で、株式数のファクトの値をmultiply倍にする。

    規則: {"tags": [...], "accn"（省略で全書類）, "end"（省略で全期）, "match_vals"（誤った値の一覧）
    または "min_val"/"max_val"（値の範囲）, "multiply", "reason", "evidence"}
    """
    log: List[Dict[str, Any]] = []
    if not rules or not isinstance(us_gaap, dict):
        return us_gaap, log
    out = dict(us_gaap)
    for tag in SHARE_TAGS:
        node = us_gaap.get(tag)
        if not node:
            continue
        new_units, changed = {}, False
        for unit, facts in (node.get("units") or {}).items():
            new_facts = []
            for f in facts:
                rule = next((r for r in rules if _rule_matches(r, tag, f)), None)
                if rule is None:
                    new_facts.append(f)
                    continue
                val = f["val"] * rule["multiply"]
                new_facts.append({**f, "val": val, "share_unit_fix": {
                    "rejected_val": f["val"], "source": "fact_overrides", "multiply": rule["multiply"]}})
                log.append({"tag": tag, "start": f.get("start"), "end": f["end"], "source": "fact_overrides",
                            "rejected_val": f["val"], "rejected_accn": f.get("accn"), "rejected_filed": f.get("filed"),
                            "rejected_form": f.get("form"), "adopted_val": val, "multiply": rule["multiply"],
                            "reason": rule.get("reason")})
                changed = True
            new_units[unit] = new_facts
        if changed:
            out[tag] = {**node, "units": new_units}
    log.sort(key=lambda x: (x["tag"], x["end"], x["start"] or "", x["rejected_accn"] or ""))
    return out, log


def with_share_unit_fix(company_facts: dict, ticker: Optional[str] = None,
                        overrides: Optional[List[Dict[str, Any]]] = None) -> Tuple[dict, List[Dict[str, Any]]]:
    """company_factsの写し（us-gaapの株式数を直したもの）と置き換えの一覧を返す。

    ①fact_overrides.jsonの個別補正（tickerを渡したとき、またはoverridesで直接）
    ②同じ期間の1,000倍・100万倍の2値の解消（resolve_share_unit_conflicts）の順に適用する。
    """
    if not isinstance(company_facts, dict):
        return company_facts, []
    facts = company_facts.get("facts") or {}
    rules = overrides if overrides is not None else load_share_unit_overrides(
        ticker or "")
    us_gaap, log_ov = apply_share_unit_overrides(facts.get("us-gaap") or {}, rules)
    us_gaap, log = resolve_share_unit_conflicts(us_gaap)
    log = log_ov + log
    if not log:
        return company_facts, log
    return {**company_facts, "facts": {**facts, "us-gaap": us_gaap}}, log
