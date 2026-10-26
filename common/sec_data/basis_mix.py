"""
common/sec_data/basis_mix.py
ベースが混在（事業の分離・売却等による組み替え）の判定（[[QUARTERLY-BASIS-MIX-1]] 案1、2026-10-26）

10-Q・10-Kが比較期間（前年の同じ四半期）を新しいベース（継続事業ベース等）に組み替えると、
TTMの4四半期のうち組み替えた申告より前の申告から来る四半期は旧ベースのまま残り、
新旧のベースが混ざる（HONの2026Q3の10-Q: 2025Q3の売上 10,408M→4,929M、TTMは
旧ベースの3四半期と新ベースのQ3の合計）。この判定を1か所に置き、TTM・normalizedの印、
TANUKI VALUATION（DCFの信頼性LOW→WATCH・fundaの部品）、HypeCore（rev_yoy・Rule40）、
rccのCHECK-66が同じ結果を使う。

規則:
  ①組み替え: 売上・営業利益・CapExの値（四半期の単独・年次。Q4の逆算を除く）のうち、最初の申告の値
    （first_val）と今の値の差が first_val の絶対値の MIN_RATIO 以上のもの。今の値の申告（filed）が
    組み替えた申告
  ②きっかけ: ①のうち、四半期で、組み替えた申告がその期の約1年後（COMPARATIVE_GAP_DAYS）の四半期も
    申告している（＝前年同期として組み替えた）もの
  ③組み替えの日: きっかけの申告から、同じ項目の①の申告を CHAIN_GAP_DAYS 以内でさかのぼった最初の申告
    の提出日（同じ事業の変化で毎回の申告が前年同期を組み替えている間〈APPの2025-08-06〜〉は1つにまとめる）。
    きっかけの申告で売上も組み替えていれば売上で、そうでなければその項目でさかのぼる
  ④混在: 最新の期末で終わる4四半期（どれかの項目で）に、組み替えの日以降の申告から来る四半期と、
    それより前の申告から来る四半期が両方ある。残りの四半期が組み替えの日以降の申告にそろえば
    （最長3四半期後）印は外れる
  判定の正はLayer3（TTMはLayer3から作る）。normalizedは別の正規化で、タグの選び方による見かけの組み替え
  （RCATの2025Q2の売上）があるため、判定には使わない
"""
from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Optional

MIN_RATIO = 0.20
COMPARATIVE_GAP_DAYS = (330, 400)
CHAIN_GAP_DAYS = 220
NOTE = "ベースが混在（事業の分離・売却等による組み替え）"

# 判定に使う項目（normalizedの名前 → Layer3の名前）
FIELDS = {
    "Revenue": "revenue",
    "OperatingIncome": "operating_income",
    "CapEx": "capital_expenditure",
}

TTM_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ttm")


def _quarters(entries: list) -> list:
    """単独四半期のエントリ（年次・YTDを除く）を期末の昇順で。"""
    qs = [e for e in entries or [] if not e.get("is_annual") and not e.get("is_ytd")
          and e.get("val") is not None and e.get("end")]
    return sorted(qs, key=lambda e: e["end"])


def _days(a: str, b: str) -> int:
    return (date.fromisoformat(b) - date.fromisoformat(a)).days


def _restatements(entries: list) -> list:
    """①の組み替え。[(エントリ, 組み替えた申告の提出日, 前年同期のきっかけか)]"""
    flows = [e for e in entries or [] if not e.get("is_ytd") and e.get("val") is not None and e.get("end")]
    filed_q_ends: dict[str, set] = {}
    for e in flows:
        if not e.get("is_annual"):
            filed_q_ends.setdefault(e.get("filed", ""), set()).add(e["end"])
    lo, hi = COMPARATIVE_GAP_DAYS
    out = []
    for e in flows:
        fv, ff, f, v = e.get("first_val"), e.get("first_filed"), e.get("filed"), e["val"]
        if e.get("is_implied") or not fv or not ff or not f or ff >= f:
            continue
        if abs(v - fv) < MIN_RATIO * abs(fv):
            continue
        trigger = (not e.get("is_annual")
                   and any(lo <= _days(e["end"], end2) <= hi for end2 in filed_q_ends.get(f, ())))
        out.append((e, f, trigger))
    return out


def _chain_start(filings: list, filed: str) -> str:
    """③: filed から、組み替えた申告の並び（昇順）を CHAIN_GAP_DAYS 以内でさかのぼった最初の提出日。"""
    start = filed
    for f in sorted((x for x in set(filings) if x < filed), reverse=True):
        if _days(f, start) > CHAIN_GAP_DAYS:
            break
        start = f
    return start


def _window(qs: list, end: str) -> list:
    """end で終わる4四半期（ttm_calculatorと同じ「end以前の最新の4つ」）。"""
    return [e for e in qs if e["end"] <= end][-4:]


def _mixed(window: list, recast_filed: str) -> bool:
    if len(window) < 2:
        return False
    new = any(e.get("filed", "") >= recast_filed for e in window)
    old = any(e.get("filed", "") < recast_filed for e in window)
    return new and old


def detect(entries_by_field: dict[str, list]) -> Optional[dict]:
    """項目ごとのエントリ（normalizedかLayer3の形。年次を含んでよい）から判定する。混在していなければ None。

    戻り値: {"mixed": True, "recast_filed"（組み替えの日）, "trigger_filed"（きっかけの申告）,
             "trigger_form", "restated": [...], "latest_end", "old_basis_quarters": {項目: [期末]},
             "mixed_ttm_ends": [期末], "note"}"""
    qs_by = {f: _quarters(es) for f, es in entries_by_field.items()}
    rest_by = {f: _restatements(es) for f, es in entries_by_field.items()}
    triggers: dict[str, list] = {}
    forms: dict[str, str] = {}
    for f, rows in rest_by.items():
        for e, filed, trigger in rows:
            if not trigger:
                continue
            triggers.setdefault(filed, []).append({
                "field": f, "end": e["end"], "first_val": e["first_val"], "val": e["val"],
                "first_filed": e.get("first_filed"),
                "change_pct": round((e["val"] - e["first_val"]) / abs(e["first_val"]) * 100, 1),
            })
            forms[filed] = e.get("form", "")
    latest_end = max((qs[-1]["end"] for qs in qs_by.values() if qs), default=None)
    if not triggers or latest_end is None:
        return None
    # 最新の期末で混在しているきっかけのうち、いちばん新しいもの
    for trigger_filed in sorted(triggers, reverse=True):
        fields_here = {r["field"] for r in triggers[trigger_filed]}
        anchor = "Revenue" if "Revenue" in fields_here else next(f for f in entries_by_field if f in fields_here)
        recast_filed = _chain_start([filed for _e, filed, _t in rest_by.get(anchor, [])], trigger_filed)
        old_basis = {}
        for f, qs in qs_by.items():
            w = _window(qs, latest_end)
            if _mixed(w, recast_filed):
                old_basis[f] = [e["end"] for e in w if e.get("filed", "") < recast_filed]
        if not old_basis:
            continue
        form = forms.get(trigger_filed, "")
        return {
            "mixed": True,
            "recast_filed": recast_filed,
            "trigger_filed": trigger_filed,
            "trigger_form": form,
            "restated": sorted(triggers[trigger_filed], key=lambda r: (r["field"], r["end"])),
            "latest_end": latest_end,
            "old_basis_quarters": old_basis,
            "mixed_ttm_ends": mixed_ttm_ends(qs_by, recast_filed),
            "note": f"{NOTE}: {form}（{trigger_filed}提出）が前年同期を組み替え、"
                    f"直近の4四半期に組み替えの前の申告の四半期が残る",
        }
    return None


def mixed_ttm_ends(qs_by: dict[str, list], recast_filed: str) -> list:
    """組み替えの日 recast_filed（qs_by は _quarters() 済み）について、4四半期が混在する期末（どれかの項目で）。昇順。"""
    ends = set()
    for qs in qs_by.values():
        for e in qs:
            if _mixed(_window(qs, e["end"]), recast_filed):
                ends.add(e["end"])
    return sorted(ends)


def from_store(store: Optional[dict]) -> Optional[dict]:
    """Layer3（layer3_builder.build_ticker_store() の戻り値）から判定する。判定の正。"""
    if not store:
        return None
    from .layer3_builder import get_field_entries
    return detect({f: get_field_entries(store, l3) for f, l3 in FIELDS.items()})


def load(ticker: str, ttm_dir: Optional[str] = None) -> Optional[dict]:
    """読み手（TANUKI VALUATION・rcc）が使う。SEC_Data_Updateが ttm/{T}_ttm_series.json に保存した
    "basis_mix" を読む。保存が無い（作り直す前の）ファイルは、Layer3をその場で作って同じ規則で判定する
    （事例39: Layer3をその場で作る読み手は日曜を待たずに出る）。"""
    path = os.path.join(ttm_dir or TTM_DIR, f"{ticker.upper()}_ttm_series.json")
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f) or {}
        if "basis_mix" in data:
            return data["basis_mix"]
    except (OSError, ValueError):
        pass
    from .layer3_builder import build_ticker_store
    return from_store(build_ticker_store(ticker))


def is_mixed(info: Any) -> bool:
    return bool(isinstance(info, dict) and info.get("mixed"))
