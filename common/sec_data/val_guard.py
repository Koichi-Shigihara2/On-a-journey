"""Layer3 の val（その期を最後に載せた申告の値）の守り（[[PARSER-LAYER3-SINGLE-SOURCE-1]] 段1、2026-11-11）。

最新の申告に取得元の誤りや内訳の欠けがあると、val にそのまま入る（COHR 2023年次のD&A 1,000倍、APP 2021・2022年次のcapexでソフト開発費の行が落ちる）。
機械で見分ける規則（銘柄の名前は書かない。根拠と全期での当たりは docs/architecture/PARSER_LAYER3_STAGE1_STEP0.md・PARSER_LAYER3_VAL_GUARD_hits.csv）:
  (i)  単位の誤り: val ÷ first_val が 10 の累乗（|k|≥3。分割の組み替えは 10 倍・100 倍が正しく起きるので 1000 倍以上だけ）で、
       隣の期（同じ種類で前後2期ずつの中央値）に first_val の方が近いとき、first_val を採る。印 val_guard="unit_pow10"、置き換え前の値 val_latest
  (ii) 内訳の行が減った: cf_lines の capex で、最新の申告が前の申告にあった内訳の行（ppe・software）を持たず、その額がどの行にも残っていないとき、
       内訳の揃った申告の値を採る（cf_lines.period_values）。印 val_guard="components_dropped:..."
  (iii) 隣の期から約3.2倍以上外れるのに first_val は約2倍以内: 15件中真の誤りは1件で、ほかは本物の再表示・訂正。値は変えず印 val_suspect だけ
"""
from __future__ import annotations

import math
import statistics
from typing import Any, Dict, List, Optional

# 持ち株数・1株の値は株式分割の組み替えで val が正しく 10 倍・100 倍になる → (iii)の対象外、(i)は |k|≥3 だけ（全項目 |k|≥3）
SPLIT_FIELDS = frozenset({"shares_diluted", "shares_basic_weighted_avg", "shares_outstanding_period_end_sec",
                          "eps_basic", "eps_diluted", "dividends_per_share"})
POW10_MIN_K = 3
POW10_TOL = 0.003           # log10 の比が整数から 0.003 以内（約0.7%）
DEV_VAL = 0.5               # (iii) val が隣の期の中央値から離れる log10
DEV_FIRST = 0.3             # (iii) first_val が離れていてもよい上限
BREADTH_LIMIT = 5           # (iii) 同じ期末で 5 項目以上が first_val から 20% 超動く期は期全体の組み替え（SPAC合併等）として対象外
MARK_WINDOW_YEARS = 3       # 読み手に渡す印は、最新の期末から 3 年以内の期だけ

UNIT_RULE = "unit_pow10"


def _neighbors(series: List[dict], i: int) -> Optional[float]:
    nb = [abs(x["val"]) for x in series[max(0, i - 2):i] + series[i + 1:i + 3] if x.get("val")]
    return statistics.median(nb) if len(nb) >= 2 else None


def _kind_series(entries: List[dict]) -> Dict[bool, List[dict]]:
    es = [e for e in entries if e.get("val") is not None and not e.get("is_ytd")]
    return {ann: sorted([e for e in es if bool(e.get("is_annual")) == ann], key=lambda e: e["end"]) for ann in (True, False)}


def guard_unit_errors(entries: List[dict]) -> List[dict]:
    """(i) 1つの系列（1タグ・マージ前または1項目）の中で、単位の誤りの val を first_val に置き換える（その場で書き換え）。置き換えた entries を返す"""
    done: List[dict] = []
    for series in _kind_series(entries).values():
        for i, e in enumerate(series):
            v, fv = e["val"], e.get("first_val")
            if fv is None or v == fv or not v or not fv:
                continue
            lg = math.log10(abs(v / fv))
            k = round(lg)
            if abs(k) < POW10_MIN_K or abs(lg - k) > POW10_TOL:
                continue
            med = _neighbors(series, i)
            if med is None:
                continue                                    # 隣の期が無ければどちらが誤りか決められない（触らない）
            if abs(math.log10(abs(v) / med)) > abs(math.log10(abs(fv) / med)):
                e["val_latest"] = v
                e["val"] = fv
                e["val_guard"] = UNIT_RULE
                done.append(e)
    return done


def mark_suspect(fields_out: Dict[str, Any]) -> int:
    """(iii) 値は変えず、隣の期から外れる val に val_suspect の印を付ける。付けた数を返す"""
    breadth: Dict[tuple, int] = {}
    for f, fd in fields_out.items():
        if f in SPLIT_FIELDS:
            continue
        for e in fd.get("entries") or []:
            v, fv = e.get("val"), e.get("first_val")
            if v is None or fv is None or e.get("is_ytd") or v == fv:
                continue
            if abs(fv) < 1e-9 or abs(v / fv - 1) > 0.2:
                key = (e["end"], bool(e.get("is_annual")))
                breadth[key] = breadth.get(key, 0) + 1
    n = 0
    for f, fd in fields_out.items():
        if f in SPLIT_FIELDS:
            continue
        for ann, series in _kind_series(fd.get("entries") or []).items():
            for i, e in enumerate(series):
                v, fv = e["val"], e.get("first_val")
                if fv is None or v == fv or not v or not fv or e.get("val_guard"):
                    continue
                if breadth.get((e["end"], ann), 0) >= BREADTH_LIMIT:
                    continue
                med = _neighbors(series, i)
                if med is None:
                    continue
                if abs(math.log10(abs(v) / med)) > DEV_VAL and abs(math.log10(abs(fv) / med)) < DEV_FIRST:
                    e["val_suspect"] = "neighbor_outlier"
                    n += 1
    return n


def marks_from_store(store: Optional[dict]) -> List[Dict[str, Any]]:
    """読み手（TANUKIのreport.txt・stock.html・latest.json）に渡す印の一覧。最新の期末から MARK_WINDOW_YEARS 年以内の期だけ。
    {field, end, kind(年次/四半期), rule, val, other}。val は Layer3 が今使っている値、other は置き換え前（val_latest）または first_val"""
    if not store:
        return []
    latest = store.get("latest_period_end") or ""
    cut = f"{int(latest[:4]) - MARK_WINDOW_YEARS}{latest[4:]}" if latest[:4].isdigit() else ""
    out = []
    for f, fd in (store.get("fields") or {}).items():
        for e in fd.get("entries") or []:
            rule = e.get("val_guard") or ("suspect:" + e["val_suspect"] if e.get("val_suspect") else None)
            if not rule or e["end"] < cut:
                continue
            out.append({"field": f, "end": e["end"], "kind": "年次" if e.get("is_annual") else "四半期", "rule": rule,
                        "val": e.get("val"), "other": e.get("val_latest") if e.get("val_latest") is not None else e.get("first_val")})
    return sorted(out, key=lambda m: (m["end"], m["field"]))


def load_marks(ticker: str) -> List[Dict[str, Any]]:
    """銘柄の Layer3 ストアから印の一覧を作る（TANUKIのdata_fetcherが呼ぶ）"""
    from .layer3_builder import build_ticker_store
    try:
        return marks_from_store(build_ticker_store(ticker))
    except Exception:
        return []


RULE_TEXT = {
    UNIT_RULE: "最新の申告の値が単位の誤り（10の累乗の差）のため、最初の申告の値を使用",
    "components_dropped": "最新の申告で内訳の行が減っているため、内訳の揃った申告の値を使用",
    "suspect:neighbor_outlier": "最新の申告の値が隣の期から大きく外れる（値は変えていない。再表示の可能性もある）",
}


def rule_text(rule: str) -> str:
    return RULE_TEXT.get(rule) or RULE_TEXT.get(rule.split(":")[0]) or rule
