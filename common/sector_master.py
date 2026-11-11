"""業種のマスター（config/sector_master.json）を作る。[[HYPECORE-STAGE-TIMESCALE-1]] 段B-1（2026-11-05）

決まり（チャット側の決定 2026-11-05）:
  1. 銘柄の業種は yfinance の sector・industry（common/market_data/attributes/{T}.json、Market Data Weekly が取る）を主にし、
     SECの SIC・社名（common/sec_data/data/{T}/submissions.json、SEC_Data_Update が保存）は控えとして持つ
  2. 銘柄の一覧は書かない。区分（景気循環株・金融・区分なし）と他の分類への写像（EPS ANALYZERの9区分・Damodaranの業種・
     Market PulseのETF）は、業種の名前の規則として本モジュールの1か所に持ち、マスターにも写しを書く
  3. 景気循環株は、TANUKI SCOREの画面にあった語の一覧と定義v2の段Aの4業種の和集合を、業種の名前の規則として持つ
     （画面は 2026-11-06 から語の一覧を持たず、docs/common/sector_class.json の cyclical を読む。[[SECTOR-MASTER-ADOPT-1]] 段5）
  4. 今はマスターを作るだけ。読み手（TANUKI・EPS ANALYZER・STONKS・Market Pulse・画面）は今のまま（[[SECTOR-MASTER-ADOPT-1]] で寄せる）

作り直し: Market Data Weekly（属性の週次の取得）の後に全銘柄（--all）。新しい銘柄の登録（register_ticker の Step 1.6）は
1銘柄だけ（--ticker T）。yfinance の sector・industry が前回から変わった銘柄は changes に残し、System Health の [P] ExternalChange に出す
（common/external_watch.py、台帳 config/external_change_acknowledged.json の kind "sector_change"）。

使い方:
  python common/sector_master.py --all          # 登録簿（retired 以外）の全銘柄
  python common/sector_master.py --ticker NVDA  # 1銘柄だけ作り直して足す（ほかの銘柄はそのまま）
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

MASTER_PATH = os.path.join(_REPO, "config", "sector_master.json")
# 画面が読む区分（公開は docs/ の下だけなので、マスターから区分だけを同じ書き手が書く。手で書かない）。[[SECTOR-MASTER-ADOPT-1]] 段5（2026-11-06）
PUBLIC_PATH = os.path.join(_REPO, "docs", "common", "sector_class.json")
ATTR_DIR = os.path.join(_REPO, "common", "market_data", "attributes")
SEC_DIR = os.path.join(_REPO, "common", "sec_data", "data")
SCHEMA_VERSION = "1.0"

# ── 業種の名前の規則（1か所） ──────────────────────────────────────────────
# 景気循環株: industry にこの語を含む（もとは TANUKI SCOREの画面の語の一覧。画面は 2026-11-06 からこの区分を読む）
CYCLICAL_KEYWORDS_SCREEN = [
    "Steel", "Aluminum", "Copper", "Gold", "Silver", "Metals", "Mining",
    "Oil", "Gas", "Coal", "Chemicals", "Agricultural",
    "Auto", "Automobile", "Airlines", "Shipping", "Marine",
    "Semiconductor Equipment", "Paper", "Lumber", "Timber",
]
# 定義v2の段Aの4業種（そのうち画面の語に当たらないものだけが和集合を広げる: Farm & Heavy Construction Machinery）
CYCLICAL_INDUSTRIES_STAGE_A = [
    "Semiconductor Equipment & Materials", "Copper", "Oil & Gas Integrated", "Farm & Heavy Construction Machinery",
]

# 区分なし（定義v2 §6: 当てはまる収益の段階・業種の区分が無い。stageは判定不能〈区分なし〉）: industry がこの語で始まる・一致する
UNCLASSIFIED_INDUSTRY_PREFIXES = ["Banks", "Insurance", "REIT"]
UNCLASSIFIED_INDUSTRIES = ["Biotechnology"]

# 金融・資産の重い業種（定義v2 §2: P/TBV÷ROTCE）: sector が Financial Services で、SIC があれば 6000〜6799（金融・保険・不動産）。
# SIC が無い間（SEC_Data_Update の保存の前）は sector だけで決め、basis に "sector_only" と書く
FINANCIAL_SECTORS = ["Financial Services"]
FINANCIAL_EXCLUDED_INDUSTRIES = ["Financial Data & Stock Exchanges"]   # データ・取引所は資産の重い業種ではない
FINANCIAL_SIC_RANGE = (6000, 6799)

# EPS ANALYZER の9区分（config/sectors.yaml の name）への写像: industry が先、無ければ sector
EPS_CATEGORY_NOT_FINANCIAL = "Technology"   # 金融でない Financial Services（決済・指数提供。V・MSCI）
EPS_CATEGORY_BY_INDUSTRY = {
    "Aerospace & Defense": "宇宙・航空",
    "Airports & Air Services": "宇宙・航空",
    "Communication Equipment": "通信・インフラ",
    "Telecom Services": "通信・インフラ",
}
EPS_CATEGORY_BY_SECTOR = {
    "Technology": "Technology",
    "Communication Services": "Technology",
    "Financial Services": "フィンテック / 銀行",
    "Healthcare": "ヘルスケア / バイオ",
    "Consumer Cyclical": "一般消費財 / 製造 / EV",
    "Consumer Defensive": "一般消費財 / 製造 / EV",
    "Industrials": "一般消費財 / 製造 / EV",
    "Basic Materials": "一般消費財 / 製造 / EV",
    "Energy": "一般消費財 / 製造 / EV",
    "Utilities": "通信・インフラ",
    "Real Estate": "不動産 (REITs)",
}

# Damodaran の業種（src/value/tanuki_valuation/growth_sanity.py の indname.xls の業種名）への写像: industry ごと
DAMODARAN_BY_INDUSTRY = {
    "Advertising Agencies": "Advertising",
    "Aerospace & Defense": "Aerospace/Defense",
    "Airports & Air Services": "Air Transport",
    "Auto Manufacturers": "Auto & Truck",
    "Beverages - Non-Alcoholic": "Beverage (Soft)",
    "Biotechnology": "Drugs (Biotechnology)",
    "Communication Equipment": "Telecom. Equipment",
    "Computer Hardware": "Computers/Peripherals",
    "Conglomerates": "Diversified",
    "Consumer Electronics": "Computers/Peripherals",
    "Copper": "Metals & Mining",
    "Credit Services": "Financial Svcs. (Non-bank & Insurance)",
    "Discount Stores": "Retail (General)",
    "Drug Manufacturers - General": "Drugs (Pharmaceutical)",
    "Electrical Equipment & Parts": "Electrical Equipment",
    "Electronic Components": "Electronics (General)",
    "Farm & Heavy Construction Machinery": "Machinery",
    "Financial Data & Stock Exchanges": "Information Services",
    "Health Information Services": "Heathcare Information and Technology",
    "Household & Personal Products": "Household Products",
    "Information Technology Services": "Computer Services",
    "Internet Content & Information": "Software (Internet)",
    "Internet Retail": "Retail (General)",
    "Oil & Gas Integrated": "Oil/Gas (Integrated)",
    "Real Estate Services": "Real Estate (Operations & Services)",
    "Restaurants": "Restaurant/Dining",
    "Scientific & Technical Instruments": "Electronics (General)",
    "Security & Protection Services": "Business & Consumer Services",
    "Semiconductor Equipment & Materials": "Semiconductor Equip",
    "Semiconductors": "Semiconductor",
    "Software - Application": "Software (System & Application)",
    "Software - Infrastructure": "Software (System & Application)",
    "Specialty Business Services": "Business & Consumer Services",
    "Specialty Industrial Machinery": "Machinery",
    "Telecom Services": "Telecom. Services",
    "Tobacco": "Tobacco",
    "Travel Services": "Hotel/Gaming",
    "Utilities - Independent Power Producers": "Power",
}

# メガテック（Phase2の成長率の上限・αの上限を緩める銘柄）: 時価総額が1兆ドル以上、かつ yfinance の industry が
# ソフトウェア（Software - *）・インターネット（Internet *）・半導体（Semiconductors）・家電（Consumer Electronics）。
# [[SECTOR-MASTER-ADOPT-1]] 段2（2026-11-06）。以前は config/maturity_config.json の `_mega_tech_tickers`（手書きの6銘柄 MSFT・AMZN・GOOGL・AAPL・META・NVDA）。
# 規則にしても今の登録銘柄では同じ6銘柄になる（LLY〈医薬〉・TSLA〈自動車〉は業種で外れる）。時価総額は週次の属性の取得で更新される
MEGA_TECH_MIN_MARKET_CAP = 1.0e12
MEGA_TECH_INDUSTRY_PREFIXES = ("Software - ", "Internet ")
MEGA_TECH_INDUSTRIES = ("Semiconductors", "Consumer Electronics")

# Market Pulse のセクターのETF（src/market/market_pulse/sector_rotation.py の SECTOR_MAP と同じ。テストで一致を確かめる）
ETF_BY_SECTOR = {
    "Technology": "XLK", "Communication Services": "XLC", "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP", "Financial Services": "XLF", "Healthcare": "XLV",
    "Industrials": "XLI", "Energy": "XLE", "Basic Materials": "XLB",
    "Real Estate": "XLRE", "Utilities": "XLU",
}


def rules() -> Dict[str, Any]:
    """マスターに書く規則の写し（読み手が規則の版を確かめられるように）"""
    return {
        "cyclical_keywords": sorted(set(CYCLICAL_KEYWORDS_SCREEN)),
        "cyclical_industries_stage_a": CYCLICAL_INDUSTRIES_STAGE_A,
        "unclassified_industry_prefixes": UNCLASSIFIED_INDUSTRY_PREFIXES,
        "unclassified_industries": UNCLASSIFIED_INDUSTRIES,
        "financial_sectors": FINANCIAL_SECTORS,
        "financial_excluded_industries": FINANCIAL_EXCLUDED_INDUSTRIES,
        "financial_sic_range": list(FINANCIAL_SIC_RANGE),
        "eps_category_by_industry": EPS_CATEGORY_BY_INDUSTRY,
        "eps_category_by_sector": EPS_CATEGORY_BY_SECTOR,
        "damodaran_by_industry": DAMODARAN_BY_INDUSTRY,
        "etf_by_sector": ETF_BY_SECTOR,
        "mega_tech_min_market_cap": MEGA_TECH_MIN_MARKET_CAP,
        "mega_tech_industry_prefixes": list(MEGA_TECH_INDUSTRY_PREFIXES),
        "mega_tech_industries": list(MEGA_TECH_INDUSTRIES),
    }


# ── 区分の判定 ──────────────────────────────────────────────────────────
def is_cyclical(industry: Optional[str]) -> bool:
    if not industry:
        return False
    return industry in CYCLICAL_INDUSTRIES_STAGE_A or any(k in industry for k in CYCLICAL_KEYWORDS_SCREEN)


def is_mega_tech(industry: Optional[str], market_cap: Optional[float]) -> bool:
    """時価総額1兆ドル以上、かつ業種がソフトウェア・インターネット・半導体・家電"""
    if not industry or not isinstance(market_cap, (int, float)) or market_cap < MEGA_TECH_MIN_MARKET_CAP:
        return False
    return industry in MEGA_TECH_INDUSTRIES or any(industry.startswith(p) for p in MEGA_TECH_INDUSTRY_PREFIXES)


def unclassified_reason(sector: Optional[str], industry: Optional[str]) -> Optional[str]:
    """区分なし（判定不能）の理由。区分があれば None"""
    if not sector or not industry:
        return "業種なし（yfinanceの属性が無い・空）"
    if industry in UNCLASSIFIED_INDUSTRIES or any(industry.startswith(p) for p in UNCLASSIFIED_INDUSTRY_PREFIXES):
        return f"区分なし（{industry}: 定義v2 §6 の銀行・保険・REIT・バイオ）"
    return None


def financial(sector: Optional[str], industry: Optional[str], sic: Optional[int]) -> Dict[str, Any]:
    """金融・資産の重い業種か。{"value": bool, "basis": "sector+sic"|"sector_only"}"""
    in_sector = bool(sector in FINANCIAL_SECTORS and industry not in FINANCIAL_EXCLUDED_INDUSTRIES)
    if sic is None:
        return {"value": in_sector, "basis": "sector_only"}
    lo, hi = FINANCIAL_SIC_RANGE
    return {"value": in_sector and lo <= sic <= hi, "basis": "sector+sic"}


def eps_category(sector: Optional[str], industry: Optional[str], financial_value: Optional[bool] = None) -> Optional[str]:
    """EPS ANALYZERの区分。金融（financial）に従う: sector が Financial Services でも financial が false（SIC 7389のV・MSCI、段3）なら
    Technology（[[SECTOR-MASTER-ADOPT-1]] 段4）。financial_value を渡さない呼び出しは今までどおり industry・sector だけ。"""
    if financial_value is False and sector in FINANCIAL_SECTORS:
        return EPS_CATEGORY_NOT_FINANCIAL
    if industry and industry in EPS_CATEGORY_BY_INDUSTRY:
        return EPS_CATEGORY_BY_INDUSTRY[industry]
    return EPS_CATEGORY_BY_SECTOR.get(sector or "")


# ── 材料の読み込み ──────────────────────────────────────────────────────
def _load_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _sic_of(submissions: Optional[Dict[str, Any]]) -> Optional[int]:
    v = (submissions or {}).get("sic")
    try:
        return int(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def build_entry(ticker: str, attr_dir: str = ATTR_DIR, sec_dir: str = SEC_DIR) -> Dict[str, Any]:
    t = ticker.upper()
    attrs = _load_json(os.path.join(attr_dir, f"{t}.json")) or {}
    subs = _load_json(os.path.join(sec_dir, t, "submissions.json")) or {}
    sector, industry = attrs.get("sector") or None, attrs.get("industry") or None
    sic = _sic_of(subs)
    fin = financial(sector, industry, sic)
    return {
        "sector": sector,
        "industry": industry,
        "attributes_fetched_at": attrs.get("fetched_at"),
        "sic": sic,
        "sic_description": subs.get("sic_description") or None,
        "entity_name": subs.get("entity_name") or None,
        "cyclical": is_cyclical(industry),
        "mega_tech": is_mega_tech(industry, attrs.get("market_cap")),
        "financial": fin,
        "unclassified": unclassified_reason(sector, industry),
        "eps_category": eps_category(sector, industry, fin["value"]),
        "damodaran_industry": DAMODARAN_BY_INDUSTRY.get(industry or ""),
        "etf": ETF_BY_SECTOR.get(sector or ""),
    }


def registered_tickers(lookup_path: Optional[str] = None) -> List[str]:
    from common.sec_data.tickers import get_all_rows   # 登録簿の読み方の1か所
    return sorted({r["ticker"].upper() for r in get_all_rows(lookup_path) if r.get("status") != "retired"})


def load(path: str = MASTER_PATH) -> Dict[str, Any]:
    return _load_json(path) or {}


_FIN_CACHE: Dict[str, Any] = {}   # path → (mtime, tickers)


def master_entry(ticker: str, path: str = MASTER_PATH) -> Optional[Dict[str, Any]]:
    """マスターの1銘柄（無ければ None）。ファイルの更新時刻が変わったときだけ読み直す。"""
    try:
        mt = os.path.getmtime(path)
    except OSError:
        return None
    hit = _FIN_CACHE.get(path)
    if hit is None or hit[0] != mt:
        hit = (mt, (load(path).get("tickers") or {}))
        _FIN_CACHE[path] = hit
    return hit[1].get((ticker or "").upper())


def is_financial_ticker(ticker: str, sector: Optional[str] = None, industry: Optional[str] = None,
                        path: str = MASTER_PATH) -> bool:
    """金融・資産の重い業種か（[[SECTOR-MASTER-ADOPT-1]] 段3。yfinance の sector=="Financial Services" の直接比較をやめ、ここ1か所に）。
    マスターにある銘柄はマスターの financial（sector＋SIC）。無い銘柄（登録の直後）は sector・industry だけの financial() と同じ。
    保険の判定（industry・ticker の別の概念）とは別。"""
    e = master_entry(ticker, path)
    fin = (e or {}).get("financial")
    if isinstance(fin, dict) and "value" in fin:
        return bool(fin["value"])
    return bool(financial(sector, industry, None)["value"])


def _detect_changes(prev: Dict[str, Any], new: Dict[str, Any], ticker: str, now: str) -> List[Dict[str, str]]:
    """yfinance の sector・industry が前回から変わった（両方とも値がある）とき、[P] の件の形で返す"""
    out = []
    for field in ("sector", "industry"):
        old, cur = (prev or {}).get(field), new.get(field)
        if old and cur and old != cur:
            out.append({"kind": "sector_change", "ticker": ticker, "match": f"{field}:{old}→{cur}",
                        "detail": f"{ticker} の yfinance の {field} が「{old}」から「{cur}」に変わった（業種のマスター）",
                        "detected_at": now})
    return out


def build(tickers: Optional[List[str]] = None, path: str = MASTER_PATH, attr_dir: str = ATTR_DIR,
          sec_dir: str = SEC_DIR, lookup_path: Optional[str] = None, now: Optional[str] = None) -> Dict[str, Any]:
    """マスターを作り直して返す（書かない）。tickers を渡すとその銘柄だけ作り直し、ほかは前回のまま"""
    now = now or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    prev = load(path)
    prev_entries = prev.get("tickers") or {}
    changes = list(prev.get("changes") or [])
    seen = {(c["ticker"], c["match"]) for c in changes}
    registered = registered_tickers(lookup_path)
    if tickers is None:
        targets, entries = registered, {}
    else:
        targets = sorted({t.upper() for t in tickers})
        entries = {t: e for t, e in prev_entries.items() if t in set(registered)}
    for t in targets:
        e = build_entry(t, attr_dir, sec_dir)
        for c in _detect_changes(prev_entries.get(t), e, t, now):
            if (c["ticker"], c["match"]) not in seen:
                changes.append(c)
                seen.add((c["ticker"], c["match"]))
        entries[t] = e
    return {
        "_schema_version": SCHEMA_VERSION,
        "_description": "業種のマスター（common/sector_master.py が機械で作る。手で書かない）。主は yfinance の sector・industry、"
                        "SIC・社名は控え。区分と写像は rules の業種の名前の規則で決める。読み手は [[SECTOR-MASTER-ADOPT-1]] で寄せる",
        "generated_at": now,
        "rules": rules(),
        "tickers": {t: entries[t] for t in sorted(entries)},
        "changes": changes,
    }


def write(master: Dict[str, Any], path: str = MASTER_PATH) -> bool:
    """中身（generated_at 以外）が前回と同じなら書かない。書いたら True"""
    prev = load(path)
    strip = lambda d: {k: v for k, v in d.items() if k != "generated_at"}
    if prev and strip(prev) == strip(master):
        return False
    with open(path, "w", encoding="utf-8") as f:
        json.dump(master, f, ensure_ascii=False, indent=1)
        f.write("\n")
    return True


def public_view(master: Dict[str, Any]) -> Dict[str, Any]:
    """画面が読む区分だけ（今は景気循環株。TANUKI SCOREのサイクリカルピークトラップ）"""
    return {
        "_description": "業種のマスター（config/sector_master.json）の区分のうち画面が読むもの。common/sector_master.py が書く（手で書かない）",
        "tickers": {t: {"cyclical": bool(e.get("cyclical"))} for t, e in sorted((master.get("tickers") or {}).items())},
    }


def write_public(master: Dict[str, Any], path: str = PUBLIC_PATH) -> bool:
    """中身が前回と同じなら書かない。書いたら True"""
    view = public_view(master)
    if _load_json(path) == view:
        return False
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(view, f, ensure_ascii=False, indent=1)
        f.write("\n")
    return True


def change_items(path: str = MASTER_PATH) -> List[Dict[str, str]]:
    """[P] ExternalChange の件（common/external_watch.py の形）"""
    return [{k: c[k] for k in ("kind", "ticker", "match", "detail")} for c in (load(path).get("changes") or [])]


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="業種のマスター（config/sector_master.json）を作る")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true", help="登録簿（retired 以外）の全銘柄を作り直す")
    g.add_argument("--ticker", action="append", help="この銘柄だけ作り直して足す（複数可）")
    args = ap.parse_args(argv)
    master = build(None if args.all else args.ticker)
    wrote = write(master)
    write_public(master)
    ents = master["tickers"]
    n_attr = sum(1 for e in ents.values() if e["sector"])
    n_sic = sum(1 for e in ents.values() if e["sic"] is not None)
    print(f"業種のマスター: {len(ents)}銘柄（業種あり {n_attr}・SICあり {n_sic}・区分なし "
          f"{sum(1 for e in ents.values() if e['unclassified'])}・景気循環株 {sum(1 for e in ents.values() if e['cyclical'])}）、"
          f"業種の変化の記録 {len(master['changes'])}件 → {'書いた' if wrote else '変化なし（書かない）'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
