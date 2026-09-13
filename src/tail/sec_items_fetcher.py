#!/usr/bin/env python3
"""
TANUKI TAIL — sec_items_fetcher.py  ([[TAIL-SEC-ITEMS-1]]、2026-09-13新設)

10-K の Item 1A（Risk Factors）・Item 3（Legal Proceedings）・
Item 7（MD&A）を年次基準として取得し、直近10-Kのreport_date以降に
提出された10-Qの期中更新分（Part II Item 1A・Part II Item 1・
Part I Item 2）もあわせて取得・保存する。

既存`sec_ctrl_fetcher.py`（Item4専用）の構造（EDGAR取得・HTML除去・
Grok翻訳・{period}.json/latest.json/index.json保存）を踏襲し、
EDGAR取得・HTML除去・Grok翻訳ユーティリティは同モジュールから再利用する
（重複実装を避けるため、`_get_recent_filings()`をform引数化した版・
`_translate_excerpt()`を項目非依存化した版として`sec_ctrl_fetcher.py`
側に汎用化済み）。

Item1A/3/7はItem4と異なり、目次(TOC)・本文中の相互参照によるItemマーカー
誤検知が実データ調査（STEP1、PLTR/SOFI/CELHの実filing）で多数確認された
ため、以下2点の追加フィルタを組み込んでいる:
  - TOC誤検知除外: 「Item番号+タイトル直後に裸のページ番号、その直後に
    次Itemマーカー」というパターンを検知し候補から除外する
  - 10-Q側Part II境界の限定: risk_factors・legal_proceedingsは
    `PART II`マーカー以降のみを検索範囲にすることで、Part Iの同一
    Item番号（例: 10-QのItem 1=Financial Statements(Part I) vs
    Legal Proceedings(Part II)）との衝突を回避する

出力:
    docs/portfolio/tail/data/{item_key}/{TICKER}/{FY}FY.json   (10-K由来、年次)
    docs/portfolio/tail/data/{item_key}/{TICKER}/{YYYY}Q{N}.json (10-Q由来、四半期)
    docs/portfolio/tail/data/{item_key}/{TICKER}/latest.json   (最新版エイリアス)
    docs/portfolio/tail/data/{item_key}/{TICKER}/index.json    (period一覧)
    item_key ∈ {risk_factors, legal_proceedings, mda}

使用方法:
    python src/tail/sec_items_fetcher.py           # tail ポジション全銘柄
    python src/tail/sec_items_fetcher.py SOUN PLTR  # 個別指定
"""

import os
import json
import re
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple

# ── パス設定 ──────────────────────────────────────────────────
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root  = os.path.abspath(os.path.join(script_dir, "..", ".."))

if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
from common.sec_data import tickers as _tickers_mod  # noqa: E402

# sec_ctrl_fetcher.py側の既存ユーティリティを再利用する（重複実装を避ける。
# [[TAIL-SEC-ITEMS-1]]対応で_get_recent_filings()・_translate_excerpt()
# として汎用化済み）
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)
import sec_ctrl_fetcher as _ctrl  # noqa: E402

DATA_DIR     = _ctrl.DATA_DIR
POS_IDX_PATH = _ctrl.POS_IDX_PATH
JST          = _ctrl.JST
EDGAR_BASE   = _ctrl.EDGAR_BASE
load_tail_tickers = _ctrl.load_tail_tickers

_get_recent_filings = _ctrl._get_recent_filings
_edgar_get          = _ctrl._edgar_get
_strip_html         = _ctrl._strip_html
_translate_excerpt  = _ctrl._translate_excerpt


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 項目定義テーブル
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# annual_*: 10-K内でのItem境界（Part分けなし）
# quarterly_*: 10-Q内でのItem境界。restrict_after_re を指定した項目は
# そのパターンの最終出現位置より後ろのみを検索範囲にする
# （STEP1調査でPart I/IIの同一Item番号衝突が実データで確認されたため）。

# [[TAIL-SEC-ITEMS-1]] STEP2-Bパイロットで発見・修正: 単純な`part\s+ii\b`は
# 文書末尾の証明書・脚注等にある「Part II, Item 5. Other Information」等の
# 他filingへのクロスリファレンスにも一致してしまい、`matches[-1]`（最終
# 出現位置）がそのクロスリファレンス位置になり本来のPart II区切りより
# 後ろに来てしまう実例（PLTR 2026-03-31 10-Qで発生、risk_factors・
# legal_proceedingsの抽出が失敗）を確認した。実際のPart II区切り見出しは
# 目次・本文とも必ず"OTHER INFORMATION"が直後（記号・空白のみを挟んで）に
# 続くのに対し、クロスリファレンスは"Part II, Item N. ..."のように
# 番号付きItem名を挟むため、この一致パターンで区別できる。
_PART2_RE = re.compile(r"(?i)part\s+ii[\.\-\s]{1,5}other\s+information")

ITEM_CONFIGS: Dict[str, Dict[str, Any]] = {
    "risk_factors": {
        "label_ja": "Item 1A「Risk Factors」",
        "annual_item_re":  re.compile(r"(?i)item\s+1a[\.\s]"),
        "annual_next_res": [re.compile(r"(?i)item\s+1b[\.\s]"), re.compile(r"(?i)item\s+2[\.\s]")],
        "annual_anchor_re": re.compile(r"(?i)risk\s+factors"),
        "quarterly_item_re":  re.compile(r"(?i)item\s+1a[\.\s]"),
        "quarterly_next_res": [re.compile(r"(?i)item\s+2[\.\s]")],
        "quarterly_anchor_re": re.compile(r"(?i)risk\s+factors"),
        "quarterly_restrict_after_re": _PART2_RE,
        "no_change_re": re.compile(
            r"(?i)no\s+material\s+changes?\s+(?:to|from|in)\s+(?:our\s+|the\s+)?risk\s+factors"
        ),
        "translate_desc_annual":    "10-K の Item 1A「Risk Factors」",
        "translate_desc_quarterly": "10-Q Part II Item 1A「Risk Factors」の期中更新",
        "translate_timeout": 60,
    },
    "legal_proceedings": {
        "label_ja": "Item 3「Legal Proceedings」",
        "annual_item_re":  re.compile(r"(?i)item\s+3[\.\s]"),
        "annual_next_res": [re.compile(r"(?i)item\s+4[\.\s]")],
        "annual_anchor_re": re.compile(r"(?i)legal\s+proceedings"),
        "quarterly_item_re":  re.compile(r"(?i)item\s+1[\.\s](?!a|b|c)"),
        "quarterly_next_res": [re.compile(r"(?i)item\s+1a[\.\s]")],
        "quarterly_anchor_re": re.compile(r"(?i)legal\s+proceedings"),
        "quarterly_restrict_after_re": _PART2_RE,
        "no_change_re": None,  # Item3は「変更なし」という枠組み自体が該当しない（STEP1確認済み）
        "translate_desc_annual":    "10-K の Item 3「Legal Proceedings」",
        "translate_desc_quarterly": "10-Q Part II Item 1「Legal Proceedings」の期中更新",
        "translate_timeout": 60,
    },
    "mda": {
        "label_ja": "Item 7「MD&A」",
        "annual_item_re":  re.compile(r"(?i)item\s+7[\.\s](?!a)"),
        "annual_next_res": [re.compile(r"(?i)item\s+7a[\.\s]"), re.compile(r"(?i)item\s+8[\.\s]")],
        "annual_anchor_re": re.compile(r"(?i)management.{0,3}s\s+discussion\s+and\s+analysis"),
        "quarterly_item_re":  re.compile(r"(?i)item\s+2[\.\s]"),
        "quarterly_next_res": [re.compile(r"(?i)item\s+3[\.\s]"), re.compile(r"(?i)item\s+4[\.\s]")],
        "quarterly_anchor_re": re.compile(r"(?i)management.{0,3}s\s+discussion\s+and\s+analysis"),
        "quarterly_restrict_after_re": None,  # MD&AはPart I側、anchor確認のみで十分（STEP1確認済み）
        "no_change_re": None,
        "translate_desc_annual":    "10-K の Item 7「Management's Discussion and Analysis」",
        "translate_desc_quarterly": "10-Q Part I Item 2「Management's Discussion and Analysis」の期中更新",
        # [[TAIL-SEC-ITEMS-1]] STEP2-Bパイロットで、MD&Aは抜粋が長く
        # 応答生成に時間がかかりやすいため60秒では複数回タイムアウト
        # する実例（PLTR 2四半期分、3回リトライ後も全て失敗）を確認した。
        # リトライ回数（GROK_MODELSの3回）は変更せず、1回あたりの
        # タイムアウトのみ120秒へ延長する。
        "translate_timeout": 120,
    },
}

ITEM_KEYS = list(ITEM_CONFIGS.keys())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Item境界抽出（汎用版）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_TOC_PAGENUM_RE = re.compile(r"\n\s*\d{1,4}\s*\n")


def _looks_like_toc_entry(text: str, start: int, next_res: List[re.Pattern],
                           window: int = 100) -> bool:
    """目次(TOC)エントリらしいかを判定する（[[TAIL-SEC-ITEMS-1]] STEP1調査:
    `Item 1A.\\nRisk Factors\\n12\\nItem 1B.\\n...`のように、タイトル直後に
    裸のページ番号、その直後に次Itemマーカーが続くパターンが目次特有と判明）。
    """
    tail = text[start:start + window]
    m = _TOC_PAGENUM_RE.search(tail)
    if not m:
        return False
    after = start + m.end()
    probe = text[after:after + 60]
    return any(nre.match(probe) for nre in next_res)


def extract_item_section(text: str, item_re: re.Pattern, next_res: List[re.Pattern],
                          anchor_re: re.Pattern,
                          restrict_after_re: Optional[re.Pattern] = None,
                          anchor_window: int = 3000,
                          max_chars: int = 20000) -> Tuple[Optional[int], str]:
    """
    Itemセクションを抽出する汎用関数（`sec_ctrl_fetcher.py::_extract_item4_ctrl()`
    の一般化版、[[TAIL-SEC-ITEMS-1]]）。

    1. `restrict_after_re`指定時、その最終出現位置より前を候補から除外する
       （10-Qの`PART II`境界指定用。Part Iの同一Item番号との衝突回避）
    2. `item_re`の全出現位置のうち、目次エントリ（`_looks_like_toc_entry()`）
       を除外する
    3. 残った候補のうち、区間先頭`anchor_window`文字以内に`anchor_re`が
       一致する最初の候補を採用し、次の`next_res`いずれかの一致位置まで を
       本文とする（一致がなければmax_chars*3を上限とする）

    Returns:
        (採用した候補の開始位置 or None, 抽出テキスト（max_chars上限）)
        該当なしの場合は (None, "")
    """
    search_start = 0
    if restrict_after_re:
        matches = list(restrict_after_re.finditer(text))
        if matches:
            search_start = matches[-1].start()

    starts = [m.start() for m in item_re.finditer(text) if m.start() >= search_start]
    if not starts:
        return None, ""

    for s in starts:
        if _looks_like_toc_entry(text, s, next_res):
            continue
        end = None
        for nre in next_res:
            m = nre.search(text, s + 200)
            if m and (end is None or m.start() < end):
                end = m.start()
        if end is None:
            end = s + max_chars * 3
        segment = text[s:end]
        if anchor_re.search(segment[:anchor_window]):
            return s, segment[:max_chars]

    return None, ""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 「変更なし」検知
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def detect_no_change(text: str, no_change_re: Optional[re.Pattern]) -> Optional[bool]:
    """「重要な変更なし」文言を検知する（正規表現のみ、STEP2確定方針）。

    Returns:
        True  = 「変更なし」文言を検知
        False = 文言を検知しなかった（実際に変更があったか、単に文言を
                使わない開示方式〈PLTR型〉かは区別できない。呼び出し側で
                changed=Trueまたは"unknown"扱いとして明示する）
        None  = この項目には「変更なし」の枠組み自体が適用されない
                （no_change_re未設定、例: legal_proceedings・mda）
    """
    if no_change_re is None:
        return None
    return bool(no_change_re.search(text))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ユーティリティ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _report_date_to_fy(report_date: str) -> str:
    try:
        dt = datetime.strptime(report_date, "%Y-%m-%d")
        return f"{dt.year}FY"
    except Exception:
        return f"{report_date}FY"


def _report_date_to_quarter(report_date: str) -> str:
    try:
        dt = datetime.strptime(report_date, "%Y-%m-%d")
        q  = (dt.month - 1) // 3 + 1
        return f"{dt.year}Q{q}"
    except Exception:
        return report_date


def _fetch_filing_text(cik_int: int, accn: str, pdoc: str) -> Optional[str]:
    accn_nd = accn.replace("-", "")
    url = f"{EDGAR_BASE}/Archives/edgar/data/{cik_int}/{accn_nd}/{pdoc}"
    resp = _edgar_get(url, timeout=120)
    if not resp:
        return None
    return _strip_html(resp.text)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# メイン取得（10-K基準1件 + 10-Q期中更新分）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def fetch_annual(ticker: str, cik: str, item_key: str) -> Optional[Dict[str, Any]]:
    """直近10-Kから指定item_keyのセクションを取得する。"""
    cfg = ITEM_CONFIGS[item_key]
    filings = _get_recent_filings(cik, form="10-K", count=1)
    if not filings:
        print(f"  [{ticker}/{item_key}] 10-K 未発見")
        return None

    filing  = filings[0]
    accn    = filing["accession"]
    pdoc    = filing["primary_document"]
    report  = filing["report_date"]
    filed   = filing["filing_date"]
    cik_int = int(cik.lstrip("0") or "0")

    if not pdoc:
        print(f"  [{ticker}/{item_key}] primaryDocument なし ({accn})")
        return None

    print(f"  [{ticker}/{item_key}] 10-K 取得: accn={accn}")
    raw_text = _fetch_filing_text(cik_int, accn, pdoc)
    if raw_text is None:
        return None

    start, section_text = extract_item_section(
        raw_text, cfg["annual_item_re"], cfg["annual_next_res"], cfg["annual_anchor_re"],
    )
    if start is None:
        print(f"  [{ticker}/{item_key}] 10-K セクション抽出失敗（該当箇所なし）")
        return None
    print(f"  [{ticker}/{item_key}] 10-K 抽出: {len(section_text)}文字 (start={start})")

    excerpt = section_text[:2000]
    excerpt_ja = _translate_excerpt(
        excerpt, cfg["translate_desc_annual"], timeout=cfg.get("translate_timeout", 60),
    )

    return {
        "ticker":        ticker.upper(),
        "item_key":      item_key,
        "period":        _report_date_to_fy(report),
        "source":        "10-K",
        "filing_date":   filed,
        "report_date":   report,
        "accession":     accn,
        "changed":       None,  # 10-K基準そのものには「変更検知」概念は適用しない
        "excerpt":       excerpt,
        "excerpt_ja":    excerpt_ja,
        "fetched_at":    datetime.now(JST).isoformat(),
    }


def fetch_quarterly_updates(ticker: str, cik: str, item_key: str,
                             after_date: str, count: int = 4) -> List[Dict[str, Any]]:
    """直近10-Kのreport_date以降に提出された10-Qから、指定item_keyの
    期中更新分を取得する。"""
    cfg = ITEM_CONFIGS[item_key]
    filings = _get_recent_filings(cik, form="10-Q", count=count, after_date=after_date)
    if not filings:
        print(f"  [{ticker}/{item_key}] 対象10-Qなし（{after_date}以降）")
        return []

    cik_int = int(cik.lstrip("0") or "0")
    results: List[Dict[str, Any]] = []
    for filing in filings:
        accn   = filing["accession"]
        pdoc   = filing["primary_document"]
        report = filing["report_date"]
        filed  = filing["filing_date"]
        if not pdoc:
            continue

        print(f"  [{ticker}/{item_key}] 10-Q 取得: accn={accn} report={report}")
        raw_text = _fetch_filing_text(cik_int, accn, pdoc)
        if raw_text is None:
            continue

        start, section_text = extract_item_section(
            raw_text, cfg["quarterly_item_re"], cfg["quarterly_next_res"],
            cfg["quarterly_anchor_re"],
            restrict_after_re=cfg.get("quarterly_restrict_after_re"),
        )
        if start is None:
            print(f"  [{ticker}/{item_key}] 10-Q({report}) セクション抽出失敗（該当箇所なし）")
            continue
        print(f"  [{ticker}/{item_key}] 10-Q({report}) 抽出: {len(section_text)}文字 (start={start})")

        no_change = detect_no_change(section_text, cfg["no_change_re"])
        if no_change is None:
            changed = None  # この項目には変更検知の枠組み自体が適用されない
        elif no_change is True:
            changed = False
        else:
            changed = "unknown"  # 「変更なし」文言を検知できなかった
            # （実際に変更ありか、単に文言を使わない開示方式かは区別不能）

        excerpt = section_text[:2000]
        excerpt_ja = _translate_excerpt(
            excerpt, cfg["translate_desc_quarterly"], timeout=cfg.get("translate_timeout", 60),
        )

        results.append({
            "ticker":        ticker.upper(),
            "item_key":      item_key,
            "period":        _report_date_to_quarter(report),
            "source":        "10-Q",
            "filing_date":   filed,
            "report_date":   report,
            "accession":     accn,
            "changed":       changed,
            "excerpt":       excerpt,
            "excerpt_ja":    excerpt_ja,
            "fetched_at":    datetime.now(JST).isoformat(),
        })
        time.sleep(0.3)

    return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 保存
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _save_result(result: Dict[str, Any]) -> None:
    """period別ファイル・latest.json・index.jsonへ保存する。

    [[TAIL-SEC-ITEMS-1]] STEP2-Bパイロットで発見・修正: `fetch_quarterly_
    updates()`はEDGARの返却順（新しい順）で複数四半期分をまとめて返す
    ため、単純にループ内で毎回latest.jsonを上書きすると、最後に処理した
    （＝最も古い）期間がlatest.jsonに残ってしまう回帰があった（PLTR/SOFI
    両方で実際に発生: 2026Q2の後に2026Q1を保存した結果latest.jsonが
    2026Q1のままになっていた）。既存latest.jsonの`report_date`と比較し、
    新しい方のみを採用する。
    """
    item_key   = result["item_key"]
    ticker     = result["ticker"]
    period     = result["period"]
    item_dir   = os.path.join(DATA_DIR, item_key, ticker)
    os.makedirs(item_dir, exist_ok=True)

    period_path = os.path.join(item_dir, f"{period}.json")
    latest_path = os.path.join(item_dir, "latest.json")
    index_path  = os.path.join(item_dir, "index.json")

    with open(period_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    should_write_latest = True
    if os.path.exists(latest_path):
        try:
            with open(latest_path, encoding="utf-8") as f:
                existing_latest = json.load(f)
            existing_report_date = existing_latest.get("report_date") or ""
            new_report_date = result.get("report_date") or ""
            if existing_report_date and new_report_date and new_report_date < existing_report_date:
                should_write_latest = False
        except Exception:
            should_write_latest = True
    if should_write_latest:
        with open(latest_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    existing_periods: List[str] = []
    if os.path.exists(index_path):
        try:
            with open(index_path, encoding="utf-8") as f:
                existing_periods = json.load(f).get("periods", [])
        except Exception:
            existing_periods = []
    merged = sorted(set(existing_periods) | {period}, reverse=True)
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump({"periods": merged}, f, ensure_ascii=False, indent=2)

    print(f"  [{ticker}/{item_key}] 保存: {period_path}")


def fetch_and_save_all_items(ticker: str, cik: str) -> Dict[str, int]:
    """3項目（risk_factors・legal_proceedings・mda）を全て取得・保存する。
    Returns: {"annual_ok": int, "quarterly_ok": int, "ng": int}
    """
    stats = {"annual_ok": 0, "quarterly_ok": 0, "ng": 0}
    for item_key in ITEM_KEYS:
        annual = fetch_annual(ticker, cik, item_key)
        if annual is None:
            stats["ng"] += 1
            continue
        _save_result(annual)
        stats["annual_ok"] += 1

        quarterlies = fetch_quarterly_updates(
            ticker, cik, item_key, after_date=annual["report_date"],
        )
        for q in quarterlies:
            _save_result(q)
            stats["quarterly_ok"] += 1

    return stats


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# エントリポイント
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    args = sys.argv[1:]
    if args:
        tickers = [t.upper() for t in args]
    else:
        tickers = load_tail_tickers()
        if not tickers:
            print("positions_index.json が見つからないか空です")
            sys.exit(1)

    ok, ng = 0, 0
    for ticker in tickers:
        cik = _tickers_mod.get_cik(ticker)
        if not cik:
            print(f"[{ticker}] CIK 未登録 — スキップ")
            ng += 1
            continue

        print(f"\n[{ticker}] CIK={cik}")
        try:
            stats = fetch_and_save_all_items(ticker, cik)
            print(f"  [{ticker}] 完了: annual={stats['annual_ok']} "
                  f"quarterly={stats['quarterly_ok']} ng={stats['ng']}")
            if stats["ng"] == 0:
                ok += 1
            else:
                ng += 1
        except Exception as e:
            print(f"  [{ticker}] エラー: {e}")
            ng += 1

        time.sleep(0.5)

    print(f"\n完了: {ok} 成功 / {ng} 失敗")


if __name__ == "__main__":
    main()
