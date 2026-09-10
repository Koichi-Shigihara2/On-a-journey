#!/usr/bin/env python3
"""
TANUKI TAIL — xbrl_segment_fetcher.py

四半期レビュー用セグメント別KPI取得スクリプト（ローカル & GHA）

処理フロー:
  1. EDGAR submissions API で直近 N 件の 10-Q ファイリングを取得
  2. 各 10-Q の XBRL Instance Document (_htm.xml) をダウンロード
  3. tail_kpi_map.json の tag_history に従い explicitMember でセグメント別数値を抽出
  4. docs/portfolio/tail/data/kpi/{ticker}_layer2.json に保存

使用方法:
    python src/tail/xbrl_segment_fetcher.py --ticker PLTR
    python src/tail/xbrl_segment_fetcher.py --ticker SOFI TSLA
    python src/tail/xbrl_segment_fetcher.py --ticker PLTR --quarters 4

必要パッケージ: requests
"""

import os
import sys
import json
import time
import re
import argparse
from datetime import datetime, date
from typing import Optional, Dict, Any, List

try:
    import requests
except ImportError:
    print("requests が必要です: pip install requests")
    sys.exit(1)

# ── パス設定 ──────────────────────────────────────────────────
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root  = os.path.abspath(os.path.join(script_dir, "..", ".."))

if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
from common.sec_data.layer3_builder import (  # noqa: E402
    build_ticker_store, get_quarterly_series,
)

# tail_kpi_map.jsonはPythonバックエンド専用の手動設定ファイルのため
# config/配下に配置（TAILKPI-CONFIG-LOCATION-1、2026-08-15移動）
KPI_MAP_PATH = os.path.join(repo_root, "config", "tail_kpi_map.json")
OUTPUT_DIR = os.path.join(
    repo_root, "docs", "portfolio", "tail", "data", "kpi"
)

# ── SEC API 設定 ──────────────────────────────────────────────
SEC_HEADERS = {
    "User-Agent": "Koichi Personal Investment Tools koichi@example.com",
    "Accept": "application/json,application/xml,*/*",
}
SEC_BASE = "https://www.sec.gov"
THROTTLE  = 0.2   # SEC リクエスト間隔（秒）


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CIK 取得
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_cik(ticker: str) -> Optional[str]:
    """EDGAR company_tickers.json からティッカー → CIK（10桁）"""
    ticker = ticker.upper()
    try:
        time.sleep(THROTTLE)
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=SEC_HEADERS, timeout=15,
        )
        r.raise_for_status()
        for entry in r.json().values():
            if entry.get("ticker", "").upper() == ticker:
                return str(entry["cik_str"]).zfill(10)
    except Exception as e:
        print(f"  CIK 取得エラー: {e}")
    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 10-Q ファイリング一覧取得
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_10q_filings(cik: str, n: int = 8) -> List[Dict[str, Any]]:
    """
    EDGAR submissions API から最新 n 件の 10-Q を取得。
    Returns list of {accn, filed, period, xml}
    """
    results: List[Dict[str, Any]] = []

    def _extract(fil: Dict[str, Any]) -> None:
        forms   = fil.get("form", [])
        accns   = fil.get("accessionNumber", [])
        filed   = fil.get("filingDate", [])
        reports = fil.get("reportDate", [])
        prims   = fil.get("primaryDocument", [])
        for i, f in enumerate(forms):
            if f != "10-Q" or len(results) >= n:
                continue
            prim = prims[i] if i < len(prims) else ""
            if prim.endswith(".htm"):
                xml_file = prim[:-4] + "_htm.xml"
            elif prim.endswith(".html"):
                xml_file = prim[:-5] + "_htm.xml"
            else:
                xml_file = ""
            results.append({
                "accn":   accns[i] if i < len(accns) else "",
                "filed":  filed[i] if i < len(filed) else "",
                "period": reports[i] if i < len(reports) else "",
                "xml":    xml_file,
            })

    try:
        time.sleep(THROTTLE)
        r = requests.get(
            f"https://data.sec.gov/submissions/CIK{cik}.json",
            headers=SEC_HEADERS, timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        _extract(data.get("filings", {}).get("recent", {}))

        # 古いファイリングが別ページに分割されている場合
        if len(results) < n:
            for sub in data.get("filings", {}).get("files", []):
                if len(results) >= n:
                    break
                nm = sub.get("name", "")
                if not nm.endswith(".json"):
                    continue
                try:
                    time.sleep(THROTTLE)
                    sr = requests.get(
                        f"https://data.sec.gov/submissions/{nm}",
                        headers=SEC_HEADERS, timeout=20,
                    )
                    sr.raise_for_status()
                    _extract(sr.json())
                except Exception:
                    continue

    except Exception as e:
        print(f"  submissions API エラー: {e}")

    return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# XBRL Instance Document ダウンロード
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def download_xbrl(cik: str, accn: str, xml_file: str) -> Optional[str]:
    url = (
        f"{SEC_BASE}/Archives/edgar/data/{int(cik)}/"
        f"{accn.replace('-', '')}/{xml_file}"
    )
    try:
        time.sleep(THROTTLE)
        r = requests.get(url, headers=SEC_HEADERS, timeout=90)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code}: {xml_file}")
            return None
        print(f"  取得: {xml_file} ({len(r.content) // 1024} KB)")
        return r.text
    except Exception as e:
        print(f"  ダウンロード例外: {e}")
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# タグ選択・ユーティリティ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def select_tag(tag_history: List[Dict[str, Any]], period_end: str) -> Optional[str]:
    """valid_from / valid_to に基づいてタグを選択。"""
    try:
        pe = date.fromisoformat(period_end[:10])
    except Exception:
        pe = date.today()
    for entry in sorted(tag_history, key=lambda x: x.get("valid_from", ""), reverse=True):
        vf = entry.get("valid_from")
        vt = entry.get("valid_to")
        if vf and date.fromisoformat(vf) > pe:
            continue
        if vt and pe > date.fromisoformat(vt):
            continue
        return entry["tag"]
    return None


def quarter_label(period_end: str) -> str:
    """'2025-03-31' → '2025Q1'"""
    try:
        d = date.fromisoformat(period_end[:10])
        return f"{d.year}Q{(d.month - 1) // 3 + 1}"
    except Exception:
        return period_end


def _is_quarterly(start: Optional[str], end: Optional[str]) -> bool:
    """期間が 60〜100 日（≒3ヶ月）かどうかを判定。"""
    if not start or not end:
        return True
    try:
        diff = (date.fromisoformat(end) - date.fromisoformat(start)).days
        return 60 <= diff <= 100
    except Exception:
        return True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# XBRL コンテキスト解析
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# コンテキストブロックを抽出する正規表現
_CTX_RE = re.compile(
    r'<(?:\w+:)?context\b[^>]*?id=["\']([^"\']+)["\'][^>]*?>(.*?)</(?:\w+:)?context>',
    re.DOTALL | re.IGNORECASE,
)
_START_RE = re.compile(r'<(?:\w+:)?startDate[^>]*>([\d\-]+)<', re.IGNORECASE)
_END_RE   = re.compile(r'<(?:\w+:)?endDate[^>]*>([\d\-]+)<',   re.IGNORECASE)


def parse_contexts(xml_text: str, dim_local: str) -> Dict[str, Dict[str, Any]]:
    """
    指定ディメンション軸（ローカル名）の explicitMember を持つ
    コンテキスト情報を返す。
    Returns: {ctx_id: {"member": "pltr:CommercialOperatingSegmentMember",
                        "start": "2025-01-01", "end": "2025-03-31"}}
    """
    # ディメンション属性にローカル名が含まれる explicitMember を抽出
    mem_re = re.compile(
        r'<[^>]*?explicitMember[^>]*?dimension=["\'][^"\']*'
        + re.escape(dim_local)
        + r'[^"\']*["\'][^>]*?>\s*(.+?)\s*</[^>]*?explicitMember>',
        re.DOTALL | re.IGNORECASE,
    )

    result: Dict[str, Dict[str, Any]] = {}
    for m in _CTX_RE.finditer(xml_text):
        ctx_id   = m.group(1)
        ctx_body = m.group(2)
        mm = mem_re.search(ctx_body)
        if not mm:
            continue
        sm = _START_RE.search(ctx_body)
        em = _END_RE.search(ctx_body)
        result[ctx_id] = {
            "member": mm.group(1).strip(),
            "start":  sm.group(1) if sm else None,
            "end":    em.group(1) if em else None,
        }
    return result


# 全ディメンションのexplicitMemberを抽出する正規表現。parse_contexts()の
# mem_re（234行目周辺）と同一パターンだが、dim_localを固定せず
# dimension属性の値そのものをキャプチャする（2026-08-19⑥新設、
# [[TAIL-XBRL-MEMBER-VALIDATION-GAP-1]]対応）。
_ALL_EXPLICIT_MEMBER_RE = re.compile(
    r'<[^>]*?explicitMember[^>]*?dimension=["\']([^"\']+)["\'][^>]*?>\s*(.+?)\s*</[^>]*?explicitMember>',
    re.DOTALL | re.IGNORECASE,
)


def extract_segment_members(xml_text: str) -> List[Dict[str, str]]:
    """XBRL Instance Document内の全コンテキストからexplicitMemberを
    抽出し、(dimension, member) のユニークな組み合わせのリストを返す。

    `parse_contexts()`と同じ`_CTX_RE`によるコンテキストブロック走査＋
    explicitMember正規表現を再利用し、対象ディメンションを固定しない
    版として実装した（新規のパース処理を書き起こしていない）。

    Returns: [{"dimension": "us-gaap:StatementBusinessSegmentsAxis",
               "member": "pltr:CommercialOperatingSegmentMember"}, ...]
    """
    seen: set = set()
    result: List[Dict[str, str]] = []
    for m in _CTX_RE.finditer(xml_text):
        ctx_body = m.group(2)
        for mm in _ALL_EXPLICIT_MEMBER_RE.finditer(ctx_body):
            dim = mm.group(1).strip()
            mem = mm.group(2).strip()
            key = (dim, mem)
            if key in seen:
                continue
            seen.add(key)
            result.append({"dimension": dim, "member": mem})
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# XBRL ファクト値の抽出
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def extract_fact(
    xml_text: str,
    ctx_ids: set,
    concept_local: str,
    period_end: str,
    contexts: Dict[str, Dict[str, Any]],
) -> Optional[float]:
    """
    指定コンテキストIDセットから concept_local の数値を取得。
    period_end に合致する四半期値を優先して返す。
    [\\w-]+ で us-gaap: のようなハイフン含む namespace prefix に対応。
    """
    fact_re = re.compile(
        r'<(?:[\w-]+:)?' + re.escape(concept_local)
        + r'\b[^>]*?contextRef=["\']([^"\']+)["\'][^>]*?>\s*(-?[\d.]+)\s*<',
        re.IGNORECASE,
    )
    pe_ym = period_end[:7]  # "2025-03"

    candidates: List[tuple] = []
    for fm in fact_re.finditer(xml_text):
        cref = fm.group(1)
        if cref not in ctx_ids:
            continue
        try:
            val = float(fm.group(2))
        except ValueError:
            continue
        ctx   = contexts.get(cref, {})
        end   = ctx.get("end") or ""
        start = ctx.get("start")
        # 優先度: ① period_end 年月一致  ② quarterly 期間  ③ その他
        pm = end.startswith(pe_ym)
        iq = _is_quarterly(start, end)
        candidates.append((val, pm, iq))

    if not candidates:
        return None
    candidates.sort(key=lambda c: (not c[1], not c[2]))
    return candidates[0][0]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# XBRL 全KPI抽出
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def parse_and_extract(
    xml_text: str,
    kpi_list: List[Dict[str, Any]],
    period_end: str,
) -> Dict[str, Optional[float]]:
    """
    XBRL Instance Document から kpi_list の全 KPI 値を抽出。
    Returns: {kpi_name: float_or_None}
    """
    # ディメンション軸ごとにコンテキストを一度だけ解析
    dims: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for kpi in kpi_list:
        dim_full  = kpi.get("dimension", "us-gaap:StatementBusinessSegmentsAxis")
        dim_local = dim_full.split(":")[-1]
        if dim_local not in dims:
            dims[dim_local] = parse_contexts(xml_text, dim_local)

    results: Dict[str, Optional[float]] = {}

    for kpi in kpi_list:
        name      = kpi["kpi_name"]
        dim_local = kpi.get("dimension", "us-gaap:StatementBusinessSegmentsAxis").split(":")[-1]
        ctx_map   = dims.get(dim_local, {})
        concept   = kpi.get("revenue_tag", "us-gaap:Revenues").split(":")[-1]
        fallbacks = kpi.get("fallback_tags", [])

        seg_tag = select_tag(kpi["tag_history"], period_end)
        if not seg_tag:
            results[name] = None
            continue

        def _try(t: str) -> Optional[float]:
            t_local = t.split(":")[-1]
            ctx_ids = {
                cid for cid, info in ctx_map.items()
                if info["member"] == t or info["member"].split(":")[-1] == t_local
            }
            if not ctx_ids:
                return None
            return extract_fact(xml_text, ctx_ids, concept, period_end, ctx_map)

        val = _try(seg_tag)
        if val is None:
            for fb in fallbacks:
                val = _try(fb)
                if val is not None:
                    print(f"    {name}: fallback '{fb}' 使用")
                    break

        results[name] = val
    return results


def fetch_layer3_kpis(ticker: str, kpi_list: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """`"source": "layer3"`のKPIについて、SEC EDGAR Layer3統合スキーマ
    （`build_ticker_store()`/`get_quarterly_series()`）から値を取得する。

    出力は既存のXBRL直接取得と同じスキーマ（`{quarter, value, filed}`の
    リスト）に揃える（消費側〈`quarterly_review_generator.py`等〉を
    変えなくて済むように、2026-08-19⑧、[[TAIL-XBRL-SEGMENT-FETCHER-
    NONDIMENSIONED-GAP-1]]対応）。

    `layer3_field`（単一フィールド直接参照）と`layer3_formula`
    （`"field_a/field_b"`形式の除算のみ対応。それ以外の演算子・複数演算
    には対応しない）の両方を扱う。

    **分母が0またはNoneの四半期は、その四半期のエントリ自体を作らず
    スキップする**（falsy-zeroを作らない・0除算例外も出さない。
    Layer3の`val`は元々`None`でも構造的に`0`になりうるため、`val==0`と
    `val is None`の両方を明示的に弾く）。
    """
    store = build_ticker_store(ticker)
    if store is None:
        return {kpi["kpi_name"]: [] for kpi in kpi_list}

    result: Dict[str, List[Dict[str, Any]]] = {}
    for kpi in kpi_list:
        name    = kpi["kpi_name"]
        field   = kpi.get("layer3_field")
        formula = kpi.get("layer3_formula")
        entries: List[Dict[str, Any]] = []

        if field:
            for e in get_quarterly_series(store, field):
                val = e.get("val")
                if val is None:
                    continue
                entries.append({
                    "quarter": quarter_label(e["end"]),
                    "value":   val,
                    "filed":   e.get("filed", ""),
                })
        elif formula and "/" in formula:
            num_field, den_field = [s.strip() for s in formula.split("/", 1)]
            num_by_end = {e["end"]: e for e in get_quarterly_series(store, num_field)}
            den_by_end = {e["end"]: e for e in get_quarterly_series(store, den_field)}
            for end in sorted(num_by_end):
                num_e = num_by_end[end]
                den_e = den_by_end.get(end)
                num_val = num_e.get("val")
                den_val = den_e.get("val") if den_e else None
                if num_val is None or den_val is None or den_val == 0:
                    continue  # 分母が0/None → その四半期はスキップ（falsy-zeroを作らない）
                entries.append({
                    "quarter": quarter_label(end),
                    "value":   round(num_val / den_val, 6),
                    "filed":   num_e.get("filed", ""),
                })

        result[name] = entries
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1銘柄処理
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def fetch_ticker(ticker: str, kpi_map: Dict[str, Any], out_dir: str, n_quarters: int = 8) -> str:
    """1銘柄分のKPI取得を実行する。

    Returns: 状態を表す文字列。
      "no_kpi_defined" — tail_kpi_map.jsonに定義なし
      "cik_failed"     — CIK取得失敗
      "filing_failed"  — 10-Qファイリング取得失敗
      "ok"             — 登録KPI全件取得成功（missing_kpisなし）
      "partial"        — 一部のKPIのみ取得失敗
      "all_failed"      — 登録KPI全件が取得失敗（0件成功）

    **2026-08-19⑥修正**: 修正前は`missing_kpis`があっても常に`True`を
    返し、`main()`の完了サマリーが部分失敗・全件失敗を「✓」（成功）と
    表示していた。これは`[[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-
    GAP-1]]`で発見したサイレント・フォールバックの一部であり、戻り値を
    多段階の状態文字列に変更して可視化した（終了コード・呼び出し側の
    分岐ロジックは変更していない、可視化のみ）。
    """
    ticker   = ticker.upper()
    kpi_list = kpi_map.get(ticker)
    if not kpi_list:
        print(f"[{ticker}] tail_kpi_map.json に定義なし → スキップ")
        return "no_kpi_defined"

    print(f"\n{'─' * 52}")
    print(f"  {ticker}")
    print(f"{'─' * 52}")

    # source=="layer3"のKPIとXBRL直接取得のKPIを分ける（2026-08-19⑧、
    # 既存エントリはsourceキー無し＝XBRL直接取得のまま後方互換）。
    # Layer3経由はSEC提出物の再取得を必要としないため、CIK/10-Q取得の
    # 成否とは独立に処理する。
    layer3_kpis = [k for k in kpi_list if k.get("source") == "layer3"]
    xbrl_kpis   = [k for k in kpi_list if k.get("source") != "layer3"]

    # KPIごとに四半期データを蓄積（両方の経路分をあらかじめ初期化）
    kpi_data: Dict[str, List[Dict[str, Any]]] = {
        kpi["kpi_name"]: [] for kpi in kpi_list
    }

    if layer3_kpis:
        print(f"  Layer3経由: {len(layer3_kpis)}件")
        layer3_results = fetch_layer3_kpis(ticker, layer3_kpis)
        for name, entries in layer3_results.items():
            kpi_data[name] = entries
            if entries:
                latest = entries[-1]
                print(f"    {name}: {latest['value']:,} ({latest['quarter']}時点、Layer3経由)")
            else:
                print(f"    {name}: 取得失敗（Layer3、該当フィールドにデータなし）")

    if not xbrl_kpis:
        return _write_layer2_output(ticker, kpi_data, out_dir)

    cik = get_cik(ticker)
    if not cik:
        print("  CIK 取得失敗")
        return "cik_failed"
    print(f"  CIK: {cik}")

    filings = get_10q_filings(cik, n=n_quarters)
    if not filings:
        print("  10-Q ファイリング取得失敗")
        return "filing_failed"
    print(f"  10-Q: {[f['period'] for f in filings]}")

    for filing in filings:
        period = filing["period"]   # "2025-03-31"
        filed  = filing["filed"]    # "2025-05-05"
        accn   = filing["accn"]
        xml_f  = filing["xml"]
        qlabel = quarter_label(period)

        if not xml_f:
            print(f"\n  [{qlabel}] XML ファイル名不明 → スキップ")
            continue

        print(f"\n  [{qlabel}] period={period}  filed={filed}")

        xml_text = download_xbrl(cik, accn, xml_f)
        if not xml_text:
            continue

        extracted = parse_and_extract(xml_text, xbrl_kpis, period)
        del xml_text  # メモリ解放

        for kpi_name, val in extracted.items():
            if val is not None:
                # 整数に近い値（USD金額）はint、小数値（比率）はfloatのまま保持
                out_val = int(round(val)) if val % 1 < 0.001 else round(val, 4)
                print(f"    {kpi_name}: {out_val:,}")
                kpi_data[kpi_name].append({
                    "quarter": qlabel,
                    "value":   out_val,
                    "filed":   filed,
                })
            else:
                print(f"    {kpi_name}: 取得失敗")

    return _write_layer2_output(ticker, kpi_data, out_dir)


# [[KPI-UNIT-HARDCODE-USD-1]]: KPI名に比率・マージン・成長率を示す語を
# 含む場合、unitを"ratio"とする（値は0〜1の小数比率として保存される
# 前提。tail_kpi_map.jsonの実データで「率」「マージン」を含むKPI名は
# 例外なく比率値〈貢献利益率=0.78・営業利益率=-0.234456等〉であることを
# 全ティッカー横断で確認済み。「希薄化後EPS成長率」は現状missing_kpis
# のまま値0件だが、KPI名の意図〈将来的にYoY成長率として実装される
# 予定、[[TAIL-LAYER3-FORMULA-YOY-UNSUPPORTED-1]]参照〉に基づき同様に
# ratio扱いとする——値の有無ではなく定義そのものに基づく判定のため、
# 将来値が入るようになった時点でも再判定不要）。
# 値の型（int/float、542-543行付近の既存ロジック）だけでは「小数の
# ドル金額（例: EPS $0.08）」と「小数の比率（例: 0.78）」を区別できない
# ため、判定はKPI名（定義）ベースに統一する。
_RATIO_KPI_NAME_KEYWORDS = ("率", "マージン", "Margin", "Rate", "Ratio")


def _infer_kpi_unit(kpi_name: str) -> str:
    """KPI名から表示単位を推定する（"ratio" or "USD"）。"""
    if any(kw in kpi_name for kw in _RATIO_KPI_NAME_KEYWORDS):
        return "ratio"
    return "USD"


def _write_layer2_output(ticker: str, kpi_data: Dict[str, List[Dict[str, Any]]], out_dir: str) -> str:
    """`{ticker}_layer2.json`を書き出し、状態文字列を返す。XBRL直接
    取得・Layer3経由取得の両経路が同じ`kpi_data`スキーマへ書き込んで
    いるため、出力構築ロジックは1箇所に共通化した（2026-08-19⑧）。
    """
    missing_kpis = [n for n, d in kpi_data.items() if not d]
    total_kpis = len(kpi_data)
    success_count = total_kpis - len(missing_kpis)
    layer2_complete = len(missing_kpis) == 0

    output = {
        "ticker":          ticker,
        "fetched_at":      datetime.now().strftime("%Y-%m-%dT%H:%M:%S+09:00"),
        "layer2_complete": layer2_complete,
        "missing_kpis":    missing_kpis,
        "kpis": {
            n: {"unit": _infer_kpi_unit(n), "data": d}
            for n, d in kpi_data.items()
        },
    }

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{ticker}_layer2.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    if layer2_complete:
        result = "ok"
        status = "✓"
    elif success_count == 0:
        result = "all_failed"
        status = "✗"
    else:
        result = "partial"
        status = "△"
    print(f"\n  {status} 保存: {out_path}  ({success_count}/{total_kpis}件取得)")
    if missing_kpis:
        print(f"  missing_kpis: {missing_kpis}")
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# エントリーポイント
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    parser = argparse.ArgumentParser(
        description="TANUKI TAIL — XBRL Segment KPI Fetcher (10-Q)"
    )
    parser.add_argument(
        "--ticker", required=True, nargs="+",
        help="対象ティッカー（複数可）: --ticker PLTR SOFI TSLA",
    )
    parser.add_argument(
        "--quarters", type=int, default=8,
        help="取得四半期数（デフォルト: 8）",
    )
    args = parser.parse_args()

    with open(KPI_MAP_PATH, encoding="utf-8") as f:
        kpi_map = json.load(f)

    # 状態ごとの表示アイコン（2026-08-19⑥、部分失敗・全件失敗を
    # 成功「✓」と区別して表示する。終了コードは変更しない）
    _RESULT_ICON = {
        "ok":             "✓",
        "partial":        "△",
        "all_failed":     "✗",
        "no_kpi_defined": "・",
        "cik_failed":     "✗",
        "filing_failed":  "✗",
    }
    _RESULT_LABEL = {
        "ok":             "全件取得",
        "partial":        "一部取得失敗",
        "all_failed":     "全件取得失敗",
        "no_kpi_defined": "KPI未定義",
        "cik_failed":     "CIK取得失敗",
        "filing_failed":  "10-Q取得失敗",
    }

    summary: Dict[str, str] = {}
    for tkr in args.ticker:
        result = fetch_ticker(tkr.upper(), kpi_map, OUTPUT_DIR, n_quarters=args.quarters)
        summary[tkr.upper()] = result

    print(f"\n{'━' * 52}")
    print("完了:")
    for tkr, result in summary.items():
        icon  = _RESULT_ICON.get(result, "?")
        label = _RESULT_LABEL.get(result, result)
        print(f"  {icon} {tkr}: {label}")


if __name__ == "__main__":
    main()
