"""
common/sec_data/dimension_aggregate_fetcher.py

[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]の「対応不可」ケース
（[[ANOMALY-PATTERN-CATALOG-1]]型D: 次元分解開示専用型）を根治的に
解消するための機構。

company_facts.json一括API（SECのcompanyfacts bulk API）は以下2種類の
事実を返さない構造的制約を持つことが実データ調査で判明している:
  (a) 次元分解（dimensionally-qualified）開示のみで、非次元
      （デフォルトコンテキスト）版が存在しない事実
      （例: PLTR/CART/CELHの転換優先株式、`StatementClassOfStockAxis`
      でRedeemable/Nonredeemable等のメンバーに分解されている）
  (b) 発行体固有のカスタム名前空間タグ（`us-gaap:`以外のprefix。
      例: V(Visa)の`v:TotalTemporaryEquityAndMinorityInterest`、
      ASTSの前身New Providence Acquisition Corp.の
      `npac:TemporaryEquitySharesRedemptions`）— company_facts.jsonの
      `facts`辞書には登録済み標準名前空間（us-gaap/dei/srt/invest/ffd等）
      のみが載り、企業独自の拡張タクソノミ名前空間は一切現れない

いずれのケースも、正しい値は個別filingの生XBRLインスタンス文書
（一括APIとは別の取得経路）には確実に存在する。本モジュールはこの
生XBRLインスタンスを直接取得・パースし、指定したコンセプトの値を
（次元分解されている場合は指定した軸の全メンバーを機械的に合算して）
取得する。取得した値は「タグ由来のない生の数値の直接注入」ではなく、
個別filingのXBRLタグから機械的に抽出・合算した値であり、再実行すれば
誰でも同じ値を再現できる（引用情報＝accn・concept・axis・periodを
`dimension_aggregate_registry.json`に記録する）。

**スコープの限定**: BS恒等式チェック（`parser.py::
_bs_identity_extra_components()`、診断・表示専用の補助情報）への適用に
限定する。revenue/cost_of_revenue等、DCF計算に直接使われるフィールド
（CDNS/INTU等で確認されている同型の構造的制約）への適用は、計算結果への
実害リスクが診断用途より格段に大きいため対象外とし、別途独立タスクとして
扱う（[[ANOMALY-PATTERN-CATALOG-1]]型D参照）。

使用方法（CLI、手動検証・登録専用。パイプライン実行時に自動で呼ばれる
ことはない——レジストリは事前に本CLIで検証・登録した結果のみを保持し、
`parser.py`はレジストリを読むだけで生XBRLへのライブ取得は行わない。
理由: 対象は2008〜2025年の確定済み過去filingのみで値が将来変化しない
ため、毎回のパイプライン実行でSEC EDGARへライブ問い合わせする必要
自体がなく、ネットワーク依存・レート制限リスクを本番パイプラインへ
持ち込まない設計とした）:

    python common/sec_data/dimension_aggregate_fetcher.py \\
        --ticker PLTR --cik 1321655 --accn 0001193125-21-060650 \\
        --concept TemporaryEquityCarryingAmountAttributableToParent \\
        --axis StatementClassOfStockAxis --period 2019-12-31 \\
        --field temporary_equity --expect 2127231000 \\
        --citation "10-K accn 0001193125-21-060650 R2.htm CONSOLIDATED BALANCE SHEETS" \\
        --register

--expect指定時、計算結果が期待値と一致しない場合は登録を拒否する
（安全装置。誤ったコンテキスト選択による誤登録を防ぐ）。
--registerを付けない場合は結果を表示するのみでレジストリは変更しない。

必要パッケージ: requests
"""

import os
import re
import sys
import json
import time
import argparse
from typing import Optional, Dict, Any, List

import requests  # common/sec_data/fetcher.pyと同様、必須依存として直接import
                  # （本モジュールはparser.pyからも読み込まれるため、CLI専用
                  # スクリプトのようなsys.exit()を伴うガードは置かない）

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
REGISTRY_PATH = os.path.join(_MODULE_DIR, "dimension_aggregate_registry.json")

SEC_HEADERS = {
    "User-Agent": "Koichi Personal Investment Tools koichi@example.com",
    "Accept": "application/json,application/xml,text/xml,*/*",
}
SEC_BASE = "https://www.sec.gov"
THROTTLE = 0.2  # SECリクエスト間隔（秒）。xbrl_segment_fetcher.pyと同一値


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# レジストリ読み込み（common/sec_data/parser.pyの
# _load_fact_overrides()/_load_fixed_registry()と同型のロード方式）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_dimension_aggregate_registry() -> dict:
    """dimension_aggregate_registry.jsonを読み込む。
    キーは`"{accn}|{end_date}"`（ticker非依存、accnは全filing横断で一意）。
    ファイル不在時・例外時は空dict。
    """
    if os.path.exists(REGISTRY_PATH):
        try:
            with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_dimension_aggregate_registry(registry: dict) -> None:
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2, sort_keys=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# filingディレクトリ・インスタンス文書の特定
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_COMPANION_SUFFIXES = ("_cal.xml", "_def.xml", "_lab.xml", "_pre.xml")
_RENDER_FILE_RE = re.compile(r"^R\d+\.xml$", re.IGNORECASE)


def get_filing_directory_items(cik: int, accn: str) -> Optional[List[str]]:
    """filingディレクトリの全ファイル名一覧を取得する
    （`{accn}-index.json`ではなく`index.json`、SEC EDGARの標準
    ディレクトリリスティングJSON）。失敗時はNone。
    """
    url = f"{SEC_BASE}/Archives/edgar/data/{int(cik)}/{accn.replace('-', '')}/index.json"
    try:
        time.sleep(THROTTLE)
        r = requests.get(url, headers=SEC_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
        return [it["name"] for it in data.get("directory", {}).get("item", [])]
    except Exception as e:
        print(f"  ディレクトリ一覧取得エラー: {e}")
        return None


def find_instance_document(items: List[str]) -> Optional[str]:
    """ディレクトリ一覧からXBRLインスタンス文書のファイル名を特定する。

    filingの年代によりインスタンス文書の命名規則が異なる:
      - Inline XBRL（おおむね2019年以降）: `{primary_doc_base}_htm.xml`
      - 従来型XBRL（それ以前）: `{ticker}-{period}.xml`
        （例: `v-20090930.xml`、`pltr-20201231.xml`、`npac-20191231.xml`）
    いずれの命名規則でも共通して除外すべき`.xml`ファイル
    （計算/定義/ラベル/表示linkbase・レンダリング用R{n}.xml・
    FilingSummary.xml等）を除外した残りから候補を選ぶ方式にすることで、
    年代に依存しない単一のロジックで両方の命名規則に対応できることを
    実データ5件（ASTS 2020・V 2009・PLTR 2021・CART 2024・CELH 2026の
    各filing）で確認済み。
    """
    candidates = []
    for name in items:
        if not name.lower().endswith(".xml"):
            continue
        lower = name.lower()
        if lower.endswith(_COMPANION_SUFFIXES):
            continue
        if lower in ("filingsummary.xml", "defnref.xml"):
            continue
        if _RENDER_FILE_RE.match(name):
            continue
        candidates.append(name)
    if not candidates:
        return None
    # 複数残った場合はhtm.xml（inline XBRL）を優先
    for c in candidates:
        if c.lower().endswith("_htm.xml"):
            return c
    return candidates[0]


def download_instance_xml(cik: int, accn: str, filename: str) -> Optional[str]:
    url = f"{SEC_BASE}/Archives/edgar/data/{int(cik)}/{accn.replace('-', '')}/{filename}"
    try:
        time.sleep(THROTTLE)
        r = requests.get(url, headers=SEC_HEADERS, timeout=90)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code}: {filename}")
            return None
        return r.text
    except Exception as e:
        print(f"  ダウンロード例外: {e}")
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# XBRLコンテキスト・ファクト解析
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_CTX_RE = re.compile(
    r'<(?:\w+:)?context\b[^>]*?id=["\']([^"\']+)["\'][^>]*?>(.*?)</(?:\w+:)?context>',
    re.DOTALL | re.IGNORECASE,
)
_INSTANT_RE = re.compile(r'<(?:\w+:)?instant[^>]*>([\d\-]+)<', re.IGNORECASE)
_EXPLICIT_MEMBER_RE = re.compile(
    r'<[^>]*?explicitMember[^>]*?dimension=["\']([^"\']+)["\'][^>]*?>\s*(.+?)\s*</[^>]*?explicitMember>',
    re.DOTALL | re.IGNORECASE,
)


def _parse_contexts(xml_text: str) -> Dict[str, Dict[str, Any]]:
    """全コンテキストを解析する。instant fact（BS項目）のみ対象とし、
    duration（start/end）コンテキストは対象外（BS恒等式チェックは
    instant factのみを扱うため、`xbrl_segment_fetcher.py`のような
    duration対応は不要）。

    Returns: {ctx_id: {"instant": "2019-12-31", "members": [(axis, member), ...]}}
    """
    result: Dict[str, Dict[str, Any]] = {}
    for m in _CTX_RE.finditer(xml_text):
        ctx_id = m.group(1)
        body = m.group(2)
        inst_m = _INSTANT_RE.search(body)
        if not inst_m:
            continue  # instant以外（duration）は対象外
        members = [
            (mm.group(1).strip(), mm.group(2).strip())
            for mm in _EXPLICIT_MEMBER_RE.finditer(body)
        ]
        result[ctx_id] = {"instant": inst_m.group(1), "members": members}
    return result


def _extract_fact_values(xml_text: str, concept_local: str, ctx_ids: set) -> Dict[str, float]:
    """指定コンテキストID集合について、concept_local（namespace prefix
    非依存）の数値ファクトを抽出する。戻り値は{ctx_id: value}。
    """
    fact_re = re.compile(
        r'<(?:[\w-]+:)?' + re.escape(concept_local)
        + r'\b[^>]*?contextRef=["\']([^"\']+)["\'][^>]*?>\s*(-?[\d.]+)\s*<',
        re.IGNORECASE,
    )
    result: Dict[str, float] = {}
    for m in fact_re.finditer(xml_text):
        cref = m.group(1)
        if cref not in ctx_ids:
            continue
        try:
            result[cref] = float(m.group(2))
        except ValueError:
            continue
    return result


def extract_dimension_aggregate(
    xml_text: str,
    concept_local: str,
    period_end: str,
    axis_local: Optional[str] = None,
) -> Dict[str, Any]:
    """生XBRLインスタンステキストから、指定コンセプトの値を
    (a) axis_local指定時: そのaxisのみを唯一の次元として持つ
        instant=period_endのコンテキスト全てについて値を合算する
        （他の軸も併せ持つコンテキスト——Statement of Stockholders'
        Equityロールフォワード表等での多重タグ付け——は二重計上防止の
        ため除外する。CELH実データで、単一軸コンテキストと2軸
        コンテキストの両方に同一金額が別々にタグ付けされている実例を
        確認済み、この制約により正しく前者のみを採用する）
    (b) axis_local未指定時: いかなる次元も持たない
        instant=period_endの単一コンテキストの値を採用する
        （カスタム名前空間タグの場合、次元分解ではなく単に
        company_facts.json一括APIの対象namespace集合から漏れている
        だけのケース。V・ASTSがこれに該当）

    戻り値: {"total": float, "components": {member_or_ctx_id: value},
             "matched_contexts": [ctx_id, ...]}
    値が1件も見つからない場合は{"total": None, "components": {}, ...}。
    """
    contexts = _parse_contexts(xml_text)

    matched_ctx_ids: List[str] = []
    labels: Dict[str, str] = {}
    for ctx_id, info in contexts.items():
        if info["instant"] != period_end:
            continue
        members = info["members"]
        if axis_local is None:
            if members:
                continue  # 次元付きは対象外（無次元コンテキストのみ）
            matched_ctx_ids.append(ctx_id)
            labels[ctx_id] = "base"
        else:
            if len(members) != 1:
                continue  # 単一軸のみのコンテキスト限定（二重計上防止）
            axis_full, member = members[0]
            if axis_full.split(":")[-1] != axis_local:
                continue
            matched_ctx_ids.append(ctx_id)
            labels[ctx_id] = member

    if not matched_ctx_ids:
        return {"total": None, "components": {}, "matched_contexts": []}

    values = _extract_fact_values(xml_text, concept_local, set(matched_ctx_ids))
    components = {labels[cid]: val for cid, val in values.items()}
    if not components:
        return {"total": None, "components": {}, "matched_contexts": []}

    return {
        "total": sum(components.values()),
        "components": components,
        "matched_contexts": sorted(values.keys()),
    }


def fetch_dimension_aggregate(
    cik: int,
    accn: str,
    concept_local: str,
    period_end: str,
    axis_local: Optional[str] = None,
    instance_filename: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """1件のfilingについてfetch→parse→合算の全工程を実行する。

    ネットワークエラー・パース失敗・該当ファクトなし、いずれの場合も
    例外を送出せずNoneを返す（呼び出し側は既存の「対応不可」記録へ
    安全にフォールバックできる）。
    """
    try:
        if instance_filename is None:
            items = get_filing_directory_items(cik, accn)
            if items is None:
                return None
            instance_filename = find_instance_document(items)
            if instance_filename is None:
                print("  インスタンス文書のファイル名を特定できませんでした")
                return None
        xml_text = download_instance_xml(cik, accn, instance_filename)
        if xml_text is None:
            return None
        result = extract_dimension_aggregate(xml_text, concept_local, period_end, axis_local)
        if result["total"] is None:
            return None
        result["instance_filename"] = instance_filename
        return result
    except Exception as e:
        print(f"  fetch_dimension_aggregate例外（安全にNoneへフォールバック）: {e}")
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLIエントリーポイント（手動検証・登録専用）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    parser = argparse.ArgumentParser(
        description="生XBRLインスタンスから次元分解値・カスタム名前空間値を"
                     "取得・検証・登録する（BS恒等式チェック専用、手動実行ツール）"
    )
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--cik", required=True, type=int)
    parser.add_argument("--accn", required=True)
    parser.add_argument("--concept", required=True, help="コンセプトのローカル名（namespace prefixなし）")
    parser.add_argument("--period", required=True, help="instant日付、例: 2019-12-31")
    parser.add_argument("--axis", default=None, help="次元軸のローカル名（省略時は無次元コンテキストを対象）")
    parser.add_argument("--field", required=True, help="BS恒等式チェックの対象フィールド名（記録用ラベル）")
    parser.add_argument("--citation", required=True, help="一次情報の引用（10-K accn・R-file・区分名等）")
    parser.add_argument("--expect", type=float, default=None, help="期待値。指定時、一致しなければ登録を拒否する")
    parser.add_argument("--register", action="store_true", help="レジストリへ書き込む（省略時は表示のみ）")
    args = parser.parse_args()

    result = fetch_dimension_aggregate(
        cik=args.cik, accn=args.accn, concept_local=args.concept,
        period_end=args.period, axis_local=args.axis,
    )
    if result is None:
        print(f"[{args.ticker}] 取得失敗（生XBRL取得不可、またはコンセプト・期間に合致するファクトなし）")
        sys.exit(1)

    print(f"[{args.ticker}] {args.concept} @ {args.period} (axis={args.axis})")
    print(f"  instance: {result['instance_filename']}")
    print(f"  components: {result['components']}")
    print(f"  total: {result['total']:,.0f}")

    if args.expect is not None:
        diff = abs(result["total"] - args.expect)
        if diff > 1.0:
            print(f"  ✗ 期待値 {args.expect:,.0f} と不一致（差分 {diff:,.0f}）。登録を拒否します。")
            sys.exit(1)
        print(f"  ✓ 期待値 {args.expect:,.0f} と一致（差分 {diff:,.0f}）")

    if args.register:
        registry = load_dimension_aggregate_registry()
        key = f"{args.accn}|{args.period}"
        registry[key] = {
            "ticker": args.ticker,
            "cik": args.cik,
            "field": args.field,
            "concept": args.concept,
            "axis": args.axis,
            "computed_value": result["total"],
            "components": result["components"],
            "instance_filename": result["instance_filename"],
            "source_citation": args.citation,
            "verification_command": " ".join(
                [
                    "python common/sec_data/dimension_aggregate_fetcher.py",
                    f"--ticker {args.ticker} --cik {args.cik} --accn {args.accn}",
                    f"--concept {args.concept} --period {args.period}",
                ]
                + ([f"--axis {args.axis}"] if args.axis else [])
            ),
        }
        _save_dimension_aggregate_registry(registry)
        print(f"  → dimension_aggregate_registry.jsonへ登録しました（key={key}）")
    else:
        print("  （--registerを付けていないため、レジストリへの書き込みはしていません）")


if __name__ == "__main__":
    main()
