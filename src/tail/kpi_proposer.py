#!/usr/bin/env python3
"""
TANUKI TAIL — kpi_proposer.py

投資テーゼとエグジット条件をGrokに渡し、
四半期監視KPIの候補を提案して保存する（Step 0）。

使用方法:
    python src/tail/kpi_proposer.py --ticker PLTR
    python src/tail/kpi_proposer.py --ticker PLTR SOFI TSLA
    python src/tail/kpi_proposer.py --ticker PLTR --dry-run

出力:
    docs/portfolio/tail/data/kpi_proposals/{ticker}_proposal.json
    config/tail_kpi_map.json（auto_fetchable=true 分を自動追記、2026-08-15
    docs/portfolio/tail/data/から移動）
環境変数:
    XAI_API_KEY  xAI Grok API キー（必須）
"""

import os
import sys
import json
import re
import time
import argparse
import requests
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List

# ── パス設定 ──────────────────────────────────────────────────
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root  = os.path.abspath(os.path.join(script_dir, "..", ".."))

if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
from src.tail.thesis_utils import thesis_narrative_fields  # noqa: E402
from src.tail.xbrl_segment_fetcher import (  # noqa: E402
    get_10q_filings, download_xbrl, extract_segment_members, parse_and_extract,
)
from common.sec_data.fetcher import load_company_facts  # noqa: E402
from common.sec_data.layer3_builder import build_ticker_store, get_latest_quarterly  # noqa: E402
from common.sec_data import tickers as _tickers_mod  # noqa: E402

SEC_CONCEPT_DEFINITIONS_PATH = os.path.join(repo_root, "config", "sec_concept_definitions.json")

# xbrl_tag（us-gaap概念）の実在確認に使う「直近」の基準（日数）。
# 730日（約2年）より古い提出にしか出現しない概念は対象外とする
# （2026-08-19⑦、[[TAIL-XBRL-MEMBER-VALIDATION-GAP-1]]対応）。
_RECENT_CONCEPT_WINDOW_DAYS = 730

# 実XBRLタグ抽出で「セグメント区分」とみなすディメンションのローカル名
# キーワード（大文字小文字区別なし）。extract_segment_members()が返す
# 全ディメンション（持分・公正価値階層等の無関係な軸を含む75件規模）
# から、セグメント・製品・地域区分に絞り込むための簡易フィルタ
# （2026-08-19⑥、[[TAIL-XBRL-MEMBER-VALIDATION-GAP-1]]対応）。
_SEGMENT_DIMENSION_KEYWORDS = ("segment", "productorservice", "geographical", "geographic")

DATA_DIR          = os.path.join(repo_root, "docs", "portfolio", "tail", "data")
POSITIONS_DIR     = os.path.join(DATA_DIR, "positions")
KPI_PROPOSALS_DIR = os.path.join(DATA_DIR, "kpi_proposals")
# tail_kpi_map.jsonはPythonバックエンド専用の手動設定ファイルのため
# config/配下に配置（TAILKPI-CONFIG-LOCATION-1、2026-08-15移動）
KPI_MAP_PATH      = os.path.join(repo_root, "config", "tail_kpi_map.json")

JST = ZoneInfo("Asia/Tokyo")

# ── Grok API 設定 ──────────────────────────────────────────────
XAI_API_KEY = os.getenv("XAI_API_KEY", "")
GROK_URL    = "https://api.x.ai/v1/chat/completions"
# [[GROK-MODEL-PRICE-1]]対応: grok-3-mini/grok-3は実際にはgrok-4.3へ
# 自動ルーティングされ、grok-2-1212は廃止済み（API側でModel not found）。
# 実態に合わせ、実際に応答する現行モデル名を明示する（フォールバック
# ループ構造自体は一時的なネットワーク障害時の再試行として維持）。
GROK_MODELS = ["grok-4.3", "grok-4.3", "grok-4.3"]

KPI_SYSTEM = (
    "あなたは投資テーゼの監視指標設計の専門家です。"
    "テーゼとエグジット条件から、四半期ごとに確認すべき具体的なKPIを提案してください。"
    "auto_fetchable=trueのKPIには必ずxbrl_tag・xbrl_dimension・xbrl_memberを記入してください。"
    "xbrl_tagはus-gaap名前空間またはカスタム名前空間で記述してください。"
    "回答はすべて日本語で記述してください。"
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Grok API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def call_grok(user_prompt: str, system_prompt: str = "", max_tokens: int = 3000, temperature: float = 0.4) -> str:
    if not XAI_API_KEY:
        raise RuntimeError("XAI_API_KEY が設定されていません")

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {XAI_API_KEY}"}
    messages: List[Dict[str, str]] = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user_prompt})

    last_error: Optional[Exception] = None
    for model in GROK_MODELS:
        try:
            print(f"  [Grok] モデル試行: {model}")
            resp = requests.post(
                GROK_URL, headers=headers,
                json={"model": model, "messages": messages, "max_tokens": max_tokens, "temperature": temperature},
                timeout=120,
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]
            print(f"  [Grok] 成功: {model}")
            return text
        except Exception as e:
            print(f"  [Grok] 失敗 ({model}): {e}")
            last_error = e
            time.sleep(1)
    raise RuntimeError(f"すべてのGrokモデルで失敗: {last_error}")


def extract_json_from_response(text: str) -> Dict[str, Any]:
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        return json.loads(m.group(1))
    start = text.find("{")
    end   = text.rfind("}") + 1
    if start >= 0 and end > start:
        return json.loads(text[start:end])
    raise ValueError(f"JSONが見つかりません: {text[:300]}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# データ読み込み
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_thesis(ticker: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(POSITIONS_DIR, f"{ticker}_thesis.json")
    if not os.path.exists(path):
        print(f"  [WARN] thesis.json 未発見: {path}")
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_kpi_map() -> Dict[str, Any]:
    if not os.path.exists(KPI_MAP_PATH):
        return {}
    with open(KPI_MAP_PATH, encoding="utf-8") as f:
        return json.load(f)


def fetch_real_segment_tags(ticker: str, cik: Optional[str]) -> List[Dict[str, str]]:
    """直近10-QのXBRLから、実際に使用されているセグメント関連タグ
    （dimension, member の組）を取得する。

    `xbrl_segment_fetcher.py::extract_segment_members()`（parse_
    contexts()の抽出ロジックを再利用した既存関数）をそのまま呼ぶ。
    取得したdimension/member全件（equity・fair value階層等の無関係な
    軸を含む）のうち、`_SEGMENT_DIMENSION_KEYWORDS`に一致するものだけ
    を返す（2026-08-19⑥、[[TAIL-XBRL-MEMBER-VALIDATION-GAP-1]]対応:
    Grokにタグ名を記憶から生成させず、実際に提出書類に存在するタグから
    選ばせるための土台）。

    **キーワード一致は暫定的な手法である**（2026-08-19⑦追記）。
    実際の企業が使うディメンション軸名は多様であり、
    `_SEGMENT_DIMENSION_KEYWORDS`の4語（segment/productorservice/
    geographical/geographic）で拾いきれない命名（業界固有の軸名等）が
    存在する可能性がある。取りこぼしを検知できるよう、絞り込みの前後
    件数と対象外にした軸の一覧を必ず表示する（本セッションで繰り返し
    否定してきた沈黙除外と同型にしないため）。

    取得できない場合（CIK不明・10-Q取得失敗等）は空リストを返す。
    呼び出し側はこれを「セグメント指標を提案しない」の判断材料とする。
    """
    if not cik:
        return []
    try:
        filings = get_10q_filings(cik, n=1)
        if not filings:
            return []
        f = filings[0]
        xml_text = download_xbrl(cik, f["accn"], f["xml"])
        if not xml_text:
            return []
        all_members = extract_segment_members(xml_text)
    except Exception as e:
        print(f"  [WARN] 実XBRLタグ取得失敗: {e}")
        return []

    kept    = [m for m in all_members if any(kw in m["dimension"].lower() for kw in _SEGMENT_DIMENSION_KEYWORDS)]
    dropped = [m for m in all_members if m not in kept]

    print(f"  セグメント軸: {len(kept)}件を抽出（全{len(all_members)}軸中、{len(dropped)}軸を対象外）")
    if dropped:
        dropped_dims = sorted({m["dimension"] for m in dropped})
        print(f"    対象外にした軸: {', '.join(dropped_dims)}")

    return kept


def fetch_real_us_gaap_concepts(ticker: str) -> List[str]:
    """`company_facts.json`（ローカル既存ファイル、新規取得なし）から、
    この企業が実際に使用しているus-gaap概念名の一覧を返す。

    直近`_RECENT_CONCEPT_WINDOW_DAYS`（730日≒2年）以内に`filed`された
    エントリを持つ概念のみに絞り込む（626件規模になりうる全概念を
    そのままプロンプトへ埋め込むのは非現実的なため）。絞り込みの
    前後件数を必ず表示する（沈黙除外にしないため、2026-08-19⑦、
    [[TAIL-XBRL-MEMBER-VALIDATION-GAP-1]]対応）。

    company_facts.jsonが存在しない場合は空リストを返す。
    """
    company_facts = load_company_facts(ticker)
    if not company_facts:
        return []

    all_concepts = company_facts.get("facts", {}).get("us-gaap", {})
    cutoff = (date.today() - timedelta(days=_RECENT_CONCEPT_WINDOW_DAYS)).isoformat()

    recent: List[str] = []
    for name, cdata in all_concepts.items():
        found = False
        for entries in cdata.get("units", {}).values():
            for e in entries:
                if e.get("filed", "") >= cutoff:
                    found = True
                    break
            if found:
                break
        if found:
            recent.append(name)

    recent.sort()
    print(
        f"  us-gaap概念: {len(recent)}件を抽出"
        f"（全{len(all_concepts)}件中、直近{_RECENT_CONCEPT_WINDOW_DAYS}日"
        f"〈約2年〉以内の提出に出現しない{len(all_concepts) - len(recent)}件を対象外）"
    )
    return recent


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# プロンプト構築
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_kpi_prompt(
    thesis: Dict[str, Any],
    cik: Optional[str] = None,
    existing_kpi_map: Optional[List[Dict[str, Any]]] = None,
    real_segment_tags: Optional[List[Dict[str, str]]] = None,
    real_us_gaap_concepts: Optional[List[str]] = None,
) -> str:
    ticker  = thesis.get("ticker", "")
    cik_str = cik or "不明"

    # thesisのスキーマ差（core/satellite）を吸収する。修正前は
    # thesis.get('thesis', ...)/thesis.get('exit_guide', ...)で
    # coreスキーマのフィールド名を直接読んでおり、satelliteの
    # thesisファイルに対しては常に「未設定」が返っていた
    # （quarterly_review_generator.pyで発見・修正した実バグと同型、
    # [[TAIL-KPI-PROPOSER-CORE-ONLY-GATE-1]]対応、2026-08-19④）。
    thesis_text, _entry_story, exit_guide_text = thesis_narrative_fields(thesis)

    existing_section = ""
    if existing_kpi_map:
        names = [e.get("kpi_name", "") for e in existing_kpi_map if e.get("kpi_name")]
        if names:
            existing_section = "\n## 既存の自動取得KPI（tail_kpi_map登録済み）\n" + "\n".join(f"- {n}" for n in names) + "\n"

    # 実XBRLタグ一覧セクション（2026-08-19⑥、[[TAIL-XBRL-MEMBER-
    # VALIDATION-GAP-1]]対応）。修正前はxbrl_memberを"{ticker}:XxxMember"
    # 形式で記憶から生成させており、実測で一致率32%（今回一括生成分は
    # 14%）という低さだった。実際の直近10-QのXBRLから抽出した
    # dimension/memberの組を提示し、その中からのみ選ばせることで、
    # 「Grokが正しいタグ名を知っている」という検証していない前提への
    # 依存をなくす。
    if real_segment_tags:
        tag_lines = "\n".join(
            f"- dimension={t['dimension']}, member={t['member']}"
            for t in real_segment_tags
        )
        segment_tags_section = f"""
## この企業が実際に使用しているセグメント関連タグ（直近10-Qより抽出）
{tag_lines}

**セグメント指標（特定事業区分・製品・地域別の指標）を提案する場合は、
必ず上記一覧の中からdimension・memberの組を選んでください。
一覧に無い名称を記憶や推測で生成しないこと。** 該当するものが一覧に
無い場合は、その指標をセグメント指標として提案せず、
`xbrl_dimension: null, xbrl_member: null`としてください（無理に
セグメント区分をでっち上げるより、全社ベースの指標として提案する方が
望ましい）。
"""
    else:
        segment_tags_section = """
## セグメント関連タグの実データ
取得できませんでした。**この場合、セグメント指標（xbrl_dimension・
xbrl_memberを伴う指標）は提案しないでください。** 全社ベースの指標
（xbrl_dimension: null, xbrl_member: null）のみを提案してください。
"""

    # 実us-gaap概念一覧セクション（2026-08-19⑦、[[TAIL-XBRL-MEMBER-
    # VALIDATION-GAP-1]]対応）。dimension/memberの精度は改善したが
    # xbrl_tag（概念そのもの）が実在しないタグとして提案される問題が
    # 別途判明したため、company_facts.jsonから実際に使用されている
    # 概念一覧も提示する。**ただしこれだけでは不十分**——タグ・member
    # 双方が個別に実在しても、その組み合わせのファクトが存在するとは
    # 限らないため、登録前に本番の取得経路で実際に値が取れるかを
    # 別途検証する（update_tail_kpi_map()側、事例5の原則）。
    if real_us_gaap_concepts:
        concepts_text = ", ".join(real_us_gaap_concepts)
        concepts_section = f"""
## この企業が実際に使用しているus-gaap概念一覧（company_facts.jsonより抽出、直近2年）
{concepts_text}

**`xbrl_tag`は必ず上記一覧の中（`us-gaap:`を付けた形）から選んで
ください。一覧に無い概念名を記憶や推測で生成しないこと。**
"""
    else:
        concepts_section = """
## us-gaap概念一覧の実データ
取得できませんでした。標準的によく使われる概念（Revenues・
OperatingIncomeLoss・NetIncomeLoss等）から選んでください。
"""

    return f"""## 銘柄: {ticker}
## CIK: {cik_str}
## 投資テーゼ
{thesis_text}

## エグジット条件
{exit_guide_text}
{existing_section}{segment_tags_section}{concepts_section}
## 以下のJSON形式でKPIを5〜8個提案してください:
{{
  "proposed_kpis": [
    {{
      "name": "NDR（ネット・ダラー・リテンション）",
      "description": "既存顧客からの売上維持・拡大率",
      "source": "決算資料（IR開示）",
      "warning_threshold": "120%以下",
      "exit_threshold": "110%未満が2四半期連続",
      "related_exit_condition": "エグジット条件①",
      "auto_fetchable": false,
      "extraction_hint": "net dollar retention",
      "xbrl_tag": null,
      "xbrl_dimension": null,
      "xbrl_member": null,
      "layer2_name": null
    }},
    {{
      "name": "米民間売上成長率",
      "description": "US Commercial売上の前年同期比成長率",
      "source": "EDGAR XBRL",
      "warning_threshold": "40%以下",
      "exit_threshold": "30%未満が2四半期連続",
      "related_exit_condition": "エグジット条件②",
      "auto_fetchable": true,
      "extraction_hint": null,
      "xbrl_tag": "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
      "xbrl_dimension": "us-gaap:StatementBusinessSegmentsAxis",
      "xbrl_member": "pltr:CommercialOperatingSegmentMember",
      "layer2_name": "Commercial売上"
    }}
  ]
}}

ルール:
- auto_fetchable=true: EDGARのXBRLから取得可能な財務指標（売上・利益・残高等）
  → xbrl_tagは標準的なus-gaap概念名で記入（会社全体の指標は
    xbrl_dimension: null, xbrl_member: nullでよい）
  → セグメント指標の場合のみ、xbrl_dimension・xbrl_memberを上記
    「実際に使用しているセグメント関連タグ」一覧から選んで記入する
    （一覧に無い名称は使わないこと）
  → 上記「既存の自動取得KPI」のいずれかと同じXBRLデータを参照する場合、layer2_nameにその既存KPI名を記入（例: "layer2_name": "Commercial売上"）
- auto_fetchable=false: NDR・解約率・顧客数・規制状況等の非財務指標
  → extraction_hintに決算資料でよく使われる英語表現を記入
  → xbrl_tag/dimension/memberはnull、layer2_nameはnull
- JSONのみ返してください（説明文不要）"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# tail_kpi_map.json への自動追記
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def post_process_proposals(ticker: str, kpis: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Grok提案にlayer2_nameが未設定の場合、xbrl_tag+memberが既存kpi_mapと一致すればセットする。"""
    kpi_map_entries = load_kpi_map().get(ticker, [])
    # (revenue_tag, member_tag) → kpi_name
    tag_to_name: Dict[tuple, str] = {}
    for e in kpi_map_entries:
        r_tag = e.get("revenue_tag", "")
        member = e.get("tag_history", [{}])[0].get("tag", "")
        if r_tag:
            tag_to_name[(r_tag, member)] = e.get("kpi_name", "")

    for kpi in kpis:
        if not kpi.get("auto_fetchable"):
            continue
        if kpi.get("layer2_name"):
            continue
        key = (kpi.get("xbrl_tag", ""), kpi.get("xbrl_member", "") or "")
        if key in tag_to_name:
            existing_name = tag_to_name[key]
            if existing_name != kpi.get("name"):
                kpi["layer2_name"] = existing_name
                print(f"    [post_process] layer2_name セット: {kpi['name']} → {existing_name}")
    return kpis


def validate_kpis_fetchable(
    cik: Optional[str],
    kpis: List[Dict[str, Any]],
    real_segment_tags: Optional[List[Dict[str, str]]] = None,
) -> tuple:
    """auto_fetchable=trueの提案KPIについて、**本番の取得経路
    （`xbrl_segment_fetcher.py::parse_and_extract()`をそのまま呼ぶ、
    検証用の簡易版は書き起こさない）で直近1四半期分のXBRLに対して
    実際に値が取れるかを検証する**（2026-08-19⑦、事例5の原則を
    KPI登録に適用、[[TAIL-XBRL-MEMBER-VALIDATION-GAP-1]]対応）。

    実タグ一覧の提示だけでは、dimension/memberの一致率が100%に
    改善しても実際の値取得は0件のまま——という実測結果を受けた
    設計変更。タグ・memberが個別に実在してもその組み合わせのファクトが
    存在するとは限らないため、個別の存在照合ではなく本番の抽出関数
    そのものを1四半期分だけ実行して判定する。

    Returns: (passed_kpis, rejected) のタプル。
      passed_kpis: 値が取得できたKPI（元のdict）のリスト
      rejected: [{"kpi": <元のdict>, "reason": <却下理由>}] のリスト
        reasonは以下のいずれか:
        - "検証不能（10-Q取得失敗）" / "検証不能（XBRL取得失敗）"
        - "非セグメント指標（構造的制約、xbrl_segment_fetcher.pyは
          非ディメンション事実を取得できない、
          [[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-GAP-1]]）"
        - "member不在（実XBRLに存在しないdimension/member）"
        - "tag不在（実XBRLに存在しない概念）"
        - "組み合わせ不在（tag・memberは個別に実在するが該当ファクト
          なし）"
    """
    auto_kpis = [k for k in kpis if k.get("auto_fetchable") and k.get("xbrl_tag")]
    if not auto_kpis:
        return [], []
    if not cik:
        return [], [{"kpi": k, "reason": "検証不能（CIK不明）"} for k in auto_kpis]

    filings = get_10q_filings(cik, n=1)
    if not filings:
        return [], [{"kpi": k, "reason": "検証不能（10-Q取得失敗）"} for k in auto_kpis]
    f = filings[0]
    period = f["period"]
    xml_text = download_xbrl(cik, f["accn"], f["xml"])
    if not xml_text:
        return [], [{"kpi": k, "reason": "検証不能（XBRL取得失敗）"} for k in auto_kpis]

    valid_dim_members = {(t["dimension"], t["member"]) for t in (real_segment_tags or [])}

    kpi_list_for_extract = [
        {
            "kpi_name":     k["name"],
            "dimension":    k.get("xbrl_dimension") or "",
            "revenue_tag":  k.get("xbrl_tag"),
            "fallback_tags": [],
            "tag_history":  [{"tag": k.get("xbrl_member") or "", "valid_from": "2010-01-01", "valid_to": None}],
        }
        for k in auto_kpis
    ]
    results = parse_and_extract(xml_text, kpi_list_for_extract, period)

    passed: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    for k in auto_kpis:
        if results.get(k["name"]) is not None:
            passed.append(k)
            continue

        xbrl_dim = k.get("xbrl_dimension")
        xbrl_mem = k.get("xbrl_member")
        if not xbrl_dim or not xbrl_mem:
            reason = (
                "非セグメント指標（構造的制約、xbrl_segment_fetcher.pyは"
                "非ディメンション事実を取得できない、"
                "[[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-GAP-1]]）"
            )
        elif (xbrl_dim, xbrl_mem) not in valid_dim_members:
            reason = "member不在（実XBRLに存在しないdimension/member）"
        else:
            tag_local = (k.get("xbrl_tag") or "").split(":")[-1]
            tag_exists = bool(tag_local) and re.search(
                r'<[\w-]*:?' + re.escape(tag_local) + r'\b', xml_text
            )
            reason = (
                "組み合わせ不在（tag・memberは個別に実在するが該当ファクトなし）"
                if tag_exists else
                "tag不在（実XBRLに存在しない概念）"
            )
        rejected.append({"kpi": k, "reason": reason})

    return passed, rejected


def _load_layer3_tag_to_field_index() -> Dict[str, str]:
    """`config/sec_concept_definitions.json`の`fields[*].candidates`
    （Layer3が各フィールドの値を拾う際に実際に参照するXBRLタグ一覧、
    Layer3自身の唯一の正）から、タグのローカル名（`us-gaap:`等の
    名前空間プレフィックスを除いた部分）→Layer3フィールド名の逆引き
    辞書を構築する。

    **機械的な照合のためのデータソースとして本ファイルを使う理由**:
    タグ→フィールドの対応は、Layer3ビルダー自身が実際に使っている
    設定であり、これ以上正確な対応表は存在しない。新たに独自の
    エイリアス表を作らず、既存の単一の正を再利用することで、Grokに
    「これはLayer3から取れる」と判断させる（＝今日tag名で失敗した
    のと同型のリスク）ことを避ける（2026-08-19⑧、[[TAIL-XBRL-SEGMENT-
    FETCHER-NONDIMENSIONED-GAP-1]]対応）。
    """
    try:
        with open(SEC_CONCEPT_DEFINITIONS_PATH, encoding="utf-8") as f:
            defs = json.load(f)
    except Exception:
        return {}

    index: Dict[str, str] = {}
    for field_name, field_def in defs.get("fields", {}).items():
        for tag in field_def.get("candidates", []):
            tag_local = tag.split(":")[-1]
            # 複数フィールドが同じタグ名を候補にしている場合は先勝ち
            # （sec_concept_definitions.json内での定義順を尊重する）
            index.setdefault(tag_local, field_name)
    return index


def route_rejected_to_layer3(
    ticker: str,
    rejected: List[Dict[str, Any]],
) -> tuple:
    """却下されたKPIについて、`xbrl_tag`のローカル名がLayer3の
    32フィールドの候補タグと機械的に一致するかを照合し、一致すれば
    Layer3経由（`source: "layer3"`）で登録可能なKPIとして振り分ける。

    **一致するだけでは登録しない**——`build_ticker_store()`を実際に
    呼び、対象ティッカーにそのLayer3フィールドの実データが存在するか
    （`get_latest_quarterly()`が非Noneを返すか）も確認する（事例5の
    原則、名前が一致しても実データがあるとは限らないため）。

    **`xbrl_dimension`が設定されたKPI（セグメント/製品/地域別等の
    区分指標として提案されたもの）はLayer3へ振り替えない**
    （2026-08-20、[[TAIL-LAYER3-ROUTING-DIMENSION-BLIND-1]]対応）。
    Layer3の32フィールドはいずれも会社全体（非ディメンション）の
    集計値であり、`xbrl_tag`のローカル名だけで照合すると、区分別の
    指標として提案されたKPIに会社全体の値が誤って紐付いてしまう
    （実例: APP「継続営業利益」〈元`dimension=us-gaap:
    StatementOperatingActivitiesSegmentAxis`〉→ 会社全体の
    `net_income`、CELH「機能性エナジードリンク売上」〈元
    `dimension=srt:ProductOrServiceAxis`〉→ 会社全体の`revenue`。
    いずれも取得された値自体は正しいが、KPI名が示す意味と実際の値が
    食い違ったまま2026-08-19⑨〜2026-08-20の実装でtail_kpi_map.jsonへ
    登録されていた）。

    Returns: (layer3_matched, still_rejected) のタプル。
      layer3_matched: [{**元のkpi, "source": "layer3",
        "layer3_field": <field>}, ...]
      still_rejected: Layer3にも一致しなかった、一致したが実データが
        無かった、またはdimension付きのため対象外とした却下KPI（元の
        `{"kpi":..., "reason":...}`形式、reasonを更新）
    """
    tag_index = _load_layer3_tag_to_field_index()
    store = build_ticker_store(ticker)

    layer3_matched: List[Dict[str, Any]] = []
    still_rejected: List[Dict[str, Any]] = []

    for r in rejected:
        k = r["kpi"]

        if k.get("xbrl_dimension"):
            print(f"    [Layer3振替対象外] {k.get('name', '?')}: "
                  f"セグメント指標のため（dimension={k.get('xbrl_dimension')}）")
            still_rejected.append({
                "kpi": k,
                "reason": f"{r['reason']}／セグメント指標のためLayer3"
                          f"（会社全体集計）へ振替不可",
            })
            continue

        tag_local = (k.get("xbrl_tag") or "").split(":")[-1]
        field = tag_index.get(tag_local)

        if not field:
            still_rejected.append({
                "kpi": k,
                "reason": f"{r['reason']}／Layer3にも一致するフィールドなし",
            })
            continue

        if store is None:
            still_rejected.append({
                "kpi": k,
                "reason": f"{r['reason']}／Layer3ストア取得不可のため未検証",
            })
            continue

        latest = get_latest_quarterly(store, field)
        if latest is None or latest.get("val") is None:
            still_rejected.append({
                "kpi": k,
                "reason": f"{r['reason']}／Layer3フィールド'{field}'に一致したが実データなし",
            })
            continue

        new_kpi = dict(k)
        new_kpi["source"]       = "layer3"
        new_kpi["layer3_field"] = field
        layer3_matched.append(new_kpi)
        print(f"    [Layer3振替] {k.get('name', '?')} → layer3_field={field}"
              f"（実データ確認済み: {latest['end']}時点 {latest['val']:,}）")

    return layer3_matched, still_rejected


def update_tail_kpi_map(
    ticker: str,
    proposed_kpis: List[Dict[str, Any]],
) -> int:
    """検証済み（`validate_kpis_fetchable()`を通過済み）のKPIを
    tail_kpi_map.jsonへ登録する。呼び出し元が実取得検証を済ませて
    いる前提のため、本関数側での追加照合は行わない（2026-08-19⑦、
    dimension/member単体照合〈2026-08-19⑥実装〉から実取得検証方式へ
    切り替え）。

    **全KPIが却下され`proposed_kpis`が空の場合も、`kpi_map[ticker] = []`
    を明示的に書き込む**（キー自体を作らないと「未処理」なのか「0件」
    なのか区別できないため、2026-08-19⑦、Step 2-3対応）。
    """
    if os.path.exists(KPI_MAP_PATH):
        with open(KPI_MAP_PATH, encoding="utf-8") as f:
            kpi_map: Dict[str, Any] = json.load(f)
    else:
        kpi_map = {}

    existing = kpi_map.get(ticker, [])
    existing_names = {e.get("kpi_name", "") for e in existing}
    # (revenue_tag, member_tag) で重複チェック（名前が違っても同一XBRLデータは追加しない）
    existing_tag_members = {
        (e.get("revenue_tag", ""), e.get("tag_history", [{}])[0].get("tag", ""))
        for e in existing
    }

    added = 0
    for kpi in proposed_kpis:
        kpi_name = kpi.get("name", "")
        if kpi_name in existing_names:
            continue

        # source=="layer3"のKPIはXBRL直接取得と異なるスキーマで登録する
        # （2026-08-19⑧、[[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-
        # GAP-1]]対応）。既存エントリはsourceキー無し＝従来通りXBRL直接
        # 取得のままで後方互換を保つ。
        if kpi.get("source") == "layer3":
            layer3_field = kpi.get("layer3_field")
            if not layer3_field:
                continue
            existing.append({
                "kpi_name":     kpi_name,
                "change_risk":  "medium",
                "source":       "layer3",
                "layer3_field": layer3_field,
            })
            existing_names.add(kpi_name)
            added += 1
            print(f"    [kpi_map] 追加（Layer3経由・実データ確認済み）: {kpi_name} → layer3_field={layer3_field}")
            continue

        xbrl_tag  = kpi.get("xbrl_tag")
        xbrl_dim  = kpi.get("xbrl_dimension")
        xbrl_mem  = kpi.get("xbrl_member") or ""

        if not xbrl_tag:
            continue

        tag_key = (xbrl_tag, xbrl_mem)
        if tag_key in existing_tag_members:
            print(f"    [kpi_map] スキップ（tag+member重複）: {kpi_name}")
            continue

        entry: Dict[str, Any] = {
            "kpi_name":     kpi_name,
            "change_risk":  "medium",
            "tag_history":  [{"tag": xbrl_mem, "valid_from": "2010-01-01", "valid_to": None}],
            "fallback_tags":    [],
            "fallback_action":  "alert",
            "revenue_tag":  xbrl_tag,
            "dimension":    xbrl_dim or "",
        }
        existing.append(entry)
        existing_names.add(kpi_name)
        existing_tag_members.add(tag_key)
        added += 1
        print(f"    [kpi_map] 追加（実取得検証済み）: {kpi_name} → {xbrl_tag}")

    kpi_map[ticker] = existing
    with open(KPI_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(kpi_map, f, ensure_ascii=False, indent=2)
    if added > 0:
        print(f"  ✓ tail_kpi_map.json を更新 (+{added}件)")
    else:
        print(f"  ✓ tail_kpi_map.json を更新（{ticker}: 0件、空リストとして明示）")

    return added


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 提案生成・保存
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def propose_kpis(ticker: str, dry_run: bool = False) -> Optional[str]:
    ticker = ticker.upper()
    print(f"\n{'─' * 60}")
    print(f"  {ticker} KPI提案生成")
    print(f"{'─' * 60}")

    thesis = load_thesis(ticker)
    if not thesis:
        return None
    # core限定ゲートは2026-08-19④に撤廃した。撤廃前はbuild_kpi_prompt()が
    # coreスキーマのフィールド名（thesis/exit_guide）を直接読んでおり、
    # satelliteに適用するとスキーマ不整合により全て「未設定」を提案の
    # 根拠として使ってしまう実バグが存在した。このゲートは意図的な方針
    # ではなく、そのバグを偶然マスクしていただけだったため、
    # thesis_narrative_fields()によるスキーマ吸収（上記build_kpi_prompt()
    # 参照）を実装した上で撤廃した（[[TAIL-KPI-PROPOSER-CORE-ONLY-
    # GATE-1]]対応）。

    cik    = _tickers_mod.get_cik(ticker)
    existing_kpi_map  = load_kpi_map().get(ticker, [])
    real_segment_tags = fetch_real_segment_tags(ticker, cik)
    print(f"  実XBRLセグメントタグ: {len(real_segment_tags)}件抽出")
    real_concepts = fetch_real_us_gaap_concepts(ticker)
    prompt = build_kpi_prompt(thesis, cik, existing_kpi_map, real_segment_tags, real_concepts)

    if dry_run:
        print("\n=== [DRY-RUN] KPIプロンプト ===")
        print(prompt[:800])
        print("...\n=== DRY-RUN 完了 ===")
        return None

    raw    = call_grok(user_prompt=prompt, system_prompt=KPI_SYSTEM, max_tokens=3000, temperature=0.4)
    result = extract_json_from_response(raw)

    os.makedirs(KPI_PROPOSALS_DIR, exist_ok=True)
    now_jst = datetime.now(JST)
    output: Dict[str, Any] = {
        "ticker":         ticker,
        "generated_at":   now_jst.isoformat(),
        "thesis_version": thesis.get("version", 1),
        "proposed_kpis":  result.get("proposed_kpis", []),
    }
    out_path = os.path.join(KPI_PROPOSALS_DIR, f"{ticker}_proposal.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    kpis = output["proposed_kpis"]
    # layer2_nameを自動補完（xbrl_tag+memberが既存kpi_mapと一致する場合）
    kpis = post_process_proposals(ticker, kpis)
    output["proposed_kpis"] = kpis
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  ✓ {len(kpis)} 件のKPI提案を保存: {out_path}")
    for k in kpis:
        auto = "✓自動" if k.get("auto_fetchable") else "手動"
        hint = k.get("xbrl_tag") or k.get("extraction_hint") or ""
        l2n = f" [layer2_name={k['layer2_name']}]" if k.get("layer2_name") else ""
        print(f"    [{auto}] {k.get('name', '?')} — 警戒: {k.get('warning_threshold', '?')} ({hint}){l2n}")

    # 登録前の実取得検証（2026-08-19⑦、事例5の原則をKPI登録に適用）。
    # 実タグ一覧の提示だけではdimension/member一致率100%でも実取得は
    # 0件のままだったため、本番のparse_and_extract()を直近1四半期分の
    # XBRLに対して実際に呼び、値が取れたKPIだけを登録する方式に変更。
    passed_kpis, rejected = validate_kpis_fetchable(cik, kpis, real_segment_tags)
    print(f"  実取得検証: 提案{len(kpis)}件中、自動取得対象"
          f"{len(passed_kpis) + len(rejected)}件を検証 → "
          f"通過{len(passed_kpis)}件 / 却下{len(rejected)}件")
    for r in rejected:
        k = r["kpi"]
        print(
            f"    [却下] {k.get('name', '?')} → tag={k.get('xbrl_tag')}, "
            f"dimension={k.get('xbrl_dimension')}, member={k.get('xbrl_member')}"
            f" — 理由: {r['reason']}"
        )

    # 却下KPIをLayer3経路へ機械的に振り分ける（2026-08-19⑧、
    # [[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-GAP-1]]対応）。
    # xbrl_tagのローカル名をsec_concept_definitions.jsonのcandidatesと
    # 照合するだけで、Grokに「これはLayer3から取れる」と判断させない。
    layer3_matched, still_rejected = route_rejected_to_layer3(ticker, rejected)
    if layer3_matched:
        print(f"  Layer3振替: 却下{len(rejected)}件中{len(layer3_matched)}件が"
              f"Layer3経由で取得可能と判明")
    passed_kpis = passed_kpis + layer3_matched
    rejected = still_rejected

    # 却下記録を提案ファイルに残す（黙って捨てない、Step 2-2対応）。
    # Layer3経由で解決したKPIはrejected_kpisから除外される
    # （もう却下されていないため）。
    output["rejected_kpis"] = [
        {**r["kpi"], "rejection_reason": r["reason"]} for r in rejected
    ]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # tail_kpi_map.jsonへは実取得検証を通過したKPI（XBRL直接＋Layer3
    # 経由）のみ登録する（手動KPI＝auto_fetchable=falseは元々
    # tail_kpi_map.json登録対象外）。
    update_tail_kpi_map(ticker, passed_kpis)

    return out_path


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# エントリーポイント
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    parser = argparse.ArgumentParser(description="TANUKI TAIL — KPI提案生成（Step 0）")
    parser.add_argument("--ticker", nargs="+", required=True, help="対象ティッカー（複数指定可）")
    parser.add_argument("--dry-run", action="store_true", help="Grok呼び出しをスキップしてプロンプト確認")
    args = parser.parse_args()

    now_jst = datetime.now(JST)
    print(f"TANUKI TAIL KPI提案生成 — {now_jst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 60)

    results: Dict[str, bool] = {}
    for t in args.ticker:
        try:
            path = propose_kpis(t, dry_run=args.dry_run)
            results[t.upper()] = path is not None or args.dry_run
        except Exception as e:
            print(f"  [ERROR] {t}: {e}")
            results[t.upper()] = False

    print(f"\n{'━' * 60}")
    for t, ok in results.items():
        print(f"  {'OK' if ok else 'NG'}: {t}")


if __name__ == "__main__":
    main()
