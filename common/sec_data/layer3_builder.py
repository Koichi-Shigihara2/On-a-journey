"""
common/sec_data/layer3_builder.py
責務: Layer1（company_facts.json）＋Layer2（config/sec_concept_definitions.json）
から Layer3（32フィールド統合ストア）を生成する。

フェーズA（新規構築、既存コード非改変）: 詳細は
docs/architecture/new_data_platform/SEC_EDGAR_LAYER_DESIGN.md 8章を参照。
既存の data/・normalized/・raw/・ttm/ には一切書き込まない。
出力先: common/sec_data/store_v2/{TICKER}.json（新規パス）。

設計上の再利用（意図的）:
XBRLエントリの期間分類（10-Q/10-K・is_annual/is_ytd判定、
common/sec_data/quarterly.py::_classify_period()）と、同一期間内で
複数エントリが競合した場合のfiled日タイブレーク（同::_process_entries()、
内部でcommon/sec_data/fact_selection.py::select_latest_filed()を使用）は、
過去に複数回のバグ修正（PARSER-ENTG-COMPYEAR-1・XBRL-TAG-KLAC-1・
BUG-CON-YTD-1等）を経て確定した実装のため、重複再実装によるバグ再導入
リスクを避け、そのままimportして再利用する。

本モジュールで新規実装する部分（フェーズAの本来の目的）:
- Layer2 candidatesに基づくフィールド抽出（最初に見つかった非空候補を採用）
- sign_normalize（符号正規化、CAPEX-SIGN-UNNORMALIZED-1対応）
- YTD→単四半期差分変換（start/period_daysの再計算込み。
  NORMALIZER-YTD-METADATA-STALE-1のメタデータ不整合を修正）
- Q4逆算ロジックは[[Q4-IMPLIED-CALC-TRIPLICATION-1]]対応として
  common/sec_data/q4_implied.py::build_q4_implied_entries()に集約
  済み（フェーズB、[[LAYER3-Q4-IMPLIED-NOT-MIGRATED-1]]対応で本
  モジュールもそこに合流した）。同モジュールはPascalCase/snake_case
  両方のfield_nameを受け付け、normalizer.py::Q4_IMPLIED_FIELDS∪
  ttm_calculator.py::FLOW_FIELDSの和集合（15フィールド）にのみ
  適用するガードを持つ。FY年次値からQ1+Q2+Q3を差し引く近似はフロー系
  （累積可能）フィールドでのみ有効であり、shares系（加重平均株式数）や
  stock系（残高スナップショット）に無条件適用すると符号反転等の
  無意味な値を生む（layer3_builder.py独自実装時〈フェーズA〉に発見・
  修正したバグ。詳細はq4_implied.pyのdocstring参照）
- FCF計算の共有関数化（quarterly/annual生成側とTTM集計側の両方から
  呼べる設計。parser.py・ttm_calculator.pyの既存計算式と同一であることを
  確認済み: FCF = OCF - max(0, |CapEx| - |FinanceLeasePmts|)）

未実装（フェーズAのスコープ外、既知の制限。回帰レポートで扱う）:
- normalizer.py::_build_missing_quarter_implied_entries() 相当
  （Q4以外の任意欠落四半期の逆算）
- normalizer.py::_calc_gross_profit() 相当
  （Revenue - cost_of_revenue からのGrossProfit逆算）
- quarterly.py::TICKER_RESTRICTIONS 相当の銘柄別override
  （MSFTのexcludeのみ簡易反映、他8銘柄は未移行）
- quarterly.py::_FALLBACK_MIN/_FALLBACK_MIN_FIELDS 相当の
  「件数が少なければより多いフォールバックを採用」ロジック
  （単純に「最初に見つかった非空候補を採用」のみ）
- quarterly.py::_REVENUE_FALLBACKS のような複数タグ源のマージ
  （Revenueはcandidatesの中で最初に見つかった1タグのみを採用し、
  複数タグを合算しない）
"""

from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta

from .quarterly import _classify_period, _process_entries
from .fact_selection import select_latest_filed  # noqa: F401  (contract of _process_entries)
from .q4_implied import build_q4_implied_entries

logger = logging.getLogger(__name__)

# LAYER3-FALLBACK-STALE-TAG-PRIORITY-1対応の対象外フィールド。
# rpo: [[LAYER3-RPO-CANDIDATE-ORDER-1]]は別途概念分離（総額系/長期系の
# 分離）で対応する前提のため、本対応では変更しない（該当BACKLOGの
# 着手条件は変更なし）。
# shares_basic_weighted_avg・shares_outstanding_period_end_sec: 加重平均
# （PL項目）と期末残高（BS項目）は異なる会計概念であり、元々別フィールドに
# 分離済み（[[SCHEMA-SHARESBASIC-CONCEPT-MISMATCH-1]]）。統合しない。
NO_CANDIDATE_MERGE_FIELDS = frozenset({
    "rpo",
    "shares_basic_weighted_avg",
    "shares_outstanding_period_end_sec",
})

# [[LAYER3-ANNUAL-MISCLASSIFICATION-BBAI-1]]対応。quarterly.py::
# _classify_period()の`fp=='FY' and days>130`判定（[[XBRL-TAG-KLAC-1]]
# 対応時に追加、[[QUARTERLY-CLASSIFY-PERIOD-NO-UPPER-BOUND-1]]で上限が
# 無いことが既知課題として登録済み）に上限が無いため、10-K内の中間期
# 比較開示（6ヶ月・9ヶ月累計等、fp='FY'タグ付きだが真の年次ではない）が
# is_annual=Trueに誤分類されることがある。この関数はnormalized/生成と
# 共有の`_classify_period()`/`_process_entries()`自体は変更せず、
# Layer3側の後処理としてのみ対応する（normalized/への影響を避けるため）。
#
# 判定基準は「同一accn・同一start日内に、fp=='FY'かつ130<days<=300の
# is_annual=Trueエントリが複数（end日付が異なる）存在する」こと
# （accnのみでグルーピングすると、SPAC合併等でstart日が異なる正当な
# predecessor/successor2エンティティのstub決算〈両方とも130〜300日
# 相当〉を誤って巻き込む実害をBBAI自身の2020年データで確認したため、
# 同一start日を要件に追加した）。同一start日で複数end日付を持つ
# エントリ群は、真の単年度決算ではあり得ない（同一起点からのYTD累計を
# 複数時点で開示している比較データの混入）と判定できる。
#
# 対象は実データ検証済みのBBAI・DELL・SPIRのみ（2026-08-06投資調査で
# 105銘柄横断スキャンした結果、同型の誤分類がNOW・RMBS等18銘柄に及ぶ
# ことが判明したが、実害未検証のため当初はBBAIのみに限定していた。
# 2026-08-30、[[LAYER3-ANNUAL-MISCLASSIFICATION-NOW-RMBS-1]]・
# [[LAYER3-ANNUAL-MISCLASSIFICATION-MINOR-5TICKERS-1]]対応時に105銘柄
# 全数を実データで再スキャンしたところ、24日間のデータ更新により
# 前提が変化していたことが判明した:
#   - NOW・METAは現在は該当エントリ自体が0件（自然解消）
#   - DELL（net_income、2グループ計4件）・SPIR（revenue、1グループ
#     2件）はBBAIと完全に同型（同一accn・同一start内で複数end日付が
#     競合）のため対象に追加した
#   - RMBS（16件）・ASTS（1件）・VRT（3件）は、同一(accn,start)内の
#     end日付が1種類のみ（グループ内で競合していない）という別パターン
#     であり、本関数の判定基準（複数end日付の競合）では検知できない。
#     実データ確認の結果、いずれも2020年の孤立した9ヶ月累計
#     （period_days=273、真の年次end=2020-12-31とは別に混入）で、
#     現在のannualエントリ列の最古（7件中1件目）に位置し、Moat Score
#     等のtrailing window（直近3〜6年）には現時点で影響しないことを
#     確認済み。この3銘柄は別種の検知ロジックが必要なため今回は対象外
#     のまま両エントリに残す。
_ANNUAL_MISCLASSIFICATION_FIX_TICKERS = frozenset({"BBAI", "DELL", "SPIR"})


def _reclassify_misannotated_fy_entries(entries: list, ticker: str) -> list:
    """
    同一accn・同一start日内に、fp=='FY'かつ130<period_days<=300の
    is_annual=Trueエントリが複数（end日付が異なる）存在する場合、
    真の単年度決算ではあり得ない（同一起点からのYTD累計を複数時点で
    開示する比較データの混入）と判定し、該当エントリを除外する。

    グルーピング単位に`start`を含める理由: `accn`のみでグルーピング
    すると、SPAC合併等でpredecessor/successor2エンティティのstub決算
    （start日が異なり、両方とも130〜300日相当になりうる正当なケース）
    を誤って巻き込む（BBAI自身の2020年データで実際に確認・回帰した
    バグ）。同一start日を要件に加えることで、YTD比較開示（同一起点から
    の累計を複数end日で開示）のみを対象にできる。

    対象は`_ANNUAL_MISCLASSIFICATION_FIX_TICKERS`に含まれるtickerのみ
    （2026-08-30時点でBBAI・DELL・SPIR）。対象外のtickerでは入力を
    そのまま返す。

    除外方式（is_annual=Falseへの再分類ではなく完全除外）を採用した
    理由: これらのエントリをis_ytd=Trueの四半期候補として復活させると
    quarterly側の解決ロジック（YTDチェーン構築等）に予期しない影響を
    与えるリスクがあり、[[XBRL-TAG-KLAC-1]]対応時の設計方針
    （四半期duration混入は誤タグ由来として除外のみ行う）と一貫性を
    持たせるため、単純に除外する。
    """
    if ticker.upper() not in _ANNUAL_MISCLASSIFICATION_FIX_TICKERS:
        return entries

    suspect_ends_by_accn_start: dict[tuple, set] = defaultdict(set)
    for e in entries:
        if (
            e.get("is_annual")
            and e.get("fp") == "FY"
            and 130 < (e.get("period_days") or 0) <= 300
        ):
            suspect_ends_by_accn_start[(e.get("accn", ""), e.get("start", ""))].add(e.get("end"))
    bad_keys = {key for key, ends in suspect_ends_by_accn_start.items() if len(ends) > 1}
    if not bad_keys:
        return entries

    return [
        e for e in entries
        if not (
            e.get("is_annual")
            and e.get("fp") == "FY"
            and 130 < (e.get("period_days") or 0) <= 300
            and (e.get("accn", ""), e.get("start", "")) in bad_keys
        )
    ]


BASE_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
DATA_DIR = os.path.join(BASE_DIR, "data")
CONFIG_PATH = os.path.join(REPO_ROOT, "config", "sec_concept_definitions.json")
STORE_V2_DIR = os.path.join(BASE_DIR, "store_v2")

_QUARTERLY_YEARS = 5
_ANNUAL_YEARS = 6


# ---------------------------------------------------------------------------
# Layer1 / Layer2 ロード
# ---------------------------------------------------------------------------

def load_company_facts(ticker: str) -> dict | None:
    """Layer1: common/sec_data/data/{TICKER}/company_facts.json を読み込む。"""
    path = os.path.join(DATA_DIR, ticker.upper(), "company_facts.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("[%s] company_facts.json 読み込み失敗: %s", ticker, e)
        return None


def load_concept_definitions() -> dict:
    """Layer2: config/sec_concept_definitions.json を読み込む。"""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# ticker_overrides（quarterly.py::TICKER_RESTRICTIONS移行、
# [[SOFI-TICKER-RESTRICTIONS-NOT-MIGRATED-1]]対応）
# ---------------------------------------------------------------------------

def _get_ticker_field_override(concept_defs: dict, ticker: str, field_name: str) -> dict | None:
    """
    config/sec_concept_definitions.jsonのticker_overridesから、指定
    ticker・field_nameに対応する単純除外/概念差し替え設定を取り出す。

    ticker_overridesは銘柄によって以下2形式のいずれかを取る:
    - 単一フィールド（field/action/override_concept/noteを直接持つ）
    - 複数フィールド（overrides: [{field/action/...}, ...]のリスト）
    どちらの形式でも、指定field_nameに一致するエントリを1件返す
    （該当なしの場合はNone）。cross_filing_tags・注記専用エントリ
    （GOOGLのnoteのみ等）はこの関数の対象外
    （cross_filing_tagsは_apply_cross_filing_tags()参照）。
    """
    ticker_config = concept_defs.get("ticker_overrides", {}).get(ticker.upper())
    if not ticker_config:
        return None
    override_list = ticker_config.get("overrides")
    if override_list is None:
        override_list = [ticker_config] if "field" in ticker_config else []
    for override in override_list:
        if override.get("field") == field_name:
            return override
    return None


def _apply_cross_filing_tags(
    company_facts: dict, concept_defs: dict, ticker: str, field_name: str, unit: str,
) -> list:
    """
    ticker_overrides.{ticker}.cross_filing_tags.{field_name}を適用し、
    期間・フォーム条件付きで複数タグを合算したエントリを生成する
    （NVDA short_term_investments等、非上場投資先の上場等により
    単一タグでの捕捉が不可能なケース向け）。

    各ruleについて、componentsに列挙されたタグそれぞれから
    end_date・forms条件に合致するエントリを検索し、valを合算する。
    periodキーの型（int=annual、str "YYYYQN"=quarterly）でis_annualを
    判定する。componentsのいずれかが見つからない場合はそのruleを
    スキップする（不完全な合算をしない）。合算結果には
    "cross_filing": Trueを付与する（is_implied等の既存フラグとは
    別扱い。呼び出し元が既存end_dateへの追加のみに使うため、
    _merge_normalized_by_priority()の再マージや優先順位判定の対象には
    ならない）。
    """
    ticker_config = concept_defs.get("ticker_overrides", {}).get(ticker.upper())
    if not ticker_config:
        return []
    rules = ticker_config.get("cross_filing_tags", {}).get(field_name)
    if not rules:
        return []

    result: list = []
    for rule in rules:
        end_date = rule.get("end_date")
        period = rule.get("period")
        components = rule.get("components", [])
        if not end_date or not components:
            continue

        total = 0.0
        found_all = True
        for comp in components:
            raw_entries = _get_concept_units(company_facts, comp.get("tag", ""), unit)
            forms = comp.get("forms", [])
            match = next(
                (e for e in raw_entries if e.get("end") == end_date and e.get("form") in forms),
                None,
            )
            if match is None or match.get("val") is None:
                found_all = False
                break
            total += match["val"]
        if not found_all:
            continue

        is_annual = isinstance(period, int)
        result.append({
            "end": end_date,
            "start": "",
            "val": total,
            "fp": "FY" if is_annual else str(period),
            "fy": None,
            "form": "",
            "filed": "",
            "accn": "",
            "period_days": 0,
            "is_ytd": False,
            "is_annual": is_annual,
            "cross_filing": True,
            "approx_residual_pct": rule.get("approx_residual_pct"),
        })

    return result


# ---------------------------------------------------------------------------
# フィールド抽出（Layer2 candidatesベースのフォールバック）
# ---------------------------------------------------------------------------

def _get_concept_units(company_facts: dict, concept: str, unit: str) -> list:
    """company_facts.facts.{namespace}.{concept}.units.{unit} を安全に取得する。

    [[LAYER3-COGS-ASTS-LRCX-RECOVERABLE-FOLLOWUP-1]]対応: conceptに
    "名前空間:タグ名"形式（コロン区切り、例: "lrcx:CostOfGoodsAndServices
    SoldExcludingRestructuringCharges"）を許容する。企業固有の拡張タグ
    （ticker_overrides::override_conceptでのみ使用想定）を明示的に参照
    できるようにするため。コロンを含まない場合は従来通りus-gaap名前空間
    を参照する（完全な後方互換。既存のNVDA/KLAC/TER/V/SOFIのticker_
    overridesはいずれもus-gaap名前空間のタグのみを参照しており、本対応
    による動作変更はない）。
    """
    namespace, _, tag = concept.partition(":")
    if not tag:
        namespace, tag = "us-gaap", concept
    try:
        return company_facts["facts"][namespace][tag]["units"][unit]
    except (KeyError, TypeError):
        return []


def extract_field_raw_entries(
    company_facts: dict, field_def: dict, field_name: str = "", ticker: str = "",
) -> tuple[list, str | None]:
    """
    Layer2のcandidatesからエントリを抽出する。

    [[LAYER3-FALLBACK-STALE-TAG-PRIORITY-1]]対応: field_nameが
    NO_CANDIDATE_MERGE_FIELDSに含まれない限り、候補タグごとに独立して
    正規化（YTD→単四半期変換）を完了させたうえで、その正規化済み系列を
    end_dateキーで優先順位マージする（_merge_candidate_entries参照）。
    「最初に見つかった非空候補を採用」方式は、直近データを持たない古い
    タグ（例: Revenues）が、より新しく実際に使われているタグより先に
    拾われてしまう問題があった（IONQ等で確認）。

    戻り値の正規化状態が呼び出し元（build_ticker_store）で分岐に使われる:
    NO_CANDIDATE_MERGE_FIELDS対象フィールドは従来通り「最初に見つかった
    非空候補を採用」方式を維持し、_process_entries()止まりの未正規化
    エントリを返す（呼び出し元が_normalize_field_entries()を適用する）。
    候補マージ対象フィールドは、本関数内で正規化まで完了させた結果を
    返す（呼び出し元は再度正規化しない。理由はモジュールdocstring・
    _merge_candidate_entries参照）。

    tickerは[[SOFI-TICKER-RESTRICTIONS-NOT-MIGRATED-1]]対応のticker_
    overrides適用箇所を明示するために保持する（exclude・
    override_concept自体はbuild_ticker_store()の呼び出し前に
    field_def.candidatesへ反映済みの前提で、本関数はその結果を
    通常通り処理するのみ）。

    戻り値: (エントリのリスト, 採用したタグ名（複数採用時は"+"区切り）or None)
    """
    unit = field_def.get("unit", "USD")
    candidates = field_def.get("candidates", [])

    if field_name in NO_CANDIDATE_MERGE_FIELDS:
        for concept in candidates:
            raw_entries = _get_concept_units(company_facts, concept, unit)
            if not raw_entries:
                continue
            processed = _process_entries(raw_entries)
            processed = _reclassify_misannotated_fy_entries(processed, ticker)
            if processed:
                return processed, concept
        return [], None

    return _merge_candidate_entries(company_facts, candidates, unit, field_name, ticker)


def _merge_candidate_entries(
    company_facts: dict, candidates: list, unit: str, field_name: str = "", ticker: str = "",
) -> tuple[list, str | None]:
    """
    候補タグごとに独立して_process_entries()→_normalize_field_entries()を
    適用し（各タグ内で完結したYTD→単四半期変換を完了させる）、得られた
    正規化済み系列同士をend_date単位で優先順位マージする。優先タグ
    （candidatesの先頭）にその期間のエントリがあれば採用し、なければ
    次候補にフォールバックする（_merge_normalized_by_priority参照）。

    設計変更の経緯: 当初は生エントリを先に統合してから正規化する順序
    だったが、異なるタグ由来のエントリが同一end_dateで競合した際の
    タイブレークが、_normalize_field_entries()内のFYチェーン判定
    （startの完全一致でグルーピング）を破壊し、YTD差分計算が中間四半期を
    1つ読み飛ばして2四半期分を1四半期として誤算出するバグを引き起こした
    （CPRT capital_expenditure・PEP他で実データ確認）。タグごとに
    独立して正規化を完了させてからマージすることで、この
    クロスタグ混入を構造的に防ぐ。

    欠落四半期逆算（[[LAYER3-ANCHOR-MISSING-LOOKBACK-WINDOW-1]]対応）は
    このタグ単位の正規化直後・クロスタグマージの前に適用する。マージ後
    （_merge_normalized_by_priority）は(end_date, is_annual)複合キーで
    候補を1件に絞り込むため、同一end_dateを持つYTD累計エントリと単独
    四半期エントリ（例: H1 YTDとQ2単独）が競合し、標準的な単四半期の
    period_days範囲外であるYTD側がマージの時点で破棄されてしまう
    （post-merge適用で実際に検証し、対象87件中10件しか復元できなかった
    ことを確認済み）。タグ単位・マージ前に適用することで、YTD累計
    エントリがまだ生きている状態で欠落四半期を逆算できる。

    各エントリに"source_tag"（採用元の候補タグ名）を付与する
    （[[LAYER3-DA-SBC-CANDIDATE-REGRESSION-1]]対応）。
    _merge_normalized_by_priority()は(end_date, is_annual)キーごとに
    独立して候補タグを選ぶため、年次キーと四半期キーで異なるタグが
    勝つことがある（優先タグが年次申告のみ・四半期申告のみ停止する
    ケース等、BSY等で実データ確認済み）。この"source_tag"を
    q4_implied.py::build_q4_implied_entries()が参照し、Q4逆算に使う
    年次・Q1・Q2・Q3の4エントリのsource_tagが完全一致しない場合は
    Q4逆算をスキップするガードに用いる。

    Q4逆算はこのタグ単位ループの中で、各タグ自身の年次・四半期
    エントリのみを使って独立して試みる（優先順位順に候補タグを
    1つずつ試し、最初に4エントリが揃ったタグの結果を採用する構造。
    実データ確認の結果、同一source_tagガード導入後に欠落した112件中
    88件が、優先タグ以外の候補タグ単独では年次・Q1・Q2・Q3が揃って
    おり復元可能だったため）。このタグ単位Q4逆算は、呼び出す
    build_q4_implied_entries()自身が「同一タグ内の年次・四半期のみ」
    を渡されるため、source_tagガードは常に自明に通過する（同一タグの
    entries全てが同じsource_tagを持つため）。生成されたQ4エントリにも
    その候補タグ名をsource_tagとして付与し、マージ時に他タグの
    エントリと同様に優先順位に従って競合解決される（優先タグに
    Q4エントリがあればそちらが採用され、無ければ次点タグのQ4
    エントリが採用される）。この結果、build_ticker_store()側の
    マージ後Q4逆算呼び出し（既存、マージ済みentriesに対して同一
    ガードを適用）は、タグ単位で既に復元済みの場合は
    existing_ends判定により重複生成せず、どのタグでも4エントリが
    揃わなかった場合のみ空リストを返す（結果的に冗長だが無害な
    二重チェックとして残置）。
    """
    used_concepts: list = []
    per_tag_normalized: list = []
    apply_missing_quarter = field_name in MISSING_QUARTER_IMPLIED_FIELDS
    for concept in candidates:
        raw_entries = _get_concept_units(company_facts, concept, unit)
        if not raw_entries:
            continue
        processed = _process_entries(raw_entries)
        processed = _reclassify_misannotated_fy_entries(processed, ticker)
        if not processed:
            continue
        normalized = _normalize_field_entries(processed)
        if not normalized:
            continue
        for e in normalized:
            e["source_tag"] = concept
        if apply_missing_quarter:
            missing_list = _build_missing_quarter_implied_entries(normalized)
            if missing_list:
                for e in missing_list:
                    e["source_tag"] = concept
                existing_ends = {e["end"] for e in normalized if not e.get("is_annual")}
                added = [e for e in missing_list if e["end"] not in existing_ends]
                if added:
                    normalized = sorted(normalized + added, key=lambda x: x["end"])

        # タグ単位Q4逆算（[[LAYER3-DA-SBC-CANDIDATE-REGRESSION-1]]対応）:
        # このタグ自身の年次・四半期エントリのみでQ4逆算を試みる。
        tag_annual_for_q4 = [e for e in normalized if e.get("is_annual")]
        tag_quarterly_for_q4 = [
            e for e in normalized
            if not e.get("is_annual") and not e.get("is_ytd")
        ]
        tag_q4_list = build_q4_implied_entries(tag_annual_for_q4, tag_quarterly_for_q4, field_name)
        if tag_q4_list:
            for e in tag_q4_list:
                e["source_tag"] = concept
            existing_ends = {e["end"] for e in normalized if not e.get("is_annual")}
            added = [e for e in tag_q4_list if e["end"] not in existing_ends]
            if added:
                normalized = sorted(normalized + added, key=lambda x: x["end"])

        used_concepts.append(concept)
        per_tag_normalized.append(normalized)

    if not per_tag_normalized:
        return [], None

    merged = _merge_normalized_by_priority(per_tag_normalized)
    source_tag = "+".join(used_concepts) if len(used_concepts) > 1 else used_concepts[0]
    return merged, source_tag


# [[LAYER3-MISSING-QUARTER-IMPLIED-GAP-1]]対応: 標準的な単四半期として
# 妥当なperiod_daysの範囲。105銘柄×32フィールド全数（is_annual・
# is_implied除外後、約38,000エントリ）のperiod_days分布を確認した結果、
# 正常な単四半期は83〜98日に密集しており、唯一の異常値（RCAT
# stock_based_compensationの優先タグ内欠落による誤合算値）は183日
# だった。75〜100日を範囲とすることで、正常値（83〜98日）には
# 十分な余裕を持たせつつ、183日の異常値は明確に除外できる。
# 27〜55日の少数の短期スタブ期間（IPO直後の端数月次報告等、APGE/CEG/
# FROG/LITE/VZで確認）はこの範囲より短いが正当なデータのため、
# range外と判定された場合でも即座に破棄せず次候補を探索し、
# 全候補が範囲外だった場合は元の優先候補の値を採用する
# （データを失わないためのフォールバック、下記_merge_normalized_by_priority参照）。
_STANDARD_QUARTER_MIN_DAYS = 75
_STANDARD_QUARTER_MAX_DAYS = 100

# [[LAYER3-ANNUAL-QUARTERLY-COLLISION-1]]対応: 標準的な年次として
# 妥当なperiod_daysの範囲。105銘柄×28フィールド全数のis_annual=True
# エントリ（11,517件）のperiod_days分布を確認した結果、95%が
# 363〜365日に、99%が370日以内に収まっていた。一方295日以下にも
# 156件のクラスタ（180〜295日、300日未満）があり、これは
# [[QUARTERLY-CLASSIFY-PERIOD-NO-UPPER-BOUND-1]]（quarterly.py::
# _classify_period()のis_annual判定に上限が無く、中間的な期間長の
# エントリを誤ってis_annual=Trueに分類するバグ）の実例と判明した
# （DELL等）。300日を下限とすることで、この誤分類クラスタと明確に
# 分離できる（_classify_period()自身が持つdays>300の代替分岐と
# 一致させた値）。上限400日は53週決算年度等の変則的な年度長にも
# 余裕を持たせるための保守的な値。
_STANDARD_ANNUAL_MIN_DAYS = 300
_STANDARD_ANNUAL_MAX_DAYS = 400


def _is_plausible_standalone_quarter(e: dict) -> bool:
    """
    標準的な単四半期として妥当なperiod_daysか判定する。
    is_annual・is_implied（Q4逆算等）エントリはチェック対象外（常にTrue）。
    period_days=0（残高スナップショット等のinstant fact）もチェック対象外
    （四半期の「長さ」という概念が存在しないフィールドのため）。
    """
    if e.get("is_annual") or e.get("is_implied"):
        return True
    period_days = e.get("period_days")
    if not period_days:
        return True
    return _STANDARD_QUARTER_MIN_DAYS <= period_days <= _STANDARD_QUARTER_MAX_DAYS


def _is_plausible_annual(e: dict) -> bool:
    """
    標準的な年次として妥当なperiod_daysか判定する
    （[[LAYER3-ANNUAL-QUARTERLY-COLLISION-1]]対応）。
    period_days=0（instant fact）はチェック対象外。
    """
    period_days = e.get("period_days")
    if not period_days:
        return True
    return _STANDARD_ANNUAL_MIN_DAYS <= period_days <= _STANDARD_ANNUAL_MAX_DAYS


def _merge_normalized_by_priority(per_tag_normalized: list) -> list:
    """
    候補タグごとに正規化済みの系列（per_tag_normalizedの並び順＝
    candidatesの優先順位）を、(end_date, is_annual)の複合キー単位で
    優先順位マージする。優先タグ（リスト先頭）にその期間・区分の
    エントリがあれば採用し、なければ次候補にフォールバックする。

    [[LAYER3-ANNUAL-QUARTERLY-COLLISION-1]]対応: end_dateのみをキーに
    すると、カレンダー年決算企業の年次エントリ（is_annual=True、365日
    前後）と「Q4単独開示」エントリ（fp='FY'だが実際は91日程度、
    is_annual=False）が同一end_dateを持つ場合に競合し、一方が黙って
    破棄される問題があった（[[XBRL-TAG-KLAC-1]]と同型のパターン）。
    is_annualをキーに含めることで、年次・四半期を別スロットに分離し、
    両方とも最終出力に残す。

    [[LAYER3-MISSING-QUARTER-IMPLIED-GAP-1]]対応: is_annual=False
    スロット内で優先タグのエントリが標準的な単四半期として妥当な
    period_daysの範囲から外れている場合（優先タグ自体が特定四半期の
    報告を欠落させ、隣接四半期と合算した値を報告しているケース。RCAT
    等で確認）、そのエントリを不採用とし、次候補タグの同一キーの
    エントリを探す。is_annual=Trueスロットについても同様の妥当性判定
    （_is_plausible_annual）を適用する。全候補が範囲外だった場合は、
    データを完全に失わないよう最も優先度の高い候補の値にフォールバック
    する（27〜55日の正当な短期スタブ期間等を誤って欠落させないため）。

    [[LAYER3-IMPLIED-BLOCKS-FALLBACK-1]]対応: 上記の選択はタグ優先順位
    のみに従っていたため、優先タグに欠落四半期逆算（is_implied=True）
    による派生値が新規に生成されると、それが実報告値（is_implied=False）
    を持つ下位タグより先に採用されてしまう問題があった（ONDS
    selling_and_marketing 2024Q1で実データ確認: 最優先タグの狭い概念
    "広告費のみ"の派生値26,143が、次点タグの正しい報告値1,321,149より
    先に採用されていた）。「実報告データは派生値より常に優先する」
    原則をタグ優先順位より上位に置くため、①is_implied=Falseかつ妥当な
    エントリをタグ優先順位順に探し、②該当が1件もない場合のみ
    is_implied=Trueのエントリをタグ優先順位順に探す、の2段階選択に
    変更する。
    """
    candidates_by_key: dict[tuple, list] = defaultdict(list)
    for normalized in per_tag_normalized:
        for e in normalized:
            candidates_by_key[(e["end"], bool(e.get("is_annual")))].append(e)

    merged: list = []
    for (end, is_annual), entries in candidates_by_key.items():
        plausible_check = _is_plausible_annual if is_annual else _is_plausible_standalone_quarter
        chosen = next(
            (e for e in entries if not e.get("is_implied") and plausible_check(e)),
            None,
        )
        if chosen is None:
            chosen = next((e for e in entries if plausible_check(e)), entries[0])
        merged.append(chosen)

    return sorted(merged, key=lambda x: x["end"])


def _apply_sign_normalize(entries: list, mode: str | None) -> list:
    """sign_normalizeが指定されていれば適用する（現状"abs"のみサポート）。"""
    if mode != "abs":
        return entries
    out = []
    for e in entries:
        e = dict(e)
        if e.get("val") is not None:
            e["val"] = abs(e["val"])
        out.append(e)
    return out


# ---------------------------------------------------------------------------
# YTD→単四半期差分変換（NORMALIZER-YTD-METADATA-STALE-1対応）
# ---------------------------------------------------------------------------

def _ytd_to_quarterly(fy_entries: list) -> tuple[list, list]:
    """
    YTDエントリのリストを受け取り、差分変換した単一四半期エントリを返す。

    normalizer.py::_ytd_to_quarterly() と同一のアルゴリズムだが、差分変換後の
    start/period_days を変換後の期間（前四半期end翌日〜当四半期end）に
    再計算する点が異なる（NORMALIZER-YTD-METADATA-STALE-1のメタデータ
    不整合を修正: 旧実装はvalのみYTD差分にし、start/period_daysをYTD期間
    のまま残していた）。

    [[NORMALIZER-YTD-METADATA-STALE-1]]normalizer.py側修正時に発見した
    本関数自体のオフバイワン修正（2026-09-10）: 起点をprev_end自体（前
    四半期のend日そのもの）としていたため、start日が前四半期のend日と
    同一になり、period_daysが1日過大だった（AAPL実データ: Q2 CapEx
    start=2025-12-27・period_days=91だったが、本人申告SAエントリ
    〈Revenue等〉の実際の慣例はstart=2025-12-28・period_days=90）。
    起点を「prev_endの翌日」に修正する。

    戻り値: (converted, unresolved)
    """
    converted: list = []
    unresolved: list = []
    prev_ytd: float = 0
    prev_ytd_known = False
    prev_end: str | None = None

    for entry in fy_entries:
        new_entry = dict(entry)

        if not entry.get("is_ytd"):
            standalone = entry["val"]
            prev_ytd += standalone
            prev_ytd_known = True
            prev_end = entry["end"]
            new_entry["val"] = standalone
            converted.append(new_entry)
            continue

        if not prev_ytd_known:
            unresolved.append(new_entry)
            prev_ytd = entry["val"]
            prev_ytd_known = True
            prev_end = entry["end"]
            continue

        standalone = entry["val"] - prev_ytd
        if prev_ytd > 0 and entry["val"] < prev_ytd:
            new_entry["anomaly"] = True
            logger.warning(
                "YTD reversal for end=%s fp=%s val=%s prev_ytd=%s",
                entry.get("end"), entry.get("fp"), entry["val"], prev_ytd,
            )

        # --- NORMALIZER-YTD-METADATA-STALE-1対応: start/period_daysを
        #     変換後の単四半期期間に再計算する（起点はprev_endの翌日） ---
        new_start = None
        period_days = entry.get("period_days")
        if prev_end:
            try:
                new_start_date = date.fromisoformat(prev_end) + timedelta(days=1)
                new_start = new_start_date.isoformat()
                period_days = (date.fromisoformat(entry["end"]) - new_start_date).days
            except (ValueError, TypeError):
                new_start = None
                period_days = entry.get("period_days")

        prev_ytd = entry["val"]
        prev_end = entry["end"]
        new_entry["is_ytd"] = False
        new_entry["val"] = standalone
        if new_start:
            new_entry["start"] = new_start
        if period_days is not None:
            new_entry["period_days"] = period_days
        converted.append(new_entry)

    return converted, unresolved


def _normalize_field_entries(raw_entries: list) -> list:
    """
    1フィールドのエントリ群をYTD→単四半期変換する
    （normalizer.py::_normalize_field()と同型のロジック）。
    """
    if not raw_entries:
        return []

    annual = [e for e in raw_entries if e.get("is_annual")]
    quarterly = [e for e in raw_entries if not e.get("is_annual")]

    sa_entries = [e for e in quarterly if not e.get("is_ytd")]
    ytd_entries = [e for e in quarterly if e.get("is_ytd")]

    if not ytd_entries:
        return sorted(annual + sa_entries, key=lambda x: x["end"])

    ytd_starts = {e["start"] for e in ytd_entries}
    chain_entries = [e for e in quarterly if e["start"] in ytd_starts]
    passthrough_entries = [e for e in quarterly if e["start"] not in ytd_starts]

    by_fy_start: dict[str, list] = defaultdict(list)
    for e in chain_entries:
        by_fy_start[e["start"]].append(e)

    converted: list = []
    unresolved: list = []
    for fy_start, fy_entries in by_fy_start.items():
        sorted_entries = sorted(fy_entries, key=lambda x: x["end"])
        c, u = _ytd_to_quarterly(sorted_entries)
        converted.extend(c)
        unresolved.extend(u)

    by_end: dict[str, list] = defaultdict(list)
    for e in passthrough_entries:
        by_end[e["end"]].append(("passthrough", e))
    for e in converted:
        by_end[e["end"]].append(("converted", e))

    all_quarterly: list = []
    for end_date, candidates in by_end.items():
        raw_sa = [e for kind, e in candidates if kind == "passthrough"]
        pool = raw_sa if raw_sa else [e for kind, e in candidates if kind == "converted"]
        all_quarterly.append(max(pool, key=lambda x: x.get("filed", "")))
    all_quarterly.extend(unresolved)

    all_quarterly.sort(key=lambda x: x["end"])
    return sorted(annual + all_quarterly, key=lambda x: x["end"])


# ---------------------------------------------------------------------------
# 欠落四半期逆算（任意欠落四半期の逆算、[[LAYER3-ANCHOR-MISSING-LOOKBACK-
# WINDOW-1]]対応）。normalizer.py::_build_missing_quarter_implied_entries()
# の移植（先頭欠落パターンのみに限定した派生版）。
#
# 【末尾欠落パターンを対象外とした理由】normalizer.py版は先頭・末尾どちらの
# 欠落も扱うが、末尾欠落（FY−Q1−Q2−Q3=Q4型）はq4_implied.pyの担当領域と
# 数学的に同型である。本関数はタグ単位・クロスタグマージ前に適用するため、
# 末尾欠落も扱うと「単一タグのみの視点」で独自にQ4相当を再計算してしまい、
# マージ後の統合系列（複数タグ横断）を使う既存q4_implied.pyの結果と乖離する
# ことが実データで判明した（DDOG NetIncome 2024-12-31: 旧パイプラインは
# クロスタグマージ後の異常な断片エントリによりq4_implied.py側のQ4逆算が
# 発火せず旧データはそのまま残っていたが、本関数はタグ単位のためこの断片を
# 見ずに独自にQ4を再計算し、値が旧normalized/と乖離した。ASTS Revenue・
# ONDS SMでも同型の乖離を確認）。末尾欠落はq4_implied.pyに完全に委ね、
# 本関数は末尾欠落と重複しない先頭欠落パターンのみを扱う。
# ---------------------------------------------------------------------------

# 適用対象フィールド（スコープ制限）。normalizer.py::normalize()内の既存
# 13フィールドスコープ（Revenue/_COGS/OCF/ICF/CFF/CapEx/RD/SM/SBC/DA/
# NetIncome/OperatingIncome/GrossProfit）をsnake_caseで踏襲する。
# q4_implied.py::Q4_IMPLIED_FIELDS（16フィールド、PascalCase）とは非対称
# であり、finance_lease_payments・buybackは含まない（normalizer.py側が
# 元々この2フィールドを欠落四半期逆算の対象にしていないため、既存動作を
# 変えない範囲でスコープを揃える。詳細はq4_implied.pyのdocstring参照）。
# この関数自体にはフィールド種別のガードが無く、shares/stock系フィールド
# （shares_diluted等）に適用すると符号異常な値を生む（105銘柄スキャンで
# 実データ確認済み）ため、呼び出し側は必ずこのスコープで絞り込むこと。
#
# [[LAYER3-SGA-Q4-MISSING-1]]対応（2026-07-29）: selling_general_and_
# administrativeを追加。Layer2スキーマ追加時の新規フィールドのため
# normalizer.py側の元々の13フィールドスコープには存在しなかったが、SM
# 同様のフロー系費用項目であり本関数の適用対象として妥当。
#
# [[LAYER3-TTM-REGRESSION-NEWFIELD-BLINDSPOT-1]]対応: 同じくLayer2スキーマ
# 追加時の新規フィールドであるshort_term_investments・total_liabilities
# （category="stock"）・eps_basic・eps_diluted（比率フィールド）は、上記
# 「shares/stock系フィールドに適用すると符号異常な値を生む」ガード対象と
# 同種の理由で意図的にこのスコープに含めていない（詳細はq4_implied.pyの
# モジュールdocstring参照）。cost_of_revenueは元々このスコープに含まれている。
MISSING_QUARTER_IMPLIED_FIELDS = frozenset({
    "revenue", "cost_of_revenue",
    "operating_cash_flow", "investing_cash_flow", "financing_cash_flow",
    "capital_expenditure",
    "research_and_development", "selling_and_marketing",
    "stock_based_compensation", "depreciation_and_amortization",
    "net_income", "operating_income", "gross_profit",
    "selling_general_and_administrative",
})


def _build_missing_quarter_implied_entries(entries: list) -> list:
    """
    複数四半期分のYTD累計（is_ytd=Trueのまま未解決で残っているエントリ、
    または年次実績）から、既知の単独四半期を差し引いて、残る1四半期分の
    値を逆算する。normalizer.py::_build_missing_quarter_implied_entries()
    のアルゴリズムをベースに、**先頭欠落パターンのみ**に限定した派生版
    （9ヶ月累計等の中間累計からも欠落四半期を復元できるようにする一般化。
    例: H1 YTD − Q2単独 = Q1）。

    対象スパン内にちょうど1四半期分の欠落がある場合のみ逆算する
    （複数四半期欠落・重複計上の疑いがある場合は対象外とし何もしない）。
    欠落位置は先頭（既知四半期群より前）のみ対応。末尾欠落
    （FY−Q1−Q2−Q3=Q4型）はq4_implied.pyの担当領域のため対象外とし、
    中間の飛び地も非対応（モジュール冒頭のコメント参照）。
    """
    cumulative_candidates = [
        e for e in entries
        if (e.get("is_annual") or e.get("is_ytd")) and e.get("val") is not None
        and e.get("start") and e.get("end")
    ]
    known_quarters = [
        e for e in entries
        if not e.get("is_annual") and not e.get("is_ytd") and e.get("val") is not None
    ]

    result: list = []
    for cand in cumulative_candidates:
        c_start, c_end, c_val = cand["start"], cand["end"], cand["val"]
        try:
            span_days = (date.fromisoformat(c_end) - date.fromisoformat(c_start)).days
        except (ValueError, TypeError):
            continue

        # 単一四半期未満のスパンは対象外（2四半期分=約150日以上のみ）
        if span_days < 150:
            continue
        n_quarters = round(span_days / 91)
        if n_quarters < 2:
            continue

        contained = [
            q for q in known_quarters
            if q.get("start", "") >= c_start and q.get("end", "") <= c_end
        ]
        if len(contained) != n_quarters - 1:
            continue  # ちょうど1四半期分の欠落でなければ対象外（安全側に倒す）

        contained_sorted = sorted(contained, key=lambda x: x["start"])
        try:
            covered_span = sum(
                (date.fromisoformat(q["end"]) - date.fromisoformat(q["start"])).days
                for q in contained_sorted
            )
        except (ValueError, TypeError):
            continue
        # 既知四半期の合計期間 + 欠落想定1四半期(約91日) がスパン全体と
        # ほぼ一致することを確認（重複・飛び地の混入を防ぐ簡易チェック）
        if abs(covered_span + 91 - span_days) > 20:
            continue

        first_known_start = contained_sorted[0]["start"]
        if first_known_start <= c_start:
            # 欠落四半期が先頭でない（末尾または中間の飛び地）。末尾欠落
            # （FY−Q1−Q2−Q3=Q4型）はq4_implied.pyの担当領域のため、本関数
            # では意図的に何もしない（モジュール冒頭のコメント参照）。
            continue
        # 欠落四半期は先頭（既知四半期群より前）
        try:
            missing_end = (
                date.fromisoformat(first_known_start) - timedelta(days=1)
            ).isoformat()
        except (ValueError, TypeError):
            continue
        missing_start = c_start

        missing_val = c_val - sum(q["val"] for q in contained_sorted)

        try:
            period_days = (date.fromisoformat(missing_end) - date.fromisoformat(missing_start)).days
        except (ValueError, TypeError):
            period_days = 90

        result.append({
            "end":         missing_end,
            "start":       missing_start,
            "val":         missing_val,
            "fp":          "implied",
            "fy":          cand.get("fy"),
            "form":        cand.get("form", ""),
            "filed":       cand.get("filed", ""),
            "accn":        cand.get("accn", ""),
            "period_days": period_days,
            "is_ytd":      False,
            "is_annual":   False,
            "is_implied":  True,
        })

    # 複数の累計候補（例: 6ヶ月YTDと9ヶ月YTDの両方）が同一の欠落四半期を
    # 独立に逆算することがあるため、(start, end) 単位で重複排除する。
    # 値が食い違う場合は不整合として両方除外する（HQY GrossProfit/_COGS
    # end=2021-04-30で実データ確認済みの挙動）。
    by_period: dict[tuple, list] = defaultdict(list)
    for e in result:
        by_period[(e["start"], e["end"])].append(e)

    deduped: list = []
    for period, candidates in by_period.items():
        vals = {round(c["val"], 2) for c in candidates}
        if len(vals) == 1:
            deduped.append(candidates[0])
        else:
            logger.warning(
                "missing quarter implied value mismatch for %s: %s",
                period, [c["val"] for c in candidates],
            )

    return deduped


# ---------------------------------------------------------------------------
# Q4逆算本体はcommon/sec_data/q4_implied.py::build_q4_implied_entries()に
# 集約済み（[[Q4-IMPLIED-CALC-TRIPLICATION-1]]・[[LAYER3-Q4-IMPLIED-
# NOT-MIGRATED-1]]対応）。ここではimportして再利用する（上部import参照）。
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# FCF共有関数（parser.py・ttm_calculator.pyの既存計算式と同一）
# ---------------------------------------------------------------------------

def calc_fcf(ocf: float | None, capex: float | None, fl: float | None) -> float | None:
    """
    FCF = OCF - max(0, |CapEx| - |FinanceLeasePmts|)

    parser.py（1503-1505行付近）・ttm_calculator.py::_calc_fcf()と
    同一の計算式。quarterly/annual生成側とTTM集計側の両方が本関数を
    呼ぶことで、[[Q4-IMPLIED-CALC-TRIPLICATION-1]]と同種の独立実装リスク
    （FCF計算式の重複実装）を解消する。

    CapExは本モジュールのLayer2定義でsign_normalize="abs"が既に
    適用されている前提のため、ここでも念のためabs()を通す
    （二重abs()は符号に影響しない）。
    """
    if ocf is None or capex is None:
        return None
    pure_capex = abs(capex) - abs(fl or 0)
    return ocf - max(0, pure_capex)


# ---------------------------------------------------------------------------
# 銘柄単位のLayer3構築
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# GrossProfitバックフィル（[[LAYER3-GROSSPROFIT-BACKFILL-MISSING-1]]対応）。
# normalizer.py::_calc_gross_profit()の移植。
# ---------------------------------------------------------------------------

def _index_quarterly_by_end(entries: list) -> dict:
    """
    非annualの四半期エントリをend日付でインデックス化する（valueではなく
    エントリ全体を保持）。normalizer.py::_index_quarterly_by_end()と同一。

    同一end日付に複数候補が存在する場合（単独四半期値=is_ytd Falseと、
    まだ最終クリーンアップ前で残っている未解決の累積値=is_ytd Trueが
    共存するケース）、単独四半期値を優先する。単独四半期値がない場合のみ
    累積値にフォールバックし、同種同士では最新filed優先とする。
    """
    by_end: dict[str, list] = defaultdict(list)
    for e in entries:
        if e.get("is_annual"):
            continue
        by_end[e["end"]].append(e)

    result: dict[str, dict] = {}
    for end_date, candidates in by_end.items():
        sa = [c for c in candidates if not c.get("is_ytd")]
        pool = sa if sa else candidates
        result[end_date] = max(pool, key=lambda x: x.get("filed", ""))
    return result


def _backfill_gross_profit(fields_out: dict) -> None:
    """
    gross_profitが欠落している四半期をrevenue − cost_of_revenueで
    逆算する。normalizer.py::_calc_gross_profit()と同一のアルゴリズム
    （[[LAYER3-GROSSPROFIT-BACKFILL-MISSING-1]]対応）。

    build_ticker_store()の全フィールドループが完了した後の後処理として
    呼び出すこと。config/sec_concept_definitions.jsonのフィールド定義
    順序ではrevenueが7番目・cost_of_revenueが30番目と大きく離れており、
    ループ内で参照するとcost_of_revenueが未処理のタイミングと衝突する
    ため、ループ完了後の一括後処理でなければrevenue・cost_of_revenue
    双方の最終値（マージ・欠落四半期逆算・Q4逆算まで完了済み）を確実に
    参照できない。

    fields_outを直接変更する（gross_profitキーのentriesに追加）。
    既存gross_profitエントリのend日付には追加しない（上書きしない）ため
    _merge_normalized_by_priority()の再実行は不要。出力エントリには
    is_impliedではなく"backfilled": Trueを設定する（normalizer.py側の
    既存規約を踏襲）。
    """
    gp_field = fields_out.get("gross_profit")
    rev_field = fields_out.get("revenue")
    cogs_field = fields_out.get("cost_of_revenue")
    if gp_field is None or rev_field is None or cogs_field is None:
        return

    gp_entries = gp_field["entries"]
    rev_entries = rev_field["entries"]
    cogs_entries = cogs_field["entries"]

    if not cogs_entries or not rev_entries:
        return

    # 既存gross_profitのend日付集合
    gp_ends = {e["end"] for e in gp_entries if not e.get("is_annual")}

    rev_by_end = _index_quarterly_by_end(rev_entries)
    cogs_by_end = _index_quarterly_by_end(cogs_entries)

    backfilled: list = []
    for end_date, rev_entry in rev_by_end.items():
        if end_date in gp_ends:
            continue
        cogs_entry = cogs_by_end.get(end_date)
        if cogs_entry is None or rev_entry.get("val") is None or cogs_entry.get("val") is None:
            continue
        # revenue・cost_of_revenueは符号が正なので単純差分
        gp_val = rev_entry["val"] - abs(cogs_entry["val"])
        backfilled.append({
            **{k: rev_entry[k] for k in ("end", "start", "fp", "fy", "form", "filed",
                                          "period_days", "is_ytd", "is_annual")
               if k in rev_entry},
            "val": gp_val,
            "accn": rev_entry.get("accn", ""),
            "backfilled": True,
        })

    if backfilled:
        gp_field["entries"] = sorted(gp_entries + backfilled, key=lambda x: x["end"])


def _backfill_operating_income(fields_out: dict) -> None:
    """
    operating_incomeが欠落している四半期を、GP法（gross_profit -
    research_and_development - selling_general_and_administrative、
    統合SGAを報告しない企業はselling_and_marketingで代替）で逆算する。
    `[[LAYER3-OI-RECONSTRUCTION-FALLBACK-GAP-1]]`対応。

    `common/sec_data/parser.py::_backfill_operating_income()`のGP法部分と
    同一アルゴリズムだが、以下2点は意図的にスコープを絞った（着手前の
    実消費者調査により判明した設計判断、詳細はBACKLOG_DONE.md参照）:

    - **四半期粒度が対象**（parser.py版は年次のみ）。Layer3の実消費者
      `src/tail/tail_dcf_bridge.py`・`quarterly_review_generator.py`は
      いずれも`get_latest_quarterly()`/`get_quarterly_series()`で
      四半期粒度のoperating_incomeを参照しており、年次粒度の消費者は
      現状存在しないため。
    - **pretax調整法のフォールバックは未実装**。gross_profit自体が
      構造的に存在しない銘柄（ASTS/XOM等、業態上COGS区分が無い）は
      本関数では回収できず欠損のまま残る。pretax法はGP法より複雑な
      非事業性項目ロジックを要し、raw XBRLの四半期/YTD変換という
      Layer3が既に持つ複雑性を重複実装するリスクがあるため、GP法単独
      でも対象10銘柄中6銘柄・92四半期を回収できると実測確認した上で
      今回は見送った（残るASTS/XOMは既知の残課題として記録）。
    - **RestructuringCharges控除
      （`[[OI-RECONSTRUCTION-MISSING-OPEX-LINES-1]]`相当）は未実装**。
      Layer3に`restructuring_charges`フィールド自体が未定義であり、
      四半期粒度でのYTD→四半期変換を新規実装するコストに見合う改善幅か
      要検討のため、GP法の基本形（RestructuringCharges控除なし）に
      とどめた。既存のparser.py側（年次・RestructuringCharges控除あり）
      と比べてやや粗い精度になる点に注意。

    **GP法入力の整合性ガード**（parser.py同型、案D）: revenue =
    gross_profit + cost_of_revenueという定義上の関係が成立しない
    （|gross_profit| > |revenue|）場合はgross_profitを信頼できない
    測定値とみなし使用しない。

    build_ticker_store()の全フィールドループ完了後、
    _backfill_gross_profit()の**後**に呼び出すこと（依存するgross_profit
    の最終値〈backfill済み含む〉を確実に参照するため）。

    fields_outを直接変更する（operating_incomeキーのentriesに追加）。
    既存operating_incomeエントリのend日付には追加しない（上書きしない）
    ため_merge_normalized_by_priority()の再実行は不要。出力エントリには
    "backfilled": True・"backfill_source": "reconstructed_gp"を設定する。
    """
    oi_field = fields_out.get("operating_income")
    gp_field = fields_out.get("gross_profit")
    rd_field = fields_out.get("research_and_development")
    sga_field = fields_out.get("selling_general_and_administrative")
    sm_field = fields_out.get("selling_and_marketing")
    rev_field = fields_out.get("revenue")
    if oi_field is None or gp_field is None:
        return

    oi_entries = oi_field["entries"]
    gp_entries = gp_field["entries"]
    if not gp_entries:
        return

    oi_ends = {e["end"] for e in oi_entries if not e.get("is_annual")}
    gp_by_end = _index_quarterly_by_end(gp_entries)
    rd_by_end = _index_quarterly_by_end(rd_field["entries"]) if rd_field else {}
    sga_by_end = _index_quarterly_by_end(sga_field["entries"]) if sga_field else {}
    sm_by_end = _index_quarterly_by_end(sm_field["entries"]) if sm_field else {}
    rev_by_end = _index_quarterly_by_end(rev_field["entries"]) if rev_field else {}

    backfilled: list = []
    for end_date, gp_entry in gp_by_end.items():
        if end_date in oi_ends:
            continue
        gp_val = gp_entry.get("val")
        if gp_val is None:
            continue

        # スケール不一致ガード（着手時の実データ検証で発見、案外の追加分）:
        # 52/53週決算企業（JNJ等）で、fp="FY"だがis_annual=Falseに分類
        # された年次スケールの値が"四半期"として紛れ込むケースが確認された
        # （JNJ実測: gross_profitのみend=2023-01-01/2023-12-31でfp="FY"・
        # 年次スケール$13.8B/$14.6Bとなる一方、同end日のR&D/SGAは
        # fp="Q4"・四半期スケール$3-5Bのまま——スケールの異なる値を
        # 引き算すると意味のない値が生成される。この期間分類自体の不整合は
        # `[[LAYER3-ANNUAL-CLASSIFICATION-DROPS-DATA-1]]`とは別種の問題
        # のため新規登録し、本関数側では逆算対象から機械的に除外する
        # 防御ガードのみ追加する）。
        if gp_entry.get("fp") == "FY":
            continue

        # GP法入力の整合性ガード（parser.py同型、案D）
        rev_entry = rev_by_end.get(end_date)
        rev_val = rev_entry.get("val") if rev_entry else None
        if rev_val is not None and abs(gp_val) > abs(rev_val):
            continue

        rd_entry = rd_by_end.get(end_date)
        rd_val = rd_entry.get("val") if rd_entry else None
        if rd_val is None:
            continue

        sga_entry = sga_by_end.get(end_date)
        sga_val = sga_entry.get("val") if sga_entry else None
        if sga_val is None:
            sm_entry = sm_by_end.get(end_date)
            sga_val = sm_entry.get("val") if sm_entry else None
        if sga_val is None:
            continue

        oi_val = gp_val - rd_val - sga_val
        backfilled.append({
            **{k: gp_entry[k] for k in ("end", "start", "fp", "fy", "form", "filed",
                                         "period_days", "is_ytd", "is_annual")
               if k in gp_entry},
            "val": oi_val,
            "accn": gp_entry.get("accn", ""),
            "backfilled": True,
            "backfill_source": "reconstructed_gp",
        })

    if backfilled:
        oi_field["entries"] = sorted(oi_entries + backfilled, key=lambda x: x["end"])


def build_ticker_store(ticker: str) -> dict | None:
    """
    1銘柄分のLayer3データ（32フィールド）を構築する。

    戻り値の構造:
    {
      "ticker": "AAPL",
      "generated_at": "...",
      "layer2_schema_version": "1.0",
      "fields": {
        "operating_cash_flow": {
          "source_tag": "NetCashProvidedByUsedInOperatingActivities",
          "entries": [...]
        },
        ...
      }
    }
    """
    ticker = ticker.upper()
    company_facts = load_company_facts(ticker)
    if company_facts is None:
        return None

    concept_defs = load_concept_definitions()
    fields_def = concept_defs.get("fields", {})

    fields_out: dict[str, dict] = {}

    for field_name, field_def in fields_def.items():
        # ticker_overrides（[[SOFI-TICKER-RESTRICTIONS-NOT-MIGRATED-1]]
        # 対応）: 抽出処理そのものに入る前に、この銘柄・フィールドに
        # 単純除外/概念差し替えの設定が無いか判定する。
        override = _get_ticker_field_override(concept_defs, ticker, field_name)
        if override and override.get("action") == "exclude":
            # 抽出処理自体をスキップする（quarterly.py::build_raw_table()の
            # excluded判定と同型）。
            raw_entries, source_tag = [], None
        elif override and override.get("action") == "override_concept":
            # candidatesを銘柄固有タグ1件に完全に置き換えてから、通常の
            # 抽出処理を行う（複数候補タグの混在によりタグ採用が不定に
            # なる問題を避けるため、SOFI等のnoteが要求する「単一タグに
            # 固定」を実現する）。
            override_field_def = dict(field_def)
            override_field_def["candidates"] = [override["override_concept"]]
            raw_entries, source_tag = extract_field_raw_entries(
                company_facts, override_field_def, field_name, ticker,
            )
        else:
            raw_entries, source_tag = extract_field_raw_entries(
                company_facts, field_def, field_name, ticker,
            )

        if field_name in NO_CANDIDATE_MERGE_FIELDS:
            # NO_CANDIDATE_MERGE_FIELDSはextract_field_raw_entries()が
            # _process_entries()止まりの未正規化エントリを返すため、ここで正規化する。
            normalized_entries = _normalize_field_entries(raw_entries)
        else:
            # 候補マージ対象フィールドはextract_field_raw_entries()内
            # （_merge_candidate_entries）で既に正規化済み。ここで再度
            # _normalize_field_entries()を適用すると、複数タグ由来の
            # エントリが混在した状態で再度FYチェーン判定が走り、
            # クロスタグ混入バグを再発させかねないため適用しない。
            normalized_entries = raw_entries

        # cross_filing_tags（NVDA short_term_investments等、
        # [[SOFI-TICKER-RESTRICTIONS-NOT-MIGRATED-1]]対応）: 期間・フォーム
        # 条件付きで複数タグを合算した値を、既存エントリに無いend_dateに
        # のみ追加する（GrossProfitバックフィルと同様、既存エントリを
        # 上書きしないため_merge_normalized_by_priority()の再実行は不要）。
        cross_filing_entries = _apply_cross_filing_tags(
            company_facts, concept_defs, ticker, field_name, field_def.get("unit", "USD"),
        )
        if cross_filing_entries:
            existing_ends = {e["end"] for e in normalized_entries}
            added = [e for e in cross_filing_entries if e["end"] not in existing_ends]
            if added:
                normalized_entries = sorted(normalized_entries + added, key=lambda x: x["end"])

        # 欠落四半期逆算（[[LAYER3-ANCHOR-MISSING-LOOKBACK-WINDOW-1]]対応）は
        # ここではなく_merge_candidate_entries()内、タグ単位・クロスタグ
        # マージ前に適用済み（マージ後に適用すると、同一end_dateを持つYTD
        # 累計エントリと単独四半期エントリが競合し、YTD側がマージの時点で
        # 破棄されてしまうため。詳細は_merge_candidate_entries()のdocstring
        # 参照）。実行順序: ①タグ正規化 → ②欠落四半期逆算（タグ単位、上記
        # extract_field_raw_entries内） → ③クロスタグマージ（同）→
        # ④Q4逆算（下記、マージ後の統一系列に対して既存通り適用） →
        # ⑤CapEx符号正規化（末尾、既存）。④が③の後段である理由: Q4逆算は
        # 複数タグから拾い集めた四半期を使うことに意味があるため、タグ単位に
        # 分離する必要がない（②とは異なりクロスタグ混入のリスクがない）。

        # field_nameがQ4_IMPLIED_FIELDS（q4_implied.py側、PascalCase/
        # snake_case両対応）に該当しない場合はbuild_q4_implied_entries()
        # 内部のガードにより空リストが返るため、ここでの事前フィルタは不要。
        # is_ytd=Trueの未解決エントリはQ1〜Q3候補から除外する（normalizer.py
        # の対応する呼び出し箇所と同様。未解決YTD累計自体の除外は本ループ
        # 後段で行うが、Q4逆算の入力としては先に除外しておく必要がある）。
        annual_for_q4 = [e for e in normalized_entries if e.get("is_annual")]
        quarterly_for_q4 = [
            e for e in normalized_entries
            if not e.get("is_annual") and not e.get("is_ytd")
        ]
        q4_list = build_q4_implied_entries(annual_for_q4, quarterly_for_q4, field_name)
        if q4_list:
            existing_ends = {e["end"] for e in normalized_entries if not e.get("is_annual")}
            added = [e for e in q4_list if e["end"] not in existing_ends]
            if added:
                normalized_entries = sorted(normalized_entries + added, key=lambda x: x["end"])

        # 未解決YTD累計の除外（is_ytd=Trueの残骸を四半期として誤計上しない）
        normalized_entries = [
            e for e in normalized_entries
            if e.get("is_annual") or not e.get("is_ytd")
        ]

        sign_normalize = field_def.get("sign_normalize")
        normalized_entries = _apply_sign_normalize(normalized_entries, sign_normalize)

        fields_out[field_name] = {
            "source_tag": source_tag,
            "category": field_def.get("category"),
            "entries": normalized_entries,
        }

    # GrossProfitバックフィル（[[LAYER3-GROSSPROFIT-BACKFILL-MISSING-1]]対応）。
    # 全フィールドループ完了後の後処理。revenue・cost_of_revenue双方の
    # 最終値を参照する必要があるため、ループ内ではなくここで実行する
    # （理由は_backfill_gross_profit()のdocstring参照）。
    _backfill_gross_profit(fields_out)

    # OperatingIncomeバックフィル（[[LAYER3-OI-RECONSTRUCTION-FALLBACK-
    # GAP-1]]対応）。gross_profitのbackfill結果に依存するため、必ず
    # _backfill_gross_profit()の後に実行する（理由は
    # _backfill_operating_income()のdocstring参照）。
    _backfill_operating_income(fields_out)

    return {
        "ticker": ticker,
        "generated_at": datetime.now().isoformat(),
        "layer2_schema_version": concept_defs.get("_schema_version"),
        "fields": fields_out,
    }


def get_field_entries(store: dict, field_name: str) -> list:
    """
    build_ticker_store()の戻り値からフィールド名を指定してentriesを
    取り出す（reader.py::get_quarterly_series()等と同じget_接頭辞の
    既存命名慣習に準拠したLayer3用アクセサ、フェーズC対応）。
    """
    return store.get("fields", {}).get(field_name, {}).get("entries", [])


def get_quarterly_series(store: dict, field_name: str) -> list:
    """
    is_annual・is_ytd両方を除外した四半期エントリをend日昇順で返す
    （reader.py::get_quarterly_series(normalized, field_name)のLayer3版、
    移行実装計画フェーズD Step1対応）。

    reader.py側は第一引数にnormalize()の戻り値（PascalCaseキー、
    fields[field_name]がentriesリストそのもの）を取るのに対し、本関数は
    build_ticker_store()の戻り値（snake_caseキー、fields[field_name]が
    {source_tag, category, entries}のdict）を取る。entries自体は
    quarterly.py::_process_entries()を両者が共通で再利用しているため
    同一shape（end/start/val/accn/fp/fy/form/filed/period_days/is_ytd/
    is_annual、Layer3側はsource_tagが追加で付与される）。

    呼び出し元がbuild_ticker_store()を1回呼んでstoreを保持し、同一
    ticker内の複数フィールド参照に使い回す設計を前提とする（第一引数を
    tickerではなくstore dictにしているのはget_field_entries()の既存
    シグネチャに揃えたもの。quarterly_review_generator.py等、1銘柄で
    get_latest_quarterly()を複数フィールド分呼ぶ既存消費者の呼び出し
    パターンに合わせ、build_ticker_store()の重複呼び出しを避ける）。

    この段階では新規追加のみで、既存normalized/経由の呼び出し
    （reader.py::get_quarterly_series()）には一切影響しない。
    """
    entries = get_field_entries(store, field_name)
    quarterly = [
        e for e in entries
        if not e.get("is_annual") and not e.get("is_ytd")
    ]
    return sorted(quarterly, key=lambda x: x["end"])


def get_latest_quarterly(store: dict, field_name: str) -> dict | None:
    """
    get_quarterly_series()の最新1件（末尾）を返す。空ならNone
    （reader.py::get_latest_quarterly()のLayer3版、フェーズD Step1対応）。
    """
    series = get_quarterly_series(store, field_name)
    return series[-1] if series else None


def get_long_term_debt_latest(store: dict) -> float:
    """
    long_term_debtの非NULLエントリのうち最新end日の値を返す
    （reader.py::SECReader.get_lt_debt_from_normalized()のLayer3版、
    フェーズD Step2-1対応）。

    normalized/へのフォールバックは行わない。2026-08-06のSEC EDGAR
    直接照合（companyconcept API）で、両者が食い違う3銘柄
    （RCAT/SPIR/CPRT）全てでLayer3側の値がより正確・現行であることを
    確認済み（`[[LAYER3-SHARESDILUTED-TAG-GAP-1]]`調査と同一セッション、
    詳細はチャット記録参照）:
    - RCAT: normalized/は2023-10-31時点の値のまま2年以上停止していた
      （candidates不足）。Layer3は2026-03-31まで正しく追随
    - CPRT: normalized/は2023-04-30時点の値のまま停止。Layer3は
      2024-07-31時点の正しい$0（実際の負債完済）まで追随
    - SPIR: 同一accn・同一end日で、normalized/はLongTermDebt
      （$103.7M、流動/非流動区分を反映しない総額系タグ）を採用する
      一方、Layer3はLongTermDebtNoncurrent（$0、優先）を採用。
      SEC EDGARでLongTermDebtCurrent=$100Mを確認し、当該四半期の
      長期負債がほぼ全額流動負債へ再分類されていたことを検証済み。
      normalized/の値をそのまま使うとshort_term_debtとの二重計上
      リスクがある

    エントリが無い場合は0.0を返す（呼び出し元は`bs.get("long_term_
    debt") or get_long_term_debt_latest(store)`のようなフォールバック
    パターンで使うことを想定）。
    """
    entries = [e for e in get_field_entries(store, "long_term_debt") if e.get("val") is not None]
    if not entries:
        return 0.0
    return float(max(entries, key=lambda e: e["end"])["val"])


def save_ticker_store(ticker: str, store: dict) -> str:
    """common/sec_data/store_v2/{TICKER}.json へ保存する（新規パス、既存ファイルは変更しない）。"""
    os.makedirs(STORE_V2_DIR, exist_ok=True)
    path = os.path.join(STORE_V2_DIR, f"{ticker.upper()}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2)
    return path


def build_and_save(ticker: str) -> str | None:
    store = build_ticker_store(ticker)
    if store is None:
        return None
    return save_ticker_store(ticker, store)


if __name__ == "__main__":
    import sys

    tickers = sys.argv[1:]
    if not tickers:
        print("使い方: python -m common.sec_data.layer3_builder TICKER [TICKER ...]")
        sys.exit(1)

    for t in tickers:
        path = build_and_save(t)
        if path:
            print(f"[{t}] 生成完了 -> {path}")
        else:
            print(f"[{t}] company_facts.json が見つからずスキップ")
