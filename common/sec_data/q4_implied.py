"""
common/sec_data/q4_implied.py
責務: 「FY年次値 - (Q1+Q2+Q3) = Q4」のQ4逆算ロジックの単一集約実装。

[[Q4-IMPLIED-CALC-TRIPLICATION-1]]対応（移行実装計画フェーズB）。
従来normalizer.py::_build_q4_implied_entries()・
ttm_calculator.py::_build_q4_quarterly_entries()・
src/value/stonks_silo/financial_trend_calculator.py::_build_q4_implied()
の3箇所に独立実装されていたロジックを本モジュールに統一する。

適用範囲のガード（重要）:
この近似（FY年次値からQ1+Q2+Q3を差し引く）はフロー系（累積可能）
フィールドでのみ数学的に有効であり、shares系（加重平均株式数）や
stock系（残高スナップショット）に適用すると符号反転等の無意味な値を
生む（layer3_builder.py実装中〈フェーズA〉にshares_diluted等で実際に
発生・修正したバグ）。このため本モジュールはQ4_IMPLIED_FIELDS
（フロー系フィールドの許可リスト）に含まれるfield_nameでのみ計算を
実行するガードを設ける。

Q4_IMPLIED_FIELDSの構成について:
normalizer.py::Q4_IMPLIED_FIELDS（13フィールド）とttm_calculator.py::
FLOW_FIELDS（14フィールド、Q4逆算にも同じ集合を使用）は、既存実装の
時点で完全には一致していなかった（FinanceLeasePmts・BuybackはttmのFLOW_
FIELDSのみに存在、_COGSはnormalizer側のみに存在）。実データ確認の結果、
FinanceLeasePmts（26銘柄）・Buyback（65銘柄）はttm_calculator.py側の
Q4逆算で実際に非空のimpliedエントリを生成していた（normalizer.py側は
この2フィールドをQ4逆算対象にしないため、ttm側の独自生成が実質的な
唯一の供給源になっている）。フェーズB集約時にこの2フィールドを除外すると
ttm/出力からimpliedエントリが消失し「値を変えない集約」という要件に反する
ため、両モジュールの既存許可フィールドの**和集合**（15フィールド）を
本モジュールのガードとして採用する。いずれも実際に加算可能なフロー系
概念であり、除外対象のshares/stock系フィールドとは性質が異なる。

[[LAYER3-SGA-Q4-MISSING-1]]対応（2026-07-29）でselling_general_and_administrative
（SGA）を追加し16フィールドとした（詳細は下記Q4_IMPLIED_FIELDS定義部のコメント参照）。

[[LAYER3-TTM-REGRESSION-NEWFIELD-BLINDSPOT-1]]対応（2026-07-29投資調査）:
Layer2スキーマ追加時に新設された新規6フィールド（short_term_investments・
total_liabilities・eps_basic・eps_diluted・cost_of_revenue・
selling_general_and_administrative）のうち、意図的にこのガード（延いてはQ4逆算
対象）に含めないフィールドとその理由:
- short_term_investments・total_liabilities: category="stock"（期末残高の
  スナップショット）。「年次−Q1−Q2−Q3」という近似はSTOCK系には数学的に無意味
  （本docstring冒頭「適用範囲のガード」参照）。
- eps_basic・eps_diluted: 比率フィールド（純利益÷加重平均株式数）。四半期ごとに
  分母（加重平均株式数）が変動し、赤字/黒字混在期の希薄化証券の扱いも変わるため、
  単純合算・差分が数学的に意味を持たない（shares系と同種のリスク）。
cost_of_revenue（_COGS）・selling_general_and_administrative（SGA）の2フィールドは
上記2グループと異なりFLOW系（期間累積可能）であるため、既にQ4_IMPLIED_FIELDSの
対象。
"""

from __future__ import annotations

from datetime import date

from .fact_selection import combine_first, value_asof

# フロー系（累積・加算可能）フィールドの許可リスト（PascalCase）。
# normalizer.py::Q4_IMPLIED_FIELDS ∪ ttm_calculator.py::FLOW_FIELDS。
# 上記docstring参照。
# [[LAYER3-SGA-Q4-MISSING-1]]対応（2026-07-29）: selling_general_and_administrative
# （Layer2スキーマ追加時の新規フィールド、旧normalizer.py/ttm_calculator.pyには
# 存在しないため上記の和集合には含まれていなかった）を追加。42銘柄・171四半期で
# Q4が恒常的に欠落していたバグの是正。SGA自体はSM同様フロー系（累積可能）の
# 費用項目であり、他フィールドと同じ「年次−Q1−Q2−Q3」近似が数学的に妥当。
Q4_IMPLIED_FIELDS = frozenset({
    "Revenue", "_COGS",
    "OCF", "ICF", "CFF", "CapEx", "FinanceLeasePmts",
    "RD", "SM", "SBC", "DA", "SGA",
    "NetIncome", "OperatingIncome", "GrossProfit",
    "Buyback",
    # [[HYPECORE-STAGE-TIMESCALE-1]] 段B-3（2026-11-05）: 資本の使い方。どれも期間で足せるフロー。
    # 性質上負にならない項目（Layer2 の non_negative 欄）は、逆算したQ4が負なら欠け（下の NON_NEGATIVE の規則）
    "DividendsPaid", "DividendsPerShare", "EquityIssuance", "OptionExercise", "StockIssuedAcquisitions",
})

# [[LAYER3-Q4-IMPLIED-NOT-MIGRATED-1]]対応: layer3_builder.py（Layer3、
# 32フィールド・snake_case命名）からもPascalCase前提のQ4_IMPLIED_FIELDS/
# CapEx符号処理判定をそのまま利用できるようにするための、
# snake_case→PascalCaseの正規化テーブル。config/sec_concept_definitions.json
# の命名（SEC_EDGAR_LAYER_DESIGN.md 4-1章参照）と対応させている。
# 既存呼び出し元（normalizer.py・ttm_calculator.py・
# financial_trend_calculator.py）はPascalCaseのまま呼び出すため、
# このテーブルに無いキーはbuild_q4_implied_entries()内で無変換のまま
# 扱われ、既存呼び出し元は無改修で動作し続ける。
_SNAKE_TO_PASCAL = {
    "revenue": "Revenue",
    "cost_of_revenue": "_COGS",
    "operating_cash_flow": "OCF",
    "investing_cash_flow": "ICF",
    "financing_cash_flow": "CFF",
    "capital_expenditure": "CapEx",
    "finance_lease_payments": "FinanceLeasePmts",
    "research_and_development": "RD",
    "selling_and_marketing": "SM",
    "stock_based_compensation": "SBC",
    "depreciation_and_amortization": "DA",
    "net_income": "NetIncome",
    "operating_income": "OperatingIncome",
    "gross_profit": "GrossProfit",
    "buyback": "Buyback",
    "selling_general_and_administrative": "SGA",
    "dividends_paid": "DividendsPaid",
    "dividends_per_share": "DividendsPerShare",
    "equity_issuance_proceeds": "EquityIssuance",
    "option_exercise_proceeds": "OptionExercise",
    "stock_issued_acquisitions": "StockIssuedAcquisitions",
}


# 性質上負にならないフローの項目（2026-11-05、[[HYPECORE-STAGE-TIMESCALE-1]] 段B-3の後）:
# 逆算したQ4が負なら、そのQ4は作らない（欠け）。呼び出し元が marks_out を渡せば印（Q4_NEGATIVE_MARK と元の値）を残す。
# 項目の性質は config/sec_concept_definitions.json の各項目の "non_negative": true の1つの欄（PascalCase に直して持つ）。
# 理由: 10-Kの年間の欄に1四半期分の額を申告する会社がある（CEGの1株配当 FY2025 は0.3878＝Q1〜Q3と同じ額で、年次−Q1〜Q3＝−0.78）。
# [[TANUKI-CALC-CONTINUITY-1]] (2)（2026-11-12の決定）: 既存の8項目（buyback・SBC・R&D・販売費・SG&A・売上・売上原価・D&A）にも当てた。
# 粗利は実際に負になる（KULR 2025年のQ4）ので対象外。千ドル単位の丸めの負（TASK・FICO・ASTSの−1,000ドル）は残す（rounding_grain、案1）
# （decimals が company_facts に無いため、逆算に使う値がすべて1,000の倍数なら粒度1,000とみなす）。
# CapEx は段2の規則（四半期は符号つき・TTMが負なら欠け）のままで対象外
Q4_NEGATIVE_MARK = "q4_implied_negative"
_NON_NEGATIVE = None


def non_negative_fields() -> frozenset:
    """Layer2（config/sec_concept_definitions.json）で "non_negative": true の項目（PascalCase）"""
    global _NON_NEGATIVE
    if _NON_NEGATIVE is None:
        import json as _json
        import os as _os
        path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))),
                             "config", "sec_concept_definitions.json")
        try:
            with open(path, encoding="utf-8") as f:
                fields = (_json.load(f) or {}).get("fields") or {}
        except (OSError, ValueError):
            fields = {}
        _NON_NEGATIVE = frozenset(_SNAKE_TO_PASCAL.get(k, k) for k, d in fields.items() if (d or {}).get("non_negative"))
    return _NON_NEGATIVE


# [[LAYER3-FIRST-FILED-DATE-1]]（2026-10-21）: Q4 の逆算は「年次と同じ時点の値どうし」にする。
#   年次: その会計年度の最初の 10-K の値（first_val）
#   Q1〜Q3: その 10-K の提出日（年次の first_filed）以前に申告された値のうち最も新しいもの（value_asof()）
# 後の 10-K は年次を事業の売却等で組み替えて載せ直すが、10-Q は2年前の Q1〜Q3 を載せ直さないため、
# 最新どうしの差では基準が混ざる（docs/architecture/Q4_IMPLIED_BASIS_MIX.md、APP 2023Q4 の売上が負）。
# Q1〜Q3 を最初の申告（first_val）にすると、10-K の前に載せ直された四半期で年次と時点がずれる（AVAV FY2026）。
# その時点の値が無い期は、今までの計算（最新どうしの差）にし、印 Q4_BASIS_KEY = "latest" を付ける。
Q4_BASIS_KEY = "q4_basis"
Q4_BASIS_LATEST = "latest"


ROUNDING_GRAIN = 1000
Q4_ROUNDED_KEY = "q4_rounded"


def q4_inputs(annual: dict, quarters: list) -> list:
    """Q4の逆算に実際に使う値（年次とQ1〜Q3）。implied_q4_value と同じ分岐"""
    as_of = annual.get("first_filed")
    q_vals = [value_asof(q, as_of) for q in quarters] if as_of else [None]
    if annual.get("first_val") is not None and all(v is not None for v in q_vals):
        return [annual["first_val"]] + list(q_vals)
    return [annual["val"]] + [q["val"] or 0 for q in quarters]


def rounding_grain(values: list) -> int | None:
    """[[TANUKI-CALC-CONTINUITY-1]] (2) 丸めの許容幅（2026-11-12の決定、案1）: 逆算に使う値がすべて1,000の倍数なら粒度1,000
    （千ドル単位の申告＝decimals=-3 と同じ意味。company_facts に decimals が無いので値から決める）。倍数でない値が1つでもあれば None"""
    for v in values:
        if v is None or abs(v - round(v)) > 1e-9 or round(v) % ROUNDING_GRAIN:
            return None
    return ROUNDING_GRAIN


def implied_q4_value(annual: dict, quarters: list, field_name: str) -> tuple[float, dict]:
    """年次と Q1〜Q3 から逆算した Q4 の値と、エントリに付け足すキー（first_filed・first_val か、印）を返す。

    q4_implied の Q4 と、normalizer.py の欠落四半期の逆算のうち年次から末尾の四半期を出すもの（同じ Q4）が使う。"""
    as_of = annual.get("first_filed")
    q_vals = [value_asof(q, as_of) for q in quarters] if as_of else [None]
    if annual.get("first_val") is not None and all(v is not None for v in q_vals):
        q4_val = annual["first_val"] - sum(q_vals)
        extra: dict = {}
    else:
        fy_val = annual["val"]
        q4_val = fy_val - sum(q["val"] or 0 for q in quarters)
        extra = {Q4_BASIS_KEY: Q4_BASIS_LATEST}
    # first_filed・first_val は、ほかの派生の値と同じく最初の申告どうしで作る（[[STAGE-V2-ROLLOUT-1]]で使う）
    extra.update(combine_first([(1, annual)] + [(-1, q) for q in quarters]))
    # 設備投資のQ4は符号つき（年次・Q1〜Q3は差し引きの前に abs 済み。年次がQ1〜Q3の合計より小さい年は負、
    # [[FCF-DEFINITION-SINGLE-SOURCE-1]] 段2、2026-11-03。以前はここで abs）
    return q4_val, extra


def build_q4_implied_entries(
    annual_entries: list,
    quarterly_entries: list,
    field_name: str,
    marks_out: list | None = None,
) -> list:
    """
    年次エントリ（is_annual=True）とQ1〜Q3の四半期エントリから、
    Q4 implied エントリを生成する。

    Q4 = FY年次値 - (Q1+Q2+Q3の合計)

    field_nameはPascalCase（"CapEx"等、normalizer.py・ttm_calculator.py・
    financial_trend_calculator.py由来）・snake_case（"capital_expenditure"等、
    layer3_builder.py由来）のどちらで渡してもよい（[[LAYER3-Q4-IMPLIED-
    NOT-MIGRATED-1]]対応）。内部で_SNAKE_TO_PASCALによりPascalCase相当に
    正規化してから判定する。

    - 正規化後のfield_nameがQ4_IMPLIED_FIELDSに含まれない場合は
      空リストを返す（shares/stock系フィールドへの誤適用防止ガード）。
    - 同一end日付の四半期エントリが既にquarterly_entries内に存在する
      場合はそのFYをスキップする（重複生成防止、旧ttm_calculator.py
      ::BUG-TTM-Q4DUP-1ガードと同等）。
    - "CapEx"のq4_valは符号つき（2026-11-03、[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段2。年次・Q1〜Q3は
      差し引きの前に abs 済み〈fact_selection.abs_entry〉。以前は[[CAPEX-SIGN-UNNORMALIZED-1]]・
      [[RICE-TTM-CAPEX-SUM-SIGN-1]]対応でここでも abs していた）。
    - 未来のFY（fy_end > 本日）はアナリスト予想化を避けるため対象外。
    - Q4の値は、最初の10-Kの年次と、その提出日以前に申告されたQ1〜Q3の最も新しい値の差
      （年次と同じ時点の値どうし。implied_q4_value()、[[LAYER3-FIRST-FILED-DATE-1]]）。その時点の値が
      無ければ最新どうしの差にし、q4_basis="latest"の印を付ける。
    - [[LAYER3-DA-SBC-CANDIDATE-REGRESSION-1]]対応: 年次エントリと
      採用するQ1〜Q3の3エントリ、計4エントリの"source_tag"
      （layer3_builder.py::_merge_candidate_entries()が付与。
      candidatesが複数タグある場合、(end_date, is_annual)キーごとに
      異なるタグが選ばれることがある）が完全一致しない場合はその
      FYをスキップする（値を生成しない）。normalizer.py・
      ttm_calculator.py・financial_trend_calculator.py等の既存呼び出し元
      はエントリに"source_tag"キーを持たないため、4エントリとも
      source_tag=Noneとなり、完全一致（全てNone）として扱われ、
      従来通りQ4逆算が発火する（無改修で動作継続）。
    """
    canonical_field_name = _SNAKE_TO_PASCAL.get(field_name, field_name)
    if canonical_field_name not in Q4_IMPLIED_FIELDS:
        return []

    today = date.today().isoformat()
    existing_ends = {e["end"] for e in quarterly_entries if not e.get("is_annual")}

    result: list = []
    for annual in annual_entries:
        fy_end = annual.get("end", "")
        fy_start = annual.get("start", "")
        fy_val = annual.get("val")

        if not fy_end or fy_val is None:
            continue
        if fy_end > today:
            continue
        if fy_end in existing_ends:
            continue

        fy_qs = [
            e for e in quarterly_entries
            if e.get("end", "") < fy_end
            and e.get("start", "") >= fy_start
            and not e.get("is_annual")
        ]
        top3 = sorted(fy_qs, key=lambda x: x["end"], reverse=True)[:3]
        if len(top3) < 3:
            continue

        source_tags = {annual.get("source_tag")} | {e.get("source_tag") for e in top3}
        if len(source_tags) != 1:
            continue

        q3_end = sorted(top3, key=lambda x: x["end"], reverse=True)[0]["end"]
        q4_val, extra = implied_q4_value(annual, top3, canonical_field_name)
        if q4_val is not None and q4_val < 0 and canonical_field_name in non_negative_fields():
            grain = rounding_grain(q4_inputs(annual, top3))
            if grain and abs(q4_val) <= grain:   # 丸めた年間と丸めた累計の差は最大で粒度1つぶん（TASK・FICO・ASTSの−1,000）
                extra[Q4_ROUNDED_KEY] = grain      # 丸めの範囲の負: 逆算の値のまま残す（0で埋めない）
            else:
                # 性質上負にならない項目の負のQ4は作らない（欠け）。印は呼び出し元が残す（上の NON_NEGATIVE の規則）
                if marks_out is not None:
                    marks_out.append({"end": fy_end, "mark": Q4_NEGATIVE_MARK, "implied_val": q4_val,
                                      "annual_accn": annual.get("accn", ""), "source_tag": annual.get("source_tag")})
                continue

        try:
            period_days = (date.fromisoformat(fy_end) - date.fromisoformat(q3_end)).days
        except (ValueError, TypeError):
            period_days = 90

        result.append({
            "end": fy_end,
            "start": q3_end,
            "val": q4_val,
            "fp": "Q4",
            "fy": annual.get("fy"),
            "form": "10-K",
            "filed": annual.get("filed", ""),
            "accn": annual.get("accn", ""),
            "period_days": period_days,
            "is_ytd": False,
            "is_annual": False,
            "is_implied": True,
            **extra,
        })

    return result
