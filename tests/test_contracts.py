"""
tests/test_contracts.py

common/sec_data/contracts.py のユニットテスト。
QUALITY-GATES-EPIC-1 Phase 3a（Gate2第一段階: 正規化契約の型導入）。

実行方法:
    python -m pytest tests/test_contracts.py -v
"""

import pytest

import json

from common.sec_data.contracts import (
    Classification,
    ContractViolation,
    EntryProvenance,
    FCFSeries,
    FinancialEntry,
    GrowthVerdict,
    validate_entries,
    validate_fields,
    validate_field_classification,
)


def _entry(**overrides):
    base = {
        "end": "2025-12-31", "start": "2025-10-01", "val": 100.0,
        "accn": "0000000000-00-000000", "fp": "Q4", "fy": 2025,
        "form": "10-Q", "filed": "2026-02-01", "period_days": 91,
        "is_ytd": False, "is_annual": False,
    }
    base.update(overrides)
    return base


# ─────────────────────────────────────────────
# 規約B: FinancialEntry（quarterly.py標準エントリ形状）
# ─────────────────────────────────────────────

class TestFinancialEntry:
    def test_valid_entry_round_trips(self):
        d = _entry()
        entry = FinancialEntry.from_dict(d)
        assert entry.end == "2025-12-31"
        assert entry.val == 100.0
        assert entry.is_implied is False
        assert entry.anomaly is False
        assert entry.backfilled is False
        assert entry.provenance.is_empty()

    def test_optional_keys_default_false_when_absent(self):
        entry = FinancialEntry.from_dict(_entry())
        assert entry.is_implied is False

    def test_optional_keys_respected_when_present(self):
        entry = FinancialEntry.from_dict(_entry(is_implied=True, anomaly=True, backfilled=True))
        assert entry.is_implied is True
        assert entry.anomaly is True
        assert entry.backfilled is True

    @pytest.mark.parametrize("missing_key", [
        "end", "start", "val", "accn", "fp", "fy", "form", "filed",
        "period_days", "is_ytd", "is_annual",
    ])
    def test_missing_required_key_raises(self, missing_key):
        d = _entry()
        del d[missing_key]
        with pytest.raises(ContractViolation):
            FinancialEntry.from_dict(d)

    def test_non_dict_raises(self):
        with pytest.raises(ContractViolation):
            FinancialEntry.from_dict(["not", "a", "dict"])  # type: ignore[arg-type]

    def test_to_dict_omits_falsy_optional_keys(self):
        entry = FinancialEntry.from_dict(_entry())
        d = entry.to_dict()
        assert "is_implied" not in d
        assert "anomaly" not in d
        assert "backfilled" not in d
        assert "_provenance" not in d

    def test_to_dict_includes_provenance_when_present(self):
        d = _entry(_provenance={"source_tag": "DebtLongtermAndShorttermCombinedAmount"})
        entry = FinancialEntry.from_dict(d)
        out = entry.to_dict()
        assert out["_provenance"] == {"source_tag": "DebtLongtermAndShorttermCombinedAmount"}

    def test_provenance_non_dict_raises(self):
        with pytest.raises(ContractViolation):
            FinancialEntry.from_dict(_entry(_provenance="not-a-dict"))


class TestValidateEntries:
    def test_valid_list_passes(self):
        entries = validate_entries("Revenue", [_entry(), _entry(end="2026-03-31")])
        assert len(entries) == 2

    def test_invalid_entry_error_includes_field_name(self):
        bad = _entry()
        del bad["val"]
        with pytest.raises(ContractViolation, match=r"\[Revenue\]"):
            validate_entries("Revenue", [bad])

    def test_validate_fields_checks_every_field(self):
        fields = {
            "Revenue": [_entry()],
            "LTDebt": [_entry(end="2026-03-31")],
        }
        # 例外が出なければOK
        validate_fields(fields)

    def test_validate_fields_raises_on_any_bad_field(self):
        bad = _entry()
        del bad["fy"]
        fields = {"Revenue": [_entry()], "LTDebt": [bad]}
        with pytest.raises(ContractViolation, match=r"\[LTDebt\]"):
            validate_fields(fields)


# ─────────────────────────────────────────────
# 規約③: EntryProvenance
# ─────────────────────────────────────────────

class TestEntryProvenance:
    def test_empty_by_default(self):
        assert EntryProvenance().is_empty()

    def test_from_dict_none_is_empty(self):
        assert EntryProvenance.from_dict(None).is_empty()

    def test_from_dict_round_trip(self):
        p = EntryProvenance.from_dict({"source_tag": "X", "duration_days": 365})
        assert p.source_tag == "X"
        assert p.duration_days == 365
        assert p.to_dict() == {"source_tag": "X", "duration_days": 365}

    def test_to_dict_partial(self):
        p = EntryProvenance(source_tag="X")
        assert p.to_dict() == {"source_tag": "X"}


# ─────────────────────────────────────────────
# 規約A: FCFSeries（fcf_listの順序規約）
# ─────────────────────────────────────────────

class TestFCFSeries:
    def test_descending_dates_pass(self):
        series = FCFSeries([300.0, 200.0, 100.0],
                            ["2026-03-31", "2025-03-31", "2024-03-31"])
        assert list(series) == [300.0, 200.0, 100.0]

    def test_ascending_dates_raise(self):
        """GROWTH-CAGR-SIGN-1相当の規約違反（順序取り違え）を検知できること"""
        with pytest.raises(ContractViolation, match="新しい順"):
            FCFSeries([100.0, 200.0, 300.0],
                      ["2024-03-31", "2025-03-31", "2026-03-31"])

    def test_mismatched_lengths_raise(self):
        with pytest.raises(ContractViolation):
            FCFSeries([100.0, 200.0], ["2025-03-31"])

    def test_no_dates_skips_validation(self):
        # reader.py::get_fcf_list() のように日付情報が失われている経路向け。
        # 誤った順序を渡しても例外にならない（検証不能なため素通り）。
        series = FCFSeries([100.0, 200.0, 300.0])
        assert list(series) == [100.0, 200.0, 300.0]

    def test_newest_and_oldest(self):
        series = FCFSeries([300.0, 200.0, 100.0],
                            ["2026-03-31", "2025-03-31", "2024-03-31"])
        assert series.newest == 300.0
        assert series.oldest == 100.0

    def test_newest_oldest_empty_series(self):
        series = FCFSeries([])
        assert series.newest is None
        assert series.oldest is None

    def test_newest_n(self):
        series = FCFSeries([500.0, 400.0, 300.0, 200.0, 100.0],
                            ["2026-03-31", "2025-03-31", "2024-03-31",
                             "2023-03-31", "2022-03-31"])
        top3 = series.newest_n(3)
        assert list(top3) == [500.0, 400.0, 300.0]
        assert isinstance(top3, FCFSeries)

    def test_list_like_indexing_and_slicing(self):
        series = FCFSeries([300.0, 200.0, 100.0],
                            ["2026-03-31", "2025-03-31", "2024-03-31"])
        assert series[0] == 300.0
        assert series[-1] == 100.0
        assert series[:2] == [300.0, 200.0]
        assert len(series) == 3
        assert sum(series) == 600.0

    def test_as_list_returns_plain_list(self):
        series = FCFSeries([300.0, 200.0], ["2026-03-31", "2025-03-31"])
        out = series.as_list()
        assert type(out) is list
        assert out == [300.0, 200.0]

    def test_equality_with_plain_list(self):
        series = FCFSeries([300.0, 200.0], ["2026-03-31", "2025-03-31"])
        assert series == [300.0, 200.0]


# ─────────────────────────────────────────────
# 規約C: フィールド分類の網羅性（GATE2-PHASE3B-1②）
# ─────────────────────────────────────────────

class TestValidateFieldClassification:
    """validate_field_classification()が、field_concepts辞書の全キーが
    分類セットのいずれかに属することを検証すること。ttm_calculator.pyの
    FLOW_FIELDS/STOCK_FIELDS/SHARES_FIELDS/EXCLUDED_FIELDSとquarterly.pyの
    FIELD_CONCEPTSが別ファイルで独立管理されており、新フィールド追加時に
    分類を忘れても黙って出力から消える問題（CurrentAssets/
    CurrentLiabilitiesの実例）の再発防止用契約チェックの単体テスト"""

    def test_no_violation_when_all_keys_classified(self):
        """全キーがいずれかの分類セットに属する場合は例外を送出しない"""
        field_concepts = {"A": 1, "B": 2, "C": 3}
        # 例外が送出されないことのみを確認（戻り値はNone）
        validate_field_classification(
            field_concepts,
            frozenset(["A"]), frozenset(["B"]), frozenset(["C"]),
        )

    def test_raises_when_a_field_is_unclassified(self):
        """いずれの分類セットにも属さないキーが1件でもあればContractViolationを
        送出する（意図的に分類漏れフィールドを追加した場合にテストが失敗する
        ことの裏付け）"""
        field_concepts = {"A": 1, "B": 2, "Unclassified": 3}
        with pytest.raises(ContractViolation) as exc_info:
            validate_field_classification(
                field_concepts,
                frozenset(["A"]), frozenset(["B"]),
            )
        assert "Unclassified" in str(exc_info.value)

    def test_raises_lists_all_unclassified_fields(self):
        """未分類キーが複数ある場合、全て列挙されること"""
        field_concepts = {"A": 1, "X": 2, "Y": 3}
        with pytest.raises(ContractViolation) as exc_info:
            validate_field_classification(field_concepts, frozenset(["A"]))
        message = str(exc_info.value)
        assert "X" in message
        assert "Y" in message

    def test_excluded_fields_set_is_a_valid_classification(self):
        """EXCLUDED_FIELDS相当の除外リストも正当な分類セットの1つとして
        扱われること（除外リストに入っているだけで違反にならない）"""
        field_concepts = {"Flow1": 1, "Stock1": 2, "InternalOnly": 3}
        validate_field_classification(
            field_concepts,
            frozenset(["Flow1"]), frozenset(["Stock1"]), frozenset(["InternalOnly"]),
        )

    def test_real_field_concepts_fully_classified(self):
        """Layer2フィールド定義（config/sec_concept_definitions.json::fields、
        snake_case・32キー）とttm_calculator.pyの実際の分類セットが現時点で
        矛盾なく全件分類されていることの統合確認（ttm_calculator.py
        モジュールロード時に同じチェックが既に走っているが、import副作用に
        依存しない明示的な回帰テストとしてここでも確認する）。

        フェーズC対応: ttm_calculator.py本体の突合対象がquarterly.py::
        FIELD_CONCEPTS（PascalCase）からLayer2フィールド定義（snake_case）へ
        切り替わったため、本テストも実際の本番コードと同じ参照元に揃える。
        quarterly.py::FIELD_CONCEPTS自体は、ttm_calculator.pyがこの完全性
        チェックの対象として消費しなくなった（Layer2定義に置き換わった）
        ため、FIELD_CONCEPTS単体を検証する同種のテストは維持しない
        （normalizer.py等はFIELD_CONCEPTSを直接参照するが、本チェックが
        保証する「flow/stock/shares/excludedのいずれかへの全件分類」という
        契約自体を要求してはいない）。"""
        from common.sec_data.layer3_builder import load_concept_definitions
        from common.sec_data.ttm_calculator import (
            FLOW_FIELDS, STOCK_FIELDS, SHARES_FIELDS, EXCLUDED_FIELDS,
        )
        layer3_field_defs = load_concept_definitions().get("fields", {})
        validate_field_classification(
            layer3_field_defs, FLOW_FIELDS, STOCK_FIELDS, SHARES_FIELDS, EXCLUDED_FIELDS,
        )


# ─────────────────────────────────────────────
# 規約D: enum風文字列の型化（GATE2-PHASE3B-1③-a）
# ─────────────────────────────────────────────

class TestGrowthVerdict:
    """GrowthVerdictがstr型として振る舞う（f-string補間・JSON serialize・
    ==比較のいずれも既存の生文字列と同じ結果になる）ことを検証する。

    Python 3.11以降、Enumの__str__/__format__はstr,Enumを継承していても
    デフォルトで`GrowthVerdict.PLAUSIBLE`というクラス名付き表記を返す仕様に
    変わっており、__str__のoverrideなしではf-string補間が期待通りに
    動作しない（実装時に発覚した罠）。__str__override後の挙動を固定する
    回帰テストとして重要。"""

    def test_equality_with_plain_string(self):
        assert GrowthVerdict.PLAUSIBLE == "PLAUSIBLE"
        assert GrowthVerdict.REVIEW == "REVIEW"
        assert GrowthVerdict.AGGRESSIVE == "AGGRESSIVE"
        assert GrowthVerdict.FLOOR_HIT_REVIEW == "FLOOR_HIT_REVIEW"

    def test_fstring_interpolation_matches_plain_string(self):
        """__str__overrideにより、f-string補間がクラス名付き表記
        （GrowthVerdict.PLAUSIBLE）ではなく素の文字列を返すこと"""
        assert f"{GrowthVerdict.PLAUSIBLE}" == "PLAUSIBLE"
        assert str(GrowthVerdict.REVIEW) == "REVIEW"

    def test_json_serialization_matches_plain_string(self):
        """str継承のため、json.dumpsが素の文字列としてシリアライズすること
        （.value付与不要）"""
        payload = {"verdict": GrowthVerdict.AGGRESSIVE}
        assert json.dumps(payload) == '{"verdict": "AGGRESSIVE"}'

    def test_used_directly_as_dict_value_in_template_string(self):
        """growth_sanity.py/pipeline.pyの実際の使い方（f"判定: {verdict}"の
        ようなreport.txt生成箇所）を模したテンプレート文字列生成で
        期待通りの出力になること"""
        verdict = GrowthVerdict.FLOOR_HIT_REVIEW
        line = f"判定         : {verdict}"
        assert line == "判定         : FLOOR_HIT_REVIEW"

    def test_unknown_member_raises_attribute_error(self):
        """存在しないメンバー参照（タイプミス）はAttributeErrorとして
        即座に検知される（規約Dの目的であるタイプミス防止の裏付け）"""
        with pytest.raises(AttributeError):
            GrowthVerdict.TYPO_NOT_A_REAL_MEMBER

    def test_all_four_expected_members_exist(self):
        """既存の4値（PLAUSIBLE/REVIEW/AGGRESSIVE/FLOOR_HIT_REVIEW）が
        全て定義されていること"""
        assert {m.value for m in GrowthVerdict} == {
            "PLAUSIBLE", "REVIEW", "AGGRESSIVE", "FLOOR_HIT_REVIEW",
        }


class TestClassification:
    """Classificationがstr型として振る舞う（f-string補間・JSON serialize・
    タプルメンバーシップ・辞書キーとしての利用のいずれも既存文字列と
    同じ結果になる）ことを検証する（GATE2-PHASE3B-1③-b）。

    GrowthVerdict同様、__str__のoverrideなしではPython 3.11+で
    f-string補間がクラス名付き表記になる罠があるため、その回帰テストも含む。"""

    def test_equality_with_plain_string(self):
        assert Classification.BUY == "BUY"
        assert Classification.WATCH == "WATCH"
        assert Classification.HOLD == "HOLD"
        assert Classification.TRIM == "TRIM"
        assert Classification.GROWTH_PREMIUM == "GROWTH_PREMIUM"
        assert Classification.SELL == "SELL"
        assert Classification.PASS == "PASS"

    def test_fstring_interpolation_matches_plain_string(self):
        """__str__overrideにより、f-string補間がクラス名付き表記
        （Classification.WATCH）ではなく素の文字列を返すこと"""
        assert f"{Classification.WATCH}" == "WATCH"
        assert str(Classification.GROWTH_PREMIUM) == "GROWTH_PREMIUM"

    def test_json_serialization_matches_plain_string(self):
        """str継承のため、json.dumpsが素の文字列としてシリアライズすること
        （.value付与不要。pipeline.pyのlatest.json/score_history.json
        永続化箇所が無改修で動作することの裏付け）"""
        payload = {"score": Classification.GROWTH_PREMIUM}
        assert json.dumps(payload) == '{"score": "GROWTH_PREMIUM"}'

    def test_used_directly_in_report_txt_template_string(self):
        """pipeline.pyのreport.txt生成（f"Classification: {score}"）を模した
        テンプレート文字列生成で期待通りの出力になること。
        report_consistency_check.pyのNG-3がこの行をregexで再パースするため
        重要な回帰テスト"""
        score = Classification.WATCH
        line = f"Classification: {score}"
        assert line == "Classification: WATCH"

    def test_tuple_membership_matches_plain_string_comparison(self):
        """pipeline.py 519/533行目のscore not in (Classification.SELL,
        Classification.PASS)相当のタプルメンバーシップ判定が、
        素の文字列同士の場合と同じ結果になること"""
        assert Classification.WATCH not in (Classification.SELL, Classification.PASS)
        assert Classification.SELL in (Classification.SELL, Classification.PASS)
        # 素の文字列との混在比較（JSON経由で読み込んだ値との比較を想定）
        assert "WATCH" not in (Classification.SELL, Classification.PASS)
        assert Classification.SELL in ("SELL", "PASS")

    def test_usable_as_dict_key_interchangeably_with_plain_string(self):
        """daily_pick.py::CATEGORY_PRIORITYのような辞書キー利用パターンで、
        Enumメンバーと素の文字列キーが同一キーとして扱われること
        （hashがstr継承のため一致する）"""
        priority = {Classification.BUY: 0, Classification.WATCH: 1}
        assert priority["BUY"] == 0
        assert priority[Classification.WATCH] == 1

    def test_unknown_member_raises_attribute_error(self):
        """存在しないメンバー参照（タイプミス）はAttributeErrorとして
        即座に検知される（規約Dの目的であるタイプミス防止の裏付け）"""
        with pytest.raises(AttributeError):
            Classification.TYPO_NOT_A_REAL_MEMBER

    def test_all_seven_expected_members_exist(self):
        """既存の7値（BUY/WATCH/HOLD/TRIM/GROWTH_PREMIUM/SELL/PASS）が
        全て定義されていること"""
        assert {m.value for m in Classification} == {
            "BUY", "WATCH", "HOLD", "TRIM", "GROWTH_PREMIUM", "SELL", "PASS",
            "UNDETERMINED",  # 株価欠損時の判定不能（2026-09-26）
        }
