"""
tests/test_fcf_outlier_ai.py

calculator/fcf_outlier_ai.py（[[FCF-OUTLIER-QUAL-1]]案B、一過性費用の
定性評価・参考表示専用）の単体テスト。ネットワークアクセス（実際の
Grok API呼び出し）は行わず、call_grokをmonkeypatchして検証する。

adjustments.py::FCFOutlierResult.to_dict()のai_assessment引数追加が
action・detected・rule・deviation_pct等の既存フィールドに影響しない
ことの回帰テストも含む（[[FCF-OUTLIER-QUAL-1]]の最重要制約）。
"""

import os
import sys

_PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

from calculator import fcf_outlier_ai as fcf_ai_module  # noqa: E402
from calculator.fcf_outlier_ai import assess_transient_qualitative  # noqa: E402
from calculator.adjustments import FCFOutlierResult  # noqa: E402


_CELH_ITEMS = [
    {"category": "金融関連", "item_name": "貸倒引当金繰入（異常変動分）",
     "amount": 7374000, "reason": "マクロ変動による一時的影響"},
    {"category": "リストラ・事業再編関連", "item_name": "リストラ費用",
     "amount": 80754000, "reason": "一過性の事業再編費用"},
]


class TestAssessTransientQualitative:
    def test_empty_items_skips_api_call_entirely(self, monkeypatch):
        """transient_itemsが空の場合はAPI呼び出し自体を行わずNoneを返す"""
        called = []
        monkeypatch.setattr(fcf_ai_module, "call_grok", lambda *a, **k: called.append(1))
        result = assess_transient_qualitative("XYZ", [])
        assert result is None
        assert called == []

    def test_successful_response_parsed_correctly(self, monkeypatch):
        monkeypatch.setattr(
            fcf_ai_module, "call_grok",
            lambda *a, **k: '```json\n{"assessment": "transient", "reasoning": "一時的な事業再編費用と判断"}\n```',
        )
        result = assess_transient_qualitative("CELH", _CELH_ITEMS)
        assert result == {"assessment": "transient", "reasoning": "一時的な事業再編費用と判断"}

    def test_structural_concern_assessment_parsed(self, monkeypatch):
        monkeypatch.setattr(
            fcf_ai_module, "call_grok",
            lambda *a, **k: '{"assessment": "structural_concern", "reasoning": "継続的な費用増加の兆候"}',
        )
        result = assess_transient_qualitative("XYZ", _CELH_ITEMS)
        assert result["assessment"] == "structural_concern"

    def test_api_call_raises_exception_returns_none(self, monkeypatch):
        """API呼び出し自体が例外を送出してもNoneを返し、例外を外へ伝播させない
        （フェイルセーフ、パイプライン全体を止めない）"""
        def _raise(*a, **k):
            raise RuntimeError("すべてのGrokモデルで失敗: timeout")
        monkeypatch.setattr(fcf_ai_module, "call_grok", _raise)
        result = assess_transient_qualitative("XYZ", _CELH_ITEMS)
        assert result is None

    def test_malformed_json_returns_none(self, monkeypatch):
        """JSON解析に失敗した場合もNoneを返す（例外を伝播させない）"""
        monkeypatch.setattr(fcf_ai_module, "call_grok", lambda *a, **k: "これはJSONではありません")
        result = assess_transient_qualitative("XYZ", _CELH_ITEMS)
        assert result is None

    def test_invalid_assessment_value_returns_none(self, monkeypatch):
        """assessmentが許容値（transient/structural_concern/uncertain）以外の
        場合はNoneを返す（AI応答の型崩れに対する防御）"""
        monkeypatch.setattr(
            fcf_ai_module, "call_grok",
            lambda *a, **k: '{"assessment": "positive", "reasoning": "..."}',
        )
        result = assess_transient_qualitative("XYZ", _CELH_ITEMS)
        assert result is None

    def test_uncertain_assessment_accepted(self, monkeypatch):
        monkeypatch.setattr(
            fcf_ai_module, "call_grok",
            lambda *a, **k: '{"assessment": "uncertain", "reasoning": "情報不足のため判断困難"}',
        )
        result = assess_transient_qualitative("XYZ", _CELH_ITEMS)
        assert result["assessment"] == "uncertain"

    def test_grok_4_3_model_used_exclusively(self):
        """[[GROK-MODEL-PRICE-1]]確定済みのgrok-4.3のみを使用し、
        新しいモデル名を独自に導入していないことの回帰テスト"""
        assert fcf_ai_module.GROK_MODELS == ["grok-4.3", "grok-4.3", "grok-4.3"]


class TestFcfOutlierResultToDictAiAssessment:
    """[[FCF-OUTLIER-QUAL-1]]の最重要制約: ai_assessment引数追加が
    action・detected・rule・deviation_pct等の既存フィールドに一切
    影響しないことの回帰テスト"""

    def _make_result(self):
        return FCFOutlierResult(
            detected=True, rule="deviation_large", fiscal_year=2025,
            fcf_value=100_000_000, threshold_pct=0.6, deviation_pct=81.8,
            transient_found=True, transient_items=_CELH_ITEMS,
            transient_total=88_128_000, action="flagged",
            note="FY2025 FCFが5年平均から82%乖離。要確認。",
        )

    def test_default_call_without_ai_assessment_yields_none(self):
        """引数なし呼び出し（既存の唯一の呼び出し元と同じ）では
        ai_assessment=Noneとなり、判定系フィールドは従来通り"""
        result = self._make_result()
        d = result.to_dict()
        assert d["transient_evidence"]["ai_assessment"] is None
        assert d["action"] == "flagged"
        assert d["detected"] is True
        assert d["rule"] == "deviation_large"
        assert d["deviation_pct"] == 81.8

    def test_ai_assessment_does_not_alter_action_or_detected_fields(self):
        """ai_assessmentを渡してもaction・detected・rule・deviation_pctは
        一切変化しない（[[FCF-OUTLIER-QUAL-1]]の核心的な安全性保証）"""
        result = self._make_result()
        d_before = result.to_dict()
        d_after = result.to_dict(ai_assessment={"assessment": "transient", "reasoning": "x"})

        assert d_after["action"] == d_before["action"]
        assert d_after["detected"] == d_before["detected"]
        assert d_after["rule"] == d_before["rule"]
        assert d_after["deviation_pct"] == d_before["deviation_pct"]
        assert d_after["fcf_value"] == d_before["fcf_value"]
        assert d_after["threshold_pct"] == d_before["threshold_pct"]
        assert d_after["note"] == d_before["note"]
        assert d_after["transient_evidence"]["found"] == d_before["transient_evidence"]["found"]
        assert d_after["transient_evidence"]["items"] == d_before["transient_evidence"]["items"]
        assert d_after["transient_evidence"]["total_transient_amount"] == \
            d_before["transient_evidence"]["total_transient_amount"]
        # ai_assessmentキーのみが変化する
        assert d_after["transient_evidence"]["ai_assessment"] == {"assessment": "transient", "reasoning": "x"}
