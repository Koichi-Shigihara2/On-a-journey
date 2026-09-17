"""
tests/test_segment_growth_outlook_ai.py

[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1]]パイロット実装
（2026-09-18）。src/tail/segment_growth_outlook_ai.pyの回帰テスト。
DCF/IV計算には使わない参考情報専用のAI抽出であることと、フェイル
セーフ動作（AI呼び出し失敗・不正データ時にNoneを返しパイプラインを
止めない）を検証する。

実行方法:
    python -m pytest tests/test_segment_growth_outlook_ai.py -v
"""

import json
import os
import sys

_TAIL_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "tail"))
if _TAIL_DIR not in sys.path:
    sys.path.insert(0, _TAIL_DIR)

import segment_growth_outlook_ai as sgoa  # noqa: E402


class TestExtractSegmentGrowthOutlook:
    def test_empty_section_text_returns_none_without_calling_api(self, monkeypatch):
        called = {"n": 0}
        monkeypatch.setattr(sgoa, "call_grok", lambda *a, **k: called.__setitem__("n", called["n"] + 1) or "{}")
        assert sgoa.extract_segment_growth_outlook("XYZ", "") is None
        assert sgoa.extract_segment_growth_outlook("XYZ", "   ") is None
        assert called["n"] == 0

    def test_valid_response_is_parsed_correctly(self, monkeypatch):
        fake_response = json.dumps({
            "segments": [
                {"name": "Lending Segment", "trend": "accelerating",
                 "summary": "貸出残高の増加により純利息収入が押し上げられた。",
                 "quote": "growth opportunity continues to be strong"},
            ]
        })
        monkeypatch.setattr(sgoa, "call_grok", lambda *a, **k: fake_response)
        result = sgoa.extract_segment_growth_outlook("SOFI", "dummy mda text")
        assert result == [{
            "name": "Lending Segment",
            "trend": "accelerating",
            "summary": "貸出残高の増加により純利息収入が押し上げられた。",
            "quote": "growth opportunity continues to be strong",
        }]

    def test_empty_segments_array_returns_none(self, monkeypatch):
        """本文にセグメント別記述が見当たらない場合（AI自身がそう判断した場合）"""
        monkeypatch.setattr(sgoa, "call_grok", lambda *a, **k: '{"segments": []}')
        assert sgoa.extract_segment_growth_outlook("XYZ", "dummy") is None

    def test_invalid_trend_value_defaults_to_uncertain(self, monkeypatch):
        """AIが許可されていないtrend値を返した場合はuncertainへフォールバックする
        （固定の列挙値のみ許可し、自由記述の混入を防ぐ）"""
        fake_response = json.dumps({
            "segments": [{"name": "Foo Segment", "trend": "explosive_growth",
                          "summary": "s", "quote": "q"}]
        })
        monkeypatch.setattr(sgoa, "call_grok", lambda *a, **k: fake_response)
        result = sgoa.extract_segment_growth_outlook("XYZ", "dummy")
        assert result[0]["trend"] == "uncertain"

    def test_segment_without_name_is_filtered_out(self, monkeypatch):
        fake_response = json.dumps({
            "segments": [
                {"name": "", "trend": "stable", "summary": "s", "quote": "q"},
                {"name": "Valid Segment", "trend": "stable", "summary": "s2", "quote": "q2"},
            ]
        })
        monkeypatch.setattr(sgoa, "call_grok", lambda *a, **k: fake_response)
        result = sgoa.extract_segment_growth_outlook("XYZ", "dummy")
        assert len(result) == 1
        assert result[0]["name"] == "Valid Segment"

    def test_api_call_failure_returns_none_not_exception(self, monkeypatch):
        def _raise(*a, **k):
            raise RuntimeError("simulated API failure")
        monkeypatch.setattr(sgoa, "call_grok", _raise)
        assert sgoa.extract_segment_growth_outlook("XYZ", "dummy") is None

    def test_malformed_json_response_returns_none_not_exception(self, monkeypatch):
        monkeypatch.setattr(sgoa, "call_grok", lambda *a, **k: "this is not json at all")
        assert sgoa.extract_segment_growth_outlook("XYZ", "dummy") is None

    def test_no_growth_rate_numeric_field_in_schema(self, monkeypatch):
        """[[FCF-CONVRATE-LOWER-DIVERGENCE-1]]と同じ設計思想: 出力スキーマに
        数値の成長率フィールドを含めない（DCF計算への誤用を構造的に防ぐ）。
        AIが数値フィールドを追加で返してきても採用しないことを確認する"""
        fake_response = json.dumps({
            "segments": [{"name": "Foo", "trend": "stable", "summary": "s",
                          "quote": "q", "growth_rate": 0.42}]
        })
        monkeypatch.setattr(sgoa, "call_grok", lambda *a, **k: fake_response)
        result = sgoa.extract_segment_growth_outlook("XYZ", "dummy")
        assert "growth_rate" not in result[0]
        assert set(result[0].keys()) == {"name", "trend", "summary", "quote"}
