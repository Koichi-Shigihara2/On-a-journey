"""
tests/test_rice_adj_asymmetric_zero.py

[[RICE-ADJ-ASYMMETRIC-ZERO-1]]の回帰テスト。

calculate_rice()（calculator/rice.py）は、rice_adj（cf_adjを使う版）にのみ
`cf_adj > 0 and wacc > 0`のガードがあり、条件を満たさない場合0.0に
フォールバックしていた。一方rice（本来のcf使用）には同等のガードが
なく、cfが0以下の場合でも符号が反転した無意味な値をそのまま返していた
（q<0とcf<0が両方負の場合に符号が打ち消し合い、あたかも正常な
「測定できた低い効率」であるかのような偽陽性の正の値になることもある）。

修正: cf<=0の場合はrice、cf_adj<=0またはwacc<=0の場合はrice_adjを、
どちらも0.0ではなくNone（測定不能）とし、理由をrice_na_reason/
rice_adj_na_reasonに残す。

実行方法:
    python -m pytest tests/test_rice_adj_asymmetric_zero.py -v
"""

import os
import sys

_PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

from calculator import rice as rice_module  # noqa: E402
from calculator.rice import calculate_rice  # noqa: E402


def _stub_calc(monkeypatch, q, cf, cf_adj, cf_warnings=None):
    """_calc_q/_calc_cf_lagged を固定値で置き換える
    （annual_dataの実データ構築を避け、境界値の組み合わせを直接検証する）。"""
    monkeypatch.setattr(rice_module, "_calc_q", lambda annual_data, years: (q, years, False))
    monkeypatch.setattr(
        rice_module, "_calc_cf_lagged",
        lambda annual_data, years: (cf, cf_adj, years, 0.5, 0.1, cf_warnings or []),
    )


_SCENARIOS = {
    "bear": {"growth_rate": 0.10},
    "base": {"growth_rate": 0.20},
    "bull": {"growth_rate": 0.30},
}


class TestRiceCfNonPositive:
    """cf<=0の場合、riceは0.0ではなくNoneになる（本テストの核心）。
    理由文言（rice_na_reason）は、guard導入前の生値（g×vc_factor×q×cf）の
    符号で2パターンに分岐する（2026-09-19仕上げ対応、実データSPIR/CIX等の
    再検証で判明した区別）:
    ①raw<0（q正常・cfのみ負の通常ケース、CIX/ENTG/XOM型）:
      旧実装が既に負値を返し「N/A (OCF赤字)」固定ラベルで表示していた
      ため、その意味論を"OCF赤字"という理由文言でそのまま踏襲する
    ②raw>=0（qも負でcfとの符号相殺により旧実装が見かけ上プラスの値を
      返していたケース、SPIR型）: cfが負であること自体が原因である旨を
      明示する（"OCF赤字"ラベルは実態と食い違うため使わない）
    """

    def test_sign_cancellation_case_reason_mentions_cf_negative_not_ocf_deficit(
        self, monkeypatch
    ):
        """SPIR型（q<0・cf<0、積は正）: 理由は「OCF赤字」ではなくcf負を
        明示する文言になること（旧実装ならこのケースで見かけ上プラスの
        値になり「低効率」に誤分類されていた）"""
        _stub_calc(monkeypatch, q=-0.5, cf=-0.2, cf_adj=0.3)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
        )
        assert result.available
        assert result.base.rice is None
        assert result.base.rice_na_reason is not None
        assert "OCF赤字" not in result.base.rice_na_reason
        assert "cf=-0.200" in result.base.rice_na_reason

    def test_normal_negative_case_reason_preserves_ocf_deficit_label(self, monkeypatch):
        """CIX/ENTG/XOM型（q>0・cf<0、積は負）: 旧実装と同じ「OCF赤字」の
        意味論をそのまま踏襲すること"""
        _stub_calc(monkeypatch, q=0.8, cf=-0.2, cf_adj=0.3)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
        )
        assert result.base.rice is None
        assert result.base.rice_na_reason == "OCF赤字"

    def test_cf_zero_rice_is_none_not_zero(self, monkeypatch):
        """cf=0ちょうどの場合も、0.0（"低効率"と誤分類されうる）ではなくNone"""
        _stub_calc(monkeypatch, q=0.8, cf=0.0, cf_adj=0.3)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
        )
        assert result.base.rice is None

    def test_rice_per_ratio_is_none_when_rice_is_none(self, monkeypatch):
        _stub_calc(monkeypatch, q=0.8, cf=-0.2, cf_adj=0.3)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
            current_per=15.0,
        )
        assert result.base.rice is None
        assert result.base.rice_per_ratio is None

    def test_cf_positive_rice_is_computed_normally_unchanged(self, monkeypatch):
        """cf>0の場合は既存の計算式のまま（後退互換）"""
        _stub_calc(monkeypatch, q=0.8, cf=4.0, cf_adj=3.0)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
        )
        expected = (0.20 * 1.0 * 0.8 * 4.0) / 0.10  # g(base) * vc_factor(1.0) * q * cf / wacc
        assert result.base.rice == expected
        assert result.base.rice_na_reason is None


class TestRiceAdjCfAdjOrWaccNonPositive:
    """cf_adj<=0またはwacc<=0の場合、rice_adjは0.0ではなくNoneになる"""

    def test_cf_adj_negative_rice_adj_is_none_not_zero(self, monkeypatch):
        _stub_calc(monkeypatch, q=0.8, cf=4.0, cf_adj=-0.5)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
        )
        assert result.base.rice_adj is None
        assert result.base.rice_adj != 0.0
        assert result.base.rice_adj_na_reason is not None
        assert "cf_adj<=0" in result.base.rice_adj_na_reason

    def test_cf_adj_zero_rice_adj_is_none(self, monkeypatch):
        _stub_calc(monkeypatch, q=0.8, cf=4.0, cf_adj=0.0)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
        )
        assert result.base.rice_adj is None

    def test_cf_adj_positive_rice_adj_is_computed_normally_unchanged(self, monkeypatch):
        """cf_adj>0かつwacc>0の場合は既存の計算式のまま（後退互換）"""
        _stub_calc(monkeypatch, q=0.8, cf=4.0, cf_adj=3.0)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
        )
        expected = (0.20 * 1.0 * 0.8 * 3.0) / 0.10
        assert result.base.rice_adj == expected
        assert result.base.rice_adj_na_reason is None

    def test_rice_available_when_rice_adj_is_none_but_rice_is_not(self, monkeypatch):
        """rice_adjのみがNoneになり、riceは正常に計算される非対称ケース
        （cf>0だがcf_adj<=0）を確認する"""
        _stub_calc(monkeypatch, q=0.8, cf=4.0, cf_adj=-1.0)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
        )
        assert result.base.rice is not None
        assert result.base.rice_adj is None


class TestToDictHandlesNone:
    """to_dict()がNoneを含む場合でもクラッシュせず、JSON上でnullとして
    出力されること（round(None, ...)はTypeErrorになるため要ガード）"""

    def test_to_dict_with_none_rice_does_not_raise(self, monkeypatch):
        _stub_calc(monkeypatch, q=0.8, cf=-0.2, cf_adj=-1.0)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
        )
        d = result.to_dict()
        base_d = d["base"]
        assert base_d["rice"] is None
        assert base_d["rice_adj"] is None
        assert "rice_na_reason" in base_d
        assert "rice_adj_na_reason" in base_d

    def test_to_dict_without_none_omits_reason_keys(self, monkeypatch):
        """測定可能な場合、reasonキー自体を追加しない（既存フィールドの形を
        変えない、to_dict()の出力を不必要に太らせない）"""
        _stub_calc(monkeypatch, q=0.8, cf=4.0, cf_adj=3.0)
        result = calculate_rice(
            annual_data=[{}] * 4, wacc=0.10, scenario_valuations=_SCENARIOS,
        )
        base_d = result.base.to_dict()
        assert "rice_na_reason" not in base_d
        assert "rice_adj_na_reason" not in base_d
