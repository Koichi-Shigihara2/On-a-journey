"""
tests/test_scenario_bearbull_sign_flip.py

[[SCENARIO-BEARBULL-SIGN-FLIP-1]]の回帰テスト。

calculator/scenarios.py::calculate_scenario_valuations()は
bear_rate=base_growth_rate×bear_multiplier・bull_rate=
base_growth_rate×bull_multiplierという単純乗算だった。base_growth_rate
が正の前提では意図通り（Bear<Base<Bull）だが、負の場合は単純乗算だと
下落幅の大小関係がラベルと逆転する（例: base=-10%のとき、
bear=-10%×0.7=-7%〈下落が緩い＝実態は楽観〉・bull=-10%×1.2=-12%
〈下落が急＝実態は悲観〉とBear/Bullの意味が入れ替わる）。

修正: base_growth_rateが負の場合のみ乗数を反転させる
（bear=×1.3・bull=×0.8）。正の場合は既存の挙動（呼び出し元が渡す
bear_multiplier/bull_multiplier）を一切変更しない。

実行方法:
    python -m pytest tests/test_scenario_bearbull_sign_flip.py -v
"""

import os
import sys

_PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

from calculator.scenarios import calculate_scenario_valuations  # noqa: E402


def _identity_calc_func(growth_rate: float) -> float:
    """理論株価計算をモック化せず、成長率そのものを返す（乗数の検証に特化）"""
    return growth_rate


class TestPositiveBaseGrowthRateUnchanged:
    """base_growth_rateが正の場合、既存の乗数（デフォルト0.7/1.2、または
    呼び出し元が渡すカスタム乗数）が従来通り適用され、本修正による
    回帰がないことを確認する"""

    def test_default_multipliers_unchanged(self):
        result = calculate_scenario_valuations(_identity_calc_func, base_growth_rate=0.10)
        assert result.bear.growth_rate == 0.07
        assert result.base.growth_rate == 0.1
        assert result.bull.growth_rate == 0.12
        assert result.bear.growth_rate < result.base.growth_rate < result.bull.growth_rate

    def test_custom_multipliers_unchanged(self):
        """呼び出し元がカスタム乗数（例: growth_sanityのFCFマージン補正で
        bear_multiplier=0.7×0.4=0.28等）を渡した場合も、正のbase_growth_
        rateでは従来通りそのまま適用される"""
        result = calculate_scenario_valuations(
            _identity_calc_func, base_growth_rate=0.10,
            bear_multiplier=0.28, bull_multiplier=1.5,
        )
        assert result.bear.growth_rate == 0.028
        assert result.bull.growth_rate == 0.15

    def test_zero_base_growth_rate_uses_positive_branch(self):
        """0は`< 0`を満たさないため正の分岐（デフォルト乗数）を使う
        （境界値の扱いを明示的に確認）"""
        result = calculate_scenario_valuations(_identity_calc_func, base_growth_rate=0.0)
        assert result.bear.growth_rate == 0.0
        assert result.bull.growth_rate == 0.0


class TestNegativeBaseGrowthRateSignFlip:
    """base_growth_rateが負の場合、乗数を反転（bear=×1.3・bull=×0.8）し、
    下落幅で見てBearが最も悲観的（bear_rate < base_rate < bull_rate）と
    なることを確認する"""

    def test_bear_more_negative_than_bull(self):
        result = calculate_scenario_valuations(_identity_calc_func, base_growth_rate=-0.10)
        assert result.bear.growth_rate == -0.13
        assert result.base.growth_rate == -0.1
        assert result.bull.growth_rate == -0.08
        # 下落幅の大小関係: Bearが最も悲観的（成長率としては最も低い）
        assert result.bear.growth_rate < result.base.growth_rate < result.bull.growth_rate
        # 下落幅（絶対値）で見てもBear > Base > Bull
        assert abs(result.bear.growth_rate) > abs(result.base.growth_rate) > abs(result.bull.growth_rate)

    def test_negative_case_ignores_caller_supplied_multipliers(self):
        """負の場合は呼び出し元がどんなbear_multiplier/bull_multiplierを
        渡していても、固定の1.3/0.8が使われる（登録時点の依頼書通り、
        正側のカスタム乗数〈growth_sanity補正等〉と混同しない設計）"""
        result = calculate_scenario_valuations(
            _identity_calc_func, base_growth_rate=-0.10,
            bear_multiplier=0.28, bull_multiplier=1.5,
        )
        assert result.bear.growth_rate == -0.13
        assert result.bull.growth_rate == -0.08

    def test_small_negative_value(self):
        """小さい負の値でも符号逆転が正しく機能する"""
        result = calculate_scenario_valuations(_identity_calc_func, base_growth_rate=-0.02)
        assert result.bear.growth_rate == round(-0.02 * 1.3, 3)
        assert result.bull.growth_rate == round(-0.02 * 0.8, 3)
        assert result.bear.growth_rate < result.base.growth_rate < result.bull.growth_rate
