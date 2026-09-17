"""
tests/test_compose_fcf_bottom_up.py

[[FCF-CONVRATE-LOWER-DIVERGENCE-1]]ボトムアップFCF移行（2026-09-17）。

calculator/adjustments.py::compose_fcf_bottom_up()の回帰テスト。業種別
固定転換率（adj_net_income×conversion_rate）方式を廃止し、raw_fcf
（=OCF-CapEx、determine_fcf_base()の出力）をそのまま採用した上で
CapEx/SBC/OCF/D&Aの内訳を開示する新方式の中核ロジック。

実行方法:
    python -m pytest tests/test_compose_fcf_bottom_up.py -v
"""

import os
import sys

_CALC_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation", "calculator")
)
if _CALC_DIR not in sys.path:
    sys.path.insert(0, _CALC_DIR)

from adjustments import compose_fcf_bottom_up  # type: ignore[import]  # noqa: E402


class TestNormalBottomUp:
    def test_estimated_fcf_always_equals_raw_fcf(self):
        """ボトムアップ方式ではraw_fcfを別の値で置き換えないため、
        estimated_fcfは常にraw_fcfと一致する"""
        result = compose_fcf_bottom_up(
            ticker="AAPL", raw_fcf=100_000_000_000.0,
            capex_list=[12_715_000_000], sbc_list=[12_863_000_000],
            ocf_list=[111_482_000_000], da_list=[11_698_000_000],
        )
        assert result.estimated_fcf == 100_000_000_000.0
        assert result.raw_fcf == 100_000_000_000.0
        assert result.applied is False
        assert result.method == "bottom_up"

    def test_latest_year_components_are_exposed_for_display(self):
        result = compose_fcf_bottom_up(
            ticker="AAPL", raw_fcf=100_000_000_000.0,
            capex_list=[12_715_000_000, 9_447_000_000],
            sbc_list=[12_863_000_000, 11_688_000_000],
            ocf_list=[111_482_000_000, 118_254_000_000],
            da_list=[11_698_000_000, 11_445_000_000],
        )
        # 直近年（index 0）のみが開示対象
        assert result.capex == 12_715_000_000
        assert result.sbc == 12_863_000_000
        assert result.ocf == 111_482_000_000
        assert result.da == 11_698_000_000

    def test_empty_lists_do_not_crash(self):
        result = compose_fcf_bottom_up(
            ticker="XYZ", raw_fcf=0.0,
            capex_list=[], sbc_list=[], ocf_list=[], da_list=[],
        )
        assert result.applied is False
        assert result.capex is None


class TestCapexFallbackDetection:
    """LYFT/PAYS/FLYW型（[[FCF-CONVRATE-LOWER-DIVERGENCE-1]]決定4）:
    実際のCapExデータが取得できずフォールバック値を使った銘柄を検知する"""

    def test_all_years_missing_capex_triggers_fallback(self):
        """LOAR/VZ型: 標準候補4タグを直近5年分いずれも申告していない"""
        result = compose_fcf_bottom_up(
            ticker="LOAR", raw_fcf=50_000_000.0,
            capex_list=[None, None, None, None, None],
            sbc_list=[1_000_000] * 5,
            ocf_list=[50_000_000] * 5,
            da_list=[500_000] * 5,
        )
        assert result.applied is True
        assert result.method == "fallback_capex_missing"
        assert "CapEx申告タグなし" in result.fallback_reason
        assert result.capex is None
        # フォールバック時もestimated_fcf=raw_fcf（生FCF≈OCF）は変わらない
        assert result.estimated_fcf == 50_000_000.0

    def test_single_year_missing_capex_does_not_trigger_fallback(self):
        """ALAB/APP等型: 5年中1年のみタグ欠損（5yr/2yr平均への影響が限定的なため
        フォールバック扱いとしない）"""
        result = compose_fcf_bottom_up(
            ticker="APP", raw_fcf=3_000_000_000.0,
            capex_list=[100_000_000, None, 90_000_000, 95_000_000, 92_000_000],
            sbc_list=[50_000_000] * 5,
            ocf_list=[500_000_000] * 5,
            da_list=[30_000_000] * 5,
        )
        assert result.applied is False
        assert result.method == "bottom_up"
        assert result.capex == 100_000_000  # 直近年は欠損していない

    def test_lyft_shaped_data_after_capex_concept_fix(self):
        """LYFT実データ形状: capex_concept適用後は直近年CapEx=0（実測値、
        ソフトウェア資産化費用が2025年に$0だったため）でフォールバックしない"""
        result = compose_fcf_bottom_up(
            ticker="LYFT", raw_fcf=1_168_438_000.0,
            capex_list=[0, 4_200_000, 8_100_000, 12_100_000],
            sbc_list=[322_268_000, 330_921_000, 484_533_000, 750_767_000],
            ocf_list=[1_168_438_000, 849_737_000, -98_244_000, -237_285_000],
            da_list=[135_227_000, 148_892_000, 116_513_000, 154_798_000],
        )
        assert result.applied is False
        assert result.capex == 0
