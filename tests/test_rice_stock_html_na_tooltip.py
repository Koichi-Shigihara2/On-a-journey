"""
tests/test_rice_stock_html_na_tooltip.py

[[RICE-ADJ-ASYMMETRIC-ZERO-1]]仕上げ対応（2026-09-19）の回帰テスト
（stock.htmlのRICE RATIOテーブル表示側）。

stock.htmlのRICE RATIOテーブルは元々`riceBear.rice != null ? ... : '—'`
という形でNoneを安全に扱っていた（クラッシュはしない）が、"—"だけでは
測定不能の理由が分からない。calculate_rice()がrice_na_reasonを返すように
なったことに合わせ、rice==nullのセルは"N/A"表示＋rice_na_reasonを
title属性（ツールチップ）で表示するよう統一した。

このJSロジックはNode.js等JS実行環境がないと直接実行できないため
（本リポジトリにはJS用のテストランナーが存在しない）、
tests/test_stock_html_fcf_cagr_years.pyと同じ手法（ソースパターン検証）
を踏襲する。

実行方法:
    python -m pytest tests/test_rice_stock_html_na_tooltip.py -v
"""

import os

_STOCK_HTML = os.path.normpath(os.path.join(
    os.path.dirname(__file__), "..",
    "docs", "value-monitor", "tanuki_valuation", "stock.html",
))


def _read_stock_html() -> str:
    with open(_STOCK_HTML, encoding="utf-8") as f:
        return f.read()


class TestRiceTableShowsReasonTooltip:
    def test_all_three_scenarios_have_na_with_title_pattern(self):
        content = _read_stock_html()
        for sc in ("riceBear", "riceBase", "riceBull"):
            assert f"{sc}.rice == null && {sc}.rice_na_reason" in content
            assert f"{sc}.rice_na_reason.replace(/\"/g, '&quot;')" in content

    def test_na_text_used_instead_of_em_dash_for_rice_cell(self):
        """RICE値セル自体は'—'ではなく'N/A'を表示する（rice_adj/rice_per_ratio
        セルは対象外、引き続き'—'のまま）"""
        content = _read_stock_html()
        assert "riceBear.rice != null ? riceBear.rice.toFixed(1) : 'N/A'" in content
        assert "riceBase.rice != null ? riceBase.rice.toFixed(1) : 'N/A'" in content
        assert "riceBull.rice != null ? riceBull.rice.toFixed(1) : 'N/A'" in content
