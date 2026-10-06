"""
tests/test_sens_matrix_dual_impl.py

[[SENS-MATRIX-DUAL-IMPL-1]]の回帰テスト。

docs/value-monitor/tanuki_valuation/stock.htmlには、バックエンドの
sensitivity.matrix（3×3、DCFタイプ〈two_stage/three_stage/tapering〉に
応じて自動的に正しい計算式が使われる、`.sensitivity-section`）とは別に、
クライアント側で常に2段階DCFのみを仮定して再計算する独自5×5マトリクス
（`calcSensIV()`）が同一ページに並存していた。three_stage/tapering DCFの
銘柄では2つのセクションが異なる数値を表示する不整合があったため、
`calcSensIV()`と5×5セクション自体を削除し、バックエンドの表示のみに
統一した。

このJSロジックはNode.js等JS実行環境がないと直接実行できないため
（本リポジトリにはJS用のテストランナーが存在しない）、本テストは
tests/test_stock_html_fcf_cagr_years.pyと同じ手法（stock.html本体から
ソースパターンを直接検証）を踏襲する。

実行方法:
    python -m pytest tests/test_sens_matrix_dual_impl.py -v
"""

import os

_STOCK_HTML = os.path.normpath(os.path.join(
    os.path.dirname(__file__), "..",
    "docs", "value-monitor", "tanuki_valuation", "stock.html",
))


def _read_stock_html() -> str:
    with open(_STOCK_HTML, encoding="utf-8") as f:
        return f.read()


class TestClientSide5x5Removed:
    def test_calc_sens_iv_function_is_gone(self):
        """calcSensIV()関数定義自体が削除されていること"""
        content = _read_stock_html()
        assert "calcSensIV" not in content

    def test_old_5x5_section_title_is_gone(self):
        """旧5×5セクション独自のh3見出し文言が削除されていること
        （バックエンド側は'📈 SENSITIVITY ANALYSIS'という別の見出しを使う
        ため、この文言が残っていれば旧実装の生き残りを意味する）"""
        content = _read_stock_html()
        assert "感応度分析（割引率 × 成長率）" not in content

    def test_dead_alpha_variable_from_old_block_is_gone(self):
        """旧5×5ブロック内にあった未使用のalpha宣言（死コード）も
        ブロックごと削除されていること"""
        content = _read_stock_html()
        assert "const alpha = d.alpha ?? 1.0" not in content


class TestBackendSectionPreserved:
    def test_backend_sensitivity_section_still_present_exactly_once(self):
        """バックエンドの.sensitivity-section（3×3、'📈 SENSITIVITY
        ANALYSIS'見出し）は削除対象ではなく、1箇所のみ残っていること"""
        content = _read_stock_html()
        assert content.count('class="sensitivity-section"') == 1
        assert content.count("SENSITIVITY ANALYSIS") == 1

    def test_backend_matrix_table_still_present(self):
        """バックエンド側の3×3マトリクステーブルが引き続き存在すること"""
        content = _read_stock_html()
        assert 'id="sensitivityTable"' in content
        assert "function renderMatrixRows" in content


class TestWaccSliderRemoved:
    """2026-10-06: 「WACC調整」スライダー（updateWacc()）を削除した。
    β込みWACCから作った倍率（wacc.value/(wacc.value+Δ)）をRm基準の表に掛ける
    近似で、行のラベルだけRm基準+Δに書き換えていたため、表の本物の行と値が
    食い違っていた（NVDA +1%: 「11.0%」行の中央$692.71 vs 本物$603.68）。"""

    def test_slider_dom_and_function_are_gone(self):
        content = _read_stock_html()
        assert 'id="waccSlider"' not in content
        assert "updateWacc" not in content
        assert "wacc-slider" not in content
        assert 'id="waccDeltaDisplay"' not in content

    def test_heading_shows_main_iv_not_beta(self):
        """見出しはメイン理論株価（Rm基準、表の中央セルと同じ）で、β込みWACCの値ではないこと"""
        content = _read_stock_html()
        i = content.index("SENSITIVITY ANALYSIS")
        head = content[i:content.index('class="sensitivity-matrix"', i)]
        assert "ivpsBeta" not in head
        assert "β込みWACC:" not in head
        assert 'id="sensBaseIvps">$${fmt(ivps)}' in head
