"""[[TAIL-CTRL-MW-COUNT-DISPLAY-1]]（2026-10-08）の回帰テスト。

sec_ctrl_fetcher.pyのmaterial_weaknessesは「material weakness」という語の出現箇所ごとの抜粋で、弱点の数ではない。
TAILの詳細画面（detail.html）と一覧のモーダル（index.htmlの内部統制タブ）は、件数を出さずに有無と該当箇所の抜粋を出す。
"""
import os
import re

import pytest

_TAIL = os.path.join(os.path.dirname(__file__), "..", "docs", "portfolio", "tail")


def _read(name):
    with open(os.path.join(_TAIL, name), encoding="utf-8") as f:
        return f.read()


@pytest.mark.parametrize("name", ["detail.html", "index.html"])
def test_mw_heading_has_no_count(name):
    html = _read(name)
    # 件数を見出しに入れる式（mw.length + '件' / material_weaknesses.length + '件'）が無い
    assert not re.search(r"(mw|material_weaknesses)\.length\s*\+\s*'件", html)
    assert not re.search(r"マテリアルウィークネス \(' \+", html)


@pytest.mark.parametrize("name", ["detail.html", "index.html"])
def test_mw_shows_presence_and_excerpt_note(name):
    html = _read(name)
    assert "マテリアルウィークネス: あり" in html
    assert "「material weakness」を含む箇所の抜粋。弱点の数ではありません" in html


def test_index_keeps_none_label_for_effective():
    # 有効で該当箇所が無いときの「なし」の表示は変えない
    assert "マテリアルウィークネス: なし" in _read("index.html")
