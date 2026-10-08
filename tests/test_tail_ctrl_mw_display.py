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


# ── 重要な不備（significant deficiency）: 2026-10-08、マテリアルウィークネスと同じ形に ──────────────

@pytest.mark.parametrize("name", ["detail.html", "index.html"])
def test_sd_heading_has_no_count_and_is_not_called_juyona_kekkan(name):
    html = _read(name)
    assert not re.search(r"(sd|significant_deficiencies)\.length\s*\+\s*'件", html)
    assert "重要な欠陥" not in html   # material weaknessを指す語。significant deficiencyは「重要な不備」
    assert "重要な不備: あり" in html
    assert "（「significant deficiency」を含む箇所の抜粋。不備の数ではありません）" in html


_DUMMY = {
    "ticker": "TEST", "quarter": "2026Q2", "filing_date": "2026-08-10", "effective": False,
    "material_weaknesses": ["... a material weakness in control environment ...", "... the material weakness remains ..."],
    "significant_deficiencies": ["... a significant deficiency in IT general controls ...",
                                 "... the significant deficiency was remediated ...",
                                 "... significant deficiency ..."],
    "item4_excerpt": "Item 4. Controls and Procedures", "item4_excerpt_ja": None,
}


@pytest.fixture(scope="module")
def tail_pages():
    pw = pytest.importorskip("playwright.sync_api")
    import socket, subprocess, sys, time, urllib.request
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    docs = os.path.join(os.path.dirname(__file__), "..", "docs")
    srv = subprocess.Popen([sys.executable, "-m", "http.server", str(port), "--directory", docs],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    base = f"http://127.0.0.1:{port}/portfolio/tail/"
    try:
        for _ in range(50):
            try:
                urllib.request.urlopen(base + "index.html", timeout=1)
                break
            except Exception:
                time.sleep(0.2)
        with pw.sync_playwright() as p:
            try:
                browser = p.chromium.launch()
            except Exception as e:  # chromium が無い環境
                pytest.skip(f"chromium を起動できない: {e}")
            ctx = browser.new_context()
            ctx.add_init_script("sessionStorage.setItem('tail_auth','1');")
            idx = ctx.new_page()
            idx.goto(base + "index.html")
            idx.wait_for_function("typeof _buildCtrlBody === 'function'", timeout=30000)
            det = ctx.new_page()
            det.goto(base + "detail.html?ticker=PLTR")
            det.wait_for_function("typeof buildCtrl === 'function' && Array.isArray(window._CTRL_ITEMS)", timeout=30000)
            yield idx, det
            browser.close()
    finally:
        srv.terminate()


def _assert_presence_only(text):
    assert "マテリアルウィークネス: あり" in text
    assert "重要な不備: あり" in text
    assert "「significant deficiency」を含む箇所の抜粋。不備の数ではありません" in text
    assert "件)" not in text and "重要な欠陥" not in text   # 件数・旧訳語を出さない（ダミーは2件・3件）


def test_index_renders_sd_presence_with_dummy(tail_pages):
    idx, _ = tail_pages
    text = idx.evaluate("""(d) => { const div = document.createElement('div'); div.innerHTML = _buildCtrlBody('TEST', d);
                                    document.body.appendChild(div); const s = div.innerText; div.remove(); return s; }""", _DUMMY)
    _assert_presence_only(text)


def test_detail_renders_sd_presence_with_dummy(tail_pages):
    _, det = tail_pages
    text = det.evaluate("""(d) => { S.ctrlItems = {item4: d};
                                    window._CTRL_ITEMS = window._CTRL_ITEMS.filter(function (i) { return i.key === 'item4'; });
                                    const div = document.createElement('div'); div.innerHTML = buildCtrl();
                                    document.body.appendChild(div); const s = div.innerText; div.remove(); return s; }""", _DUMMY)
    _assert_presence_only(text)
