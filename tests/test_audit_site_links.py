"""ページ間リンクの監査（scripts/audit_site_links.py、[[NAV-LINK-INTEGRITY-1]]、2026-10-17）のテスト。"""
import os
import sys

import pytest

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_REPO, "scripts"))
import audit_site_links as asl  # noqa: E402


def _write(root, rel, text):
    path = os.path.join(root, *rel.split("/"))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


@pytest.fixture
def docs(tmp_path, monkeypatch):
    root = str(tmp_path / "docs")
    _write(root, "common/site-nav.js", "var BASE = '/On-a-journey';\n"
           "var ITEMS = [{ href: BASE + '/' }, { href: BASE + '/tool/' }];\n")
    _write(root, "index.html", '<a href="tool/">tool</a>')
    _write(root, "tool/index.html",
           '<a href="detail.html?ticker=${t.ticker}">x</a>'
           '<a href="${T_BASE}stock.html?ticker=${s.ticker}">y</a>'
           "<script>const T_BASE = '../other/';</script>")
    _write(root, "tool/detail.html", '<a href="index.html">back</a>')
    _write(root, "other/stock.html", '<a href="/On-a-journey/tool/">tool</a>')
    monkeypatch.setattr(asl, "DOCS", root)
    monkeypatch.setattr(asl, "ORPHAN_ALLOWED", {})
    return root


def test_clean_tree(docs):
    broken, orphans, unverifiable, checked = asl.audit()
    assert broken == [] and orphans == [] and unverifiable == []
    assert checked == 7  # ITEMS 2・HTML 5（テンプレートのリンクはパスだけ、${T_BASE}は同じファイルの定数で置き換え）


def test_broken_link_and_orphan(docs):
    _write(docs, "tool/detail.html", '<a href="missing.html">x</a><a href="../../market-monitor/">y</a>')
    _write(docs, "lonely.html", "<p>no links here</p>")
    broken, orphans, _u, _c = asl.audit()
    assert [(s, t) for s, _r, t in broken] == [
        ("tool/detail.html", "tool/missing.html"),
        ("tool/detail.html", "../market-monitor/index.html"),
    ]
    assert orphans == ["lonely.html"]


def test_self_link_does_not_count_and_comments_are_ignored(docs):
    _write(docs, "solo.html", '<a href="solo.html">me</a><!-- <a href="gone.html">x</a> -->')
    broken, orphans, _u, _c = asl.audit()
    assert broken == []
    assert orphans == ["solo.html"]


def test_unresolvable_template_is_counted_not_failed(docs):
    _write(docs, "tool/detail.html", '<a href="index.html">b</a><a href="${escHtml(x.link)}">n</a>')
    broken, orphans, unverifiable, _c = asl.audit()
    assert broken == [] and orphans == []
    assert unverifiable == [("tool/detail.html", "${escHtml(x.link)}")]


def test_external_and_fragment_links_are_skipped(docs):
    _write(docs, "tool/detail.html",
           '<a href="index.html">b</a><a href="https://example.com/">e</a><a href="#top">t</a>'
           '<a href="javascript:void(0)">j</a>')
    broken, _o, _u, _c = asl.audit()
    assert broken == []


def test_repo_docs_have_no_broken_links_or_orphans():
    """現在のリポジトリ: リンク切れ・孤立ページが無い"""
    broken, orphans, _u, _c = asl.audit()
    assert broken == []
    assert orphans == []
