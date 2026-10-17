"""docs/ 配下のページ間リンクを監査する（[[NAV-LINK-INTEGRITY-1]]、2026-10-17）。

検出するもの:
  1. リンク切れ: docs/ 配下の全 .html の href・src と、docs/common/*.js の `BASE + '/...'`
     （site-nav.js の ITEMS・ticker-links.js の各ページ）が指す先のファイルが docs/ に無い
  2. 孤立ページ: どのページ（自分自身を除く）・共通JSからもリンクされていない .html

扱い:
  - GitHub Pages のベースパス /On-a-journey/ で始まる絶対パスは docs/ を根として解決する
  - 外部（http(s)://・//）・mailto:・javascript:・data:・# だけのリンクは対象外
  - テンプレートのリンク（${...}）はパスの部分だけを検証する（?以降・#以降は見ない）。
    パスに ${NAME} が残る場合は、同じファイルの `const|let|var NAME = '...'` で置き換え、
    置き換えられなければ「検証できない」として数えるだけにする（失敗にしない）
  - HTML のコメントの中は見ない
  - ディレクトリへのリンクは index.html があれば有効

使い方:
  python scripts/audit_site_links.py            # 結果を表示。リンク切れ・孤立ページがあれば終了コード1
  python scripts/audit_site_links.py --verbose  # 検証できなかったテンプレートのリンクも表示
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Dict, List, Optional, Set, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(REPO, "docs")
BASE = "/On-a-journey"

# 意図的にどこからもリンクしないページ（docs/ からの相対パス → 理由）。今は無し
ORPHAN_ALLOWED: Dict[str, str] = {}

_ATTR_RE = re.compile(r"""\b(?:href|src)\s*=\s*(["'])(.*?)\1""", re.S)
_JS_BASE_RE = re.compile(r"""\bBASE\s*\+\s*'([^']*)'""")
_COMMENT_RE = re.compile(r"<!--.*?-->", re.S)
_CONST_RE = re.compile(r"""\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*(['"`])([^'"`$]*)\2""")
_TEMPLATE_RE = re.compile(r"\$\{\s*([A-Za-z_$][\w$]*)\s*\}")
_SKIP_PREFIXES = ("http://", "https://", "//", "mailto:", "javascript:", "data:", "tel:")


def _rel(path: str) -> str:
    return os.path.relpath(path, DOCS).replace(os.sep, "/")


def list_pages() -> List[str]:
    pages = []
    for root, _dirs, files in os.walk(DOCS):
        for f in files:
            if f.endswith(".html"):
                pages.append(os.path.join(root, f))
    return sorted(pages)


def path_part(raw: str) -> str:
    """リンクの文字列からパスの部分だけを取り出す（?・#・JSの連結・空白の手前まで）。"""
    s = raw.strip()
    cut = len(s)
    for ch in ("?", "#", "'", '"', "`", " ", "+", "\n"):
        i = s.find(ch)
        if i != -1:
            cut = min(cut, i)
    return s[:cut]


def resolve(base_dir: str, path: str) -> str:
    """リンクのパスを docs/ 配下のファイルの絶対パスに解決する。"""
    if path.startswith(BASE + "/") or path == BASE:
        target = os.path.join(DOCS, path[len(BASE):].lstrip("/"))
    elif path.startswith("/"):
        target = os.path.join(DOCS, path.lstrip("/"))  # ベースパスなしの絶対パス（Pagesでは404）
    else:
        target = os.path.join(base_dir, path)
    target = os.path.normpath(target)
    if path.endswith("/") or os.path.isdir(target):
        target = os.path.join(target, "index.html")
    return target


def substitute_templates(path: str, consts: Dict[str, str]) -> Optional[str]:
    """パスの ${NAME} を同じファイルの定数で置き換える。置き換えられなければ None。"""
    def repl(m: "re.Match[str]") -> str:
        name = m.group(1)
        if name not in consts:
            raise KeyError(name)
        return consts[name]
    try:
        return _TEMPLATE_RE.sub(repl, path)
    except KeyError:
        return None


def extract_links(src_file: str) -> List[Tuple[str, str]]:
    """(元の文字列, 検証するパス) の一覧。パスが空のもの・外部のものは除く。"""
    with open(src_file, encoding="utf-8") as fh:
        text = fh.read()
    out: List[Tuple[str, str]] = []
    if src_file.endswith(".js"):
        for m in _JS_BASE_RE.finditer(text):
            raw = BASE + m.group(1)
            out.append((raw, path_part(raw)))
        return out
    text = _COMMENT_RE.sub("", text)
    consts = {m.group(1): m.group(3) for m in _CONST_RE.finditer(text)}
    for m in _ATTR_RE.finditer(text):
        raw = m.group(2).strip()
        if not raw or raw.startswith("#") or raw.lower().startswith(_SKIP_PREFIXES):
            continue
        path = path_part(raw)
        if "${" in path:
            sub = substitute_templates(path, consts)
            path = sub if sub is not None else path
        if not path:
            continue
        out.append((raw, path))
    return out


def audit() -> Tuple[List[Tuple[str, str, str]], List[str], List[Tuple[str, str]], int]:
    pages = list_pages()
    sources = pages + sorted(
        os.path.join(DOCS, "common", f)
        for f in os.listdir(os.path.join(DOCS, "common")) if f.endswith(".js"))
    broken: List[Tuple[str, str, str]] = []
    unverifiable: List[Tuple[str, str]] = []
    linked_from: Dict[str, Set[str]] = {}
    checked = 0
    for src in sources:
        base_dir = os.path.dirname(src)
        for raw, path in extract_links(src):
            if "${" in path:
                unverifiable.append((_rel(src), raw))
                continue
            checked += 1
            target = resolve(base_dir, path)
            if not os.path.isfile(target):
                broken.append((_rel(src), raw, _rel(target)))
                continue
            if target.endswith(".html") and os.path.normcase(target) != os.path.normcase(src):
                linked_from.setdefault(os.path.normcase(target), set()).add(_rel(src))
    orphans = [
        _rel(p) for p in pages
        if os.path.normcase(p) not in linked_from and _rel(p) not in ORPHAN_ALLOWED
    ]
    return broken, orphans, unverifiable, checked


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    broken, orphans, unverifiable, checked = audit()
    print(f"ページ {len(list_pages())}件 / 検証したリンク {checked}件 / "
          f"リンク切れ {len(broken)}件 / 孤立ページ {len(orphans)}件 / "
          f"検証できないテンプレートのリンク {len(unverifiable)}件")
    for src, raw, target in broken:
        print(f"  [リンク切れ] {src}: {raw} -> {target}")
    for page in orphans:
        print(f"  [孤立ページ] {page}")
    for page, reason in sorted(ORPHAN_ALLOWED.items()):
        print(f"  [孤立を許可] {page}: {reason}")
    if args.verbose:
        for src, raw in unverifiable:
            print(f"  [検証できない] {src}: {raw}")
    return 1 if (broken or orphans) else 0


if __name__ == "__main__":
    sys.exit(main())
