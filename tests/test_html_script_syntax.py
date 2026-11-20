"""docs/ 以下の全HTMLの埋め込み <script> を取り出して node --check にかけ、JSの構文の誤りで失敗させる。

背景: TANUKI SCOREの337行目で、単引用符の文字列の中に ${...} と単引用符を入れて構文エラーになり、画面が
何も描画されないまま本番に2026-11-19から出ていた（FINDINGS 2026-11-20）。データ・判定の値の検査では見つからない。
node が無い環境では skip する（理由を出す）。
"""
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_RE = re.compile(r"<script(?P<attrs>[^>]*)>(?P<body>.*?)</script>", re.S | re.I)
SKIP_TYPES = ("application/json", "application/ld+json", "text/template", "text/x-template", "importmap")


def _html_files():
    return sorted(p for p in (ROOT / "docs").rglob("*.html") if "node_modules" not in p.parts)


def _inline_scripts(text):
    for m in SCRIPT_RE.finditer(text):
        attrs = m.group("attrs")
        if re.search(r"\bsrc\s*=", attrs, re.I):
            continue
        t = re.search(r"type\s*=\s*[\"']?([^\"'\s>]+)", attrs, re.I)
        if t and t.group(1).lower() in SKIP_TYPES:
            continue
        is_module = bool(t and t.group(1).lower() == "module")
        body = m.group("body")
        if body.strip():
            yield is_module, text[: m.start("body")].count("\n") + 1, body


def test_inline_scripts_have_valid_syntax():
    node = shutil.which("node")
    if not node:
        pytest.skip("node が見つからない（docs/ の埋め込みJSの構文検査には node --check が要る）")
    failures = []
    with tempfile.TemporaryDirectory() as tmp:
        for html in _html_files():
            text = html.read_text(encoding="utf-8", errors="replace")
            for i, (is_module, line, body) in enumerate(_inline_scripts(text)):
                f = Path(tmp) / f"s{abs(hash((str(html), i)))}.{'mjs' if is_module else 'js'}"
                f.write_text(body, encoding="utf-8")
                r = subprocess.run([node, "--check", str(f)], capture_output=True, text=True, encoding="utf-8")
                if r.returncode != 0:
                    msg = (r.stderr or "").strip().splitlines()
                    err = next((x for x in msg if "Error" in x), msg[0] if msg else "")
                    ln = re.search(r":(\d+)\n", r.stderr or "")
                    at = line + int(ln.group(1)) - 1 if ln else line
                    failures.append(f"{html.relative_to(ROOT).as_posix()} 付近の行 {at}: {err}")
    assert not failures, "埋め込みJSの構文の誤り:\n" + "\n".join(failures)
