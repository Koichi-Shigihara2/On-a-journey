"""audit.py: テキストのファイルに NUL バイトが無いこと（2026-11-28）。区切り文字の `\0` を書いたつもりが実際の NUL になった事故
（scripts/inventory/inventory_analysis.py）を、git に追加済み・未追跡のテキストのファイルから見つける。ファイル名と行番号を出し、audit は NG（exit 1）"""
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from common.sec_data import audit  # noqa: E402

NUL = bytes([0])


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def test_nul_in_text_files_is_found_with_line_number(tmp_path):
    _git(tmp_path, "init", "-q")
    (tmp_path / ".gitignore").write_bytes(b"ignored/\n")
    (tmp_path / "a.py").write_bytes(b"x = 1\ny = 2\nz = 'a" + NUL + b"b'\n")                  # 追跡のファイル、3行目に NUL
    (tmp_path / "clean.json").write_bytes(b'{"a": 1}\n')
    (tmp_path / "pic.png").write_bytes(b"\x89PNG" + NUL * 4)                                  # バイナリは対象外
    (tmp_path / "utf16.txt").write_bytes(bytes([0xFF, 0xFE]) + "abc".encode("utf-16-le"))      # UTF-16 の BOM のあるファイルは対象外
    (tmp_path / "ignored").mkdir()
    (tmp_path / "ignored" / "e.py").write_bytes(b"a" + NUL)                                   # .gitignore で除く
    _git(tmp_path, "add", ".gitignore", "a.py", "clean.json", "pic.png", "utf16.txt")
    _git(tmp_path, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "x")
    (tmp_path / "new.md").write_bytes(b"# t\n" + NUL + b"\n")                                  # 未追跡のファイルも見る（2行目）
    found = audit.find_nul_files(str(tmp_path))
    assert found == [("a.py", 3, 1), ("new.md", 2, 1)], found
    assert audit.find_nul_files(str(tmp_path), files=["clean.json"]) == []


def test_main_exits_with_ng_when_nul_is_found():
    """audit の main は、NUL のあるテキストのファイルがあれば critical と同じく exit 1 にする"""
    src = open(os.path.join(REPO, "common", "sec_data", "audit.py"), encoding="utf-8").read()
    assert "nul_files = find_nul_files()" in src and "if critical or nul_files:" in src


def test_this_repository_has_no_nul_in_text_files():
    """本番のリポジトリのテキストのファイルに NUL が無い（無ければ audit が通る）"""
    assert audit.find_nul_files() == []
