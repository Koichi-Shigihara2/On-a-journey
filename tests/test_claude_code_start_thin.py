"""CLAUDE_CODE_START.md を細く保つ（2026-11-06、使用量を減らす進め方の整備）。

置くのは「今の状態（アクティブの件数1行）・次回の確認手順（未済のものだけ）・決まりへの参照」だけ。
日付つきの記録・件数の推移・済んだ手順は docs/history/CLAUDE_CODE_START_HISTORY.md、手順・決まりは CLAUDE_CODE_PROCEDURES.md に置く。
行数の上限を超えたら失敗する（超えたら、済んだ手順や記録を履歴へ移す）。
"""
import os
import re

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAX_LINES = 300


def _read(rel):
    with open(os.path.join(REPO, rel), encoding="utf-8") as f:
        return f.read()


def test_claude_code_start_is_thin():
    n = len(_read("CLAUDE_CODE_START.md").splitlines())
    assert n <= MAX_LINES, (f"CLAUDE_CODE_START.md が {n} 行（上限 {MAX_LINES}）。済んだ手順・日付つきの記録を"
                            "docs/history/CLAUDE_CODE_START_HISTORY.md へ移す")


def test_required_sections_and_references_exist():
    text = _read("CLAUDE_CODE_START.md")
    for h in ("## 今の状態", "## 次回の確認手順", "## 決まりへの参照"):
        assert h in text, h
    assert re.search(r"アクティブは \*\*\d+件\*\*", text), "アクティブの件数の1行が無い"
    for rel in ("CLAUDE_CODE_PROCEDURES.md", "docs/history/CLAUDE_CODE_START_HISTORY.md", "PROJECT_STATUS.md", "CHAT_RULES.md", "tools/sim/README.md"):
        assert os.path.exists(os.path.join(REPO, rel)), rel


def test_procedure_sections_referenced_in_the_table_exist():
    proc = _read("CLAUDE_CODE_PROCEDURES.md")
    for h in ("毎回の作業開始時に必ず実行すること", "作業ルール", "計算方法の変更の手順", "新規銘柄登録時の必須手順", "銘柄削除時の必須手順",
              "TANUKI TAIL 銘柄追加手順", "管理の操作の手順", "作業完了時のチェックリスト", "月次メンテナンスタスク", "BACKLOG管理ルール",
              "リンク整合性チェック", "重要ルール"):
        assert re.search(r"^#{2,3} " + re.escape(h), proc, re.M), h


def test_active_count_line_matches_backlog():
    """BACKLOG.md の `^### [` の機械カウントと、CLAUDE_CODE_START.md の件数が同じ（終業のたびに更新する）"""
    backlog = _read("BACKLOG.md")
    n = len(re.findall(r"^### \[", backlog, re.M))
    m = re.search(r"アクティブは \*\*(\d+)件\*\*", _read("CLAUDE_CODE_START.md"))
    assert m and int(m.group(1)) == n, f"CLAUDE_CODE_START.md は {m and m.group(1)} 件、BACKLOG.md は {n} 件"
