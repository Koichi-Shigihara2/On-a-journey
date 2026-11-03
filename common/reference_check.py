"""文書・説明文・ワークフローの名前が、書き手のコード・リポジトリの今の状態と食い違っていないか（rcc CHECK-68、WARN）。

[[CHECK-REFERENCE-DEFINITION-1]]（2026-11-03）。原則（CHAT_RULES.md「検査・一覧・説明文が照合する相手」）: 照合する相手は
「書き手のコード」と「その出力が実際に読んだ入力」。実行の時刻・データの有る無し・過去のファイルで結果が変わる検査は誤り。
ここで見るのは git にあるファイル（コード・文書・設定）だけで、データの中身・時刻には依らない。

1. missing_doc_paths: 文書（DOC_FILES・DOC_GLOBS）とモジュールの説明文（テストを除く .py の先頭の docstring）に書かれた
   リポジトリ内のパス（git にあるトップのフォルダ〈common/・src/・docs/ 等〉で始まるもの）が、git にあるか。
   - 型（{T}・{TICKER}・YYYY-MM-DD・<...>・*）は何にでも当たるとして照合する。_ や - や . で終わる書きかけ（common/macro_ 等）は前方一致
   - 調べない: 「data/」で始まるもの（各サブシステムの data/ からの相対の書き方で、どこからの相対か決められない）、
     .gitignore で除外されるもの（手元にしか無い）、SKIP_LINE_RE の言葉（削除・廃止・予定 等）がある行・打ち消し線（~~…~~）の中
     （消したもの・これから作るものを書いた行）
   - 過去の記録（BACKLOG_DONE.md・FINDINGS.md・日付の記録の PROJECT_STATUS.md・CLAUDE_CODE_START.md の引き継ぎサマリー・
     docs/architecture/ の archive・BUNDLING_*・output_inventory）は対象外
2. consumer_claims: モジュールの説明文が「消費者（読み手）がいない」と書くのに、出力の一覧（docs/architecture/output_inventory/*_layer.json）
   ではそのフォルダの出力をフォルダの外の本番のコード・画面が読んでいるもの
3. workflow_name_mismatch: config/workflow_dependencies.json の各ワークフローの name（表示名）・scripts（動かすスクリプト）と、
   .github/workflows/*.yml の name: と実際に動かすスクリプト（run の python …）が一致するか。名前と中身の対応はこの JSON の1か所に書く
"""
from __future__ import annotations

import ast
import fnmatch
import json
import os
import posixpath
import re
import subprocess
from typing import Dict, List, Optional, Set, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DOC_FILES = ["SYSTEM_MAP.md", "CHAT_RULES.md", "CLAUDE_CODE_START.md"]
DOC_GLOBS = ["docs/architecture/*.md", "docs/architecture/*/*.md"]
DOC_EXCLUDE_RE = re.compile(r"(^|/)(archive|output_inventory)/|BUNDLING_")
# CLAUDE_CODE_START.md は引き継ぎサマリー（日付の記録）の後の、手順・ルールの部分だけ
DOC_START_HEADING = {"CLAUDE_CODE_START.md": "## 毎回の作業開始時に必ず実行すること"}
SKIP_LINE_RE = re.compile(r"削除|廃止|消した|やめた|移した|移設|無くなった|なくなった|予定|未実装|名前を変えた|置き換え|旧|まだ無い|例[:：]|（例")
STRIKE_RE = re.compile(r"~~.*?~~")
CLAIM_RE = re.compile(r"消費者(?:は|が)?(?:まだ)?(?:存在しない|いない|なし)|消費者なし|本番消費者は存在しない")
WORKFLOW_DEPS = "config/workflow_dependencies.json"


def _git_ls(*args: str) -> List[str]:
    r = subprocess.run(["git", "-C", REPO_ROOT, "ls-files", *args], capture_output=True, check=True)
    return [p for p in r.stdout.decode("utf-8").splitlines() if p]


def _read(rel: str) -> str:
    with open(os.path.join(REPO_ROOT, rel), encoding="utf-8") as f:
        return f.read()


class _Repo:
    def __init__(self, files: Optional[List[str]] = None):
        self.files = files if files is not None else _git_ls()
        self.fileset = set(self.files)
        self.dirs: Set[str] = set()
        for f in self.files:
            p = f
            while "/" in p:
                p = p.rsplit("/", 1)[0]
                self.dirs.add(p)
        tops = sorted({f.split("/")[0] for f in self.files if "/" in f} - {"data"})
        self.path_re = re.compile(r"(?<![\w./-])((?:" + "|".join(re.escape(t) for t in tops) + r")/(?:[A-Za-z0-9_./*<>\-]|\{[^}\s]*\})*)")
        self._ignored: Dict[str, bool] = {}

    def exists(self, p: str) -> bool:
        """そのままか、行末で折れた・書きかけ（_ や - や . で終わる）なら前方一致で、git にあるか"""
        q = re.sub(r"YYYY-MM-DD|YYYY|MM-DD|\{[^}]*\}|<[^>]*>", "*", p)
        cands = [q.rstrip(".,/:;)")]
        if q.rstrip(",/:;)").endswith(("_", "-", ".")):
            cands.append(q.rstrip(",/:;)") + "*")
        for c in cands:
            if "*" in c:
                rx = re.compile("^" + re.escape(c).replace(r"\*", "[^/]*") + "(/.*)?$")
                if any(rx.match(f) for f in self.files):
                    return True
            elif c in self.fileset or c in self.dirs:
                return True
        return False

    def ignored(self, p: str) -> bool:
        if p not in self._ignored:
            r = subprocess.run(["git", "-C", REPO_ROOT, "check-ignore", "-q", "--no-index", p.split("*")[0].split("{")[0]])
            self._ignored[p] = r.returncode == 0
        return self._ignored[p]


def doc_files(repo: Optional[_Repo] = None) -> List[str]:
    repo = repo or _Repo()
    out = [f for f in DOC_FILES if f in repo.fileset]
    out += sorted(f for f in repo.files if any(fnmatch.fnmatch(f, g) for g in DOC_GLOBS) and not DOC_EXCLUDE_RE.search(f))
    return out


def _scan_lines(rel: str, lines: List[Tuple[int, str]], repo: _Repo) -> List[Tuple[str, int, str]]:
    out = []
    for n, line in lines:
        if SKIP_LINE_RE.search(line):
            continue
        for m in repo.path_re.finditer(STRIKE_RE.sub("", line)):
            p = m.group(1)
            if not repo.exists(p) and not repo.ignored(p.rstrip(".,/:;)")):
                out.append((rel, n, p))
    return out


def module_docstring(rel: str) -> Tuple[Optional[str], int]:
    try:
        tree = ast.parse(_read(rel))
    except (SyntaxError, ValueError, OSError):
        return None, 0
    if tree.body and isinstance(tree.body[0], ast.Expr) and isinstance(getattr(tree.body[0], "value", None), ast.Constant) \
            and isinstance(tree.body[0].value.value, str):
        return tree.body[0].value.value, tree.body[0].lineno
    return None, 0


def missing_doc_paths(repo: Optional[_Repo] = None) -> List[Tuple[str, int, str]]:
    """(ファイル, 行, パス) の一覧。"""
    repo = repo or _Repo()
    out: List[Tuple[str, int, str]] = []
    for rel in doc_files(repo):
        lines = _read(rel).splitlines()
        start = 0
        head = DOC_START_HEADING.get(rel)
        if head:
            start = next((i for i, ln in enumerate(lines) if ln.startswith(head)), len(lines))
        out += _scan_lines(rel, [(i + 1, ln) for i, ln in enumerate(lines) if i >= start], repo)
    for rel in repo.files:
        if not rel.endswith(".py") or rel.startswith("tests/"):
            continue
        ds, line0 = module_docstring(rel)
        if ds:
            out += _scan_lines(rel, [(line0 + i, ln) for i, ln in enumerate(ds.splitlines())], repo)
    return out


def _inventory_readers_by_package() -> Dict[str, Set[str]]:
    """出力の一覧の、書き手のフォルダ → そのフォルダの外の本番の読み手（テストを除く）"""
    out: Dict[str, Set[str]] = {}
    inv_dir = os.path.join(REPO_ROOT, "docs", "architecture", "output_inventory")
    for name in sorted(os.listdir(inv_dir)) if os.path.isdir(inv_dir) else []:
        if not name.endswith("_layer.json"):
            continue
        with open(os.path.join(inv_dir, name), encoding="utf-8") as f:
            inv = json.load(f)
        for o in inv.get("outputs", []):
            for w in o.get("writers", []):
                pkg = posixpath.dirname(w.get("file", ""))
                for r in o.get("readers", []) + o.get("readers_compute", []):
                    rf = r.get("file", "")
                    if rf and not rf.startswith("tests/") and not rf.startswith(pkg + "/"):
                        out.setdefault(pkg, set()).add(rf)
    return out


def consumer_claims(repo: Optional[_Repo] = None) -> List[Tuple[str, int, int]]:
    """(説明文のファイル, 行, 一覧の読み手の数)。説明文が「消費者なし」と書くのに一覧では読み手がいるもの。"""
    repo = repo or _Repo()
    readers = _inventory_readers_by_package()
    out = []
    for rel in repo.files:
        if not rel.endswith(".py") or rel.startswith("tests/"):
            continue
        ds, line0 = module_docstring(rel)
        if not ds:
            continue
        for i, ln in enumerate(ds.splitlines()):
            if CLAIM_RE.search(ln) and not SKIP_LINE_RE.search(ln):
                n = len(readers.get(posixpath.dirname(rel), set()))
                if n:
                    out.append((rel, line0 + i, n))
    return out


_RUN_PY_RE = re.compile(r"\bpython3?\s+(?:-u\s+)?(?:(-m)\s+([\w.]+)|([\w./-]+\.py))")


def workflow_scripts(wf: dict) -> Set[str]:
    """ワークフローの run が動かすスクリプト（working-directory を足したリポジトリの中のパス。python -c・python - は除く）。"""
    out: Set[str] = set()
    wf_wd = ((wf.get("defaults") or {}).get("run") or {}).get("working-directory") or ""
    for job in (wf.get("jobs") or {}).values():
        job_wd = ((job.get("defaults") or {}).get("run") or {}).get("working-directory") or wf_wd
        for st in job.get("steps") or []:
            run = st.get("run")
            if not run:
                continue
            wd = st.get("working-directory") or job_wd
            for line in str(run).splitlines():
                if line.strip().startswith("#"):
                    continue
                for m in _RUN_PY_RE.finditer(line):
                    if m.group(1):
                        out.add(m.group(2).replace(".", "/") + ".py")
                    else:
                        out.add(posixpath.normpath(posixpath.join(wd, m.group(3))) if wd else m.group(3))
    return out


def workflow_name_mismatch() -> List[str]:
    import yaml
    with open(os.path.join(REPO_ROOT, WORKFLOW_DEPS), encoding="utf-8") as f:
        deps = json.load(f)
    out = []
    for key, v in sorted((deps.get("workflows") or {}).items()):
        path = os.path.join(REPO_ROOT, ".github", "workflows", v.get("yml", ""))
        if not os.path.exists(path):
            continue   # yml が無いことは CHECK-58（gen_update_schedule.py）が出す
        with open(path, encoding="utf-8") as f:
            wf = yaml.safe_load(f) or {}
        if "name" not in v or "scripts" not in v:
            out.append(f"{key}: {WORKFLOW_DEPS} に name（表示名）・scripts（動かすスクリプト）が無い")
            continue
        if wf.get("name") != v["name"]:
            out.append(f"{key}: 表示名が違う（{v['yml']} は「{wf.get('name')}」、{WORKFLOW_DEPS} は「{v['name']}」）")
        got, want = workflow_scripts(wf), set(v["scripts"])
        if got != want:
            out.append(f"{key}: 動かすスクリプトが違う（{v['yml']} は {sorted(got)}、{WORKFLOW_DEPS} は {sorted(want)}）。"
                       "中身を変えたら表示名・キーも中身に合わせる")
    return out


def check() -> List[str]:
    """rcc CHECK-68 の WARN の文の一覧。"""
    repo = _Repo()
    msgs = [f"  [WARN-68 文書のパスが無い] {f}:{n} の `{p}`。今のパスに直すか、消した・これから作るものなら行にそう書く"
            for f, n, p in missing_doc_paths(repo)]
    msgs += [f"  [WARN-68 説明文と一覧の読み手] {f}:{n} の説明文は消費者（読み手）がいないと書くが、出力の一覧ではフォルダの外の読み手が{k}件"
             for f, n, k in consumer_claims(repo)]
    msgs += [f"  [WARN-68 ワークフローの名前と中身] {m}" for m in workflow_name_mismatch()]
    return msgs


if __name__ == "__main__":
    for m in check():
        print(m)
