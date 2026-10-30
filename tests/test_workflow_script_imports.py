"""
ワークフローが `python <path>.py` で直接動かすスクリプトが、`common.`・`src.` を読めること（[[MACRODATA-FRED-IMPORT-PATH-1]]、2026-10-30）。

`python common/macro_data/fetcher.py` では検索パスの先頭がスクリプトのフォルダ（`common/macro_data/`）になり、リポジトリの根は入らない。
R-E2（2026-10-24）で関数の中に足した `from common.external_api import fred_client` が、CIでだけ No module named 'common' で失敗し、
FREDの25系列が6日間取れなかった。pytestは根をパスに入れて動くため、ふつうのテストでは見つからない。

確かめ方（スクリプトの本体は動かさない。通信・書き込みをしないため）:
  1. ワークフローの各stepの `run` から `python <path>.py` を拾う（`working-directory` を足す）
  2. スクリプトのASTの全体（関数の中も含む）から、`common`・`src` で始まるimportを拾う
  3. 子プロセス（cwd=根、PYTHONPATHなし）で `sys.path[0]` をスクリプトのフォルダにする（`python <path>` と同じ。`python -c` のままだと
     sys.path[0] が '' ＝cwd＝根になり、根が入っていないことを見逃す）。モジュールの最上位の文のうち、パスを決める文
     （os・sys・pathlibのimport、代入、sys.pathを触るif・try・式）だけを順に実行し、2で拾ったモジュールがすべて根の下に見つかるか
"""
import ast
import glob
import json
import os
import re
import subprocess
import sys

import pytest
import yaml

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# 子プロセスで動かす検査。argv: スクリプトの絶対パス、根、見つかるべきモジュール（JSON）
_PROBE = r'''
import ast, importlib.util, json, os, sys
path, root, mods = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
sys.path[0] = os.path.dirname(path)            # python <path> と同じ先頭（-c の '' ＝cwd＝根を置き換える）
_root = os.path.abspath(root)
sys.path = sys.path[:1] + [p for p in sys.path[1:] if os.path.abspath(p or ".") != _root]   # 念のため根を除く
tree = ast.parse(open(path, encoding="utf-8").read())

def touches_sys_path(node):
    return any(isinstance(x, ast.Attribute) and ast.unparse(x).startswith("sys.path") for x in ast.walk(node))

def is_main_guard(node):
    return isinstance(node, ast.If) and "__name__" in ast.unparse(node.test)

SAFE = ("os", "sys", "pathlib")

def path_statements(node):
    """最上位の1文から、パスを決めるために実行する文を返す（無ければ空）。"""
    if isinstance(node, ast.Import):          # import os, sys, json … からは os・sys・pathlib だけ
        names = [a for a in node.names if a.name.split(".")[0] in SAFE]
        return [ast.Import(names=names)] if names else []
    if isinstance(node, ast.ImportFrom):
        return [node] if (node.module or "").split(".")[0] in SAFE and node.level == 0 else []
    if isinstance(node, (ast.FunctionDef, ast.Assign, ast.AnnAssign)):   # 関数は定義するだけ
        return [node]
    if is_main_guard(node):                   # if __name__ == "__main__": の中は sys.path を触る文だけ
        return [s for s in node.body if touches_sys_path(s)]
    if isinstance(node, (ast.If, ast.Try, ast.Expr)) and touches_sys_path(node):
        return [node]
    return []

g = {"__file__": path, "__name__": "__main__"}   # python <path> で動かすときの名前
for node in tree.body:
    for stmt in path_statements(node):
        try:
            mod = ast.fix_missing_locations(ast.Module(body=[stmt], type_ignores=[]))
            exec(compile(mod, path, "exec"), g)
        except BaseException:
            pass

root_on_path = any(os.path.abspath(p or ".") == os.path.abspath(root) for p in sys.path)
missing = []
for m in mods:
    try:
        spec = importlib.util.find_spec(m)
    except BaseException as e:
        spec = None
    if spec is None or not os.path.abspath(spec.origin or "").startswith(os.path.abspath(root)):
        missing.append(m)
print(json.dumps({"root_on_path": root_on_path, "missing": missing}))
'''


def _workflow_scripts():
    scripts = set()
    for wf in sorted(glob.glob(os.path.join(_ROOT, ".github", "workflows", "*.yml"))):
        with open(wf, encoding="utf-8") as f:
            d = yaml.safe_load(f)
        for job in (d.get("jobs") or {}).values():
            for st in job.get("steps") or []:
                wd = st.get("working-directory") or ""
                for m in re.finditer(r"\bpython3?\s+([\w./-]+\.py)\b", st.get("run") or ""):
                    scripts.add(os.path.normpath(os.path.join(wd, m.group(1))).replace("\\", "/"))
    return sorted(scripts)


def _project_imports(path):
    """ASTの全体（関数の中も含む）から、common・srcで始まるimportのモジュール名を拾う。"""
    with open(path, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    mods = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.level == 0 and n.module:
            names = [n.module]
        elif isinstance(n, ast.Import):
            names = [a.name for a in n.names]
        else:
            continue
        mods |= {m for m in names if m.split(".")[0] in ("common", "src")}
    return sorted(mods)


def _probe(path, root, mods):
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    out = subprocess.run([sys.executable, "-c", _PROBE, path, root, json.dumps(mods)],
                         cwd=root, env=env, capture_output=True, text=True, encoding="utf-8", timeout=120)
    assert out.returncode == 0, out.stderr[-2000:]
    return json.loads(out.stdout.strip().splitlines()[-1])


_SCRIPTS = _workflow_scripts()


def test_workflow_scripts_found():
    assert "common/macro_data/fetcher.py" in _SCRIPTS
    assert "src/value/tanuki_valuation/pipeline.py" in _SCRIPTS   # working-directory 形
    assert len(_SCRIPTS) >= 25


@pytest.mark.parametrize("script", _SCRIPTS)
def test_workflow_script_can_import_project_modules(script):
    path = os.path.join(_ROOT, script)
    mods = _project_imports(path)
    if not mods:
        pytest.skip("common・srcを読まない")
    r = _probe(path, _ROOT, mods)
    assert r["root_on_path"], f"{script}: python {script} で動かすとリポジトリの根がsys.pathに入らない（{mods}）"
    assert not r["missing"], f"{script}: 見つからないモジュール {r['missing']}"


def test_probe_detects_missing_root_path(tmp_path):
    """直す前の fetcher.py の形（関数の中だけで common を読み、根を足さない）は見つかり、直した形は通る。"""
    pkg = tmp_path / "common" / "macro_data"
    pkg.mkdir(parents=True)
    (tmp_path / "common" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "common" / "external_api.py").write_text("def fred_client():\n    return 1\n", encoding="utf-8")
    body = "def _get_fred_client():\n    from common.external_api import fred_client\n    return fred_client()\n"
    before = pkg / "before.py"
    before.write_text("import os\n\n" + body, encoding="utf-8")
    after = pkg / "after.py"
    after.write_text(
        "import os\nimport sys\n\n"
        "_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))\n"
        "if _REPO_ROOT not in sys.path:\n    sys.path.insert(0, _REPO_ROOT)\n\n" + body, encoding="utf-8")
    assert _project_imports(str(before)) == ["common.external_api"]
    r = _probe(str(before), str(tmp_path), ["common.external_api"])
    assert r["root_on_path"] is False and r["missing"] == ["common.external_api"]
    r = _probe(str(after), str(tmp_path), ["common.external_api"])
    assert r["root_on_path"] is True and r["missing"] == []
