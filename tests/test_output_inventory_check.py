"""[[OUTPUT-INVENTORY-CHECK-1]] 段階3: 出力の一覧の CHECK-INV-1〜4（scripts/inventory/build_output_inventory.py::check_inventory）。

保存した一覧（sec_layer.json）と、今のリポジトリから作った一覧を比べる。ここでは小さな一覧を手で作って比べる。
"""
import copy
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_INV_DIR = os.path.join(_REPO_ROOT, "scripts", "inventory")
if _INV_DIR not in sys.path:
    sys.path.insert(0, _INV_DIR)

import build_output_inventory as inv  # noqa: E402

# SEC層の既知の規則の二重。inv.KNOWN_DUPLICATE_RULES はその時の段階（set_layer() を最後に呼んだもの）で変わるため、段階の設定から取る
SEC_KNOWN = inv.LAYERS["sec"]["KNOWN_DUPLICATE_RULES"]


def _check_sec(saved, now):
    return inv.check_inventory(saved, now, known_rules=SEC_KNOWN, out_json="sec_layer.json", tag="SEC層")


def _output(path, readers=("src/value/tanuki_valuation/pipeline.py",), group="1件", note="注記"):
    return {
        "path": path,
        "writers": [{"file": "common/sec_data/x.py", "func": "save", "line": 1}],
        "readers": [{"file": f, "line": 10, "type": "Python", "how": "保存済みファイル", "via": "AST"} for f in readers],
        "readers_compute": [],
        "group": group,
        "note": note,
    }


def _rule(rule_id, files, in_layer=True):
    return {"id": rule_id, "name": rule_id, "files": list(files), "in_layer": in_layer, "duplicated": len(files) >= 2}


def _inventory():
    return {
        "outputs": [_output("common/sec_data/data/{T}/a.json"),
                    _output("common/sec_data/data/{T}/b.json", readers=("src/tail/x.py", "docs/p.html"), group="2件以上")],
        "rules": [_rule(r, ["common/sec_data/a.py", "src/b.py"]) for r in SEC_KNOWN],
    }


def test_same_inventory_has_no_ng_and_known_rules_are_warn():
    saved = _inventory()
    ng, warn = _check_sec(saved, copy.deepcopy(saved))
    assert ng == []
    assert len([w for w in warn if "WARN-INV-3" in w]) == len(SEC_KNOWN) == 5


def test_output_not_in_inventory_is_ng():
    saved = _inventory()
    now = copy.deepcopy(saved)
    now["outputs"].append(_output("common/sec_data/data/{T}/new_log.json"))
    ng, _ = _check_sec(saved, now)
    assert any("NG-INV-1" in x and "new_log.json" in x for x in ng)


def test_reader_no_longer_reading_is_ng():
    saved = _inventory()
    now = copy.deepcopy(saved)
    now["outputs"][1]["readers"] = now["outputs"][1]["readers"][:1]   # docs/p.html が読まなくなった
    ng, _ = _check_sec(saved, now)
    assert any("NG-INV-2" in x and "docs/p.html" in x for x in ng)


def test_zero_reader_output_is_ng():
    saved = _inventory()
    now = copy.deepcopy(saved)
    now["outputs"][0]["readers"] = []
    now["outputs"][0]["group"] = "読み手0件"
    ng, _ = _check_sec(saved, now)
    assert any("読み手0件" in x and "a.json" in x for x in ng)


def test_sixth_duplicated_rule_is_ng_but_outside_layer_is_not():
    saved = _inventory()
    now = copy.deepcopy(saved)
    now["rules"].append(_rule("quarter_label_new", ["common/sec_data/q.py", "src/tail/q.py"]))
    now["rules"].append(_rule("outside_only", ["src/a.py", "src/b.py"], in_layer=False))
    ng, warn = _check_sec(saved, now)
    assert any("NG-INV-3" in x and "quarter_label_new" in x for x in ng)
    assert not any("outside_only" in x for x in ng + warn)


def _tanuki_inventory():
    inv_ = _inventory()
    inv_["rules"] = [_rule(r, ["src/value/tanuki_valuation/a.py", "docs/p.html"]) for r in inv.LAYERS["tanuki"]["KNOWN_DUPLICATE_RULES"]]
    inv_["screen"] = {
        "items": [{"page": "docs/value-monitor/tanuki_valuation/stock.html", "key": "rice_na_reason", "status": "書き手なし", "lines": [1]},
                  {"page": "docs/value-monitor/tanuki_valuation/stock.html", "key": "upside_percent", "status": "この段階の書き手あり", "lines": [2]}],
        "unread_keys": [{"key": "v0_note", "sources": ["docs/value-monitor/tanuki_valuation/data/{T}/latest.json"]}],
    }
    return inv_


def _check_tanuki(saved, now):
    known = inv.LAYERS["tanuki"]["KNOWN_DUPLICATE_RULES"]
    return inv.check_inventory(saved, now, known_rules=known, out_json="tanuki_layer.json", tag="TANUKI")


def test_tanuki_known_rules_and_known_screen_keys_are_not_ng():
    saved = _tanuki_inventory()
    ng, warn = _check_tanuki(saved, copy.deepcopy(saved))
    assert ng == []
    assert len([w for w in warn if "WARN-INV-3" in w]) == len(inv.LAYERS["tanuki"]["KNOWN_DUPLICATE_RULES"]) == 5


def _new_screen_key_without_writer():
    saved = _tanuki_inventory()
    now = copy.deepcopy(saved)
    now["screen"]["items"].append({"page": "docs/value-monitor/tanuki_valuation/index.html", "key": "new_key", "status": "書き手なし", "lines": [9]})
    return saved, now


def test_tanuki_new_screen_key_without_writer_is_ng(monkeypatch):
    """手元のゲート（GITHUB_ACTIONS が無い）では NG。"""
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    ng, warn = _check_tanuki(*_new_screen_key_without_writer())
    assert any("NG-INV-5" in x and "new_key" in x for x in ng)
    assert not any("INV-5" in x for x in warn)


def test_tanuki_new_screen_key_without_writer_is_warn_on_github_actions(monkeypatch):
    """本番のワークフロー（GITHUB_ACTIONS=true）では WARN にして、JSON の中身の変化で公開を止めない。"""
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    ng, warn = _check_tanuki(*_new_screen_key_without_writer())
    assert ng == []
    assert any("WARN-INV-5" in x and "new_key" in x for x in warn)


def test_github_actions_does_not_soften_code_only_checks(monkeypatch):
    """INV-1〜4 はコードだけで決まるので、本番のワークフローでも NG のまま。"""
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    saved = _tanuki_inventory()
    now = copy.deepcopy(saved)
    now["outputs"].append(_output("docs/value-monitor/tanuki_valuation/data/{T}/new.json"))
    now["outputs"][0]["readers"] = []
    now["outputs"][0]["group"] = "読み手0件"
    now["rules"].append(_rule("new_rule", ["src/value/tanuki_valuation/a.py", "docs/q.html"]))
    ng, _ = _check_tanuki(saved, now)
    assert any("NG-INV-1" in x for x in ng)
    assert any("NG-INV-2" in x for x in ng)
    assert any("NG-INV-3" in x and "new_rule" in x for x in ng)


def test_tanuki_new_unread_key_is_warn():
    saved = _tanuki_inventory()
    now = copy.deepcopy(saved)
    now["screen"]["unread_keys"].append({"key": "new_unread", "sources": ["docs/x.json"]})
    ng, warn = _check_tanuki(saved, now)
    assert ng == []
    assert any("WARN-INV-6" in x and "new_unread" in x for x in warn)


def test_tanuki_output_not_in_inventory_and_reader_removed_are_ng():
    saved = _tanuki_inventory()
    now = copy.deepcopy(saved)
    now["outputs"].append(_output("docs/value-monitor/tanuki_valuation/data/{T}/new.json"))
    now["outputs"][1]["readers"] = now["outputs"][1]["readers"][:1]
    ng, _ = _check_tanuki(saved, now)
    assert any("NG-INV-1" in x and "new.json" in x for x in ng)
    assert any("NG-INV-2" in x and "docs/p.html" in x for x in ng)


def test_missing_annotation_is_warn():
    saved = _inventory()
    now = copy.deepcopy(saved)
    now["outputs"][0]["note"] = ""
    ng, warn = _check_sec(saved, now)
    assert ng == []
    assert any("WARN-INV-4" in x and "a.json" in x for x in warn)


def test_hypecore_layer_is_checked_and_ttm_rule_is_known_warn():
    """HypeCore の段階3（2026-10-21）: CHECK-INV を当て、既知の規則の二重（TTMの作り方）は WARN、それ以外の新しい二重は NG。"""
    assert "hypecore" in inv.CHECK_LAYERS
    known = inv.LAYERS["hypecore"]["KNOWN_DUPLICATE_RULES"]
    assert known == ["ttm_sum"]
    saved = _inventory()
    saved["rules"] = [_rule("ttm_sum", ["src/value/hypecore/hypecore.py", "common/sec_data/ttm_calculator.py"])]
    now = copy.deepcopy(saved)
    now["rules"].append(_rule("stage_rule", ["src/value/hypecore/hypecore.py", "docs/x.html"]))
    ng, warn = inv.check_inventory(saved, now, known_rules=known, out_json="hypecore_layer.json", tag="HypeCore")
    assert any("WARN-INV-3" in x and "ttm_sum" in x for x in warn)
    assert any("NG-INV-3" in x and "stage_rule" in x for x in ng)



def test_eps_layer_is_checked_and_known_rules_are_warn():
    """EPS の段階3（2026-10-21）: CHECK-INV を当て、既知の規則の二重（Q4の逆算・同じ期の値の選び方・TTMの作り方）は WARN、それ以外の新しい二重は NG。"""
    assert "eps" in inv.CHECK_LAYERS
    known = inv.LAYERS["eps"]["KNOWN_DUPLICATE_RULES"]
    assert known == ["q4_implied", "latest_filed", "ttm_sum"]
    saved = _inventory()
    saved["rules"] = [_rule(r, ["src/value/adjusted_eps_analyzer/extract_key_facts.py", "common/sec_data/layer3_builder.py"]) for r in known]
    now = copy.deepcopy(saved)
    now["rules"].append(_rule("adj_rule", ["src/value/adjusted_eps_analyzer/pipeline.py", "docs/x.html"]))
    ng, warn = inv.check_inventory(saved, now, known_rules=known, out_json="eps_layer.json", tag="EPS")
    assert len([x for x in warn if "WARN-INV-3" in x]) == 3
    assert any("NG-INV-3" in x and "adj_rule" in x for x in ng)


def test_eps_layer_has_no_admin_page_checks():
    """EPS の段階2（2026-10-21）で管理のページ2つを消した。一覧のスクリプトが消したページを確かめに行かない。"""
    assert inv.LAYERS["eps"]["PAGE_CHECKS"] == []
    assert not os.path.exists(os.path.join(inv.REPO, "docs", "value-monitor", "admin.html"))
    assert not os.path.exists(os.path.join(inv.REPO, "docs", "value-monitor", "adjusted_eps_analyzer", "admin.html"))

# ---- 読み手の照合（2026-10-21）: 行のパスを解決して出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルは読み手にしない

TANUKI_TICKERS = "docs/value-monitor/tanuki_valuation/data/tickers.json"
HYPECORE_TICKERS = "docs/value-monitor/hypecore/data/tickers.json"


def _mention(monkeypatch, rel, src, line_no, tmpl):
    import ast
    monkeypatch.setattr(inv, "TOP_DIRS", {"docs", "src", "common", "config"})
    if rel.endswith(".py"):
        monkeypatch.setitem(inv.MODULES, rel, inv.Module(rel, ast.parse(src), src))
    inv._MENTIONS.pop((rel, line_no), None)
    pats, _ = inv.needles_for(tmpl)
    line = src.splitlines()[line_no - 1]
    return inv.mention_class(tmpl, pats, rel, line_no, line, src)


def test_page_reading_same_name_file_in_other_folder_is_not_a_reader(monkeypatch):
    src = "<script>\nfetch('data/tickers.json').then(r => r.json());\n</script>\n"
    rel = "docs/value-monitor/tanuki_valuation/test_page_a.html"
    assert _mention(monkeypatch, rel, src, 2, TANUKI_TICKERS) == "reader"
    assert _mention(monkeypatch, rel, src, 2, HYPECORE_TICKERS) == "no"


def test_page_path_built_from_constant_is_resolved(monkeypatch):
    src = "const BASE = '/On-a-journey';\nconst H_BASE = BASE + '/value-monitor/hypecore/data/';\nfetch(H_BASE + 'tickers.json');\n"
    rel = "docs/portfolio/test_page_b.html"
    assert _mention(monkeypatch, rel, src, 3, HYPECORE_TICKERS) == "reader"
    assert _mention(monkeypatch, rel, src, 3, TANUKI_TICKERS) == "no"


def test_python_join_to_other_folder_and_dict_key_are_not_readers(monkeypatch):
    src = ("import os\n"
           "P = os.path.join(ROOT, 'docs', 'value-monitor', 'tanuki_valuation', 'data', 'tickers.json')\n"
           "x = data.get('tickers', [])\n")
    rel = "src/x/test_reader_c.py"
    assert _mention(monkeypatch, rel, src, 2, TANUKI_TICKERS) == "reader"
    assert _mention(monkeypatch, rel, src, 2, HYPECORE_TICKERS) == "no"
    assert _mention(monkeypatch, rel, src, 3, HYPECORE_TICKERS) == "no"


def test_relative_path_in_shared_script_is_undetermined(monkeypatch):
    """.js の相対パスは読み込むページのフォルダからになるため、フォルダを決めきれない（止める条件で数える）。"""
    src = "fetch('data/tickers.json');\n"
    assert _mention(monkeypatch, "docs/common/test_shared_d.js", src, 1, HYPECORE_TICKERS) == "undetermined"


def test_import_from_own_folder_is_resolved(monkeypatch):
    """sys.path に自分のフォルダを足して読む形（from core_calculator import …）は、同じフォルダのモジュールにする（2026-10-21）。"""
    monkeypatch.setattr(inv, "ALL_PY", {"src/v/tv/core_calc.py", "src/v/tv/calculator/adj.py", "src/v/tv/calculator/__init__.py",
                                        "src/v/tv/pipeline.py", "common/x.py"})
    assert inv.resolve_import_module("src/v/tv/pipeline.py", "core_calc", 0) == "src/v/tv/core_calc"
    assert inv.resolve_import_module("src/v/tv/pipeline.py", "calculator.adj", 0) == "src/v/tv/calculator/adj"
    assert inv.resolve_import_module("src/v/tv/pipeline.py", "common.x", 0) == "common/x"
    assert inv.resolve_import_module("src/v/tv/pipeline.py", "json", 0) == "json"


def test_empty_string_default_is_resolved_from_callers(monkeypatch):
    """引数の既定値が空の文字列（渡されなければ使わない）のときは、呼び出し元の引数からパスを決める（2026-10-21）。"""
    import ast
    src = ("import os\n"
           "def load(t, d: str = ''):\n"
           "    return os.path.join(d, t, 'annual.json')\n"
           "def run():\n"
           "    load('X', os.path.join('docs', 'eps', 'data'))\n")
    rel = "src/x/test_default_e.py"
    mod = inv.Module(rel, ast.parse(src), src)
    monkeypatch.setitem(inv.MODULES, rel, mod)
    monkeypatch.setitem(inv.CALLERS, (rel, "load"), [(rel, 5)])
    join = next(n for n in ast.walk(mod.tree) if isinstance(n, ast.Call) and getattr(n, "lineno", 0) == 3)
    assert inv.normalize_path(inv.resolve(join, mod, join)) == "docs/eps/data/{T}/annual.json"
