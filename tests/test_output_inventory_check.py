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
        "rules": [_rule(r, ["common/sec_data/a.py", "src/b.py"]) for r in inv.KNOWN_DUPLICATE_RULES],
    }


def test_same_inventory_has_no_ng_and_known_rules_are_warn():
    saved = _inventory()
    ng, warn = inv.check_inventory(saved, copy.deepcopy(saved))
    assert ng == []
    assert len([w for w in warn if "WARN-INV-3" in w]) == len(inv.KNOWN_DUPLICATE_RULES) == 5


def test_output_not_in_inventory_is_ng():
    saved = _inventory()
    now = copy.deepcopy(saved)
    now["outputs"].append(_output("common/sec_data/data/{T}/new_log.json"))
    ng, _ = inv.check_inventory(saved, now)
    assert any("NG-INV-1" in x and "new_log.json" in x for x in ng)


def test_reader_no_longer_reading_is_ng():
    saved = _inventory()
    now = copy.deepcopy(saved)
    now["outputs"][1]["readers"] = now["outputs"][1]["readers"][:1]   # docs/p.html が読まなくなった
    ng, _ = inv.check_inventory(saved, now)
    assert any("NG-INV-2" in x and "docs/p.html" in x for x in ng)


def test_zero_reader_output_is_ng():
    saved = _inventory()
    now = copy.deepcopy(saved)
    now["outputs"][0]["readers"] = []
    now["outputs"][0]["group"] = "読み手0件"
    ng, _ = inv.check_inventory(saved, now)
    assert any("読み手0件" in x and "a.json" in x for x in ng)


def test_sixth_duplicated_rule_is_ng_but_outside_layer_is_not():
    saved = _inventory()
    now = copy.deepcopy(saved)
    now["rules"].append(_rule("quarter_label_new", ["common/sec_data/q.py", "src/tail/q.py"]))
    now["rules"].append(_rule("outside_only", ["src/a.py", "src/b.py"], in_layer=False))
    ng, warn = inv.check_inventory(saved, now)
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


def test_tanuki_new_screen_key_without_writer_is_ng():
    saved = _tanuki_inventory()
    now = copy.deepcopy(saved)
    now["screen"]["items"].append({"page": "docs/value-monitor/tanuki_valuation/index.html", "key": "new_key", "status": "書き手なし", "lines": [9]})
    ng, _ = _check_tanuki(saved, now)
    assert any("NG-INV-5" in x and "new_key" in x for x in ng)


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
    ng, warn = inv.check_inventory(saved, now)
    assert ng == []
    assert any("WARN-INV-4" in x and "a.json" in x for x in warn)
