"""[[OUTPUT-INVENTORY-CHECK-1]] 段階3: 出力の一覧の CHECK-INV-1〜4（scripts/inventory/inventory_analysis.py::check_inventory）。

保存した一覧（sec_layer.json）と、今のリポジトリから作った一覧を比べる。ここでは小さな一覧を手で作って比べる。
"""
import copy
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_INV_DIR = os.path.join(_REPO_ROOT, "scripts", "inventory")
if _INV_DIR not in sys.path:
    sys.path.insert(0, _INV_DIR)

import inventory_analysis as inv  # noqa: E402  解析・CHECK-INV（段階の設定も set_layer() でここに入る）

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
    rel = "src/x/default_e.py"   # test_ で始まる名前はテストの呼び出し（候補にしない）になるので避ける
    mod = inv.Module(rel, ast.parse(src), src)
    monkeypatch.setitem(inv.MODULES, rel, mod)
    monkeypatch.setitem(inv.CALLERS, (rel, "load"), [(rel, 5)])
    join = next(n for n in ast.walk(mod.tree) if isinstance(n, ast.Call) and getattr(n, "lineno", 0) == 3)
    assert inv.normalize_path(inv.resolve(join, mod, join)) == "docs/eps/data/{T}/annual.json"


def test_workflow_working_directory_runs_file():
    """working-directory: <フォルダ> の段で python <ファイル名> と動かす形も、そのファイルを動かすワークフローとする（2026-10-21、STONKS SILO・TANUKI）。"""
    wf = ("      - name: Run\n        working-directory: src/value/stonks_silo\n        run: |\n          python pipeline.py $TICKERS\n")
    assert inv.workflow_runs_file(wf, "src/value/stonks_silo/pipeline.py")
    assert not inv.workflow_runs_file(wf, "src/value/stonks_silo/analyzer.py")
    assert not inv.workflow_runs_file(wf, "src/value/tanuki_valuation/pipeline.py")
    assert inv.workflow_runs_file("python -m src.value.adjusted_eps_analyzer.pipeline", "src/value/adjusted_eps_analyzer/pipeline.py")


def test_stonks_layer_is_checked_and_staleness_rule_is_known_warn():
    """STONKS SILO の段階3（2026-10-21）: CHECK-INV を当て、既知の規則の二重（データの古さ・遅れの判定）は WARN、それ以外の新しい二重は NG。"""
    assert "stonks" in inv.CHECK_LAYERS
    known = inv.LAYERS["stonks"]["KNOWN_DUPLICATE_RULES"]
    assert known == ["staleness"]
    assert inv.LAYERS["stonks"]["LAYER_DIRS"] == ("src/value/stonks_silo",)
    saved = _inventory()
    saved["rules"] = [_rule("staleness", ["src/value/stonks_silo/financial_as_of.py", "common/sec_data/freshness.py"])]
    now = copy.deepcopy(saved)
    now["rules"].append(_rule("verdict_rule", ["src/value/stonks_silo/analyzer.py", "docs/x.html"]))
    ng, warn = inv.check_inventory(saved, now, known_rules=known, out_json="stonks_layer.json", tag="STONKS")
    assert any("WARN-INV-3" in x and "staleness" in x for x in warn)
    assert any("NG-INV-3" in x and "verdict_rule" in x for x in ng)


def test_loose_reference_compares_whole_path():
    """名前だけで読む使い方の参照: 引用符の中のパスに / があればパスの全体で照合する（2026-10-21、register_ticker.py の取り違え）。"""
    rel = "src/value/stonks_silo/pipeline.py"
    assert not inv.loose_ref('    rc = _run([PYTHON, "src/value/tanuki_valuation/pipeline.py", ticker],', rel)
    assert inv.loose_ref('    path = "src/value/stonks_silo/pipeline.py"', rel)
    assert inv.loose_ref("spec = load('pipeline.py')", rel)
    assert not inv.loose_ref("spec = load('mypipeline.py')", rel)
    assert inv.loose_ref("          python pipeline.py $TICKERS", rel)
    assert inv.loose_ref("from pipeline import run", rel)


def _strict_module(monkeypatch, rel, src, callers):
    """小さなモジュールと呼び出し元の索引を作る（解決の厳しさは2026-10-23から全段階で同じ）。"""
    import ast
    mod = inv.Module(rel, ast.parse(src), src)
    monkeypatch.setitem(inv.MODULES, rel, mod)
    for name, lines in callers.items():
        monkeypatch.setitem(inv.CALLERS, (rel, name), [(r, ln) for r, ln in lines])
    return mod


def test_strict_join_joins_each_caller_alternative_and_skips_test_callers(monkeypatch):
    """呼び出し元ごとに違う base_dir は候補ごとにつなぎ、テストが渡す一時のフォルダは候補にしない（2026-10-22、市場データ層）。"""
    import ast
    src = ("import os\n"
           "def save(sym, base_dir):\n"
           "    return os.path.join(base_dir, 'daily', f'{sym}.json')\n"
           "def run_a():\n"
           "    save('X', 'common/md')\n"
           "def run_b():\n"
           "    save('X', 'common/md2')\n")
    rel = "common/md/join_f.py"
    test_rel = "tests/test_join_f.py"
    test_src = "def test_x(tmp_path):\n    save('X', tmp_path)\n"
    monkeypatch.setitem(inv.MODULES, test_rel, inv.Module(test_rel, ast.parse(test_src), test_src))
    mod = _strict_module(monkeypatch, rel, src, {"save": [(rel, 5), (rel, 7), (test_rel, 2)]})
    join = next(n for n in ast.walk(mod.tree) if isinstance(n, ast.Call) and getattr(n, "lineno", 0) == 3)
    got = inv.normalize_path(inv.resolve(join, mod, join)).split(inv.ALT)
    assert got == ["common/md/daily/{T}.json", "common/md2/daily/{T}.json"]


def test_strict_cli_argument_path_is_resolved_from_workflow(monkeypatch):
    """argparse の引数（args.status_file）のパスは、ワークフローがそのファイルに渡す値から決める（2026-10-22、--status-file /tmp/…）。"""
    import ast
    src = ("import argparse, json\n"
           "p = argparse.ArgumentParser()\n"
           "p.add_argument('--status-file', default='')\n"
           "args = p.parse_args()\n"
           "with open(args.status_file, 'w') as f:\n"
           "    json.dump({}, f)\n")
    rel = "common/md/cli_g.py"
    mod = _strict_module(monkeypatch, rel, src, {})
    wf = "      - run: python common/md/cli_g.py --layer daily --status-file /tmp/st.json\n"
    monkeypatch.setattr(inv, "git_ls_files", lambda *a: [".github/workflows/x.yml"])
    monkeypatch.setattr(inv, "read_text", lambda rel_: wf)
    writes, _ = inv.io_sites(mod)
    assert [w["path"] for w in writes] == ["/tmp/st.json"]


def test_strict_helper_callers_are_the_readers_and_writers(monkeypatch):
    """パスを引数で受けて読み書きする汎用の関数は、そのパスを渡す呼び出し箇所を読み手・書き手にする（IO_HELPER_BY_CALLER、2026-10-22）。"""
    import ast
    src = ("import os, json\n"
           "def _load(path):\n"
           "    with open(path) as f:\n"
           "        return json.load(f)\n"
           "def get_attrs(sym):\n"
           "    return _load(os.path.join('common', 'md', 'attributes', f'{sym}.json'))\n"
           "def get_daily(sym):\n"
           "    return _load(os.path.join('common', 'md', 'daily', f'{sym}.json'))\n")
    rel = "common/md/helper_h.py"
    _strict_module(monkeypatch, rel, src, {"_load": [(rel, 6), (rel, 8)]})
    got = inv.helper_callers(rel, {"line": 3})
    assert [(c[1], c[2], c[3]) for c in got] == [(6, "get_attrs", "common/md/attributes/{T}.json"), (8, "get_daily", "common/md/daily/{T}.json")]


def test_strict_external_module_call_is_not_matched_by_name(monkeypatch):
    """リポジトリの外のモジュールの関数（mcal.get_calendar 等）を、同じ名前のリポジトリの関数の呼び出しにしない（2026-10-22）。"""
    import ast
    src = "import pandas_market_calendars as mcal\ncal = mcal.get_calendar_nyse('NYSE')\nx = obj.get_calendar_nyse('NYSE')\n"
    rel = "common/md/ext_i.py"
    mod = _strict_module(monkeypatch, rel, src, {})
    monkeypatch.setitem(inv.DEF_INDEX, "get_calendar_nyse", [("common/md/reader.py", "get_calendar_nyse")])
    assert [(ln, nm) for ln, _r, nm in inv._calls_in(mod)] == [(3, "get_calendar_nyse")]


def test_strict_git_add_line_and_index_symbol(monkeypatch):
    """ワークフローの git add の行は読み手にしない。{T} は指数の記号（^GSPC）にも当たる（2026-10-22）。"""
    tmpl = "common/md/_retry_log.json"
    pats, _ = inv.needles_for(tmpl)
    line = "          git add common/md/_retry_log.json || true"
    assert inv.mention_class(tmpl, pats, ".github/workflows/x.yml", 1, line, line) == "no"
    assert inv.template_regex("common/md/daily/{T}.json").match("common/md/daily/^GSPC.json")


def test_market_data_layer_settings():
    """市場データ層（段階1・3、2026-10-22）: 画面を持たない共有のデータ層。
    段階ごとの印は2026-10-23に全段階で1つの設定にした。汎用の読み書きの関数の印だけは市場データ層・マクロデータ層だけ（SEC層で本当の読み手が消えるため）。"""
    md = inv.LAYERS["market_data"]
    assert md["LAYER_DIRS"] == ("common/market_data",)
    assert md["IO_HELPER_BY_CALLER"] and not md["SCREEN_PAGES"]
    assert inv.CHECK_LAYERS[-6:] == ("market_data", "macro_data", "macro_pulse", "tail", "market_pulse", "portfolio")
    assert [k for k in inv.LAYERS if inv.LAYERS[k]["IO_HELPER_BY_CALLER"]] == ["market_data", "macro_data"]
    for flag in ("RESOLVE_STRICT", "FULL_SCAN", "SCREEN_SECTIONS", "WORKFLOW_SUMMARY", "NONPROD_SCAN", "DATA_UNREAD_KEYS", "CSV_COLUMNS", "JS_ID_HELPERS"):
        assert not any(flag in inv.LAYERS[k] for k in inv.LAYERS) and not hasattr(inv, flag)
    assert md["KNOWN_DUPLICATE_RULES"] == ["trading_day", "provisional_bar", "sp500_constituents"]


def test_market_data_check_known_rules_warn_and_new_unread_key_warns():
    """市場データ層の CHECK-INV（2026-10-22）: 既知の規則の二重3つは WARN、それ以外の新しい二重は NG。
    誰も読まないキー（data_keys）は新しくできたものだけ WARN-INV-6。"""
    known = inv.LAYERS["market_data"]["KNOWN_DUPLICATE_RULES"]
    saved = _inventory()
    saved["rules"] = [_rule("trading_day", ["common/market_data/fetcher.py", "src/market/market_pulse/breadth_calculator.py"])]
    saved["data_keys"] = {"unread_keys": [{"key": "eps_actual", "sources": ["common/market_data/analyst_history/{T}.json"]}],
                          "written_key_count": 2}
    now = copy.deepcopy(saved)
    now["rules"].append(_rule("bar_close_rule", ["common/market_data/fetcher.py", "docs/x.html"]))
    now["data_keys"]["unread_keys"].append({"key": "new_key", "sources": ["common/market_data/daily/{T}.json"]})
    ng, warn = inv.check_inventory(saved, now, known_rules=known, out_json="market_data_layer.json", tag="市場データ層")
    assert any("WARN-INV-3" in x and "trading_day" in x for x in warn)
    assert any("NG-INV-3" in x and "bar_close_rule" in x for x in ng)
    assert [x for x in warn if "WARN-INV-6" in x and "new_key" in x] and not [x for x in warn if "eps_actual" in x]


def test_strict2_loop_over_names_and_same_folder_import(monkeypatch):
    """（TAIL の段階から）for path in (a, b) の変数は並びのそれぞれのパス。
    import sec_ctrl_fetcher as _ctrl（sys.path に自分のフォルダを足す形）は同じフォルダのモジュール（2026-10-22）。"""
    import ast
    src = ("import os\n"
           "BASE = os.path.join('docs', 'tail', 'data')\n"
           "def save(t, q):\n"
           "    a = os.path.join(BASE, t, f'{q}.json')\n"
           "    b = os.path.join(BASE, t, 'latest.json')\n"
           "    for path in (a, b):\n"
           "        open(path, 'w')\n")
    rel = "src/tl/ctrl_k.py"
    mod = _strict_module(monkeypatch, rel, src, {})
    writes, _ = inv._io_sites(mod)
    assert [w["path"] for w in writes] == ["docs/tail/data/{T}/{Q}.json", "docs/tail/data/{T}/latest.json"]
    monkeypatch.setattr(inv, "ALL_PY", {"src/tl/ctrl_k.py", "src/tl/items_l.py"})
    isrc = "import ctrl_k as _ctrl\nDATA_DIR = _ctrl.BASE\n"
    assert inv.Module("src/tl/items_l.py", ast.parse(isrc), isrc).mod_imports == {"_ctrl": "src/tl/ctrl_k.py"}
    monkeypatch.setattr(inv, "ALL_PY", {"src/tl/items_l.py"})   # 同じフォルダに無いモジュールはリポジトリの外
    assert inv.Module("src/tl/items_l.py", ast.parse(isrc), isrc).mod_imports == {}


def test_tail_layer_settings():
    """TAIL（段階1・3、2026-10-22）: 汎用の読み書きの関数の印は使わない。
    既知の規則の二重は TAIL を含む4つ（同じ期の値の選び方・期の名前・会計年度・AIの呼び方）。"""
    tl = inv.LAYERS["tail"]
    assert tl["LAYER_DIRS"] == ("src/tail",)
    assert not tl["IO_HELPER_BY_CALLER"]
    assert tl["SCREEN_PAGES"] == ("docs/portfolio/tail",)
    assert inv.CHECK_LAYERS[-3] == "tail"
    assert tl["KNOWN_DUPLICATE_RULES"] == ["latest_filed", "quarter_label", "fiscal_year", "grok_call"]


def test_api_page_relative_fetch_is_also_page_relative(monkeypatch):
    """GitHub の API を使うページでも fetch('data/…' + 変数 + '/index.json') はページのフォルダから読む（2026-10-22、TAIL の index.html の ctrl/{T}/index.json）。"""
    monkeypatch.setattr(inv, "TOP_DIRS", {"docs", "src", "common", "config", "data"})   # リポジトリの最上位にも data/ がある
    src = ("<script>\nconst API = 'https://api.github.com/repos/o/r/contents/';\n"
           "fetch('data/ctrl/' + ticker + '/index.json?_=' + Date.now());\n</script>\n")
    rel = "docs/portfolio/tail/test_page_m.html"
    tmpl = "docs/portfolio/tail/data/ctrl/{T}/index.json"
    inv._MENTIONS.pop((rel, 3), None)
    pats, _ = inv.needles_for(tmpl)
    assert inv.mention_class(tmpl, pats, rel, 3, src.splitlines()[2], src) == "reader"


def test_strict3_workflow_commit_copy_and_continuation(monkeypatch):
    """（Portfolio の段階から）git commit -m の件名・cp の写し先・git add の \\ の続きの行は読みでない。
    git commit -F <ファイル> は読み（TAIL の /tmp/tail_commit_message.txt）。cp の元は読み（2026-10-22）。"""
    wf = ("      - run: |\n"
          "          cp config/theme_x.json docs/pf/data/theme_x.json\n"
          "          git add docs/a.json \\\n"
          "                  docs/pf/data/theme_x.json\n"
          "          git commit -m \"chore: theme_x.json を同期\"\n"
          "          git commit -F /tmp/msg_x.txt\n")
    rel = ".github/workflows/x.yml"
    tmpl = "docs/pf/data/theme_x.json"
    pats, _ = inv.needles_for(tmpl)

    def cls(n, t=tmpl, p=pats):
        inv._MENTIONS.pop((rel, n), None)
        return inv.mention_class(t, p, rel, n, wf.splitlines()[n - 1], wf)
    assert [cls(n) for n in (2, 4)] == ["no", "no"]
    spats, _ = inv.needles_for("config/theme_x.json")
    assert cls(2, "config/theme_x.json", spats) == "reader"
    mpats, _ = inv.needles_for("/tmp/msg_x.txt")
    assert cls(6, "/tmp/msg_x.txt", mpats) == "reader"


def test_portfolio_layer_settings():
    """Portfolio（段階1、2026-10-22。段階3、2026-10-23）: 画面は docs/portfolio/index.html だけ（同じフォルダに TAIL の画面がある）。
    Pythonの書き手の無いファイル（portfolio.json・cp の写し）も出力に加える。CHECK-INV の最後の段階。"""
    pf = inv.LAYERS["portfolio"]
    assert pf["SCREEN_PAGES"] == ("docs/portfolio/index.html",) and pf["EXTRA_OUTPUT_DIRS"] == ("docs/portfolio/data",)
    assert inv.CHECK_LAYERS[-1] == "portfolio"
    assert [k for k in inv.CHECK_LAYERS if inv.LAYERS[k]["EXTRA_OUTPUT_DIRS"]] == ["macro_data", "market_pulse", "portfolio"]


def test_macro_layers_settings():
    """MACRO PULSE（段階1・2、2026-10-23）: 共有のデータ層 macro_data（画面なし、市場データ層と同じ汎用の読み書きの関数の印。series_meta.json は書き手の無いファイル）と、
    macro_pulse（画面あり）。段階3（2026-10-23）で CHECK-INV に入れた。"""
    md, mp = inv.LAYERS["macro_data"], inv.LAYERS["macro_pulse"]
    assert md["LAYER_DIRS"] == ("common/macro_data",) and not md["SCREEN_PAGES"]
    assert md["IO_HELPER_BY_CALLER"] and md["EXTRA_OUTPUT_DIRS"] == ("common/macro_data",)
    assert mp["LAYER_DIRS"] == ("src/market/macro_pulse",) and mp["SCREEN_PAGES"] == ("docs/market-monitor/macro-pulse",)
    assert not (mp["IO_HELPER_BY_CALLER"] or mp["EXTRA_OUTPUT_DIRS"])
    assert "macro_data" in inv.CHECK_LAYERS and "macro_pulse" in inv.CHECK_LAYERS
    assert md["KNOWN_DUPLICATE_RULES"] == ["fred_api", "atomic_write_json"]
    assert mp["KNOWN_DUPLICATE_RULES"] == ["trading_day", "recession_score", "step_signal", "fred_api", "grok_endpoint"]
    # 本番外のコードの注記は MACRO PULSE の 05_audit.py だけ（Market Pulse は段階2で4つとも消して空）
    assert [k for k in inv.LAYERS if inv.LAYERS[k]["NONPROD_NOTES"]] == ["macro_pulse"]


def test_csv_columns_are_keys(tmp_path, monkeypatch):
    """CSV の見出しの行の列を {列: ""} にする（BOM 付きでも。MACRO PULSE の段階から、2026-10-23から全段階）。"""
    (tmp_path / "x.csv").write_text("\ufeffdate,score , phase\n2026-10-23,40,中立\n", encoding="utf-8")
    monkeypatch.setattr(inv, "REPO", str(tmp_path))
    assert inv.data_file_obj("x.csv") == {"date": "", "score": "", "phase": ""}
    assert inv.data_output_ext() == (".json", ".csv")
    assert inv.data_file_obj("missing.csv") is None


def test_js_id_helper_definitions():
    """getElementById を包んだ関数の名前（矢印関数・function の両方。MACRO PULSE の段階から、2026-10-23から全段階）。"""
    page = ("const el = id => document.getElementById(id);\n"
            "function $id(x) { return document.getElementById(x); }\n"
            "const other = document.getElementById('a');\n")
    assert {a or b for a, b in inv.JS_ID_HELPER_DEF_RE.findall(page)} == {"el", "$id"}


def test_market_pulse_layer_settings():
    """Market Pulse（段階1、2026-10-23）: 02_market_rss.txt（人の手で書く入力）も出力に加える。段階3（2026-10-23）で CHECK-INV に入れた。"""
    mp = inv.LAYERS["market_pulse"]
    assert mp["LAYER_DIRS"] == ("src/market/market_pulse",) and mp["SCREEN_PAGES"] == ("docs/market-monitor/market-pulse",)
    assert mp["EXTRA_OUTPUT_DIRS"] == ("src/market/market_pulse",) and not mp["IO_HELPER_BY_CALLER"]
    assert inv.CHECK_LAYERS[-2] == "market_pulse"
    assert mp["KNOWN_DUPLICATE_RULES"] == ["trading_day", "provisional_bar", "sp500_constituents", "fred_api", "grok_endpoint", "market_holidays"]
    assert "market_holidays" in [r["id"] for r in mp["RULES"]]
    # 段階2（2026-10-23）: 手で動かすスクリプト4つを消し、誰も読まないキー48と、消したスクリプトだけが読んでいた true・false は理由を書いて残す
    assert not mp["NON_PRODUCTION_FILES"] and len(mp["UNREAD_KEY_NOTES"]) == 50 and "過去の行の記録" in mp["UNREAD_KEY_NOTES"]["backfilled_at"]


def test_mail_notification_is_output(monkeypatch):
    """smtplib の send_message・sendmail はメールの通知の出力（Market Pulse の段階から）。smtplib を使わないファイルの同じ名前の呼び出しは数えない。"""
    src = ("import os, smtplib\n"
           "GMAIL_USER = os.getenv('GMAIL_USER')\n"
           "def send(msg):\n"
           "    with smtplib.SMTP('smtp.gmail.com', 587) as smtp:\n"
           "        smtp.send_message(msg)\n")
    mod = _strict_module(monkeypatch, "src/mp/send_k.py", src, {})
    assert [(w["path"], w["kind"]) for w in inv._io_sites(mod)[0]] == [("メール（GMAIL_USER）", "通知")]
    other = _strict_module(monkeypatch, "src/mp/queue_k.py", "def f(q, m):\n    q.send_message(m)\n", {})
    assert inv._io_sites(other)[0] == []


def _fresh_key_cache(monkeypatch, repo):
    """データのファイルのキーの控えを、一時のリポジトリで空から始める（R-A の終わりの見直し2）。"""
    monkeypatch.setattr(inv, "REPO", str(repo))
    for name in ("_BLOBS", "_SUMMARY", "_KEY_CACHE"):
        monkeypatch.setattr(inv, name, {})
    monkeypatch.setattr(inv, "_KEY_CACHE_STATE", {"loaded": False, "dirty": False})


def _git(repo, *args):
    import subprocess
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def test_key_summary_follows_file_content_hash(tmp_path, monkeypatch):
    """キーの要約は中身の blob の hash で引く。commit 前に作業ツリーで中身を変えたら、前の控えを使わずに読み直す（2026-10-23）。"""
    _git(tmp_path, "init", "-q")
    (tmp_path / "d.json").write_text('{"a": {"x": 1}}', encoding="utf-8")
    _git(tmp_path, "add", "d.json")
    _git(tmp_path, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "x")
    _fresh_key_cache(monkeypatch, tmp_path)
    assert inv.data_summary("d.json")["keys"] == ["a", "x"]
    assert inv.data_summary("d.json")["parents"] == {"a": [""], "x": ["a"]}
    # 作業ツリーだけで中身を変える（index の blob は古いまま）。新しいプロセスと同じに、パスごとの控えを捨てる
    (tmp_path / "d.json").write_text('{"b": 2}', encoding="utf-8")
    monkeypatch.setattr(inv, "_BLOBS", {})
    monkeypatch.setattr(inv, "_SUMMARY", {})
    assert inv.data_summary("d.json")["keys"] == ["b"]
    assert len(inv._KEY_CACHE) == 2   # 中身ごとに別の控え
    # 元の中身に戻すと、読まずに前の控えを使う
    (tmp_path / "d.json").write_text('{"a": {"x": 1}}', encoding="utf-8")
    monkeypatch.setattr(inv, "_BLOBS", {})
    monkeypatch.setattr(inv, "_SUMMARY", {})
    monkeypatch.setattr(inv, "_summarize", lambda rel: (_ for _ in ()).throw(AssertionError("読み直した")))
    assert inv.data_summary("d.json")["keys"] == ["a", "x"]


def test_key_cache_file_roundtrip_drops_stale_blobs(tmp_path, monkeypatch):
    """控えのファイルを書いて読み直すと同じ要約を使う。今のどのファイルの中身でもない blob は書かない（2026-10-23）。"""
    import json as _json
    _git(tmp_path, "init", "-q")
    (tmp_path / "d.json").write_text('{"a": 1}', encoding="utf-8")
    (tmp_path / "e.csv").write_text("﻿date,score\n2026-10-23,1\n", encoding="utf-8")
    _git(tmp_path, "add", "d.json", "e.csv")
    _fresh_key_cache(monkeypatch, tmp_path)
    inv._KEY_CACHE["0" * 40] = {"j": None, "c": None}   # 古い中身の控え
    inv._KEY_CACHE_STATE["dirty"] = True
    assert inv.data_summary("e.csv")["keys"] == ["date", "score"] and inv.file_summary("e.csv")["j"] is None
    inv.data_summary("d.json")
    inv.save_key_cache()
    saved = _json.load(open(tmp_path / inv.KEY_CACHE_FILE, encoding="utf-8"))
    assert "0" * 40 not in saved["blobs"] and len(saved["blobs"]) == 2
    _fresh_key_cache(monkeypatch, tmp_path)
    monkeypatch.setattr(inv, "_summarize", lambda rel: (_ for _ in ()).throw(AssertionError("読み直した")))
    assert inv.data_summary("d.json")["keys"] == ["a"]
