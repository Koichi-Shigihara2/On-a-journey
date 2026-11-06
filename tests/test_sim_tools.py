"""試算の共通の道具 tools/sim/sim.py（前後の比べ・保有銘柄・hook）。作った値と一時フォルダだけ。
本物のパイプライン（TANUKI・HypeCore）は回さない（safe_environment は環境を変えるので呼ばない）"""
import importlib.util
import json
import os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location("sim_tool", os.path.join(REPO, "tools", "sim", "sim.py"))
sim = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sim)


def _put(root, name, ticker, iv, judgment, rec=None, sub="tanuki"):
    d = os.path.join(root, name, sub, ticker)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "latest.json"), "w", encoding="utf-8") as f:
        json.dump({"intrinsic_value_per_share": iv, "tanuki_score": judgment, "recommended_g": rec,
                   "upside_percent": 1.0, "components": {"terminal_growth_used": 0.03}}, f)


def test_compare_orders_by_change_lists_all_judgment_changes_and_holdings(tmp_path):
    root = str(tmp_path)
    for t, a, b, ja, jb in [("AAA", 100.0, 150.0, "HOLD", "BUY"), ("BBB", 50.0, 49.0, "TRIM", "TRIM"),
                            ("CCC", 10.0, 10.0, "WATCH", "WATCH"), ("DDD", 20.0, 12.0, "BUY", "WATCH")]:
        _put(root, "before", t, a, ja)
        _put(root, "after", t, b, jb)
    res = sim.compare(root, "before", "after", hold={"BBB"})
    assert [r["t"] for r in sorted(res["changed"], key=lambda r: -abs(r["pct"]))] == ["AAA", "DDD", "BBB"]   # CCC は変化なし
    assert {r["t"] for r in res["judgments"]} == {"AAA", "DDD"}
    assert [r["t"] for r in res["holdings"]] == ["BBB"]
    assert "BBB（保有）" in res["text"] and res["text"].startswith("銘柄 4・理論株価が変わる 3・判定が変わる 2")


def test_compare_prefers_the_last_chain_output_tanuki2(tmp_path):
    root = str(tmp_path)
    _put(root, "before", "AAA", 100.0, "HOLD")
    _put(root, "after", "AAA", 100.0, "HOLD")                      # 1回目
    _put(root, "after", "AAA", 90.0, "TRIM", sub="tanuki2")        # 読み合いの後
    res = sim.compare(root, "before", "after", hold=set())
    assert res["rows"][0]["iv1"] == 90.0 and res["rows"][0]["j1"] == "TRIM"


def test_holdings_reads_all_broker_positions(tmp_path, monkeypatch):
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({"brokers": {"a": {"positions": {"NVDA": {}}}, "b": {"positions": {"TSLA": {}, "NVDA": {}}}}}), encoding="utf-8")
    monkeypatch.setattr(sim, "PORTFOLIO_JSON", str(p))
    assert sim.holdings() == {"NVDA", "TSLA"}
    monkeypatch.setattr(sim, "PORTFOLIO_JSON", str(tmp_path / "none.json"))
    assert sim.holdings() == set()


def test_hook_apply_receives_the_context(tmp_path):
    hook = tmp_path / "h.py"
    hook.write_text("def apply(ctx):\n    ctx['touched'] = ctx['GS'] + 1\n", encoding="utf-8")
    ctx = {"GS": 41}
    sim._load_hooks([str(hook)], ctx)
    assert ctx["touched"] == 42


def test_default_output_root_is_gitignored():
    assert os.path.basename(sim.DEFAULT_ROOT) == ".sim_out"
    assert ".sim_out/" in open(os.path.join(REPO, ".gitignore"), encoding="utf-8").read().split()
