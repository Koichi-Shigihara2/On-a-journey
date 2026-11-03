"""[[COMMON-PATH-GUARD-1]]（2026-11-03）: 共通化の後の抜け道を静的な検査で禁止する。

- 外部への GET（requests.get）は common/external_api.py だけ（再試行・タイムアウト・User-Agent・2xx・非JSONの決まりを1か所に）
- 市場の日付（終値の日・t+N）に連邦の営業日の関数を使わない（NYSEの取引日は common/market_data/calendar_universe.py）
- 文字コードを指定しない open（ruff PLW1514）を置かない
- 全銘柄の集計ファイルを、一部の銘柄の実行で書き直さない（common/sec_data/rotce.py の _summary.json）

使ってよい場所は下の一覧（*_ALLOWED）の1か所だけに書く。
"""
import ast
import json
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

# requests.get を直接呼んでよいファイル
REQUESTS_GET_ALLOWED = {"common/external_api.py"}

# 連邦の祝日・営業日の関数（経済指標の公表の予定用。NYSEの休場日ではない）
FEDERAL_DAY_FUNCS = {"us_federal_holidays", "us_federal_business_days_add", "previous_us_federal_business_day"}
# 使ってよい（ファイル, 関数）。市場の日付ではない日だけ
FEDERAL_DAY_ALLOWED = {
    ("common/market_data/calendar_universe.py", "us_federal_business_days_add"),        # 定義
    ("common/market_data/calendar_universe.py", "previous_us_federal_business_day"),    # 定義
    # MACRO PULSE の実行の対象日（経済指標の公表の予定の日。終値は get_sp500_with_asof がその日以前の直近を引く）
    ("src/market/macro_pulse/05_main.py", "default_target_date"),
}


def _py_files():
    out = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO, capture_output=True, text=True, encoding="utf-8", check=True)
    return [p for p in out.stdout.splitlines() if os.path.exists(os.path.join(REPO, p))]


def _parse(rel):
    with open(os.path.join(REPO, rel), encoding="utf-8") as f:
        return ast.parse(f.read(), filename=rel)


def _parents(tree):
    par = {}
    for node in ast.walk(tree):
        for ch in ast.iter_child_nodes(node):
            par[ch] = node
    return par


def _enclosing_function(node, par):
    while node in par:
        node = par[node]
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return node.name
    return None


def _requests_get_uses(tree):
    """requests.get（import requests as X の X.get も）・from requests import get の行"""
    aliases = {"requests"}
    lines = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            aliases |= {a.asname or a.name for a in node.names if a.name == "requests"}
        elif isinstance(node, ast.ImportFrom) and node.module == "requests":
            lines += [node.lineno for a in node.names if a.name in ("get", "request", "Session")]
    for node in ast.walk(tree):
        if (isinstance(node, ast.Attribute) and node.attr == "get"
                and isinstance(node.value, ast.Name) and node.value.id in aliases):
            lines.append(node.lineno)
    return sorted(lines)


def test_requests_get_only_in_external_api():
    bad = []
    for rel in _py_files():
        if rel in REQUESTS_GET_ALLOWED:
            continue
        bad += [f"{rel}:{ln}" for ln in _requests_get_uses(_parse(rel))]
    assert not bad, ("requests.get を直接呼んでいる（common/external_api.py の web_get・sec_get・fred_get・frb_get を使う。"
                     f"許すなら REQUESTS_GET_ALLOWED に）: {bad}")


def test_requests_get_detector_finds_aliases():
    tree = ast.parse("import requests as rq\nfrom requests import get\nrq.get('u')\nrequests.post('u')\n")
    assert _requests_get_uses(tree) == [2, 3]


def test_federal_business_day_functions_not_used_for_market_dates():
    bad = []
    for rel in _py_files():
        if rel.startswith("tests/"):
            continue
        tree = _parse(rel)
        par = _parents(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            name = f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else None
            if name in FEDERAL_DAY_FUNCS and (rel, _enclosing_function(node, par)) not in FEDERAL_DAY_ALLOWED:
                bad.append(f"{rel}:{node.lineno} {name}（{_enclosing_function(node, par)}）")
    assert not bad, ("連邦の営業日の関数を市場の日付に使っている（NYSEの取引日は calendar_universe.nyse_trading_days_add 等。"
                     f"市場の日付でなければ FEDERAL_DAY_ALLOWED に）: {bad}")


def test_no_open_without_encoding_ruff_plw1514():
    """文字コードを指定しない open（ruff PLW1514、preview の規則）。ruff は venv に入れる（pip install ruff）"""
    r = subprocess.run([sys.executable, "-m", "ruff", "check", "--select", "PLW1514", "--preview", "--no-cache",
                        "--output-format", "concise", "."], cwd=REPO, capture_output=True, text=True, encoding="utf-8")
    if "No module named ruff" in (r.stderr or ""):
        pytest.fail("ruff が無い（venv/Scripts/python.exe -m pip install ruff）。このテストがゲートの PLW1514 の検査")
    assert r.returncode == 0, "文字コードを指定しない open がある（encoding=\"utf-8\" を付ける）:\n" + r.stdout[-3000:]


# ── rotce.py の _summary.json（一部の銘柄の実行で全銘柄の集計を書き直さない） ──
def _fake_rotce(monkeypatch, rotce, value):
    monkeypatch.setattr(rotce, "load_split_history", lambda: {})
    monkeypatch.setattr(rotce, "load_company_reported", lambda: {})
    monkeypatch.setattr(rotce, "compute_ticker", lambda t, repo_root, splits=None: {
        "ticker": t.upper(), "reference_only": True, "history": [], "notes": [],
        "current": {"rotce": value, "ptbv": 1.0}, "percentile": {"n_quarters": 0}})


def _summary(out):
    with open(os.path.join(out, "_summary.json"), encoding="utf-8") as f:
        return json.load(f)


def test_rotce_partial_run_keeps_other_rows(monkeypatch, tmp_path):
    from common.sec_data import rotce
    out = str(tmp_path)
    _fake_rotce(monkeypatch, rotce, 0.10)
    rotce.build_all(["AAA", "BBB", "CCC"], out_dir=out)
    full = _summary(out)
    _fake_rotce(monkeypatch, rotce, 0.20)
    rotce.build_all(["BBB"], out_dir=out, partial=True)
    s = _summary(out)
    assert list(s["tickers"]) == ["AAA", "BBB", "CCC"]                     # ほかの銘柄の行も順も残る
    assert s["tickers"]["AAA"] == full["tickers"]["AAA"] and s["tickers"]["CCC"] == full["tickers"]["CCC"]
    assert s["tickers"]["BBB"]["rotce"] == 0.20                           # 指定の銘柄だけ差し替える
    assert s["generated_at"] == full["generated_at"]                      # 全銘柄の実行の時刻のまま


def test_rotce_full_run_rewrites_summary(monkeypatch, tmp_path):
    from common.sec_data import rotce
    out = str(tmp_path)
    _fake_rotce(monkeypatch, rotce, 0.10)
    rotce.build_all(["AAA", "BBB"], out_dir=out)
    rotce.build_all(["BBB"], out_dir=out)                                 # 全銘柄の実行（partial=False）は今までどおり丸ごと
    assert list(_summary(out)["tickers"]) == ["BBB"]


def test_rotce_main_passes_partial_only_with_tickers(monkeypatch):
    from common.sec_data import rotce
    import common.sec_data.config as cfg
    seen = []
    monkeypatch.setattr(cfg, "get_all", lambda: ["AAA", "BBB"])
    monkeypatch.setattr(rotce, "build_all", lambda tickers, partial=False: seen.append((tickers, partial)) or
                        {"tickers": {t: {"rotce": None, "ptbv": None} for t in ("AAA", "BBB")}})
    rotce.main([])
    rotce.main(["bbb"])
    assert seen == [(["AAA", "BBB"], False), (["BBB"], True)]
