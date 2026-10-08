"""[[TAIL-DETAIL-SEC-ITEMS-PATH-MISMATCH-1]]（2026-10-08）の回帰テスト。

- sec_items_fetcher.py: 保存済みと同じ書類（同じ期間・提出日）なら、本文の取得・Grokの翻訳・セグメント別見通しの
  AI抽出・書き込みをしない。成功は「書いた」と「変更なし」の合計、失敗は本当の失敗だけ。成功0銘柄のときだけ終了コード1
- detail.html: Item 1A・3・7をsec_items_fetcher.pyの保存先から読み、取得していないItem 1を読まない
- TANUKI_TAIL_SEC_Items.yml: 週1回の定時の実行

ネットワーク・Grokは使わない（呼ばれたら失敗するスタブに差し替える）。
"""
import json
import os
import re
import sys

import pytest
import yaml

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_TAIL_DIR = os.path.join(_REPO, "src", "tail")
if _TAIL_DIR not in sys.path:
    sys.path.insert(0, _TAIL_DIR)

import sec_items_fetcher as sif  # noqa: E402

_10K = {"accession": "0001-26-000010", "primary_document": "k.htm", "report_date": "2025-12-31", "filing_date": "2026-02-20"}
_10Q = {"accession": "0001-26-000020", "primary_document": "q.htm", "report_date": "2026-06-30", "filing_date": "2026-08-04"}


def _no_call(name):
    def f(*a, **k):
        raise AssertionError(f"保存済みと同じ書類なのに{name}を呼んだ")
    return f


def _filings(k=_10K, q=_10Q):
    def f(cik, form="10-Q", count=1, after_date=None):
        return [dict(k)] if form == "10-K" else [dict(q)]
    return f


def _store(data_dir, ticker, item_key, period, filed):
    d = data_dir / item_key / ticker
    d.mkdir(parents=True, exist_ok=True)
    body = {"ticker": ticker, "item_key": item_key, "period": period, "filing_date": filed, "excerpt": "old"}
    (d / f"{period}.json").write_text(json.dumps(body), encoding="utf-8")
    return d / f"{period}.json"


def _store_all(data_dir, ticker):
    paths = []
    for key in sif.ITEM_KEYS:
        paths.append(_store(data_dir, ticker, key, "2025FY", _10K["filing_date"]))
        paths.append(_store(data_dir, ticker, key, "2026Q2", _10Q["filing_date"]))
    return paths


@pytest.fixture
def items(tmp_path, monkeypatch):
    monkeypatch.setattr(sif, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sif.time, "sleep", lambda s: None)
    monkeypatch.setattr(sif, "_get_recent_filings", _filings())
    return tmp_path


def test_same_filings_skip_fetch_translate_and_write(items, monkeypatch):
    paths = _store_all(items, "PLTR")
    before = {p: p.read_text(encoding="utf-8") for p in paths}
    monkeypatch.setattr(sif, "_fetch_filing_text", _no_call("本文の取得"))
    monkeypatch.setattr(sif, "_translate_excerpt", _no_call("Grokの翻訳"))
    monkeypatch.setattr(sif, "_extract_segment_outlook_safe", _no_call("セグメント別見通しのAI抽出"))
    stats = sif.fetch_and_save_all_items("PLTR", "0000000001")
    assert stats["ng"] == 0 and stats["written"] == 0 and stats["unchanged"] == 6
    assert {p: p.read_text(encoding="utf-8") for p in paths} == before
    for key in sif.ITEM_KEYS:   # latest.json・index.jsonも作らない
        assert sorted(os.listdir(items / key / "PLTR")) == ["2025FY.json", "2026Q2.json"]


def test_new_filing_is_fetched_and_written(items, monkeypatch):
    _store_all(items, "PLTR")
    new_q = dict(_10Q, filing_date="2026-08-05", accession="0001-26-000021")   # 同じ四半期で提出日が違う（訂正等）
    monkeypatch.setattr(sif, "_get_recent_filings", _filings(q=new_q))
    monkeypatch.setattr(sif, "_fetch_filing_text", lambda *a, **k: "raw")
    monkeypatch.setattr(sif, "extract_item_section", lambda *a, **k: (0, "section text"))
    calls = []
    monkeypatch.setattr(sif, "_translate_excerpt", lambda text, desc, timeout=60: calls.append(desc) or "訳")
    monkeypatch.setattr(sif, "_extract_segment_outlook_safe", lambda t, s: calls.append("segment") or [])
    stats = sif.fetch_and_save_all_items("PLTR", "0000000001")
    assert stats["ng"] == 0 and stats["written"] == 3 and stats["unchanged"] == 3   # 10-Qの3項目だけ書く
    assert len(calls) == 4   # 翻訳3項目＋mdaのセグメント別見通し1回（10-Kは呼ばない）
    for key in sif.ITEM_KEYS:
        q = json.load(open(items / key / "PLTR" / "2026Q2.json", encoding="utf-8"))
        assert q["filing_date"] == "2026-08-05" and q["excerpt_ja"] == "訳"
        assert json.load(open(items / key / "PLTR" / "2025FY.json", encoding="utf-8"))["excerpt"] == "old"


def _run_main(monkeypatch, capsys, tickers, cik=lambda t: "0000000001"):
    monkeypatch.setattr(sif._tickers_mod, "get_cik", cik)
    monkeypatch.setattr(sys, "argv", ["sec_items_fetcher.py"] + tickers)
    code = 0
    try:
        sif.main()
    except SystemExit as e:
        code = e.code
    return code, capsys.readouterr().out


def test_all_unchanged_week_exits_zero(items, monkeypatch, capsys):
    for t in ("PLTR", "SOFI"):
        _store_all(items, t)
    monkeypatch.setattr(sif, "_fetch_filing_text", _no_call("本文の取得"))
    monkeypatch.setattr(sif, "_translate_excerpt", _no_call("Grokの翻訳"))
    monkeypatch.setattr(sif, "_extract_segment_outlook_safe", _no_call("セグメント別見通しのAI抽出"))
    code, out = _run_main(monkeypatch, capsys, ["PLTR", "SOFI"])
    assert code == 0
    assert "完了: 2 成功（更新 0 / 変更なし 2） / 0 失敗" in out


def test_real_failures_count_but_run_succeeds_if_any_success(items, monkeypatch, capsys):
    _store_all(items, "SOFI")
    monkeypatch.setattr(sif, "_fetch_filing_text", lambda *a, **k: None)   # 新しい書類の本文の取得に失敗
    monkeypatch.setattr(sif, "_translate_excerpt", _no_call("Grokの翻訳"))
    monkeypatch.setattr(sif, "_extract_segment_outlook_safe", _no_call("セグメント別見通しのAI抽出"))
    code, out = _run_main(monkeypatch, capsys, ["PLTR", "SOFI", "TSLA"],
                          cik=lambda t: None if t == "PLTR" else "0000000001")
    assert code == 0
    # PLTR: CIKなし、TSLA: 10-Kの本文の取得に失敗、SOFI: 変更なし
    assert "完了: 1 成功（更新 0 / 変更なし 1） / 2 失敗" in out


def test_no_success_exits_one(items, monkeypatch, capsys):
    code, out = _run_main(monkeypatch, capsys, ["PLTR", "SOFI"], cik=lambda t: None)
    assert code == 1
    assert "完了: 0 成功（更新 0 / 変更なし 0） / 2 失敗" in out


# ── 詳細画面の読み込み先 ─────────────────────────────────────

def _ctrl_items():
    html = open(os.path.join(_REPO, "docs", "portfolio", "tail", "detail.html"), encoding="utf-8").read()
    block = re.search(r"var CTRL_ITEMS = \[(.*?)\];", html, re.S).group(1)
    return html, dict(re.findall(r"key:\s*'(\w+)',\s*dir:\s*'(\w+)'", block))


def test_detail_reads_items_from_sec_items_fetcher_dirs():
    html, items = _ctrl_items()
    assert items == {"item1a": "risk_factors", "item3": "legal_proceedings", "item4": "ctrl", "item7": "mda"}
    assert set(items.values()) - {"ctrl"} == set(sif.ITEM_KEYS)   # 保存先と一致
    assert "'data/' + item.dir + '/' + ticker + '/latest.json" in html
    assert "item1/latest.json" not in html and "item1a/latest.json" not in html   # 一度も作られていないパスを読まない


def test_detail_shows_period_for_items():
    html, _ = _ctrl_items()
    assert "d.quarter || d.period" in html   # Item 1A・3・7はquarterではなくperiodを持つ


# ── 定時の実行 ─────────────────────────────────────────────

def test_weekly_workflow_runs_items_fetcher():
    p = os.path.join(_REPO, ".github", "workflows", "TANUKI_TAIL_SEC_Items.yml")
    raw = open(p, encoding="utf-8").read()
    d = yaml.safe_load(raw)
    on = d.get(True, d.get("on"))
    assert [c["cron"] for c in on["schedule"]] == ["20 1 * * 1"]
    steps = d["jobs"]["fetch-items"]["steps"]
    run = next(s for s in steps if "sec_items_fetcher.py" in (s.get("run") or ""))
    assert run["env"]["XAI_API_KEY"] == "${{ secrets.XAI_API_KEY }}"
    for key in sif.ITEM_KEYS:
        assert f"git add docs/portfolio/tail/data/{key}/" in raw
    assert "push_with_retry.sh" in raw
