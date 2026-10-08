"""
tests/test_tail_sec_ctrl_fetcher.py

TANUKI TAIL — src/tail/sec_ctrl_fetcher.py の対象銘柄の読み込みと、毎週の実行の成否
（[[TAIL-CTRL-WEEKLY-NOOP-POSITIONS-INDEX-1]]）。ネットワークアクセスなし。

- positions_index.json は `{"positions": ["PLTR_thesis.json", …]}` の形。以前は dict をそのまま回し、
  キーの "positions" だけをティッカー POSITIONS として読んで、毎週0銘柄の処理で success になっていた
- 成功 = 取得して書いた ＋ 保存済みと同じ10-Q（四半期・提出日）なので書かなかった。失敗 = CIK未登録・取得の失敗
- 1銘柄も成功しなかった実行は終了コード1。全銘柄が「変更なし」の週は終了コード0
"""

import json
import os
import sys

import pytest

_REPO = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_TAIL_DIR = os.path.join(_REPO, "src", "tail")
sys.path.insert(0, _TAIL_DIR)

import sec_ctrl_fetcher as scf  # noqa: E402
import sec_items_fetcher as sif  # noqa: E402

_REAL_INDEX = os.path.join(_REPO, "docs", "portfolio", "tail", "data", "positions_index.json")
_TEN = ["PLTR", "SOFI", "TSLA", "CELH", "APP", "NVDA", "ADBE", "SOUN", "CRWV", "APGE"]

# 2026-10-07時点の docs/portfolio/tail/data/positions_index.json と同じ形・同じ内容
# （APGEは2026-10-08に登録解除したが、ここは一時フォルダに書く架空の10銘柄として残す）
_INDEX = {"positions": [f"{t}_thesis.json" for t in _TEN]}


@pytest.fixture
def tail(tmp_path, monkeypatch):
    """positions_index.json と ctrl/ を一時フォルダに置き換え、ネットワークを使う関数を差し替える。"""
    idx = tmp_path / "positions_index.json"
    idx.write_text(json.dumps(_INDEX, ensure_ascii=False, indent=2), encoding="utf-8")
    ctrl = tmp_path / "ctrl"
    monkeypatch.setattr(scf, "POS_IDX_PATH", str(idx))
    monkeypatch.setattr(scf, "CTRL_DIR", str(ctrl))
    monkeypatch.setattr(scf.time, "sleep", lambda s: None)
    monkeypatch.setattr(scf, "_translate_excerpt", lambda *a, **k: None)
    monkeypatch.setattr(sys, "argv", ["sec_ctrl_fetcher.py"])
    return ctrl


def _filing(report="2026-06-30", filed="2026-08-04"):
    return [{"accession": "0001-26-000001", "primary_document": "q.htm", "report_date": report, "filing_date": filed}]


def _store(ctrl, ticker, quarter="2026Q2", filed="2026-08-04"):
    d = ctrl / ticker
    d.mkdir(parents=True, exist_ok=True)
    (d / "latest.json").write_text(json.dumps({"ticker": ticker, "quarter": quarter, "filing_date": filed}), encoding="utf-8")


class _Resp:
    text = "<html><p>Item 4. Controls and Procedures</p><p>Our disclosure controls and procedures were effective as of June 30, 2026.</p></html>"


def _no_fetch(*a, **k):
    raise AssertionError("保存済みと同じ10-Qなのに本文を取得した")


# ── 対象銘柄の読み込み ─────────────────────────────────────────

def test_load_tail_tickers_reads_positions_list(tail):
    assert scf.load_tail_tickers() == _TEN


def test_load_tail_tickers_real_file_has_no_positions_key_as_ticker():
    """実際の positions_index.json から、載っている *_thesis.json のティッカーがそのまま返る。"""
    with open(_REAL_INDEX, encoding="utf-8") as f:
        expected = [p.replace("_thesis.json", "").upper() for p in json.load(f)["positions"]]
    got = scf.load_tail_tickers()
    assert "POSITIONS" not in got
    assert got == expected and len(got) >= 9   # 2026-10-08にAPGEを登録解除して9銘柄


def test_sec_items_fetcher_uses_same_loader(tail, monkeypatch):
    """sec_items_fetcher.py も同じ関数を使う。引数なしの実行で10銘柄を順に処理する。"""
    seen = []
    monkeypatch.setattr(sif._tickers_mod, "get_cik", lambda t: "0000000001")
    monkeypatch.setattr(sif.time, "sleep", lambda s: None)
    monkeypatch.setattr(sif, "fetch_and_save_all_items",
                        lambda t, cik: seen.append(t) or {"annual_ok": 1, "quarterly_ok": 0, "ng": 0})
    monkeypatch.setattr(sys, "argv", ["sec_items_fetcher.py"])
    sif.main()
    assert seen == _TEN


# ── 毎週の実行の成否 ─────────────────────────────────────────

def test_all_unchanged_week_exits_zero_and_writes_nothing(tail, monkeypatch, capsys):
    for t in _TEN:
        _store(tail, t)
    before = {p: open(p, encoding="utf-8").read() for p in (str(tail / t / "latest.json") for t in _TEN)}
    monkeypatch.setattr(scf._tickers_mod, "get_cik", lambda t: "0000000001")
    monkeypatch.setattr(scf, "_get_recent_filings", lambda *a, **k: _filing())
    monkeypatch.setattr(scf, "_edgar_get", _no_fetch)
    scf.main()   # SystemExitを出さない＝終了コード0
    out = capsys.readouterr().out
    assert "完了: 10 成功（更新 0 / 変更なし 10） / 0 失敗" in out
    assert {p: open(p, encoding="utf-8").read() for p in before} == before
    assert sorted(os.listdir(tail)) == sorted(_TEN)   # 四半期のファイル・index.jsonも作らない


def test_new_filing_is_written_and_same_quarter_other_date_is_refetched(tail, monkeypatch, capsys):
    _store(tail, "PLTR", quarter="2026Q1", filed="2026-05-05")      # 前の四半期 → 書く
    _store(tail, "SOFI", quarter="2026Q2", filed="2026-08-01")      # 同じ四半期で提出日が違う（訂正等） → 書く
    _store(tail, "TSLA")                                           # 同じ → 書かない
    monkeypatch.setattr(scf, "POS_IDX_PATH", _write_index(tail, ["PLTR", "SOFI", "TSLA", "APGE"]))
    monkeypatch.setattr(scf._tickers_mod, "get_cik", lambda t: "0000000001")
    monkeypatch.setattr(scf, "_get_recent_filings", lambda *a, **k: _filing())
    monkeypatch.setattr(scf, "_edgar_get", lambda *a, **k: _Resp())
    scf.main()
    out = capsys.readouterr().out
    assert "完了: 4 成功（更新 3 / 変更なし 1） / 0 失敗" in out
    for t in ("PLTR", "SOFI", "APGE"):
        latest = json.load(open(tail / t / "latest.json", encoding="utf-8"))
        assert (latest["quarter"], latest["filing_date"]) == ("2026Q2", "2026-08-04")
        assert (tail / t / "2026Q2.json").exists()
    assert json.load(open(tail / "PLTR" / "index.json", encoding="utf-8"))["quarters"] == ["2026Q2"]
    assert not (tail / "TSLA" / "2026Q2.json").exists()


def test_cik_missing_and_fetch_failure_count_as_failures_but_run_succeeds(tail, monkeypatch, capsys):
    for t in _TEN[2:]:
        _store(tail, t)
    monkeypatch.setattr(scf._tickers_mod, "get_cik", lambda t: None if t == "PLTR" else "0000000001")
    monkeypatch.setattr(scf, "_get_recent_filings", lambda *a, **k: _filing())
    monkeypatch.setattr(scf, "_edgar_get", lambda *a, **k: None)   # SOFIの本文の取得に失敗
    scf.main()   # 成功が1銘柄以上あるので終了コード0
    out = capsys.readouterr().out
    assert "完了: 8 成功（更新 0 / 変更なし 8） / 2 失敗" in out


def test_no_success_exits_one(tail, monkeypatch, capsys):
    monkeypatch.setattr(scf._tickers_mod, "get_cik", lambda t: None)
    with pytest.raises(SystemExit) as e:
        scf.main()
    assert e.value.code == 1
    assert "完了: 0 成功（更新 0 / 変更なし 0） / 10 失敗" in capsys.readouterr().out


def test_missing_index_exits_one(tail, monkeypatch):
    monkeypatch.setattr(scf, "POS_IDX_PATH", str(tail / "nothing.json"))
    with pytest.raises(SystemExit) as e:
        scf.main()
    assert e.value.code == 1


def test_workflow_passes_xai_api_key():
    with open(os.path.join(_REPO, ".github", "workflows", "TANUKI_TAIL_SEC_Ctrl.yml"), encoding="utf-8") as f:
        assert "XAI_API_KEY: ${{ secrets.XAI_API_KEY }}" in f.read()


def _write_index(tmp, tickers):
    p = tmp.parent / "positions_index_sub.json"
    p.write_text(json.dumps({"positions": [f"{t}_thesis.json" for t in tickers]}), encoding="utf-8")
    return str(p)
