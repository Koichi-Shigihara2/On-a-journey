"""
tests/test_eps_run_status.py

[[EPS-SILENT-TICKER-FAILURE-1]]・[[EPS-SUMMARY-GHOST-ROWS-1]]の回帰テスト。

- run(): 成功・失敗（データなし／例外）を数え、summary.jsonにrun_status
  （全銘柄の実行）またはpartial_run_status（--tickerの部分実行）を書く。
  一部の失敗は終了コード0、成功0件は1（summaryは書かない）
- 各行のgenerated_at: 今回作り直した行だけ更新し、失敗した銘柄の行は前回の値のまま
- generate_summary(): eps=true（登録処理中を含む）でない銘柄の行を消す
- System Health [I]: generated_atがrun_status.started_atより前の行を⚠️にする

実行方法:
    python -m pytest tests/test_eps_run_status.py -v
"""

import json
import os
import sys

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import src.value.adjusted_eps_analyzer.pipeline as aea_pipeline  # noqa: E402
import common.system_health as sh  # noqa: E402


def _quarter(filing_date="2026-06-30", gaap_eps=1.0, adjusted_eps=1.2, quarter=2):
    return {
        "filing_date": filing_date,
        "gaap_eps": gaap_eps,
        "adjusted_eps": adjusted_eps,
        "quarter": quarter,
        "special_flags": [],
    }


def _write_summary(data_root, rows, **top):
    os.makedirs(data_root, exist_ok=True)
    path = os.path.join(data_root, "summary.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"last_updated": "2026-10-04T16:13:05", "tickers": rows, **top}, f)
    return path


def _read_summary(data_root):
    with open(os.path.join(data_root, "summary.json"), encoding="utf-8") as f:
        return json.load(f)


def _row(ticker, generated_at=None, **kw):
    r = {"ticker": ticker, "company_name": ticker, "latest_filing_date": "2026-03-31",
         "gaap_eps": 0.5, "adjusted_eps": 0.6, "health": "調整小"}
    if generated_at:
        r["generated_at"] = generated_at
    r.update(kw)
    return r


@pytest.fixture
def fake_run(monkeypatch, tmp_path):
    """process_one_ticker・銘柄リストを差し替えてrun()を動かす。
    outcomes: {ticker: "ok" | "no_data" | "error"}"""
    data_root = str(tmp_path / "data")
    step_summary = tmp_path / "step_summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(step_summary))

    def setup(outcomes, eps=None, tanuki=None):
        eps = list(eps if eps is not None else outcomes)
        monkeypatch.setattr(aea_pipeline, "get_eps_tickers", lambda: list(eps))
        monkeypatch.setattr(aea_pipeline, "get_registrable_tickers",
                            lambda flag=None: list(eps) if flag == "eps" else list(tanuki or []))

        def fake_process(ticker, *args, **kwargs):
            o = outcomes[ticker]
            if o == "ok":
                return (ticker, {"quarters": [_quarter()], "company_name": f"{ticker} Inc"}, None)
            if o == "no_data":
                return (ticker, None, "データなし")
            raise RuntimeError(f"boom {ticker}")

        monkeypatch.setattr(aea_pipeline, "process_one_ticker", fake_process)
        return data_root, step_summary

    return setup


# ── run(): 失敗の数え方と終了コード ─────────────────────────────────────

def test_partial_failure_exit0_and_run_status(fake_run):
    data_root, step_summary = fake_run({"AAA": "ok", "BBB": "no_data", "CCC": "error"})
    rc = aea_pipeline.run(force=True, data_root=data_root)
    assert rc == 0
    s = _read_summary(data_root)
    rs = s["run_status"]
    assert rs["scope"] == "all"
    assert rs["requested"] == 3
    assert rs["succeeded"] == 1
    failed = {f["ticker"]: f["reason"] for f in rs["failed"]}
    assert set(failed) == {"BBB", "CCC"}
    assert failed["BBB"] == "データなし"
    assert "RuntimeError" in failed["CCC"] and "boom CCC" in failed["CCC"]
    assert rs["started_at"] <= rs["finished_at"]
    # Step Summaryに件数と失敗した銘柄・理由
    text = step_summary.read_text(encoding="utf-8")
    assert "成功 1" in text and "失敗 2" in text
    assert "BBB" in text and "データなし" in text and "CCC" in text


def test_all_failed_exit1_and_summary_not_written(fake_run):
    data_root, step_summary = fake_run({"AAA": "no_data", "BBB": "error"})
    rc = aea_pipeline.run(force=True, data_root=data_root)
    assert rc == 1
    assert not os.path.exists(os.path.join(data_root, "summary.json"))
    assert "成功 0" in step_summary.read_text(encoding="utf-8")


def test_all_failed_keeps_existing_summary_untouched(fake_run):
    data_root, _ = fake_run({"AAA": "no_data"})
    path = _write_summary(data_root, [_row("AAA", generated_at="2026-10-04T16:00:00+00:00")])
    before = open(path, encoding="utf-8").read()
    assert aea_pipeline.run(force=True, data_root=data_root) == 1
    assert open(path, encoding="utf-8").read() == before


def test_failed_row_keeps_generated_at(fake_run):
    data_root, _ = fake_run({"AAA": "ok", "BBB": "no_data"})
    old = "2026-10-04T16:00:00+00:00"
    _write_summary(data_root, [_row("AAA", generated_at=old), _row("BBB", generated_at=old)])
    assert aea_pipeline.run(force=True, data_root=data_root) == 0
    s = _read_summary(data_root)
    rows = {r["ticker"]: r for r in s["tickers"]}
    started = s["run_status"]["started_at"]
    assert rows["AAA"]["generated_at"] >= started
    assert rows["BBB"]["generated_at"] == old  # 失敗した銘柄の行は前回のまま
    assert rows["BBB"]["latest_filing_date"] == "2026-03-31"


def test_idempotency_guard_skip_returns_zero(fake_run):
    data_root, _ = fake_run({"AAA": "ok"})
    _write_summary(data_root, [_row("AAA")])
    with open(os.path.join(data_root, "summary.json"), encoding="utf-8") as f:
        d = json.load(f)
    from datetime import datetime
    d["last_updated"] = datetime.now().isoformat()
    with open(os.path.join(data_root, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(d, f)
    assert aea_pipeline.run(force=False, data_root=data_root) == 0
    assert "run_status" not in _read_summary(data_root)  # 何もしていない


# ── 行の整理（eps=trueでない銘柄の行を消す） ──────────────────────────────

def test_run_removes_rows_not_eps(fake_run, capsys):
    data_root, _ = fake_run({"AAA": "ok"}, eps=["AAA"])
    _write_summary(data_root, [_row("AAA"), _row("FIG"), _row("ZS")])
    assert aea_pipeline.run(force=True, data_root=data_root) == 0
    tickers = [r["ticker"] for r in _read_summary(data_root)["tickers"]]
    assert tickers == ["AAA"]
    out = capsys.readouterr().out
    assert "FIG" in out and "ZS" in out


def test_partial_run_keeps_other_eps_rows_and_run_status(fake_run):
    data_root, _ = fake_run({"AAA": "ok", "BBB": "ok"}, eps=["AAA", "BBB"])
    full_status = {"scope": "all", "started_at": "2026-10-04T16:00:00+00:00",
                   "finished_at": "2026-10-04T16:10:00+00:00", "requested": 2, "succeeded": 2, "failed": []}
    old = "2026-10-04T16:05:00+00:00"
    _write_summary(data_root, [_row("AAA", generated_at=old), _row("BBB", generated_at=old), _row("ZS")],
                   run_status=full_status)
    assert aea_pipeline.run(ticker_filter="AAA", data_root=data_root) == 0
    s = _read_summary(data_root)
    rows = {r["ticker"]: r for r in s["tickers"]}
    assert set(rows) == {"AAA", "BBB"}  # eps=trueのBBBは残り、ZSは消える
    assert rows["BBB"]["generated_at"] == old
    assert rows["AAA"]["generated_at"] > old
    # 全銘柄の実行の記録は部分実行で上書きしない（[I]の判定の基準のため）
    assert s["run_status"] == full_status
    assert s["partial_run_status"]["scope"] == "partial"
    assert s["partial_run_status"]["tickers"] == ["AAA"]


def test_generate_summary_prunes_and_sets_tanuki_flag():
    existing_rows = [_row("AAA"), _row("FIG"), _row("RKLB")]
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        path = _write_summary(d, existing_rows)
        s = aea_pipeline.generate_summary(
            {"AAA": {"quarters": [_quarter(quarter=3)], "company_name": "A"}},
            existing_summary_path=path, allowed_tickers=["AAA", "RKLB"], tanuki_tickers=["AAA"],
            generated_at="2026-10-11T06:00:00+00:00",
        )
    rows = {r["ticker"]: r for r in s["tickers"]}
    assert set(rows) == {"AAA", "RKLB"}
    assert rows["AAA"]["tanuki"] is True and rows["RKLB"]["tanuki"] is False
    assert rows["AAA"]["generated_at"] == "2026-10-11T06:00:00+00:00"
    assert rows["AAA"]["latest_fiscal_quarter"] == 3
    assert "generated_at" not in rows["RKLB"]  # 作り直していない行には付けない


def test_generate_summary_without_allowed_keeps_all_rows():
    """allowed_tickersを渡さない呼び出し（既存のテスト等）は従来どおり行を消さない"""
    s = aea_pipeline.generate_summary({"AAA": {"quarters": [_quarter()], "company_name": "A"}})
    assert [r["ticker"] for r in s["tickers"]] == ["AAA"]


# ── System Health [I] ─────────────────────────────────────────────────

def _i(tmp_path, rows, eps, **top):
    path = _write_summary(str(tmp_path), rows, **top)
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    d["last_updated"] = "2026-10-11T06:00:00"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(d, f)
    from datetime import date
    return sh._check_i(path=path, eps_tickers=eps, today=date(2026, 10, 11))


RS = {"scope": "all", "started_at": "2026-10-11T05:00:00+00:00", "finished_at": "2026-10-11T05:10:00+00:00",
      "requested": 3, "succeeded": 2, "failed": [{"ticker": "CCC", "reason": "データなし"}]}


def test_i_flags_rows_not_regenerated(tmp_path):
    rows = [_row("AAA", generated_at="2026-10-11T05:03:00+00:00"),
            _row("BBB", generated_at="2026-10-11T05:04:00+00:00"),
            _row("CCC", generated_at="2026-10-04T16:00:00+00:00"),
            _row("DDD")]  # generated_atの無い行（作り直されたことがない）も数える
    r = _i(tmp_path, rows, ["AAA", "BBB", "CCC", "DDD"], run_status=RS)
    assert r["ok"] is False
    assert r["label"].startswith("⚠️")
    assert sh._item_severity(r["label"], r["ok"]) == 1
    assert "CCC" in r["detail"] and "DDD" in r["detail"]
    assert "AAA" not in r["detail"].split("未更新")[1]


def test_i_ok_when_all_regenerated(tmp_path):
    rows = [_row("AAA", generated_at="2026-10-11T05:03:00+00:00")]
    r = _i(tmp_path, rows, ["AAA"], run_status=dict(RS, failed=[], succeeded=1, requested=1))
    assert r["ok"] is True and r["label"].startswith("✅")


def test_i_without_run_status_does_not_flag(tmp_path):
    """実装前のsummary.json（generated_at・run_statusが無い）は誤検知しない"""
    r = _i(tmp_path, [_row("AAA"), _row("BBB")], ["AAA", "BBB"])
    assert r["ok"] is True and r["label"].startswith("✅")
    assert "run_status" in r["detail"]


def test_i_ignores_rows_of_non_eps_tickers(tmp_path):
    rows = [_row("AAA", generated_at="2026-10-11T05:03:00+00:00"), _row("ZS")]
    r = _i(tmp_path, rows, ["AAA"], run_status=dict(RS, failed=[], succeeded=1, requested=1))
    assert r["ok"] is True


def test_check_i_eps_wraps_check_i(monkeypatch, tmp_path):
    rows = [_row("AAA", generated_at="2026-10-04T00:00:00+00:00")]
    path = _write_summary(str(tmp_path), rows, run_status=RS)
    monkeypatch.setattr(sh, "_EPS_SUMMARY", path)
    monkeypatch.setattr(sh._tickers_mod, "get_eps_tickers", lambda: ["AAA"])
    label, ok, detail = sh.check_i_eps()
    assert ok is False and "AAA" in detail
