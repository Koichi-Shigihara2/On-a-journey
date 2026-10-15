"""
update.pyの1銘柄の失敗の扱い・巻き戻し・通知（[[SEC-UPDATE-EXIT1-BLOCKS-ALL-1]]、2026-10-15）

- parse_and_saveのRuntimeError・Noneの戻り値、normalizeの例外は、その銘柄だけFAILEDにして次の銘柄へ進む
- 失敗が対象の10%以下は終了コード0、10%超は1。銘柄のループの外の例外は1
- _freshness.json・Step Summary・失敗の一覧は例外の場合も書かれる
- rollback_failed.pyは失敗した銘柄の出力だけをHEADに戻す（一時のgitリポジトリで確認）
- CHECK-60・System Health [M]・Discordの本文・SEC_Data_Auditの先頭の行
SECへの通信・実データの書き込みはしない（処理の各段を差し替える）。
"""
import json
import os
import subprocess
import sys

import pytest

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data import freshness as fr  # noqa: E402
from common.sec_data import update as upd  # noqa: E402
from common.sec_data import rollback_failed as rb  # noqa: E402
from common.sec_data import update_notify as un  # noqa: E402
from common.sec_data import audit  # noqa: E402
from common.sec_data import report_consistency_check as rcc  # noqa: E402
from common import system_health as sh  # noqa: E402


class _FakeFetcher:
    def __init__(self, data_dir=None):
        self.request_count = 0


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    """update.run()の各段を差し替える。behaviorに銘柄→段→振る舞いを入れると、その段で失敗させる。"""
    behavior = {}
    calls = {"ttm_saved": [], "normalized_saved": []}

    def plan(fetcher, ticker, data_dir, dry_run=False):
        return {"ticker": ticker, "case": "c", "local": {"facts": {}}, "payload": {}, "filing_dates": {},
                "latest_filed": "2026-08-01", "latest_submission": "2026-08-01", "pending": []}

    def execute(fetcher, p):
        return fr.entry(fr.OK_NO_NEW, "c", p["latest_filed"], p["latest_submission"], []), {"facts": {}}

    class _FakeParser:
        def __init__(self, data_dir=None):
            pass

        def parse_and_save(self, ticker):
            b = behavior.get(ticker, {}).get("parse")
            if b == "raise":
                raise RuntimeError(f"{ticker} 2023: fixed_registry.json登録済みだが今回の抽出結果に該当年度が存在しない")
            if b == "none":
                return None
            return {"annual": {2025: {}}}

    def normalize(ticker, raw_table):
        if behavior.get(ticker, {}).get("normalize") == "raise":
            raise ValueError("ContractViolation: 規約B違反")
        return {"fields": {}}

    def revenue(ticker, normalized):
        if ticker == "REVW":
            return {"status": "WARN", "latest_rev_yoy": 12.5, "issues": [], "warnings": ["急増"]}
        return {"status": "OK", "latest_rev_yoy": None, "issues": [], "warnings": []}

    monkeypatch.setattr(upd, "SECFetcher", _FakeFetcher)
    monkeypatch.setattr(upd, "SECParser", _FakeParser)
    monkeypatch.setattr(upd, "plan_company_facts", plan)
    monkeypatch.setattr(upd, "execute_company_facts", execute)
    monkeypatch.setattr(upd, "load_company_facts", lambda t, data_dir=None: {"facts": {}})
    monkeypatch.setattr(upd, "build_raw_table", lambda t, cf: {"fields": {}})
    monkeypatch.setattr(upd, "normalize", normalize)
    monkeypatch.setattr(upd, "save_normalized", lambda t, n: calls["normalized_saved"].append(t))
    monkeypatch.setattr(upd, "check_revenue_quality", revenue)
    monkeypatch.setattr(upd, "check_revenue_tag_conflict", lambda t, data_dir=None: {"status": "OK", "conflicts": []})
    monkeypatch.setattr(upd, "build_ticker_store", lambda t: {"fields": {}})
    monkeypatch.setattr(upd, "calc_ttm_series", lambda t, store: [])
    monkeypatch.setattr(upd, "save_ttm_series", lambda t, s: calls["ttm_saved"].append(t))

    summary = tmp_path / "step_summary.md"
    result = tmp_path / "runner_temp" / "sec_update_result.json"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setenv("SEC_UPDATE_RESULT", str(result))
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    class P:
        pass

    p = P()
    p.behavior, p.calls, p.data_dir = behavior, calls, str(data_dir)
    p.summary, p.result = summary, result
    p.freshness = lambda: json.loads((data_dir / fr.FRESHNESS_FILENAME).read_text(encoding="utf-8"))
    p.run = lambda tickers, full_run=True: upd.run(tickers, full_run, data_dir=str(data_dir))
    return p


TEN = [f"T{i:02d}" for i in range(10)]


def test_parse_runtime_error_fails_one_ticker_and_continues(pipeline):
    pipeline.behavior["T03"] = {"parse": "raise"}
    code = pipeline.run(TEN)
    assert code == 0  # 1/10 = 10%以下
    assert pipeline.calls["ttm_saved"] == [t for t in TEN if t != "T03"]  # 後ろの銘柄も処理した
    e = pipeline.freshness()["tickers"]["T03"]
    assert e["status"] == fr.FAILED and e["failed_stage"] == "parse"
    assert "RuntimeError" in e["failed_reason"] and e["fetch_status"] == fr.OK_NO_NEW
    assert pipeline.freshness()["counts"][fr.FAILED] == 1
    res = json.loads(pipeline.result.read_text(encoding="utf-8"))
    assert res["failed"] == [{"ticker": "T03", "stage": "parse", "reason": e["failed_reason"]}]
    assert res["exit_code"] == 0 and res["succeeded"] == 9 and res["aborted"] is None


def test_parse_returning_none_is_failed(pipeline):
    pipeline.behavior["T05"] = {"parse": "none"}
    assert pipeline.run(TEN) == 0
    e = pipeline.freshness()["tickers"]["T05"]
    assert e["status"] == fr.FAILED and e["failed_stage"] == "parse"
    assert "T05" not in pipeline.calls["normalized_saved"]


def test_normalize_exception_is_failed_and_ttm_not_written(pipeline):
    pipeline.behavior["T07"] = {"normalize": "raise"}
    assert pipeline.run(TEN) == 0
    e = pipeline.freshness()["tickers"]["T07"]
    assert e["status"] == fr.FAILED and e["failed_stage"] == "normalize"
    assert "ValueError" in e["failed_reason"]
    assert "T07" not in pipeline.calls["ttm_saved"]  # 以前は成功として数え、古いTTMのまま混在していた


@pytest.mark.parametrize("n_failed, expected", [(0, 0), (1, 0), (2, 1), (10, 1)])
def test_exit_code_by_failure_ratio(pipeline, n_failed, expected):
    for t in TEN[:n_failed]:
        pipeline.behavior[t] = {"parse": "raise"}
    assert pipeline.run(TEN) == expected


def test_single_ticker_failure_exits_one(pipeline):
    pipeline.behavior["T00"] = {"parse": "raise"}
    assert pipeline.run(["T00"], full_run=False) == 1  # 1/1 = 100%


def test_exit_code_function():
    assert upd.exit_code(98, 9, False) == 0
    assert upd.exit_code(98, 10, False) == 1
    assert upd.exit_code(98, 0, True) == 1


def test_step_summary_lists_failures_first_and_revenue_header_before_rows(pipeline):
    pipeline.behavior["T01"] = {"parse": "raise"}
    pipeline.run(TEN + ["REVW"])
    text = pipeline.summary.read_text(encoding="utf-8")
    assert text.lstrip().startswith("## ⚠ 失敗した銘柄 1 / 対象 11")
    assert "| T01 | parse | RuntimeError" in text
    # Revenue品質の見出しが行より前（[[SEC-UPDATE-SUMMARY-HEADER-ORDER-1]]）、鮮度の節が最後
    i_head = text.index("## Revenue品質チェック")
    i_row = text.index("| REVW | 🟡 WARN |")
    i_fresh = text.index("## SECデータの鮮度")
    assert text.index("失敗した銘柄") < i_head < i_row < i_fresh
    assert "**FAILED**（1）" in text


def test_step_summary_marks_exit_one_when_over_ratio(pipeline):
    for t in TEN[:3]:
        pipeline.behavior[t] = {"parse": "raise"}
    pipeline.run(TEN)
    text = pipeline.summary.read_text(encoding="utf-8")
    assert text.lstrip().startswith("## ❌ 失敗した銘柄 3 / 対象 10")
    assert "10%を超えたため終了コード1" in text


def test_outside_loop_exception_still_writes_freshness_summary_and_result(pipeline, monkeypatch):
    # 前回の全銘柄の実行の記録がある状態で、ループの外（SECFetcherの初期化）で例外
    fr.write({"T00": fr.entry(fr.OK_NO_NEW, "c", "2026-08-01", None, [])}, full_run=True,
             path=fr.default_path(pipeline.data_dir))
    before = pipeline.freshness()["generated_at"]

    def boom(data_dir=None):
        raise OSError("disk full")

    monkeypatch.setattr(upd, "SECFetcher", boom)
    assert pipeline.run(TEN) == 1
    doc = pipeline.freshness()
    assert doc["generated_at"] == before  # 途中までの実行を全銘柄の実行として数えない
    assert "T00" in doc["tickers"]
    text = pipeline.summary.read_text(encoding="utf-8")
    assert "銘柄のループの外で例外" in text and "OSError: disk full" in text
    res = json.loads(pipeline.result.read_text(encoding="utf-8"))
    assert res["exit_code"] == 1 and "OSError" in res["aborted"]


def test_freshness_write_failure_is_exit_one_but_summary_written(pipeline, monkeypatch):
    def bad_write(*a, **k):
        raise PermissionError("read-only")

    monkeypatch.setattr(fr, "write", bad_write)
    assert pipeline.run(TEN) == 1
    text = pipeline.summary.read_text(encoding="utf-8")
    assert "_freshness.jsonを書けない" in text
    assert json.loads(pipeline.result.read_text(encoding="utf-8"))["exit_code"] == 1


# ── 巻き戻し（一時のgitリポジトリ） ──────────────────────────────

def _git(repo, *args):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "core.autocrlf=false",
                    *args], cwd=repo, check=True, capture_output=True)


def _write(repo, rel, text):
    p = os.path.join(repo, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


def _read(repo, rel):
    with open(os.path.join(repo, rel), encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def repo(tmp_path):
    r = str(tmp_path / "repo")
    os.makedirs(r)
    _git(r, "init", "-q")
    for t in ("AAA", "BBB"):
        _write(r, f"common/sec_data/data/{t}/company_facts.json", f"{t} old facts")
        _write(r, f"common/sec_data/data/{t}/annual_2025.json", f"{t} old 2025")
        _write(r, f"common/sec_data/normalized/{t}_quarterly_normalized.json", f"{t} old norm")
        _write(r, f"common/sec_data/ttm/{t}_ttm_series.json", f"{t} old ttm")
    _write(r, "common/sec_data/data/_freshness.json", "old freshness")
    _git(r, "add", "-A")
    _git(r, "commit", "-qm", "init")
    return r


def test_rollback_restores_failed_existing_and_removes_new_ticker(repo, tmp_path):
    # update.pyの実行後の状態: AAA（失敗）・BBB（成功）が書き換わり、AAAに新しい年度・新規銘柄NEWの作りかけ
    for t in ("AAA", "BBB"):
        _write(repo, f"common/sec_data/data/{t}/company_facts.json", f"{t} new facts")
        _write(repo, f"common/sec_data/normalized/{t}_quarterly_normalized.json", f"{t} new norm")
        _write(repo, f"common/sec_data/ttm/{t}_ttm_series.json", f"{t} new ttm")
    _write(repo, "common/sec_data/data/AAA/annual_2026.json", "AAA new 2026")
    _write(repo, "common/sec_data/data/NEW/company_facts.json", "NEW facts")
    _write(repo, "common/sec_data/data/NEW/annual_2025.json", "NEW 2025")
    _write(repo, "common/sec_data/data/_freshness.json", "new freshness")
    result = tmp_path / "sec_update_result.json"
    result.write_text(json.dumps({"failed": [
        {"ticker": "AAA", "stage": "parse", "reason": "RuntimeError"},
        {"ticker": "NEW", "stage": "normalize", "reason": "ValueError"},
    ]}), encoding="utf-8")

    assert rb.main([str(result)], repo=repo) == 0

    assert _read(repo, "common/sec_data/data/AAA/company_facts.json") == "AAA old facts"
    assert _read(repo, "common/sec_data/normalized/AAA_quarterly_normalized.json") == "AAA old norm"
    assert _read(repo, "common/sec_data/ttm/AAA_ttm_series.json") == "AAA old ttm"
    assert not os.path.exists(os.path.join(repo, "common/sec_data/data/AAA/annual_2026.json"))
    assert not os.path.exists(os.path.join(repo, "common/sec_data/data/NEW"))
    # 成功した銘柄と共有のファイルはそのまま
    assert _read(repo, "common/sec_data/data/BBB/company_facts.json") == "BBB new facts"
    assert _read(repo, "common/sec_data/ttm/BBB_ttm_series.json") == "BBB new ttm"
    assert _read(repo, "common/sec_data/data/_freshness.json") == "new freshness"


def test_rollback_without_failures_changes_nothing(repo, tmp_path):
    _write(repo, "common/sec_data/data/AAA/company_facts.json", "AAA new facts")
    result = tmp_path / "r.json"
    result.write_text(json.dumps({"failed": []}), encoding="utf-8")
    assert rb.main([str(result)], repo=repo) == 0
    assert _read(repo, "common/sec_data/data/AAA/company_facts.json") == "AAA new facts"


def test_rollback_missing_result_file_is_error(repo, tmp_path):
    assert rb.main([str(tmp_path / "missing.json")], repo=repo) == 1


# ── _freshness.jsonの読み手（CHECK-60・System Health [M]） ─────────────

def _freshness_with_failed(tmp_path, n_ok, n_failed):
    tickers = {f"OK{i}": fr.entry(fr.OK_NO_NEW, "c", "2026-08-01", None, []) for i in range(n_ok)}
    for i in range(n_failed):
        e = fr.entry(fr.FAILED, "b", "2026-08-01", "2026-10-01", [], "[parse] RuntimeError")
        e.update({"failed_stage": "parse", "failed_reason": "RuntimeError: x", "fetch_status": fr.REFRESHED})
        tickers[f"NG{i}"] = e
    p = str(tmp_path / fr.FRESHNESS_FILENAME)
    fr.write(tickers, full_run=True, path=p)
    return p


def test_check60_warns_failed(tmp_path):
    p = _freshness_with_failed(tmp_path, 9, 1)
    out = rcc._check_sec_freshness(path=p)
    assert [t for t, _ in out] == ["NG0"]
    assert "FAILED（parse: RuntimeError: x" in out[0][1]


def test_m_failed_within_ratio_is_warning(tmp_path):
    r = sh._check_m(path=_freshness_with_failed(tmp_path, 9, 1), ack_path=str(tmp_path / "none.json"))
    assert r["ok"] is False and r["critical"] is False
    assert "FAILED 1/10件" in r["detail"] and "NG0(parse)" in r["detail"]


def test_m_failed_over_ratio_is_critical(tmp_path):
    r = sh._check_m(path=_freshness_with_failed(tmp_path, 8, 2), ack_path=str(tmp_path / "none.json"))
    assert r["critical"] is True and r["label"].startswith("🔴")


def test_m_healthy_label_counts_failed_zero(tmp_path):
    r = sh._check_m(path=_freshness_with_failed(tmp_path, 3, 0), ack_path=str(tmp_path / "none.json"))
    assert r["ok"] is True and "FAILED 0" in r["detail"]


# ── Discordの本文 ─────────────────────────────────────────────

_ALL_OK = {k: "success" for k, _ in un.STEPS}


def test_notify_success():
    msg = un.build_message(_ALL_OK, {"targets": 98, "succeeded": 98, "failed": [], "failed_exit_ratio": 0.1})
    assert msg.startswith("✅ **SEC Data Update: 成功**") and "止まった段" not in msg


def test_notify_partial_failure_lists_tickers():
    res = {"targets": 98, "succeeded": 97, "failed_exit_ratio": 0.1,
           "failed": [{"ticker": "XOM", "stage": "parse", "reason": "RuntimeError: XOM 2023"}]}
    msg = un.build_message(_ALL_OK, res, "https://example/run/1")
    assert msg.startswith("⚠ ") and "`XOM` parse: RuntimeError: XOM 2023" in msg
    assert msg.rstrip().endswith("https://example/run/1")


def test_notify_reports_stopped_step():
    outcomes = dict(_ALL_OK, OUTCOME_GATE="failure", OUTCOME_SYNC="skipped", OUTCOME_COMMIT="skipped")
    msg = un.build_message(outcomes, {"targets": 98, "succeeded": 98, "failed": [], "failed_exit_ratio": 0.1})
    assert msg.startswith("❌ ") and "止まった段: Consistency Check Gate（failure）" in msg


def test_notify_install_failure_without_result():
    outcomes = {k: "skipped" for k, _ in un.STEPS}
    outcomes["OUTCOME_INSTALL"] = "failure"
    msg = un.build_message(outcomes, None)
    assert "止まった段: 依存パッケージの導入（failure）" in msg


def test_notify_truncates_long_message():
    res = {"targets": 500, "succeeded": 0, "failed_exit_ratio": 0.1,
           "failed": [{"ticker": f"T{i}", "stage": "parse", "reason": "x" * 150} for i in range(500)]}
    msg = un.build_message(dict(_ALL_OK, OUTCOME_UPDATE="failure"), res, "https://example/run/2")
    assert len(msg) <= 2000 and msg.endswith("https://example/run/2")


# ── SEC_Data_Auditの先頭の行 ─────────────────────────────────

@pytest.mark.parametrize("conclusion, expected", [
    ("success", ""), ("", ""), ("failure", "⚠ SEC更新が失敗：前回のデータを監査（SEC Data Update: failure）"),
    ("cancelled", "⚠ SEC更新が失敗：前回のデータを監査（SEC Data Update: cancelled）"),
])
def test_audit_upstream_prefix(conclusion, expected):
    assert audit.upstream_failed_prefix(conclusion) == expected


def test_audit_upstream_prefix_reads_env(monkeypatch):
    monkeypatch.setenv("SEC_UPDATE_CONCLUSION", "failure")
    assert audit.upstream_failed_prefix().startswith(audit.UPSTREAM_FAILED_PREFIX)
    monkeypatch.delenv("SEC_UPDATE_CONCLUSION")
    assert audit.upstream_failed_prefix() == ""
