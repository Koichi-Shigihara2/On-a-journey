"""
tests/test_sec_freshness.py

[[SEC-FETCH-CACHE-MTIME-CI-1]]（2026-10-10）の回帰テスト。SEC_Data_Update（CI）がファイルのmtimeに
よる24時間キャッシュのためSEC APIを一度も呼んでいなかった問題への対応として、update.pyが
submissionsを毎回取り直し、手元のcompany_facts.jsonに無い10-K/10-Q(/A)の提出がある銘柄だけ
company_factsを取り直すようにした。その判定・記録（_freshness.json）・検知
（report_consistency_check.pyのCHECK-60、System Healthの[M]）を検証する。ネットワークは使わない
（SECFetcher._getを差し替える）。

実行方法:
    python -m pytest tests/test_sec_freshness.py -v
"""
import json
import os
import sys
from datetime import date, datetime, timezone

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data import fetcher as fetcher_mod  # noqa: E402
from common.sec_data import freshness as fr  # noqa: E402
from common.sec_data import update as upd  # noqa: E402
from common.sec_data import report_consistency_check as rcc  # noqa: E402
from common import system_health as sh  # noqa: E402

CIK = "0000000001"


@pytest.fixture(autouse=True)
def _isolated_ack_ledger(tmp_path, monkeypatch):
    """本番のconfig/warn_acknowledged.jsonを読まない（確認済みのテストは台帳を個別に書く）。"""
    monkeypatch.setattr(fr, "ACK_LEDGER_PATH", str(tmp_path / "warn_acknowledged_test.json"))
LEGACY_CIK = "0000000002"


# ── テスト用のデータ ────────────────────────────────────────────────────
def _fact(accn, filed, val=1, end="2026-06-30", form="10-Q"):
    return {"accn": accn, "filed": filed, "val": val, "end": end, "form": form, "fy": 2026, "fp": "Q2"}


def _company_facts(*facts, dei_facts=()):
    doc = {"cik": 1, "entityName": "TEST", "facts": {
        "us-gaap": {"Revenues": {"label": "Revenues", "units": {"USD": list(facts)}}}}}
    if dei_facts:
        doc["facts"]["dei"] = {"EntityCommonStockSharesOutstanding": {"units": {"shares": list(dei_facts)}}}
    return doc


def _submissions(rows, files=()):
    """rows: [(form, accn, filingDate, reportDate)]"""
    return {
        "formerNames": [],
        "filings": {
            "recent": {
                "form": [r[0] for r in rows],
                "accessionNumber": [r[1] for r in rows],
                "filingDate": [r[2] for r in rows],
                "reportDate": [r[3] for r in rows],
            },
            "files": [{"name": f} for f in files],
        },
    }


class _Resp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload


def _make_fetcher(tmp_path, monkeypatch, routes, ticker="TEST", legacy=None):
    """routes: {url末尾の部分文字列: (status, payload) | Exception}。呼ばれたURLはcallsに記録する。"""
    monkeypatch.setattr(fetcher_mod.SECFetcher, "RATE_LIMIT_DELAY", 0)
    monkeypatch.setattr(fetcher_mod, "_load_cik_history",
                        lambda: ({ticker: {"legacy_ciks": [legacy]}} if legacy else {}))
    f = fetcher_mod.SECFetcher(data_dir=str(tmp_path))
    f.cik_cache[ticker] = CIK
    f.calls = []

    def fake_get(url, timeout):
        f.request_count += 1
        f.calls.append(url)
        for key, val in routes.items():
            if url.endswith(key):
                if isinstance(val, Exception):
                    raise val
                return _Resp(*val)
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(f, "_get", fake_get)
    return f


def _write_local(tmp_path, ticker, name, doc):
    d = tmp_path / ticker
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(json.dumps(doc), encoding="utf-8")


def _cf_url(cik=CIK):
    return f"companyfacts/CIK{cik}.json"


def _sub_url(cik=CIK):
    return f"submissions/CIK{cik}.json"


OLD_Q = ("10-Q", "0000000001-26-000001", "2026-05-01", "2026-03-31")
NEW_Q = ("10-Q", "0000000001-26-000002", "2026-08-01", "2026-06-30")


def _run(fetcher, tmp_path, ticker="TEST"):
    plan = upd.plan_company_facts(fetcher, ticker, str(tmp_path))
    entry, raw = upd.execute_company_facts(fetcher, plan)
    return plan, entry, raw


# ── freshness.py の純粋な判定 ───────────────────────────────────────────
def test_facts_index_includes_dei_accns():
    cf = _company_facts(_fact("A-1", "2026-05-01"), dei_facts=[_fact("A-2", "2026-06-01")])
    latest, accns = fr.facts_index(cf)
    assert latest == "2026-06-01"
    assert accns == {"A-1", "A-2"}


def test_pending_filings_rules():
    filing_dates = {"A-old": "2026-04-01", "A-same-in": "2026-05-01", "A-same-missing": "2026-05-01",
                    "A-new": "2026-08-01"}
    pending = fr.pending_filings(filing_dates, "2026-05-01", {"A-same-in", "A-old"})
    # Lより前は対象外、Lと同じ日でcompany_factsに無いものとLより後は対象
    assert [p["accn"] for p in pending] == ["A-same-missing", "A-new"]


# ── (a)〜(c) の分岐 ─────────────────────────────────────────────────────
def test_case_a_no_local_company_facts_fetches(tmp_path, monkeypatch):
    cf = _company_facts(_fact(OLD_Q[1], OLD_Q[2]))
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q])),
        _cf_url(): (200, cf),
    })
    plan, entry, raw = _run(f, tmp_path)
    assert plan["case"] == "a"
    assert entry["status"] == fr.REFRESHED
    assert raw == cf
    assert (tmp_path / "TEST" / "company_facts.json").exists()


def test_case_a_fetch_failure_is_failed_without_data(tmp_path, monkeypatch):
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q])),
        _cf_url(): (500, None),
    })
    plan, entry, raw = _run(f, tmp_path)
    assert plan["case"] == "a"
    assert entry["status"] == fr.FETCH_FAILED
    assert raw is None  # update.pyは従来どおり失敗に数える


def test_case_b_new_filing_refreshes(tmp_path, monkeypatch):
    _write_local(tmp_path, "TEST", "company_facts.json", _company_facts(_fact(OLD_Q[1], OLD_Q[2])))
    new_cf = _company_facts(_fact(OLD_Q[1], OLD_Q[2]), _fact(NEW_Q[1], NEW_Q[2]))
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q, NEW_Q])),
        _cf_url(): (200, new_cf),
    })
    plan, entry, raw = _run(f, tmp_path)
    assert plan["case"] == "b"
    assert [p["accn"] for p in plan["pending"]] == [NEW_Q[1]]
    assert entry["status"] == fr.REFRESHED
    assert entry["local_latest_filed"] == NEW_Q[2]
    saved = json.loads((tmp_path / "TEST" / "company_facts.json").read_text(encoding="utf-8"))
    assert saved == new_cf


def test_case_c_no_new_filing_does_not_call_company_facts(tmp_path, monkeypatch):
    _write_local(tmp_path, "TEST", "company_facts.json", _company_facts(_fact(OLD_Q[1], OLD_Q[2])))
    f = _make_fetcher(tmp_path, monkeypatch, {_sub_url(): (200, _submissions([OLD_Q]))})
    plan, entry, raw = _run(f, tmp_path)
    assert plan["case"] == "c"
    assert entry["status"] == fr.OK_NO_NEW
    assert not any("companyfacts" in u for u in f.calls)
    assert f.request_count == 1


def test_case_c_ignores_file_mtime(tmp_path, monkeypatch):
    """mtimeが古くても（ローカル）新しくても（CI）、判定はsubmissionsとfiledだけで決まる。"""
    _write_local(tmp_path, "TEST", "company_facts.json", _company_facts(_fact(OLD_Q[1], OLD_Q[2])))
    path = tmp_path / "TEST" / "company_facts.json"
    os.utime(path, (0, 0))  # 1970年 → 旧方式なら必ず取り直していた
    f = _make_fetcher(tmp_path, monkeypatch, {_sub_url(): (200, _submissions([OLD_Q]))})
    _, entry, _ = _run(f, tmp_path)
    assert entry["status"] == fr.OK_NO_NEW
    assert not any("companyfacts" in u for u in f.calls)


def test_amendment_only_new_filing_triggers_refresh(tmp_path, monkeypatch):
    """10-K/Aだけの新しい提出でも取り直す（Part IIIだけの訂正もdeiのfactでaccnが入る）。"""
    amend = ("10-K/A", "0000000001-26-000009", "2026-06-15", "2025-12-31")
    _write_local(tmp_path, "TEST", "company_facts.json", _company_facts(_fact(OLD_Q[1], OLD_Q[2])))
    new_cf = _company_facts(_fact(OLD_Q[1], OLD_Q[2]), dei_facts=[_fact(amend[1], amend[2])])
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q, amend])),
        _cf_url(): (200, new_cf),
    })
    plan, entry, _ = _run(f, tmp_path)
    assert plan["case"] == "b"
    assert entry["status"] == fr.REFRESHED


def test_sec_lag_when_accn_still_missing_after_refresh(tmp_path, monkeypatch):
    local = _company_facts(_fact(OLD_Q[1], OLD_Q[2]))
    _write_local(tmp_path, "TEST", "company_facts.json", local)
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q, NEW_Q])),
        _cf_url(): (200, local),  # SECのcompanyfactsに新しい10-Qがまだ入っていない
    })
    plan, entry, raw = _run(f, tmp_path)
    assert plan["case"] == "b"
    assert entry["status"] == fr.SEC_LAG
    assert entry["pending"] == [{"accn": NEW_Q[1], "filing_date": NEW_Q[2]}]
    assert raw == local  # エラーにせず続行


def test_sec_lag_keeps_earlier_missing_filing_when_later_one_arrives(tmp_path, monkeypatch):
    """取り直しで後の提出だけ入った場合も、先の提出の未反映を見落とさない（新しいLで数え直さない）。"""
    later = ("10-K/A", "0000000001-26-000010", "2026-09-01", "2025-12-31")
    _write_local(tmp_path, "TEST", "company_facts.json", _company_facts(_fact(OLD_Q[1], OLD_Q[2])))
    new_cf = _company_facts(_fact(OLD_Q[1], OLD_Q[2]), dei_facts=[_fact(later[1], later[2])])
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q, NEW_Q, later])),
        _cf_url(): (200, new_cf),
    })
    _, entry, _ = _run(f, tmp_path)
    assert entry["status"] == fr.SEC_LAG
    assert [p["accn"] for p in entry["pending"]] == [NEW_Q[1]]


def test_company_facts_refetch_failure_uses_local(tmp_path, monkeypatch):
    local = _company_facts(_fact(OLD_Q[1], OLD_Q[2]))
    _write_local(tmp_path, "TEST", "company_facts.json", local)
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q, NEW_Q])),
        _cf_url(): (503, None),
    })
    _, entry, raw = _run(f, tmp_path)
    assert entry["status"] == fr.FETCH_FAILED
    assert raw == local
    assert json.loads((tmp_path / "TEST" / "company_facts.json").read_text(encoding="utf-8")) == local


def test_submissions_failure_continues_with_local_files(tmp_path, monkeypatch):
    local = _company_facts(_fact(OLD_Q[1], OLD_Q[2]))
    _write_local(tmp_path, "TEST", "company_facts.json", local)
    old_sub = {"ticker": "TEST", "cik": CIK, "accn_to_reportdate": {OLD_Q[1]: OLD_Q[3]}}
    _write_local(tmp_path, "TEST", "submissions.json", old_sub)
    f = _make_fetcher(tmp_path, monkeypatch, {_sub_url(): ConnectionError("down")})
    plan, entry, raw = _run(f, tmp_path)
    assert plan["case"] == "submissions_failed"
    assert entry["status"] == fr.FETCH_FAILED
    assert raw == local
    assert not any("companyfacts" in u for u in f.calls)
    # 手元のsubmissions.jsonはそのまま
    assert json.loads((tmp_path / "TEST" / "submissions.json").read_text(encoding="utf-8")) == old_sub


# ── submissions.json の保存（STEP1） ────────────────────────────────────
def test_submissions_saves_filingdate_and_keeps_reportdate(tmp_path, monkeypatch):
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q, NEW_Q, ("8-K", "X-1", "2026-08-02", "")])),
    })
    f.fetch_submissions("TEST", force_refresh=True)
    saved = json.loads((tmp_path / "TEST" / "submissions.json").read_text(encoding="utf-8"))
    assert saved["accn_to_reportdate"] == {OLD_Q[1]: OLD_Q[3], NEW_Q[1]: NEW_Q[3]}
    assert saved["accn_to_filingdate"] == {OLD_Q[1]: OLD_Q[2], NEW_Q[1]: NEW_Q[2]}
    assert {"ticker", "cik", "legacy_ciks_merged", "former_names"} <= set(saved)


def test_legacy_cik_submissions_merged_and_refreshed(tmp_path, monkeypatch):
    legacy_q = ("10-K", "0000000002-14-000001", "2014-02-01", "2013-12-31")
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q])),
        _sub_url(LEGACY_CIK): (200, _submissions([legacy_q])),
    }, legacy="2")
    payload = f.fetch_submissions("TEST", force_refresh=True, return_payload=True)
    assert payload["accn_to_reportdate"][legacy_q[1]] == legacy_q[3]
    assert payload["accn_to_filingdate"][legacy_q[1]] == legacy_q[2]
    assert any(u.endswith(_sub_url(LEGACY_CIK)) for u in f.calls)  # force_refreshで旧CIKも取り直す
    assert (tmp_path / "TEST" / f"submissions_legacy_{LEGACY_CIK}.json").exists()


def test_legacy_cik_failure_falls_back_to_local_legacy_file(tmp_path, monkeypatch):
    legacy_q = ("10-K", "0000000002-14-000001", "2014-02-01", "2013-12-31")
    _write_local(tmp_path, "TEST", f"submissions_legacy_{LEGACY_CIK}.json",
                 {"ticker": "TEST", "cik": LEGACY_CIK, "accn_to_reportdate": {legacy_q[1]: legacy_q[3]}})
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q])),
        _sub_url(LEGACY_CIK): (500, None),
    }, legacy="2")
    payload = f.fetch_submissions("TEST", force_refresh=True, return_payload=True)
    assert payload["accn_to_reportdate"][legacy_q[1]] == legacy_q[3]  # 旧CIK分が欠けない


def test_legacy_cik_company_facts_failure_uses_local_legacy(tmp_path, monkeypatch):
    legacy_cf = _company_facts(_fact("0000000002-14-000001", "2014-02-01", end="2013-12-31", form="10-K"))
    _write_local(tmp_path, "TEST", f"company_facts_legacy_{LEGACY_CIK}.json", legacy_cf)
    _write_local(tmp_path, "TEST", "company_facts.json", _company_facts(_fact(OLD_Q[1], OLD_Q[2])))
    new_cf = _company_facts(_fact(OLD_Q[1], OLD_Q[2]), _fact(NEW_Q[1], NEW_Q[2]))
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q, NEW_Q])),
        _sub_url(LEGACY_CIK): (200, _submissions([])),
        _cf_url(): (200, new_cf),
        _cf_url(LEGACY_CIK): (500, None),
    }, legacy="2")
    _, entry, raw = _run(f, tmp_path)
    assert entry["status"] == fr.REFRESHED
    accns = fr.facts_index(raw)[1]
    assert "0000000002-14-000001" in accns  # 旧CIKのfactがマージされたまま


def test_archive_failure_keeps_existing_submissions(tmp_path, monkeypatch):
    old_sub = {"ticker": "TEST", "cik": CIK, "accn_to_reportdate": {"OLD-ARCHIVED": "2010-12-31"}}
    _write_local(tmp_path, "TEST", "submissions.json", old_sub)
    f = _make_fetcher(tmp_path, monkeypatch, {
        _sub_url(): (200, _submissions([OLD_Q], files=["CIK0000000001-submissions-001.json"])),
        "CIK0000000001-submissions-001.json": (500, None),
    })
    assert f.fetch_submissions("TEST", force_refresh=True) is None
    assert json.loads((tmp_path / "TEST" / "submissions.json").read_text(encoding="utf-8")) == old_sub


def test_dry_run_plan_writes_nothing(tmp_path, monkeypatch):
    _write_local(tmp_path, "TEST", "company_facts.json", _company_facts(_fact(OLD_Q[1], OLD_Q[2])))
    f = _make_fetcher(tmp_path, monkeypatch, {_sub_url(): (200, _submissions([OLD_Q, NEW_Q]))})
    before = sorted(os.listdir(tmp_path / "TEST"))
    plan = upd.plan_company_facts(f, "TEST", str(tmp_path), dry_run=True)
    assert plan["case"] == "b"
    assert sorted(os.listdir(tmp_path / "TEST")) == before
    assert not any("companyfacts" in u for u in f.calls)


# ── _freshness.json の記録 ──────────────────────────────────────────────
def test_write_full_and_partial(tmp_path):
    path = str(tmp_path / "_freshness.json")
    e_ok = fr.entry(fr.OK_NO_NEW, "c", "2026-05-01", "2026-05-01", [])
    doc = fr.write({"AAA": e_ok, "BBB": e_ok}, full_run=True, path=path)
    gen = doc["generated_at"]
    assert gen and doc["counts"][fr.OK_NO_NEW] == 2
    e_ref = fr.entry(fr.REFRESHED, "b", "2026-08-01", "2026-08-01", [])
    doc2 = fr.write({"BBB": e_ref}, full_run=False, path=path)
    assert doc2["generated_at"] == gen  # 絞った実行ではgenerated_atを変えない
    assert doc2["tickers"]["AAA"]["status"] == fr.OK_NO_NEW
    assert doc2["tickers"]["BBB"]["status"] == fr.REFRESHED
    assert "last_partial_run_at" in doc2


def test_step_summary_lists_non_ok(tmp_path):
    doc = {"tickers": {
        "AAA": fr.entry(fr.OK_NO_NEW, "c", "2026-05-01", "2026-05-01", []),
        "BBB": fr.entry(fr.SEC_LAG, "b", "2026-05-01", "2026-08-01", [{"accn": "X", "filing_date": "2026-08-01"}]),
    }}
    md = fr.step_summary_markdown(doc)
    assert "| SEC_LAG | 1 |" in md and "BBB" in md and "AAA" not in md.split("**SEC_LAG**")[1]


# ── CHECK-60（report_consistency_check.py） ────────────────────────────
def _freshness_doc(tmp_path, tickers, generated_at="2026-10-10T00:00:00Z"):
    path = tmp_path / "_freshness.json"
    path.write_text(json.dumps({"generated_at": generated_at, "tickers": tickers}), encoding="utf-8")
    return str(path)


def _lag_entry(filing_date):
    e = fr.entry(fr.SEC_LAG, "b", "2026-05-01", filing_date, [{"accn": "X-1", "filing_date": filing_date}])
    return e


def test_check60_missing_file_warns(tmp_path):
    out = rcc._check_sec_freshness(path=str(tmp_path / "none.json"))
    assert len(out) == 1 and out[0][0] == "[GLOBAL]" and "WARN-60" in out[0][1]


def test_check60_fetch_failed_and_old_sec_lag(tmp_path):
    path = _freshness_doc(tmp_path, {
        "AAA": fr.entry(fr.FETCH_FAILED, "submissions_failed", "2026-05-01", None, [], "submissionsを取得できない"),
        "BBB": _lag_entry("2026-09-20"),   # 20日 → WARN
        "CCC": _lag_entry("2026-10-01"),   # 9日 → 対象外
        "DDD": fr.entry(fr.OK_NO_NEW, "c", "2026-05-01", "2026-05-01", []),
    })
    out = rcc._check_sec_freshness(path=path, today=date(2026, 10, 10))
    assert [t for t, _ in out] == ["AAA", "BBB"]
    assert all("[WARN-60" in m for _, m in out)


def test_check60_is_warn_only_and_ledger_matchable():
    msg = "  [WARN-60 SECデータの鮮度] KO: SEC_LAG 73日"
    shown, is_new = rcc.annotate_warn("KO", msg, {("WARN-60", "KO")})
    assert is_new is False and shown == msg


# ── System Health [M] ──────────────────────────────────────────────────
NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


def _ok_entries(n):
    return {f"T{i:03d}": fr.entry(fr.OK_NO_NEW, "c", "2026-08-01", "2026-08-01", []) for i in range(n)}


def test_m_healthy(tmp_path):
    path = _freshness_doc(tmp_path, _ok_entries(20), generated_at="2026-10-05T00:00:00Z")
    r = sh._check_m(path=path, now=NOW)
    assert r["ok"] is True and r["critical"] is False


def test_m_missing_file_is_critical(tmp_path):
    r = sh._check_m(path=str(tmp_path / "none.json"), now=NOW)
    assert r["critical"] is True


def test_m_stale_generated_at_is_critical(tmp_path):
    path = _freshness_doc(tmp_path, _ok_entries(20), generated_at="2026-10-01T00:00:00Z")  # 9.5日前
    r = sh._check_m(path=path, now=NOW)
    assert r["critical"] is True and "全銘柄の実行" in r["detail"]


def test_m_partial_only_without_generated_at_is_critical(tmp_path):
    path = _freshness_doc(tmp_path, _ok_entries(20), generated_at=None)
    assert sh._check_m(path=path, now=NOW)["critical"] is True


def test_m_fetch_failed_ratio(tmp_path):
    tickers = _ok_entries(20)
    tickers["T000"] = fr.entry(fr.FETCH_FAILED, "b", "2026-08-01", "2026-09-01", [], "x")
    r = sh._check_m(path=_freshness_doc(tmp_path, tickers, "2026-10-09T00:00:00Z"), now=NOW)
    assert r["ok"] is False and r["critical"] is False  # 1/20 = 5% → ⚠️
    tickers["T001"] = fr.entry(fr.FETCH_FAILED, "b", "2026-08-01", "2026-09-01", [], "x")
    r = sh._check_m(path=_freshness_doc(tmp_path, tickers, "2026-10-09T00:00:00Z"), now=NOW)
    assert r["critical"] is True  # 2/20 = 10% → 🔴


def test_m_sec_lag_thresholds(tmp_path):
    tickers = _ok_entries(20)
    tickers["KO"] = _lag_entry("2026-09-25")  # 15日 → ⚠️
    r = sh._check_m(path=_freshness_doc(tmp_path, tickers, "2026-10-09T00:00:00Z"), now=NOW)
    assert r["ok"] is False and r["critical"] is False
    tickers["KO"] = _lag_entry("2026-07-29")  # 73日 → 🔴
    r = sh._check_m(path=_freshness_doc(tmp_path, tickers, "2026-10-09T00:00:00Z"), now=NOW)
    assert r["critical"] is True and "KO(73日)" in r["detail"]


def test_main_exits_two_when_m_is_critical(monkeypatch):
    ok3 = lambda *a: ("✅", True, "OK")  # noqa: E731
    for name in ("check_a_sec", "check_b_score_history", "check_c_latest"):
        monkeypatch.setattr(sh, name, lambda tickers: ("✅", True, "0件"))
    for name in ("check_d_actions", "check_e_silo", "check_f_tail", "check_g_hypecore", "check_h_config",
                 "check_i_eps", "check_k_ticker_audit", "check_l_macro_data"):
        monkeypatch.setattr(sh, name, ok3)
    monkeypatch.setattr(sh, "_check_j", lambda: {"label": "✅", "ok": True, "detail": "OK", "critical": False})
    m_detail = "98銘柄 / SEC_LAG 30日超1件: KO(73日)"
    monkeypatch.setattr(sh, "_check_m", lambda: {"label": "🔴 " + m_detail, "ok": False,
                                                 "detail": m_detail, "critical": True})
    monkeypatch.setattr(sh, "get_registered_tickers", lambda: [])
    sent = []
    monkeypatch.setattr(sh, "post_discord", lambda text: sent.append(text) or False)
    monkeypatch.setattr(sys, "argv", ["system_health.py", "--quiet"])
    assert sh.main() == 2
    assert m_detail in sent[0]  # [M]は切らずに通知に出る


def _write_ack(tmp_path, entries):
    path = tmp_path / "warn_acknowledged_test.json"
    path.write_text(json.dumps({"acknowledged": entries}), encoding="utf-8")
    return str(path)


def test_m_acknowledged_accn_is_not_critical(tmp_path):
    tickers = _ok_entries(20)
    tickers["KO"] = _lag_entry("2026-07-29")  # 73日（accn X-1）
    ack = _write_ack(tmp_path, [{"check": "WARN-60", "ticker": "KO", "match": "X-1",
                                 "acknowledged_date": "2026-10-10", "comment": "[[SEC-COMPANYFACTS-API-LAG-1]]"}])
    r = sh._check_m(path=_freshness_doc(tmp_path, tickers, "2026-10-09T00:00:00Z"), now=NOW, ack_path=ack)
    assert r["critical"] is False and r["ok"] is True
    assert "確認済み1件: KO(73日・X-1)" in r["detail"]  # 詳細には出し続ける


def test_m_other_accn_of_same_ticker_is_still_critical(tmp_path):
    tickers = _ok_entries(20)
    e = _lag_entry("2026-07-29")
    e["pending"].append({"accn": "X-2", "filing_date": "2026-08-20"})  # 51日、未確認
    tickers["KO"] = e
    ack = _write_ack(tmp_path, [{"check": "WARN-60", "ticker": "KO", "match": "X-1"}])
    r = sh._check_m(path=_freshness_doc(tmp_path, tickers, "2026-10-09T00:00:00Z"), now=NOW, ack_path=ack)
    assert r["critical"] is True and "KO(51日)" in r["detail"]


def test_check60_one_line_per_accn_and_ledger_match(tmp_path):
    e = _lag_entry("2026-07-29")
    e["pending"].append({"accn": "X-2", "filing_date": "2026-08-20"})
    out = rcc._check_sec_freshness(path=_freshness_doc(tmp_path, {"KO": e}), today=date(2026, 10, 10))
    assert len(out) == 2
    ledger = {("WARN-60", "KO", "X-1")}
    flags = [rcc.annotate_warn(t, m, ledger)[1] for t, m in out]
    assert flags == [False, True]  # X-1だけ確認済み、X-2は未確認のまま


def test_m_in_one_line_summary():
    line = sh.build_one_line("2026-10-10", {"M": {"ok": False, "short": "x"}})
    assert "SecFresh⚠️x" in line
