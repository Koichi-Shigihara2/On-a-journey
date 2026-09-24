"""
tests/test_registration_consistency_gate.py

[[REGISTRATION-VALIDATOR-P2A-PERIOD-MISMATCH-1]]の回帰テスト（2026-09-24）。
registration_validator.pyのP2-A（期末の異なる年次売上とTTM売上の比較）を廃止し、
新規登録フローのStep 7.5で同一期間の整合性チェック（report_consistency_check.pyの
CHECK-35/41/47等）をprovisioning中の銘柄に対して実行するようにした。

- report_consistency_check.py: --include-provisioningでprovisioning銘柄を対象化、
  --tickerで指定した銘柄が0件になる場合はexit(2)（黙って「ゲート通過」にしない）
- register_ticker.py::step7_5_consistency_check(): NG・対象0件で昇格を止め、
  yfinance突合が実行できなかった場合は止めずに明示する
- P2-A廃止: 急成長銘柄（ONDS型、FY期末から半年でTTM売上が3.4倍）がNGにならない

実行方法:
    python -m pytest tests/test_registration_consistency_gate.py -v
"""

import json
import os
import shutil
import subprocess
import sys
import types

import pytest

_REPO = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)
_SEC_DATA_DIR = os.path.join(_REPO, "common", "sec_data")
if _SEC_DATA_DIR not in sys.path:
    sys.path.insert(0, _SEC_DATA_DIR)

from common.registration import register_ticker as rt  # noqa: E402
from common.sec_data import report_consistency_check as rcc  # noqa: E402
import registration_validator as rv  # noqa: E402


# ── report_consistency_check.py --include-provisioning ───────────────────

def _args(**kw):
    base = dict(ticker=None, include_provisioning=False, include_yfinance_checks=False,
                quiet=True, fail_on_ng=True)
    base.update(kw)
    return types.SimpleNamespace(**base)


@pytest.fixture
def provisioning_universe(tmp_path, monkeypatch):
    """ACTIV（active）とPROVI（provisioning）の2銘柄だけの世界を作る"""
    for t in ("ACTIV", "PROVI"):
        (tmp_path / t).mkdir()
        (tmp_path / t / "report.txt").write_text("dummy", encoding="utf-8")
    monkeypatch.setattr(rcc, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(rcc._tickers_mod, "get_tanuki_tickers", lambda *a, **k: ["ACTIV"])
    monkeypatch.setattr(rcc._tickers_mod, "get_registrable_tickers", lambda *a, **k: ["ACTIV", "PROVI"])
    checked = []
    monkeypatch.setattr(rcc, "check_ticker", lambda t, *a, **k: (checked.append(t) or ([], [])))
    return checked


def test_include_provisioning_targets_provisioning_ticker(provisioning_universe):
    rcc.run_checks(_args(ticker="PROVI", include_provisioning=True))
    assert provisioning_universe == ["PROVI"]


def test_ticker_excluded_to_zero_exits_2(provisioning_universe):
    # フラグなしではprovisioning銘柄は対象外 → 0件で黙って成功せずexit(2)
    with pytest.raises(SystemExit) as e:
        rcc.run_checks(_args(ticker="PROVI"))
    assert e.value.code == 2
    assert provisioning_universe == []


def test_ticker_meaning_unchanged_for_active(provisioning_universe):
    rcc.run_checks(_args(ticker="ACTIV"))
    assert provisioning_universe == ["ACTIV"]


def test_no_ticker_filter_keeps_active_only(provisioning_universe):
    rcc.run_checks(_args())
    assert provisioning_universe == ["ACTIV"]


# ── register_ticker.py Step 7.5 ─────────────────────────────────────────

def _fake_run(returncode, stdout):
    def run(cmd, **kw):
        run.cmd = cmd
        return subprocess.CompletedProcess(cmd, returncode, stdout=stdout, stderr="")
    run.cmd = None
    return run


_OUT_OK = ("  [🆕未確認 WARN-41 revenue yfinance突合] FY2025: SEC=1 yfinance=1（乖離+0.0%）\n"
           "結果: NG=0件 / WARN=1件\n✅ ゲート通過\n")
_OUT_NO_YF = "結果: NG=0件 / WARN=0件\n✅ ゲート通過\n"


def test_step7_5_runs_consistency_check_with_expected_flags(monkeypatch):
    run = _fake_run(0, _OUT_OK)
    monkeypatch.setattr(rt.subprocess, "run", run)
    assert rt.step7_5_consistency_check("ONDS", {"tanuki": "true"}) is True
    assert run.cmd[1].endswith("report_consistency_check.py")
    assert {"--ticker", "ONDS", "--include-provisioning", "--include-yfinance-checks",
            "--fail-on-ng"} <= set(run.cmd)


@pytest.mark.parametrize("rc", [1, 2])
def test_step7_5_blocks_on_ng_or_zero_target(monkeypatch, rc):
    monkeypatch.setattr(rt.subprocess, "run", _fake_run(rc, "結果: NG=1件\n"))
    assert rt.step7_5_consistency_check("ONDS", {"tanuki": "true"}) is False


def test_step7_5_yfinance_unavailable_passes_with_notice(monkeypatch, capsys):
    monkeypatch.setattr(rt.subprocess, "run", _fake_run(0, _OUT_NO_YF))
    assert rt.step7_5_consistency_check("ONDS", {"tanuki": "true"}) is True
    assert "CHECK-41のrevenue yfinance突合が実行されませんでした" in capsys.readouterr().out


def test_step7_5_notice_absent_when_check41_ran(monkeypatch, capsys):
    # WARN行の先頭に台帳注記（🆕未確認）が入っていても実行済みと判定する
    monkeypatch.setattr(rt.subprocess, "run", _fake_run(0, _OUT_OK))
    rt.step7_5_consistency_check("ONDS", {"tanuki": "true"})
    assert "実行されませんでした" not in capsys.readouterr().out


def test_step7_5_skips_non_tanuki(monkeypatch):
    run = _fake_run(2, "")
    monkeypatch.setattr(rt.subprocess, "run", run)
    assert rt.step7_5_consistency_check("XXX", {"tanuki": "false"}) is True
    assert run.cmd is None


def test_register_one_does_not_promote_when_step7_5_fails(monkeypatch):
    calls = []
    for name in ("step1_sec_data", "step3_pipeline"):
        monkeypatch.setattr(rt, name, lambda *a, **k: True)
    for name in ("step2_beta", "step3_5_segment_config_gate", "step4_audit", "step5_hypecore",
                 "step5b_eps_analyzer", "step6_discover_register", "step7_monitor_register"):
        monkeypatch.setattr(rt, name, lambda *a, **k: None)
    monkeypatch.setattr(rt, "_load_cik_row", lambda t: {"ticker": t, "status": "provisioning",
                                                         "tanuki": "true", "stonks_silo": "false",
                                                         "eps": "false", "hypecore": "false"})
    monkeypatch.setattr(rt, "step7_5_consistency_check", lambda *a, **k: False)
    monkeypatch.setattr(rt, "step8_validate_and_promote", lambda *a, **k: calls.append("step8") or True)
    assert rt.register_one("ZZZ", "active", dry_run=False) is False
    assert calls == []


# ── P2-A廃止: 急成長銘柄（ONDS型）がNGにならない ─────────────────────────

def test_hypergrowth_not_ng_after_p2a_removal(tmp_path, monkeypatch):
    """本番のONDSのttmファイル（TTM売上174.1M）と、FY2025の年次売上50.7Mで
    旧P2-AならNG（3.4倍）になった状態を再現し、NGが出ないことを確認する"""
    ttm_dir = tmp_path / "ttm"
    tanuki_dir = tmp_path / "tanuki" / "ONDS"
    ttm_dir.mkdir()
    tanuki_dir.mkdir(parents=True)
    shutil.copy(os.path.join(_SEC_DATA_DIR, "ttm", "ONDS_ttm_series.json"), ttm_dir)
    (tanuki_dir / "latest.json").write_text(json.dumps(
        {"components": {"latest_revenue": 50_731_000, "sector": "Technology"}}), encoding="utf-8")
    monkeypatch.setattr(rv, "TTM_DIR", str(ttm_dir))
    monkeypatch.setattr(rv, "TANUKI_DIR", str(tmp_path / "tanuki"))
    monkeypatch.setattr(rv, "SEC_DATA_DIR", str(tmp_path / "sec"))
    issues = rv.Issues()
    rv.check_p2_data_quality("ONDS", issues)
    assert issues.count_ng() == 0
    assert not [c for _, c, _ in issues.all() if c.startswith("P2-A")]
