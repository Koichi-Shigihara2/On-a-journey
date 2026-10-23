"""
tests/test_register_ticker_preflight.py

[[QUALITY-GATES-EPIC-1]]ゲート0（[[PREFLIGHT-CHECK-1]]）対応（2026-09-05）の
回帰テスト。common/registration/register_ticker.py::register_one()が、
Step 0.5（cik_lookup.csv行のロード）直後・Step 1実行前にプリフライト
チェックを実行すること、かつフラグが立っても登録処理全体を自動停止
しない（想定機能④）ことを検証する。

ネットワークアクセス・サブプロセス実行はいずれもmonkeypatchで置き換え、
本テストファイル自体はSEC EDGARへのアクセスやSECデータ取得等の実処理を
一切行わない。

実行方法:
    python -m pytest tests/test_register_ticker_preflight.py -v
"""

import os
import sys

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.registration import register_ticker as rt  # noqa: E402
from common.registration.preflight_check import PreflightResult  # noqa: E402


def _fake_row(ticker="AAA"):
    return {
        "ticker": ticker, "status": "provisioning", "tanuki": "false",
        "stonks_silo": "false", "eps": "false", "hypecore": "false",
    }


def _patch_all_steps_success(monkeypatch, call_log):
    """Step 1〜8をすべて成功として短絡させ、呼び出し順序だけを記録する"""
    monkeypatch.setattr(rt, "step1_sec_data", lambda t: call_log.append("step1") or True)
    monkeypatch.setattr(rt, "step2_beta", lambda t, dry_run: call_log.append("step2"))
    monkeypatch.setattr(rt, "step4_audit", lambda t: call_log.append("step4"))
    monkeypatch.setattr(rt, "step7_monitor_register", lambda t, dry_run: call_log.append("step7"))
    monkeypatch.setattr(
        rt, "step8_validate_and_promote",
        lambda t, target_status, dry_run: call_log.append("step8") or True,
    )


class TestPreflightIntegration:
    def test_preflight_runs_after_row_load_before_step1(self, monkeypatch):
        call_log: list[str] = []
        monkeypatch.setattr(rt, "_load_cik_row", lambda t: _fake_row(t))

        def fake_preflight(ticker):
            call_log.append("preflight")
            return PreflightResult(ticker=ticker)

        monkeypatch.setattr(rt, "run_preflight_check", fake_preflight)
        monkeypatch.setattr(rt, "print_preflight_report", lambda result: call_log.append("preflight_report"))
        _patch_all_steps_success(monkeypatch, call_log)

        ok = rt.register_one("AAA", "candidate", dry_run=True)

        assert ok is True
        assert call_log.index("preflight") < call_log.index("step1")
        assert "preflight_report" in call_log

    def test_preflight_flags_do_not_block_registration(self, monkeypatch):
        """想定機能④: フラグが立っても登録処理（Step1以降）は
        自動停止せず続行すること"""
        call_log: list[str] = []
        monkeypatch.setattr(rt, "_load_cik_row", lambda t: _fake_row(t))

        def fake_preflight_with_flags(ticker):
            result = PreflightResult(ticker=ticker)
            result.flags.append("①上場後1.0年 → データ不安定リスクあり")
            result.flags.append("③収益系XBRLタグが1件も存在しません → 売上ゼロ企業の可能性")
            return result

        monkeypatch.setattr(rt, "run_preflight_check", fake_preflight_with_flags)
        monkeypatch.setattr(rt, "print_preflight_report", lambda result: call_log.append("preflight_report"))
        _patch_all_steps_success(monkeypatch, call_log)

        ok = rt.register_one("AAA", "candidate", dry_run=True)

        assert ok is True
        assert "step1" in call_log
        assert "step8" in call_log

    def test_missing_cik_row_returns_false_without_running_preflight(self, monkeypatch):
        """cik_lookup.csvに行が存在しない場合（Step 0.5未実施）は、
        従来通りプリフライト実行前にFalseで早期returnすること"""
        call_log: list[str] = []
        monkeypatch.setattr(rt, "_load_cik_row", lambda t: None)
        monkeypatch.setattr(
            rt, "run_preflight_check",
            lambda t: call_log.append("preflight") or PreflightResult(ticker=t),
        )

        ok = rt.register_one("ZZZZ", "candidate", dry_run=True)

        assert ok is False
        assert "preflight" not in call_log

    def test_step1_failure_still_aborts_after_preflight(self, monkeypatch):
        """プリフライト追加後も、Step1失敗時の既存の中断挙動が
        壊れていないこと"""
        call_log: list[str] = []
        monkeypatch.setattr(rt, "_load_cik_row", lambda t: _fake_row(t))
        monkeypatch.setattr(rt, "run_preflight_check", lambda t: PreflightResult(ticker=t))
        monkeypatch.setattr(rt, "print_preflight_report", lambda result: None)
        monkeypatch.setattr(rt, "step1_sec_data", lambda t: call_log.append("step1") or False)
        monkeypatch.setattr(
            rt, "step8_validate_and_promote",
            lambda t, target_status, dry_run: call_log.append("step8") or True,
        )

        ok = rt.register_one("AAA", "candidate", dry_run=True)

        assert ok is False
        assert "step1" in call_log
        assert "step8" not in call_log
