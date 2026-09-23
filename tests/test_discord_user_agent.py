"""
tests/test_discord_user_agent.py

[[DISCORD-NOTIFY-403-SILENT-1]]の回帰テスト。urllib経由のDiscord webhook
送信はUser-Agent未指定だと既定の"Python-urllib/x.y"が付き、Cloudflareに
error code 1010（HTTP 403）で拒否される。全urllib送信箇所（audit.py・
score_watcher.py・beta_fetcher.py の post_discord）がUser-Agentを明示し、
system_health.pyが「未設定」と「送信失敗」を区別して表示することを検証する。

実行方法:
    python -m pytest tests/test_discord_user_agent.py -v
"""

import os
import sys
import urllib.error
import urllib.request

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
# score_watcher.py/beta_fetcher.pyはパッケージ経由ではimportできないため、
# tests/test_beta_fetcher.pyと同じパターンで対象ディレクトリをsys.pathへ追加。
_PIPELINE_DIR = os.path.join(_REPO_ROOT, "src", "value", "tanuki_valuation")
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

import beta_fetcher  # noqa: E402
import score_watcher  # noqa: E402
from common import system_health as sh  # noqa: E402
from common.sec_data import audit  # noqa: E402

_DUMMY_WEBHOOK = "https://discord.invalid/api/webhooks/0/dummy-token"


class _FakeResp:
    status = 204

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.mark.parametrize("module", [audit, score_watcher, beta_fetcher])
def test_post_discord_sets_user_agent(module, monkeypatch):
    captured = {}

    def fake_urlopen(req, timeout=None):
        captured["req"] = req
        return _FakeResp()

    monkeypatch.setenv("DISCORD_WEB_HOOK", _DUMMY_WEBHOOK)
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert module.post_discord("test") is True
    ua = captured["req"].get_header("User-agent")
    assert ua, "User-Agent未指定だとPython-urllib既定値になりCloudflareに403で拒否される"
    assert not ua.startswith("Python-urllib")


@pytest.mark.parametrize("module", [audit, score_watcher, beta_fetcher])
def test_post_discord_http_error_prints_status_without_url(module, monkeypatch, capsys):
    def fake_urlopen(req, timeout=None):
        raise urllib.error.HTTPError(_DUMMY_WEBHOOK, 403, "Forbidden", {}, None)

    monkeypatch.setenv("DISCORD_WEB_HOOK", _DUMMY_WEBHOOK)
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert module.post_discord("test") is False
    err = capsys.readouterr().err
    assert "403" in err
    assert "dummy-token" not in err and "discord.invalid" not in err


def test_system_health_label_distinguishes_unset_and_failure(monkeypatch):
    monkeypatch.delenv("DISCORD_WEB_HOOK", raising=False)
    assert "未設定" in sh.discord_notify_label(False)

    monkeypatch.setenv("DISCORD_WEB_HOOK", _DUMMY_WEBHOOK)
    failed = sh.discord_notify_label(False)
    assert "送信失敗" in failed and "未設定" not in failed
    assert "送信完了" in sh.discord_notify_label(True)
