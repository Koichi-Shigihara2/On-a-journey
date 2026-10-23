"""common/external_api.py（外部APIの呼び出し口、[[EXTERNAL-API-CLIENT-UNIFY-1]]、2026-10-24）。"""
import pytest
import requests

from common import external_api as ea


class _Resp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or {}
        self.text = str(payload)

    def json(self):
        return self._payload


@pytest.fixture
def no_wait(monkeypatch):
    waits = []
    monkeypatch.setattr(ea.time, "sleep", waits.append)
    return waits


def test_sec_retries_429_and_5xx_then_returns_success(monkeypatch, no_wait):
    seq = [_Resp(429), _Resp(503), _Resp(200, {"ok": 1})]
    sent = []

    def fake_get(url, timeout=None, headers=None, params=None):
        sent.append((timeout, headers["User-Agent"]))
        return seq.pop(0)

    monkeypatch.setenv("SEC_USER_AGENT_EMAIL", "me@example.invalid")
    monkeypatch.setattr(ea.requests, "get", fake_get)
    r = ea.sec_get("https://data.sec.gov/x.json", "test")
    assert r.status_code == 200
    assert sent == [(120, "On-a-journey me@example.invalid")] * 3
    assert [w for w in no_wait if w >= 1] == list(ea.RETRY_WAITS_SEC)


def test_sec_404_is_returned_without_retry(monkeypatch, no_wait):
    calls = []
    monkeypatch.setattr(ea.requests, "get", lambda url, **kw: calls.append(url) or _Resp(404))
    assert ea.sec_get("https://www.sec.gov/missing", "test").status_code == 404
    assert len(calls) == 1


def test_sec_network_failure_raises_request_exception(monkeypatch, no_wait):
    def boom(url, **kw):
        raise requests.exceptions.ConnectionError("down")

    monkeypatch.setattr(ea.requests, "get", boom)
    with pytest.raises(requests.exceptions.RequestException):
        ea.sec_get("https://www.sec.gov/x", "test")


def test_sec_without_user_agent_env_stops(monkeypatch):
    monkeypatch.delenv("SEC_USER_AGENT_EMAIL", raising=False)
    monkeypatch.setattr(ea.requests, "get", lambda *a, **k: pytest.fail("SECを呼んではいけない"))
    with pytest.raises(SystemExit):
        ea.sec_get("https://www.sec.gov/x", "test")


def test_grok_non_2xx_raises_and_success_returns_json(monkeypatch, no_wait):
    monkeypatch.setattr(ea.requests, "post", lambda url, **kw: _Resp(400, {"error": "bad"}))
    with pytest.raises(ea.ExternalApiError):
        ea.grok_request({"model": "grok-4.3"}, "test", api_key="k")
    body = {"choices": [{"message": {"content": "hi"}}], "usage": {"total_tokens": 3}}
    monkeypatch.setattr(ea.requests, "post", lambda url, **kw: _Resp(200, body))
    assert ea.grok_text([{"role": "user", "content": "x"}], "grok-4.3", "test", api_key="k") == "hi"


def test_grok_without_key_raises_without_calling(monkeypatch):
    monkeypatch.delenv("XAI_API_KEY", raising=False)
    monkeypatch.setattr(ea.requests, "post", lambda *a, **k: pytest.fail("Grokを呼んではいけない"))
    with pytest.raises(ea.ExternalApiError):
        ea.grok_request({"model": "grok-4.3"}, "test")


def test_fred_get_adds_key_and_file_type(monkeypatch, no_wait):
    sent = {}

    def fake_get(url, timeout=None, params=None):
        sent.update(url=url, timeout=timeout, params=params)
        return _Resp(200, {"release_dates": []})

    monkeypatch.setattr(ea.requests, "get", fake_get)
    assert ea.fred_get("releases/dates", {"limit": 1}, api_key="k") == {"release_dates": []}
    assert sent == {"url": "https://api.stlouisfed.org/fred/releases/dates", "timeout": 30,
                    "params": {"limit": 1, "api_key": "k", "file_type": "json"}}
