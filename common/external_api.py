"""
common/external_api.py

外部APIの呼び出し口（Grok・SEC・FRED。[[EXTERNAL-API-CLIENT-UNIFY-1]]、2026-10-24）。
そろえるのは通信の部分だけ: 宛先・タイムアウト・再試行（429・5xx・通信の失敗は待って再試行。回数と待ち時間は下の1つの設定）・
失敗の戻り値の形・ログ。プロンプト・モデル・温度・応答の読み方は呼び出し側に残す。

  - Grok（xAI）: grok_request(payload) が応答のJSONを返す。失敗は ExternalApiError。トークンの使用量をログに出す。
    Grokが使えないときに画面・データに出す代わりの文は GROK_UNAVAILABLE_TEXT の1つ
  - SEC（EDGAR）: sec_get(url) が応答を返す（404等はそのまま。429・5xxは再試行し、それでもだめなら最後の応答）。
    User-Agent は環境変数 SEC_USER_AGENT_EMAIL から作り、無ければ止める（架空のアドレスで続けない）。毎秒10件の制限を守る
  - FRED: fred_get(path, params) がREST APIの応答のJSONを返す。fred_client() は fredapi の Fred（系列の取得、マクロデータ層）

失敗の形: 通信が最後まで失敗したら ExternalApiError（requests.exceptions.RequestException の仲間なので、
これまでの `except requests.exceptions.RequestException` / `except Exception` で受けられる）。
"""

import os
import time
from typing import Any, Dict, Optional

import requests

# ── 1つの設定 ────────────────────────────────────────────────────
RETRY_ATTEMPTS = 3                       # 1回目＋再試行2回
RETRY_WAITS_SEC = (2, 5)                 # 再試行の前に待つ秒数（429・5xx・通信の失敗）
TIMEOUT_SEC = {"grok": 120, "sec": 120, "fred": 30}   # 2026-10-24 までの呼び出しごとの値の最大
GROK_URL = "https://api.x.ai/v1/chat/completions"
SEC_MIN_INTERVAL_SEC = 0.11              # SECは毎秒10件まで
FRED_URL = "https://api.stlouisfed.org/fred/"
GROK_UNAVAILABLE_TEXT = "AI（Grok）の解説は取得できませんでした"
SEC_USER_AGENT_ENV = "SEC_USER_AGENT_EMAIL"
FRED_API_KEY_ENV = "FRED_API_KEY"


class ExternalApiError(requests.exceptions.RequestException):
    """外部APIの呼び出しが最後まで失敗した（通信の失敗、再試行しても429・5xx、Grokの2xx以外）。"""


def _retryable(status: int) -> bool:
    return status == 429 or status >= 500


def _request(kind: str, method: str, url: str, label: str, **kw) -> requests.Response:
    """再試行つきの1回の呼び出し。429・5xxで尽きたら最後の応答を返し、通信の失敗で尽きたら ExternalApiError。"""
    last_resp: Optional[requests.Response] = None
    last_err = ""
    for i in range(RETRY_ATTEMPTS):
        try:
            send = requests.post if method == "POST" else requests.get
            resp = send(url, timeout=TIMEOUT_SEC[kind], **kw)
        except requests.exceptions.RequestException as e:
            last_resp, last_err = None, f"{type(e).__name__}: {e}"
        else:
            if not _retryable(resp.status_code):
                return resp
            last_resp, last_err = resp, f"HTTP {resp.status_code}"
        if i < RETRY_ATTEMPTS - 1:
            wait = RETRY_WAITS_SEC[min(i, len(RETRY_WAITS_SEC) - 1)]
            print(f"  [{kind}] {label}: {last_err} → {wait}秒待って再試行（{i + 2}/{RETRY_ATTEMPTS}）")
            time.sleep(wait)
    print(f"  [{kind}] {label}: {RETRY_ATTEMPTS}回とも失敗（{last_err}）")
    if last_resp is not None:
        return last_resp
    raise ExternalApiError(f"{label}: {last_err}")


# ── Grok ─────────────────────────────────────────────────────────
def grok_request(payload: Dict[str, Any], label: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """Grok（xAI の chat completions）に payload をそのまま送り、応答のJSONを返す。2xx以外・通信の失敗は ExternalApiError。"""
    key = api_key if api_key is not None else os.environ.get("XAI_API_KEY", "")
    if not key:
        raise ExternalApiError(f"{label}: XAI_API_KEY が無い")
    resp = _request("grok", "POST", GROK_URL, f"Grok {label}",
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"}, json=payload)
    if not 200 <= resp.status_code < 300:
        raise ExternalApiError(f"{label}: HTTP {resp.status_code} {resp.text[:200]}")
    data = resp.json()
    u = data.get("usage") or {}
    print(f"  [grok] {label}: model={payload.get('model')} prompt_tokens={u.get('prompt_tokens')} "
          f"completion_tokens={u.get('completion_tokens')} total_tokens={u.get('total_tokens')}")
    return data


def grok_text(messages: list, model: str, label: str, api_key: Optional[str] = None, **params) -> str:
    """messages・model・その他の引数（temperature・max_tokens 等）で Grok を呼び、最初の答えの文字を返す。失敗は ExternalApiError。"""
    data = grok_request({"model": model, "messages": messages, **params}, label, api_key=api_key)
    return data["choices"][0]["message"]["content"]


def grok_messages(user_prompt: str, system_prompt: str = "") -> list:
    """system（あれば）と user の2つの発言。"""
    return ([{"role": "system", "content": system_prompt}] if system_prompt else []) + [{"role": "user", "content": user_prompt}]


# ── SEC ──────────────────────────────────────────────────────────
_SEC_LAST_AT = [0.0]


def sec_user_agent() -> str:
    """SECに名乗る User-Agent（"On-a-journey <連絡先のメール>"）。環境変数 SEC_USER_AGENT_EMAIL が無ければ止める。"""
    email = os.environ.get(SEC_USER_AGENT_ENV, "").strip()
    if not email:
        raise SystemExit(f"[ERROR] 環境変数 {SEC_USER_AGENT_ENV} が無い。SECは連絡先のメールを User-Agent に求める"
                         "（GitHub Actions は secrets.SEC_USER_AGENT_EMAIL、手元は CLAUDE_CODE_START.md の手順）")
    return f"On-a-journey {email}"


def sec_get(url: str, label: str = "", params: Optional[dict] = None) -> requests.Response:
    """SEC（www.sec.gov・data.sec.gov）から取る。毎秒10件の制限を守る。404等はそのまま返し、呼び出し側が読む。"""
    headers = {"User-Agent": sec_user_agent(), "Accept-Encoding": "gzip, deflate"}
    wait = SEC_MIN_INTERVAL_SEC - (time.monotonic() - _SEC_LAST_AT[0])
    if wait > 0:
        time.sleep(wait)
    try:
        return _request("sec", "GET", url, f"SEC {label or url}", headers=headers, params=params)
    finally:
        _SEC_LAST_AT[0] = time.monotonic()


# ── FRED ─────────────────────────────────────────────────────────
_FRED_CLIENT = [None]


def _fred_key(api_key: Optional[str] = None) -> str:
    key = api_key if api_key else os.environ.get(FRED_API_KEY_ENV, "")
    if not key:
        raise RuntimeError(f"{FRED_API_KEY_ENV} not set.")
    return key


def fred_get(path: str, params: Optional[dict] = None, api_key: Optional[str] = None) -> Dict[str, Any]:
    """FREDのREST API（https://api.stlouisfed.org/fred/<path>）の応答のJSON。2xx以外・通信の失敗は ExternalApiError。"""
    q = dict(params or {})
    q.update(api_key=_fred_key(api_key), file_type="json")
    resp = _request("fred", "GET", FRED_URL + path, f"FRED {path}", params=q)
    if not 200 <= resp.status_code < 300:
        raise ExternalApiError(f"FRED {path}: HTTP {resp.status_code}")
    return resp.json()


def fred_client():
    """fredapi の Fred（プロセスで1つ。系列の値の取得、common/macro_data/fetcher.py）。キーが無ければ RuntimeError。"""
    if _FRED_CLIENT[0] is None:
        from fredapi import Fred
        _FRED_CLIENT[0] = Fred(api_key=_fred_key())
    return _FRED_CLIENT[0]
