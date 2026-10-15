"""
SEC_Data_UpdateのDiscord通知（[[SEC-UPDATE-EXIT1-BLOCKS-ALL-1]]、2026-10-15）。

SEC_Data_Update.ymlの最後の段（if: always()）で実行し、結果・止まった段・失敗した銘柄と理由を送る。
各段の結果（steps.<id>.outcome）は環境変数 OUTCOME_<段> で受け取る。失敗した銘柄は
update.pyが書いた失敗の一覧（sec_update_result.json、rollback_failed.default_result_path()）から読む。
送信はaudit.post_discord()（DISCORD_WEB_HOOK。User-Agentの対策済み）。標準ライブラリだけで動く
（依存パッケージの導入の段が失敗した日にも通知するため）。
終了コードは常に0（通知の失敗でワークフローの結果を変えない）。
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data.rollback_failed import default_result_path  # noqa: E402

# SEC_Data_Update.ymlの段の順（環境変数名, 表示名）
STEPS = [
    ("OUTCOME_INSTALL", "依存パッケージの導入"),
    ("OUTCOME_UPDATE", "update.py（SECデータの取得・計算）"),
    ("OUTCOME_ROLLBACK", "失敗した銘柄を前回の状態に戻す"),
    ("OUTCOME_GATE", "Consistency Check Gate"),
    ("OUTCOME_SYNC", "normalizedのdocs/への同期"),
    ("OUTCOME_COMMIT", "commit・push"),
]
MAX_LEN = 1900  # Discordの本文の上限（2000字）より少し短く


def load_result(path: str | None = None) -> dict | None:
    try:
        with open(path or default_result_path(), encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def build_message(outcomes: dict[str, str], result: dict | None, run_url: str = "",
                  now: datetime | None = None) -> str:
    now = now or datetime.now(timezone(timedelta(hours=9)))
    stopped = next(((name, outcomes.get(key)) for key, name in STEPS
                    if outcomes.get(key) in ("failure", "cancelled")), None)
    failed = (result or {}).get("failed") or []
    if stopped:
        head = "❌ **SEC Data Update: 失敗（commitしていない。日曜の連鎖は動かない）**"
    elif failed:
        head = "⚠ **SEC Data Update: 一部の銘柄が失敗（前回の状態に戻し、他の銘柄はcommit済み）**"
    else:
        head = "✅ **SEC Data Update: 成功**"
    lines = [f"{head} `{now.strftime('%Y-%m-%d %H:%M')} JST`"]
    if stopped:
        lines.append(f"止まった段: {stopped[0]}（{stopped[1]}）")
    if result:
        lines.append(f"対象 {result.get('targets')}・成功 {result.get('succeeded')}・失敗 {len(failed)}"
                     f"（{int((result.get('failed_exit_ratio') or 0) * 100)}%を超えると終了コード1）")
        if result.get("aborted"):
            lines.append(f"銘柄のループの外の例外: {result['aborted'][:300]}")
    elif outcomes.get("OUTCOME_UPDATE") not in (None, "", "skipped"):
        lines.append("失敗の一覧（sec_update_result.json）が無い")
    if failed:
        lines.append("失敗した銘柄:")
        for f in failed:
            lines.append(f"　`{f.get('ticker')}` {f.get('stage')}: {str(f.get('reason'))[:200]}")
    if run_url:
        lines.append(run_url)
    text = "\n".join(lines)
    if len(text) > MAX_LEN:
        text = text[:MAX_LEN - 20] + "\n…（以下省略）"
        if run_url:
            text += "\n" + run_url
    return text


def main() -> int:
    outcomes = {key: os.environ.get(key, "") for key, _ in STEPS}
    message = build_message(outcomes, load_result(), os.environ.get("RUN_URL", ""))
    print(message)
    try:
        from common.sec_data.audit import post_discord
        sent = post_discord(message)
    except Exception as e:  # 通知の失敗でワークフローの結果を変えない
        print(f"Discord送信エラー: {type(e).__name__}", file=sys.stderr)
        sent = False
    print("Discord通知: " + ("送信完了" if sent else "送れなかった（DISCORD_WEB_HOOK未設定または送信失敗）"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
