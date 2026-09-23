"""
src/value/tanuki_valuation/score_watcher.py
TANUKI SCORE 変化検知スクリプト（ACTION-10）

TANUKI VALUATIONパイプライン実行後に呼び出す。
前回通知時との差分を比較し、仮説検証に影響する変化のみ Discord へ通知する。

検知対象:
  - tanuki_score の変化（BUY / HOLD / TRIM / SELL / PASS）
  - upside_percent の ±10pt 以上の変化
  - funda_score の ±10pt 以上の変化
  - HypeCore stage の変化（poc.json の最新月）

状態管理:
  docs/value-monitor/tanuki_valuation/data/.watcher_state.json
  に「前回通知時の状態」を保存し、重複通知を防ぐ。

使用方法:
    python score_watcher.py
    python score_watcher.py NVDA PLTR   # 特定銘柄のみ
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path


REPO_ROOT   = Path(__file__).resolve().parents[3]
DATA_DIR    = REPO_ROOT / "docs" / "value-monitor" / "tanuki_valuation" / "data"
HYPE_DIR    = REPO_ROOT / "docs" / "value-monitor" / "hypecore" / "data"
STATE_FILE  = DATA_DIR / ".watcher_state.json"

UPSIDE_THRESHOLD = 10.0   # 乖離率変化の通知閾値（pt）
FUNDA_THRESHOLD  = 10     # ファンダスコア変化の通知閾値（pt）

JST = timezone(timedelta(hours=9))


def get_tickers(targets: list[str]) -> list[str]:
    if targets:
        return [t.upper() for t in targets]
    path = DATA_DIR / "tickers.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8")).get("tickers", [])


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_latest(ticker: str) -> dict:
    path = DATA_DIR / ticker / "latest.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_hype_stage(ticker: str) -> tuple[int | None, str]:
    """最新月の (stage, stage_label) を返す"""
    path = HYPE_DIR / f"{ticker}_poc.json"
    if not path.exists():
        return None, ""
    monthly = json.loads(path.read_text(encoding="utf-8")).get("monthly", [])
    if not monthly:
        return None, ""
    last = monthly[-1]
    return last.get("stage"), last.get("stage_label", "")


def detect_changes(ticker: str, prev_state: dict) -> tuple[list[dict], dict]:
    """
    前回通知状態との差分を検知する。
    戻り値: (変化リスト, 今回の状態dict)
    """
    latest = load_latest(ticker)
    if not latest:
        return [], prev_state

    cur_score  = latest.get("tanuki_score")
    cur_funda  = latest.get("funda_score")
    cur_upside = latest.get("upside_percent")
    cur_stage, cur_label = load_hype_stage(ticker)

    new_state = {
        "tanuki_score": cur_score,
        "funda_score":  cur_funda,
        "upside_percent": cur_upside,
        "hype_stage":   cur_stage,
        "hype_label":   cur_label,
        "checked_at":   datetime.now(JST).strftime("%Y-%m-%d"),
    }

    # 前回通知状態がなければ現状を初期値として記録（初回は通知しない）
    if not prev_state:
        return [], new_state

    changes = []

    # ── 1. tanuki_score 変化 ──
    prev_score = prev_state.get("tanuki_score")
    if cur_score and prev_score and cur_score != prev_score:
        changes.append({
            "type":    "score",
            "label":   "判定変化",
            "detail":  f"{prev_score} → **{cur_score}**",
            "comment": latest.get("score_comment", ""),
        })

    # ── 2. upside_percent 変化 ──
    prev_upside = prev_state.get("upside_percent")
    if cur_upside is not None and prev_upside is not None:
        delta = cur_upside - prev_upside
        if abs(delta) >= UPSIDE_THRESHOLD:
            arrow = "▲" if delta > 0 else "▼"
            changes.append({
                "type":   "upside",
                "label":  "乖離率 大幅変化",
                "detail": f"{prev_upside:+.1f}% → {cur_upside:+.1f}% ({arrow}{abs(delta):.1f}pt)",
            })

    # ── 3. funda_score 変化 ──
    prev_funda = prev_state.get("funda_score")
    if cur_funda is not None and prev_funda is not None:
        delta = cur_funda - prev_funda
        if abs(delta) >= FUNDA_THRESHOLD:
            arrow = "▲" if delta > 0 else "▼"
            changes.append({
                "type":   "funda",
                "label":  "ファンダスコア変化",
                "detail": f"{prev_funda} → {cur_funda} ({arrow}{abs(delta)}pt)",
            })

    # ── 4. HypeCore stage 変化 ──
    prev_stage = prev_state.get("hype_stage")
    prev_label = prev_state.get("hype_label", "")
    if cur_stage is not None and prev_stage is not None and cur_stage != prev_stage:
        changes.append({
            "type":   "hype",
            "label":  "HypeCoreフェーズ転換",
            "detail": f"Phase{prev_stage}({prev_label}) → **Phase{cur_stage}({cur_label})**",
        })

    return changes, new_state


def build_discord_message(ticker_changes: dict[str, list[dict]]) -> str:
    if not ticker_changes:
        return ""

    total = sum(len(v) for v in ticker_changes.values())
    now   = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
    lines = [
        f"📊 **TANUKI SCORE 変化検知** `{now}` — {len(ticker_changes)}銘柄 / {total}件",
        "",
    ]

    priority = {"score": 0, "hype": 1, "upside": 2, "funda": 3}
    for ticker, changes in sorted(ticker_changes.items()):
        sorted_changes = sorted(changes, key=lambda c: priority.get(c["type"], 9))
        lines.append(f"**{ticker}**")
        for c in sorted_changes:
            comment = f"　*{c['comment']}*" if c.get("comment") else ""
            lines.append(f"　{c['label']}: {c['detail']}{comment}")
        lines.append("")

    lines.append("*長期投資仮説の検証に影響する変化のみ通知しています*")
    return "\n".join(lines)


def post_discord(message: str) -> bool:
    webhook = os.environ.get("DISCORD_WEB_HOOK", "")
    if not webhook or not message:
        return False
    try:
        import urllib.error
        import urllib.request
        payload = json.dumps({"content": message}).encode("utf-8")
        # [[DISCORD-NOTIFY-403-SILENT-1]]: User-Agent未指定だとurllib既定の
        # "Python-urllib/x.y"が付き、Discord前段のCloudflareにerror code 1010
        # （HTTP 403）で拒否され通知が一度も届いていなかった。
        req = urllib.request.Request(
            webhook,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "On-a-journey-notifier/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status in (200, 204)
    except urllib.error.HTTPError as e:
        # webhook URL（トークンを含む）は出力しない
        print(f"Discord送信エラー: HTTP {e.code}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Discord送信エラー: {type(e).__name__}", file=sys.stderr)
        return False


def main():
    targets = sys.argv[1:]
    tickers = get_tickers(targets)
    if not tickers:
        print("監視対象銘柄が見つかりません")
        sys.exit(1)

    print(f"=== TANUKI SCORE 変化検知 ({len(tickers)}銘柄) ===")

    state = load_state()
    new_state = {}
    ticker_changes: dict[str, list[dict]] = {}

    for ticker in tickers:
        prev = state.get(ticker, {})
        changes, cur_state = detect_changes(ticker, prev)
        new_state[ticker] = cur_state
        if changes:
            ticker_changes[ticker] = changes
            for c in changes:
                print(f"  {ticker}: [{c['type']}] {c['label']} — {c['detail']}")

    # 変化があった銘柄の state のみ更新（変化なし銘柄も new_state に含めて上書き）
    state.update(new_state)
    save_state(state)

    if not ticker_changes:
        print("変化なし")
        return

    print(f"\n変化あり: {len(ticker_changes)}銘柄")
    message = build_discord_message(ticker_changes)
    if post_discord(message):
        print("Discord通知: 送信完了")
    elif not os.environ.get("DISCORD_WEB_HOOK", ""):
        print("Discord通知: スキップ（DISCORD_WEB_HOOK未設定）")
    else:
        print("Discord通知: ❌ 送信失敗（HTTPステータスはstderr参照）")

    # GitHub Actions サマリー
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("\n## TANUKI SCORE 変化検知\n\n")
            for ticker, changes in ticker_changes.items():
                f.write(f"**{ticker}**\n")
                for c in changes:
                    f.write(f"- {c['label']}: {c['detail']}\n")
                f.write("\n")


if __name__ == "__main__":
    main()
