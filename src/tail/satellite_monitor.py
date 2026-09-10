#!/usr/bin/env python3
"""
TANUKI TAIL Satellite Monitor
satelliteポジションの変化を監視してDiscordに通知する。

監視条件:
  ① 現在価格がエントリー価格から±20%以上変動
  ② エグジット条件の数値基準充足
  ③ テーゼ否定ニュース（Grok Web検索）
  ④ 決算接近（2週間前）

使用方法:
  python src/tail/satellite_monitor.py --dry-run    # 通知なし・判定結果のみ出力
  python src/tail/satellite_monitor.py              # 実通知
"""

import os
import sys
import json
import re
import argparse
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple

import requests

_THIS_DIR  = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR   = os.path.dirname(_THIS_DIR)
_REPO_ROOT = os.path.dirname(_SRC_DIR)

DATA_DIR      = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data")
POSITIONS_DIR = os.path.join(DATA_DIR, "positions")
VALUATION_DIR = os.path.join(_REPO_ROOT, "docs", "value-monitor", "tanuki_valuation", "data")
PORTFOLIO_PATH = os.path.join(_REPO_ROOT, "docs", "portfolio", "data", "portfolio.json")
RSS_STATE_PATH = os.path.join(DATA_DIR, "rss_state.json")
ALERTS_PATH   = os.path.join(DATA_DIR, "satellite_alerts.json")
JOURNAL_PATH  = os.path.join(DATA_DIR, "journal.json")

JST = timezone(timedelta(hours=9))

XAI_API_KEY = os.getenv("XAI_API_KEY", "")
GROK_URL    = "https://api.x.ai/v1/chat/completions"

# [[GROK-MODEL-PRICE-1]]対応: grok-3-mini/grok-3は実際にはgrok-4.3へ
# 自動ルーティングされ、grok-2-1212は廃止済み（API側でModel not found）。
# 実態に合わせ、実際に応答する現行モデル名を明示する（フォールバック
# ループ構造自体は一時的なネットワーク障害時の再試行として維持）。
GROK_MODELS            = ["grok-4.3", "grok-4.3", "grok-4.3"]
PRICE_CHANGE_THRESHOLD = 20.0    # ±20%
EARNINGS_WARN_DAYS     = 14      # 2週間前
EARNINGS_CYCLE_DAYS    = 90      # 前回決算から次回予測サイクル
ALERT_COOLDOWN_HOURS   = 24      # 同一アラート再通知インターバル


# ── データ読み込み ──────────────────────────────────────────────

def _load_satellite_positions() -> List[Dict[str, Any]]:
    idx_path = os.path.join(DATA_DIR, "positions_index.json")
    if not os.path.exists(idx_path):
        return []
    with open(idx_path, encoding="utf-8") as f:
        idx = json.load(f)
    result = []
    for fname in idx.get("positions", []):
        path = os.path.join(POSITIONS_DIR, fname)
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as f:
            p = json.load(f)
        if p.get("type") == "satellite" and p.get("status") != "archived":
            result.append(p)
    return result


def _load_portfolio_avg_costs() -> Dict[str, float]:
    """portfolio.json から加重平均取得単価を計算して返す"""
    if not os.path.exists(PORTFOLIO_PATH):
        return {}
    with open(PORTFOLIO_PATH, encoding="utf-8") as f:
        pf = json.load(f)
    totals: Dict[str, Dict[str, float]] = {}
    for broker in pf.get("brokers", {}).values():
        for ticker, pos in broker.get("positions", {}).items():
            avg_cost = pos.get("avg_cost") or 0
            shares   = pos.get("shares") or 0
            if avg_cost <= 0 or shares <= 0:
                continue
            if ticker not in totals:
                totals[ticker] = {"cost": 0.0, "shares": 0.0}
            totals[ticker]["cost"]   += avg_cost * shares
            totals[ticker]["shares"] += shares
    return {
        t: round(d["cost"] / d["shares"], 4)
        for t, d in totals.items()
        if d["shares"] > 0
    }


def _load_current_price(ticker: str) -> Optional[float]:
    path = os.path.join(VALUATION_DIR, ticker, "latest.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    return d.get("components", {}).get("current_price")


def _load_rss_state() -> Dict[str, Any]:
    if not os.path.exists(RSS_STATE_PATH):
        return {}
    with open(RSS_STATE_PATH, encoding="utf-8") as f:
        return json.load(f)


def _load_alerts() -> Dict[str, Any]:
    if not os.path.exists(ALERTS_PATH):
        return {}
    with open(ALERTS_PATH, encoding="utf-8") as f:
        return json.load(f)


def _save_alerts(alerts: Dict[str, Any]) -> None:
    with open(ALERTS_PATH, "w", encoding="utf-8") as f:
        json.dump(alerts, f, ensure_ascii=False, indent=2)


def _load_journal() -> Dict[str, Any]:
    if not os.path.exists(JOURNAL_PATH):
        return {"entries": []}
    with open(JOURNAL_PATH, encoding="utf-8") as f:
        return json.load(f)


def _save_journal(journal: Dict[str, Any]) -> None:
    with open(JOURNAL_PATH, "w", encoding="utf-8") as f:
        json.dump(journal, f, ensure_ascii=False, indent=2)


def _append_journal_watchlist(
    ticker: str,
    reason: str,
    tags: List[str],
    dry_run: bool = False,
) -> None:
    """satellite_monitorのアラートをjournal.jsonにwatchlistエントリとして追記"""
    now_jst = datetime.now(JST)
    date_str = now_jst.strftime("%Y-%m-%d")
    journal = _load_journal()
    entries = journal.get("entries", [])

    # IDの重複を避けるために連番を決定
    existing_ids = {e.get("id", "") for e in entries}
    seq = 1
    while f"{date_str}-{ticker}-{seq:03d}" in existing_ids:
        seq += 1
    entry_id = f"{date_str}-{ticker}-{seq:03d}"

    entry: Dict[str, Any] = {
        "id": entry_id,
        "timestamp": now_jst.isoformat(),
        "ticker": ticker,
        "type": "watchlist",
        "action": "satellite_alert",
        "reason": reason,
        "thesis_version": None,
        "health_score_at_action": None,
        "macro_score_at_action": None,
        "tags": tags,
    }
    if not dry_run:
        entries.append(entry)
        journal["entries"] = entries
        _save_journal(journal)
        print(f"  [Journal] 追記: {entry_id} ({ticker} watchlist)")
    else:
        print(f"  [DRY-RUN] Journal追記予定: {entry_id} ({ticker} watchlist) — {reason[:60]}")


# ── アラート重複制御 ─────────────────────────────────────────────

def _alert_key(ticker: str, condition: str) -> str:
    return f"{ticker}:{condition}"


def _is_cooldown(alerts: Dict[str, Any], ticker: str, condition: str) -> bool:
    key = _alert_key(ticker, condition)
    last_str = alerts.get(key)
    if not last_str:
        return False
    try:
        last_dt = datetime.fromisoformat(last_str)
        now = datetime.now(JST)
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=JST)
        return (now - last_dt).total_seconds() < ALERT_COOLDOWN_HOURS * 3600
    except Exception:
        return False


def _record_alert(alerts: Dict[str, Any], ticker: str, condition: str) -> None:
    key = _alert_key(ticker, condition)
    alerts[key] = datetime.now(JST).isoformat()


# ── Discord通知 ──────────────────────────────────────────────────

def send_discord(message: str, dry_run: bool = False) -> bool:
    if dry_run:
        print(f"  [DRY-RUN] Discord: {message[:120]}")
        return True
    webhook = os.environ.get("DISCORD_WEBHOOK", "")
    if not webhook:
        print(f"  Discord スキップ (DISCORD_WEBHOOK 未設定): {message[:80]}")
        return False
    try:
        r = requests.post(webhook, json={"content": message}, timeout=10)
        ok = r.status_code in (200, 204)
        print(f"  Discord {'送信OK' if ok else f'失敗 HTTP {r.status_code}'}")
        return ok
    except Exception as e:
        print(f"  Discord 送信エラー: {e}")
        return False


# ── Grok API ─────────────────────────────────────────────────────

def _call_grok_news(ticker: str, strategy_name: str, exit_condition: str) -> Dict[str, Any]:
    """Grok でテーゼ否定ニュースを検索する"""
    if not XAI_API_KEY:
        print("  [WARN] XAI_API_KEY 未設定 → ニュース検索スキップ")
        return {"alert": False}

    system_prompt = (
        "あなたは投資リスク監視AIです。"
        "以下の銘柄のテーゼを否定する重大ニュースがあればJSONで報告してください。"
        "なければ {\"alert\": false} のみを返してください。"
        "余計な説明文・前置き不要。JSONのみ回答。"
    )
    user_prompt = (
        f"## 銘柄: {ticker}\n"
        f"## 戦略名: {strategy_name}\n"
        f"## エグジット条件: {exit_condition}\n\n"
        "直近2週間のニュースを確認して、"
        "エグジット条件に該当する事象が発生していないか確認してください。\n\n"
        "出力形式（JSONのみ）:\n"
        '{"alert": true, "news_title": "...", "news_url": "...", "relevance": "エグジット条件との関連性"}\n'
        "または\n"
        '{"alert": false}'
    )

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {XAI_API_KEY}",
    }
    base_messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_prompt},
    ]

    print(f"  [Grok] ニュース検索: {ticker}")
    for model in GROK_MODELS:
        # まず live search 付きで試行、失敗したら search なしで再試行
        for use_search in (True, False):
            payload: Dict[str, Any] = {
                "model":       model,
                "messages":    base_messages,
                "max_tokens":  500,
                "temperature": 0.1,
            }
            if use_search:
                payload["search_parameters"] = {"mode": "auto", "recency_filter": "2w"}
            try:
                resp = requests.post(GROK_URL, headers=headers, json=payload, timeout=60)
                resp.raise_for_status()
                text = resp.json()["choices"][0]["message"]["content"].strip()
                m = re.search(r'\{.*\}', text, re.DOTALL)
                result = json.loads(m.group()) if m else {"alert": False}
                print(f"  [Grok] ニュース検索成功: {model}{' (search)' if use_search else ''}")
                return result
            except Exception as e:
                status = getattr(getattr(e, 'response', None), 'status_code', None)
                # 410 Gone は search_parameters 非サポート → search なしで再試行
                if use_search and status == 410:
                    continue
                print(f"  [Grok] 失敗 ({model}{'+search' if use_search else ''}): {e}")
                if not use_search:
                    break  # このモデルは完全スキップ

    print(f"  [Grok] ニュース検索: 全モデル失敗")
    return {"alert": False}


# ── 条件判定 ──────────────────────────────────────────────────────

def _check_price_change(
    ticker: str,
    current_price: float,
    entry_price: float,
) -> Tuple[bool, float]:
    """条件①: 価格変動±20%以上"""
    change_pct = (current_price - entry_price) / entry_price * 100
    triggered = abs(change_pct) >= PRICE_CHANGE_THRESHOLD
    return triggered, round(change_pct, 2)


def _extract_numeric_exit(exit_condition: str, entry_price: Optional[float]) -> Optional[float]:
    """
    exit_conditionから数値目標を抽出する。
    「簿価の倍」→ entry_price × 2
    「$120到達」→ 120
    抽出不可は None
    """
    if not exit_condition:
        return None

    # 「倍」「2倍」「x2」 → entry_price * 2
    if re.search(r'(\d+倍|倍になったら|倍に)', exit_condition):
        m = re.search(r'(\d+)倍', exit_condition)
        multiplier = int(m.group(1)) if m else 2
        if entry_price:
            return round(entry_price * multiplier, 2)

    # 「$XXX到達」「XXXドル」
    m = re.search(r'\$\s*(\d+(?:\.\d+)?)', exit_condition)
    if m:
        return float(m.group(1))

    m = re.search(r'(\d+(?:\.\d+)?)\s*ドル', exit_condition)
    if m:
        return float(m.group(1))

    return None


def _check_exit_numeric(
    ticker: str,
    current_price: float,
    exit_condition: str,
    entry_price: Optional[float],
) -> Tuple[bool, Optional[float]]:
    """条件②: エグジット条件の数値充足"""
    target = _extract_numeric_exit(exit_condition, entry_price)
    if target is None:
        return False, None
    triggered = current_price >= target
    return triggered, target


def _check_earnings_approach(ticker: str, rss_state: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[int]]:
    """条件④: 決算接近（last_filed + 90日が2週間以内）"""
    ts = rss_state.get(ticker)
    if not ts:
        return False, None, None
    last_filed_str = ts.get("last_filed")
    if not last_filed_str:
        return False, None, None
    try:
        last_filed = datetime.fromisoformat(last_filed_str[:10])
        estimated = last_filed + timedelta(days=EARNINGS_CYCLE_DAYS)
        now = datetime.now(JST).replace(tzinfo=None)
        days_until = (estimated - now).days
        triggered = 0 <= days_until <= EARNINGS_WARN_DAYS
        return triggered, estimated.strftime("%Y-%m-%d"), days_until
    except Exception:
        return False, None, None


# ── メイン処理 ───────────────────────────────────────────────────

def monitor_ticker(
    pos: Dict[str, Any],
    avg_costs: Dict[str, float],
    rss_state: Dict[str, Any],
    alerts: Dict[str, Any],
    dry_run: bool = False,
    skip_news: bool = False,
) -> List[str]:
    """1銘柄を監視して発生した通知メッセージリストを返す。アラート発生時はjournalにも追記する。"""
    ticker        = pos["ticker"]
    exit_cond     = pos.get("exit_condition", "（条件未設定）")
    strategy_name = pos.get("strategy_name", "")

    # エントリー価格: thesis > portfolio加重平均
    entry_price = pos.get("entry_price") or avg_costs.get(ticker)
    current_price = _load_current_price(ticker)

    print(f"\n  {ticker}:")
    if current_price is None:
        print(f"    [SKIP] latest.json なし")
        return []
    if entry_price is None:
        print(f"    [SKIP] entry_price 不明")
        return []

    change_pct = round((current_price - entry_price) / entry_price * 100, 2)
    print(f"    現在: ${current_price:.2f} / エントリー: ${entry_price:.2f} / 変動: {change_pct:+.1f}%")

    messages: List[str] = []
    now_jst = datetime.now(JST)

    # ── 条件① 価格変動 ──
    cond1_triggered, _ = _check_price_change(ticker, current_price, entry_price)
    cond1_label = "price_change"
    in_cooldown = _is_cooldown(alerts, ticker, cond1_label)
    print(f"    ①価格変動±20%: {'🔔 TRIGGERED' if cond1_triggered else 'OK'}"
          f"{' (cooldown)' if cond1_triggered and in_cooldown else ''}")
    if cond1_triggered and not in_cooldown:
        direction = "上昇" if change_pct > 0 else "下落"
        reason_j = f"価格変動アラート: {change_pct:+.1f}%（{direction}）（自動記録）"
        tags_j = ["satellite_alert", "price_up" if change_pct > 0 else "price_drop"]
        msg = (
            f"⚡ **{ticker}** 価格変動アラート\n"
            f"変動率: {change_pct:+.1f}% （{direction}）\n"
            f"現在価格: ${current_price:.2f} / エントリー: ${entry_price:.2f}\n"
            f"エグジット条件: {exit_cond}"
        )
        messages.append(msg)
        _record_alert(alerts, ticker, cond1_label)
        _append_journal_watchlist(ticker, reason_j, tags_j, dry_run=dry_run)

    # ── 条件② エグジット数値充足 ──
    cond2_triggered, target = _check_exit_numeric(ticker, current_price, exit_cond, entry_price)
    cond2_label = "exit_numeric"
    in_cooldown2 = _is_cooldown(alerts, ticker, cond2_label)
    if target is not None:
        print(f"    ②エグジット数値: 目標=${target:.2f} → {'🔔 TRIGGERED' if cond2_triggered else 'OK'}"
              f"{' (cooldown)' if cond2_triggered and in_cooldown2 else ''}")
    else:
        print(f"    ②エグジット数値: 数値抽出不可（定性条件）")
    if cond2_triggered and not in_cooldown2:
        reason_j = f"エグジット条件充足: 目標${target:.2f}到達（自動記録）"
        msg = (
            f"🚨 **{ticker}** エグジット条件充足\n"
            f"条件: {exit_cond}\n"
            f"目標: ${target:.2f} / 現在価格: ${current_price:.2f}"
        )
        messages.append(msg)
        _record_alert(alerts, ticker, cond2_label)
        _append_journal_watchlist(ticker, reason_j, ["satellite_alert", "exit_target"], dry_run=dry_run)

    # ── 条件③ テーゼ否定ニュース ──
    cond3_label = "thesis_news"
    in_cooldown3 = _is_cooldown(alerts, ticker, cond3_label)
    if skip_news or in_cooldown3:
        print(f"    ③テーゼ否定ニュース: スキップ"
              f"{' (cooldown)' if in_cooldown3 else ' (--skip-news)' if skip_news else ''}")
    else:
        news = _call_grok_news(ticker, strategy_name, exit_cond)
        news_triggered = news.get("alert", False)
        print(f"    ③テーゼ否定ニュース: {'🔔 TRIGGERED' if news_triggered else 'OK (なし)'}")
        if news_triggered:
            news_title = news.get('news_title', '(タイトル不明)')
            reason_j = f"テーゼ否定ニュース検知: {news_title[:80]}（自動記録）"
            msg = (
                f"📰 **{ticker}** 要注意ニュース\n"
                f"{news_title}\n"
                f"関連性: {news.get('relevance', '')}\n"
                f"URL: {news.get('news_url', 'N/A')}"
            )
            messages.append(msg)
            _record_alert(alerts, ticker, cond3_label)
            _append_journal_watchlist(ticker, reason_j, ["satellite_alert", "thesis_news"], dry_run=dry_run)

    # ── 条件④ 決算接近 ──
    cond4_triggered, est_date, days_until = _check_earnings_approach(ticker, rss_state)
    cond4_label = "earnings_approach"
    in_cooldown4 = _is_cooldown(alerts, ticker, cond4_label)
    if est_date:
        print(f"    ④決算接近: 推定{est_date}（{days_until}日後）"
              f" → {'🔔 TRIGGERED' if cond4_triggered else 'OK'}"
              f"{' (cooldown)' if cond4_triggered and in_cooldown4 else ''}")
    else:
        print(f"    ④決算接近: rss_stateデータなし → スキップ")
    if cond4_triggered and not in_cooldown4:
        reason_j = f"決算接近: 推定{est_date}（約{days_until}日後）（自動記録）"
        msg = (
            f"📅 **{ticker}** 決算接近\n"
            f"推定決算日: {est_date}（約{days_until}日後）\n"
            f"エグジット条件を再確認してください。"
        )
        messages.append(msg)
        _record_alert(alerts, ticker, cond4_label)
        _append_journal_watchlist(ticker, reason_j, ["satellite_alert", "earnings_approach"], dry_run=dry_run)

    return messages


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TANUKI TAIL Satellite Monitor — 変化通知"
    )
    parser.add_argument("--dry-run",    action="store_true", help="通知を送らず判定結果のみ出力")
    parser.add_argument("--skip-news",  action="store_true", help="Grokニュース検索をスキップ（API節約）")
    parser.add_argument("--ticker",     nargs="+",           help="対象ティッカー（省略時は全satellite）")
    args = parser.parse_args()

    now_jst = datetime.now(JST)
    mode_label = "[DRY-RUN]" if args.dry_run else "[LIVE]"
    print(f"TANUKI TAIL Satellite Monitor {mode_label} — {now_jst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 60)

    positions  = _load_satellite_positions()
    avg_costs  = _load_portfolio_avg_costs()
    rss_state  = _load_rss_state()
    alerts     = _load_alerts()

    if args.ticker:
        positions = [p for p in positions if p["ticker"] in args.ticker]

    print(f"satellite銘柄: {[p['ticker'] for p in positions]}")
    if args.dry_run:
        print("（通知は送信しません）")

    all_messages: List[Tuple[str, str]] = []  # (ticker, message)

    for pos in positions:
        msgs = monitor_ticker(
            pos, avg_costs, rss_state, alerts,
            dry_run=args.dry_run,
            skip_news=args.skip_news,
        )
        for m in msgs:
            all_messages.append((pos["ticker"], m))

    print(f"\n{'━' * 60}")
    print(f"通知発生: {len(all_messages)} 件")

    for ticker, msg in all_messages:
        print(f"\n─── {ticker} 通知 ───")
        print(msg)
        if not args.dry_run:
            send_discord(msg, dry_run=False)

    # アラート履歴保存（dry-run でも保存しない）
    if not args.dry_run:
        _save_alerts(alerts)
        print(f"\n✓ アラート履歴保存: {ALERTS_PATH}")
    else:
        print("\n（DRY-RUN: アラート履歴は保存しません）")


if __name__ == "__main__":
    main()
