#!/usr/bin/env python3
"""
TANUKI TAIL — workflow_write.py

tail/index.html からの workflow_dispatch を受けて、
ポジション登録・ジャーナル記録・KPI確定の書き込みを実行する（TAIL-SEC-1 phase2）。
ブラウザからGitHub Contents APIへ直接書き込ませる方式を廃止し、
GitHub Actions（GITHUB_TOKEN・contents:write）経由の書き込みに統一する。

使用方法（GitHub Actions専用。環境変数で入力を受け取る）:
    ACTION=register_position PAYLOAD='{"ticker":"PLTR",...}' python src/tail/workflow_write.py

環境変数:
    ACTION   register_position | register_journal | confirm_kpis
    PAYLOAD  アクション別のJSON文字列
出力:
    /tmp/tail_commit_message.txt にコミットメッセージを書き出す
    （ワークフロー側の git commit -F で使用。シェルへの直接埋め込みを避けるため）
"""

import os
import re
import json
from datetime import datetime
from zoneinfo import ZoneInfo

script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root  = os.path.abspath(os.path.join(script_dir, "..", ".."))

DATA_DIR      = os.path.join(repo_root, "docs", "portfolio", "tail", "data")
POSITIONS_DIR = os.path.join(DATA_DIR, "positions")
INDEX_PATH    = os.path.join(DATA_DIR, "positions_index.json")
JOURNAL_PATH  = os.path.join(DATA_DIR, "journal.json")

JST = ZoneInfo("Asia/Tokyo")
TICKER_RE = re.compile(r"^[A-Z0-9.\-]{1,10}$")

# TAILKPI-FIELD-VALIDATION-GAP-1（2026-09-19実装）: xbrl_tagは
# "namespace:LocalName"形式（us-gaap:Revenues、pltr:CommercialSegmentMember等、
# 実データ調査で確認したカスタム名前空間も許容）。
KPI_XBRL_TAG_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]*:[A-Za-z][A-Za-z0-9]*$")


def _now_iso():
    return datetime.now(JST).strftime("%Y-%m-%dT%H:%M:%S+09:00")


def _validate_ticker(ticker):
    ticker = (ticker or "").strip().upper()
    if not TICKER_RE.match(ticker):
        raise ValueError(f"invalid ticker: {ticker!r}")
    return ticker


def register_position(payload):
    ticker = _validate_ticker(payload.get("ticker"))
    ptype  = payload.get("type")
    now    = _now_iso()

    if ptype == "core":
        for field in ("thesis", "entry_story", "exit_guide"):
            if not payload.get(field):
                raise ValueError(f"missing required field: {field}")
        thesis = {
            "ticker": ticker, "type": "core", "status": "active", "version": 1,
            "created_at": now, "updated_at": now,
            "thesis": payload["thesis"], "entry_story": payload["entry_story"],
            "exit_guide": payload["exit_guide"], "kpis": [],
            "entry_price": payload.get("entry_price"), "entry_date": payload.get("entry_date"),
        }
    elif ptype == "satellite":
        for field in ("strategy_name", "entry_condition", "exit_condition", "holding_period"):
            if not payload.get(field):
                raise ValueError(f"missing required field: {field}")
        thesis = {
            "ticker": ticker, "type": "satellite", "status": "active", "version": 1,
            "created_at": now, "updated_at": now,
            "strategy_name": payload["strategy_name"], "entry_condition": payload["entry_condition"],
            "exit_condition": payload["exit_condition"], "holding_period": payload["holding_period"],
            "entry_price": payload.get("entry_price"), "entry_date": payload.get("entry_date"),
        }
    else:
        raise ValueError(f"unknown type: {ptype!r}")

    os.makedirs(POSITIONS_DIR, exist_ok=True)
    fname = f"{ticker}_thesis.json"
    with open(os.path.join(POSITIONS_DIR, fname), "w", encoding="utf-8") as f:
        json.dump(thesis, f, ensure_ascii=False, indent=2)

    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH, encoding="utf-8") as f:
            index = json.load(f)
    else:
        index = {"positions": []}
    positions = [p for p in index.get("positions", []) if p != fname]
    positions.append(fname)
    index["positions"] = positions
    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    return f"feat: TANUKI TAIL {ticker} テーゼ登録（workflow_dispatch）"


def register_journal(payload):
    ticker = (payload.get("ticker") or "").strip()
    if not ticker:
        raise ValueError("ticker is required")
    jtype  = payload.get("type")
    date_v = payload.get("date")
    reason = (payload.get("reason") or "").strip()
    if not date_v:
        raise ValueError("date is required")
    if not reason:
        raise ValueError("reason is required")

    with open(JOURNAL_PATH, encoding="utf-8") as f:
        jnl = json.load(f)

    existing = {e.get("id") for e in jnl.get("entries", [])}
    seq = 1
    while f"{date_v}-{ticker}-{seq:03d}" in existing:
        seq += 1
    entry_id = f"{date_v}-{ticker}-{seq:03d}"

    entry = {
        "id": entry_id,
        "timestamp": f"{date_v}T00:00:00+09:00",
        "ticker": ticker,
        "type": jtype,
        "action": jtype,
        "reason": reason,
        "thesis_version": None,
        "health_score_at_action": payload.get("health_score_at_action"),
        "macro_score_at_action": None,
        "tags": payload.get("tags") or [],
    }
    if payload.get("price") is not None:
        entry["price"] = payload["price"]
    if payload.get("shares") is not None:
        entry["shares"] = payload["shares"]
    if jtype == "thesis_revision":
        for key in ("revised_item", "old_thesis", "new_thesis"):
            if payload.get(key):
                entry[key] = payload[key]

    jnl.setdefault("entries", []).append(entry)
    with open(JOURNAL_PATH, "w", encoding="utf-8") as f:
        json.dump(jnl, f, ensure_ascii=False, indent=2)

    return f"journal: {ticker} {jtype} — {date_v}（workflow_dispatch）"


def _validate_kpi_fields(kpi, index):
    """TAILKPI-FIELD-VALIDATION-GAP-1（2026-09-19実装）: kpis[]の個別
    フィールド妥当性検証。

    実データ調査（docs/portfolio/tail/data/positions/*_thesis.json 全件、
    config/tail_kpi_map.json）の結果、warning_threshold は常に「120%以下」
    「前四半期比横ばい」のような自然文字列であり数値ではないと判明したため
    （登録時の対応方針メモにあった「数値妥当性」という想定は実データと
    不一致だった）、ここでは「空でない文字列であること」のみを検証する。
    xbrl_tag は auto_fetchable=False でも値を持つ実例（SOFI NCO/NIM）が
    あり auto_fetchable との相関では検証できないため、値が存在する場合
    のみ "namespace:LocalName" 形式を検証し、None または未設定（手動追加
    KPIで発生）は許容する。
    """
    name = kpi.get("name") if isinstance(kpi, dict) else None
    label = name or f"kpis[{index}]"

    warn = kpi.get("warning_threshold")
    if not isinstance(warn, str) or not warn.strip():
        raise ValueError(
            f"invalid warning_threshold for {label!r}: must be a non-empty string, got {warn!r}"
        )

    tag = kpi.get("xbrl_tag")
    if tag is not None and (not isinstance(tag, str) or not KPI_XBRL_TAG_RE.match(tag)):
        raise ValueError(
            f"invalid xbrl_tag for {label!r}: must be 'namespace:LocalName' format "
            f"(e.g. 'us-gaap:Revenues'), got {tag!r}"
        )


def confirm_kpis(payload):
    ticker   = _validate_ticker(payload.get("ticker"))
    selected = payload.get("kpis")
    if not selected:
        raise ValueError("kpis must be a non-empty list")
    for i, kpi in enumerate(selected):
        if not isinstance(kpi, dict):
            raise ValueError(f"kpis[{i}] must be an object")
        _validate_kpi_fields(kpi, i)

    thesis_path = os.path.join(POSITIONS_DIR, f"{ticker}_thesis.json")
    with open(thesis_path, encoding="utf-8") as f:
        thesis = json.load(f)
    thesis["kpis"]       = selected
    thesis["updated_at"] = _now_iso()
    with open(thesis_path, "w", encoding="utf-8") as f:
        json.dump(thesis, f, ensure_ascii=False, indent=2)

    return f"feat: TANUKI TAIL {ticker} KPI確定（{len(selected)}件・workflow_dispatch）"


ACTIONS = {
    "register_position": register_position,
    "register_journal": register_journal,
    "confirm_kpis": confirm_kpis,
}


def main():
    action      = os.environ.get("ACTION", "")
    payload_raw = os.environ.get("PAYLOAD", "")
    if action not in ACTIONS:
        raise SystemExit(f"unknown action: {action!r}")
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError as e:
        raise SystemExit(f"invalid payload JSON: {e}")

    commit_message = ACTIONS[action](payload)

    with open("/tmp/tail_commit_message.txt", "w", encoding="utf-8") as f:
        f.write(commit_message)
    print(commit_message)


if __name__ == "__main__":
    main()
