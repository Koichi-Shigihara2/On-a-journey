#!/usr/bin/env python3
"""
TANUKI TAIL — quarterly_review_generator.py

review_queue.json の pending エントリに対して Grok を2回呼び出し、
四半期レビューレポートを生成する。

Stage 1: 投資テーゼ健全度評価（自然言語）
Stage 2: DCF入力用パラメータ生成（構造化JSON）

使用方法:
    python src/tail/quarterly_review_generator.py
    python src/tail/quarterly_review_generator.py --dry-run

出力: docs/portfolio/tail/data/reviews/{ticker}_{quarter}_review.json
環境変数:
    XAI_API_KEY  xAI Grok API キー（必須）
"""

import os
import sys
import json
import re
import csv
import time
import argparse
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple

# ── パス設定 ──────────────────────────────────────────────────
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root  = os.path.abspath(os.path.join(script_dir, "..", ".."))

if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
# [フェーズD Step2-3対応、2026-08-07] SEC EDGAR Layer3
# （common/sec_data/layer3_builder.py、company_facts.json由来の統合
# スキーマ）経由でnormalized/参照を廃止した。本ファイル内で扱う
# TANUKI TAIL独自の「layer2」「layer3」（kpi_data_layer2/
# kpi_data_layer3引数等、KPI自動取得/AI text抽出の精度階層）とは
# 完全に別概念のため、SEC側を指す場合は必ず「SEC EDGAR Layer3」と
# 明示して区別する。
from common.sec_data.layer3_builder import (  # noqa: E402
    build_ticker_store, get_quarterly_series, get_latest_quarterly,
)
from src.tail.thesis_utils import thesis_narrative_fields  # noqa: E402

DATA_DIR          = os.path.join(repo_root, "docs", "portfolio", "tail", "data")
POSITIONS_DIR     = os.path.join(DATA_DIR, "positions")
KPI_DIR           = os.path.join(DATA_DIR, "kpi")
KPI_PROPOSALS_DIR = os.path.join(DATA_DIR, "kpi_proposals")
REVIEWS_DIR       = os.path.join(DATA_DIR, "reviews")
REVIEW_QUEUE_PATH = os.path.join(DATA_DIR, "review_queue.json")
TANUKI_DATA_DIR       = os.path.join(repo_root, "docs", "value-monitor", "tanuki_valuation", "data")
MACRO_DATA_DIR        = os.path.join(repo_root, "docs", "market-monitor", "macro-pulse", "data")
PORTFOLIO_PATH        = os.path.join(repo_root, "docs", "portfolio", "data", "portfolio.json")
PREDICTION_HISTORY_PATH  = os.path.join(DATA_DIR, "prediction_history.json")
KPI_MAP_PATH          = os.path.join(repo_root, "config", "tail_kpi_map.json")

# PascalCase（本ファイル内の既存呼び出し表記）→ SEC EDGAR Layer3の
# snake_caseフィールド名の対応表（フェーズD Step2-3対応）。
_SEC_LAYER3_FIELD_MAP = {
    "Revenue": "revenue",
    "OperatingIncome": "operating_income",
    "SBC": "stock_based_compensation",
    "NetIncome": "net_income",
    "SharesDiluted": "shares_diluted",
}

JST = ZoneInfo("Asia/Tokyo")

# ── Grok API 設定 ──────────────────────────────────────────────
XAI_API_KEY = os.getenv("XAI_API_KEY", "")
GROK_URL    = "https://api.x.ai/v1/chat/completions"
GROK_MODELS = ["grok-3-mini", "grok-3", "grok-2-1212"]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Grok API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def call_grok(
    user_prompt: str,
    system_prompt: str = "",
    max_tokens: int = 2000,
    temperature: float = 0.3,
) -> str:
    if not XAI_API_KEY:
        raise RuntimeError("XAI_API_KEY が設定されていません")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {XAI_API_KEY}",
    }
    messages: List[Dict[str, str]] = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user_prompt})

    last_error: Optional[Exception] = None
    for model in GROK_MODELS:
        try:
            print(f"  [Grok] モデル試行: {model}")
            resp = requests.post(
                GROK_URL,
                headers=headers,
                json={
                    "model":       model,
                    "messages":    messages,
                    "max_tokens":  max_tokens,
                    "temperature": temperature,
                },
                timeout=120,
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]
            print(f"  [Grok] 成功: {model}")
            return text
        except Exception as e:
            print(f"  [Grok] 失敗 ({model}): {e}")
            last_error = e
            time.sleep(1)
    raise RuntimeError(f"すべてのGrokモデルで失敗: {last_error}")


def extract_json_from_response(text: str) -> Dict[str, Any]:
    # ```json ... ``` ブロックを優先して抽出
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        return json.loads(m.group(1))
    # フォールバック: 生テキストから { } を探す
    start = text.find("{")
    end   = text.rfind("}") + 1
    if start >= 0 and end > start:
        return json.loads(text[start:end])
    raise ValueError(f"JSONが見つかりません: {text[:300]}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# データ読み込み
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_thesis(ticker: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(POSITIONS_DIR, f"{ticker}_thesis.json")
    if not os.path.exists(path):
        print(f"  [WARN] thesis.json 未発見: {path}")
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_layer2_kpi(ticker: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(KPI_DIR, f"{ticker}_layer2.json")
    if not os.path.exists(path):
        print(f"  [WARN] layer2.json 未発見: {path}")
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_layer3_kpi(ticker: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(KPI_DIR, f"{ticker}_layer3.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_tanuki_valuation(ticker: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(TANUKI_DATA_DIR, ticker, "latest.json")
    if not os.path.exists(path):
        print(f"  [WARN] latest.json 未発見: {path}")
        return None
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    comp = data.get("components", {})
    return {
        "intrinsic_value": round(data.get("intrinsic_value_per_share") or 0, 2),
        "current_price":   round(comp.get("current_price") or 0, 2),
        "deviation_rate":  round(data.get("upside_percent") or 0, 1),
        "tanuki_score":    data.get("tanuki_score", "N/A"),
    }


def load_layer1_financials(ticker: str) -> Dict[str, Any]:
    """SEC EDGAR Layer3（company_facts.json由来の統合スキーマ）四半期
    データ + latest.json から財務指標（TANUKI TAIL側の「layer1」区分、
    ＝財務諸表由来のKPI）を取得する。

    ※ 戻り値の意味上の区分名「layer1」（財務諸表由来）は、本関数が参照
    するデータソース「SEC EDGAR Layer3」（layer3_builder.py、統合
    スキーマ）とは無関係の別軸の用語。混同しないこと。
    """
    result: Dict[str, Any] = {}

    store = build_ticker_store(ticker)
    if store is not None:

        def _end_to_quarter(end: str) -> str:
            yr, mo, _ = end.split("-")
            return f"{yr}Q{(int(mo) - 1) // 3 + 1}"

        rev = get_latest_quarterly(store, _SEC_LAYER3_FIELD_MAP["Revenue"])
        oi  = get_latest_quarterly(store, _SEC_LAYER3_FIELD_MAP["OperatingIncome"])
        sbc = get_latest_quarterly(store, _SEC_LAYER3_FIELD_MAP["SBC"])
        ni  = get_latest_quarterly(store, _SEC_LAYER3_FIELD_MAP["NetIncome"])
        sd  = get_latest_quarterly(store, _SEC_LAYER3_FIELD_MAP["SharesDiluted"])

        if rev and oi and rev.get("val"):
            result["operating_margin"] = round(oi["val"] / rev["val"], 4)
        if sbc:
            result["sbc_quarterly"] = sbc["val"]
        if ni and sd and sd.get("val"):
            result["eps_diluted"] = round(ni["val"] / sd["val"], 4)

        # 直近4四半期の営業利益率推移
        rev_qs = sorted(
            get_quarterly_series(store, _SEC_LAYER3_FIELD_MAP["Revenue"]),
            key=lambda x: x.get("end", ""), reverse=True,
        )[:4]
        oi_map = {
            x["end"]: x["val"]
            for x in get_quarterly_series(store, _SEC_LAYER3_FIELD_MAP["OperatingIncome"])
        }
        opm_history = []
        for r in reversed(rev_qs):
            end = r.get("end", "")
            rev_val = r.get("val", 0)
            oi_val  = oi_map.get(end)
            if rev_val and oi_val is not None:
                opm_history.append({
                    "quarter":          _end_to_quarter(end),
                    "operating_margin": round(oi_val / rev_val, 4),
                })
        if opm_history:
            result["operating_margin_history"] = opm_history

    latest_path = os.path.join(TANUKI_DATA_DIR, ticker, "latest.json")
    if os.path.exists(latest_path):
        with open(latest_path, encoding="utf-8") as f:
            lat = json.load(f)
        sbc_ttm = (lat.get("financial_health") or {}).get("sbc_ttm")
        if sbc_ttm:
            result["sbc_ttm"] = sbc_ttm
        fwd_eps = (lat.get("components") or {}).get("forward_eps")
        if fwd_eps:
            result["eps_forward"] = fwd_eps

    return result


def load_macro_context() -> Optional[Dict[str, Any]]:
    wa_path = os.path.join(MACRO_DATA_DIR, "05_weekly_analysis.csv")
    lq_path = os.path.join(MACRO_DATA_DIR, "05_liquidity.csv")

    ctx: Dict[str, Any] = {}

    if os.path.exists(wa_path):
        with open(wa_path, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if rows:
            last = rows[-1]
            ctx.update({
                "score":            last.get("score", ""),
                "phase":            last.get("phase", ""),
                "score_change_1w":  last.get("score_change_1w", ""),
                "score_change_1m":  last.get("score_change_1m", ""),
                "watchpoints":      last.get("watchpoints", ""),
                "indicator_deltas": last.get("indicator_deltas", ""),
            })

    if os.path.exists(lq_path):
        with open(lq_path, encoding="utf-8") as f:
            rows = [r for r in csv.DictReader(f) if r.get("date")]
        if rows:
            last = rows[-1]
            ctx.update({
                "stealth_signal": last.get("stealth_signal", ""),
                "stealth_alert":  last.get("stealth_alert", ""),
            })

    return ctx if ctx else None


def get_avg_cost(ticker: str) -> Optional[float]:
    if not os.path.exists(PORTFOLIO_PATH):
        return None
    with open(PORTFOLIO_PATH, encoding="utf-8") as f:
        portfolio = json.load(f)
    total_cost   = 0.0
    total_shares = 0.0
    for broker_data in portfolio.get("brokers", {}).values():
        pos = broker_data.get("positions", {}).get(ticker)
        if pos:
            shares   = float(pos.get("shares",   0))
            avg_cost = float(pos.get("avg_cost",  0))
            total_cost   += avg_cost * shares
            total_shares += shares
    if total_shares == 0:
        return None
    return round(total_cost / total_shares, 2)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# KPI テーブル整形
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _fmt_kpi_value(value: Any, unit: str = "") -> str:
    if value is None:
        return "N/A"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)

    if unit == "USD":
        if abs(v) < 2:  # 比率（貢献利益率等）→ パーセント表示
            # [[KPI-UNIT-HARDCODE-USD-1]]修正後もこの分岐自体は削除しない
            # （unit判定漏れ・未分類KPIへの保険として維持。比率KPIは
            # 本来下のunit=="ratio"分岐で処理される）
            return f"{v * 100:.1f}%"
        elif abs(v) >= 1_000_000_000:
            return f"${v / 1_000_000_000:.2f}B"
        elif abs(v) >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        else:
            return f"${v:,.0f}"
    elif unit == "ratio":
        # [[KPI-UNIT-HARDCODE-USD-1]]: 0〜1の小数比率をパーセント表示に
        # 変換する（貢献利益率0.78→78.0%等）。上のunit=="USD"分岐が
        # abs(v)<2の場合に行っていた変換と同一だが、こちらは値の大きさ
        # ではなくKPI定義（unit）に基づく正式な分岐。
        return f"{v * 100:.1f}%"
    elif unit == "%":
        return f"{v:.1f}%"
    else:
        return str(v)


def build_kpi_table(kpi_data: Dict[str, Any], max_quarters: int = 8) -> str:
    kpis = kpi_data.get("kpis", {})
    if not kpis:
        return "（KPIデータなし）"

    all_quarters: set = set()
    for kinfo in kpis.values():
        for dp in kinfo.get("data", []):
            all_quarters.add(dp["quarter"])

    sorted_q = sorted(all_quarters, reverse=True)[:max_quarters]

    header = "| KPI | " + " | ".join(sorted_q) + " |"
    sep    = "| --- | " + " | ".join(["---"] * len(sorted_q)) + " |"

    rows = [header, sep]
    for kname, kinfo in kpis.items():
        unit   = kinfo.get("unit", "")
        dp_map = {dp["quarter"]: dp["value"] for dp in kinfo.get("data", [])}
        vals   = [_fmt_kpi_value(dp_map.get(q), unit) for q in sorted_q]
        rows.append(f"| {kname} | " + " | ".join(vals) + " |")

    return "\n".join(rows)


def build_kpi_snapshot(kpi_data: Dict[str, Any], max_quarters: int = 4) -> Dict[str, Any]:
    kpis = kpi_data.get("kpis", {})
    all_quarters: set = set()
    for kinfo in kpis.values():
        for dp in kinfo.get("data", []):
            all_quarters.add(dp["quarter"])
    recent_q = sorted(all_quarters, reverse=True)[:max_quarters]

    snapshot: Dict[str, Any] = {}
    for kname, kinfo in kpis.items():
        dp_map = {dp["quarter"]: dp["value"] for dp in kinfo.get("data", [])}
        snapshot[kname] = {q: dp_map.get(q) for q in recent_q}
    return snapshot


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 閾値判定
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _resolve_kpi_value(
    kpi: Dict[str, Any],
    kpi_data_layer2: Optional[Dict[str, Any]],
    quarter: str,
) -> Tuple[Optional[float], str]:
    """
    KPIの実績値を取得し、閾値比較に適した値（YoY%・比率%）に変換する。

    Returns: (comparable_value, display_str)
      - comparable_value: 閾値と同スケールの値（None = データなし or 変換不可）
      - display_str: テーブル表示用文字列

    変換ルール:
      - KPI名に「成長率」「YoY」「前年」を含む、または警戒ラインに「前年比」「YoY」を含む
        → layer2の生値から前年同期比（YoY%）を計算して使用
      - 生値が大きな通貨値（>=2）かつ警戒ラインが%形式
        → YoY成長率を計算して使用
      - 小数比率（0-1の範囲）
        → ×100してパーセント変換
      - それ以外
        → 生値をそのまま使用
    """
    lookup_key = kpi.get("layer2_name") or kpi.get("name", "")
    kpis       = (kpi_data_layer2 or {}).get("kpis", {})
    kinfo      = kpis.get(lookup_key)
    if not kinfo:
        return None, "—"

    dp_map = {dp["quarter"]: dp["value"] for dp in kinfo.get("data", [])}
    curr_val = dp_map.get(quarter)
    if curr_val is None:
        return None, "—"

    unit      = kinfo.get("unit", "")
    kpi_name  = kpi.get("name", "")
    warn_thr  = str(kpi.get("warning_threshold") or "")

    try:
        curr_f = float(curr_val)
    except (TypeError, ValueError):
        return None, str(curr_val)

    # YoY計算が必要か判定
    needs_yoy = any(kw in kpi_name for kw in ["成長率", "YoY", "前年"])
    needs_yoy = needs_yoy or any(kw in warn_thr for kw in ["前年比", "YoY"])
    needs_yoy = needs_yoy or (abs(curr_f) >= 2 and "%" in warn_thr)

    if needs_yoy:
        year   = int(quarter[:4])
        prev_q = f"{year - 1}{quarter[4:]}"
        prev_v = dp_map.get(prev_q)
        if prev_v is not None:
            try:
                prev_f = float(prev_v)
                if prev_f != 0:
                    yoy = (curr_f - prev_f) / abs(prev_f) * 100
                    if abs(curr_f) >= 2:
                        display = f"{_fmt_kpi_value(curr_f, unit)} (YoY {yoy:+.1f}%)"
                    else:
                        display = f"{yoy:+.1f}%"
                    return yoy, display
            except (TypeError, ValueError):
                pass
        if abs(curr_f) >= 2:
            return None, f"{_fmt_kpi_value(curr_f, unit)} (前年比不明)"

    # 小数比率（0〜1）→ パーセント変換
    if -1.0 < curr_f < 1.0 and curr_f != 0:
        pct = curr_f * 100
        return pct, f"{pct:.1f}%"

    return curr_f, _fmt_kpi_value(curr_f, unit)


def _compare_threshold(value: Any, threshold_str: Any) -> Optional[bool]:
    """
    実績値が警戒ラインを超えているか（問題ありか）を判定。

    Returns:
      True  = 警戒ライン超え（問題あり）
      False = 正常（ライン内）
      None  = 判定不能（数値変換失敗 or 閾値に数値なし）

    threshold_str 例: "30%未満", "110%以下", "15%超", "前年比+30%以上"
    """
    if value is None or threshold_str is None or str(threshold_str).strip() == "—":
        return None
    try:
        actual = float(value)
    except (TypeError, ValueError):
        return None

    s = str(threshold_str).strip()
    m = re.search(r"([+-]?\d+(?:\.\d+)?)", s)
    if not m:
        return None  # 数値なし → 判定不能（"前Q比上昇"等）

    threshold_val = float(m.group(1))

    # 万・億などの日本語単位を実数に変換（例: "60万人以下" → 600000）
    _JP_UNITS = (("億", 100_000_000), ("万", 10_000))
    for kanji, mult in _JP_UNITS:
        if kanji in s:
            threshold_val *= mult
            break

    # 実績値が小数比率（-1〜1）でthresholdが%形式なら%変換
    if "%" in s and -1.0 < actual < 1.0 and actual != 0:
        actual = actual * 100

    # 比較方向を判定（キーワード優先順序に注意）
    if "未満" in s:
        return actual < threshold_val
    elif "以下" in s or "割れ" in s or "下回" in s:
        return actual <= threshold_val
    elif "超え" in s or "超過" in s or "上回" in s:
        return actual > threshold_val
    elif "以上" in s:
        return actual >= threshold_val
    elif "超" in s:
        return actual > threshold_val
    else:
        # デフォルト: 下回りを警戒（成長率等の下限閾値想定）
        return actual < threshold_val


def _check_consecutive_warning(
    kpi: Dict[str, Any],
    kpi_data_layer2: Optional[Dict[str, Any]],
    current_quarter: str,
    threshold_str: Any,
    n: int = 2,
) -> bool:
    """直近n四半期（current_quarterを含む最新n件）で連続して警戒ライン超えか確認"""
    if not kpi_data_layer2:
        return False
    lookup_key = kpi.get("layer2_name") or kpi.get("name", "")
    kpis       = kpi_data_layer2.get("kpis", {})
    kinfo      = kpis.get(lookup_key)
    if not kinfo:
        return False

    all_q    = sorted({dp["quarter"] for dp in kinfo.get("data", [])}, reverse=True)
    target_q = [q for q in all_q if q <= current_quarter][:n]

    if len(target_q) < n:
        return False

    for q in target_q:
        comp_val, _ = _resolve_kpi_value(kpi, kpi_data_layer2, q)
        if _compare_threshold(comp_val, threshold_str) is not True:
            return False
    return True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# プロンプト構築
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STAGE1_SYSTEM = (
    "あなたは長期投資家Koichiの投資テーゼ検証パートナーです。"
    "感情的な応援ではなく、証拠に基づいた冷静な評価をしてください。"
    "楽観的バイアスには明示的に警告を発してください。"
    "回答はすべて日本語で記述してください。"
)

STAGE2_SYSTEM = (
    "あなたはDCFモデルの入力パラメータを生成する専門家です。"
    "楽観バイアスを避け、ベア/ベース/ブルの3シナリオを必ず提示してください。"
    "経営者の発言は10%割り引いて解釈してください。"
    "rationale・key_assumptions・risk_factorsはすべて日本語で記述してください。"
)

CALL2_SYSTEM = (
    "あなたは長期投資家Koichiの広い視野を補う投資パートナーです。"
    "定量データではなく、定性的な視点・歴史的類比・構造的問いかけを提供してください。"
    "「売れ」「買え」は言わない。最後は必ず問いかけで締める。"
    "根拠がある事象のみ言及し、データ不足時は「確認が必要」と明示する。"
    "必ずWeb検索を使って前回レビュー以降の最新情報を調べてください。"
    "検索した情報には出典（メディア名・日付）を明示してください。"
    "テーゼに書かれていることの言い換えは禁止です。"
    "Koichiさんが気づいていない視点・盲点を提供してください。"
    "回答はすべて日本語で記述してください。"
)


def _build_macro_text(macro_ctx: Optional[Dict[str, Any]]) -> str:
    if not macro_ctx:
        return "（マクロデータ未取得）"
    lines = []
    score = macro_ctx.get("score", "")
    if score:
        lines.append(
            f"景気スコア: {score} ({macro_ctx.get('phase', '')}) "
            f"週次変化: {macro_ctx.get('score_change_1w', 'N/A')} / "
            f"月次変化: {macro_ctx.get('score_change_1m', 'N/A')}"
        )
    if macro_ctx.get("watchpoints"):
        lines.append(f"注視点: {macro_ctx['watchpoints']}")
    if macro_ctx.get("indicator_deltas"):
        lines.append(f"指標デルタ: {macro_ctx['indicator_deltas']}")
    if macro_ctx.get("stealth_signal"):
        lines.append(f"ステルス流動性シグナル: {macro_ctx['stealth_signal']}")
    if macro_ctx.get("stealth_alert"):
        lines.append(f"流動性アラート: {macro_ctx['stealth_alert']}")
    return "\n".join(lines) or "（主要フィールドが空）"


def _load_tail_kpi_map() -> Dict[str, List[Dict[str, Any]]]:
    """config/tail_kpi_map.json を読み込む（存在しなければ空dict）"""
    if not os.path.exists(KPI_MAP_PATH):
        return {}
    with open(KPI_MAP_PATH, encoding="utf-8") as f:
        return json.load(f)


def _kpi_map_entry_to_thesis_kpi(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    tail_kpi_map.json の1エントリ（KPI自動取得フェッチャー向けの取得
    定義）を、thesis.jsonの`kpis`エントリと同じ形（_build_kpi_status_
    table()等が読む形）に変換する。

    `kpi_name`が{ticker}_layer2.jsonの"kpis"辞書のキーと一致するため、
    そのまま`name`として使う（layer2_nameは明示せずnameで直接引かせる）。
    tail_kpi_map.jsonには現状warning_threshold/exit_thresholdフィールド
    が存在しないため、.get()で取得を試みつつ通常はNone
    （= 表示側で「—」に変換される）になる。
    """
    return {
        "name":                   entry.get("kpi_name", ""),
        "layer2_name":            None,
        "description":            None,
        "source":                 "SEC EDGAR（Layer3統合スキーマ）" if entry.get("source") == "layer3" else "EDGAR XBRL",
        "warning_threshold":      entry.get("warning_threshold"),
        "exit_threshold":         entry.get("exit_threshold"),
        "related_exit_condition": None,
        "auto_fetchable":         True,
        "extraction_hint":        None,
        "xbrl_tag":               entry.get("revenue_tag"),
        "xbrl_dimension":         entry.get("dimension"),
        "xbrl_member":            (entry.get("tag_history") or [{}])[0].get("tag"),
    }


def _get_effective_thesis_kpis(thesis: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    レビューで監視するKPIリストを返す（[[TAIL-THESIS-KPIS-EMPTY-ADBE-
    APGE-1]]根本修正）。

    thesis.json側に`kpis`が個別登録されていれば、それをそのまま使う
    （PLTR/SOFI/TSLA等、警戒ライン・エグジット閾値を含めて手動で
    キュレーションされたリストが既にある場合は変更しない）。

    thesis.json側が空/未登録の場合のみ、config/tail_kpi_map.json
    （新規銘柄登録時に既に自動生成されるKPI自動取得の定義ファイル）
    から自動的にフォールバック生成する。これにより、新規銘柄登録の
    たびにthesis.jsonへ手作業でKPIをコピーする必要がなくなる。
    """
    kpis = thesis.get("kpis") or []
    if kpis:
        return kpis

    ticker      = thesis.get("ticker", "")
    kpi_map     = _load_tail_kpi_map()
    map_entries = kpi_map.get(ticker) or []
    return [_kpi_map_entry_to_thesis_kpi(e) for e in map_entries]


def _build_kpi_monitoring_section(
    thesis: Dict[str, Any],
    kpi_data_layer2: Optional[Dict[str, Any]] = None,
    kpi_data_layer3: Optional[Dict[str, Any]] = None,
    quarter: str = "",
) -> str:
    kpis = _get_effective_thesis_kpis(thesis)
    if not kpis:
        return ""
    table = _build_kpi_status_table(kpis, kpi_data_layer2, kpi_data_layer3, quarter)
    return (
        f"\n## 監視KPI実績（{quarter}）\n{table}\n\n"
        "※ 各KPIの実績値と警戒ラインとの距離感を評価に含めること。\n"
        "※ 実績値が「— 未取得」のKPIも次四半期の確認優先度に含めること。\n"
    )


def _build_kpi_trend_suffix(
    kpi: Dict[str, Any],
    kpi_data_layer2: Optional[Dict[str, Any]],
    quarter: str,
    n: int = 4,
) -> str:
    """
    直近n四半期（当四半期含む）の推移を「(前期$X・2期前$Y・3期前$Z)」の
    ような接尾辞文字列で返す（[[TAIL-KPI-TREND-DISPLAY-1]]）。

    satellite銘柄（ADBE/APGE/APP/CELH/CRWV/NVDA/SOUN）はwarning_
    threshold/exit_thresholdという固定閾値方式を導入せず、傾向・周辺
    環境を踏まえた総合判断をレビュー生成プロセス（Stage2/Call2）と
    Koichiさんご自身が都度行う方針としたため、判断材料として実績値
    列に複数四半期の推移を表示する。

    過去四半期が無い（当四半期の1件のみ、または該当KPIがlayer2に
    存在しない）場合は空文字列を返す——呼び出し側は_resolve_kpi_
    value()が返す単一値のdisplayをそのまま使えばよく、エラーには
    ならない。
    """
    lookup_key = kpi.get("layer2_name") or kpi.get("name", "")
    kpis  = (kpi_data_layer2 or {}).get("kpis", {})
    kinfo = kpis.get(lookup_key)
    if not kinfo:
        return ""

    dp_map = {dp["quarter"]: dp["value"] for dp in kinfo.get("data", [])}
    unit   = kinfo.get("unit", "")
    # 当四半期以前（未来の四半期を誤って拾わないためのガード）を新しい順に
    all_q  = sorted((q for q in dp_map if q <= quarter), reverse=True)
    past_q = all_q[1:n]  # all_q[0]が当四半期。以降を「前期」「2期前」…として使う
    if not past_q:
        return ""

    labels = ["前期", "2期前", "3期前"]
    parts: List[str] = []
    for i, q in enumerate(past_q):
        val = dp_map.get(q)
        if val is None:
            continue
        label = labels[i] if i < len(labels) else f"{i + 1}期前"
        parts.append(f"{label}{_fmt_kpi_value(val, unit)}")

    if not parts:
        return ""
    return " (" + "・".join(parts) + ")"


def _lookup_layer2_value(kpi: Dict[str, Any], kpi_data_layer2: Dict[str, Any], quarter: str) -> Optional[Any]:
    lookup_key = kpi.get("layer2_name") or kpi.get("name", "")
    kpis = kpi_data_layer2.get("kpis", {})
    for name, kinfo in kpis.items():
        if name == lookup_key:
            for dp in kinfo.get("data", []):
                if dp.get("quarter") == quarter:
                    return dp.get("value")
    return None


def _build_kpi_status_table(
    thesis_kpis: List[Dict[str, Any]],
    kpi_data_layer2: Optional[Dict[str, Any]],
    kpi_data_layer3: Optional[Dict[str, Any]],
    quarter: str,
) -> str:
    header = "| KPI名 | 実績値 | 警戒ライン | エグジット閾値 | 状態 |"
    sep    = "| --- | --- | --- | --- | --- |"
    rows   = [header, sep]

    l3_kpis = (kpi_data_layer3 or {}).get("kpis", {})

    for k in thesis_kpis:
        name     = k.get("name", "")
        # .get(key, default)のdefaultはキー自体が無い場合にしか効かないため、
        # 値が明示的にNone（閾値未設定のKPI）の場合はorで「—」にフォールバックする
        warn     = k.get("warning_threshold") or "—"
        exit_thr = k.get("exit_threshold") or "—"
        auto_f   = k.get("auto_fetchable", False)

        comp_val: Optional[float] = None
        display  = "—"
        confidence = "high"

        # layer2から取得（_resolve_kpi_valueでYoY/比率変換も実施）
        if auto_f and kpi_data_layer2:
            comp_val, display = _resolve_kpi_value(k, kpi_data_layer2, quarter)
            # 閾値判定に使うcomp_valは変えず、表示のみ複数四半期の推移を
            # 追記する（display=="—"＝当四半期の値自体が無い場合は対象外、
            # 過去データが無ければ_build_kpi_trend_suffix側が空文字列を
            # 返すため無変化）
            if display != "—":
                display += _build_kpi_trend_suffix(k, kpi_data_layer2, quarter)

        # auto_fetchable=True でも layer2 にデータがなければ layer3 にフォールバック
        if comp_val is None and name in l3_kpis:
            entry_l3   = l3_kpis[name]
            v_num      = entry_l3.get("value_numeric")
            raw_v      = v_num if v_num is not None else entry_l3.get("value")
            confidence = entry_l3.get("confidence", "medium")
            if raw_v is not None:
                try:
                    comp_val = float(raw_v)
                    display  = str(raw_v)
                except (TypeError, ValueError):
                    display = str(raw_v)

        if comp_val is not None:
            # exit_thresholdに「連続」が含まれる場合は連続チェックで判定
            exit_thr_str = str(exit_thr)
            if "連続" in exit_thr_str and kpi_data_layer2:
                exit_breach = (
                    _compare_threshold(comp_val, exit_thr) is True
                    and _check_consecutive_warning(k, kpi_data_layer2, quarter, exit_thr)
                )
            else:
                exit_breach = _compare_threshold(comp_val, exit_thr) is True

            warn_breach = _compare_threshold(comp_val, warn)

            if exit_breach:
                status = "❌ 危険"
            elif warn_breach is True:
                if kpi_data_layer2 and _check_consecutive_warning(
                    k, kpi_data_layer2, quarter, warn
                ):
                    status = "❌ 2Q連続"
                else:
                    status = "⚠ 警戒"
            elif warn_breach is False:
                status = "✅ 正常"
            else:
                # 閾値が数値でないため判定不能 → 信頼度で代替表示
                if confidence == "high":
                    status = "✅ 取得済"
                elif confidence == "medium":
                    status = "⚠ 中精度"
                else:
                    status = "❌ 低精度"
        else:
            status = "— 未取得"

        rows.append(f"| {name} | {display} | {warn} | {exit_thr} | {status} |")

    return "\n".join(rows)


def _build_layer3_text(kpi_data_layer3: Optional[Dict[str, Any]]) -> str:
    """Layer3 KPI（テキスト抽出）を整形テキストで返す"""
    if not kpi_data_layer3:
        return ""
    kpis = kpi_data_layer3.get("kpis", {})
    if not kpis:
        return ""
    lines: List[str] = []
    for name, info in kpis.items():
        v_num   = info.get("value_numeric")
        v_str   = info.get("value")
        conf    = info.get("confidence", "medium")
        period  = info.get("period", "")
        display = str(v_num) if v_num is not None else (str(v_str) if v_str is not None else "—")
        lines.append(f"  {name}: {display}  ({period}, 信頼度:{conf})")
    return "\n".join(lines)


def _calc_yoy_text(kpi_data: Optional[Dict[str, Any]]) -> str:
    if not kpi_data:
        return ""
    kpis = kpi_data.get("kpis", {})
    lines: List[str] = []
    for kname, kinfo in kpis.items():
        dp = {d["quarter"]: d["value"] for d in kinfo.get("data", [])}
        # 最新四半期を探して前年同期と比較
        for q in sorted(dp, reverse=True):
            year = int(q[:4])
            qnum = q[4:]
            prev_q = f"{year - 1}{qnum}"
            if prev_q in dp:
                curr, prev = dp[q], dp[prev_q]
                unit = kinfo.get("unit", "")
                if unit == "USD" and abs(curr) >= 2:
                    yoy = (curr - prev) / abs(prev) * 100 if prev else 0
                    lines.append(f"  {kname}: {q}={_fmt_kpi_value(curr, unit)} vs {prev_q}={_fmt_kpi_value(prev, unit)} → YoY {yoy:+.1f}%")
                else:
                    diff = (curr - prev) * 100
                    lines.append(f"  {kname}: {q}={curr*100:.1f}% vs {prev_q}={prev*100:.1f}% → 前年比 {diff:+.1f}pt")
                break
    return "\n".join(lines)


def _calc_qoq_text(kpi_data: Optional[Dict[str, Any]], quarter: str = "") -> str:
    """各KPIの前四半期比（QoQ）を計算して整形テキストで返す"""
    if not kpi_data:
        return ""
    kpis = kpi_data.get("kpis", {})
    lines: List[str] = []
    for kname, kinfo in kpis.items():
        dp    = {d["quarter"]: d["value"] for d in kinfo.get("data", [])}
        all_q = sorted(dp.keys(), reverse=True)
        if not all_q:
            continue
        curr_q = quarter if (quarter and quarter in dp) else all_q[0]
        # 前四半期を計算
        try:
            year  = int(curr_q[:4])
            q_num = int(curr_q[5:])  # "Q1" → "1"
        except (ValueError, IndexError):
            continue
        strict_prev_q = f"{year - 1}Q4" if q_num == 1 else f"{year}Q{q_num - 1}"
        is_strict_qoq = strict_prev_q in dp
        if is_strict_qoq:
            prev_q = strict_prev_q
        else:
            # 前四半期データ欠損（10-K未処理等）→ 直近利用可能な前四半期にフォールバック
            candidates = sorted([q for q in dp if q < curr_q], reverse=True)
            if not candidates:
                continue
            prev_q = candidates[0]
        curr_val = dp[curr_q]
        prev_val = dp[prev_q]
        if curr_val is None or prev_val is None:
            continue
        try:
            curr_f = float(curr_val)
            prev_f = float(prev_val)
        except (TypeError, ValueError):
            continue
        unit  = kinfo.get("unit", "")
        label = "QoQ" if is_strict_qoq else f"前回比({prev_q}→{curr_q})"
        if unit == "USD" and abs(curr_f) >= 2:
            if prev_f != 0:
                qoq = (curr_f - prev_f) / abs(prev_f) * 100
                lines.append(
                    f"  {kname}: {curr_q}={_fmt_kpi_value(curr_f, unit)}"
                    f" vs {prev_q}={_fmt_kpi_value(prev_f, unit)} → {label} {qoq:+.1f}%"
                )
        else:
            diff = (curr_f - prev_f) * 100
            lines.append(
                f"  {kname}: {curr_q}={curr_f * 100:.1f}%"
                f" vs {prev_q}={prev_f * 100:.1f}% → {label} {diff:+.1f}pt"
            )
    return "\n".join(lines)


def _load_past_health_scores(ticker: str, current_quarter: str, n: int = 8) -> str:
    """過去n四半期分の health_score 推移を文字列で返す（current_quarter を除く）"""
    if not os.path.exists(REVIEWS_DIR):
        return ""
    entries: List[Tuple[str, int, str]] = []
    for fname in os.listdir(REVIEWS_DIR):
        if not (fname.startswith(f"{ticker}_") and fname.endswith("_review.json")):
            continue
        q = fname[len(ticker) + 1 : -len("_review.json")]
        if q == current_quarter:
            continue
        fpath = os.path.join(REVIEWS_DIR, fname)
        try:
            with open(fpath, encoding="utf-8") as f:
                rv = json.load(f)
            hs  = rv.get("stage1", {}).get("health_score")
            rec = rv.get("stage1", {}).get("recommendation", "")
            if hs is not None:
                entries.append((q, int(hs), rec))
        except Exception:
            pass
    if not entries:
        return ""
    entries.sort(key=lambda x: x[0])
    recent = entries[-n:]
    return " / ".join(f"{q}:{hs}({rec})" for q, hs, rec in recent)


def _load_past_predictions(ticker: str, max_entries: int = 2) -> str:
    """prediction_history.json から直近2件の予測振り返りテキストを返す"""
    if not os.path.exists(PREDICTION_HISTORY_PATH):
        return ""
    try:
        with open(PREDICTION_HISTORY_PATH, encoding="utf-8") as f:
            history = json.load(f)
    except Exception:
        return ""

    entries = history.get(ticker, [])
    matchable = [
        e for e in entries
        if e.get("kpi_forecast_available") and e.get("matchable") and e.get("scenario") == "base"
    ]
    if not matchable:
        return ""

    recent = matchable[-max_entries:]
    lines = []
    for entry in recent:
        rq = entry["review_quarter"]
        tq = entry["forecast_target"]
        preds = entry.get("predictions", {})
        if not preds:
            continue

        def _fmt(v: Any) -> str:
            if v is None:
                return "N/A"
            if isinstance(v, float) and abs(v) <= 10:
                return f"{v * 100:.1f}%"
            if abs(v) >= 1_000_000_000:
                return f"${v / 1e9:.2f}B"
            if abs(v) >= 1_000_000:
                return f"${v / 1e6:.0f}M"
            return str(v)

        lines.append(f"【{rq}レビュー → {tq}実績】")
        lines.append("| KPI | 予測値 | 実績値 | 乖離率 |")
        lines.append("|-----|--------|--------|--------|")
        for kpi_name, p in preds.items():
            dev = p.get("deviation_pct")
            dev_str = f"{dev:+.1f}%" if dev is not None else "N/A"
            lines.append(f"| {kpi_name} | {_fmt(p['predicted'])} | {_fmt(p['actual'])} | {dev_str} |")
        lines.append("")

    if not lines:
        return ""

    body = "\n".join(lines)
    return (
        f"## 過去予測の振り返り\n"
        f"{body}\n"
        f"過去の予測精度を踏まえて今回の評価の楽観バイアスを調整してください。"
        f"特に過去に過大評価したKPIには追加の保守的調整を加えてください。\n"
    )




def build_stage1_prompt(
    thesis: Dict[str, Any],
    kpi_table: str,
    macro_ctx: Optional[Dict[str, Any]],
    valuation: Optional[Dict[str, Any]],
    ticker: str,
    quarter: str,
    entry_price: Optional[float] = None,
    kpi_data_layer2: Optional[Dict[str, Any]] = None,
    kpi_data_layer3: Optional[Dict[str, Any]] = None,
    past_health_scores: str = "",
    past_predictions: str = "",
) -> str:
    thesis_text, entry_story_text, exit_guide_text = thesis_narrative_fields(thesis)

    val_section = ""
    if valuation:
        val_section = (
            f"\n## 理論株価との乖離\n"
            f"現在価格: ${valuation['current_price']}\n"
            f"理論株価（WACC Rm=10%）: ${valuation['intrinsic_value']}\n"
            f"乖離率（upside）: {valuation['deviation_rate']}%\n"
            f"TANUKI判定: {valuation['tanuki_score']}\n"
        )
        if entry_price is not None:
            val_section += f"加重平均取得単価: ${entry_price}\n"
    elif entry_price is not None:
        val_section = f"\n## 取得コスト\n加重平均取得単価: ${entry_price}\n"

    # YoY / QoQ セクション
    yoy_text = _calc_yoy_text(kpi_data_layer2)
    qoq_text = _calc_qoq_text(kpi_data_layer2, quarter)
    yoy_section = f"\n## KPI 前年同期比（YoY）\n{yoy_text}\n" if yoy_text else ""
    qoq_section = f"\n## KPI 前四半期比（QoQ）\n{qoq_text}\n" if qoq_text else ""

    # 健全度推移セクション
    health_trend_section = (
        f"\n## テーゼ健全度の推移（過去）\n{past_health_scores}\n"
        if past_health_scores else ""
    )

    # 過去予測振り返りセクション
    prediction_section = f"\n{past_predictions}" if past_predictions else ""

    return f"""## 投資テーゼ（{ticker}）
{thesis_text}

## エントリーストーリー
{entry_story_text}

## エグジットの目安
{exit_guide_text}

## 直近KPI実績（{quarter}）
{kpi_table}
{yoy_section}{qoq_section}
## マクロ環境
{_build_macro_text(macro_ctx)}
{val_section}{health_trend_section}{prediction_section}{_build_kpi_monitoring_section(thesis, kpi_data_layer2, kpi_data_layer3, quarter)}
## 評価してください

1. テーゼ健全度（0-100点）と根拠
   - KPIはテーゼの方向と一致しているか
   - 想定外の変化はあるか
   - 楽観バイアスの兆候はあるか

2. 今四半期の注目点（良い点・懸念点 各2-3項目）

3. テーゼ継続/修正/撤退の推奨
   - CONTINUE: テーゼは健全
   - WATCH: 一部懸念あり・次回要確認
   - REVISE: テーゼの修正が必要
   - EXIT: テーゼが崩れている

4. 次四半期に確認すべきKPI（優先度順）
   （監視KPI設定がある場合、警戒ラインへの接近度も含めて評価すること）

5. 上記エグジット条件との距離感（現在どの程度近いか）

以下のJSON形式のみで回答してください（説明文・前置き不要）：
{{
  "health_score": 75,
  "health_label": "WATCH",
  "summary": "...",
  "positives": ["...", "..."],
  "concerns": ["...", "..."],
  "recommendation": "WATCH",
  "recommendation_reason": "...",
  "next_kpis": ["...", "..."],
  "exit_distance": "遠い",
  "exit_distance_reason": "...",
  "optimism_bias_warning": null
}}"""


def _calc_yoy_text_stage2(kpi_data: Optional[Dict[str, Any]]) -> str:
    """Stage 2 プロンプト専用の YoY テキスト（_calc_yoy_text と同一ロジック）"""
    return _calc_yoy_text(kpi_data)


def build_stage2_prompt(
    ticker: str,
    quarter: str,
    kpi_table: str,
    stage1: Dict[str, Any],
    kpi_data: Optional[Dict[str, Any]] = None,
    thesis_kpis: Optional[List[Dict[str, Any]]] = None,
    layer1: Optional[Dict[str, Any]] = None,
) -> str:
    yoy_text = _calc_yoy_text(kpi_data)
    yoy_section = f"\n## KPI 前年同期比（参考）\n{yoy_text}\n" if yoy_text else ""
    concerns_str = json.dumps(stage1.get("concerns", []), ensure_ascii=False)

    kpi_schema_lines = ""
    kpi_instruction = ""
    l2_kpis = (kpi_data or {}).get("kpis", {})

    # ── KPIあり: layer2 全KPI（thesis参照に限定しない） ──
    l2_keys: List[str] = []
    l2_display: List[str] = []
    l2_unit_hints: List[str] = []
    for l2_name, entry in l2_kpis.items():
        data = entry.get("data", [])
        cur_val = data[0].get("value") if data else None
        if cur_val is not None:
            if abs(cur_val) >= 1e6:
                l2_display.append(
                    f"  - {l2_name}: 現在={int(cur_val):,}（USD絶対値）"
                )
                l2_unit_hints.append(f"  {l2_name}: USD絶対値（整数）例: {int(cur_val):,}")
            elif abs(cur_val) <= 10:
                l2_display.append(f"  - {l2_name}: 現在={cur_val*100:.1f}%")
                l2_unit_hints.append(f"  {l2_name}: 比率（小数）例: {cur_val}")
            else:
                l2_display.append(f"  - {l2_name}: 現在={cur_val}")
                l2_unit_hints.append(f"  {l2_name}: 数値")
        else:
            l2_display.append(f"  - {l2_name}（現在値取得なし）")
            l2_unit_hints.append(f"  {l2_name}: USD絶対値（整数）")
        l2_keys.append(l2_name)

    # ── KPIなし: layer1 financial metrics ──
    l1_keys: List[str] = []
    l1_display: List[str] = []
    l1_unit_hints: List[str] = []
    if layer1:
        if "operating_margin" in layer1:
            l1_keys.append("営業利益率")
            l1_display.append(
                f"  - 営業利益率: {layer1['operating_margin']*100:.1f}%（最新四半期実績）"
            )
            l1_unit_hints.append(
                f"  営業利益率: 比率（小数）例: {layer1['operating_margin']}"
            )
        if "sbc_quarterly" in layer1:
            l1_keys.append("SBC")
            sbc_q = int(layer1["sbc_quarterly"])
            l1_display.append(
                f"  - SBC: {sbc_q:,}（${sbc_q/1e6:.0f}M/Q, USD絶対値, 最新四半期）"
            )
            l1_unit_hints.append(
                f"  SBC: USD絶対値（整数）例: {sbc_q:,}"
            )
        if "eps_diluted" in layer1:
            l1_keys.append("希薄化後EPS")
            l1_display.append(
                f"  - 希薄化後EPS: {layer1['eps_diluted']:.4f}（USD/株, 最新四半期）"
            )
            l1_unit_hints.append(
                f"  希薄化後EPS: USD/株（小数）例: {layer1['eps_diluted']:.4f}"
            )

    all_keys = l2_keys + l1_keys

    if all_keys:
        schema_vals = ",\n          ".join(f'"{k}": 0' for k in all_keys)
        kpi_schema_lines = (
            ',\n      "kpi_forecasts": {\n'
            '        "1年後": {\n          ' + schema_vals + '\n        },\n'
            '        "3年後": {\n          ' + schema_vals + '\n        }\n'
            '      }'
        )
        l2_sec   = "\n".join(l2_display)   if l2_display   else "  （なし）"
        l1_sec   = "\n".join(l1_display)   if l1_display   else "  （なし）"
        unit_sec = "\n".join(l2_unit_hints + l1_unit_hints)

        kpi_instruction = f"""
## KPI予想値の指示

以下の2種類のデータを起点にKPI予想値を生成してください。

【KPIあり（layer2実績値・高精度）】
{l2_sec}
→ 各シナリオの成長率を適用して1年後・3年後を予想してください。

【KPIなし（財務指標・全社レベル）】
{l1_sec}
→ 全社成長率とシナリオの最終営業利益率を踏まえて1年後・3年後を予想してください。

出力単位:
{unit_sec}

注意: 現在値が取得できている項目のみ予想値を生成してください。
"""

    # 営業利益率推移セクション
    opm_history = (layer1 or {}).get("operating_margin_history", [])
    opm_trend_section = ""
    if opm_history:
        trend_str  = " → ".join(
            f"{h['quarter']}: {h['operating_margin']*100:.1f}%"
            for h in opm_history
        )
        latest_opm = opm_history[-1]["operating_margin"] * 100
        opm_trend_section = f"""
## 営業利益率の直近推移（GAAP）
{trend_str}
※ 4四半期連続で急改善中。シナリオの最終営業利益率はこの推移を踏まえて設定してください。
現在{latest_opm:.1f}%から下落するシナリオは特別な理由がない限り避けてください。
"""

    return f"""## 銘柄: {ticker}
## 対象四半期: {quarter}

## 直近KPI
{kpi_table}
{yoy_section}
## Stage 1 評価結果
健全度: {stage1.get('health_score', 'N/A')}点 ({stage1.get('health_label', 'N/A')})
懸念点: {concerns_str}
{opm_trend_section}{kpi_instruction}
## DCFパラメータを生成してください

以下のJSONフォーマットのみで出力してください（説明文不要）：
{{
  "ticker": "{ticker}",
  "quarter": "{quarter}",
  "scenarios": {{
    "bear": {{
      "revenue_growth_y1": 0.15,
      "revenue_growth_y2": 0.12,
      "revenue_growth_y3": 0.10,
      "terminal_growth": 0.03,
      "operating_margin_terminal": 0.20,
      "rationale": "..."{kpi_schema_lines}
    }},
    "base": {{
      "revenue_growth_y1": 0.25,
      "revenue_growth_y2": 0.20,
      "revenue_growth_y3": 0.18,
      "terminal_growth": 0.03,
      "operating_margin_terminal": 0.25,
      "rationale": "..."{kpi_schema_lines}
    }},
    "bull": {{
      "revenue_growth_y1": 0.35,
      "revenue_growth_y2": 0.30,
      "revenue_growth_y3": 0.25,
      "terminal_growth": 0.035,
      "operating_margin_terminal": 0.30,
      "rationale": "..."{kpi_schema_lines}
    }}
  }},
  "key_assumptions": ["...", "..."],
  "risk_factors": ["...", "..."]
}}"""


def _load_past_call2(ticker: str, current_quarter: str, max_items: int = 2) -> List[Dict[str, Any]]:
    """同一tickerの直近2回分のcall2結果を返す（current_quarterを除く）"""
    if not os.path.exists(REVIEWS_DIR):
        return []
    candidates: List[tuple] = []
    for fname in os.listdir(REVIEWS_DIR):
        if fname.startswith(f"{ticker}_") and fname.endswith("_review.json"):
            q = fname[len(ticker) + 1 : -len("_review.json")]
            if q != current_quarter:
                candidates.append((q, fname))
    candidates.sort(key=lambda x: x[0], reverse=True)

    results: List[Dict[str, Any]] = []
    for q, fname in candidates[:max_items]:
        fpath = os.path.join(REVIEWS_DIR, fname)
        try:
            with open(fpath, encoding="utf-8") as f:
                rv = json.load(f)
            c2 = rv.get("call2")
            if c2:
                results.append({"quarter": q, "call2": c2})
        except Exception:
            pass
    return results


def build_call2_prompt(
    ticker: str,
    quarter: str,
    thesis: Dict[str, Any],
    stage1: Dict[str, Any],
    macro_ctx: Optional[Dict[str, Any]],
    kpi_status_table: str = "",
    past_call2: Optional[List[Dict[str, Any]]] = None,
    kpi_data: Optional[Dict[str, Any]] = None,
    kpi_data_layer3: Optional[Dict[str, Any]] = None,
    past_health_scores: str = "",
) -> str:
    # Stage1 全結果
    concerns_str   = "\n".join(f"・{c}" for c in (stage1.get("concerns") or []))
    positives_str  = "\n".join(f"・{p}" for p in (stage1.get("positives") or []))
    next_kpis_str  = "\n".join(f"・{k}" for k in (stage1.get("next_kpis") or []))
    bias_warning   = stage1.get("optimism_bias_warning") or "なし"
    recommendation = stage1.get("recommendation") or stage1.get("health_label") or "N/A"
    exit_dist      = stage1.get("exit_distance") or "不明"
    exit_dist_rsn  = stage1.get("exit_distance_reason") or ""

    macro_score  = (macro_ctx or {}).get("score", "不明")
    macro_phase  = (macro_ctx or {}).get("phase", "不明")
    thesis_text, entry_story_full, _exit_guide = thesis_narrative_fields(thesis)
    entry_story  = entry_story_full[:500]
    last_quarter = (past_call2[0]["quarter"] if past_call2 else "前回") if past_call2 else "前回"

    # 過去Call2の引き継ぎセクション（拡張版: five_perspectives・historical_analogy・entry_story_progressを追加）
    past_section = ""
    if past_call2:
        parts: List[str] = []
        for item in past_call2:
            q  = item["quarter"]
            c2 = item["call2"]
            q_parts: List[str] = []
            # 5観点サマリー
            if c2.get("five_perspectives"):
                fp = c2["five_perspectives"]
                fp_lines = [f"    {k}: {str(v)[:60]}" for k, v in fp.items() if v]
                if fp_lines:
                    q_parts.append(f"前回5観点サマリー ({q}):\n" + "\n".join(fp_lines))
            # 歴史的類比
            ha = c2.get("historical_analogy") or {}
            if ha.get("company"):
                q_parts.append(
                    f"前回歴史的類比 ({q}): {ha['company']}"
                    f"（示唆: {str(ha.get('implication', ''))[:80]}）"
                )
            # エントリーストーリー進捗
            if c2.get("entry_story_progress"):
                q_parts.append(
                    f"前回エントリー進捗 ({q}): {str(c2['entry_story_progress'])[:100]}"
                )
            # 問いかけ・次回確認論点
            if c2.get("thesis_questions"):
                q_parts.append(
                    f"前回の問いかけ ({q}):\n" +
                    "\n".join(f"・{x}" for x in c2["thesis_questions"])
                )
            if c2.get("next_review_focus"):
                q_parts.append(
                    f"前回の次回確認論点 ({q}):\n" +
                    "\n".join(f"・{x}" for x in c2["next_review_focus"])
                )
            if q_parts:
                parts.append("\n".join(q_parts))
        if parts:
            past_section = "\n## 過去レビューの引き継ぎ事項\n" + "\n\n".join(parts) + "\n"

    # KPIステータステーブルセクション
    kpi_section = ""
    if kpi_status_table:
        kpi_section = (
            f"\n## 今四半期のKPIステータス（{quarter}）\n"
            f"{kpi_status_table}\n"
            "各KPIの現状値と警戒ライン対比を参照して定性分析に活かしてください。\n"
        )

    # KPI実績テーブル（直近8四半期）＋ YoY・QoQ
    # kpi_tbl / yoy_text / qoq_text は命令文への直接埋め込みにも使用するため先に計算
    kpi_tbl  = build_kpi_table(kpi_data, max_quarters=8) if kpi_data else "（KPIデータなし）"
    yoy_text = _calc_yoy_text(kpi_data) if kpi_data else ""
    qoq_text = _calc_qoq_text(kpi_data, quarter) if kpi_data else ""

    kpi_table_section = ""
    if kpi_data:
        kpi_table_section = f"\n## KPI実績データ（直近8四半期）\n{kpi_tbl}\n"
        if yoy_text:
            kpi_table_section += f"\n### YoY（前年同期比）\n{yoy_text}\n"
        if qoq_text:
            kpi_table_section += f"\n### QoQ（前四半期比）\n{qoq_text}\n"

    # Layer3抽出値セクション
    l3_section = ""
    l3_text = _build_layer3_text(kpi_data_layer3)
    if l3_text:
        l3_section = f"\n## Layer3 テキスト抽出KPI（非自動取得分）\n{l3_text}\n"

    # 健全度推移セクション
    health_trend_section = (
        f"\n## テーゼ健全度の推移（過去）\n{past_health_scores}\n"
        if past_health_scores else ""
    )

    return f"""## 銘柄: {ticker}
## 対象四半期: {quarter}

## 投資テーゼ
{thesis_text}

## エントリーストーリー
{entry_story}

## Call 1 評価結果（全体）
健全度: {stage1.get('health_score', 'N/A')}点 ({stage1.get('health_label', 'N/A')})
判定: {recommendation}
ポジティブ点:
{positives_str if positives_str else '（なし）'}
主な懸念:
{concerns_str if concerns_str else '（なし）'}
楽観バイアス警告: {bias_warning}
エグジット距離: {exit_dist}（{exit_dist_rsn}）
次四半期確認KPI:
{next_kpis_str if next_kpis_str else '（なし）'}

## マクロ環境
{_build_macro_text(macro_ctx)}
{health_trend_section}{past_section}{kpi_table_section}{l3_section}{kpi_section}
## 以下の7項目を分析してください:

1. 5観点での分析（KPI実績を起点として必ず使用すること）

   【重要】以下のKPI実績データを各観点の「起点」として分析してください。
   各観点で必ず対応するKPI数値（$額・YoY%・QoQ%等）を具体的に引用してください。
   Web検索はKPI数値の文脈補完・競合情報確認のために使用し（数値が先、文脈が後）、
   出典（メディア名・日付）を明示してください。
   テーゼの言い換えは禁止。Koichiさんが見落としている視点を指摘してください。

   ## 今四半期KPI実績（起点として必ず使用すること）
{kpi_tbl}

   YoY（前年同期比）:
{yoy_text if yoy_text else "   （前年同期比データなし）"}

   QoQ（前回比）:
{qoq_text if qoq_text else "   （前回比データなし）"}

   ① ビジネスモデル
     上記KPI実績を踏まえて収益構造の変化・収益認識の質を分析する。
     数値が示す実態と経営陣のナラティブの整合性を論じること。

   ② 成長性
     KPIのYoY・QoQ（前回比）トレンドから成長の持続可能性を分析する。
     成長が加速・減速している部分の数値的根拠と背景理由を論じること。

   ③ 競争優位
     KPI数値が競合優位の維持・強化・侵食のどれを示しているか分析する。
     Web検索で競合動向を確認してKPI数値と対比して補完すること。

   ④ 経営
     KPI数値から経営陣の実行力・資本配分の妥当性を評価する。
     数値の変化が経営判断の結果として妥当かを論じること。

   ⑤ 市場環境
     マクロ環境（スコア{macro_score}・{macro_phase}）とKPIトレンドの
     整合性を分析する。KPI数値をマクロ変数と紐づけて解釈すること。

2. エントリーストーリーの進捗
   「{entry_story[:100]}」は現在どの程度実現しているか。
   Web検索で最新情報を確認して回答してください。

3. 世の中の注目ポイント
   Web検索でこの銘柄に対して市場・メディア・アナリストが今最も注目している点を調べてください。
   出典（メディア名・日付）を明示してください。

4. 歴史的類比
   {ticker}のビジネスモデル・成長軌跡・リスク構造が最も似ている歴史的企業を1社選んでください。
   その企業が最終的にどうなったか・何が転換点だったかを{ticker}のテーゼに当てはめて具体的に論じてください。
   一般的な類比（SaaSはSalesforce等）は禁止。テーゼ固有の課題に対応した類比を選んでください。

5. マクロ環境を踏まえた留意点
   現在のマクロ環境（スコア{macro_score}・{macro_phase}）がこの銘柄のテーゼに与える影響。

6. テーゼへの問いかけ
   以下の注意を守ってください：
   ・テーゼに書かれていることの言い換えになっている問いは禁止
   ・Koichiさんが見落としている盲点・反論・構造的リスクを3つ問いかけてください
   ・テーゼの前提そのものを揺るがす問いを作ってください
   ・過去の問いかけと重複する問いも禁止

7. 次回確認すべき論点
   次の四半期決算（{ticker} 次回）で実際に確認できる具体的な論点を
   優先度順に3つ挙げてください。
   過去の次回確認論点と重複するものは除いてください。

以下のJSON形式のみで回答してください（説明文・前置き不要）：
{{
  "five_perspectives": {{
    "business_model": "...",
    "growth": "...",
    "competitive_advantage": "...",
    "management": "...",
    "market": "..."
  }},
  "entry_story_progress": "...",
  "market_attention": "...",
  "historical_analogy": {{
    "company": "...",
    "similarity": "...",
    "outcome": "...",
    "implication": "..."
  }},
  "macro_implications": "...",
  "thesis_questions": ["問い1", "問い2", "問い3"],
  "next_review_focus": ["論点1", "論点2", "論点3"]
}}"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# キュー I/O
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_queue() -> Dict[str, Any]:
    if os.path.exists(REVIEW_QUEUE_PATH):
        with open(REVIEW_QUEUE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"queue": []}


def save_queue(queue: Dict[str, Any]) -> None:
    with open(REVIEW_QUEUE_PATH, "w", encoding="utf-8") as f:
        json.dump(queue, f, ensure_ascii=False, indent=2)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1銘柄レビュー生成
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _mark_older_reviews_not_latest(ticker: str) -> None:
    if not os.path.exists(REVIEWS_DIR):
        return
    for fname in os.listdir(REVIEWS_DIR):
        if fname.startswith(f"{ticker}_") and fname.endswith("_review.json"):
            fpath = os.path.join(REVIEWS_DIR, fname)
            try:
                with open(fpath, encoding="utf-8") as f:
                    existing = json.load(f)
                if existing.get("is_latest"):
                    existing["is_latest"] = False
                    with open(fpath, "w", encoding="utf-8") as f:
                        json.dump(existing, f, ensure_ascii=False, indent=2)
            except Exception:
                pass


def generate_review(entry: Dict[str, Any], dry_run: bool = False) -> Optional[str]:
    ticker     = entry["ticker"]
    quarter    = entry["quarter"]
    accn       = entry.get("accn", "")
    filed_date = entry.get("filed", "")
    now_jst    = datetime.now(JST)

    print(f"\n{'─' * 60}")
    print(f"  {ticker} {quarter} レビュー生成開始")
    print(f"{'─' * 60}")

    thesis     = load_thesis(ticker)
    kpi_data   = load_layer2_kpi(ticker)
    kpi_layer3 = load_layer3_kpi(ticker)
    macro_ctx  = load_macro_context()
    valuation  = load_tanuki_valuation(ticker)
    layer1     = load_layer1_financials(ticker)

    if not thesis:
        print(f"  [ERROR] {ticker} の thesis.json がないためスキップ")
        return None

    entry_price: Optional[float] = thesis.get("entry_price")
    if entry_price is None:
        entry_price = get_avg_cost(ticker)
        if entry_price is not None:
            print(f"  [INFO] entry_price: portfolio.jsonから取得 ${entry_price}")

    kpi_table    = build_kpi_table(kpi_data)    if kpi_data else "（KPIデータなし）"
    kpi_snapshot = build_kpi_snapshot(kpi_data) if kpi_data else {}
    macro_snapshot = {
        "score":          macro_ctx.get("score")          if macro_ctx else None,
        "phase":          macro_ctx.get("phase")          if macro_ctx else None,
        "stealth_signal": macro_ctx.get("stealth_signal") if macro_ctx else None,
    }

    # 過去health_score推移を取得（プロンプト構築前）
    past_health = _load_past_health_scores(ticker, quarter)
    if past_health:
        print(f"  [INFO] 過去健全度推移: {past_health}")

    # 過去予測振り返りテキストを取得
    past_predictions = _load_past_predictions(ticker)
    if past_predictions:
        print(f"  [INFO] 過去予測振り返り: 利用可能")

    stage1_prompt = build_stage1_prompt(
        thesis=thesis,
        kpi_table=kpi_table,
        macro_ctx=macro_ctx,
        valuation=valuation,
        ticker=ticker,
        quarter=quarter,
        entry_price=entry_price,
        kpi_data_layer2=kpi_data,
        kpi_data_layer3=kpi_layer3,
        past_health_scores=past_health,
        past_predictions=past_predictions,
    )

    if dry_run:
        print("\n=== [DRY-RUN] Stage 1 プロンプト（先頭1000文字） ===")
        print(stage1_prompt[:1000])
        print("...\n=== DRY-RUN 完了（Grok呼び出しなし） ===")
        return None

    # 成功フラグ
    call1_success:     bool = False
    stage2_json_valid: bool = False
    call2_success:     bool = False
    stage2:            Dict[str, Any] = {}
    call2:             Optional[Dict[str, Any]] = None

    # Stage 1 — 失敗時は例外を再 raise
    print(f"\n  ── Stage 1: テーゼ健全度評価 ({ticker} {quarter}) ──")
    try:
        stage1_raw = call_grok(
            user_prompt=stage1_prompt,
            system_prompt=STAGE1_SYSTEM,
            max_tokens=2000,
            temperature=0.3,
        )
        stage1 = extract_json_from_response(stage1_raw)
        call1_success = True
        print(f"  → health_score={stage1.get('health_score')}, recommendation={stage1.get('recommendation')}")
    except Exception as e:
        print(f"  [ERROR] Stage 1 失敗: {e}")
        raise

    # Stage 2 — 失敗しても継続
    effective_kpis = _get_effective_thesis_kpis(thesis)
    stage2_prompt = build_stage2_prompt(
        ticker, quarter, kpi_table, stage1, kpi_data,
        thesis_kpis=effective_kpis, layer1=layer1,
    )
    print(f"\n  ── Stage 2: DCFパラメータ生成 ({ticker} {quarter}) ──")
    try:
        stage2_raw = call_grok(
            user_prompt=stage2_prompt,
            system_prompt=STAGE2_SYSTEM,
            max_tokens=3000,
            temperature=0.2,
        )
        stage2 = extract_json_from_response(stage2_raw)
        stage2_json_valid = True
        print(f"  → シナリオ: {list(stage2.get('scenarios', {}).keys())}")
    except Exception as e:
        print(f"  [WARN] Stage 2 失敗（スキップ）: {e}")

    # Call 2 — 定性分析、失敗しても継続
    call2_kpi_table = (
        _build_kpi_status_table(effective_kpis, kpi_data, kpi_layer3, quarter)
        if effective_kpis else ""
    )
    past_call2_items = _load_past_call2(ticker, quarter)
    if past_call2_items:
        print(f"  [INFO] 過去Call2引き継ぎ: {[x['quarter'] for x in past_call2_items]}")
    call2_prompt = build_call2_prompt(
        ticker, quarter, thesis, stage1, macro_ctx,
        kpi_status_table=call2_kpi_table,
        past_call2=past_call2_items,
        kpi_data=kpi_data,
        kpi_data_layer3=kpi_layer3,
        past_health_scores=past_health,
    )
    print(f"\n  ── Call 2: 定性分析 ({ticker} {quarter}) ──")
    try:
        call2_raw = call_grok(
            user_prompt=call2_prompt,
            system_prompt=CALL2_SYSTEM,
            max_tokens=3000,
            temperature=0.5,
        )
        call2 = extract_json_from_response(call2_raw)
        call2_success = True
        print(f"  → 5観点・歴史的類比・問いかけ 生成完了")
    except Exception as e:
        print(f"  [WARN] Call 2 失敗（スキップ）: {e}")

    # 同一tickerの既存レビューをis_latest=falseに更新してから保存
    _mark_older_reviews_not_latest(ticker)

    os.makedirs(REVIEWS_DIR, exist_ok=True)
    output_path = os.path.join(REVIEWS_DIR, f"{ticker}_{quarter}_review.json")

    review = {
        "ticker":               ticker,
        "quarter":              quarter,
        "generated_at":         now_jst.isoformat(),
        # idempotencyフィールド
        "data_version":         accn,
        "filed_date":           filed_date,
        "is_latest":            True,
        "layer1_complete":      bool(accn),
        "layer2_complete":      kpi_data.get("layer2_complete", False) if kpi_data else False,
        "layer2_missing_kpis":  kpi_data.get("missing_kpis", []) if kpi_data else [],
        "stage2_json_valid":    stage2_json_valid,
        "grok_call1_success":   call1_success,
        "grok_call2_success":   call2_success,
        "transcript_available": False,
        # コンテンツ
        "stage1":          stage1,
        "stage2":          stage2,
        "call2":           call2,
        "kpi_snapshot":    kpi_snapshot,
        "layer3_snapshot": kpi_layer3.get("kpis", {}) if kpi_layer3 else {},
        "macro_snapshot":  macro_snapshot,
        "thesis_version":  thesis.get("version", 1),
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(review, f, ensure_ascii=False, indent=2)

    print(f"\n  ✓ レビュー保存: {output_path}")
    return output_path


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# エントリーポイント
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    parser = argparse.ArgumentParser(description="TANUKI TAIL — 四半期レビュー生成")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Grok呼び出しをスキップしてプロンプト内容を確認する",
    )
    args = parser.parse_args()

    now_jst = datetime.now(JST)
    print(f"TANUKI TAIL 四半期レビュー生成 — {now_jst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 60)

    queue   = load_queue()
    pending = [e for e in queue.get("queue", []) if e.get("status") == "pending"]

    if not pending:
        print("pending エントリなし → 終了")
        sys.exit(0)

    print(f"pending: {len(pending)} 件")
    for e in pending:
        print(f"  {e['ticker']} {e['quarter']} (filed: {e.get('filed', '')})")

    generated: List[str] = []
    failed:    List[str] = []

    for entry in pending:
        try:
            output_path = generate_review(entry, dry_run=args.dry_run)
            if output_path:
                entry["status"]       = "completed"
                entry["completed_at"] = now_jst.isoformat()
                entry["review_path"]  = output_path
                generated.append(f"{entry['ticker']} {entry['quarter']}")
                if not args.dry_run:
                    try:
                        _tail_dir = os.path.dirname(os.path.abspath(__file__))
                        if _tail_dir not in sys.path:
                            sys.path.insert(0, _tail_dir)
                        from tail_dcf_bridge import generate_scenario_files as _dcf_gen
                        _dcf_gen(entry["ticker"])
                    except Exception as _bridge_e:
                        print(f"  [WARN] DCF bridge 失敗（スキップ）: {_bridge_e}")
        except Exception as e:
            print(f"  [ERROR] {entry['ticker']} {entry['quarter']} 失敗: {e}")
            entry["status"] = "error"
            entry["error"]  = str(e)
            failed.append(f"{entry['ticker']} {entry['quarter']}")

    if not args.dry_run:
        save_queue(queue)
        print("\n✓ review_queue.json 更新完了")

    print(f"\n{'━' * 60}")
    print(f"完了: 生成 {len(generated)} 件 / 失敗 {len(failed)} 件")
    for g in generated:
        print(f"  OK: {g}")
    for f_item in failed:
        print(f"  NG: {f_item}")


if __name__ == "__main__":
    main()
