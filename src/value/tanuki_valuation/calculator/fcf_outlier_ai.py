"""
TANUKI VALUATION - FCF Outlier Qualitative Assessment (AI)
[[FCF-OUTLIER-QUAL-1]]案B: 一過性費用の内容に関するAI定性評価

責務: FCF外れ値の一過性費用（transient_evidence.items）の内容が
「事業の一時的な問題」か「構造的な問題の兆候」かをAIに評価させる。

設計方針（[[FCF-OUTLIER-QUAL-1]]確定方針・案B）:
- 本モジュールの結果はreport.txt上の参考表示専用。fcf_outlier.action判定・
  DCF計算（adjustments.py::estimate_fcf_from_eps()のskip_guard_a等）には
  一切使用しない。呼び出し元（core_calculator.py）はFCFOutlierResult.
  action等を全て決定し終えた"後"に、本モジュールの結果を
  FCFOutlierResult.to_dict(ai_assessment=...)へ追加で渡すのみ
- 既存の src/tail/quarterly_review_generator.py::call_grok() と同型の
  設計（モデル名・エンドポイント・リトライ・タイムアウト・
  temperature・JSON抽出処理）を踏襲する。ディレクトリを跨ぐ直接import
  はモジュール結合を強めるため行わず、同一パターンを本モジュールに
  複製する（既存コードベースの慣習: 各サブシステムが独立してAI呼び出し
  コードを持つ、[[GROK-MODEL-PRICE-1]]で確認した9ファイル分と同型）
- モデルは[[GROK-MODEL-PRICE-1]]確定済みのgrok-4.3をそのまま使う
  （新しいモデル名は導入しない）
- AI呼び出し失敗・タイムアウト・JSON解析失敗時は例外を外へ伝播させず
  Noneを返す（フェイルセーフ、パイプライン全体を止めない）
"""

import json
import os
import re
import time
from typing import Any, Dict, List, Optional

import requests

XAI_API_KEY = os.getenv("XAI_API_KEY", "")
GROK_URL = "https://api.x.ai/v1/chat/completions"
# [[GROK-MODEL-PRICE-1]]確定済みの現行モデル名をそのまま使う
GROK_MODELS = ["grok-4.3", "grok-4.3", "grok-4.3"]

_VALID_ASSESSMENTS = {"transient", "structural_concern", "uncertain"}

_SYSTEM_PROMPT = (
    "あなたは株式投資の財務分析アシスタントです。企業のFCF（フリー"
    "キャッシュフロー）が過去実績から大きく乖離した年度について、"
    "EPS Analyzerが検出した一過性費用（本業以外の一時的な費用）の"
    "内容を提示するので、その内容が「事業運営上の一時的な問題」なのか"
    "「収益力の構造的な悪化を示す兆候」なのかを判定してください。"
    "金額の大小ではなく、費用の性質・理由（reason欄）の記述内容から"
    "判断してください。必ず次のJSON形式のみで回答し、他の文章は"
    "含めないでください: "
    '{"assessment": "transient または structural_concern または '
    'uncertain（判断材料が不十分な場合）", "reasoning": "40〜120字程度の'
    '日本語で判断根拠を簡潔に説明"}'
)


def call_grok(
    user_prompt: str,
    system_prompt: str = "",
    max_tokens: int = 500,
    temperature: float = 0.3,
) -> str:
    """[[quarterly_review_generator.py::call_grok()]]と同型実装
    （フォールバックループ・タイムアウト・エラーハンドリングを踏襲）。"""
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
            resp = requests.post(
                GROK_URL,
                headers=headers,
                json={
                    "model": model,
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                },
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            last_error = e
            time.sleep(1)
    raise RuntimeError(f"すべてのGrokモデルで失敗: {last_error}")


def extract_json_from_response(text: str) -> Dict[str, Any]:
    """quarterly_review_generator.py::extract_json_from_response()と同型実装"""
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        return json.loads(m.group(1))
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        return json.loads(text[start:end])
    raise ValueError(f"JSONが見つかりません: {text[:300]}")


def _build_prompt(ticker: str, transient_items: List[Dict[str, Any]]) -> str:
    lines = [f"銘柄: {ticker}", "", "一過性費用として検出された項目:"]
    for i, item in enumerate(transient_items, 1):
        amt = item.get("amount", 0)
        lines.append(
            f"{i}. カテゴリ: {item.get('category', '')} / "
            f"項目名: {item.get('item_name', '')} / "
            f"金額: ${amt/1e6:,.1f}M / "
            f"理由: {item.get('reason', '')}"
        )
    return "\n".join(lines)


def assess_transient_qualitative(
    ticker: str,
    transient_items: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """一過性費用項目の内容をAIに定性評価させる。

    Args:
        ticker: 銘柄コード
        transient_items: FCFOutlierResult.transient_items
            （category/item_name/amount/reasonを持つ辞書のリスト）

    Returns:
        {"assessment": "transient"|"structural_concern"|"uncertain",
         "reasoning": "..."} または、呼び出し不能・失敗時はNone
        （transient_itemsが空の場合も呼び出し自体を行わずNoneを返す）
    """
    if not transient_items:
        return None

    try:
        user_prompt = _build_prompt(ticker, transient_items)
        raw = call_grok(user_prompt, system_prompt=_SYSTEM_PROMPT, temperature=0.3)
        parsed = extract_json_from_response(raw)
        assessment = parsed.get("assessment")
        if assessment not in _VALID_ASSESSMENTS:
            return None
        reasoning = str(parsed.get("reasoning", "")).strip()
        return {"assessment": assessment, "reasoning": reasoning}
    except Exception as e:
        print(f"   [{ticker}] FCF一過性費用のAI定性評価に失敗（参考情報なしで継続）: {e}")
        return None
