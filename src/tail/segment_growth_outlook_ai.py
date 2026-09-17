"""
TANUKI TAIL - Segment Growth Outlook (AI)
[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1]]: 10-K/10-Q Item 7
（MD&A）本文から、経営陣が言及しているセグメント別（または主要事業
カテゴリ別）の成長見通し・業績動向に関する定性的な記述を抽出する。

責務: 抽出結果はreport.txt上の参考表示専用。DCF/IV計算には一切使用
しない（呼び出し元はsec_items_fetcher.py::fetch_annual()/fetch_
quarterly_updates()内で、既存のexcerpt/excerpt_ja生成を一切変更せず、
その後に追加でこのモジュールを呼び出すのみ）。

設計方針（調査フェーズ・2026-09-18確定、Koichiさん承認済み）:
- src/value/tanuki_valuation/calculator/fcf_outlier_ai.py（[[FCF-
  OUTLIER-QUAL-1]]）と同型の設計（モデル名・エンドポイント・リトライ・
  temperature・JSON抽出処理）を踏襲する。ディレクトリを跨ぐ直接import
  はモジュール結合を強めるため行わず、同一パターンを本モジュールに
  複製する（既存コードベースの慣習: 各サブシステムが独立してAI呼び出し
  コードを持つ）
- キーワードナビゲーション方式は不採用。調査フェーズでSOFI（セグメント
  記述が約12000〜14000文字目）・NVDA（約18000〜19300文字目）・TSLA
  （約32000〜41000文字目、"Segment"という単語自体を使わない）で
  位置パターンに一貫性がないことを実データで確認済み。代わりに、
  extract_item_section()が切り詰め前に生成する`section_text`
  （MD&A全文、最大60000文字）をそのまま1回のAI呼び出しに渡し、
  セグメントの特定自体もAIに任せる設計とした（SOFI・TSLAの両ケースで
  実データ検証済み、TSLAの"Segment"という語を使わない難しいケースでも
  Automotive/Energy Generation and Storageを正しく特定できることを
  確認済み）
- 出力スキーマは数値（成長率等）を一切含めない。セグメント名・
  trend（定性的方向感）・summary（要約）・quote（原文抜粋）のみとし、
  DCF計算への誤用を構造的に防ぐ（FCF-OUTLIER-QUAL-1のassessment設計
  と同じ哲学）
- モデルは[[GROK-MODEL-PRICE-1]]確定済みのgrok-4.3をそのまま使う
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

_VALID_TRENDS = {"accelerating", "stable", "decelerating", "mixed", "uncertain"}

# MD&A全文を渡すため入力が長く応答生成に時間がかかりやすい
# （sec_ctrl_fetcher.py::_translate_excerpt()のmda項目120秒指定と同型の配慮）
_TIMEOUT_SECONDS = 120

_SYSTEM_PROMPT = (
    "あなたは株式投資の財務分析アシスタントです。企業の10-K/10-Q "
    "Item 7（MD&A）本文から、経営陣が言及しているセグメント別（または"
    "主要事業カテゴリ別）の成長見通し・業績動向に関する記述を抽出して"
    "ください。数値化された成長率の算出や予測は行わず、経営陣が実際に"
    "本文中で述べている定性的な記述内容を要約してください。セグメント名は"
    "本文中の実際の表記をそのまま使ってください（"
    "\"Segment\"という単語が本文になくても、Automotive/Energy等の"
    "主要事業カテゴリ別の記述があればそれをセグメントとして扱って"
    "構いません）。読み取れるセグメント・事業カテゴリ別の記述が本文に"
    "見当たらない場合は空配列を返してください。必ず次のJSON形式のみで"
    "回答し、他の文章は含めないでください: "
    '{"segments": [{"name": "セグメント名", "trend": "accelerating '
    'または stable または decelerating または mixed または uncertain'
    '（本文の記述から読み取れるトレンド方向）", "summary": "60〜150字'
    '程度の日本語で経営陣の発言内容を要約", "quote": "根拠となった原文の'
    '該当箇所を30語程度以内で抜粋（英語のまま）"}]}'
)


def call_grok(
    user_prompt: str,
    system_prompt: str = "",
    max_tokens: int = 1500,
    temperature: float = 0.3,
) -> str:
    """[[fcf_outlier_ai.py::call_grok()]]と同型実装
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
                timeout=_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            last_error = e
            time.sleep(1)
    raise RuntimeError(f"すべてのGrokモデルで失敗: {last_error}")


def extract_json_from_response(text: str) -> Dict[str, Any]:
    """fcf_outlier_ai.py::extract_json_from_response()と同型実装"""
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        return json.loads(m.group(1))
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        return json.loads(text[start:end])
    raise ValueError(f"JSONが見つかりません: {text[:300]}")


def extract_segment_growth_outlook(
    ticker: str,
    section_text: str,
) -> Optional[List[Dict[str, Any]]]:
    """MD&A本文（section_text、切り詰め前）からセグメント別成長見通しを
    抽出する。

    Args:
        ticker: 銘柄コード
        section_text: extract_item_section()が返すMD&A本文
            （sec_items_fetcher.pyの既存excerpt/excerpt_ja生成用の
            [:2000]切り詰めより前の全文。最大60000文字を想定）

    Returns:
        [{"name": str, "trend": str, "summary": str, "quote": str}, ...]
        または、呼び出し不能・失敗時・該当セグメントなしの場合はNone
        （section_textが空の場合も呼び出し自体を行わずNoneを返す）
    """
    if not section_text or not section_text.strip():
        return None

    try:
        raw = call_grok(section_text, system_prompt=_SYSTEM_PROMPT, temperature=0.3)
        parsed = extract_json_from_response(raw)
        segments = parsed.get("segments", [])
        if not isinstance(segments, list) or not segments:
            return None

        result = []
        for seg in segments:
            if not isinstance(seg, dict):
                continue
            trend = seg.get("trend")
            if trend not in _VALID_TRENDS:
                trend = "uncertain"
            name = str(seg.get("name", "")).strip()
            if not name:
                continue
            result.append({
                "name": name,
                "trend": trend,
                "summary": str(seg.get("summary", "")).strip(),
                "quote": str(seg.get("quote", "")).strip(),
            })
        return result if result else None
    except Exception as e:
        print(f"   [{ticker}] セグメント別成長見通しのAI抽出に失敗（参考情報なしで継続）: {e}")
        return None
