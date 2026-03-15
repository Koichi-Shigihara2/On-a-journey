"""
ai_analyzer.py
AI分析モジュール（XAI API利用）
- 調整項目リストを分析し、健全性・コメント・引用ソースを返す
- 調整項目がない場合は早期に「調整なし」レスポンスを返す
- 戻り値はJSON文字列（pipeline.py が json.loads する想定）
- プロンプトは config/prompts.yaml から読み込む
"""
import json
import os
import yaml
import requests
from typing import List, Dict, Any

CONFIG_DIR = "config"
PROMPTS_FILE = os.path.join(CONFIG_DIR, "prompts.yaml")

# デフォルトプロンプト（ファイルがない場合のフォールバック）
DEFAULT_PROMPT = """
あなたは財務分析のエキスパートです。以下の調整項目リストを分析し、健全性とコメントを返してください。
ティッカー: {ticker}
期: {fiscal_period}
GAAP EPS: {gaap_eps}
Adjusted EPS: {adjusted_eps}
調整項目: {adjustments_json}

以下のJSON形式で返してください：
{{
  "health": "Excellent/Good/Caution/Warning/Error",
  "comment": "分析コメント（日本語）",
  "sources": [
    {{"item": "項目名", "snippet": "引用テキスト"}}
  ]
}}
"""

def load_prompt() -> str:
    """config/prompts.yaml から分析プロンプトを読み込む"""
    try:
        with open(PROMPTS_FILE, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            prompt = config.get('adjustment_analysis', DEFAULT_PROMPT)
            return prompt
    except FileNotFoundError:
        print(f"Warning: {PROMPTS_FILE} not found. Using default prompt.")
        return DEFAULT_PROMPT
    except Exception as e:
        print(f"Error loading prompt: {e}. Using default.")
        return DEFAULT_PROMPT

# ★★★ XAI API用の設定 ★★★
XAI_API_KEY = os.environ.get("XAI_API_KEY")  # GitHub Secretsのキー
XAI_API_URL = "https://api.x.ai/v1/chat/completions"
XAI_MODEL = "grok-4.20-beta-0309-reasoning"  # 画像にあるモデル名

def analyze_adjustments(ticker: str, fiscal_period_data: Dict[str, Any], adjustments: List[Dict[str, Any]]) -> str:
    if not adjustments:
        return json.dumps({
            "health": "Good",
            "comment": "調整項目はありません。GAAP EPSがそのまま実質EPSと見なせます。",
            "sources": []
        }, ensure_ascii=False)

    if not XAI_API_KEY:
        return json.dumps({
            "health": "Caution",
            "comment": "AI分析にはXAI_API_KEY環境変数が必要です。",
            "sources": []
        }, ensure_ascii=False)

    fiscal_period = fiscal_period_data.get('filing_date', 'unknown')
    gaap_eps = fiscal_period_data.get('gaap_eps', 0)
    adjusted_eps = fiscal_period_data.get('adjusted_eps', 0)

    prompt_template = load_prompt()
    prompt = prompt_template.format(
        ticker=ticker,
        fiscal_period=fiscal_period,
        gaap_eps=gaap_eps,
        adjusted_eps=adjusted_eps,
        adjustments_json=json.dumps(adjustments, ensure_ascii=False, indent=2)
    )

    try:
        response = requests.post(
            XAI_API_URL,
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": XAI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "response_format": {"type": "json_object"}
            },
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        content = result['choices'][0]['message']['content']
        parsed = json.loads(content)
        return json.dumps(parsed, ensure_ascii=False)
    except Exception as e:
        error_msg = str(e)
        return json.dumps({
            "health": "Error",
            "comment": f"AI分析中にエラーが発生しました: {error_msg}",
            "sources": []
        }, ensure_ascii=False)
