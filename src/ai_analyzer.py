from openai import OpenAI
import os

def analyze_adjustments(ticker, data, adjustments):
    client = OpenAI(
        api_key=os.environ.get("XAI_API_KEY"),
        base_url="https://api.x.ai/v1",
    )

    # 調整内訳を文字列化（f-string外で\nを扱う）
    adj_lines = []
    for adj in adjustments:
        snippet = adj.get("context_snippet", "N/A")
        adj_lines.append(f"- {adj['item_name']}: {adj['net_amount']:,} USD (理由: {adj['reason']}) Snippet: {snippet}")

    adj_text = "\n".join(adj_lines)   # ← ここで\nを結合（f-stringの外）

    prompt = f"""
あなたは米国株のNon-GAAP調整専門のシニアアナリストです。
銘柄: {ticker}
GAAP純利益: {data['gaap_net_income']:,} USD
調整後純利益: {data['adjusted_net_income']:,} USD
調整項目詳細:
{adj_text}

以下の4段階で健全性を評価してください：
- Excellent: 調整が極めて合理的で本質的成長を示唆
- Good: 標準的な調整、問題なし
- Caution: 一部調整が恣意的or連続発生
- Warning: 調整が利益水増しに見える、または連続Caution

評価理由を日本語で150-250文字。各調整項目に引用した原文snippetを必ず明記。
連続Cautionが複数四半期なら強調。
出力はJSON形式で:
{{
  "health": "Excellent|Good|Caution|Warning",
  "comment": "詳細解説...",
  "sources": [{{"item": "リストラ費用", "snippet": "..."}}, ...]
}}
"""

    response = client.chat.completions.create(
        model="grok-4.20-beta-0309-reasoning",  # 必要に応じて最新モデル名に変更
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
        max_tokens=800
    )

    try:
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f'{{"health": "Error", "comment": "API call failed: {str(e)}"}}'
