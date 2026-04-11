"""
TANUKI VALUATION - AI Calculation Validator
Gemini APIを使用して計算結果の妥当性を検証

検証項目:
1. P_t / shares = intrinsic_value_per_share 整合性
2. pv_high + pv_terminal = v0 検証
3. 算式の正確性検証
4. 異常値検出（極端な乖離率など）
"""

import os
import json
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# Gemini API設定
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_ENDPOINT = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"


def build_validation_prompt(ticker: str, data: Dict[str, Any]) -> str:
    """検証用プロンプトを構築"""
    
    c = data.get("components", {})
    
    # 基礎数値の抽出
    ivps = data.get("intrinsic_value_per_share", 0)
    v0 = data.get("v0", 0)
    alpha = data.get("alpha", 0)
    
    pv_high = c.get("pv_high", 0)
    pv_terminal = c.get("pv_terminal", 0)
    rpo_pv = c.get("rpo_pv", 0)
    diluted_shares = c.get("diluted_shares", 0)
    current_price = c.get("current_price", 0)
    fcf_5yr_avg = c.get("fcf_5yr_avg", 0)
    roe_avg = c.get("roe_10yr_avg", c.get("roe_used", 0))
    high_growth_rate = c.get("high_growth_rate_used", 0.15)
    
    # 企業価値 P_t
    total_v0 = v0 + rpo_pv
    p_t = total_v0 * (1 + alpha)
    
    # 乖離率
    divergence = ((ivps - current_price) / current_price * 100) if current_price > 0 else 0
    
    prompt = f"""あなたはDCFバリュエーションの専門家です。以下の計算結果を検証してください。

## 銘柄: {ticker}

## Koichi式 v5.2 算式

```
1. FCF 5年平均: FCF_avg = Σ(OCF - CapEx) / 5
2. 高成長率: 15%〜50%でクリップしたCAGR
3. 2段階DCF:
   V₀ = Σ[FCF×(1+g)^t / (1+WACC)^t] + TV/(1+WACC)^n
   - 高成長期: 3年間、企業別CAGR
   - ターミナル: 永続成長率3%
   - WACC: 8.5%
4. 成長期待プレミアム:
   α = min(1.0, max(0, (ROE × 内部留保率60% / WACC) × 0.7))
5. 本質的価値:
   P_t = (V₀ + RPO_PV) × (1 + α)
   1株当り = P_t / 希薄化後株式数
```

## 入力数値

| 項目 | 値 |
|------|-----|
| FCF 5年平均 | ${fcf_5yr_avg:,.0f} |
| ROE平均 | {roe_avg*100:.2f}% |
| 高成長率 | {high_growth_rate*100:.1f}% |
| WACC | 8.5% |
| ターミナル成長率 | 3.0% |
| 希薄化後株式数 | {diluted_shares:,.0f} |

## 計算結果

| 項目 | 値 |
|------|-----|
| PV (高成長期) | ${pv_high:,.0f} |
| PV (ターミナル) | ${pv_terminal:,.0f} |
| V₀ | ${v0:,.0f} |
| RPO現在価値 | ${rpo_pv:,.0f} |
| α | {alpha:.4f} |
| 企業価値 P_t | ${p_t:,.0f} |
| 1株当り本質価値 | ${ivps:.2f} |
| 現在市場価格 | ${current_price:.2f} |
| 乖離率 | {divergence:+.1f}% |

## 検証項目

以下4項目を検証し、JSON形式で回答してください:

1. **pt_shares_consistency**: P_t / shares = intrinsic_value_per_share が正しいか
2. **dcf_components**: pv_high + pv_terminal ≈ v0 が成立するか
3. **formula_verification**: 算式が正しく適用されているか（α計算、DCF計算など）
4. **anomaly_detection**: 異常値がないか（乖離率が極端すぎる、株式数が異常など）

## 回答形式（JSONのみ、他のテキストは不要）

```json
{{
  "checks": {{
    "pt_shares_consistency": {{"pass": true/false, "detail": "検証詳細"}},
    "dcf_components": {{"pass": true/false, "detail": "検証詳細"}},
    "formula_verification": {{"pass": true/false, "detail": "検証詳細"}},
    "anomaly_detection": {{"pass": true/false, "detail": "検証詳細"}}
  }},
  "overall": "PASS/WARN/FAIL",
  "ai_comment": "総合評価コメント（50字以内）"
}}
```
"""
    return prompt


def call_gemini_api(prompt: str) -> Optional[Dict[str, Any]]:
    """Gemini APIを呼び出して検証結果を取得"""
    
    if not GEMINI_API_KEY:
        print("[WARN] GEMINI_API_KEY not set, skipping AI validation")
        return None
    
    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 1024
        }
    }
    
    try:
        url = f"{GEMINI_ENDPOINT}?key={GEMINI_API_KEY}"
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        
        # JSONを抽出
        json_start = text.find("{")
        json_end = text.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            json_str = text[json_start:json_end]
            return json.loads(json_str)
        
        print(f"[WARN] Could not parse JSON from Gemini response: {text[:200]}")
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Gemini API request failed: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse Gemini response as JSON: {e}")
        return None


def run_basic_checks(ticker: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """基本的な整合性チェック（API不要）"""
    
    c = data.get("components", {})
    
    ivps = data.get("intrinsic_value_per_share", 0)
    v0 = data.get("v0", 0)
    alpha = data.get("alpha", 0)
    
    pv_high = c.get("pv_high", 0)
    pv_terminal = c.get("pv_terminal", 0)
    rpo_pv = c.get("rpo_pv", 0)
    diluted_shares = c.get("diluted_shares", 0)
    current_price = c.get("current_price", 0)
    
    checks = {}
    
    # 1. P_t / shares 整合性
    if diluted_shares > 0:
        total_v0 = v0 + rpo_pv
        p_t = total_v0 * (1 + alpha)
        calculated_ivps = p_t / diluted_shares
        diff_pct = abs(calculated_ivps - ivps) / ivps * 100 if ivps > 0 else 0
        
        checks["pt_shares_consistency"] = {
            "pass": diff_pct < 1.0,  # 1%未満なら合格
            "detail": f"P_t ${p_t/1e9:.2f}B / {diluted_shares/1e9:.3f}B = ${calculated_ivps:.2f} (差異{diff_pct:.2f}%)"
        }
    else:
        checks["pt_shares_consistency"] = {"pass": False, "detail": "株式数が0"}
    
    # 2. DCF構成要素
    v0_calculated = pv_high + pv_terminal
    diff_v0 = abs(v0_calculated - v0) / v0 * 100 if v0 > 0 else 0
    
    checks["dcf_components"] = {
        "pass": diff_v0 < 1.0,
        "detail": f"pv_high ${pv_high/1e9:.2f}B + pv_terminal ${pv_terminal/1e9:.2f}B = ${v0_calculated/1e9:.2f}B (差異{diff_v0:.2f}%)"
    }
    
    # 3. 算式検証（基本チェック）
    roe_avg = c.get("roe_10yr_avg", c.get("roe_used", 0))
    wacc = 0.085
    retention = 0.6
    g_individual = roe_avg * retention
    alpha_calculated = min(1.0, max(0, (g_individual / wacc) * 0.7))
    alpha_diff = abs(alpha_calculated - alpha)
    
    checks["formula_verification"] = {
        "pass": alpha_diff < 0.01,
        "detail": f"α計算: ROE {roe_avg*100:.1f}% × 60% / 8.5% × 0.7 = {alpha_calculated:.4f} (実際{alpha:.4f})"
    }
    
    # 4. 異常値検出
    anomalies = []
    divergence = ((ivps - current_price) / current_price * 100) if current_price > 0 else 0
    
    if abs(divergence) > 500:
        anomalies.append(f"乖離率{divergence:+.0f}%が極端")
    if diluted_shares < 1000000:
        anomalies.append(f"株式数{diluted_shares:,}が異常に少ない")
    if ivps > 10000:
        anomalies.append(f"理論株価${ivps:.0f}が異常に高い")
    
    checks["anomaly_detection"] = {
        "pass": len(anomalies) == 0,
        "detail": "、".join(anomalies) if anomalies else "異常値なし"
    }
    
    # 総合判定
    all_pass = all(c["pass"] for c in checks.values())
    any_fail = any(not c["pass"] for c in checks.values())
    
    overall = "PASS" if all_pass else ("FAIL" if checks["anomaly_detection"]["pass"] == False else "WARN")
    
    return {
        "validated_at": datetime.now().strftime("%Y-%m-%d"),
        "model": "basic_checks",
        "checks": checks,
        "overall": overall,
        "ai_comment": None
    }


def validate_calculation(ticker: str, data: Dict[str, Any], use_ai: bool = True) -> Dict[str, Any]:
    """
    計算結果を検証
    
    Args:
        ticker: ティッカーシンボル
        data: latest.jsonのデータ
        use_ai: AI検証を使用するか
    
    Returns:
        検証結果のDict
    """
    
    # まず基本チェックを実行
    validation = run_basic_checks(ticker, data)
    
    # AI検証（オプション）
    if use_ai and GEMINI_API_KEY:
        prompt = build_validation_prompt(ticker, data)
        ai_result = call_gemini_api(prompt)
        
        if ai_result:
            validation["model"] = GEMINI_MODEL
            validation["checks"] = ai_result.get("checks", validation["checks"])
            validation["overall"] = ai_result.get("overall", validation["overall"])
            validation["ai_comment"] = ai_result.get("ai_comment")
    
    return validation


if __name__ == "__main__":
    # テスト用
    test_data = {
        "intrinsic_value_per_share": 71.22,
        "v0": 236712227595,
        "alpha": 0.0617,
        "components": {
            "pv_high": 25000000000,
            "pv_terminal": 211712227595,
            "rpo_pv": 8450000000,
            "diluted_shares": 3528000000,
            "current_price": 348.95,
            "fcf_5yr_avg": 5000000000,
            "roe_10yr_avg": 0.12,
            "high_growth_rate_used": 0.25
        }
    }
    
    result = validate_calculation("TSLA", test_data, use_ai=False)
    print(json.dumps(result, indent=2, ensure_ascii=False))
