"""
TANUKI VALUATION - AI Calculation Validator v6.2
xAI API (Grok) を使用して計算結果の妥当性を検証

v6.2 変更点:
  - 動的WACC（CAPM計算値）に対応
  - 3段階DCFに対応（pv_phase1 + pv_phase2 + pv_terminal）
  - 銘柄別terminal_growth / high_growth_years に対応
  - 成長オプション・FCFベース判定結果を検証プロンプトに含める

検証項目:
1. P_t / shares = intrinsic_value_per_share 整合性
2. DCF構成要素の整合性（2段階 / 3段階）
3. 算式の正確性検証（実際に使用されたWACCで再計算）
4. 異常値検出
"""

import os
import json
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# [[VALIDATOR-ALPHA-CAP-STALE-1]]対応（2026-08-20）: alpha_capの
# セクター別・業種別解決ロジックはcore_calculator.py側に一本化し、
# validator.pyでは複製せずimportして使う（同一ロジックを2箇所に
# 独立実装すると将来core_calculator.py側の判定が変わった際に
# 再び乖離するため）。pipeline.pyがvalidator.pyと同一ディレクトリの
# flatインポートで動作させる前提を踏襲し、同じ方式でimportする。
from core_calculator import resolve_alpha_cap, DEFAULT_ALPHA_CAP

# xAI API設定
XAI_API_KEY = os.environ.get("XAI_API_KEY", "")
# [[GROK-MODEL-PRICE-1]]対応: grok-3-miniは実際にはgrok-4.3へ自動
# ルーティングされる（実測確認済み）ため、実態に合わせ現行モデル名を
# 明示する。
XAI_MODEL = "grok-4.3"
XAI_ENDPOINT = "https://api.x.ai/v1/chat/completions"


def _extract_params(data: Dict[str, Any], ticker: str = "") -> Dict[str, Any]:
    """
    latest.jsonから検証に必要なパラメータを動的抽出

    v6.x対応: ハードコード値ではなく実際の計算値を使用
    """
    c = data.get("components", {})

    # WACC（動的）
    wacc = data.get("wacc", {}).get("value", c.get("wacc", 0.085))

    # DCFパラメータ
    high_growth_years = c.get("high_growth_years", 5)
    terminal_growth = c.get("terminal_growth_used", 0.03)
    high_growth_rate = c.get("high_growth_rate_used", 0.25)

    # α計算パラメータ
    retention_rate = 0.60
    discount_factor = 0.7
    # [[VALIDATOR-ALPHA-CAP-STALE-1]]対応: 固定値1.0ではなく
    # core_calculator.pyと同じresolve_alpha_cap()でセクター別・
    # 業種別の実際の上限を解決する。
    alpha_cap = resolve_alpha_cap(
        ticker, c.get("sector"), c.get("industry"), DEFAULT_ALPHA_CAP
    )

    # DCFタイプと内訳
    dcf_type = data.get("dcf_type", "two_stage")
    dcf_components = data.get("dcf_components", {})

    # RM基準（βなし・市場期待リターン）: α計算に使用
    rm = data.get("wacc", {}).get("market_return", 0.10)

    return {
        "wacc": wacc,
        "rm": rm,
        "high_growth_years": high_growth_years,
        "terminal_growth": terminal_growth,
        "high_growth_rate": high_growth_rate,
        "retention_rate": retention_rate,
        "discount_factor": discount_factor,
        "alpha_cap": alpha_cap,
        "dcf_type": dcf_type,
        "dcf_components": dcf_components,
    }


def build_validation_prompt(ticker: str, data: Dict[str, Any]) -> str:
    """検証用プロンプトを構築（v6.x対応）"""

    c = data.get("components", {})
    p = _extract_params(data, ticker)

    ivps = data.get("intrinsic_value_per_share", 0)
    # プロンプト内ではβ込みWACC基準V0を使用（DCF構成要素との整合性のため）
    v0 = data.get("v0", 0)
    alpha = data.get("alpha", 0)

    pv_high = c.get("pv_high", 0)
    pv_terminal = c.get("pv_terminal", 0)
    rpo_pv = c.get("rpo_pv", 0)
    growth_option_pv = c.get("growth_option_pv", 0)
    diluted_shares = c.get("diluted_shares", 0)
    current_price = c.get("current_price", 0)
    fcf_base_used = c.get("fcf_base_used", c.get("fcf_5yr_avg", 0))
    fcf_base_method = c.get("fcf_base_method", "avg_5yr")
    roe_avg = c.get("roe_10yr_avg") or c.get("roe_used") or 0.0

    # ALPHA-REDESIGN-1: core_calculator.pyはP_t算出にalphaを乗算しない
    # （calculate_intrinsic_value(..., alpha=0.0, ...)固定）。alphaは参考値。
    total_v0 = v0 + rpo_pv + growth_option_pv
    p_t = total_v0
    divergence = ((ivps - current_price) / current_price * 100) if current_price > 0 else 0

    if p["dcf_type"] == "three_stage":
        dcf_desc = "3段階DCF（Phase1高成長 → Phase2移行期 → ターミナル永続）"
    else:
        dcf_desc = "2段階DCF（高成長期 → ターミナル永続）"

    prompt = f"""あなたはDCFバリュエーションの専門家です。以下の計算結果を検証してください。

## 銘柄: {ticker}

## Koichi式 v6.2 算式

```
1. FCFベース: {fcf_base_method}（直近1yr/2yr平均 or 5yr平均の自動判定）
2. WACC: CAPM動的計算（銘柄別β反映）
3. DCF: {dcf_desc}
4. α（成長期待プレミアム、参考値。ALPHA-REDESIGN-1によりP_t算出には使用しない）:
   α = min(alpha_cap, max(0, (ROE × retention × discount_factor / Rm)))  ※Rm=市場期待リターン（βなし）
5. 本質的価値:
   P_t = V₀ + RPO_PV + GrowthOption_PV
   1株当り = P_t / 希薄化後株式数 + ネットキャッシュ/株
```

## 入力数値（動的）

| 項目 | 値 |
|------|-----|
| FCFベース | ${fcf_base_used:,.0f} ({fcf_base_method}) |
| ROE平均 | {roe_avg*100:.2f}% |
| 高成長率 | {p["high_growth_rate"]*100:.1f}% |
| 高成長期間 | {p["high_growth_years"]}年 |
| WACC | {p["wacc"]*100:.2f}% |
| ターミナル成長率 | {p["terminal_growth"]*100:.1f}% |
| 内部留保率 | {p["retention_rate"]*100:.0f}% |
| αディスカウント係数 | {p["discount_factor"]} |
| 希薄化後株式数 | {diluted_shares:,.0f} |

## 計算結果

| 項目 | 値 |
|------|-----|
| PV (高成長期) | ${pv_high:,.0f} |
| PV (ターミナル) | ${pv_terminal:,.0f} |
| V₀ | ${v0:,.0f} |
| RPO現在価値 | ${rpo_pv:,.0f} |
| 成長オプションPV | ${growth_option_pv:,.0f} |
| α | {alpha:.4f} |
| 企業価値 P_t | ${p_t:,.0f} |
| 1株当り本質価値 | ${ivps:.2f} |
| 現在市場価格 | ${current_price:.2f} |
| 乖離率 | {divergence:+.1f}% |

## 検証項目

以下4項目を検証し、JSON形式で回答してください:

1. **pt_shares_consistency**: P_t / shares = intrinsic_value_per_share が正しいか
2. **dcf_components**: DCF構成要素の合計 = v0 が成立するか
3. **formula_verification**: 算式が正しく適用されているか（Rm=10%基準でα再計算）
4. **anomaly_detection**: 異常値がないか

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
    v0_rm = data.get("dcf_components", {}).get("v0_rm", 0)
    prompt += f"""
## 補足（v7.3設計）

- メイン理論株価（${ivps:.2f}）はRmβなし（10%）で計算したV0_rm=${v0_rm/1e9:.2f}Bから算出
- V₀（${v0/1e9:.2f}B）はβ込みWACC基準。DCF構成要素（Phase1+2+TV）はV₀と整合する
- intrinsic_value_beta（${data.get('intrinsic_value_beta', 0):.2f}）はβ込みWACCの参考値。メイン理論株価との差異は設計上正常
- DCF構成要素の整合性はV₀（β込みWACC）と比較すること
"""
    return prompt


def call_xai_api(prompt: str) -> Optional[Dict[str, Any]]:
    """xAI API (Grok) を呼び出して検証結果を取得"""

    if not XAI_API_KEY:
        print("[WARN] XAI_API_KEY not set, skipping AI validation")
        return None

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {XAI_API_KEY}"
    }
    payload = {
        "model": XAI_MODEL,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 1024
    }

    for attempt in range(2):  # 最大2回試行（タイムアウト時1回リトライ）
        try:
            response = requests.post(XAI_ENDPOINT, headers=headers, json=payload, timeout=60)
            response.raise_for_status()

            result = response.json()
            text = result.get("choices", [{}])[0].get("message", {}).get("content", "")

            json_start = text.find("{")
            json_end = text.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                json_str = text[json_start:json_end]
                return json.loads(json_str)

            print(f"[WARN] Could not parse JSON from xAI response: {text[:200]}")
            return None

        except requests.exceptions.Timeout:
            if attempt == 0:
                print(f"[WARN] xAI API timeout (attempt 1/2), retrying...")
                continue
            print(f"[ERROR] xAI API request timed out after 2 attempts")
            return None
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] xAI API request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse xAI response as JSON: {e}")
            return None
    return None


def recalc_ivps_from_components(
    v0: float,
    rpo_pv: float,
    growth_option_pv: float,
    diluted_shares: float,
    net_cash_per_share: float = 0.0,
) -> float:
    """`intrinsic_value_per_share`をDCF構成要素から再計算する
    （`pt_shares_consistency`チェックの中核式）。

    ALPHA-REDESIGN-1: `core_calculator.py`はP_t算出にalphaを乗算しない
    （`calculate_intrinsic_value(..., alpha=0.0, ...)`固定）。alphaは
    参考値としてのみ扱い、本関数には一切登場しない。

    **[[TEST-STALE-IV-1]]対応（2026-08-20）**: 元々`run_basic_checks()`
    にインライン実装されていた式を関数として切り出した（計算内容は
    変更していない）。`tests/test_iv_formula.py`も本関数をimportして
    使うことで、同一概念の計算が2箇所以上に独立実装される状態
    （`[[QUALITY-GATES-EPIC-1]]`ゲート3がNG検知の対象とする状態）を
    解消する——`test_iv_formula.py`側が本式へ追従せず、ALPHA-
    REDESIGN-1前の「alpha乗算あり」の旧式のまま2026-08-20まで残存し
    pytest既知failed2件（MSFT/NVDA）の原因になっていた。
    """
    total_v0 = v0 + rpo_pv + growth_option_pv
    p_t = total_v0
    return p_t / diluted_shares + net_cash_per_share


def run_basic_checks(ticker: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    基本的な整合性チェック（API不要、v6.x対応）

    動的WACC・3段階DCF・成長オプション・alpha_capに対応
    """

    c = data.get("components", {})
    p = _extract_params(data, ticker)

    ivps = data.get("intrinsic_value_per_share", 0)
    # pt_shares_consistencyはRM基準V0で検証（メイン理論株価と整合）
    v0 = data.get("dcf_components", {}).get("v0_rm") or data.get("v0", 0)
    alpha = data.get("alpha", 0)
    alpha_was_capped = data.get("alpha_was_capped", False)

    pv_high = c.get("pv_high", 0)
    pv_terminal = c.get("pv_terminal", 0)
    rpo_pv = c.get("rpo_pv", 0)
    growth_option_pv = c.get("growth_option_pv", 0)
    diluted_shares = c.get("diluted_shares", 0)
    current_price = c.get("current_price", 0)

    checks = {}

    # ── 1. P_t / shares 整合性（v7.0: BS補正考慮）──
    bs_adj = data.get("bs_adjustment", {})
    net_cash_per_share = bs_adj.get("net_cash_per_share", 0.0) if bs_adj.get("applied", False) else 0.0

    if diluted_shares > 0:
        calculated_ivps = recalc_ivps_from_components(
            v0, rpo_pv, growth_option_pv, diluted_shares, net_cash_per_share
        )
        diff_pct = abs(calculated_ivps - ivps) / abs(ivps) * 100 if ivps != 0 else 0

        bs_note = f" + BS ${net_cash_per_share:+.2f}/株" if net_cash_per_share != 0 else ""
        checks["pt_shares_consistency"] = {
            "pass": diff_pct < 1.0,
            "detail": f"(V₀ ${v0/1e9:.2f}B + RPO ${rpo_pv/1e9:.2f}B + GO ${growth_option_pv/1e9:.2f}B) / {diluted_shares/1e9:.3f}B{bs_note} = ${calculated_ivps:.2f} (差異{diff_pct:.2f}%、α={alpha:.3f}は参考値・P_t未乗算)"
        }
    else:
        checks["pt_shares_consistency"] = {"pass": False, "detail": "株式数が0"}

    # ── 2. DCF構成要素（3段階 or 2段階） ──
    # DCF構成要素はβWACC基準で計算されているため、βWACC基準V0と比較する
    v0_beta = p["dcf_components"].get("v0") or data.get("v0", 0)
    if p["dcf_type"] == "three_stage":
        dcf_c = p["dcf_components"]
        pv_phase1 = dcf_c.get("pv_phase1", 0)
        pv_phase2 = dcf_c.get("pv_phase2", 0)
        pv_tv = dcf_c.get("pv_terminal", pv_terminal)
        v0_calculated = pv_phase1 + pv_phase2 + pv_tv
        diff_v0 = abs(v0_calculated - v0_beta) / v0_beta * 100 if v0_beta > 0 else 0

        checks["dcf_components"] = {
            "pass": diff_v0 < 1.0,
            "detail": f"3段階: P1 ${pv_phase1/1e9:.2f}B + P2 ${pv_phase2/1e9:.2f}B + TV ${pv_tv/1e9:.2f}B = ${v0_calculated/1e9:.2f}B (差異{diff_v0:.2f}%)"
        }
    else:
        v0_calculated = pv_high + pv_terminal
        diff_v0 = abs(v0_calculated - v0_beta) / v0_beta * 100 if v0_beta > 0 else 0

        checks["dcf_components"] = {
            "pass": diff_v0 < 1.0,
            "detail": f"2段階: pv_high ${pv_high/1e9:.2f}B + pv_terminal ${pv_terminal/1e9:.2f}B = ${v0_calculated/1e9:.2f}B (差異{diff_v0:.2f}%)"
        }

    # ── 3. 算式検証（Rm基準でα再計算） ──
    roe_avg = c.get("roe_10yr_avg") or c.get("roe_used") or 0.0
    rm = p["rm"]   # RM基準（βなし・10%固定）でα検証
    retention = p["retention_rate"]
    discount_factor = p["discount_factor"]
    alpha_cap = p["alpha_cap"]

    g_individual = max(0.0, roe_avg * retention)
    if rm > 0:
        alpha_uncapped = (g_individual / rm) * discount_factor
    else:
        alpha_uncapped = 0.0
    alpha_calculated = min(alpha_cap, max(0.0, alpha_uncapped))

    alpha_diff = abs(alpha_calculated - alpha)

    if alpha_was_capped:
        # cap適用時: αが alpha_cap と一致すればOK
        formula_pass = abs(alpha - alpha_cap) < 0.01
        detail = f"α計算: ROE {roe_avg*100:.1f}% × {retention*100:.0f}% / Rm {rm*100:.2f}% × {discount_factor} = {alpha_uncapped:.4f} → cap適用 → {alpha:.4f}"
    else:
        formula_pass = alpha_diff < 0.01
        detail = f"α計算: ROE {roe_avg*100:.1f}% × {retention*100:.0f}% / Rm {rm*100:.2f}% × {discount_factor} = {alpha_calculated:.4f} (実際{alpha:.4f})"

    checks["formula_verification"] = {
        "pass": formula_pass,
        "detail": detail
    }

    # ── 4. 異常値検出 ──
    anomalies = []
    divergence = ((ivps - current_price) / current_price * 100) if current_price > 0 else 0

    if abs(divergence) > 1000:
        anomalies.append(f"乖離率{divergence:+.0f}%が極端")

    if diluted_shares < 1_000_000:
        anomalies.append(f"株式数{diluted_shares:,}が異常に少ない")

    if ivps > 50000:
        anomalies.append(f"理論株価${ivps:.0f}が異常に高い")

    if ivps <= 0:
        anomalies.append(f"理論株価${ivps:.2f}がゼロ以下（FCF恒久マイナス銘柄）")
    checks["anomaly_detection"] = {
        "pass": len(anomalies) == 0,
        "detail": "、".join(anomalies) if anomalies else "異常値なし"
    }

    # ── 総合判定 ──
    all_pass = all(c["pass"] for c in checks.values())

    if all_pass:
        overall = "PASS"
    elif not checks["anomaly_detection"]["pass"]:
        overall = "FAIL"
    else:
        overall = "WARN"

    return {
        "validated_at": datetime.now().strftime("%Y-%m-%d"),
        "model": "basic_checks_only",  # v7.3: Grok廃止、basic_checksで代替
        "checks": checks,
        "overall": overall,
        "ai_comment": None
    }


def validate_calculation(ticker: str, data: Dict[str, Any], use_ai: bool = False) -> Dict[str, Any]:
    """計算結果を検証"""

    validation = run_basic_checks(ticker, data)

    if use_ai and XAI_API_KEY:
        prompt = build_validation_prompt(ticker, data)
        ai_result = call_xai_api(prompt)

        if ai_result:
            validation["model"] = XAI_MODEL
            validation["ai_comment"] = ai_result.get("ai_comment")

            # v7.3: ai_commentが存在しoverallがPASSでも懸念があれば記録
            ai_comment = ai_result.get("ai_comment", "")
            ai_checks = ai_result.get("checks", {})
            ai_anomaly = ai_checks.get("anomaly_detection", {})
            # anomaly_detectionがFAILの場合はその詳細をai_concernsに記録
            if ai_anomaly.get("detail") and not ai_anomaly.get("pass", True):
                validation["ai_concerns"] = ai_anomaly.get("detail")
            # ai_commentに懸念ワードが含まれる場合も記録（誤判定フィルタ付き）
            elif ai_comment and any(w in ai_comment.lower() for w in ["inconsistency", "error", "concern", "incorrect"]):
                # ただしbasic_checksが全PASS済みなら無視（Grokの誤判定）
                basic_all_pass = all(v.get("pass", False) for v in validation.get("checks", {}).values())
                if not basic_all_pass:
                    validation["ai_concerns"] = ai_comment

    return validation


if __name__ == "__main__":
    # v6.2のNVDAデータでテスト
    test_data = {
        "intrinsic_value_per_share": 200.61,
        "v0": 2434239420907,
        "alpha": 1.0,
        "alpha_was_capped": True,
        "dcf_type": "three_stage",
        "wacc": {"value": 0.176095, "beta": 2.335},
        "dcf_components": {
            "pv_phase1": 626417505849,
            "pv_phase2": 770109919697,
            "pv_terminal": 1037711995360,
        },
        "components": {
            "pv_high": 1396527425546,
            "pv_terminal": 1037711995360,
            "rpo_pv": 1865009616,
            "growth_option_pv": 1859714437,
            "diluted_shares": 24305000000,
            "current_price": 201.09,
            "fcf_base_used": 78764500000,
            "fcf_base_method": "recent_2yr",
            "roe_10yr_avg": 0.4564,
            "high_growth_rate_used": 0.363,
            "high_growth_years": 5,
            "terminal_growth_used": 0.03,
        }
    }

    result = validate_calculation("NVDA", test_data, use_ai=False)
    print(json.dumps(result, indent=2, ensure_ascii=False))
