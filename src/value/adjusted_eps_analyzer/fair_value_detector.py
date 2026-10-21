"""
公正価値変動の自動検出モジュール

自動検出条件（全て満たす場合のみ調整項目として処理）:
  条件1: 純利益の前年同期比変動が売上高変動の5倍以上
         ※売上高データが両期間ともゼロの場合はスキップ
  条件2: FairValue系XBRLタグが存在する
  条件3: その金額が純利益の30%以上を占める
"""
from typing import Dict, List, Optional, Tuple

# ============================================================
# 検出対象タグ定義
#
# negate_value=False : expense(income)形式
#   正値 = 費用/損失（GAAPの純利益を減らした） → add_back（正の調整）
#   負値 = 収益/利益（GAAPの純利益を増やした） → subtract（負の調整）
#   → XBRL値をそのまま amount に使う
#
# negate_value=True : gain(loss)形式
#   正値 = 利得（GAAPの純利益を増やした）→ subtract（除去）
#   負値 = 損失（GAAPの純利益を減らした）→ add_back（戻し入れ）
#   → XBRL値の符号を反転して amount に使う
# ============================================================
FAIR_VALUE_ITEM_DEFS: List[Dict] = [
    {
        "item_id": "warrant_fv_change",
        "item_name": "ワラント公正価値変動損益",
        "xbrl_tags": [
            "us-gaap:FairValueAdjustmentOfWarrants",
        ],
        "negate_value": False,
        "pre_tax": True,
        "reason": "非現金・非経常のワラント時価評価変動",
        "category": "公正価値変動関連",
    },
    {
        "item_id": "contingent_consideration_fv",
        "item_name": "偶発対価公正価値変動損益",
        "xbrl_tags": [
            "us-gaap:BusinessCombinationContingentConsiderationArrangementsChangeInAmountOfContingentConsiderationLiability1",
        ],
        "negate_value": False,
        "pre_tax": True,
        "reason": "非現金・非経常の買収偶発対価時価変動",
        "category": "公正価値変動関連",
    },
    {
        "item_id": "derivative_fv_change",
        "item_name": "デリバティブ公正価値変動損益",
        "xbrl_tags": [
            "us-gaap:GainLossOnDerivativeInstrumentsNetPretax",
            "us-gaap:DerivativeGainLossOnDerivativeNet",
            "us-gaap:UnrealizedGainLossOnDerivatives",
        ],
        "negate_value": True,
        "pre_tax": True,
        "reason": "非現金・非経常のデリバティブ時価評価変動",
        "category": "公正価値変動関連",
    },
    {
        "item_id": "investment_fv_change",
        "item_name": "投資公正価値変動損益",
        "xbrl_tags": [
            "us-gaap:GainLossOnInvestments",
            "us-gaap:UnrealizedGainLossOnInvestments",
        ],
        "negate_value": True,
        "pre_tax": True,
        "reason": "非現金・非経常の投資時価評価変動",
        "category": "公正価値変動関連",
    },
]

# 既知タグセット（動的スキャンの重複除外用）
_KNOWN_FV_TAGS: set = {
    tag
    for item_def in FAIR_VALUE_ITEM_DEFS
    for tag in item_def["xbrl_tags"]
}

# 動的スキャンで除外するタグパターン（標準調整項目で使用済み）
_EXCLUDED_FROM_DYNAMIC_SCAN: set = {
    "us-gaap:FairValueOptionChangesInFairValueGainLoss1",  # loan_fair_value に使用中
    "us-gaap:OtherComprehensiveIncome",                    # crypto_fair_value に使用中
}

# 閾値定数
MATERIALITY_THRESHOLD = 0.30          # 純利益の30%以上（条件3）
INCOME_REVENUE_MULTIPLIER = 5.0       # 純利益変動 >= 売上変動 × N倍（条件1）
EXTREME_MATERIALITY_THRESHOLD = 0.80  # FV額が純利益の80%超なら条件1スキップ
FAIR_VALUE_PATTERN = "FairValue"      # 動的検出用パターン


# ============================================================
# 内部ヘルパー関数
# ============================================================

def _get_tax_rate(q: dict) -> float:
    """既存の調整項目から実効税率を取得（なければデフォルト21%）"""
    for adj in q.get("adjustments", []):
        rate = adj.get("tax_rate_applied")
        if rate is not None and 0.0 <= rate <= 0.5:
            return rate
    return 0.21


def _scan_for_fv_items(period_data: dict, net_income: float) -> List[dict]:
    """
    period_data から公正価値変動タグを検出（条件2+3チェック）

    Returns:
        検出された項目リスト（amount / xbrl_value フィールド付き）
    """
    detected = []
    found_item_ids: set = set()

    # 既知タグの検出
    for item_def in FAIR_VALUE_ITEM_DEFS:
        for tag in item_def["xbrl_tags"]:
            val_dict = period_data.get(tag)
            if not isinstance(val_dict, dict):
                continue
            xbrl_value = val_dict.get("value", 0)
            if not xbrl_value:
                continue

            # 条件3: |金額| >= |純利益| × 30%
            if not net_income or abs(xbrl_value) < abs(net_income) * MATERIALITY_THRESHOLD:
                continue

            amount = -xbrl_value if item_def["negate_value"] else xbrl_value
            detected.append({
                "item_id": item_def["item_id"],
                "item_name": item_def["item_name"],
                "amount": amount,
                "xbrl_value": xbrl_value,
                "unit": val_dict.get("unit", "USD"),
                "direction": "add_back",
                "pre_tax": item_def["pre_tax"],
                "reason": item_def["reason"],
                "extracted_from": tag,
                "category": item_def["category"],
            })
            found_item_ids.add(item_def["item_id"])
            break  # 同じ item_id の最初のヒットのみ使用

    # 動的スキャン: "FairValue" を含む未知タグ
    for tag, val_dict in period_data.items():
        if not isinstance(tag, str):
            continue
        if FAIR_VALUE_PATTERN not in tag:
            continue
        if tag in _KNOWN_FV_TAGS or tag in _EXCLUDED_FROM_DYNAMIC_SCAN:
            continue
        if not isinstance(val_dict, dict):
            continue
        xbrl_value = val_dict.get("value", 0)
        if not xbrl_value:
            continue

        # 条件3
        if not net_income or abs(xbrl_value) < abs(net_income) * MATERIALITY_THRESHOLD:
            continue

        short_name = tag.split(":")[-1] if ":" in tag else tag
        item_id = f"fv_auto_{short_name[:20]}"
        if item_id in found_item_ids:
            continue
        found_item_ids.add(item_id)

        # 動的タグは expense(income) 形式とみなし negate_value=False
        detected.append({
            "item_id": item_id,
            "item_name": f"公正価値変動（{short_name[:30]}）",
            "amount": xbrl_value,
            "xbrl_value": xbrl_value,
            "unit": val_dict.get("unit", "USD"),
            "direction": "add_back",
            "pre_tax": True,
            "reason": "非現金・非経常の公正価値変動（自動検出）",
            "extracted_from": tag,
            "category": "公正価値変動関連",
        })

    return detected


def _check_condition1(
    curr_income: float,
    prev_income: float,
    curr_revenue: float,
    prev_revenue: float,
) -> Tuple[bool, str]:
    """
    条件1: 純利益の前年比変動が売上変動の INCOME_REVENUE_MULTIPLIER 倍以上

    Returns:
        (is_met, reason_str)
    """
    # 両期間とも売上ゼロ → 条件1スキップ
    if curr_revenue == 0 and prev_revenue == 0:
        return True, "売上高データなし（条件1スキップ）"

    # 前年純利益ほぼゼロ → 0除算回避のためスキップ
    if abs(prev_income) < 1_000:
        return True, "前年同期純利益ほぼゼロ（条件1スキップ）"

    income_change = abs(curr_income - prev_income) / abs(prev_income)

    if prev_revenue > 0 and curr_revenue > 0:
        rev_change = abs(curr_revenue - prev_revenue) / abs(prev_revenue)
        if rev_change < 0.001:
            # 売上変動ほぼゼロ → 純利益変動 N 倍以上で条件成立
            met = income_change >= INCOME_REVENUE_MULTIPLIER
            return met, (
                f"売上変動<0.1%、純利益変動{income_change:.0%}（閾値{INCOME_REVENUE_MULTIPLIER:.0f}倍超）"
            )
        multiplier = income_change / rev_change
        met = multiplier >= INCOME_REVENUE_MULTIPLIER
        return met, (
            f"純利益変動{income_change:.0%} / 売上変動{rev_change:.0%} "
            f"= {multiplier:.1f}倍（閾値{INCOME_REVENUE_MULTIPLIER:.0f}倍）"
        )
    else:
        met = income_change >= INCOME_REVENUE_MULTIPLIER
        return met, f"売上データ不完全・純利益変動{income_change:.0%}"


# ============================================================
# メイン公開関数
# ============================================================

def apply_fair_value_detection(
    quarterly_raw: List[dict],
    quarterly_results: List[dict],
) -> List[dict]:
    """
    全四半期に対して公正価値変動の自動検出を実行し、quarterly_results を更新する。

    条件1+2+3を全て満たす四半期のみ調整項目を追加し、Adj EPS を再計算する。
    FAIR_VALUE_ADJUSTED フラグと special_notes も自動設定する
    （[[EPS-DISCREPANCY-FLAG-OVERLOAD-1]]対応: check_eps_discrepancy()の
    EPS_DISCREPANCYフラグとは意味が異なるため別名にした）。

    Args:
        quarterly_raw:     extract_quarterly_facts が返す生データリスト（FV XBRLタグ含む）
        quarterly_results: pipeline で処理済みの四半期結果リスト

    Returns:
        更新された quarterly_results（新しいリストオブジェクト）
    """
    if not quarterly_raw or not quarterly_results:
        return quarterly_results

    # period_data を (fiscal_year, quarter) でインデックス化
    raw_map: Dict[Tuple[int, int], dict] = {}
    for pd in quarterly_raw:
        fy = pd.get("fiscal_year")
        qn = pd.get("quarter")
        if fy is not None and qn is not None:
            raw_map[(fy, qn)] = pd

    # quarterly_results を (fiscal_year, quarter) でインデックス化（前年比較用）
    results_map: Dict[Tuple[int, int], dict] = {}
    for q in quarterly_results:
        fy = q.get("fiscal_year")
        qn = q.get("quarter")
        if fy is not None and qn is not None:
            results_map[(fy, qn)] = q

    modified: List[dict] = []

    for q in quarterly_results:
        fy = q.get("fiscal_year")
        qn = q.get("quarter")

        if fy is None or qn is None:
            modified.append(q)
            continue

        period_data = raw_map.get((fy, qn), {})
        net_income = q.get("gaap_net_income", 0)

        # 条件2+3: FVタグを検出
        fv_items = _scan_for_fv_items(period_data, net_income)

        if not fv_items:
            modified.append(q)
            continue

        # 超高materiality チェック（FV額が純利益の80%超なら条件1スキップ）
        fv_total_abs = sum(abs(it.get("xbrl_value", it["amount"])) for it in fv_items)
        extreme_ratio = fv_total_abs / abs(net_income) if net_income else 0

        if extreme_ratio >= EXTREME_MATERIALITY_THRESHOLD:
            cond1_met = True
            cond1_reason = (
                f"FV変動が純利益の{extreme_ratio:.0%}占有"
                f"（超高materiality → 条件1スキップ）"
            )
        else:
            # 条件1: 前年同期比較
            prev_q = results_map.get((fy - 1, qn))
            if prev_q is not None:
                cond1_met, cond1_reason = _check_condition1(
                    net_income,
                    prev_q.get("gaap_net_income", 0),
                    q.get("revenue", 0),
                    prev_q.get("revenue", 0),
                )
            else:
                cond1_met, cond1_reason = True, "前年同期データなし（条件1スキップ）"

        if not cond1_met:
            print(
                f"  [FV Auto] {q.get('filing_date', '?')} FY{fy}Q{qn}: "
                f"条件1未達 → スキップ ({cond1_reason})"
            )
            modified.append(q)
            continue

        # 重複チェック（すでに同じ item_id が追加済みなら除外）
        existing_ids = {a.get("item_id") for a in q.get("adjustments", [])}
        new_items = [it for it in fv_items if it["item_id"] not in existing_ids]

        if not new_items:
            modified.append(q)
            continue

        # ── 調整項目を追加して Adj EPS を再計算 ────────────────────────────
        tax_rate = _get_tax_rate(q)
        print(
            f"  [FV Auto] {q.get('filing_date', '?')} FY{fy}Q{qn}: "
            f"{len(new_items)} 件の公正価値変動を検出 (条件1: {cond1_reason})"
        )

        additional_net = 0.0
        adjusted_items: List[dict] = []

        for fv_it in new_items:
            if fv_it["pre_tax"]:
                net_amount = fv_it["amount"] * (1.0 - tax_rate)
                tax_applied = tax_rate
            else:
                net_amount = fv_it["amount"]
                tax_applied = 0.0

            adj_item = {k: v for k, v in fv_it.items() if k != "xbrl_value"}
            adj_item["net_amount"] = net_amount
            adj_item["tax_rate_applied"] = tax_applied
            adjusted_items.append(adj_item)
            additional_net += net_amount

            print(
                f"    {fv_it['item_name']}: "
                f"XBRL={fv_it['xbrl_value']:+,.0f} → "
                f"調整額={fv_it['amount']:+,.0f} → "
                f"net={net_amount:+,.0f}"
            )

        # quarterly_result を shallow copy して更新
        q = dict(q)
        q["adjustments"] = list(q.get("adjustments", [])) + adjusted_items

        old_net = q.get("net_adjustment_total", 0.0)
        q["net_adjustment_total"] = old_net + additional_net
        q["adjusted_net_income"] = q["gaap_net_income"] + q["net_adjustment_total"]

        diluted = q.get("diluted_shares_used") or q.get("diluted_shares") or 0
        q["adjusted_eps"] = q["adjusted_net_income"] / diluted if diluted else 0.0

        print(
            f"    → GAAP EPS={q['gaap_eps']:+.4f} "
            f"Adj EPS={q['adjusted_eps']:+.4f}"
        )

        # [[EPS-DISCREPANCY-FLAG-OVERLOAD-1]]: 公正価値変動の自動検出・調整は
        # XBRL vs Alpha Vantage公式値の乖離（check_eps_discrepancy()、意味の
        # 異なる別処理）とは別のフラグ名にする（special_notesのどちらの
        # サブキーが埋まっているかを確認しないと原因を判別できない問題を
        # 解消するため）
        flags = list(q.get("special_flags", []))
        if "FAIR_VALUE_ADJUSTED" not in flags:
            flags.append("FAIR_VALUE_ADJUSTED")
        q["special_flags"] = flags

        # special_notes に検出詳細を追加
        notes = dict(q.get("special_notes", {}))
        notes["fair_value_auto_detect"] = {
            "flag": "FAIR_VALUE_ADJUSTED",
            "note": (
                "公正価値変動（非現金・非経常）が純利益の30%以上を占めると判定されました。"
                "GAAP EPSは経済実態を反映していない可能性があります。"
                "Adj EPSは以下の項目を除外して再計算しています: "
                + "、".join(it["item_name"] for it in adjusted_items)
            ),
        }
        q["special_notes"] = notes

        modified.append(q)

    return modified
