"""
src/value/tanuki_valuation/beta_fetcher.py
β自動取得・beta_config.json 更新スクリプト

用途:
  - 新規銘柄登録時の β 初期設定
  - 既存銘柄の β 定期リフレッシュ
  - β 乖離の検出

使用方法:
    # 全銘柄リフレッシュ
    python beta_fetcher.py

    # 特定銘柄のみ
    python beta_fetcher.py NVDA AAPL META

    # ドライラン（config を書き換えずに差分だけ表示）
    python beta_fetcher.py --dry-run

ルール:
    - yfinance 5年βをベース。取得元はcommon/market_data/attributes/
      （reader.get_attributes()）— [[MARKETDATA-LAYER-CONSTRUCTION-1]]
      着手順序4-1でyfinance直接呼び出しから切替済み。データ実体は
      Market_Data_Weekly_Update.yml（毎週日曜）が生成する。対象銘柄の
      attributes/{TICKER}.jsonが未生成の場合はスキップされる
      （yfinanceへの直接フォールバックは行わない・市場データ層専任）
    - 上限 2.5 / 下限 0.3
    - source が "damodaran_*" の銘柄は上書きしない（手動設定を保護）
    - DISCORD_WEB_HOOK が設定されていれば大きな変化を通知
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

REPO_ROOT   = Path(__file__).resolve().parents[3]
CONFIG_PATH = REPO_ROOT / "config" / "beta_config.json"
DATA_DIR    = REPO_ROOT / "docs" / "value-monitor" / "tanuki_valuation" / "data"
SEC_DATA_DIR = REPO_ROOT / "common" / "sec_data" / "data"

# common/ を import できるように repo root を sys.path に追加
_repo_str = str(REPO_ROOT)
if _repo_str not in sys.path:
    sys.path.insert(0, _repo_str)

# [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-1: β取得元をyfinance直接
# 呼び出しからmarket_data統合層（reader.get_attributes()）経由に切替。
# yfinance直接呼び出し（safe_yf_ticker等）は本ファイルから不要になった
# （取得の実体はfetcher.py::fetch_weekly_attributes()に一本化済み、
# そちらが同等のリトライ機構=safe_yf_ticker経由を持つ）。
from common.market_data.reader import get_attributes as _get_market_data_attributes

BETA_CAP    = 2.5
BETA_FLOOR  = 0.3
DRIFT_WARN  = 0.5   # この差分以上で「大きな乖離」と判定


# Damodaran業種別β（手動設定が必要な特例銘柄向け）
DAMODARAN_OVERRIDES: dict[str, tuple[float, str]] = {
    "LMT": (0.74, "damodaran_2025_aerospace_defense"),
    # 必要に応じて追加
}


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {"overrides": {}}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def save_config(cfg: dict) -> None:
    cfg["_updated_at"] = datetime.now().strftime("%Y-%m-%d")
    CONFIG_PATH.write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ── Software_System 新規銘柄暫定判定（FCF-CONVRATE-DESIGN-LIMIT-1）──────────
# 前受収益（Deferred Revenue/Contract Liability）の売上高比率で、
# 成熟ライセンス型（Software_System_Mature）とサブスクリプション型SaaS
# （Software_System_SaaS）を暫定的に判定する。実測FCF/調整済み純利益データが
# 蓄積されるまでの初期値（18銘柄の実績検証で分離精度は約78%と確認済み。
# calculator/adjustments.py::check_software_system_reclassification()が
# 実績蓄積後に毎回再判定するため、ここでの誤判定は自動的に補正されうる）。
SOFTWARE_SYSTEM_DR_REV_THRESHOLD = 0.40
SOFTWARE_SYSTEM_DR_REV_BORDERLINE = (0.30, 0.50)

_DR_COMBINED_TAGS = ["ContractWithCustomerLiability", "DeferredRevenue"]
_DR_CURRENT_TAGS = ["ContractWithCustomerLiabilityCurrent", "DeferredRevenueCurrent"]
_DR_NONCURRENT_TAGS = ["ContractWithCustomerLiabilityNoncurrent", "DeferredRevenueNoncurrent"]


def _instant_series(facts: dict, tag: str) -> dict:
    """company_facts.json の us-gaap タグから {end_date: val} を返す（10-Kのみ）"""
    if tag not in facts:
        return {}
    out = {}
    for entries in facts[tag].get("units", {}).values():
        for e in entries:
            if e.get("form") == "10-K" and e.get("end"):
                out.setdefault(e["end"], e["val"])
    return out


def compute_deferred_revenue_ratio(ticker: str) -> Optional[tuple[float, str]]:
    """
    common/sec_data/data/{ticker}/company_facts.json から直近10-K時点の
    前受収益（Deferred Revenue/Contract Liability）/売上高比率を計算する。

    優先順位: ContractWithCustomerLiability（結合値）→ DeferredRevenue（結合値）
              → Current+Noncurrent合算（同タグ系統内でのみ）

    Returns:
        (ratio, end_date) or None（company_facts.json不在 or タグ不在 or revenue取得不可）
    """
    import glob

    facts_path = SEC_DATA_DIR / ticker.upper() / "company_facts.json"
    if not facts_path.exists():
        return None
    with open(facts_path, encoding="utf-8") as f:
        facts = json.load(f).get("facts", {}).get("us-gaap", {})

    combined: dict = {}
    for tag in _DR_COMBINED_TAGS:
        s = _instant_series(facts, tag)
        for k, v in s.items():
            combined.setdefault(k, v)
        if combined:
            break

    dr_by_date = dict(combined)
    if not dr_by_date:
        cur, noncur = {}, {}
        for tag in _DR_CURRENT_TAGS:
            s = _instant_series(facts, tag)
            for k, v in s.items():
                cur.setdefault(k, v)
            if cur:
                break
        for tag in _DR_NONCURRENT_TAGS:
            s = _instant_series(facts, tag)
            for k, v in s.items():
                noncur.setdefault(k, v)
            if noncur:
                break
        for k, v in cur.items():
            dr_by_date[k] = v + noncur.get(k, 0)

    if not dr_by_date:
        return None

    latest_date = max(dr_by_date.keys())
    latest_dr = dr_by_date[latest_date]

    # revenue は正規化済み annual_*.json から取得（同一年度に最も近いもの）
    ticker_dir = SEC_DATA_DIR / ticker.upper()
    target_year = int(latest_date[:4])
    revenue = None
    for path in sorted(glob.glob(str(ticker_dir / "annual_*.json")), reverse=True):
        try:
            with open(path, encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            continue
        try:
            _period = int(d.get("period"))
        except (TypeError, ValueError):
            continue
        if _period in (target_year, target_year - 1):
            revenue = d.get("pl", {}).get("revenue")
            if revenue:
                break

    if not revenue:
        return None

    return latest_dr / revenue, latest_date


def classify_software_system_subgroup(ticker: str, dry_run: bool = False) -> Optional[dict]:
    """
    新規銘柄向け: sector="Software_System"（未分類の広義カテゴリ）の銘柄を、
    前受収益比率に基づき Software_System_Mature / Software_System_SaaS に暫定分類する。

    既に Software_System_Mature/SaaS へ確定済みの銘柄・sector未設定の銘柄は対象外
    （既存18銘柄の実績ベース分類を上書きしないためのガード）。

    common/sec_data/update.py [TICKER]（新規銘柄登録Step1）実行後に呼び出すこと
    （company_facts.jsonが存在しない場合は判定不可）。

    Returns:
        {"ticker", "subgroup", "dr_ratio", "borderline", "note"} or None（対象外 or データ不足）
    """
    cfg = load_config()
    overrides = cfg.setdefault("overrides", {})
    cur = overrides.get(ticker, {})
    if cur.get("sector") != "Software_System":
        return None

    result = compute_deferred_revenue_ratio(ticker)
    if result is None:
        print(f"  [{ticker}] company_facts.jsonから前受収益データ取得不可。手動でSoftware_System_Mature/SaaSを設定してください")
        return None
    ratio, end_date = result

    subgroup = (
        "Software_System_SaaS" if ratio >= SOFTWARE_SYSTEM_DR_REV_THRESHOLD
        else "Software_System_Mature"
    )
    lo, hi = SOFTWARE_SYSTEM_DR_REV_BORDERLINE
    borderline = lo <= ratio <= hi

    provisional_note = (
        f"前受収益/売上高比率={ratio:.2f}@{end_date}が境界近傍({lo}-{hi})のため"
        f"{subgroup}は暫定判定。実績データ蓄積後に自動見直しされる可能性あり"
        if borderline else ""
    )

    print(f"  [{ticker}] Software_System暫定分類: {subgroup} (DR/Rev={ratio:.2f}@{end_date})"
          + ("  ⚠境界近傍" if borderline else ""))

    if not dry_run:
        cur["sector"] = subgroup
        cur["software_system_provisional"] = True
        cur["software_system_provisional_note"] = provisional_note
        overrides[ticker] = cur
        save_config(cfg)

    return {
        "ticker": ticker, "subgroup": subgroup, "dr_ratio": ratio,
        "borderline": borderline, "note": provisional_note,
    }


def get_registered_tickers() -> list[str]:
    path = DATA_DIR / "tickers.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8")).get("tickers", [])


def fetch_market_data_beta(ticker: str) -> Optional[float]:
    """common/market_data/attributes/{ticker}.json（reader.get_attributes()）
    からβを取得する。

    データ未生成（Market_Data_Weekly_Update.yml未実行・対象銘柄未収録）・
    βキー欠落のいずれの場合もNoneを返す（呼び出し側のrefresh_tickers()が
    「スキップ」として扱う。yfinanceへの直接フォールバックは行わない
    ——[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-1の設計判断、
    「market_data専任型」）。

    値の生成元自体はfetcher.py::fetch_weekly_attributes()が呼ぶ同じ
    yfinance `.info["beta"]`であり、値の意味・算出方法は変わらない
    （層またぎ再計算はしていない。common/market_data/reader.pyの
    docstring「層またぎ再計算の禁止」参照）。
    """
    attrs = _get_market_data_attributes(ticker)
    if attrs is None:
        return None
    return attrs.get("beta")


def calc_capped_beta(raw_beta: float, ticker: str) -> tuple[float, str]:
    """上限・下限を適用し (capped_beta, source_string) を返す。

    [[BETA-FALLBACK-DESIGN-GAPS-1]]: raw_beta<=0は、data_fetcher.py::
    _determine_beta()の0.1〜3.0範囲チェックであれば無効値として弾かれる
    はずの値だが、beta_config.jsonにoverrideとして書き込まれると
    _determine_beta()側のチェックは経由せず無条件採用されてしまう
    （2026-09-12調査時点でoverrideは101銘柄全てに存在し、raw_beta<=0の
    実例は0件と確認済み。実害は無いが、将来データ異常が発生した場合に
    無警告でフロア値0.3へ丸められて書き込まれるのを防ぐため、検知時の
    みWARNログを出す）。クリップ処理自体（下限0.3・上限2.5）は変更しない。
    """
    if raw_beta <= 0:
        print(f"  [WARN] {ticker}: raw_beta={raw_beta}が0以下です。"
              f"下限{BETA_FLOOR}へクリップして書き込みますが、取得元データ"
              f"（common/market_data/attributes/）の異常の可能性があるため"
              f"確認してください。")
    capped = max(BETA_FLOOR, min(BETA_CAP, raw_beta))
    src = "yfinance_5yr"
    if capped != round(raw_beta, 3):
        src += f"_capped_from_{round(raw_beta, 2)}"
    return round(capped, 3), src


def refresh_tickers(
    tickers: list[str],
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    指定銘柄のβを common/market_data/attributes/（reader.get_attributes()）
    から取得して beta_config.json を更新する。

    overrides[ticker]への書き込みは既存エントリとマージする（beta/source
    キーのみ更新し、他のキー——sector・software_system_provisional等——は
    保持する）。従来は`overrides[ticker] = {"beta":..., "source":...}`と
    丸ごと置換していたため、classify_software_system_subgroup()が設定した
    sector等が次回のβ更新時（値が実際に変化した回のみ）に消去される
    リスクがあった（[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-1調査で
    発見、本切替と同時に修正）。

    Returns:
        (updated_list, skipped_list)
        updated_list: 変化した銘柄のログ
        skipped_list: スキップされた銘柄のログ
    """
    cfg      = load_config()
    overrides = cfg.setdefault("overrides", {})

    updated  = []
    skipped  = []

    for ticker in tickers:
        cur = overrides.get(ticker, {})

        # Damodaran手動設定は上書きしない
        if ticker in DAMODARAN_OVERRIDES:
            b, src = DAMODARAN_OVERRIDES[ticker]
            if cur.get("source", "").startswith("damodaran"):
                skipped.append({"ticker": ticker, "reason": "Damodaran手動設定を保護"})
                continue
            # 初回登録のみ Damodaran 値を適用（既存キーを保持したままマージ）
            if not dry_run:
                overrides[ticker] = {**cur, "beta": b, "source": src}
            updated.append({"ticker": ticker, "old": cur.get("beta"), "new": b, "source": src})
            continue

        raw_beta = fetch_market_data_beta(ticker)
        if raw_beta is None:
            skipped.append({
                "ticker": ticker,
                "reason": "market_data属性データなし（common/market_data/attributes/未生成、"
                          "またはβキー欠落）",
            })
            continue

        new_beta, src = calc_capped_beta(raw_beta, ticker)
        old_beta = cur.get("beta")

        if old_beta is not None and abs(new_beta - old_beta) < 0.01:
            # 変化なし
            continue

        drift = f"{new_beta - old_beta:+.2f}" if old_beta is not None else "(新規)"
        updated.append({
            "ticker":  ticker,
            "old":     old_beta,
            "new":     new_beta,
            "drift":   drift,
            "source":  src,
            "warn":    old_beta is not None and abs(new_beta - old_beta) >= DRIFT_WARN,
        })

        if not dry_run:
            overrides[ticker] = {**cur, "beta": new_beta, "source": src}

    if not dry_run and updated:
        save_config(cfg)

    return updated, skipped


def build_discord_message(
    updated: list[dict],
    skipped: list[dict],
    run_date: str,
    dry_run: bool,
) -> str:
    warn_items = [u for u in updated if u.get("warn")]
    if not updated and not warn_items:
        return (
            f"✅ **β設定リフレッシュ** `{run_date}`\n"
            f"変化なし（全銘柄βが安定）"
        )

    mode = "【DRY RUN】" if dry_run else ""
    lines = [
        f"⚡ **β設定リフレッシュ{mode}** `{run_date}` — {len(updated)}銘柄更新",
        "",
    ]
    if warn_items:
        lines.append("**⚠️ 大きな乖離あり（要確認）**")
        for u in warn_items:
            lines.append(f"　`{u['ticker']}`: {u['old']} → {u['new']} ({u['drift']})")
        lines.append("")
    if updated:
        lines.append("**更新一覧**")
        for u in updated:
            lines.append(f"　`{u['ticker']}`: {u.get('old','N/A')} → {u['new']} ({u.get('drift','新規')})")
    if skipped:
        lines.append(f"\nスキップ: {', '.join(s['ticker'] for s in skipped)}")

    return "\n".join(lines)


def post_discord(message: str) -> bool:
    webhook = os.environ.get("DISCORD_WEB_HOOK", "")
    if not webhook or not message:
        return False
    try:
        import urllib.request
        payload = json.dumps({"content": message}).encode("utf-8")
        req = urllib.request.Request(
            webhook,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status in (200, 204)
    except Exception as e:
        print(f"Discord送信エラー: {e}", file=sys.stderr)
        return False


def main():
    dry_run = "--dry-run" in sys.argv
    targets = [a for a in sys.argv[1:] if not a.startswith("--")]

    if "--classify-software-system" in sys.argv:
        # FCF-CONVRATE-DESIGN-LIMIT-1: 新規銘柄登録時、sector="Software_System"
        # （未分類の広義カテゴリ）の銘柄を前受収益比率でMature/SaaSに暫定分類する。
        # 使用例: python beta_fetcher.py TICKER --classify-software-system
        if not targets:
            print("--classify-software-system は対象ティッカーの指定が必須です")
            sys.exit(1)
        for t in targets:
            classify_software_system_subgroup(t, dry_run=dry_run)
        return

    tickers = targets if targets else get_registered_tickers()
    if not tickers:
        print("対象銘柄が見つかりません")
        sys.exit(1)

    run_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    mode_str = "【DRY RUN】" if dry_run else ""
    print(f"=== β設定リフレッシュ {mode_str}{run_date} ({len(tickers)}銘柄) ===")

    updated, skipped = refresh_tickers(tickers, dry_run=dry_run)

    warn_items = [u for u in updated if u.get("warn")]
    for u in updated:
        marker = "⚠️" if u.get("warn") else "  "
        print(f"  {marker} {u['ticker']:6s}: {str(u.get('old','N/A')):6s} → {u['new']:<6}  {u.get('drift','新規')}")
    for s in skipped:
        print(f"  スキップ {s['ticker']}: {s['reason']}")

    if not updated and not skipped:
        print("  変化なし")

    # Discord通知（大きな乖離があるか、新規登録の場合のみ）
    if warn_items or any(u.get("drift") == "(新規)" for u in updated):
        message = build_discord_message(updated, skipped, run_date, dry_run)
        if post_discord(message):
            print("Discord通知: 送信完了")

    # GitHub Actions サマリー
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "")
    if summary_path and updated:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("## β設定リフレッシュ\n\n")
            f.write(f"更新: {len(updated)}銘柄  スキップ: {len(skipped)}銘柄\n\n")
            if warn_items:
                f.write("### ⚠️ 大きな乖離\n")
                for u in warn_items:
                    f.write(f"- `{u['ticker']}`: {u.get('old')} → {u['new']} ({u.get('drift')})\n")


if __name__ == "__main__":
    main()
