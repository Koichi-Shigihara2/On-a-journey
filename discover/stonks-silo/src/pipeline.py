# discover/stonks-silo/src/pipeline.py
"""
Stonks Silo Pipeline
config/cik_lookup.csv の stonks_silo=true 銘柄を一括処理して results.json に保存する。

出力先: docs/value-monitor/stonks-silo/data/results.json  (GitHub Pages 公開パス)

使い方:
  python discover/stonks-silo/src/pipeline.py          # stonks_silo=true 全件
  python discover/stonks-silo/src/pipeline.py SOUN BBAI  # 指定ティッカーのみ
"""

import dataclasses
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_SRC_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SRC_DIR.parents[2]
sys.path.insert(0, str(_SRC_DIR))
sys.path.insert(0, str(_REPO_ROOT))

from fetcher import load_annual_data
from analyzer import StonksAnalyzer
from valuation_fetcher import fetch_valuation
from financial_trend_calculator import compute_vectors, load_all_normalized
from common.sec_data import tickers
from common.sec_data.reader import SECReader


_OUTPUT_DIR = _REPO_ROOT / "docs" / "value-monitor" / "stonks-silo" / "data"
_OUTPUT_FILE = _OUTPUT_DIR / "results.json"
_YEARS = 5


# ---------------------------------------------------------------------------
# ティッカー取得
# ---------------------------------------------------------------------------

def stonks_tickers() -> list[str]:
    """cik_lookup.csv から stonks_silo=true の銘柄を返す"""
    return tickers.get_stonks_silo_tickers()


def _filter_stonks_silo_tickers(target: list[str]) -> list[str]:
    """CLI引数等でticker明示指定時にstonks_silo=trueのみへ絞り込む。

    FLAG-CONSUMER-AUDIT-2: tanuki_valuation/pipeline.pyのCLI引数パスと
    同型の構造的ギャップ（stonks_tickers()はデフォルト実行時のみ使われ、
    ticker明示指定時はstonks_silo=trueフラグの検証を一切行わずそのまま
    処理していた）への対応。範囲外のticker指定は警告し無条件実行しない。
    """
    stonks_set = set(stonks_tickers())
    excluded = [t for t in target if t not in stonks_set]
    if excluded:
        print(
            f"警告: stonks_silo=false のため除外: {', '.join(excluded)}"
            f"（cik_lookup.csvでstonks_silo=trueに変更しない限り処理されません）"
        )
    return [t for t in target if t in stonks_set]


# ---------------------------------------------------------------------------
# シリアライズ
# ---------------------------------------------------------------------------

def _to_dict(obj) -> object:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _to_dict(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, dict):
        return {k: _to_dict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_dict(v) for v in obj]
    if isinstance(obj, float):
        if obj == float("inf") or obj != obj:
            return None
        return obj
    return obj


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def run(tickers: list[str] | None = None) -> dict:
    """
    tickers=None のとき stonks_tickers() で stonks_silo=true 全件処理。
    特定ティッカー指定時は既存 results.json とマージする。
    """
    partial = tickers is not None
    if partial:
        target = _filter_stonks_silo_tickers([t.upper() for t in tickers])
    else:
        target = stonks_tickers()

    if not target:
        print("対象ティッカーが見つかりません。cik_lookup.csv の stonks_silo 列を確認してください。")
        return {}

    print(f"対象: {len(target)} 銘柄  {target}")

    analyzer = StonksAnalyzer()
    sec_reader = SECReader()
    results = {}
    errors = {}

    for ticker in target:
        try:
            data = load_annual_data(ticker, years=_YEARS)
            val = fetch_valuation(ticker)
            # [[NETCASH-DUAL-CALC-1]]: 独自計算（cash+STI - yfinance totalDebt）を廃止し、
            # TANUKI VALUATIONと同じSECReader.get_net_cash()（SEC XBRL・四半期
            # フォールバック・セクターガードあり）に統一（2026-08-13）。
            # [[BBAI-RDW-RUNWAY-VERIFICATION-1]]: Runwayのcashも同じ返却値から
            # 四半期優先で算出するため、analyze()より前に取得して渡す。
            net_cash_data = sec_reader.get_net_cash(ticker, sector=val["sector"], industry=val["industry"])
            analysis = analyzer.analyze(data, net_cash_data=net_cash_data)
            result = _to_dict(analysis)
            # [[NETINCOME-DUAL-PIPELINE-1]]: 出力表示用フィールドをnet_income_fy
            # へ改名（単年度決算であることの明示、NAMING_CONVENTIONS.md規則2）。
            # analyzer.analyze()が参照するdata["records"][yr]["pl"]["net_income"]
            # （internal scoring用）は変更しない——両者は別スコープであり、
            # overall_score/overall_verdict算出には無関係（2026-08-13確認済み）。
            result["records"] = {
                str(yr): {
                    "revenue":       _to_dict(rec["pl"].get("revenue")),
                    "net_income_fy": _to_dict(rec["pl"].get("net_income")),
                }
                for yr, rec in data["records"].items()
            }

            latest_rev = None
            for yr in reversed(data["years"]):
                r = data["records"][yr]["pl"].get("revenue_sanitized")
                if r:
                    latest_rev = r
                    break

            psr = val["market_cap"] / latest_rev if val["market_cap"] and latest_rev else None
            ev_sales = val["enterprise_value"] / latest_rev if val["enterprise_value"] and latest_rev else None

            total_debt = val["total_debt"] or 0
            net_cash = net_cash_data["net_cash"] if net_cash_data.get("available") else None

            result["valuation"] = {
                "market_cap":       val["market_cap"],
                "current_price":    val["current_price"],
                "enterprise_value": val["enterprise_value"],
                "total_debt":       val["total_debt"],
                "psr":              round(psr, 1) if psr else None,
                "ev_sales":         round(ev_sales, 1) if ev_sales else None,
                "net_cash":         net_cash,
                "fetched_at":       val["fetched_at"],
                "error":            val["error"],
            }

            results[ticker] = result
            print(f"  [{ticker}] {analysis.overall_verdict} (score={analysis.overall_score})")
        except FileNotFoundError:
            errors[ticker] = "annual_*.json not found"
            print(f"  [{ticker}] SKIP — データなし")
        except Exception as e:
            errors[ticker] = str(e)
            print(f"  [{ticker}] ERROR — {e}")

    # 特定ティッカー指定時は既存データとマージ（全件消去を防ぐ）
    if partial and _OUTPUT_FILE.exists():
        try:
            with open(_OUTPUT_FILE, encoding="utf-8") as f:
                existing = json.load(f)
            merged = existing.get("tickers", {})
            merged_errors = existing.get("errors", {})
            merged.update(results)
            merged_errors.update(errors)
            results = merged
            errors = merged_errors
        except Exception:
            pass

    # stonks_silo=true の銘柄のみ残す（削除・false変更に追従）
    valid = set(stonks_tickers())
    results = {k: v for k, v in results.items() if k in valid}
    errors  = {k: v for k, v in errors.items()  if k in valid}

    # 財務ベクトル計算（全銘柄のパーセンタイルを一括計算）
    try:
        all_normalized = load_all_normalized(list(results.keys()))
        vectors = compute_vectors(all_normalized)
        for ticker, vec in vectors.items():
            if ticker in results:
                results[ticker]["financial_vectors"] = vec
        print(f"財務ベクトル計算完了: {len(vectors)} 銘柄")
    except Exception as e:
        print(f"財務ベクトル計算エラー: {e}")
        import traceback
        traceback.print_exc()

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ticker_count": len(results),
        "tickers": results,
        "errors": errors,
        # [[STONKS-FINANCIAL-VECTORS-RELATIVE-1]]、2026-09-06: 各ticker配下の
        # financial_vectors.fields.{field}.{yoy|qoq}.angle/length/percentileは
        # 絶対的な変化率ではなく、この実行時点の相対順位であることの注記
        # （開発者向けメタ情報。画面表示には影響しない。実測の結果、現行の
        # docs/value-monitor/stonks-silo/index.htmlはangle/length/percentile/
        # compositeを一切参照しておらず〈change_pct等の絶対値のみ表示〉、
        # 画面表示箇所は見つからなかったため対応方針③を採用した）。
        "financial_vectors_note": (
            "financial_vectors配下のangle/length/percentile/compositeは、"
            "同時点で有効なchange_pctを持つ銘柄集合に対する相対順位であり、"
            "絶対的な変化率ではない。新規銘柄の追加・除外のたびに、対象銘柄"
            "自身のデータが変わっていなくても値が変動しうる。各yoy/qoq辞書の"
            "population_sizeが、そのフィールド・期間における実際の比較対象"
            "銘柄数（母集団サイズ）。change_pct/val_latest/val_prev/series_q"
            "は絶対値のため母集団変動の影響を受けない。過去の保存値と時系列"
            "比較する際はこの点に留意すること（[[STONKS-FINANCIAL-VECTORS-"
            "RELATIVE-1]]参照）。"
        ),
    }

    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n保存完了: {_OUTPUT_FILE}")
    print(f"成功={len(results)}  スキップ/エラー={len(errors)}")
    return output


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # [[STONKS-SILO-CLI-TICKERS-SHADOW-1]]: この変数名を`tickers`にすると
    # ファイル冒頭の`from common.sec_data import tickers`（モジュール参照、
    # stonks_tickers()が`tickers.get_stonks_silo_tickers()`として使う）を
    # グローバルスコープごと上書きしてしまい、CLI引数あり・なし両方の実行
    # パスで`AttributeError`になる（2026-07-13以降、Stonks_Silo_Update.ymlの
    # 自動cronも含め全実行が失敗していた実障害）。モジュール名と衝突しない
    # 名前にする。
    cli_tickers = sys.argv[1:] if len(sys.argv) > 1 else None
    run(cli_tickers)
