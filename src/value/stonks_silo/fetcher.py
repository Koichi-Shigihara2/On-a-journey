# src/value/stonks_silo/fetcher.py
"""
Stonks Silo Fetcher
annual_*.json から複数年データを読み込む専用フェッチャー。
TANUKIパイプラインとは独立して動作する。

データパス: common/sec_data/data/{TICKER}/annual_*.json
"""

import json
import re
from pathlib import Path
from typing import Optional


# リポジトリルートからの相対パス
# このファイルは src/value/stonks_silo/ に置かれる想定
_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parents[2]  # src/value/stonks_silo/ → 3階層上
_SEC_DATA_DIR = _REPO_ROOT / "common" / "sec_data" / "data"


def _parse_year(filename: str) -> Optional[int]:
    """annual_2022.json → 2022"""
    m = re.search(r"annual_(\d{4})\.json$", filename)
    return int(m.group(1)) if m else None


def load_annual_data(ticker: str, years: int = 5) -> dict:
    """
    指定ティッカーの annual_*.json を最新N年分読み込む。

    Returns
    -------
    {
        "ticker": "PLTR",
        "years": [2020, 2021, 2022, 2023, 2024],
        "records": {
            2020: { "pl": {...}, "cf": {...}, "bs": {...} },
            ...
        },
        "errors": []   # 読み込めなかった年・フィールドの記録
    }
    """
    ticker = ticker.upper()
    ticker_dir = _SEC_DATA_DIR / ticker

    if not ticker_dir.exists():
        raise FileNotFoundError(
            f"Ticker directory not found: {ticker_dir}\n"
            f"Available tickers: {_list_available_tickers()}"
        )

    # annual_*.json を列挙してソート
    annual_files = sorted(
        [(f, _parse_year(f.name)) for f in ticker_dir.glob("annual_*.json")
         if _parse_year(f.name) is not None],
        key=lambda x: x[1],
        reverse=True,
    )

    if not annual_files:
        raise FileNotFoundError(f"No annual_*.json found in {ticker_dir}")

    # 最新 N 年分を取得（古い順に並べ直す）
    selected = annual_files[:years][::-1]

    records = {}
    errors = []

    for path, year in selected:
        try:
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
            records[year] = _normalize_record(raw, year, ticker, errors)
        except json.JSONDecodeError as e:
            errors.append({"year": year, "error": f"JSON parse error: {e}"})
        except Exception as e:
            errors.append({"year": year, "error": str(e)})

    _sanitize_revenue(records, errors)

    return {
        "ticker": ticker,
        "years": sorted(records.keys()),
        "records": records,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# 内部ヘルパー
# ---------------------------------------------------------------------------

_PL_FIELDS = [
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "research_and_development",
    "selling_and_marketing",
    "selling_general_and_administrative",
]

_CF_FIELDS = [
    "operating_cash_flow",
    "capital_expenditure",
    "free_cash_flow",
    "stock_based_compensation",
    "depreciation_and_amortization",
]

_BS_FIELDS = [
    "cash_and_equivalents",
    "short_term_investments",
    "total_debt",
    "stockholders_equity",
    "shares_outstanding",
    "shares_diluted",
]


def _normalize_record(raw: dict, year: int, ticker: str, errors: list) -> dict:
    """
    annual_*.json の生データを Stonks Silo 標準形式に変換する。
    フィールドが存在しない場合は None を入れ、errors に記録する。
    """
    def _extract(section: str, fields: list) -> dict:
        section_data = raw.get(section, {})
        result = {}
        for f in fields:
            val = section_data.get(f)
            if val is None:
                errors.append({
                    "year": year,
                    "section": section,
                    "field": f,
                    "warning": "missing",
                })
            result[f] = val
        return result

    pl = _extract("pl", _PL_FIELDS)
    cf = _extract("cf", _CF_FIELDS)
    bs = _extract("bs", _BS_FIELDS)

    # CapEx符号正規化（CAPEX-SIGN-UNNORMALIZED-1）: SEC XBRLのCapExは
    # 発行体によって正負どちらの符号でも報告されるため、生値抽出直後に
    # abs()を適用する（既存のfree_cash_flowフォールバック計算が
    # abs(capex)を使っているのと同じ正規化を生値自体にも適用）。
    if cf["capital_expenditure"] is not None:
        cf["capital_expenditure"] = abs(cf["capital_expenditure"])

    # free_cash_flow が欠損でも OCF + CapEx から補完
    if cf["free_cash_flow"] is None:
        ocf = cf["operating_cash_flow"]
        capex = cf["capital_expenditure"]
        if ocf is not None and capex is not None:
            # CapEx は通常マイナス値で格納されているが、符号が揃っていない場合も考慮
            cf["free_cash_flow"] = ocf - abs(capex)
            errors.append({
                "year": year,
                "section": "cf",
                "field": "free_cash_flow",
                "warning": "derived_from_ocf_capex",
            })

    # gross_profit が欠損の場合、revenue - cost 系タグから補完
    if pl["gross_profit"] is None:
        rev = pl.get("revenue")
        pl_raw = raw.get("pl", {})
        cost = pl_raw.get("cost_of_revenue")
        if rev is not None and cost is not None:
            pl["gross_profit"] = rev - cost
            pl["gross_profit_derived"] = True
            errors.append({
                "year": year,
                "section": "pl",
                "field": "gross_profit",
                "warning": "derived_from_revenue_minus_cost",
            })

    return {"pl": pl, "cf": cf, "bs": bs, "raw_year": year}


def _sanitize_revenue(records: dict, errors: list) -> None:
    """
    全年の revenue を比較し外れ値を検出する（2パス方式）。
    生データ pl["revenue"] は一切変更せず、結果を以下に書き込む:
      pl["revenue_sanitized"]  : 外れ値なら None、正常なら revenue と同値
      pl["revenue_is_outlier"] : 外れ値除去された場合 True

    Pass1: 最大値を除いた中央値の10倍超 → 上限外れ値 (XBRL過大申告対策)
           最新年は保護（事業収益化開始による急成長を誤除去しない）
    Pass2: Pass1後の残存値（最新年除く）で中央値を再計算し1/10未満 → 下限外れ値
           最新年を中央値計算・判定対象の両方から除外
           (収益化開始期の急成長で中央値が歪んで過去年を誤除去するのを防ぐ)
    """
    # 全レコードの sanitized フィールドを初期化
    for rec in records.values():
        rec["pl"]["revenue_sanitized"] = rec["pl"].get("revenue")
        rec["pl"]["revenue_is_outlier"] = False

    values = [
        (yr, rec["pl"]["revenue"])
        for yr, rec in records.items()
        if rec["pl"].get("revenue") is not None and rec["pl"]["revenue"] > 0
    ]
    if len(values) < 2:
        return
    sorted_vals = sorted(v for _, v in values)
    latest_year = max(yr for yr, _ in values)

    # Pass1: 上限外れ値 — 最大値を除いた中央値の10倍超
    non_max = sorted_vals[:-1]
    median_hi = non_max[len(non_max) // 2]
    if median_hi <= 0:
        return
    for yr, v in values:
        if yr == latest_year:
            continue  # 最新年は保護
        if v > median_hi * 10:
            records[yr]["pl"]["revenue_sanitized"] = None
            records[yr]["pl"]["revenue_is_outlier"] = True
            errors.append({
                "year": yr, "section": "pl", "field": "revenue",
                "warning": f"outlier_removed_high (val={v:.0f} > median*10={median_hi*10:.0f})",
            })

    # Pass2: 下限外れ値 — Pass1後の残存値（最新年除く）で中央値を再計算し1/10未満
    remaining = sorted(
        v for yr, v in values
        if yr != latest_year and not records[yr]["pl"]["revenue_is_outlier"]
    )
    if len(remaining) < 2:
        return
    non_min = remaining[1:]
    median_lo = non_min[len(non_min) // 2]
    if median_lo <= 0:
        return
    for yr, v in values:
        if yr == latest_year:
            continue  # 最新年は保護
        if not records[yr]["pl"]["revenue_is_outlier"] and v < median_lo / 10:
            records[yr]["pl"]["revenue_sanitized"] = None
            records[yr]["pl"]["revenue_is_outlier"] = True
            errors.append({
                "year": yr, "section": "pl", "field": "revenue",
                "warning": f"outlier_removed_low (val={v:.0f} < median/10={median_lo/10:.0f})",
            })


def _list_available_tickers() -> list[str]:
    if not _SEC_DATA_DIR.exists():
        return []
    return sorted(d.name for d in _SEC_DATA_DIR.iterdir() if d.is_dir())


def available_tickers() -> list[str]:
    """利用可能なティッカー一覧を返す"""
    return _list_available_tickers()


def _fmt(v) -> str:
    if v is None:
        return "N/A"
    if abs(v) >= 1e9:
        return f"${v/1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"${v/1e6:.0f}M"
    return f"${v:,.0f}"


# ---------------------------------------------------------------------------
# CLI 簡易テスト
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    ticker = sys.argv[1] if len(sys.argv) > 1 else "PLTR"
    try:
        data = load_annual_data(ticker, years=5)
        print(f"\n=== {data['ticker']} ({len(data['years'])} years) ===")
        print(f"Years: {data['years']}")
        for yr, rec in data["records"].items():
            rev = rec["pl"].get("revenue")
            ocf = rec["cf"].get("operating_cash_flow")
            cash = rec["bs"].get("cash_and_equivalents")
            print(
                f"  {yr}: Revenue={_fmt(rev)}  OCF={_fmt(ocf)}  Cash={_fmt(cash)}"
            )
        if data["errors"]:
            print(f"\nWarnings ({len(data['errors'])}):")
            for e in data["errors"][:10]:
                print(f"  {e}")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
