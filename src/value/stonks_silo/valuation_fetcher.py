"""
src/value/stonks_silo/valuation_fetcher.py
STONKS SILO 評価指標取得（market_cap/current_price/enterprise_value/total_debt/sector/industry）

[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-3: yfinance直接呼び出し
（.info単発）をcommon.market_data.reader経由に切替（2026-08-11）。
- current_price: reader.get_latest_price()["close"]（daily/層、前日終値化。
  data_fetcher.py/beta_fetcher.pyと一貫した方針）
- market_cap/enterprise_value/total_debt: reader.get_attributes()から
  （enterprise_valueは本切替のために新規追加したattributes/フィールド）

[[NETCASH-DUAL-CALC-1]]実装（2026-08-13）: pipeline.py側のnet_cash計算を
SECReader.get_net_cash()経由に統一するため、セクターガード判定に必要な
sector/industryをreader.get_attributes()から追加取得する
（data_fetcher.py:562-565と同じ取得パターン・デフォルト値
"default"/""を踏襲）。PSR/EV_Sales算出ロジックには手を入れていない。
"""

import os
import sys
from datetime import datetime, timezone

# common/market_data - beta_fetcher.py/data_fetcher.pyと同型の二段構え
# sys.path解決パターン。本ファイルはsrc/value/stonks_silo/pipeline.py
# 経由でimportされる場合はpipeline.py自身が既にリポジトリルートを
# sys.pathへ追加済みだが、本ファイル単体でimportされるケース
# （単体テスト等）にも対応できるよう独立して解決する。
HAS_MARKET_DATA = False
_md_get_latest_price = None
_md_get_attributes = None

try:
    _current_dir = os.path.dirname(os.path.abspath(__file__))
    _repo_root = os.path.dirname(os.path.dirname(os.path.dirname(_current_dir)))
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)

    from common.market_data.reader import get_latest_price as _md_get_latest_price
    from common.market_data.reader import get_attributes as _md_get_attributes
    HAS_MARKET_DATA = True
except Exception:
    pass

if not HAS_MARKET_DATA:
    try:
        _github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if _github_workspace and _github_workspace not in sys.path:
            sys.path.insert(0, _github_workspace)
        from common.market_data.reader import get_latest_price as _md_get_latest_price
        from common.market_data.reader import get_attributes as _md_get_attributes
        HAS_MARKET_DATA = True
    except Exception:
        pass


def fetch_valuation(ticker: str) -> dict:
    """market_cap/current_price/enterprise_value/total_debt/sector/industryを取得する。

    reader側がデータ未生成銘柄に対してNoneを返す場合、該当フィールドは
    Noneのまま返す（旧コードが.info経由の値がたまたまNoneだった場合と
    同じ扱い、errorは設定しない）。common.market_data.reader自体が
    import不可の場合（HAS_MARKET_DATA=False）・予期しない例外発生時のみ
    errorにメッセージを設定する（旧来のyfinance完全失敗時except節と
    同じ中立デフォルトパターン、data_fetcher.py切替時の選択肢Aを踏襲）。

    sector/industryはSECReader.get_net_cash()のセクターガード引数用
    （[[NETCASH-DUAL-CALC-1]]）。data_fetcher.py:562-565と同じく、
    未取得時のデフォルト値は"default"/""とする（Noneにしない。
    get_net_cash()内の_is_insurance_local()がsector比較に文字列を
    期待するため）。
    """
    fetched_at = datetime.now(timezone.utc).isoformat()

    if not HAS_MARKET_DATA:
        return {
            "market_cap": None,
            "current_price": None,
            "enterprise_value": None,
            "total_debt": None,
            "sector": "default",
            "industry": "",
            "fetched_at": fetched_at,
            "error": "common.market_data.reader unavailable",
        }

    try:
        latest_price = _md_get_latest_price(ticker)
        current_price = (
            float(latest_price["close"])
            if latest_price is not None and latest_price.get("close") is not None
            else None
        )

        attrs = _md_get_attributes(ticker)
        market_cap = attrs.get("market_cap") if attrs is not None else None
        enterprise_value = attrs.get("enterprise_value") if attrs is not None else None
        total_debt = attrs.get("total_debt") if attrs is not None else None
        sector = (attrs.get("sector") if attrs is not None else None) or "default"
        industry = (attrs.get("industry") if attrs is not None else None) or ""

        return {
            "market_cap": market_cap,
            "current_price": current_price,
            "enterprise_value": enterprise_value,
            "total_debt": total_debt,
            "sector": sector,
            "industry": industry,
            "fetched_at": fetched_at,
            "error": None,
        }
    except Exception as e:
        return {
            "market_cap": None,
            "current_price": None,
            "enterprise_value": None,
            "total_debt": None,
            "sector": "default",
            "industry": "",
            "fetched_at": fetched_at,
            "error": str(e),
        }
