"""
TANUKI VALUATION - Segment Growth Configuration v8.0
セグメント別成長率設定

v8.0 変更点:
  - 設定値を config/segment_config.json / config/growth_options_config.json から読み込む
  - ハードコードの SEGMENT_OVERRIDES / GROWTH_OPTIONS を廃止
  - 後方互換: get_segment_growth() / get_growth_options() 等のAPIは変更なし
  - フォールバック: JSONが存在しない場合はエラーを出力して空dictを返す

設定変更方法:
  - admin.html（docs/value-monitor/admin.html）からフォーム入力
  - GitHub API経由で config/segment_config.json / config/growth_options_config.json を更新
  - ローカル直接編集は非推奨（admin.htmlを使用すること）
"""

import json
import os
import sys
from typing import Dict, Any, Optional, List
from datetime import datetime


# ============================================================
# JSON設定ファイルの読み込み
# ============================================================

def _find_config_dir() -> str:
    """
    config/ ディレクトリを探す

    探索順:
      1. GITHUB_WORKSPACE 環境変数（GitHub Actions）
      2. このファイルから4階層上（ローカル: src/value/tanuki_valuation/ → repo root）
    """
    # GitHub Actions
    workspace = os.environ.get("GITHUB_WORKSPACE", "")
    if workspace:
        path = os.path.join(workspace, "config")
        if os.path.isdir(path):
            return path

    # ローカル
    current = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(current)))
    path = os.path.join(repo_root, "config")
    if os.path.isdir(path):
        return path

    raise FileNotFoundError(
        f"config/ ディレクトリが見つかりません。"
        f"migrate_config_to_json.py を実行してください。"
    )


def _load_json(filename: str) -> Dict[str, Any]:
    """JSONファイルを読み込む"""
    try:
        config_dir = _find_config_dir()
        path = os.path.join(config_dir, filename)
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as e:
        print(f"[ERROR] {filename} が見つかりません: {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"[ERROR] {filename} のJSON解析エラー: {e}")
        return {}


# 起動時に一度だけ読み込む
_SEGMENT_CONFIG: Dict[str, Any] = {}
_GROWTH_OPTIONS_CONFIG: Dict[str, Any] = {}

# General fallback銘柄へのTTM Revenue Growth注入（pipeline.pyから設定）
_GROWTH_OVERRIDES: Dict[str, float] = {}

def _ensure_loaded() -> None:
    """設定が未ロードなら読み込む（遅延初期化）"""
    global _SEGMENT_CONFIG, _GROWTH_OPTIONS_CONFIG
    if not _SEGMENT_CONFIG:
        _SEGMENT_CONFIG = _load_json("segment_config.json")
    if not _GROWTH_OPTIONS_CONFIG:
        _GROWTH_OPTIONS_CONFIG = _load_json("growth_options_config.json")


def reload_config() -> None:
    """設定を強制再読み込み（テスト・デバッグ用）"""
    global _SEGMENT_CONFIG, _GROWTH_OPTIONS_CONFIG
    _SEGMENT_CONFIG = {}
    _GROWTH_OPTIONS_CONFIG = {}
    _ensure_loaded()


def set_growth_override(ticker: str, growth_rate: float) -> None:
    """General fallback銘柄にTTM Revenue Growthを注入する（pipeline.pyから呼ぶ）"""
    _GROWTH_OVERRIDES[ticker] = growth_rate


def clear_growth_override(ticker: str) -> None:
    """TTMオーバーライドをクリアする"""
    _GROWTH_OVERRIDES.pop(ticker, None)


# ============================================================
# 公開API（既存コードとの後方互換を維持）
# ============================================================

def get_segment_growth(ticker: str) -> Optional[Dict[str, Any]]:
    """
    セグメント加重平均成長率を取得

    Returns:
        {enabled, weighted_growth, fiscal_year, segments, source} or None
    """
    _ensure_loaded()

    # _GROWTH_OVERRIDES が設定されていれば全銘柄で最優先（DCF自動再計算用）
    if ticker in _GROWTH_OVERRIDES:
        return {
            "enabled": True,
            "weighted_growth": _GROWTH_OVERRIDES[ticker],
            "source": "growth_override",
        }

    config = _SEGMENT_CONFIG.get(ticker)
    if not config or config.get("_meta") or not config.get("enabled", False):
        return None

    segments = config.get("segments", {})
    if not segments:
        return None

    weighted_growth = sum(
        seg.get("weight", 0) * seg.get("growth", 0)
        for seg in segments.values()
    )
    total_weight = sum(seg.get("weight", 0) for seg in segments.values())
    if not (0.95 <= total_weight <= 1.05):
        print(f"[WARN] {ticker} segment weights sum to {total_weight:.2f}, expected ~1.0")

    return {
        "enabled": True,
        "weighted_growth": weighted_growth,
        "fiscal_year": config.get("fiscal_year"),
        "segments": segments,
        "source": "segment_config"
    }


def get_growth_options(ticker: str) -> List[Dict[str, Any]]:
    """
    成長オプションリストを取得（PV計算済み）

    Returns:
        [{name, tam, penetration, ..., expected_fcf, pv}, ...]
    """
    _ensure_loaded()
    ticker_data = _GROWTH_OPTIONS_CONFIG.get(ticker, {})
    options = ticker_data.get("options", [])
    if not options:
        return []

    enriched = []
    for opt in options:
        expected_fcf = (
            opt["tam"] * opt["penetration"]
            * opt["fcf_margin"] * opt["probability"]
        )
        pv = expected_fcf / (1 + opt["discount_rate"]) ** opt["delay_years"]
        enriched.append({**opt, "expected_fcf": expected_fcf, "pv": pv})
    return enriched


def calculate_growth_option_total_pv(ticker: str) -> Dict[str, Any]:
    """成長オプションの合計PVを計算"""
    options = get_growth_options(ticker)
    total_pv = sum(opt["pv"] for opt in options)
    return {"total_pv": total_pv, "options": options, "count": len(options)}


if __name__ == "__main__":
    print("=== Segment Growth（JSON読み込み版）===")
    tickers = ["NVDA", "TSLA", "PLTR", "MSFT", "AMZN", "AMD", "SOFI", "RKLB", "APP", "CELH", "SOUN", "ONDS"]
    for ticker in tickers:
        result = get_segment_growth(ticker)
        if result:
            print(f"{ticker}: {result['weighted_growth']:.1%}  ({result['fiscal_year']})")
        else:
            print(f"{ticker}: not configured")

    print("\n=== Growth Options ===")
    for ticker in ["NVDA", "TSLA", "PLTR", "MSFT", "AMZN", "AMD", "APP"]:
        opts = get_growth_options(ticker)
        total_pv = sum(o["pv"] for o in opts)
        print(f"{ticker}: {len(opts)}件  total_pv=${total_pv/1e9:.2f}B")
