"""
TANUKI VALUATION - Maturity Profile Configuration v8.0
銘柄別成熟曲線パラメータ設定

v8.0 変更点:
  - 設定値を config/maturity_config.json から読み込む
  - ハードコードの MATURITY_PROFILES を廃止
  - 後方互換: get_maturity_profile() / is_three_stage() / get_terminal_growth() のAPIは変更なし
  - フォールバック: JSONが存在しない場合は two_stage をデフォルトとして返す

成熟タイプ:
  "three_stage" : Phase1（高成長）→ Phase2（移行）→ ターミナル
  "two_stage"   : 既存モデルと同一（デフォルト）

phase1.growth = null の場合は segment_config の加重平均成長率を流用する。

設定変更方法:
  - admin.html（docs/value-monitor/admin.html）からフォーム入力
  - GitHub API経由で config/maturity_config.json を更新
  - ローカル直接編集は非推奨（admin.htmlを使用すること）
"""

import json
import os
from typing import Dict, Any, Optional


# ============================================================
# JSON設定ファイルの読み込み
# ============================================================

def _find_config_dir() -> str:
    """config/ ディレクトリを探す"""
    workspace = os.environ.get("GITHUB_WORKSPACE", "")
    if workspace:
        path = os.path.join(workspace, "config")
        if os.path.isdir(path):
            return path

    current = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(current)))
    path = os.path.join(repo_root, "config")
    if os.path.isdir(path):
        return path

    raise FileNotFoundError(
        "config/ ディレクトリが見つかりません。"
        "migrate_config_to_json.py を実行してください。"
    )


# ============================================================
# WACC-1: Damodaran 業種別ターミナル成長率テーブル
# 長期 GDP 成長率（〜2%）＋セクター別構造成長プレミアム
# ============================================================
_DAMODARAN_TV_G: Dict[str, float] = {
    # テクノロジー：デジタル経済の長期構造成長を反映（3.5%）
    "Software (System & Application)": 0.035,
    "Software (Internet)":             0.035,
    "Computers/Peripherals":           0.035,
    "Semiconductor":                   0.035,
    "Semiconductor Equip":             0.035,
    "Information Services":            0.035,
    "Heathcare Information and Technology": 0.035,
    # 広告・エンターテインメント
    "Advertising":                     0.030,
    "Software (Entertainment)":        0.030,
    "Entertainment":                   0.025,
    "Broadcasting":                    0.020,
    # 防衛・産業
    "Aerospace/Defense":               0.030,
    "Machinery":                       0.025,
    "Electrical Equipment":            0.025,
    # ヘルスケア・製薬
    "Drugs (Biotechnology)":           0.030,
    "Drugs (Pharmaceutical)":          0.030,
    "Healthcare Products":             0.030,
    "Healthcare Support Services":     0.025,
    # 金融
    "Financial Svcs. (Non-bank & Insurance)": 0.030,
    "Bank (Money Center)":             0.025,
    "Banks (Regional)":                0.025,
    # 消費者：成熟市場・価格決定力は限定的（2.5%）
    "Restaurant/Dining":               0.025,
    "Beverage (Soft)":                 0.025,
    "Beverage (Alcoholic)":            0.020,
    "Retail (General)":                0.025,
    "Retail (Special Lines)":          0.025,
    "Apparel":                         0.020,
    # モビリティ・通信
    "Auto & Truck":                    0.030,
    "Air Transport":                   0.030,
    "Telecom. Equipment":              0.025,
    "Telecom. Services":               0.020,
    # 公益・不動産（規制産業、低成長）
    "Power":                           0.020,
    "Utility (General)":               0.020,
    "Utility (Water)":                 0.020,
    "R.E.I.T.":                        0.025,
    "Real Estate (Development)":       0.025,
}

# ============================================================
# WACC-1: ポートフォリオ銘柄別ターミナル成長率（直引きテーブル）
# _DAMODARAN_TV_G の業種マッピングを適用した結果をキャッシュ。
# 新規銘柄追加時はここに追記する。
# ============================================================
_TICKER_TV_G: Dict[str, float] = {
    # ── 半導体・フォトニクス（3.5%）──
    "NVDA": 0.035, "AMD": 0.035, "AMAT": 0.035,
    "MRVL": 0.035, "COHR": 0.035, "LITE": 0.035, "SITM": 0.035,
    "ALAB": 0.035,
    # ── ビッグテック（3.5%）──
    "MSFT": 0.035, "AMZN": 0.035, "GOOGL": 0.035, "AAPL": 0.035,
    "TSLA": 0.035,
    # ── 広告プラットフォーム（3.0%）──
    "META": 0.030,
    # ── エンタープライズ・SaaS（3.5%）──
    "NOW": 0.035, "CRM": 0.035, "ADBE": 0.035, "ADSK": 0.035,
    "INTU": 0.035, "CDNS": 0.035, "BSY": 0.035, "CSGP": 0.035,
    # ── サイバーセキュリティ・SaaS（3.5%）──
    "DDOG": 0.035, "ZS": 0.035, "NET": 0.035, "S": 0.035,
    "GTLB": 0.035, "ESTC": 0.035, "RBRK": 0.035, "IOT": 0.035,
    # ── インターネット・プラットフォーム（3.5%）──
    "BKNG": 0.035, "CART": 0.035,
    # ── AI・アナリティクス（3.5%）──
    "PLTR": 0.035, "APP": 0.035, "ZETA": 0.035, "CRWV": 0.035,
    # ── FinTech（金融寄り 3.0%）──
    "PAYS": 0.030, "SOFI": 0.030,
    # ── 防衛・航空宇宙（3.0%）──
    "LMT": 0.030, "HEI": 0.030, "HWM": 0.030, "TDY": 0.030,
    "KULR": 0.030, "AVAV": 0.030,
    # ── 産業機械（2.5%）──
    "HON": 0.025,
    # ── 電力インフラ（3.0%）──
    "VRT": 0.030, "CEG": 0.030,
    # ── ヘルスケア・製薬（3.0%）──
    "LLY": 0.030, "RXRX": 0.030,
    # ── 消費者・飲食料（2.5%）──
    "CAKE": 0.025, "KO": 0.025, "CELH": 0.025, "ELF": 0.025,
    # ── 宇宙・投機的テック（3.5% or 3.0%）──
    "RKLB": 0.035, "ASTS": 0.035, "SPIR": 0.030,
    "IONQ": 0.030, "JOBY": 0.030, "QBTS": 0.030,
    "SOUN": 0.030, "BBAI": 0.030, "RCAT": 0.030, "ONDS": 0.030,
}

_DEFAULT_PROFILE: Dict[str, Any] = {
    "type": "two_stage",
    "terminal_growth": 0.03
}

_MATURITY_CONFIG: Dict[str, Any] = {}


def resolve_maturity_config_path() -> Optional[str]:
    """maturity_config.jsonのパス解決ロジック（report_consistency_check.pyの
    設定ファイル読み込み横断チェックと共用するため、2026-09-13に
    _ensure_loaded()から切り出した。[[CONFIG-LOAD-SILENT-FALLBACK-1]]、
    data_fetcher.py::resolve_beta_config_path()と同型パターン）。

    Returns:
        解決できたパス（存在確認済み）、解決できなければNone
    """
    try:
        config_dir = _find_config_dir()
    except FileNotFoundError:
        return None
    path = os.path.join(config_dir, "maturity_config.json")
    return path if os.path.exists(path) else None


def _ensure_loaded() -> None:
    """設定が未ロードなら読み込む（遅延初期化）"""
    global _MATURITY_CONFIG
    if not _MATURITY_CONFIG:
        try:
            config_dir = _find_config_dir()
            path = os.path.join(config_dir, "maturity_config.json")
            with open(path, encoding="utf-8") as f:
                _MATURITY_CONFIG = json.load(f)
        except FileNotFoundError as e:
            print(f"[ERROR] maturity_config.json が見つかりません: {e}")
            _MATURITY_CONFIG = {"_default": _DEFAULT_PROFILE.copy()}
        except json.JSONDecodeError as e:
            print(f"[ERROR] maturity_config.json のJSON解析エラー: {e}")
            _MATURITY_CONFIG = {"_default": _DEFAULT_PROFILE.copy()}


def reload_config() -> None:
    """設定を強制再読み込み（テスト・デバッグ用）"""
    global _MATURITY_CONFIG
    _MATURITY_CONFIG = {}
    _ensure_loaded()


# ============================================================
# 公開API（既存コードとの後方互換を維持）
# ============================================================

def get_maturity_profile(ticker: str) -> Dict[str, Any]:
    """
    銘柄の成熟プロファイルを取得

    Args:
        ticker: 銘柄コード

    Returns:
        成熟プロファイルのdict（未定義の場合は _default を返す）
    """
    _ensure_loaded()
    profile = _MATURITY_CONFIG.get(ticker)
    if profile is None or ticker.startswith("_"):
        default = _MATURITY_CONFIG.get("_default", _DEFAULT_PROFILE)
        return default.copy()
    return profile.copy()


def is_three_stage(ticker: str) -> bool:
    """3段階DCFを使用するか判定"""
    profile = get_maturity_profile(ticker)
    return profile.get("type") == "three_stage"


def _lookup_tv_g_by_industry(ticker: str) -> Optional[float]:
    """
    ターミナル成長率をセクター別テーブルから解決する。

    優先順位:
    1. _TICKER_TV_G（ポートフォリオ銘柄直引きテーブル）
    2. growth_sanity.TICKER_INDUSTRY_OVERRIDES → _DAMODARAN_TV_G
       （TICKER_INDUSTRY_OVERRIDES はSIC誤分類銘柄のみ登録済み）
    growth_sanity がテストでモック済みの場合は gracefully に None を返す。
    """
    # 直引きテーブルを優先
    tv_g = _TICKER_TV_G.get(ticker)
    if tv_g is not None:
        return tv_g
    # growth_sanity の SIC 上書きマッピングで解決
    try:
        import growth_sanity as _gs
        overrides = getattr(_gs, "TICKER_INDUSTRY_OVERRIDES", None)
        if isinstance(overrides, dict):
            industry = overrides.get(ticker)
            if industry and isinstance(industry, str):
                return _DAMODARAN_TV_G.get(industry)
    except Exception:
        pass
    return None


def get_terminal_growth(ticker: str) -> float:
    """銘柄のターミナル成長率を取得

    優先順位:
    1. maturity_config.json の ticker 個別設定
       - terminal_growth_explicit: true が明示されていれば、値がデフォルト
         3.0%と一致していても常にこれを採用する（[[TVGROWTH-EXPLICIT-
         DEFAULT-AMBIGUOUS-1]]対応: 従来は差分チェックのみのため、3.0%を
         意図的に指定しても「未設定」とみなされセクター別テーブルの値に
         上書きされてしまう抜け道がなかった）
       - 上記フラグがない場合は従来通り、3.0% 以外に明示設定された場合のみ
         採用する（後方互換。既存のmaturity_config.jsonエントリは全て
         このフラグを持たないため、本変更による既存挙動への影響はない）
    2. growth_sanity.TICKER_INDUSTRY_OVERRIDES → _DAMODARAN_TV_G（セクター別）
    3. デフォルト 3.0%
    """
    _DEFAULT_TV_G = 0.03
    profile = get_maturity_profile(ticker)
    ticker_tv_g = profile.get("terminal_growth", _DEFAULT_TV_G)
    if profile.get("terminal_growth_explicit") is True:
        return ticker_tv_g
    # JSON に 3.0% 以外の値が明示設定されていればそれを使う
    if abs(ticker_tv_g - _DEFAULT_TV_G) > 1e-5:
        return ticker_tv_g
    # セクター別フォールバック
    sector_tv_g = _lookup_tv_g_by_industry(ticker)
    if sector_tv_g is not None:
        return sector_tv_g
    return _DEFAULT_TV_G


if __name__ == "__main__":
    print("=== Maturity Profile（JSON読み込み版）===\n")
    tickers = ["NVDA", "TSLA", "PLTR", "MSFT", "AMZN", "AMD", "APP", "CELH", "UNKNOWN"]
    for ticker in tickers:
        profile = get_maturity_profile(ticker)
        ptype = profile.get("type")
        if ptype == "three_stage":
            p1 = profile["phase1"]
            p2 = profile["phase2"]
            tg = profile["terminal_growth"]
            g1 = f"{p1['growth']:.0%}" if p1.get("growth") else "segment_weighted"
            print(f"{ticker:6}: three_stage  P1={p1['years']}yr@{g1}  P2={p2['years']}yr@{p2['growth']:.0%}  TV={tg:.1%}")
        else:
            print(f"{ticker:6}: two_stage (既存モデル)")
