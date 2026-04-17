"""
TANUKI VALUATION - Maturity Profile Configuration
銘柄別成熟曲線パラメータ設定

責務: 企業ごとの成長鈍化カーブを定義
     既存2段階DCFにフォールバックする後方互換設計

成熟タイプ:
  "three_stage" : Phase1（高成長）→ Phase2（移行）→ ターミナル
  "two_stage"   : 既存モデルと同一（デフォルト）

未定義銘柄は自動的に two_stage（既存動作）にフォールバック。
"""

from typing import Dict, Any, Optional


# ========================================
# 銘柄別成熟プロファイル
# ========================================
# three_stage 構造:
#   phase1.years  : 高成長期の年数
#   phase1.growth : 高成長期の成長率（segment_configの加重平均を上書きしない）
#                   ※ growth=None の場合は determine_growth_rate() の結果を流用
#   phase2.years  : 移行期の年数
#   phase2.growth : 移行期の成長率
#   terminal_growth: 永続成長率（デフォルト 3.0%）
# ========================================

MATURITY_PROFILES: Dict[str, Dict[str, Any]] = {

    # ── NVIDIA ──
    # AI需要は5年以上継続見込み。移行期はCUDA競合台頭・市場成熟で鈍化。
    "NVDA": {
        "type": "three_stage",
        "phase1": {"years": 5,  "growth": None},   # segment_weighted を流用
        "phase2": {"years": 5,  "growth": 0.15},   # 競合台頭後の安定成長
        "terminal_growth": 0.03
    },

    # ── Tesla ──
    # EV競争激化で早めに鈍化。Energy事業が移行期を下支え。
    "TSLA": {
        "type": "three_stage",
        "phase1": {"years": 4,  "growth": None},
        "phase2": {"years": 4,  "growth": 0.08},
        "terminal_growth": 0.03
    },

    # ── Palantir ──
    # AIP商業化が牽引。政府部門の安定性で移行期も比較的高め。
    "PLTR": {
        "type": "three_stage",
        "phase1": {"years": 5,  "growth": None},
        "phase2": {"years": 5,  "growth": 0.15},
        "terminal_growth": 0.03
    },

    # ── Microsoft ──
    # 成熟大企業。Azureが牽引するが成長速度は安定的。
    "MSFT": {
        "type": "three_stage",
        "phase1": {"years": 5,  "growth": None},
        "phase2": {"years": 5,  "growth": 0.08},
        "terminal_growth": 0.03
    },

    # ── Amazon ──
    # AWS+広告が牽引。小売成熟で移行期は緩やか。
    "AMZN": {
        "type": "three_stage",
        "phase1": {"years": 5,  "growth": None},
        "phase2": {"years": 5,  "growth": 0.10},
        "terminal_growth": 0.03
    },

    # ── AMD ──
    # MI300X rampが牽引するが、NVDAとの差は大きい。移行期は早め。
    "AMD": {
        "type": "three_stage",
        "phase1": {"years": 4,  "growth": None},
        "phase2": {"years": 4,  "growth": 0.10},
        "terminal_growth": 0.03
    },

    # ── AppLovin ──
    # AXON急成長。ただしモバイル広告市場の集中度次第で鈍化リスク高い。
    "APP": {
        "type": "three_stage",
        "phase1": {"years": 4,  "growth": None},
        "phase2": {"years": 4,  "growth": 0.12},
        "terminal_growth": 0.03
    },

    # ── Celsius ──
    # 高成長期は短め。エナジードリンク市場の飽和リスク考慮。
    "CELH": {
        "type": "three_stage",
        "phase1": {"years": 3,  "growth": None},
        "phase2": {"years": 5,  "growth": 0.10},
        "terminal_growth": 0.03
    },

    # ── 未定義銘柄のデフォルト ──
    # two_stage = 既存モデルそのまま（変更なし）
    "_default": {
        "type": "two_stage",
        "terminal_growth": 0.03
    }
}


def get_maturity_profile(ticker: str) -> Dict[str, Any]:
    """
    銘柄の成熟プロファイルを取得

    Args:
        ticker: 銘柄コード

    Returns:
        成熟プロファイルのdict（未定義の場合は _default を返す）
    """
    profile = MATURITY_PROFILES.get(ticker)
    if profile is None:
        return MATURITY_PROFILES["_default"].copy()
    return profile.copy()


def is_three_stage(ticker: str) -> bool:
    """3段階DCFを使用するか判定"""
    profile = get_maturity_profile(ticker)
    return profile.get("type") == "three_stage"


def get_terminal_growth(ticker: str) -> float:
    """銘柄のターミナル成長率を取得"""
    profile = get_maturity_profile(ticker)
    return profile.get("terminal_growth", 0.03)


if __name__ == "__main__":
    print("=== Maturity Profile 確認 ===\n")
    for ticker in ["NVDA", "TSLA", "PLTR", "MSFT", "AMZN", "AMD", "APP", "CELH", "UNKNOWN"]:
        profile = get_maturity_profile(ticker)
        ptype = profile.get("type")
        if ptype == "three_stage":
            p1 = profile["phase1"]
            p2 = profile["phase2"]
            tg = profile["terminal_growth"]
            g1 = f"{p1['growth']:.0%}" if p1["growth"] else "segment_weighted"
            print(f"{ticker:6}: three_stage  P1={p1['years']}yr@{g1}  P2={p2['years']}yr@{p2['growth']:.0%}  TV={tg:.1%}")
        else:
            print(f"{ticker:6}: two_stage (既存モデル)")
