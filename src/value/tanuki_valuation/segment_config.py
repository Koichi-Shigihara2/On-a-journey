"""
TANUKI VALUATION - Segment Growth Configuration
セグメント別成長率設定

使用方法:
1. SEGMENT_OVERRIDES に企業のセグメント構成を定義
2. core_calculator.py が自動的に加重平均成長率を計算
3. 未定義の企業は従来のFCF CAGRベースで計算

v6.1 追加:
  - GROWTH_OPTIONS: 銘柄別の仮説セグメント（成長オプション）定義
  - get_growth_options(): 仮説セグメントの期待FCFを取得
  - 案B: 成長オプションPVはV₀への加算項として独立

データソース:
- 各社10-Kのセグメント別売上高
- アナリスト予測ではなく、過去実績ベースの成長率推定
"""

from typing import Dict, Any, Optional, List


# ========================================
# セグメント別成長率設定（既存・変更なし）
# ========================================

SEGMENT_OVERRIDES: Dict[str, Dict[str, Any]] = {

    # ── NVIDIA ──
    "NVDA": {
        "enabled": True,
        "fiscal_year": "FY2025",
        "segments": {
            "Data Center": {
                "weight": 0.88,
                "growth": 0.40,
                "note": "GPU/Networking for AI training & inference"
            },
            "Gaming": {
                "weight": 0.08,
                "growth": 0.05,
                "note": "PC/Console GPU"
            },
            "Professional Visualization": {
                "weight": 0.02,
                "growth": 0.10,
                "note": "Workstation GPU"
            },
            "Automotive": {
                "weight": 0.02,
                "growth": 0.25,
                "note": "DRIVE platform"
            }
        }
    },

    # ── Tesla ──
    "TSLA": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": {
            "Automotive": {
                "weight": 0.82,
                "growth": 0.10,
                "note": "Vehicle sales & leasing"
            },
            "Energy": {
                "weight": 0.10,
                "growth": 0.30,
                "note": "Powerwall, Megapack, Solar"
            },
            "Services": {
                "weight": 0.08,
                "growth": 0.15,
                "note": "Supercharging, maintenance, insurance"
            }
        }
    },

    # ── Palantir ──
    "PLTR": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": {
            "Government": {
                "weight": 0.42,
                "growth": 0.20,
                "note": "US Gov + Allies defense/intel"
            },
            "Commercial US": {
                "weight": 0.38,
                "growth": 0.50,
                "note": "US Enterprise (AIP platform)"
            },
            "Commercial International": {
                "weight": 0.20,
                "growth": 0.25,
                "note": "Non-US Enterprise"
            }
        }
    },

    # ── Microsoft ──
    "MSFT": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": {
            "Intelligent Cloud": {
                "weight": 0.44,
                "growth": 0.22,
                "note": "Azure, Server products, Enterprise services"
            },
            "Productivity and Business Processes": {
                "weight": 0.32,
                "growth": 0.12,
                "note": "Office 365, LinkedIn, Dynamics"
            },
            "More Personal Computing": {
                "weight": 0.24,
                "growth": 0.05,
                "note": "Windows, Xbox, Surface, Search"
            }
        }
    },

    # ── Amazon ──
    "AMZN": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": {
            "AWS": {
                "weight": 0.17,
                "growth": 0.18,
                "note": "Cloud infrastructure"
            },
            "Online Stores": {
                "weight": 0.40,
                "growth": 0.08,
                "note": "1P e-commerce"
            },
            "Third-Party Seller Services": {
                "weight": 0.24,
                "growth": 0.12,
                "note": "3P marketplace fees"
            },
            "Advertising": {
                "weight": 0.09,
                "growth": 0.20,
                "note": "Sponsored products/brands"
            },
            "Subscription Services": {
                "weight": 0.07,
                "growth": 0.10,
                "note": "Prime membership"
            },
            "Other": {
                "weight": 0.03,
                "growth": 0.05,
                "note": "Physical stores, etc."
            }
        }
    },

    # ── AMD ──
    "AMD": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": {
            "Data Center": {
                "weight": 0.50,
                "growth": 0.35,
                "note": "EPYC CPUs, Instinct GPUs"
            },
            "Client": {
                "weight": 0.25,
                "growth": 0.08,
                "note": "Ryzen CPUs"
            },
            "Gaming": {
                "weight": 0.12,
                "growth": -0.05,
                "note": "Console APUs, Radeon GPUs"
            },
            "Embedded": {
                "weight": 0.13,
                "growth": 0.05,
                "note": "Xilinx FPGAs, industrial"
            }
        }
    },

    # ── AppLovin ──
    "APP": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": {
            "Software Platform": {
                "weight": 0.70,
                "growth": 0.45,
                "note": "AppDiscovery, MAX, AXON"
            },
            "Apps": {
                "weight": 0.30,
                "growth": 0.05,
                "note": "1P mobile games portfolio"
            }
        }
    },

    # ── Celsius ──
    "CELH": {
        "enabled": True,
        "fiscal_year": "FY2024",
        "segments": {
            "North America": {
                "weight": 0.95,
                "growth": 0.25,
                "note": "US/Canada retail & e-commerce"
            },
            "International": {
                "weight": 0.05,
                "growth": 0.50,
                "note": "Europe, Asia expansion"
            }
        }
    }
}


# ========================================
# 仮説セグメント（成長オプション）定義 v6.1 新規
# ========================================
# 案B: V₀への独立加算項として計算
#
# 各仮説セグメントのパラメータ:
#   tam          : Total Addressable Market（$単位、USD）
#   penetration  : 市場侵入率（獲得シェア仮定）
#   fcf_margin   : FCFマージン仮定（類似事業の実績ベース）
#   probability  : 事業が主力化する確率（0〜1）
#   delay_years  : 収益貢献が本格化するまでの年数
#   discount_rate: 仮説PVの割引率（固定 15%、リスク反映）
#   note         : 仮説の根拠メモ
#
# 期待FCF = TAM × penetration × fcf_margin × probability
# 仮説PV  = 期待FCF / (1 + discount_rate)^delay_years
# ========================================

GROWTH_OPTIONS: Dict[str, List[Dict[str, Any]]] = {

    # ── NVIDIA ──
    "NVDA": [
        {
            "name": "Sovereign AI",
            "tam": 80_000_000_000,
            "penetration": 0.15,
            "fcf_margin": 0.20,
            "probability": 0.70,
            "delay_years": 3,
            "discount_rate": 0.15,
            "note": "各国政府のAIインフラ独自整備需要。CUDAエコシステムの延長。"
        },
        {
            "name": "Robotics / Isaac",
            "tam": 200_000_000_000,
            "penetration": 0.05,
            "fcf_margin": 0.20,
            "probability": 0.40,
            "delay_years": 5,
            "discount_rate": 0.15,
            "note": "Isaacプラットフォームによるロボティクス向けGPU需要。実現期間長い。"
        },
        {
            "name": "NIM / Inference SaaS",
            "tam": 50_000_000_000,
            "penetration": 0.10,
            "fcf_margin": 0.25,
            "probability": 0.50,
            "delay_years": 4,
            "discount_rate": 0.15,
            "note": "NIMAによる推論SaaS化。AWSのようなソフトウェア収益転換。"
        }
    ],

    # ── Tesla ──
    "TSLA": [
        {
            "name": "Robotaxi / FSD Network",
            "tam": 300_000_000_000,
            "penetration": 0.05,
            "fcf_margin": 0.25,
            "probability": 0.30,
            "delay_years": 5,
            "discount_rate": 0.15,
            "note": "FSD完全自律化後の配車ネットワーク収益。規制リスク高い。"
        },
        {
            "name": "Optimus Robot",
            "tam": 150_000_000_000,
            "penetration": 0.05,
            "fcf_margin": 0.15,
            "probability": 0.25,
            "delay_years": 6,
            "discount_rate": 0.15,
            "note": "汎用ヒューマノイドロボット。量産コスト・需要ともに不確実。"
        }
    ],

    # ── Palantir ──
    "PLTR": [
        {
            "name": "AI Platform (AIP) Global",
            "tam": 100_000_000_000,
            "penetration": 0.08,
            "fcf_margin": 0.30,
            "probability": 0.60,
            "delay_years": 3,
            "discount_rate": 0.15,
            "note": "AIPの国際展開。米国商業での実績を他地域へ横展開。"
        },
        {
            "name": "Defense AI / NATO",
            "tam": 50_000_000_000,
            "penetration": 0.12,
            "fcf_margin": 0.25,
            "probability": 0.55,
            "delay_years": 4,
            "discount_rate": 0.15,
            "note": "地政学的緊張継続によるNATO加盟国への拡大。"
        }
    ],

    # ── Microsoft ──
    "MSFT": [
        {
            "name": "Copilot Enterprise",
            "tam": 80_000_000_000,
            "penetration": 0.20,
            "fcf_margin": 0.35,
            "probability": 0.70,
            "delay_years": 3,
            "discount_rate": 0.15,
            "note": "M365 Copilotの企業採用拡大。Officeの自然延長。"
        }
    ],

    # ── Amazon ──
    "AMZN": [
        {
            "name": "Alexa+ / AI Assistant",
            "tam": 60_000_000_000,
            "penetration": 0.15,
            "fcf_margin": 0.20,
            "probability": 0.50,
            "delay_years": 4,
            "discount_rate": 0.15,
            "note": "Alexa+の有料化・企業向け展開。"
        }
    ],

    # ── AMD ──
    "AMD": [
        {
            "name": "AI PC / Client AI",
            "tam": 40_000_000_000,
            "penetration": 0.20,
            "fcf_margin": 0.15,
            "probability": 0.55,
            "delay_years": 3,
            "discount_rate": 0.15,
            "note": "NPU搭載RyzenによるAI PC市場の取り込み。"
        }
    ],

    # ── AppLovin ──
    "APP": [
        {
            "name": "E-commerce Ad Network",
            "tam": 50_000_000_000,
            "penetration": 0.08,
            "fcf_margin": 0.30,
            "probability": 0.45,
            "delay_years": 3,
            "discount_rate": 0.15,
            "note": "モバイル広告からEC広告への拡張。AXONエンジン転用。"
        }
    ],

    # CELH・他は仮説セグメントなし
}


def get_segment_growth(ticker: str) -> Optional[Dict[str, Any]]:
    """
    セグメント別成長率を取得（既存・変更なし）

    Returns:
        {
            "enabled": True,
            "weighted_growth": 0.346,
            "segments": {...},
            "source": "segment_config"
        }
        または None（未定義の場合）
    """
    config = SEGMENT_OVERRIDES.get(ticker)

    if not config or not config.get("enabled", False):
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
    仮説セグメント（成長オプション）リストを取得

    Args:
        ticker: 銘柄コード

    Returns:
        仮説セグメントのリスト（未定義の場合は空リスト）
        各要素に "expected_fcf" と "pv" が計算済みで付加される
    """
    options = GROWTH_OPTIONS.get(ticker, [])
    if not options:
        return []

    enriched = []
    for opt in options:
        tam = opt["tam"]
        pen = opt["penetration"]
        margin = opt["fcf_margin"]
        prob = opt["probability"]
        delay = opt["delay_years"]
        dr = opt["discount_rate"]

        # 期待FCF = TAM × 侵入率 × FCFマージン × 確率
        expected_fcf = tam * pen * margin * prob

        # 仮説PV = 期待FCF / (1 + discount_rate)^delay_years
        pv = expected_fcf / (1 + dr) ** delay

        enriched.append({
            **opt,
            "expected_fcf": expected_fcf,
            "pv": pv
        })

    return enriched


def calculate_growth_option_total_pv(ticker: str) -> Dict[str, Any]:
    """
    仮説セグメントの合計PVを計算

    Args:
        ticker: 銘柄コード

    Returns:
        {
            "total_pv": float,
            "options": List[Dict],  # 各仮説の詳細（pv付き）
            "count": int
        }
    """
    options = get_growth_options(ticker)

    total_pv = sum(opt["pv"] for opt in options)

    return {
        "total_pv": total_pv,
        "options": options,
        "count": len(options)
    }


def calculate_scenario_growth(ticker: str, scenario: str = "base") -> Dict[str, Any]:
    """
    シナリオ別成長率を計算（既存・変更なし）
    """
    segment_data = get_segment_growth(ticker)

    if not segment_data:
        return {"rate": None, "scenario": scenario, "source": "not_configured"}

    base_rate = segment_data["weighted_growth"]

    adjustments = {
        "bull": 1.2,
        "base": 1.0,
        "bear": 0.7
    }

    adjustment = adjustments.get(scenario, 1.0)
    adjusted_rate = base_rate * adjustment
    adjusted_rate = max(0.0, min(0.50, adjusted_rate))

    return {
        "rate": adjusted_rate,
        "base_rate": base_rate,
        "scenario": scenario,
        "adjustment": adjustment,
        "source": "segment_config"
    }


if __name__ == "__main__":
    print("=== Segment Growth ===")
    for ticker in ["NVDA", "TSLA", "PLTR", "MSFT", "AMZN", "AMD", "APP", "CELH", "UNKNOWN"]:
        result = get_segment_growth(ticker)
        if result:
            print(f"{ticker}: weighted_growth = {result['weighted_growth']:.1%}")
        else:
            print(f"{ticker}: not configured")

    print("\n=== Growth Options (成長オプション) ===")
    for ticker in ["NVDA", "TSLA", "PLTR", "MSFT", "CELH"]:
        result = calculate_growth_option_total_pv(ticker)
        if result["count"] > 0:
            print(f"\n{ticker}: {result['count']}件  合計PV = ${result['total_pv']/1e9:.2f}B")
            for opt in result["options"]:
                print(f"  [{opt['name']}] 期待FCF=${opt['expected_fcf']/1e9:.2f}B  PV=${opt['pv']/1e9:.2f}B  (確率{opt['probability']:.0%} / {opt['delay_years']}年後)")
        else:
            print(f"{ticker}: 仮説セグメントなし")
