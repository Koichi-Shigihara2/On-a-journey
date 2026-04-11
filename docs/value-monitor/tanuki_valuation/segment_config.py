"""
TANUKI VALUATION - Segment Growth Configuration
セグメント別成長率設定

使用方法:
1. SEGMENT_OVERRIDES に企業のセグメント構成を定義
2. core_calculator.py が自動的に加重平均成長率を計算
3. 未定義の企業は従来のFCF CAGRベースで計算

データソース:
- 各社10-Kのセグメント別売上高
- アナリスト予測ではなく、過去実績ベースの成長率推定
"""

from typing import Dict, Any, Optional

# ========================================
# セグメント別成長率設定
# ========================================
# weight: セグメント売上構成比（合計1.0）
# growth: セグメント成長率（年率）
# source: データソース（10-K, 推定など）
# ========================================

SEGMENT_OVERRIDES: Dict[str, Dict[str, Any]] = {
    
    # ── NVIDIA ──
    "NVDA": {
        "enabled": True,
        "fiscal_year": "FY2025",
        "segments": {
            "Data Center": {
                "weight": 0.88,
                "growth": 0.40,  # AI需要継続
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
                "growth": 0.10,  # EV競争激化
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
                "growth": 0.50,  # AIP急成長
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
                "growth": 0.22,  # Azure成長
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
                "growth": 0.35,  # MI300X ramp
                "note": "EPYC CPUs, Instinct GPUs"
            },
            "Client": {
                "weight": 0.25,
                "growth": 0.08,
                "note": "Ryzen CPUs"
            },
            "Gaming": {
                "weight": 0.12,
                "growth": -0.05,  # Console cycle down
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
                "growth": 0.45,  # AXON急成長
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


def get_segment_growth(ticker: str) -> Optional[Dict[str, Any]]:
    """
    セグメント別成長率を取得
    
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
    
    # 加重平均成長率を計算
    weighted_growth = sum(
        seg.get("weight", 0) * seg.get("growth", 0)
        for seg in segments.values()
    )
    
    # 構成比の検証（合計が0.95〜1.05の範囲）
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


def calculate_scenario_growth(ticker: str, scenario: str = "base") -> Dict[str, Any]:
    """
    シナリオ別成長率を計算
    
    Args:
        ticker: ティッカーシンボル
        scenario: "bull" | "base" | "bear"
    
    Returns:
        {
            "rate": 0.346,
            "scenario": "base",
            "adjustment": 1.0
        }
    """
    segment_data = get_segment_growth(ticker)
    
    if not segment_data:
        return {"rate": None, "scenario": scenario, "source": "not_configured"}
    
    base_rate = segment_data["weighted_growth"]
    
    # シナリオ別調整
    adjustments = {
        "bull": 1.2,   # +20%
        "base": 1.0,
        "bear": 0.7    # -30%
    }
    
    adjustment = adjustments.get(scenario, 1.0)
    adjusted_rate = base_rate * adjustment
    
    # 上下限クリップ
    adjusted_rate = max(0.0, min(0.50, adjusted_rate))
    
    return {
        "rate": adjusted_rate,
        "base_rate": base_rate,
        "scenario": scenario,
        "adjustment": adjustment,
        "source": "segment_config"
    }


if __name__ == "__main__":
    # テスト
    for ticker in ["NVDA", "TSLA", "PLTR", "MSFT", "AMZN", "AMD", "APP", "CELH", "UNKNOWN"]:
        result = get_segment_growth(ticker)
        if result:
            print(f"{ticker}: weighted_growth = {result['weighted_growth']:.1%}")
        else:
            print(f"{ticker}: not configured")
