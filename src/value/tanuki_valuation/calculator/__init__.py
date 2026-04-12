"""
TANUKI VALUATION - Calculator Module
Koichi式株価評価モデル v6.0

モジュール構成:
- wacc: WACC計算（CAPM）
- growth: 成長率決定ロジック
- dcf: 2段階DCF計算
- adjustments: FCF補正、RPO補正、α計算
- sensitivity: 感度分析マトリクス
- scenarios: シナリオ別理論株価
- future_values: 将来価値予測
"""

from .wacc import (
    calculate_wacc,
    get_default_beta,
    WACCResult,
    SECTOR_DEFAULT_BETA,
    DEFAULT_RISK_FREE_RATE,
    DEFAULT_MARKET_RETURN,
)

from .growth import (
    determine_growth_rate,
    get_segment_growth,
    calculate_fcf_cagr,
    get_scenario_growth_rates,
    GrowthResult,
    SEGMENT_CONFIG,
)

from .dcf import (
    calculate_two_stage_dcf,
    calculate_dcf_with_varying_wacc,
    calculate_dcf_with_varying_years,
    DCFResult,
    DEFAULT_HIGH_GROWTH_YEARS,
    DEFAULT_TERMINAL_GROWTH,
)

from .adjustments import (
    adjust_fcf,
    adjust_rpo,
    calculate_alpha,
    calculate_intrinsic_value,
    calculate_per_share_value,
    calculate_upside,
    FCFAdjustmentResult,
    RPOAdjustmentResult,
    AlphaResult,
    DEFAULT_RETENTION_RATE,
    DEFAULT_ALPHA_CAP,
    DEFAULT_RPO_DISCOUNT_RATE,
    DEFAULT_FCF_FLOOR_RATIO,
)

from .sensitivity import (
    calculate_sensitivity_matrix,
    create_sensitivity_calc_func,
    format_matrix_for_display,
    SensitivityResult,
)

from .scenarios import (
    calculate_scenario_valuations,
    create_scenario_calc_func,
    format_scenario_for_display,
    ScenarioResult,
    ScenarioValuation,
    DEFAULT_BEAR_MULTIPLIER,
    DEFAULT_BULL_MULTIPLIER,
)

from .future_values import (
    calculate_future_values,
    calculate_return_metrics,
)


__all__ = [
    # WACC
    "calculate_wacc",
    "get_default_beta",
    "WACCResult",
    "SECTOR_DEFAULT_BETA",
    "DEFAULT_RISK_FREE_RATE",
    "DEFAULT_MARKET_RETURN",
    
    # Growth
    "determine_growth_rate",
    "get_segment_growth",
    "calculate_fcf_cagr",
    "get_scenario_growth_rates",
    "GrowthResult",
    "SEGMENT_CONFIG",
    
    # DCF
    "calculate_two_stage_dcf",
    "calculate_dcf_with_varying_wacc",
    "calculate_dcf_with_varying_years",
    "DCFResult",
    "DEFAULT_HIGH_GROWTH_YEARS",
    "DEFAULT_TERMINAL_GROWTH",
    
    # Adjustments
    "adjust_fcf",
    "adjust_rpo",
    "calculate_alpha",
    "calculate_intrinsic_value",
    "calculate_per_share_value",
    "calculate_upside",
    "FCFAdjustmentResult",
    "RPOAdjustmentResult",
    "AlphaResult",
    "DEFAULT_RETENTION_RATE",
    "DEFAULT_ALPHA_CAP",
    "DEFAULT_RPO_DISCOUNT_RATE",
    "DEFAULT_FCF_FLOOR_RATIO",
    
    # Sensitivity
    "calculate_sensitivity_matrix",
    "create_sensitivity_calc_func",
    "format_matrix_for_display",
    "SensitivityResult",
    
    # Scenarios
    "calculate_scenario_valuations",
    "create_scenario_calc_func",
    "format_scenario_for_display",
    "ScenarioResult",
    "ScenarioValuation",
    "DEFAULT_BEAR_MULTIPLIER",
    "DEFAULT_BULL_MULTIPLIER",
    
    # Future Values
    "calculate_future_values",
    "calculate_return_metrics",
]

__version__ = "6.0.0"
