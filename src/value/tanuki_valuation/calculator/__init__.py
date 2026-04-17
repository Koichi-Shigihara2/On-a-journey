"""
TANUKI VALUATION - Calculator Module
Koichi式株価評価モデル v6.2

v6.2 追加エクスポート:
- determine_fcf_base
- FCFBaseResult
- DEFAULT_FCF_BASE_THRESHOLD
"""

from .wacc import (
    calculate_wacc, get_default_beta, WACCResult,
    SECTOR_DEFAULT_BETA, DEFAULT_RISK_FREE_RATE, DEFAULT_MARKET_RETURN,
)

from .growth import (
    determine_growth_rate, get_segment_growth,
    calculate_fcf_cagr, get_scenario_growth_rates, GrowthResult,
)

from .dcf import (
    calculate_two_stage_dcf, calculate_three_stage_dcf,
    calculate_dcf_with_varying_wacc, calculate_dcf_with_varying_years,
    DCFResult, ThreeStageDCFResult,
    DEFAULT_HIGH_GROWTH_YEARS, DEFAULT_TERMINAL_GROWTH,
)

from .adjustments import (
    adjust_fcf, adjust_rpo, calculate_alpha,
    calculate_intrinsic_value, calculate_per_share_value, calculate_upside,
    calculate_growth_option_pv, determine_fcf_base,
    FCFAdjustmentResult, RPOAdjustmentResult, AlphaResult,
    GrowthOptionResult, FCFBaseResult,
    DEFAULT_RETENTION_RATE, DEFAULT_ALPHA_CAP,
    DEFAULT_RPO_DISCOUNT_RATE, DEFAULT_FCF_FLOOR_RATIO,
    DEFAULT_FCF_BASE_THRESHOLD,
)

from .sensitivity import (
    calculate_sensitivity_matrix, create_sensitivity_calc_func,
    format_matrix_for_display, SensitivityResult,
)

from .scenarios import (
    calculate_scenario_valuations, create_scenario_calc_func,
    format_scenario_for_display, ScenarioResult, ScenarioValuation,
    DEFAULT_BEAR_MULTIPLIER, DEFAULT_BULL_MULTIPLIER,
)

from .future_values import (
    calculate_future_values, calculate_return_metrics,
)


__all__ = [
    # WACC
    "calculate_wacc", "get_default_beta", "WACCResult",
    "SECTOR_DEFAULT_BETA", "DEFAULT_RISK_FREE_RATE", "DEFAULT_MARKET_RETURN",
    # Growth
    "determine_growth_rate", "get_segment_growth",
    "calculate_fcf_cagr", "get_scenario_growth_rates", "GrowthResult",
    # DCF
    "calculate_two_stage_dcf", "calculate_three_stage_dcf",
    "calculate_dcf_with_varying_wacc", "calculate_dcf_with_varying_years",
    "DCFResult", "ThreeStageDCFResult",
    "DEFAULT_HIGH_GROWTH_YEARS", "DEFAULT_TERMINAL_GROWTH",
    # Adjustments
    "adjust_fcf", "adjust_rpo", "calculate_alpha",
    "calculate_intrinsic_value", "calculate_per_share_value", "calculate_upside",
    "calculate_growth_option_pv", "determine_fcf_base",
    "FCFAdjustmentResult", "RPOAdjustmentResult", "AlphaResult",
    "GrowthOptionResult", "FCFBaseResult",
    "DEFAULT_RETENTION_RATE", "DEFAULT_ALPHA_CAP",
    "DEFAULT_RPO_DISCOUNT_RATE", "DEFAULT_FCF_FLOOR_RATIO",
    "DEFAULT_FCF_BASE_THRESHOLD",
    # Sensitivity
    "calculate_sensitivity_matrix", "create_sensitivity_calc_func",
    "format_matrix_for_display", "SensitivityResult",
    # Scenarios
    "calculate_scenario_valuations", "create_scenario_calc_func",
    "format_scenario_for_display", "ScenarioResult", "ScenarioValuation",
    "DEFAULT_BEAR_MULTIPLIER", "DEFAULT_BULL_MULTIPLIER",
    # Future Values
    "calculate_future_values", "calculate_return_metrics",
]

__version__ = "6.2.0"
