"""Layer3（と normalized）の保持期間を、読み手が要る長さから決める（[[PARSER-LAYER3-SINGLE-SOURCE-1]] 段0、2026-11-11）。

年数を手で書かない。読み手の側の定数（ここ1か所）から導く:
  - ROE_AVG_YEARS: 年次を一番長く読む読み手＝reader.get_roe_avg_detail（TANUKIのROE平均）の年数
  - TTM_POINTS: TTMの系列の点数（ttm_calculator.calc_ttm_series の n_periods。約1年ずつ遡る）
四半期: TTMの最後の点は (点数-1) 年前の四半期末から、その4四半期（1年）と提出の遅れ（0.75年）に及ぶ
年次: 年次を読む年数と、四半期の期間に1年足した年数（Q4の逆算に要る）の長い方
"""
import math

ROE_AVG_YEARS = 10
TTM_POINTS = 6
FILING_LAG_YEARS = 0.75

QUARTERLY_YEARS = math.ceil((TTM_POINTS - 1) + 1 + FILING_LAG_YEARS)
ANNUAL_YEARS = max(ROE_AVG_YEARS, QUARTERLY_YEARS) + 1
