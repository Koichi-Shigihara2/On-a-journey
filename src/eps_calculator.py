def calculate_eps(data, net_adjustment, detailed_adjustments):
    """
    厳密な調整後EPS計算
    - 希薄化後加重平均株式数を使用
    - 非支配持分を考慮（TODO: 必要に応じて実装）
    """
    # GAAP EPS（基本）
    gaap_eps = data["net_income"] / data["diluted_shares"] if data["diluted_shares"] else 0
    
    # 調整後純利益
    adjusted_income = data["net_income"] + net_adjustment
    
    # 調整後EPS
    adjusted_eps = adjusted_income / data["diluted_shares"] if data["diluted_shares"] else 0
    
    # YoY成長率（過去データがあれば計算）
    yoy_growth = None
    # TODO: 前年同期比を計算
    
    return {
        "gaap_net_income": data["net_income"],
        "gaap_eps": gaap_eps,
        "adjusted_net_income": adjusted_income,
        "adjusted_eps": adjusted_eps,
        "diluted_shares_used": data["diluted_shares"],
        "adjustments": detailed_adjustments,
        "net_adjustment_total": net_adjustment,
        "effective_tax_rate": data.get("tax_expense", 0) / data.get("pretax_income", 1) if data.get("pretax_income", 0) != 0 else 0.21,
        "yoy_growth": yoy_growth
    }
