def calculate_eps(data, net_adjustment, detailed_adjustments):
    adjusted_income = data["net_income"] + net_adjustment
    # 希薄化後加重平均株式数（必須）
    diluted_shares = data.get("diluted_shares", data.get("shares", 1))  # xbrl_parserで取得前提
    adjusted_eps = adjusted_income / diluted_shares if diluted_shares else 0

    return {
        "gaap_net_income": data["net_income"],
        "gaap_eps": data.get("gaap_eps", 0),
        "adjusted_net_income": adjusted_income,
        "adjusted_eps": adjusted_eps,
        "diluted_shares_used": diluted_shares,
        "adjustments": detailed_adjustments  # 詳細リスト保存（UI用）
    }
