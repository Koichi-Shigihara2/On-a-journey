def calculate_eps(data, adjustments):
    adjusted_income = data["net_income"] + adjustments
    eps = adjusted_income / data["shares"]

    return {
        "gaap_net_income": data["net_income"],
        "adjusted_net_income": adjusted_income,
        "adjusted_eps": eps
    }
