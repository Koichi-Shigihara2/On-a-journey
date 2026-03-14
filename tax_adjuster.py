def apply_tax_adjustments(adjustments, data):
    etr = 0.21
    if data.get("pretax_income", 0) > 0 and data.get("tax_expense", 0):
        etr = data["tax_expense"] / data["pretax_income"]

    total_net = 0
    detailed = []
    for adj in adjustments:
        gross = adj["amount"]
        if adj["pre_tax"]:
            if adj["direction"] == "add_back":  # 費用除外 → 節税分減らす
                net = gross * (1 - etr)
            elif adj["direction"] == "subtract":  # 益除外 → 税負担分減らす
                net = gross * (1 - etr)
            else:
                net = gross
        else:
            net = gross

        adj["net_amount"] = net
        total_net += net if adj["direction"] == "add_back" else -net
        detailed.append(adj)

    return total_net, detailed
