def apply_tax(adjustments, data):
    etr = data["tax_expense"] / data["pretax_income"]
    total = 0
    for adj in adjustments:
        if adj["pretax"]:
            net = adj["amount"] * (1 - etr)
        else:
            net = adj["amount"]
        total += net
    return total
