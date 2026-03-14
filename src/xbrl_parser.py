from edgar import Company, set_identity
import pandas as pd

def parse_xbrl(filing):
    # SECのデータから財務諸表（Fact）を取得
    try:
        obj = filing.obj()
        facts = obj.get_facts()
        
        # 1. 希薄化後加重平均株式数 (Weighted Average Shares Outstanding Diluted)
        # 期間（3ヶ月/12ヶ月）の平均値を取得
        shares = facts.get("WeightedAverageNumberOfSharesOutstandingDiluted", "CommonStockSharesOutstanding")
        
        # 2. 当期純利益 (Net Income)
        net_income = facts.get("NetIncomeLoss")
        
        # 3. 法人税費用 (Income Tax Expense)
        tax_expense = facts.get("IncomeTaxExpenseBenefit", "CurrentIncomeTaxExpenseBenefit")
        
        # 4. 税引前利益 (Income Before Tax)
        pretax_income = facts.get("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest")
        
        # すべての調整項目用XBRLタグを一括取得（adjustment_items.jsonとの照合用）
        raw_data = {fact.concept: fact.value for fact in facts}
        
        return {
            "net_income": net_income,
            "shares": shares,
            "tax_expense": tax_expense,
            "pretax_income": pretax_income,
            "raw_facts": raw_data # 全タグを保持して後の調整検知に使う
        }
    except Exception as e:
        print(f"Error parsing XBRL: {e}")
        return None
