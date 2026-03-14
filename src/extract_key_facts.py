from edgar import Company, set_identity
from typing import Dict, Any, Optional

set_identity("jamablue01@gmail.com")

def extract_key_facts(ticker: str) -> Optional[Dict[str, Any]]:
    try:
        company = Company(ticker)
        facts = company.get_facts()  # 会社レベルの事実を取得（推奨方法）
        if not facts:
            print(f"{ticker} のfactsがありません")
            return None

        # 最新値を取得（to_pandasでDataFrame化）
        def get_latest(tag: str, default=None):
            try:
                df = facts.to_pandas(tag)
                if df.empty:
                    return default
                return df['value'].iloc[-1]  # 最新期間の値
            except:
                return default

        diluted_shares = get_latest("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding") or \
                         get_latest("dei:EntityCommonStockSharesOutstanding")

        net_income = get_latest("us-gaap:NetIncomeLoss") or \
                     get_latest("us-gaap:NetIncomeLossAttributableToParent")

        tax_expense = get_latest("us-gaap:IncomeTaxExpenseBenefit")

        pretax_income = get_latest("us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxes")

        # raw_factsとして一部サンプル
        raw_facts = {
            "Revenues": get_latest("us-gaap:Revenues"),
            "RestructuringCharges": get_latest("us-gaap:RestructuringCharges")
        }

        return {
            "net_income": net_income,
            "diluted_shares": diluted_shares,
            "tax_expense": tax_expense,
            "pretax_income": pretax_income,
            "raw_facts": raw_facts,
            "ticker": ticker
        }

    except Exception as e:
        print(f"{ticker} のfacts抽出エラー: {e}")
        return None
