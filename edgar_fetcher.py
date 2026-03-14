from edgar import Company, set_identity

def fetch_filings(ticker, count=45): # countをデフォルトで45に設定
    set_identity("jamablue01@gmail.com") 
    company = Company(ticker)
    # 10-Q（四半期）と10-K（年次）を取得
    filings = company.get_filings(form=["10-Q", "10-K"])
    # 最新から指定数分（約10年分）を返す
    return filings[:count]
