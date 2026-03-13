from edgar import Company

def fetch_filings(ticker):
    company = Company(ticker)
    filings = company.get_filings(form=["10-Q","10-K"])
    return [str(filing.accession_no) for filing in filings[:8]]
