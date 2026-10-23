"""
企業メタデータ取得モジュール
- SECのSubmissions APIから企業情報（SICコード、会社名など）を取得
"""
from typing import Dict, Optional

# User-Agent（環境変数 SEC_USER_AGENT_EMAIL）・毎秒10件の制限・再試行は common/external_api.py（[[EXTERNAL-API-CLIENT-UNIFY-1]]）
from common.external_api import sec_get

def get_company_metadata(cik: str) -> Dict:
    """
    CIKから企業メタデータを取得
    Args:
        cik: 10桁のCIK番号（例: '0001321655'）
    Returns:
        Dict: {
            'name': 会社名,
            'sic': SICコード,
            'sic_description': SIC説明,
            'exchange': 取引所,
            'tickers': ティッカーリスト
        }
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    try:
        response = sec_get(url, "EPS 会社の情報")
        if response.status_code == 200:
            data = response.json()
            return {
                'name': data.get('name', ''),
                'sic': data.get('sic', ''),
                'sic_description': data.get('sicDescription', ''),
                'exchange': data.get('exchange', ''),
                'tickers': data.get('tickers', [])
            }
        else:
            print(f"SEC API returned status {response.status_code} for CIK {cik}")
    except Exception as e:
        print(f"Error fetching metadata for CIK {cik}: {e}")
    return {}