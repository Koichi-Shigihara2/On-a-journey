"""
SEC データ一括更新スクリプト
GitHub Actions から実行される

使用方法:
    python update.py              # 全ティッカー
    python update.py TSLA PLTR    # 特定ティッカーのみ
"""

import sys
import os

# 親ディレクトリをパスに追加（GitHub Actions実行時用）
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.sec_data import SECFetcher, SECParser, get_all


def main():
    tickers = sys.argv[1:] if len(sys.argv) > 1 else get_all()
    
    fetcher = SECFetcher()
    parser = SECParser()
    
    print("=" * 60)
    print("SEC EDGAR データ更新")
    print(f"対象: {len(tickers)} 銘柄")
    print("=" * 60)
    
    success = 0
    failed = []
    
    for ticker in tickers:
        print(f"\n--- {ticker} ---")
        
        # 1. データ取得
        raw = fetcher.fetch_company_facts(ticker)
        if not raw:
            failed.append(ticker)
            continue
        
        # 2. パース＆保存
        parsed = parser.parse_and_save(ticker)
        if parsed:
            success += 1
        else:
            failed.append(ticker)
    
    # サマリー
    print("\n" + "=" * 60)
    print(f"完了: {success}/{len(tickers)}")
    if failed:
        print(f"失敗: {', '.join(failed)}")
    print("=" * 60)
    
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
