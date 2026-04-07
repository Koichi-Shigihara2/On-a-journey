"""
TANUKI VALUATION - Pipeline v2.0
全ティッカーを処理し、latest.jsonを生成

使用方法:
    python pipeline.py
    python pipeline.py TSLA PLTR  # 特定ティッカーのみ
"""

import json
import os
import sys
from datetime import datetime
from typing import List, Optional

# 同一ディレクトリからのインポート
from data_fetcher import TanukiDataFetcher
from core_calculator import KoichiValuationCalculator


class TanukiValuationPipeline:
    """TANUKI VALUATION パイプライン"""

    # 監視対象ティッカー
    DEFAULT_TICKERS = [
        "TSLA", "PLTR", "SOFI", "CELH", "NVDA",
        "AMD", "APP", "SOUN", "RKLB", "ONDS",
        "MSFT", "AMZN", "FIG"
    ]

    def __init__(self, output_dir: str = None):
        self.fetcher = TanukiDataFetcher()
        self.calculator = KoichiValuationCalculator()
        
        # 出力ディレクトリ（デフォルト: docs/value-monitor/tanuki_valuation/data）
        if output_dir:
            self.output_dir = output_dir
        else:
            # リポジトリルートからの相対パス（GitHub Actions実行時）
            # src/value/tanuki_valuation/ から見て ../../../docs/value-monitor/tanuki_valuation/data
            script_dir = os.path.dirname(os.path.abspath(__file__))
            repo_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
            self.output_dir = os.path.join(repo_root, "docs", "value-monitor", "tanuki_valuation", "data")
        
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"   出力先: {self.output_dir}")

    def run(self, tickers: Optional[List[str]] = None) -> dict:
        """
        パイプライン実行
        
        Args:
            tickers: 処理対象ティッカーリスト（Noneの場合はDEFAULT_TICKERS）
        
        Returns:
            dict: ティッカー別の計算結果
        """
        print("=" * 60)
        print("TANUKI VALUATION Phase 4 実行開始")
        print(f"  Koichi式 v5.1（成長率減衰カーブ＋FCF補正＋将来予測）")
        print("=" * 60)
        
        if tickers is None:
            tickers = self.DEFAULT_TICKERS

        results = {}
        success_count = 0
        error_count = 0

        for ticker in tickers:
            print(f"\n{'─' * 40}")
            print(f"🔄 処理中: {ticker}")
            print(f"{'─' * 40}")
            
            try:
                # データ取得
                financials = self.fetcher.get_financials(ticker)
                
                # バリデーション
                if "error" in financials:
                    print(f"❌ {ticker} スキップ: {financials['error']}")
                    error_count += 1
                    continue
                
                if financials.get("diluted_shares", 0) <= 100_000:
                    print(f"❌ {ticker} スキップ: diluted_shares不足")
                    error_count += 1
                    continue

                # 計算実行
                valuation = self.calculator.calculate_pt(financials)
                
                if "error" in valuation:
                    print(f"❌ {ticker} 計算エラー: {valuation['error']}")
                    error_count += 1
                    continue

                # 結果保存
                self._save_result(ticker, valuation)
                results[ticker] = valuation
                success_count += 1
                
                # サマリー表示
                per_share = valuation.get("intrinsic_value_per_share", 0)
                current = financials.get("current_price", 0)
                upside = valuation.get("upside_percent", 0)
                
                print(f"✅ {ticker} 完了:")
                print(f"   理論株価: ${per_share:,.2f}")
                print(f"   現在株価: ${current:,.2f}")
                print(f"   乖離率: {upside:+.1f}%")

            except Exception as e:
                print(f"❌ {ticker} 例外発生: {e}")
                error_count += 1
                import traceback
                traceback.print_exc()

        # 最終サマリー
        print("\n" + "=" * 60)
        print("🎉 TANUKI VALUATION 実行完了")
        print(f"   成功: {success_count} / 失敗: {error_count}")
        print(f"   出力先: {self.output_dir}")
        print("=" * 60)

        return results

    def _save_result(self, ticker: str, valuation: dict) -> None:
        """
        計算結果をJSONファイルに保存
        
        ファイル構造:
            data/{TICKER}/latest.json
            data/{TICKER}/history/{YYYY-MM-DD}.json
        """
        ticker_dir = os.path.join(self.output_dir, ticker)
        history_dir = os.path.join(ticker_dir, "history")
        os.makedirs(history_dir, exist_ok=True)

        # latest.json（calculation_stepsを除外した軽量版）
        latest_data = {k: v for k, v in valuation.items() if k != "calculation_steps"}
        latest_path = os.path.join(ticker_dir, "latest.json")
        with open(latest_path, "w", encoding="utf-8") as f:
            json.dump(latest_data, f, ensure_ascii=False, indent=2)

        # 履歴ファイル（フル版）
        date_str = valuation.get("calculation_date", datetime.now().strftime("%Y-%m-%d"))
        history_path = os.path.join(history_dir, f"{date_str}.json")
        with open(history_path, "w", encoding="utf-8") as f:
            json.dump(valuation, f, ensure_ascii=False, indent=2)

        print(f"   💾 保存: {latest_path}")

    def run_single(self, ticker: str) -> dict:
        """単一ティッカーの処理"""
        return self.run([ticker]).get(ticker, {})


def main():
    """コマンドライン実行"""
    tickers = sys.argv[1:] if len(sys.argv) > 1 else None
    
    pipeline = TanukiValuationPipeline()
    results = pipeline.run(tickers)
    
    # 終了コード
    sys.exit(0 if results else 1)


if __name__ == "__main__":
    main()
