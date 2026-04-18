"""
TANUKI VALUATION - Pipeline v2.1
全ティッカーを処理し、latest.jsonを生成

使用方法:
    python pipeline.py
    python pipeline.py TSLA PLTR  # 特定ティッカーのみ

v2.1変更点:
    - AI検証機能（validator.py）を統合
    - latest.jsonに"validation"フィールドを追加
"""

import json
import os
import sys
from datetime import datetime
from typing import List, Optional

# 同一ディレクトリからのインポート
from data_fetcher import TanukiDataFetcher
from core_calculator import KoichiValuationCalculator
from validator import validate_calculation


class TanukiValuationPipeline:
    """TANUKI VALUATION パイプライン"""

    # 監視対象ティッカー
    DEFAULT_TICKERS = [
        "TSLA", "PLTR", "SOFI", "CELH", "NVDA",
        "AMD", "APP", "SOUN", "RKLB", "ONDS",
        "MSFT", "AMZN", "FIG"
    ]

    def __init__(self, output_dir: str = None, use_ai_validation: bool = True):
        self.fetcher = TanukiDataFetcher()
        self.calculator = KoichiValuationCalculator()
        self.use_ai_validation = use_ai_validation
        
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
        print(f"  Koichi式 v5.2（成長率減衰カーブ＋FCF補正＋将来予測＋AI検証）")
        print("=" * 60)
        
        if tickers is None:
            tickers = self.DEFAULT_TICKERS

        results = {}
        success_count = 0
        error_count = 0
        validation_stats = {"pass": 0, "warn": 0, "fail": 0, "error": 0}

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

                # AI検証を実行
                try:
                    validation = validate_calculation(
                        ticker, 
                        valuation, 
                        use_ai=self.use_ai_validation
                    )
                    valuation["validation"] = validation
                    
                    overall = validation.get("overall", "ERROR")
                    if overall == "PASS":
                        print(f"   ✅ 検証パス")
                        validation_stats["pass"] += 1
                    elif overall == "WARN":
                        print(f"   ⚠️  検証警告: {self._get_warn_details(validation)}")
                        validation_stats["warn"] += 1
                    else:
                        print(f"   ❌ 検証失敗: {self._get_warn_details(validation)}")
                        validation_stats["fail"] += 1
                        
                except Exception as e:
                    print(f"   ⚠️  検証エラー: {e}")
                    valuation["validation"] = {
                        "validated_at": datetime.now().strftime("%Y-%m-%d"),
                        "model": "error",
                        "checks": {},
                        "overall": "ERROR",
                        "ai_comment": str(e)
                    }
                    validation_stats["error"] += 1

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
        print(f"   検証結果: PASS={validation_stats['pass']} WARN={validation_stats['warn']} FAIL={validation_stats['fail']} ERROR={validation_stats['error']}")
        print(f"   出力先: {self.output_dir}")
        print("=" * 60)

        # 銘柄一覧インデックスを更新（index.htmlが参照）
        if results:
            self._save_tickers_index(list(results.keys()))

        return results

    def _get_warn_details(self, validation: dict) -> str:
        """検証警告の詳細を取得"""
        checks = validation.get("checks", {})
        failed = [k for k, v in checks.items() if not v.get("pass", True)]
        return ", ".join(failed) if failed else "unknown"

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

    def _save_tickers_index(self, success_tickers: List[str]) -> None:
        """
        成功した銘柄一覧を tickers.json に保存
        index.html がこのファイルを参照して銘柄カードを動的生成する

        部分実行（特定銘柄のみ）の場合は既存のtickers.jsonとマージして
        全成功銘柄を維持する
        """
        index_path = os.path.join(self.output_dir, "tickers.json")

        # 既存のtickers.jsonを読み込んでマージ
        existing_tickers = []
        if os.path.exists(index_path):
            try:
                with open(index_path, encoding="utf-8") as f:
                    existing_tickers = json.load(f).get("tickers", [])
            except Exception:
                pass

        # 成功した銘柄を追加（重複排除・アルファベット順）
        merged = sorted(set(existing_tickers) | set(success_tickers))

        # latest.jsonが存在しない銘柄は除外（削除された銘柄の掃除）
        valid = [
            t for t in merged
            if os.path.exists(os.path.join(self.output_dir, t, "latest.json"))
        ]

        index_data = {
            "tickers": valid,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(valid)
        }

        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)

        print(f"   📋 tickers.json 更新: {valid}")

    def run_single(self, ticker: str) -> dict:
        """単一ティッカーの処理"""
        return self.run([ticker]).get(ticker, {})


def main():
    """コマンドライン実行"""
    tickers = sys.argv[1:] if len(sys.argv) > 1 else None
    
    # AI検証を有効化（XAI_API_KEYが設定されている場合）
    use_ai = bool(os.environ.get("XAI_API_KEY"))
    
    pipeline = TanukiValuationPipeline(use_ai_validation=use_ai)
    results = pipeline.run(tickers)
    
    # 終了コード
    sys.exit(0 if results else 1)


if __name__ == "__main__":
    main()
