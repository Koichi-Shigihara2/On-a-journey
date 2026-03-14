import yaml
import json
import os
from datetime import datetime

# モジュールインポート（実際のファイル名に合わせて調整済み）
from edgar_fetcher import fetch_filings
from xbrl_parser import parse_xbrl
from adjustment_detector import detect_adjustments
from tax_adjuster import apply_tax_adjustments   # ← ここを関数名に合わせ修正
from eps_calculator import calculate_eps
from ai_analyzer import analyze_adjustments      # ← Grok/xAI版を使う想定

def save_result(ticker, accession_no, result):
    """結果をJSONとして保存"""
    os.makedirs(f"data/{ticker}", exist_ok=True)
    filepath = f"data/{ticker}/{accession_no}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"Saved: {filepath}")

def run():
    """メイン処理：監視銘柄のデータを更新"""
    try:
        with open("config/monitor_tickers.yaml", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            tickers = config.get("tickers", [])
    except Exception as e:
        print(f"Failed to load monitor_tickers.yaml: {e}")
        return

    if not tickers:
        print("No tickers found in monitor_tickers.yaml")
        return

    for ticker in tickers:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Processing {ticker}...")
        try:
            filings = fetch_filings(ticker)
            if not filings:
                print(f"No filings found for {ticker}")
                continue

            results_history = []
            for i, filing in enumerate(filings):
                try:
                    data = parse_xbrl(filing)
                    if not data:
                        print(f"No XBRL data for {filing.accession_no}")
                        continue

                    # 調整項目検出 → 税調整 → EPS計算
                    adjustments_raw = detect_adjustments(data)
                    net_adjustment, detailed_adjustments = apply_tax_adjustments(adjustments_raw, data)
                    result = calculate_eps(data, net_adjustment, detailed_adjustments)

                    # AI分析：最新の四半期（i==0）のみ実行
                    if i == 0:
                        try:
                            ai_result_str = analyze_adjustments(ticker, result, detailed_adjustments)
                            result["ai_analysis"] = json.loads(ai_result_str)
                        except json.JSONDecodeError:
                            result["ai_analysis"] = {
                                "health": "Error",
                                "comment": "AI output is not valid JSON",
                                "raw": ai_result_str
                            }
                        except Exception as ai_err:
                            result["ai_analysis"] = {
                                "health": "Error",
                                "comment": f"AI analysis failed: {str(ai_err)}"
                            }

                    # メタデータ追加
                    result["date"] = str(filing.period_end_date)
                    result["form"] = filing.form
                    result["accession_no"] = filing.accession_no
                    result["processed_at"] = datetime.now().isoformat()

                    save_result(ticker, filing.accession_no, result)
                    results_history.append(result)

                except Exception as e:
                    print(f"Skipping filing {filing.accession_no} for {ticker}: {e}")

            # 最新 + 履歴保存
            if results_history:
                latest = results_history[0]
                with open(f"data/{ticker}/latest.json", "w", encoding="utf-8") as f:
                    json.dump(latest, f, indent=2, ensure_ascii=False)

                with open(f"data/{ticker}/history.json", "w", encoding="utf-8") as f:
                    json.dump(results_history, f, indent=2, ensure_ascii=False)

                print(f"Completed {ticker}: {len(results_history)} filings processed")

        except Exception as outer_err:
            print(f"Critical error processing {ticker}: {outer_err}")

if __name__ == "__main__":
    run()
