# On a Journey — 投資分析ツール群

このリポジトリは、以下の4つのサブシステムから構成される投資分析ツール群です。

- **Market Pulse**  
  米国市場の主要指標（VIX、S&P500、原油、金利など）を収集し、xAI Grok による分析レポートを生成、メール配信します。

- **MACRO PULSE**  
  経済指標（NFP、ISM、ミシガン大学消費者信頼感指数など）を自動取得し、イベントベースで記録します。S&P500 の反応（リターン）を後日補完します。

- **Adjusted EPS Analyzer (AEA)**  
  SEC EDGAR から企業の四半期・年次財務データを抽出し、GAAP EPS と Adjusted EPS を計算します。調整項目の内訳や AI 分析（xAI Grok）も含みます。

- **Stock Event Timeline**  
  Streamlit アプリケーションで、株価チャートと出来高スパイクを可視化します。

---

## ディレクトリ構成
On-a-journey-git/
├── .github/workflows/
│ ├── 05_update.yml # MACRO PULSE の定期実行
│ ├── collect_and_send.yml # Market Pulse の定期実行
│ └── update.yml # Adjusted EPS Analyzer の定期実行
├── config/ # 設定ファイル（CIKマップ、調整項目定義、セクター定義等）
├── data/ # MACRO PULSE の出力（events, fed_context 等）
├── docs/ # GitHub Pages 配信用HTML + 各システムの出力データ
│ ├── market-monitor/ # Market Pulse の出力（JSON/CSV）とHTML
│ │ └── data/
│ └── value-monitor/ # AEA の出力（銘柄別JSON）とHTML
│ └── adjusted_eps_analyzer/data/
├── src/ # ソースコード
│ ├── market/
│ │ ├── macro_pulse/ # MACRO PULSE モジュール
│ │ └── market_pulse/ # Market Pulse モジュール
│ └── value/
│ ├── adjusted_eps_analyzer/ # AEA モジュール
│ └── stock_event_timeline/ # Streamlit アプリ
├── venv/ # ローカル仮想環境（任意）
├── pyproject.toml # 依存パッケージ定義（pip install -e . でインストール）
└── README.md # 本ファイル


---

## 環境構築

### 必要条件
- Python 3.10 以上
- 以下の環境変数（GitHub Secrets またはローカル環境変数）

| 変数名 | 用途 |
|--------|------|
| `XAI_API_KEY` | xAI Grok API（Market Pulse, AEA） |
| `GMAIL_USER` / `GMAIL_PASSWORD` | Market Pulse メール送信（Gmail） |
| `FRED_API_KEY` | MACRO PULSE の FRED データ取得 |
| `GEMINI_API_KEY` | MACRO PULSE の FOMC 分析 |
| `DISCORD_WEB_HOOK` | MACRO PULSE の Discord 通知 |

### インストール
```bash
git clone https://github.com/Koichi-Shigihara2/On-a-journey.git
cd On-a-journey
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -e .
各システムの使用方法
Market Pulse
bash
python src/market/market_pulse/collect_and_send.py
RSS からニュース取得 → 主要指標取得（yfinance） → xAI Grok 分析 → メール送信 → docs/market-monitor/data/ に JSON/CSV 保存

MACRO PULSE
bash
# 通常実行（前日分）
python src/market/macro_pulse/05_main.py

# リターン補完のみ
python src/market/macro_pulse/05_main.py --fill-returns

# スケジュール更新
python src/market/macro_pulse/05_main.py --update-schedule
Adjusted EPS Analyzer
bash
# 全銘柄更新（monitor_tickers.yaml に登録された全銘柄）
python -m src.value.adjusted_eps_analyzer.pipeline

# 個別銘柄更新
python -m src.value.adjusted_eps_analyzer.pipeline --ticker TSLA
Stock Event Timeline（Streamlit）
bash
python -m streamlit run src/value/stock_event_timeline/app.py
ブラウザで http://localhost:8501 を開きます。

GitHub Actions 定期実行スケジュール
ワークフロー	実行タイミング (UTC)	日本時間 (JST)	処理内容
05_update.yml	0 22 * * *	毎日 07:00	前日の経済指標データ更新（通常実行）
05_update.yml	0 13 * * *	毎日 22:00	S&P500 リターン補完（当日終値反映）
05_update.yml	0 22 * * 6	毎週日曜 07:00	スケジュール更新 + FOMC コンテキスト更新
collect_and_send.yml	0 23 * * *	毎日 08:00	Market Pulse 実行
update.yml	0 10 * * 1	毎週月曜 19:00	AEA 全銘柄更新
トラブルシューティング
1. Market Pulse の出力先が src/docs/ になる
原因：collect_and_send.py 内の REPO_ROOT 計算が間違っている。

修正：REPO_ROOT を os.path.dirname 4回実行するように変更。
最新のコードでは修正済み。

2. AEA の summary.json が個別更新で消える
原因：個別銘柄更新時にも summary.json を上書きしていたため。

修正：個別更新時は既存の summary.json にマージするように変更。
最新の pipeline.py では修正済み。

3. SOFI のデータが取得できない（404）
原因：config/cik_lookup.csv の CIK が 0001817374 と誤っていた。

修正：正しい CIK 0001818874 に変更。
CIK は SEC EDGAR の URL や company_tickers.json から確認可能。

4. pip install -e . で ModuleNotFoundError
原因：pyproject.toml に依存パッケージが不足している（例：plotly など）。

修正：不足パッケージを pyproject.toml の dependencies に追加し、再インストール。

開発ブランチ
現在の開発ブランチは kaihatsu です。
すべての GitHub Actions ワークフローはこのブランチを対象としています。

ライセンス
このプロジェクトは個人利用を目的としています。
使用する外部 API（xAI, FRED, SEC, yfinance 等）は各サービスの利用規約に従ってください。

問い合わせ
リポジトリ管理者：Koichi Shigihara
GitHub Issues または直接連絡ください。
