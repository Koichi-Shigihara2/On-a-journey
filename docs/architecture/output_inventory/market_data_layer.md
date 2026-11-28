# 市場データ層（common/market_data）の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`market_data_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-22、Koichiさんの判断）。読み手0件の2つ（検証の結果のログ・取り直しの記録）と誰も読まないキー1つを消し、analyst_history の8キーは溜まる履歴のため残す。_validation_warnings は品質の印として残す（読み手は後の画面の整理で決める）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 9（ファイル 9・人が見るもの 0）／読み手0件 0／テストだけ 0／1件 2／2件以上 7／規則の二重 0（検出式 3種のうち。市場データ層のファイルを含むもの 0）

**誰も読まないキー**: **23**（この段階のJSONのキーで今の書き手のコードにある 113 のうち。下の「誰も読まないキー」。今の書き手のコードに無いキー 1 は数えない）

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋1件（10.0%）、読み方を決めきれない読み込み 1件＋ファイル名で当たった行 4件／解決できた読み込み 21件＋ファイル名で当たった行 37件（7.9%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

フォルダを決めきれない行（ファイル名で当たった行）:
- `common/market_data/split_reconcile.py` 行 64（`common/market_data/daily/{T}.json`）: `for p in sorted(glob.glob(os.path.join(base, "daily", "*.json"))):`
- `common/market_data/split_reconcile.py` 行 179（`common/market_data/daily/{T}.json`）: `p = os.path.join(base, "daily", f"{s}.json")`
- `common/market_data/split_reconcile.py` 行 202（`common/market_data/daily/{T}.json`）: `with open(os.path.join(base, "daily", f"{s}.json"), encoding="utf-8") as f:`
- `scripts/analysis/market_pulse_redesign.py` 行 60（`docs/market-monitor/market-pulse/data/sp500_tickers.json`）: `sp500 = json.loads(subprocess.run(["git", "-C", REPO, "show", f"{DATA_REV}:docs/market-monitor/market-pulse/data/sp500_t`

## 消したもの（段階2、2026-10-22）

- `common/market_data/{T}/market_data_violations_log.json（596ファイル）と書き手（fetcher.py の _write_violations_section・_violations_log_path）`: 削除（2026-10-22、Koichiさんの判断）。読み手0件（FINDINGS 2026-10-15 の F3）。保存前の検証は残し、警告は各レコードの _validation_warnings と標準出力に出す。ワークフロー2本の git add の行と .gitattributes の行も消した
- `common/market_data/_daily_close_retry_log.json と fetch_daily_prices の追記の処理`: 削除（2026-10-22、Koichiさんの判断）。読み手0件（書き手が追記のために読み直すだけ）。終値の無い足の取り直し（_retry_missing_closes）と、Yahooの作り直しの時間帯の判定は残す
- `attributes の next_quarter_eps_date の書き込み`: 書き込みをやめた（2026-10-22）。読み手0件（TANUKI が読むのは next_quarter_eps_estimate だけ）。前後で daily・attributes・analyst_history は消したキーのほか同じ（yfinance の応答を記録して再生、11銘柄）、TANUKI の96銘柄の理論株価・判定と HypeCore の98銘柄の stage は前後で同じ

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `/tmp/daily_status.json`<br>（0ファイル） | Market_Data_Daily_Updateの1回の日次取得の結果（status）。同じワークフローの後の段が読む | `fetcher.py:<module>` | 1 | — | — | — | 残す（2026-10-22） |
| 1件 | `common/market_data/valuation_history/{T}.json`<br>（90ファイル） | yfinanceの倍率（forward_pe等）の週1回の値を日付つきで積む履歴（追記だけ、[[HYPECORE-STAGE-TIMESCALE-1]] 段B-4）。読み手は倍率の部品（common/multiples.py） | `valuation_history.py:append` | 1 | — | — | — | 残す（2026-10-22） |
| 2件以上 | `common/market_data/_daily_rejected_overwrites.json`<br>（1ファイル） | 確定の日足の行に違う値が来て上書きを拒んだ記録（どの行・どの項目・前と新しい値・経路・回数、90日分。fetcher.write_daily_rows、[[MARKETDATA-DAILY-ROW-WRITE-1]]、2026-11-03）。Market_Data_Daily_Updateが commit し、その実行の分を Step Summary に出す | `fetcher.py:_log_rejected` | 2 | — | — | — | 残す（2026-10-22） |
| 2件以上 | `common/market_data/_sp500_constituents_cache.json`<br>（1ファイル） | S&P500の構成銘柄の一覧の控え（正。calendar_universe.sp500_constituents が7日以内なら取り直さずに使う）。市場データ層の取得の銘柄の範囲（fetcher）・reader・Market Pulseのブレッス（写しを data/sp500_tickers.json に書く）が使う | `calendar_universe.py:sp500_constituents` | 2・テスト1 | market-monitor/market-pulse/index.html（経由） | — | — | 残す（2026-10-22） |
| 2件以上 | `common/market_data/analyst_history/{T}.json`<br>（589ファイル） | 銘柄ごとのアナリストのイベント（格付け・目標株価の変更）、EPSの実績と予想、推奨の内訳の推移。3系統とも追記型の履歴（Yahooが返すのは直近の分だけで、過去の分はこのファイルにしか残らない） | `fetcher.py:fetch_analyst_events` | 3・テスト2 | index.html（経由）<br>portfolio/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: HypeCore（TANUKI のtiming・判定の入力） | — | 残す（2026-10-22） |
| 2件以上 | `common/market_data/attributes/{T}.json`<br>（589ファイル） | 銘柄ごとの週次の属性（yfinanceの.infoのスナップショット: β・時価総額・PER・セクター・アナリストの目標株価・次の決算のEPSの予想等） | `fetcher.py:fetch_weekly_attributes` | 18・テスト18 | index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・HypeCore（TANUKI のtiming・判定の入力） | — | 残す（2026-10-22） |
| 2件以上 | `common/market_data/daily/{T}.json`<br>（589ファイル） | 銘柄・指数・先物・ETFごとの日次の株価（始値・高値・安値・終値・出来高）。暫定の足・Yahooに足の無い日の印を含む。TANUKI・HypeCore・STONKS・Market Pulse・Score Verifier等が読む | `fetcher.py:write_daily_rows` | 22・テスト18（1） | index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・HypeCore（TANUKI のtiming・判定の入力） | AS-IS-258・AS-IS-285・AS-IS-286・AS-IS-287・AS-IS-297・AS-IS-298・AS-IS-299 | 残す（2026-10-22） |
| 2件以上 | `docs/market-monitor/market-pulse/data/nyse_holidays.json`<br>（1ファイル） | 前年〜翌年のNYSEの休場日（土日以外、XNYS）。Market Pulse（collect_and_send.py）が毎晩 calendar_universe.write_nyse_holidays で書き、画面（market-pulse/index.html）が休場の行に使う（2026-10-23に書き手を calendar_universe.py へ移した） | `calendar_universe.py:write_nyse_holidays` | 4 | market-monitor/market-pulse/index.html（直接）<br>value-monitor/tanuki_score/index.html（経由） | — | — | 残す（2026-10-22） |
| 2件以上 | `docs/market-monitor/market-pulse/data/sp500_tickers.json`<br>（1ファイル） | S&P500の構成銘柄の写し（正は common/market_data/_sp500_constituents_cache.json）。Market Pulseの breadth_calculator.py が calendar_universe.write_sp500_copy で書き、browser_checks/market_pulse_elements.py が読む（2026-10-23に書き手を calendar_universe.py へ移した） | `calendar_universe.py:write_sp500_copy` | 3 | — | — | — | 残す（2026-10-22） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|

## 読み手の内訳

### `/tmp/daily_status.json`（1件）
- 起動元: .github/workflows/Market_Data_Daily_Update.yml・.github/workflows/Market_Data_Weekly_Update.yml
- ワークフロー・保存済みファイル: `.github/workflows/Market_Data_Daily_Update.yml` 行 80・83・85・87

### `common/market_data/valuation_history/{T}.json`（1件）
- 起動元: .github/workflows/Market_Data_Weekly_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `records`(1)・`symbol`(0)
- Python・保存済みファイル: `common/multiples.py` 行 433・462・470（経由 `common/multiples.py::_load`、`common/multiples.py::build`）

### `common/market_data/_daily_rejected_overwrites.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Daily_Update.yml・.github/workflows/Market_Data_Weekly_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `entries`(2)・`note`(1)
- Python・保存済みファイル: `common/market_data/fetcher.py` 行 367
- ワークフロー・保存済みファイル: `.github/workflows/Market_Data_Daily_Update.yml` 行 145

### `common/market_data/_sp500_constituents_cache.json`（2件以上）
- 起動元: .github/workflows/Beta_Config_Update.yml・.github/workflows/Market_Data_Daily_Update.yml・.github/workflows/Market_Data_Weekly_Update.yml・.github/workflows/Market_Pulse_Update.yml・.github/workflows/SEC_Data_Audit.yml・.github/workflows/Score_Verifier.yml・.github/workflows/TANUKI_VALUATION_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `count`(2)・`fetched_at`(2)・`tickers`(2)
- Python・保存済みファイル: `common/market_data/calendar_universe.py` 行 307・370
- Python・保存済みファイル: `src/market/market_pulse/breadth_calculator.py` 行 71・434（経由 `common/market_data/calendar_universe.py::write_sp500_copy`）
- テスト・保存済みファイル: `tests/test_calendar_universe.py` 行 45（経由 `common/market_data/calendar_universe.py::write_sp500_copy`）
- 画面（経由（common/market_data/calendar_universe.py → docs/market-monitor/market-pulse/data/nyse_holidays.json））: `docs/market-monitor/market-pulse/index.html`

### `common/market_data/analyst_history/{T}.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Daily_Update.yml・.github/workflows/Market_Data_Weekly_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `earnings_history`(2)・`events`(2)・`recommendations_history`(2)・`symbol`(2)
- Python・保存済みファイル: `common/market_data/reader.py` 行 296・297・317・318・341・342（経由 `common/market_data/reader.py::_load_json`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 417・454・513・545・1084（経由 `common/market_data/reader.py::get_analyst_events`、`common/market_data/reader.py::get_earnings_history`、`common/market_data/reader.py::get_recommendations_history`）
- ワークフロー・保存済みファイル: `.github/workflows/Market_Data_Weekly_Update.yml` 行 116
- テスト・保存済みファイル: `tests/test_market_data_reader.py` 行 330・338・347・358・367・378・389・397・400・411・423・433 ほか2（経由 `common/market_data/reader.py::get_analyst_events`、`common/market_data/reader.py::get_earnings_history`、`common/market_data/reader.py::get_recommendations_history`）
- テスト・保存済みファイル: `tests/test_output_inventory_check.py` 行 424
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `common/market_data/attributes/{T}.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Daily_Update.yml・.github/workflows/Market_Data_Weekly_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `_validation_warnings`(2)・`analyst_count`(5)・`analyst_recommendation_key`(2)・`average_volume`(1)・`beta`(9)・`calendar`(4)・`country`(2)・`current_price`(8)・`dividend_yield`(2)・`earnings_growth`(1)・`enterprise_value`(4)・`estimates`(1)・`ev_to_ebitda`(4)・`fetched_at`(9)・`forward_eps`(4)・`forward_pe`(6)・`gross_margins`(1)・`implied_shares_outstanding`(3)・`industry`(9)・`market_cap`(8)・`next_quarter_eps_date`(0)・`next_quarter_eps_estimate`(1)・`payout_ratio`(2)・`peg_ratio`(3)・`price_to_sales`(2)・`recommendation_mean`(3)・`revenue_growth`(2)・`sector`(12)・`shares_outstanding`(5)・`short_pct_float`(3)・`short_ratio`(1)・`symbol`(6)・`target_high_price`(2)・`target_low_price`(2)・`target_mean_price`(2)・`target_median_price`(2)・`total_debt`(5)・`trailing_pe`(5)
- Python・保存済みファイル: `common/earnings_pending.py` 行 117（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `common/guidance_gap.py` 行 49（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `common/market_data/reader.py` 行 257・260・277（経由 `common/market_data/reader.py::_load_json`、`common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `common/market_data/valuation_history.py` 行 38・64・70（経由 `common/market_data/valuation_history.py::_load`、`common/market_data/valuation_history.py::append`）
- Python・保存済みファイル: `common/sec_data/audit.py` 行 73・191・317・369・390・468（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `common/sector_master.py` 行 206・291・350・363（経由 `common/sector_master.py::_load_json`、`common/sector_master.py::build_entry`）
- Python・保存済みファイル: `common/system_health.py` 行 89（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `scripts/analysis/market_pulse_redesign.py` 行 404・478（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/extract_key_facts.py` 行 1084・1209・1232（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 639・653・1007（経由 `common/market_data/reader.py::get_attributes`、`common/sector_master.py::build_entry`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/sector_classifier_v2.py` 行 47（経由 `common/sector_master.py::build_entry`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 236・490・545・548・1084（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 115・267（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `src/value/stonks_silo/valuation_fetcher.py` 行 94（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `src/value/tanuki_valuation/beta_fetcher.py` 行 104・176・288（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 802（経由 `common/market_data/reader.py::get_attributes`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 397・460・995・1080・1141・1182・3397・3420・3981（経由 `common/market_data/reader.py::get_attributes`）
- ワークフロー・保存済みファイル: `.github/workflows/Market_Data_Weekly_Update.yml` 行 115
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 42・66・71・78・87・95・99・104・111（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_beta_fetcher.py` 行 59・67・71（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 73・81・88・96・120・130・139・150・158・187・199・206 ほか1（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_earnings_pending.py` 行 43・68（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_eps_analyzer_q2q3_zero_quarterly_presence_guard.py` 行 103・123・143・163（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_eps_analyzer_q4_zero_quarterly_presence_guard.py` 行 106・128・151・174（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_eps_cik_history_merge.py` 行 70・84・92（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_eps_sector_classifier_master.py` 行 32・33・34・35・36・37・43・45・54（経由 `common/sector_master.py::build_entry`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_net_income_selection.py` 行 107・154（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_share_fallback.py` 行 88・122・149・190（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_split.py` 行 100・124・149・176（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_market_data_reader.py` 行 286・294・301・313・325・469・471（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_multiples.py` 行 110・111・113（経由 `common/market_data/valuation_history.py::append`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 461・500・522・2323・2340・2383・2408・2437・2460・2472・2485・2496 ほか12（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_sector_master.py` 行 49・68・69・70・76・82・85・89・96・141（経由 `common/sector_master.py::build_entry`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 131（経由 `common/market_data/reader.py::get_attributes`）
- テスト・保存済みファイル: `tests/test_valuation_fetcher_market_data_switch.py` 行 64・85・101・117・128・141・155（経由 `common/market_data/reader.py::get_attributes`）
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（common/sector_master.py → docs/common/sector_class.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `common/market_data/daily/{T}.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Daily_Update.yml・.github/workflows/Market_Data_Weekly_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `records`(17)・`symbol`(13)
- Python・その場で計算: `common/market_data/split_reconcile.py` 行 62・175・196・217（経由 `common/market_data/fetcher.py::_resolve_base_dir`）
- Python・保存済みファイル: `browser_checks/check_dependency_map.py` 行 193・411（経由 `common/market_data/reader.py::_load_daily_payload`）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 808・829・846・873・950（経由 `common/market_data/reader.py::_load_daily_payload`）
- Python・保存済みファイル: `common/external_watch.py` 行 50（経由 `common/market_data/futures_roll.py::_load`）
- Python・保存済みファイル: `common/market_data/daily_guard.py` 行 71・77・91・101・114・121（経由 `common/market_data/daily_guard.py::jpx_pending`）
- Python・保存済みファイル: `common/market_data/fetcher.py` 行 513・514・534・556・557・566・570・571・585・586・724・741 ほか21（経由 `common/market_data/fetcher.py::_load_json`、`common/market_data/fetcher.py::_mark_no_bar_rows`、`common/market_data/fetcher.py::_repair_close_missing_records` ほか）
- Python・保存済みファイル: `common/market_data/futures_roll.py` 行 39・41・86・107・109・124（経由 `common/market_data/futures_roll.py::_load`）
- Python・保存済みファイル: `common/market_data/reader.py` 行 70・71・96・119・148・154・178・202・227・357・376（経由 `common/market_data/reader.py::_load_daily_payload`、`common/market_data/reader.py::_load_json`）
- Python・保存済みファイル: `common/multiples.py` 行 75・164・418・423・425・428・434・462・470（経由 `common/multiples.py::_daily_rows`、`common/multiples.py::_load`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1570・3580・3752（経由 `common/market_data/fetcher.py::find_missing_trading_days`）
- Python・保存済みファイル: `common/sec_data/rotce.py` 行 342・345・432・703・743（経由 `common/sec_data/rotce.py::_load_prices`）
- Python・保存済みファイル: `common/stage_v2.py` 行 339・355・361（経由 `common/multiples.py::_daily_rows`）
- Python・保存済みファイル: `common/system_health.py` 行 1111・1314・1397・1473（経由 `common/market_data/futures_roll.py::_load`、`common/system_health.py::_check_r`）
- Python・保存済みファイル: `scripts/migrate_market_data_latest.py` 行 57（経由 `common/market_data/reader.py::_load_daily_payload`）
- Python・保存済みファイル: `src/market/market_pulse/breadth_calculator.py` 行 144・304・305・435・437（経由 `common/market_data/reader.py::_load_daily_payload`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 142・248・412・413・418・456・463・464・780・781・858・888 ほか17（経由 `common/market_data/reader.py::_load_daily_payload`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 159・238・536・548（経由 `common/market_data/reader.py::_load_daily_payload`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 115（経由 `common/market_data/reader.py::_load_daily_payload`）
- Python・保存済みファイル: `src/value/stonks_silo/valuation_fetcher.py` 行 87（経由 `common/market_data/reader.py::_load_daily_payload`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 795・858（経由 `common/market_data/reader.py::_load_daily_payload`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 397（経由 `common/market_data/reader.py::_load_daily_payload`）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_verifier.py` 行 47・77（経由 `common/market_data/reader.py::_load_daily_payload`）
- ワークフロー・保存済みファイル: `.github/workflows/Market_Data_Daily_Update.yml` 行 139
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/market_data/fetcher.py::find_missing_trading_days`）
- テスト・保存済みファイル: `tests/test_close_missing_price.py` 行 47・59・70・85・97・103・108・137・143・150・161・237 ほか15（経由 `common/market_data/daily_guard.py::jpx_pending`、`common/market_data/fetcher.py::_replace_provisional_records`、`common/market_data/fetcher.py::fetch_daily_prices` ほか）
- テスト・保存済みファイル: `tests/test_common_path_guard.py` 行 137・140・152・153（経由 `common/sec_data/rotce.py::_load_prices`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 73・81・88・96・120・130・139・150・158・187・199・206 ほか1（経由 `common/market_data/reader.py::_load_daily_payload`）
- テスト・保存済みファイル: `tests/test_futures_roll.py` 行 55・60（経由 `common/market_data/futures_roll.py::_load`）
- テスト・保存済みファイル: `tests/test_market_data_daily_write.py` 行 132・136・144・161・176・183（経由 `common/market_data/fetcher.py::backfill_daily_prices`、`common/market_data/fetcher.py::find_missing_trading_days`、`common/market_data/fetcher.py::mark_and_repair_provisional_rows`）
- テスト・保存済みファイル: `tests/test_market_data_fetcher.py` 行 725・756・773・784・801（経由 `common/market_data/fetcher.py::backfill_daily_prices`）
- テスト・保存済みファイル: `tests/test_market_data_reader.py` 行 43・48・57・64・69・74・75・80・96・108・122・127 ほか30（経由 `common/market_data/reader.py::_load_daily_payload`）
- テスト・保存済みファイル: `tests/test_market_data_stale_provisional.py` 行 67・79・89・107・121・129・136・137・138・164・175・185 ほか7（経由 `common/market_data/fetcher.py::fetch_daily_prices`、`common/market_data/fetcher.py::find_missing_trading_days`、`common/market_data/reader.py::_load_daily_payload`）
- テスト・保存済みファイル: `tests/test_output_inventory_check.py` 行 428
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 65・72・78・83（経由 `common/market_data/fetcher.py::find_missing_trading_days`）
- テスト・保存済みファイル: `tests/test_rotce.py` 行 64・70・81・94・96・101・110・119・130・138・145・153 ほか20（経由 `common/sec_data/rotce.py::_load_prices`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 505（経由 `common/system_health.py::_check_r`）
- テスト・保存済みファイル: `tests/test_sector_master.py` 行 106（経由 `common/market_data/futures_roll.py::_load`）
- テスト・保存済みファイル: `tests/test_system_health_daily_validation.py` 行 30・37・40・46・59・62・72・75・77（経由 `common/system_health.py::_check_r`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 295・303・435・448（経由 `common/system_health.py::_check_r`）
- テスト・保存済みファイル: `tests/test_valuation_fetcher_market_data_switch.py` 行 64・85・101・117・128・141・155（経由 `common/market_data/reader.py::_load_daily_payload`）
- テスト・保存済みファイル: `tests/test_value_freshness.py` 行 95・96・102・109・111・114・151・154・162（経由 `common/market_data/daily_guard.py::jpx_pending`）
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/{T}.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/_summary.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/market-monitor/market-pulse/data/nyse_holidays.json`（2件以上）
- 起動元: .github/workflows/Market_Pulse_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `from`(2)・`holidays`(2)・`source`(2)・`to`(2)
- Python・保存済みファイル: `browser_checks/check_dependency_map.py` 行 398・408（経由 `browser_checks/market_pulse_elements.py::expected_holiday_rows`）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 220・223・541（経由 `browser_checks/market_pulse_elements.py::expected_holiday_rows`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 2041
- JS・HTML・保存済みファイル: `docs/market-monitor/market-pulse/index.html` 行 1422
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/latest.json））: `docs/value-monitor/tanuki_score/index.html`

### `docs/market-monitor/market-pulse/data/sp500_tickers.json`（2件以上）
- 起動元: .github/workflows/Market_Pulse_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `count`(0)・`fetched_at`(0)・`tickers`(1)
- Python・保存済みファイル: `browser_checks/check_dependency_map.py` 行 411・447・457（経由 `browser_checks/market_pulse_elements.py::classify_base_dates`、`browser_checks/market_pulse_elements.py::run_derivation_checks`）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 47・866・946
- Python・保存済みファイル: `src/market/market_pulse/breadth_calculator.py` 行 30

## 誰も読まないキー

この段階の出力のJSON（全ファイル、入れ子8段まで）のキーのうち、テストを除くコード（.py・.js・.html。書き手のファイルを含む）に読みの形（`.key`・`['key']`〈代入でない〉・`.get('key')`）で出てこないもの。銘柄名・日付など小文字を含まないキーは除く。Pythonで`for k in d`・`d.items()`のように回して読むものは拾えないので、消す前にキーの名前でgrepする

- `_contract_roll`（`common/market_data/daily/{T}.json`）: 残す（動的な読み）: 先物の限月の切り替えの印。fetcher が既存の行の印を上書きしないために自分で読む（_MARK_KEYS）
- `ago_30d`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の履歴〈estimates_history/〉の元。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）
- `ago_60d`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の履歴〈estimates_history/〉の元。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）
- `ago_7d`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の履歴〈estimates_history/〉の元。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）
- `ago_90d`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の履歴〈estimates_history/〉の元。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）
- `current_price_target`（`common/market_data/analyst_history/{T}.json`）: 残す（記録）: 作り直せない。アナリストの目標株価・EPS予想と実績の履歴（Yahooは直近の分しか返さず、analyst_history は追記型で溜める）
- `down_7d`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の件数〈直近7日〉。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）
- `eps_actual`（`common/market_data/analyst_history/{T}.json`）: 残す（記録）: 作り直せない。アナリストの目標株価・EPS予想と実績の履歴（Yahooは直近の分しか返さず、analyst_history は追記型で溜める）
- `eps_difference`（`common/market_data/analyst_history/{T}.json`）: 残す（記録）: 作り直せない。アナリストの目標株価・EPS予想と実績の履歴（Yahooは直近の分しか返さず、analyst_history は追記型で溜める）
- `eps_estimate`（`common/market_data/analyst_history/{T}.json`）: 残す（記録）: 作り直せない。アナリストの目標株価・EPS予想と実績の履歴（Yahooは直近の分しか返さず、analyst_history は追記型で溜める）
- `fiscal`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の履歴〈estimates_history/〉の元。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）
- `last_fiscal_year_end`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の履歴〈estimates_history/〉の元。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）
- `most_recent_quarter`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の履歴〈estimates_history/〉の元。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）
- `next_fiscal_year_end`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の履歴〈estimates_history/〉の元。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）
- `period_ends`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の履歴〈estimates_history/〉の元。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）
- `prev`（`common/market_data/daily/{T}.json`）: 残す（監査用）: 足の訂正の前の値（_corrections の prev）。訂正が正しいかを、人（Claude Code）が日足の訂正の前後を比べて確かめる。テストも参照する
- `price_target_action`（`common/market_data/analyst_history/{T}.json`）: 残す（記録）: 作り直せない。アナリストの目標株価・EPS予想と実績の履歴（Yahooは直近の分しか返さず、analyst_history は追記型で溜める）
- `price_to_sales`（`common/market_data/attributes/{T}.json`、`common/market_data/valuation_history/{T}.json`）: 残す（記録）: 作り直せない。valuation_history が4つの倍率（先行PER・実績PER・PSR・EV/EBITDA）を同じ形で月ごとに溜めた履歴（Yahooは直近の値しか返さない）
- `prior_price_target`（`common/market_data/analyst_history/{T}.json`）: 残す（記録）: 作り直せない。アナリストの目標株価・EPS予想と実績の履歴（Yahooは直近の分しか返さず、analyst_history は追記型で溜める）
- `reason_label`（`common/market_data/daily/{T}.json`）: 残す（監査用）: 足の訂正の理由の表示名（_corrections の台帳）。日足の訂正（分割の調整・平らな足の取り直し）を、人（Claude Code）が WARN-62 の確認や調整の検証で読む
- `strong_buy`（`common/market_data/analyst_history/{T}.json`）: 残す（記録）: 作り直せない。アナリストの目標株価・EPS予想と実績の履歴（Yahooは直近の分しか返さず、analyst_history は追記型で溜める）
- `strong_sell`（`common/market_data/analyst_history/{T}.json`）: 残す（記録）: 作り直せない。アナリストの目標株価・EPS予想と実績の履歴（Yahooは直近の分しか返さず、analyst_history は追記型で溜める）
- `up_7d`（`common/market_data/attributes/{T}.json`）: これから作る: [[EPS-REVISION-OUTLOOK-MARK-1]]（EPS予想の改定の件数〈直近7日〉。見通しの悪化の確認の印〔遅れて追う印。予測ではない〕を、履歴が回り始めた後に stock.html の GUIDANCE のブロックの下の行と TANUKI SCORE の1列に出す。BUY チェックリストには出さない。読み手は画面）

### 今の書き手のコードに無いキー（古いファイル・古いエントリに残るだけ。誰も読まないキーに数えない）

データのキーのうち、書き手のコード（書き手の .py・それを import する呼び出し元・それらが import する .py）に文字列として出てこないもの。書くのをやめて履歴のスナップショット・古いエントリに残るキーや、取得元のデータをそのまま写したキー（SEC の単位・次元の名前等）

- `next_quarter_eps_date`（`common/market_data/attributes/{T}.json`）

## ワークフローが自分で書く出力（.pyの外）

段階の.pyを動かすワークフローの中で、`$GITHUB_STEP_SUMMARY`に書く段。`.get()`で読むキーが、この段階のJSON（全ファイル）にあるか。

- `.github/workflows/Market_Data_Daily_Update.yml`（行 135・136・137・138・140・142）: `entries`（ある）
- `.github/workflows/Market_Data_Weekly_Update.yml`（行 113・114・117・118）: 
- `.github/workflows/TANUKI_VALUATION_Update.yml`（行 104・105・106・107・112）: `intrinsic_value_per_share`（**無い**）・`overall`（**無い**）・`validation`（**無い**）

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | 市場データ層か | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1074`<br>`src/value/hypecore/hypecore.py:1119` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:21` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。市場データ層のファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | 市場データ層を含む |
|---|---|---|---|

1か所だけのもの: 取引日（市場の営業日）の決め方（取引所の暦を直接読む所・手で書いた営業日の計算）（1）・暫定の足の判定（日足の確定時刻・暫定かどうか・暫定の種類を決める関数）（1）・S&P500の構成銘柄の一覧の取り方（取得元を直接読む所）（1）

## 本番外のコード（common/market_data）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

候補は0件。見たファイル（7ファイル・2,983行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |
|---|---|---|---|---|---|
| `calendar_universe.py` | 390 | — | `common/sec_data/report_consistency_check.py`<br>`common/system_health.py`<br>`common/value_freshness.py`<br>`src/market/macro_pulse/05_main.py`<br>ほか7 | `common/market_data/daily_guard.py`<br>`common/market_data/fetcher.py`<br>`common/market_data/reader.py` | 5件 |
| `daily_guard.py` | 121 | `.github/workflows/Market_Data_Daily_Update.yml` | — | `common/market_data/calendar_universe.py` | 4件 |
| `fetcher.py` | 1,632 | `.github/workflows/Market_Data_Daily_Update.yml`<br>`.github/workflows/Market_Data_Weekly_Update.yml` | `common/macro_data/fetcher.py`<br>`common/registration/register_ticker.py`<br>`common/sec_data/report_consistency_check.py` | `common/market_data/__init__.py`<br>`common/market_data/calendar_universe.py`<br>`common/market_data/futures_roll.py`<br>`common/market_data/reader.py`<br>ほか1 | 10件 |
| `futures_roll.py` | 127 | `.github/workflows/Market_Data_Daily_Update.yml` | `common/external_watch.py` | — | 1件 |
| `reader.py` | 379 | — | `browser_checks/check_dependency_map.py`<br>`browser_checks/market_pulse_elements.py`<br>`common/earnings_pending.py`<br>`common/guidance_gap.py`<br>ほか12 | `common/market_data/__init__.py` | 8件 |
| `split_reconcile.py` | 264 | `.github/workflows/Market_Data_Weekly_Update.yml` | — | — | 2件 |
| `valuation_history.py` | 70 | `.github/workflows/Market_Data_Weekly_Update.yml` | — | — | 1件 |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 1件
  - `common/market_data/split_reconcile.py:234` record_skips → `<?>/_split_reconcile_skips.json`
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 1件
  - `common/market_data/fetcher.py:1596` <module> → `<?>`
