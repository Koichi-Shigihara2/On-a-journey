# common直下の出力の一覧（段階1）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`common_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`があれば「効く」
- 残す／消す: 段階1の判断（2026-11-27、[[OUTPUT-INVENTORY-COMMON-LAYER-1]]）。common直下の出力8と、ファイルを書かない部品10を一覧にした。読み手0件の出力は無い。読み手のいないキー（誰も読まないキー54・部品のキー4）は、消さず『残す』とその理由を下に書いた（消すのは読み手が0と確かめてから。キーを書く側のテストが検証する値・根拠の記録・公開前の定義v2の入力なので、消すと後で作り直せない）

**件数**: 出力 8（ファイル 7・人が見るもの 1）／読み手0件 0／テストだけ 0／1件 1／2件以上 6／規則の二重 0（検出式 0種のうち。common直下のファイルを含むもの 0）

**誰も読まないキー**: **40**（この段階のJSONのキーで今の書き手のコードにある 166 のうち。下の「誰も読まないキー」。今の書き手のコードに無いキー 0 は数えない）

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 5件＋ファイル名で当たった行 1件／解決できた読み込み 168件＋ファイル名で当たった行 13件（3.2%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

フォルダを決めきれない行（ファイル名で当たった行）:
- `src/value/tanuki_valuation/maturity_config.py` 行 119（`config/sector_master.json`）: `path = os.path.join(_find_config_dir(), "sector_master.json")`

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `docs/common/stage_v2/{T}.json`<br>（84ファイル） | HypeCore の stage の定義v2の並行計算（stage_v2.py。公開前の並行運用。読み手は multiples の部品側の1件で、切り替えは STAGE-V2-ROLLOUT-1） | `stage_v2.py:build` | 1 | — | — | — | 残す（2026-11-27） |
| 2件以上 | `config/monitor_tickers.yaml`<br>（1ファイル） | 監視銘柄の一覧（人の入力。register_ticker.py の登録で足す）。市場データの取得・SEC・EPS ANALYZER・System Health が読む | `registration/register_ticker.py:step7_monitor_register` | 5・テスト5 | value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由） | — | — | 残す（2026-11-27） |
| 2件以上 | `config/sector_master.json`<br>（1ファイル） | 業種のマスター（銘柄ごとの業種・区分と、業種の名前の規則の写し）。書き手は sector_master.py（yfinance の sector・industry を主、SEC の SIC を控え）。読み手は tickers の区分を使う各所（TANUKI・EPS ANALYZER・STONKS SILO・定義v2・収益の段階・System Health）。rules の写しは人が読む控えで、正は sector_master.py の定数 | `sector_master.py:write` | 18・テスト6 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION） | AS-IS-059 | 残す（2026-11-27） |
| 2件以上 | `config/split_price_ledger.json`<br>（1ファイル） | 株価の系列の分割の調整と split_history.yaml の比の食い違い（HON型）の台帳。multiples.py が書き、その分割の日より前の月の倍率を判定不能にする | `multiples.py:build`<br>`multiples.py:build` | 2・テスト1 | — | — | — | 残す（2026-11-27） |
| 2件以上 | `docs/common/multiples/{T}.json`<br>（84ファイル） | 倍率の履歴（月末ごと、2021-01〜）。PER・EV/売上・EV/粗利・5年平均の利益の倍率・P/TBV を根拠つきで持つ（multiples.py）。株数は shares_base.py のとおり。TANUKI の分母・時価総額・STONKS SILO・HypeCore・定義v2が読む | `multiples.py:build` | 6・テスト1 | index.html（経由）<br>portfolio/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・HypeCore（TANUKI のtiming・判定の入力） | — | 残す（2026-11-27） |
| 2件以上 | `docs/common/profitability.json`<br>（1ファイル） | 収益の段階（黒字・黒字化目前・赤字）・黒字化の見込み・構造的赤字・赤字の質の点数（TTM の営業利益から。profitability.py）。定義v2・STONKS SILO・画面が読む。回帰の傾き・点・R² 等は根拠の記録 | `profitability.py:write` | 8・テスト6 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION） | — | 残す（2026-11-27） |
| 2件以上 | `docs/common/sector_class.json`<br>（1ファイル） | 景気循環株・金融・区分なしの区分（銘柄→区分）。sector_master.py が config/sector_master.json から公開用に書き、TANUKI SCORE の画面が読む | `sector_master.py:write_public` | 2・テスト1 | value-monitor/tanuki_score/index.html（直接） | — | — | 残す（2026-11-27） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `Discord（DISCORD_WEB_HOOK）` | System Health の通知（毎日の実行の結果） | `system_health.py:_fetch_runs_since`<br>`system_health.py:_fetch_workflow_created_at` | .github/workflows/System_Health.yml |

## 読み手の内訳

### `docs/common/stage_v2/{T}.json`（1件）
- 起動元: .github/workflows/Market_Data_Weekly_Update.yml・.github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `_description`(1)・`_schema_version`(1)・`months`(1)・`ticker`(1)
- Python・保存済みファイル: `common/multiples.py` 行 419・426・437・447・462・470（経由 `common/multiples.py::_load`、`common/multiples.py::_write_if_changed`）

### `config/monitor_tickers.yaml`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- Python・保存済みファイル: `common/market_data/fetcher.py` 行 51・1245・1533・1548・1617・1630（経由 `common/market_data/fetcher.py::get_monitor_tickers`）
- Python・保存済みファイル: `common/registration/register_ticker.py` 行 101
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 53・73・449・653・667・669（経由 `common/sec_data/registration_validator.py::_load_monitor_tickers`）
- Python・保存済みファイル: `common/system_health.py` 行 785・1360・1448（経由 `common/sec_data/registration_validator.py::_load_monitor_tickers`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 970・1081（経由 `src/value/adjusted_eps_analyzer/pipeline.py::run`）
- テスト・保存済みファイル: `tests/test_eps_run_status.py` 行 95・115・125・133・151・160・174（経由 `src/value/adjusted_eps_analyzer/pipeline.py::run`）
- テスト・保存済みファイル: `tests/test_market_data_fetcher.py` 行 245・267・280・290・319・335・350・361・371・393・401・422 ほか17（経由 `common/market_data/fetcher.py::get_monitor_tickers`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 505（経由 `common/sec_data/registration_validator.py::_load_monitor_tickers`）
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/sec_data/registration_validator.py::_load_monitor_tickers`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 298・306・438・451（経由 `common/sec_data/registration_validator.py::_load_monitor_tickers`）
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/tanuki_score/index.html`

### `config/sector_master.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Weekly_Update.yml・.github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `_description`(3)・`_schema_version`(3)・`changes`(2)・`generated_at`(4)・`tickers`(15)
- Python・保存済みファイル: `common/external_watch.py` 行 54（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `common/profitability.py` 行 596・657・670（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `common/sec_data/reader.py` 行 838（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2774（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `common/sector_master.py` 行 34・233・247・257・280・341・350・363（経由 `common/sector_master.py::_load_json`、`common/sector_master.py::load`）
- Python・保存済みファイル: `common/stage_v2.py` 行 335・355・361（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `common/stonks_target.py` 行 85・104（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `common/system_health.py` 行 1091（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 663（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/sector_classifier_v2.py` 行 44（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `src/value/stonks_silo/analyzer.py` 行 468（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `src/value/tanuki_valuation/calculator/adjustments.py` 行 483（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `src/value/tanuki_valuation/calculator/fcfe.py` 行 54・56（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `src/value/tanuki_valuation/calculator/rice.py` 行 368（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 944（経由 `common/sector_master.py::load`）
- Python・保存済みファイル: `src/value/tanuki_valuation/growth_sanity.py` 行 180・198・209・214・288・472（経由 `src/value/tanuki_valuation/growth_sanity.py::_load_sector_tables`）
- Python・保存済みファイル: `src/value/tanuki_valuation/maturity_config.py` 行 213・216・249（経由 `src/value/tanuki_valuation/growth_sanity.py::_load_sector_tables`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 1619・2501・2871・3156・3305（経由 `common/sector_master.py::load`）
- テスト・保存済みファイル: `tests/test_deficit_quality_adopt.py` 行 163（経由 `common/sector_master.py::load`）
- テスト・保存済みファイル: `tests/test_eps_sector_classifier_master.py` 行 32・33・34・35・36・37・43・45・54（経由 `common/sector_master.py::load`）
- テスト・保存済みファイル: `tests/test_financial_judgment_master.py` 行 27・28・29・34・35・36・37・41・46・47・48（経由 `common/sector_master.py::load`）
- テスト・保存済みファイル: `tests/test_profitability.py` 行 157・159・161（経由 `common/sector_master.py::load`）
- テスト・保存済みファイル: `tests/test_sector_exceptions.py` 行 24・37・38・39・40・50・71・107（経由 `src/value/tanuki_valuation/growth_sanity.py::_load_sector_tables`）
- テスト・保存済みファイル: `tests/test_sector_master.py` 行 49・68・69・70・76・82・85・86・89・90・96・106 ほか3（経由 `common/sector_master.py::load`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（common/sector_master.py → docs/common/sector_class.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `config/split_price_ledger.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Weekly_Update.yml・.github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `_description`(2)・`entries`(3)
- Python・保存済みファイル: `common/multiples.py` 行 44・172・447・463・470（経由 `common/multiples.py::_load`、`common/multiples.py::load_ledger`）
- Python・保存済みファイル: `common/stage_v2.py` 行 344・355・361（経由 `common/multiples.py::_write_if_changed`）
- テスト・保存済みファイル: `tests/test_multiples.py` 行 145（経由 `common/multiples.py::load_ledger`）

### `docs/common/multiples/{T}.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Weekly_Update.yml・.github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `_description`(2)・`_schema_version`(2)・`current`(1)・`months`(2)・`ticker`(4)
- Python・保存済みファイル: `common/multiples.py` 行 315・358・447（経由 `common/multiples.py::_load`、`common/multiples.py::load_part`）
- Python・保存済みファイル: `common/stage_v2.py` 行 337・344・355・361（経由 `common/multiples.py::_write_if_changed`、`common/stage_v2.py::_load`、`common/stage_v2.py::build`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 245・548（経由 `common/multiples.py::load_part`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 115（経由 `common/multiples.py::load_part`）
- Python・保存済みファイル: `src/value/stonks_silo/valuation_fetcher.py` 行 99（経由 `common/multiples.py::load_part`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 101・928（経由 `common/multiples.py::load_part`）
- テスト・保存済みファイル: `tests/test_valuation_fetcher_market_data_switch.py` 行 64・85・101・117・128・141・155（経由 `common/multiples.py::load_part`）
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/common/profitability.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Weekly_Update.yml・.github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `_description`(1)・`_schema_version`(1)・`generated_at`(2)・`rules`(1)・`tickers`(8)
- Python・保存済みファイル: `common/profitability.py` 行 47・545・598・625・657・670（経由 `common/profitability.py::_load`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 329・478（経由 `common/stonks_target.py::_load_profitability`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1858（経由 `common/stonks_target.py::_load_profitability`）
- Python・保存済みファイル: `common/sec_data/tickers.py` 行 84（経由 `common/stonks_target.py::_load_profitability`）
- Python・保存済みファイル: `common/stonks_target.py` 行 28・63・75・95・103（経由 `common/stonks_target.py::_load_profitability`）
- Python・保存済みファイル: `src/value/stonks_silo/analyzer.py` 行 187・576・671・758（経由 `common/profitability.py::load_entry`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 122（経由 `common/profitability.py::load_entry`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 460・1362・1414・2969・2981・3981（経由 `common/profitability.py::load_entry`）
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `common/profitability.py::load_entry`）
- テスト・保存済みファイル: `tests/test_deficit_quality_adopt.py` 行 102・103・104・105・129・130・148・150・159・163・164（経由 `common/stonks_target.py::_load_profitability`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 211・233・253・266・285・289・294・299・304・309・642・662 ほか22（経由 `common/profitability.py::load_entry`）
- テスト・保存済みファイル: `tests/test_profitability.py` 行 157・159・161（経由 `common/profitability.py::_load`）
- テスト・保存済みファイル: `tests/test_stonks_silo_breakeven_unify.py` 行 185・194（経由 `common/profitability.py::load_entry`）
- テスト・保存済みファイル: `tests/test_stonks_silo_path_score.py` 行 117・125（経由 `common/profitability.py::load_entry`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/common/sector_class.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Weekly_Update.yml・.github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `_description`(1)・`tickers`(3)
- Python・保存済みファイル: `common/sector_master.py` 行 36
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 243
- テスト・保存済みファイル: `tests/test_sector_master.py` 行 153

## 手で動かすスクリプトの出力（出力先が引数）

- `common/screening/dcf_validity_checker.py:main`（行 432）: 手で動かす検査（--output で指定したJSON。省略すると標準出力だけ）。ワークフローは動かさない。check_c_data_jump() は report_consistency_check.py（CHECK-21）が import する
- `common/screening/report_txt_parser.py:main`（行 295）: 手で動かす報告書のパーサー（--output で指定したJSON。省略すると標準出力だけ）。ワークフローは動かさない

## 共通の部品が作る値（ファイルを書かない部品）

部品の関数の返り値が別の出力（TANUKI の latest.json 等）のキーに入る。出力のファイルは書き手のほうの段階の一覧にある。ここでは部品ごとに、呼ぶ所（import しているファイル）と、キーを読む所（出る先の名前の語とキーの名前の両方が出てくる本番のファイル。推定）を grep で出す。読み手が0のキーは「誰も読まない」で、消すか残すかを下の判断に書く（消すのは0と確かめてから）

### `common/earnings_pending.py`
- 決算の発表後でLayer3に未反映の印（表示だけ）
- 出る先: TANUKI の latest.json の `earnings_pending`（pipeline.py が build の返り値を入れる）
- 呼ぶ所: `src/value/tanuki_valuation/pipeline.py`

| キー | 画面 | コード | 判断 |
|---|---|---|---|
| `status` | `portfolio/tail/detail.html`<br>`value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `common/market_data/fetcher.py`<br>`src/value/tanuki_valuation/pipeline.py` | — |
| `earnings_date` | — | `common/market_data/fetcher.py`<br>`src/value/tanuki_valuation/pipeline.py` | — |
| `earnings_source`（**読み手なし**） | — | — | 残す（2026-11-27）: 予定日の出どころ（Yahoo／8-K Item 2.02）。部品のテストが検証する。8-Kを使い始めたら確かめに要る。画面・コードの読み手は無い |
| `layer3_period_end` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | — | — |
| `announced_period_end_est`（**読み手なし**） | — | — | 残す（確認用）: 発表された期の期末の推定（status・label の根拠）。印が付いた・外れた理由を、決算の後の確認手順で、人（Claude Code）が latest.json を開いて確かめる |
| `label` | `portfolio/tail/detail.html`<br>`value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |
| `note` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `common/market_data/fetcher.py`<br>`src/value/tanuki_valuation/pipeline.py` | — |

### `common/guidance_gap.py`
- TANUKIの成長率とアナリストの売上予想の伸びの差・EPS予想の改定（表示だけ）
- 出る先: TANUKI の latest.json の `guidance_gap`
- 呼ぶ所: `src/value/tanuki_valuation/pipeline.py`

| キー | 画面 | コード | 判断 |
|---|---|---|---|
| `status` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |
| `growth_gap_pt` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | — | — |
| `tanuki_growth` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | — | — |
| `analyst_revenue_growth` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |
| `analyst_count` | `value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |
| `revisions_up_30d` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | — | — |
| `revisions_down_30d` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | — | — |
| `reason` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |
| `estimates_fetched_at`（**読み手なし**） | — | — | 残す（2026-11-27）: 予想を取った日（古さの確かめ用）。画面・コードの読み手は無い |

### `common/reverse_growth.py`
- 市場が織り込んだ成長率の逆算（表示だけ）
- 出る先: TANUKI の latest.json の `upside_context`（context）と `valuation.reverse_growth`（solve）
- 呼ぶ所: `src/value/tanuki_valuation/core_calculator.py`、`src/value/tanuki_valuation/pipeline.py`

| キー | 画面 | コード | 判断 |
|---|---|---|---|
| `upside_percent_beta` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |
| `reverse_g` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | — | — |
| `tanuki_growth` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | — | — |
| `analyst_revenue_growth` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |
| `gap_tanuki_pt` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | — | — |
| `gap_analyst_pt` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | — | — |
| `status` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |
| `reason` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |

### `common/handset_growth.py`
- 手書きの成長率の設定の古さの印（表示だけ）
- 出る先: TANUKI の latest.json の `handset_growth_age`
- 呼ぶ所: `src/value/tanuki_valuation/pipeline.py`

| キー | 画面 | コード | 判断 |
|---|---|---|---|
| `status` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |
| `updated`（**読み手なし**） | — | — | 残す（2026-11-27）: 設定の最後の更新日。label の元で、部品のテストが検証する。画面・コードの読み手は無い |
| `age_days`（**読み手なし**） | — | — | 残す（2026-11-27）: 経過日数（STALE_DAYS超で印）。label の元で、部品のテストが検証する。画面・コードの読み手は無い |
| `label` | `value-monitor/tanuki_score/index.html`<br>`value-monitor/tanuki_valuation/stock.html` | `src/value/tanuki_valuation/pipeline.py` | — |

### `common/shares_base.py`
- 理論株価の分母・時価総額の株数（加重平均の希薄化後株数＋発行の分）と、その基準・表紙の株数との差
- 出る先: docs/common/multiples/{T}.json の `current` の行（multiples.py 経由。TANUKI・STONKS SILO・HypeCore が同じ値を読む）
- 呼ぶ所: `common/dilution.py`、`common/multiples.py`

| キー | 画面 | コード | 判断 |
|---|---|---|---|
| `shares_wd` | — | `common/multiples.py`<br>`src/value/tanuki_valuation/data_fetcher.py`<br>`src/value/tanuki_valuation/pipeline.py` | — |
| `shares_basis` | — | `common/multiples.py`<br>`src/value/tanuki_valuation/data_fetcher.py`<br>`src/value/tanuki_valuation/pipeline.py` | — |
| `shares_issued` | — | `common/multiples.py`<br>`src/value/tanuki_valuation/data_fetcher.py`<br>`src/value/tanuki_valuation/pipeline.py` | — |
| `shares_note` | — | `common/multiples.py`<br>`src/value/tanuki_valuation/data_fetcher.py`<br>`src/value/tanuki_valuation/pipeline.py` | — |
| `cover_shares` | — | `common/multiples.py`<br>`src/value/tanuki_valuation/data_fetcher.py` | — |
| `cover_date` | — | `common/multiples.py`<br>`src/value/tanuki_valuation/data_fetcher.py` | — |
| `outstanding_gap_pct` | — | `common/multiples.py`<br>`src/value/tanuki_valuation/data_fetcher.py`<br>`src/value/tanuki_valuation/pipeline.py` | — |

### `common/dilution.py`
- 株数の変化（約3年前との年率）。分割は split_history.yaml で調整
- 出る先: TANUKI の latest.json の `share_count_change`（DILUTION。株数の年率の変化）
- 呼ぶ所: `src/value/tanuki_valuation/pipeline.py`

| キー | 画面 | コード | 判断 |
|---|---|---|---|
| `cagr_pct` | — | `src/value/tanuki_valuation/pipeline.py` | — |
| `shares_now` | — | `src/value/tanuki_valuation/pipeline.py` | — |
| `shares_start` | — | `src/value/tanuki_valuation/pipeline.py` | — |
| `split_suspect` | — | `src/value/tanuki_valuation/pipeline.py` | — |
| `basis` | — | `src/value/tanuki_valuation/pipeline.py` | — |

### `common/value_freshness.py`
- 値の日付と鮮度の判定（Market Pulse の data_quality・画面・System Health・MACRO PULSE の05_liquidity.csv の asof）。ファイルを書かない
- 呼ぶ所: `common/market_data/daily_guard.py`、`common/system_health.py`、`src/market/market_pulse/stage_conclusions.py`

### `common/step_rules.py`
- 段・閾値の切り替わり（補間・境目の幅）の部品。Market Pulse・MACRO PULSE が呼ぶ。ファイルを書かない
- 呼ぶ所: `src/market/macro_pulse/05_main.py`、`src/market/market_pulse/collect_and_send.py`、`src/market/market_pulse/sector_rotation.py`

### `common/market_pulse_data.py`
- market_data.json の最新のエントリだけ（latest.json）を読む部品。latest.json は Market Pulse の段階の一覧に出力として入っている（書き手 collect_and_send.py）
- 呼ぶ所: `browser_checks/check_dependency_map.py`、`scripts/migrate_market_data_latest.py`、`src/market/market_pulse/collect_and_send.py`、`src/portfolio/snapshot.py`、`src/value/tanuki_valuation/pipeline.py`

### `common/fetch_failure.py`
- 取得の失敗の重大度の表（System Health の [L][M][O]）と、失敗が続いた日数（failed_since）の記録。ファイルは書かない（failed_since は各書き手が自分のファイルに残す）
- 呼ぶ所: `common/macro_data/fetcher.py`、`common/sec_data/freshness.py`、`common/system_health.py`

### `common/external_watch.py`
- 外の変化・ばらつきの検知を集める（System Health の [P]）。台帳 config/external_change_acknowledged.json は人の入力
- 呼ぶ所: `common/system_health.py`

### `common/external_api.py`
- 外部API（Grok・SEC・FRED）の呼び出し口（2026-10-24、EXTERNAL-API-CLIENT-UNIFY-1）。ファイルは書かない
- 呼ぶ所: `common/macro_data/fetcher.py`、`common/market_data/calendar_universe.py`、`common/registration/preflight_check.py`、`common/sec_data/cf_lines.py`、`common/sec_data/dimension_aggregate_fetcher.py`、`common/sec_data/fetcher.py`、`src/market/macro_pulse/05_main.py`、`src/market/market_pulse/collect_and_send.py`、`src/market/market_pulse/market_calendar.py`、`src/market/market_pulse/news_headlines.py`、`src/subport/fg_level2_backtest.py`、`src/tail/edgar_rss_monitor.py`、`src/tail/kpi_proposer.py`、`src/tail/quarterly_review_generator.py`、`src/tail/sec_ctrl_fetcher.py`、`src/tail/text_kpi_extractor.py`、`src/tail/xbrl_segment_fetcher.py`、`src/value/adjusted_eps_analyzer/ai_analyzer.py`、`src/value/adjusted_eps_analyzer/company_metadata.py`、`src/value/adjusted_eps_analyzer/extract_key_facts.py`、`src/value/adjusted_eps_analyzer/pipeline.py`、`src/value/tanuki_valuation/calculator/fcf_outlier_ai.py`、`src/value/tanuki_valuation/data_fetcher.py`、`src/value/tanuki_valuation/validator.py`、`tools/sim/sim.py`、`.github/workflows/Adjusted_Eps_Analyzer_update.yml`、`.github/workflows/SEC_Data_Update.yml`、`.github/workflows/TANUKI_TAIL_KPI_Update.yml`、`.github/workflows/TANUKI_TAIL_RSS_Monitor.yml`、`.github/workflows/TANUKI_TAIL_SEC_Ctrl.yml`、`.github/workflows/TANUKI_TAIL_SEC_Items.yml`、`.github/workflows/TANUKI_VALUATION_Update.yml`

### `common/reference_check.py`
- 文書・ワークフローの名前の参照の検査（rcc CHECK-68）。ファイルは書かない
- 呼ぶ所: `common/sec_data/report_consistency_check.py`

### `common/stonks_target.py`
- STONKS SILO の対象の銘柄を決める部品。ファイルは書かない
- 呼ぶ所: `common/sec_data/registration_validator.py`、`common/sec_data/report_consistency_check.py`、`common/sec_data/tickers.py`

### `common/yfinance_utils.py`
- yfinance の呼び出しの共通の部品。ファイルは書かない
- 呼ぶ所: `common/market_data/fetcher.py`、`common/sec_data/report_consistency_check.py`

### `common/system_health.py`
- System Health の判定と Discord への通知（Discord は一覧の出力に入っている）。ファイルは書かない
- 呼ぶ所: `.github/workflows/System_Health.yml`

### `common/method_change.py`
- 計算方法の変更の印（method_change.json の scope と各出力の method_change_applied）。ファイルは config/method_change.json（人の入力）
- 呼ぶ所: `common/sec_data/report_consistency_check.py`、`common/sec_data/rotce.py`、`scripts/method_change_scope.py`、`src/value/hypecore/hypecore.py`、`src/value/stonks_silo/pipeline.py`、`src/value/tanuki_valuation/pipeline.py`、`src/value/tanuki_valuation/score_watcher.py`

## 誰も読まないキー

この段階の出力のJSON（全ファイル、入れ子8段まで）のキーのうち、テストを除くコード（.py・.js・.html。書き手のファイルを含む）に読みの形（`.key`・`['key']`〈代入でない〉・`.get('key')`）で出てこないもの。銘柄名・日付など小文字を含まないキーは除く。Pythonで`for k in d`・`d.items()`のように回して読むものは拾えないので、消す前にキーの名前でgrepする

- `basis_ends`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `breakeven_method`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `breakeven_min_r2`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `breakeven_points`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `breakeven_start`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `current_definition_label`（`docs/common/stage_v2/{T}.json`）: これから作る: [[STAGE-V2-ROLLOUT-1]]（定義v2の並行計算の根拠。切り替えで読み手を作る）
- `current_definition_stage`（`docs/common/stage_v2/{T}.json`）: これから作る: [[STAGE-V2-ROLLOUT-1]]（定義v2の並行計算の根拠。切り替えで読み手を作る）
- `daily_close`（`config/split_price_ledger.json`）: 残す（監査用）: 分割の調整の台帳の根拠。分割の調整を確かめるとき、人（Claude Code）が台帳（config/split_price_ledger.json）と突き合わせて読む（price_factor は multiples.py が台帳から読む〈動的〉）
- `daily_date`（`config/split_price_ledger.json`）: 残す（監査用）: 分割の調整の台帳の根拠。分割の調整を確かめるとき、人（Claude Code）が台帳（config/split_price_ledger.json）と突き合わせて読む（price_factor は multiples.py が台帳から読む〈動的〉）
- `etf`（`config/sector_master.json`）: 残す（写し）: 業種→ETF の写し。tests/test_sector_master.py が定数との一致を検査する（Market Pulse の sector_rotation.py は自分の SECTOR_MAP を持つ）
- `ev_gross_profit`（`docs/common/multiples/{T}.json`）: 残す（動的な読み）: 定義v2（common/stage_v2.py）が倍率の種類を名前で選んで読む（一覧の読みの形では拾えない）
- `expected_factor`（`config/split_price_ledger.json`）: 残す（監査用）: 分割の調整の台帳の根拠。分割の調整を確かめるとき、人（Claude Code）が台帳（config/split_price_ledger.json）と突き合わせて読む（price_factor は multiples.py が台帳から読む〈動的〉）
- `fair_value_change`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測から除いた分の種類別の内訳（by_kind のキー名）。画面・report.txt は exclusions（文）を読む。除外の額が合わないとき、人（Claude Code）が種類別の額をこのファイルで確かめる
- `fcf_points`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `fiscal_year_end`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `forward_pe_snapshot`（`docs/common/multiples/{T}.json`）: 残す（記録）: 作り直せない。その月末に取った先行PERの値と日付（Yahooは過去の値を返さない）
- `forward_pe_snapshot_date`（`docs/common/multiples/{T}.json`）: 残す（記録）: 作り直せない。その月末に取った先行PERの値と日付（Yahooは過去の値を返さない）
- `g_f`（`docs/common/stage_v2/{T}.json`）: これから作る: [[STAGE-V2-ROLLOUT-1]]（定義v2の並行計算の根拠。切り替えで読み手を作る）
- `g_m`（`docs/common/stage_v2/{T}.json`）: これから作る: [[STAGE-V2-ROLLOUT-1]]（定義v2の並行計算の根拠。切り替えで読み手を作る）
- `impairment`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測から除いた分の種類別の内訳（by_kind のキー名）。画面・report.txt は exclusions（文）を読む。除外の額が合わないとき、人（Claude Code）が種類別の額をこのファイルで確かめる
- `l_industry`（`docs/common/stage_v2/{T}.json`）: これから作る: [[STAGE-V2-ROLLOUT-1]]（定義v2の並行計算の根拠。切り替えで読み手を作る）
- `l_own`（`docs/common/stage_v2/{T}.json`）: これから作る: [[STAGE-V2-ROLLOUT-1]]（定義v2の並行計算の根拠。切り替えで読み手を作る）
- `margins`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `multiple_type`（`docs/common/stage_v2/{T}.json`）: これから作る: [[STAGE-V2-ROLLOUT-1]]（定義v2の並行計算の根拠。切り替えで読み手を作る）
- `near_horizon_quarters`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `net_income_5y_years`（`docs/common/multiples/{T}.json`）: 残す（監査用）: 倍率の分母・株数の時点の根拠。倍率の履歴の点が疑わしいとき、人（Claude Code）が common/multiples の月末の行で確かめる
- `observed_factor`（`config/split_price_ledger.json`）: 残す（監査用）: 分割の調整の台帳の根拠。分割の調整を確かめるとき、人（Claude Code）が台帳（config/split_price_ledger.json）と突き合わせて読む（price_factor は multiples.py が台帳から読む〈動的〉）
- `p_tbv`（`docs/common/multiples/{T}.json`）: 残す（動的な読み）: 定義v2（common/stage_v2.py）が倍率の種類を名前で選んで読む（一覧の読みの形では拾えない）
- `pe_5y_avg_earnings`（`docs/common/multiples/{T}.json`）: 残す（動的な読み）: 定義v2（common/stage_v2.py）が倍率の種類を名前で選んで読む（一覧の読みの形では拾えない）
- `previous_cover_date`（`docs/common/multiples/{T}.json`）: 残す（監査用）: 倍率の分母・株数の時点の根拠。倍率の履歴の点が疑わしいとき、人（Claude Code）が common/multiples の月末の行で確かめる
- `previous_cover_shares`（`docs/common/multiples/{T}.json`）: 残す（監査用）: 倍率の分母・株数の時点の根拠。倍率の履歴の点が疑わしいとき、人（Claude Code）が common/multiples の月末の行で確かめる
- `price_factor`（`config/split_price_ledger.json`）: 残す（監査用）: 分割の調整の台帳の根拠。分割の調整を確かめるとき、人（Claude Code）が台帳（config/split_price_ledger.json）と突き合わせて読む（price_factor は multiples.py が台帳から読む〈動的〉）
- `price_factor_reason`（`config/split_price_ledger.json`）: 残す（監査用）: 分割の調整の台帳の根拠。分割の調整を確かめるとき、人（Claude Code）が台帳（config/split_price_ledger.json）と突き合わせて読む（price_factor は multiples.py が台帳から読む〈動的〉）
- `recorded_price`（`config/split_price_ledger.json`）: 残す（監査用）: 分割の調整の台帳の根拠。分割の調整を確かめるとき、人（Claude Code）が台帳（config/split_price_ledger.json）と突き合わせて読む（price_factor は multiples.py が台帳から読む〈動的〉）
- `rules`（`docs/common/profitability.json`）: 残す（監査用）: この JSON を作った規則の記録（回帰の方法・点の数・R²の下限・時点の基準・足し戻しの規則）。黒字化の予測の規則を変えた前後の比較のとき、人（Claude Code）が profitability.json で確かめる
- `shares_end`（`docs/common/multiples/{T}.json`）: 残す（監査用）: 倍率の分母・株数の時点の根拠。倍率の履歴の点が疑わしいとき、人（Claude Code）が common/multiples の月末の行で確かめる
- `slope_per_quarter`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `stage_reason`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む
- `tce_end`（`docs/common/multiples/{T}.json`）: 残す（監査用）: 倍率の分母・株数の時点の根拠。倍率の履歴の点が疑わしいとき、人（Claude Code）が common/multiples の月末の行で確かめる
- `time_basis`（`docs/common/profitability.json`）: 残す（監査用）: 黒字化の予測の根拠（回帰の傾き・点・R²・基準の四半期）。予測年が動いた理由を、人（Claude Code）が profitability.json の根拠で確かめる（SOUN の 2026→2030 など）。画面は段階と年数だけ読む

### 今の書き手のコードに無いキー（古いファイル・古いエントリに残るだけ。誰も読まないキーに数えない）

データのキーのうち、書き手のコード（書き手の .py・それを import する呼び出し元・それらが import する .py）に文字列として出てこないもの。書くのをやめて履歴のスナップショット・古いエントリに残るキーや、取得元のデータをそのまま写したキー（SEC の単位・次元の名前等）

なし

## ワークフローが自分で書く出力（.pyの外）

段階の.pyを動かすワークフローの中で、`$GITHUB_STEP_SUMMARY`に書く段。`.get()`で読むキーが、この段階のJSON（全ファイル）にあるか。

- `.github/workflows/Market_Data_Weekly_Update.yml`（行 113・114・117・118）: 
- `.github/workflows/SEC_Data_Update.yml`（行 133・134・135・136・142）: 
- `.github/workflows/TANUKI_VALUATION_Update.yml`（行 104・105・106・107・112）: `intrinsic_value_per_share`（**無い**）・`overall`（**無い**）・`validation`（**無い**）

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | common直下か | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1074`<br>`src/value/hypecore/hypecore.py:1119` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:21` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。common直下のファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | common直下を含む |
|---|---|---|---|

## 本番外のコード（common/screening）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

### `common/screening`（1ファイル・305行）

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 残す／消す／移す |
|---|---|---|---|---|---|---|
| `report_txt_parser.py` | 305 | — | — | — | `tests/test_report_txt_parser.py` | 残す（2026-11-27） |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 5件
  - `common/method_change.py:203` previous_output → `docs/common/sec_data/rotce/<?>`
  - `common/method_change.py:203` previous_output → `docs/value-monitor/tanuki_valuation/data/<?>`
  - `common/method_change.py:231` read_json → `<?>`
  - `common/reference_check.py:215` workflow_name_mismatch → `.github/workflows/<?>`
  - `common/screening/dcf_validity_checker.py:66` _load_json → `<?>`
