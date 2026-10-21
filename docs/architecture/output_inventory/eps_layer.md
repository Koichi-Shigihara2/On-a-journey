# EPS（adjusted_eps_analyzer）の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`eps_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`・`docs/value-monitor/adjusted_eps_analyzer/`・`docs/value-monitor/tanuki_valuation/`があれば「効く」
- 残す／消す: 段階2でKoichiさんが決める（今は空）

**件数**: 出力 5（ファイル 4・人が見るもの 1）／読み手0件 0／テストだけ 0／1件 1／2件以上 3／規則の二重 9（検出式 10種のうち。EPSのファイルを含むもの 3）

**画面の項目**: 画面が読むキー 112（この段階の書き手あり 65・ほかの書き手あり 41・画面の中で作る値 2・**書き手なし 4**）／書き手があるのに誰も読まないキー **14**

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 1件＋ファイル名で当たった行 0件／解決できた読み込み 11件＋ファイル名で当たった行 20件（3.1%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json`<br>（95ファイル） | 銘柄ごとの直近4四半期の合計（TTM）の純利益・調整後純利益・EPSの時系列。EPSの個別ページが読む | `pipeline.py:process_one_ticker` | 1 | value-monitor/adjusted_eps_analyzer/stock.html（直接） | 効く: EPSの画面 | — |  |
| 2件以上 | `docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>（1ファイル） | 全銘柄の一覧（銘柄ごとの最新期のGAAP EPS・調整後EPS・差・前年比・健全性）と実行の結果（run_status）。EPSの画面の一覧・TANUKI VALUATIONの調整後EPSが読む | `pipeline.py:run` | 9・テスト8 | common/ticker-links.js（直接）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（直接）<br>value-monitor/adjusted_eps_analyzer/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/stock.html（直接）<br>value-monitor/adjusted_eps_analyzer/tickers.html（直接）<br>value-monitor/admin.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・EPSの画面 | — |  |
| 2件以上 | `docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>（95ファイル） | 銘柄ごとの年次の調整後EPS（年ごとの調整項目）。TANUKI VALUATIONのFCFの外れ値の判定が、一過性の費用の突合に読む（calculator/adjustments.py） | `pipeline.py:process_one_ticker` | 3・テスト4 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/admin.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION） | AS-IS-020・AS-IS-021 |  |
| 2件以上 | `docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`<br>（95ファイル） | 銘柄ごとの四半期の調整後EPS（調整項目・税の調整・AIの分析・特記）と経営者実行力の指標（execution_metrics）。EPSの個別ページ・TANUKI VALUATIONの調整後PERが読む | `pipeline.py:process_one_ticker` | 8・テスト12 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（直接）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（直接）<br>value-monitor/admin.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（直接）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・EPSの画面 | AS-IS-181・AS-IS-265 |  |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `${GITHUB_STEP_SUMMARY}` | GitHub Actionsの実行の画面に出す要約（成功・失敗の件数と失敗した銘柄、人が見る） | `pipeline.py:_write_step_summary` | .github/workflows/Adjusted_Eps_Analyzer_update.yml |

## 読み手の内訳

### `docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json`（1件）
- 起動元: .github/workflows/Adjusted_Eps_Analyzer_update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `last_updated`(1)・`ticker`(1)・`ttm`(1)
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 633
- テスト・その場で計算: `tests/test_split_history_adjustments.py` 行 41・52（経由 `src/value/adjusted_eps_analyzer/pipeline.py::load_split_history`）

### `docs/value-monitor/adjusted_eps_analyzer/data/summary.json`（2件以上）
- 起動元: .github/workflows/Adjusted_Eps_Analyzer_update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `last_updated`(6)・`run_status`(3)・`tickers`(12)
- Python・保存済みファイル: `common/system_health.py` 行 204・350・404・1046・1110（経由 `common/system_health.py::_check_i`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 507・901・905
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 490・585・590・810・991・1069・1144・1323・1530・2952（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- JS・HTML・保存済みファイル: `docs/common/ticker-links.js` 行 31
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/admin.html` 行 358
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/index.html` 行 240
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 595
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/tickers.html` 行 244
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 238
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- テスト・保存済みファイル: `tests/test_eps_run_status.py` 行 191・206・220・232・242・248・255・264（経由 `common/system_health.py::_check_i`、`src/value/adjusted_eps_analyzer/pipeline.py::generate_summary`）
- テスト・保存済みファイル: `tests/test_eps_upc_prereorg_filter.py` 行 177（経由 `src/value/adjusted_eps_analyzer/pipeline.py::generate_summary`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 211・233・253・266・285・289・294・299・304・309・332・348 ほか64（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::_check_i`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::_check_i`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 181・193・194・216・220（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`（2件以上）
- 起動元: .github/workflows/Adjusted_Eps_Analyzer_update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `last_updated`(0)・`ticker`(4)・`years`(5)
- Python・保存済みファイル: `src/value/tanuki_valuation/calculator/adjustments.py` 行 972・976・1073（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 246・1023（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 454・1047・1115（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- テスト・その場で計算: `tests/test_split_history_adjustments.py` 行 41・52（経由 `src/value/adjusted_eps_analyzer/pipeline.py::load_split_history`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 3203・3212
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`（2件以上）
- 起動元: .github/workflows/Adjusted_Eps_Analyzer_update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `execution_metrics`(1)・`last_updated`(3)・`quarters`(7)・`ticker`(15)
- Python・保存済みファイル: `common/sec_data/audit.py` 行 126・130・191・369・468（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1339・1387・2449・2453・2952・2984・3018・3455・3657・3701（経由 `common/sec_data/report_consistency_check.py::_read_eps_quarterly`）
- Python・保存済みファイル: `common/system_health.py` 行 86・1038（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 933・984・988・1023（経由 `src/value/tanuki_valuation/core_calculator.py::_calc_adjusted_per`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 454・490・991・1047・1069・1115・1323・1540・1544・2611・2614・3293 ほか2（経由 `src/value/tanuki_valuation/core_calculator.py::_calc_adjusted_per`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/admin.html` 行 372
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 617
- JS・HTML・保存済みファイル: `docs/value-monitor/stonks-silo/index.html` 行 654
- テスト・その場で計算: `tests/test_split_history_adjustments.py` 行 41・52（経由 `src/value/adjusted_eps_analyzer/pipeline.py::load_split_history`）
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 66・71・78（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_read_eps_quarterly`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23・27・28・41（経由 `src/value/tanuki_valuation/core_calculator.py::_calc_adjusted_per`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `src/value/tanuki_valuation/core_calculator.py::_calc_adjusted_per`）
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 211・233・253・266・285・289・294・299・304・309・461・500 ほか48（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_read_eps_quarterly`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 1483（経由 `common/sec_data/report_consistency_check.py::_read_eps_quarterly`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `src/value/tanuki_valuation/core_calculator.py::_calc_adjusted_per`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 181・193・194・216・220（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

## 画面の項目

画面（`docs/value-monitor/adjusted_eps_analyzer/admin.html`・`docs/value-monitor/adjusted_eps_analyzer/index.html`・`docs/value-monitor/adjusted_eps_analyzer/stock.html`・`docs/value-monitor/adjusted_eps_analyzer/tickers.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（全ファイル、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、どのJSONにも無いキー）

- `cik_str`（`docs/value-monitor/adjusted_eps_analyzer/admin.html` 行 344）
- `book_append_sheet`（`docs/value-monitor/adjusted_eps_analyzer/index.html` 行 469）
- `book_new`（`docs/value-monitor/adjusted_eps_analyzer/index.html` 行 467）
- `table_to_sheet`（`docs/value-monitor/adjusted_eps_analyzer/index.html` 行 468）

### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

- `adjusted_income`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json`）
- `adjustment_amount`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `condition1`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `detected_items`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `dta_detected`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `fair_value_auto_detect`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `finished_at`（`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`）
- `maturity_monitor`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `sbc_per_share`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `sector_exclusions`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `sector_threshold`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `share_structure_mismatch`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `split_adjusted`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）
- `xbrl_eps`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）

### `docs/value-monitor/adjusted_eps_analyzer/admin.html`（5項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `cik_str` | 書き手なし | 344 |  |
| — | `eps_sector` | 画面の中で作る値 | 327・549・572 |  |
| 不明 | `last_updated` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 363・377 |  |
| SEC API 応答エラー | `ticker` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 327・343・343・408 |  |
| 取得中... | `value` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 385・453・488・494 |  |

### `docs/value-monitor/adjusted_eps_analyzer/index.html`（19項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `adjusted_eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 395 |  |
| — | `book_append_sheet` | 書き手なし | 469 |  |
| — | `book_new` | 書き手なし | 467 |  |
| — | `company_name` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 328・392 |  |
| 👁 過小評価候補 — Non-GAAP調整で実力が隠れている可能性 | `deviation_rate` | 画面の中で作る値 | 214・255・271・284 |  |
| 過大調整 | `eps_diff` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 370・396・396 |  |
| 差分 | `eps_ratio` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 212・261・261・265 |  |
| — | `gaap_eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 394 |  |
| — | `gaap_to_adj_positive` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 265・320・368 |  |
| 過大調整 | `health` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 369・401・401 |  |
| データが見つかりません | `last_updated` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 244 |  |
| — | `latest_filing_date` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 193・383・383・384 |  |
| — | `latest_fiscal_quarter` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 199 |  |
| — | `table_to_sheet` | 書き手なし | 468 |  |
| — | `tanuki` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 249 |  |
| ⚠️ 過大評価注意 — 調整でGAAPより悪く見せている可能性 | `ticker` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 251・286・287・299 |  |
| データが見つかりません | `tickers` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 243 |  |
| — | `upside_percent` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 254 |  |
| YoY成長 | `yoy_growth` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 213・337・337・387 |  |

### `docs/value-monitor/adjusted_eps_analyzer/stock.html`（87項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `adjusted_eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 1091・1092・1092・1092 |  |
| — | `adjusted_net_income` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1321 |  |
| 項目 | `adjustments` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1319・1438・1438・1443 |  |
| — | `ai_analysis` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1013・1445・1445・1445 |  |
| 調整なし | `comment` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1198 |  |
| — | `company_reported` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 812 |  |
| — | `components` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 960 |  |
| — | `computed` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 820 |  |
| 引用ソース: | `confidence` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1202・1202・1448・1448 |  |
| — | `current` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 762 |  |
| >出典: ${co.source \|\| | `definition` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 835 |  |
| — | `diff` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 821 |  |
| — | `diluted_shares_used` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1322 |  |
| — | `end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 843・867 |  |
| — | `eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json` | 1242 |  |
| — | `eps_diff_pct` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1110 |  |
| — | `eps_discrepancy` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1105 |  |
| — | `excluded_from_percentile` | ほかの書き手あり<br>`docs/common/sec_data/rotce/APP.json`<br>`docs/common/sec_data/rotce/ASTS.json` | 857 |  |
| — | `execution_metrics` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 669 |  |
| — | `extracted_from` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1459 |  |
| — | `filing_date` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 983・1034・1066・1075 |  |
| ・ | `filled_components` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 928 |  |
| — | `filled_from_annual` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 854・927 |  |
| — | `fiscal_year` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1222・1223 |  |
| XBRL計算EPS: | `gaap_eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 1113・1124・1137・1163 |  |
| — | `gaap_net_income` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1318 |  |
| — | `guidance` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 822 |  |
| 調整なし | `health` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1195 |  |
| 長期目標 <span style= | `high` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 829 |  |
| — | `history` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 765 |  |
| — | `item` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1209・1447・1447 |  |
| — | `item_id` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1339・1447 |  |
| — | `item_name` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1339・1447・1456 |  |
| — | `label` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 908 |  |
| （登録済みの最新 ${co.last_registered_quarter_en | `last_registered_quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 825 |  |
| — | `last_updated` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 653 |  |
| >直近の四半期末（）の会社発表の値: 未登録 | `latest_quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 824 |  |
| — | `latest_registered` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 823 |  |
| — | `long_term_target` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 822 |  |
| 長期目標 <span style= | `low` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 829 |  |
| 算出不可 | `missing_components` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ADSK.json` | 771・771・772 |  |
| 。無形資産を0と仮定した期は除外 | `n_excluded_assumed_zero` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 808・808 |  |
| 比較できる四半期が期（8期以上で判定） | `n_quarters` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 801・807 |  |
| — | `net_adjustment_total` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1320 |  |
| — | `net_amount` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1337・1457 |  |
| 純利益（普通株主帰属のタグなし） | `net_income_source` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 787 |  |
| — | `next_earnings_date` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 951 |  |
| ・ | `notes` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 774 |  |
| — | `official_eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1109 |  |
| — | `per` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 961 |  |
| — | `per_adjusted` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 962 |  |
| — | `percentile` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 762 |  |
| ガイダンス（）<span style= | `period` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json` | 828・1022・1239・1514 |  |
| 期末: | `period_end` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 720・720・1226・1226 |  |
| 末と末の平均 | `prev_quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 790 |  |
| 。参考（TTM）（自社比の目安はこちらで比較） | `price` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 794 |  |
| 。参考（TTM）（自社比の目安はこちらで比較） | `price_date` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 794 |  |
| — | `prior_4q_avg_yoy` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 691 |  |
| 。参考（TTM）（自社比の目安はこちらで比較） | `ptbv` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ADBE.json` | 766・769・792・792 |  |
| — | `quarter` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1222・1223 |  |
| （直近の四半期: ） | `quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 777・777・789・790 |  |
| — | `quarters` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 625・626・626・655 |  |
| 算出不可 | `reason` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 687・702・704・717 |  |
| — | `recent_4q_avg_yoy` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 691 |  |
| — | `revenue_growth_acceleration` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 684 |  |
| 直近16四半期分のデータ不足 | `roic_trend` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 698 |  |
| — | `roic_wacc_ratio` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 704 |  |
| 純利益（普通株主帰属のタグなし） | `rotce` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 766・769・782・785 |  |
| 末と末の平均 | `rotce_basis` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 790 |  |
| 。参考（TTM）（自社比の目安はこちらで比較） | `rotce_ttm` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 791・791 |  |
| 直近4年中3年以上で算出不能（営業赤字等） | `sbc_ratio` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 714 |  |
| 。参考（TTM）（自社比の目安はこちらで比較） | `shares_quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ALAB.json` | 794 |  |
| — | `signal` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 797・798 |  |
| — | `snippet` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1210 |  |
| >出典: ${co.source \|\| | `source` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json`<br>`docs/portfolio/tail/data/legal_proceedings/ADBE/latest.json` | 835 |  |
| 引用ソース: | `sources` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1199・1199・1201・1445 |  |
| — | `special_flags` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 627・627・1104・1104 |  |
| — | `special_notes` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1105・1105・1144・1144 |  |
| — | `tanuki` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 641 |  |
| — | `ticker` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 597 |  |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 597 |  |
| — | `ttm` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json` | 1020・1020・1021・1505 |  |
| — | `upc_prereorg_zero_profit` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1144・1424 |  |
| ガイダンス（）<span style= | `value` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 588・609・687・688 |  |
| — | `values` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 817・867 |  |
| （ROIC、WACC=10%基準） | `year` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 707 |  |
| — | `years` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 701 |  |

### `docs/value-monitor/adjusted_eps_analyzer/tickers.html`（1項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `value` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 315 |  |

## 画面の区画

HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後15行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。

### `docs/value-monitor/adjusted_eps_analyzer/admin.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| 新規ティッカー登録 | `cik_str`（書き手なし）<br>`eps_sector`（画面の中で作る値）<br>`ticker`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| EPSセクター手動設定 | `eps_sector`（画面の中で作る値）<br>`ticker`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| （区画を決められない） | `last_updated`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |

### `docs/value-monitor/adjusted_eps_analyzer/index.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| （ページの先頭） | `company_name`（この段階の書き手あり）<br>`deviation_rate`（画面の中で作る値）<br>`eps_ratio`（この段階の書き手あり）<br>`gaap_to_adj_positive`（この段階の書き手あり）<br>`last_updated`（この段階の書き手あり）<br>`tanuki`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`tickers`（この段階の書き手あり）<br>`upside_percent`（ほかの書き手あり）<br>`yoy_growth`（この段階の書き手あり） |  |
| （JSで作る要素 #main-table） | `adjusted_eps`（この段階の書き手あり）<br>`book_append_sheet`（書き手なし）<br>`book_new`（書き手なし）<br>`company_name`（この段階の書き手あり）<br>`deviation_rate`（画面の中で作る値）<br>`eps_diff`（この段階の書き手あり）<br>`eps_ratio`（この段階の書き手あり）<br>`gaap_eps`（この段階の書き手あり）<br>`gaap_to_adj_positive`（この段階の書き手あり）<br>`health`（この段階の書き手あり）<br>`latest_filing_date`（この段階の書き手あり）<br>`latest_fiscal_quarter`（この段階の書き手あり）<br>`table_to_sheet`（書き手なし）<br>`ticker`（この段階の書き手あり）<br>`yoy_growth`（この段階の書き手あり） |  |

### `docs/value-monitor/adjusted_eps_analyzer/stock.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| ヘッダー | `value`（この段階の書き手あり） |  |
| エラー | `execution_metrics`（この段階の書き手あり）<br>`quarters`（この段階の書き手あり）<br>`special_flags`（この段階の書き手あり） |  |
| メインコンテンツ | `last_updated`（この段階の書き手あり）<br>`quarters`（この段階の書き手あり） |  |
| 銘柄ヘッダー | `next_earnings_date`（ほかの書き手あり）<br>`tanuki`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`tickers`（この段階の書き手あり） |  |
| メトリクスカード | `adjusted_eps`（この段階の書き手あり）<br>`eps_diff_pct`（この段階の書き手あり）<br>`eps_discrepancy`（この段階の書き手あり）<br>`filing_date`（この段階の書き手あり）<br>`gaap_eps`（この段階の書き手あり）<br>`official_eps`（この段階の書き手あり）<br>`special_flags`（この段階の書き手あり）<br>`special_notes`（この段階の書き手あり）<br>`upc_prereorg_zero_profit`（この段階の書き手あり） |  |
| 経営者実行力評価（参考指標、[[HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1]]④、2026-09-23） | `period_end`（この段階の書き手あり）<br>`prior_4q_avg_yoy`（この段階の書き手あり）<br>`reason`（この段階の書き手あり）<br>`recent_4q_avg_yoy`（この段階の書き手あり）<br>`revenue_growth_acceleration`（この段階の書き手あり）<br>`roic_trend`（この段階の書き手あり）<br>`roic_wacc_ratio`（この段階の書き手あり）<br>`sbc_ratio`（この段階の書き手あり）<br>`value`（この段階の書き手あり）<br>`year`（この段階の書き手あり）<br>`years`（この段階の書き手あり） |  |
| ROTCE・P/TBV（[[ROTCE-PTBV-1]]、2026-10-10、参考表示） | `current`（ほかの書き手あり）<br>`history`（ほかの書き手あり）<br>`missing_components`（ほかの書き手あり）<br>`net_income_source`（ほかの書き手あり）<br>`notes`（ほかの書き手あり）<br>`percentile`（ほかの書き手あり）<br>`prev_quarter_end`（ほかの書き手あり）<br>`price`（ほかの書き手あり）<br>`price_date`（ほかの書き手あり）<br>`ptbv`（ほかの書き手あり）<br>`quarter_end`（ほかの書き手あり）<br>`reason`（この段階の書き手あり）<br>`rotce`（ほかの書き手あり）<br>`rotce_basis`（ほかの書き手あり）<br>`rotce_ttm`（ほかの書き手あり）<br>`shares_quarter_end`（ほかの書き手あり）<br>`signal`（ほかの書き手あり） |  |
| 会社発表のROTCE（[[ROTCE-QUARTERLY-ANNUALIZED-1]]、config/company_reported_metrics.jsonに登録した銘柄だけ） | `company_reported`（ほかの書き手あり）<br>`computed`（ほかの書き手あり）<br>`definition`（ほかの書き手あり）<br>`diff`（ほかの書き手あり）<br>`end`（ほかの書き手あり）<br>`excluded_from_percentile`（ほかの書き手あり）<br>`filled_components`（ほかの書き手あり）<br>`filled_from_annual`（ほかの書き手あり）<br>`guidance`（ほかの書き手あり）<br>`high`（ほかの書き手あり）<br>`label`（ほかの書き手あり）<br>`last_registered_quarter_end`（ほかの書き手あり）<br>`latest_quarter_end`（ほかの書き手あり）<br>`latest_registered`（ほかの書き手あり）<br>`long_term_target`（ほかの書き手あり）<br>`low`（ほかの書き手あり）<br>`n_excluded_assumed_zero`（ほかの書き手あり）<br>`n_quarters`（ほかの書き手あり）<br>`period`（この段階の書き手あり）<br>`ptbv`（ほかの書き手あり）<br>`quarter_end`（ほかの書き手あり）<br>`reason`（この段階の書き手あり）<br>`rotce`（ほかの書き手あり）<br>`signal`（ほかの書き手あり）<br>`source`（ほかの書き手あり）<br>`value`（この段階の書き手あり）<br>`values`（ほかの書き手あり） |  |
| PER比較（Addition G） | `components`（ほかの書き手あり）<br>`per`（ほかの書き手あり）<br>`per_adjusted`（ほかの書き手あり） |  |
| 期間ピッカー | `ai_analysis`（この段階の書き手あり）<br>`filing_date`（この段階の書き手あり）<br>`period`（この段階の書き手あり）<br>`quarters`（この段階の書き手あり）<br>`ttm`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| チャート | `adjusted_eps`（この段階の書き手あり）<br>`eps`（この段階の書き手あり）<br>`fiscal_year`（この段階の書き手あり）<br>`gaap_eps`（この段階の書き手あり）<br>`period`（この段階の書き手あり）<br>`period_end`（この段階の書き手あり）<br>`quarter`（この段階の書き手あり）<br>`special_flags`（この段階の書き手あり） |  |
| AI分析 | `comment`（この段階の書き手あり）<br>`confidence`（この段階の書き手あり）<br>`health`（この段階の書き手あり）<br>`item`（この段階の書き手あり）<br>`snippet`（この段階の書き手あり）<br>`sources`（この段階の書き手あり） |  |
| ウォーターフォール図 | `adjusted_net_income`（この段階の書き手あり）<br>`adjustments`（この段階の書き手あり）<br>`diluted_shares_used`（この段階の書き手あり）<br>`gaap_net_income`（この段階の書き手あり）<br>`item_id`（この段階の書き手あり）<br>`item_name`（この段階の書き手あり）<br>`net_adjustment_total`（この段階の書き手あり）<br>`net_amount`（この段階の書き手あり） |  |
| 調整内訳 | `adjustments`（この段階の書き手あり）<br>`ai_analysis`（この段階の書き手あり）<br>`confidence`（この段階の書き手あり）<br>`extracted_from`（この段階の書き手あり）<br>`filing_date`（この段階の書き手あり）<br>`item`（この段階の書き手あり）<br>`item_id`（この段階の書き手あり）<br>`item_name`（この段階の書き手あり）<br>`net_amount`（この段階の書き手あり）<br>`period`（この段階の書き手あり）<br>`quarters`（この段階の書き手あり）<br>`reason`（この段階の書き手あり）<br>`sources`（この段階の書き手あり）<br>`special_flags`（この段階の書き手あり）<br>`special_notes`（この段階の書き手あり）<br>`ttm`（この段階の書き手あり）<br>`upc_prereorg_zero_profit`（この段階の書き手あり） |  |

### `docs/value-monitor/adjusted_eps_analyzer/tickers.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| DataTables CSS | `value`（この段階の書き手あり） |  |

## 範囲外の確認（ページ）

### `docs/value-monitor/admin.html`（VALUE MONITOR · Admin、最後のcommit 2026-10-17）

- 読むJSON `https://data.sec.gov/submissions/CIK${paddedCik}.json` → `docs/value-monitor/https:/data.sec.gov/submissions/CIK{T}.json`（0ファイル）。書き手（.pyの書き込みの場所で照合）: 見つからない
- GitHub の API（contents）でリポジトリのファイルを読み書きするページ（パスはリポジトリの根から。0ファイルは、リポジトリに無いファイル）
  - 読む（fetchFile）:
    - `config/beta_config.json`（1ファイル、行 1715・1843・1996）。.pyの書き手: `src/value/tanuki_valuation/beta_fetcher.py:save_config`
    - `config/cik_lookup.csv`（1ファイル、行 1296・1411・1502・1651・1672・1696）。.pyの書き手: `common/sec_data/registration_validator.py:promote_ticker_status`
    - `config/cik_lookup_result.json`（1ファイル、行 1457・1496・1639）。.pyの書き手: なし
    - `config/discover_config.json`（1ファイル、行 3321・3436）。.pyの書き手: `common/registration/register_ticker.py:step6_discover_register`
    - `config/fcf_conversion_config.json`（0ファイル、行 2027・2110）。.pyの書き手: なし
    - `config/growth_options_config.json`（1ファイル、行 2248）。.pyの書き手: なし
    - `config/maturity_config.json`（1ファイル、行 1731・2248・2625）。.pyの書き手: なし
    - `config/monitor_tickers.yaml`（1ファイル、行 1295・1491・1749・1792・1805・3322）。.pyの書き手: `common/registration/register_ticker.py:step7_monitor_register`
    - `config/rpo_config.json`（1ファイル、行 2143・2234）。.pyの書き手: なし
    - `config/segment_config.json`（1ファイル、行 1722・2248・2461）。.pyの書き手: なし
    - `config/workflow_dependencies.json`（1ファイル、行 2893）。.pyの書き手: `scripts/gen_update_schedule.py:main`
    - `docs/portfolio/data/portfolio.json`（1ファイル、行 3435）。.pyの書き手: なし
    - `docs/portfolio/index.html`（1ファイル、行 3631）。.pyの書き手: なし
    - `docs/value-monitor/tanuki_valuation/data/tickers.json`（1ファイル、行 2788）。.pyの書き手: `src/value/tanuki_valuation/pipeline.py:TanukiValuationPipeline._save_tickers_index`
    - `docs/value-monitor/tanuki_valuation/data/{T}/latest.json`（96ファイル、行 2330・2794）。.pyの書き手: `src/value/tanuki_valuation/pipeline.py:TanukiValuationPipeline._save_result`
    - `src/value/tanuki_valuation/growth_sanity.py`（1ファイル、行 2340・2364）。.pyの書き手: なし
    - `src/value/tanuki_valuation/kpi_config.py`（0ファイル、行 1738）。.pyの書き手: なし
  - 書く（commitFile・複数ファイルの1commit）:
    - `config/beta_config.json`（1ファイル、行 1719・1997）。.pyの書き手: `src/value/tanuki_valuation/beta_fetcher.py:save_config`
    - `config/cik_lookup.csv`（1ファイル、行 1420・1507・1681・1689・1704・1711）。.pyの書き手: `common/sec_data/registration_validator.py:promote_ticker_status`
    - `config/discover_config.json`（1ファイル、行 3401）。.pyの書き手: `common/registration/register_ticker.py:step6_discover_register`
    - `config/fcf_conversion_config.json`（0ファイル、行 2111）。.pyの書き手: なし
    - `config/growth_options_config.json`（1ファイル、行 2756）。.pyの書き手: なし
    - `config/maturity_config.json`（1ファイル、行 1735・2623・2755）。.pyの書き手: なし
    - `config/monitor_tickers.yaml`（1ファイル、行 1492・1753・1794・1807）。.pyの書き手: `common/registration/register_ticker.py:step7_monitor_register`
    - `config/rpo_config.json`（1ファイル、行 2235）。.pyの書き手: なし
    - `config/segment_config.json`（1ファイル、行 1728・2754）。.pyの書き手: なし
    - `docs/portfolio/data/portfolio.json`（1ファイル、行 3604）。.pyの書き手: なし
    - `docs/portfolio/index.html`（1ファイル、行 3637）。.pyの書き手: なし
    - `src/value/tanuki_valuation/growth_sanity.py`（1ファイル、行 2391）。.pyの書き手: なし
    - `src/value/tanuki_valuation/kpi_config.py`（0ファイル、行 1743）。.pyの書き手: なし
  - 起動するワークフロー: `SEC_Data_Update.yml`・`Stonks_Silo_Update.yml`・`TANUKI_CIK_Lookup.yml`・`TANUKI_Segment_AI.yml`・`TANUKI_VALUATION_Update.yml`
- リンク・参照しているファイル: `docs/common/site-nav.js`・`docs/common/ticker-links.js`・`docs/index.html`・`docs/portfolio/index.html`・`docs/portfolio/tail/detail.html`・`docs/portfolio/tail/index.html`・`docs/value-monitor/adjusted_eps_analyzer/admin.html`・`docs/value-monitor/stonks-silo/index.html`・`docs/value-monitor/tanuki_valuation/stock.html`

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | EPSか | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | `discover/stonks-silo/src/financial_trend_calculator.py:448` |
| `maturity_monitor` | はい | — | `src/value/adjusted_eps_analyzer/pipeline.py:22`<br>`src/value/adjusted_eps_analyzer/pipeline.py:814` |
| `market_data_violations_log` | いいえ（後の段階） | — | `common/market_data/fetcher.py:315` |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。EPSのファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | EPSを含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:723`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:866`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:894` | はい |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:817`<br>`common/sec_data/layer3_builder.py:1055`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:229`<br>`src/tail/edgar_rss_monitor.py:433`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:605` | はい |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1633`<br>`common/sec_data/report_consistency_check.py:1774`<br>`common/system_health.py:955`<br>`discover/stonks-silo/src/financial_as_of.py:70`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192` | いいえ |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:405`<br>`src/value/hypecore/hypecore.py:284` | はい |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:389`<br>`src/tail/xbrl_segment_fetcher.py:196`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | いいえ |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:143` | いいえ |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 5 | `docs/portfolio/index.html:673`<br>`docs/value-monitor/tanuki_score/index.html:571`<br>`docs/value-monitor/tanuki_score/index.html:1094`<br>`docs/value-monitor/tanuki_score/index.html:1323`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1135`<br>`docs/value-monitor/tanuki_valuation/stock.html:1136`<br>`src/value/tanuki_valuation/pipeline.py:622`<br>`src/value/tanuki_valuation/pipeline.py:624`<br>`src/value/tanuki_valuation/pipeline.py:626`<br>`src/value/tanuki_valuation/pipeline.py:865`<br>`src/value/tanuki_valuation/pipeline.py:878`<br>`src/value/tanuki_valuation/pipeline.py:898`<br>`src/value/tanuki_valuation/pipeline.py:900`<br>`src/value/tanuki_valuation/pipeline.py:967`<br>`src/value/tanuki_valuation/pipeline.py:968`<br>`src/value/tanuki_valuation/pipeline.py:969`<br>`src/value/tanuki_valuation/pipeline.py:970`<br>`src/value/tanuki_valuation/pipeline.py:1355`<br>`src/value/tanuki_valuation/pipeline.py:1382`<br>`src/value/tanuki_valuation/pipeline.py:1387`<br>`src/value/tanuki_valuation/pipeline.py:1433`<br>`src/value/tanuki_valuation/pipeline.py:1738`<br>`src/value/tanuki_valuation/pipeline.py:1916` | いいえ |
| β・WACCの作り方 | 4 | `docs/value-monitor/admin.html:1236`<br>`docs/value-monitor/admin.html:1838`<br>`docs/value-monitor/admin.html:1860`<br>`docs/value-monitor/admin.html:1898`<br>`docs/value-monitor/admin.html:1915`<br>`docs/value-monitor/admin.html:1919`<br>`docs/value-monitor/admin.html:1972`<br>`src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1121` | いいえ |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:220`<br>`src/value/tanuki_valuation/calculator/adjustments.py:184`<br>`src/value/tanuki_valuation/data_fetcher.py:114`<br>`src/value/tanuki_valuation/data_fetcher.py:1105` | いいえ |

1か所だけのもの: 成長率の上限・下限（1）

## 本番外のコード（src/value/adjusted_eps_analyzer）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

候補は0件。見たファイル（11ファイル・3,557行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |
|---|---|---|---|---|---|
| `adjustment_detector.py` | 165 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |
| `ai_analyzer.py` | 164 | — | `common/sec_data/report_consistency_check.py` | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |
| `company_metadata.py` | 43 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |
| `eps_calculator.py` | 73 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |
| `execution_metrics.py` | 125 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 1件 |
| `extract_key_facts.py` | 1,235 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 8件 |
| `fair_value_detector.py` | 414 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 1件 |
| `maturity_monitor.py` | 84 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |
| `pipeline.py` | 1,089 | `.github/workflows/Adjusted_Eps_Analyzer_update.yml` | `common/registration/register_ticker.py`<br>`common/sec_data/report_consistency_check.py` | — | 10件 |
| `sector_classifier_v2.py` | 107 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 1件 |
| `tax_adjuster.py` | 58 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 1件
  - `src/value/adjusted_eps_analyzer/pipeline.py:507` generate_summary → `<?>`
