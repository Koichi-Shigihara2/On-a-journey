# HypeCoreの出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`hypecore_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/tail/`・`docs/value-monitor/hypecore/`・`docs/value-monitor/tanuki_valuation/`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-21）。画面の区画はKoichiさんの判断で detail.html の7区画をすべて残す。Extreme Fear のページはKoichiさんの判断で消した（範囲外の確認の結果）。出力・本番外のコード・Step Summary の列はチャット側の判断。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 3（ファイル 3・人が見るもの 0）／読み手0件 0／テストだけ 0／1件 1／2件以上 2／規則の二重 9（検出式 11種のうち。HypeCoreのファイルを含むもの 1）

**画面の項目**: 画面が読むキー 61（この段階の書き手あり 57・ほかの書き手あり 2・画面の中で作る値 0・**書き手なし 2**）／書き手があるのに誰も読まないキー **1**（今の書き手のコードに無いキー 0 は数えない）

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 0件＋ファイル名で当たった行 1件／解決できた読み込み 12件＋ファイル名で当たった行 32件（2.2%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

フォルダを決めきれない行（ファイル名で当たった行）:
- `common/method_change.py` 行 44（`docs/value-monitor/hypecore/data/{T}_poc.json`）: `"hypecore": ("docs/value-monitor/hypecore/data", "{T}_poc.json"),`

## 消したもの（段階2、2026-10-21）

- `src/value/hypecore/hypecore.html`: 削除（2026-10-21、チャット側の判断）。2026-05-17 に poc_chart.html から改名した古い単ページ版（436行）。.gitignore の作業用の src/value/hypecore/data/ を読む手元のプレビューで、docs/ の画面とは detail.html と19%・index.html と8%の行だけが同じ。参照は pipeline.py の docstring（移植元が消えたことを書いた）だけ
- `HypeCore_Update.yml の Summary の段の LC・Rec の列`: 削除（2026-10-21、チャット側の判断）。lifecycle_label・recommendation はどの poc.json にも無く、常に「--」だった（FINDINGS 2026-10-21）
- `docs/value-monitor/extreme-fear/index.html（Extreme Fear のページ、394行）と、ナビの項目・HOME のカード・site-header.js の 'ef'・site-theme.css の --tool-ef と body[data-tool="ef"]`: 削除（2026-10-21、Koichiさんの判断: 使っていない）。このページだけが読む出力・キーは無い（market_data.json・TANUKI の tickers.json・latest.json はほかの読み手がある）。scripts/audit_site_links.py でリンク切れ0件

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `src/value/hypecore/data/{T}_poc.json`<br>（0ファイル） | 銘柄ごとの月次の期待・実体の指標とステージ判定の作業用の出力（.gitignore。docs/ に写す元） | `hypecore.py:run_poc` | 1 | index.html（経由）<br>portfolio/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | — | AS-IS-391 | 残す（2026-10-21） |
| 2件以上 | `docs/value-monitor/hypecore/data/tickers.json`<br>（1ファイル） | 一覧のページ用の、HypeCoreの対象銘柄の名前の一覧（tickers・count・updated_at）。読み手は docs/common/ticker-links.js・docs/index.html・docs/value-monitor/hypecore/index.html の3つ（2026-10-21 に読み手をパスの全体で照合するよう直し、同じ名前の tanuki_valuation/data/tickers.json の読み手が混ざらなくなった） | `hypecore.py:_save_tickers_index` | 3・テスト1 | common/ticker-links.js（直接）<br>index.html（直接）<br>value-monitor/hypecore/index.html（直接） | 効く: HypeCoreの画面 | — | 残す（2026-10-21） |
| 2件以上 | `docs/value-monitor/hypecore/data/{T}_poc.json`<br>（98ファイル） | 銘柄ごとの月次の期待・実体の指標とステージ・小区分の判定（src/value/hypecore/data/ から写したもの。画面とTANUKIのtimingが読む） | `hypecore.py:<module>` | 19・テスト12 | index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（直接）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（直接）<br>value-monitor/hypecore/index.html（直接）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 理論株価・判定（TANUKI VALUATION）・HypeCoreの画面・TANUKI VALUATIONの画面 | AS-IS-391 | 残す（2026-10-21） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|

## 読み手の内訳

### `src/value/hypecore/data/{T}_poc.json`（1件）
- 起動元: .github/workflows/HypeCore_Update.yml
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 1294
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/value-monitor/hypecore/data/tickers.json`（2件以上）
- 起動元: .github/workflows/HypeCore_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `count`(1)・`tickers`(4)・`updated_at`(0)
- JS・HTML・保存済みファイル: `docs/common/ticker-links.js` 行 34
- JS・HTML・保存済みファイル: `docs/index.html` 行 234
- JS・HTML・保存済みファイル: `docs/value-monitor/hypecore/index.html` 行 214
- テスト・保存済みファイル: `tests/test_output_inventory_check.py` 行 216

### `docs/value-monitor/hypecore/data/{T}_poc.json`（2件以上）
- 起動元: .github/workflows/HypeCore_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `generated`(2)・`generated_at`(9)・`method_change`(2)・`method_change_applied`(1)・`monthly`(19)・`ticker`(22)
- Python・保存済みファイル: `browser_checks/check_signal_consistency_section.py` 行 52・67・68・175・206（経由 `browser_checks/check_signal_consistency_section.py::_piv_points`、`browser_checks/check_signal_consistency_section.py::pick_sparse_ticker`）
- Python・保存済みファイル: `common/registration/register_ticker.py` 行 299・459・501・512（経由 `common/registration/register_ticker.py::step5_hypecore`）
- Python・保存済みファイル: `common/screening/dcf_validity_checker.py` 行 66・80・163・166・203・226・251・254・365・368・369・370 ほか3（経由 `common/screening/dcf_validity_checker.py::_load_json`、`common/screening/dcf_validity_checker.py::check_b_source_label`、`common/screening/dcf_validity_checker.py::check_e_transition_sample`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 175・493・674・688・690（経由 `common/sec_data/registration_validator.py::check_p1_registration_completeness`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2875・2904・2915（経由 `common/screening/dcf_validity_checker.py::_load_json`）
- Python・保存済みファイル: `common/stage_v2.py` 行 80・308・337・344・355・361（経由 `common/stage_v2.py::_load`、`common/stage_v2.py::finalize`）
- Python・保存済みファイル: `common/system_health.py` 行 241・1220・1303（経由 `common/system_health.py::check_g_hypecore`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 1517・1911（経由 `src/market/market_pulse/sector_rotation.py::_hype_phase`）
- Python・保存済みファイル: `src/market/market_pulse/sector_rotation.py` 行 241・266（経由 `src/market/market_pulse/sector_rotation.py::_hype_phase`）
- Python・保存済みファイル: `src/portfolio/snapshot.py` 行 46・142（経由 `src/portfolio/snapshot.py::main`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 1225・1295・1317（経由 `src/value/hypecore/hypecore.py::_save_tickers_index`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 486・537・869・873・1092・1236・1285・1366・1440・1507・1511・1611 ほか10（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._compute_matrix_position`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._compute_tanuki_score`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report` ほか）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_watcher.py` 行 87・91・98・101・124・190・344・381（経由 `src/value/tanuki_valuation/score_watcher.py::load_hype_method_change`、`src/value/tanuki_valuation/score_watcher.py::load_hype_stage`）
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 343
- JS・HTML・保存済みファイル: `docs/value-monitor/hypecore/detail.html` 行 406・647
- JS・HTML・保存済みファイル: `docs/value-monitor/hypecore/index.html` 行 222
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 437
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 582・663
- ワークフロー・保存済みファイル: `.github/workflows/HypeCore_Update.yml` 行 79
- テスト・保存済みファイル: `tests/test_basis_mix.py` 行 180・195（経由 `tests/test_basis_mix.py::_write_poc`）
- テスト・保存済みファイル: `tests/test_close_missing_price.py` 行 127（経由 `src/value/tanuki_valuation/score_watcher.py::load_hype_stage`）
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_dcf_validity_checker.py` 行 46・64・77・86・95・107・118・128・133・143・155・165（経由 `common/screening/dcf_validity_checker.py::_load_json`、`common/screening/dcf_validity_checker.py::check_b_source_label`）
- テスト・保存済みファイル: `tests/test_market_pulse_sector_rotation.py` 行 165
- テスト・保存済みファイル: `tests/test_method_change.py` 行 78・88・197・211・218・269・303・306・311・315（経由 `src/value/tanuki_valuation/score_watcher.py::load_hype_stage`、`tests/test_method_change.py::TestCheckPreviousOutputs._write`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 151・207・211・226・233・253・266・285・289・294・299・304 ほか44（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._compute_tanuki_score`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`tests/test_pipeline_logic.py::_write_poc_json`）
- テスト・保存済みファイル: `tests/test_register_ticker_preflight.py` 行 68・90・106・124・141（経由 `common/registration/register_ticker.py::step5_hypecore`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 150（経由 `common/registration/register_ticker.py::step5_hypecore`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 505（経由 `common/system_health.py::check_g_hypecore`）
- テスト・保存済みファイル: `tests/test_stage_v2.py` 行 75（経由 `common/stage_v2.py::finalize`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 293・301・433・446（経由 `common/system_health.py::check_g_hypecore`）
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`

## 画面の項目

画面（`docs/value-monitor/hypecore/detail.html`・`docs/value-monitor/hypecore/index.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の書き手のコードが書くキー（データに今無くても）と、この段階の出力のJSON（全ファイル、入れ子8段まで）のキーのうち今の書き手のコードにあるもの、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する（[[CHECK-REFERENCE-DEFINITION-1]]、2026-11-03）。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、今の書き手のコードにもどのJSONにも無いキー）

- `per_gaap_ttm_sec`（`docs/value-monitor/hypecore/detail.html` 行 566）
- `ps_ttm_sec`（`docs/value-monitor/hypecore/detail.html` 行 570）

### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

- `method_change_applied`（`docs/value-monitor/hypecore/data/{T}_poc.json`）

### 今の書き手のコードに無いキー（古いファイル・古いエントリに残るだけ。誰も読まないキーに数えない）

データのキーのうち、書き手のコード（書き手の .py・それを import する呼び出し元・それらが import する .py）に文字列として出てこないもの。書くのをやめて履歴のスナップショット・古いエントリに残るキーや、取得元のデータをそのまま写したキー（SEC の単位・次元の名前等）

なし

### `docs/value-monitor/hypecore/detail.html`（38項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `analyst_upgrade_rate` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 320 | 残す（2026-10-21） |
| — | `components` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 544 | 残す（2026-10-21） |
| — | `eps_surprise` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 320・354 | 残す（2026-10-21） |
| — | `error` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 410 | 残す（2026-10-21） |
| — | `ev_ebitda` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 571・571・571・571 | 残す（2026-10-21） |
| — | `forward_pe` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 318 | 残す（2026-10-21） |
| — | `from_peak` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 317・353・372・527 | 残す（2026-10-21） |
| — | `label` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 423・440・488 | 残す（2026-10-21） |
| MA200乖離% | `ma200_dev_local` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 317・353・372・494 | 残す（2026-10-21） |
| — | `ma200_mom` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 376 | 残す（2026-10-21） |
| — | `month` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 464・524・526・526 | 残す（2026-10-21） |
| — | `monthly` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 301・301・414 | 残す（2026-10-21） |
| — | `ocf_yield_q` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 319 | 残す（2026-10-21） |
| — | `peg` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 569 | 残す（2026-10-21） |
| — | `peg_ratio` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 318・569 | 残す（2026-10-21） |
| — | `per` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 566 | 残す（2026-10-21） |
| — | `per_gaap_ttm_sec` | 書き手なし | 566 | 残す（2026-10-21） |
| — | `per_is_forward` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 566 | 残す（2026-10-21） |
| — | `price` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 435・482・489・538 | 残す（2026-10-21） |
| — | `price_iv_ratio` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 321・354 | 残す（2026-10-21） |
| — | `ps` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 570 | 残す（2026-10-21） |
| — | `ps_ttm_sec` | 書き手なし | 570 | 残す（2026-10-21） |
| — | `psr` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 318・570 | 残す（2026-10-21） |
| — | `real_strong` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 375 | 残す（2026-10-21） |
| — | `rev_yoy` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 303・319・354・504 | 残す（2026-10-21） |
| — | `revenue_growth_yf` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 302・303 | 残す（2026-10-21） |
| MA200乖離% | `rsi` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 317・353・495・537 | 残す（2026-10-21） |
| — | `rule40_yoy_netmargin` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 319 | 残す（2026-10-21） |
| 成長率${lcInfo.g!=null?(lcInfo.g>=0? | `source` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 425 | 残す（2026-10-21） |
| [現在] | `stage` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 372・372・391・391 | 残す（2026-10-21） |
| — | `substage_label` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 430・458・533 | 残す（2026-10-21） |
| — | `substage_next` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 461 | 残す（2026-10-21） |
| — | `substage_phase` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 418・528・533 | 残す（2026-10-21） |
| — | `substage_watch` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 432・460 | 残す（2026-10-21） |
| — | `t` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 452 | 残す（2026-10-21） |
| — | `trailing_pe` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 567 | 残す（2026-10-21） |
| — | `values` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 392 | 残す（2026-10-21） |
| — | `vol_surge_6m` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 320 | 残す（2026-10-21） |

### `docs/value-monitor/hypecore/index.html`（23項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `from_peak` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 162・248 | 残す（2026-10-21） |
| — | `generated_at` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 234・234・234 | 残す（2026-10-21） |
| — | `ma200` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 300・300 | 残す（2026-10-21） |
| — | `ma200_dev_local` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 162・247 | 残す（2026-10-21） |
| — | `ma200_mom` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 166 | 残す（2026-10-21） |
| — | `monthly` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 154・154・229・229 | 残す（2026-10-21） |
| 成長率${r.lcGrowth!=null?(r.lcGrowth>=0? | `phase` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 294・294 | 残す（2026-10-21） |
| — | `price` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 243・296・296 | 残す（2026-10-21） |
| — | `price_iv_ratio` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 244 | 残す（2026-10-21） |
| — | `real_strong` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 165 | 残す（2026-10-21） |
| — | `rev_yoy` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 156・245 | 残す（2026-10-21） |
| — | `revenue_growth_yf` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 155・156 | 残す（2026-10-21） |
| — | `rule40_yoy_netmargin` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 246 | 残す（2026-10-21） |
| — | `source` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 238 | 残す（2026-10-21） |
| 成長率${r.lcGrowth!=null?(r.lcGrowth>=0? | `stage` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 162・162・232・239 | 残す（2026-10-21） |
| — | `status` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 228 | 残す（2026-10-21） |
| — | `substage_label` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 241 | 残す（2026-10-21） |
| — | `substage_phase` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 240 | 残す（2026-10-21） |
| 成長率${r.lcGrowth!=null?(r.lcGrowth>=0? | `text` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 295 | 残す（2026-10-21） |
| — | `ticker` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 264・290・291・291 | 残す（2026-10-21） |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 217 | 残す（2026-10-21） |
| — | `val` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 309 | 残す（2026-10-21） |
| — | `value` | この段階の書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/hypecore/data/{T}_poc.json` | 228・320 | 残す（2026-10-21） |

## 画面の区画

HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後15行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。

### `docs/value-monitor/hypecore/detail.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| HERO BAR | `from_peak`（この段階の書き手あり）<br>`label`（この段階の書き手あり）<br>`ma200_dev_local`（この段階の書き手あり）<br>`ma200_mom`（この段階の書き手あり）<br>`monthly`（この段階の書き手あり）<br>`price`（この段階の書き手あり）<br>`real_strong`（この段階の書き手あり）<br>`rev_yoy`（この段階の書き手あり）<br>`revenue_growth_yf`（この段階の書き手あり）<br>`source`（この段階の書き手あり）<br>`stage`（この段階の書き手あり）<br>`substage_label`（この段階の書き手あり）<br>`substage_phase`（この段階の書き手あり）<br>`substage_watch`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| 期待の正体（折りたたみ） | `label`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| 左：期待指標＋遷移確率 | `analyst_upgrade_rate`（この段階の書き手あり）<br>`eps_surprise`（この段階の書き手あり）<br>`forward_pe`（この段階の書き手あり）<br>`from_peak`（この段階の書き手あり）<br>`ma200_dev_local`（この段階の書き手あり）<br>`ocf_yield_q`（この段階の書き手あり）<br>`peg_ratio`（この段階の書き手あり）<br>`price_iv_ratio`（この段階の書き手あり）<br>`psr`（この段階の書き手あり）<br>`rev_yoy`（この段階の書き手あり）<br>`rsi`（この段階の書き手あり）<br>`rule40_yoy_netmargin`（この段階の書き手あり）<br>`stage`（この段階の書き手あり）<br>`values`（この段階の書き手あり）<br>`vol_surge_6m`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| 右：実体指標＋現状解釈＋内部フェーズ | `eps_surprise`（この段階の書き手あり）<br>`from_peak`（この段階の書き手あり）<br>`ma200_dev_local`（この段階の書き手あり）<br>`month`（この段階の書き手あり）<br>`price_iv_ratio`（この段階の書き手あり）<br>`rev_yoy`（この段階の書き手あり）<br>`rsi`（この段階の書き手あり）<br>`substage_label`（この段階の書き手あり）<br>`substage_next`（この段階の書き手あり）<br>`substage_watch`（この段階の書き手あり）<br>`t`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| 追加H: バリュエーション倍率パネル | `ev_ebitda`（この段階の書き手あり）<br>`peg`（ほかの書き手あり）<br>`peg_ratio`（この段階の書き手あり）<br>`per`（この段階の書き手あり）<br>`per_gaap_ttm_sec`（書き手なし）<br>`per_is_forward`（ほかの書き手あり）<br>`ps`（この段階の書き手あり）<br>`ps_ttm_sec`（書き手なし）<br>`psr`（この段階の書き手あり）<br>`trailing_pe`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| チャート | `from_peak`（この段階の書き手あり）<br>`label`（この段階の書き手あり）<br>`ma200_dev_local`（この段階の書き手あり）<br>`ma200_mom`（この段階の書き手あり）<br>`price`（この段階の書き手あり）<br>`real_strong`（この段階の書き手あり）<br>`rsi`（この段階の書き手あり）<br>`stage`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| テーブル | `components`（この段階の書き手あり）<br>`from_peak`（この段階の書き手あり）<br>`ma200_dev_local`（この段階の書き手あり）<br>`ma200_mom`（この段階の書き手あり）<br>`month`（この段階の書き手あり）<br>`price`（この段階の書き手あり）<br>`real_strong`（この段階の書き手あり）<br>`rsi`（この段階の書き手あり）<br>`stage`（この段階の書き手あり）<br>`substage_label`（この段階の書き手あり）<br>`substage_phase`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| （区画を決められない） | `error`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |

### `docs/value-monitor/hypecore/index.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| （ページの先頭） | `from_peak`（この段階の書き手あり）<br>`generated_at`（この段階の書き手あり）<br>`ma200`（この段階の書き手あり）<br>`ma200_dev_local`（この段階の書き手あり）<br>`ma200_mom`（この段階の書き手あり）<br>`monthly`（この段階の書き手あり）<br>`phase`（この段階の書き手あり）<br>`price`（この段階の書き手あり）<br>`price_iv_ratio`（この段階の書き手あり）<br>`real_strong`（この段階の書き手あり）<br>`rev_yoy`（この段階の書き手あり）<br>`revenue_growth_yf`（この段階の書き手あり）<br>`rule40_yoy_netmargin`（この段階の書き手あり）<br>`source`（この段階の書き手あり）<br>`stage`（この段階の書き手あり）<br>`status`（この段階の書き手あり）<br>`substage_label`（この段階の書き手あり）<br>`substage_phase`（この段階の書き手あり）<br>`text`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`tickers`（この段階の書き手あり）<br>`val`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |

## ワークフローが自分で書く出力（.pyの外）

段階の.pyを動かすワークフローの中で、`$GITHUB_STEP_SUMMARY`に書く段。`.get()`で読むキーが、この段階のJSON（全ファイル）にあるか。

- `.github/workflows/HypeCore_Update.yml`（行 69・70・71・72・76）: `monthly`（ある）・`stage`（ある）・`substage_label`（ある）

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | HypeCoreか | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | はい | — | `src/value/hypecore/hypecore.py:1074`<br>`src/value/hypecore/hypecore.py:1119` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:21` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。HypeCoreのファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | HypeCoreを含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2325`<br>`common/sec_data/q4_implied.py:162`<br>`common/sec_data/q4_implied.py:171`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:720`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:863`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:891` | いいえ |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 10 | `common/sec_data/bs_lines.py:245`<br>`common/sec_data/bs_lines.py:264`<br>`common/sec_data/bs_lines.py:333`<br>`common/sec_data/cf_lines.py:438`<br>`common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:917`<br>`common/sec_data/layer3_builder.py:1140`<br>`common/sec_data/normalizer.py:168`<br>`common/sec_data/normalizer.py:431`<br>`common/sec_data/share_unit_fix.py:104`<br>`common/sec_data/split_adjust.py:125`<br>`src/tail/edgar_rss_monitor.py:219`<br>`src/tail/edgar_rss_monitor.py:419`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:602`<br>`tools/sim/parser_l3/val_guard_proto.py:138`<br>`tools/sim/parser_l3/val_guard_proto.py:139` | いいえ |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3048`<br>`common/sec_data/report_consistency_check.py:1588`<br>`common/sec_data/report_consistency_check.py:1785`<br>`common/system_health.py:975`<br>`docs/portfolio/tail/index.html:943`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192`<br>`src/value/stonks_silo/financial_as_of.py:70` | いいえ |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:239`<br>`src/value/adjusted_eps_analyzer/pipeline.py:402`<br>`src/value/hypecore/hypecore.py:293` | はい |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:375`<br>`src/tail/xbrl_segment_fetcher.py:180`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | いいえ |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:455`<br>`common/sec_data/parser.py:464`<br>`common/sec_data/utils.py:112`<br>`common/sec_data/utils.py:293`<br>`common/sec_data/utils.py:328`<br>`common/sec_data/utils.py:368`<br>`src/tail/edgar_rss_monitor.py:137` | いいえ |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 5 | `docs/portfolio/index.html:650`<br>`docs/value-monitor/tanuki_score/index.html:590`<br>`docs/value-monitor/tanuki_score/index.html:1113`<br>`docs/value-monitor/tanuki_score/index.html:1337`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1145`<br>`docs/value-monitor/tanuki_valuation/stock.html:1146`<br>`src/value/tanuki_valuation/pipeline.py:669`<br>`src/value/tanuki_valuation/pipeline.py:671`<br>`src/value/tanuki_valuation/pipeline.py:673`<br>`src/value/tanuki_valuation/pipeline.py:944`<br>`src/value/tanuki_valuation/pipeline.py:957`<br>`src/value/tanuki_valuation/pipeline.py:977`<br>`src/value/tanuki_valuation/pipeline.py:979`<br>`src/value/tanuki_valuation/pipeline.py:1051`<br>`src/value/tanuki_valuation/pipeline.py:1052`<br>`src/value/tanuki_valuation/pipeline.py:1053`<br>`src/value/tanuki_valuation/pipeline.py:1054`<br>`src/value/tanuki_valuation/pipeline.py:1472`<br>`src/value/tanuki_valuation/pipeline.py:1499`<br>`src/value/tanuki_valuation/pipeline.py:1504`<br>`src/value/tanuki_valuation/pipeline.py:1550`<br>`src/value/tanuki_valuation/pipeline.py:1856`<br>`src/value/tanuki_valuation/pipeline.py:2034` | いいえ |
| β・WACCの作り方 | 3 | `src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1172` | いいえ |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:221`<br>`src/value/tanuki_valuation/calculator/adjustments.py:184`<br>`src/value/tanuki_valuation/data_fetcher.py:124`<br>`src/value/tanuki_valuation/data_fetcher.py:1156` | いいえ |

1か所だけのもの: 成長率の上限・下限（1）・EPSの分母の株式数（期間の利益＝期間の加重平均の株式数〈最大EPSはEPS ANALYZERのTTMの値、無ければ今の株式数〉・1株あたりの理論株価・時価総額＝今の完全希薄化後の株式数）（1）

## 本番外のコード（src/value/hypecore）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

候補は0件。見たファイル（1ファイル・1,317行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |
|---|---|---|---|---|---|
| `hypecore.py` | 1,317 | `.github/workflows/HypeCore_Update.yml` | `common/registration/register_ticker.py`<br>`tools/sim/sim.py` | — | 9件 |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 0件
