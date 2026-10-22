# マクロデータ層（common/macro_data）の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`macro_data_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`・`src/market/macro_pulse/`・`docs/market-monitor/macro-pulse/index.html`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-23、Koichiさんの判断）。移行のスクリプトと誰も読まないキー3つを消し、出力はすべて残す（macro_data_violations_log.json は System Health の [L] が読む）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 4（ファイル 3・人が見るもの 1）／読み手0件 0／テストだけ 0／1件 1／2件以上 2／規則の二重 14（検出式 15種のうち。マクロデータ層のファイルを含むもの 2）

**誰も読まないキー**: **1**（この段階のJSONのキー 41 のうち。下の「誰も読まないキー」）

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 0件＋ファイル名で当たった行 0件／解決できた読み込み 6件＋ファイル名で当たった行 7件（0.0%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

## 消したもの（段階2、2026-10-20）

- `common/macro_data/migrate_bamlh0a0hym2_history.py`: 削除（2026-10-23、Koichiさんの判断）。05_events.csv の HY Spread の行を series/BAMLH0A0HYM2.json へ移す一度きりの移行で、2026-08-12（2ff481b3ad）に適用済み
- `series_meta.json の consumers・input_id、series/BAMLH0A0HYM2.json の fallback_used`: 削除（2026-10-23）。誰も読まない。consumers は reader.py に切り替える前の取り方の古い説明（読み手は一覧が出す）、input_id は INPUT_DATA_AS_IS.md の表から作り直せる参照の番号、fallback_used は消した移行のスクリプトが移した6,947件に書いた値（すべて False）

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `common/macro_data/series_meta.json`<br>（1ファイル） | 系列ごとのメタ情報（fred_release_id・obs_to_release_lag・category・note）。**コードの書き手は無い（人の手で書く静的なファイル）**。fetcher.py が取得する系列の一覧として読む |  | 1・テスト2 | — | — | — | 残す（2026-10-23） |
| 2件以上 | `common/macro_data/macro_data_violations_log.json`<br>（1ファイル） | 系列ごとの保存前の検証の結果（警告と取得の成否 fetch_status）。System Health の [L]（check_l_macro_data）が取得の失敗を見るのに読む | `fetcher.py:_write_violations_section` | 2・テスト3 | — | — | — | 残す（2026-10-23） |
| 2件以上 | `common/macro_data/series/{series_id}.json`<br>（26ファイル） | FREDの系列ごとの観測値（records）。Macro_Data_Update（毎日10:00 UTC、直近400日）が取り直す。MACRO PULSE（05_main.py）・Market Pulse（collect_and_send.py・backfill_tech_pulse.py）が reader.py 経由で読む | `fetcher.py:update_series` | 9・テスト3 | market-monitor/macro-pulse/index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由） | 効く: MACRO PULSE（景気後退リスクの点数・局面） | AS-IS-019・AS-IS-046 | 残す（2026-10-23） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `${GITHUB_STEP_SUMMARY}` | Macro_Data_Update の取得の成功・失敗の件数と、失敗した系列と理由の表 | `fetcher.py:<module>` | .github/workflows/Macro_Data_Update.yml |

## 読み手の内訳

### `common/macro_data/series_meta.json`（1件）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `BAMLH0A0HYM2`(3)・`CFNAIMA3`(1)・`DFEDTARL`(0)・`DFEDTARU`(0)・`DGS1`(0)・`DGS3MO`(0)・`FEDFUNDS`(0)・`GACDFSA066MSFRBPHI`(0)・`IC4WSA`(1)・`M2SL`(1)・`MICH`(0)・`PAYEMS`(1)・`PERMIT`(1)・`RRPONTSYD`(1)・`SAHMCURRENT`(0)・`SP500`(1)・`T10Y2Y`(1)・`T5YIE`(0)・`UMCSENT`(1)・`VIXCLS`(0)・`VXNCLS`(0)・`WALCL`(1)・`WDTGAL`(0)・`WRBWFRBL`(1)・`WTREGEN`(1)
- Python・保存済みファイル: `common/macro_data/fetcher.py` 行 113・365・411（経由 `common/macro_data/fetcher.py::_load_json`、`common/macro_data/fetcher.py::fetch_all_series`）
- テスト・保存済みファイル: `tests/test_macro_data_fetcher.py` 行 253・285・405・420・436・439・458・471・478（経由 `common/macro_data/fetcher.py::fetch_all_series`）
- テスト・保存済みファイル: `tests/test_macro_pulse_accuracy.py` 行 279・280

### `common/macro_data/macro_data_violations_log.json`（2件以上）
- 起動元: .github/workflows/Macro_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `BAMLH0A0HYM2`(1)・`CFNAI`(1)・`CFNAIMA3`(0)・`DFEDTARL`(0)・`DFEDTARU`(0)・`DGS1`(0)・`DGS3MO`(0)・`FEDFUNDS`(0)・`FTSD`(1)・`GACDFSA066MSFRBPHI`(0)・`IC4WSA`(0)・`M2SL`(0)・`MICH`(0)・`PAYEMS`(1)・`PERMIT`(0)・`RRPONTSYD`(0)・`SAHMCURRENT`(0)・`SP500`(0)・`T10Y2Y`(0)・`T5YIE`(0)・`UMCSENT`(0)・`VIXCLS`(0)・`VXNCLS`(0)・`WALCL`(0)・`WDTGAL`(0)・`WRBWFRBL`(0)・`WTREGEN`(0)
- Python・保存済みファイル: `common/macro_data/fetcher.py` 行 117
- Python・保存済みファイル: `common/system_health.py` 行 53・840・1050・1110（経由 `common/system_health.py::check_l_macro_data`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_l_macro_data`）
- テスト・保存済みファイル: `tests/test_system_health_macro_data.py` 行 44・62・78・95・103（経由 `common/system_health.py::check_l_macro_data`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_l_macro_data`）

### `common/macro_data/series/{series_id}.json`（2件以上）
- 起動元: .github/workflows/Macro_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `records`(5)・`series_id`(3)
- Python・保存済みファイル: `browser_checks/check_dependency_map.py` 行 448（経由 `common/macro_data/reader.py::_load_series_payload`）
- Python・保存済みファイル: `browser_checks/check_macro_pulse.py` 行 239・630・1087・1117・1131・1156・1247・1273（経由 `browser_checks/check_macro_pulse.py::load_series`）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 965（経由 `common/macro_data/reader.py::_load_series_payload`）
- Python・保存済みファイル: `common/macro_data/fetcher.py` 行 109
- Python・保存済みファイル: `common/macro_data/reader.py` 行 41・49・50・62・80・116（経由 `common/macro_data/reader.py::_load_json`、`common/macro_data/reader.py::_load_series_payload`）
- Python・保存済みファイル: `src/market/macro_pulse/05_main.py` 行 750・773・791・792・795・811・849・850・851・895・1009・1016 ほか16（経由 `common/macro_data/reader.py::_load_series_payload`）
- Python・保存済みファイル: `src/market/market_pulse/backfill_tech_pulse.py` 行 304・366（経由 `common/macro_data/reader.py::_load_series_payload`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 478・505・1197・1247・1891・1934（経由 `common/macro_data/reader.py::_load_series_payload`）
- ワークフロー・保存済みファイル: `.github/workflows/Macro_Data_Update.yml` 行 75
- テスト・保存済みファイル: `tests/test_macro_data_reader.py` 行 40・45・52・59・64・70・76・82・89・97・103・108 ほか10（経由 `common/macro_data/reader.py::_load_series_payload`）
- テスト・保存済みファイル: `tests/test_macro_pulse_accuracy.py` 行 218（経由 `common/macro_data/reader.py::_load_series_payload`）
- テスト・保存済みファイル: `tests/test_macro_pulse_logic.py` 行 57・66・73・397・418・436・437・457（経由 `common/macro_data/reader.py::_load_series_payload`）
- 画面（経由（src/market/macro_pulse/05_main.py → docs/market-monitor/macro-pulse/data/05_events.csv））: `docs/market-monitor/macro-pulse/index.html`
- 画面（経由（src/market/market_pulse/backfill_tech_pulse.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/market/market_pulse/backfill_tech_pulse.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/value-monitor/tanuki_score/index.html`

## 誰も読まないキー

この段階の出力のJSON（全ファイル、入れ子8段まで）のキーのうち、テストを除くコード（.py・.js・.html。書き手のファイルを含む）に読みの形（`.key`・`['key']`〈代入でない〉・`.get('key')`）で出てこないもの。銘柄名・日付など小文字を含まないキーは除く。Pythonで`for k in d`・`d.items()`のように回して読むものは拾えないので、消す前にキーの名前でgrepする

- `source_detail`（`common/macro_data/series/{series_id}.json`）: 残す: 値の出所の記録（取得した系列。BAMLH0A0HYM2 の古い6,947件は 05_events.csv から移した印 migrated_from）。読んでいたのは2026-10-23に消した移行のスクリプト（二重に動かさない判定）だけ

## ワークフローが自分で書く出力（.pyの外）

段階の.pyを動かすワークフローの中で、`$GITHUB_STEP_SUMMARY`に書く段。`.get()`で読むキーが、この段階のJSON（全ファイル）にあるか。

- `.github/workflows/Macro_Data_Update.yml`（行 73・74・76）: 

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | マクロデータ層か | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:22` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。マクロデータ層のファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | マクロデータ層を含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:723`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:866`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:894` | いいえ |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:817`<br>`common/sec_data/layer3_builder.py:1055`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:229`<br>`src/tail/edgar_rss_monitor.py:433`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:605` | いいえ |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1634`<br>`common/sec_data/report_consistency_check.py:1775`<br>`common/system_health.py:955`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192`<br>`src/value/stonks_silo/financial_as_of.py:70` | いいえ |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:402`<br>`src/value/hypecore/hypecore.py:284` | いいえ |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:389`<br>`src/tail/xbrl_segment_fetcher.py:196`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | いいえ |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:143` | いいえ |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 5 | `docs/portfolio/index.html:673`<br>`docs/value-monitor/tanuki_score/index.html:571`<br>`docs/value-monitor/tanuki_score/index.html:1094`<br>`docs/value-monitor/tanuki_score/index.html:1323`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1135`<br>`docs/value-monitor/tanuki_valuation/stock.html:1136`<br>`src/value/tanuki_valuation/pipeline.py:622`<br>`src/value/tanuki_valuation/pipeline.py:624`<br>`src/value/tanuki_valuation/pipeline.py:626`<br>`src/value/tanuki_valuation/pipeline.py:865`<br>`src/value/tanuki_valuation/pipeline.py:878`<br>`src/value/tanuki_valuation/pipeline.py:898`<br>`src/value/tanuki_valuation/pipeline.py:900`<br>`src/value/tanuki_valuation/pipeline.py:967`<br>`src/value/tanuki_valuation/pipeline.py:968`<br>`src/value/tanuki_valuation/pipeline.py:969`<br>`src/value/tanuki_valuation/pipeline.py:970`<br>`src/value/tanuki_valuation/pipeline.py:1355`<br>`src/value/tanuki_valuation/pipeline.py:1382`<br>`src/value/tanuki_valuation/pipeline.py:1387`<br>`src/value/tanuki_valuation/pipeline.py:1433`<br>`src/value/tanuki_valuation/pipeline.py:1738`<br>`src/value/tanuki_valuation/pipeline.py:1916` | いいえ |
| β・WACCの作り方 | 3 | `src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1121` | いいえ |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:220`<br>`src/value/tanuki_valuation/calculator/adjustments.py:184`<br>`src/value/tanuki_valuation/data_fetcher.py:114`<br>`src/value/tanuki_valuation/data_fetcher.py:1105` | いいえ |
| 景気後退リスクの点数（8指標の段階の点の合計）の計算 | 2 | `docs/market-monitor/macro-pulse/index.html:1443`<br>`docs/market-monitor/macro-pulse/index.html:2389`<br>`src/market/macro_pulse/05_main.py:1580`<br>`src/market/macro_pulse/05_main.py:1700` | いいえ |
| 指標ごとの段階の信号（しきい値で点を付ける） | 2 | `docs/market-monitor/macro-pulse/index.html:1410`<br>`src/market/macro_pulse/05_main.py:1563`<br>`src/market/macro_pulse/05_main.py:1576` | いいえ |
| FREDのAPIを直接呼ぶ所（系列の値・公表日） | 3 | `common/macro_data/fetcher.py:69`<br>`src/market/macro_pulse/05_main.py:527`<br>`src/market/market_pulse/market_calendar.py:63` | はい |
| AI（Grok）のAPIの宛先の直書き | 10 | `src/market/macro_pulse/05_main.py:1339`<br>`src/market/macro_pulse/05_main.py:1832`<br>`src/market/market_pulse/collect_and_send.py:1147`<br>`src/market/market_pulse/news_headlines.py:41`<br>`src/tail/kpi_proposer.py:71`<br>`src/tail/quarterly_review_generator.py:78`<br>`src/tail/sec_ctrl_fetcher.py:50`<br>`src/tail/text_kpi_extractor.py:47`<br>`src/value/adjusted_eps_analyzer/ai_analyzer.py:74`<br>`src/value/tanuki_valuation/calculator/fcf_outlier_ai.py:35`<br>`src/value/tanuki_valuation/validator.py:37` | いいえ |
| JSONの書き込み（一時ファイル→置き換え）の実装 | 2 | `common/macro_data/fetcher.py:79`<br>`common/market_data/fetcher.py:180` | はい |

1か所だけのもの: 成長率の上限・下限（1）

## 本番外のコード（common/macro_data）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

候補は0件。見たファイル（2ファイル・564行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |
|---|---|---|---|---|---|
| `fetcher.py` | 439 | `.github/workflows/Macro_Data_Update.yml` | — | `common/macro_data/__init__.py`<br>`common/macro_data/reader.py` | 2件 |
| `reader.py` | 125 | — | `browser_checks/market_pulse_elements.py`<br>`src/market/macro_pulse/05_main.py`<br>`src/market/market_pulse/backfill_tech_pulse.py`<br>`src/market/market_pulse/collect_and_send.py` | `common/macro_data/__init__.py` | 1件 |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 0件
