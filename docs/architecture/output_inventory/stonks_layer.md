# STONKS SILO（src/value/stonks_silo）の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`stonks_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`・`docs/value-monitor/stonks-silo/`・`docs/value-monitor/tanuki_valuation/`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-21）。画面の区画はKoichiさんの判断ですべて残す。フォルダの付け替えと誰も読まないキーはチャット側の判断。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 1（ファイル 1・人が見るもの 0）／読み手0件 0／テストだけ 0／1件 0／2件以上 1／規則の二重 9（検出式 10種のうち。STONKSのファイルを含むもの 1）

**画面の項目**: 画面が読むキー 80（この段階の書き手あり 72・ほかの書き手あり 8・画面の中で作る値 0・**書き手なし 0**）／書き手があるのに誰も読まないキー **2**

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 1件＋ファイル名で当たった行 0件／解決できた読み込み 6件＋ファイル名で当たった行 11件（5.6%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

## 消したもの（段階2、2026-10-21）

- `discover/stonks-silo/src/ の6ファイル`: src/value/stonks_silo/ へ移した（2026-10-21、チャット側の判断）。Discover は削除済みのサブシステムで、フォルダの名前と中身が合わなかった（型E）。根からの深さは同じ3段で、根を階層の数で求めるコードは変えていない。出力 results.json と画面の場所は変えない。画面の見出しの「Discover · 10x Candidates」も「Value Monitor · Early-Stage Growth」に直した
- `誰も読まないキー10個（results.json への書き込み）`: 書き込みをやめた（2026-10-21、チャット側の判断）: angle・composite・population_size・fields_available・fields_missing・heatmap_fields・financial_vectors_note・gross_margin_note・ticker_count・unit_economics_label（data_quality はこの3つの入れ物ごと）。合成ベクトル・角度の計算と使われなくなった定数（COMPOSITE_FIELDS・HEATMAP_FIELDS・VECTOR_FIELDS の weight）も消した。全24銘柄の results.json は消したキーと時刻のほか前後で同じ、overall_verdict・overall_score も同じ

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 2件以上 | `docs/value-monitor/stonks-silo/data/results.json`<br>（1ファイル） | 赤字・小型の成長株の全銘柄の評価（生存能力〈runway〉・黒字化の道筋・拡大再生産・バリュエーション・総合の判定と点数）と、四半期・年次の財務の推移（financial_vectors）。STONKS SILOの画面・TANUKI VALUATIONの赤字銘柄の評価とTANUKI Score・rcc・System Healthが読む | `pipeline.py:run` | 8・テスト5 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（直接）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 理論株価・判定（TANUKI VALUATION）・STONKS SILOの画面・TANUKI VALUATIONの画面 | — | 残す（2026-10-21） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|

## 読み手の内訳

### `docs/value-monitor/stonks-silo/data/results.json`（2件以上）
- 起動元: .github/workflows/Stonks_Silo_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `errors`(1)・`generated_at`(5)・`tickers`(10)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 178・214・1069・2623・3433（経由 `common/sec_data/report_consistency_check.py::_load_stonks_results`）
- Python・保存済みファイル: `common/system_health.py` 行 50・160・183・1041・1042・1110（経由 `common/system_health.py::check_d_actions`、`common/system_health.py::check_e_silo`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 36
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 439・490・824・828・1144・1323・1609・1614・3524・3528・3864（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._compute_tanuki_score`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._inject_ttm_for_general_segment`）
- JS・HTML・保存済みファイル: `docs/value-monitor/stonks-silo/index.html` 行 392
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 240
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 2726
- ワークフロー・保存済みファイル: `.github/workflows/Stonks_Silo_Update.yml` 行 81
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_latest_period_guard.py` 行 67・79・89（経由 `tests/test_pipeline_logic.py::_write_stonks_json`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 166・211・233・253・266・285・289・294・299・304・309・328 ほか59（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._compute_tanuki_score`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`tests/test_pipeline_logic.py::_write_stonks_json`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_e_silo`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 289・297・429・442（経由 `common/system_health.py::check_e_silo`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`

## 画面の項目

画面（`docs/value-monitor/stonks-silo/index.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（全ファイル、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、どのJSONにも無いキー）


### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

- `gross_margin_trend`（`docs/value-monitor/stonks-silo/data/results.json`）: 残す: 書き手の中の計算（Unit Economics の点数の GM トレンド）に使う
- `latest_year`（`docs/value-monitor/stonks-silo/data/results.json`）: 残す: テストが参照する（DeficitQuality・RunwayAnalysis の引数）

### `docs/value-monitor/stonks-silo/index.html`（80項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `adjusted_eps` | ほかの書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/AAPL/quarterly.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/ABBV/quarterly.json` | 660・663 | 残す（2026-10-21） |
| — | `breakeven_estimate` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/BBAI/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/IONQ/latest.json` | 631 | 残す（2026-10-21） |
| — | `breakeven_reason` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 632 | 残す（2026-10-21） |
| 安全 | `cagr_3yr` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 990・1080・1080 | 残す（2026-10-21） |
| 月次資金消費 | `capex_annual` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1109 | 残す（2026-10-21） |
| 現金 | `cash` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1108 | 残す（2026-10-21） |
| — | `change_pct` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1194・1195 | 残す（2026-10-21） |
| — | `current_price` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 474・565・565・1025 | 残す（2026-10-21） |
| 赤字固定化リスク | `deficit_fixed_risk` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1091 | 残す（2026-10-21） |
| — | `deficit_quality` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 463・534・875 | 残す（2026-10-21） |
| 希薄化リスク | `dilution_risk` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1092 | 残す（2026-10-21） |
| ⚠ 非連続成長の可能性あり | `discontinuous_growth` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1131・1139 | 残す（2026-10-21） |
| ⚠ 非連続成長の可能性あり | `discontinuous_growth_note` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1131 | 残す（2026-10-21） |
| — | `end` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 735・736・1220・1220 | 残す（2026-10-21） |
| （売上: | `ev_sales` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1030・1030 | 残す（2026-10-21） |
| — | `fields` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 730・1181・1192・1219 | 残す（2026-10-21） |
| ピン留め | `financial_as_of` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 421・422・560・1042 | 残す（2026-10-21） |
| — | `financial_vectors` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 730・1163・1358 | 残す（2026-10-21） |
| 純利益黒字化予測 | `gaap_breakeven_reason` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1130・1453・1466 | 残す（2026-10-21） |
| × Runway ヶ月安全 | `gaap_breakeven_year` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1002・1130・1454・1454 | 残す（2026-10-21） |
| — | `generated_at` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 415 | 残す（2026-10-21） |
| 粗利率${dq.gross_margin_derived?' | `gross_margin` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 475・566・566・1081 | 残す（2026-10-21） |
| 粗利率${dq.gross_margin_derived?' | `gross_margin_derived` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1081 | 残す（2026-10-21） |
| × OCF黒字達成済み | `hidden_profit_already` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1003・1128・1128・1450 | 残す（2026-10-21） |
| — | `incremental_gp_delta` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1019・1019 | 残す（2026-10-21） |
| — | `incremental_margin` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1014・1016・1018・1019 | 残す（2026-10-21） |
| — | `incremental_margin_prev` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1013・1014・1015・1016 | 残す（2026-10-21） |
| — | `incremental_margin_trend` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1013・1015 | 残す（2026-10-21） |
| — | `incremental_rev_delta` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1019 | 残す（2026-10-21） |
| — | `label` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 839・1200・1201・1251 | 残す（2026-10-21） |
| 不明 | `latest_quarter_end` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 422・1512・1521 | 残す（2026-10-21） |
| 古い 銘柄 | `length` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 419・422・424・425 | 残す（2026-10-21） |
| — | `market_cap` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1025・1027・1027 | 残す（2026-10-21） |
| 成熟想定利益 | `mature_profit` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1085・1085・1085 | 残す（2026-10-21） |
| 成熟想定利益 | `mature_profit_note` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1085 | 残す（2026-10-21） |
| × Runway ∞（CF黒字） | `monthly_burn` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 999・1109・1109・1109 | 残す（2026-10-21） |
| （売上: | `net_cash` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1031・1031 | 残す（2026-10-21） |
| — | `net_income_fy` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 972・974 | 残す（2026-10-21） |
| — | `next_earnings_date` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 623 | 残す（2026-10-21） |
| 月次資金消費 | `ocf_annual` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 966・967・1109 | 残す（2026-10-21） |
| 営業CF黒字化予測 | `ocf_breakeven_reason` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1129 | 残す（2026-10-21） |
| × Runway ヶ月安全 | `ocf_breakeven_year` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1002・1129・1451・1451 | 残す（2026-10-21） |
| OCFトレンド（金額） | `ocf_trend` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 473・564・1127 | 残す（2026-10-21） |
| — | `overall_score` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 469・538 | 残す（2026-10-21） |
| ピン留め | `overall_verdict` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 470・514・561・984 | 残す（2026-10-21） |
| 黒字化進捗 | `path_score_basis` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1060・1120 | 残す（2026-10-21） |
| — | `profitability_path` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 465・536・877 | 残す（2026-10-21） |
| （売上: | `psr` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1029・1029 | 残す（2026-10-21） |
| — | `qoq` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1193 | 残す（2026-10-21） |
| — | `quarters` | ほかの書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/AAPL/quarterly.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/ABBV/quarterly.json` | 657 | 残す（2026-10-21） |
| — | `records` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 961・961・971・972 | 残す（2026-10-21） |
| — | `reproduction_label` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1008 | 残す（2026-10-21） |
| × 年黒字化予測 | `reproduction_score` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1007 | 残す（2026-10-21） |
| — | `revenue` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 971・973 | 残す（2026-10-21） |
| ※年は計算から除外（外れ値） | `revenue_outlier_years` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1146・1146 | 残す（2026-10-21） |
| 成長投資比率 | `rnd_ratio` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1084・1084 | 残す（2026-10-21） |
| 売上成長率(3年CAGR)+営業利益率の合計。40以上が優良SaaS基準 | `rule40_cagr3y_opmargin` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1082・1082・1082・1082 | 残す（2026-10-21） |
| — | `runway` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 464・472・535・876 | 残す（2026-10-21） |
| 売上成長データなし | `runway_months` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 998・1481 | 残す（2026-10-21） |
| （売上: | `sales_basis` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1029・1029 | 残す（2026-10-21） |
| SBC調整後FCF | `sbc_adjusted_fcf` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1086・1086・1086 | 残す（2026-10-21） |
| SBC比率（売上比） | `sbc_ratio` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1087 | 残す（2026-10-21） |
| SBCトレンド（前年比） | `sbc_yoy_change` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1088・1088・1088 | 残す（2026-10-21） |
| — | `score` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 471・977・978・979 | 残す（2026-10-21） |
| FY（期末 ${fa.scoring_fy_end \|\| | `scoring_fy` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1522・1522 | 残す（2026-10-21） |
| FY（期末 ${fa.scoring_fy_end \|\| | `scoring_fy_end` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1522 | 残す（2026-10-21） |
| — | `series_q` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 731・1219・1233・1359 | 残す（2026-10-21） |
| 成長投資比率 | `sm_ratio` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1084・1084 | 残す（2026-10-21） |
| 財務 不明 | `stale` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 421・1514・1523 | 残す（2026-10-21） |
| 古い | `stale_threshold_days` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1515 | 残す（2026-10-21） |
| ※黒字化予測は過去トレンドの外挿であり、直近の急成長局面では実際の黒字化が予測よ | `summary` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1139 | 残す（2026-10-21） |
| — | `tanuki_score` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 449 | 残す（2026-10-21） |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 412 | 残す（2026-10-21） |
| データ不足 | `type` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AMD/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/AMZN/latest.json` | 798・799・800 | 残す（2026-10-21） |
| — | `val` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 715・732・735・736 | 残す（2026-10-21） |
| 拡大再生産 | `valuation` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 466・537・1024 | 残す（2026-10-21） |
| 成長・投資力 | `verdict` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 562・1050・1055・1073 | 残す（2026-10-21） |
| ② 生存能力 | `verdict_reason` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1101・1101 | 残す（2026-10-21） |
| — | `years` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 878 | 残す（2026-10-21） |
| — | `yoy` | この段階の書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 1193 | 残す（2026-10-21） |

## 画面の区画

HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後15行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。

### `docs/value-monitor/stonks-silo/index.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| （ページの先頭） | `financial_as_of`（この段階の書き手あり）<br>`generated_at`（この段階の書き手あり）<br>`latest_quarter_end`（この段階の書き手あり）<br>`length`（この段階の書き手あり）<br>`stale`（この段階の書き手あり）<br>`tickers`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断: 一覧の表・買い／回避シグナル・黒字化への道のり・拡大再生産・①〜③・推移のグラフ・財務トレンド・使い方のすべて） |
| Table | `adjusted_eps`（ほかの書き手あり）<br>`breakeven_estimate`（ほかの書き手あり）<br>`breakeven_reason`（ほかの書き手あり）<br>`cagr_3yr`（この段階の書き手あり）<br>`capex_annual`（この段階の書き手あり）<br>`cash`（この段階の書き手あり）<br>`current_price`（この段階の書き手あり）<br>`deficit_fixed_risk`（この段階の書き手あり）<br>`deficit_quality`（この段階の書き手あり）<br>`dilution_risk`（この段階の書き手あり）<br>`discontinuous_growth`（この段階の書き手あり）<br>`discontinuous_growth_note`（この段階の書き手あり）<br>`end`（この段階の書き手あり）<br>`fields`（この段階の書き手あり）<br>`financial_as_of`（この段階の書き手あり）<br>`financial_vectors`（この段階の書き手あり）<br>`gaap_breakeven_reason`（この段階の書き手あり）<br>`gaap_breakeven_year`（この段階の書き手あり）<br>`gross_margin`（この段階の書き手あり）<br>`gross_margin_derived`（この段階の書き手あり）<br>`hidden_profit_already`（この段階の書き手あり）<br>`incremental_gp_delta`（この段階の書き手あり）<br>`incremental_margin`（この段階の書き手あり）<br>`incremental_margin_prev`（この段階の書き手あり）<br>`incremental_margin_trend`（この段階の書き手あり）<br>`incremental_rev_delta`（この段階の書き手あり）<br>`latest_quarter_end`（この段階の書き手あり）<br>`length`（この段階の書き手あり）<br>`mature_profit`（この段階の書き手あり）<br>`mature_profit_note`（この段階の書き手あり）<br>`monthly_burn`（この段階の書き手あり）<br>`net_income_fy`（この段階の書き手あり）<br>`next_earnings_date`（ほかの書き手あり）<br>`ocf_annual`（この段階の書き手あり）<br>`ocf_breakeven_reason`（この段階の書き手あり）<br>`ocf_breakeven_year`（この段階の書き手あり）<br>`ocf_trend`（この段階の書き手あり）<br>`overall_score`（この段階の書き手あり）<br>`overall_verdict`（この段階の書き手あり）<br>`path_score_basis`（この段階の書き手あり）<br>`profitability_path`（この段階の書き手あり）<br>`quarters`（ほかの書き手あり）<br>`records`（この段階の書き手あり）<br>`reproduction_label`（この段階の書き手あり）<br>`reproduction_score`（この段階の書き手あり）<br>`revenue`（この段階の書き手あり）<br>`revenue_outlier_years`（この段階の書き手あり）<br>`rnd_ratio`（この段階の書き手あり）<br>`rule40_cagr3y_opmargin`（この段階の書き手あり）<br>`runway`（この段階の書き手あり）<br>`runway_months`（この段階の書き手あり）<br>`sbc_adjusted_fcf`（この段階の書き手あり）<br>`sbc_ratio`（この段階の書き手あり）<br>`sbc_yoy_change`（この段階の書き手あり）<br>`score`（この段階の書き手あり）<br>`series_q`（この段階の書き手あり）<br>`sm_ratio`（この段階の書き手あり）<br>`stale`（この段階の書き手あり）<br>`stale_threshold_days`（この段階の書き手あり）<br>`summary`（この段階の書き手あり）<br>`tanuki_score`（ほかの書き手あり）<br>`val`（この段階の書き手あり）<br>`valuation`（この段階の書き手あり）<br>`verdict`（この段階の書き手あり）<br>`verdict_reason`（この段階の書き手あり）<br>`years`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断: 一覧の表・買い／回避シグナル・黒字化への道のり・拡大再生産・①〜③・推移のグラフ・財務トレンド・使い方のすべて） |
| （区画を決められない） | `change_pct`（この段階の書き手あり）<br>`current_price`（この段階の書き手あり）<br>`end`（この段階の書き手あり）<br>`ev_sales`（この段階の書き手あり）<br>`fields`（この段階の書き手あり）<br>`financial_vectors`（この段階の書き手あり）<br>`label`（ほかの書き手あり）<br>`latest_quarter_end`（この段階の書き手あり）<br>`length`（この段階の書き手あり）<br>`market_cap`（この段階の書き手あり）<br>`net_cash`（この段階の書き手あり）<br>`psr`（この段階の書き手あり）<br>`qoq`（この段階の書き手あり）<br>`sales_basis`（この段階の書き手あり）<br>`scoring_fy`（この段階の書き手あり）<br>`scoring_fy_end`（この段階の書き手あり）<br>`series_q`（この段階の書き手あり）<br>`stale`（この段階の書き手あり）<br>`type`（ほかの書き手あり）<br>`val`（この段階の書き手あり）<br>`valuation`（この段階の書き手あり）<br>`yoy`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断: 一覧の表・買い／回避シグナル・黒字化への道のり・拡大再生産・①〜③・推移のグラフ・財務トレンド・使い方のすべて） |

## ワークフローが自分で書く出力（.pyの外）

段階の.pyを動かすワークフローの中で、`$GITHUB_STEP_SUMMARY`に書く段。`.get()`で読むキーが、この段階のJSON（全ファイル）にあるか。

- `.github/workflows/Stonks_Silo_Update.yml`（行 75・76・77・78・87）: `overall_score`（ある）・`overall_verdict`（ある）・`tickers`（ある）

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | STONKSか | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:22` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。STONKSのファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | STONKSを含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:723`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:866`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:894` | いいえ |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:817`<br>`common/sec_data/layer3_builder.py:1055`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:229`<br>`src/tail/edgar_rss_monitor.py:433`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:605` | いいえ |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1585`<br>`common/sec_data/report_consistency_check.py:1726`<br>`common/system_health.py:955`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192`<br>`src/value/stonks_silo/financial_as_of.py:70` | はい |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:402`<br>`src/value/hypecore/hypecore.py:284` | いいえ |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:389`<br>`src/tail/xbrl_segment_fetcher.py:196`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | いいえ |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:143` | いいえ |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 5 | `docs/portfolio/index.html:650`<br>`docs/value-monitor/tanuki_score/index.html:571`<br>`docs/value-monitor/tanuki_score/index.html:1094`<br>`docs/value-monitor/tanuki_score/index.html:1323`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1135`<br>`docs/value-monitor/tanuki_valuation/stock.html:1136`<br>`src/value/tanuki_valuation/pipeline.py:622`<br>`src/value/tanuki_valuation/pipeline.py:624`<br>`src/value/tanuki_valuation/pipeline.py:626`<br>`src/value/tanuki_valuation/pipeline.py:865`<br>`src/value/tanuki_valuation/pipeline.py:878`<br>`src/value/tanuki_valuation/pipeline.py:898`<br>`src/value/tanuki_valuation/pipeline.py:900`<br>`src/value/tanuki_valuation/pipeline.py:967`<br>`src/value/tanuki_valuation/pipeline.py:968`<br>`src/value/tanuki_valuation/pipeline.py:969`<br>`src/value/tanuki_valuation/pipeline.py:970`<br>`src/value/tanuki_valuation/pipeline.py:1355`<br>`src/value/tanuki_valuation/pipeline.py:1382`<br>`src/value/tanuki_valuation/pipeline.py:1387`<br>`src/value/tanuki_valuation/pipeline.py:1433`<br>`src/value/tanuki_valuation/pipeline.py:1738`<br>`src/value/tanuki_valuation/pipeline.py:1916` | いいえ |
| β・WACCの作り方 | 3 | `src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1121` | いいえ |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:220`<br>`src/value/tanuki_valuation/calculator/adjustments.py:184`<br>`src/value/tanuki_valuation/data_fetcher.py:114`<br>`src/value/tanuki_valuation/data_fetcher.py:1105` | いいえ |

1か所だけのもの: 成長率の上限・下限（1）

## 本番外のコード（src/value/stonks_silo）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

候補は0件。見たファイル（6ファイル・2,514行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |
|---|---|---|---|---|---|
| `analyzer.py` | 1,342 | — | — | `src/value/stonks_silo/pipeline.py` | 7件 |
| `fetcher.py` | 298 | — | — | `src/value/stonks_silo/analyzer.py`<br>`src/value/stonks_silo/pipeline.py` | 0件 |
| `financial_as_of.py` | 104 | — | — | `src/value/stonks_silo/pipeline.py` | 3件 |
| `financial_trend_calculator.py` | 393 | — | — | `src/value/stonks_silo/pipeline.py` | 3件 |
| `pipeline.py` | 256 | `.github/workflows/Stonks_Silo_Update.yml` | — | — | 4件 |
| `valuation_fetcher.py` | 121 | — | — | `src/value/stonks_silo/pipeline.py` | 1件 |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 1件
  - `src/value/stonks_silo/fetcher.py:73` load_annual_data → `<?>`
