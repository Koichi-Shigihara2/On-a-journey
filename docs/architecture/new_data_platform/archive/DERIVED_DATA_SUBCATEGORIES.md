# DERIVED_DATA_SUBCATEGORIES.md — 導出データの性格別サブ分類（着手前ステップ）

作成日: 2026-07-23
出発点: `TO_BE_FINAL_LIST.md`ステップ7（ステップ6確定後499件ベース）の
「導出データ」バケット

## 本ドキュメントの位置づけ

`FIELD_DEFINITIONS.md`はこれまでシステム設定データ（15件）・移送データ
（6件）・一次データ（29件→本ドキュメント作成過程での訂正を経て42件）・
手動入力データ（44件）の定義を終えている。残る導出データは母数が最大
（当初405件）かつ性質が雑多であるため、個別定義に入る前に性格別の
サブ分類を行い、段階的に定義作業を進める土台とする。

**本ドキュメントは分類のみを対象とし、個々の項目の計算式分解・定義記述は
次フェーズ以降で行う。実装（コード修正）は一切行っていない。**

---

## 着手前の訂正: 導出データ405件→392件（計13件を一次データへ再分類）

`TO_BE_FINAL_LIST.md`ステップ7の時点で導出データ405件とされていたが、
本ドキュメント作成の過程で以下の2段階の訂正を行った。いずれも
「外部ソースの値をそのまま採用しているだけで計算式を伴わない」項目
であり、`FIELD_DEFINITIONS.md`フェーズ2で確立した基準
（「優先順位の選択・フォールバックは計算ではない」）に基づいて判定した。

### 訂正1: HypeCoreの8件（依頼文で明示的に指摘された項目）

`hypecore.py:fetch_info_snapshot()`内で`info.get(...)`によりyfinance
`.info`の単一フィールドをそのまま取得しているだけで、計算を一切加えて
いないことをコードで確認した。

| AS-IS ID | 項目名 | 根拠コード |
|---|---|---|
| AS-IS-097 | `forward_pe` | `info.get("forwardPE")` |
| AS-IS-098 | `peg_ratio` | `info.get("pegRatio")` |
| AS-IS-099 | `psr` | `info.get("priceToSalesTrailing12Months")` |
| AS-IS-100 | `revenue_growth` | `info.get("revenueGrowth")` |
| AS-IS-101 | `earnings_growth` | `info.get("earningsGrowth")` |
| AS-IS-102 | `recommendation_mean` | `info.get("recommendationMean")` |
| AS-IS-103 | `short_pct_float` | `info.get("shortPercentOfFloat")` |
| AS-IS-117 | `ev_ebitda` | `info.get("enterpriseToEbitda")` |

いずれも`fetch_info_snapshot()`の戻り値を`compute_scores()`内で
`df.loc[today_ts, key] = info.get(key)`という形で最新月にそのまま
代入しているのみで、計算処理は介在しない。

### 訂正2: 横断検索で追加発見した5件

HypeCore8件の調査中、同一パターン（外部ソース単一値のそのまま採用・
選択・丸めのみ）を持つ項目が他にもないか、「そのまま」「yfinance」
「\.info」「history\(」等のキーワードで397件全体を横断検索し、
以下5件を追加で発見・コード確認した。

| AS-IS ID | サブシステム | 項目名 | 根拠コード |
|---|---|---|---|
| AS-IS-084 | HypeCore | `price` | `fetch_price_data():80` `resample("ME").agg({"price":"last"})`（月末値の選択、計算なし） |
| AS-IS-125 | STONKS SILO | `years` | `fetcher.load_annual_data()`が返す年リストのそのまま透過 |
| AS-IS-130 | STONKS SILO | `valuation.market_cap` | `valuation_fetcher.py:8` `info.get("marketCap")` |
| AS-IS-131 | STONKS SILO | `valuation.current_price` | `valuation_fetcher.py:9` `info.get("currentPrice") or info.get("regularMarketPrice")`（フォールバック選択） |
| AS-IS-185 | MACRO PULSE | 「1Y EXPECTED FF」 | `get_implied_cuts():737-753` `round(dgs1,4)`のみでFRED `DGS1`をそのまま採用、四捨五入以外の計算式は不使用 |

**この13件はいずれも`TO_BE_FINAL_LIST.md`の一次データ表に追加し、
同ファイルのステップ7集計（515件ベース・499件ベース）も更新済み。**
確定後の内訳: 一次データ42件（29+13）、導出データ392件（405-13）。
詳細は`TO_BE_FINAL_LIST.md`ステップ7を参照。

---

## サブ分類の方法

以下8分類を出発点とし、実際の397→392件の内容を確認しながら
キーワード・コード参照に基づき機械的に振り分けた。当初の想定より
「その他」バケットが大きくなりすぎた（一次分類で87件）ため、
個別内容を確認して以下のルールで再配分し、最終的に30件まで縮小した:

- **評価倍率・バリュエーション系**: PER/PEG/PSR/EV_EBITDA/upside_percent/
  price_iv_ratio等、株価と企業価値・売上・利益の比率
- **DCF/WACC構成要素系**: intrinsic_value、WACC、成長率アサンプション、
  シナリオ前提、感応度分析、ターミナル成長率等のDCF計算チェーン構成要素
  （TANUKI TAILのstage2 DCFシナリオも含む）
- **成長率・トレンド系**: CAGR、YoY、モメンタムスコア、RSI、MA乖離、
  ライフサイクル/ステージ判定等の企業個別テクニカル・成長トレンド指標
- **キャッシュフロー・収益性系**: FCF、OCF、moat_score、粗利率、
  ランウェイ、EPS/純利益の実額等
- **信頼性・品質判定系**: 総合スコア/verdict（TANUKI VALUATIONの
  tanuki_score等含む）、DCF Reliability、内部統制有効性、AI分析の
  confidence、ポジション健全性（TANUKI TAIL stage1）等の判定・
  メタ情報全般
- **マクロ・市場環境系**: MACRO PULSE・Market Pulseのほぼ全項目
  （regime、RECESSION RISK SCORE、sentiment、fear_greed、breadth、
  credit判定、資金フロー等）
- **カタリスト・イベント予測系**: Discoverのcatalysts/macro_themes、
  次回決算日、黒字化目算、EPS Analyzerのyoy_growth/eps_diff、
  TANUKI TAILのKPI予測・実績・予測精度検証・AI視点分析（call2）等の
  将来事象・予測関連
- **その他**: 上記に当てはまらない、構造用コンテナキー・単純インデックス・
  識別子・内部設定/監視状態等の非分析的項目

当初の8分類定義から、以下2点を実態に合わせて調整した:
1. TANUKI VALUATIONの`tanuki_score`/`funda_score`/`timing_score`等の
   総合スコア・判定は「信頼性・品質判定系」に統合（DCFの単なる出力では
   なく、判定・verdict的性格が強いため）
2. TANUKI TAILのKPI予測・実績突合・AI視点分析（call2以下）は
   「カタリスト・イベント予測系」に統合（将来の企業イベント・KPI動向を
   扱う点でDiscoverのcatalystsと同じ性格のため）

---

## サブ分類 集計結果（392件、2026-09-19訂正後383件・後述）

| サブカテゴリ | 件数 |
|---|---|
| マクロ・市場環境系 | 124件 |
| 信頼性・品質判定系 | 60件 |
| カタリスト・イベント予測系 | 50件 |
| DCF/WACC構成要素系 | 48件（訂正後、後述） |
| 成長率・トレンド系 | 43件 |
| その他 | 18件（2026-09-19訂正後、後述。フェーズ9時点は27件） |
| キャッシュフロー・収益性系 | 27件 |
| 評価倍率・バリュエーション系 | 13件 |
| **合計** | **383件（2026-09-19訂正後。フェーズ9時点は392件）** |

**訂正（フェーズ9・FIELD_DEFINITIONS.md作成時）**: AS-IS-447
（current_price）・AS-IS-453（kpi_layer1_keys）・AS-IS-454
（kpi_format）の3件は、`tail_dcf_bridge.py:generate_scenario_files()`が
生成するシナリオファイルの一部であり、既にDCF/WACC構成要素系として
定義済みのAS-IS-442〜451と完全に同一の戻り値オブジェクトに属することが
判明したため、「その他」から「DCF/WACC構成要素系」へ再分類した
（その他30→27件、DCF/WACC構成要素系45→48件、合計392件は不変）。
詳細は`FIELD_DEFINITIONS.md`フェーズ9セクション参照。

**訂正2（2026-09-19、[[FIVE-CATEGORY-RECLASSIFY-1]]）**: フェーズ9で
「次の判断機会に委ねる」としていたAS-IS-437〜441（tail_kpi_map.json、
5件）・AS-IS-404（last_filed、1件）・AS-IS-057/058/060（Reverse DCF
比較表の行ラベル、3件）の5分類再判定が確定した（詳細は
`FIELD_DEFINITIONS.md`フェーズ9「判断確定（2026-09-19追記）」参照）:
AS-IS-437〜441は「導出データ」→「手動入力データ」、AS-IS-404は
「導出データ」→「システム設定データ」へ再分類（＝いずれも「導出データ」
の母数から離脱）、AS-IS-057/058/060はカタログから除外。この結果、
「その他」バケットは27→18件（9件全て除去）、導出データ全体は392→
**383件**となる。下記「サブ分類 集計結果」表・「サブシステム別内訳」の
数値はいずれも訂正後（383件・その他18件）で記載している。

### サブシステム別内訳（クロス集計）

| サブシステム | 主な分布 |
|---|---|
| Market Pulse（79件） | マクロ・市場環境系100% |
| MACRO PULSE（45件） | マクロ・市場環境系100% |
| Discover（16件） | カタリスト・イベント予測系100% |
| TANUKI VALUATION（68件） | DCF/WACC構成要素系28件、信頼性・品質判定系14件、評価倍率8件、CF収益性8件、その他6件、カタリスト2件、成長率2件 |
| HypeCore（29件） | 成長率・トレンド系21件、信頼性・品質判定系4件、カタリスト4件、その他3件、評価倍率3件、CF収益性1件（※延べ件数は各カテゴリ内訳の合計であり重複計上ではない） |
| STONKS SILO（49件） | 成長率・トレンド系20件、CF収益性12件、信頼性・品質判定系11件、カタリスト3件、評価倍率2件、その他1件 |
| TANUKI TAIL（71件） | カタリスト・イベント予測系21件、信頼性・品質判定系18件、DCF/WACC構成要素系17件、その他15件 |
| TANUKI SCORE（11件） | 信頼性・品質判定系7件、その他4件 |
| EPS Analyzer（13件） | 信頼性・品質判定系6件、カタリスト4件、CF収益性3件 |
| Portfolio（4件） | CF収益性3件、その他1件 |

サブシステムごとの分布は各サブシステムの機能的性格と一致している
（Market Pulse/MACRO PULSEは市場環境監視に特化、Discoverはカタリスト
発見に特化、TANUKI VALUATIONはDCFエンジンが主体、HypeCoreは
ライフサイクル/モメンタム判定が主体、STONKS SILOは赤字企業の成長性・
CF・ランウェイ判定が主体）ため、この分類は妥当と判断する。

---

## サブカテゴリ別 明細一覧

### 評価倍率・バリュエーション系（13件）

| AS-IS ID | サブシステム | 項目名 |
|---|---|---|
| AS-IS-002 | 5-1. TANUKI VALUATION | intrinsic_value_beta |
| AS-IS-003 | 5-1. TANUKI VALUATION | upside_percent_beta |
| AS-IS-004 | 5-1. TANUKI VALUATION | intrinsic_value_rf |
| AS-IS-005 | 5-1. TANUKI VALUATION | upside_percent_rf |
| AS-IS-006 | 5-1. TANUKI VALUATION | upside_percent |
| AS-IS-031 | 5-1. TANUKI VALUATION | per_adjusted |
| AS-IS-055 | 5-1. TANUKI VALUATION | ① |
| AS-IS-056 | 5-1. TANUKI VALUATION | ② |
| AS-IS-113 | 5-2. HypeCore | `expectation_score` |
| AS-IS-116 | 5-2. HypeCore | `price_iv_ratio` |
| AS-IS-122 | 5-2. HypeCore | バリュエーション倍率パネル（PER/PS/PEG/EV-EBITDA） |
| AS-IS-132 | 5-3. STONKS SILO | `valuation.psr` |
| AS-IS-133 | 5-3. STONKS SILO | `valuation.ev_sales` |

### DCF/WACC構成要素系（48件、フェーズ9でAS-IS-447/453/454を「その他」から再分類後）

| AS-IS ID | サブシステム | 項目名 |
|---|---|---|
| AS-IS-001 | 5-1. TANUKI VALUATION | intrinsic_value_per_share |
| AS-IS-007 | 5-1. TANUKI VALUATION | v0 |
| AS-IS-008 | 5-1. TANUKI VALUATION | v0_adjusted |
| AS-IS-009 | 5-1. TANUKI VALUATION | alpha / alpha_was_capped |
| AS-IS-010 | 5-1. TANUKI VALUATION | future_values |
| AS-IS-011 | 5-1. TANUKI VALUATION | return_metrics |
| AS-IS-012 | 5-1. TANUKI VALUATION | growth.rate/source |
| AS-IS-013 | 5-1. TANUKI VALUATION | wacc.value/beta/risk_free_rate/market_return |
| AS-IS-014 | 5-1. TANUKI VALUATION | sensitivity.matrix/wacc_values/growth_years |
| AS-IS-015 | 5-1. TANUKI VALUATION | scenario_valuations.bear/base/bull |
| AS-IS-016 | 5-1. TANUKI VALUATION | growth_options.total_pv/count/options |
| AS-IS-017 | 5-1. TANUKI VALUATION | maturity_profile |
| AS-IS-018 | 5-1. TANUKI VALUATION | dcf_components.*（v0,pv_high_growth,pv_terminal,high_growth_detail,terminal_fcf,terminal_value等） |
| AS-IS-019 | 5-1. TANUKI VALUATION | fcf_base.base_fcf/method/cv |
| AS-IS-020 | 5-1. TANUKI VALUATION | fcf_outlier.detected/rule/action/note/deviation_pct |
| AS-IS-021 | 5-1. TANUKI VALUATION | fcf_estimation.applied/conversion_rate/estimated_fcf等 |
| AS-IS-022 | 5-1. TANUKI VALUATION | software_system_reclassification.* |
| AS-IS-023 | 5-1. TANUKI VALUATION | rd_capitalization.* |
| AS-IS-024 | 5-1. TANUKI VALUATION | rpo_adjustment.rpo_pv/application_rate/sector_category/rpo_incremental等 |
| AS-IS-025 | 5-1. TANUKI VALUATION | bs_adjustment.net_cash/net_cash_per_share/sector_guard |
| AS-IS-027 | 5-1. TANUKI VALUATION | rice.q/cf_conversion/q_years/cf_years/avg_intensity/avg_rev_growth/vc_factor/bear・base・bull |
| AS-IS-029 | 5-1. TANUKI VALUATION | pv_high / pv_terminal |
| AS-IS-030 | 5-1. TANUKI VALUATION | alpha_uncapped |
| AS-IS-059 | 5-1. TANUKI VALUATION | terminal_growthの出所 |
| AS-IS-064 | 5-1. TANUKI VALUATION | 将来価値予測（シナリオ別テーブル） |
| AS-IS-065 | 5-1. TANUKI VALUATION | 5年BASE年率換算リターン |
| AS-IS-066 | 5-1. TANUKI VALUATION | 感応度分析（独自5×5マトリクス） |
| AS-IS-067 | 5-1. TANUKI VALUATION | Reverse DCF |
| AS-IS-442 | 1-10. TANUKI TAIL | assumptions.Y1_growth / Y2_growth / Y3_growth |
| AS-IS-443 | 1-10. TANUKI TAIL | assumptions.terminal_growth |
| AS-IS-444 | 1-10. TANUKI TAIL | assumptions.operating_margin |
| AS-IS-445 | 1-10. TANUKI TAIL | assumptions.weighted_growth |
| AS-IS-446 | 1-10. TANUKI TAIL | base_intrinsic_value |
| AS-IS-447 | 1-10. TANUKI TAIL | current_price（フェーズ9でその他から再分類） |
| AS-IS-448 | 1-10. TANUKI TAIL | future_values["1年後"] |
| AS-IS-449 | 1-10. TANUKI TAIL | future_values["3年後"] |
| AS-IS-450 | 1-10. TANUKI TAIL | future_values["5年後"] |
| AS-IS-451 | 1-10. TANUKI TAIL | kpi_forecasts["1年後"/"3年後"].{KPI名} |
| AS-IS-453 | 1-10. TANUKI TAIL | kpi_layer1_keys（フェーズ9でその他から再分類） |
| AS-IS-454 | 1-10. TANUKI TAIL | kpi_format.{KPI名}（フェーズ9でその他から再分類） |
| AS-IS-492 | 1-10. TANUKI TAIL | stage2.scenarios.{bear,base,bull}.revenue_growth_y1/y2/y3 |
| AS-IS-493 | 1-10. TANUKI TAIL | stage2.scenarios.{...}.terminal_growth |
| AS-IS-494 | 1-10. TANUKI TAIL | stage2.scenarios.{...}.operating_margin_terminal |
| AS-IS-495 | 1-10. TANUKI TAIL | stage2.scenarios.{...}.rationale |
| AS-IS-496 | 1-10. TANUKI TAIL | stage2.scenarios.{...}.kpi_forecasts["1年後"/"3年後"][KPI名] |
| AS-IS-497 | 1-10. TANUKI TAIL | stage2.key_assumptions |
| AS-IS-498 | 1-10. TANUKI TAIL | stage2.risk_factors |
| AS-IS-509 | 1-10. TANUKI TAIL | scenario |

### 成長率・トレンド系（43件）

| AS-IS ID | サブシステム | 項目名 |
|---|---|---|
| AS-IS-068 | 5-1. TANUKI VALUATION | FCF CAGR(3yr) |
| AS-IS-076 | 5-1. TANUKI VALUATION | 200MA乖離 |
| AS-IS-077 | 5-2. HypeCore | HypeCore `stage_label` |
| AS-IS-079 | 5-2. HypeCore | STONKS SILO `deficit_quality.revenue_growth_pct` |
| AS-IS-085 | 5-2. HypeCore | `stage` |
| AS-IS-086 | 5-2. HypeCore | `stage_label` |
| AS-IS-087 | 5-2. HypeCore | `ma200_dev` |
| AS-IS-088 | 5-2. HypeCore | `ma50_dev` |
| AS-IS-089 | 5-2. HypeCore | `from_peak` |
| AS-IS-090 | 5-2. HypeCore | `rsi` |
| AS-IS-091 | 5-2. HypeCore | `volume_ratio` |
| AS-IS-092 | 5-2. HypeCore | `vol_surge` |
| AS-IS-093 | 5-2. HypeCore | `rev_yoy` |
| AS-IS-094 | 5-2. HypeCore | `ni_yoy` |
| AS-IS-095 | 5-2. HypeCore | `rule40` |
| AS-IS-109 | 5-2. HypeCore | `substage_phase` |
| AS-IS-110 | 5-2. HypeCore | `substage_label` |
| AS-IS-111 | 5-2. HypeCore | `substage_watch` |
| AS-IS-112 | 5-2. HypeCore | `substage_next` |
| AS-IS-115 | 5-2. HypeCore | `momentum_score` |
| AS-IS-118 | 5-2. HypeCore | `low_base_effect` |
| AS-IS-119 | 5-2. HypeCore | ライフサイクル（黎明/成長/拡大/成熟） |
| AS-IS-121 | 5-2. HypeCore | 1ヶ月後のステージ遷移確率 |
| AS-IS-135 | 5-3. STONKS SILO | `financial_vectors.fields.*` |
| AS-IS-136 | 5-3. STONKS SILO | `cagr_3yr` |
| AS-IS-137 | 5-3. STONKS SILO | `rnd_ratio` |
| AS-IS-138 | 5-3. STONKS SILO | `sm_ratio` |
| AS-IS-143 | 5-3. STONKS SILO | `rule_of_40` |
| AS-IS-151 | 5-3. STONKS SILO | `revenue_outlier_years` |
| AS-IS-152 | 5-3. STONKS SILO | `revenue_growth_pct` |
| AS-IS-161 | 5-3. STONKS SILO | `ocf_trend` |
| AS-IS-165 | 5-3. STONKS SILO | `discontinuous_growth` |
| AS-IS-166 | 5-3. STONKS SILO | `discontinuous_growth_note` |
| AS-IS-167 | 5-3. STONKS SILO | `incremental_margin` |
| AS-IS-168 | 5-3. STONKS SILO | `incremental_margin_prev` |
| AS-IS-169 | 5-3. STONKS SILO | `incremental_margin_trend` |
| AS-IS-170 | 5-3. STONKS SILO | `incremental_rev_delta`/`incremental_gp_delta` |
| AS-IS-171 | 5-3. STONKS SILO | `reproduction_score` |
| AS-IS-172 | 5-3. STONKS SILO | `reproduction_label` |
| AS-IS-174 | 5-3. STONKS SILO | `fields.{name}.yoy/qoq.change_pct,val_latest,val_prev,end_latest,end_prev,fp` |
| AS-IS-175 | 5-3. STONKS SILO | `fields.{name}.yoy/qoq.percentile` |
| AS-IS-176 | 5-3. STONKS SILO | `fields.{name}.yoy/qoq.angle,length` |
| AS-IS-177 | 5-3. STONKS SILO | `fields.{name}.series_q`（四半期時系列） |

### キャッシュフロー・収益性系（27件）

| AS-IS ID | サブシステム | 項目名 |
|---|---|---|
| AS-IS-026 | 5-1. TANUKI VALUATION | moat_score系（components.moat_score等） |
| AS-IS-028 | 5-1. TANUKI VALUATION | moat_score / moat_phase1_years / moat_gross_margin_norm / moat_roic_norm / moat_fcf_margin_norm |
| AS-IS-045 | 5-1. TANUKI VALUATION | financial_health.*（net_debt,total_debt,cash_and_equivalents,sbc_ttm,dilution_3yr_annual_pct等） |
| AS-IS-046 | 5-1. TANUKI VALUATION | dupont.net_margin/asset_turnover/financial_leverage/roe_decomposed |
| AS-IS-047 | 5-1. TANUKI VALUATION | fcf_history[] |
| AS-IS-049 | 5-1. TANUKI VALUATION | computed_runway_months |
| AS-IS-071 | 5-1. TANUKI VALUATION | キャッシュフロー分析セクション |
| AS-IS-073 | 5-1. TANUKI VALUATION | 平均Moat |
| AS-IS-096 | 5-2. HypeCore | `fcf_yield` |
| AS-IS-139 | 5-3. STONKS SILO | `gross_margin` |
| AS-IS-144 | 5-3. STONKS SILO | `mature_profit` |
| AS-IS-145 | 5-3. STONKS SILO | `mature_profit_note` |
| AS-IS-146 | 5-3. STONKS SILO | `sbc_adjusted_fcf` |
| AS-IS-147 | 5-3. STONKS SILO | `sbc_ratio` |
| AS-IS-148 | 5-3. STONKS SILO | `sbc_yoy_change` |
| AS-IS-153 | 5-3. STONKS SILO | `cash` |
| AS-IS-154 | 5-3. STONKS SILO | `monthly_burn` |
| AS-IS-155 | 5-3. STONKS SILO | `runway_months` |
| AS-IS-156 | 5-3. STONKS SILO | `ocf_annual` |
| AS-IS-157 | 5-3. STONKS SILO | `capex_annual` |
| AS-IS-160 | 5-3. STONKS SILO | `ocf_annual`（年次dict） |
| AS-IS-267 | 5-6. EPS Analyzer | `quarters[].gaap_eps/adjusted_eps/gaap_net_income/adjusted_net_income/diluted_shares_used/adjustments/net_adjustment_total` |
| AS-IS-274 | 5-6. EPS Analyzer | `gaap_eps/adjusted_eps` |
| AS-IS-281 | 5-6. EPS Analyzer | `ttm.json`（`ttm[].period/net_income/adjusted_income/diluted_shares/eps/adjusted_eps`） |
| AS-IS-391 | 1-9. Portfolio | total_assets_usd |
| AS-IS-392 | 1-9. Portfolio | total_assets_jpy |
| AS-IS-393 | 1-9. Portfolio | total_pnl_usd |

### 信頼性・品質判定系（60件）

| AS-IS ID | サブシステム | 項目名 |
|---|---|---|
| AS-IS-033 | 5-1. TANUKI VALUATION | max_eps / max_eps_per / max_eps_reliability |
| AS-IS-034 | 5-1. TANUKI VALUATION | tanuki_score |
| AS-IS-035 | 5-1. TANUKI VALUATION | funda_score |
| AS-IS-036 | 5-1. TANUKI VALUATION | score_comment |
| AS-IS-037 | 5-1. TANUKI VALUATION | timing_score |
| AS-IS-038 | 5-1. TANUKI VALUATION | sell_reason |
| AS-IS-039 | 5-1. TANUKI VALUATION | pre_rounding_score |
| AS-IS-040 | 5-1. TANUKI VALUATION | rounded_by_policy |
| AS-IS-041 | 5-1. TANUKI VALUATION | matrix.*（quadrant/label/key_metric_y/qx/qy） |
| AS-IS-042 | 5-1. TANUKI VALUATION | growth_sanity.verdict/signals/warnings/recommended_g |
| AS-IS-043 | 5-1. TANUKI VALUATION | phase1_growth_auto_adjusted |
| AS-IS-044 | 5-1. TANUKI VALUATION | fcf_margin_bear_mult_applied |
| AS-IS-052 | 5-1. TANUKI VALUATION | validation.* |
| AS-IS-053 | 5-1. TANUKI VALUATION | dilution_severity / dilution_comment |
| AS-IS-078 | 5-2. HypeCore | HypeCore `expectation_score` |
| AS-IS-107 | 5-2. HypeCore | `sell_on_good_news` |
| AS-IS-114 | 5-2. HypeCore | `fundamental_score` |
| AS-IS-120 | 5-2. HypeCore | HypeCore推奨（買い/保有/売り等） |
| AS-IS-126 | 5-3. STONKS SILO | `overall_score` |
| AS-IS-127 | 5-3. STONKS SILO | `overall_verdict` |
| AS-IS-128 | 5-3. STONKS SILO | `summary` |
| AS-IS-140 | 5-3. STONKS SILO | `gross_margin_derived` |
| AS-IS-141 | 5-3. STONKS SILO | `verdict` |
| AS-IS-142 | 5-3. STONKS SILO | `score` |
| AS-IS-149 | 5-3. STONKS SILO | `dilution_risk` |
| AS-IS-150 | 5-3. STONKS SILO | `deficit_fixed_risk` |
| AS-IS-158 | 5-3. STONKS SILO | `verdict` |
| AS-IS-159 | 5-3. STONKS SILO | `score` |
| AS-IS-173 | 5-3. STONKS SILO | `score` |
| AS-IS-268 | 5-6. EPS Analyzer | `quarters[].adjustments[].item_name/reason/extracted_from` |
| AS-IS-269 | 5-6. EPS Analyzer | `quarters[].adjustments[].net_amount` |
| AS-IS-270 | 5-6. EPS Analyzer | `quarters[].ai_analysis.health/comment` |
| AS-IS-271 | 5-6. EPS Analyzer | `quarters[].ai_analysis.sources[].item/snippet/confidence` |
| AS-IS-272 | 5-6. EPS Analyzer | `quarters[].special_flags(EPS_DISCREPANCY)` / `special_notes.eps_discrepancy` |
| AS-IS-277 | 5-6. EPS Analyzer | `gaap_to_adj_positive` |
| AS-IS-288 | 1-7. TANUKI SCORE | selection_reason |
| AS-IS-291 | 1-7. TANUKI SCORE | category |
| AS-IS-292 | 1-7. TANUKI SCORE | report.fundamental |
| AS-IS-293 | 1-7. TANUKI SCORE | report.expectation |
| AS-IS-294 | 1-7. TANUKI SCORE | report.news |
| AS-IS-295 | 1-7. TANUKI SCORE | report.timing |
| AS-IS-296 | 1-7. TANUKI SCORE | report.summary |
| AS-IS-396 | 1-10. TANUKI TAIL | effective |
| AS-IS-397 | 1-10. TANUKI TAIL | material_weaknesses |
| AS-IS-398 | 1-10. TANUKI TAIL | significant_deficiencies |
| AS-IS-399 | 1-10. TANUKI TAIL | item4_excerpt |
| AS-IS-400 | 1-10. TANUKI TAIL | item4_excerpt_ja |
| AS-IS-417 | 1-10. TANUKI TAIL | layer2_complete |
| AS-IS-418 | 1-10. TANUKI TAIL | missing_kpis |
| AS-IS-424 | 1-10. TANUKI TAIL | kpis.{name}.confidence |
| AS-IS-482 | 1-10. TANUKI TAIL | stage1.health_score |
| AS-IS-483 | 1-10. TANUKI TAIL | stage1.health_label |
| AS-IS-484 | 1-10. TANUKI TAIL | stage1.summary |
| AS-IS-485 | 1-10. TANUKI TAIL | stage1.positives |
| AS-IS-486 | 1-10. TANUKI TAIL | stage1.concerns |
| AS-IS-487 | 1-10. TANUKI TAIL | stage1.recommendation |
| AS-IS-488 | 1-10. TANUKI TAIL | stage1.next_kpis |
| AS-IS-489 | 1-10. TANUKI TAIL | stage1.exit_distance |
| AS-IS-490 | 1-10. TANUKI TAIL | stage1.exit_distance_reason |
| AS-IS-491 | 1-10. TANUKI TAIL | stage1.optimism_bias_warning |

### マクロ・市場環境系（124件）

| AS-IS ID | サブシステム | 項目名 |
|---|---|---|
| AS-IS-182 | 5-4. MACRO PULSE | REGIME |
| AS-IS-183 | 5-4. MACRO PULSE | regime_source |
| AS-IS-184 | 5-4. MACRO PULSE | FF RATE |
| AS-IS-186 | 5-4. MACRO PULSE | IMPLIED CUTS |
| AS-IS-187 | 5-4. MACRO PULSE | FRB主眼(dominant_label) |
| AS-IS-188 | 5-4. MACRO PULSE | 判断理由(ai_reason) |
| AS-IS-189 | 5-4. MACRO PULSE | FOMC日付 |
| AS-IS-191 | 5-4. MACRO PULSE | S&P500前日比 |
| AS-IS-193 | 5-4. MACRO PULSE | 10Y-2Y判定(INVERTED/FLAT/NORMAL) |
| AS-IS-198 | 5-4. MACRO PULSE | NET LIQUIDITY |
| AS-IS-201 | 5-4. MACRO PULSE | 各カードの前月比/前週比(chg) |
| AS-IS-202 | 5-4. MACRO PULSE | 各カードのパーセンタイル/水準バー |
| AS-IS-203 | 5-4. MACRO PULSE | 各カードの解説コメント(m2Comment/nlComment/hyComment/fedComment) |
| AS-IS-204 | 5-4. MACRO PULSE | Hollow Rallyバッジ |
| AS-IS-206 | 5-4. MACRO PULSE | LAYER2（ステルス供給/吸収バッジ） |
| AS-IS-207 | 5-4. MACRO PULSE | LAYER3（NET流動性連続減少週数） |
| AS-IS-208 | 5-4. MACRO PULSE | 警戒アラート文 |
| AS-IS-209 | 5-4. MACRO PULSE | ステルス吸収週数(stealth_absorb_weeks) |
| AS-IS-213 | 5-4. MACRO PULSE | フェーズbadge / phase-sub |
| AS-IS-214 | 5-4. MACRO PULSE | RECESSION RISK SCOREバー・マーカー |
| AS-IS-215 | 5-4. MACRO PULSE | RECESSION RISK SCORE数値 |
| AS-IS-216 | 5-4. MACRO PULSE | シグナルテキスト |
| AS-IS-217 | 5-4. MACRO PULSE | ALERTバナー |
| AS-IS-218 | 5-4. MACRO PULSE | 8指標シグナルグリッド |
| AS-IS-219 | 5-4. MACRO PULSE | スコア比較バー（3ヶ月前/2ヶ月前/前月比/先週比/カスタム） |
| AS-IS-220 | 5-4. MACRO PULSE | surprise_alerts |
| AS-IS-221 | 5-4. MACRO PULSE | 週次カード日付/スコア/フェーズ |
| AS-IS-222 | 5-4. MACRO PULSE | 週差/月差(chg1w/chg1m) |
| AS-IS-223 | 5-4. MACRO PULSE | 総括(summary) |
| AS-IS-224 | 5-4. MACRO PULSE | 要因分析(factor_analysis) |
| AS-IS-225 | 5-4. MACRO PULSE | 注視ポイント(watchpoints) |
| AS-IS-226 | 5-4. MACRO PULSE | 各指標コメント(indicator_comments) |
| AS-IS-227 | 5-4. MACRO PULSE | 週差/月差バッジ(各指標) |
| AS-IS-228 | 5-4. MACRO PULSE | model表示 |
| AS-IS-229 | 5-4. MACRO PULSE | 8指標の値/シグナル(BULL/CAUTION/NEUTRAL/BEAR)/バー位置 |
| AS-IS-230 | 5-4. MACRO PULSE | スコア推移折れ線 |
| AS-IS-231 | 5-4. MACRO PULSE | NBER後退期帯 |
| AS-IS-232 | 5-4. MACRO PULSE | フェーズゾーン背景(0-25/25-52/52-70/70-100) |
| AS-IS-234 | 5-4. MACRO PULSE | レーダーチャート（現在/2019/2001/スライダー） |
| AS-IS-235 | 5-4. MACRO PULSE | 類似度スコア(2019年/2001年、%) |
| AS-IS-237 | 5-4. MACRO PULSE | DATE/INDICATOR/ACTUAL |
| AS-IS-238 | 5-4. MACRO PULSE | PREV |
| AS-IS-239 | 5-4. MACRO PULSE | DIR(↑/↓/→)・CHANGE |
| AS-IS-240 | 5-4. MACRO PULSE | DATE/INDICATOR |
| AS-IS-241 | 5-4. MACRO PULSE | DAYS |
| AS-IS-301 | 1-8. Market Pulse | judgment |
| AS-IS-302 | 1-8. Market Pulse | indicators |
| AS-IS-303 | 1-8. Market Pulse | sentiment |
| AS-IS-304 | 1-8. Market Pulse | fear_greed |
| AS-IS-305 | 1-8. Market Pulse | tech_pulse |
| AS-IS-306 | 1-8. Market Pulse | asset_flow |
| AS-IS-307 | 1-8. Market Pulse | credit |
| AS-IS-308 | 1-8. Market Pulse | take_profit_checklist |
| AS-IS-309 | 1-8. Market Pulse | buy_checklist |
| AS-IS-310 | 1-8. Market Pulse | summary |
| AS-IS-311 | 1-8. Market Pulse | comments_history |
| AS-IS-313 | 1-8. Market Pulse | 上記各指標のchange_percent |
| AS-IS-314 | 1-8. Market Pulse | 上記各指標のchange（絶対値） |
| AS-IS-315 | 1-8. Market Pulse | 上記各指標のvolume_ratio |
| AS-IS-316 | 1-8. Market Pulse | 上記各指標のdate |
| AS-IS-317 | 1-8. Market Pulse | 上記各指標のis_fallback |
| AS-IS-318 | 1-8. Market Pulse | NYSE Composite（value, change_percent, volume_ratio, date） |
| AS-IS-319 | 1-8. Market Pulse | NYSE Composite.divergence_vs_sp |
| AS-IS-323 | 1-8. Market Pulse | グロース対バリュー比.diff_percent |
| AS-IS-324 | 1-8. Market Pulse | 大型対小型比.diff_percent |
| AS-IS-326 | 1-8. Market Pulse | VIX9D対VIX比.value |
| AS-IS-327 | 1-8. Market Pulse | VIX9D対VIX比.contango |
| AS-IS-328 | 1-8. Market Pulse | HYG対LQD比（value, change, date） |
| AS-IS-329 | 1-8. Market Pulse | sentiment.score |
| AS-IS-330 | 1-8. Market Pulse | sentiment.label |
| AS-IS-331 | 1-8. Market Pulse | sentiment.sub_scores.{8指標}.score |
| AS-IS-332 | 1-8. Market Pulse | 同上.weight |
| AS-IS-334 | 1-8. Market Pulse | sentiment.breadth.advances / declines |
| AS-IS-335 | 1-8. Market Pulse | sentiment.breadth.ad_ratio_5d |
| AS-IS-336 | 1-8. Market Pulse | sentiment.breadth.new_highs_52w / new_lows_52w |
| AS-IS-337 | 1-8. Market Pulse | sentiment.breadth.nh_nl_diff |
| AS-IS-338 | 1-8. Market Pulse | sentiment.breadth.pct_above_50ma / pct_above_200ma |
| AS-IS-339 | 1-8. Market Pulse | sentiment.breadth.rsp_spy_divergence_1d |
| AS-IS-340 | 1-8. Market Pulse | sentiment.breadth.rsp_spy_divergence_20d_avg |
| AS-IS-341 | 1-8. Market Pulse | sentiment.breadth.ad_line |
| AS-IS-342 | 1-8. Market Pulse | sentiment.breadth.mcclellan_oscillator |
| AS-IS-343 | 1-8. Market Pulse | sentiment.breadth.date |
| AS-IS-344 | 1-8. Market Pulse | fear_greed.score |
| AS-IS-345 | 1-8. Market Pulse | fear_greed.rating |
| AS-IS-346 | 1-8. Market Pulse | fear_greed.previous_close |
| AS-IS-347 | 1-8. Market Pulse | fear_greed.one_week_ago |
| AS-IS-348 | 1-8. Market Pulse | fear_greed.one_month_ago |
| AS-IS-349 | 1-8. Market Pulse | tech_pulse.score |
| AS-IS-350 | 1-8. Market Pulse | tech_pulse.label |
| AS-IS-351 | 1-8. Market Pulse | tech_pulse.components.qqq_vs_ma125 |
| AS-IS-353 | 1-8. Market Pulse | tech_pulse.components.vxn_vs_ma50 |
| AS-IS-354 | 1-8. Market Pulse | tech_pulse.components.qqq_vs_spy_20d |
| AS-IS-355 | 1-8. Market Pulse | tech_pulse.components.fg_score |
| AS-IS-356 | 1-8. Market Pulse | tech_pulse.components.vxn_available |
| AS-IS-357 | 1-8. Market Pulse | tech_pulse.divergence.value |
| AS-IS-358 | 1-8. Market Pulse | tech_pulse.divergence.zscore |
| AS-IS-359 | 1-8. Market Pulse | tech_pulse.divergence.signal |
| AS-IS-360 | 1-8. Market Pulse | asset_flow.{key}.label / ticker |
| AS-IS-361 | 1-8. Market Pulse | asset_flow.{key}.desc |
| AS-IS-363 | 1-8. Market Pulse | asset_flow.{key}.change_pct |
| AS-IS-364 | 1-8. Market Pulse | asset_flow.{key}.date |
| AS-IS-365 | 1-8. Market Pulse | asset_flow.{key}.is_fallback |
| AS-IS-366 | 1-8. Market Pulse | credit.stock |
| AS-IS-367 | 1-8. Market Pulse | credit.bond |
| AS-IS-368 | 1-8. Market Pulse | credit.credit |
| AS-IS-369 | 1-8. Market Pulse | credit.risk_off_score |
| AS-IS-370 | 1-8. Market Pulse | take_profit_checklist.triggered/fg_score/points/action/checks[] |
| AS-IS-371 | 1-8. Market Pulse | buy_checklist.triggered/extreme/points/action/fg_score/checks.* |
| AS-IS-372 | 1-8. Market Pulse | date |
| AS-IS-373 | 1-8. Market Pulse | advances / declines |
| AS-IS-374 | 1-8. Market Pulse | unchanged |
| AS-IS-375 | 1-8. Market Pulse | ad_ratio_1d |
| AS-IS-376 | 1-8. Market Pulse | ad_ratio_5d |
| AS-IS-377 | 1-8. Market Pulse | new_highs_52w / new_lows_52w |
| AS-IS-378 | 1-8. Market Pulse | nh_nl_diff |
| AS-IS-379 | 1-8. Market Pulse | total_stocks |
| AS-IS-380 | 1-8. Market Pulse | pct_above_50ma / pct_above_200ma |
| AS-IS-381 | 1-8. Market Pulse | rsp_return_1d / spy_return_1d |
| AS-IS-382 | 1-8. Market Pulse | rsp_spy_divergence_1d |
| AS-IS-383 | 1-8. Market Pulse | rsp_spy_divergence_20d_avg |
| AS-IS-384 | 1-8. Market Pulse | ad_line |
| AS-IS-385 | 1-8. Market Pulse | mcclellan_oscillator |
| AS-IS-386 | 1-8. Market Pulse | market_data.csv 各列 |
| AS-IS-387 | 1-8. Market Pulse | extreme-fear参照: date |

### カタリスト・イベント予測系（50件）

| AS-IS ID | サブシステム | 項目名 |
|---|---|---|
| AS-IS-048 | 5-1. TANUKI VALUATION | next_earnings_date |
| AS-IS-051 | 5-1. TANUKI VALUATION | breakeven_estimate |
| AS-IS-104 | 5-2. HypeCore | `eps_surprise` |
| AS-IS-105 | 5-2. HypeCore | `analyst_upgrade_rate` |
| AS-IS-106 | 5-2. HypeCore | `analyst_downgrade_rate` |
| AS-IS-108 | 5-2. HypeCore | `buy_hold_ratio` |
| AS-IS-162 | 5-3. STONKS SILO | `gaap_breakeven_year`/`gaap_breakeven_reason` |
| AS-IS-163 | 5-3. STONKS SILO | `ocf_breakeven_year`/`ocf_breakeven_reason` |
| AS-IS-164 | 5-3. STONKS SILO | `hidden_profit_already` |
| AS-IS-243 | 5-5. Discover | `catalysts[].id` |
| AS-IS-244 | 5-5. Discover | `catalysts[].title/detail/timing/importance/type/probability` |
| AS-IS-245 | 5-5. Discover | `catalysts[].status` |
| AS-IS-246 | 5-5. Discover | `catalysts[].first_detected` |
| AS-IS-248 | 5-5. Discover | 影響予測`{direction, magnitude, thesis_effect, summary}` |
| AS-IS-250 | 5-5. Discover | `classified.items[].{title,category,importance,summary,url,source,published_at}` |
| AS-IS-251 | 5-5. Discover | `classified.summary` |
| AS-IS-252 | 5-5. Discover | `classified.conditions_met[]` / `classified.risk_flags[]` |
| AS-IS-253 | 5-5. Discover | `top_importance`（tickers[ticker]直下） |
| AS-IS-254 | 5-5. Discover | `candidates[].{ticker,company,sector,reason,risk}` |
| AS-IS-255 | 5-5. Discover | `candidates[].screening_pass[]` |
| AS-IS-256 | 5-5. Discover | `candidates[].catalyst_type` |
| AS-IS-257 | 5-5. Discover | `candidates[].conviction` |
| AS-IS-258 | 5-5. Discover | `macro_themes[].{theme,horizon,conviction,background,catalyst}` |
| AS-IS-259 | 5-5. Discover | `macro_themes[].related_tickers[].{ticker,role,note}` |
| AS-IS-260 | 5-5. Discover | `macro_themes[].sources[]` |
| AS-IS-275 | 5-6. EPS Analyzer | `eps_diff` |
| AS-IS-276 | 5-6. EPS Analyzer | `eps_ratio` |
| AS-IS-278 | 5-6. EPS Analyzer | `yoy_growth` |
| AS-IS-279 | 5-6. EPS Analyzer | `health` |
| AS-IS-419 | 1-10. TANUKI TAIL | kpis.{kpi_name}.unit |
| AS-IS-420 | 1-10. TANUKI TAIL | kpis.{kpi_name}.data[].quarter |
| AS-IS-421 | 1-10. TANUKI TAIL | kpis.{kpi_name}.data[].value |
| AS-IS-422 | 1-10. TANUKI TAIL | kpis.{name}.value |
| AS-IS-423 | 1-10. TANUKI TAIL | kpis.{name}.value_numeric |
| AS-IS-452 | 1-10. TANUKI TAIL | kpi_current.{KPI名} |
| AS-IS-499 | 1-10. TANUKI TAIL | call2.five_perspectives.{5観点} |
| AS-IS-500 | 1-10. TANUKI TAIL | call2.entry_story_progress |
| AS-IS-501 | 1-10. TANUKI TAIL | call2.market_attention |
| AS-IS-502 | 1-10. TANUKI TAIL | call2.historical_analogy |
| AS-IS-503 | 1-10. TANUKI TAIL | call2.macro_implications |
| AS-IS-504 | 1-10. TANUKI TAIL | call2.thesis_questions |
| AS-IS-505 | 1-10. TANUKI TAIL | call2.next_review_focus |
| AS-IS-507 | 1-10. TANUKI TAIL | review_quarter |
| AS-IS-508 | 1-10. TANUKI TAIL | forecast_target |
| AS-IS-510 | 1-10. TANUKI TAIL | predictions[KPI名].predicted |
| AS-IS-511 | 1-10. TANUKI TAIL | predictions[KPI名].actual |
| AS-IS-512 | 1-10. TANUKI TAIL | predictions[KPI名].deviation_pct |
| AS-IS-513 | 1-10. TANUKI TAIL | predictions[KPI名].accuracy |
| AS-IS-514 | 1-10. TANUKI TAIL | kpi_forecast_available |
| AS-IS-515 | 1-10. TANUKI TAIL | matchable |

### その他（27件、フェーズ9でAS-IS-447/453/454をDCF/WACC構成要素系へ再分類後）

| AS-IS ID | サブシステム | 項目名 |
|---|---|---|
| AS-IS-050 | 5-1. TANUKI VALUATION | segments[] |
| AS-IS-057 | 5-1. TANUKI VALUATION | 場所 |
| AS-IS-058 | 5-1. TANUKI VALUATION | 用途 |
| AS-IS-060 | 5-1. TANUKI VALUATION | ガード |
| AS-IS-072 | 5-1. TANUKI VALUATION | 銘柄数 |
| AS-IS-074 | 5-1. TANUKI VALUATION | 平均RICE |
| AS-IS-081 | 5-2. HypeCore | `monthly` |
| AS-IS-082 | 5-2. HypeCore | `tickers`（配列） |
| AS-IS-083 | 5-2. HypeCore | `month` |
| AS-IS-124 | 5-3. STONKS SILO | `tickers`（辞書, ticker→result） |
| AS-IS-286 | 1-7. TANUKI SCORE | ticker |
| AS-IS-287 | 1-7. TANUKI SCORE | company |
| AS-IS-297 | 1-7. TANUKI SCORE | date（history.json各エントリ） |
| AS-IS-299 | 1-7. TANUKI SCORE | all_categories（history.json各エントリ） |
| AS-IS-389 | 1-9. Portfolio | date |
| AS-IS-394 | 1-10. TANUKI TAIL | quarter |
| AS-IS-402 | 1-10. TANUKI TAIL | quarters（index.json） |
| AS-IS-404 | 1-10. TANUKI TAIL | last_filed（rss_state.json） |
| AS-IS-408 | 1-10. TANUKI TAIL | status（review_queue.json） |
| AS-IS-410 | 1-10. TANUKI TAIL | review_path（review_queue.json） |
| AS-IS-437 | 1-10. TANUKI TAIL | tail_kpi_map.json: kpi_name |
| AS-IS-438 | 1-10. TANUKI TAIL | tail_kpi_map.json: tag_history[].tag/valid_from/valid_to |
| AS-IS-439 | 1-10. TANUKI TAIL | tail_kpi_map.json: fallback_tags |
| AS-IS-440 | 1-10. TANUKI TAIL | tail_kpi_map.json: revenue_tag |
| AS-IS-441 | 1-10. TANUKI TAIL | tail_kpi_map.json: dimension |
| AS-IS-481 | 1-10. TANUKI TAIL | is_latest（トップレベル） |
| AS-IS-506 | 1-10. TANUKI TAIL | {TICKER}（トップレベルキー） |
