# PARSER-LAYER3-SINGLE-SOURCE-1 段0（Layer3の定義の修正）

2026-11-11。計算方法の変更 ID: `PARSER-LAYER3-SINGLE-SOURCE-1-0`。段の全体の案は `PARSER_LAYER3_STEP0.md`（STEP 0）。
表: `PARSER_LAYER3_STAGE0_tags.csv`（全項目の候補タグの調べ）・`PARSER_LAYER3_STAGE0_total_liabilities.csv`（負債合計の変更の全件）。

## 1. チャット側の決定（2026-11-11）
| | 決定 | 段0での扱い |
|---|---|---|
| (a) | 置き換えは Layer3 の `val`（最新の申告）。`first_val` は [[POINT-IN-TIME-HISTORY-INTEGRITY-1]] の側の役割。各段の公開の前に、理論株価が5%を超えて動く銘柄・判定が変わる銘柄・保有銘柄は、理由を原本の申告で確かめて一覧にする（SOFI +33%・TSLA +25%・CAKE +29%・ABBV・DDOG・HQY は必須） | 段0は parser を置き換えない（Layer3の定義の修正だけ）ので、上の銘柄は動かない。必須の確認は段1以降の公開の前に |
| (b) | Layer3の保持は読み手が要る一番長い期間（`get_roe_avg` の10年）から決める。年数を手で書かない | `common/sec_data/retention.py` に読み手の定数だけを置き、四半期7年・年次11年を導く |
| (c) | 0で埋めない。申告に無い値は無いまま | 段0の修正は0を足さない。S2でSOUNがWATCH→SELLになった原因は **0埋め**（§6） |

## 2. 全項目の候補タグの機械的な調べ（依頼1）
`config/sec_concept_definitions.json` の41項目×全候補（+銘柄別の設定・条件付き・`cf_lines`・`bs_lines` が足すタグ）を、98銘柄のLayer3で採用された期の数と突き合わせた（表: `PARSER_LAYER3_STAGE0_tags.csv` 141行）。合計・小計・部分・別の概念が混ざる候補:

| 項目 | タグ | 問題 | 処置 |
|---|---|---|---|
| total_liabilities | `LiabilitiesAndStockholdersEquity` | 資産合計そのもの（負債でない）。426期・19銘柄に採用（KO・WMT・ABBV等、Liabilitiesを申告しない会社） | **段0で削除**＋恒等式で埋める（§3） |
| depreciation_and_amortization | `Depreciation`・`AmortizationOfIntangibleAssets` | 部分概念。合計のタグが無い期に片方だけ採ると過小（ABBV: 減価償却4.7億に対し償却12.9億）。採用は四半期365+123期・年次86+9期 | **段0で修正**（§4） |
| long_term_debt | `LongTermDebt`（252期・23銘柄）・`DebtLongtermAndShorttermCombinedAmount`（23期） | 1年内返済・短期を含む合計。short_term_debt と二重計上の恐れ（ADBE 2022: 長期3,626M・短期499M。ABBVは短期が別概念で二重にならない）。銘柄ごとに違い機械では決まらない | 段1（`bs_lines` の値を正にする案）で判断 |
| rpo | `ContractWithCustomerLiability`（348期）・`...Current`（315期）・`DeferredRevenue*` | **RPO（受注残）ではなく前受金・繰延収益**。RPOより小さい値が入る | 段1で判断（候補から外すと、これらを採っている銘柄〈最大38〉のrpoが空になる） |
| stockholders_equity | `...IncludingPortionAttributableToNoncontrollingInterest`（202期・16銘柄） | 非支配持分を含む（親会社の純資産と定義がずれる） | 段1で判断 |
| cash_and_equivalents | `CashCashEquivalentsAndShortTermInvestments`（9期・BBAI・CAT） | 現金＋短期投資の合算。短期投資の値が無い期だけなので二重計上は起きていない | 残す |
| selling_and_marketing / cost_of_revenue / short_term_investments | `AdvertisingExpense`（広告費だけ）・`CostDirectMaterial`・`Investments`ほか | 部分・広い概念 | 段1で判断（表に全件） |
| research_and_development | `CapitalizedComputerSoftware*`（2つ） | 別の概念。今は採用0 | 段1で候補から外す案 |
| shares_diluted | `CommonStockSharesOutstanding`（493期・72銘柄） | 期末の発行済株式数（加重平均でない）。既知。読み手は source_tag で加重平均だけに絞る | 残す |

売上・営業利益・純利益・EPS・CFの3項目・SBC・自社株買い等の候補は、同じ概念の名前違い・優先順位だけで問題なし。

## 3. 負債合計（total_liabilities）
- 候補から `LiabilitiesAndStockholdersEquity` を外した（`["Liabilities"]` だけ）。
- `Liabilities` の無い期末日は、貸借対照表の恒等式で埋める（`layer3_builder._derive_total_liabilities`）: **資産合計 − 純資産 − 非支配持分（`minority_interest`、無ければ0）− 一時的持分（`TemporaryEquityCarryingAmount*`、あれば）**。結果が0以下・純資産が無い期は作らない（0で埋めない）。印: `source_tag="identity:..."`・`derived=True`。
- **KO 2025-12-31: 104.816B → 70.541B**（チャット側の見込みは72.6B＝資産−親会社の純資産。非支配持分2.106Bも引くので70.5B。parserの恒等式は非支配持分を引かず、負債に混ぜている）。
- 検証: `Liabilities` を申告している期末日2,083点で、恒等式の値と申告の値は1,868点（90%）が0.5%以内。2%を超える184点はほぼ一時的持分のある会社（ASTSの合併前: 申告24.3M・恒等式226.8M＝一時的持分202.6M）。一時的持分を引く規則を入れた。
- 変わった期: 424期・17銘柄（表: `PARSER_LAYER3_STAGE0_total_liabilities.csv`）。読み手はTANUKIの警告（total_debt=0かつ負債大）だけで、**理論株価は動かない**。

## 4. D&A・capexの定義のずれ（依頼2）
**D&A（四半期565セル＋年次）:** 原因の大半は parser 側。parser は `Depreciation`・`AmortizationOfIntangibleAssets`（部分概念）を合計のタグより先に採ることがある（ADBE 2020Q2: parser 92M＝償却だけ、Layer3 188M＝DepreciationDepletionAndAmortization（376M−188M）。申告のCFは188M）。Layer3 は「合計のタグ ＞ 部分」の優先順位で正しい。Layer3 側の問題は2つ:
1. 合計のタグが無い銘柄で部分概念を片方だけ採る（ABBV等）。→ **段0で修正**: 両方ある期は足す（優先順位は 合計のタグ ＞ 2つの合計 ＞ 片方だけ。`PARTIAL_SUM_FIELDS`）。片方しか無い期は値を変えない。ABBV 2019: 2,017M（旧 464M）。TTMで19銘柄（ABBV・CEG・CELH・COHR・ENTG・ESTC・GEV・GOOGL・HON・INTU・LITE・MSCI・ONDS・RCAT・RKLB・RMBS・TASK・TER・VRT）が動く。
2. 最新の申告の値（`val`）にSEC側の単位の誤りがある（COHR 2023年次: 2025-08-15の10-Kだけ 681,687,000,000＝1,000倍、ほかの申告は 681,687,000）。決定(a)の `val` を使うと誤りがそのまま入る → FINDINGSに登録（異常値の守りは段1で決める）。
そのほか（HWM 2019: 536→295は再表示、ELF 2024: 35.9→30.2は再表示）は改訂の違い。

**capex（年次75セル）:** 原因は parser 側。73件は parser が 0 か過小（2021年より前の申告に `cf_lines` が無い年等）で、Layer3 は `cf_lines` を正にして正しい。Layer3 側の気づき: APP 2021・2022は Layer3 が `PaymentsToAcquirePropertyPlantAndEquipment` だけ（1.39M・0.66M）、parser は ppe＋software（5.457M・6.611M）。`cf_lines` の期の値は「その期を最後に載せた申告」を採るため、後の10-Kがソフト開発費の行を別にしなかった年は software が落ちる → FINDINGS。定義の変更なし（FCF-DEFINITION-SINGLE-SOURCE-1 の `cf_lines` を正にする決定のまま）。

## 5. 保持期間（依頼3、決定(b)）
`common/sec_data/retention.py`: `ROE_AVG_YEARS=10`（`reader.get_roe_avg_detail`・`data_fetcher`が使う）・`TTM_POINTS=6`（`ttm_calculator` の `n_periods`）・`FILING_LAG_YEARS=0.75` から、四半期 = ceil((6−1)+1+0.75) = **7年**、年次 = max(ROE 10年, 四半期7年)+1 = **11年**（旧8年）。`quarterly.py` の `_QUARTERLY_YEARS`・`_ANNUAL_YEARS`・`reader.py`・`ttm_calculator.py`・`data_fetcher.py` はここを読む（手書きの 10・6 を廃止）。

## 6. 全銘柄の試算（依頼4）
手元で前（HEAD）と後を TANUKI → HypeCore → TANUKI で比べた（`tools/sim/sim.py chain/compare/scope`、派生ファイルは `tools/sim/regen_derived.py` で作り直し、`tools/sim/revert_noise.py` で丸めだけの変更を戻した）。

| 見るもの | 結果 |
|---|---|
| 理論株価 | 変わる銘柄 **0**（96銘柄） |
| 判定（verdict） | 変わる銘柄 **0** |
| 保有9銘柄 | 全て ±0.0% |
| HypeCore stage | 0銘柄（hypecore の出力96銘柄が前後で完全一致） |
| 調整後EPS・STONKS | 読み手に負債合計・D&A・年次の保持期間が無い（STONKSは parser の年次と四半期の直近分） → 動かない |
| TANUKI latest.json | 変わる項目は `fcf_estimation.da` だけ（表示用）。9銘柄: ABBV 777M→7,644M・COHR 266M→477M・ESTC 3M→12.8M・GEV 717M→1,429M・INTU 187M→672M・LITE 145M→279M・TASK 46M→66M・CEG・GOOGL |
| ttm/ | 実質の変更は19銘柄の D&A だけ（ほか79ファイルは丸めのみで戻した）。normalized/ は98銘柄とも年次が8→11年に延びる（追加のみ） |
| profitability.json | 変化 **0**（保持期間だけを8→11年にした比較） |
| multiples（倍率の履歴） | 64銘柄の**12か月より前**の月で `net_income_5y_*`・`pe_5y_avg_earnings` が変わる（昔の月は年次の履歴が足りず2〜3年平均だったものが5年平均になる。None→値 334・値→値 4,386セル、ADBE の最初の月 61.9→81.5 等）。直近12か月は0。次の定時の実行（市場データ）で作り直される |

**(c) S2の原因（SOUN WATCH→SELL）:** S2（parserに無い項目をLayer3で足す、6,289セル）から**0の値の追加（1,528セル）だけを除いた試算（S2nz、4,761セル）**では、保有9銘柄が全て ±0.0%・SOUNはWATCHのまま。S2で動いた保有（NVDA −7.8%・PLTR −4.1%・SOFI −32.3%・SOUN →SELL）は **全て0埋めが原因**。0を足さなければ保有は動かない（S2nzで動くのは31銘柄・判定5: ABBV・ALAB・DDOG・GEV・HQY、ほかSITM +214.6% 等）。→ parser に無い項目は足さないが原則。`tools/sim/parser_l3/parser_overlay.py` に `S2nz` を追加。

## 7. 次（段1以降）への申し送り
- 段1以降は parser の置き換えの試算（S1）で動く銘柄（ENTG +41%・ABBV +31.8%・CAKE +29%・INTU +27.7%・DDOG −43.3%・HQY・SOFI・TSLA）の理由を原本の申告で確かめた一覧を公開の前に出す（決定(a)）。S1のCAKE +29%は負債の内訳（Q3に長期→短期）が入る可能性があり、個別の確認が要る。
- 段1で判断する候補タグ（§2の表）: `LongTermDebt`・rpo の前受金・純資産の非支配持分含む・広告費だけ等。COHRの1,000倍の誤りの守り。
