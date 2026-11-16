# 外の変化の新しい6件の調べ（System Health [P] ExternalChange、2026-11-15 の日曜の更新で出た分）

調べた日: 2026-11-16。**読み取りと手元の試算だけ**（コード・データ・確認済みの台帳は変えていない。試算は `tools/sim` で `company_facts.json` を一時的に直して回し、`git checkout` で戻した）。対象は `config/external_change_acknowledged.json` に無い6件。

## 0. 結論

| # | 件 | 事実 | 今の公開の値への効き方 | 直すか |
|---|---|---|---|---|
| 1 | COHR D&A（タグの切り替え） | **タグが混ざった値**。最新の10-Qで無形資産の償却が `AmortizationOfIntangibleAssets` → `OtherAmortizationOfDeferredCharges` に付け替わり、Layer3は `Depreciation` だけ（77.7M）を採った。実際は 77.7M＋69.5M＝**147.1M** | ttm D&A が 546.2M → 476.8M と 69.5M 少ない。**理論株価・判定は0**（D&Aは `fcf_estimation` の表示だけ） | 要（案は §3） |
| 2 | RCAT D&A（タグの切り替え） | **実際の値**。内訳の `Depreciation`＋`AmortizationOfIntangibleAssets` から合計の `DepreciationDepletionAndAmortization` に変わっただけで、Q1（0.812M）はどちらでも同じ。Q2 の 1.47M（1.81倍）は買収後の本当の増加 | なし | 不要（確認済みにしてよい） |
| 3 | KULR 売上（タグの切り替え） | **実際の値**。`Revenues` と `RevenueFromContractWithCustomerExcludingAssessedTax` は同じ値で出ている（Q3 7.40M、Q2 2.08M＝3.56倍は本当の増加）。ただし**同じ申告で上半期が下方修正**されている（下の注） | なし | 不要（確認済み。修正は別件） |
| 4 | KULR 営業CF（タグの途切れ） | **別の概念に変わった**。最新の10-Qは `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations`（継続事業）だけで、前年の比較も継続事業に組み替え（9か月 −31.04M → −22.58M） | 最新のTTMで OCF・FCF が欠ける。`latest_period_missing` の守りが効き、`fcf_estimation` は TTM 2025-09-30 に戻る。**IV 6.10・判定とも変わらない**（FCFの基準は5年平均）。収益の段階も変わらない | 要（案は §3。基準が違うので自動の別名にはしない） |
| 5 | SPIR 短期投資（タグの途切れ） | **タグが付け替わった**。バランスシートの同じ行が `MarketableSecuritiesCurrent` → `AvailableForSaleSecuritiesDebtSecurities`（2025-12-31 は同額 56,969,000。2026-09-30 は 47,052,000） | `bs_adjustment` の短期投資は FY2025 の 56.97M のまま（実際は 47.05M）。試算で候補に足しても **IV・判定は0**（年度の値を使う作り） | 要（表示の古さ。案は §3） |
| 6 | NIY=F 限月 | **データの欄の異常**。2026-11-09 の行だけ `contract` が `NIY.CME`（ほかは `NIYZ26.CME`）で、`contract_close` 等が無い。終値 60,935 は翌日の前日終値と一致、**値は正しい**。11-10 の open が 11-09 と同じ 58,055（その日の安値 59,660 より下＝提供元の古い値） | 価格の用途は Market Pulse の先物の表示だけ。理論株価・判定・STONKS・stage は読まない | 不要（確認済みでよい。open の1件はFINDINGS） |

**効く値は今の公開の値にはありません**（96銘柄の理論株価・判定・保有・funda・stage が前後で同じ）。表示と履歴が古い・ずれるものが3件（COHR D&A・KULR OCF/FCF・SPIR 短期投資）。**同じ型は DOCN にもあり、そちらは値に効く（§2）**。

## 1. 原本の申告で確かめた事実

### 1.1 COHR（D&A）— 最新の10-Q `0000820318-26-000023`（FY27 Q1、2026-09-30、提出 11-04）
- `Depreciation` 2026-07-01〜09-30 = 77,665,000（前年同期 52,983,000）
- 無形資産の償却は `AmortizationOfIntangibleAssets` が**この申告に無く**、`OtherAmortizationOfDeferredCharges` = 69,451,000（前年同期の比較 69,446,000。前年の申告では `AmortizationOfIntangibleAssets` = 69,446,000）。5桁違いの丸めだけで同じ項目
- 前の期は Depreciation 52,983,000＋Amortization 69,446,000 = 122.4M、今期は 77,665,000＋69,451,000 = **147.1M**。Layer3 は片方だけ（77.7M＝合計の0.53倍）
- 合計の `DepreciationAndAmortization` は年次（10-K）だけで、四半期には無い

### 1.2 RCAT（D&A）
- `DepreciationDepletionAndAmortization` 2026 上半期 = 2,284,000、Q1 = 812,000 → Q2 = 1,472,000。内訳（丸めた値）の Depreciation 1.3M＋Amortization 1.0M = 2.3M と一致。**合計と内訳は同じ値**で、Q1（Layer3 は内訳の和 0.8M）との差は丸めだけ

### 1.3 KULR（売上）
- 2026-Q3（07-01〜09-30）= 7,396,716、Q2 = 2,080,177。**両方のタグが同じ値**（`Revenues` と `RevenueFromContractWithCustomerExcludingAssessedTax`）
- 注（別件）: Q3の申告の9か月累計 12,155,030 − Q3 7,396,716 = 上半期 4,758,314 で、Q2の申告の上半期 6,026,607 より 1.27M 少ない（Q1も 4,846,430→3,946,430 と前の10-Qで 0.9M 下方修正）。**四半期ごとの値は別の申告のものが混ざる**（TTMの売上 16.29M は最新のQ3と前の申告のQ1・Q2の和）。FINDINGS に1行

### 1.4 KULR（営業CF）— 最新の10-Q `0001104659-26-124824`
- `NetCashProvidedByUsedInOperatingActivities` が無く、`…ContinuingOperations` が 2026 9か月 −32,278,672・2025 9か月 −22,577,445。前の申告の 2025 9か月は −31,039,539（**事業の廃止の分を除いた組み替え**で、前年も値が違う）
- Layer3 の候補は `NetCashProvidedByUsedInOperatingActivities` だけ → 最新の四半期の営業CFが無い

### 1.5 SPIR（短期投資）— 最新の10-Q `0001628280-26-072823`
- 流動資産 91,842,000 − (現金 30,902,000＋売掛金 3,375,000＋契約資産 6,245,000＋その他 4,268,000) = 47.05M＝`AvailableForSaleSecuritiesDebtSecurities` 47,052,000（満期1年以内）。2025-12-31 の同じタグ 56,969,000 は前の申告の `MarketableSecuritiesCurrent` 56,969,000 と**同額**＝同じ行のタグ付け替え
- `bs_lines` の記録（`bs_lines/0001628280-26-072823.json`）は3行（現金・無形・のれん）だけで、この行を短期投資に分類できていない（`short_term_investments` の正規表現は `AvailableForSaleSecuritiesDebtSecurities` と `MarketableSecurities` を含まない）

### 1.6 NIY=F
- `common/market_data/daily/NIY=F.json` 2026-11-09: `contract: NIY.CME`・`_provisional`、`contract_close`・`contract_prev_*` なし。11-10 の `contract_prev_close` = 60,935 = 11-09 の `close`。検知は `futures_roll.contract_anomalies`（日足から毎回作る）

## 2. 効き方（`tools/sim`、前＝今の kaihatsu `ffc3d07d44` の上、後＝正しい値にした場合）

- 直した値: COHR 2026-09 の償却 +69,451,000、KULR の営業CF 2026 9か月 −32,278,672（別名として足した）、SPIR の短期投資 47,052,000
- 全96銘柄 TANUKI→HypeCore→TANUKI: **理論株価が変わる 0・判定 0・保有 ±0.0%・funda・stage 0**（`.sim_out/cmp_ec_base_ec_fix.txt`）
- 派生の変化: COHR ttm D&A 476.8M → 546.2M。KULR は `latest_period_missing` が消え、`fcf_estimation` が TTM 2026-09-30（OCF −46.1M・capex 5.70M）に進む。収益の段階（KULR `revenue_stage`=loss・`structural_deficit`=true）は同じ、`fcf_points` が06-30基準→09-30基準に動くだけ（5年平均 −25.1M → −27.7M）
- SPIR: 短期投資を足しても `bs_adjustment` は同じ（年度の値を使う）
- **DOCN（同じ型・値に効く）**: 最新の10-Q `0001628280-26-069141` の `MarketableSecurities` = 178,645,000（2026-09-30、2025-12-31 は 0）が候補のタグに無く、`bs_lines` も分類できないため `short_term_investments` が **0（`sti_estimated_zero`）** に読まれ、正味現金が −531.7M。178.6M を入れると +1.51ドル/株（IV 64.44 の約2.3%、手元の概算。company_facts だけ直しても動かない＝`bs_lines` の分類と合わせて直す必要がある）
- STONKS・ROTCE・収益の段階: D&A・OCF の読み手は `src/value/stonks_silo/fetcher.py`・`common/profitability.py`・`common/stage_v2.py`・TAIL。今回の3件で段階・stage は動かない（KULR の収益の段階は上のとおり同じ）

## 3. 直し方の案（既存の仕組みで）

**共通の案: 「再タグの突き合わせ」を1か所に**（`common/sec_data/layer3_builder.py` の候補選び。銘柄名は書かない）
- 最新の申告で候補のタグが欠けた項目について、同じ申告の**前年同期（または前の期末）の比較値**が、前の申告の候補のタグの同じ期の値と一致する（0.5%以内）別のタグがあれば、**そのタグをその申告の別名として採る**（印 `retag_tie_out` を付ける）。突き合わせが合わないものは採らず、現状の「最新期が欠けた」守り（`latest_period_missing`）に任せる
- COHR: `OtherAmortizationOfDeferredCharges`（前年同期 69,446k＝前の申告の `AmortizationOfIntangibleAssets` 69,446k）→ 合う。SPIR: `AvailableForSaleSecuritiesDebtSecurities`（2025-12-31 56,969k＝`MarketableSecuritiesCurrent` 56,969k）→ 合う。RCAT: 合計のタグは内訳の和と丸めの範囲で合う
- **KULR は合わない**（−22.58M ≠ −31.04M、廃止事業の組み替えで基準が違う）→ 自動の別名にはしない。案: `…ContinuingOperations` を営業CFの**2番目の候補**にして、基準が変わったことを `basis_mix`（既存の印）に載せる。Q3単独は 9か月 −32.28M − 上半期 −17.69M（旧基準）なので混ざる。直すなら年次・上半期の継続事業の値が出るまで印つきの試算値にとどめるか、チャット側の判断
- **DOCN・SPIR の短期投資**は `bs_lines.py` の `short_term_investments` の正規表現に `MarketableSecurities`（`Current` 無し）と `AvailableForSaleSecuritiesDebtSecurities` を足す案が最小（`Noncurrent`・`LongTerm` は既存の除外）。ただし `AvailableForSaleSecuritiesDebtSecurities` は注記の合計（現金同等物を含む）で出す会社もある（CAT・NVDA・KLAC・SOFI 等）ので、**バランスシートの行（`bs_lines` の `role` が CondensedConsolidatedBalanceSheets の行）に限る**
- 既存の印・守りで足りるか: COHR の D&A は「**前の期の同じ項目より最新の1期だけ内訳が減った**」型で、Layer3 の `PARTIAL_SUM_FIELDS`（両方あれば足す）が片方しか見えなかった例。守り（`val_guard`）の対象は財務の値の桁・符号で、この型は見ていない→上の突き合わせを足すのが筋

### 同じ型の機械的な探索（全98銘柄の `ttm`・`company_facts`。`scratchpad/scan.py`）
- A. 最新のTTMに前の期にあった項目が無い: **KULR（営業CF）・LITE（株式報酬）**。LITE は未調査（FINDINGS）
- B. 最新のTTMの D&A が前の期の0.85倍未満: **APP 0.46・ENTG 0.78・FROG 0.80・MO 0.79**。APP は事業の売却の可能性があるが未調査
- C. 最新の申告に短期投資らしい候補外のタグがあり、候補のタグが無い: CAT・CDNS・DELL・DOCN・FICO・KLAC・NVDA・SOFI・SPIR・TER・V の11。このうち**同額で前の候補と突き合う（同じ行の付け替え）のは SPIR**。DOCN は候補が2024-12まで・新しいタグ。KLAC・NVDA は候補が途切れているが、短期投資は別の経路（`bs_lines`・近似）で入っている（`bs_adjustment` で確認）。ほかは注記の合計で、行ではない

## 4. 未公開の5つへの効き方（とくに CONTINUITY の公開）

- **CONTINUITY（公開は明日の予定）**: **効かない**。6件は FCF の基準の決め方（CV 0.5・Q4逆算が負）に触れず、試算で全銘柄の理論株価・判定が前後で同じ。必須の6銘柄（HQY・DOCN・TASK・FRSH・APP・KULR）のうち **KULR は `fcf_estimation` の表示だけが変わる**が、直すのは別の公開にしてよい（CONTINUITY の scope と掛け合わさらない）。DOCN は上の短期投資の件があるが、CONTINUITY の −13.1%（IV 64.4→56.0前後）に +1.5ドル/株を足しても上向きの差は約2.7ポイントで、概算では HOLD→TRIM（−25%の境）は動かない（**この組み合わせの試算はしていない**。DOCN を先に直すなら CONTINUITY の試算をやり直す）。**明日の公開は予定どおりでよい**
- DILUTION・DEFICIT・MULTIPLES・SHARES: D&A・OCF の読み手に当たるのは DEFICIT（`profitability.py`、KULR の `fcf_points`）だけで、段階は同じ。MULTIPLES・SHARES は触れない
- 直す順の案: **5つの公開の後**に、再タグの突き合わせ（COHR・SPIR・RCAT）と `bs_lines` の短期投資の正規表現（DOCN）を1件の計算方法の変更として（IDは新しく登録。DOCN だけ理論株価に効く）。KULR の営業CFは基準の判断が要るので別

## 5. 決めてほしいこと

1. 確認済みの台帳に入れてよいか: RCAT D&A・KULR 売上・NIY=F 限月（理由は上）。COHR・KULR 営業CF・SPIR 短期投資は直すまで未確認のまま残すか、理由つきで確認済み（値に効かないため）にするか
2. 直し方の案（再タグの突き合わせ＋`bs_lines` の正規表現）を1件として登録し、5つの公開の後に着手してよいか。KULR の継続事業の営業CFの扱い（印つきの2番目の候補か、待つか）
3. DOCN の短期投資（+2.3%）だけを先に直すか（値に効く唯一の件）
