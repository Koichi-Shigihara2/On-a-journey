# SEC_DATA_BUG_TAXONOMY.md — common/sec_data/ 全面再設計に向けたバグパターン類型化

作成日: 2026-08-03
出発点: `common/sec_data/`（SEC EDGAR由来の一次財務データ取得層）を一から
作り直すにあたり、2026年4月頃から現在までに発見された全てのバグ・課題を
`BACKLOG_DONE.md`・`BACKLOG.md`から棚卸しし、体系的に類型化する（チャット
記録の調査に基づく）。

## 本ドキュメントの位置づけ

- **本ドキュメント**: 過去の全ての発見・対応を「何が起きたか」の記録から
  「同種の問題がどのパターンに属するか」の類型へ整理する。実装方針の決定は
  行わない（次フェーズで別途検討する）。
- `EXTRACTION_DESIGN_PRINCIPLES.md`: 5バグから抽出した3原則（新規データ層
  向け）。本ドキュメントは、その3原則が実際には数十件規模で繰り返し発生して
  いたことを示し、原則の追加候補（後述）も提示する。
- `BACKLOG.md`/`BACKLOG_DONE.md`: 個別課題・完了記録の一次情報源（「何が
  起きたか」）。本ドキュメントはこれらを再分類した二次情報源であり、
  一次情報源の記述は変更しない。
- `SEC_EDGAR_LAYER_DESIGN.md`: 現行のLayer2/Layer3統合スキーマ設計判断
  （「なぜこの設計にしたか」）。

## 調査範囲・方法

`BACKLOG_DONE.md`（11,407行）・`BACKLOG.md`（9,605行）を横断的に検索し、
`common/sec_data/`配下のファイル（`parser.py`・`quarterly.py`・
`normalizer.py`・`layer3_builder.py`・`ttm_calculator.py`・
`tag_definitions.py`・`fetcher.py`・`segment_fetcher.py`・`q4_implied.py`・
`fact_selection.py`・`contracts.py`・`report_consistency_check.py`等）・
XBRLタグ選定・accn選定・会計年度判定・CIK照合・SEC EDGAR取得に関わる項目を
抽出した。下流消費者（TANUKI VALUATION計算ロジック・STONKS SILO・HypeCore
等）固有の課題は、根本原因が`common/sec_data/`の抽出バグに遡る場合のみ含めた。

**件数**: 完了103件・未解決（後述の一部を含む）を合わせて棚卸し。以下、
根本原因テーマ別（セクション2）・企業属性別（セクション3）・設計原則対応
（セクション4）の3つの軸で整理する。

---

## 1. 全件棚卸し（根本原因テーマ別、詳細）

各項目は `**[[ID]]** — 状態（DONE=対応済み／OPEN=未解決）。内容。企業属性。
根本原因。対応。` の形式。

### テーマA: 候補タグリストの不備・誤ったフォールバック意味論

1. **[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]** — DONE。
   `total_liabilities`の2番目の候補`LiabilitiesAndStockholdersEquity`は
   定義上`Assets`と同一のため、`Liabilities`タグ不在時に
   `total_liabilities`が実質`total_assets`と同じ値になっていた。
   一般的な設計欠陥（銘柄非依存）。278件/22銘柄（AMZN/GOOGL/MSFT/NVDA/
   AMD/WMT等）。根本原因: 意味論的に誤ったフォールバックタグ。対応:
   `_backfill_total_liabilities_via_identity()`でTA−SEから逆算。
2. **[[JNJ-RD-TAG-PRIORITY-1]]** — DONE。JNJのR&Dが約1/30に過小計上。
   JNJが2023年以降、真のR&D本体タグ
   （`...ExcludingAcquiredInProcessCost`）とM&A時のIPR&D即時費用化タグ
   （`ResearchAndDevelopmentExpense`、CF計算書由来）を両方報告するように
   なり、優先順位1位の後者が誤採用され続けていた。根本原因: 複数タグ
   並存時の優先順位誤り。対応: `tag_definitions.py`の優先順位入替え。
3. **[[LAYER3-CONFIG-RD-TAG-PRIORITY-1]]** — OPEN。同型バグがLayer3の
   独立候補リスト（`config/sec_concept_definitions.json`）に残存。
   JNJのTTM/RICE経路が別途機能不全のため現状無害。未対応。
4. **[[CASH-TAG-MISSING-1]]** — OPEN。`CASH_AND_EQUIVALENTS`候補に
   ASU 2016-18対応タグ`CashCashEquivalentsRestrictedCashAnd
   RestrictedCashEquivalents`が未登録。CAT/CPRT/ELF/GEV/HEI/SITMが
   該当。根本原因: 会計基準移行への候補リスト未追従。未対応（restricted
   cash過大算入リスクあり）。
5. **[[BS-FIELD-NEWLY-MISSING-2026-1]]** — OPEN。LLY(STI)/SCCO(STD)/
   SPIR(LTD)が最新年度で非ゼロ→Noneに転じた。CASH-TAG-MISSING-1と
   同型だが一次情報未確認。未対応。
6. **[[SEC-TAG-FICO-CPRT-1]]** — DONE。FICO/CPRT/LITEの2020年revenue
   急増は`_extract_values_merged()`の「先勝ち」により91日間の比較値と
   365日の年次値が同一end_dateで衝突した結果。根本原因: マージ抽出に
   期間長のtie-breakが無い。対応: 365日近似のtie-break追加。
7. **[[PERIOD-LENGTH-VALIDATION-GAP-1]]** — DONE（重要、
   EXTRACTION_DESIGN_PRINCIPLES.md原則1の根拠）。`_extract_single_key()`
   経路（FLOW型12フィールド中9）に期間長検証が皆無で、91日の四半期
   スタブが年次値としてそのまま採用されていた。MRVL(gross_profit)/
   COHR/INTU(cost_of_revenue)/VRT(revenue)/RCAT(D&A)で発見。対応:
   340-380日フィルタを両経路に必須化。
8. **[[XBRL-TAG-KLAC-1]]・[[XBRL-TAG-KLAC-1-FOLLOWUP]]** — DONE。
   KLACの`_classify_period()`が`fp=='FY'`のみで日数下限を見ず、91日の
   比較値を年次と誤認。KLACは年次`OperatingIncomeLoss`も報告しない。
   フォローアップで2件追加発見（フォールバック条件の狭さ、3タグ
   TTM-operating-income合成が独立per-tagの「直近4四半期」を使う設計
   ミス）、ASTS/JNJ/LLY/SOFI/XOMに影響。根本原因: 期間誤分類＋フィールド
   独立抽出。対応済み。
9. **[[CHECK-QREV-FYE-1]]・[[DILUTION-FYE-1]]** — DONE。非12月決算銘柄
   （KLAC/LRCX/DELL/ESTC）が四半期データを暦年ラベルでグループ化して
   おり、疑似的な収益品質エラー・LRCXの10:1分割が109%希薄化と誤検知
   された。根本原因: 暦年グループ化（会計年度グループ化ではない）、
   同日に2回独立発見された再発パターン。対応: 370日トレイリング窓
   グループ化（後に`utils.py::quarters_in_trailing_window()`として共通化）。
10. **[[PARSER-ENTG-COMPYEAR-1]]** — DONE。ENTG FY2022 revenueの
    誤抽出、10-Qの比較累計コンテキストが`is_annual`バケットに混入。
    根本原因: `is_annual`判定にform制限が無かった。対応済み。
11. **[[LLY-CAPEX-STALE-1]]** — DONE。LLYのCapExが2022-09-30で凍結。
    企業がタグを無言で切替え（`PaymentsToAcquireProductiveAssets`→
    新タグ）、旧「最小件数を満たす最初の候補」選定が再評価されなかった。
    根本原因: サイレントなタグ移行の未検知（JNJ-RD・total-liabilities
    と同系統）。対応: `tag_definitions.py`新設＋「適格候補の中で最新
    end_date」選定へ変更。
12. **[[LAYER3-FALLBACK-STALE-TAG-PRIORITY-1]]** — DONE。同型の
    「新タグより古いタグを優先」バグがlayer3_builder.pyの独立
    フォールバックに存在（IONQ revenueで発見）。対応: 候補を個別正規化
    後end_dateでマージする方式に変更。
13. **[[LAYER3-GA-STANDALONE-TAG-UNMAPPED-1]]** — OPEN。
    `GeneralAndAdministrativeExpense`（Selling抜きG&A）タグがLayer2の
    どのフィールドにも未マッピング。APGE/ASTS/CON/ENB/RXRXでSM/SGA
    両方が空。根本原因: フォールバックではなく概念そのものの候補
    カバレッジ欠如。未対応（新規フィールド推奨、フォールバック統合は
    SM/SGA混同リスクを再導入するため不可）。
14. **[[LITE-COGS-DA-TAG-UNMERGED-1]]** — OPEN。LITEの
    `cost_of_revenue`がCOGS側D&Aタグを含まず9年間過小計上。根本原因:
    複数タグ合算が未実装（LLY-CAPEX-STALEと隣接するが「置換」ではなく
    「加算」が必要）。未対応。
15. **[[LAYER3-COGS-STRUCTURAL-GAP-16TICKERS-1]]** — OPEN。16/105銘柄
    が構造的にcost_of_revenueタグを欠く。CAKEを一次情報で裏取りした
    結果、企業がFY2022でP&L科目名を変更しXBRLタグ付け自体を停止して
    いたと確認（発行体側のデータ不在、回収不可能）。根本原因: 一部の
    欠損は抽出ロジックでは原理的に回収不可能。未対応（UI側「未開示」
    表示を推奨）。
16. **[[LAYER3-COGS-ASTS-LRCX-RECOVERABLE-FOLLOWUP-1]]** — DONE
    （回収不可能と確定）。ASTS/LRCXは回収可能に見えたが、LRCXは
    企業固有namespace（`lrcx:`）タグに移行しSECの`companyfacts.json`
    APIには一切露出しない（filingレンダリング画面でのみ可視）、ASTSは
    ディメンション付き注記にCOGSを移し標準コンテキストAPIから不可視と
    判明。根本原因/教訓: 発行体固有namespaceタグ・ディメンション限定
    開示はcompanyfacts.jsonから不可視——一次情報確認はfiling本文でなく
    実際のAPI応答を見る必要がある。対応: `_get_concept_units()`に
    namespace prefix対応を追加（将来再利用のため）。
17. **[[STONKS-SILO-COGS-DEAD-FALLBACK-1]]** — DONE。STONKS SILO
    fetcherの3キーCOGSフォールバックのうち後者2キーはparser.py出力に
    存在せず恒久的なデッドコード。加えてfalsy-zeroバグ（cost_of_revenue
    ==0が`or`連鎖で誤って次にフォールスルー）も発見。根本原因: parser.py
    の単一キー統一設計の誤解＋Python truthy/falsyの罠。対応: 単一キー
    参照に簡略化（falsy-zeroも副次的に修正）。
18. **[[REVENUE-TAG-PRIORITY-FRAGILE-1]]** — OPEN。`revenue`優先順位が
    脆弱: TDYのセグメント限定`Revenues`タグ（$831.7M）が正しい連結
    `SalesRevenueNet`（$2,127.3M）に「先勝ち」で勝ってしまう。ASTSは
    発行体自身のXBRL入力誤りが偶然良い順序で処理され「救われている」
    だけ。根本原因: 継続性チェック等の二次検証を伴わない優先順位
    リストのみのtie-break。未対応。
19. **[[REVENUE-TAG-CONFLICT-SCAN-1]]** — DONE（調査・分類記録）。
    新設`revenue_tag_conflict_check.py`（2倍以上の乖離検知）で14銘柄を
    検出、既対応済み（SOFI/IONQ/CPRT/FICO）・誤検知（LITE/TER）・
    業界固有の真の差異（PM、物品税込み/抜き収益）・
    [[FY52WEEK-BUCKET-MISPLACE-1]]（AVGO/DELL/CAKE/ELF/RCAT）・
    [[REVENUE-TAG-PRIORITY-FRAGILE-1]]（TDY/ASTS）に振り分け。タグの
    包含関係（例: `DepreciationAndAmortization`⊇
    `AmortizationOfIntangibleAssets`）による誤検知率の高さも記録。
20. **[[SEC-BKNG-SHARES-ANOMALY-1]]** — OPEN。BKNGの希薄化株式数タグ
    自体が生SEC XBRLデータで24倍の異常値（発行体側のエラー、パイプ
    ラインバグではない）。未対応。
21. **[[LAYER3-IONQ-REVENUE-2022Q1-ANOMALY-1]]** — DONE（原因確定、
    対応は別エントリに統合）。IONQ 2022Q1の`Revenues`タグ自体に
    SPAC信託金（$1,235M）が発行体により誤ってrevenueとしてタグ付け
    されていた。[[SOFI-TICKER-RESTRICTIONS-NOT-MIGRATED-1]]の既知
    IONQオーバーライドに統合。
22. **[[SEC-XBRL-MISSING-START-ENTRY-1]]** — OPEN。AVAV/ELF/ESTCの
    生XBRLに`start`日付欠落エントリが存在。現状`shares_diluted`が
    候補マージロジックを迂回しているため無害。影響未調査。

### テーマB: フィールド単位の独立accn/期間選定（「フィールド間整合性の未検証」）

23. **[[SPAC-SHELL-BS-ENTITY-MIXING-1]]** — DONE（主要、2段階）。
    SPAC合併銘柄のBS（instant fact）フィールドが合併前シェル会社・
    合併後本体という異なる法的実体から独立に混在採用され、
    current_assets>total_assets等の数学的矛盾が発生。BBAI/RDW/RKLB/
    SOFI/VRT(2019)・SPIR（矛盾未顕在化の"事故的正しさ"）。根本原因:
    候補がフィールド単位で選定され、フィールド間・同一accnの整合性
    チェックが無い。対応（段階1）: `_resolve_bs_entity_mixing()`が
    「①複数accn混在②本人データaccn一意③現に矛盾④統一で実際に解消」の
    4条件を全て満たす場合のみ単一accnへ強制。無条件版（案A）は
    シミュレーションで56件/41銘柄の誤検知が判明し不採用。（段階2）:
    `formerNames`区間一致によるSPAC合併疑いの事前検知（SPIR等の
    "事故的正しさ"型を検知）。
24. **[[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-MISSELECT-1]]** — OPEN
    （優先度高、VRT(2017)は現在進行形の実害可能性あり）。CRM(2011)/
    VRT(2017)のstockholders_equityがtotal_assets/total_liabilities
    採用accnとは無関係な年度・filingから選定されていた。根本原因:
    `_collect_own_data_instant()`が各BSフィールドを独自の`fy`タグ
    バケットで完全に独立選定し、共有accnに紐付かない
    （EXTRACTION_DESIGN_PRINCIPLES.md原則2が警告するパターンそのもの）。
    全105銘柄・1249年度中13件のみ該当（1.04%）、うち9件は無害
    （メタデータのみ不一致、値は一致）、1件（CWAN）は構造的に必然、
    2件（CRM/VRT）が真のバグ。2つの設計案（VRT型「0アンカー」・CRM型
    「2アンカー競合」）を提案済み、いずれも未実装。
25. **[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]** — OPEN（一部解決）。
    revenue/cost_of_revenue/gross_profitが独立にaccn/期間を選定し
    異なる年度のデータが混在。9銘柄確定（AMD/BSY/CRM/JNJ/KO/LRCX/
    MRVL/ONDS/RMBS）、4サブパターンに分解:(a)候補タグ欠落
    (b)クロスaccn/期間不整合(c)複数タグ合算漏れ(d)同一filing内の
    類似タグ誤選択。(a)(c)(d)の単純修正は全母集団シミュレーションで
    危険と判明（FCX/CAT/LLY/ABBV等で新規破壊）。「案b」（同一accn＋
    期間優先、既存の数学的矛盾をゲート条件とする）のみ実装、LRCX(2010)
    のみ解消。CRM/JNJ/MRVL/ONDS/RMBS/BSY/AMD/KO未対応。
26. **[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]** — DONE
    （一部、スコープ分割）。14銘柄のgross_profit vs revenue−COGS不一致。
    MO/PM（タバコ物品税会計慣行）・SCCO（鉱業D&A別建て慣行）は
    genuine確定。CRM/JNJ/MRVL は#25へ、LITEは#14へ移管。
27. **[[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]** — OPEN。
    HON(2009)のみ期間長修正後も$827M乖離が残存、他8銘柄は解消。原因
    未特定。未対応。
28. **[[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]** — OPEN。
    gross_profit/cost_of_revenue整合性の常設WARNチェックが存在しない
    （全て場当たり的発見）。未対応（PERIOD-LENGTH-VALIDATION-GAP-1系の
    修正が落ち着くまで意図的に保留）。
29. **[[LAYER3-ANNUAL-QUARTERLY-COLLISION-1]]** — DONE。
    `_merge_normalized_by_priority()`がend_dateのみでグループ化し
    `is_annual`を無視、同一end_dateの年次エントリが単独Q4エントリを
    無言で上書き（234件/16銘柄/28フィールド）。根本原因: 重複排除
    キーの次元不足。対応: 複合キー`(end_date, is_annual)`。
30. **[[QUARTERLY-CLASSIFY-PERIOD-NO-UPPER-BOUND-1]]** — OPEN。
    `_classify_period()`のis_annual判定に下限（>130/>300日）はあるが
    上限が無く、中間長期間（DELLの181日H1エントリ）が年次と誤分類
    される。本番`normalized/`データに影響。根本原因: 範囲検証の非対称
    （下限はKLAC対応で追加済みだが上限は未追加）。未対応。
31. **[[LAYER3-CROSS-TAG-YEARLY-QUARTERLY-GENERAL-RISK-1]]** — DONE
    （機構修正済み、設計メモとして残置）。
    `_merge_normalized_by_priority()`のper-(end_date,is_annual)独立
    候補選定が年次タグ由来と四半期タグ由来の値を43銘柄/9フィールドで
    混在させうる一般的リスク。#32の同一ソースタグガードで修正済みだが、
    新規Layer2候補タグ追加のたびに再監査すべき設計メモとして残す。
32. **[[LAYER3-DA-SBC-CANDIDATE-REGRESSION-1]]** — DONE。D&A/SBC TTM
    系列のquarters_usedが一部銘柄で悪化。`source_tag`が"+"結合の複合
    タグで、年次/四半期スロットが独立に異なる下位タグを選んだため
    Q4逆算が非互換タグを合成（BSYで負のQ4）。根本原因: Q4逆算計算内の
    クロスタグ不整合（#31と同系統）。対応: 同一ソースタグガードで7銘柄
    解消、残り7銘柄（DDOG/ELF/PEP/SPIR/APGE/CART/ESTC）は単一タグ
    整合済みで原因は別。
33. **[[LAYER3-MISSING-QUARTER-IMPLIED-GAP-1]]** — DONE。優先タグ自体が
    四半期を欠落（RCAT SBCがQ2欠落）、その誤った合成Q2+Q3値が次候補
    タグより優先採用される。根本原因: 優先選定タグの完全性未検証。
    対応: period_daysが四半期標準範囲（75-100日）外なら次候補へ
    フォールバック。
34. **[[LAYER3-IMPLIED-BLOCKS-FALLBACK-1]]** — DONE。タグレベルの
    欠測四半期逆算バックフィルが優先タグの元々空だったスロットに
    新規導出値を作り、下位タグの実報告値へのフォールバックを阻害
    （ONDS S&M 2024Q1: 導出値$26,143が実値$1,321,149に勝つ）。根本
    原因: 導出値（`is_implied=True`）がマージ順序で実報告データより
    優先されない。対応: 実データを常にタグ優先順位に関わらず優先。
35. **[[LAYER3-RPO-CANDIDATE-ORDER-1]]** — OPEN（優先度高）。Layer3の
    RPO候補統合が総額系タグを長期のみタグより優先し値が大きく変動
    （AMZN 4.4B→25B）、15銘柄影響。根本原因: 候補統合が異なる概念
    （総額 vs 長期のみ）を区別しない。未対応。
36. **[[LAYER3-UNEXPLAINED-SINGLE-TICKER-DIFFS-1]]** — DONE（一部）。
    各種単一銘柄diffを追跡: capex(LLY)/SBC(CAT)は他修正の副産物として
    解消、gross_profit(ABBV/HON)はLayer3側未移植の既知スコープギャップ、
    cash_and_equivalents(PAYS/RCAT)・short_term_investments(7銘柄)・
    total_liabilities(AVAV/ELF/ESTC)は未調査のまま将来の移行フェーズへ
    保留（STOCK型フィールド）。
37. **[[LAYER3-ANCHOR-MISSING-LOOKBACK-WINDOW-1]]** — OPEN（優先度中）。
    Layer3に旧パイプラインのH1-YTD-Q2四半期回復ロジックが無く、
    16銘柄/88フィールドでアンカー四半期再構築に影響。根本原因: 既知の
    フェーズA時点スコープギャップ（回帰ではない）。未対応（フィールド
    型ガード付きで移植が必要、無条件適用は株式数系フィールドを破壊する）。
38. **[[LAYER3-GROSSPROFIT-BACKFILL-MISSING-1]]** — DONE。
    `normalizer.py`のGrossProfitバックフィル（Revenue−COGS）に
    Layer3相当が存在しなかった。既知のフェーズAギャップがTTM全履歴で
    測定すると508件/30銘柄に拡大。対応: `layer3_builder.py`へ移植
    （11銘柄はcost_of_revenue候補自体が存在せず構造的に対応不可、
    parser.py側と同じ限界）。
39. **[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]** — DONE。
    `normalizer.py`/`layer3_builder.py`のGrossProfitバックフィルが
    中間TTM/storeデータにのみ書き込まれ、TANUKI VALUATION/STONKS SILOが
    実際に読む本番`annual_YYYY.json`に一切到達しない。根本原因:
    バックフィルロジックは存在するが本番書き込み経路に未接続。対応:
    `_backfill_gross_profit_from_revenue_cogs()`をparser.py本経路へ
    追加（34銘柄/342ファイル、標準タグ不在時のみ）。
40. **[[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]** — DONE
    （デッドコードとしてクローズ、削除はせず）。同一のRevenue−COGS
    gross_profitバックフィルが3箇所（normalizer.py/layer3_builder.py/
    STONKS SILO fetcher.py）に重複。#39でproduction経路が修正された
    結果、STONKS SILO側のコピーは発火実績0件と確認。将来の
    common/sec_data統合まで温存。
41. **[[LAYER3-Q4-IMPLIED-NOT-MIGRATED-1]]** — DONE。
    `layer3_builder.py`が共有モジュール`q4_implied.py`を使わず独自の
    Q4逆算実装を維持、スコープ/None安全性/CapEx符号処理が乖離。対応:
    共有モジュールへ移行、PascalCase/snake_case両対応を追加。
42. **[[Q4-IMPLIED-CALC-TRIPLICATION-1]]** — OPEN。「FY−(Q1+Q2+Q3)=Q4」
    ロジックが依然3箇所（normalizer.py/ttm_calculator.py/STONKS SILO
    financial_trend_calculator.py）に独立実装、各々独自の重複防止
    ガードを持つ。現状の不整合は無いが、1箇所だけ修正すると挙動が
    分岐する構造的リスク。未対応（優先度低）。
43. **[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]** — OPEN（重要な構造的発見）。
    `common/sec_data/ttm/`はparser.pyとは完全に独立した実装
    （layer3_builder.py）で、fact_overrides.json未読込、
    `_resolve_bs_entity_mixing()`・`_backfill_total_liabilities_via_
    identity()`・`_align_cost_of_revenue_to_revenue_period()`の相当品
    を持たない。本セッションのparser.py側修正の大半がTTM/RICE計算
    経路に到達しない。実際に3系統の独立生成パス（parser.py→annual、
    quarterly.py/normalizer.py→normalized、layer3_builder.py/
    ttm_calculator.py→ttm）が並存すると確定（`SEC_EDGAR_LAYER_
    DESIGN.md`の既知の「3スキーマ併存」と一致）。影響測定の結果
    現在進行形の実害はゼロ（該当フィールド・年度がTTM出力対象外
    または現行TTM窓の外）と確認済みのため優先度を高→中に引き下げ、
    新DB構築プロジェクトのフェーズD consumer切替まで構造的リスクとして
    保留。
44. **[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]** — DONE（重要、
    EXTRACTION_DESIGN_PRINCIPLES.md原則1の根拠）。
    `calc_ttm_series()`の「アンカー日以前の直近4件」ロジックが件数のみ
    検証し日付の連続性を検証しなかった。RCATの標準タグが約11ヶ月
    途切れ（継続/非継続区分＋決算期変更の重複）、同一2四半期が2つの
    異なるTTMアンカーで二重計上。18銘柄・3サブパターン。対応:
    `_last4_is_contiguous()`（合計スパン305-425日・隣接ギャップ±10日）、
    失敗時は`quarters_used=0`（古い代替を探す代替設計は6ヶ月古い
    データを黙って引き込むと判明し不採用）。
45. **[[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]** — DONE
    （原因特定、対応は#44）。RCATのTTMベースFCF計算（年次パイプライン
    を迂回）が同種の継続/非継続タグ問題を持つか調査、根本原因はより
    一般的な連続性チェック欠如（#44）と判明。ΔIV=$0確認済み。
46. **[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]** — DONE（原因特定/対応不要で
    クローズ）。当初`get_fcf_5yr_avg()`が実質3年平均になっていると
    疑われたが、本番FCF計算はこの関数を使わずTTM系列経路
    （`_select_fcf_source()`）を使用、#44/#45の対象と確認。
47. **[[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]** — DONE
    （スコープ拡大し#48へ）。RCAT annual_2024/2025.jsonの
    operating_cash_flowが完全欠損（FY2024から単一OCFタグ→継続+非継続
    分割タグへ移行、候補リストは非分割タグのみ）。「広義の非継続タグが
    偶然、営業活動限定の非継続タグと同額になる」設計トラップも発見
    （RCATの投資/財務活動非継続CFがたまたま$0のため）。
48. **[[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]** — OPEN
    （優先度中、影響調査後に高→中）。25/105銘柄（AAPL/MSFT/TSLA/XOM/
    CAT/ABBV等）が標準OCFタグを欠き継続/非継続分割タグのみ持つ。
    2サブパターン:(A)大半は非継続タグ自体が存在せず継続=総額（低
    リスク）(B)少数（RCAT/HON/AVAV）は非継続タグが存在し真の合算が
    必要（#47の「広義タグの罠」への注意が必要）。影響調査の結果RCATの
    現行5年窓のみ実際に影響、他24銘柄のギャップ年度は2011-2017年で
    現行窓の外（構造的に再侵入不可）。未対応（低緊急度）。
49. **[[LAYER3-SGA-Q4-MISSING-1]]** — DONE。
    `selling_general_and_administrative`が`Q4_IMPLIED_FIELDS`にも
    `MISSING_QUARTER_IMPLIED_FIELDS`にも含まれず、42銘柄/171四半期で
    Q4が慢性的に欠落（新規フィールドのため旧パイプラインとの比較
    ベースラインが無く既存TTM回帰チェックで未検知）。対応: 両リストへ
    追加、新設`newfield_q4_cutoff_check.py`。
50. **[[LAYER3-TTM-REGRESSION-NEWFIELD-BLINDSPOT-1]]** — DONE
    （テストカバレッジに関するメタバグ）。TTM回帰比較スクリプトが旧
    パイプラインのキーのみを走査するため、新Layer2/Layer3スキーマ
    のみに存在するフィールド（6件: STI/total_liabilities/eps_basic/
    eps_diluted/cost_of_revenue/SGA）が構造的に回帰テストから不可視
    （#49が複数回の回帰通過を経て見逃された原因そのもの）。対応:
    回帰スクリプトの汎用化ではなく6フィールド individually検討
    （STOCK型フィールドは対象外・EPS系は比率フィールドで加算比較
    不可、cost_of_revenue/SGAは専用常設チェック新設）。

### テーマB2（特殊ケース）: BS会計恒等式検証層

51. **[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]** — DONE。
    傘型発見: 本セッションの5バグ（期間長・total-liabilitiesフォール
    バック・PL cross-accn・SPACシェル混在・TTM連続性）が全て同一の
    設計欠陥「純粋な新しさだけで候補選定し、他フィールド/期間/会計
    制約と一切照合しない」に帰着すると判明
    （EXTRACTION_DESIGN_PRINCIPLES.md原則2・3に直接対応）。105銘柄
    予備スキャン: TA≠TL+SE 156件/50銘柄、GP≠Rev−COGS 43件/9銘柄、
    OI>GP 22件/LMT型のみ、NI≠EPS×Shares 67件/31銘柄。分類:
    TA=TL+SE→#52で解決、GP≠Rev−COGS→GOOGL順序バグ（#71）に帰着、
    OI>GP→genuine確定（LMT防衛産業年金調整）、NI≠EPS×Shares→COHR
    単位スケールバグ（#70）に帰着。
52. **[[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]** — DONE。
    新設CHECK-29/WARN-29層: `TA==TL+SE`本体一致→不一致時のみ
    `+NCI+一時的持分`のOR条件拡張形（無条件加算は33銘柄で二重計上する
    と判明し不採用）。許可リスト7タグ＋「Including...」supersedes
    ルール。156件中133件（85.3%）を解消、残り23件→#53。
53. **[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]** — DONE（23→13件へ
    段階的解消、残り13件は個別トリアージ済み）。混在する原因:
    cross-accn限定開示（COHR→#54へ）、RedemptionValue vs
    CarryingAmount基準相違（HEI、5件、許可リスト条件付きフォールバック
    で解消）、合算タグと内訳タグの重複計上（ONDS、SUPERSEDESルール
    拡張で解消）。最終状態: genuine 2件（BKNG、FairValue基準タグのみ
    のため設計上除外が正当）、許可リスト拡張で対応可能2件（ASTS2020・
    RDW2020、未実装）、原因不明7件（PLTR2019/CART2023-25/V2008/
    CELH2025/ASTS2019）。CRM(2011)/VRT(2017)はCHECK29のタグ不足問題
    ではない独立バグとして#24へ分離。
54. **[[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]** — DONE。
    CHECK29の本人accn限定照合設計では、該当NCI/一時的持分タグが
    後続filingの比較列にのみ開示され当該年度自身の10-Kには一切
    存在しないケース（COHR 2022/2023合併後優先株式、CRWV/VRTでも
    シミュレーションで発見）を原理的に検知できない。実装1回目
    （own-accn0件時の無条件cross-accnフォールバック）で回帰が発生:
    9件の想定外変化のうち5件は既存の正しい解消を破壊（別タグ族での
    二重計上、既に均衡していた年度への不要な加算）——対象17件のみ
    シミュレーションし全母集団（既に解消済みの約88件）への影響確認を
    怠ったため発生。対応: 2段階ガード（ベースゲート: own-accnのみで
    既に均衡する場合はフォールバック自体をスキップ／重複値ガード:
    既にmatched済みの値と同額の候補は不採用）。**明記された教訓**:
    対象サブセットのみへのシミュレーションでは既解決集団への回帰を
    見逃す——常に全母集団で再シミュレーションすること。
55. **[[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]** — OPEN
    （優先度低、外見上の問題のみ）。`bs_identity_violations_log.json`
    のキー順序がPython `frozenset`のハッシュランダム化により実行の
    たびに非決定的に変化、データへの実害はないがgit diffノイズを
    生む。未対応。

### テーマC: 会計年度・期間境界の判定

56. **[[FY52WEEK-BUCKET-MISPLACE-1]]** — DONE（主要な根本修正）。
    `determine_fiscal_year()`の月のみ比較（`month > fiscal_end_month`）
    が52/53週企業（決算期末日が年ごとに月境界を跨いで漂う、AVGO/DELL/
    CAKE/ELF）で失敗し、真の年次値が誤った年度バケットに押し込まれ
    90日の四半期スタブが空いたバケットを未検証のまま埋めていた。根本
    原因: 月のみ比較（アンカー日ウィンドウ比較ではない）。また12ヶ月
    決算企業は年度跨ぎ補正を一切トリガーしない構造的副作用も判明
    （CAKE/CDNS/JNJ/TDYにも影響）。対応: SEC submissions
    `reportDate`ベースの本人データ判定層を既存フォールバックの上に
    追加、後にARCH-DATA-1の3段階再設計に統合。
57. **[[ARCH-DATA-1]]** — DONE（数ヶ月規模の基盤エピック）。「SECデータ
    正規化レイヤー強化」。根本診断: 2026年6月の大半のバグは「未対応の
    SECデータ形状バリエーション」であり、ロジックミスではなかった。
    年次値決定の3段階再設計を実施: (1)値決定——同一(タグ,日付)キーで
    filed日が新しい方を優先、10-K/Aを候補プールに拡張しfiled日
    tie-breakを適用（10-K/Aが以前100%除外されていたと判明、30/105
    銘柄に影響）(2)会計年度ラベル算出——月比較をアンカー日
    （月+日）距離ベースへ置換、真のFYE変更を検知する複数クラスター
    アンカー検知を追加（RCAT/AVGO/MSCI/NOW）(3)fyタグ照合——算出年度
    と発行体自身のXBRL `fy`タグをCHECK-23として相互検証、発行体自身の
    タグが単純に誤っているケース（NVDA・CAKE、一次情報で確認）を検知。
    加えて第4段階: 本人データ判定をBS instant fact（start日付を持たず
    従来の全安全策の対象外だった）へ拡張、VZ型偽陽性リスク（instant
    factでは同一end_date≠同一概念）も発見。残存サブ課題も完了:
    reader.py/pipeline.pyに散在した4つの重複「最新四半期ファイル」
    実装の重複排除、revenue-tag-conflict検知ツール。RCAT型FYE変更の
    「検知」（是正ではない）は後続（#60）へ委譲。
58. **[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]** — DONE。
    `_detect_fiscal_end_month()`/`_detect_fiscal_anchor_date()`が
    銘柄あたり単一アンカーのみ対応、真に決算期変更した企業（ELF:
    12月→3月）を表現できない。期間長フィルタ導入後に連鎖的な年度値
    入替えを引き起こした。根本原因: 単一アンカー設計が時代別FYE変更を
    表現できない（#56と同テーマだが52/53週の漂いではなく真の変更）。
    対応: `detect_fiscal_anchor_clusters()`（副次クラスターを
    min_support≥2で検知）、`determine_fiscal_year()`が`extra_anchors`
    を受け取り全クラスターを横断探索。105銘柄検証でELFのみが実際に
    変更していると確認（RCAT/AVGO/MSCI/NOWは副次クラスター検知される
    が実際の日付は常に主アンカー付近のため影響ゼロ）。
59. **[[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]** — DONE（誤警報、
    正しく除外）。RCAT最新10-Kが12月・4月両方のFYEアンカーに「投票」
    しているように見え3回目の変更を疑われたが、調査の結果真の変更は
    2回のみで、両票はSEC Regulation S-Xの比較列開示要件による正常な
    現象と確認。調査中の副次発見（RCATのFYE移行期間10-Kスタブが両
    年次ファイルから完全欠落）は#62へ分離。
60. **[[FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1]]** — DONE。真にFYE
    変更した企業で「真の本人データ」と「翌年度10-Kの比較再掲」が
    同一バケットで衝突するケースを既存チェック（CHECK-22/23）は
    検知できない。根本原因: 「異なるfyタグが同一バケットで衝突」の
    チェック不在。対応: 新設CHECK-24/WARN-24、`_fiscal_anchors_far_
    apart()`（30日超の円環距離）フィルタでゲート——初期版は同一
    (月,日)隣接年度の漂いを持つ7銘柄で誤検知（WARN-23型パターン、
    境界衝突ではない）。RCAT(2件)+LITE(1件)+WST(1件)を検出、後者2件は
    未確認のまま#61へ分離。
61. **[[FYE-BOUNDARY-COLLISION-UNCONFIRMED-1]]** — DONE（2026-09-05）。
    LITE/WSTのWARN-24ヒットを一次情報（SEC EDGAR原本）で確認。LITEは
    2015年JDSUスピンオフに伴うPredecessor/Successor会計処理、WSTは
    開示書類に表示されない非表示のXBRLタグ付けアーティファクト（古い
    コンテキストの使い回し）と確定、いずれも真のFYE変更ではない。
    `config/warn_acknowledged.json`へ登録の上クローズ。
62. **[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]** — DONE。`fetcher.py`の
    `relevant_forms`集合が`10-KT`/`10-QT`（FYE変更後の移行期間報告書）
    を除外しており、`is_own_data`がこれらのaccnに対し恒久的にFalseと
    なり、正しく提出された移行期間データが年次バケット競争から脱落。
    RCATの8ヶ月の2024移行期間スタブが両年次ファイルから完全欠落と
    確認。根本原因: 候補form種別フィルタがFYE変更移行filingを
    網羅していない。対応: オプション③（検知のみ）——新設
    CHECK-28/WARN-28が`accn_to_reportdate`から欠落した10-KT/10-QT
    accnを検知するのみでバケットロジックは変更せず（オプション①
    ——form追加＋バケット再設計は、RCATの本人データ10-Kと10-KTが
    SEC自身により両方`fy=2024`とタグ付けされ真のバケットキー衝突を
    引き起こすため、現状1銘柄規模には過大な再設計と判断し不採用）。
63. **[[FY-COLLISION-LOG-NONDETERMINISTIC-1]]** — DONE。
    `fy_collision_log.json`の重複エントリは非決定的と思われたが、
    根本原因は完全に決定的と判明:
    `_extract_values_best_candidate()`が候補タグごとに
    `_collect_own_data()`を呼び出し、複数候補タグが独立に同一衝突を
    検知するたびに重複追記される。根本原因: ロギングループ構造
    （選定ロジック自体ではない、値は常に正しかった）。対応: ログ
    書き込み層での重複排除キー（対症療法的修正、根本のロジック変更は
    リスクが大きいと判断）。
64. **[[MRVL-2019-2020-NULL-1]]** — DONE（無害・構造的と根本原因
    特定）。MRVL annual_2019.jsonが完全に空、2021年のCIK切替
    （持株会社再編）が原因。新CIKの最古10-Kの比較窓がFY2019まで届かず、
    真のFY2019データは旧CIK配下にのみ存在するがシステムが追跡しない。
    根本原因: CIK不連続が構造的な最古年度ギャップを生む（約86ファイル
    に及ぶ一般パターンと確認、MRVLは単に異常に最近発生しただけ）。
    #65へ分離。
65. **[[CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1]]** — DONE。CIK不連続
    パターンを3類型に正式分類: ①同一事業の持株会社再編（MRVL/GOOGL/
    AVGO/DELL）——複数CIKマージで解決（`cik_history.json`、fetcherが
    旧CIKデータを取得・統合）②スピンオフ/カーブアウト（CEG/LITE/ABBV/
    GEV/SN/CON）——意図的に非連結のまま（親会社連結の代理データは
    単独実績として誤表示するため）③破産fresh-start会計（VST）——
    意図的に非連結のまま（fresh-start会計は設計上連続性を断つ）。
    個別にGOOGL FY2012/2013も`fact_overrides.json`で修正（Motorola
    Mobile非継続事業再表示、他の仕組みでは捕捉不可）——この発見が
    CHAT_RULES.md新原則「銘柄固定のハードコード禁止」の契機となった。
    `registration_validator.py`にP6汎用検知器を新設（既知9件で精度
    約65%/再現率100%）、将来の新規登録向け。
66. **[[SPAC-STUB-PERIOD-VERIFICATION-1]]** — DONE（11銘柄/12銘柄年度
    調査、全て現状で正しいと確認）。SPAC合併/上場前スタブ期間の
    非365日データ（ASTS/IONQ/JOBY/RKLB/SOFI/SPIR 2019-2020、SOUN/
    APGE/NOW/RCAT/VRT）を個別に10-K一次情報で検証、全て正しいか
    期間長フィルタで安全にNone化されていると確認。修正不要。
67. **[[SPAC-STUB-PERIOD-FIELD-SPLIT-1]]** — DONE（スコープ2回修正）。
    BBAI/RDW 2020のPredecessor/Successor分割期間を10-K一次情報で
    検証、PL/CFフィールドは既に期間長フィルタで安全にNone化済み
    （対応不要）。ELF(2015)/KULR(2015)は当初同型と疑われたが精査の
    結果SPAC関連ではないと確認（ELF=既に別途修正済みの比較開示混同、
    KULR=単純な短い初回会計年度、合併なし）。BS側の実害（BBAI/RDWの
    数学的に矛盾する混在実体BS）は#23へ分離。

### テーマD: クロスfiling/tie-break選定ロジックの欠陥

68. **[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]** — DONE（#69に統合）。
    `_extract_single_key()`のtie-breakは、本人データが無く複数の比較
    再掲が競合する場合FIFO（「最初に見つかった＝実質最古」が勝つ）と
    いう未文書化の挙動。COHR(2010): 正しい値は後のfilingの比較列に
    存在したが、より古い（誤った）FY2011 10-Kのエントリが先に見つかる。
    「常に新しいfilingを優先」への全母集団シミュレーションで31銘柄/
    124件変化、うち genuine改善は2件（COHR）のみ、122件は回帰/危険
    （VZ(2008) NI符号反転、SOUN/KULR SPAC実体混同、HON/FCX/HEIの
    再表示ノイズ）、加えてtotal-liabilities恒等式バックフィル安全策
    との危険な相互作用（WMT 2014）。一般的なtie-break変更ではなく
    #70のゲート付き設計に統合。
69. **[[XBRL-UNIT-SCALE-MISMATCH-DETECTION-1]]** — OPEN（大半は
    随伴修正で解決済み、汎用検知器は未実装）。提案された汎用チェック:
    同一タグ/期間の値がfiling間で10のべき乗倍異なる（スケール表記
    エラー、例: 「単位:千」の見落とし）。単純な比率≥100閾値は72銘柄で
    ノイズ過多、「比率≈10^n（±2%）」に絞ると18銘柄/126件（株式数/
    EPS/D&A/純利益/長期負債/負債/営業利益フィールドに及ぶ）。ガード
    条件（同符号＋10のべき乗比率）は現行母集団ではCOHRの既知2件でしか
    発火しないと確認済み——一般的なtie-breakコード変更はリスク/便益に
    見合わないと判断し保留、COHRの件は`fact_overrides.json`個別
    オーバーライドで解決（#71参照）。将来の同種検知向けの常設
    WARN-30型検知器: 未実装、設計記録として残置。
70. **[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]** — DONE。COHR自身の
    FY2011 10-Kが`WeightedAverageNumberOfDilutedSharesOutstanding`を
    2009-2011年について1/1000スケールで自らタグ付けしていた（発行体
    自身のfiling誤り、後にFY2012の再表示で自己修正）——本人/一次
    データ自体に単位スケールエラーが含まれるという新種のバグ類型
    （既知の全てのタグ選定/cross-accnパターンとは別種）。根本原因:
    クロスfiling間の桁一貫性チェックが皆無。対応:
    `fact_overrides.json`個別オーバーライド（tie-breakコード変更は
    #69参照の通り広範すぎリスク過大として不採用）。
71. **[[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]** — DONE。
    `fact_overrides.json`の手動revenue補正（Motorola Mobile非継続
    事業再表示）が`save_parsed_data()`内で
    `_backfill_gross_profit_from_revenue_cogs()`実行**後**に適用され、
    gross_profitが補正前revenueから逆算されたまま再計算されなかった。
    根本原因: パイプライン実行順序バグ（オーバーライド適用が導出
    フィールドバックフィルに対して順序付けされていない）。対応:
    `_apply_fact_overrides()`を全バックフィル/導出ステップの前、抽出
    直後に実行するよう移動。

### テーマE: 銘柄固有オーバーライド機構・移行ギャップ

72. **[[SOFI-TICKER-RESTRICTIONS-NOT-MIGRATED-1]]** — DONE。
    `config/sec_concept_definitions.json`の`ticker_overrides`
    フィールドが`build_ticker_store()`から実際には一切読まれておらず
    無言で機能不全だった。旧`quarterly.py::TICKER_RESTRICTIONS`は
    素朴なfield/action/noteスキーマでは表現できないより豊かなパターン
    （単純除外、単一/複数概念オーバーライド、cross-filing-tag条件付き
    合算、非機能的な注意書き）をカバーしていた。具体的に7銘柄影響
    （APP CapEx汚染、GOOGL LTDebt急増、IONQ revenue異常
    [#21と同根]、KLAC、V/TER/NVDA STIギャップ）。対応: 3パターン型を
    持つ完全な`ticker_overrides`機構を実装。
73. **[[FY52WEEK-BS-STI-OVERRIDE-DESIGN-1]]** — DONE（5銘柄中4件、
    NVDAは分離）。short_term_investmentsがKLAC/NVDA/SOFI/TER/Vで
    銘柄別オーバーライドを必要とする、それぞれ*異なる*正しいXBRL概念
    を要するため（汎用候補リスト修正では対応不可）: KLACは
    「Current」接尾辞なしタグが必要（接尾辞付きタグは2021年以降報告
    停止——別のサイレントタグ移行）；TER/Vはより狭い/異なる範囲の
    タグが必要（過大/過小計上防止）；SOFI（未分類BS）は
    `OtherInvestments`が必要（当初「5%近似で許容可能」との想定は
    真のタグ発見後不要と判明）。対応: 4/5銘柄を`sti_concept`
    ticker_restrictionsで解決（SOFIの既存`ltdebt_concept`と同型
    パターン）。
74. **[[NVDA-STI-TAG-UNIDENTIFIED-1]]** — DONE。NVDAのSTIは
    sti_concept機構では対応不可、正しい値は**2つのタグを2つの異なる
    filingにまたがって合算**する必要があった（新規上場投資先の株式
    評価額が四半期途中で非公開→上場評価替えされ、株式相当部分の
    タグが後続10-Qの比較列にのみ出現し当該10-K自体には一切現れない）。
    根本原因: 単一filing/単一タグのオーバーライド機構では真のクロス
    filing複数タグ合成に不十分。対応: 新設`cross_filing_tags`機構
    （ticker×期間×フィールドの狙い撃ち複数タグ合算、通常の本人データ
    filingフィルタを迂回）、加えて将来の再利用向けにWARN-27（残差
    パーセンテージの健全性チェック）を新設。
75. **[[SPLIT-REALTIME-GAP-1]]** — DONE（7銘柄、「構造的限界」から
    「解決可能」へ再分類）。正しいfact選定（filed日最新のtie-break）
    があっても、分割から1年以上経過し以降のどの10-Qでも比較列として
    再掲されない四半期（10-Qは前年同四半期のみ比較列表示）は分割前
    株式数のまま恒久的に固着する。当初のNVDA単独スコープを超える
    全101銘柄スキャンで確認: AVGO/CPRT/WMT/LRCX/CELH（+TSLA、+KLAC
    事前登録・無効）。RCATは除外（分割は一度も発生していない、
    見かけの「振動」は真のXBRL四半期開示ギャップへの正しいフォール
    バック挙動で、真のFYE変更が重なっていた）。根本原因: 根本的な
    データ可用性ギャップ、既存の`split_history.yaml`＋
    `apply_split_adjustments()`機構で銘柄別登録により解決（構造的に
    不可能ではなかった）。一次情報補正でCPRTの実際の分割日がyfinance
    報告日と異なると判明。
76. **[[SPLIT-REALTIME-GAP-REVERSE-1]]** — OPEN。同型ギャップが
    KULR/SPIRの*逆*分割（1-for-8）でも疑われる、yfinanceで両分割とも
    実在と確認済みだが、`apply_split_adjustments()`の比率計算が
    ratio<1（逆分割）で正しく機能するかは未検証（順分割でのみ
    検証済み）。未対応（優先度低）。
77. **[[SPLIT-AUTO-CHECK-1]]** — DONE。EPS Analyzerの独立SEC抽出
    パイプライン（`extract_key_facts.py`、`common/sec_data/`とは
    完全に別）が、同一期間に複数のSEC fact（分割前 vs 分割後再表示値）
    が競合する場合のfact選定ルールがQ1-Q3ループ（無条件上書き＝
    後勝ち）とQ4ロジック（先一致でbreak＝先勝ち）で不整合だった。
    根本原因: 兄弟コードパスで同一概念問題に2つの異なる選定ルール。
    対応: 両ループとも「最も新しくfiledされたfactを優先」に統一。
    副産物としてASTS希薄化株式数の振動を発見→#78。
78. **[[ASTS-SHARES-OSCILLATION-1]]** — DONE（スコープ3→9銘柄に拡大）。
    EPS Analyzerの株式数フォールバック#4（無条件の当日yfinance株式
    数）は「全期間欠損」銘柄（例: Visa）向けに設計されていたが、
    「一部四半期のみ欠損」銘柄でもトリガーされ、今日の株式数を
    数年前の四半期（ASTS SPAC合併前期間含む）へ逆伝播していた。根本
    原因: フォールバック段階のスコープが広すぎ、意図と異なる失敗
    モードに適用されていた。対応: yfinanceフォールバックの前に新中間
    フォールバック#3（「直近の実四半期から継承」）を挿入。実データ
    比較で真の影響範囲は当初推定3銘柄ではなく9銘柄（ASTS/AVAV/RCAT/
    CART/CEG/BROS/GEV/XOM/CON）と判明。
79. **[[EPS-UPC-PREREORG-1]]** — OPEN。#78の副作用: Up-C構造銘柄
    （BROS）の組織再編前四半期は`net_income`が正確に$0（PubCoがまだ
    OpCoの経済的持分を保有しない）——株式数が正しく埋まった結果、これら
    の四半期がSBC加算により見かけ上の正のAdjusted EPSを生成する
    ように新たになった。根本原因: 「Up-C再編前$0純利益」四半期を
    Adjusted EPS計算から除外すべきという検知が存在しない。未対応
    （設計上の未確定事項）。
80. **[[EPS-ANALYZER-NORMALIZE-SCOPE-1]]** — DONE。EPS Analyzerの
    独立SECパイプラインを統合common/sec_data正規化へ統合すべきかの
    スコープ判断: 部分統合のみ——net_income候補タグリストを
    `tag_definitions.py`へ統一（この過程で真の潜伏バグを発見・修正:
    3つ目の矛盾する候補リスト`net_income_priority`が41銘柄で常に
    サイレントに`ProfitLoss`へフォールスルーし、正しい
    `NetIncomeLoss`系タグを使っていなかった）、`quarterly.py`と
    `extract_key_facts.py`が再利用する共有プリミティブ
    `fact_selection.py::select_latest_filed()`を抽出。revenueタグ
    処理は意図的に未統合（銘柄固有`revenue_concept`オーバーライドとの
    非互換性を文書化）。

### テーマF: SEC EDGAR submissions/company_facts APIレイヤー

81. **[[SECDATA-COMPANYFACTS-OVERLOOKED-1]]** — OPEN（記録訂正のみ）。
    `company_facts.json`（フィルタなしの完全SEC EDGAR応答、582MB/
    105銘柄）が過去のデータインベントリ調査で見落とされていた。
    記録訂正のみ、修正なし。
82. **[[SEC-SUBMISSIONS-DUAL-FETCH-1]]** — OPEN。SEC submissions API
    が`fetcher.py`（週次、キャッシュあり）と`edgar_rss_monitor.py`
    （毎日、live取得、キャッシュなし）に独立に重複取得されている
    ——鮮度目的の意図的設計だが冗長。未対応（優先度低〜中）。
83. **[[SEC-XBRL-MISSING-START-ENTRY-1]]** — 項目22と同一（重複参照）。

### テーマG: TTM/四半期層の構造的・技術的負債

84. **[[TTM-STOCK-FIELDS-DEAD-1]]** — DONE。`ttm_calculator.py`の
    `STOCK_FIELDS`/`SHARES_FIELDS`分類は本番で構造的に到達不能——
    これらを処理する唯一の関数（`calc_ttm()`/`save_ttm()`）は
    2026-05-07の`calc_ttm_series()`移行後デッドコード化しており
    2026-05-11の2分間の偶発的呼び出し以来本番で呼ばれていない。根本
    原因: 移行時の不完全な非推奨化（旧経路を削除/リダイレクトせずに
    新経路を出荷）。対応（オプションa）:
    `calc_ttm()`/`save_ttm()`と専用ヘルパーを削除、
    STOCK_FIELDS/SHARES_FIELDS定数は残置（import時契約検証チェックが
    必要とするため、現在は痕跡的である旨のコメント付き）。
85. **[[TTM-FLOW-FIELDS-FROZENSET-NONDETERMINISTIC-1]]** — OPEN。
    `FLOW_FIELDS`が`frozenset`定義のためプロセス起動ごとにキー順序が
    非決定的、TTM再生成のたびに無関係な巨大git diffが発生。未対応
    （優先度低）。
86. **[[LAYER3-TTM-TEST-SUITE-SHAPE-MISMATCH-1]]** — OPEN。
    `tests/test_ttm_calculator.py`（3テスト）が新Layer3ストア形状
    （`{"fields": {name: {"entries": [...]}}}` vs 旧
    `{"fields": {name: [...]}}`）に対してクラッシュ——フェーズC完了に
    必要。未対応。
87. **[[LAYER3-EPS-UNIT-MISMATCH-1]]** — OPEN（優先度高と記載だが
    現在進行形の実害ゼロ）。`config/sec_concept_definitions.json`が
    eps_basic/eps_dilutedに`"unit": "USD"`を指定（正しくは
    `"USD/shares"`）——厳密一致の単位検索が全105銘柄で失敗し
    Layer3/TTM経路で両フィールドが完全に空になる。新規フィールドで
    旧パイプラインとの比較ベースラインが無いためフェーズA回帰レビュー
    で未検知（#50と同種の死角）。現状このフィールドをTTM経由で読む
    消費者が存在しないため実害なし——ただしconfig自体は機能不全。
    未対応。

### テーマH: segment_fetcher/セグメントデータ重複

88. **[[SEGMENT-FETCHER-DUPLICATE-ORPHAN-1]]** — DONE。
    `segment_fetcher.py`が2箇所（`src/value/tanuki_valuation/`と
    `common/sec_data/`）に存在し、単一の2026-04-25コミットが
    バグ修正（XBRLコンテキスト境界正規表現修正＋金融業界タグ）を
    `common/sec_data/`側のみに適用し、`src/value/`側は誤って修正前
    スナップショットを「新規」ファイルとして再コミットしたことに
    由来する乖離。両ファイルともimport元ゼロ・CI呼び出しゼロの孤立
    状態（手動の一回限りセグメントデータ入力にのみ使用、現在は
    Claude Code経由に移行予定）。対応: 参照ゼロを確認しコメント1件を
    移植後、劣った方の複製を削除。
89. **[[DOCS-SECDATA-NORMALIZED-DIR-STALE-1]]** — DONE。
    `docs/common/sec_data/normalized/`——7つ目の重複データ
    ディレクトリ——が`quarterly_review_generator.py`/
    `tail_dcf_bridge.py`（TANUKI TAIL）から`COMMON_NORMALIZED_DIR`
    として読まれていたが、2026-05-23以降何も同期していなかった（51
    ファイル vs 実ストアの105）。再調査で**3人目の消費者**を発見:
    公開GitHub Pagesの`stock.html`フロントエンドもこの同じ陳腐化パス
    をクライアント側でfetchしていた——公開サイトのキャッシュフロー
    表示も約2ヶ月間サイレントに陳腐化していた。根本原因: 自動同期
    ステップの無い手動作成の公開向けデータコピー。対応: TAILのPython
    読み取り側を実ストアへリダイレクト、`SEC_Data_Update.yml`に週次
    `rsync --delete`ステップを追加し今後の公開コピーの鮮度を維持。
90. **[[NORMALIZER-YTD-METADATA-STALE-1]]** — OPEN。
    `normalizer.py::_ytd_to_quarterly()`はYTD値から四半期差分への
    変換自体は正しいが、`start`/`period_days`メタデータを変換前の
    ままにする。根本原因: `dict(entry)`が浅いコピーでメタデータを
    再計算せずコピーする。現状これらのメタデータフィールドをロジックで
    読む消費者は確認されていないが、潜在的な罠として記録。未対応。
91. **[[SECDATA-STORAGE-FRAGMENTATION-1]]** — OPEN。
    `common/sec_data/`は6系統以上のファイルシステムスキーマ
    （data/annual+quarterly、raw/、normalized/、ttm/）を持ち
    `normalized/`経由で最低5つの独立消費モジュールへ供給。統合は
    複数の前提修正（CAPEX-SIGN-UNNORMALIZED-1は完了）でほぼ解消
    されているがブロック中。未対応（優先度中、`INPUT_DATA_TOBE.md`の
    より広範な統一ストア設計待ち）。
92. **[[SCHEMA-NORMALIZED-ISSUES-1]]** — OPEN（6つのサブ課題の統合、
    個別記述）: ①STDebtタグ網羅性が`normalized/`で著しく劣化
    （単一タグ・フォールバックなし）vs `data/quarterly`（9タグ
    フォールバック）——一部銘柄で最大100%空②SM vs SGA概念混同
    ——`normalized/`は両方を単一`SM`フィールドに折り畳み、純S&Mタグを
    欠く銘柄では黙ってSGA総額へフォールバック、同一フィールド名が
    銘柄により異なる意味を持つ③LTDebt優先順序が`normalized/`で
    `parser.py`の明示的に二重計上防止済みの順序と逆転（105銘柄全数の
    数値確認で現在進行形の実害ゼロと確認済みだが構造的リスクは残存）
    ④SharesBasicが2システム間で*異なる会計概念*を指す（BS期末値 vs
    P&L期中加重平均）——消費者側の監査は未実施⑤
    `normalized/{TICKER}_quarterly_normalized.json`というファイル名は
    四半期限定を示唆するが実際はannualエントリも混在⑥D&Aが
    `normalized/`でフォールバック候補を持たず、代替D&Aタグ名を使う
    銘柄（例: LMT）で空になる——ただし実際の成長率消費者はannual/
    quarterly（parser.py由来）データを読むため計算への実害なし。
    いずれもLayer2/Layer3統一スキーマ移行時の解消を予定、個別未対応。

### テーマI: ワークフロー/クロスパイプライン同期ギャップ

93. **[[WORKFLOW-SEC-TANUKI-GAP-1]]** — OPEN。`SEC_Data_Update.yml`
    （毎週日曜21:00 JST）と`TANUKI_VALUATION_Update.yml`（平日
    23:05 JSTのみ）に、`config/workflow_dependencies.json`が論理的
    依存関係を宣言しているにもかかわらず自動トリガー依存が一切無い
    ——SECデータは新鮮だがTANUKI VALUATION出力が陳腐化したままの
    構造的な約26時間の窓（日〜月）が生じる。[[WARN12-COHR-ONDS-1]]
    はこのギャップの具体的な顕在化例。未対応（提案対応: `workflow_run`
    トリガーチェーン化）。
94. **[[WARN12-COHR-ONDS-1]]** — DONE（コードバグではないと根本原因
    特定）。COHR/ONDSのCash-STI期ズレWARNは純粋なタイミング/生成
    順序のアーティファクトと判明: TANUKI VALUATIONのlatest.jsonが、
    STI値を変更したSEC自動更新の約20時間前に生成されていた——#93の
    ギャップそのもの。コード変更なし、2銘柄分のパイプライン再実行で
    解決。

### テーマJ: SPAC/候補タグ/accn検知ツールのスキャン

95. **[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]** — DONE。
    KULR(2019)の`current_liabilities>total_liabilities`矛盾は当初
    SPAC実体混在が疑われたが、#1（`TOTAL-LIABILITIES-FALLBACK-
    TAG-DESIGN-FLAW-1`）と同型の`LiabilitiesAndStockholdersEquity`
    フォールバックバグと根本原因特定され、その大きなエントリへ統合。
96. **[[HEI-LRCX-TA-TLSE-UNEXPLAINED-RESIDUAL-1]]** — DONE
    （誤登録後に訂正）。HEI(2020)/LRCX(2012)は既知のNCI/一時的持分
    タグを含めても未解明の残差があると思われたが、**網羅的**タグ
    列挙（限定的な候補名チェックではなく、かつ「直近6エントリのみ」
    表示ではない）による再調査で正しいタグが実在し完全に解消すると
    判明。**明記された教訓**: 候補タグの完全性チェックはaccn/end_date
    の全タグを列挙すべきで固定候補リストでは不十分、「直近N件のみ
    表示」する履歴閲覧ツールは古い年度のデータを隠しうる。

### テーマK: その他のスコープ/ツーリングに関するメモ

97. **[[CIK-ORPHAN-FLAGS-1]]** — DONE（BX）/OPEN（ENB）。BXとENB両方が
    2026-07-02のスキーマ移行由来の不明な経緯で4システムフラグ全て
    falseの`active`として孤立登録されていた。BXは完全登録抹消
    （選択肢A）。ENB（yfinanceでカナダ企業と確認済み——本来
    CLAUDE_CODE_START.md Step 0で登録拒否されるべきだった）は
    TANUKI-FIN-1（代替DDM型評価フレームワーク）へのルーティング判断
    待ちで開いたまま。
98. **[[DEAD-CODE-AUDIT-BATCH-1]]** — DONE（2026-09-09）。
    `phase1_scan.py`（2026-06-11一回限りの診断）・`quality_checker.py`
    （独立Q01-Q13チェックカタログ、何にもimportされず）・
    `backfill_history.py`（一回限りのバックフィル）は3件とも参照ゼロを
    確認しファイルごと削除。`report_txt_parser.py`は当初「孤立モジュール」
    と見立てていたが、CHAT_RULES.md「銘柄スクリーニング着手前の確認事項」
    （2026-07-10の教訓）の標準フロー手順②として現役で参照されている
    （Pythonの`import`ではなくCLI直接実行のため、当初のgrep調査が
    見落としていた）ことが判明し、削除せず現状維持とした
    （report_consistency_check.py側の独自パース実装とは対象フィールド・
    用途が異なり、統合は不採用）。
99. **[[TICKER-DIRECT-ACCESS-GUARD-1]]** — DONE（CIガード）。新設
    `tests/test_no_direct_ticker_access.py`（AST解析ベースのCI
    チェック）が`cik_lookup.csv`の直接パースまたは共有`tickers.py`
    アクセサを迂回するルートデータディレクトリ`os.listdir()`を検知
    ——許可リスト方式。`phase1_scan.py`等の独立直接アクセススクリプトを
    発見（修正はしていない）、複数が個別の優先度低BACKLOG項目に分離
    （現在#98へ統合）。
100. **[[TTM-SBC-QUARTERS-GAP-1]]** — OPEN。`build_rice_annual_
     shape()`がOCF/CapEx/Revenue/NetIncomeの四半期完全性チェックを
     適用するがSBCには**適用していない**——意図的か不明。未対応。
101. **[[BS-FIELD-FADEOUT-NONZERO-LAST-VALUE-1]]** — OPEN。CSGP/KULR/
     RCATのBSフィールド「フェードアウト」（データが存在し、その後
     消える）は、他22銘柄向けに実装された「最後の既知値が明示的$0
     だった」フォールバックパターンに合致しない（最後の既知値が
     非ゼロ、CSGPは$10M超）——誤ったゼロ仮定を避けるため意図的に
     除外。フォールバックロジックは未設計。未対応（優先度低〜中）。
102. **[[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]** — OPEN。STONKS
     SILOのYoY計算（`financial_trend_calculator.py::_calc_yoy_
     change()`）がfpラベル（Q1-Q4）のみで一致判定し期間長の健全性
     チェックが無い——RCATのFYE移行期間「Q4」（8ヶ月差、12ヶ月では
     ない）で歪んだYoYシグナルを生成した。`fetcher.py`側の修正
     （#62）とは独立（別の`quarterly_normalized.json`パイプラインを
     読むため）。現在はデータ蓄積により自己解消済み、一般的な修正
     としては未対応。
103. **[[TEST-STALE-IV-1]]** — OPEN（テスト保守、sec_data本体では
     ないが上記全項目で「既知の失敗」として繰り返し言及されるため
     記載）。`test_iv_formula.py`が旧ALPHA-REDESIGN-1以前の
     `v0*(1+alpha)`式のまま——抽出ロジックとは無関係、上記検証
     セクション全てで参照される定数的な背景ノイズ（既知失敗2件）の
     ベースラインとしてのみ言及。

---

## 2. 企業属性ベースの類型化（横断インデックス）

同一の企業属性・状況が繰り返しバグを引き起こしている軸で、上記103項目を
再分類する。1項目が複数の属性に該当する場合は両方に記載する。

### ① SPAC上場銘柄（合併前後の法的実体混在）
**該当**: #23(SPAC-SHELL-BS-ENTITY-MIXING-1, BBAI/RDW/RKLB/SOFI/VRT/
SPIR/ONDS)・#59(RCAT)・#66(SPAC-STUB-PERIOD-VERIFICATION-1, ASTS/IONQ/
JOBY/RKLB/SOFI/SPIR/SOUN/APGE/NOW/RCAT/VRT)・#67(SPAC-STUB-PERIOD-
FIELD-SPLIT-1, BBAI/RDW)・#78(ASTS-SHARES-OSCILLATION-1, ASTSのSPAC
合併前期間)・#95(KULR、SPAC疑いだったが非該当と確定)

**件数**: 直接該当7件（うち1件は「非該当と確定」）。**現在の対応状況**:
`_resolve_bs_entity_mixing()`（条件付き単一accn強制）・`formerNames`
区間検知・期間長フィルタの組み合わせで大半は対応済み。ただし
[[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-MISSELECT-1]]（#24）の
VRT(2017)はSPAC上場銘柄特有の「合併前シェル会社の10-Kに正しい値が
存在するのに全く無関係な後年filingの値が採用される」という、
既存のSPAC対応策の対象外にある変種であり、SPAC銘柄は**複数の異なる
サブパターンで繰り返しバグを生む**ことを示している。

### ② 決算期変更経験銘柄（単一/複数回、52/53週企業含む）
**該当**: #9(CHECK-QREV-FYE-1/DILUTION-FYE-1, KLAC/LRCX/DELL/ESTC)・
#56(FY52WEEK-BUCKET-MISPLACE-1, AVGO/DELL/CAKE/ELF)・
#58(ELF-FISCAL-END-MONTH-MISDETECTION-1, ELF)・
#59(RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1, RCAT)・
#60(FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1, RCAT/LITE/WST)・
#61(FYE-BOUNDARY-COLLISION-UNCONFIRMED-1, LITE/WST)・
#62(FETCHER-10KT-10QT-FORM-EXCLUSION-1, RCAT)

**件数**: 7件、うちRCATが4件に登場（同一銘柄が決算期変更に起因する
複数の異なるバグの震源地になっている）。**現在の対応状況**: 検知
（アンカークラスター・CHECK-24/WARN-24・CHECK-28/WARN-28）は充実
してきたが、是正（特に10-KT/10-QT移行期間データの本来の取り込み）は
「1銘柄規模には過大な再設計」として意図的に見送られており、検知止まり
の項目が多い。

### ③ M&A・組織再編直後の銘柄
**該当**: #23(SPAC-SHELL-BS-ENTITY-MIXING-1)・#54(COHR、II-VI合併)・
#64・65(MRVL-2019-2020-NULL-1/CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1、
GOOGL/AVGO/DELL/MRVL持株会社再編、CEG/LITE/ABBV/GEV/SN/CONスピン
オフ、VST破産fresh-start)・#71(GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1、
Motorola Mobile非継続事業)・#79(EPS-UPC-PREREORG-1、BROS Up-C再編)

**件数**: 5件（うち1件は3類型に細分）。**現在の対応状況**:
「同一事業の再編ならCIKを統合」「スピンオフ/破産fresh-startは意図的に
非連結のまま」という明確な設計方針が確立済み。ただしCRM(2011)の
「本人データ選定が2つの異なる年度で競合する」パターンはM&A固有では
なく、通常の事業継続企業でも起こりうる（テーマ4「フィールド独立抽出」
の一般形）。

### ④ 非継続事業区分を持つ銘柄
**該当**: #47・48(RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1/OPERATING-
CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1, RCAT/HON/AVAV+AAPL/MSFT/
TSLA/XOM/CAT/ABBV等25銘柄)・#71(GOOGL、Motorola Mobile)

**件数**: 2件（うち1件が25銘柄規模）。**現在の対応状況**:
「広義の非継続タグと営業活動限定タグが偶然一致する」設計トラップが
発見され、単純な候補タグ追加は危険と判明。25銘柄中実際に現行の分析
窓に影響するのはRCATのみで残り24銘柄は窓外（2011-2017年）——低
緊急度で未対応。

### ⑤ IPO前・上場直後の銘柄（複雑な資本構成、比較列の信頼性）
**該当**: #66・67(SPAC-STUB-PERIOD-VERIFICATION-1/FIELD-SPLIT-1)・
#78(ASTS-SHARES-OSCILLATION-1)・#79(EPS-UPC-PREREORG-1)・
CHECK29未解決7件のうちPLTR(2019、上場前資本構成)・CART(2023、IPO
直後)・[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（#53）

**件数**: 6件相当。**現在の対応状況**: 期間長フィルタ・SPAC検知で
大半は対応済みだが、PLTR(2019)のような「上場前の複雑な優先株式/
ワラント構成」は現行の抽出ロジックでは原因を特定できないまま残って
いる（原因不明類型⑧と重複）。

### ⑥ 業界特有の会計慣行を持つ銘柄
**該当**: #26(GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1、
MO/PMタバコ物品税会計・SCCO鉱業D&A別建て)・#19(REVENUE-TAG-CONFLICT-
SCAN-1、PM物品税込み/抜き収益)

**件数**: 2件（3銘柄）。**現在の対応状況**: いずれも一次情報で
genuineと確定済み、対応不要（現状の抽出結果が正しい）。**この類型は
「バグではなく仕様」と確定できる数少ないクリーンな類型**であり、
再設計時にはこれらを「除外リスト」として明示的に保持すべき。

### ⑦ 標準タグ体系から外れる銘柄（独自命名、タグ不使用、発行体側エラー）
**該当**: #11(LLY-CAPEX-STALE-1)・#15(LAYER3-COGS-STRUCTURAL-GAP-
16TICKERS-1、CAKE)・#16(LAYER3-COGS-ASTS-LRCX-RECOVERABLE-FOLLOWUP-1、
LRCX企業固有namespace)・#20(SEC-BKNG-SHARES-ANOMALY-1)・
#70(COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1、発行体自身のfiling誤り)・
#73(FY52WEEK-BS-STI-OVERRIDE-DESIGN-1)・#74(NVDA-STI-TAG-
UNIDENTIFIED-1)

**件数**: 7件。**現在の対応状況**: 銘柄ごとの`ticker_overrides`/
`cross_filing_tags`/`fact_overrides.json`という3種類の個別オーバー
ライド機構で対応してきたが、根本的な解決ではなく「例外リストの
継続的な追加」に依存している。**この類型が最も個別対応コストの
高い類型**であり、再設計時の一次データ層設計の中心的な検討対象と
なるべき。

### ⑧ 原因不明・現行ロジックでは説明できない銘柄
**該当**: #4(BS-FIELD-NEWLY-MISSING-2026-1)・#22(SEC-XBRL-MISSING-
START-ENTRY-1)・#27(HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1)・
[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]の残り7件（PLTR2019/
CART2023-25/V2008/CELH2025/ASTS2019）（#61は2026-09-05の一次情報確認で
原因確定・DONEのため本カテゴリから除外）

**件数**: 3件＋CHECK29残り7件 = 実質10件規模。**現在の対応状況**:
「原因不明」であること自体が1つのパターンとして記録されている
（HON-GROSSPROFIT-2009等）。共通する特徴: いずれも詳細な一次情報
照合・網羅的タグ列挙を行っても標準的な会計恒等式では説明しきれない
——今の抽出ロジックの延長では解決不能で、個別企業の実際の財務諸表
構造（連結範囲・優先株式・少数株主持分の特殊な組み合わせ）を人手で
読み解く必要がある可能性が高い。**再設計後も一定数はこの類型に
残ると想定すべき**。

---

## 3. 根本原因ベースの類型化（設計原則との対応）

`EXTRACTION_DESIGN_PRINCIPLES.md`が策定した3原則との対応関係、および
本棚卸しで新たに見えた2つの追加パターン候補を示す。

### 原則1: 候補選定は「新しさ」だけでなく期間・時系列の妥当性を検証すること
**該当項目**: #6,7,8,9（テーマA、期間長・期間分類）・#44（TTM連続性）・
#30（四半期上限未検証）

この原則に該当するバグは比較的「発見しやすく・直しやすい」（期間長の
数値検証を1箇所追加するだけで機械的に検出できる）傾向があり、実際に
#7・#44は大規模な既知バグ（それぞれ複数銘柄）を一度に解消している。
一方#30（上限未検証）は#7・#44が「下限」側を修正した後も「上限」側の
非対称な見落としとして残存しており、**片側だけの修正では再発する**
という教訓を示す。

### 原則2: 相互に関連するフィールドは独立に抽出・選定しないこと
**該当項目**: テーマB全体（#23-50）・テーマB2（#51-55）・#24・#25

**本棚卸しで最大のクラスター**（件数にして28件以上）。SPAC実体混在
（#23）・PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-MISSELECT-1（#24）・
PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1（#25）は、いずれも「BS/PLの
複数フィールドが同一年度・同一accnに紐付くべきという制約を検証せず、
フィールドごとに完全独立して候補（タグ・accn・期間）を選ぶ」という
**全く同じ設計判断**に起因する、別々の実装箇所（`_collect_own_data_
instant()`・`_extract_values_best_candidate()`・
`_merge_normalized_by_priority()`）での再現である。**この原則こそが
再設計で最優先に組み込むべき制約**であることを、本棚卸しの規模が
裏付けている。

### 原則3: ドメイン固有の既知の制約・恒等式を、抽出後・保存前に検証する仕組みを最初から組み込むこと
**該当項目**: テーマB2全体（#51-55）・#1（total-liabilitiesフォール
バック、恒等式隣接ケース）

CHECK29（会計恒等式検証層）は事後的に追加された横断検証レイヤーだが、
`ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1`（#51）自体が
「この種の検証が最初から存在しないこと」を発見の対象としている点が
示唆的——**原則3は新規データ層では「後付け」ではなく最初から設計に
組み込む必要がある**という`EXTRACTION_DESIGN_PRINCIPLES.md`自体の
主張を、common/sec_dataでの実例が裏付けている。

### 追加候補原則4: サイレントな発行体側タグ移行の検知
**該当項目**: #2(JNJ)・#11(LLY CapEx)・#16(LRCX cost_of_revenue)・
#15(CAKE COGS)・#73(NVDA STI等)

企業が同一会計期間中またはある年度を境にXBRLタグを無言で切り替える
（旧タグの報告を停止し新タグへ移行する）ケースが5件以上見つかった。
「最初に見つかった／最優先候補にデータがあるものを採用する」という
選定ロジックは、この切替を検知できず**古いタグの陳腐化データに
サイレントに固着する**。現行の`EXTRACTION_DESIGN_PRINCIPLES.md`の
3原則には明示的に含まれておらず、新規データ層の設計原則として
追加する価値がある候補（例: 「同一概念の候補タグ群について、直近
N期間のうちどのタグが実際にデータを持つかを定期的に再評価し、
主要候補が沈黙した場合は警告する」）。

### 追加候補原則5: 独立した並行パイプラインの分岐を防ぐ
**該当項目**: #43(TTM-DATA-DRIFT-BEHIND-PIPELINE-1)・
#91(SECDATA-STORAGE-FRAGMENTATION-1)・#88(segment_fetcher重複)・
#40(STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1)・
#42(Q4-IMPLIED-CALC-TRIPLICATION-1)

`parser.py`→annual・`quarterly.py`/`normalizer.py`→normalized・
`layer3_builder.py`/`ttm_calculator.py`→ttmという3系統の構造的に
独立した再実装が並存し、一方の修正が他方に伝播しない。これは
「新規データ層の抽出ロジック設計」というより「一次データ層全体の
アーキテクチャ設計」の問題であり、**今回のcommon/sec_data全面
再設計の直接の動機**そのものでもある。新設計では「単一の抽出・
正規化パイプラインが全消費者に供給する」という構造自体を保証する
ことが、個々の抽出ロジックの正しさ以上に重要になる。

---

## 4. まとめ

- 完了103件・複数の未解決項目を棚卸しした結果、**個々のバグは独立した
  偶然の出来事ではなく、少数（3〜5個）の設計判断の欠如が形を変えて
  何十回も再現したもの**であることが定量的に裏付けられた。特に
  「原則2（フィールド独立抽出）」に該当する件数が突出して多い。
- 企業属性別に見ると、SPAC上場・決算期変更・標準タグ外れの3類型は
  「都度の個別オーバーライド機構」で対応してきたが、これは対応コストが
  高く蓄積し続ける構造であり、新設計ではこれらを事前に想定した
  一般的な仕組み（例: 単一accn解決、cross-filing-tags機構の一般化）
  として組み込むことが望ましい。
- 業界特有の会計慣行（MO/PM/SCCO）のようにgenuineと確定できた少数の
  類型は、新設計でも「例外として保持すべきホワイトリスト」として
  引き継げる。
- 原因不明のまま残る類型（HON-GROSSPROFIT-2009、CHECK29残り7件等）は、
  再設計後も一定数残ると想定し、「原因不明であること自体を記録して
  クローズしない」という既存のCHAT_RULES.md運用方針（バグが0に
  ならなければ次に進まない）を新設計後も維持すべきである。

本ドキュメントは類型化・記録に留める。次フェーズ（実際の再設計方針
策定）への着手はユーザーの指示を待つ。

## 参照

- `EXTRACTION_DESIGN_PRINCIPLES.md`: 新規データ層向け設計原則（3原則）
- `SEC_EDGAR_LAYER_DESIGN.md`: Layer2/Layer3統合スキーマの設計判断
- `BACKLOG_DONE.md`/`BACKLOG.md`: 個別項目の一次情報源（本ドキュメント
  内の全`[[ID]]`はこれらのファイル内で検索可能）
