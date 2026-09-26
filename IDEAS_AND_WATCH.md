# IDEAS AND WATCH（構想・監視メモ・消費者のいない整理項目）

**ここは課題数に数えない。着手する時点で BACKLOG.md に戻す。**

BACKLOG.md には、実害がある、または実害の確認が必要な不具合だけを登録する
（CHAT_RULES.md「BACKLOG登録基準」参照）。構想・監視メモ・消費者のいない
整理項目はこのファイルへ登録する。本文は BACKLOG.md から移した時点のまま
（2026-09-26移動分は書き換えなし）。

---

### [SEGMENT-XBRL-GROWTH-EXPANSION-CANDIDATES-1] segment_xbrl（[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1]]案①）の対象銘柄拡張候補3件
**優先度:** 低（今すぐ着手する項目ではない、Koichiさんが着手タイミングを判断する）
**分類:** 新機能拡張候補 / TANUKI VALUATION
**登録日:** 2026-09-18
**発見:** [[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1]]案①実装（7銘柄:
APP/CRWV/NVDA/PLTR/SOFI/SOUN/TSLA）完了時に、対応不可・対応保留と
判明した銘柄群の記録

#### 背景
`calculator/segment_growth_xbrl.py`は、TANUKI TAILのXBRLセグメント
売上データ（`docs/portfolio/tail/data/kpi/{ticker}_layer2.json`）
またはSEC EDGAR Layer3の全社売上から、セグメント別成長率を決定論的に
算出しDCF Phase1のGとして採用する。現状7銘柄のみ対応済みで、以下3種の
拡張候補が残っている。

#### ① ADBE・CELH（分類軸不一致、対応不可と判明）
`config/segment_config.json`のセグメント区分とXBRL/MD&A側の実際の
開示区分が一致しない:
- ADBE: config側は会計セグメント（Digital Media / Digital Experience）
  だが、MD&A記述は顧客区分（Creative & Marketing Professionals /
  Business Professionals & Consumers）
- CELH: config側は地域（North America / International）だが、
  XBRLはブランド（Celsius / Alani Nu / Rockstar）

名寄せによる対応が原理的に成立しないため、既存のsegment_config.json
静的値のまま維持する方針とした（今回スコープ外、対応不可）。将来
対応する場合は、config側のセグメント定義自体を実際の開示区分に
合わせて再設計する必要がある（TSLAの3→2セグメント修正と同種の
作業だが、ADBE/CELHは軸そのものが違うため単純な統合では済まない）。

#### ② 第2陣候補: Layer 1（手動segment_config.json設定済み）だが
`tail_kpi_map.json`にXBRLセグメント取得設定が未整備の銘柄群（29銘柄）
`config/segment_config.json`でLayer 1（複数セグメント設定済み、
Generalプレースホルダでない）の38銘柄のうち、今回対応した7銘柄
（+対応不可のADBE/CELH）を除く29銘柄が該当。これらも同様に
`config/tail_kpi_map.json`へXBRLセグメントメンバータグを追加すれば
同じ仕組みで対応できる可能性が高い。

調査フェーズで残した未検証の論点（着手前に確認する価値がある）:
`us-gaap:StatementBusinessSegmentsAxis`は標準ディメンションのため、
`company_facts`からセグメントメンバーを自動列挙できる可能性がある。
これが可能なら、29銘柄個別に手動でtail_kpi_map.jsonへ追記する
コストを大幅に下げられる（現状は10銘柄分のみ手動キュレーション）。

#### ③ 第3陣候補: Layer 2（Generalプレースホルダ）の27銘柄
`segment_config.json`で`{"General": {...}}`単一セグメントの
プレースホルダ状態にある27銘柄。これらを実セグメントへ昇格させる
には、まず該当銘柄が複数の実報告セグメントを持つか自体の確認から
必要（①②より作業規模が大きい）。

#### 着手条件
なし（優先順位含め次回以降のセッションでKoichiさんが判断する。
②の「company_facts自動列挙可否」の調査だけでも、着手前に価値の
検証として独立して行う余地がある）

---

### [FUTURE-FEATURE-IDEAS-CATALOG-1] 将来構想8件の統合カタログ（元UX-FLOW-1/MULTI-1/ARCH-1/EVAL-2/DESIGN-8-3/DESIGN-8-4/SPAC-SHELL-MAINTAINED-FIELDS-FREEZE-CONSIDERATION-1/TANUKI-ROE-2）
**優先度:** 低（いずれも構想段階・実装未着手のアイデアメモ）
**分類:** 将来構想 / 複数画面・複数サブシステム横断
**登録日:** 各サブ項目の元登録日は各①〜⑧の記載を参照。統合日: 2026-09-05（①〜⑥）・2026-09-16（⑦⑧追加吸収）
**発見:** 2026-09-05のBACKLOG横断整理（⑦⑧は2026-09-16の件数削減棚卸しで追加吸収）

#### 統合の経緯
UX-FLOW-1・MULTI-1・ARCH-1・EVAL-2・DESIGN-8-3・DESIGN-8-4の6件は、いずれも
将来的な機能拡張・設計改善のアイデアメモであり、現時点で個別に着手予定は
ないため、2026-09-05に1つのカタログエントリへ統合した。元の6件は
BACKLOG.mdから削除し、内容は要約せず全文そのまま以下の①〜⑥に保持する。
DESIGN-8-3・DESIGN-8-4については、統合時点で判明している注意点を各項目
末尾に「注記（2026-09-05追記）」として追記した（元の構想自体は変更・
削除していない）。

2026-09-16の件数削減棚卸しで、同種の将来検討事項2件
（SPAC-SHELL-MAINTAINED-FIELDS-FREEZE-CONSIDERATION-1・TANUKI-ROE-2）を
⑦⑧として同様に全文吸収した。元の2件もBACKLOG.mdから削除し、内容は要約せず
全文そのまま保持する。

#### ① 元[UX-FLOW-1] On a Journey標準利用フローの設計
**優先度:** 低（思想設計タスク、実装ではなく方針検討から開始）
**分類:** 設計課題 / 全画面横断

##### 内容
画面間を行き来する非線形な利用が前提だが、緩やかな標準利用フロー
（例: stock.htmlで個別検証→TANUKI SCOREで横断相対判断、等）を
今後設計したい。

##### 格上げ検討理由（2026-07-01）
EXTREME-FEAR-1対応時、買い候補TOP10機能（TANUKI score×乖離率×funda×phaseベースの
銘柄選定）のナビ登録先を検討した際、本来TANUKI SCOREの役割に近い機能をMarket Pulse
配下に置く形で暫定決着した。これは各画面の役割定義はあるものの、画面間の回遊動線・
機能配置の指針が未設計であることに起因する。今後複数システムの性質を跨ぐ機能が増える
たびに同種の判断コストが発生するため、次セッション以降の設計着手候補として優先的に
検討する。

#### ② 元[MULTI-1] マルチバリュエーション表示
- 現状: DCF一本槍
- 改善: DCF / PEG / EV/Sales / RICE / HypeCoreを並列スコアカード表示
- GPT提案: 2026-05-30
- 関連（2026-07-10追記、2026-09-05更新）: [[SCREENING-SIGNAL-
  INTEGRATION-EPIC-1]]（旧RICE-INTEGRATE-1、2026-09-05に同エピックへ
  統合済み）とRICE指標の活用目的が部分重複。本タスク（MULTI-1）は
  画面表示（並列スコアカード）が主眼、旧RICE-INTEGRATE-1はスクリーニング
  判定への組み込みが主眼という役割分担。どちらかの着手時に統合要否を
  判断する。

#### ③ 元[ARCH-1] ボトルネック企業プレミアム
- 現状: 未実装
- 内容: NVDA・ASML等の独占的ポジションを持つ企業への追加プレミアム
- 設計: 手動フラグ（bottleneck: true）+ Moat Scoreへの上乗せ or Phase1延長の形
- 注記: ALPHA-REDESIGN-1（2026-06-25）でalphaが廃止されたため、
  α加算方式は使用不可。設計を再検討する必要あり。
- 記録日: 2026-04-12

#### ④ 元[EVAL-2] 期待値エンジン（仮称）
- 現状: 構想中
- 内容: 各サブポート戦略の期待値を統合管理するエンジン

#### ⑤ 元[DESIGN-8-3] 8-3 ワンクリック銘柄登録〜更新
- 概要: Discover画面から「➕ 登録」ボタンで
  CIK取得→β/セグメント/Damodaran業種AI提案→承認→一括更新
  を一気通貫で実行
- 実装難易度: 高
- **注記（2026-09-05追記）**: 「Discover画面」は2026-09-01に
  [[DISCOVER-SUBSYSTEM-REMOVAL-1]]で削除済みのため、実装時はUI設置
  場所の再設計が必要。

#### ⑥ 元[DESIGN-8-4] 8-4 指数採用候補銘柄の発掘（設計見直し済み・実装保留）
- 概要: S&P MidCap 400 → S&P 500 昇格候補を定期サーチ
  GS・バンカメ等が発表する昇格候補レポートをGrok Web検索で収集
  機械的条件判定（yfinance）ではなくアナリストレポートベースの設計
- 実装方針: Grokのweb検索で「S&P 500 addition candidates」を定期検索
  週次でDiscover候補セクションに表示
- 実装難易度: 中
- 状態: 実装保留（着手時期未定）
- **注記（2026-09-05追記）**: 実装方針の「Grok Web検索」は、
  risk_fetcher/Discover撤去（2026-09-01〜02）で確立した「根拠不明の
  生成をそのまま採用しない」方針と抵触するため、実装時は代替手法
  （yfinance機械的判定等）を検討すること。

#### ⑦ 元[SPAC-SHELL-MAINTAINED-FIELDS-FREEZE-CONSIDERATION-1] BBAI/RKLB/SOFI/VRT/ONDSグループの「維持フィールド」の凍結検討
**優先度:** 低
**分類:** データ品質 / 将来検討事項
**登録日:** 2026-08-05
**発見:** [[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]] Stage 3準備調査（チャット記録）

##### 内容
[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1でBS項目をNone化・修正した
BBAI(2020)・RDW(2020)・RKLB(2020)・SOFI(2020)・VRT(2019)・ONDS(2017)の
6件は、None化されたフィールド自体（current_assets/current_liabilities/
long_term_debt/short_term_debt等）に「凍結すべき正しい値」が存在しない
ため、現行のfixed_registry.jsonスキーマでは登録不可と確定済み
（Stage 3調査、BACKLOG_DONE.md「2026-08-05（完了）」Stage 2エントリ
参照）。

一方、各銘柄でNone化されず**維持**されたフィールド（例: BBAIの
total_assets/stockholders_equity/total_liabilities/cash_and_equivalents）
は、`_resolve_bs_entity_mixing()`の数学的整合性チェック
（current_assets<=total_assets等）を通過済みであり、「誤った値をNone化
した」修正の裏返しとして「正しいと確認済みの値」というカテゴリに
位置づけられる可能性がある。

##### 影響
未確定。仮に凍結対象とする場合、Stage 1/2とは異なる「除外的検証
（誤りが混入していないことの消去法的確認）」という性質を持つため、
Stage 1/2の「積極的な値の検証」基準にそのまま当てはめてよいか設計判断が
必要。

##### 対応方針
未定。次回以降、余力があれば検討する将来課題。

#### ⑧ 元[TANUKI-ROE-2] デュポン分解 業種平均比較・潜在ROE試算
**優先度:** 低
**状態:** 部分完了（2026-06-26）
- ✅ stock.htmlにDUPONT ANALYSISパネルを追加（4カード：純利益率・資産回転率・財務レバレッジ・ROE）
- [ ] 業種平均との比較表示（Damodaranにデータなし・データソース確保が必要）
- [ ] 潜在ROE試算（業種平均データ確保後に実装）
- **着手条件（2026-09-16追記、統合時に明記）**: 残り2項目（業種平均比較・
  潜在ROE試算）の着手条件はいずれもDamodaran業種平均データソースの確保。

#### 着手条件
なし（いずれも構想段階、個別項目ごとに着手可否を判断する）

---

### [DATA-JUMP-CHECK-NETINCOME-SBC-1] 純利益・SBCの段差型急変検知（比率方式以外の代替方式検討）
**優先度:** 低（着手急がず）
**分類:** アーキテクチャ / 品質管理
**登録日:** 2026-09-06
**発見:** [[DATA-JUMP-CHECK-GENERALIZE-1]]実装時の実データ比率分布確認

#### 背景
[[DATA-JUMP-CHECK-GENERALIZE-1]]で`check_c_data_jump()`（YoY比率が閾値以上/
以下で発火する段差型検知）を売上総利益・CapExへ展開する際、当初は純利益・
SBCも対象候補としていたが、実データで比率分布を確認した結果、比率方式が
本質的に機能しないことが判明したため、この2フィールドは今回のスコープから
除外した：

- **純利益（pl.net_income）**: tanuki=true全100銘柄・直近6年のYoY比率477件中
  53件が負値（黒字↔赤字の符号反転）。符号反転を跨ぐ比率は数学的に意味を
  持たない（例: LITE 2025→2026: $25.9M→$-69.35億、比率-267.76倍という値
  自体が「267倍悪化」を意味しない）。閾値方式で符号反転を捕捉しようとすると
  「負の比率は全て閾値以下」という粗い判定にしかならず、実質的に「符号が
  変わったかどうか」の二値判定と変わらない
- **SBC（cf.stock_based_compensation）**: ゼロ近傍の小額から上場後の本格的な
  株式報酬制度導入で急増するケースが頻発し、実測でZETA（2020→2021、
  $105K→$259.16M、倍率2468.18倍）のような正当な急増が比率の上限を
  無意味化する。SBCはスタートアップ〜上場直後の企業で「ほぼゼロから
  始まり数年で定常化する」という成長曲線自体がありふれているため、
  段差型検知が想定する「タグ切替による不連続 vs 正当な急変」の区別が
  比率方式では原理的に困難

#### 対応方針（未確定・次回セッション以降で判断）
比率方式（YoY倍率）以外のアプローチを検討する必要がある。候補（いずれも
未検証、次回セッションで実データを見ながら判断）：
- 純利益: 符号反転自体を検知する二値チェック（「前年黒字→当年赤字」等の
  遷移を、[[BS-FIELD-NONE-TRANSITION-DETECT-1]]（WARN-26、有値→None遷移
  検知）と同型の「状態遷移検知」として設計する案
- SBC: 絶対額ベースの閾値（例: 直近年のSBCが売上の一定比率を超えて
  急増した場合のみ検知）、またはゼロ近傍を除外した上での比率方式再検討
- いずれも「NGにするには誤検知率が高すぎる」というWARN-21/44/45と同じ
  教訓が当てはまる可能性が高く、実装する場合もWARNレベルに留める前提で
  設計すること

#### 着手条件
なし（優先度含め次回以降のセッションで判断。急ぎではない）

---

### [LAYER3-GA-STANDALONE-TAG-UNMAPPED-1] GeneralAndAdministrativeExpense（Selling抜きG&A単体タグ）がLayer2のどのフィールドにもマッピングされていない
**優先度:** 低〜中（2026-07-30投資調査により中→低〜中に修正。理由は下記対応方針参照）
**分類:** データ品質 / タグ網羅性
**登録日:** 2026-07-24
**発見:** SM/SGA分離258件全数検証

#### 内容
GeneralAndAdministrativeExpense（Selling抜きのG&A単体タグ）が、
Layer2の32フィールドのいずれにもマッピングされていない。

【2026-07-30投資調査で規模を再確認】当初「少なくとも6銘柄」としていた
規模認識は過小評価だった。全105銘柄スキャンの結果、
GeneralAndAdministrativeExpenseタグを報告している銘柄は56銘柄に及ぶ。
ただし影響度で3分類できる:
- **実害なし（4銘柄: AAPL/AMAT/CELH/TER）**: SGA総額
  （selling_general_and_administrative）が別途取得済みのため対応不要
- **部分的ギャップ（47銘柄）**: selling_and_marketingは機能するが、
  selling_general_and_administrativeのみ空
- **完全なギャップ（5〜6銘柄: APGE/ASTS/CON/ENB/RXRX）**: SM・SGA両方が
  完全に空。当初報告の6銘柄のうちCAKE/CPRTは、実際にはAdvertisingExpense
  （SM候補タグ）を報告しており部分的ギャップ側に該当することが判明
  （ただし四半期粒度では取り込まれないため実質SM空という当初の観測自体は
  誤りではない）。ENBは今回の調査で新たに完全なギャップ銘柄として発見

#### 影響
5〜6銘柄（APGE/ASTS/CON/ENB/RXRX、CAKE/CPRTは部分的ギャップ）でSM・SGA
両フィールドが完全に空になる。GeneralAndAdministrativeExpenseタグ自体の
存在という意味では56銘柄規模。

#### 対応方針
- **選択肢B（既存selling_general_and_administrativeへのフォールバック
  候補化）は非推奨**: [[SCHEMA-NORMALIZED-ISSUES-1]]（旧SCHEMA-SM-SGA-
  CONFLATION-1）と同型の概念混在リスクを再導入する。G&A単体とSGA総額は金額の性質が異なり（SGA総額は
  Selling費用を含むため同規模の企業でもG&A単体より必然的に大きい）、
  同一フィールドに混在させると時系列比較・銘柄間比較の両方で不整合が
  生じる
- **選択肢A（新規フィールド化、例: general_and_administrative_expense）
  を推奨するが、優先度は低〜中に留める**: selling_general_and_
  administrative自体が現状TANUKI VALUATION計算に一切消費されておらず
  （report_consistency_check.pyのSGA整合性チェック用途のみ）、新規
  フィールド追加の実利は当面限定的なため
- 技術的には、GeneralAndAdministrativeExpenseはFLOW系（duration型）
  のため、Q4_IMPLIED_FIELDS・MISSING_QUARTER_IMPLIED_FIELDS・
  newfield_q4_cutoff_check.pyの対象に加えることは可能
  （selling_general_and_administrativeと同型の扱いができる）

#### 着手条件
SGA（selling_general_and_administrative）・SM（selling_and_marketing）
のいずれかが新機能（投資強度分析の精緻化等）で実消費される計画が
立った時点、またはreport_consistency_check.pyのSGA整合性チェックを
強化するタイミングで、選択肢A（新規フィールド化）を再検討する。

#### 優先度変更（2026-09-19、低〜中→低）
本件は[[SCHEMA-NORMALIZED-ISSUES-1]]②で解消される項目に統合される
見込みのため、個別に「低〜中」の幅を維持する根拠が薄い。優先度を
「低〜中」から「低」へ変更する。

#### 再確認（2026-09-24、クローズ見送り）
2026-09-19の優先度変更では「[[SCHEMA-NORMALIZED-ISSUES-1]]②で解消される
項目に統合される見込み」としていたが、同②の完了記録（BACKLOG_DONE.md、
2026-09-23クローズ）を確認したところ、扱っているのは**normalized/の`SM`
フィールドがSGA総額へフォールバックする概念混同**のみで、
`GeneralAndAdministrativeExpense`（G&A単体タグ）の未マッピングには
一切言及していない（②は罠防止コメントのみで対応、タグマッピングの変更なし）。
統合による解消という前提が不一致のためクローズを見送った。
参考: `selling_general_and_administrative`の参照は現在も抽出・生成系
6ファイル（`layer3_builder.py`・`newfield_q4_cutoff_check.py`・
`parser.py`・`q4_implied.py`・`quarterly.py`・`ttm_calculator.py`）のみで、
`src/`配下の消費者は0件（本文「着手条件」の前提は変わらず）。

---
