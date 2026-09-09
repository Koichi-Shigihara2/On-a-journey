# SEC_EDGAR_LAYER_DESIGN.md — SEC EDGARレイヤー統合設計

作成日: 2026-07-24
位置づけ: 新一次データベース構築プロジェクト フェーズ1、
common/sec_data統合（INPUT-A-001〜018・048）の設計判断・根拠を記録する。
進捗ステータスは`PROJECT_STATUS.md`、実装後の実態は`SYSTEM_MAP.md`を
参照。本ドキュメントは「なぜこの設計にしたか」の記録に特化する。

---

## 1. 背景

現状9系統（後に10系統と判明、下記2-4参照）に分岐した読み取り経路
（`data/{annual,quarterly}_*.json`・`raw/`・`normalized/`・`ttm/`・
`company_facts.json`・EPS Analyzer独自3ファイル）を、統合スキーマへ
一本化する設計を、複数回の投資調査（読み取り専用）を通じて確定した。

---

## 2. 現状分析で判明した事実

### 2-1. quarterly系統の構造的欠陥

`data/quarterly_{FYQ}.json`のQ2以降の値は会計年度累計（YTD）値の
ままであり、真の単四半期値ではない。`normalized/`は真の単四半期値に
変換済みだが、タグ網羅性が乏しく（例: STDebtは単一タグのみ、
AAPL/XOM/V等主要銘柄で0件）、フィールド網羅性でも
`free_cash_flow`・`short_term_investments`等の最高影響度フィールドを
欠いている（105銘柄全数で確認済み、[[SCHEMA-STDEBT-COVERAGE-GAP-1]]・
[[SCHEMA-SM-SGA-CONFLATION-1]]としてBACKLOG登録済み）。

### 2-2. annual系統

`annual_{FY}.json`と`quarterly_{FYQ}.json`はフィールド集合が完全一致
（105銘柄全数で確認）。BS系10フィールド（Cash/STDebt/LTDebt/
DeferredRevenue/Equity/Assets/SharesBasic/RPO/CurrentAssets/
CurrentLiabilities）は、四半期末＝年度末となる期のQ4エントリが
年度末値を兼ねる構造的帰結であり、annual専用の別ストアは不要と判断。

### 2-3. segmentKPI（INPUT-A-016）

当初`INPUT_DATA_TOBE.md`は正式ASC280セグメントを想定していたが、
実態（`xbrl_segment_fetcher.py`）は銘柄固有カスタムKPI（
`tail_kpi_map.json`定義）であり、取得方式（生XBRL XML直接解析）も
他フィールドと異なる。TOBE側の前提誤りと判明したため、
`INPUT_DATA_TOBE.md`側を訂正（2026-07-23実施済み）し、**フェーズ1の
統合スコープからは除外**。現状の独立実装を維持する。

### 2-4. company_facts.json（Layer1の実態）

`common/sec_data/data/{TICKER}/company_facts.json`が、SEC EDGAR
company_facts APIの完全な生レスポンス（フィルタなし、AAPL実測505
concept、105銘柄合計582.2MB）を無加工のまま週次保存していることが
判明した（過去の複数回の投資調査でも見落とされていた、
[[SECDATA-COMPANYFACTS-OVERLOOKED-1]]参照）。統合設計上、これが
系統数を「9→10」に訂正する根拠であり、かつ後述Layer1として
そのまま活用できる。

### 2-5. EPS Analyzer独立取得（52タグ）

EPS Analyzer（`extract_key_facts.py`）は同一のSEC EDGAR
companyfacts APIエンドポイントを独自に週次で叩いており、
common/sec_dataとは19タグが共通、52タグが独自（税務・一過性項目・
公正価値変動・銀行業向け、`INPUT-A-048`として登録済み）。
company_facts APIはconcept単位の部分取得に対応せず、同一銘柄・
同一タイミングであれば理論上バイト同一のレスポンスとなるため、
2-4のLayer1が既に52タグ全量を含んでいる。「独自取得層の統合」では
なく「Layer2設定への52概念追加」で対応可能という結論に至った
（詳細は4章）。

### 2-6. raw/の実態

`raw/{TICKER}_quarterly_raw.json`は「正規化前生XBRL」という名称に
反し、`FIELD_CONCEPTS`（26エントリ、うち1件は内部専用フィールド
`_COGS`、出力からは除外）で既にフィルタ済みの狭い
サブセット（AAPL実測26 concept、company_facts.jsonの505概念の
約5.2%）。診断専用（`audit.py`のみ参照）であり、Layer1の役割は
果たせないことが定量的に裏付けられた。

---

## 3. 決定した統合スキーマ（Layer3、32フィールド）

既存`normalized/`の25フィールドのうちSharesBasicを4-1章の設計に
従い2フィールドへ分離（[[SCHEMA-SHARESBASIC-CONCEPT-MISMATCH-1]]、
24＋2＝26フィールド）、これに`data/quarterly`側にのみ存在し
参照実績のある6フィールドを追加した合計32フィールド。

| # | フィールド | 追加理由 |
|---|---|---|
| 1〜24 | （既存normalized 24フィールド、SharesBasicを除く: OCF/ICF/CFF/CapEx/FinanceLeasePmts/SBC/DA/Revenue/GrossProfit/OperatingIncome/NetIncome/Cash/STDebt/LTDebt/DeferredRevenue/Equity/Assets/SharesDiluted/RD/SM/RPO/CurrentAssets/CurrentLiabilities/Buyback） | 既存 |
| 25 | shares_basic_weighted_avg | 期中加重平均株式数（PL項目）。[[SCHEMA-SHARESBASIC-CONCEPT-MISMATCH-1]]によりshares_outstanding_period_end_secと分離 |
| 26 | shares_outstanding_period_end_sec | 期末発行済株式数（BS項目）。yfinance由来の同一概念（shares_outstanding）との将来的な衝突を避けるため規則1（データソース接尾辞）を適用 |
| 27 | short_term_investments | Net Debt計算の中核入力、既存欠落 |
| 28 | total_liabilities | 診断・警告表示で参照実績あり |
| 29 | eps_basic | reader.py代替推計で参照実績あり |
| 30 | eps_diluted | reader.py代替推計で参照実績あり |
| 31 | cost_of_revenue | 内部取得済み（_COGS）、露出するのみ |
| 32 | selling_general_and_administrative | SM/SGA明示分離（[[SCHEMA-SM-SGA-CONFLATION-1]]解消） |

`free_cash_flow`は生フィールドとして維持（既存設計原則「符号正規化は
取得レイヤーで一度」に整合すると判断、導出データ層への移動は不要と
結論、詳細4章参照）。

**表記法についての注記**: 規則7（`NAMING_CONVENTIONS.md`）に基づき、
本一覧は将来のターゲットスキーマとしてsnake_case表記が正だが、
既存フィールド名との対応が分かるよう当面は両表記を併記する（1〜24行の
PascalCase表記は現行`normalized/`のフィールド名そのものであり、実際の
一括リネームは行わない）。

---

## 4. Layer1〜3アーキテクチャ

タグはSEC提出者（発行体）側が定義するものであり、内部システム側の
利用ニーズによって必要タグ数が可変であるという性質を踏まえ、
「取得（Acquisition）」と「抽出（Extraction）」を分離する3層構造を
採用する。

- **Layer1（無加工アーカイブ）**: `company_facts.json`（既存、
  新規構築不要）。SEC APIレスポンス全量を無加工のまま保持する唯一の
  層。
- **Layer2（概念定義）**: 現状Pythonコードにハードコードされている
  `FIELD_CONCEPTS`/`XBRL_MAPPING`を、設定ファイル（1エントリ＝1概念：
  内部フィールド名・タグ候補リスト・フォールバック順・カテゴリ・
  利用サブシステム）による管理へ移行する（詳細設計は別タスク）。
  新規フィールド追加（52タグ含む）はコード変更ではなく設定追加で
  対応可能にする。
- **Layer3（抽出済み正規化ストア）**: Layer1にLayer2を適用して
  生成される、3章の32フィールド統合ストア。`raw/`は本アーキテクチャ
  移行後は冗長化するため廃止候補（[[SECDATA-STORAGE-FRAGMENTATION-1]]
  で検討）。

---

## 4-1. Layer2スキーマ設計

概念定義（現状`FIELD_CONCEPTS`/`XBRL_MAPPING`としてPythonコードに
ハードコード）を、`config/sec_concept_definitions.json`（仮称）という
JSON設定ファイル形式へ移行する。設計方針は以下の通り。

- フィールド名はsnake_caseに統一（`NAMING_CONVENTIONS.md`規則7）
- `category`（flow/stock/shares/excluded）は`contracts.py::
  validate_field_classification()`の完全性契約をそのまま踏襲し、
  必須項目とする
- 意図的に除外した候補タグは`excluded_candidates`に理由とともに
  記録する（DA・SharesBasic等で発覚した「候補数が多い方を機械的に
  採用すると別の会計概念が混入する」問題の再発防止）
- 既存`TICKER_RESTRICTIONS`（銘柄ごとにキー体系が不統一だった）は
  `exclude`/`override_concept`/`note`の3キーに統一する

以下は代表フィールドの抜粋であり、全32フィールド分の完全なエントリは
実装タスク側で本スキーマ形式に沿って`FIELD_CONCEPTS`・`XBRL_MAPPING`
全量を機械変換する。

```json
{
  "_schema_version": "1.0",
  "_description": "SEC EDGAR XBRLタグ→内部フィールド定義（Layer2）。新規フィールド追加はコード変更ではなく本ファイルへの追記で行う。",
  "fields": {
    "capital_expenditure": {
      "category": "flow",
      "unit": "USD",
      "candidates": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
        "PaymentsForCapitalImprovements",
        "PaymentsToAcquireOtherPropertyPlantAndEquipment"
      ],
      "sign_normalize": "abs",
      "_note": "2026-07-23 CAPEX-SIGN-UNNORMALIZED-1対応に基づきsign_normalizeを明示"
    },
    "depreciation_and_amortization": {
      "category": "flow",
      "unit": "USD",
      "candidates": [
        "DepreciationAndAmortization",
        "DepreciationDepletionAndAmortization",
        "Depreciation",
        "AmortizationOfIntangibleAssets"
      ],
      "_note": "2026-07-24 SCHEMA-DA-FALLBACK-MISSING-1対応。primaryはDepreciationAndAmortization（Depletion除外、実際の成長率計算消費箇所の前提と一致）。DepreciationDepletionAndAmortizationは資源セクター銘柄（FCX/XOM/SCCO/CAT/HON等、Depletion込みタグのみ報告）向けの意図的なフォールバックとして2番目に維持する（完全除外するとこれらの銘柄でDA値が空になるため）。両タグは会計上厳密には異なる概念（Depletion込み/除外）である点に留意"
    },
    "shares_diluted": {
      "category": "shares",
      "unit": "shares",
      "candidates": [
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "CommonStockSharesOutstanding"
      ]
    },
    "shares_basic_weighted_avg": {
      "category": "shares",
      "unit": "shares",
      "candidates": [
        "WeightedAverageNumberOfSharesOutstandingBasic"
      ],
      "_note": "2026-07-24 期中加重平均株式数（PL項目）。SCHEMA-SHARESBASIC-CONCEPT-MISMATCH-1によりshares_outstanding_period_end_secと分離"
    },
    "shares_outstanding_period_end_sec": {
      "category": "shares",
      "unit": "shares",
      "candidates": [
        "CommonStockSharesOutstanding"
      ],
      "_note": "2026-07-24 期末発行済株式数（BS項目）。2026-07-24 NAMING_CONVENTIONS.md適用チェックリスト確認により、yfinance由来の同一概念（shares_outstanding、現状PER/PEG/PSR統一関数案の未実装構成要素として言及のみ）との将来的な衝突を避けるため、規則1（データソース接尾辞）を適用し_secを付与"
    },
    "long_term_debt": {
      "category": "stock",
      "unit": "USD",
      "candidates": [
        "LongTermDebtNoncurrent",
        "LongTermDebt",
        "LongTermNotesPayable",
        "SeniorNotes",
        "LongTermDebtAndCapitalLeaseObligations",
        "UnsecuredLongTermDebt",
        "ConvertibleLongTermNotesPayable",
        "ConvertibleDebtNoncurrent",
        "OtherLongTermDebt"
      ],
      "_note": "2026-07-24 SCHEMA-LTDEBT-DOUBLECOUNT-RISK-1対応。Noncurrentを先頭固定（二重計上防止）"
    },
    "investing_cash_flow": {
      "category": "flow",
      "unit": "USD",
      "candidates": [
        "NetCashProvidedByUsedInInvestingActivities"
      ],
      "_note": "2026-07-24 ICF（投資キャッシュフロー）。raw/・ttm/廃止方針検討時に命名未定と判明し、operating_cash_flowの命名パターンに揃えて新規命名"
    },
    "financing_cash_flow": {
      "category": "flow",
      "unit": "USD",
      "candidates": [
        "NetCashProvidedByUsedInFinancingActivities"
      ],
      "_note": "2026-07-24 CFF（財務キャッシュフロー）。同上の理由で新規命名"
    }
  },
  "ticker_overrides": {
    "MSFT": {
      "field": "revenue",
      "action": "exclude",
      "note": "既存TICKER_RESTRICTIONS移行"
    }
  }
}
```

**未確定事項**:
- Revenue・RPO・_COGS（単純網羅性差、union採用予定）の具体的な統合後
  candidates順序は実装時に確定する
- `TICKER_RESTRICTIONS`（9銘柄分の既存override）の3キー形式への変換
  内容は実装時に個別確認する

---

## 4-2. raw/・ttm/の廃止・統合方針

raw/・ttm/の廃止・統合について、投資調査（2026-07-24）で以下の方針が
確定した。

**raw/の廃止**:
唯一の消費者であるaudit.py（UP-C構造検知、SharesBasic/SharesDiluted
タグの10-Q欠如チェック）は、Layer1（company_facts.json）から
`facts["us-gaap"][concept]["units"][unit]`のエントリをform=="10-Q"で
フィルタし空か否かを判定するだけで直接再現可能（raw/の重複排除・filed
日タイブレーク処理は本チェックには不要）。raw/生成処理
（build_raw_table()/save_raw_table()）と共に廃止する。raw/とttm/の間に
依存関係はなく、独立して廃止可能。

**ttm/の位置づけ**:
`calc_ttm_series()`は`normalize()`の戻り値をインメモリで直接受け取る
設計のため、「ファイルとしてのnormalized/統合完了」ではなく「normalize()
相当関数がLayer2/Layer3準拠のfields{}辞書を返せるようになった時点」で
ttm_calculator.py側を追随させる、という順序になる。

**重要な実装制約**: `ttm_calculator.py::FLOW_FIELDS`（現状PascalCase
frozenset）と、normalize()相当関数の出力キー名は、**同一コミットで
同時に変更する必要がある**。片方だけ変更すると`flow.get(field_name, [])`
が常に空を返し、データが静かに消える（サイレント障害のリスク）。
移行実装時は、この2箇所の同時変更をタスクの必須要件として明記する。

`_build_q4_quarterly_entries()`（[[Q4-IMPLIED-CALC-TRIPLICATION-1]]の
3重実装の1つ）は、normalized側のQ4逆算エントリのend日付と照合する
設計のため、本移行と[[Q4-IMPLIED-CALC-TRIPLICATION-1]]の解消は連動して
検討する。

FCF（free_cash_flow）はLayer3の32フィールドの概念モデル（XBRLタグ→
概念の1対1対応）に乗らない派生値のため、TTM集計層に個別の計算ロジック
（現行`_calc_fcf()`相当）として残す。

---

## 4-3. layer3_builder.py実装時に発見した設計変更（2026-07-24）

フェーズA完了後の105銘柄回帰検証・フェーズC着手前の前提整理の過程で、
以下2件の設計変更を行った。

**候補選択方式の変更（[[LAYER3-FALLBACK-STALE-TAG-PRIORITY-1]]対応、
コミット925a02733）**:
当初実装（候補タグの生エントリを先にend_date単位でマージしてから
YTD→単四半期変換）は、複数タグ由来の生エントリが同一FYチェーンに
混在し、変換結果が破壊されるバグを引き起こした（CPRT・PEP等6銘柄・
20エントリで確認）。修正として、候補タグごとに独立してYTD→単四半期
変換を行った上で、正規化済み系列同士をend_date単位でマージする
方式に変更した（優先タグにその期間のエントリがあれば採用、なければ
次候補にフォールバック）。この変更により、revenue（IONQ等6銘柄）・
short_term_debt・stockholders_equity・research_and_developmentの
タグ鮮度問題も解消した。

**優先タグ内欠落四半期のフォールバック追加（
[[LAYER3-MISSING-QUARTER-IMPLIED-GAP-1]]対応、コミットfd7473e57）**:
優先タグ自体が特定四半期の報告を欠落させ隣接期間と合算されて誤計上
される問題（RCAT stock_based_compensationで確認）に対し、正規化済み
エントリのperiod_daysが標準的な四半期範囲（75〜100日、is_annual/
is_implied除外）から外れる場合に次候補へフォールバックする完全性
チェックを追加した。105銘柄×32フィールド全数のperiod_days分布に
基づき閾値を設定し、正当な短期スタブ期間（IPO直後等5件）は
「全候補が範囲外の場合は最優先候補へフォールバック」する設計で
誤って除外されないことを確認済み。

いずれも既存フィールド名の変更を伴わないため、NAMING_CONVENTIONS.md
の命名チェックリストは適用対象外。

---

## 5. スコープ確定事項

| 対象 | 判断 | 理由 |
|---|---|---|
| quarterly/annual（32フィールド） | 統合スコープに含める | 3章参照 |
| segments（セグメントKPI） | **除外**（現状維持） | 2-3参照、TOBE前提の誤りが判明 |
| filing_text（内部統制テキスト） | 統合スコープに含める（低コスト、書き込み側1＋消費側3ファイル） | 既にfiling_text概念と構造的親和性高 |
| submissions | 現状の`fetcher.py`キャッシュ経路を正とし、`edgar_rss_monitor.py`側は個別修正 | [[SEC-SUBMISSIONS-DUAL-FETCH-1]]で対応、統合スキーマ構造への影響なし |
| EPS Analyzer（52タグ） | Layer2設定拡張で対応、独自API取得は将来的に廃止候補 | 2-5・4章参照 |

---

## 6. 未確定・今後の検討事項

- Layer2（概念定義設定ファイル）の詳細スキーマ設計
- 統合スキーマへの実際の移行順序・既存consumer（reader.py・
  pipeline.py等）の切り替え計画
- EPS Analyzer変換ロジック（調整項目検出・DTA異常検知等）の
  配線変更実装
- filing_text吸収の実装
- 現行105銘柄ユニバースにJPM/GS等の伝統的金融機関・古典的REITが
  含まれておらず、これら業態でのフィールド網羅性は未検証。金融機関の
  FCFF/FCFE比較評価（TANUKI-FIN-2）に着手する際は、本設計が前提とする
  フィールド網羅性が同様に成立するか、別途検証が必要
- Equity（stockholders_equity）とNetIncome（net_income）の非支配持分
  （NCI）扱いの一貫性未検証。両フィールドとも現状の消費箇所
  （ROE計算・DuPont分解・投下資本計算等）はNCI除外版（親会社株主帰属分）
  のprimaryタグを使用しているが、NI側とEquity側でNCI包含・除外の定義が
  実際に揃っているかを検証したコード・調査は存在しない。ROE等の比率
  計算は分子・分母でNCI扱いが揃っていないと歪みが生じるため、Layer2
  統合時までに検証が必要（2026-07-24、Layer2設計調査で発見）

---

## 7. 関連BACKLOG項目

[[CAPEX-SIGN-UNNORMALIZED-1]]（対応完了）・
[[RICE-TTM-CAPEX-SUM-SIGN-1]]（対応完了）・
[[SCHEMA-STDEBT-COVERAGE-GAP-1]]・[[SCHEMA-SM-SGA-CONFLATION-1]]・
[[NORMALIZER-YTD-METADATA-STALE-1]]・
[[DOCS-SECDATA-NORMALIZED-DIR-STALE-1]]・
[[SEGMENT-FETCHER-DUPLICATE-ORPHAN-1]]・
[[SCHEMA-NORMALIZED-ANNUAL-NAMING-MISMATCH-1]]・
[[SEC-SUBMISSIONS-DUAL-FETCH-1]]・
[[SECDATA-COMPANYFACTS-OVERLOOKED-1]]・
[[Q4-IMPLIED-CALC-TRIPLICATION-1]]・
[[TTM-FLOW-FIELDS-FROZENSET-NONDETERMINISTIC-1]]・
[[SCHEMA-LTDEBT-DOUBLECOUNT-RISK-1]]・
[[SCHEMA-SHARESBASIC-CONCEPT-MISMATCH-1]]・
[[SCHEMA-DA-FALLBACK-MISSING-1]]

---

## 8. 移行実装計画

既存9→10系統から統合スキーマへの移行は、一斉切替ではなく
「新旧併存→段階的切替→旧経路廃止」の順で進める。

**フェーズA（新規構築、既存コード非改変）**:
`config/sec_concept_definitions.json`（Layer2）を実際に作成し、
Layer1（company_facts.json）＋Layer2からLayer3（32フィールド）を
生成する新規モジュールを構築する。FCF（free_cash_flow）計算式は
parser.pyとttm_calculator.pyに独立実装されている状態
（[[Q4-IMPLIED-CALC-TRIPLICATION-1]]と同種のリスク）を解消するため、
共有関数として1箇所に集約し、通常のquarterly/annual生成側とttm集計側
の両方がこれを呼ぶ設計にする。出力は既存data/・normalized/・raw/とは
別の新規パスに書き、105銘柄全数で新旧比較の回帰レポートを作成する。
既存consumerのコードは一切変更しない。

**フェーズB（低リスク独立消費者の切替）**:
audit.pyのUP-C構造検知をLayer1直接判定に切替（raw/依存を断つ）。
このタイミングで[[Q4-IMPLIED-CALC-TRIPLICATION-1]]（3重実装）を集約。

**【2026-07-24完了】** コミット`ebef5e46a`（audit.py Layer1直接
判定切替、105銘柄全数で新旧ロジック完全一致確認済み）・
`a7678d16c`（Q4逆算ロジック集約、`common/sec_data/q4_implied.py`
新規作成）。

集約作業の過程で、`normalizer.py::Q4_IMPLIED_FIELDS`（13フィールド）
と`ttm_calculator.py::FLOW_FIELDS`（Q4逆算適用対象14フィールド）が
完全一致しておらず、`FinanceLeasePmts`・`Buyback`の2フィールドで
`ttm_calculator.py`側のみが実際に非空のQ4 impliedエントリを生成して
いたという、未文書化のスコープ差異が判明した。この差異は集約前の
2モジュール間で気づかれないまま存在していたもので、バグとしての
実害は確認されていない（両モジュールとも自身のスコープ内では
正しく動作していた）。共有関数化にあたり「値を変えない集約」を
優先し、両者の許可フィールドの**和集合（15フィールド）**をガード
条件として採用した（元のガードの目的＝shares/stock系フィールドへの
誤適用防止は維持）。105銘柄全数でnormalized/・ttm/・STONKS SILO
financial_vectorsいずれも出力不変（idempotent）を確認済み。

**フェーズC（ttm_calculator.py移行）**:
FLOW_FIELDSとnormalize()相当関数の出力キー名を同一コミットで
同時変更する（片方だけ変更するとサイレントにデータが消えるため）。
移行直後に105銘柄全数のTTM値回帰確認を行う。

**【2026-07-24完了】** コミット`0148301c1`（tests/
test_ttm_calculator.py・tests/test_contracts.py・
common/sec_data/ttm_calculator.py・common/sec_data/update.py）。

実装完了までに、layer3_builder.py（フェーズAの成果物）側で複数の
バグを発見・修正した:
- [[LAYER3-FALLBACK-STALE-TAG-PRIORITY-1]]（タグ鮮度問題、候補
  タグごと正規化→期間単位マージへの処理順序変更で解消）
- [[LAYER3-MISSING-QUARTER-IMPLIED-GAP-1]]（優先タグ内欠落四半期、
  period_days完全性チェック追加）
- [[LAYER3-ANNUAL-QUARTERLY-COLLISION-1]]（年次/四半期の(end_date,
  is_annual)複合キー分離）
- [[LAYER3-Q4-IMPLIED-NOT-MIGRATED-1]]（Q4逆算のq4_implied.py統一、
  PascalCase/snake_case両対応）
- [[LAYER3-EPS-UNIT-MISMATCH-1]]（unit指定誤り修正）
- [[LAYER3-GROSSPROFIT-BACKFILL-MISSING-1]]（GrossProfitバックフィル
  移植、全フィールドループ後処理として実装）
- [[SOFI-TICKER-RESTRICTIONS-NOT-MIGRATED-1]]（ticker_overrides
  機構の新規実装、9銘柄移行）
- [[LAYER3-DA-SBC-CANDIDATE-REGRESSION-1]]（年次/四半期クロスタグ
  混入、同一source_tagガード＋単独タグ完結フォールバック）
- [[LAYER3-TTM-TEST-SUITE-SHAPE-MISMATCH-1]]（既存テストの新形状
  対応）

先頭欠落四半期逆算（H1YTD−Q2SA型、normalizer.py::
_build_missing_quarter_implied_entries()相当）も本フェーズの過程で
layer3_builder.pyへ移植した。

TTM系列レベルの総不一致は、最初の回帰検証時点（633件）から410件
まで減少。残る410件は既知の限界（selling_and_marketingのSM/SGA
分離副作用等）または個別の残差（[[LAYER3-ASTS-DDOG-Q4-RESIDUAL-1]]
は判定済みで対応不要と結論）であり、新規の未解明バグは残って
いない。pytest 442件全通過（既知の無関係2件を除く）。

### フェーズC完了後に発覚した検証の死角（2026-07-24）

フェーズC完了を「問題ゼロ」の基準で再点検した結果、TTM回帰の
残差410件のうちselling_and_marketing 258件を個別検証したところ、
これまでの回帰比較スクリプト自体の設計欠陥が判明した
（[[LAYER3-TTM-REGRESSION-NEWFIELD-BLINDSPOT-1]]）: 旧パイプライン
に存在しなかった新規6フィールド（short_term_investments・
total_liabilities・eps_basic・eps_diluted・cost_of_revenue・
selling_general_and_administrative）は、旧ttm/データのキーを起点に
突合する現行スクリプトでは検証対象に入らない「死角」になっていた。

この死角を通じて、selling_general_and_administrativeに実際に2件の
未解決バグが存在することが判明した:
- [[LAYER3-SGA-Q4-MISSING-1]]（Q4_IMPLIED_FIELDS等のスコープに
  未登録のためQ4が恒常的に欠落、42銘柄・171四半期）
- [[LAYER3-GA-STANDALONE-TAG-UNMAPPED-1]]（GeneralAndAdministrative
  Expense単体タグが32フィールドいずれにも未マッピング、
  少なくとも6銘柄でSM・SGA両方が空になる）

残る新規5フィールド（short_term_investments・total_liabilities・
eps_basic・eps_diluted・cost_of_revenue）は、同種の死角に入ったまま
未検証。次回セッションはこの5フィールドの個別検証
（[[LAYER3-TTM-REGRESSION-NEWFIELD-BLINDSPOT-1]]着手時、または
個別に）から再開することを推奨する。

**フェーズCの完了判定について**: コード自体（ttm_calculator.py・
update.py）はコミット済みだが、「原因不明の問題がゼロ件」という
基準では未達成のまま本日のセッションを終了する。次回はこの3件
（[[LAYER3-SGA-Q4-MISSING-1]]・[[LAYER3-GA-STANDALONE-TAG-
UNMAPPED-1]]・[[LAYER3-TTM-REGRESSION-NEWFIELD-BLINDSPOT-1]]）の
解消、および死角に入っている残り5フィールドの検証から再開する。

**フェーズD（本体consumer切替、優先順位確定済み）**:
1. TANUKI VALUATION本体（reader.py・pipeline.py） — 最大の影響範囲
   だが中核システムのため最優先。切替時は105銘柄全数の実データ回帰
   確認を最も手厚く行う
2. STONKS SILO（financial_trend_calculator.py・fetcher.py・
   analyzer.py）
3. TANUKI TAIL（quarterly_review_generator.py・tail_dcf_bridge.py）
4. HypeCore
5. stock.htmlフロントエンド

**フェーズD対象リストへの追記（診断・補助スクリプト群、2026-07-30
`common/sec_data`統合実施計画策定投資調査で判明）**:
上記1〜5の主要パイプラインとは別に、`data/annual_*.json`または
`normalized/`を直接参照する診断・補助スクリプトが以下6件存在する。
フェーズE（旧経路廃止）実施時にこれらを見落とすと、
`[[TTM-PASCALCASE-KEY-STALE-1]]`と同型の「消費者取り残し」が再発する
リスクがあるため、フェーズD完了判定の対象に含める。

| ファイル | 参照するストレージ |
|---|---|
| `common/screening/dcf_validity_checker.py` | `data/annual_*.json`直読み（独自glob） |
| `src/value/tanuki_valuation/beta_fetcher.py` | `data/annual_*.json`直読み |
| `src/value/tanuki_valuation/kpi_fetcher.py` | `data/annual_*.json`直読み（segments） |
| `common/sec_data/segment_fetcher.py` | `data/annual_*.json`へ書き込み（`[[SEGMENT-FETCHER-DUPLICATE-ORPHAN-1]]`の重複ファイルの一つ） |
| `src/value/tanuki_valuation/segment_fetcher.py` | 同上（重複ファイルのもう一つ） |
| `common/sec_data/registration_validator.py` | `data/annual_*.json`存在チェック・年次/TTM revenue比較 |

（`common/sec_data/quality_checker.py`は表から削除。全リポジトリ
importゼロの死蔵コードと判明し[[DEAD-CODE-AUDIT-BATCH-1]]（2026-09-09）
でファイルごと削除済み）

**注記（3スキーマ併存の実態）**: 上記調査により、現状は「新旧2スキーマ
併存」ではなく、実態として**3スキーマが併存**していることが判明した。
Layer3（`layer3_builder.py`、snake_case）・`data/annual_*.json`等
（`parser.py`、snake_case）に加え、`normalizer.py`が出力する
`normalized/`は今も**PascalCase**のまま
`financial_trend_calculator.py`（STONKS SILO）から直接消費されている。
フェーズD・E実施時は、この`normalized/`のPascalCase系統も明示的な
移行対象として扱うこと。

**フェーズE（旧経路廃止）**:
全consumer移行完了後、raw/・旧normalized/・FIELD_CONCEPTS/
XBRL_MAPPING（コード版）を削除する。

**フェーズF・G（並行トラック）**:
EPS Analyzer52タグのLayer2追加（独自取得層の廃止）／filing_text吸収。

**スコープ外の確認事項**: net_cash・rule40等の導出データ層における
計算式重複（[[NETCASH-DUAL-CALC-1]]・[[RULE40-DEFINITION-MISMATCH-1]]）
は、本プロジェクト（一次データ層構築）のスコープ外とし、既存の個別
BACKLOGタスク側で扱う。FCF計算式の集約（上記フェーズA）のみ、Layer3
自体の一部として本プロジェクトに含める。
