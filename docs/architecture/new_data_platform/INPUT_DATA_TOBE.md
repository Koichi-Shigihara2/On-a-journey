# INPUT_DATA_TOBE.md — 一次データ層のTO-BE設計

作成日: 2026-07-23
更新日: 2026-08-12（`common/macro_data/`実装設計確定を反映。①A-3節
（FRED、24系列）へ`FTSD`（`WTREGEN`のフォールバック専用系列、
`[[MACRODATA-FTSD-MISSING-FROM-INVENTORY-1]]`対応）を`INPUT-A-049`
として追加、分類A件数を48件→49件・合計を65件→66件に更新。②2-C節へ
保存形式のJSON確定（当初のCSV案から`common/market_data/`と形式統一す
るため変更）・`series_meta.json`新設・provenance標準の適用方針を追記。
③3-B節へ`fetcher.py::fetch_series()`/`reader.py`の具体API・保存前
検証ログ（`macro_data_violations_log.json`）・重複3系列
（`BAMLH0A0HYM2`・`T10Y2Y`・`VIXCLS`）の`reader.py`一本化方針を追記。
詳細はBACKLOG.md`[[MACRODATA-LAYER-CONSTRUCTION-1]]`参照。実装コード
変更・データ再生成なし）
更新日: 2026-07-23（3分類再構成・ID付番。従来「1-A〜1-D」というソース別
構成から、性質別の3分類「A: 一次データ本体／B: 取得前提条件／C: 導出
データの入力」へ再整理し、全項目にID（`INPUT-A-NNN`等）を付番した）
更新日: 2026-07-24（分類A件数を47件→48件に訂正。①`INPUT-A-016`
〈セグメント別売上・KPI〉を正式ASC280セグメントから`tail_kpi_map.json`
ベースの銘柄固有カスタムKPI〈フェーズ1統合スコープ外〉に訂正、
②Adjusted EPS算出専用の税務・一過性項目タグ群52種を`INPUT-A-048`として
新規追加、③`common/sec_data/data/{TICKER}/company_facts.json`〈SEC
EDGAR company_facts API生レスポンス全量、既存〉がLayer1〈無加工
アーカイブ〉の要件を既に満たしていることが判明し新規構築不要と判明、の
3点を反映した結果。詳細はPROJECT_STATUS.md該当箇所・BACKLOG.md
`[[SECDATA-COMPANYFACTS-OVERLOOKED-1]]`参照）
出発点: `FIELD_DEFINITIONS.md`（499項目の計算式最小単位分解、完了済み）の
「データ取得元」列、`CONCEPT_PARAMETER_VARIATIONS.md`軸2（取得元・データ
ソース）、`OUTPUT_ITEMS_INVENTORY.md`「外部データ取得経路」セクション、
`NAMING_CONVENTIONS.md`規則4（provenance明示）。

## 本ドキュメントの位置づけ

`FIELD_DEFINITIONS.md`で499項目全件（一次データ42件＋導出データ392件、
残り65件は手動入力・移送・システム設定）の計算式を最小単位まで分解した
結果、これらを充足するために実際に必要な一次データ（外部ソース由来の
生データ）の全体像が判明した。本ドキュメントは、この一次データ層を
**現状の実装（複数経路・複数ファイルでの重複取得）にとらわれず、白紙から
「あるべき姿（TO-BE）」として設計する**。

現状（AS-IS）の実装との具体的な比較・移行計画は次ステップで行う。
本ドキュメントは設計のみを記録し、実装（コード修正）は一切行っていない。

## 3分類の定義と総数

FIELD_DEFINITIONS.md499項目の分解から導かれた項目には、性質の異なる
3種類が混在していたため、以下の3分類に再整理した。

- **分類A: 一次データ本体** — SEC EDGAR/yfinance/FRED各項目そのもの
  （外部ソース由来の生データ）
- **分類B: 取得前提条件** — 一次データ取得そのものに先立って必要な設定
  （監視銘柄マスタ・CIKマッピング等）
- **分類C: 導出データの入力** — 一次データではなく、FIELD_DEFINITIONS.md
  の導出データ側（392件）が消費する入力（手動設定によるDCF前提・
  AIプロンプトテンプレート等）

| 分類 | 件数 | ID範囲 |
|---|---|---|
| A. 一次データ本体 | **50件** | `INPUT-A-001`〜`INPUT-A-050`（2026-09-13追加の`INPUT-A-050`＝`WDTGAL`含む） |
| B. 取得前提条件 | **3件** | `INPUT-B-001`〜`INPUT-B-003` |
| C. 導出データの入力 | **14件** | `INPUT-C-001`〜`INPUT-C-014` |
| **合計** | **67件** | — |

**判定に迷った項目の分類根拠（実コード確認済み）**:
- `config/split_history.yaml`: `src/value/adjusted_eps_analyzer/pipeline.py`
  の`apply_split_adjustments()`が、SEC EDGARから取得済みの
  `quarterly_results`（EPS Analyzerの導出データ、AS-IS-267）に対して
  遡及補正を**事後適用**する処理であり、「取得前」の前提条件ではなく
  「取得後の導出計算」に対する入力であることをコードで確認した。
  → **分類C**（取得前提条件＝分類Bではない）
- `config/adjustment_items.json`: `adjustment_detector.py::
  load_adjustment_items()`が読み込み、`sector_exclusions`と組み合わせて
  `adjusted_eps`（導出データ、AS-IS-267）の調整項目検出ロジックに直接
  投入されることをコードで確認した。→ **分類C**
- `config/sectors.yaml`: `sector_classifier_v2.py::SectorClassifierV2`が
  読み込み、`classifier.classify()`→`get_exclusions_for_sector()`という
  経路でEPS Analyzerの調整項目除外リストに使われることをコードで確認
  した。**依頼文の例示（分類Bの例として挙げられていた）とは異なり、
  実際には「取得前提条件」ではなく「導出データ（adjusted_eps）側が
  消費する入力」であることが判明したため、分類Cとする**（依頼文の
  想定を実コード確認の結果で訂正した項目）
- `config/beta_config.json`: β自体（yfinance由来の生データ）は分類A
  （`INPUT-A-020`の`.info`属性に含まれる）に既に計上済み。本ファイルは
  そのβ値のセクター別デフォルト・手動オーバーライドという、WACC計算
  （導出データ、AS-IS-013）への追加的な入力を担う部分が主目的のため、
  ファイル自体は**分類C**（生のβ値の保持先ではなく、オーバーライドの
  管理主体という性質を優先した）

---

## 分類A: 一次データ本体（49件）

`FIELD_DEFINITIONS.md`の「データ取得元」列を全件走査し、499項目が最終的に
依存する一次データを、実際にソースコードを直接確認しながら整理した
（Grok/AI生成コンテンツは、この一次データ層を入力として消費する側であり、
一次データそのものではないため対象外とする。5分類上も「導出データ」に
分類済み）。

### A-1. SEC EDGAR（XBRL構造化データ、18件: INPUT-A-001〜018）

499項目のうち、TANUKI VALUATION／HypeCore／STONKS SILO／Discover／
TANUKI TAIL／EPS Analyzerの各サブシステムが依存する財務諸表項目を、
「銘柄×決算期（四半期・通期）」単位のXBRLファクトとして整理すると、
以下の一意なファクト種別に集約される（同一概念が複数サブシステムの
式に登場しても、必要なXBRLタグ自体は1種類）。

| ID | ファクト種別 | 代表XBRLタグ相当 | 用途（依存する主な計算） |
|---|---|---|---|
| INPUT-A-001 | 売上高 | `Revenues`等 | PSR分母、成長率、Rule of 40成長率項、moat_score fcf_norm分母 |
| INPUT-A-002 | 純利益 | `NetIncomeLoss` | EPS、rule40純利益率項、net_income系 |
| INPUT-A-003 | 営業利益 | `OperatingIncomeLoss` | rule_of_40営業利益率項、moat_score ROIC計算のNOPAT |
| INPUT-A-004 | 売上総利益 | `GrossProfit`（欠如時はRevenue−COGSで代替） | moat_score gross_margin |
| INPUT-A-005 | 営業キャッシュフロー | `NetCashProvidedByUsedInOperatingActivities` | FCF計算、STONKS SILO runway |
| INPUT-A-006 | 設備投資（CapEx） | `PaymentsToAcquirePropertyPlantAndEquipment`等（符号が発行体によって正負混在） | FCF計算（符号正規化必須、AS-IS-071既知バグの根本原因） |
| INPUT-A-007 | ファイナンスリース関連 | 各種リース系タグ | FCF計算の`FinanceLease`控除項 |
| INPUT-A-008 | 研究開発費 | `ResearchAndDevelopmentExpense` | moat_score・成熟利益計算の投資強度分母 |
| INPUT-A-009 | 販管費・S&M | `SellingGeneralAndAdministrativeExpense`等（S&M単独タグは発行体により非開示） | 投資強度分母（欠如時の扱いが既知の課題） |
| INPUT-A-010 | 株式報酬（SBC） | `ShareBasedCompensation` | RICE Q項、純利益への足し戻し |
| INPUT-A-011 | 現金・現金同等物 | `CashAndCashEquivalentsAtCarryingValue` | net_cash計算 |
| INPUT-A-012 | 短期投資 | `ShortTermInvestments` | net_cash計算 |
| INPUT-A-013 | 長期有利子負債 | `LongTermDebtNoncurrent`等 | net_cash・total_debt計算 |
| INPUT-A-014 | 短期有利子負債 | `LongTermDebtCurrent`等 | net_cash・total_debt計算 |
| INPUT-A-015 | 希薄化後株式数 | `WeightedAverageNumberOfDilutedSharesOutstanding` | EPS、1株あたり価値、希薄化率 |
| INPUT-A-016 | セグメント別売上・KPI | 正式ASC280セグメントタグではなく、`tail_kpi_map.json`で銘柄ごとに個別定義された投資テーゼ用カスタムKPI（生XBRL XML文書の直接ダウンロード＋explicitMemberディメンション解析、company_facts APIとは別方式） | TANUKI TAILのセグメントKPI抽出（`docs/portfolio/tail/data/kpi/{ticker}_layer2.json`、現状common/sec_dataとは完全に独立した取得・保持経路） |
| INPUT-A-017 | 内部統制関連テキスト | 10-Q Item4本文（非XBRL、全文テキスト） | TANUKI TAILの内部統制監視 |
| INPUT-A-018 | 直近提出日・提出書類一覧 | EDGAR submissions API（`data.sec.gov/submissions/`） | 新規提出監視、CIKルックアップ |
| INPUT-A-048 | 税務・一過性項目・銀行業向け詳細タグ群（52種） | `IncomeTaxExpenseBenefit`系・`IncomeLossBeforeIncomeTaxExpenseBenefit`系・`GoodwillImpairmentLoss`・`RestructuringCharges`・`LitigationSettlementExpense`・`FairValueAdjustmentOfWarrants`・`GainLossOnDerivativeInstrumentsNetPretax`・`ProvisionForLoanLosses`系・`NetInterestIncome`・`NoninterestIncome`等（全量はEPS Analyzer調査完了報告を参照） | Adjusted EPS算出専用（調整項目検出・税効果調整・DTA異常検知・公正価値変動検出）。現状はEPS Analyzer（`extract_key_facts.py`）が独自にSEC EDGAR company_facts APIを取得しているが、`company_facts.json`（後述）に既に全量含まれているため、新規API取得なしでLayer2設定の追加のみで対応可能 |

**必要なアクセス方式**: (a) XBRL構造化ファクト取得（Company Facts API相当）、
(b) 提出書類一覧・メタデータ取得（submissions API）、(c) 10-Q本文の
全文テキスト取得（Archives経由）——の3方式に整理できる。

### A-2. yfinance（株価・市場データ・企業属性スナップショット、5件: INPUT-A-019〜023）

| ID | データ種別 | 内容 | 用途 |
|---|---|---|---|
| INPUT-A-019 | 個別銘柄の価格・出来高履歴 | 日次OHLCV（少なくとも過去252営業日） | 移動平均・乖離率・ボラティリティ・52週高安値・出来高比 |
| INPUT-A-020 | 個別銘柄の`.info`属性 | trailingPE/forwardPE/pegRatio/priceToSalesTrailing12Months/enterpriseToEbitda/forwardEps/targetMeanPrice/dividendYield/payoutRatio/heldPercentInsiders/**beta**/shortRatio/marketCap/sector/industry | PER/PEG/PSR/EV_EBITDA等の倍率系、moat_score・timing_score入力。**βの生値もここに含む**（`config/beta_config.json`＝分類`INPUT-C-005`はこの生値のセクター別デフォルト・手動オーバーライドを管理する別レイヤー） |
| INPUT-A-021 | アナリスト格上げ・格下げ履歴 | `upgrades_downgrades` | analyst_upgrade_rate/downgrade_rate |
| INPUT-A-022 | 指数・ETF・商品の価格・出来高履歴 | S&P500(`^GSPC`)、NASDAQ、NYSE Composite(`^NYA`)、日経平均、VIX(`^VIX`)、VIX9D(`^VIX9D`)、WTI原油、金、HYG、LQD、QQQ、SPY、RSP、IVW（グロース）、IVE（バリュー）、RUT（小型株）、TLT、SHV、USD/JPY | MACRO/Market Pulseの各種指標、asset_flow、breadth計算 |
| INPUT-A-023 | S&P500構成銘柄一括データ | 構成銘柄リスト＋日次OHLCV一括ダウンロード | market breadth（騰落レシオ・新高値新安値・NH-NL等）算出 |

**フォールバック方針の設計**: `^IRX`（3ヶ月T-Bill）はGitHub Actions環境
からの取得が構造的に不安定であることが既に判明しているため、TO-BE設計
でも短期金利は最初からFRED `DGS3MO`（`INPUT-A-047`）を正とし、yfinance
での取得を試みない（現状のMarket Pulseが既に採用している回避策を、
設計レベルの標準として採用する）。

### A-3. FRED（マクロ経済系列、24件: INPUT-A-024〜047）

ソースコード（`05_main.py`の`INDICATOR_CONFIG`辞書、`update_liquidity_csv()`、
Market Pulseの`collect_and_send.py`/`backfill_tech_pulse.py`）を直接確認し、
必要なFRED系列を重複なく24系列に整理した。

| ID | 系列コード | 内容 | 用途 |
|---|---|---|---|
| INPUT-A-024 | `T10Y2Y` | 10年債-2年債利回り格差 | RECESSION RISK SCORE・イールドカーブ判定 |
| INPUT-A-025 | `BAMLH0A0HYM2` | ハイイールド債OASスプレッド | RECESSION RISK SCORE・流動性カード（現状3箇所で重複取得、後述） |
| INPUT-A-026 | `GACDFSA066MSFRBPHI` | フィラデルフィア連銀製造業景況指数 | RECESSION RISK SCORE |
| INPUT-A-027 | `CFNAI` | シカゴ連銀全米活動指数 | RECESSION RISK SCORE |
| INPUT-A-028 | `IC4WSA` | 新規失業保険申請件数4週平均 | RECESSION RISK SCORE |
| INPUT-A-029 | `MICH` | ミシガン大学インフレ期待(1年) | RECESSION RISK SCORE |
| INPUT-A-030 | `T5YIE` | 5年ブレークイーブンインフレ率(ミシガン5年の市場ベース代替) | RECESSION RISK SCORE |
| INPUT-A-031 | `UMCSENT` | ミシガン大学消費者信頼感指数 | RECESSION RISK SCORE |
| INPUT-A-032 | `PERMIT` | 住宅着工許可件数 | RECESSION RISK SCORE |
| INPUT-A-033 | `SAHMCURRENT` | Sahm Ruleリセッション指標 | RECESSION RISK SCORE |
| INPUT-A-034 | `PAYEMS` | 非農業部門雇用者数(NFP) | マクロサプライズ検知 |
| INPUT-A-035 | `VIXCLS` | VIX恐怖指数(FRED版) | MACRO PULSE文脈記録 |
| INPUT-A-036 | `SP500` | S&P500指数終値 | MACRO PULSEティッカー表示 |
| INPUT-A-037 | `DGS1` | 1年国債利回り | 利下げ/利上げ織り込み計算 |
| INPUT-A-038 | `DFEDTARU` | FF金利誘導目標レンジ上限 | FF金利現在値（`DFEDTARL`と平均） |
| INPUT-A-039 | `DFEDTARL` | FF金利誘導目標レンジ下限 | FF金利現在値（`DFEDTARU`と平均） |
| INPUT-A-040 | `FEDFUNDS` | 実効FF金利(フォールバック) | FF金利現在値のフォールバック |
| INPUT-A-041 | `WALCL` | FRB総資産(バランスシート) | NET LIQUIDITY計算 |
| INPUT-A-042 | `WTREGEN` | 財務省一般勘定(TGA)残高 | NET LIQUIDITY計算・ステルス供給/吸収判定 |
| INPUT-A-043 | `RRPONTSYD` | オーバーナイトリバースレポ残高 | NET LIQUIDITY計算・ステルス供給/吸収判定 |
| INPUT-A-044 | `WRBWFRBL` | 銀行準備預金残高 | ステルス供給/吸収判定の補助指標 |
| INPUT-A-045 | `M2SL` | M2マネーサプライ | 流動性カード表示 |
| INPUT-A-046 | `VXNCLS` | ナスダック版VIX(VXN) | Tech Pulse divergence計算 |
| INPUT-A-047 | `DGS3MO` | 3ヶ月国債利回り | asset_flow短期金利(yfinance `^IRX`の構造的フォールバック先) |
| INPUT-A-049 | `FTSD`（**2026-09-13判明: FRED API上に実在しない無効な系列コード**、`WDTGAL`へ置換済み。経緯記録として残置） | 財務省一般勘定(TGA)残高、`WTREGEN`のフォールバック専用系列（旧設計、実際には常に取得失敗していた） | NET LIQUIDITY計算（`05_main.py::update_liquidity_csv()`、`WTREGEN`取得失敗時のみ使用。現行実装は`WDTGAL`を参照） |
| INPUT-A-050 | `WDTGAL` | 財務省一般勘定(TGA)残高〈Wednesday Level〉、`WTREGEN`のフォールバック専用系列（`FTSD`置換先） | NET LIQUIDITY計算（`05_main.py::update_liquidity_csv()`、`WTREGEN`取得失敗時のみ使用） |

**設計上の指摘**: `TANUKI VALUATION`の`risk_free_rate`（DCF計算のCAPM構成
要素、現状は`0.043`のハードコード定数）は、本来であればこのFRED系列層
から`DGS10`（10年国債利回り、現状は未取得。24件のIDには含まれない
＝新規追加が必要な系列）を都度取得すべき性質のデータである。TO-BE設計
では、DCF計算が参照する「無リスク金利」もこの一次データ層の管理対象に
含める（現状比較・実装要否の判断は次ステップ）。

---

## 分類B: 取得前提条件（3件: INPUT-B-001〜003）

一次データ（分類A）の取得そのものに先立って必要な設定。

| ID | ファイル | 内容 | 更新方式 |
|---|---|---|---|
| INPUT-B-001 | `config/monitor_tickers.yaml` | 監視銘柄マスタリスト（全サブシステムが対象とする銘柄の起点） | 手動編集。全ての一次データ取得（SEC EDGAR/yfinance/FRED）に先立って参照される最上流の設定 |
| INPUT-B-002 | `config/cik_lookup.csv` | Ticker→CIKマッピング（分類A-1のSEC EDGAR取得の前提データ） | 半自動（`TANUKI_CIK_Lookup.yml`手動実行） |
| INPUT-B-003 | `config/cik_lookup_result.json` | CIKルックアップ結果キャッシュ | `TANUKI_CIK_Lookup.yml`実行結果の永続化 |

---

## 分類C: 導出データの入力（14件: INPUT-C-001〜014）

一次データそのものではなく、`FIELD_DEFINITIONS.md`の導出データ側
（392件）が直接消費する手動設定・AIプロンプト等。

| ID | ファイル | 内容 | 消費先（導出データ側の計算） |
|---|---|---|---|
| INPUT-C-001 | `config/segment_config.json` | セグメント別加重成長率の手動設定 | growth.rate（AS-IS-012）。**【2026-08-15調査完了・現状維持】`_meta:{description,encoding,updated_at,schema_version}`を既に保持し、`growth_options_config.json`・`maturity_config.json`と同一スキーマで統一済み。`value-monitor/admin.html`に編集UIあり（commitMultipleFiles経由）。実害報告なし、対応不要と判断（`[[SECDATA-STORAGE-FRAGMENTATION-1]]`系フェーズ3未登録11件調査）** |
| INPUT-C-002 | `config/growth_options_config.json` | 成長オプション（TAM/浸透率/FCFマージン等）の銘柄別手動設定 | growth_options（AS-IS-016）。**【2026-08-15調査完了・現状維持】INPUT-C-001と同一理由（`_meta`統一済み・admin.html編集UIあり・実害なし）** |
| INPUT-C-003 | `config/maturity_config.json` | DCF成熟プロファイル（2段階/3段階、フェーズ年数・成長率）の銘柄別設定 | maturity_profile（AS-IS-017）、terminal_growth（AS-IS-059）。**【2026-08-15調査完了・現状維持】INPUT-C-001と同一理由（`_meta`統一済み・admin.html編集UIあり・実害なし）** |
| INPUT-C-004 | `config/rpo_config.json` | RPO（残存履行義務）調整設定 | rpo_adjustment（AS-IS-024）。**【2026-08-15調査完了・現状維持】`_meta`相当のメタ情報を持たず、admin.html編集UIも存在しない（手動JSON編集のみ）が、この欠如は既存`[[RPO-ADMIN-1]]`（2026-06-26登録）で既に捕捉済みのため重複登録しない。同項目に本調査での再確認・`_meta`欠如の追記のみ実施** |
| INPUT-C-005 | `config/beta_config.json` | β値のセクター別デフォルト・手動オーバーライド（生のβ自体は`INPUT-A-020`） | wacc.value/beta（AS-IS-013）。**【2026-08-15調査完了・現状維持】`value-monitor/admin.html`に編集UIあり（commitFile経由）。`_comment`はあるが更新日時等の`_meta`相当は持たない。実害報告なし、対応不要と判断** |
| INPUT-C-006 | `config/discover_config.json` | 銘柄別テーマ・区分・メモ | Discoverのカタリスト・テーマ関連導出データ |
| INPUT-C-007 | `config/theme_config.json` | テーママスタ（ID/ラベル/カラー） | Discoverのテーマ分類関連導出データ |
| INPUT-C-008 | `docs/portfolio/data/portfolio.json` | 保有株数・平均取得単価（ブローカー別） | Portfolio総資産評価額（AS-IS-391/392等）。**【2026-08-15修正】唯一の保持場所を`docs/portfolio/data/portfolio.json`とする（`config/portfolio.json`側は廃止）。理由: GitHub Pagesの公開ソースは`docs/`配下のみであり、`config/`はHTTP経由でfetch不可能なことを実測で確認済み（`https://.../config/portfolio.json`→404、`https://.../portfolio/data/portfolio.json`→200、`SYSTEM_MAP.md`「`config/`と`docs/`の配置原則」参照）。当初案「`config/`を唯一の保持場所に」はこの制約と技術的に矛盾するため撤回。また`config/portfolio.json`を読むPythonコードは現状ゼロ件であり、実際の読み取りは既に全て`docs/`側経由という実態にも整合する。根拠は`[[PORTFOLIO-CONFIG-DUP-1]]`2026-08-15付フェーズ3合同設計調査** |
| INPUT-C-009 | `config/tail_kpi_map.json` | TANUKI TAILのKPI設定（AI提案＋人手確定のハイブリッド） | TANUKI TAILのセグメントKPI関連導出データ。現状`config/`ではなく`docs/portfolio/tail/data/`配下（生成データと同居）に置かれているため、TO-BEでは他の手動設定ファイルと同様`config/`配下への集約を設計方針とする。**【2026-08-15確認】消費者は全てPythonバックエンド（GitHub Actionsワークフロー2件・`kpi_proposer.py`・`xbrl_segment_fetcher.py`）でフロントエンドからの直接fetchは無く、GitHub Pages制約の対象外と確認済み。本方針は変更不要、そのまま実装可能（`[[TAILKPI-CONFIG-LOCATION-1]]`参照）** |
| INPUT-C-010 | `config/fcf_conversion_config.json` | Damodaran業種別FCF変換率等の銘柄別上書き（`ticker_overrides`含む） | FCF変換率関連の導出データ。現状は`src/value/tanuki_valuation/`直下（`config/`外）に配置されており、TO-BEでは`config/`への集約を設計方針とする。**【2026-08-15確認】消費者は`adjustments.py`（Pythonバックエンド）と`admin.html`のGitHub Contents API経由アクセス（パスを問わず機能）のみで、フロントエンドからの直接fetchは無く、GitHub Pages制約の対象外と確認済み。本方針は変更不要、そのまま実装可能（`[[FCFCONFIG-LOCATION-1]]`参照）** |
| INPUT-C-011 | `config/prompts.yaml` | Grok/AI分析プロンプトテンプレート集約（EPS調整分析・カタリスト予測・TANUKI TAIL Stage2シナリオ等） | 392件の導出データのうちAI生成コンテンツ全般。その生成品質・再現性を左右するプロンプト自体を一次データ層に準じる管理対象として扱う（プロンプトの変更履歴・バージョン管理も将来的な設計対象）。**【2026-08-15調査完了・現状維持】編集UIなし（手動編集のみ）。低頻度・専門知識を要する編集であり、GitHub上での直接編集＋PRレビューの方が管理方法として適切と判断、編集UIを作る動機がない** |
| INPUT-C-012 | `config/split_history.yaml` | 株式分割の遡及補正用手動記録（比率・効力発生日） | `apply_split_adjustments()`が`quarterly_results`（EPS Analyzer導出データ、AS-IS-267）へ事後適用する遡及補正。**実コード確認: SEC EDGAR取得後のderived計算に対する入力であり、取得前提条件（分類B）ではない**。**【2026-08-15調査完了・現状維持】INPUT-C-011と同一理由（低頻度・専門知識を要する編集、編集UI不要と判断）** |
| INPUT-C-013 | `config/sectors.yaml` | セクター/業種のキーワードマッピング | `sector_classifier_v2.py`経由でEPS Analyzerの調整項目除外リスト（`adjusted_eps`計算、AS-IS-267）に投入。**実コード確認: `classifier.classify()`→`get_exclusions_for_sector()`という経路で導出データ側の計算に使われており、取得前提条件ではない**。**【2026-08-15調査完了・現状維持】`docs/value-monitor/adjusted_eps_analyzer/admin/`の編集UIは実在しないパス（`../config/sectors.yaml`）をfetchする死蔵ページと判明（`[[EPSANALYZER-ADMIN-ORPHAN-PAGE-1]]`）。**【2026-08-15追記・削除完了】参照ゼロを確認の上`git rm -r`で削除済み。編集経路は手動ファイル編集のみで確定（正当な編集UIは元々存在しなかった）** |
| INPUT-C-014 | `config/adjustment_items.json` | EPS Analyzerの調整項目カテゴリ・XBRLタグ定義 | `adjustment_detector.py::load_adjustment_items()`が`adjusted_eps`（AS-IS-267）の調整項目検出ロジックに直接投入。**【2026-08-15調査完了・現状維持】`version:"2026-04"`という軽量メタのみ保持。編集UIは`[[EPSANALYZER-ADMIN-ORPHAN-PAGE-1]]`と同じ死蔵ページのみで実質手動編集。実害報告なし、対応不要と判断**。**【2026-08-15追記・削除完了】死蔵ページは参照ゼロを確認の上削除済み。編集経路は手動ファイル編集のみで確定** |

**対象外と判定した項目（一次データ層3分類のいずれにも該当しない）**:
`config/warn_acknowledged.json`（品質ゲートの確認済み状態台帳）・
`config/workflow_dependencies.json`（ワークフロー依存関係定義）は、
いずれも499項目のいずれの計算にも入力されない運用状態データ・
メタ設定であり、5分類上は「システム設定データ」に相当するため、
一次データ層（分類A/B/C）の対象には含めない。

---

## ステップ2: 保持方法の設計

### 2-A. SEC EDGAR層（分類A-1・INPUT-A-001〜018）: 銘柄×決算期の正規化ストアに一本化

**設計方針**: XBRLファクトは「銘柄×決算期（四半期/通期）」を主キーとする
単一の正規化ストアに保持する。現状`common/sec_data/`が担っているこの役割を
TO-BEの唯一の正とし、**EPS Analyzer・TANUKI TAILが現在独自に持つ3系統の
独立SEC EDGARアクセス（B・C1〜C3）は、このストアの利用に一本化する**
（提出書類テキスト全文やセグメントKPIのような、現状の正規化ストアが
保持していないデータ種別は、同じストアの中に別テーブル/別ファイルとして
追加する形で吸収し、別パイプラインとして並存させない）。

**統合スコープの明確化（2026-07-23、`INPUT_DATA_AS_IS.md`との突合で是正）**:
「単一の正規化ストア」への一本化と述べたが、現状`common/sec_data/`自体が
既に`data/{TICKER}/annual_*.json・quarterly_*.json`に加え、`raw/`
（正規化前の生XBRL）・`normalized/`（`data/quarterly_*.json`とは別スキーマの
独立した正規化四半期データ）・`ttm/`（TTM系列）という**最低6ファイル系統**に
分岐している。特に`normalized/{TICKER}_quarterly_normalized.json`は
stock.htmlのキャッシュフロー分析セクション（CapEx符号未処理バグ、
`FIELD_DEFINITIONS.md`AS-IS-071）が直接参照する独立スキーマであるため、
統合時は「どちらのスキーマを正とするか」を明示的に決定する必要がある
（本ドキュメントは設計のみのため、この決定自体は次ステップの移行計画に
委ねる）。TO-BEの統合対象は`data/`だけでなく`raw/`・`normalized/`・
`ttm/`を含めた全系統である旨をここに明示する。

**Layer1（無加工アーカイブ）は新規構築不要、既に存在する**:
`common/sec_data/data/{TICKER}/company_facts.json`が、SEC EDGAR
company_facts APIの完全な生レスポンス（フィルタなし、AAPL実測505
concept）を無加工のまま週次で保存済み（`fetcher.py::
fetch_company_facts()`、git管理下）。統合設計はこのファイルを
Layer1として位置づけ、以下の`annual_{FY}.json`等（Layer3相当）は
Layer1からの抽出結果として再設計する。現状の`raw/
{TICKER}_quarterly_raw.json`（26概念（うち1件は内部専用フィールド
`_COGS`、出力からは除外）に絞ったフィルタ後データ、
Layer1ではなくLayer2設定の狭さの副産物）は、統合後は廃止候補
（[[SECDATA-STORAGE-FRAGMENTATION-1]]で検討）。

概念抽出ロジック（現状`FIELD_CONCEPTS`/`XBRL_MAPPING`としてPython
コードにハードコード）は、将来的に設定ファイル（YAML/JSON、
1エントリ＝1概念：内部フィールド名・タグ候補リスト・カテゴリ・
利用サブシステム）による管理へ移行する方向で検討する（Layer2、
詳細設計は別タスク）。

保持構造の案:
```
common/sec_data/data/{TICKER}/
  company_facts.json      # Layer1: SEC EDGAR company_facts API生レスポンス（既存）
  annual_{FY}.json        # 通期ファクト（正規化済み、INPUT-A-001〜015等）
  quarterly_{FYQ}.json    # 四半期ファクト（正規化済み）
                           # 【2026-08-05実装済み】pl/cf/shares区分は従来
                           # YTD累積値のまま保存されていたが（約65〜66%の
                           # エントリが該当）、単一四半期(SA)優先＋YTD差分
                           # 計算フォールバックの統一アルゴリズムを
                           # parser.py::parse_company_facts()に実装し、
                           # 本設計記述の「正規化済み」を実データで満たす
                           # 状態にした（[[SECDATA-STORAGE-FRAGMENTATION-1]]）。
                           # normalized/側で実績のあるquarterly.py::
                           # _classify_period()・normalizer.py::
                           # _ytd_to_quarterly()を再利用。差分計算が
                           # 数学的に無効な加重平均フィールド
                           # （shares_diluted等）はSA候補なし時に欠損を
                           # 許容する。この修正はdata/系統単独の正確性
                           # 向上であり、Layer3（store_v2/）統合の一部
                           # ではない（下記「本節の位置づけ」注記参照）。
                           # 詳細はBACKLOG_DONE.md参照。
  filing_meta.json        # 提出日・CIK・最終確認日等のメタ情報（INPUT-A-018）
  segments/{FYQ}.json     # 対象外（INPUT-A-016、フェーズ1統合スコープ除外）
                           # 【2026-07-23方針転換】当初は正式ASC280セグメントを
                           # 想定していたが、実態は銘柄固有カスタムKPIであり、
                           # 取得方式（生XBRL XML解析）も他フィールドと異なる。
                           # フェーズ1の統合スコープからは除外し、現状の独立実装
                           # （docs/portfolio/tail/data/kpi/{ticker}_layer2.json）
                           # を維持する（投資調査により方針転換）
  filing_text/{accession}.json  # 10-Q本文抽出結果（新規吸収、INPUT-A-017）
```

**本節の位置づけ（2026-08-06追記）**: 本節の保持構造案は2026-07-23
策定時点のものであり、翌日策定の`SEC_EDGAR_LAYER_DESIGN.md`（統合
スキーマ・Layer1〜3アーキテクチャ）により、実際の統合先は
`common/sec_data/store_v2/`（Layer3、`layer3_builder.py`生成）に
確定している。本節の`data/{TICKER}/...`構造は、Layer3移行後も
`annual_*.json`（年次データ、フェーズDリストに切替計画なし）・
診断/補助スクリプト7件の参照先としては存続するが、5本番消費者
（TANUKI VALUATION本体・STONKS SILO・TANUKI TAIL・HypeCore・
stock.htmlフロントエンド）の最終的な参照先ではない。詳細は
`SEC_EDGAR_LAYER_DESIGN.md`8章（移行実装計画）参照。

四半期データへのフォールバック（TANUKI側の`get_net_cash()`が既に持つ
設計）は、正規化ストア自身の標準機能として全サブシステムに開放する
（STONKS SILO等が年次データのみ参照する現状の制約を、ストア側の
API仕様に含める）。

### 2-B. yfinance層（分類A-2・INPUT-A-019〜023）: 更新頻度でサブレイヤーを分離

**設計方針**: yfinanceのデータは性質上、更新頻度が大きく異なる3つの
サブレイヤーに分割して保持する。

1. **日次スナップショット層**（毎営業日更新）: 個別銘柄・指数・ETF・
   商品の価格/出来高（`INPUT-A-019`・`INPUT-A-022`・`INPUT-A-023`）。
   `common/market_data/daily/{SYMBOL}.json`（または時系列DB）に、
   シンボルをキーとして一元管理する。個別銘柄・指数・ETFを区別せず
   同一スキーマで扱う（現状「個別銘柄用」「指数用」「ETF用」が
   別ファイル・別関数で管理されている構造を統合する）。
2. **準静的属性層**（週次〜月次更新で十分）: `.info`辞書由来の
   PER/PEG/PSR/EV_EBITDA/配当利回り/配当性向/インサイダー保有/β/
   セクター/業種等（`INPUT-A-020`）。日次で再取得する必要性が薄いため、
   独立した更新スケジュールで`common/market_data/attributes/{TICKER}.json`
   に保持する。
3. **イベント履歴層**（発生都度追記）: アナリスト格上げ・格下げ履歴
   （`INPUT-A-021`）。`common/market_data/analyst_history/{TICKER}.json`
   に追記型で保持し、月次集計（3ヶ月移動平均等）は参照側（HypeCore等）
   が都度計算する。

S&P500構成銘柄の一括ダウンロード（`INPUT-A-023`、market breadth用）は、
上記1の日次スナップショット層と同じ取得バッチ内で完結させ、個別銘柄の
日次取得と可能な限り1回のAPI呼び出しに統合する。

### 2-C. FRED層（分類A-3・INPUT-A-024〜047）: 系列単位の時系列ストア

**設計方針**: FRED系列は「系列コード」を主キーとする単一の時系列ストア
に保持する。`common/macro_data/series/{SERIES_ID}.csv`（または同等の
時系列形式）に、観測日・値・（該当する場合は）公表日・改定履歴を保持する。

**保存形式・スキーマの確定事項（2026-08-12、実装設計投資調査を受けた
確定）**:
- 保存形式は`common/macro_data/series/{SERIES_ID}.json`（系列ごとの
  JSONファイル、観測日昇順のリスト）に確定する。当初案のCSVではなく
  `common/market_data/`（`daily/{SYMBOL}.json`等）と形式を揃える。
  理由: 2つの新設データ層が別々の保存慣習を持つ状態を避けるため。
- 各エントリのスキーマは2-D節のprovenance標準（`value`/`as_of`/
  `fetched_at`/`source`/`source_detail`/`fallback_used`）をそのまま
  適用する。`source`は`"FRED"`固定、`source_detail`に系列コードを含める。
- 系列単位のメタ情報（`fred_release_id`/`obs_to_release_lag`等、現状
  `INDICATOR_CONFIG`にフラット埋め込み）は`common/macro_data/
  series_meta.json`へ切り出す。

MACRO PULSEの`INDICATOR_CONFIG`が持つ`fred_release_id`/`obs_to_release_lag`
等のメタ情報（サプライズ検知・重複判定に必須）は、系列コードに紐づく
メタデータとしてこのストアの一部に統合する（現状MACRO PULSE内部にのみ
存在するメタ情報を、FRED層全体で共有可能な形にする）。

Market Pulse固有の`VXNCLS`/`DGS3MO`（`INPUT-A-046`/`INPUT-A-047`）も、
MACRO PULSEが使う`T10Y2Y`（`INPUT-A-024`）等と全く同じストア・同じ
スキーマで管理し、「MACRO PULSE用FRED」「Market Pulse用FRED」という
現状のサブシステム別分断を解消する。

### 2-D. Provenance（出所・取得日時）の付与設計

`NAMING_CONVENTIONS.md`規則4（生データを直接転記する項目への出所明示）
を一次データ層自体にも適用する。各データポイントに以下を付随させる:

```json
{
  "value": ...,
  "as_of": "2026-07-23",           // データが表す時点（決算期・観測日等）
  "fetched_at": "2026-07-23T07:15:00+09:00",  // 実際に取得した日時
  "source": "SEC_EDGAR_XBRL",      // SEC_EDGAR_XBRL / YFINANCE / FRED / MANUAL
  "source_detail": "10-Q accession 0000320193-26-000050",  // 具体的な出所
  "fallback_used": false           // フォールバック値を採用した場合true
}
```

これにより、①どの時点のデータか（`as_of`）と②いつ取得したか
（`fetched_at`）を区別できる（現状のMACRO PULSE/Market Pulse間の
非同期実行タイミングのズレのような問題を、後から機械的に検知できる
ようにするための最小限のメタデータ）。四半期フォールバック
（TANUKIのnet_cash計算等）を使った場合は`fallback_used`で明示し、
利用側が「正規のannualデータか、フォールバック値か」を区別できる
ようにする。

分類B（`INPUT-B-001〜003`）・分類C（`INPUT-C-001〜014`）の手動設定
ファイルにも、更新者・更新日時を`_meta`フィールド等で明示することを
推奨する（現状は`docs/portfolio/data/portfolio.json`の`last_updated`
〈2026-08-15、`config/portfolio.json`から統一済み〉のように一部
ファイルのみ独自に持つ慣行であり、統一されていない）。

---

## ステップ3: 取得方法の設計

### 3-A. 取得頻度・タイミングの設計

| 対象 | 頻度 | 想定タイミング | 設計根拠 |
|---|---|---|---|
| SEC EDGAR新規提出監視（submissions API、`INPUT-A-018`） | 日次（平日） | 1日1回、市場クローズ後 | 10-Q/10-Kは不定期提出のため、ポーリングで検知する以外に方法がない。現状TANUKI TAILが平日17:00 JSTで実施している頻度を踏襲すれば十分 |
| SEC EDGAR XBRLファクト取得（新規提出検知時のみ、`INPUT-A-001〜017`） | イベント駆動 | 新規提出検知の都度 | 決算期ごとにしか値が変わらないデータを毎日再取得する必要はない |
| yfinance日次スナップショット（`INPUT-A-019`・`022`・`023`） | 日次（平日） | 市場クローズ後、1回のみ | 現状12ファイル（`INPUT_DATA_AS_IS.md`で実測、既存記載の「13〜14」はF&G Level2 TQQQトレーダー等の対象外プロジェクト混入分を含む数だったと判明。2026-08-07訂正: 当初実測の「11」は`common/sec_data/audit.py`を見落としていた、`[[MARKETDATA-AS-IS-AUDIT-PY-OMITTED-1]]`参照）がそれぞれ独自のタイミングで同じ銘柄の価格を取得しているのを1回に統合する |
| yfinance準静的属性（`INPUT-A-020`） | 週次 | 週次バッチ1回 | PER・β・配当性向等は日次で変動しても実務上の意味が薄い。過度な頻度はAPI呼び出し回数の浪費 |
| yfinanceアナリスト履歴（`INPUT-A-021`） | 週次 | 週次バッチ1回 | 格上げ・格下げは高頻度イベントではない |
| FRED系列（`INPUT-A-024〜047`） | 各系列の公表頻度に整合（日次系列は日次、月次系列は月次） | 系列ごとに設定された`obs_to_release_lag`を考慮した日次ポーリング | 月次公表データを日次ポーリングすること自体は問題ないが（値が変わらない日は差分なしとして処理）、重複排除（`dedupe_new_rows()`相当）は一次データ層側で一元的に行う |
| 取得前提条件（分類B、`INPUT-B-001〜003`） | イベント駆動（手動更新時） | 銘柄追加・CIK変更の都度 | 監視銘柄マスタ・CIKマッピングは頻繁に変わらない。追加・除外の都度、以降のA取得に反映されればよい |
| 導出データの入力（分類C、`INPUT-C-001〜014`） | イベント駆動（admin.html保存時等） | 保存操作の都度 | 定期実行は不要。ただし保存内容のバリデーション（現状ゼロ、既知の問題）は取得方法の設計とは別軸のため本ドキュメントでは扱わない |

### 3-B. 単一共有レイヤーとしてのアクセス設計

全サブシステムが一次データ層（分類A）を参照する際、**個々のサブシステムが
外部APIを直接呼び出すことを禁止し、必ず共有アクセサ経由で取得済みデータを
読む**という設計を基本方針とする。

```
common/
  sec_data/       # 既存。SEC EDGAR正規化ストア（唯一の正、2-A参照、INPUT-A-001〜018）
    fetcher.py    # 外部取得を行う唯一のモジュール
    reader.py     # 全サブシステムが読み取りに使う唯一のモジュール
  market_data/    # 新設。yfinance統合層（2-B参照、INPUT-A-019〜023）
    fetcher.py    # 外部取得を行う唯一のモジュール（日次/週次バッチを内包）
    reader.py     # 全サブシステムが読み取りに使う唯一のモジュール
  macro_data/     # 新設。FRED統合層（2-C参照、INPUT-A-024〜047）
    fetcher.py    # 外部取得を行う唯一のモジュール
    reader.py     # 全サブシステムが読み取りに使う唯一のモジュール
```

**`common/macro_data/`のAPI確定事項（2026-08-12、実装設計投資調査を
受けた確定）**:
- `fetcher.py`: `fetch_series(series_id, start=None)`を外部アクセスの
  唯一の窓口とし、リトライ＋指数バックオフを全系列で統一する（現状は
  `05_main.py::fetch_event_row()`/`fred_release_dates()`/
  `05_import_history.py::_load_ctx_cache()`の3箇所のみ実装、他は
  素朴なtry/exceptという不統一を解消する）。保存前に系列ごとの定義域
  チェック（比率系の範囲外検知、前回値からの桁違い変化検知）を行い、
  `common/macro_data/macro_data_violations_log.json`（0件でも毎回
  書き込む、`common/market_data/market_data_violations_log.json`と
  同型）へ記録する（`EXTRACTION_DESIGN_PRINCIPLES.md`原則3対応）。
- `reader.py`: `get_latest(series_id)`・`get_series(series_id,
  start=None, end=None)`・`get_value_as_of(series_id, date)`を提供する。
  `get_series`の返り値は観測日を含み、消費側（VXN50日MA等の「直近N件」
  ロジック）が期間の連続性を自前で検証できるようにする
  （`EXTRACTION_DESIGN_PRINCIPLES.md`原則1対応）。
- 重複取得3系列（`BAMLH0A0HYM2`・`T10Y2Y`・`VIXCLS`）は、`05_main.py::
  get_financial_context()`・`INDICATOR_CONFIG`の`daily`系列ループ・
  `update_liquidity_csv()`、および`collect_and_send.py`側3関数の
  いずれも独自に`Fred()`を呼ぶのをやめ、全て`reader.py`経由に統一する
  ことで解消する（`TO_BE_FINAL_LIST.md`⑮-final「取得共通化・出力3件
  存続」に対応。出力項目自体は3件とも存続し削除しない）。

**この設計が現状の重複取得を構造的に防止する理由**:
- SEC EDGAR: 現状7系統（`common/sec_data`本体1・EPS Analyzer独自1・
  TANUKI TAIL独自3ファイル・手動運用2）に分散しているアクセスコードを
  `common/sec_data/fetcher.py`1箇所に統合すれば、新規サブシステムが
  「自分で取得する」という選択肢自体を持たなくなる（`reader.py`しか
  importできない設計にする）。
- yfinance: 現状12ファイル（`INPUT_DATA_AS_IS.md`実測値。2026-08-07
  訂正: 当初実測の「11」は`common/sec_data/audit.py`を見落としていた、
  `[[MARKETDATA-AS-IS-AUDIT-PY-OMITTED-1]]`参照）が個別に
  `yf.Ticker()`/`yf.download()`を呼んでいる状態を、
  `common/market_data/fetcher.py`1箇所（＋日次/週次のバッチ実行
  スケジュール）に統合すれば、同一銘柄・同一指数への重複リクエスト
  （現状確認済み: 現在株価2系統、PER/PEG/PSR/EV_EBITDA3系統、
  アナリストコンセンサス2系統、β3系統、`^GSPC`のMarket Pulse内4重取得等）
  が構造的に発生しなくなる。
- FRED: 現状MACRO PULSE内部2箇所＋Market Pulse1箇所で計3箇所から独立に
  取得している`BAMLH0A0HYM2`（`INPUT-A-025`、HYスプレッド）のような
  事例は、`common/macro_data/fetcher.py`1箇所に統合すれば、同一系列
  への複数回リクエスト自体が発生しない設計になる。

**サブシステム固有の後処理は引き続き各サブシステム側で行う**: この
統合はあくまで「取得・保持」の一元化であり、「加工・表示」（例:
Market PulseのHYG/LQD比率による代理判定、MACRO PULSEの実際のスプレッド
値としての利用）はサブシステムごとの目的に応じて個別に行ってよい
（`TO_BE.md`⑮群で既に確認済みの設計思想と整合させる）。分類B・C
（`config/`配下の設定ファイル群）は既に`config/`への集約が概ね実現して
いるため、fetcher/reader分離の対象は主に分類A（外部API由来データ）と
する。

### 3-C. 「唯一の正」と接尾辞規則の適用

一次データ層自体が複数の経路を持つことがないため、`NAMING_CONVENTIONS.md`
規則1（データソース接尾辞）は主に導出データ側（一次データ層を加工した後の
出力フィールド）に適用される。一次データ層のフィールド名自体は、
ソースの性質を示す前置（`sec_`/`yf_`/`fred_`）を持つ形で統一し、
各サブシステムが「唯一の正」を参照していることをコード上も明示する。

---

## 機械的網羅性証明（INPUT_DATA_AS_IS.mdとの突合）

`INPUT_DATA_TOBE.md`（本ファイル）で付番した分類A/B/C全66件のIDが、
`INPUT_DATA_AS_IS.md`側にも同一IDで記録され、過不足がないことを機械的に
確認した（2026-08-12再実行）。

```
grep -oE 'INPUT-[ABC]-[0-9]+' INPUT_DATA_TOBE.md | sort -u   → 66件
grep -oE 'INPUT-[ABC]-[0-9]+' INPUT_DATA_AS_IS.md | sort -u  → 66件
diff <(上記2つの出力)                                         → 差分0件
```

確認の結果、両ファイルのID集合は完全に一致し、差分は0件であった
（再実行時、`INPUT-A-048`が`INPUT_DATA_AS_IS.md`側に未反映のまま
残存していたことが判明したため同ファイルへ追加し解消。詳細は
`INPUT_DATA_AS_IS.md`「完了報告時の参照用サマリー」2026-08-12追記
参照）。分類A49件は全件が現状（AS-IS）のいずれかの取得経路によって
現に取得されていること、分類B3件・分類C14件は全件が現状`config/`配下等に
現存する設定ファイルとして確認されていることを、`INPUT_DATA_AS_IS.md`
「ID対応表」で個別に確認済み（詳細は同ファイル参照）。

**新規設計であり現状に対応が存在しないもの**: `DGS10`（10年国債利回り、
risk_free_rateの動的取得に必要だが現状未取得）は、分類A49件のIDには
含まれていない（現行のFIELD_DEFINITIONS.md 499項目のいずれの計算式も
DGS10を直接参照していないため、機械的網羅性証明の対象外。追加が必要と
判断される場合は分類Aへ次番号（現行49件の次、未採番）として追加する）。

---

## 次ステップへの申し送り

- **2026-07-23、3分類再構成・ID付番完了**: 従来のソース別構成
  （1-A〜1-D）を性質別3分類（A/B/C）に再整理し、全64項目にIDを付番。
  `INPUT_DATA_AS_IS.md`との機械的網羅性証明を実施し差分0件を確認した
- **2026-07-23、`INPUT_DATA_AS_IS.md`との突合完了**: 現状（AS-IS）の実装
  との項目単位の比較を実施済み（比較結果・考慮漏れの判定は
  `INPUT_DATA_AS_IS.md`参照）
- 移行コスト評価・移行手順の設計（何を・どの順序で・どう移行するか）は
  引き続き次ステップで行う（本ドキュメント自体は設計のみに留める）
- `common/market_data/`・`common/macro_data/`という新設レイヤーの命名・
  配置場所は暫定案であり、既存の`common/`配下の他モジュールとの整合は
  次ステップで確認する
- risk_free_rate（TANUKI VALUATION、現状0.043ハードコード）をFRED
  `DGS10`（分類Aへの新規追加候補、未採番）経由に切り替えるかどうかは設計上の
  指摘に留め、実装可否の判断は次ステップに委ねる
- Discoverのconfig二重管理（`admin.html`保存先と`docs/discover/index.html`
  参照先の不一致）は一次データ層の設計とは別軸の問題（手動入力データの
  同期バグ）であり、本ドキュメントでは設計対象に含めていない。ただし
  Portfolio（`INPUT-C-008`、`config/portfolio.json`と`docs/portfolio/
  data/portfolio.json`の重複〈2026-08-15実装完了、`docs/portfolio/
  data/portfolio.json`に統一済み〉）については、同型の同期リスクとして
  分類Cの「唯一の保持場所」設計に含めた
- `common/sec_data`の`raw/`・`normalized/`・`ttm/`統合時にどちらのスキーマ
  を正とするか（特に`normalized/`をAS-IS-071バグの温床として廃止するか、
  修正して存置するか）の決定は、次ステップの移行計画に委ねる
- 分類B・Cの新規追加・削除は、`CHAT_RULES.md`「一次データ層の件数管理」
  ルールに従い、以後の実装依頼時に本ドキュメントのID・総数を都度更新する
