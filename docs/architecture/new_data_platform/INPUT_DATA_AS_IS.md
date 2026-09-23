# INPUT_DATA_AS_IS.md — 一次データ層の現状（AS-IS）

作成日: 2026-07-23
更新日: 2026-09-13（`[[MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1]]`
FTSDケース対応。`FTSD`（`INPUT-A-049`）がFRED API上に実在しない無効な
系列コードと判明したため、05_main.pyのフォールバック先を`WDTGAL`
（`INPUT-A-050`として新規採番）へ置換。1-E節のFRED表を更新、下記
「機械的網羅性証明」の対象は67件・差分0件へ変化。詳細はBACKLOG_DONE.md
「2026-09-13（完了）」参照）
更新日: 2026-08-12（`common/macro_data/`実装設計確定に伴い、1-E節へ
`FTSD`（`INPUT-A-049`）を追加。機械的網羅性証明を再実行した結果、
`INPUT-A-048`（税務・一過性項目タグ群52種）が`INPUT_DATA_TOBE.md`側
にのみ存在し本ファイルへの反映漏れ（2026-07-24時点で発生していた既存
の乖離）になっていたことが判明したため、あわせて追加・解消。再実行後
は両ファイルとも66件・差分0件を確認。分類A件数表記を47件→49件に
統一。詳細はBACKLOG.md`[[MACRODATA-LAYER-CONSTRUCTION-1]]`参照）
更新日: 2026-07-23（`INPUT_DATA_TOBE.md`の3分類再構成・ID付番に対応し、
本ファイルにもID対応表〈1-E〉を新設。両ファイル間のID機械的網羅性証明を
実施）
出発点: `INPUT_DATA_TOBE.md`と同一構成（ステップ1: 棚卸し、ステップ2:
保持方法、ステップ3: 取得方法）。実コード（`.github/workflows/*.yml`・
`common/sec_data/`・各サブシステムのfetcher/pipeline）を直接確認して作成。

## 本ドキュメントの位置づけ

`INPUT_DATA_TOBE.md`（白紙設計）と項目単位で突き合わせるため、現状の
実装を同じ粒度・同じ表形式で記録する。実装（コード修正）は行っていない。
本ドキュメント自体は現状の**観測記録**であり、是正提案は含まない
（是正提案・考慮漏れの判定は本ファイルの末尾および`INPUT_DATA_TOBE.md`
側に記録する）。

---

## ステップ1: 現状取得されている一次データの棚卸し

### 1-A. SEC EDGAR（実測7経路、既存ドキュメントの「8〜9経路」表記を検証）

`OUTPUT_ITEMS_INVENTORY.md`「外部データ取得経路」セクションは「計9経路」
としているが、実際にコードを直接確認したところ、物理的に独立した
取得メカニズムは以下**7個**であり、同ドキュメントの「D-F」行は「Cと
同一ファイル群の呼称違い」と自ら明記している通りCと重複計上されている
ことを確認した（本ドキュメントでは重複を排したC1〜C3の3ファイルとして
数える）。

| 経路 | 実装ファイル | 取得先エンドポイント | 用途 |
|---|---|---|---|
| A | `common/sec_data/fetcher.py` | SEC EDGAR Company Facts API相当 | TANUKI VALUATION/HypeCore/STONKS SILO/Discover/TANUKI TAIL(一部)が共用 |
| B | `src/value/adjusted_eps_analyzer/extract_key_facts.py` | 独自XBRL抽出 | EPS Analyzerのみ |
| C1 | `src/tail/sec_ctrl_fetcher.py` | `www.sec.gov/Archives/edgar/data/.../{primaryDocument}`（10-Q Item4本文） | TANUKI TAIL内部統制監視 |
| C2 | `src/tail/edgar_rss_monitor.py` | `www.sec.gov/cgi-bin/browse-edgar?output=atom`＋`submissions/`＋`company_tickers.json` | TANUKI TAIL新規提出監視 |
| C3 | `src/tail/xbrl_segment_fetcher.py` | `company_tickers.json`＋`submissions/`＋`.../{doc}_htm.xml` | TANUKI TAILセグメントKPI抽出 |
| G | `common/sec_data/segment_fetcher.py` | SEC EDGAR（GitHub Actions未使用） | 手動実行専任（`TANUKI_Segment_AI.yml`はworkflow_dispatchのみ、スケジュールなし） |
| H | `config/cik_lookup.csv`／`cik_lookup_result.json`生成処理 | `company_tickers.json` | CIKルックアップ（`TANUKI_CIK_Lookup.yml`、workflow_dispatchのみ） |

C1〜C3は3ファイルとも`common/sec_data`を一切importせず、独立にrequestsで
SEC EDGARへ直接アクセスしている（User-Agent文字列も3ファイル間で統一
されていない）。

**取得ファクト種別**（`INPUT_DATA_TOBE.md`1-Aの表と対応）: 売上高・純利益・
営業利益・売上総利益・OCF・CapEx・R&D・S&M・SBC・現金・短期投資・
長期/短期有利子負債・希薄化後株式数は経路A（`common/sec_data`）が
一次情報源。EPS Analyzerの経路Bも同じXBRLタグ群（純利益・希薄化後株式数
中心）を独自に再抽出している。セグメント別KPIは経路C3、内部統制テキストは
経路C1、新規提出監視は経路C2が担う。

### 1-B. yfinance（実測12ファイル、既存ドキュメントの「13〜14ファイル」を検証）

`import yfinance`/`from yfinance`を直接grepしたところ、`src/`配下で
ヒットする14ファイルのうち4ファイル（`src/subport/fg_level2/trader.py`・
`src/subport/day_trade/backtest.py`・`src/subport/fg_level2_extended.py`・
`src/subport/fg_level2_backtest.py`）は**F&G Level2 TQQQトレーダー
（別プロジェクト、`FIELD_DEFINITIONS.md`499項目のいずれのサブシステムにも
属さない）**であり、499項目には無関係。加えてSTONKS SILOのyfinance利用
（`discover/stonks-silo/src/valuation_fetcher.py`）は`src/`配下ではなく
`discover/`配下にあるため通常のgrepでは見落としやすい。499項目に実際に
関係する箇所を正確に数え直すと**12ファイル**であった。

**訂正（2026-08-07、`common/market_data/`新設事前調査で発見・
`[[MARKETDATA-AS-IS-AUDIT-PY-OMITTED-1]]`）**: 当初の調査は「`src/`配下
でヒットする14ファイル」を起点としており、`common/`配下は探索範囲外
だった。`common/sec_data/audit.py`（β乖離監査、`audit_beta_drift()`）が
`import yfinance`をローカルスコープで2箇所行っており、
`SEC_Data_Audit.yml`（`workflow_run`で"SEC Data Update"完了後に連鎖
実行）から呼ばれる本番稼働中の診断ツールだが、当初の11ファイルには
含まれていなかった。12番目のファイルとして下表に追加し、総数を
11→12件に訂正する。

| サブシステム | ファイル | 取得内容 |
|---|---|---|
| TANUKI VALUATION | `src/value/tanuki_valuation/pipeline.py` | 現在株価・出来高等 |
| TANUKI VALUATION | `src/value/tanuki_valuation/data_fetcher.py` | `.info`属性（PER/PEG/PSR/EV_EBITDA/配当/インサイダー等）、diluted shares |
| TANUKI VALUATION | `src/value/tanuki_valuation/beta_fetcher.py` | β（5年月次） |
| TANUKI VALUATION | `src/value/tanuki_valuation/score_verifier.py` | 検証用の株価再取得 |
| HypeCore | `src/value/hypecore/hypecore.py` | 株価履歴・出来高・PSR・アナリスト格上げ格下げ履歴・空売り比率 |
| STONKS SILO | `discover/stonks-silo/src/valuation_fetcher.py` | 時価総額・株価 |
| Discover | `src/discover/collect.py` | 株価（カタリスト判定用の参考） |
| Market Pulse | `src/market/market_pulse/collect_and_send.py` | 主要9銘柄・NYSE Composite・VIX9D・HYG/LQD・グロース/バリューETF等 |
| Market Pulse | `src/market/market_pulse/breadth_calculator.py` | S&P500構成銘柄一括ダウンロード |
| Market Pulse | `src/market/market_pulse/backfill_tech_pulse.py` | Tech Pulse履歴バックフィル用 |
| EPS Analyzer | `src/value/adjusted_eps_analyzer/extract_key_facts.py` | フォールバックのみ（通常はSEC優先） |
| SEC Data品質監査 | `common/sec_data/audit.py`（`audit_beta_drift()`） | β（`beta_config.json`との乖離監査、`--check-beta`時） |

同一情報の重複取得（既知・`OUTPUT_ITEMS_INVENTORY.md`記載分を実ファイルで
再確認）: 現在株価（TANUKI vs STONKS SILO）、PER/PEG/PSR/EV_EBITDA
（TANUKI・HypeCore・STONKS SILOの3系統）、アナリストコンセンサス
（TANUKI vs HypeCore）、β（日次/月次/監査トリガー時の3経路）、
`^GSPC`（Market Pulse内部で4箇所独立取得）。

### 1-C. FRED（実測2サブシステム、既存ドキュメントの「3サブシステムに拡大確定」を検証）

`OUTPUT_ITEMS_INVENTORY.md`は「FRED取得経路（3サブシステムに拡大確定）」
と見出しを付けているが、本文で列挙されているのはMACRO PULSEとMarket
Pulseの2つのみであり、3つ目として言及されている「TANUKI VALUATIONの
risk_free_rate」は**「FRED非参照（ハードコード0.043）を再確認済み」**、
すなわち**FRED を実際には取得していないことを確認した**という趣旨の
記載である。実際にFREDを取得しているサブシステムは**2つ**であり、
見出しの「3」は検証観点での言及を実際の取得経路数と混同した表記の
不整合と判断する。

| サブシステム | 実装ファイル | 取得系列（実コード確認） |
|---|---|---|
| MACRO PULSE | `src/market/macro_pulse/05_main.py` | `INDICATOR_CONFIG`辞書内12系列（`GACDFSA066MSFRBPHI`/`CFNAI`/`PAYEMS`/`IC4WSA`/`MICH`/`T5YIE`/`UMCSENT`/`PERMIT`/`SAHMCURRENT`/`T10Y2Y`/`BAMLH0A0HYM2`/`VIXCLS`）＋流動性カード用8系列（`SP500`/`DGS1`/`DFEDTARU`/`DFEDTARL`/`FEDFUNDS`/`WALCL`/`WTREGEN`/`RRPONTSYD`/`WRBWFRBL`/`M2SL`、`BAMLH0A0HYM2`は上記と重複） |
| Market Pulse | `src/market/market_pulse/collect_and_send.py`／`backfill_tech_pulse.py` | `VXNCLS`（VXN、`^IRX`障害を機にFRED切替）、`BAMLH0A0HYM2`（`checks.hy_spread`、⑮群の3箇所目）、`DGS3MO`（`asset_flow.short_bond`、`^IRX`のフォールバック先） |

**未取得として確認したもの**: `DGS10`（10年国債利回り）はいずれの
サブシステムからも取得されていない。TANUKI VALUATIONの`risk_free_rate`
（CAPM構成要素）は`calculate_wacc()`のデフォルト引数`0.043`という
コード内ハードコード定数であり、FRED含むいかなる外部APIからも都度
取得していない。

### 1-D. 手動入力データ（実測、`config/`以外の分散も含めて棚卸し）

`config/`ディレクトリの全ファイルを直接列挙し、`INPUT_DATA_TOBE.md`
作成時に把握していなかったものを含めて棚卸しした。IDは
`INPUT_DATA_TOBE.md`の3分類再構成（分類B: 取得前提条件／分類C: 導出
データの入力）に対応する（詳細は1-E参照）。

| ID | ファイル | 内容 | `INPUT_DATA_TOBE.md`での扱い |
|---|---|---|---|
| `INPUT-C-001` | `config/segment_config.json` | セグメント別加重成長率 | 記載済み |
| `INPUT-C-002` | `config/growth_options_config.json` | 成長オプション設定 | 記載済み |
| `INPUT-C-003` | `config/maturity_config.json` | DCF成熟プロファイル | 記載済み |
| `INPUT-C-004` | `config/rpo_config.json` | RPO調整設定 | 記載済み |
| `INPUT-C-005` | `config/beta_config.json` | β値オーバーライド | 記載済み |
| `INPUT-C-006` | `config/discover_config.json` | 銘柄別テーマ・区分 | 記載済み |
| `INPUT-C-007` | `config/theme_config.json` | テーママスタ | 記載済み |
| `INPUT-C-008` | `config/portfolio.json` | 保有株数・取得単価 | 記載済み（ただし保持場所が`docs/portfolio/data/portfolio.json`と重複、2-Dで詳述）。**【2026-08-15追記】`[[PORTFOLIO-CONFIG-DUP-1]]`実装完了、`config/portfolio.json`は削除・`docs/portfolio/data/portfolio.json`に統一済み** |
| `INPUT-C-014` | `config/adjustment_items.json` | EPS Analyzerの調整項目カテゴリ・XBRLタグ定義（`version: "2026-04"`） | 記載済み（当初考慮漏れ→追加済み） |
| `INPUT-C-011` | `config/prompts.yaml` | Grok/AI分析プロンプトテンプレート（`adjustment_analysis`等） | 記載済み（当初考慮漏れ→追加済み、重要） |
| `INPUT-C-013` | `config/sectors.yaml` | セクター/業種のキーワードマッピング | 記載済み（当初考慮漏れ→追加済み。実コード確認の結果、`sector_classifier_v2.py`経由でEPS Analyzerの調整項目除外に使われる分類C項目と判明） |
| `INPUT-C-012` | `config/split_history.yaml` | 株式分割の遡及補正用手動記録（比率・効力発生日） | 記載済み（当初考慮漏れ→追加済み。実コード確認の結果、`apply_split_adjustments()`が事後適用する分類C項目と判明） |
| `INPUT-B-001` | `config/monitor_tickers.yaml` | 監視銘柄マスタリスト | 記載済み（当初考慮漏れ→追加済み、重要） |
| `INPUT-B-002` | `config/cik_lookup.csv` | Ticker→CIKマッピング | 記載済み（当初考慮漏れ→追加済み） |
| `INPUT-B-003` | `config/cik_lookup_result.json` | CIKルックアップ結果キャッシュ | 記載済み（当初考慮漏れ→追加済み） |
| — | `config/warn_acknowledged.json` | `report_consistency_check.py`のWARN確認済み台帳 | 対象外と判定（下記参照、IDなし） |
| — | `config/workflow_dependencies.json` | ワークフロー依存関係定義（System Health用） | 対象外と判定（下記参照、IDなし） |
| `INPUT-C-010` | `src/value/tanuki_valuation/fcf_conversion_config.json` | Damodaran業種別FCF変換率・ticker override | 記載済み（当初考慮漏れ→追加済み、`config/`外に配置されている点も特記）。**【2026-08-15追記】`[[FCFCONFIG-LOCATION-1]]`実装完了、`config/fcf_conversion_config.json`へ移動済み** |
| `INPUT-C-009` | `docs/portfolio/tail/data/tail_kpi_map.json` | TANUKI TAIL KPI設定（AI提案＋人手確定） | 記載済み（ただし`config/`ではなく`docs/`配下、下記2-D参照）。**【2026-08-15追記】`[[TAILKPI-CONFIG-LOCATION-1]]`実装完了、`config/tail_kpi_map.json`へ移動済み** |

### 1-E. ID対応表（`INPUT_DATA_TOBE.md`分類A/B/C全67件との対応）

`INPUT_DATA_TOBE.md`が付番した分類A（一次データ本体、50件。2026-09-13
追加の`INPUT-A-050`＝`WDTGAL`含む）・分類B（取得前提条件、3件）・
分類C（導出データの入力、14件）の全IDについて、現状（AS-IS）のどの
取得経路・保持場所が対応するかを確認した。

#### 分類A: 一次データ本体（50件）— SEC EDGAR（`INPUT-A-001`〜`018`）

| ID | 現状の取得経路 | 確認状況 |
|---|---|---|
| `INPUT-A-001`（売上高）、`INPUT-A-002`（純利益）、`INPUT-A-003`（営業利益）、`INPUT-A-004`（売上総利益）、`INPUT-A-005`（OCF） | 経路A（`common/sec_data`）が一次情報源。経路B（EPS Analyzer）も純利益を独立再抽出 | 確認済み |
| `INPUT-A-006`（CapEx） | 経路A。符号正規化は`parser.py`側で実施（`normalized/`側は未正規化、AS-IS-071参照） | 確認済み（既知バグあり） |
| `INPUT-A-007`（ファイナンスリース関連）、`INPUT-A-008`（R&D）、`INPUT-A-009`（S&M）、`INPUT-A-010`（SBC） | 経路A | 確認済み |
| `INPUT-A-011`（現金）、`INPUT-A-012`（短期投資）、`INPUT-A-013`（長期有利子負債）、`INPUT-A-014`（短期有利子負債） | 経路A | 確認済み |
| `INPUT-A-015`（希薄化後株式数） | 経路A・経路B（EPS Analyzer独自抽出）の両方 | 確認済み（重複取得） |
| `INPUT-A-016`（セグメント別売上・KPI） | 経路C3（`xbrl_segment_fetcher.py`） | 確認済み |
| `INPUT-A-017`（内部統制関連テキスト） | 経路C1（`sec_ctrl_fetcher.py`） | 確認済み |
| `INPUT-A-018`（直近提出日・提出書類一覧） | 経路C2（`edgar_rss_monitor.py`）・経路H（CIKルックアップ）が使用 | 確認済み |
| `INPUT-A-048`（税務・一過性項目・銀行業向け詳細タグ群52種） | 経路B（EPS Analyzer`extract_key_facts.py`）が独自にSEC EDGAR company_facts APIから取得。`common/sec_data/data/{TICKER}/company_facts.json`（経路A、既存）に全量含まれているため新規API取得は不要 | 確認済み（`INPUT_DATA_TOBE.md`には2026-07-24付で追加済みだったが本ファイルへの反映漏れがあり、2026-08-12の機械的網羅性証明再実行で発覚・今回追加） |

#### 分類A: 一次データ本体（50件）— yfinance（`INPUT-A-019`〜`023`）

| ID | 現状の取得経路 | 確認状況 |
|---|---|---|
| `INPUT-A-019`（価格・出来高履歴） | TANUKI(`pipeline.py`)・HypeCore・STONKS SILO・Market Pulseが個別取得 | 確認済み（重複取得） |
| `INPUT-A-020`（`.info`属性、β含む） | TANUKI(`data_fetcher.py`)・HypeCore・Beta_Config_Update.ymlが個別取得 | 確認済み（重複取得、β3経路） |
| `INPUT-A-021`（アナリスト格上げ・格下げ履歴） | HypeCore(`hypecore.py`) | 確認済み |
| `INPUT-A-022`（指数・ETF・商品） | Market Pulse(`collect_and_send.py`) | 確認済み（`^GSPC`は内部4重取得） |
| `INPUT-A-023`（S&P500構成銘柄一括） | Market Pulse(`breadth_calculator.py`) | 確認済み |

#### 分類A: 一次データ本体（50件）— FRED（`INPUT-A-024`〜`047`）

| ID | 系列コード | 現状の取得経路 |
|---|---|---|
| `INPUT-A-024` | `T10Y2Y` | MACRO PULSE |
| `INPUT-A-025` | `BAMLH0A0HYM2` | MACRO PULSE(内部2箇所)＋Market Pulse(1箇所)＝3箇所重複取得（既知） |
| `INPUT-A-026` | `GACDFSA066MSFRBPHI` | MACRO PULSE |
| `INPUT-A-027` | `CFNAI` | MACRO PULSE |
| `INPUT-A-028` | `IC4WSA` | MACRO PULSE |
| `INPUT-A-029` | `MICH` | MACRO PULSE |
| `INPUT-A-030` | `T5YIE` | MACRO PULSE |
| `INPUT-A-031` | `UMCSENT` | MACRO PULSE |
| `INPUT-A-032` | `PERMIT` | MACRO PULSE |
| `INPUT-A-033` | `SAHMCURRENT` | MACRO PULSE |
| `INPUT-A-034` | `PAYEMS` | MACRO PULSE |
| `INPUT-A-035` | `VIXCLS` | MACRO PULSE |
| `INPUT-A-036` | `SP500` | MACRO PULSE |
| `INPUT-A-037` | `DGS1` | MACRO PULSE |
| `INPUT-A-038` | `DFEDTARU` | MACRO PULSE |
| `INPUT-A-039` | `DFEDTARL` | MACRO PULSE |
| `INPUT-A-040` | `FEDFUNDS` | MACRO PULSE |
| `INPUT-A-041` | `WALCL` | MACRO PULSE |
| `INPUT-A-042` | `WTREGEN` | MACRO PULSE |
| `INPUT-A-043` | `RRPONTSYD` | MACRO PULSE |
| `INPUT-A-044` | `WRBWFRBL` | MACRO PULSE |
| `INPUT-A-045` | `M2SL` | MACRO PULSE |
| `INPUT-A-046` | `VXNCLS` | Market Pulse |
| `INPUT-A-047` | `DGS3MO` | Market Pulse |
| `INPUT-A-049` | `FTSD`（**2026-09-13判明: FRED API上に実在しない無効な系列コード**、`WDTGAL`へ置換済み） | MACRO PULSE（`05_main.py::update_liquidity_csv()`、`WTREGEN`フォールバック時のみ。現行実装は`WDTGAL`を参照、本行は経緯記録として残置） |
| `INPUT-A-050` | `WDTGAL` | MACRO PULSE（`05_main.py::update_liquidity_csv()`、`WTREGEN`フォールバック時のみ。`FTSD`置換先、2026-09-13追加） |

全24系列＋`FTSD`（`INPUT-A-049`）・`WDTGAL`（`INPUT-A-050`）とも現状
いずれかのサブシステムから確認済み。`DGS10`（risk_free_rate用、
`INPUT_DATA_TOBE.md`が分類Aへの新規追加候補〈未採番〉とした系列）は
現状いずれのサブシステムからも未取得——これは分類A50件のIDには
含まれないため、機械的網羅性証明（両ファイルのID集合一致）の対象外で
あり、証明結果には影響しない。

#### 分類B: 取得前提条件（3件）

| ID | ファイル | 現状の存在確認 |
|---|---|---|
| `INPUT-B-001` | `config/monitor_tickers.yaml` | 存在確認済み（手動編集） |
| `INPUT-B-002` | `config/cik_lookup.csv` | 存在確認済み（`TANUKI_CIK_Lookup.yml`で半自動生成） |
| `INPUT-B-003` | `config/cik_lookup_result.json` | 存在確認済み（同上の実行結果キャッシュ） |

#### 分類C: 導出データの入力（14件）

| ID | ファイル | 現状の存在確認 |
|---|---|---|
| `INPUT-C-001`（`segment_config.json`）、`INPUT-C-002`（`growth_options_config.json`）、`INPUT-C-003`（`maturity_config.json`）、`INPUT-C-004`（`rpo_config.json`）、`INPUT-C-005`（`beta_config.json`）、`INPUT-C-006`（`discover_config.json`）、`INPUT-C-007`（`theme_config.json`） | いずれも`config/`配下（1-D表参照） | 存在確認済み |
| `INPUT-C-008` | `config/portfolio.json` | 存在確認済み。ただし`docs/portfolio/data/portfolio.json`と重複（2-D参照）。**【2026-08-15追記】実装完了、`docs/portfolio/data/portfolio.json`に統一済み** |
| `INPUT-C-009` | `docs/portfolio/tail/data/tail_kpi_map.json` | 存在確認済み。`config/`外に配置（2-D参照）。**【2026-08-15追記】実装完了、`config/tail_kpi_map.json`へ移動済み** |
| `INPUT-C-010` | `src/value/tanuki_valuation/fcf_conversion_config.json` | 存在確認済み。`config/`外に配置（2-D参照）。**【2026-08-15追記】実装完了、`config/fcf_conversion_config.json`へ移動済み** |
| `INPUT-C-011` | `config/prompts.yaml` | 存在確認済み |
| `INPUT-C-012` | `config/split_history.yaml` | 存在確認済み |
| `INPUT-C-013` | `config/sectors.yaml` | 存在確認済み |
| `INPUT-C-014` | `config/adjustment_items.json` | 存在確認済み |

**機械的網羅性証明（実行結果、2026-08-12再実行）**:
```
grep -oE 'INPUT-[ABC]-[0-9]+' INPUT_DATA_TOBE.md | sort -u   → 66件
grep -oE 'INPUT-[ABC]-[0-9]+' INPUT_DATA_AS_IS.md | sort -u  → 66件
diff <(上記2つの出力)                                         → 差分0件
```
`FTSD`（`INPUT-A-049`）追加に伴う再実行時、`INPUT-A-048`（税務・一過性
項目タグ群52種）が`INPUT_DATA_TOBE.md`側にのみ存在し本ファイルへの
反映漏れ（2026-07-24時点で発生していた既存の乖離、今回の再実行で発覚）
していたことが判明したため、本ファイル1-E（SEC EDGAR表）へ追加し
解消した上で、両ファイルのID集合が完全に一致し差分0件であることを
実際に確認済み。分類A49件は全件が
現状いずれかの取得経路で実際に取得されていること、分類B3件・分類C14件は
全件が現状のファイルとして存在することを、上記1-Eの表で個別に確認した。

---

## ステップ2: 現状の保持方法（実ディレクトリ構造）

### 2-A. SEC EDGAR

`common/sec_data/`配下だけで**5種類の異なる保持形式**が並存している
ことを確認した（`INPUT_DATA_TOBE.md`が提案した「annual/quarterly/
filing_meta/segments/filing_text」という単一スキーマより実際には
細分化されている）。

```
common/sec_data/data/{TICKER}/annual_{YEAR}.json       # 正規化済み通期
common/sec_data/data/{TICKER}/quarterly_{FYQ}.json     # 正規化済み四半期
common/sec_data/data/{TICKER}/submissions.json         # 提出メタ情報
common/sec_data/raw/{TICKER}_quarterly_raw.json        # 正規化前の生XBRL
common/sec_data/normalized/{TICKER}_quarterly_normalized.json  # 別スキーマの正規化四半期
common/sec_data/ttm/{TICKER}_ttm_series.json           # TTM系列
```

**重要な発見**: `normalized/{TICKER}_quarterly_normalized.json`は
`data/{TICKER}/quarterly_{FYQ}.json`とは**別ファイル・別スキーマ**であり、
`FIELD_DEFINITIONS.md`AS-IS-071（stock.htmlのCF分析セクション独自FCF計算、
CapEx符号未処理バグ）が参照しているのはこの`normalized/`側である。
つまり同一の四半期XBRLデータが、正規化ロジックが独立した最低2つの
ファイル系統に分岐して保持されている。

EPS Analyzer・TANUKI TAILは、上記いずれとも異なる独自の保持先を持つ:

```
docs/value-monitor/adjusted_eps_analyzer/data/{TICKER}/annual.json
docs/value-monitor/adjusted_eps_analyzer/data/{TICKER}/quarterly.json
docs/value-monitor/adjusted_eps_analyzer/data/{TICKER}/ttm.json

docs/portfolio/tail/data/rss_state.json                # 新規提出監視状態
config/tail_kpi_map.json                                # KPI設定（2026-08-15、docs/portfolio/tail/data/から移動）
docs/portfolio/tail/data/ctrl/{TICKER}/{FYQ}.json      # 内部統制個別
docs/portfolio/tail/data/ctrl/{TICKER}/index.json      # 内部統制インデックス
docs/portfolio/tail/data/ctrl/{TICKER}/latest.json     # 内部統制最新
docs/portfolio/tail/data/kpi/{TICKER}/...              # セグメントKPI(layer2)
docs/portfolio/tail/data/kpi_proposals/...             # AI提案（未確定）
```

### 2-B. yfinance

サブシステムごとに完全に独立したファイル・スキーマで保持されている
（`INPUT_DATA_TOBE.md`が提案した頻度別3層構造は現状は存在しない）。

```
docs/value-monitor/tanuki_valuation/data/{TICKER}/latest.json  # components.*にPER/PEG/PSR/β等を格納
docs/value-monitor/hypecore/data/{TICKER}_poc.json             # monthly[]配列に価格等を格納
docs/value-monitor/stonks-silo/data/results.json               # 全銘柄分を1ファイルに集約
docs/market-monitor/market-pulse/data/market_data.json         # indicators{}に主要指標を格納
docs/market-monitor/market-pulse/data/market_data.csv          # 上記のCSVフラット化（列欠落あり、既知）
docs/market-monitor/market-pulse/data/breadth_data.json        # S&P500構成銘柄集計結果
docs/market-monitor/market-pulse/data/sp500_tickers.json       # 構成銘柄リストキャッシュ
```

いずれも「日次データ」と「準静的な`.info`属性」を区別せず同一ファイル
（`latest.json`/`market_data.json`等）に混在させて保持している。
準静的属性（β・配当性向等）も日次バッチの一部として同じタイミングで
再取得・上書きされる設計であり、`INPUT_DATA_TOBE.md`が提案した
「更新頻度別サブレイヤー分離」は現状は行われていない。

### 2-C. FRED

```
docs/market-monitor/macro-pulse/data/05_events.csv             # 指標別発表イベント
docs/market-monitor/macro-pulse/data/05_liquidity.csv          # 流動性カード用系列
docs/market-monitor/macro-pulse/data/05_fed_context.csv        # FF金利・regime文脈
docs/market-monitor/macro-pulse/data/05_indicator_schedule.csv # 次回発表予定
docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv    # 週次AI解説
docs/market-monitor/macro-pulse/data/05_meta.json              # メタ情報

docs/market-monitor/market-pulse/data/market_data.json         # tech_pulse.components.vxn_latex、asset_flow.short_bond、buy_checklist.checks.hy_spread等に混在
```

MACRO PULSE側はFRED専用のCSV群を持つのに対し、Market Pulse側はFRED由来の
値（VXN・HYスプレッド・DGS3MO）を専用ファイルに分離せず、yfinance由来の
値と同じ`market_data.json`に混在させて保持している。「FRED由来のデータを
1箇所にまとめる」という`INPUT_DATA_TOBE.md`2-Cの設計は、現状は全く
存在しない。

### 2-D. 手動入力データ

`config/`ディレクトリへの集約が概ね徹底されているが、**2つの例外**を
確認した（**【2026-08-15追記】両方とも`[[FCFCONFIG-LOCATION-1]]`・
`[[TAILKPI-CONFIG-LOCATION-1]]`実装完了により`config/`へ移動済み**）:

1. **`fcf_conversion_config.json`**は`config/`ではなく
   `src/value/tanuki_valuation/`直下に配置されている（TANUKI VALUATION
   固有のロジックファイルと同じディレクトリに手動設定ファイルが混在）。
2. **`tail_kpi_map.json`**は`config/`ではなく`docs/portfolio/tail/data/`
   配下（TANUKI TAILの生成データと同じディレクトリ）に配置されている。

また、`config/portfolio.json`と`docs/portfolio/data/portfolio.json`は
**バイト完全一致の重複ファイル**であることを確認した（`diff`コマンドで
確認済み）。両ファイルを同期する自動処理は見当たらず（`config/`側を
参照するPythonコードも見つからない）、Discoverの`discover_config.json`/
`theme_config.json`二重管理（既知バグ、`FIELD_DEFINITIONS.md`記載）と
**同型の手動コピー依存パターン**が、Portfolioの保有データについても
存在する可能性が高い。

---

## ステップ3: 現状の取得頻度・タイミング（`.github/workflows/*.yml`実測）

| ワークフロー | スケジュール(UTC) | JST換算 | 対象データ |
|---|---|---|---|
| `SEC_Data_Update.yml` | 毎週日曜 12:00 | 月曜21:00 | SEC EDGAR経路A（共通パイプライン） |
| `Adjusted_Eps_Analyzer_update.yml` | 毎週月曜 10:07 | 月曜19:07 | SEC EDGAR経路B |
| `TANUKI_TAIL_SEC_Ctrl.yml` | 毎週月曜 01:00 | 月曜10:00 | SEC EDGAR経路C1（10-Q提出後を想定した設計） |
| `TANUKI_TAIL_RSS_Monitor.yml` | 平日 08:00 | 平日17:00 | SEC EDGAR経路C2 |
| `TANUKI_TAIL_KPI_Update.yml` | 毎週日曜 23:00 | 月曜08:00 | SEC EDGAR経路C3 |
| `TANUKI_Segment_AI.yml` | なし（workflow_dispatchのみ） | — | SEC EDGAR経路G |
| `TANUKI_CIK_Lookup.yml` | なし（workflow_dispatchのみ） | — | SEC EDGAR経路H |
| `TANUKI_VALUATION_Update.yml` | 平日 14:05 | 平日23:05 | yfinance（TANUKI VALUATION4ファイル）＋SEC EDGAR経路A読取 |
| `HypeCore_Update.yml` | 毎週日曜 13:08 | 日曜22:08（SEC Data Updateの1時間後を意図） | yfinance（HypeCore） |
| `Stonks_Silo_Update.yml` | 平日 15:05 | 翌0:05 | yfinance（STONKS SILO） |
| `Discover_Update.yml` | 毎日 22:03 | 毎日7:03 | yfinance（Discover）＋Grok |
| `Catalyst_Update.yml` | 毎週日曜 14:30 | 日曜23:30（HypeCore Updateの1時間後を意図） | Grok（カタリスト生成、HypeCore対象銘柄が前提） |
| `Market_Pulse_Update.yml` | 平日 21:35 | 平日翌6:35 | yfinance＋FRED（Market Pulse） |
| `MACRO_PULSE_Update.yml` | 毎日22:15／毎日13:03／土曜22:07／土曜22:11 | 毎日7:15／毎日22:03／土曜7:07／土曜7:11 | FRED（MACRO PULSE、4種類のcronが並存） |
| `Beta_Config_Update.yml` | 毎月第1日曜 23:00 | 月初週月曜8:00 | yfinance（β、月次） |
| `TANUKI_Score_Update.yml` | TANUKI VALUATION完了後（`workflow_run`）＋土日23:30独立実行 | — | 各サブシステムの成果物を集約 |
| `TANUKI_TAIL_Position_Write.yml` | なし（workflow_dispatchのみ） | — | 手動 |
| `TANUKI_TAIL_Satellite_Monitor.yml`（**2026-09-23削除済み**、以下は削除前の記録） | 平日(月-木)23:00＋平日08:00 | — | 補助監視 |
| `Score_Verifier.yml` | 毎日00:00 | 毎日9:00 | yfinance再取得（検証用） |
| `System_Health.yml` | 毎日23:30 | 毎日8:30 | ワークフロー依存関係チェック（`workflow_dependencies.json`参照） |
| `SEC_Data_Audit.yml` | なし（`workflow_run`トリガー、SEC_Data_Update完了後） | — | 監査 |

**実行タイミングの非同期性（既知・実測で再確認）**:
- FRED HYスプレッド: Market Pulse(JST翌6:35頃起動)がMACRO PULSE
  (JST7:15頃起動)より約40分早く同一系列を独立取得
- SEC EDGAR経路C2/C3: `TANUKI_TAIL_RSS_Monitor.yml`(平日17:00)と
  `TANUKI_TAIL_KPI_Update.yml`(月曜08:00)は独立スケジュールのため、
  月曜のみ両方が実行され得る
- HypeCore(日曜22:08)はSEC_Data_Update(月曜21:00起動、実行完了は
  週によって前後）の「1時間後」を意図した設計コメントがあるが、
  実際にはSEC_Data_Updateの方が**曜日をまたいで後**（月曜起動）に
  実行されており、意図した順序関係が成立していない週がある

---

## AS-ISとTO-BEの比較結果

### A. TO-BEにあってAS-ISにないもの（新規設計・現状は未実装）

| 項目 | TO-BEでの設計 | AS-ISの現状 |
|---|---|---|
| `common/market_data/`（yfinance統合層） | 頻度別3層構造の新設レイヤー | 存在しない。12ファイルがそれぞれ独立管理 |
| `common/macro_data/`（FRED統合層） | 系列単位の時系列ストア | 存在しない。MACRO PULSE専用CSV群とMarket Pulseの混在JSONに分断 |
| risk_free_rateのFRED `DGS10`取得 | 一次データ層の管理対象として提案 | 未実装。`0.043`ハードコードのまま、`DGS10`はどのサブシステムからも未取得 |
| 全データポイントへのprovenanceメタデータ（`as_of`/`fetched_at`/`source`/`fallback_used`） | 標準スキーマとして提案 | 現状は`net_debt_period`（TANUKI）のような個別実装が部分的に存在するのみで、統一スキーマはない |
| fetcher/reader分離による直接アクセス禁止 | 設計原則として提案 | 現状は各サブシステムが自由に`yf.Ticker()`等を直接呼んでおり、アクセス制御は存在しない |

### B. AS-ISにあってTO-BEにないもの（考慮漏れの疑いが強い項目）

| 項目 | AS-ISの実態 | 考慮漏れの内容 |
|---|---|---|
| `config/prompts.yaml` | Grok/AIプロンプトテンプレート集約ファイル | TO-BEの「手動入力データ」棚卸しから完全に漏れていた。カタリスト予測・TANUKI TAIL Stage2シナリオ等、392件の導出データのうち多数がAI生成であり、その生成プロンプト自体が一次データ層の一部を構成するという視点が欠落していた |
| `config/monitor_tickers.yaml` | 監視銘柄マスタリスト | 「そもそもどの銘柄を対象にするか」という最も基礎的な一次データが棚卸しから漏れていた |
| `config/split_history.yaml` | 株式分割遡及補正の手動記録 | 希薄化後株式数の正規化に直接影響する重要な手動データだが未記載 |
| `config/sectors.yaml` | セクター/業種キーワードマッピング | β・成長率のセクターデフォルト判定に使われる基礎データが未記載 |
| `config/adjustment_items.json` | EPS Analyzerの調整項目カテゴリ定義 | EPS調整ロジックの根幹となる手動定義が未記載 |
| `config/cik_lookup.csv`／`cik_lookup_result.json` | Ticker→CIKマッピング | SEC EDGAR取得の前提となる基礎データが未記載 |
| `common/sec_data/raw/`・`normalized/`・`ttm/`という3系統の並存 | SEC EDGARが単一の正規化ストアではなく既に4〜5系統に分岐している実態 | TO-BEの2-Aは「単一ストアに一本化する」設計を示したが、統合対象が`data/annual・quarterly`だけでなく`raw/normalized/ttm`も含めた最低5系統に及ぶことの明示が甘かった |
| Portfolio `config/portfolio.json`と`docs/portfolio/data/portfolio.json`の二重保持 | バイト完全一致の重複ファイル、同期処理不在 | TO-BEは手動入力データの保持先を単一パスとして扱っていたが、Portfolioについては実際には2箇所に重複保持されており、Discoverと同型の同期リスクがある |

### C. 両方にあるが粒度・扱いが異なるもの

| 項目 | TO-BEの扱い | AS-ISの実態 | 統合時の懸念 |
|---|---|---|---|
| SEC EDGARの正規化ストア | 「銘柄×決算期」の単一スキーマ（`annual`/`quarterly`/`filing_meta`/`segments`/`filing_text`） | 実際は`data/{TICKER}/annual_*.json`・`quarterly_*.json`・`submissions.json`・`raw/*_quarterly_raw.json`・`normalized/*_quarterly_normalized.json`・`ttm/*_ttm_series.json`の6ファイル系統に分岐 | `normalized/`は`data/quarterly_*.json`と別スキーマで、stock.htmlのCF分析セクション（AS-IS-071の符号バグの温床）が直接参照している。単純に1ファイルへ統合すると、どちらのスキーマを正とするかで既存の消費コードに影響が出る |
| SEC EDGAR取得経路数 | （設計時点では現状分析なし） | 実測7経路（既存ドキュメントの「8〜9経路」は数え方の重複を含む） | 数値自体は設計に影響しないが、次ステップの移行計画で「何を統合対象とするか」のスコープ確定に必要 |
| yfinanceファイル数 | （設計時点では現状分析なし） | 実測12ファイル（既存ドキュメントの「13〜14ファイル」はF&G Level2 TQQQトレーダー等の対象外プロジェクトを含めた数と判明。2026-08-07訂正: 当初の11ファイルは`common/sec_data/audit.py`を見落としていた、`[[MARKETDATA-AS-IS-AUDIT-PY-OMITTED-1]]`参照） | 統合スコープを12ファイルに正しく絞る必要がある |
| FRED取得サブシステム数 | 「2サブシステム」という前提で設計 | 実測2サブシステム（既存ドキュメントの見出し「3」は検証結果の記述と数値の混同） | TO-BE設計自体は結果的に正しかったが、参照元ドキュメントの見出し表記の不整合は解消しておく必要がある |
| 手動入力データの保持先 | `config/`配下に統一される前提 | `fcf_conversion_config.json`（`src/value/tanuki_valuation/`）・`tail_kpi_map.json`（`docs/portfolio/tail/data/`）は`config/`外 | TO-BEが「配置場所」まで設計するなら、この2ファイルの扱い（`config/`への移動を推奨するか、現状維持を許容するか）を明示する必要がある |

---

## 考慮漏れの判定結果

以下、B節「AS-ISにあってTO-BEにないもの」それぞれについて、TO-BEへの
正式追加が必要か、意図的対象外とすべきかを判定した。判定結果は
`INPUT_DATA_TOBE.md`側に反映済み（追記箇所は同ファイル参照）。

| 項目 | 判定 | 理由 |
|---|---|---|
| `config/prompts.yaml` | **TO-BEに追加すべき考慮漏れ** | AI生成コンテンツ（カタリスト予測・TANUKI TAIL Stage2等、392件の導出データの相当数）の再現性・品質はプロンプトの管理次第であり、一次データ層の設計対象から外す理由がない |
| `config/monitor_tickers.yaml` | **TO-BEに追加すべき考慮漏れ** | 全ての一次データ取得の前提となる「対象銘柄」を決める最上流のデータであり、欠落は設計上の重大な抜け |
| `config/split_history.yaml` | **TO-BEに追加すべき考慮漏れ** | 希薄化後株式数という複数の計算式が依存する基礎データの正規化に必須 |
| `config/sectors.yaml` | **TO-BEに追加すべき考慮漏れ** | β・成長率のフォールバック判定に使われる基礎データ |
| `config/adjustment_items.json` | **TO-BEに追加すべき考慮漏れ** | EPS Analyzerの調整ロジックが依存する手動定義データ |
| `config/cik_lookup.csv`／`cik_lookup_result.json` | **TO-BEに追加すべき考慮漏れ** | SEC EDGAR取得（1-A）の前提データであり、1-Aの一部として扱うべき |
| `common/sec_data`内`raw/`・`normalized/`・`ttm/`の並存 | **TO-BEの記述を是正（統合スコープの明確化）** | 「単一ストアへの一本化」という設計方針自体は維持しつつ、統合対象に`raw/`・`normalized/`・`ttm/`を含めることを明示する |
| Portfolio `config/portfolio.json`との二重保持 | **TO-BEに追加すべき考慮漏れ** | Discoverの既知の二重管理バグと同型のリスクがあり、一次データ層の設計として「唯一の保持場所」を明示すべき |
| `config/warn_acknowledged.json` | **意図的に対象外** | 品質ゲート（`report_consistency_check.py`）の確認済み状態を記録する運用状態データであり、499項目のいずれの計算にも入力されない。5分類上は「システム設定データ」に相当し、一次データ層の対象外 |
| `config/workflow_dependencies.json` | **意図的に対象外** | `System_Health.yml`が参照するワークフロー依存関係の定義であり、499項目の計算には一切使われない。同じく「システム設定データ」に相当し対象外 |

---

## 完了報告時の参照用サマリー

- SEC EDGAR実測経路数: **7**（既存記載の「8〜9」は数え方の重複含む）
- yfinance実測ファイル数: **12**（既存記載の「13〜14」は対象外プロジェクト混入含む。2026-08-07訂正: 当初の実測「11」は`common/sec_data/audit.py`を見落としていた、`[[MARKETDATA-AS-IS-AUDIT-PY-OMITTED-1]]`参照）
- FRED実測サブシステム数: **2**（既存記載の見出し「3」は表記の不整合）
- 考慮漏れとしてTO-BEに追加した項目: 8件（`config/prompts.yaml`・
  `monitor_tickers.yaml`・`split_history.yaml`・`sectors.yaml`・
  `adjustment_items.json`・`cik_lookup.csv`／`cik_lookup_result.json`・
  SEC EDGAR統合スコープの明確化・Portfolio二重保持問題）
- 意図的に対象外とした項目: 2件（`warn_acknowledged.json`・
  `workflow_dependencies.json`、いずれもシステム設定データであり
  一次データ層の対象外）
- **2026-07-23追記（3分類再構成後の機械的網羅性証明）**: `INPUT_DATA_TOBE.md`
  が付番した分類A（一次データ本体、47件）・分類B（取得前提条件、3件）・
  分類C（導出データの入力、14件）、合計64件のIDについて、本ファイル
  （1-E）にも同一IDを付番し、`grep -oE 'INPUT-[ABC]-[0-9]+'`による
  両ファイルのID集合の実際の`diff`を実行した結果、**両ファイルとも
  64件・差分0件**であることを確認した（実行結果は本ファイル1-E末尾に
  記載）。分類A47件は全件が現状いずれかの取得経路で実際に取得されて
  いること、分類B3件・分類C14件は全件が現状ファイルとして存在する
  ことを個別に確認済み
- **2026-08-12追記（`common/macro_data/`実装設計確定に伴う機械的網羅性
  証明の再実行）**: `FTSD`（`INPUT-A-049`）を`INPUT_DATA_TOBE.md`・本
  ファイルの両方へ追加した上で再実行したところ、**`INPUT-A-048`
  （税務・一過性項目タグ群52種）が`INPUT_DATA_TOBE.md`側にのみ存在し
  本ファイルへの反映漏れになっていた**（2026-07-24時点で発生していた
  既存の乖離、今回の再実行で初めて発覚）。本ファイル1-E（SEC EDGAR表）
  へ追加して解消した上で再度`diff`を実行し、**両ファイルとも66件・
  差分0件**であることを確認した（実行結果は本ファイル1-E末尾に記載）。
  分類A49件は全件が現状いずれかの取得経路で実際に取得されていることを
  再確認済み
