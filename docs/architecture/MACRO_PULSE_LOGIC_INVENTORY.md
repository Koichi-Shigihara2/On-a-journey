# MACRO PULSE データ適切性の確認とロジック棚卸し

作成日: 2026-10-03（指示書 M-1、読み取り専用の調査）。対象は`docs/market-monitor/macro-pulse/index.html`の
全48要素（ID・依存関係は`SYSTEM_MAP.md`「MACRO PULSE 全画面要素（MAC-01〜MAC-48）」と共通）。
実ブラウザでの突き合わせは`browser_checks/check_macro_pulse.py`（MAC-01〜MAC-48・導出D-01〜D-08・説明N-01〜N-12）。

本番のコード・データ・ワークフローは変更していない。不具合は修正せず、BACKLOG.mdに登録した。
「気になる点」は観察のみで、修正案は書かない。時刻はことわりがなければUTC。

---

## STEP 0: 現状確認

- worktree `../On-a-journey-macro`（`feature/macro-pulse-audit`、origin/kaihatsu `5080ff9fca`から分岐）。clean。
- ゲート3種: pytest 1835 passed / audit.py 正常91・警告8（NGなし） / report_consistency_check.py --fail-on-ng NG=0（WARN 124）。
  venvはメインの作業フォルダのものを読み取りで使った（worktreeにvenvは無い）。gitignore対象のデータの欠けによる失敗はなかった。

### 関連する既存項目（「MACRO」「macro_data」「MACRO_PULSE」「RECESSION」「FRED」でgrep）

BACKLOG.md・IDEAS_AND_WATCH.mdにMACRO PULSEのアクティブ項目は無い（BACKLOG.mdの該当行は過去の更新履歴の文中のみ）。
BACKLOG_DONE.mdの該当（見出し）:

| ID | 状態 | 要旨 |
|---|---|---|
| MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1 | 完了 2026-09-19 | 系列単位の取得失敗をviolations_logで区別できない → `fetch_status`・系列単位try/except・Check L・FTSD撤去 |
| MACRO-PULSE-STALENESS-DISCLOSURE-GAP-1 | 完了 2026-09-10 | CFNAI・Building Permitsの鮮度注記の欠如 → `idxLatestAsOf()`拡張、8指標カードのtooltipに「観測日」、AIプロンプトに観測日 |
| MACRODATA-SCHEDULED-SILENT-GAP-CSCICP-USALOL-1 | 完了 | 旧系列のscheduled行の削除 |
| MACRODATA-FULL-HISTORY-DAILY-REFETCH-1 | 完了 | 日次cronの取得を直近400日に限定 |
| MACRO-TOOLTIP-THRESH-LABEL-MISMATCH-1 | 完了 | tooltipの閾値表記と計算の境界の不一致（5指標） |
| MACRO-THRESHOLD-INCONSISTENCY-1 | 完了 | 閾値の不一致・重複判定（YCの閾値3セットは用途別で意図的と明文化） |
| MACRO-STYLE-FCF-ZERO-TRUTHY-EXCLUDE-1 | 完了 | （MACRO PULSE外）Moat ScoreのFCF=0除外 |
| MACRODATA-LAYER-CONSTRUCTION-1 | 完了 | `common/macro_data/`新設 |
| MACRO-TRUTHY-ZERO-BUG-1 | 完了 2026-08-26 | 履歴バックフィルのゼロ値欠落（扱い済み、蒸し返さない） |
| RECESSION-SCORE-TRIPLE-CALC-1 | 完了 2026-08-26 | 3計算式の併存・境界25/30の不一致 |
| MACRO-PULSE-3M-FORECAST-SNAPSHOT-MISMATCH-1 | 完了 | ゲージとAIコメンタリーのスコア一致（扱い済み、蒸し返さない） |
| MACRO-PULSE-ZONE-25-STALE-1 | 完了 | 境界25→30 |
| MACRODATA-IMPORT-HISTORY-CONFIG-DRIFT-1 ほか MACRODATA系6件・FRED-HYSPREAD-TRIPLE-FETCH-1 | 完了 | FRED取得の重複・台帳の整理 |
| MACRO-NFP-HIST-1・MACRO-NFP-1・DESIGN-13 | 完了 | NFPの前月比化・サプライズ検知 |
| MACRO-BUG-1（本文中の記録、2026-06-20） | 完了 | 過去時点の再構築に`updated_at`を使い先読みを除外 |
| MACRO-COMPUTE-DUP-1（本文中の記録） | 完了 | 本日=ステップ関数・過去=lerp（意図的設計、蒸し返さない） |

- MACRO-PULSE-STALENESS-DISCLOSURE-GAP-1・MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1は、いずれも完了済みで、
  前提が崩れた証拠は無い。ただし前者で追加した「観測日」tooltipは、Michigan・Building Permits・Initial Claimsでは
  観測日ではなく発表予定日の枠の日付を出している（後述N-09。新しい発見として扱う。完了項目はBACKLOG_DONE.mdに
  あり、本指示で変更してよいファイルに含まれないため、追記はせずSTEP 6で別項目として登録する）。
- MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1は2026-09-26に完了・kaihatsuに統合済み。MACRO PULSEは
  `common/market_data/daily/`を読まないため、この件の影響を受けない。
- 扱い済みとして蒸し返さないもの: lerp補間（MACRO-COMPUTE-DUP-1）、ゲージとAIコメンタリーのスコア一致、
  MACRO-TRUTHY-ZERO-BUG-1・HOLLOW-RALLY-DEAD-1。前提が崩れている証拠は見つからなかった
  （ゲージ=AI最新行=27、本日のライブ計算も27。Hollow Rallyのsp500列は最新行まで埋まっている）。

---

## STEP 1: 画面要素の洗い出し

48要素（表示コンポーネント単位。50以下）。一覧と(a)表示コンポーネント→(b)導出関数→(c)生データソースは
`SYSTEM_MAP.md`「MACRO PULSE 全画面要素（MAC-01〜MAC-48）」に記載した。

### Market Pulse・Market Data Dailyとの共有
- データ: FRED `BAMLH0A0HYM2`（MAC-12・16・33・41・43。Market Pulseのクレジット判定も`common.macro_data.reader`で読む）。
  `Macro_Data_Update.yml`の1回の取得結果を両画面が使う。
- 部品: `docs/common/site-nav.js`・`info-tooltip.js`・`glossary.json`・`site-theme.css`。
- Market Data Daily（`common/market_data/daily/`）・yfinanceは使わない（S&P500はFRED `SP500`、取得できない時だけstooq.com）。
  先物も使わない（1Y EXPECTED FFはDGS1。ZQ=Fは廃止済み）。

### 同じ概念を別の経路で計算している箇所
| 概念 | 経路1 | 経路2 | 備考 |
|---|---|---|---|
| 本日のスコア | `05_main.py::_compute_current_score()`（週1回、`wk`に保存。ゲージはこれを表示） | `index.html::computeCurrentScore()`（ライブ、`wk`未読込時と8指標カード） | 式は同じ（ステップ関数＋Philly・Claimsのトレンド補正）。実測で一致（27=27） |
| 過去のスコア | `computeScoreAsOf()`（lerp・`updated_at`で先読み除外） | `_compute_score_change()`（ステップ関数・先読み除外なし） | 比較バーの「先週比」とAIカードの「週±」が別の計算（MM-07） |
| 10Y-2Yの閾値 | ティッカー・ヘルスバー（−0.2/0.5） | スコア（−0.5/0/0.5） | 意図的（MACRO-THRESHOLD-INCONSISTENCY-1） |
| 他7指標の閾値 | ヘルスバー`L2_CFG` | スコアのステップ関数 | HY 4.0/6.5 vs 3.5/6.0、Claims 215K/245K vs 215K/300K、Philly ±5 vs 5/−10、Michigan 90/65 vs 90/60 等。根拠の記載なし（MM-08） |
| REGIME | REGIMEバー（`record_date`で並べ替えた最新行） | ステルスのLAYER 1（ファイル末尾の行） | 今は同じ行（並びが日付順のため） |
| 前週比・変化 | フロントの流動性カード（7日前の行） | Hollow Rally（直前の行）・連続減少（直前の行） | 同じ「前週比」の文言で間隔が違う（不具合B-04、STEP 6でBACKLOGに登録） |
