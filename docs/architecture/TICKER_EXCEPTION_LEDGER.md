# 銘柄別の例外の台帳（TICKER EXCEPTION LEDGER）設計書

- 作成: 2026-10-15（BACKLOG `[[TICKER-EXCEPTION-LEDGER-1]]`）
- 状態: 設計のみ。**実装は 2026-10-18〜19 の確認（CLAUDE_CODE_START.md「次回の確認手順」4・6）の後に行う**
- 元にした調査: 2026-10-15 の読み取り調査「銘柄ごとの例外の記録の棚卸し」（件数は 10-15 時点の公開データ。`_freshness.json` は 10-12 02:05 UTC の全銘柄の実行）

---

## 1. 目的

- 銘柄ごとの例外（ある銘柄だけデータが欠ける・代用している・除外している・SEC 側が遅れている等）は、**直す対象ではなく、記録して表示する対象**とする。
- BACKLOG には**仕組みの問題だけ**を残す。「CDNS の 10-Q が companyfacts に入っていない」のような銘柄の事情は台帳に移し、BACKLOG の項目にはしない。
- いまは銘柄別の例外が、parser の検知ログ・`_freshness.json`・latest.json の各欄・Stonks / EPS の各欄・report_consistency_check の WARN・人が書く台帳（`warn_acknowledged.json` 等）・BACKLOG の個別項目に散らばっている。出力先・作り直しか追記か・読み手の有無もばらばらで、読み手が 0 件の出力が 6 種ある（10-15 の棚卸し）。
  これを 1 つの台帳にまとめ、「いまどの銘柄に、どんな値に効く例外があるか」を 1 か所で見られるようにする。

## 2. 載せる範囲（3 段階）

| 段階 | 意味 | 台帳での扱い | 表示 |
|---|---|---|---|
| A | 評価値に影響する（値を変える・除外する・代用する・判定を変える・元データが古い） | 1 銘柄 1 行（kind ごと） | stock.html のバッジ、admin.html、score_watcher の新規発生の通知 |
| B | 注記（値は変えないが、読む人が知っておくべきこと） | 1 銘柄 1 行（kind ごと） | admin.html |
| C | 内部の検知（処理の中で吸収済み、または品質の監視用） | **行は作らず、kind ごとの件数だけ** | admin.html に件数だけ |

### A: 評価値に影響する（10-15 時点）

| kind | 元の出力 | 件数 | 銘柄 |
|---|---|---:|---|
| `latest_period_missing` | TANUKI latest.json `latest_period_missing`（年次で代用・Policy B で LOW→WATCH） | 5 | APP・LOAR・LYFT・RCAT・VZ |
| `fcf_outlier_excluded` | TANUKI latest.json `fcf_outlier.action == "excluded"` | 5 | AVAV・BBAI・COHR・KULR・RDW |
| `fcf_estimation_fallback` | TANUKI latest.json `fcf_estimation.fallback_reason` が空でない | 2 | LOAR・VZ |
| `sector_guard` | TANUKI latest.json `bs_adjustment.sector_guard != "none"`（短期借入金を除く） | 3 | MSCI・SOFI・V |
| `dupont_excluded` | TANUKI latest.json `dupont.excluded` | 9 | ABBV・BKNG・DELL・FICO・MO・MSCI・PM・QBTS・RBRK |
| `rice_error` | TANUKI latest.json `rice.available == false` かつ note が「エラー:」で始まる（計算の例外） | 4 | CIX・ENTG・SPIR・XOM（原因は `core_calculator.py:756` の表示用の print。判定・理論株価には効かず、MATRIX の分類と RICE の表示に効く。`[[RICE-WARNINGS-ALWAYS-TRUE-1]]`） |
| `undetermined` | TANUKI latest.json `tanuki_score == "UNDETERMINED"` | 0 | — |
| `sec_lag` | `_freshness.json` の SEC_LAG（提出はあるが companyfacts に無い＝TTM が古い） | 4 | CDNS・KO・RMBS・V |
| `sec_fetch_failed` | `_freshness.json` の FETCH_FAILED | 0 | — |
| `sec_update_failed` | `_freshness.json` の FAILED（出力を前回の状態に戻した） | 0 | — |
| `fact_override` | 人が書く台帳 `common/sec_data/fact_overrides.json`（値の差し替え） | 5 | GOOGL・COHR・CIX・ONDS・LOAR |
| 手書き | `config/ticker_exceptions_manual.json`（4 章）の level A の行 | — | 移す BACKLOG 7 件のうち値に効くもの（8 章） |

### B: 注記（10-15 時点）

| kind | 元の出力 | 件数 | 銘柄 |
|---|---|---:|---|
| `growth_sanity_warning` | TANUKI latest.json `growth_sanity.warnings` が空でない | 15 | ALAB・ASTS・BBAI・BKNG・BROS・CELH・ELF・IONQ・JOBY・KULR・LLY・ONDS・RDW・SITM・TER |
| `fcf_margin_note` | TANUKI latest.json `growth_sanity.fcf_margin_note` | 15 | AAPL・ABBV・AMAT・AMZN・FLYW・GOOGL・KLAC・KO・LRCX・META・MRVL・MSFT・SNPS・TASK・VST |
| `fcf_outlier_flagged` | TANUKI latest.json `fcf_outlier.action == "flagged"`（除外はしない） | 63 | （省略。棚卸しの検知 68 から除外 5 を引いた数） |
| `sti_quarterly_missing` | TANUKI latest.json `bs_adjustment.sti_quarterly_missing` | 38 | （省略）。値への影響の有無は実装時に確かめ、効くなら A に上げる |
| `cash_missing` | TANUKI latest.json `bs_adjustment.cash_missing` | 1 | CPRT。同上 |
| `segment_xbrl_status` | TANUKI latest.json `segment_xbrl_status` | 7 | APP・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA |
| `stonks_fields_missing` | Stonks results.json `financial_vectors.data_quality.fields_missing` | 2 | ASTS・LYFT |
| `stonks_runway_sti_missing` | Stonks results.json `runway.sti_quarterly_missing` | 2 | RDW・RXRX |
| `stonks_financial_stale` | Stonks results.json `financial_as_of.stale` | 0 | — |
| `eps_ai_analysis_error` | EPS quarterly.json の最新四半期 `ai_analysis.health == "Error"` | 20 | ADBE・AMAT・APP・ASTS・AVAV・BBAI・CRM・CSGP・ELF・ESTC・HQY・INTU・JNJ・JOBY・LITE・MRVL・NVDA・RXRX・TASK・VRT |
| `eps_special_flags` | EPS quarterly.json `special_flags`（いずれかの四半期） | 38 | （省略） |
| `cik_history` | 人が書く台帳 `common/sec_data/cik_history.json`（旧 CIK の結合） | 4 | MRVL・GOOGL・DELL・XOM |
| `warn_acknowledged` | 人が書く台帳 `config/warn_acknowledged.json`（確認済みの WARN） | 68 行 | 銘柄ごとにまとめて 1 行 |
| 手書き | `config/ticker_exceptions_manual.json` の level B の行 | — | 8 章 |

### C: 内部の検知（件数だけ。10-15 時点）

| kind | 元の出力 | 銘柄数（件数） |
|---|---|---|
| `fy_collision` | `common/sec_data/data/{T}/fy_collision_log.json` | 8（194） |
| `fy_tag_mismatch` | `.../fy_tag_mismatch_log.json` | 12（336） |
| `fye_boundary_collision` | `.../fye_boundary_collision_log.json` | 1（2） |
| `bs_identity_violation` | `.../bs_identity_violations_log.json` | 48（153） |
| `spac_shell_detection` | `.../spac_shell_detection_log.json` | 17（18） |
| `share_unit_fix` | `.../share_unit_fix_log.json`（parser が単位を補正済み） | 15（170） |
| `fixed_registry` | `common/sec_data/fixed_registry.json`（凍結） | 42 |
| `rpo_not_applied` | TANUKI latest.json `rpo_adjustment.exclusion_reason`（RPO/売上 < 0.3 の規則で不適用） | 12 |
| `eps_maturity_alert` | EPS quarterly.json `maturity_monitor.alert` | 39 |
| `market_data_violation` | `common/market_data/{SYM}/market_data_violations_log.json` のいずれかの一覧が空でない | 91 / 593 |
| `rice_warning` | TANUKI latest.json `rice.warnings` | 96（全銘柄。`[[RICE-WARNINGS-ALWAYS-TRUE-1]]` を直すまでは件数にも意味がない） |

C に入れたものも、`share_unit_fix`・`fixed_registry` のように値を変えた結果の記録はある。parser の中で正しく吸収済みで、評価値の読み手が意識する必要がないので C にした。吸収できていない（値が誤っている）とわかったら、手書きの A で記録する。

## 3. 作り方

### 3.1 方針

- **既存の書き手は変えない。** parser・update.py・TANUKI・Stonks・EPS・HypeCore の出力の形・書き方はそのまま。
- **集約の処理を 1 本新設する**（`common/ticker_exceptions/build_ledger.py` を想定）。読むのは保存済みの出力ファイルだけで、元データからその場で計算しない（CHAT_RULES.md 事例 39。その場で計算すると、反映の時期が書き手とずれる）。
- 出力: `docs/value-monitor/tanuki_valuation/data/ticker_exceptions.json`。TANUKI VALUATION の既存の commit の `git add docs/value-monitor/tanuki_valuation/data/` に含まれる。同じフォルダを一覧する処理は、どれもフォルダだけに絞っているので影響しない（6 章）。
- **first_seen は前回の台帳から引き継ぐ。** 同じ (ticker, kind) が前回の台帳にあれば first_seen をそのまま使い、last_seen を今回にする。
- **検知されなくなったら解消（status = resolved）**とし、resolved_at を記録する。resolved_at から **30 日後に行を削除**する。再び検知されたら open に戻し、first_seen は新しく付ける（解消前の履歴は残さない）。
- **自動で検知できない銘柄別の問題は、手書きの入力ファイル `config/ticker_exceptions_manual.json` に書く**（BACKLOG から移す 7 件など）。手書きの行は集約処理が台帳に写すだけで、解消は手書きのファイルから消す（または `status: resolved` を書く）ことで行う。

### 3.2 集約処理が読むもの

| 読むもの | 段階 | 読み方 |
|---|---|---|
| `docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | A・B・C | 保存済みのファイル（同じ実行で TANUKI が書いた後） |
| `common/sec_data/data/_freshness.json` | A | 保存済みのファイル |
| `common/sec_data/data/{T}/*_log.json`（parser の 6 種） | C | 保存済みのファイル |
| `docs/value-monitor/stonks-silo/data/results.json` | B | 保存済みのファイル |
| `docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | B・C | 保存済みのファイル |
| `common/market_data/{SYM}/market_data_violations_log.json` | C | 保存済みのファイル |
| `common/sec_data/fact_overrides.json`・`cik_history.json`・`fixed_registry.json`・`config/warn_acknowledged.json` | A・B・C | 保存済みのファイル（人が書く台帳） |
| `config/ticker_exceptions_manual.json` | A・B | 保存済みのファイル（手書き） |
| 前回の `ticker_exceptions.json` | — | first_seen の引き継ぎ・解消の判定 |

対象の銘柄は `get_registrable_tickers()` に絞る（retired の銘柄の行は作らない）。

## 4. 項目

台帳 `ticker_exceptions.json` の 1 行（A・B）:

| 項目 | 内容 |
|---|---|
| `ticker` | 銘柄 |
| `kind` | 2 章の kind（手書きは自由な名前。`manual:` で始める） |
| `level` | `A` または `B` |
| `source` | 検知した処理（例: `tanuki_valuation/latest.json:latest_period_missing`、`sec_data/_freshness.json`、`manual`） |
| `first_seen` | 最初に検知した日時（UTC、ISO 8601） |
| `last_seen` | 最後に検知した日時 |
| `status` | `open`・`resolved` |
| `note` | 元の出力の注記（例: 「最新期の設備投資が未取得（最新期は年次の値で代用）」、SEC_LAG なら accn と提出日） |
| `backlog_ref` | 関係する BACKLOG の ID（仕組みの問題の項目。無ければ空） |
| `resolved_at` | 解消した日時（resolved のときだけ） |

台帳の全体: `generated_at`・`rows`（上の行の一覧）・`counts_c`（C の kind ごとの銘柄数・件数）・`sources`（読んだ各ファイルの更新日時、読めなかったファイル）。

手書きの入力 `config/ticker_exceptions_manual.json` の 1 行: `ticker`・`kind`・`level`・`note`・`backlog_ref`・`since`（日付）・任意の `status`。

## 5. 表示と通知

- **stock.html（TANUKI VALUATION の個別ページ）**: その銘柄の A の open の行をバッジで出す（kind の日本語名と note のツールチップ）。B は出さない（今は既存の欄がそれぞれ注記を出している）。
- **admin.html**: タブ「⚠ 銘柄別の例外」を足す。A・B の一覧（絞り込み: level・kind・status）と C の件数の表。
- **score_watcher**: 台帳で **A の open が新しく出た行（first_seen が今回）だけ**を Discord に通知する。B・C と解消は通知しない。既存の判定変化の通知とは別の欄にする。
  - 台帳が無い、または台帳の `generated_at` が今日（JST）でない場合は、**台帳を使った通知だけを省き**、判定変化などほかの通知は今までどおり送る（集約の段が失敗した回に、前回の台帳で通知しない）。
  - 前回の台帳が無い初回の実行（first_seen の引き継ぎ元が無い）では、全行が「新しく出た行」になるため、**新規発生の通知を出さない**。集約の処理は台帳に `initial_run: true` を書き、score_watcher はそれを見て省く。
- **実行の位置（2026-10-15 決定）**: TANUKI_VALUATION_Update.yml の「**Gate → 台帳の集約 → score_watcher → commit**」。
  - score_watcher（台帳を読んで通知する）と commit（台帳を含める）より前に置く必要があるため。
  - 集約の段は `continue-on-error: true`。**失敗しても後ろの段（score_watcher・commit）を止めず**、ワークフローの成否も変えない。失敗した回は前回の台帳がそのまま残る（score_watcher は上の条件で台帳の通知だけを省く）。

## 6. 影響表

「読み方」は、その処理が台帳（または台帳の入力）をどう読むか。ファイル＝保存済みのファイル、その場＝元データから計算。

| 処理 | 何を読むか | 読み方 | 今の状態 | 対応 |
|---|---|---|---|---|
| 集約の処理（新設） | 3.2 の各ファイル | ファイル | 無い | 新設 |
| TANUKI_VALUATION_Update.yml | — | — | Gate → score_watcher → commit | 集約の段を Gate の後・score_watcher の前に足す（continue-on-error）。commit の `git add` は既存のフォルダに含まれるので変更不要 |
| score_watcher.py | latest.json・poc.json | ファイル | 判定・乖離率・ファンダ・stage の変化を通知 | 台帳の A の新規を読んで別の欄で通知する。台帳が無い・`generated_at` が今日（JST）でない・初回（`initial_run`）なら台帳の通知だけ省く |
| stock.html | latest.json | ファイル | A の各欄は一部だけ表示（latest_period_missing・fcf_outlier 等。cash_missing・sti_quarterly_missing・segment_xbrl_status・method_change は表示 0 件） | 台帳を読んで A のバッジ |
| admin.html | 各設定 | ファイル | タブ 6 つ（銘柄管理・銘柄設定・β・FCF 転換率・RPO・実行） | タブを 1 つ足す |
| TANUKI pipeline.py | latest.json を書く | その場 | — | 不要（書き手は変えない） |
| tanuki_score/index.html・daily_pick.py | latest.json | ファイル | — | 不要（台帳を読まない） |
| tanuki_valuation/data を一覧する処理 | フォルダの一覧 | — | `growth_model_audit.py:27-30`（`d.is_dir()`）、`score_verifier.py:113-116`・`tail_dcf_bridge.py:329-332`（`os.path.isdir` かつ TANUKI の銘柄）、`score_watcher.py:49`（`tickers.json`） | 不要。フォルダだけに絞っており、直下のファイルを銘柄と見なさない（根拠: `grep -rn -E "listdir\(\|iterdir\(" src/value src/tail common/system_health.py` の結果のうち、このフォルダを一覧するのは上の 3 か所だけ） |
| Score_Verifier.yml | `data/*/score_history.json` | ファイル | — | 不要（直下のファイルを含まないパターン） |
| TANUKI_VALUATION_Update.yml の Summary | `data/*/latest.json` | ファイル | — | 不要（同上） |
| report_consistency_check.py | 各出力 | ファイル | CHECK-60・63 などが一部の例外を WARN で出す | 不要（台帳とは別に残す。台帳は表示、CHECK はゲートと監視） |
| System Health | `_freshness.json` 等 | ファイル | [M] などが監視 | 不要（同上） |
| parser・update.py・Stonks・EPS・HypeCore | — | — | 例外の各出力の書き手 | 不要（書き手は変えない） |
| BACKLOG.md | — | — | 銘柄別の項目が 7 件 | 実装の後、7 件の銘柄の事情を手書きの入力に移し、BACKLOG には仕組みの問題だけ残す（8 章） |

## 7. 読み手の一覧と読み方（台帳の入力になる出力）

10-15 の棚卸しの結果。「台帳の後」は、この設計で台帳が新しく読む（表示する）もの。

| 出力 | 今の読み手 | 読み方 | 画面 | 台帳の後 |
|---|---|---|---|---|
| `_freshness.json` | CHECK-60、System Health [M]、update.py | ファイル | 出ない | A として stock.html に出る |
| parser のログ 4 種（fy_collision 等） | report_consistency_check.py:2282-2342 | ファイル | 出ない | C の件数 |
| `spac_shell_detection_log`・`share_unit_fix_log` | 0 件 | — | 出ない | C の件数 |
| `latest_period_missing` | TANUKI pipeline（その場）、CHECK-63（ファイル） | 両方 | stock.html・tanuki_score | A |
| `fcf_outlier` | TANUKI（その場） | その場 | stock.html・tanuki_score・admin.html | A（除外）・B（注記） |
| `bs_adjustment.cash_missing`・`sti_quarterly_missing` | pipeline・CHECK | その場・ファイル | 出ない | B（値への影響を確かめて A に上げる候補） |
| `segment_xbrl_status` | pipeline だけ | その場 | 出ない | B |
| `method_change` | score_watcher | ファイル | 出ない | 台帳の対象外（銘柄の例外ではなく実行の印） |
| HypeCore `low_base_effect` | 0 件 | — | 出ない | 対象外（月ごとの指標の性質で、銘柄の例外ではない） |
| Stonks `fields_missing` | 0 件 | — | 出ない | B |
| EPS `maturity_monitor.alert` | 0 件 | — | 出ない | C の件数 |
| `market_data_violations_log` | 0 件 | — | 出ない | C の件数 |

## 8. BACKLOG から移す候補（7 件）

実装の後に、銘柄の事情を `config/ticker_exceptions_manual.json` に移す。仕組みの問題が残るものは、BACKLOG の項目を仕組みの問題だけに書き直す。

| BACKLOG | 銘柄 | 想定の level | 備考 |
|---|---|---|---|
| `[[SEC-COMPANYFACTS-API-LAG-1]]` | CDNS・KO・RMBS・V | A | `_freshness.json` の SEC_LAG で自動検知（手書き不要）。BACKLOG は SEC 側の未反映への対応方針だけになる |
| `[[SBC-TAG-STOCKOPTIONPLAN-1]]` | HEI・TDY | A | 株式報酬が取れていない |
| `[[ROTCE-PREFERRED-CONVERTIBLE-TAG-1]]` | GOOGL | A | TCE から優先株が引かれていない |
| `[[ROTCE-TCE-COMPONENT-TAG-GAPS-1]]` | KO・AVAV・PLTR・V | A | TCE の成分が欠ける・過小 |
| `[[LAYER3-DUPLICATE-QUARTER-ONE-DAY-1]]` | AVAV | B | 純利益の同じ四半期が 2 件。TTM の 6 点目（平均に入れない点）が欠けになるだけ（BACKLOG の優先度の欄） |
| `[[LAYER3-CUSTOM-TAG-MIGRATION-1]]` | CRWV・ADBE・LRCX | B | 2026-10-14 時点で評価への影響なし。効果が出たら A に上げる |
| `[[STONKS-TANUKI-BADGE-404-1]]` | RKLB・ZS | B | STONKS SILO が TANUKI の latest.json を読みに行き 404 |

仕組みの問題で、今は特定の銘柄に出ているもの（`[[EPS-AI-ANALYSIS-ERROR-SILENT-1]]`・`[[MARKETDATA-FUTURES-BACKFILL-ROWS-1]]`・`[[MARKETDATA-FUTURES-ROLL-1]]`・`[[CIK-REORG-DETECTION-1]]`）は BACKLOG に残す。銘柄の部分（EPS の Error の 20 銘柄など）は自動検知の B として台帳に出る。

## 9. 実装の順序（10-18〜19 の確認の後）

1. 集約の処理と台帳の形（A・B・C、first_seen の引き継ぎ・解消・30 日後の削除）とテスト
2. 手書きの入力ファイルと、BACKLOG の 7 件からの移し替え
3. TANUKI_VALUATION_Update.yml に段を足す（continue-on-error）
4. stock.html のバッジ・admin.html のタブ
5. score_watcher の A の新規の通知
6. `[[RICE-WARNINGS-ALWAYS-TRUE-1]]` を直してから、`rice_warning` を C から外すか見直す
