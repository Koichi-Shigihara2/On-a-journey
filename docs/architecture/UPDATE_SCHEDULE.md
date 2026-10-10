# 更新スケジュール

作成日: 2026-09-30（指示書㉘）。GitHub Actionsのワークフローが「いつ・何を起点に・何を書き」、各画面が「どのデータの何時点の値」を
表示しているかの唯一の説明。起動時刻・連鎖について、他の文書（SYSTEM_MAP.md・CLAUDE_CODE_START.md・PROJECT_STATUS.md・
各ワークフローのYAMLのコメント）はこの文書を参照する。

- **1章のワークフロー一覧は `.github/workflows/*.yml` から自動で生成する**（`python scripts/gen_update_schedule.py`）。
  YAMLを変えたら生成し直す。一覧と文書・`config/workflow_dependencies.json`がずれると、report_consistency_check.pyの
  CHECK-58（WARN）が出る
- `config/workflow_dependencies.json` のうち、YAMLから決まる項目（depends_on＝workflow_runの起動元、outputs＝commitするパス）も
  同じスクリプトが生成する。表示名（label）・yml・accepts_tickers・input_param・一括更新の順序（bulk_update_*・new_ticker_order）は、
  docs/value-monitor/admin.htmlの一括更新が使う手作業の定義として残す（accepts_tickers・input_paramは、YAMLの手動実行の入力と
  合っているかをCHECK-58で確認する）。このファイルはadmin.htmlが読むため廃止しない
- 時刻はUTCとJST（UTC+9）で書く。米国の日付はニューヨーク時間（夏時間 EDT＝UTC−4、冬時間 EST＝UTC−5）

---

## 1. ワークフロー一覧（自動生成）

<!-- BEGIN GENERATED: workflows（scripts/gen_update_schedule.pyが生成。手で編集しない） -->

全23本（.github/workflows/）。時刻はcronの指定（GitHubの混雑で遅れて起動することがある）。

| ワークフロー | 起動（cron） UTC | 起動（cron） JST | 起動元（workflow_run、完了で起動） | 手動の入力 | 同時実行 | ガード・動く条件 | 出力先（commitするパス） | 下流 |
|---|---|---|---|---|---|---|---|---|
| `Adjusted_Eps_Analyzer_update.yml`<br>Adjusted_EPS_Data_Update | 月 04:10 | 月 13:10 | SEC_Data_Update.yml | ticker | — | 起動元が成功したときだけ動く（手動・cronは常に動く） | `docs/value-monitor/adjusted_eps_analyzer/data/` | TANUKI_VALUATION_Update.yml |
| `Beta_Config_Update.yml`<br>Beta Config Update | 毎日（1〜7日） 23:00 | 毎日（2〜8日） 08:00 | — | tickers・dry_run | — | 段「Check first-Sunday schedule」 | `config/beta_config.json` | — |
| `Discover_Config_Sync.yml`<br>Discover Config Sync | — | — | push: config/discover_config.json・config/theme_config.json | あり（入力なし） | — | — | `docs/portfolio/data/discover_config.json` | — |
| `HypeCore_Update.yml`<br>HypeCore Update | 月 04:00 | 月 13:00 | SEC_Data_Update.yml | tickers | — | 起動元が成功したときだけ動く（手動・cronは常に動く） | `docs/value-monitor/hypecore/data/` | TANUKI_VALUATION_Update.yml |
| `MACRO_PULSE_Update.yml`<br>MACRO_PULSE_Update | 毎日 22:15<br>毎日 13:03<br>土 22:07<br>土 22:11 | 毎日（翌日） 07:15<br>毎日 22:03<br>日 07:07<br>日 07:11 | — | mode | — | — | `docs/market-monitor/macro-pulse/data/` | — |
| `Macro_Data_Update.yml`<br>Macro Data Update | 毎日 10:00 | 毎日 19:00 | — | series_ids | — | — | `common/macro_data/macro_data_violations_log.json`<br>`common/macro_data/series/` | — |
| `Market_Data_Daily_Update.yml`<br>Market Data Daily Update | 月〜金 20:47<br>月〜金 21:17・21:47・22:17・22:47<br>月〜金 23:17<br>火〜土 01:47<br>火〜土 02:17 | 火〜土 05:47<br>火〜土 06:17・06:47・07:17・07:47<br>火〜土 08:17<br>火〜土 10:47<br>火〜土 11:17 | — | tickers・guard | market-data-daily | 段「Guard (NYSE close + 20 min / closes already saved)」<br>段「Cancel this run when no new data was saved (downstream stays idle)」 | `common/market_data/*/market_data_violations_log.json`<br>`common/market_data/_daily_close_retry_log.json`<br>`common/market_data/_sp500_constituents_cache.json`<br>`common/market_data/daily/`<br>`docs/common/sec_data/rotce/` | Market_Pulse_Update.yml<br>Stonks_Silo_Update.yml |
| `Market_Data_Weekly_Update.yml`<br>Market Data Weekly Update | 日 13:20 | 日 22:20 | — | tickers | — | — | `common/market_data/*/market_data_violations_log.json`<br>`common/market_data/_sp500_constituents_cache.json`<br>`common/market_data/analyst_history/`<br>`common/market_data/attributes/` | — |
| `Market_Pulse_Update.yml`<br>Market_Pulse_Update | — | — | Market_Data_Daily_Update.yml | あり（入力なし） | — | 起動元が成功したときだけ動く（手動・cronは常に動く） | `docs/market-monitor/market-pulse/data/` | — |
| `SEC_Data_Audit.yml`<br>SEC Data Quality Audit | — | — | SEC_Data_Update.yml | tickers | — | — | （commitしない） | — |
| `SEC_Data_Update.yml`<br>SEC Data Update | 日 12:00 | 日 21:00 | — | tickers | — | — | `common/sec_data/data/`<br>`common/sec_data/normalized/`<br>`common/sec_data/ttm/`<br>`docs/common/sec_data/normalized/` | Adjusted_Eps_Analyzer_update.yml<br>HypeCore_Update.yml<br>SEC_Data_Audit.yml<br>Stonks_Silo_Update.yml |
| `Score_Verifier.yml`<br>Score Verifier | 毎日 00:00 | 毎日 09:00 | — | あり（入力なし） | — | — | `docs/value-monitor/tanuki_valuation/data/*/score_history.json` | — |
| `Stonks_Silo_Update.yml`<br>Stonks Silo Update | — | — | SEC_Data_Update.yml<br>Market_Data_Daily_Update.yml | tickers | — | 起動元が成功したときだけ動く（手動・cronは常に動く） | `docs/value-monitor/stonks-silo/data/` | TANUKI_VALUATION_Update.yml |
| `System_Health.yml`<br>System Health Check | 毎日 23:30 | 毎日（翌日） 08:30 | — | あり（入力なし） | — | — | （commitしない） | — |
| `TANUKI_CIK_Lookup.yml`<br>TANUKI CIK Lookup | — | — | — | ticker | — | — | `config/cik_lookup.csv`<br>`config/cik_lookup_result.json` | — |
| `TANUKI_Score_Update.yml`<br>TANUKI_Score_Update | 日・土 13:30 | 日・土 22:30 | TANUKI_VALUATION_Update.yml | あり（入力なし） | — | 起動元が成功したときだけ動く（手動・cronは常に動く） | `docs/integrated-dashboard/daily_pick.json` | — |
| `TANUKI_Segment_AI.yml`<br>TANUKI Segment AI | — | — | — | ticker | — | — | `config/segment_config.json` | — |
| `TANUKI_TAIL_KPI_Update.yml`<br>TANUKI TAIL KPI Update | 日 23:00 | 月 08:00 | — | ticker・quarters | — | — | `docs/portfolio/tail/data/kpi/` | — |
| `TANUKI_TAIL_Position_Write.yml`<br>TANUKI TAIL Position Write | — | — | — | action・payload | — | — | `docs/portfolio/tail/data/positions/` | — |
| `TANUKI_TAIL_RSS_Monitor.yml`<br>TANUKI TAIL RSS Monitor | 月〜金 08:00 | 月〜金 17:00 | — | ticker | — | — | `docs/portfolio/tail/data/rss_state.json` | — |
| `TANUKI_TAIL_SEC_Ctrl.yml`<br>TANUKI TAIL SEC Ctrl Update | 月 01:00 | 月 10:00 | — | ticker | — | — | `docs/portfolio/tail/data/ctrl/` | — |
| `TANUKI_TAIL_SEC_Items.yml`<br>TANUKI TAIL SEC Items Update | 月 01:20 | 月 10:20 | — | ticker | — | — | `docs/portfolio/tail/data/legal_proceedings/`<br>`docs/portfolio/tail/data/mda/`<br>`docs/portfolio/tail/data/risk_factors/` | — |
| `TANUKI_VALUATION_Update.yml`<br>TANUKI VALUATION Daily Update | — | — | HypeCore_Update.yml<br>Adjusted_Eps_Analyzer_update.yml<br>Stonks_Silo_Update.yml | tickers | tanuki-valuation | 起動元が成功したときだけ動く。Stonks Silo Updateは失敗でも動く（手動・cronは常に動く） | `docs/value-monitor/tanuki_valuation/data/` | TANUKI_Score_Update.yml |

<!-- END GENERATED: workflows -->

**連鎖の読み方**: 「起動元」のワークフローが完了すると起動する（workflow_run）。ほとんどのワークフローは「起動元が成功したときだけ動く」
条件を持つため、起動元が失敗・取り消し（cancelled）・スキップになった場合、下流の実行は作られるがジョブは動かない（skipped）。
Market Data Daily Updateは、何もしなかった実行・新しいデータを保存しなかった実行を自分で取り消し、下流を動かさない（2章）。
金曜のcron（Market Pulse 22:50・Stonks Silo 22:40・TANUKI VALUATION 22:30 UTC）は2026-10-03に削除した（kaihatsu `791faa1f02`。
GitHubの遅延で、Market Data Dailyの取得より前に前日のデータで動いていた。5章）。この3本の起点は連鎖（workflow_run）と手動実行だけ。
Market Data Daily Updateは外部（Cloudflare Worker）からも起動する（2章「外部起動と保険の関係」。外部起動は1章の表には出ない）。

---

## 2. 夜間の流れ（米国の引けから日本時間7:00まで）

目標は、日本時間7:00（22:00 UTC）までに下流まで完了させること。

| | 夏時間（3月第2日曜〜11月第1日曜） | 冬時間 |
|---|---|---|
| NYSEの引け | 20:00 UTC（JST 05:00） | 21:00 UTC（JST 06:00） |
| Market Data Dailyのガードが取得を許す時刻（引け＋20分） | 20:20 UTC | 21:20 UTC |
| 取得する最初の起動 | 外部起動 20:25 UTC（JST 05:25）。scheduleでは20:47 UTC（JST 05:47） | 外部起動 21:25 UTC（JST 06:25）。scheduleでは21:47 UTC（JST 06:47） |
| 下流 | 取得の完了で Market Pulse・Stonks Silo → Stonks Siloの完了で TANUKI VALUATION → その完了で TANUKI Score（daily pick・ポートフォリオのスナップショット） | 同左 |
| MACRO PULSE | 22:15 UTC（JST 07:15、連鎖とは独立） | 同左 |
| System Health | 23:30 UTC（JST 08:30） | 同左 |
| Score Verifier | 00:00 UTC（JST 09:00） | 同左 |

- **Market Data Dailyの起動**: 外部起動（下の節）と、GitHubのschedule＝20:47〜23:17 UTCの30分おき（6回。20:17は2026-10-01に削除）と保険の01:47・02:17 UTC（UTCの火〜土）。各起動の最初に
  `common/market_data/daily_guard.py` が「NYSEのその日の引けから20分経っていない（休場日を含む）」「その日の終値が既にそろっている
  （直近に行がある銘柄の95%以上）」を判定し、どちらかなら何もしない。取得した結果、半数以上の銘柄に終値が無ければ（3章のYahooの
  作り直しの時間帯）取り直さず保存せずに終了する。いずれも最後に自分を取り消し、下流を動かさない。同時に1本だけ動く（concurrency）
- **GitHubの遅延**: cronは混雑で遅れて起動する。旧・cron 21:25 UTCの実測（2026-09）は23:05〜01:06 UTCの起動だった
  （[[MARKETDATA-DAILY-CLOSE-NONE-RECUR-1]]）。複数のcronを置くのはこのため。2026-09-30（米国）の夜は、9回の予定に対して
  実行が作られたのは2回（23:52 UTCの起動が取得して23:55にpush、02:07の起動はそろい済みで何もせず）。下流の完了は00:10 UTCで22:00に間に合わなかった
- **2026-10-01・10-02（米国）の夜の実測**: GitHubのscheduleは夜の枠（20:47〜23:17 UTC）で2晩とも1本も起動しなかった。
  最初の起動は00:13 UTC（10-01の分、手動実行で23:36に取得済み）・23:59 UTC（10-02の分。00:00〜01:26 UTCの作り直しに入り
  reset_windowが2回、01:55の起動で取得）。この結果から外部起動を入れた（次の節）
- **所要時間の例**（2026-09-30 JSTの実行、旧方式）: Market Data Daily 00:35〜00:49 UTC（取り直しの待ち9分を含む）→ Market Pulse
  00:49〜00:51 → Stonks Silo 00:49〜00:51 → TANUKI VALUATION 00:51〜01:00 → TANUKI Score 01:00
- **外部起動の実測**（夏時間）: 2026-10-06（米国10-06の足）は、20:25:44 UTCの起動が取得（20:29:36完了）→ Stonks Silo 20:31:39・
  Market Pulse 20:32:14 → TANUKI VALUATION 20:42:52 → TANUKI Score 20:44:19に完了。20:55・21:25の起動はそろい済みで取り消し。
  2026-10-05（米国10-05の足）は下流が20:58 UTCに完了したが、Market Pulseと20:55の起動がGitHubのランナー未割り当てでfailure
  （[[EXTERNAL-TRIGGER-DOWNSTREAM-UNCHECKED-1]]）
- **週次**: SEC Data Update（日曜12:00 UTC）の完了で HypeCore・Adjusted EPS・Stonks Silo・SEC Data Audit が動き、
  HypeCore・Adjusted EPS・Stonks Siloの完了でTANUKI VALUATIONが動く。Market Data Weekly（日曜13:20 UTC）は属性・アナリスト情報
- **その他の定時**: Macro Data（毎日10:00 UTC、FRED）、MACRO PULSEの補完（毎日13:03 UTC）、TANUKI TAILのRSS（平日08:00 UTC）など（1章）

### 外部起動と保険の関係（2026-10-03）

Market Data Daily Updateは、主にCloudflare Workers Cron Triggers（`tools/external_trigger/`、設定手順は同じフォルダのREADME.md）から
`workflow_dispatch`（ref=kaihatsu、入力`guard=true`）で起動する。GitHubのscheduleは保険として残す。

| 起動 | 時刻（UTC、平日） | JST | 役割 |
|---|---|---|---|
| 外部（Worker） | 20:25・20:55・21:25 | 05:25・05:55・06:25 | 主の起動。応答が204以外ならDiscordに通知 |
| 外部（Worker）の確認 | 21:50 | 06:50 | その日の20:00 UTC以降に作られて成功した実行が無ければ、もう一度起動してDiscordに通知 |
| GitHubのschedule（保険） | 20:47〜23:17の30分おき・01:47・02:17（UTCの火〜土） | 05:47〜08:17・10:47・11:17 | 外部起動が失敗した日の取得。遅延して作られることが多い（上の実測） |

- **ガードは共通**: `guard=true`の起動はscheduleと同じ`daily_guard.py`を通る。夏時間は20:25、冬時間は21:25の起動で取得し、それ以外の起動
  （引けから20分経っていない・既に取得済み）は何もせず自分を取り消す。保険のscheduleが外部起動の後に遅れて来ても、取得済みなので何もしない。
  同時に1本だけ動く（concurrency）ため、外部起動とscheduleが重なっても二重に取得しない
- **人の手動実行**は`guard`を付けない（既定false）。従来どおりガードを通さず取得する
- **外部起動が失敗した日**（Cloudflareの障害・トークンの期限切れ〈HTTP 401〉など）: Workerが失敗をDiscordに通知し、21:50の確認でも
  成功した実行が無ければ通知する。取得はGitHubのscheduleに任せる（間に合わない日は、Discordの通知を見て手動実行する）。
  scheduleが00:00〜01:26 UTCの作り直しの時間帯に入った場合はreset_windowで終わり（commitしない）、01:47・02:17の保険か、その後の遅れた起動で取得する
- **休場日**: WorkerはNYSEの休場日（`lib.js`の表、2026〜2028年）は何もしない。scheduleの起動はガードの「休場日」で何もしない
- **無料プラン**: Cron Triggersはアカウントあたり5本まで、Workerは3本を使う
- **監視**: Workerの21:50の確認はMarket Data Dailyの成功だけを見る。下流（Market Pulse・Stonks Silo・TANUKI VALUATION・TANUKI Score）の失敗は、
  System Health Check（23:30 UTC）の[J]が見る（2026-10-07から。1章の一覧と同じYAMLの読み方で、cronのあるものとworkflow_runの下流を対象にし、
  下流は起動元の想定間隔を継ぐ。ガードの取り消し・スキップは数えない）。日本時間7:00より前には気づけない
  （[[EXTERNAL-TRIGGER-DOWNSTREAM-UNCHECKED-1]]・[[EXTERNAL-TRIGGER-MARKETPULSE-RECHECK-1]]）

---

## 3. データの確定時刻と情報源の癖

| 情報源・対象 | 何が起きるか | 本システムでの扱い | 根拠（BACKLOG・実測日） |
|---|---|---|---|
| Yahoo（yfinance）の米国の個別株・ETF | 毎日00:00 UTCに直近の日足を作り直し、00:00〜01:22〜01:26 UTC頃は直近の足の終値が欠ける（全取得経路・yfinance 1.2.0/1.7.0で同じ）。^GSPC・^VIX・先物は残る。2026-08はこの時間帯でも欠けなかった | 取得は00:00 UTCより前（2章）。欠けた足は保存しない。半数以上が欠けたら取り直さず終了 | [[MARKETDATA-DAILY-CLOSE-NONE-RECUR-1]]、2026-09-29〜30に実測（22:21〜03:30 UTC） |
| Yahooの^N225（日経平均） | 東京の引け（15:30 JST）後も翌朝まで、当日の足に終値が無い（出来高0）。00:00 UTC（東京の寄り付き）に前日の足が消え、当日の取引中の足（出来高0）に置き換わる。確定した足（出来高つき）は01:26 UTC頃に出る | 出来高0の足は暫定（`_provisional`）。確定した足が届いたら置き換える。同じ実行の中の取り直しの対象外 | [[MARKETDATA-DAILY-PROVISIONAL-ROWS-1]]、2026-08-10以降の実行23回と2026-09-30の実測 |
| Yahooの先物（CL=F・GC=F、実装CでES=F・NQ=F・NIY=F） | 日足はニューヨークの暦日。確定した終値は清算値で、ニューヨークの暦日の終わり（翌日0時＝夏04:00・冬05:00 UTC）まで取引中の値のまま。09-29の足は09-30 04:05 UTCの取得で清算値（CL 89.38・GC 4179.70。直前の取引中の値とGCで27ドル違う）に置き換わり、以後不変。04:15 UTCから次の日の足が出る | ニューヨークの翌日0時の24時間後までは暫定（`FUTURES_FINAL_DELAY`。実測より24時間安全側、下記の理由で維持）、毎晩の取得で更新。夜の取得の当日の行は必ず暫定（下記「設計上の暫定値」） | [[MARKETDATA-DAILY-PROVISIONAL-ROWS-1]]、2026-09-29〜30に実測（09-30は23:05〜08:55 UTCに10分おき） |
| 先物の限月の乗り換え | 連続シンボルは乗り換え日に前日比が不連続になる。夕方の取得の乗り換え日は、Yahooの過去の系列の乗り換え日より早い（CL: 09-18と09-23） | 行に限月（underlyingSymbol）を記録し、乗り換え日は同じ限月どうしで前日比を計算。できなければ判定から外す | [[MARKETDATA-FUTURES-ROLL-1]]、2026-09-30 |
| Yahooの為替（JPY=X） | 日足はロンドンの暦日（翌0:00 Europe/London＝夏23:00・冬00:00 UTCで確定）。確定後も値が改訂されることがある | 確定前は暫定 | [[MARKETDATA-DAILY-PROVISIONAL-ROWS-1]]、2026-09-30 |
| Yahooのドル指数（DX-Y.NYB、実装B） | 日足はニューヨークの暦日 | ニューヨークの翌日0時までは暫定 | 2026-09-30（history_metadataで確認） |
| FRED（DGS3MO・VXNCLS・BAMLH0A0HYM2） | 公表が1〜3営業日遅れる（2026-09-26の実行でVXNが3営業日前、DGS3MOが09-23） | data_qualityの判定から外し、観測日を表示 | 設計書 MARKET_PULSE_REDESIGN.md 6章、MARKET_PULSE_LOGIC_INVENTORY.md（2026-09-26） |
| FREDの経済指標の予定（releases/dates） | 公表日だけで時刻が無い | 日付だけを表示（時刻を推測で付けない） | 実装C（指示書㉗）、2026-09-30 |
| CNN Fear & Greed | Market Pulseの実行時に取得（その時点の値） | 取得時刻の値として表示 | MARKET_PULSE_LOGIC_INVENTORY.md |
| S&P500構成銘柄のブレッス | 一部の銘柄だけ新しい日の終値を持つ日がある | 半数以上の銘柄がそろう日を基準日にする | [[MARKETPULSE-BREADTH-BASE-DATE-1]]、2026-09-30 |
| SEC（EDGAR）の財務データ | 週1回の取得（日曜12:00 UTC） | 決算の反映は次の日曜以降 | 1章 |

### 設計上の暫定値と想定外の暫定値（Market Pulseのdata_quality）

daily/の行のうち、日足の確定前に保存したものには`_provisional`の印が付く（確定した足が届いたら置き換える）。Market Pulseはこれを2つに分ける。

- **設計上の暫定値**: 日足の確定時刻（`common/market_data/fetcher.py`の`bar_final_at`）が、夜の取得の時点（NYSEの引け＋`CLOSE_WAIT`〈20分、
  `daily_guard.py`〉）より後の銘柄。夜の実行の時点では構造上必ず暫定になる。data_qualityを**partialにしない**
  （`expected_provisional_elements`に記録）。画面のカード・表には種類つきで表示する。
  - 「暫定（清算前）」: 先物（`FUTURES_SYMBOLS`: CL=F・GC=F、実装CでES=F・NQ=F・NIY=F）。確定値は清算値
  - 「暫定（日中）」: 日の区切りがNYSEの引けより後の銘柄（JPY=X〈ロンドンの暦日〉・DX-Y.NYB〈ニューヨークの暦日〉）
- **想定外の暫定値**: それ以外（米国株・ETF・指数・^N225など、夜の取得の時点で確定しているはずの足）が暫定だった場合。data_qualityを
  今までどおり**partial**にし（`provisional_elements`）、画面には「暫定」と表示する。

区別は`fetcher.expected_provisional_kind(symbol, day)`が日足の区切りの定義から判定する（銘柄の一覧は持たない。先物を増やすときは
`FUTURES_SYMBOLS`に加えるだけで「清算前」になる）。NYSEの休場日の足（為替等）は、その日の16:00（ニューヨーク時間）を引けとみなす。
段階8の先物・ドル円の最新値（実装C、15分足）も同じ判定で種類を付ける（data_qualityの判定からは元々外している）。
暫定の行は翌晩以降の取得で確定値に置き換わる（先物は`FUTURES_FINAL_DELAY`のため、確定の印が付くのは翌々晩の取得）。

**`FUTURES_FINAL_DELAY`（+24時間）を維持する理由**（2026-09-30決定）: 実測では先物の日足はニューヨークの翌日0時に清算値へ置き換わるため、0にもできる。しかし0にすると、清算値の反映が遅れた日（翌日0時を過ぎても取引中の値のままの日）に、取引中の値が確定値として保存され、以後置き換わらない。+24時間の不利益は暫定の印が一晩長く残るだけ（値は翌晩の取得で清算値に置き換わり、Market Pulseでは設計上の暫定値としてdata_qualityをpartialにしない）。[[MARKETDATA-DAILY-PROVISIONAL-ROWS-1]]

---

## 4. 画面ごとの表示データと時点

| 画面 | 表示するデータ | 何時点の値か | 更新するワークフロー |
|---|---|---|---|
| Market Pulse | 米国市場の終値・資金フロー・ブレッス・センチメント・8段階の結論・AIの見解 | 期待する終値日（実行時点で取引が終わった直近のNYSEの取引日）の終値。CNN F&Gは実行時点。FRED系列は観測日（遅れあり）。暫定の値を含む要素はdata_qualityがpartial | Market Pulse Update（Market Data Dailyの完了で起動） |
| MACRO PULSE | FREDのマクロ指標・流動性・週次の分析 | Macro Data（10:00 UTC）で取得したFREDの観測日。22:15 UTCの実行で再計算 | Macro Data Update・MACRO_PULSE_Update |
| TANUKI VALUATION | 本質価値・分類（TANUKI SCORE） | 株価はdaily/の最新の終値（Stonks Siloの後に計算）。財務はSEC（週次）。Stonks Siloのrunway等は同じ夜の結果 | TANUKI VALUATION Daily Update |
| TANUKI SCORE（daily pick） | 特選銘柄 | TANUKI VALUATIONの完了後。土日は金曜時点のlatest.jsonで独立に実行 | TANUKI_Score_Update |
| ポートフォリオ | 評価額・資産推移 | TANUKI_Score_Updateの中で作るスナップショット。ドル円はMarket Pulseの最新エントリの値 | TANUKI_Score_Update |
| Stonks Silo | 赤字成長株の評価 | 株価はdaily/の最新の終値（Market Data Dailyの後） | Stonks Silo Update |
| HypeCore | 期待のフェーズ（月次） | SEC Data Updateの完了後（週次）。月曜04:00 UTCの安全網 | HypeCore Update |
| EPS Analyzer | 調整後EPS | SEC Data Updateの完了後（週次） | Adjusted_EPS_Data_Update |
| Extreme Fear | CNN F&G | Market Pulseのエントリの値 | Market Pulse Update |
| TANUKI TAIL | 保有銘柄のRSS・KPI・SECの管理指標 | RSSは平日08:00 UTC、KPIは日曜23:00 UTC、SECの管理指標は月曜01:00 UTC | TANUKI TAIL の各ワークフロー |

---

## 5. 変更履歴

YAMLのコメント・`config/workflow_dependencies.json`にあった経緯をここへ移した（2026-09-30、指示書㉘）。日付はcommitの日付。

| 日付 | ワークフロー | 変更 | BACKLOG・commit |
|---|---|---|---|
| 2026-05-24 | 複数 | UTC 22:00に集中していた起動時刻をずらす | `7d8f453356` |
| 2026-06-02 | Market Pulse | cron 23:05 → 21:35 UTC | `bac179e5d7` |
| 2026-06-20 | TANUKI_Score_Update | TANUKI VALUATIONの完了（workflow_run）で起動。daily_pick.pyがlatest.jsonの分類に依存するため、cronの時刻差ではなく連鎖で順序を保証。土日は独立cron（13:30 UTC）で金曜時点のlatest.jsonを使う | [[ARCH-SCORE-SYNC-1]]、`873474664d` |
| 2026-06-24 | Market Pulse | cronを月〜金のみに | MP-BIZDAY-1、`77f6ac9673` |
| 2026-08-10 | Market Data Weekly | 新設。日曜13:20 UTC（当時の注記: HypeCoreの13:08 UTCと同時刻を避けて取得の集中を分散〈設計確定事項8〉。HypeCoreは2026-08-22に連鎖へ変わり、この注記は古くなったため削除した） | `c8db0eca97` |
| 2026-08-12 | Macro Data | 新設。毎日10:00 UTC（MACRO PULSEの補完〈13:03 UTC〉・本体〈22:15 UTC〉より前に確実に取得を終える、設計確定事項）。当時のYAMLの注記にあった他のワークフローの時刻（21:35/22:07/22:11/22:15 UTC）は、Market Pulseの21:35が連鎖に変わり古くなったため削除した | `1e7ecb7e6f` |
| 2026-08-22 | TANUKI VALUATION | 旧・独立cron（平日14:05 UTC）はMarket Data Daily（当時21:40 UTC完了）より先に動き、current_priceが常に前営業日の終値だった。Market Data Daily・HypeCore・Adjusted EPS・Stonks Siloの完了で起動する連鎖に変更、金曜22:30 UTCの安全網 | [[TANUKI-VALUATION-PRICE-SCHEDULE-LAG-1]]・[[WORKFLOW-SEC-TANUKI-GAP-1]]、`ca925ffa27` |
| 2026-08-22 | HypeCore・Adjusted EPS・Stonks Silo | 旧・独立cron（HypeCore 日曜13:08 UTC、Adjusted EPS 月曜10:07 UTC＝SEC Data Updateからの時刻差の推測）を、SEC Data Updateの完了で起動する連鎖に変更。週1回の安全網（月曜04:00・04:10 UTC）。どちらもmarket_dataの日次に依存しないため、SECの完了で鮮度を満たす。Stonks SiloにもSEC Data Updateの完了での起動を追加（当時は平日cronと併存） | [[WORKFLOW-SEC-TANUKI-GAP-1]]、`ca925ffa27` |
| 2026-08-22 | Market Data Daily | （当時の注記）Stonks Silo（cron 15:05 UTC）も同じ「前営業日の終値を参照する」ラグの疑い → 2026-08-31に解消（次行）。この注記はYAMLに残っていたが、解消済みのため削除した | [[STONKS-SILO-PRICE-SCHEDULE-LAG-SUSPECT-1]] |
| 2026-08-29 | Market Data Daily | cron 21:40 → 21:25 UTC（日本時間7時の閲覧に間に合わせる。冬時間の引け21:00 UTCから25分） | [[MARKET-DATA-SCHEDULE-7AM-JST-1]]、`2381f80cc1` |
| 2026-08-31 | Stonks Silo | 旧・独立平日cron（15:05 UTC。平日cronを独立の起動として併存させた旧設計〈案②〉）がMarket Data Daily（21:25 UTC完了）より6時間以上先に動き、current_priceが常に前営業日の終値だった（ASTS・AVAV・BBAI・CRWVで実データを突合）。Market Data Dailyの完了での起動を追加して平日cronを廃止し、金曜22:40 UTC（TANUKI VALUATIONの22:30から10分ずらす）の安全網 | [[STONKS-SILO-PRICE-SCHEDULE-LAG-SUSPECT-1]]、`a797fb9f2c` |
| 2026-09-02 | Market Pulse | cronをMarket Data Dailyと同じ21:25 UTCに揃えたところpushの競合の恐れが出たため、21:35 UTCに戻した | `4d53a51d55`・`19326420b5` |
| 2026-09-10 | Adjusted EPS・TANUKI_Score_Update | 週次の安全網cronと連鎖の重複実行を、pipeline側の冪等性ガード（24時間以内に完了していればスキップ）で防ぐ。手動実行は--forceで常に実行 | [[WORKFLOW-FALLBACK-CRON-DUPLICATE-1]]、`17e437ffa7`・`4676594039` |
| 2026-09-13 | Macro Data | 日次cronの取得を直近400日に限定（手動実行は全期間） | [[MACRODATA-FULL-HISTORY-DAILY-REFETCH-1]]、`5c9b106415` |
| 2026-09-26 | Market Pulse | 独立cron（21:35 UTC）はMarket Data Daily（21:25 UTC）との10分差だけに依存し、GitHubの遅延でcheckoutが日次データのpushより先になった日（直近20回中3回）は画面全体が1営業日古かった。Market Data Dailyの完了で起動する連鎖に変更、金曜22:50 UTCの安全網 | [[MARKETPULSE-MDD-CHECKOUT-RACE-1]]、`69ff44bd44` |
| 2026-09-30 | Market Data Daily | 20:17〜23:17 UTCの30分おき＋ガード（引け＋20分・そろい済み）、作り直しの時間帯は取り直さず終了、新しいデータが無い実行は自分を取り消し下流を動かさない。保険の01:47・02:17 UTC | [[MARKETDATA-DAILY-CLOSE-NONE-RECUR-1]]、`001e1d028a`・`06e33c84e1` |
| 2026-09-30 | TANUKI VALUATION | 一晩に2回動いていた（Market Data Dailyの完了と、その後のStonks Siloの完了の両方で起動）。Market Data Dailyを起動元から外し、Stonks Siloのfailureでも動く条件とconcurrencyを追加 | 指示書㉕ STEP C、`67915329a0` |
| 2026-09-30 | config/workflow_dependencies.json | `known_issues`（「TANUKI Score（22:30）がMarket Pulse（翌8:05）より先に実行されるため前日データを参照」「HypeCore循環依存」）を削除。前者の時刻は現在の連鎖と合わない。後者（HypeCoreとTANUKIが互いのlatest.jsonを参照し、初回は前回値を使う）は、この文書の2章の週次の流れで表す | 指示書㉘ |
| 2026-09-30 | Beta Config Update | cron `0 23 1-7 * 0`は日付と曜日がORになり「1〜7日の毎日＋毎週日曜」に起動していた。`0 23 1-7 * *`にし、最初の段で「今の時刻−6時間」の日付（UTC）が日曜の起動だけ続ける | [[BETA-CONFIG-CRON-DOM-DOW-OR-1]]、`1f73f1d215` |
| 2026-09-30 | Market Pulse | 夜の実行の時点で構造上必ず暫定になる値（先物の清算前・為替とドル指数の日の区切り前）はdata_qualityをpartialにせず「暫定（清算前）」「暫定（日中）」と表示（3章「設計上の暫定値と想定外の暫定値」） | [[MARKETDATA-DAILY-PROVISIONAL-ROWS-1]]、feature/mp-impl-b `36309d41f3`・feature/mp-impl-c `2e45457c9b` |
| 2026-10-01 | Market Data Daily | 20:17 UTCのcronを削除（夏時間は引けから17分でガードが必ず何もせず、冬時間は引け前） | `6de1c89644` |
| 2026-10-03 | Market Data Daily | 外部起動（Cloudflare Worker、平日20:25・20:55・21:25 UTC＋21:50の確認）を追加し、scheduleは保険に。workflow_dispatchに入力`guard`（既定false、trueでガードを通す）。reset_windowで終わった実行はcommitしない（`_daily_close_retry_log.json`だけのcommitが残っていた） | `4af62bcd1f`・`8dc0deaf0e` |
| 2026-10-03 | Market Pulse・Stonks Silo・TANUKI VALUATION | 金曜の安全網のcron（22:50・22:40・22:30 UTC）を削除。10-03（土）01:15〜01:28 UTCに遅れて起動し、Market Data Dailyの取得（02:06）より前の前日のデータで動いた（TANUKI VALUATIONはこの夜3回） | `791faa1f02` |
