# FINDINGS — 発見の記録（直さずに溜める場所）

運用は`CHAT_RULES.md`「発見の溜め方と根治の判断（2026-10-16新設）」。

- 発見したら直さず、BACKLOGにも入れず、ここに1行で記録する（範囲外としてClaude Codeが報告したものを含む）
- その場で直してよいのは、投資判断（理論株価・判定・保有銘柄の監視）に今、誤った値が出ている場合だけ。その場合も先にここに記録する
- 週1回（日曜の確認の後）に型ごとに束ねる。同じ型が2件以上、または影響の大きいものがあれば、型に対する根治策を1件設計してBACKLOGに1件だけ登録し、束ねた行の「状態」にそのIDを書く。
  影響が無く長く気づかれなかったものは、機能を削除するか記録を捨てる。行は消さない
- 根治策は2026-07-23のトップダウンの手順（出力項目の棚卸し → 定義 → 必要データ → 取得の確認）に沿う

## 原因の型

| 型 | 名前 | 意味 |
|---|---|---|
| A | 読み手・検証の無い出力 | 書いているが、誰も読まない・画面に出ない・届いたかを誰も確かめない出力。壊れても気づかれない |
| B | 同じ規則の二重実装 | 同じ決まり（「最新」の選び方など）を複数の場所で別々に実装し、片方だけがずれる |
| C | 時点のずれ | 値・出力が「いつの時点のものか」と、使う側が想定する時点が食い違う（作った時点の最新データで過去を書く、反映の時期の想定違い等） |

## 記録

「影響」は投資判断（理論株価・判定・保有銘柄の監視）への影響。「状態」は、済（commit）／未着手（BACKLOGのID）／未登録（束ねる前）。

| 日付 | 症状 | 型 | 影響 | 関係する既存ID | 状態 |
|---|---|---|---|---|---|
| 2026-10-16 | TANUKI TAIL RSS Monitorが存在しないsecret名（`DISCORD_WEBHOOK`）を参照し、新規提出・提出遅延のDiscord通知が2026-06-06から一度も届いていなかった | A | あり（保有銘柄の監視の通知が届かない） | [[TAIL-RSS-SECRET-NAME-1]] | 済（`1c2529cf6c`） |
| 2026-10-15 | RICEの`warnings`が「noteが空でない」をそのまま警告にしており、96銘柄すべてでTrue。⚠が常に出て、本当の警告（事例40の「エラー」表示）が約4週間埋もれた | A | なし（表示だけ。理論株価・判定は変わらない） | [[RICE-WARNINGS-ALWAYS-TRUE-1]] | 未着手（[[RICE-WARNINGS-ALWAYS-TRUE-1]]） |
| 2026-10-15 | 読み手が0件の出力が6種ある（`spac_shell_detection_log`・`share_unit_fix_log`・HypeCore `low_base_effect`・Stonks `fields_missing`・EPS `maturity_monitor.alert`・`market_data_violations_log`。`docs/architecture/TICKER_EXCEPTION_LEDGER.md` 7章） | A | なし | [[TICKER-EXCEPTION-LEDGER-1]] | 未着手（[[TICKER-EXCEPTION-LEDGER-1]]） |
| 2026-10-15 | `segment_xbrl_status`（読み手はTANUKI pipelineだけ）と`method_change`（読み手はscore_watcherだけ）が、どの画面にも出ない（stock.htmlの表示0件。同設計書 6章・7章） | A | なし（値は変えない。読む人が例外・計算方法の変更を画面で知る手段が無い） | [[TICKER-EXCEPTION-LEDGER-1]]・[[METHOD-CHANGE-LAYER3-EARLY-PUBLISH-1]] | 未着手（[[TICKER-EXCEPTION-LEDGER-1]]。`method_change`は台帳の対象外とされている） |
| 2026-10-16 | TAILの提出遅延のアラートは、遅延の日数が`ALERT_BIZ_DAYS`（3）に「ちょうど」なった回にしか出ない（`edgar_rss_monitor.py`の`no_filing_days == ALERT_BIZ_DAYS`）。その回の通知が届かなければ二度と出ない（TSLAは10-07に1回だけ出てDiscordに届かず、10-15時点で9日） | A | あり（保有銘柄の決算の遅れを見逃しうる） | [[TAIL-RSS-SECRET-NAME-1]] | 未登録 |
| 2026-10-16 | TAILの「最新のレビュー」の決め方が二重実装で、画面（キューのcompletedのquarterの最大）と`tail_dcf_bridge.py`（レビューのファイルの`is_latest`）がずれ、6銘柄のDCFシナリオが古いレビュー（2025Q4）から作られていた | B | あり（保有銘柄のDCFの数字。PLTRは1年目の売上成長率28%〈2025Q4〉と52%〈2026Q2〉） | [[TAIL-REVIEW-LATEST-MISMATCH-1]] | 済（`e615dc2b21`。シナリオの作り直しは次の平日のRSS Monitor、`CLAUDE_CODE_START.md`「次回の確認手順」9） |
| 2026-10-16 | TAILのRSS監視で、10-QのRSSがタイムアウトすると古い10-Kが「最新」になり新規提出と誤検知し、戻る回にも再び新規提出になっていた（CELHは09-22〜09-29に交互に検知。SOUN・APP・CELHの2025Q4がキューに入りGrokのレビューが作られた） | B（「最新」の決め方が取得の成否で変わる。チャット側の判断） | あり（保有銘柄の監視の通知・レビュー） | [[TAIL-RSS-FALSE-NEW-FILING-1]] | 済（`2d8dcf33ab`） |
| 2026-10-16 | レビューのファイルの`is_latest`は、[[TAIL-REVIEW-LATEST-MISMATCH-1]]で`tail_dcf_bridge.py`が読まなくなり、読み手が0件になった（書き手は`quarterly_review_generator.py`だけ。FIELD_DEFINITIONS.md AS-IS-481） | A | なし | [[TAIL-REVIEW-LATEST-MISMATCH-1]] | 未登録 |
| 2026-10-16 | TAILのレビューは対象の期にかかわらず、作った時点の最新の財務で書かれる。後から作った「2025Q4」の中身は2025Q4の決算ではなく、06-06の一括生成（2023Q3〜2026Q1）も同じ可能性がある。スコアの推移・履歴・予測の振り返りが期ごとの比較になっていないかもしれない | C | あり（保有銘柄の監視の履歴の意味） | [[TAIL-REVIEW-BACKFILL-DATA-PERIOD-1]]・[[TAIL-REVIEW-LATEST-MISMATCH-1]] | 一部済（古い期の生成を止めるskipped_old、`e615dc2b21`）。残りは未着手（[[TAIL-REVIEW-BACKFILL-DATA-PERIOD-1]]、調査） |
| 2026-10-14（10-16に再調査） | Layer3の`filed`が「その値を最後に申告した書類」の日付で、過去の時点で使えた値が分からない（25,682件で最初の提出日より遅い） | C | 今はなし（定義v2のstageの検証で使う。案Aなら理論株価・判定・注記は変わらない見込み） | [[LAYER3-FIRST-FILED-DATE-1]] | 未着手（[[LAYER3-FIRST-FILED-DATE-1]]、案Aで決定。着手は10-19の確認の後） |
| 2026-10-15 | 「計算方法の変更は日曜の連鎖で公開に反映される」前提だったが、Layer3をcompany_factsからその場で作る読み手（TANUKI・HypeCore・Stonks Silo）があり、変更の一部（APPの`latest_period_missing`）がpushの翌朝に`method_change`の印なしで公開された（CHAT_RULES.md 事例39） | C | あり（注記が印なしで公開。score_watcherの「🛠 計算方法の変更」の欄に出ない） | [[METHOD-CHANGE-LAYER3-EARLY-PUBLISH-1]]・[[LAYER3-RETENTION-PERIOD-1]] | 未着手（[[METHOD-CHANGE-LAYER3-EARLY-PUBLISH-1]]） |
