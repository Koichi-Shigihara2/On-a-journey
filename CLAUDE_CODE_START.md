# Claude Code 作業開始テンプレート

> 2026-11-06 から細くした。**今の状態・未済の確認手順・決まりへの参照だけ**を置く（行数の上限は `tests/test_claude_code_start_thin.py`）。
> 日付つきの記録・件数の推移・済んだ手順は `docs/history/CLAUDE_CODE_START_HISTORY.md`。終業のまとめは `PROJECT_STATUS.md`。

## 今の状態（2026-11-06 終業）

- BACKLOG.md のアクティブは **9件**（`^### [` の機械カウント）。rcc NG 0・WARN 207、pytest 2922件
- 次の作業の順と注記（週の使用量が91%に達したため、判断の要る作業は枠が戻るまで止める）は `PROJECT_STATUS.md` の「次の作業」が正
- 依頼文の見出しの【Opus】【Sonnet】に合わせて `/model` で切り替える（`CHAT_RULES.md`「モデルの使い分け」。付いていない依頼はOpus）
- 報告は20行以内（結論・変わった数・判定の変化・決めてほしいこと）。詳しい表はファイルに置き、場所だけ書く（`CHAT_RULES.md`「報告の形」）
- 範囲外の発見は直さず、BACKLOGにも入れず、`FINDINGS.md` に1行で記録する（`CHAT_RULES.md`「発見の溜め方と根治の判断」・事例41）。
  GitHubのscheduleは数時間遅れることがあるため、実行の有無は `gh run list` で先に確かめる

## 次回の確認手順（未済のものだけ。日時〈JST〉の順。番号は前のまま〈BACKLOGが番号を参照するため〉）

済んだ手順・日付つきの確認の記録は `docs/history/CLAUDE_CODE_START_HISTORY.md`。各項目は「日時｜期限｜確認の手順」。

26. **外の変化の欄 [P] ExternalChange と予備のFOMCのキャッシュの最初の本番**（`[[EXTERNAL-SOURCE-CHANGE-WATCH-1]]` A、2026-11-04 実装 `16e969f384`・`c6333c6dd4`）**2026-11-05 夜に合格（移設済み）、残りは`fomc_date_source`だけ**: 11-05 JSTの実行30本のRunner Imageはすべてubuntu-26.04・割り当ての失敗なし。MACRO PULSE run `41955454716`（`7decfcc81b`）のcommitに`fomc_calendar_cache.json`（`last_success_utc` 2026-11-05T02:30:53Z）、ログに「キャッシュ（予備）の声明を使う」なし。System Health run `41962421074`は16欄・[P] ✅・Discord通知 送信完了。
   - **残り（2026-11-07〈土〉のMACRO PULSEの週1回の回、期限 2026-11-08）**: `docs/market-monitor/macro-pulse/data/05_fed_context.csv`の新しい列`fomc_date_source`が書かれている（予定のページが取れた回なら`cache`ではない値）。確かめたらBACKLOG_DONE.mdの`[[EXTERNAL-SOURCE-CHANGE-WATCH-1]]`の「確認」に書き足す。以下は11-05 夜までの記録
   - 日時: 2026-11-05（木）のMACRO PULSEの毎日の実行（01:50 UTC頃）と、08:30 JST のSystem Health（手順23の残りと同じ回）
   - 期限: 2026-11-06
   - 確認の手順:
     - 全ワークフローのログの Runner Image が ubuntu-26.04（`runs-on: ubuntu-26.04`）で、ランナーの割り当ての失敗が無い
     - MACRO PULSEの回: 予定のページが取れ、commitに`common/macro_data/fomc_calendar_cache.json`が入る（`last_success_utc`が11-05。中身が同じ日は書かない）。
       「キャッシュ（予備）の声明を使う」の行がログに無い（取れた回）。`git add`のパスの誤りで失敗していない
     - System Health: 16欄（[P]が増えた）。[P] は11-04夜の時点で「⚠️ 12件検知（確認済み8・新しい件4）」（手順27。以前の見込み「26件検知（確認済み0・新しい件26）」は①の台帳・②の補いで変わった）（B1〜B3、2026-11-04 夕）。FOMCの予定のキャッシュの古さの⚠️が無いこと（あれば理由に「最後に取れてから」）。Discordの1行に`ExtChange⚠️`。台帳に入れた後の回で`ExtChange✅`・「確認済み26・新しい件0」になれば`[[EXTERNAL-SOURCE-CHANGE-WATCH-1]]`を移設
   - 合格なら、BACKLOGの`[[EXTERNAL-SOURCE-CHANGE-WATCH-1]]`の「A」に書き足す（Bが残るので項目はBACKLOGに残す）

28. **業種のマスターのSICの最初の保存**（`[[HYPECORE-STAGE-TIMESCALE-1]]` 段B-1、2026-11-05 実装 `9dbb2fa974`、SEC_Data_Updateの後の作り直しは2026-11-05 夜の2回目）
   - 日時: 2026-11-08（日）のSEC_Data_Update（12:00 UTC）と Market Data Weekly（13:20 UTC）の後。`gh run list --workflow SEC_Data_Update.yml`・`--workflow Market_Data_Weekly_Update.yml`で先に確かめる
   - 期限: 2026-11-09
   - 確認の手順:
     - `common/sec_data/data/{T}/submissions.json`に`sic`・`sic_description`・`entity_name`が入る（submissionsを取り直した銘柄。リクエストの数が増えていない）
     - SEC_Data_Updateの段「Rebuild sector master」が成功し、commitに`config/sector_master.json`が入る。マスターの各銘柄の`sic`が埋まり、`financial.basis`が`sector+sic`になる
     - Vが金融から外れる（SIC 7389、`financial.value` false）。SOFIは金融のまま（SIC 6xxx の見込み）。ほかに区分（景気循環株・金融・区分なし）が変わった銘柄があれば一覧にする
     - 同じ回の段「Rebuild profitability」（段B-2、`docs/common/profitability.json`）が成功し、commitに入る（SICで金融から外れるVは区分なし→黒字になる見込み）。Market Data Weeklyの同じ段も成功
     - Market Data Weeklyの段「Rebuild sector master」も成功し、`changes`（yfinanceの業種の変化）があれば System Health の[P]に`sector_change`が出る。どちらが先に終わっても最新のSIC・業種がそろっている
   - 合格なら、BACKLOGの`[[HYPECORE-STAGE-TIMESCALE-1]]`の段B-1に確認を書き足す

25. **申告のCF計算書の行（cf_lines）の最初のCIの取得**（`[[FCF-DEFINITION-SINGLE-SOURCE-1]]` 段5a・段5b、2026-11-04 実装 `f56a1a10fd`・`6230e12749`）
   - 日時: 2026-11-08（日）のSEC_Data_Update（`SEC_Data_Update.yml`）の後
   - 期限: 2026-11-09
   - 確認の手順:
     - ログの各銘柄に「cf_lines: 新しい申告 n件 → 保存 n件・次の実行で取り直す 0件」が、11-04より後に10-K・10-Qを出した銘柄にだけ出る（出ない銘柄は新しい申告なし）。
       「cf_lines の取得エラー」・SystemExit（SEC_USER_AGENT_EMAIL）が無い。リクエストの数が数十〜数百で、1回の上限（3,000）に届いていない
     - commitに`common/sec_data/data/{T}/cf_lines/{accn}.json`の新しいファイルが入り、既存の2,229ファイルが変わっていない（取り直さない）
     - 新しい申告の status が ok（no_cf_statement・not_found なら reason を報告）。APPの新しい10-Qが「CF計算書にCapExの行なし」（取得系の行なし）なら、TTMのCapExの0の点が増える
     - rcc NG 0。WARNは208前後（11-04 段5dの後）
     - 取得元の変化の検知（`[[EXTERNAL-SOURCE-CHANGE-WATCH-1]]` B1・B2）: ログの最後に「CIKの照合: 食い違い 0件」「取得元の変化の検知: n件」（11-04の手元で24件）、`common/sec_data/data/_source_watch.json`がcommitに入り、`cik_checked_at`がその回。銘柄ごとの「取得元の変化の検知のエラー」が無い
     - 新しい申告にタグだけでは決まらない行（PaymentsToAcquireOtherProductiveAssets）があれば、その申告だけラベルも取り（1リクエスト増）、記録の lines[].labels に表示名が入る
   - 合格なら、BACKLOG_DONE.mdの`[[FCF-DEFINITION-SINGLE-SOURCE-1]]`（2026-11-04 移設）に「確認」を書き足す。取得元の変化の検知（B1・B2）の分は`[[EXTERNAL-SOURCE-CHANGE-WATCH-1]]`（2026-11-05 移設）の「確認」に書き足す

5. **SECのcompanyfactsの取り込みの遅れ**（`[[SEC-COMPANYFACTS-API-LAG-1]]`、残りはVだけ）
   - 日時: 毎週日曜のSEC_Data_Updateの後（10-18・10-25〈run `40205244163`〉とも4銘柄ともSEC_LAG。**11-01〈run `41330561316`〉でCDNS・KO・RMBSはREFRESHED〈Q3の10-Qまで〉、VはSEC_LAG**〈local 2026-04-29、submissions 2026-07-29〉、XOMはOK_NO_NEW。2026-11-02 朝にwarn_acknowledged.jsonのCDNS・KO・RMBSの3件を消した）
   - 期限: Vの7月の10-Q（`0001403161-26-000104`）がcompanyfactsに入るまで
   - 確認の手順: `common/sec_data/data/_freshness.json`でVがREFRESHEDになったか。なっていれば`config/warn_acknowledged.json`のVの1件を消して移設する。入らないままなら直し方の候補2をチャット側が判断

29. **定義v2の後の段階（S3・S4）の12か月の並びの評価**（`[[STAGE-V2-ROLLOUT-1]]` ②、2026-11-06 記録開始 `common/stage_v2.py`）
   - 日時: 2027-11ごろ（並行計算の記録が12か月たまった後）
   - 期限: 2027-12-31
   - 確認の手順: `docs/common/stage_v2/{T}.json`の2026-11以降の月末の`stage`ごとに、その後12か月のS&P500に対する超過リターン（中央値）を出し、S3・S4が前の段階（S1・S0・消化）より下に来るかを見る（段Cは2022-01〜でS3・S4が0付近、2024-12〜で−0.214・−0.184）。件数も並べる。切り替えの条件にはしない（決定 2026-11-06）
   - 結果は`[[STAGE-V2-ROLLOUT-1]]`の②に書き、§0でS3・S4を売りの判断に使うかの材料にする

9. **TANUKI TAILのRSS Monitorの残り**（`[[TAIL-RSS-FALSE-NEW-FILING-1]]`・`[[TAIL-RSS-SECRET-NAME-1]]`、どちらも移設済み。残りは「RSS 取得エラー」の回だけ。ほかの経路は2026-10-19・10-23に確認済み〈BACKLOG_DONE.mdの「確認」の欄〉）
   - 日時: 平日の実行（cronは08:00 UTC＝17:00 JST。scheduleは数時間遅れるため`gh run list --workflow TANUKI_TAIL_RSS_Monitor.yml`で先に確かめる）の後
   - 期限: 起きた回（10-16・10-22の実行はRSS 取得エラー0件で、この経路は通っていない）
   - 確認の手順: 「RSS 取得エラー」が出た回のログで、その銘柄に「★ 新規提出」が出ず「新規提出の判定をしない」が出ている。確かめたら、BACKLOG_DONE.mdの2件の「確認」の欄に書き足す

15. **TAILのレビューを対象の期のデータで作る**（`[[TAIL-REVIEW-PERIOD-FILTER-1]]`、2026-10-30 実装 `f9a7563010`・移設済み）
   - 日時: 保有銘柄の次のTANUKI TAILのレビューの生成（Q3の10-Qの後、11月上旬の見込み。`gh run list --workflow TANUKI_TAIL_RSS_Monitor.yml`で先に確かめる）の後
   - 期限: 2026-11-30
   - 確認の手順: 新しいレビュー（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）に`data_as_of`（キューのfiled）が入り、`backfilled`がfalse、`kpi_snapshot`・`layer3_snapshot`の期が対象の期まで（対象より後の期が無い）、SECの財務の値が対象の期の申告日以前の値（Layer3の`first_val`）であること。確かめたらBACKLOG_DONE.mdの`[[TAIL-REVIEW-PERIOD-FILTER-1]]`の「確認」に書き足す

18. **Market Pulseのニュースの見出しの配信元3本**（`[[MARKETPULSE-HEADLINES-CNBC-STALE-1]]`、2026-11-01 実装 `faa452e1b9`）
   - 日時: 手順17と同じ2026-11-02 21:30 UTC頃（11-03 06:30 JST）のMarket Pulseの実行の後（平日の本番の実行）
   - 期限: 2026-11-03
   - 確認の手順: `docs/market-monitor/market-pulse/data/market_data.json`の最新エントリの`headlines`で、
     - `failed`が空（`failed_reasons`も空、`status`がok）
     - `items`の`source`に「CNBC Earnings」「Yahoo Finance S&P 500」「Google News ビジネス（US）」の3本とも入っている（最大48件）
     - 表示の本表が20件以内（`news_headlines.split_for_display()`の1つ目。`translation`・`relevance`の`status`がokで、ログに「途中で切れた」が無いことも見る）
   - 満たしたらBACKLOG_DONE.mdへ移す。それまではBACKLOGに残す

19. **見出しの日ごとの積み足し**（`[[MARKETPULSE-MOVE-POSTMORTEM-EPIC-1]]` ①、2026-11-01 実装 `32f336a225`）
   - 日時: 新しいワークフロー`Market_Pulse_Headlines.yml`の最初の数回（11-01 12:40 UTCの枠から6時間ごと）と、11-02 21:30 UTC頃のMarket Pulseの実行の後（11-03朝）
   - 期限: 2026-11-03
   - 確認の手順:
     - `gh run list --workflow Market_Pulse_Headlines.yml`で最初の数回がsuccess。commitは「新しい記事があった回だけ」（`git log --format="%h %ad %s" --date=iso -- docs/market-monitor/market-pulse/data/headlines/`の件名「chore: auto-update market pulse headlines」と実行のログの「新しい記事が無い（commitしない）」が対になる）
     - `docs/market-monitor/market-pulse/data/headlines/`のファイルの`fetches`に`run: scheduled`の記録があり、`sources`が3本ともok
     - 11-03の朝に、`2026-11-02.json`に`run: evening`の取得の記録があり、夜の回の訳・判定（`title_ja`・`market`）が写っている記事がある（夜の回の`headlines.items`に入った記事）
     - System Healthの[N]が✅（11-02 23:30 UTCの実行の通知）
   - 満たしたらBACKLOGの①に確認を書き足す（①はEPICの中の段階なので、EPICが済むまでBACKLOGに残す）

20. **5分足の日ごとの保存**（`[[MARKETPULSE-MOVE-POSTMORTEM-EPIC-1]]` ②、2026-11-01 機能 `9cd26116a8`・最初の一回のデータ `21c4db3b1d`）
   - 日時: 手順17と同じ2026-11-02 21:30 UTC頃（11-03 06:30 JST）のMarket Pulseの実行の後
   - 期限: 2026-11-03
   - 確認の手順:
     - `docs/market-monitor/market-pulse/data/intraday/2026-11-02.json`があり、`tickers`に10本（^GSPC・^IXIC・^VIX・^TNX・DX-Y.NYB・CL=F・GC=F・HYG・ES=F・NQ=F）がそろい、`missing`が空
     - 11-01（日）の夜の先物などの分（`2026-11-01.json`）も入っている（最初の一回は10-30まで）
     - 実行のログに「5分足の保存: 書いた …」があり、「5分足の保存に失敗」が無い。System Healthの[O]が✅（11-02 23:30 UTCの実行）
     - 2026-11-02.jsonは、ETで11-02が終わる前に書いた分なので、次の回（11-03の夜）で夜の先物の足が足される（足が増える）
   - 満たしたらBACKLOGの②に確認を書き足す（②はEPICの中の段階なので、EPICが済むまでBACKLOGに残す）

## 決まりへの参照（本文は `CLAUDE_CODE_PROCEDURES.md` の同名の節）

| 知りたいこと | 節 |
|---|---|
| 作業の始め方（pull・現状確認・テスト・宣言）、運用の基本、STEP 0 の運用ルール | 「毎回の作業開始時に必ず実行すること」・冒頭の運用の基本 |
| ファイル変更の原則・調査での書き込みの注意・履歴JSONへの記録 | 「作業ルール」 |
| 計算方法（理論株価・判定・funda・stage・Layer3・TTM）を変えるとき（scope・派生ファイル・読み合い・印） | 「計算方法の変更の手順」 |
| 試算の共通の道具（前後の全銘柄比較・scope・変化の表） | `tools/sim/README.md` |
| 新規銘柄の登録・削除・TAILの銘柄追加 | 「新規銘柄登録時の必須手順」・「銘柄削除時の必須手順」・「TANUKI TAIL 銘柄追加手順」 |
| 管理の操作（ポートフォリオ・設定の編集をClaude Codeが代行） | 「管理の操作の手順」 |
| 作業完了時・月次・BACKLOG管理 | 「作業完了時のチェックリスト」・「月次メンテナンスタスク」・「BACKLOG管理ルール」 |
| HTMLを新規作成・移設・削除したとき | 「リンク整合性チェック」 |
| 外部AIレビューの誤指摘の予防・フロントエンドの調査順序 | 「重要ルール」の各節 |
| 決まり全般・過去の失敗事例 | `CHAT_RULES.md` |
