# FED-POLICY-PAGE-1 STEP 0（読み取りだけの調査。コードは変えていない）

2026-11-28。対象: `[[FED-POLICY-PAGE-1]]`（政策金利の専用ページ。BACKLOG に登録済み・実装は許可されていない）。
依頼: ① ZQ 先物の取得をやめた理由 ② 会合ごとの確率の取得元の候補 ③ 今のデータの流れ ④ 新ページで影響を受ける所。
**限界（先に）**: 取得元は公開の応答を数件読んだだけ（書き込み・認証なし）。**Kalshi の利用条件（再配布・自動取得）は読めていない**ので、使う前にチャット側が公式の規約を確かめる。yfinance の各限月の値が正しいか（出来高・更新の遅れ）は確かめていない。

## 1. ZQ 先物をやめた理由（git log・BACKLOG_DONE）

| 日付 | commit | 内容 |
|---|---|---|
| 2026-06-03 | `7be3e07df8` [MP-5] | `get_implied_cuts()` を新規。**ZQ=F（フロントマンス）の implied rate で term premium を補正**（`term_premium = DGS1 − ZQ_implied`、±0.5% で頭打ち）。DGS1 生値の IMPLIED −0.82 回（誤差: term premium 込み）→ 補正後 +0.01 回。ZQ=F が取れなければ DGS1 に戻す |
| 2026-06-14 | `aee4c8dd14` [MP-1YEFF-FIX] | 「1Y EXPECTED FF」のラベルが `FRED T1YFF` だったが、T1YFF は DGS1−FEDFUNDS のスプレッドで絶対金利ではなかった（表示 3.62%→3.85%）。**ZQ=F の補正を廃止して DGS1 をそのまま使う**（commit の言葉は「簡略化」）。IMPLIED CUTS は +0.02 → −0.90 回。符号は「正＝利下げ織り込み、負＝利上げ・高止まり織り込み」 |
| 同日 | `915fddbf19` ほか | 説明文・プロンプトから「ZQ=F front-month corrected」を外す（N-11）。`_fallback_regime` の文言を「ZQ 先物が…」→「DGS1 ベースで…」に |

- **やめた理由として commit に書かれているのは「簡略化」と、ラベルのずれ（T1YFF）の修正**。ZQ=F が使えなかった（取得失敗・値の異常）という記録は見つからない。
- **推測（記録にない）**: ZQ=F はフロントマンスの1本で、今月の平均 FF 金利を映す。「12か月先」の織り込みではないので、`DGS1 − ZQ_implied` を term premium と呼ぶのは意味が合わない。ZQ の各限月を並べれば会合ごとの経路は作れるが、当時は1本だけを使っていた。
- 名残: `zq_ticker`・`zq_price`・`zq_rate`（変数名・CSV の列。`zq_rate` の中身は DGS1）、`get_zq_futures()`（`get_implied_cuts` の互換の別名）。

## 2. 会合ごとの確率の取得元の候補

| 候補 | 取れる項目 | 確かめたこと | 未確認・リスク |
|---|---|---|---|
| **Kalshi の公開データ**（`KXFED` 系列、会合ごとのイベント `KXFED-27APR` など。取引所が出す市場の価格） | 会合ごとに「会合の後の政策金利が X 以上」の二値市場が並び、`last_price`・`yes/no の売り買い気配`・`close_time`・`floor_strike` が取れる。確率に直すには各行使価格の価格から分布を作る（自前の計算） | 2026-11-28 に公開の `markets`・`events` を認証なしで取得できた（HTTP 200、`series_ticker=KXFED`）。公式の開発者向け文書は「公開の市場データは API キー不要」 | **利用条件（自動取得・再配布）は未確認**。注文板は認証が要る可能性（文書に食い違い）。気配が薄い（出来高が少ない）限月は確率が不安定。サービスの仕様変更・停止のリスク（`EXTERNAL-SOURCE-CHANGE-WATCH-1` の監視対象に足す） |
| **yfinance の FF 金利先物**（`ZQ=F`・各限月 `ZQZ26.CBT` 形式） | 限月ごとの終値（`100 − 平均 FF 金利`）。会合の日を挟む2限月の差から、その会合の織り込みの金利が出せる（CME FedWatch と同じ考え方） | 2026-11-28 に `ZQ=F`・`ZQZ26.CBT`・`ZQF27.CBT`・`ZQH27.CBT`・`ZQZ27.CBT` が直近5日の終値を返した（`ZQ=F` と `ZQF27.CBT` は同じ値＝フロントが 2027-01 限月） | **各限月の取り方（コード）・欠け・更新の遅れ・出来高は未確認**。yfinance は非公式（利用条件・仕様変更のリスクは既存の Yahoo の日足と同じ型）。月の途中の会合は日割りの補正が要る |
| CME FedWatch | 会合ごとの確率（本家） | **無料の API は無い**（BACKLOG の既存の記述どおり） | 使えない |

- **独立した2つの取得元を持つのが望ましい**（片方が欠けたとき、もう片方で「取れない」を出す。0やスキップで埋めない）。ただし**独自の予想はしない**（Koichi 決定）ので、織り込みの確率は取得元の値を表示するだけにする。
- 初回の実装の前に、Kalshi の規約・yfinance の各限月の欠けを確かめる（読み取りの調査 STEP 1）。

## 3. 今のデータの流れ（`src/market/macro_pulse/05_main.py`）

```
FRED（Macro Data の保存: DFEDTARU・DFEDTARL・FEDFUNDS・DGS1）
   │ fred_latest()（common.macro_data.reader 経由）
   ▼
get_ff_current()  ………… (上限+下限)/2。無ければ FEDFUNDS。取れないと update_fed_context が手書きの 3.625 を使う
get_implied_cuts() ……… ("FRED:DGS1", None, DGS1)   ← get_zq_futures() は同じ関数の別名
   │
   ▼  update_fed_context(target_date)       … 週1回（--update-schedule、土曜 22:07 UTC の cron。22:11 は週次の AI 解説）
        cuts_implied = round((ff_current − DGS1) / 0.25, 2)
        新しい FOMC 声明があれば analyze_fomc_with_grok(fomc_date, 声明本文, ff_current, DGS1, cuts_implied)
            Grok が regime（EASING/TIGHTENING/BALANCED）・dominant_concern・ai_reason を返す。失敗・不正な値は _fallback_regime（DGS1 の数値だけで判定）
        → 05_fed_context.csv に1行（record_date・fomc_date・regime・dominant_concern・ff_current・zq_*・cuts_implied・ai_reason・regime_source・fomc_date_source）
   ▲
refresh_fed_context_on_new_statement()      … 毎日の実行（22:15 UTC の normal。13:03 UTC は fill-returns で呼ばない）。新しい声明が出ていれば update_fed_context を呼ぶ（Grok を呼ぶ）。無ければ何もしない
        声明の日付は common/macro_data/fomc_calendar.py（FRB のページを毎日取る。取れない日が7日続くと予備のキャッシュ）

05_fed_context.csv の最後の行
   ├─ get_financial_context() → 05_events.csv の各行の regime・ff_rate・cuts_implied（指標の発表の行に、その時点の局面を付ける）
   ├─ 週次の AI 解説（05_main.py の1883行）のプロンプト「FED政策局面: {regime}, FF金利, 利下げ織り込み: {cuts}回」
   └─ 画面 docs/market-monitor/macro-pulse/index.html
        ├─ loadFedContext() → renderRegimeBar(): REGIME・「1Y EXPECTED FF」（DGS1）・「IMPLIED CUTS」・判定の元（regime_source）
        └─ updateStealthLayer1(): 流動性の判定（ステルス）の Layer 1 に regime を表示（色だけ。点数には入っていない）
```

- **regime を点数に使うコードは見つからなかった**: `stealthLayer1` は表示（文字と色）だけ。景気後退スコアの計算（`_compute_current_score`・`SIGNAL_STEPS`）に `regime` を読む所は grep で無かった（読む所は events.csv の列・週次の AI 解説・画面の帯）。点数・判定への影響は着手時に再確認する。
- **既知の問題**（BACKLOG の FED-POLICY-PAGE-1 に既出）: 1Y EXPECTED FF に DGS1 をそのまま使う・ベーシス未補正・小数2桁・手書きの `3.625`・`zq_*` の名前・Grok の局面に DGS1 と cuts_implied を渡している（同じ10-28の声明が BALANCED と TIGHTENING に分かれた）。2026-11-07 の行（TIGHTENING・cuts −2.98）が現在の最新。

## 4. 新ページを作ったときに影響を受ける所

| 区分 | 所 | 影響 |
|---|---|---|
| 書き手 | `05_main.py` の `get_ff_current`・`get_implied_cuts`・`get_zq_futures`・`update_fed_context`・`analyze_fomc_with_grok`・`_fallback_regime`・`refresh_fed_context_on_new_statement`・`FED_CONTEXT_COLUMNS` | 作る側を新ページへ移す（MACRO PULSE は読む側）。列名 `zq_*` の整理 |
| データ | `05_fed_context.csv`・`05_events.csv`（regime・ff_rate・cuts_implied）・`common/macro_data/fomc_calendar.py` | 新ページが作る形に合わせて、MACRO PULSE の読み方を変える。events.csv の過去の行の局面は作り直さない（時点の値） |
| 画面 | `macro-pulse/index.html`（REGIME・1Y EXPECTED FF・IMPLIED CUTS の帯、`stealthLayer1`）、ナビ（全ページの上のメニュー）、新ページ | 帯の表示を新ページへのリンクと「読む側」の値に。ナビの項目が1つ増える（ナビの崩れの確認 `NAV-MOBILE-1` 型） |
| ワークフロー | `MACRO_PULSE_Update.yml`（毎日 22:15 UTC＝normal・13:03 UTC＝fill-returns、土曜 22:07 UTC＝update-schedule・22:11 UTC＝weekly-analysis）、`Macro_Data_Update.yml`（FRED の保存）、**新しい取得のワークフロー（会合ごとの確率）が1つ増える** | 取得の頻度（市場の織り込みは日中に動く）と、外部 API の呼び方（`common/external_api.py` に集約する決まり〈R-E2〉）を決める |
| 監視 | System Health（`common/system_health.py`）、`browser_checks/check_macro_pulse.py` の D-07（FF RATE・1Y EXPECTED FF vs fed_context の最新行）、CHECK-INV（出力の一覧）・`EXTERNAL-SOURCE-CHANGE-WATCH-1` | 新ページの鮮度・取れない日の扱いを足す。D-07 の比べ先が変わる |
| テスト | `tests/test_macro_pulse_logic.py`・`test_macro_pulse_accuracy.py`・`test_fomc_calendar_cache.py` | `get_implied_cuts`・fed_context の列に依存する検査を直す |
| 一覧・文書 | `docs/architecture/output_inventory/macro_pulse_layer.*`・SYSTEM_MAP・`workflow_dependencies.json` | 出力が増える・読み手が変わる |
| 売りの判断 | サテライトの TAIL の `exit_condition`（「利下げ局面での売り」）の検討 | 新ページの完成時（Koichi の方針）。CRWV は当面売らない |

## 5. この調査で見つけたこと（範囲外・直していない）

- **`common/system_health.py:164` が、存在しない旧名 `fed_context.csv`（`05_` の接頭辞なし）を見ていた**（2026-11-28 に `05_fed_context.csv` に直した）。`_git_last_commit_date` が `None` を返す行は `continue` で飛ばされるため、**System Health の「ワークフロー」の項は MACRO PULSE の鮮度を見ていなかった**（他の3つ〈MarketPulse・TANUKI・StonksSilo〉だけ）。直した後は、`05_fed_context.csv` の最後の commit からの日数（7日超で ⚠️）で判定する（11-28 の時点では 11-22 の commit で6日＝✅）。FINDINGS の1行も解消。
- `get_implied_cuts` の docstring・`zq_*` の列・`get_zq_futures` の別名は名残（BACKLOG に既出）。

## 6. 次（STEP 1 の案。着手は Koichi の指示）

1. Kalshi の規約（自動取得・再配布）をチャット側で確認。yfinance の各限月の欠け・更新の遅れを 20営業日ぶん読み取りで確かめる（`common/external_api.py` を通す）
2. 新ページの中身（会合ごとの確率・期待金利の経路・最初の変更の時期）を、独自の予想なしで作れるかを、実データで試算（画面なし）
3. MACRO PULSE を「読む側」にする変更の範囲（regime・cuts_implied・1Y EXPECTED FF の出し所）を決める

[[FED-POLICY-PAGE-1]]・[[MACROPULSE-RECESSION-SCORE-STEPS-1]]・[[MARKETPULSE-REVIEW-EPIC-1]]

## 7. 追記（2026-11-28、Koichi の決定）: 取得元は yfinance の各限月に絞る。Kalshi は使わない

- **Kalshi は使わない**。Developer Agreement で、取得したデータの保存は Kalshi での自分の取引のためだけに限られ、書面の同意なしに第三者と共有することも禁じられている。**公開サイトでの表示は共有にあたる**。§2 の表の Kalshi の行（「認証なしで取得できた」）は事実だが、使えない。
- **取得元の候補は、yfinance の FF 金利先物の各限月（`ZQ=F`・`ZQZ26.CBT` 形式）に絞る**。各限月の欠けと更新の遅れ（20営業日ぶん程度）の確認は、着手するとき（STEP 1）に行う。取れない日は0やスキップで埋めず「取れない」と出す。
- §6 の STEP 1 の1番は「Kalshi の規約の確認」ではなく「yfinance の各限月の確認」に読み替える。独立した2つ目の取得元は、今は持たない（FRED の DGS1 は 1年のレートで会合ごとではないため、2つ目の取得元にはならない）。
