# MARKETPULSE-REVIEW-EPIC-1 STEP 0（2026-11-27、読み取りと手元の試算のみ。コード・データは変えていない）

依頼: BACKLOG の9項目（1-a〜3-b）のそれぞれについて、原因・直し方の案・効く画面と出力・費用を出し、同じ型をまとめ、[[FED-POLICY-PAGE-1]]・[[MACROPULSE-RECESSION-SCORE-STEPS-1]]・[[HYPECORE-MARKET-WIDE-DRAWDOWN-1]]・[[MACROPULSE-DATA-LAG-AND-REGIME-DRIFT-1]]・画面の要望④（`MACRO-PULSE-CONCEPT-SPLIT-1`、MACRO PULSE の整理）と束ねられるものを示し、進める順の案と決めてほしいことを出す。

**確かめた範囲**（各項目の「確認」欄に印）: ◎＝この STEP 0 でコード・データを読んで再確認、○＝コードを読んだが数字は BACKLOG の記述のまま、△＝BACKLOG の記述のまま（再確認していない）。**効きの試算（バックテスト）はしていない**。直し方の案の効果は、着手時に履歴（`market_data.json` の197エントリ・`common/market_data/daily/`）で確かめる。
費用: S＝半日以内（表示・文言・1か所の定数）、M＝1〜2日（出力のキー・計算を変える。試算つき）、L＝3日以上（複数の読み手・画面に及ぶ）。

## 0. 先に分かったこと（BACKLOG の記述と違う所・新しい事実）

1. **3-a の大きさは BACKLOG と違う**: ファイルは13.2MB（内容は10.7MB・197エントリ）。うち **`comments_history` が6.0MB（56%）**（BACKLOG は2.6MB）、`sector_rotation` 1.3MB（12%）、`summary` 0.7MB、`headlines` 0.7MB。1エントリ約57KB。日々増える主因は `comments_history`（毎日の要約を丸ごと複製）。
2. **1-a の日経の遅れは「1営業日ずつ古い」で一定**（◎）: `market_data.json` のエントリ（朝06:31JST）は 11-21 に 59,444（＝11-19 の日足）、11-25 に 56,436（＝11-20 の日足。11-24 のエントリと同じ値の再掲）、11-26 に 58,644（＝11-24）。`daily/^N225.json` の最新は 11-24 で、11-25・11-26 の行が無い。つまり**直近の日本の取引日の日足が、翌朝の定時の取得でもまだ保存されていない**（`is_provisional_bar` が `^N225` の出来高0の足を暫定扱い）。
3. **同じ「古い日付を黙って使う」型が、日経・HYスプレッド・FRED系列・S&P500（MACRO PULSE の流動性）に共通**（1-a・3-b・[[MACROPULSE-DATA-LAG-AND-REGIME-DRIFT-1]]）。画面の鮮度判定（`DQ_INDICATORS`）が手書きの一覧で、入力の一覧と別に管理されている。

## 1. 9項目

### 優先度1

| 項目 | 原因 | 直し方の案 | 効く画面・出力 | 費用 |
|---|---|---|---|---|
| **1-a 日経が毎日1日遅れる** | ◎ 上の §0-2。日本の取引日の日足の確定が翌日以降で、定時の取得が間に合わない。`get_realtime_data` は最後の2件の「closeがある行」を使うので古い日付を**黙って使う**。`stage_conclusions.DQ_INDICATORS` に日経が無く、`data_quality` が拾わない | ①日経の「日付」を表示に出し、`DQ_INDICATORS`（と画面の鮮度の印）に加える（型L の根治策）。②日経は「前日の日本の引け」と明示（日本の暦で1日遅れは構造的）するか、取引時間後の遅い取得（日本の引け＋翌朝のYahoo確定）の回を足して1営業日遅れをなくす。②の取得の回は、Market Data Daily の既存の暫定の仕組み（`provisional_final`）の延長で足りるか着手時に確かめる | `market_data.json` の `indicators.日経平均`・`data_quality`、画面の日経の行、段階6の指数の読み、AI の要約（`ai_facts`）、Tech Pulse（`^N225` を読まない見込み。着手時に確認） | **S**（①）／**M**（②） |
| **1-b BUYチェックリスト** | ◎ `calc_buy_checklist`: ③は `hindenburg_active` が False なら1点、②は `hy_current < hy_max_90d − 0.30`（現在値と**90日最大**の差だけで、方向・直近の山を見ない）。発動はF&G≤25、2点以上で「BUY」。F&G9.6では③が実質タダの1点になり、①＋③で「BUY」が出る | ③を発動条件から除く／点数を下げる（オーメンは上昇トレンドが前提のため、下落局面では情報が無い）。②は「直近のピーク（例: 20営業日の最大）から縮小」かつ「直近数日が縮小方向」へ。文言（画面）と判定を**同じ定義から作る**（型E） | `buy_checklist`（`collect_and_send.py`）、`index.html` の `renderBuyChecklist`、Discord/メールの要約、AI の要約。**買いの判断の根拠**。TANUKI への読み手なし | **S〜M**（配点の変更は小。②の定義を変えるときは過去の発動日で試算） |
| **1-c 「資金が向かっているセクター」** | ○ `sector_rotation.py` の象限は RS（セクター÷SPY）の RS-Ratio・Momentum（境目100）で、**SPYとの相対**。全体が下げる局面でも、SPYより下げが小さければ「Strong」（11/20は11業種中9がStrong、5日騰落は全業種マイナス） | 文言を「SPYより相対的に強い」に。象限と**実際の1日・5日の騰落**を並べて出す | 段階5・7の文言、`watch_list`、`ai_facts`（AIの要約・俳句）、画面の4象限の表 | **S** |

### 優先度2

| 項目 | 原因 | 直し方の案 | 効く画面・出力 | 費用 |
|---|---|---|---|---|
| **2-a 段階8「明日どうなるか」** | ◎ `S8_HEADLINE_SIGNAL = "stage1"`（`stage_conclusions.py:32`）固定。条件が当日のS&P500の騰落5区分だけで、同じ区分なら毎回同じ答え。`stage1_vix`（VIX組み合わせ）は計算済みだが見出しに使われない。題名「明日」に対し見出しは5営業日後。窓が重なる5日・20日の z は過大 | 題名を「過去の似た日、その後どうなったか」に（型E、**S**）。条件に局面（VIXの帯・F&Gの帯・200日線の上下・高値からの下落率）を足し、件数が足りる最も絞った条件を見出しに（**M**）。1・5・20日を並べ、上昇割合＋平均＋下振れ。重なる窓の z は件数を窓で割った実効件数で出す | `stage_conclusions.stage8`、`stage_conclusions` の `8`、画面の段階8、AI の要約（引き継ぎ）。**買いの判断の根拠** | **S**（題名）＋**M〜L**（再設計。件数の確かめと履歴の再計算） |
| **2-b セクター4象限の基準** | ◎ `sector_rotation.py` の RS＝セクター÷**SPY**（情報技術が約3割）。XLKが崩れると他業種は機械的に強く出る。境目は100ちょうどで余裕の幅が無い（XLC 101.0でStrong） | 基準をRSP（等ウェイト）か11業種の単純平均に。境目に幅（例: 99〜101は据え置き＝型Pのヒステリシス）。段階5の1行で1日のG/Vと13週の象限を分ける | 段階5・7の文言、`watch_list`、`ai_facts`、画面の4象限の図。段階8・センチメント指数は影響なし | **M**（基準の変更で履歴の軌跡〈`sector_rotation` 1.3MB〉を作り直すか、切り替え日から新基準にするかを決める） |
| **2-c 「買い」を出す仕組みが3つ** | △ センチメントのシグナル（自前指数≤20）・BUYチェックリスト（F&G≤25）・Tech Pulse の反発シグナルで指数も閾値も別。BACKLOG の「14日のうち10日がHOLD」は再確認していない | 主にする仕組みを1つ決め、指数と閾値を揃える（決めるのは Koichi）。他は参考表示に降ろす | 画面の買いの表示3か所、Discord/メール、AI の要約、`score_history`（TANUKI は読まない見込み）。**買いの判断の根拠** | **M**（決めた後）。決めるまでは作業なし |
| **2-d センチメント指数** | ○ `compute_sentiment`（8指標、docstring にウェイトと上下限）。上下限で切る直線換算のため、極端な局面で6/8指標が0に張り付き、極端どうしを区別できない。RSP/SPY乖離（重み10%）は「RSP優勢＝強気」で加点するため、大型株主導の下落でも強気側に出る。期間が1・5・20・50日で混在 | 上下限の外も区別する（例: 履歴のパーセンタイル、または上下限を履歴の分布から導く）。RSP/SPY乖離の向きを見直す（大型株主導の下落は強気としない）。期間の混在は文書化か揃える | `sentiment`（`market_data.json`）、画面のセンチメント、`signal`（BUY/TAKE PROFIT）、AI の要約。**買いの判断の根拠**。履歴197日の指数が全部変わる（作り直すか切り替え日を残すか） | **M〜L**（履歴の作り直しと買いのシグナルの再確認） |

### 優先度3

| 項目 | 原因 | 直し方の案 | 効く画面・出力 | 費用 |
|---|---|---|---|---|
| **3-a `market_data.json` が大きい** | ◎ §0-1。`comments_history`（各エントリが直近12件の要約を丸ごと持つ）が56%、`sector_rotation` の軌跡（毎日8週分の複製）が12% | 最新用 `latest.json`（画面の初期表示）と履歴用に分ける。`comments_history` は日ごとに1回だけ保存（複製をやめる）。`sector_rotation` の軌跡は最新だけを持ち、履歴は週次の値だけ | 画面の読み込み（`index.html`）、Tech Pulse の履歴読み込み、`_load_*` 関数群、**TANUKI の `_load_live_fg`（`pipeline.py`）・`tanuki_score/index.html`**、`portfolio/snapshot.py`、`system_health.py`、`headlines_*`、`browser_checks/`。読み手が10か所以上で、出力の形を変える | **L**（読み手の洗い出しと各画面の確認。出力の値は変えない） |
| **3-b 軽微（7件）** | △ ①Tech Pulse の `fg_score` が CNN と別の取得元 ②Tech Pulse が過去90日の順位と絶対水準のF&Gの差で反発を出す ③DGS3MO を価格の向きで表示（上昇の意味が逆）・FRED系列が鮮度判定の対象外 ④HYスプレッド（`BAMLH0A0HYM2`）が日付を保存しない（FREDの1日遅れが見えない） ⑤クレジット判定の閾値なし・株の二択 ⑥エントリの日付が実行日で終値日と1日ずれる・センチメントの窓も1日ずれる ⑦天気が下落幅を見ない | ③④⑥は型L（日付の持たせ方と鮮度の一覧）で一緒に直す。①②は表示と計算の整合（型E）。⑤⑦は型P（閾値・段）。⑥は `date` を終値日にし、実行日は別のキーに | 各項目ごと（`tech_pulse`・`credit`・`asset_flow`・`weather`・`data_quality`）。⑥は `market_data.json` の `date` を読む全部（画面・`score_history`・TANUKI の `_load_live_fg`・TAIL）に及ぶので**M〜L** | **S〜L**（項目ごと。⑥が重い） |

## 2. 同じ型でまとめる（共通の根治策を先に）

| 型 | 含まれるもの | 共通の根治策（案） |
|---|---|---|
| **L 日付・鮮度（古い値を黙って使う）** | 1-a、3-b③④⑥、[[MACROPULSE-DATA-LAG-AND-REGIME-DRIFT-1]]（S&P500が1日遅れ・FOMC声明の局面の判定の揺れ〈取得の日付は別の話〉）、[[MACROPULSE-RECESSION-SCORE-STEPS-1]] の「8指標の取り込みの遅れ（`known_at` と発表日の差）の点検」 | **すべての入力が「値の日付」を持ち、鮮度の一覧（`DQ_INDICATORS`・画面の鮮度の印・`System Health`）を入力の一覧から機械で作る**。手書きの一覧に日経・FRED系列・HYスプレッドが無いことが、遅れが見えない共通の原因。日付を持たない入力は「日付なし」と表示する |
| **P 段・閾値の切り替わり** | 1-b（90日最大−0.30、配点の段）、2-b（境目100）、2-c（閾値がばらばら）、2-d（上下限0に張り付き）、3-b⑤⑦、[[MACROPULSE-RECESSION-SCORE-STEPS-1]]（境目30で27→33）、[[HYPECORE-MARKET-WIDE-DRAWDOWN-1]]（stage≥3の段で TRIM・`_calc_timing` がF&G<30で一律+40） | **段の境目の扱いを1つの部品にする**（境目の幅〈ヒステリシス〉か直線補間。`common/dilution.py::dilution_penalty` の「傾斜」が前例）。閾値と文言を1つの設定（定義）に置き、**画面の文言を定義から作る**（型E と共通）。個別の閾値の値は変えず、まず部品を入れて前後の履歴で比べる |
| **E 説明と計算のずれ** | 1-b（文言「縮小に転じているか」対 判定「90日最大−0.30」）、1-c（「資金が向かっている」対 SPYとの相対）、2-a（題名「明日」対 5営業日後）、3-b①②、[[FED-POLICY-PAGE-1]]（「1Y EXPECTED FF」対 DGS1）、[[MACROPULSE-RECESSION-SCORE-STEPS-1]]（AIの説明が寄与0の指標を理由に挙げる） | **文言は判定の定義から作る**（定義に「題・単位・基準」を持たせる）。AI の要約には**確定した点数・寄与を渡す**（Market Pulse の `ai_facts` は確定した事実だけを渡す作りで「問題なし」と点検済み。MACRO PULSE の `05_weekly_analysis` は渡していない） |
| **S サイズ・構造** | 3-a、`_load_live_fg`（TANUKI が F&G 1値のために12.9MBを読む） | 最新用と履歴用の分割。型 L・P の変更で出力のキーが増える前にやると、前後の比較（`market_data.json`）が軽い |

### 既存・今日登録した項目との束ね

| 項目 | EPIC のどれと束ねられるか | 束ね方 |
|---|---|---|
| [[MACROPULSE-DATA-LAG-AND-REGIME-DRIFT-1]] | 型L（1-a・3-b）と同じ根治策 | 日付と鮮度の部品を Market Pulse・MACRO PULSE で共通にする。局面の判定の揺れ（同じ声明で BALANCED と TIGHTENING）は [[FED-POLICY-PAGE-1]] の影響範囲に入る |
| [[MACROPULSE-RECESSION-SCORE-STEPS-1]] | 型P（段の切り替わり）・型E（AIの説明）・型L（`known_at` の遅れ） | 型P の部品（ヒステリシス・補間）の最初の使い道に向く。境目30（拡張→踊り場）で試せる |
| [[HYPECORE-MARKET-WIDE-DRAWDOWN-1]] | 型P（stage≥3の段・`_calc_timing`の一律+40） | 部品は同じだが**TANUKI SCORE の判定が変わる**ため、EPIC とは別に計算方法の変更の手順（scope・印）で行う。EPIC には含めず、部品ができた後に着手する順にする |
| [[FED-POLICY-PAGE-1]] | 型E（題と中身のずれ）・型L（取得元・日付）・型P（Grok の局面の判定にDGS1を入れる） | 画面の分割（下の `MACRO-PULSE-CONCEPT-SPLIT-1`）の後。MACRO PULSE の金利の表示は読む側になる |
| `MACRO-PULSE-CONCEPT-SPLIT-1`（画面の要望④、IDEAS_AND_WATCH.md、MACRO PULSE の整理） | MACRO PULSE 側の入れ物 | MARKET PULSE の型 L・P・E の部品を MACRO PULSE にも入れる前に、**MACRO PULSE の分け方（景気後退の予測／足元の見方）を先に決める**。[[FED-POLICY-PAGE-1]]・[[MACROPULSE-RECESSION-SCORE-STEPS-1]] はその分け方に従う |

**EPIC に束ねない**もの: [[FED-POLICY-PAGE-1]] の新ページの作成（新機能）、[[HYPECORE-MARKET-WIDE-DRAWDOWN-1]]（TANUKI の判定の変更）。

## 3. 進める順の案

共通の根治策（型）を先に、項目ごとの個別の直しを後に。

| 順 | 内容 | 含まれる項目 | 費用 | 理由 |
|---|---|---|---|---|
| 1 | **型L: 日付と鮮度の部品**（入力に値の日付を持たせ、鮮度の一覧を入力から作る）。日経の日付表示・`DQ_INDICATORS` への追加を最初の使い道に | 1-a①、3-b③④、MACROPULSE-DATA-LAG の取得日（MACRO PULSE 側） | S〜M | 出力の値を変えず（日付を足すだけ）、ほかの変更の前提（遅れを見える化）になる。1-a②（日経の取得の回）はこの後に判断 |
| 2 | **型E: 文言を定義から作る**（題・基準の併記）。1-c・2-a の題名・1-b の②の文言 | 1-c、2-a（題名）、1-b（文言）、3-b①② | S | 表示だけで、判定・出力のキーは変えない。順1の部品と同時でもよい |
| 3 | **型S: `market_data.json` の分割**（値は変えず形だけ） | 3-a、`_load_live_fg` | L | 以降の型P の変更は履歴の再計算を伴うので、先に軽くしておくと前後の比較が楽。**ただし読み手が10か所以上**で、順1・2の出力のキー（日付）が入った後の形に合わせたい |
| 4 | **型P: 境目の部品**（幅・補間）を作り、最初に使う場所を1つ決めて前後で履歴を比べる | 2-b（境目の幅）、2-d、1-b②、[[MACROPULSE-RECESSION-SCORE-STEPS-1]]（境目30） | M〜L | 買いの判断に効く。部品を先に作り、閾値の値は変えずに前後を比べる。最初の使い道は2-b（買いに直結しない）か RECESSION-SCORE |
| 5 | **2-c: 「買い」の仕組みの統一** | 2-c、1-b（③の配点） | M | 順4の部品ができた後、主にする仕組みを決める。Koichi の決定が要る |
| 6 | **2-a: 段階8の再設計**（局面の条件を足す） | 2-a（再設計） | M〜L | 履歴の件数の確かめが要る。型P・L の後 |
| 7 | MACRO PULSE の分け方（`MACRO-PULSE-CONCEPT-SPLIT-1`）の決定 → [[FED-POLICY-PAGE-1]] | | | 着手条件に既に書かれた「画面の要望④と一緒」 |
| 別 | [[HYPECORE-MARKET-WIDE-DRAWDOWN-1]] | | M | 型P の部品の後。計算方法の変更の手順で別に |

## 4. 決めてほしいこと

- **(a)** 進める順（上の 1→2→3→4→5→6）でよいか。特に**順3（分割）を順1・2の後**に置いてよいか（先に分割すると日付のキーを足すときに形を2回変える）
- **(b)** 1-a は①（日付を表示・鮮度の対象に）だけでよいか。②（日経の引け後の取得の回を足して1営業日遅れをなくす）まで求めるか。②は Market Data Daily の取得の回が増える（System Health の監視も増える）
- **(c)** 2-c で「買い」の主にする仕組みはどれか（センチメントのシグナル・BUYチェックリスト・Tech Pulse）。決めるまで順5は作業しない
- **(d)** 2-b・2-d の履歴の扱い（切り替え日から新基準にする／過去を作り直す）。作り直すと画面の履歴・score_history の過去の値が変わる
- **(e)** [[HYPECORE-MARKET-WIDE-DRAWDOWN-1]] を EPIC の外（TANUKI の計算方法の変更）にしてよいか
- **(f)** MACRO PULSE の分け方（`MACRO-PULSE-CONCEPT-SPLIT-1`）を、[[FED-POLICY-PAGE-1]]・[[MACROPULSE-RECESSION-SCORE-STEPS-1]] の前に決めてよいか（MACRO PULSE 側の型 L・P・E の部品の入れ先が決まる）

## 5. 限界

- 直し方の効果（買いの判断が変わる日の数・センチメント指数の変化）は**試算していない**。順4・5・6 の着手時に、履歴197エントリと `daily/` で前後を比べる
- 1-b・2-c・2-d・3-b の数字（発動14日のうち10日がHOLD・8指標中6つが0・各ウェイト）は BACKLOG の記述のまま（○△）。着手時に再確認する
- 1-a の原因は §0-2 のとおり日足の保存の遅れまでは確かめたが、**なぜ翌朝の定時の取得でも11-25・11-26の行が入らないか**（Yahooの確定が遅い・保存の条件のどちらか）は、`Market Data Daily` の実行ログで確かめていない
- 読み手の洗い出し（3-a）は `market_data.json` を参照するファイルの一覧だけ（`grep`）。各読み手が読むキーは着手時に洗い出す

## 6. 決定と順1の結果（2026-11-27）

**決定**（§4 の (a)〜(f)）: (a) 順は案のとおり（L → E → S → P → 2-c → 2-a） (b) 1-a は①（日付の表示・鮮度の監視）だけ、取得の回は足さない。ただし ^N225 の行が入らない原因は L の中で確かめて直す (c) 「買い」の主は BUY チェックリスト（F&G）。自前の指数・Tech Pulse は参考の表示（順5で実装。今は記録だけ） (d) 履歴は作り直さず、切り替えた日から新しい基準にし、画面のグラフに基準の変更の線を引く (e) HYPECORE-MARKET-WIDE-DRAWDOWN-1 は EPIC の外 (f) MACRO-PULSE-CONCEPT-SPLIT-1 は 11-22 の Koichi の決定（MACRO PULSE は景気後退の予測に特化、政策金利は FED-POLICY-PAGE-1 へ）で答えとする。

**順1（型L）の結果**:
- **^N225 の原因**（確かめた）: ①Yahooの ^N225 の日足は、引け（15:30 JST）の約15時間後〈定時の取得 06:28 JST〉にも終値が無く、約39時間後には入る（実測: 11-24・11-25 の足が定時の取得で「終値が無い（未確定のため保存せず、次回取り直す）」→ 11-24 は翌日の定時の取得〈38.9時間後〉で保存、11-25 は 11-27 09:44 JST に終値あり）。構造上の遅れで、保存の条件の誤りではない。②ただし「次回取り直す」は、米国の休場日（11-26 感謝祭）と、米国の終値がそろった日には `daily_guard` が起動を止め、取り直しが無かった（`completeness` が ^N225 を数えない）。→ **`daily_guard` に、期待する日本の取引日の行が未保存なら起動する条件を足した**（期待する日から4日以内。Yahooに足が無い日で起動し続けない）
- **部品**: `common/value_freshness.py`（kind: us_close・jpx〈Yahooの確定の遅れ39時間を見込む〉・fred_daily〈FREDの1営業日遅れ〉）。`judge()` が fresh・stale・unknown（日付なし）を返す1か所。Market Pulse の `data_quality`（日経・HYスプレッドを含む。新しくない要素だけを `freshness` に、期待する日を `expected_by_kind` に）・画面・System Health [Q]・MACRO PULSE の `05_liquidity.csv`（`sp500_asof`・`hy_spread_asof`）が同じ部品を読む。BUYチェックリスト・TAKE PROFIT の HYスプレッドに `as_of` を保存
- **画面**（Playwright で確認）: 6枚のカードに値の日付（MM/DD）、古い値に「古い」の印とツールチップ（期待する基準日と種類）、段階0の表に日経・HYスプレッドの行（期待する日つき）、BUYチェックリストのHYスプレッドに日付
- **効かないこと**: 理論株価・判定・funda・HypeCore の stage・TANUKI（全銘柄の前後比較は不要〈TANUKI・HypeCore のコードを変えていない〉。Market Pulse の判定〈天気・段階1〜8・センチメント〉は入力を変えていない）。変わるのは `data_quality` の判定の仕方だけ（日経の遅れを見込み、HYスプレッド・日経が古ければ partial になる）
- 確認は `CLAUDE_CODE_START.md` 手順48

## 7. 順2（型E）の結果（2026-11-27）

- 文言を定義から作った: 段階5・7を「SPYより相対的に強い（Strong）」＋業種の実際の1日・5日の騰落（`sector_rotation` の `change_5d_pct`、画面のセクター表に5日の列）、段階8の題名を「過去の似た日、その後どうなったか」（再設計は順6）、BUYチェックリスト②の文言を判定と同じ閾値の定数から（判定は変えない）、F&Gの表示に取得元（CNN／feargreedchart.com）
- AIの入力: MACRO PULSE の週次AI解説に8指標の点数と寄与（`score_contributions`。合計＝スコアの変化）を渡し、寄与0の指標を理由に挙げさせない（挙げたら1回だけ言い直させる）。Market Pulse の見解にセンチメント指数の内訳（寄与pt。合計＝指数−50）を渡す
- 触っていない: FED-POLICY-PAGE-1 の部分（金利見通しの表示）。理論株価・判定に効かない。確認は `CLAUDE_CODE_START.md` 手順49

## 8. 順3（型S: market_data.json の分割）の結果（2026-11-27）

### 8-1. 読み手（`market_data.json` を読むファイルと、読むキー）

| 読み手 | 読み方 | 読むキー |
|---|---|---|
| `docs/market-monitor/market-pulse/index.html`（画面） | 全エントリ（履歴）を1回で読む（`.キー` の使われ方をコードから数えた） | 履歴で読む: `date`・`judgment`・`weather`・`sentiment`・`fear_greed`・`tech_pulse`・`indicators`・`asset_flow`・`credit`・`stage_conclusions`・`data_quality`・`summary`・`haiku`・`take_profit_checklist`・`buy_checklist`・`data_freshness`・`semis_m7`・`commodity_fx`。**最新のエントリだけ**: `sector_rotation`（`trail` を含む）・`watch_list`・`headlines`・`calendar`・`futures`。**画面は読まない**: `ai_facts`（0.16MB）・`hindenburg`（画面の計算には `breadth` を使う）。旧キー `comments_history` は読まない |
| `docs/value-monitor/tanuki_score/index.html` | 全体を読み最新のエントリだけ使う | `fear_greed.score` |
| `src/value/tanuki_valuation/pipeline.py::_load_live_fg` | 全体を読み最新のエントリだけ使う | `fear_greed.score` |
| `src/portfolio/snapshot.py` | 全体を読み最新のエントリだけ使う | `indicators.ドル円.value`・`indicators.S&P500.value` |
| `src/market/market_pulse/collect_and_send.py`（書き手） | 全体を読み、追記して書く（書き手は1か所） | 前のエントリ: `indicators`・`asset_flow`（取得失敗の引き継ぎ）、`tech_pulse`・`sentiment`・乖離の履歴（直近90日）。**`comments_history` は読まない** |
| `common/system_health.py` | [D] はファイルの最終コミット日、[Q]（順1）は最新のエントリ | `data_quality.as_of`・`expected_close_date`・`data_freshness.checked_at` |
| `browser_checks/market_pulse_elements.py`・`check_dependency_map.py` | 最新のエントリ | 画面の全要素の突き合わせ（`sentiment.sub_scores`・`sector_rotation.trail` を含む）、`fear_greed.score`・`sentiment.breadth` |
| `scripts/analysis/market_pulse_redesign.py` | 設計書作成時の commit のファイル（git show） | 変更の影響なし |
| rcc（`report_consistency_check.py`） | 読まない | — |
| テスト | `tests/test_collect_and_send_market_data_switch.py`・`test_market_data_comments_history.py`・`test_value_freshness.py` ほか | 書き手の出力・移行・鮮度 |

**`comments_history` を読むのは画面だけ**（`buildCommentsHistory`）。Python・System Health・rcc・ブラウザの検査は読まない。

### 8-2. やったこと

- **別のファイルに移すのではなく、持たないことにした**: `comments_history` は「そのエントリの `summary` ＋ 直前の最大11エントリの `summary`（新しい順）」の複製で、**保存済みの130エントリすべてで、前のエントリの `summary` から作った履歴と1件ずつ一致した**。別のファイルに移すと同じ `summary`（0.7MB）をもう1か所に持つことになるので、画面が前のエントリから作る（`commentsHistoryOf`）。履歴のキーは各エントリの `date`（実行の日時。順1の鮮度の部品が見るのは値の日付で、この履歴は見ない）
- 書き手（`collect_and_send.py`）は旧キーを書かない。旧エントリの旧キーは `scripts/migrate_market_data_comments_history.py` が、保存済みの履歴と作った履歴が1件でも違えば何も消さずに止まる形で消す（130エントリ・13.2MB → 7.2MB。ほかのキーは変わらない）
- 画面は前のエントリから作る。旧キーの読みは入れていない（同じ push で旧キーを消すので読む場面が無く、書き手のないキーを画面が読むと出力の一覧の検査 INV-5 が NG になる）。**履歴を持っていた最初のエントリ（2026-06-24）より前は、これまでどおり履歴なし**（表示を変えないため。前のエントリが有るのに履歴を出さない67エントリがある）

### 8-3. 前後の確認（Playwright、HEAD の画面＋HEAD のデータ 対 新しい画面＋旧キーを消したデータ）

| 見るもの | 結果 |
|---|---|
| 全197エントリの「過去のAIコメント」の表示（`buildCommentsHistory`） | **197/197 同じ**（履歴あり130・なし67） |
| 詳細の表示・6枚のカード・段階0の表・段階の結論の行 | **同じ** |
| ページの読み込み（画面の最初の描画まで、5回の中央値、手元のサーバー） | **921〜1003ms → 611〜657ms**（2回の測定）（ファイル 13.2MB → 7.2MB。回線では転送の差がさらに出る） |
| エラー | 前後とも0 |
| Market Pulse の判定・出力 | 変わらない（書き手が旧キーを足さないだけ。ほかのキーは `new == old − comments_history`） |

### 8-4. ほかに分けたほうがよい大きな部分（案。今回は実装しない）

旧キーを消した後の中身は 4.7MB（ファイル 7.2MB。書き込みの `indent=2` の空白が約2.5MB）:

| 部分 | 大きさ | 画面の読み方 | 案 |
|---|---|---|---|
| `sector_rotation`（うち `trail` 1.05MB＝毎日8週分の軌跡） | 27%（1.28MB） | **最新のエントリだけ**を読む | 古いエントリでは `trail` を持たない（最新だけ）。象限の履歴が要るなら週次の値だけ。1.05MBの減 |
| `headlines` | 15%（0.68MB） | **最新のエントリだけ**を読む（別に日ごとの積み足し `headlines/{date_et}.json` がある） | 古いエントリでは持たない |
| `summary` | 15%（0.73MB） | 履歴で読む（履歴の表示の元） | 残す |
| 書き込みの形 | 空白 約2.5MB | — | `indent=2` をやめる（差分は読みにくくなる。gitの履歴の差分に影響） |
| 画面が読まないキー | `ai_facts` 3%（0.16MB） | 読み手なし（AIの入力の記録。再現・点検用） | 残す理由を決める（`ai_facts` は AI の見解の根拠を後から確かめる記録） |
| 最新用のファイル | — | tanuki_score・pipeline・snapshot・System Health・検査は**最新のエントリだけ**のために 7.2MB を読む | `latest.json`（最新のエントリだけ、数十KB）を同じ書き手が書き、これらの読み手が読む。画面の履歴は今のまま |

これらは読み手の変更（`trail`・`headlines` の履歴を読む所が無いことの確認、`latest.json` への切り替え）を伴うので、決めてもらってから進める。

### 8-5. 順3の残り: latest.json と古いエントリの trail（2026-11-27）

- **latest.json**（最新のエントリだけ、76KB）: 同じ書き手（`save_data_to_json_and_csv`）が履歴と一緒に毎回書く。最新だけを読む所を移した: TANUKI SCORE の画面（`fear_greed`・`indicators`・`credit`）・`pipeline._load_live_fg`・`snapshot.py`・System Health [Q]・`check_dependency_map.py`。共通の読み口は `common/market_pulse_data.py::load_latest`（latest.json が無ければ market_data.json の最後のエントリ）。画面（Market Pulse）と `market_pulse_elements.py` は履歴を読むので market_data.json のまま。TANUKI SCORE の画面は 442ms → 248ms（5回の中央値）・ページの文字は同じ・エラー0
- **`sector_rotation.trail`（1.05MB）は消した**: 最新以外のエントリの trail は、日足から `sector_rotation(as_of)` で作り直した trail と**197エントリ・1,595セクターすべてで一致**した（消す前の検査。移し替えのスクリプトは1件でも違えば、そのエントリの trail を残す）。象限・RS-Ratio・Momentum・騰落（1日・5日）は残る。読み手は、画面と検査が最新のエントリだけから読むだけで、MARKETPULSE-MOVE-POSTMORTEM-EPIC-1 の実装済みの部分（1/8 予定・2/8 CNBC・3/8 見出しの保存・4/8 5分足）は過去の日の trail を読まない。残り（6/8〜8/8 の材料・Grok・別ページ）は、過去の日のセクターを使うなら象限と騰落（残してある）か日足から作り直す。書き手は以後、書くたびに最新以外の trail を消す。market_data.json 7.2MB → 5.1MB
- **`headlines`（0.68MB）は残した**: 過去のエントリの `headlines` を読む所は無い（画面は最新のエントリだけ）が、**消すと情報が無くなる**。日ごとの積み足し（`headlines/{date_et}.json`、2026-10-28〜）と照らすと、エントリの記事1,344件のうち840件は積み足しにもあるが、`title_ja`（Grok の訳）・`market`（関連の判定）・`region` は積み足しに無い、または違う（訳170件・題名66件・時刻142件が違う）。10-07〜10-27 のエントリ（15）は積み足しが無く、記事はエントリにしか残っていない。消すのは、訳・判定を積み足しに持たせてから
- `indent=2` はそのまま
