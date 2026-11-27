# 計算方法の変更の通知（🛠）の内訳の照合（2026-11-27、METHOD-CHANGE-NOTICE-ACCURACY-1）

依頼: ここ2週間の公開（PARSER段1〜UPSIDE・STONKS ①）の通知の内訳を、実際の変化と照らし合わせ、誤りの件数と型を出す。直し方の案は小さく直す（通知の表示だけ。`config/method_change.json` の値は変えない）。

## 方法

Discord の通知本文は残っていないので、**通知の組み立て（`score_watcher.detect_changes`）をそのまま再生した**。2026-11-16〜11-26 の TANUKI VALUATION の実行23回（`.watcher_state.json` を更新した commit）について、前の回の状態と、その回の `latest.json`・HypeCore の `{T}_poc.json`（commit 時点）を渡し、🛠 の節に入る変化（`method_change` が付くもの）と「計算方法の分／残り」を出した。さらに、公開の直後の最初の実行で、**scope の全銘柄・全項目**の「試算（after）」を公開の値と比べた。再現は scratchpad の `mc/replay.py`・`mc/scope_acc.py`（`git show` で commit ごとのファイルを読む。SEC・Grok は呼ばない）。

## 結果

| 公開（ID） | 最初に印が付いた実行 | scope | その実行の通知 | 🛠 の節 | scope の項目の試算と公開の差 |
|---|---|---|---|---|---|
| PARSER-LAYER3-SINGLE-SOURCE-1-1（11-16） | a6775aa236 | 17銘柄・239項目 | 1件 | 1（CAKE 乖離率 −64.5% → −54.2%） | 0 |
| TANUKI-CALC-CONTINUITY-1-1（11-17） | 1ebb202884 | 16銘柄・416項目 | 1件 | 1（DOCN −18.3% → −29.0%） | 0 |
| DILUTION-DEFINITION-UNIFY-1-1（11-18） | d27c4aad12 | 90銘柄・328項目 | 7件 | 7（funda 7銘柄） | 0 |
| TANUKI-SHARES-SINGLE-SOURCE-1-1（11-23） | cffabe2a51 | 96銘柄・1,922項目 | 6件 | 6（乖離率5・判定1） | 0 |
| SEC-RETAG-RECONCILE-1-1（11-24） | 5d8c720e42（DOCN・COHR に印） | 5銘柄・28項目 | 0件 | 0（変化が通知の閾値〈乖離率10pt・funda10pt・判定・stage〉より小さい） | 0 |
| TANUKI-UPSIDE-MAGNITUDE-INFLATED-1-1（11-25） | f841ee0fb5 | 22銘柄・726項目 | 14件 | 14（乖離率11・判定3） | 1（META の `timing_score` 70 対 公開65） |
| STONKS-DEFICIT-REALITY-EPIC-1-1（11-26） | 1a15b9c5bf | 17銘柄（STONKS の `runway.*` 等・TANUKI の `computed_runway_months`） | TANUKI SCORE の通知の項目（判定・乖離率・funda・stage）に変化なし → 通知なし（scope は `computed_runway_months` と STONKS の `runway.*`・`summary` で、通知の項目に入らない） | 0 | 公開の最初の実行は `.watcher_state.json` が変わらず commit が無いため再生の対象外。STONKS 側の項目は照合していない |

- 再生した23回の通知の変化の合計は **205件**。🛠 の節に入ったのは **29件**（すべて上の公開の最初の実行）。🛠 の外（相場の節）に印のある銘柄の scope 外の変化が混ざった例は**0件**、🛠 に入った変化の「残り」は29件とも **+0.0**（試算＝公開）
- scope の項目（3,659項目）の試算と公開の差は **1件だけ**: META の `timing_score`（試算 70・公開 65）。`timing_score` は株価で決まる項目で、試算は株価を固定しているため、公開の実行の株価の動きの分が出る（相場の分。計算方法の分ではない）

## 誤りの件数と型

**この2週間の通知に、🛠 の内訳の誤りは0件**だった。BACKLOG に登録した2つの型（2026-11-03 の発見）は、この期間には起きていない:

| 型 | 内容 | この2週間 | 理由 |
|---|---|---|---|
| (1) 古い形の印 | `{id, date}` の古い形はその日の全銘柄に印を付けるため、新しい件を足すと Q3 の10-Q・相場の変化まで 🛠 の下に入る | **0件** | 古い形の件は9件で、すべて 2026-11-02 まで。11-06 以降の13件はすべて新しい形（scope つき）。ただし**古い形を足すことを止める仕組みは無かった** |
| (2) 試算の EPS が公開と違う | 試算は SEC・Grok・Alpha Vantage を呼ばない。EPS が動く変更では試算の funda・理論株価が公開と違い、「計算方法の分／残り」が誤る（SOUN: 試算 60→50・実際 60→35。「残り −15」は相場ではなかった） | **0件**（この期間の公開に EPS が動くものは無い） | scope の項目の試算と公開の差が0。ただし**型は残っている**（次に EPS が動く変更で再発する）。funda は株価に依らないので、funda の「残り（相場等）」は常に試算と公開の違いで、相場ではない |

## 直した（通知の表示と形の検査だけ。`method_change.json` の値は変えない）

1. **形の検査**: `common/method_change.py::validate_changes`（古い形は 2026-11-02 まで・新しい形は `committed_at` と scope の形・同じ ID の新しい形の重複）。`scripts/method_change_scope.py --write` が足す前に検査し、崩れていれば止める。`tests/test_method_change.py` が現在の設定ファイルを検査する
2. **「残り」の読み分け**（`score_watcher._method_part`）: ファンダの「残り」は「残り（試算と公開の違い）」と書き、**相場の変化ではない**と添える（SOUN 型の誤読を防ぐ）。乖離率の「残り（相場等）」は、scope の理論株価（after）と公開の理論株価が0.5%超違うとき、「試算の理論株価と公開が違う（試算の入力の違い。残りの一部は相場ではない）」を添える

## 残り（直していない）

- scope を作る試算の EPS を公開の値にそろえる（BACKLOG の方針の後半）。試算（`tools/sim`）の入力側の話で、EPS が動く変更を出すときに公開の EPS を使う手順にする（手順に1行足す）
- STONKS の `results.json` の変化の通知は、TANUKI SCORE の通知（`score_watcher`）とは別で、照合していない（STONKS ①の公開では TANUKI SCORE の通知の項目に当たる変化が無かった）
- `timing_score` のような株価で決まる項目が scope に入る（株価を固定した試算でも、公開の実行の株価の動きの分が出る）。通知の項目（判定・乖離率・funda・stage）には入らないので、今は通知に影響しない
