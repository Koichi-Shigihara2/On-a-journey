# browser_checks/

Playwrightを使った実ブラウザ確認スクリプト置き場。

`pytest`の自動収集対象外（`tests/`配下ではなく、ファイル名も`test_`
プレフィックスを使わない）。Playwrightのブラウザバイナリが無い環境
（通常のpytest実行環境）でエラーにならないよう、意図的に分離している。

**CI組み込み・定期自動実行化はスコープ外**（2026-08-26時点）。
手動実行のみを想定している。CI化は投資判断（実行コスト・失敗時の
運用負荷）を伴うためKoichiさんの確認を要する。

## check_dependency_map.py

`SYSTEM_MAP.md`「Market Pulse・MACRO PULSE 画面要素→導出関数→生データ
ソース 依存関係マップ」（2026-08-26新設）に記載の7要素について、
生データファイル（CSV/JSON）から独立に計算した期待値と、実ブラウザで
レンダリングした値を突き合わせる。

対象7要素:
1. MACRO PULSEゲージ（`#pg-score-num`）
2. MACRO PULSE AIウィークリーコメンタリー（`.ai-card-score`）
3. MACRO PULSEスコア推移チャート・tooltip
4. Hindenburg omen関連表示（Market PulseのTake Profit/Buyチェックリスト）
5. Hollow Rally関連表示（MACRO PULSE流動性モニター上部バッジ）
6. Fear & Greed関連表示（Market Pulse F&Gゲージ）
7. breadth_summary関連表示（Market Pulse 市場の広がり）

### 前提

```bash
cd C:\Users\shigi\Documents\On-a-journey-git
venv\Scripts\activate
python -m pip show playwright   # インストール済みか確認
python -m playwright install chromium   # 未インストールなら実行
```

### 実行方法

```bash
cd C:\Users\shigi\Documents\On-a-journey-git
venv\Scripts\activate
python browser_checks\check_dependency_map.py
```

`docs/`全体をルート配信するローカルHTTPサーバー（`python -m http.server`、
ポート8791）をスクリプト自身が起動・終了する。本番のGitHub Pagesルート
配信と同じ相対パス構造で確認するため、`docs/`をそのままルートとして
配信する（`docs/market-monitor/...`のようなサブパスではなく、
`docs/`自体をサーバールートにする）。相対パスアセットが正しく解決できて
いるかは、実行結果末尾の「consoleエラー0件」で間接確認できる
（試験環境の配信構造が本番とズレていると404がconsoleエラーとして
検出される）。

終了コード0=全項目一致、1=不一致あり（または実行中に例外）。

### 注意点

- ④Hindenburg omenは、現在のF&Gスコアが25〜75の中立域にある間は
  Take Profit/Buyチェックリストの個別項目がDOM上に描画されない
  （「非発動」バナーのみ表示）。このため本チェックは、画面上の可視
  表示そのものではなく、**ブラウザがロード・保持しているデータ**
  （`filteredData[...].take_profit_checklist`等、JSのグローバル変数
  経由で取得）が`breadth_data.json`からの独立再計算と一致するかを
  検証している。F&Gが75以上または25以下になれば、チェックリストが
  画面上にも可視化される。
- ⑦breadth_summaryは、Market Pulse側では`collect_and_send.py`が
  既に計算済みの値を`market_data.json`にそのまま格納・表示するだけの
  経路のため、期待値も同じ`market_data.json`を読み直したもの。
  これは計算式の妥当性チェックではなく、ブラウザ側のデータ取得・
  パース経路が壊れていないかの確認である点に留意。
- 不一致が検出された場合、スクリプトはその場で修正を行わない
  （報告のみ）。真の影響範囲を確認してから対応方針を判断すること。

### 2026-08-26 初回実行結果（ベースライン）

7要素すべて一致、両ページともconsoleエラー0件。詳細は
`BACKLOG.md`該当箇所（追記予定）参照。

## market_pulse_elements.py（2026-09-26追加）

`check_dependency_map.py`から呼ばれる。Market Pulse画面の全29要素
（`SYSTEM_MAP.md`のMP-01〜MP-29）について、`market_data.json`から独立に算出した
表示期待値（index.htmlの`toFixed`・`Math.round`・閾値をPython側で再実装）と、
実ブラウザのDOM・チャート系列を突き合わせる。あわせて次を出力する。

- D-01〜D-05: 導出層・データ層の独立再計算（センチメントスコアの加重平均、
  指標・資産フローの前日比をdaily/の同日終値から再計算、ブレッスの全銘柄が
  同じ日付の比較か、Tech Pulseの構成要素の欠落）
- データ基準日の分類: 要素ごとの基準日を、生成時刻から見た直近の米国終値日と比べ、
  「前営業日の終値基準（正常）」「1営業日以上古い」「混在」等に分類する

結果は一致・不一致・判定不能の3値で、件数を末尾に表示する。不一致の備考欄の
`[描画]`/`[導出]`/`[データ]`は、食い違っている層を表す。

注意点:
- `innerText`はCSSの`text-transform: uppercase`適用後の文字列を返すため、
  サブスコア名・ブレッス欄は大文字小文字を区別せずに比較している
- 期間フィルタ（既定30日）はスクリプト実行時刻で計算する。日付をまたぐ直前に
  実行すると、ブラウザ側と1件ずれることがある
