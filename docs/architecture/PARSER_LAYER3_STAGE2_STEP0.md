# PARSER-LAYER3-SINGLE-SOURCE-1 段2（四半期の CF・PL）の STEP 0（2026-11-27、読み取りと手元の試算のみ。コード・データは変えていない）

依頼: 段1と同じ形で、四半期の CF・PL を Layer3 の `val` から作った場合のセル数・理論株価・判定・stage の変化を出す（`PARSER_LAYER3_STEP0.md` §5 の段2）。守り（`val_guard`）・SEC-RETAG の再タグの規則が段2でもそのまま使えるかを確かめ、読み手を洗い出し（`PARSER_LAYER3_STAGE1_readers.csv` と同じ形）、**試算の置き換えは本番のコードにも入れる前提**で書く（段1の抜けの教訓）。
表: `PARSER_LAYER3_STAGE2_cells.csv`（置き換えるセル979の全件。区分つき）・`PARSER_LAYER3_STAGE2_readers.csv`（読み手）。

**試算の方法**（再現）: 本番の `common/sec_data/quarterly_bs.py::replace_record` の `REPLACED_FIELDS`（今は bs 全部＋cf の D&A）を、cf の buyback・OCF・SBC、pl の全項目、shares、other.rpo に広げて、**既存の `quarterly_*.json`（2,986本）に当てた**コピーを scratchpad に作った（リポジトリは触っていない）。つまり試算で動かした関数は本番に入れる関数そのもので、**差は定数1つ**（段1で試算の差し替え〈`parser_overlay.py`＋`parser_redirect.py`〉と本番のコードが別物だったのを避ける）。その上で `tools/sim/sim.py chain`（TANUKI → HypeCore → TANUKI、前＝今の kaihatsu、後＝置き換え後の四半期ファイルを `parser_redirect.py` で読む）を全銘柄で回した。比べたスクリプトは scratchpad（`cells.py`・`overlay.py`・`chk59.py`・`endcheck.py`・`retag_chk.py`）。

## 結論（先に）

1. **置き換えるセルは979（277ファイル・54銘柄）**。内訳は **CF 151・PL（EPS除く）497・EPS 145・株数 153・RPO 33**。約6割（640）は「parser は原本の値、Layer3 は改訂後の値」の改訂、**比が1.5倍超の分割の調整後などの値が201**（EPS・株数。parser は申告のまま）、千株・小数の丸め63、定義の違い70、印つき（`val_suspect`）5、`val_guard`（単位の誤り・内訳の行の減り）の当たりは0。
2. **理論株価・判定・funda・HypeCore の stage の変化は0**（TANUKI 82銘柄、保有9銘柄とも ±0.0%。`latest.json` の差は `calculation_date` だけ。HypeCore 82銘柄は出力の差0）。四半期の CF・PL・株数・RPO は parser のファイルからは**どの評価も読んでいない**（読み手は§3）。STONKS も同じ（年次の parser と Layer3 を読むだけで、四半期の parser の pl・cf は読まない。外部取得が要るので回していない）。
3. **method_change は要らない**（出力が変わらない。POINT-IN-TIME-HISTORY-INTEGRITY-1 (4) と同じ扱い）。変わって見えるのは、`quarterly_*.json` のセル（979）と、rcc の WARN-59（株数の単位誤りの疑い）が3銘柄→2銘柄（SOUN の 2022Q1 の `shares_basic` 4,161,000〈SPAC 前の値〉が Layer3 の値に直って消える）だけ。
4. **守り・再タグの規則は段2でもそのまま使える**（§2）。ただし**ひと手間が3つ**: (a) EPS・株数は「申告のまま」から「分割の調整後」に意味が変わる（201セル）、(b) 符号が反転する置き換え19セルのうち、SBC が正から負になる2セル（HON）は置き換えない、(c) 印つき5セルは段1の (iii) と同じく置き換えて印を残す。
5. **段を分ける案**: **2a＝CF＋PL（EPS除く）＋RPO＝681セル**、**2b＝EPS・株数＝298セル**（分割・丸めの意味の話で、年次の株数・EPS と一緒に決めたい）。2a・2b とも試算の変化は0。
6. 先に決めてほしいこと: §6。

## 1. セルの数（Layer3 と比べた結果。期末日が決まった期だけ）

parser の四半期 2,986本のうち、期末日が決まった期は1,591本（残り1,395本は Layer3 の保持期間〈四半期7年〉の外の古い期で、置き換えない）。期末日は段1と同じ（その期の値と一致する Layer3 のエントリの期末日の多数決。決まらない期は触らない）。段1で置き換えた bs と、SEC-RETAG で置き換えた cf の D&A は除く。

| 項目 | 同じ | 置き換える | 改訂 | 分割の調整 | 丸め | 定義の違い | 印つき | Layer3だけ（足さない） | parserだけ（触らない） |
|---|---|---|---|---|---|---|---|---|---|
| cf.buyback | 881 | 21 | 15 | — | — | 6 | 0 | 87 | 16 |
| cf.operating_cash_flow | 1,470 | 80 | 70 | — | — | 8 | 2 | 25 | 15 |
| cf.stock_based_compensation | 1,401 | 50 | 32 | — | — | 18 | 0 | 58 | 3 |
| cf.capital_expenditure | （parserの四半期に項目が無い） | 0 | — | — | — | — | — | **1,577** | — |
| pl.revenue | 1,505 | 62 | 62 | — | — | 0 | 0 | 17 | 0 |
| pl.cost_of_revenue | 1,306 | 79 | 79 | — | — | 0 | 0 | 55 | 0 |
| pl.gross_profit | 832 | 67 | 67 | — | — | 0 | 0 | 544 | 0 |
| pl.operating_income | 1,393 | 74 | 73 | — | — | 0 | 1 | 95 | 0 |
| pl.net_income | 1,507 | 80 | 75 | — | — | 3 | 2 | 4 | 0 |
| pl.research_and_development | 1,104 | 58 | 57 | — | — | 1 | 0 | 35 | 0 |
| pl.selling_and_marketing | 735 | 44 | 41 | — | — | 3 | 0 | 18 | 0 |
| pl.selling_general_and_administrative | 696 | 33 | 33 | — | — | 0 | 0 | 11 | 0 |
| pl.eps_basic | 1,405 | 71 | 14 | 56 | 1 | 0 | 0 | 44 | 1 |
| pl.eps_diluted | 1,403 | 74 | 15 | 56 | 1 | 2 | 0 | 46 | 1 |
| shares.shares_basic | 1,397 | 75 | 0 | 45 | 30 | 0 | 0 | 48 | 1 |
| shares.shares_diluted | 1,392 | 78 | 3 | 44 | 31 | 0 | 0 | 78 | 1 |
| other.rpo | 1,135 | 33 | 4 | — | — | 29 | 0 | 12 | 39 |
| **合計** | 19,562 | **979** | 640 | 201 | 63 | 70 | 5 | 2,754（capex 1,577含む） | 77 |

- 「改訂」＝parser の値が原本（その期を最初に申告した値、`first_val`）と同じで、Layer3 の `val`（最新の申告）は改訂後。決定(a)（置き換えは `val`）のとおり置き換える
- 「分割の調整」＝EPS・株数で parser と Layer3 の比が1.5倍超（20銘柄）。大半は分割で、Layer3 の `val` は分割の調整後（CSGP 10:1・NVDA 10:1・TSLA 3:1・CPRT・AAPL・AMZN・LRCX・NOW・WMT ほか）、parser は申告のまま。SPAC 合併後の株数・EPS の組み替え（SOUN・VRT・CELH など）も同じ区分に入っている。「丸め」＝ADSK・APP などで千株単位に丸められた差（1%未満）
- 「定義の違い」70（parser が原本の値とも違う）の内訳: **AMZN 21（RPO。parser は 10.2B・11.6B など丸めた値、Layer3 は 8.864B・10.539B など申告の行の値）**、CELH 6（S&M 3・buyback 2・R&D 1）、HEI 6、ADSK 5、HON 5、ASTS 4（初期の殻の会社の OCF・EPS）ほか。HON の `quarterly_2021Q2.json` は、中身が 2021-09-30 の四半期（多数決で BS・PL の22項目が一致）で、ラベルが誤り。CF の3セル（OCF 2,397M → 1,119M・buyback 1,677M → 650M・SBC 95M → 56M）は Layer3 の単独の値と違う（累計に近い大きさ）ので、Layer3 の値に直る
- 「足さない」（Layer3だけ・2,754）は決定(c)（0で埋めない）のまま。capex は parser の四半期に項目が無く、Layer3 だけが持つ（読み手は Layer3 を直接読む）

## 2. 守り・再タグの規則が段2でも使えるか

| 規則 | 段2での確認 | 結論 |
|---|---|---|
| `val_guard` (i) 単位の誤り（10の累乗） | 置き換える979セルの `val_guard` の当たりは**0**（EPS・株数は |k|≥3 だけを対象にする規則なので、分割の調整は当たらない） | そのまま使える。Layer3 の `val` は守りの後の値 |
| `val_guard` (ii) cf_lines の内訳の行の減り | capex だけが対象（parser の四半期に capex が無いので置き換え0） | 影響なし |
| `val_guard` (iii) 隣の期から外れる（印だけ） | 置き換えるセルのうち**5セル**が `val_suspect=neighbor_outlier`: ASTS 2020Q2 OCF（殻の会社の値 → 合併後の −9.2M）・CELH 2021Q2 純益と営業利益・2021Q3 純益・SITM 2023Q2 OCF。CELH の3つは段1の STEP 0 で本物の再表示と確認済み（val が正）。ASTS 2020Q2 は SPAC 合併前の殻の会社の値（parser −0.18M・最初の申告 −0.39M・最新 −9.2M と3つ違う）。**ASTS と SITM は原本で確認する** | 段1の (iii) と同じ扱い（置き換えて印を残す）。実装の前に5セルを原本で確かめる |
| SEC-RETAG（再タグの突き合わせ。`retag_alias_rows`、対象は D&A・SBC・capex） | 置き換える SBC 50セルのうち再タグ（`retag_alias`・`@retag`）で採った値は**0**。再タグは「候補のタグが最新の申告で欠けた期」を Layer3 に**足す**規則で、足された期は parser にも無い（Layer3だけ＝足さない）。OCF は継続事業の組み替えがあるので自動の別名にしない（RETAG_NAME_HINT に無い） | そのまま使える。段2の置き換え対象に再タグ由来の値は入らない |
| 合計のタグが部分の値（`PARTIAL_SUM_FIELDS`） | D&A だけ（SEC-RETAG-RECONCILE-1 で cf の D&A は置き換え済み）。段2の項目に該当なし | 影響なし |
| **期末日の決め方**（段1と同じ多数決） | 段1は BS（時点の値）で多数決が安定していた。段2は CF・PL（期間の値）を含むが、置き換える期の期末日が1つの銘柄で2つのファイルに重なる例は**0**。ラベルの並びと期末の並びが逆になるファイルは5本（期末が前のラベルの期末以前。fiscal の呼び名のずれ）（COHR 2026Q2・HEI 2020Q2・2022Q3・ZS 2021Q2・2023Q2）。このうち置き換えるセルがあるのは HEI 2022Q3 の2セル（OCF 245.9M → 149.2M・SBC 6.2M → 3.0M）だけ。**HON 2021Q2 は逆に、parser のファイルのラベルが誤り（中身は2021Q3）で、多数決が正しい期末を選んで CF を直す** | 段1と同じ規則で使える。HEI の2セルは原本で確認する |

**符号が反転する置き換えは19セル**: 非負であるべき項目が正から負になる誤りは **SBC の HON 2025Q3（36M → −22M）・2026Q3（74M → −104M）の2セルだけ**（HEI 2022Q2 の SBC は −3.76M → 3.24M で、負だった parser のほうが誤り）。HON の2つは Layer3 の `val` が、継続事業の組み替え（累計が減る）で単独の四半期を累計の差から作った値で、負の SBC は誤り（QUARTERLY-BASIS-MIX の型）。ほか17セルは、SPAC 合併の前後の値の直し（SOFI 2021Q1・Q2 OCF、JOBY 2021Q1・Q2 純益、VRT 2020Q2 純益・EPS）、再表示（CELH 2021Q3、RCAT 2023Q3 粗利）、parser の累計・符号の誤り（NVDA 2020Q2 OCF、CAT 2020Q2 buyback、HEI 2022Q2 OCF・SBC）、印つきの SITM 2023Q2 OCF で、Layer3 が正に見える（原本で確認）。→ **規則: 非負の項目（売上・費用・R&D・S&M・SG&A・SBC）が正から負へ反転する置き換えはしない（HON の2セル）**。純益・営業利益・OCF・粗利・buyback の反転は殻の会社・再表示が多いので置き換える。

## 3. 読み手（`PARSER_LAYER3_STAGE2_readers.csv`）

parser の四半期ファイルの pl・cf・shares・other を読むコードは、リポジトリ内で次の**3つだけ**（`quarterly_` の参照を全ファイルで確かめ、各ファイルが Layer3・`ttm/`・`normalized/`・`company_facts` を読むのか parser の四半期を読むのかを分けた）:
- `SECReader.get_quarterly`・`get_quarterly_range`・`get_eps_diluted`（`common/sec_data/reader.py`）: **呼び出し元が無い**（`common/sec_data/__init__.py` の docstring の例だけ。src・common・scripts・tools・tests を grep）。
- rcc CHECK-59（`_check_share_count_outliers`）: 株数 shares_diluted・shares_basic を前後2期の中央値と比べる。置き換えで WARN-59 が3銘柄 → 2銘柄（SOUN が解消。CIX・LOAR は変わらない）。**2b（株数）の話**。
- `SECReader.get_net_cash`・DuPont（pipeline.py）・rcc CHECK-12: 最新の四半期の **bs だけ**（段1で置き換え済み）。

そのほかの評価（TANUKI の DCF・TTM・FCF・ROTCE・倍率・収益の段階・stage_v2、HypeCore、TAIL、STONKS の四半期の傾き）は**Layer3 かその派生（`ttm/`・`normalized/`・`company_facts`）を直接読み**、parser の四半期は通らない。だから置き換えは評価に届かない。`ttm/`・`normalized/` は parser から作られていないので、派生ファイルの作り直しは不要。

## 4. 全銘柄の試算（`tools/sim/sim.py chain`、前＝今の kaihatsu、後＝979セルを置き換えた四半期ファイル）

| 見るもの | 結果 |
|---|---|
| TANUKI 理論株価 | 変化 **0**（82銘柄） |
| TANUKI 判定・funda・乖離率ほか | 変化 **0**（`latest.json` の差は `calculation_date` だけ） |
| 保有9銘柄（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA） | すべて ±0.0%・判定の変化0 |
| HypeCore stage | 変化 **0**（82銘柄の出力の差0） |
| STONKS | 読み手の確認で0（四半期の parser の pl・cf を読まない。外部取得が要るので回していない） |
| 調整後 EPS | 0（`company_facts` を直接読む） |
| `ttm/`・`normalized/` | 0（parser から作られていない） |
| rcc | CHECK-59: 3銘柄 → 2銘柄（2b の株数を入れたとき）。ほかの CHECK は四半期の pl・cf を読まない |
| 保存されるデータ | `quarterly_*.json` が277本（54銘柄、979セル）。2aだけなら 681セル |

2a（CF・PL〈EPS除く〉・RPO）だけにしても、979セルの試算の部分集合なので変化は0。

## 5. 本番に入れる変更（段1の抜けの教訓: 試算と本番を同じ関数にする）

**段1では、試算が `parser_overlay.py`＋`parser_redirect.py`（差し替え）にだけあり、本番の実装〈revert した2 commit〉に置き換えが入っていなかった**（`PARSER_LAYER3_STAGE1.md` §4）。段2は最初から本番の関数の定数を広げて試算したので、実装は次の変更で足りる:

| 場所 | 内容 |
|---|---|
| `common/sec_data/quarterly_bs.py` | `REPLACED_FIELDS` に、cf の buyback・operating_cash_flow・stock_based_compensation、pl（revenue・cost_of_revenue・gross_profit・operating_income・net_income・R&D・S&M・SG&A）、other.rpo を足す（**2a**）。EPS・株数は2bで足す。`replace_record` の `bs = record.get("bs"); if not bs: return []` を区分ごとに（bs が空の四半期ファイルでも cf・pl を置き換える）。ファイル名は変えない（呼ぶ側 `SECParser.parse_and_save`・テスト・出力の一覧の参照が増えるため。docstring の「年次・PL・株数は置き換えない」を直す） |
| 守り（段2で足す2つ） | ① 非負の項目（revenue・cost_of_revenue・R&D・S&M・SG&A・SBC）が正から負になる置き換えはしない（HON の SBC 2セル）。② `val_suspect` の5セルは置き換えて印を残す（段1の (iii)）。印の出し先は `val_guard_marks` と同じ経路でよいか確認する（置き換えたセルは parser のファイルに印を持てないので、置き換えた件数と一覧を `report.txt` ではなく `quarterly_replace` のログに残す案） |
| 既存データへの当て方 | `python common/sec_data/quarterly_bs.py`（同じ関数を既存の `quarterly_*.json` に当てる。SEC は呼ばない）。**実装後に、この STEP 0 の試算の出力（277本・979セル／2aなら681セル）と一致することを確かめる**（`diff`）。段1の公開では「試算 = 本番」を後で突き合わせて抜けを見つけたので、今回は実装の完了条件にする |
| `SECParser.parse_and_save` | 変更なし（保存の前に `replace_from_layer3` を呼ぶ仕組みは段1のまま）。日曜の `SEC_Data_Update` が新しい四半期を同じ規則で作る |
| テスト | `tests/test_quarterly_bs.py` に追加: 改訂が置き換わる・Layer3だけ／parserだけ／符号だけの違い／期末日が決まらない期は触らない・非負の項目の符号の反転は置き換えない・`val_suspect` のセルは置き換えて件数に数える・書き手は1か所 |
| 出力の一覧 | `build_output_inventory.py --layer sec` を作り直すか確認（キーの読み手は変わらない見込み。ゲートの INV で確かめる） |
| method_change | **登録しない**（理論株価・判定・funda・stage が動かない）。公開後の確認は手順で行う: 実装の公開 → 当て直しの commit → **次の日曜の `SEC_Data_Update` の後**、`quarterly_*.json` に新しい四半期だけの差が出て、既に置き換えた期が parser の規則に戻っていないこと |

## 6. 決めてほしいこと

- **(a)** 範囲: **2a＝CF＋PL（EPS除く）＋RPO（681セル）を先に、2b＝EPS・株数（298セル。分割の調整後への意味の変更201・丸め63）は別段**でよいか。2b を段2に入れるなら、年次の株数・EPS（段3〜5）と意味（申告のまま／分割の調整後）をそろえる決めが要る（推奨: 別段）
- **(b)** 非負の項目が正から負になる置き換えを見送る（HON の SBC 2セル）でよいか。ほか17セルの符号の反転（殻の会社・再表示・parser の誤り）は置き換える
- **(c)** 印つき5セル（ASTS・CELH 3・SITM）は、原本で確認した上で、段1の (iii) と同じく置き換えて印を残すでよいか（ASTS 2020Q2 の OCF と SITM 2023Q2 の OCF −4.8M → +0.07M は確認が要る）
- **(d)** method_change を登録しない（出力0）でよいか
- **(e)** `quarterly_bs.py` のファイル名を変えず広げてよいか（改名すると参照が増える）
- **(f)** 実装の順: 2a の実装 → 既存データへの当て直し（681セル）→ 次の日曜の確認手順（実装の完了条件に「試算との一致」）

## 7. 限界

- 期末日の決め方は段1と同じ多数決で、決まらない期（1,395本）は置き換えない。HON 2021Q2 のようにラベルが誤りのファイルは多数決が正しく直したが、HEI 2022Q3 のように fiscal の並びが特殊なファイルの2セルは原本の確認がまだ
- 「定義の違い」70セルのうち、HON・HEI・NVDA・CAT などの期の値の食い違いの理由（累計と単独の混在・組み替え）は、符号が反転する19セルと印つき5セル以外は原本で確かめていない（実装の前に、段1の `movers.csv` と同じ形で列挙する）
- AMZN の RPO 21セル（parser の丸めた値と Layer3 の申告の行の値）は、parser がどのタグを採っているかを確かめていない（RPO は別段の案があるので、2a に含めるか確認）
- STONKS は外部取得が要るため回していない（読み手の確認で0）


---

# §2b 段2b（四半期の EPS・株数、298セル）の STEP 0（2026-11-27、読み取りと手元の試算のみ。コード・データは変えていない）

依頼: 段2a と同じ形で、EPS・株数の置き換えのセル数・理論株価・判定・funda・stage の変化を出し、株数・EPS の基準を TANUKI-SHARES（`shares_base`）・DILUTION（`share_count_change`）・POINT-IN-TIME（as-of・分割の換算）と照らし合わせる。**置き換えは本番の `quarterly_bs.py` を広げる前提で試算した**（段1の教訓）。
表: `PARSER_LAYER3_STAGE2B_cells.csv`（298セルの全件。区分・分割の調整の内訳つき。§1 の `PARSER_LAYER3_STAGE2_cells.csv` の EPS・株数の行に `detail`・`split_history_factor_after_end` を足したもの）。

**試算の方法**（再現）: 作業用の worktree に段2a の `quarterly_bs.py`（`24bbb38d91`）を当て、既存の `quarterly_*.json` に置き換えを当てて**状態A（2a、681セル）**を作った。次に `REPLACED_FIELDS` の条件を **`pl` のすべて（EPS 含む）＋ `shares`** に広げる2行の変更だけを足して当て直し、**状態B（2a＋2b）**を作った。A と B の差が2bのセルで、**298セル・104ファイル・35銘柄、§1 の表（`PARSER_LAYER3_STAGE2_cells.csv`）の EPS・株数298行とキーまで完全に一致**した（片方だけにあるセルは0）。つまり試算で動かした関数は本番に入れる関数そのもの。理論株価・判定は `tools/sim/sim.py chain`（TANUKI → HypeCore → TANUKI、前＝今の kaihatsu、後＝状態B）で全銘柄を回した。

## 2b-1. 結論（先に）

1. **置き換えるセルは298（104ファイル・35銘柄）**: eps_basic 71・eps_diluted 74・shares_basic 75・shares_diluted 78。内訳は **分割の調整201**（登録の全分割と一致146・一部の分割だけ調整済み40・登録の分割なし〈SPAC の組み替え・再表示〉15）・**丸め63**・**改訂32**・**定義の違い2**（ASTS）。`val_guard` の当たり0・`val_suspect` 0。符号が反転するのは EPS の4セル（CELH 2021Q3 の基本・希薄化後、VRT 2020Q2 の基本・希薄化後）。
2. **理論株価・判定・funda・HypeCore の stage の変化は0**（TANUKI 82銘柄、保有9銘柄とも ±0.0%。`latest.json`・`history.json`・`tickers.json`・HypeCore `poc.json` の差は日時の欄だけ）。四半期の parser の EPS・株数を読む評価は無い（§3）。
3. **基準: 2b のあと、parser の四半期の EPS・株数4項目は、期末日が決まる全期（1,471期）で Layer3 の `val` と一致する**（2b の前は298セルが不一致＝2b のセルそのもの）。TANUKI が使う株数（`shares_by_end` ＝ Layer3 の `val` ＋ `split_history.yaml` の換算）との比べ（`shares_diluted`、比べられる1,468期）では、2b の前は160期が2%超ずれ（うち parser が Layer3 とも違う値だったのが38期）、**2b のあとは122期で、すべて「parser ＝ Layer3 の val、`shares_by_end` が split_history の換算を上に足した分」**（設計どおり。parser のファイルに換算を入れない）。
4. **同じ株数を別の基準で持つ箇所は、parser の外に残る**（§2b-4）: EPS ANALYZER の `diluted_shares_used`（`companyfacts` から自前で選ぶ）は、`shares_by_end` と比べられる2,160期のうち38期（1.8%）で2%超ずれる（RCAT 13・NVDA 5・GOOGL 4・TSLA 4・ESTC 3 ほか）。本件の範囲外（FINDINGS）。
5. **method_change は要らない**（出力0）。rcc の WARN-59（株数の単位誤りの疑い）が SOUN 2022Q1 の `shares_basic` 4,161,000 で出ていたのが解消する（WARN 181 → 178のうち1件。残り2件は作業用の worktree が古い版で出る docs の差で、2b と無関係）。
6. 決めてほしいこと: §2b-6（推奨つき）。

## 2b-2. セルの数

| 項目 | 置き換える | 分割の調整 | 丸め | 改訂 | 定義の違い |
|---|---|---|---|---|---|
| pl.eps_basic | 71 | 56 | 1 | 14 | 0 |
| pl.eps_diluted | 74 | 56 | 1 | 15 | 2 |
| shares.shares_basic | 75 | 45 | 30 | 0 | 0 |
| shares.shares_diluted | 78 | 44 | 31 | 3 | 0 |
| **合計** | **298** | **201** | **63** | **32** | **2** |

分割の調整201の内訳は、**登録の全分割と一致する146**（AAPL・AMZN・BKNG・CELH・CPRT・CSGP・GOOGL・HON・KLAC・LRCX・NOW・NVDA・TSLA・WMT の分割の前の期。Layer3 の `val` が、その期より後に登録された分割をすべて反映した値）と、**一部の分割だけ調整済みの40**（CPRT 16・NVDA 12・TSLA 8・CELH 4。たとえば NVDA 2020Q2 の株数は parser 626M → Layer3 2,504M で×4だけ。登録の分割は 4:1〈2021〉と 10:1〈2024〉で、10:1は後の申告が再掲していない期なので反映されない＝Layer3 の `val` は**最新の申告の基準**であって、現在の基準ではない）。

**登録の分割なし15**（`split_history.yaml` に分割が無いのに比が1.5倍超）: SPAC 合併の組み替え・再表示と、parser 側の誤りの混在。VRT 4（2020Q2・Q3 の EPS。**VRT 2020Q2 は符号も反転**）・SOUN 3（2022Q1 の EPS 2・株数1。合併の交換比率で組み替え。SOUN の `shares_basic` 4,161,000 → 69,604,188 が WARN-59 の原因）・AVAV 2（2026Q3 の EPS −3.15 → −4.9。**分割ではない。原本で確認する**）・BROS 2（2021Q3 の EPS）・DOCN 2（2023Q1 の EPS。原因は未確認〈再表示・組み替え〉）・COHR 2（2020Q3 の EPS 70.0 → 0.07、60.0 → 0.06。**`first_val` も70・60＝原本の申告の誤記が後の申告で直った**。Layer3 の `val` が正）。

**定義の違い2**: ASTS 2020Q2・Q3 の `eps_diluted`（parser −17,600,000 と +17,600,000＝**株数の値が EPS の欄に入った誤り**）。Layer3 の `val` は **0**（`first_val` も0）。0 で埋めない決定(c)があるので、決めてほしいこと(d)。

**符号の反転4**（EPS）: CELH 2021Q3 の基本 0.04 → −0.13・希薄化後 0.03 → −0.13（SPAC でない。CELH は再表示で純益が負になった期）、VRT 2020Q2 の基本・希薄化後 0.08 → −0.17（SPAC 合併前の殻の会社の値）。2a と同じ区分（殻の会社・再表示）で、置き換える側が正に見える（原本で確認する）。EPS と株数は非負の項目の規則（§2）の対象ではない（EPS は負が普通）。

## 2b-3. 全銘柄の試算

| 見るもの | 結果 |
|---|---|
| TANUKI 理論株価・判定 | 変化 **0**（82銘柄。`sim.py compare`） |
| 保有9銘柄（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA） | すべて ±0.0%・判定の変化0 |
| TANUKI の `latest.json`・`history.json`・`tickers.json`（funda・乖離率・components ほか全項目） | 差は `calculation_date`・`date`・`updated_at` だけ（全項目の差分で確認） |
| HypeCore stage | 変化 **0**（82銘柄の `poc.json` の差は `generated_at` だけ） |
| STONKS・調整後 EPS・TAIL | 読み手の確認で0（Layer3・`companyfacts` を直接読み、parser の四半期の EPS・株数は読まない。§3 のとおり） |
| `ttm/`・`normalized/` | 0（parser から作られていない） |
| rcc | NG 0のまま。**WARN-59 が1件解消**（SOUN 2022Q1 の `shares_basic`） |
| 保存されるデータ | `quarterly_*.json` が104本（35銘柄、298セル）。2a の681セルと重なるファイルがある（2a＋2bで 979セル・277本） |

## 2b-4. 株数・EPS の基準の照合（TANUKI-SHARES・DILUTION・POINT-IN-TIME）

株数（希薄化後の加重平均）を持つ箇所と、その基準:

| 箇所 | 基準 | 2b のあと |
|---|---|---|
| Layer3 `shares_diluted` の `val` | その期を**最後に**申告した値（その申告が分割後の基準なら調整後、再掲しなければ分割前のまま） | 基準の正。parser の四半期はこれと**同じ値**になる（本件） |
| Layer3 `first_val`・`first_filed` | その期を**最初に**申告した値と日（as-of） | 変わらない。POINT-IN-TIME の履歴（`multiples.py` の過去の月末の株数）が読む。parser の四半期は読まない |
| `split_adjust.shares_by_end` | `val` に `split_history.yaml` の換算を足して**現在の基準**にした値（`as_of` を渡すとその日までの申告の値と、その日の時点の分割だけで換算） | 変わらない。TANUKI-SHARES（`shares_base` の Wd）・DILUTION（`share_count_change`）・`multiples.py` が通る1か所。parser の四半期の値との差122期は、ここの換算の分（設計どおり） |
| `shares_base.base_shares`（TANUKI-SHARES） | `shares_by_end` の最新の期 ＋ 表紙の株数の補正 | 変わらない（parser の四半期を読まない） |
| `dilution.share_count_change`（DILUTION） | `shares_by_end` の系列（補正前の Wd）の年率 | 変わらない |
| `multiples.py` の `shares_wd`・`shares_end` | 現在の行は `shares_base`、過去の月末は `shares_by_end(as_of=月末)` | 変わらない |
| **parser の四半期の `shares`・`pl.eps_*`** | 申告の値（今は parser が `companyfacts` から独自に選ぶ） | **Layer3 の `val`（最新の申告の基準）に置き換わる**（本件）。換算は持たない |
| **EPS ANALYZER の `diluted_shares_used`** | `companyfacts` から自前で選び、`apply_split_adjustments`（`unadjusted_indices` 共用）で換算 | **別の経路のまま**（2,160期のうち38期が `shares_by_end` と2%超ずれる。RCAT 13・NVDA 5〈2.5倍〉・GOOGL 4・TSLA 4・ESTC 3・FROG 2・AMD・CRM・DOCN・LITE・LYFT・SITM・VRT 各1）。TANUKI の `select_max_eps_shares` が読む `ttm.json` の `diluted_shares` の元で、`EPS-SHARES-BASIS-MISMATCH-1` の「同じ期末の値どうし」の対策の外側。FINDINGS |

**同じ株数を別の基準で持つ箇所が残るか**: parser の中では**残らない**（2b で四半期の株数・EPS を Layer3 の `val` 1つにする）。parser の外には、(1) 意図した2つ（`first_val` の as-of と、`val`＋換算の現在の基準）と、(2) EPS ANALYZER の自前の選び方（上の表の最後の行）がある。(2) は2b では直らない。

**EPS には換算の部品が無い**: `shares_by_end` は株数だけで、EPS を split_history で換算する関数は無い（EPS は純益÷株数なので、TANUKI は純益と株数から作る）。parser の四半期の EPS を Layer3 の `val`（最新の申告の基準。分割の調整201のうち一部の分割だけ調整済みの40は、EPS も同じく部分調整）にしても、読み手が無いので影響は0。

## 2b-5. 分割の前後で意味が変わるセルの扱いの案

分割の調整201セルは、parser が「申告のまま」（分割前）、Layer3 の `val` が「最新の申告の基準」（分割後の再掲があれば調整後）。意味が変わるので、案を3つ出す:

- **案1（推奨）: Layer3 の `val` に置き換える（換算は入れない）**。parser の四半期の株数・EPS の意味を「Layer3 の `val`＝最新の申告の基準」とそろえる。完全な現在の基準が要る読み手は `shares_by_end` を通す（今のまま）。登録の全分割と一致する146セルは `val` が結果的に現在の基準。一部の分割だけ調整済み40・登録の分割なし15の**55セル**には `replace_marks`（`split_basis_partial`・`recast`）の印を付け、基準が「現在」でないことを残す
- 案2: 置き換えない（201セル）。parser の四半期の EPS・株数は「申告のまま」のまま、置き換えるのは丸め63・改訂32・定義2の97セルだけ。意味は変わらないが、parser と Layer3 が一部で別の基準のままになり、「parser を Layer3 から作る」目的に半分届かない（同じ期の `val` が2つある状態が残る）
- 案3: `shares_by_end` の換算後の値にそろえる（株数のみ。EPS は換算の関数が無いので別途）。parser のファイルに split_history の換算が入り、換算の規則が parser と `split_adjust.py` の二重になる（`common/sec_data/split_adjust.py` のとおり換算は1か所に置く方針と合わない）。**推奨しない**

## 2b-6. 決めてほしいこと

- **(a)** 2b の範囲: 298セルのすべてを Layer3 の `val` に置き換える案1でよいか（推奨）。案2（97セルだけ）にするか
- **(b)** 基準の意味を文書に書く: 「parser の四半期の株数・EPS は Layer3 の `val`（最新の申告の基準）。現在の基準が要る読み手は `shares_by_end`」（`quarterly_bs.py` の docstring と `SYSTEM_MAP`）。parser に split_history の換算を埋め込まない
- **(c)** 一部の分割だけ調整済み40・登録の分割なし15の55セルに `replace_marks` の印を付けてよいか（推奨: 付ける。読み手がいない今は表示に出ない）
- **(d)** ASTS 2020Q2・Q3 の `eps_diluted`（parser が株数の値を入れた誤り、Layer3 の `val` は0）: 0 に置き換えず**欠け（None）にして印**を付けるか（推奨）。それとも Layer3 の 0 に置き換えるか（0 埋めの決定(c)に反する）。原本で確かめてから
- **(e)** 符号が反転するEPS 4セル（CELH 2021Q3・VRT 2020Q2）と、AVAV 2026Q3（−3.15 → −4.9）は、2a と同じく原本で確認した上で置き換えるでよいか（COHR 2020Q3 の EPS 70・60 は、`first_val` も70・60で、原本の誤記を後の申告が0.07・0.06に直した値。置き換える）
- **(f)** method_change を登録しない（出力0）でよいか
- **(g)** 実装の順: 段2a のブランチ（`parser-layer3-stage2a`、kaihatsu へ未公開）に2bを足し、**2a と2b を1回の公開にする**か（推奨: 1回。試算はどちらも0で、重なるファイルがある）、2a を先に公開するか。段2a のブランチは古い kaihatsu から分かれているので、公開の前に kaihatsu へ取り込む（`quarterly_bs.py` とテストの変更は今の kaihatsu に当たることを確認済み）
- **(h)** EPS ANALYZER の `diluted_shares_used` と `shares_by_end` の38期のずれは、本件の範囲外として FINDINGS にし、別の項目にするか

## 2b-7. 実装の中身（決まったあと。本番に入れる変更）

| 場所 | 内容 |
|---|---|
| `common/sec_data/quarterly_bs.py` | `REPLACED_FIELDS` を **`pl` のすべて＋`shares`**に広げる2行（試算と同じ）。docstring の「EPS・株数は段2b」を直す。(c)(d) の印は `replace_marks` に足す（ASTS は欠け〈None〉＋印）。`replace_record` の入口の条件は2aのまま |
| テスト | `tests/test_quarterly_bs.py` に、EPS・株数が Layer3 の `val` に置き換わる・丸めは置き換わる・ASTS 型（Layer3 が0で parser が非ゼロ）は欠け＋印・一部の分割だけ調整済みの印・parser の四半期の `shares_diluted` が Layer3 の `val` と一致する（全期）を追加 |
| 既存データへの当て方 | `python common/sec_data/quarterly_bs.py`（SEC は呼ばない）。**実装後に、この STEP 0 の試算の出力（298セル・104本）と一致することを確かめる**（`PARSER_LAYER3_STAGE2B_cells.csv` と突き合わせる）。完了の条件 |
| 出力の一覧 | `build_output_inventory.py --layer sec` を作り直すか確認（キーの読み手は変わらない見込み。INV で確かめる） |
| method_change | **登録しない**。公開の後の確認は、次の日曜の `SEC_Data_Update` の後に `quarterly_*.json` の新しい四半期だけが増え、既に置き換えた期が戻っていないこと |

## 2b-8. 限界

- 「期末日が決まる1,471期」は、Layer3 の保持期間（四半期7年）の中で多数決が決まった期。外の1,210期は置き換えない（`shares_by_end` との比較にも入らない）
- 登録の分割なし15・符号の反転4・定義2のうち、AVAV・VRT・SOUN・CELH・ASTS は原本を確かめていない（実装の前に原本で確認する）
- EPS ANALYZER との比較は `shares_by_end` との2%の差で、各期の差の理由（組み替え・未登録の分割・選び方の違い）は RCAT・NVDA 以外は調べていない
- STONKS は外部取得が要るため回していない（読み手の確認で0）


---

# §2c 段2（2a＋2b）の実装と公開の手順（2026-11-27 決定・実装、ブランチ `parser-layer3-stage2a`・未公開）

**決定（§2b-6 の(a)〜(h)）**: (a) 298セルすべてを Layer3 の `val` に置き換える (b) 基準の意味を文書に書く（下） (c) 一部だけ調整済み40・登録の分割なし15（計55セル）に印 (d) ASTS の2セルは0に置き換えず欠け＋印 (e) 原本の確認は公開の前の宿題にしない（印で残す） (f) method_change は登録しない (g) 2a と 2b を **1回の公開**にする (h) EPS ANALYZER の `diluted_shares_used` のずれ（38期）は今回は触らない（FINDINGS。束ねの候補）。

**基準の意味**（`quarterly_bs.py` の docstring にも書いた）: 四半期のファイル（`quarterly_*.json`）の値は**今の時点の見方**＝Layer3 の `val`（その期を最後に申告した値）。分割の調整は最新の申告が再掲した分だけ反映される（再掲の無い期は分割前のまま）。現在の基準が要る読み手は `split_adjust.shares_by_end`（`val` ＋ `split_history.yaml` の換算）を通す。このファイルに換算を入れない。**倍率の履歴は別**で、その月末までに申告された値（`first_val`・as-of、`common/multiples.py`）。

## 実装（ブランチ `parser-layer3-stage2a`、kaihatsu の `9f362db984` に rebase 済み）

| commit | 内容 |
|---|---|
| 段2a のコード・データ | CF・PL（EPS除く）・RPO（681セル・47銘柄）。非負の項目が正→負は欠け＋印（HON の SBC 2セル）、`val_suspect` は置き換えて印（5セル） |
| 段2b のコード | `REPLACED_FIELDS` を **`pl` のすべて＋`shares`**（2行）。`replace_record(record, store, splits)`: EPS・株数は、登録の分割（`split_history.yaml` の、期末より後の分割の積）と比が合えば印なし、合わなければ `split_basis_partial`、登録の分割が無くて比が1.5倍超なら `recast_no_registered_split`。Layer3 の `val` が0で parser が0でないEPS・株数（ASTS 型）は0に置き換えず**欠け（None）＋`layer3_zero_parser_nonzero`** |
| 段2b のデータ | 298セル・35銘柄に当てた（印: `split_basis_partial` 40・`recast_no_registered_split` 15・`layer3_zero_parser_nonzero` 2） |
| テスト | `tests/test_quarterly_bs.py`: 段2b の置き換え・分割の印3種・ASTS 型・印は EPS・株数だけ・置き換え済みのファイルが AAPL・NVDA で Layer3 の `val` と一致 |

## 試算（2a＋2b を合わせて）

- 置き換え **979セル**（2a 681＋2b 298。277ファイル・54銘柄。CF 151・PL 642〈EPS 145 を含む〉・株数 153・RPO 33 で §1 の表と一致）。印の内訳: `split_basis_partial` 40・`recast_no_registered_split` 15・`layer3_zero_parser_nonzero` 2・`val_suspect` 5・`nonneg_layer3_negative` 2
- `tools/sim/sim.py chain`（前＝kaihatsu、後＝このブランチ）: **TANUKI 82銘柄の理論株価・判定・funda・乖離率ほか全項目の変化0**（`latest.json`・`history.json`・`tickers.json` は日時の欄だけ）、**HypeCore stage の変化0**（`poc.json` は `generated_at` だけ）、保有9銘柄 ±0.0%
- テスト: pytest 全件・rcc NG 0（WARN-59 が SOUN の1件解消）。出力の一覧（`--layer sec`）は変わらない見込み（INV で確認）

## 公開の手順（12-01〈火〉の依頼文で。②の2回目の確認〈11-29 日曜の連鎖〉の後）

1. **前提の確認**: 11-29（日）の SEC_Data_Update → HypeCore → TANUKI の連鎖が済み、②の2回目の確認が合格している。合格していなければ公開しない
**ブランチの記録の commit の扱い**: ブランチ末尾の commit `37d45f7310`（BACKLOG・PROJECT_STATUS・FINDINGS・本書の更新）の内容は、2026-11-27 に docs だけで kaihatsu に入れ済み。rebase で衝突したら**kaihatsu 側を取る**（`git rebase -X ours kaihatsu`〈rebase の ours＝上流の kaihatsu〉、または**その commit を drop**）。ブランチ側の記録を kaihatsu に上書きしない（kaihatsu のほうが新しい）。コード・データの commit だけ残す。
2. **kaihatsu を最新にしてブランチを rebase**（`git fetch`・`git pull` の後、`git rebase kaihatsu`）。**11-29 の日曜の SEC_Data_Update が `quarterly_*.json` を作り直す**（今の kaihatsu のコードは bs・D&A だけ置き換える）ので、ブランチの**データの commit〈2a・2b の2つ〉はおそらく衝突する**。衝突したらデータの2 commit は捨て、**コードの2 commit だけを残して `python common/sec_data/quarterly_bs.py` で当て直す**（同じ関数。SEC は呼ばない。何度当てても同じ結果）
3. **当て直しの結果を確かめる**: kaihatsu との差が、置き換えるセル979 ＋ 11-29 に増えた四半期の分（`parse_and_save` は bs・D&A だけ置き換えているので、その期の CF・PL・EPS・株数が増分）であること。印は `split_basis_partial` 40・`recast_no_registered_split` 15・`layer3_zero_parser_nonzero` 2 が残る（`PARSER_LAYER3_STAGE2B_cells.csv` と突き合わせる）。`tests/test_quarterly_bs.py::test_committed_eps_and_shares_match_layer3_val` が通る
4. **ゲート**: pytest（全件）・rcc（NG 0。WARN は 181 から SOUN の WARN-59 の分が減る）・`--layer sec` の CHECK-INV（作り直しの要否）
5. **sim で最後の確認**: `tools/sim/sim.py chain` の前後で理論株価・判定・stage の変化0（11-29 のデータで作り直す）。0でなければ公開しない
6. **push（kaihatsu）**。method_change は登録しない。ID は `PARSER-LAYER3-SINGLE-SOURCE-1-2`（段2。2a＋2b）
7. **公開の後の確認**（`CLAUDE_CODE_START.md` に手順を足す。次の日曜 12-06 の SEC_Data_Update の後）: 新しい四半期のファイルで、CF・PL・EPS・株数・RPO が Layer3 の `val` と一致し（`replace_from_layer3` が `splits` を渡して印を付ける）、既に置き換えた期が parser の値に戻っていないこと。TANUKI の理論株価・判定の変化0
8. **移設**: 手順7の確認の合格後、段2は PARSER-LAYER3-SINGLE-SOURCE-1 の記録に「済」と書く（本件は段3〈年次〉以降が残るので閉じない）

## 残し・範囲外

- EPS ANALYZER の `diluted_shares_used` と `shares_by_end` の38期のずれは、今回は触らない（FINDINGS 2026-11-27。束ねの候補）
- 印の付いた55セル・ASTS 2セルは、読み手が無いので画面に出ない。原本の確認（AVAV・VRT・SOUN・CELH・ASTS）は、読み手ができるとき（または年次の段）に行う
