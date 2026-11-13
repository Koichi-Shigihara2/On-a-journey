# TANUKI-SHARES-SINGLE-SOURCE-1 STEP 0（2026-11-13、読み取りと手元の試算のみ）

コードのcommit・push・公開は無し。全96銘柄の表は `TANUKI_SHARES_STEP0_table.csv`。道具はscratchpadの`ma/`（`sh1.py`〜`sh3.py`＝株数の突き合わせ・分類、`hook_shares.py`＝案Aの試算フック、`hook_sharesB.py`＝案Bの試算フック、`cmp3.py`＝前後の比較）。基準はkaihatsuの今のコード（`907ffa0ab5`の上、公開）。原本は、各社の10-Q・10-Kの**XBRLの値**（`company_facts.json`の`WeightedAverageNumberOfDilutedSharesOutstanding`・`…BasicSharesOutstanding`・`CommonStockSharesOutstanding`〈貸借対照表の期末の発行済〉・`dei:EntityCommonStockSharesOutstanding`〈表紙〉。accn付き）で確かめた。種類株の表紙（TSLA以外のASTS・PLTR・SOUNの次元つきの値）は、`sec.gov/Archives`の取得に連絡先つきのUser-Agentが要る（このセッションでは403）ため**原本の表紙は未確認**と書いた。

## 結論

| 見るもの | 結果 |
|---|---|
| 今の株数の出所 | `components.diluted_shares`＝**96/96がyfinanceの`impliedSharesOutstanding`**（`_shares_source=yf_implied`）。SECの値（最新年度の加重平均の希薄化後）は、yfが無い時だけの代わり |
| 名前と中身 | 「完全希薄化後」と呼んでいるが、中身は**最新の表紙の発行済株数（基本、全クラス）**にほぼ等しい（表紙のある73銘柄のうち63銘柄が0.5%以内、71銘柄が3%以内）。オプション・RSU・転換社債の希薄化は入らない。名前と中身がずれている（型E） |
| 何に効くか | 理論株価（1株あたり）・ネットキャッシュ／株・FCFE・Valuation Gap・validator・report・画面の時価総額と株数表示。funda・timing・stageには直接は効かない（理論株価の乖離率経由でtimingと判定に効く） |
| SECの加重平均（直近四半期、倍率の部品〈`shares_by_end`〉・DILUTIONの`share_count_change`と同じ基準）との差 | 3%超は**23銘柄**。原因は5種類（下）: 希薄化証券15・期中の発行／買戻し・時点の違い8・未確定の株1・種類株1（うち原本の表紙で確かめた銘柄と未確認の銘柄を表に分けた） |
| 試算（案A: 部品の株数、96銘柄、TANUKI→HypeCore→TANUKI） | 理論株価が変わる**92銘柄**・5%超**12銘柄**・判定が変わる**2銘柄**（CPRT HOLD→TRIM、RMBS WATCH→HOLD）・timing点2・**funda点0**・**stage0**・保有9銘柄の判定は全て変化0（理論株価はSOUN +11.3%・TSLA +11.6%・PLTR −6.4%・SOFI −4.4%・ほか−1.7%以内）・scope 95銘柄（理論株価が0.05%超動く銘柄が印の対象） |
| 案の勧め | **案A（部品の株数＝加重平均の希薄化後）を標準にし、DILUTIONの補助の印`outstanding_gap_pct`で「期末の発行済との差」を表示する**。理由は§4。案B（期末の発行済＋希薄化の増分）は試算したが、TSLAで二重に数える等の穴があり勧めない |

## 1. diluted_shares の書き手・読み手

### 書き手

| 場所 | 内容 |
|---|---|
| `common/market_data/fetcher.py` L1221 | `attributes/{T}.json`の`implied_shares_outstanding`＝yfinanceの`impliedSharesOutstanding`。同じく`shares_outstanding`＝`sharesOutstanding`（L1220付近）。週1回の取得 |
| `common/sec_data/reader.py::get_diluted_shares` L270 | 最新**年度**の`shares_diluted`（加重平均の希薄化後。100万未満なら`shares_basic`）。四半期ではない |
| `tanuki_valuation/data_fetcher.py::_determine_diluted_shares` L1205 | ①`yf_implied`＞100,000ならそれ（`yf_implied`）②無ければyfの`outstanding`とSEC年度値で: 比5倍超→`yf_outstanding_post_dilution`、0.2倍未満→SEC、それ以外は大きい方（`max_sec`／`max_yf`）③片方だけ ④無し→0。**実際は96/96が①** |
| `core_calculator.py` L912・L925 | `components.diluted_shares`・`_shares_source`に保存 |

### 読み手（`components.diluted_shares`）

| 読み手 | 用途 | 理論株価・判定への効き |
|---|---|---|
| `core_calculator.py` L178〜702、`calculator/{scenarios,sensitivity,adjustments,fcfe}.py` | 1株あたりの理論株価＝`PT÷diluted_shares＋ネットキャッシュ／株`（シナリオ・感度・RPO・FCFE含む） | **理論株価そのもの**（IV∝1÷株数） |
| `pipeline.py` L481 | 100,000以下ならその銘柄をスキップ | 有無 |
| `pipeline.py` L721・L2035・L2173・L3328 | reverse DCF（Valuation Gap）・ネットキャッシュ総額・FI（FCFE）の分母 | 理論株価・report |
| `pipeline.py` L2371 | 「期中に大規模な株式発行／買戻し」の注記（yf対直近annualの乖離5%超） | 表示のみ |
| `pipeline.py` L3217・L3284 | 希薄化率の計算のスキップ判定・流動資産の比（時価総額＝株数×株価） | 注記 |
| `pipeline.py` L1414〜1428（`select_max_eps_shares`） | 最大EPSの分母のフォールバック（TTMの加重平均が無い時の今の株数） | 表示のみ（最大EPS） |
| `validator.py` L101〜447 | 理論株価の再現・株数の異常検知 | 検証 |
| `stock.html` L892・L1002・L2388 | 時価総額（MULTIPLES-ADOPT-1で部品の値へ）・ネットキャッシュ／株・シナリオの再計算 | 画面 |
| `common/sec_data/report_consistency_check.py` L2797 | CHECK-10（PSの乖離）の分母 | 検査 |
| `common/sec_data/audit.py` L139 | EPS ANALYZERの株数との差の検査 | 検査 |

STONKS・HypeCore・ROTCEは`diluted_shares`を読まない（時価総額・株数は部品へ移行中）。EPS ANALYZERの`diluted_shares`は別（四半期のSECの値）。

## 2. 全銘柄の突き合わせと原因の分類（`TANUKI_SHARES_STEP0_table.csv`）

比べる値: **Y**＝TANUKIの`diluted_shares`（yfinance implied）、**Wd**＝Layer3の直近の加重平均の希薄化後株数（部品の`current`の株数、分割換算）、**Wb**＝同じ四半期の加重平均の基本株数、**BS**＝貸借対照表の期末の発行済株数、**表紙**＝`dei`の最新の表紙の株数。

| 分類 | 銘柄数 | 判定のしかた |
|---|---|---|
| 一致（Y対Wdが3%以内） | 72 | |
| 比較不能（株数欠け） | 3 | V（Layer3に株数なし）・RKLB・ZS（TANUKI対象外でYなし） |
| 希薄化証券（Y＜Wd。Wdにオプション・RSU・転換社債が入る） | 15 | Y≒BS≒表紙で、Wd−Wbが差の大部分 |
| 期中の発行・買戻し（加重平均と期末の違い） | 2 | JOBY・RXRX。期末の発行済が期中の加重平均より大きい |
| 時点の違い（Layer3が最新四半期を未収載） | 3 | ONDS・RCAT・SOUN。Layer3は2026-06-30まで（Q3の10-Qは11月に提出） |
| 未確定の株（TSLA型） | 1 | TSLA |
| 種類株 | 1 | ASTS |
| 要原本 | 1 | IONQ |

### 原本で確かめた一覧（5%超12銘柄＋判定が変わる2銘柄＋保有）

数値は百万株。accnは貸借対照表・損益計算書の値を取った10-Q。

| 銘柄 | Y→Wd（IV） | 原本の値 | 原因 |
|---|---|---|---|
| **TSLA（保有）** | 3,952.8→3,541.0（+11.6%） | 2026-09-30: 期末の発行済 3,953.0（前期末 3,949.0＝ほぼ増えていない）、加重平均 基本 3,240.0・希薄化後 3,541.0（accn 0001628280-26-066775）。表紙 3,952.8（2026-10-15） | **未確定の株**。期末の発行済と基本の加重平均の差が713M（22%）あるのに期中の発行はほぼ無い＝発行済に入っていて基本のEPSの分母に入らない株（制限付株式の未確定分）。希薄化後に301Mが入る |
| **SOUN（保有）** | 488.4→438.6（+11.3%） | Layer3は2026-06-30（Q2）: Wd 438.6・Wb 430.5・反希薄化 13.1（accn 0001840856-26-000022）。**Q3の10-Qは2026-11-09提出**（`submissions.json`）でLayer3は未取り込み。yfの発行済は455.9、implied 488.4（差+7%） | **時点の違い**（Layer3が1四半期遅れ。次の日曜のSEC_Data_Updateで追いつく）。yfの implied と発行済の差7%は種類株（A・B）の可能性があるが、表紙の原本は**未確認** |
| **PLTR（保有）** | 2,409.5→2,573.9（−6.4%） | 2026-09-30: BS 2,409.2、加重平均 基本 2,405.7・希薄化後 2,573.9（accn 0001321655-26-000048）。反希薄化は小さい | **希薄化証券**。Y＝BS（基本の発行済）。希薄化後−基本＝168M（オプション・RSU）がWdに入る。Yは希薄化証券を数えない |
| **SOFI（保有）** | 1,297.8→1,357.8（−4.4%） | 2026-09-30: BS 1,296.6、表紙 1,297.8（2026-10-30）、加重平均 基本 1,292.5・希薄化後 1,357.8（accn 0001818874-26-000070）。反希薄化の記載は2024年の81M（転換社債） | **希薄化証券**（転換社債のif-converted等で+65M） |
| ASTS | 390.1→299.1（+30.4%） | Layer3 Q2 2026-06-30: 基本＝希薄化後＝299.1（赤字のため。accn 0001193125-26-342550）。`CommonStockSharesOutstanding`・表紙が`company_facts`に無い（クラスごとの次元つき）。yfの発行済 300.7、implied 390.1 | **種類株**。yfの発行済（クラスAだけ）≒Wd、implied（全クラス）が+89M。クラスB・Cの換算の扱いは**原本の表紙で未確認** |
| IONQ | 464.7→400.9（+15.9%） | 2026-09-30: BS 408.9（前期末 381.0）、加重平均 基本 388.6・希薄化後 400.9。表紙 432.5（**2026-10-28**、accn 0001193125-26-449829）。yfの implied 464.7（表紙の+7.5%） | **期中・期末後の発行**（期末408.9→表紙432.5が+5.8%）。Yが表紙よりさらに7.5%大きい理由は**未確認**（yfの時点か計算） |
| ONDS | 582.3→503.6（+15.6%） | Layer3は2026-06-30: BS 529.8（前期末 469.1）、加重平均 基本 500.7・希薄化後 503.6。表紙 570.6（2026-08-11） | 期中の発行が大きい＋Layer3がQ3未収載 |
| RXRX | 623.4→543.0（+14.8%） | 2026-09-30: BS 561.1（前期末 534.1）、加重平均 543.0。表紙 622.8（2026-10-26） | **期末後の発行**（期末561.1→表紙622.8が+11%）。Yは表紙と一致 |
| RCAT | 152.7→136.9（+11.6%） | Layer3は2026-06-30: BS 152.7（前期末 121.8、+25%）、加重平均 136.9。表紙 152.7（2026-08-04） | 期中の大きな発行＋Layer3がQ3未収載 |
| LITE | 90.7→101.7（−10.8%） | 2026-09-26: BS 90.6、表紙 90.7、加重平均 基本 92.5・希薄化後 101.7（accn 0001628280-26-069615） | **希薄化証券**（転換社債のif-converted +9.2M）。基本の加重平均が期末の発行済より大きい＝自社株買い／転換の途中 |
| PAYS | 56.9→62.0（−8.3%） | 2026-06-30: BS 56.4、表紙 56.5、加重平均 基本 55.9・希薄化後 62.0（accn 0001683168-26-006021） | 希薄化証券（+6.1M）。Layer3がQ3未収載 |
| FICO | 21.4→23.0（−7.0%） | 年度（2026-09-30）の加重平均 基本 22.7・希薄化後 23.0＝**年度の平均**（Q4の株数が無い）、BS 21.4、表紙 21.4 | **年度の平均**（5銘柄: AAPL・CPRT・FICO・INTU・ZSでQ4の株数が無く年度平均を使う）。自社株買いが進む銘柄では期末より大きい |
| ALAB | 174.8→184.1（−5.0%） | 2026-09-30: BS 174.8、表紙 174.8、加重平均 基本 174.1・希薄化後 184.1 | 希薄化証券（+10M） |
| **判定が変わる** CPRT | 926.3→956.9（−3.2%） | 年度（2026-07-31）の加重平均 基本 936.3・希薄化後 956.9（年度平均）、BS 926.3 | 年度の平均＋希薄化証券。HOLD→TRIMは境目（乖離率の−3%の動き） |
| **判定が変わる** RMBS | 107.1→108.8（−1.6%） | 2026-09-30: BS 107.1、加重平均 基本 107.6・希薄化後 108.8 | 希薄化証券（+1.2M）。WATCH→HOLDは境目 |

残り（5%以下・判定の変化なし）の分類は表のCSVの`cls2`列。

## 3. 何が正しい「1株あたりの分母」か

理論株価は「今の株主の持ち分の価値」を「今の持ち分の数」で割る。分母の候補:

| 候補 | 何を数えるか | 長所 | 短所 |
|---|---|---|---|
| Y yfinance implied（今） | 最新の表紙の発行済（基本、全クラス） | 最新。種類株も全クラスを数える（GOOGL・DELL・META・HEI・TASKでyの発行済より大きい） | **希薄化証券を数えない**（PLTR・SOFI・LITE等で理論株価が5〜11%高く出る）。未確定の株を数える（TSLA）。外部の計算で、期末・時点・クラスが銘柄ごとに違う。部品・DILUTIONと別の基準 |
| 案A 部品の株数（加重平均の希薄化後、直近の四半期、分割換算） | 期間の平均の、希薄化証券込みの株数 | SEC由来で時点が揃う（`first_filed`）。**倍率の部品・DILUTIONの`share_count_change`と同じ関数**（`shares_by_end`）。TSLAの未確定の株は入らず、転換・RSUは入る | 期中に大きく発行／買戻しした銘柄は遅れる（RXRX・IONQ・ONDS・RCATで理論株価が11〜16%高く出る。**発行する銘柄を過大評価する向き**）。年度の平均を使う5銘柄がある（Q4の株数が無い） |
| 案B 期末（表紙・BS）の発行済＋（希薄化後−基本の増分） | 現在の持ち分＋希薄化証券 | 発行済に最も近い。RXRX型を直す | **TSLAで二重に数える**（未確定の株713Mが発行済に入っているうえ、希薄化の増分301Mも足す＝4,254M）。表紙が複数クラスの合計かクラスAだけかが銘柄で違う（LLYで表紙940.7に対しWd 893.4）。種類株（ASTS・PLTR・SOUN）は表紙の次元つきの値が取れず代わりが要る |

**勧め: 案A**。理由:
1. 1つの部品に揃えられる。倍率の部品の時価総額（MULTIPLES-ADOPT-1）・DILUTIONの株数の変化と**同じ株数**になり、同じ画面に違う株数が並ばない
2. 分母の定義が「希薄化後・加重平均」なので、希薄化証券（PLTR・SOFI・LITE）とTSLAの未確定の株の扱いがEPSと同じ基準で決まる
3. 弱点（期中・期末後の発行）は、DILUTIONで決めた**補助の印`outstanding_gap_pct`**（表紙÷加重平均−1が10%超で表示）が既に出す設計なので、同じ印を理論株価の画面にも出して、遅れの大きい銘柄（RXRX・IONQ・ONDS・RCAT・JOBY）に「株数が最新でない」と示す
4. 将来、期末の発行済を使いたければ、部品に`outstanding_diluted`（案B）を別の列として足す。案Bを標準にするのは、未確定の株と種類株の扱いを先に決めてから

**Vのように欠ける銘柄**: 部品の株数が無い（V）・100,000以下の場合は、今のyfinanceのimplied（種類株の全クラスを数える）に**印つきで**戻す（`_shares_source=yf_fallback`）。MULTIPLES-ADOPT-1の時価総額の代わりと同じ形。Vは理論株価が2つの株数の基準で動くため、画面に`yf値`の注記を出す。Layer3の取り込みを直す案（生のXBRLの次元つきの値を換算率で換算）は別の設計。

**年度の平均を使う5銘柄**（AAPL・CPRT・FICO・INTU・ZS）: Q4の株数が無く、部品は年度の加重平均になる。四半期の値（Q4）が取れれば直す、取れない間は`outstanding_gap_pct`の印を出す。

## 4. 全銘柄の試算（`tools/sim`、TANUKI→HypeCore→TANUKI、96銘柄）

前＝kaihatsu。後＝`diluted_shares`を部品の株数（案A）／期末の発行済＋希薄化の増分（案B）に差し替え（`_shares_source`を変えて、Vは今のまま）。

| 見るもの | 案A | 案B |
|---|---|---|
| 理論株価が変わる銘柄 | 92 | 77 |
| 5%超 | 12（ASTS +30.4%・IONQ +15.9%・ONDS +15.6%・RXRX +14.8%・TSLA +11.6%・RCAT +11.6%・SOUN +11.3%・LITE −10.8%・PAYS −8.3%・FICO −7.0%・PLTR −6.4%・ALAB −5.0%） | 10（ASTS +30.4%・SOUN +11.3%・LITE −9.2%・PAYS −9.1%・CAKE −8.6%・TSLA −7.1%・PLTR −6.5%・CART −5.8%・LLY −5.5%・ALAB −5.4%） |
| 判定が変わる | 2（CPRT HOLD→TRIM −3.2%・RMBS WATCH→HOLD −1.6%） | 1（RMBS） |
| funda点 | 変化0 | 変化0 |
| timing点 | 2（JOBY 25→35・RMBS 35→25） | 1（RMBS） |
| HypeCoreのstage（月次の履歴ごと） | 変化0 | 変化0 |
| 保有9銘柄の判定 | 変化0（ADBE −1.6%・APP −1.0%・CELH −1.7%・CRWV +0.1%・NVDA −0.6%・PLTR −6.4%・SOFI −4.4%・SOUN +11.3%・TSLA +11.6%） | 変化0（TSLA −7.1%〈二重に数える〉・SOUN +11.3%・PLTR −6.5%・SOFI −4.8%） |
| STONKS | 点数・判定は`diluted_shares`を読まない（時価総額は部品・yfのまま）。変化0 | 同じ |
| `sim.py scope`（理論株価0.05%超の変化） | 95銘柄中**95**（全員の理論株価が動く。株数の定義が全銘柄に効く＝method_changeの印は全銘柄に付く） | 80 |

案Aで動く理論株価の方向: 発行済が加重平均より大きい銘柄（TSLA・SOUN・RXRX・ONDS・RCAT・IONQ・ASTS）は**上がる**（分母が小さくなる）、希薄化証券が大きい銘柄（PLTR・SOFI・LITE・PAYS・ALAB）は**下がる**（分母が大きくなる）。

## 5. 未公開の5つとの重なりと公開の順の案

| 未公開 | 重なる箇所 | 影響 |
|---|---|---|
| PARSER-LAYER3 段1（revert済み） | `layer3_builder.py`・`bs_lines.py`（借入金・純資産・負債合計） | 株数（`shares_diluted`）は`SPLIT_FIELDS`の扱い。株数の値は変わらない見込みだが、段1の再検証で確かめる |
| TANUKI-CALC-CONTINUITY-1 | `q4_implied.py`（負のQ4）、`pipeline.py` | 株数のQ4の逆算（`_QUARTERLY_NO_DIFF_FIELDS`）に当たらない。重ならない |
| DILUTION-DEFINITION-UNIFY-1 | `common/dilution.py`（`share_count_change`、`outstanding_gap_pct`）、`pipeline.py`の`_load_extra_data`の希薄化率、`stock.html` | **同じ株数**（`shares_by_end`）の基準。`outstanding_gap_pct`が本件の補助の印になる。`pipeline.py` L3217（`_yf_shares`）はDILUTIONが書き換える行 |
| DEFICIT-QUALITY-ADOPT-1 | `pipeline.py`・`tanuki_score/index.html`・STONKS | 株数と重ならない |
| MULTIPLES-ADOPT-1 | `data_fetcher.py`（PER・PSR・時価総額）、`core_calculator.py`（`diluted_shares`の出力の周辺）、`stock.html`の時価総額 | 本件は同じ`data_fetcher.py`の`_determine_diluted_shares`と`core_calculator.py`の`diluted_shares`の行を変える。時価総額の`market_cap_sec`（`株価×部品の株数`）と**同じ株数**になる |

**公開の順の案**: PARSER段1 → CONTINUITY → DILUTION → DEFICIT → MULTIPLES → **本件（最後）**。理由: ①本件は理論株価を全銘柄で動かす計算方法の変更で、他の5つの上に積んでから試算し直すのが安全 ②DILUTIONの株数の基準とMULTIPLESの時価総額が先に揃うので、本件は「分母を同じ部品に付け替える」だけになる ③method_changeの印は全銘柄に付く（scope 95）ので、他の公開と同じ日に重ねない

## 6. 決めてほしいこと

1. 分母は**案A（部品の株数＝加重平均の希薄化後）**でよいか。案B（期末の発行済＋希薄化の増分）にするなら、未確定の株（TSLA）と種類株の扱いを先に決める必要がある
2. 期中・期末後に大きく発行した銘柄（RXRX・IONQ・ONDS・RCAT・JOBY）の遅れは、補助の印`outstanding_gap_pct`の表示だけで許容してよいか（理論株価は+11〜16%高めに出る）
3. 株数が欠ける銘柄（V）は、yfinanceのimpliedに印つきで戻す案でよいか
4. 種類株（ASTS・PLTR・SOUN）の表紙の原本を取る許可（`sec.gov/Archives`の取得に連絡先つきのUser-Agentが要る。環境変数`SEC_USER_AGENT_EMAIL`が手元に無い）。取れれば§2の「未確認」を確かめる
5. 年度の平均を使う5銘柄（AAPL・CPRT・FICO・INTU・ZS）のQ4の株数を、実装の前に直すか（QUARTERLY-BASIS-MIX-1型）、印だけで許容するか

---

# 続き（2026-11-13、決定1・3・4・5を受けて）

決定: 1 案Aを基本 ／ 3 Vはyfinance impliedに印つきで戻す ／ 4 SECの取得を許可 ／ 5 Q4の株数が無い5銘柄は印だけで許容。

## 1. 原本の確かめ

### 1-1. SECの取得（決定4）: **未実施（止まっている）**

許可は「定時の取得と同じ設定の連絡先」だが、その値はGitHubのsecret `SEC_USER_AGENT_EMAIL`にだけあり（secretは読み出せない）、手元の環境変数・ユーザー環境・設定ファイルには無い。連絡先を推測して付けることはしない。`https://www.sec.gov/Archives/...`（表紙のR1.htm・10-Qの本文）は、連絡先なしのUser-Agentでは403（"Undeclared Automated Tool"）。`data.sec.gov`のAPIだけは通るが、表紙の種類株ごとの値・注記の本文は返らない。**ASTS・PLTR・SOUNの表紙と、TSLAの注記の本文は、連絡先の値が使えるようになるまで確かめられない**（その回だけ環境変数に付ける手順は`CLAUDE_CODE_PROCEDURES.md`「SECの連絡先」。回数は`external_api.sec_get`の毎秒10件の制限内）。

### 1-2. TSLAの713M（期末の発行済 − 基本の加重平均）: XBRLの値で分けられる所まで

2026-09-30（accn 0001628280-26-066775）、百万株:

| 値 | 株数 | 意味 |
|---|---|---|
| 期末の発行済（`CommonStockSharesOutstanding`＝`CommonStockSharesIssued`） | 3,953.0（前期末 3,949.0） | 貸借対照表の発行済。四半期で+4M（0.1%）しか増えていない＝期中の発行で説明できない |
| 表紙（`dei`、2026-10-15） | 3,952.8 | 同じ |
| 加重平均 基本（Q3） | 3,240.0 | EPSの基本の分母 |
| 加重平均 希薄化後（Q3） | 3,541.0 | |
| 希薄化の増分（`IncrementalCommonSharesAttributableToShareBasedPaymentArrangements`、Q3） | 301 | 株式報酬（制限付株・オプション・業績連動）が希薄化後に入る分 |

内訳: 期末の発行済 3,953.0 − 基本 3,240.0 = **713M**。このうち**301Mは希薄化後の分母に入る**（株式報酬の増分）。残りの**412M（3,953.0 − 3,541.0）は希薄化後にも入らない**。期末の発行済に入っていて、基本にも希薄化後にも入らない株が412Mある。XBRLの数値だけでは、この412Mが「業績連動で条件が未達の株（未確定）」か、別の種類（例えばトレジャリー・信託の株）かは**区別できない**。10-Qの注記（EPSの注・株式報酬の注）の本文が要るので、1-1の許可の実行が先。

## 2. 案A2の試算

### 2-1. 定義と当てる条件

**案A2** = 加重平均の希薄化後株数（Wd）× （表紙の最新の発行済 ÷ 同じ期の基本の加重平均 Wb）。**基本÷基本の比**なので、希薄化の増分（Wd−Wb）は比に入らない。当てる条件（全て満たす時だけ。満たさなければ案A）:
1. 表紙が1値（種類株で複数値でない）・表紙の日付が直近四半期末より後（120日以内）
2. Wdが四半期の加重平均（年度の平均でない）で、Wbが同じ期にある
3. **増え方の検査**: 比−1（e）が4%超で、表紙の前の四半期の表紙からの増え方（n）がe/3未満なら当てない（期中の発行で説明できない差＝EPSの分母に入らない株〈制限付株・信託等〉が含まれるため）

### 2-2. 二重の計上が起きないか（全銘柄、機械的）

| 確かめ | 結果 |
|---|---|
| A2を当てられる銘柄 | 60（表紙が四半期末より後で1値・Wbが同じ期） |
| A2とその加算形（表紙＋(Wd−Wb)）の差 | 条件3で外れない57銘柄で最大**0.31%**＝比に希薄化の増分は入っていない（比は基本÷基本） |
| 条件3で外れる銘柄 | **TSLA・CAKE・LLY**（3）。表紙÷Wbが+22%・+6.6%・+5.5%なのに、表紙の増え方が+0.1%・+0.2%・−0.1%。期中の発行でない差（TSLA 412M〈上の1-2〉・LLYは従業員給付信託の株が発行済に入る型の可能性・CAKEの制限付株。**原本の本文は未確認**）。これらにA2を当てると、発行済に入っているがEPSの分母にない株を、希薄化の増分と**二重に数える**（TSLAでA2＝4,320M、加算形＝4,254M） |
| 単位の異常 | LOARの`WeightedAverageNumberOfSharesOutstandingBasic`が`company_facts`で千株単位（93,688）。Layer3のWdは換算済み。A2の実装は`company_facts`でなくLayer3の`shares_basic_weighted_avg`を読む（FINDINGS） |

### 2-3. 全銘柄の試算（`tools/sim`、前＝kaihatsu、96銘柄、funda・stageは全て変化0）

| | 案A | 案A2（条件3つき） | 案A2＋TSLAの未確定の株を入れる |
|---|---|---|---|
| 理論株価が変わる | 92 | 82 | 81 |
| 5%超 | 12 | 9（ALAB −5.4・ASTS +30.4・CART −5.9・FICO −7.0・LITE −9.1・PAYS −9.2・PLTR −6.4・SOUN +11.3・TSLA +11.6） | 8（TSLAが0.0に） |
| 判定が変わる | 2（CPRT HOLD→TRIM・RMBS WATCH→HOLD） | 同じ2 | 同じ2 |
| funda点・stage | 0・0 | 0・0 | 0・0 |
| 保有9銘柄の判定 | 変化0 | 変化0 | 変化0 |
| scope（理論株価0.05%超） | 95 | 87 | 86 |

保有9銘柄の理論株価（前比）: ADBE −1.6→**−0.1**、APP −1.0、CELH −1.7→**−0.9**、CRWV +0.1、NVDA −0.6→**−0.2**、PLTR −6.4、SOFI −4.4→−4.8、SOUN +11.3、TSLA +11.6（入れない）／**0.0（入れる）**（A→A2で動くのは、期末後に増えた銘柄の表紙で直す分）。

### 2-4. 期中・期末後に発行した銘柄と実際の株数（百万株）

実際＝表紙（`dei`、原本のXBRL）／期末の発行済（貸借対照表）。IVの変化は前（yfinance implied）比。

| 銘柄 | yf implied | 案A（Wd） | 案A2 | 表紙（日付）／期末の発行済 | IV 案A→案A2 |
|---|---|---|---|---|---|
| RXRX | 623.4 | 543.0 | 622.8 | 622.8（10-26）／561.1 | +14.8% → **+0.1%** |
| IONQ | 464.7 | 400.9 | 446.1 | 432.5（10-28）／408.9 | +15.9% → **+4.2%**（A2＝Wd×1.113。Wdの希薄化の増分12Mが乗る。yfは表紙より+7.5% 大きく理由は未確認） |
| ONDS | 582.3 | 503.6 | 573.8 | 570.6（08-11）／529.8 | +15.6% → **+1.5%** |
| RCAT | 152.7 | 136.9 | 152.7 | 152.7（08-04）／152.7 | +11.6% → **0.0%** |
| JOBY | 1,016.6 | 978.5 | 1,016.6 | 1,016.6（11-02）／1,013.0 | +3.9% → **0.0%** |
| TSLA | 3,952.8 | 3,541.0 | 3,541.0（条件3で外れる）／入れる＝3,952.8 | 3,952.8（10-15）／3,953.0 | +11.6%（入れない）／0.0%（入れる） |
| ASTS | 390.1 | 299.1 | 299.1（表紙が取れない） | 表紙・期末とも`company_facts`に無い（**原本は未確認**） | +30.4% |
| SOUN | 488.4 | 438.6 | 438.6（表紙が取れない） | `dei`は2022年の旧値のみ（**原本は未確認**）。Q3の10-Qは11-09提出でLayer3未収載 | +11.3% |
| PLTR | 2,409.5 | 2,573.9 | 2,573.9（表紙が取れない） | 期末の発行済 2,409.2。表紙は次元つきで未確認 | −6.4% |

A2は、期末後・期中の発行で加重平均が遅れる5銘柄（RXRX・ONDS・RCAT・JOBY・IONQ）の理論株価のずれを、+11〜16%から+0〜4%へ縮める。

## 3. 結論と決めてほしいこと

- **A2は、期末後・期中の発行の遅れを直す点で案Aより正確**で、二重の計上は条件3で除ける（機械的に確認済み、0.31%以内）。ただし表紙が取れる60銘柄にしか当たらず（ASTS・PLTR・SOUNは当たらない）、**条件3の閾値（e＞4%・n＜e/3）は今回の98銘柄に合わせた経験的な値**。**勧め: A2（条件3つき）を採る**。判定の変化は案Aと同じ2銘柄（CPRT・RMBS、どちらも境目）で、funda・stageは動かない
- **TSLAの未確定の株**: 入れない（案A＝3,541M、IV +11.6%）か、入れる（表紙 3,953M、IV 0.0%＝今のまま）か。XBRLの数値では**301Mは希薄化後に既に入り、412Mはどちらにも入らない**ことまで分かった。412Mの正体（未確定の株か別か）は10-Qの注記の本文が要る。**勧め: 本文を確かめるまで入れない（案A）。入れるなら表紙の3,953Mでなく、希薄化後＋未確定の分で二重に数えない形**
- 決めてほしいこと:
  1. **A（Wd）かA2（条件3つき）か**
  2. **TSLAの未確定の株: 入れる／入れない**（本文の確認の後でもよいか）
  3. 原本の確認のために、`SEC_USER_AGENT_EMAIL`の値（定時と同じ連絡先）を、この回だけ環境変数で使う方法（`! SEC_USER_AGENT_EMAIL='…' python …`の形で渡す、または値をこのセッションに渡す）

# 仕上げ（2026-11-13、決定1〜3を受けて）

## 1. SEC の取得（決定3）: **未実施（止まっている）**

環境変数 `SEC_USER_AGENT_EMAIL` がこのセッションに設定されていない（`-n` の有無だけ確認。値は見ていない・書かない）。決定3に従い、ASTS・PLTR・SOUN の表紙と TSLA の 10-Q 本文（412M の内訳）は取っていない。IONQ の yfinance と表紙の +7.5% の差の理由も、8-K 等の原本が要るため未確認。

## 2. 案A2（実際の発行で決める版）の試算

### 2-1. 定義（閾値なし）

- 補正後 = Wd × (1 + 発行の分 ÷ Wb)。Wd・Wb は同じ期（Layer3 の部品の期末。Wb は `company_facts` の同じ期の Wb÷Wd の比を Wd に掛けて単位をそろえる＝LOAR の千株単位を避ける）
- c0 = 前の期の表紙（`dei`）、c1 = 最新の表紙。どちらも1値（種類株でない）で、c1 の日付が期末より後・120日以内
- 発行の分 = max(0, (c1−c0) − 加重平均に既に入っている分)。既に入っている分 = (c1−c0) の範囲で clip(Wb−c0, 0, c1−c0)
- **e>4%・n<e/3 のような閾値は使わない。** 発行が無ければ（c1≦c0、または Wb が既に c1 側にある）補正は 0
- 当たる前提（構造の条件）: Wd が四半期の加重平均・表紙が1値で期末より後。当たらない銘柄は案A（補正0）。前の A2 と同じ60銘柄に表紙が取れる

### 2-2. 結果（60銘柄に表紙の連続値あり）

| | 前のA2（閾値つき） | 新A2（発行の分だけ） | 新A2・符号つき（期末後の買戻しも引く） |
|---|---|---|---|
| 補正が働く銘柄 | 57（買戻し側の縮みを含む） | **29**（補正＞0） | 29＋買戻し21（−0.0〜−1.6%） |
| 補正の大きさ | RXRX +14.7%…買戻し側 −5% まで | 1%超は6銘柄（RXRX +14.7・ONDS +13.9・RCAT +11.6・IONQ +11.3・JOBY +2.8・CART +1.6）、残り23は1%以下 | 同左＋買戻し側 |
| **TSLA・CAKE・LLY が自然に0か** | 閾値で除外 | **LLY=0（c1<c0）。TSLA=+0.1%（c0→c1の+3.2M）・CAKE=+0.2%（+0.09M）**。閾値なしで、差の412M・CAKEの差は入らない（表紙の増え方に基づくため）。0ではなく実際の増加分だけ入る | 同左 |

TSLA の IV は案Aと比べて +11.6% → +11.5%（Wd 3,541.0→3,544.5M）。412M は入らない（決定2）。

### 2-3. 全銘柄の試算（`tools/sim`、前＝kaihatsu、96銘柄）

| | 案A | 前のA2 | **新A2** | 新A2・符号つき |
|---|---|---|---|---|
| 理論株価が変わる銘柄 | 92 | 82 | **88** | 86 |
| 5%超 | 12 | 9 | **9**（ALAB・ASTS・CART・FICO・LITE・PAYS・PLTR・SOUN・TSLA） | 9 |
| 判定が変わる | 2（CPRT HOLD→TRIM・RMBS WATCH→HOLD） | 2 | **2（同じ）** | 2 |
| funda・stage | 0・0 | 0・0 | **0・0** | 0・0 |
| scope（変わる銘柄） | 95 | 87 | **95** | 95 |

保有9銘柄の判定はどれも変わらない。理論株価（前比）: 新A2 = ADBE −1.6・APP −1.0・CELH −1.7・CRWV +0.1・NVDA −0.6・PLTR −6.4・SOFI −4.8・SOUN +11.3・TSLA +11.5（符号つきでは ADBE −0.1・CELH −0.9・NVDA −0.2）。

新A2が前のA2より変わる銘柄が多いのは、**期末後の買戻し側の補正（ADBE・BKNG・CEG・MRVL・LITE・RKLB 等、約30銘柄）を採らない**から（「発行が無ければ0」）。LITE（前のA2 −9.1→新A2 −10.8%）・ADBE（−0.1→−1.6%）が案A寄りに戻るのは、買戻し側を採らない分。符号つきは買戻しも引くので前のA2に近い（ADBE −0.1・CELH −0.9・NVDA −0.2）。

### 2-4. 実際の株数との並べ（百万株、IV 前比）

| 銘柄 | yf implied | 案A | 前のA2 | **新A2** | 表紙（c1） | IV A → 前A2 → 新A2 |
|---|---|---|---|---|---|---|
| RXRX | 623.4 | 543.0 | 622.8 | **622.8** | 622.8（10-26） | +14.8% → +0.1% → +0.1% |
| IONQ | 464.7 | 400.9 | 446.1 | **446.1** | 432.5（10-28） | +15.9% → +4.2% → +4.2%（Wdの希薄化の増分が乗る。yfは表紙より+7.5%、理由は未確認） |
| ONDS | 582.3 | 503.6 | 573.8 | **573.8** | 570.6（08-11） | +15.6% → +1.5% → +1.5% |
| RCAT | 152.7 | 136.9 | 152.7 | **152.7** | 152.7（08-04） | +11.6% → 0.0% → 0.0% |
| JOBY | 1,016.6 | 978.5 | 1,016.6 | **1,006.1** | 1,016.6（11-02） | +3.9% → 0.0% → **+1.1%**（c0=989.1が既にWb 978.5より大きく、c0〜Wbの差10.6Mは「c0→c1の増加」に入らないため拾わない。式どおり） |
| TSLA | 3,952.8 | 3,541.0 | 3,541.0 | **3,544.5** | 3,952.8（10-15） | +11.6% → +11.6% → +11.5%（412M は入らない） |
| ASTS | 390.1 | 299.1 | 299.1 | **299.1** | 表紙なし（SEC未取得） | +30.4%（同じ） |
| SOUN | 488.4 | 438.6 | 438.6 | **438.6** | 表紙は2022年の旧値のみ | +11.3%（同じ） |

RXRX・IONQ・ONDS・RCAT は新A2でも表紙どおりに直る。JOBY だけ式の性質で1%強ずれる。

### 2-5. 二重の計上の機械的確認

- 補正の発行の分は、全29銘柄で c1−c0 以下（確認済み）
- 補正後が加算形（c1＋Wd−Wb）を超える幅: 最大 IONQ +0.31%・CART +0.09%・ONDS +0.07%、他は0.03%以下＝発行の分に Wd÷Wb を掛ける形の差（希薄化の増分の乗り方）で、二重の計上ではない
- TSLA は発行の分 3.2M（0.1%）だけで、412M・301M は入っていない。CAKE も同様（+0.09M）

## 3. 決めてほしいこと

- **新A2（実際の発行・閾値なし）を採る** — 閾値が無く、TSLA・CAKE・LLY が自然に0か小さい。RXRX・IONQ・ONDS・RCAT は前のA2と同じに直る。判定・funda・stage・保有の判定は案A・前のA2と同じ
- 決めてほしいこと:
  1. **期末後の買戻し（約21銘柄、−0.0〜−1.6%）を引くか**（符号つき）。引かない版（「発行が無ければ0」の字どおり）を新A2とした。引くと ADBE −0.1・CELH −0.9・NVDA −0.2 と前のA2に近づく
  2. TSLA の412M は決定2どおり入れず、画面・report.txt の印 `outstanding_gap_pct`（表紙÷Wd−1＝+11.6%）で出す。その印を今の案A/A2の実装の対象にするか
  3. `SEC_USER_AGENT_EMAIL` を設定した環境で、ASTS・PLTR・SOUN の表紙と TSLA の10-Q本文の取得を行う（値は書かない）

# 実装済み（ブランチ・未公開、2026-11-13）

ブランチ `tanuki-shares-single-source-1`（`multiples-adopt-1`〈`587496219b`〉の上、`8cdf37598c`。originに控えあり。kaihatsuには無い・PRなし）。決定: 分母は新A2（発行の分＝表紙の前→最新の純の変化〈買い戻しも引く〉から加重平均に既に入っている分を除く。閾値なし）、TSLA型の差は分母に入れず印（`outstanding_gap_pct`）で出す、Vは yfinance implied を印つき、Q4の株数が無い銘柄は印だけ。

## 実装

- `common/shares_base.py`（新）: 株数 = Wd × (1 + 発行の分 ÷ Wb)。`corrected_shares` が補正の計算の唯一の場所（`|発行の分| ≤ |c1−c0|`＝二重に数えない）。`base_shares` が表紙（dei）の前後・同じ期のWb（`company_facts`の同じ期の Wb÷Wd の比で単位を揃える。LOAR型を避ける）・分割・種類株・年度平均を見て、補正できない時はWdのまま `shares_note` に理由（`no_cover`・`cover_multi_class`・`wd_not_quarterly:…`・`cover_not_after_period_end:…`・`split_after_period_end`・`no_previous_cover`）
- `common/multiples.py`: 部品の `current` の行の株数＝`base_shares`の値。時価総額（`at_price`）・TANUKIの分母・STONKS・HypeCoreが**同じ値**。月末の行（履歴）は今までどおりWd。印 `shares_wd`・`shares_basis`・`shares_issued`・`shares_note`・`cover_shares`・`outstanding_gap_pct`
- `data_fetcher._determine_diluted_shares`（分母の唯一の書き手）: 部品の株数を読む（`_shares_source`＝`sec_weighted`／`sec_weighted_plus_issuance`）。部品に株数が無い銘柄（V）は `yf_implied_fallback`（印）。`components` に `shares_wd`・`shares_issued`・`shares_note`・`shares_cover`・`shares_cover_date`・`outstanding_gap_pct`、`report.txt` に `Shares_Basis`・`Shares_OutstandingGap`（表紙が分母より10%超違う時）、`stock.html` の「希薄化後株式数」の下に印
- `config/method_change.json` に `TANUKI-SHARES-SINGLE-SOURCE-1-1`（scope 96銘柄。`committed_at`は公開前の統合で作り直す）。部品 `docs/common/multiples/*.json`・出力の一覧（tanuki・hypecore・eps・stonks）を作り直し
- テスト `tests/test_shares_base.py`（11件）: 発行が無ければ補正0・買い戻しで下がる・加重平均に入っている分は足さない・TSLA型は入らない・表の格子で`|発行の分|≤|c1−c0|`・補正の種類の印・LOAR型・部品の株数＝`base_shares`（時価総額と同じ値）・分母の書き手は1か所

## 試算（`tools/sim`、前＝`multiples-adopt-1`、後＝ブランチ、TANUKI→HypeCore→TANUKI、96銘柄）

| 項目 | 前→後 |
|---|---|
| 理論株価が変わる | 86銘柄（5%超は9銘柄） |
| 判定が変わる | 2（CPRT HOLD→TRIM・RMBS WATCH→HOLD） |
| funda・stage | 変化0・0 |
| 保有9銘柄の判定 | 変化0。理論株価（前比）: ADBE −0.1・APP −1.0・CELH −0.9・CRWV +0.1・NVDA −0.2・PLTR −6.4・SOFI −4.8・SOUN +11.3・TSLA +11.5 |
| scope | 96銘柄（理論株価・乖離率・分母の印などが変わる銘柄 90） |
| 分母の出どころ | `sec_weighted_plus_issuance` 50・`sec_weighted` 45・`yf_implied_fallback` 1（V） |
| 補正の印 | 表紙が取れず補正なし（`no_cover`）20・年度平均（`wd_not_quarterly`）5・表紙が期末後でない 11 |

### 5%超の9銘柄と判定が変わる2銘柄の理由（原本の値は上の「原本で確かめた一覧」）

| 銘柄 | 理論株価 | 理由 |
|---|---|---|
| ASTS | +30.4% | yf implied 390.1（全クラス）→ Wd 299.1。種類株。表紙が`company_facts`に無く補正なし。**SECの取得待ち（次回の確認手順33）** |
| TSLA（保有） | +11.5% | 3,952.8→3,544.5。表紙の3,953と加重平均の差412Mは未確定の株で、発行でも希薄化後でもない。**入れない**（補正は表紙の増加3.2Mだけ）。印 `outstanding_gap_pct` +11.5%。本文は未確認（SEC取得待ち） |
| SOUN（保有） | +11.3% | Layer3が1四半期遅れ（Q3の10-Qは11-09提出）。表紙は2022年の旧値のみで補正なし。次の日曜のSEC_Data_Updateで追いつく |
| LITE | −10.8% | 希薄化証券（転換社債のif-converted +9.2M）。発行の補正は0（表紙の増加1.0Mは基本の加重平均に既に入る） |
| PAYS | −9.2% | 希薄化証券（+6.1M）。補正 +1.0%（Layer3がQ3未収載のため期末後の発行） |
| FICO | −7.0% | 年度の平均（Q4の株数が無い）。補正なし・印のみ |
| PLTR（保有） | −6.4% | 希薄化証券（オプション・RSU +168M）。表紙は次元つきで補正なし |
| CART | −5.9% | 希薄化証券（希薄化後−基本 14.6M）。補正 +1.6%（表紙 231.5→236.6M） |
| ALAB | −5.4% | 希薄化証券（+10M） |
| **CPRT**（判定） | −3.2% | 年度の平均＋希薄化証券。HOLD→TRIMは境目 |
| **RMBS**（判定） | −1.6% | 希薄化証券（+1.2M）。WATCH→HOLDは境目 |

IONQ・RXRX・ONDS・RCAT・JOBYは期末後・期中の発行を表紙で補い、前の案Aの+11〜16%が+0〜4%へ縮む（RXRX +0.1・IONQ +4.2・JOBY +1.1。ONDS・RCATは試算の表のとおり）。

## DILUTIONとの重なり（`dilution-unify-1`、未公開）

- 株数の元は同じ（Layer3・`shares_by_end`の加重平均の希薄化後・分割換算）。`share_count_change`は補正前のWdの系列でCAGRを出すので変えない（CAGRの端点に表紙の補正を混ぜない）。TANUKI・MULTIPLESの株数だけ `shares_base` の1か所
- `outstanding_gap_pct`の名前が重なる: DILUTIONの`financial_health.outstanding_gap_pct`は期末の発行済÷加重平均−1（`pipeline.py`・`stock.html`の希薄化の欄）、本件の`components.outstanding_gap_pct`は表紙÷分母−1（分母の欄）。場所が違い衝突しないが、画面の注記が2つ並ぶ。公開後に1つにするかは別の判断（FINDINGS）
- `pipeline.py`の同じ周辺を触る（`Valuation_Multiples`の下の`Shares_Basis`の行。DILUTIONは希薄化の行）。rebase時に衝突したら両方残す

## 公開の手順（公開の順: PARSER段1 → CONTINUITY → DILUTION → DEFICIT → MULTIPLES → **本件**）

各公開の後、そのたびに:

1. kaihatsuの最新に`tanuki-shares-single-source-1`をrebaseする（`multiples-adopt-1`の公開の後なら`git rebase kaihatsu`。MULTIPLESの公開前に本件だけ先に出さない＝本件は`multiples-adopt-1`の上）
2. `python common/multiples.py --all` → `python scripts/inventory/build_output_inventory.py --layer {tanuki,hypecore,eps,stonks}`。PARSER段1・CONTINUITYは分母の元（Wd・Wb・TTM）を動かすので、作り直した後に試算し直す
3. `python tools/sim/sim.py chain before/after`（前＝rebase前のkaihatsu）→ 理論株価・判定の変化が上の表と同じ（PARSER等の前の公開で動いた分は前後で0になる）。`python tools/sim/sim.py scope before after --id TANUKI-SHARES-SINGLE-SOURCE-1-1 --write`で`method_change.json`の`scope`・`committed_at`を作り直す
4. ゲート（`PYTHONUTF8=1 python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider --deselect tests/test_common_path_guard.py::test_no_open_without_encoding_ruff_plw1514`・`common/sec_data/audit.py`・`report_consistency_check.py --fail-on-ng`）→ kaihatsuに`--no-ff`で統合して`git push`
5. 連鎖（TANUKI VALUATION → HypeCore → TANUKI、3つ目は手で起動しない）の後、`components._shares_source`が`sec_*`（Vだけ`yf_implied_fallback`）・`method_change`の印がscope銘柄だけに付く・2回目は0件。理論株価の変化が試算（TSLA +11.5・SOUN +11.3・ASTS +30.4等）と一致
6. 画面・report.txtの目視: TSLAの「希薄化後株式数」の下に「表紙の発行済は +11.5%（分母に入れない）」、`Shares_OutstandingGap`の行
7. 公開後、BACKLOGの`[[TANUKI-SHARES-SINGLE-SOURCE-1]]`をBACKLOG_DONE.mdへ移設

SECの原本（ASTS・PLTR・SOUNの表紙・TSLAの10-Q本文・IONQ）は`SEC_USER_AGENT_EMAIL`が環境変数にある時だけ取る。無ければ次回の確認手順33に残した。
