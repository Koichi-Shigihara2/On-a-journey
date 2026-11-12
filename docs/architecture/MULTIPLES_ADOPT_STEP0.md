# MULTIPLES-ADOPT-1 STEP 0（2026-11-13、読み取りと手元の試算のみ）

コードのcommit・push・公開は無し。全98銘柄の突き合わせ表は `MULTIPLES_ADOPT_STEP0_table.csv`。道具はscratchpadの`ma/`（`cur.py`＝部品の「今日」の行を全銘柄で作る・`t1.py`＝突き合わせ表・`hook_adopt.py`＝試算フック・`cmp.py`＝前後の比較・`ptbv.py`＝P/TBVのTCEを時点の値にする試算・`why.py`＝純利益の原本確認）。基準はkaihatsuの今のコード（`9adfa15898`、公開）。

## 結論

| 見るもの | 結果 |
|---|---|
| 計算の場所 | 11-05の表の9か所は**増減なし**（grepし直した）。ただし1行**古い**: TANUKI `stock.html`のPSRはもう「最新の年次」でなく**TTM売上**（2026-09-26から。取れない時だけ最新年度）。残る問題は「分子が株価×**yfinance由来の株数**」（下の§1） |
| 値が何に効くか | `per`・`ps`・`max_eps_per`・HypeCoreの`psr`・`trailing_pe`・STONKSの`psr`・ROTCEの`ptbv`は**理論株価・判定・funda点・timing点・stage・TANUKI SCOREのどれにも効かない**（表示・RICE/PERの比・DCF妥当性のFフラグだけ）。HypeCoreのstageが読む倍率は`forward_pe`（S3の条件C＞60、S4の底打ちの除外＞100）と`peg`で、**どちらも部品に無い**（フォワードは過去が取れない） |
| 部品と今の読み手のずれ | 「PER」（yfinanceのtrailing）98銘柄: **一致63・時点の株価2・分母〈純利益〉10・比較不能23**（赤字等でyf無20・部品無2・両方1）。時価総額（yf対 株価×SECの株数）97銘柄: 一致62・株数の違い18・株価の時点17。P/TBV（ROTCE）対部品: **69銘柄で差0** |
| 全銘柄の試算（TANUKI→HypeCore→TANUKI、96銘柄） | per・PSRを部品に差し替えて**理論株価・判定・funda点・timing点・保有9銘柄・stage・scope すべて変化0**。変わるのは表示の値だけ（`components.ps` 96・`components.per` 78・`rice_per_ratio` 54・`per_is_forward` 3、HypeCoreの月次の`psr`・`trailing_pe`）。DCF妥当性のFフラグだけ4銘柄が反転（ADSK・CELH・ZETA 真→偽、PAYS 偽→真） |
| P/TBVのTCEを時点の値（first_val）に | 98銘柄中**31銘柄**で履歴の四半期のP/TBVが変わる（最大 IONQ +714%・CELH +96%・SPIR +75%）。**シグナル（割安・割高）の変化0**、パーセンタイルが5pt以上動くのは ALAB 64→73・CELH 29→35 の2銘柄 |
| 見つけた穴 | **V**は`shares_diluted`がLayer3に無く、部品の70か月すべてが`no_shares`（倍率が1つも出ない）。部品に寄せるとVの`per`が24.8（forward）に落ちる（FINDINGS） |

## 1. 計算の場所の確かめ直し（2026-11-13）

| 場所 | 倍率 | 分子 | 分母 | 効く先（書き手→読み手） |
|---|---|---|---|---|
| `common/market_data/fetcher.py` L1207〜1213 | `trailing_pe`・`forward_pe`・`price_to_sales`・`ev_to_ebitda`・`market_cap` | yfinance | yfinance | `attributes/{T}.json` → TANUKI・HypeCore・STONKS。週1回の履歴は`valuation_history/`に積む（11-05〜） |
| TANUKI `data_fetcher.py` L804〜828 | `per`（trailing、無ければforward〈`per_is_forward`〉）・`peg`・`ps`・`ev_ebitda` | 上のyf | 上のyf | `components.per/ps/peg/ev_ebitda` → ①`core_calculator.py` L752（RICE/PERの比、`rice_per_ratio`）②画面（TANUKI SCOREの「GAAP PER」列・低PERの印）③`dcf_validity_checker.py`のF（GAAP PERと調整後PERの差±10x）④report.txtのValuation_Multiples。**理論株価・判定・funda・timingは読まない** |
| TANUKI `core_calculator.py` L950 | `per_adjusted` | 今の株価 | 調整後EPS（EPS ANALYZERのTTM） | 同上②③ |
| TANUKI `pipeline.py::compute_max_eps` L217〜248 | `max_eps_per` | 今の株価 | (TTM純利益＋SBC)÷TTMの加重平均株数 | 画面の「最大EPSのPER」列・乖離の列。表示のみ |
| TANUKI `stock.html` L996〜1011 | PSR・フェアPER・PEG | 株価×`diluted_shares`（**yf由来**、`_shares_source=yf_implied` 96/96） | `revenue_ttm`（無ければ最新年度） | 画面の表示のみ。JSで再計算している（`components.ps`を読んでいない） |
| HypeCore `hypecore.py` L241〜255・`detail.html` | `forward_pe`・`trailing_pe`・`psr`・`ev_ebitda`・`peg` | yf属性 | 同 | `_poc.json`の月次。**stageに効くのは`forward_pe`（L741・L895）と`peg`（L743）だけ**。`psr`・`trailing_pe`は保存して画面に出すだけ |
| STONKS `pipeline.py` L150〜165 | `psr`・`ev_sales` | yfの時価総額・EV | Layer3のTTM売上 | STONKSの画面の表示。`analyzer.py`の点数は読まない |
| ROTCE `rotce.py` L501〜549 | `ptbv`（自社比のパーセンタイル） | 四半期末の終値×その期の希薄化後株数（`shares_by_end`、部品と共通） | TCE（履歴は**最新の申告の値**） | `_summary.json` → 画面（TANUKI SCOREのROTCE図・EPS ANALYZER）。表示のみ |
| `dcf_validity_checker.py`・EPS ANALYZER画面 | `per`と`per_adjusted`の差 | TANUKIの2値 | 同 | Fフラグ・画面の注記 |

## 2. 部品の値と今の読み手の値（全98銘柄、`MULTIPLES_ADOPT_STEP0_table.csv`）

部品の「今日」の行は、`common/multiples.py::compute`を月末でなく最新の日足の日（2026-11-12）で呼んで作った（`cur.json`。現在の部品は前月末までで、毎日の読み手には古い）。

### 2-1. 「PER」（yfinance trailing対 部品の`per`＝株価×SEC株数÷TTM純利益）

差を 株価の時点・株数・純利益 の3つに分け、いちばん大きい要因で分類した（`ln(部品/yf)`の分解）。

| 分類 | 銘柄数 | 内容 |
|---|---|---|
| 一致（5%未満） | 63 | |
| 時点の株価 | 2 | AMD・ELF。`attributes`の株価は取得時刻（11-12 10:25 UTC）の値で、部品は11-12の終値。分母は一致 |
| 分母（純利益） | 10 | 下の表 |
| 比較不能 | 23 | yfが無い20（赤字で`trailing_pe`なし。TANUKIは`forward_pe`で代用して`per_is_forward`）・部品が無い2（V＝株数の欠け、ZETA＝純利益−2M）・両方1（KULR） |

**5%を超えて動く12銘柄の原因**（部品の純利益は`net_income`のTTM＝直近4四半期の合計。四半期の値は`store`の`NetIncomeLoss`〈accn付き〉で確認）。yf側のEPSの内訳は原本が取れないため「候補」と書いたものは**未確認**。

| 銘柄 | yf→部品 | 要因の分解（株価／株数／純利益） | 原因 |
|---|---|---|---|
| AMD | 118.1→127.6 | +5.9%／+1.3%／−0.7% | 株価の時点（`attributes`の取得時刻11-12 10:25 UTCと日足終値の違い）。純利益はTTM 7,619M〈1,511＋1,383＋2,297＋2,428、原本〉でyfと一致 |
| ELF | 83.4→78.7 | −4.2%／+1.6%／+3.2% | 株価の時点 |
| CEG | 22.1→29.3 | +2.8%／+2.4%／−20.8% | **SECの取り込みが1四半期遅れている可能性**: 部品のTTMは2026-06-30まで〈930＋432＋1,590＋513=3,465M、原本のNetIncomeLoss〉で2026Q3が無い。yfの暗黙の純利益は4,374M。yfがQ3を含むかは**未確認**（SEC_Data_Updateの次の回で追いつくか見る） |
| VST | 18.0→20.6 | +0.8%／0.0%／−11.6% | 同上（TTM end 2026-06-30、2,219M。未確認） |
| PAYS | 28.7→37.3 | +0.1%／+9.0%／−15.9% | 同上（TTM end 2026-06-30、15.8M。未確認）＋株数+9% |
| CELH | 37.1→25.6 | −3.5%／+1.7%／+42.2% | 純利益の定義（候補: 優先株の配当・累積の控除後のEPSをyfが使う。SECのTTM純利益は317.1M〈直近4四半期24.7＋110.1＋55.3＋127.0〉）。**未確認** |
| CRM | 19.9→18.5 | −0.2%／−0.2%／+7.4% | 純利益の定義（SEC TTM 9,662Mが、yfの暗黙の純利益より7%小さい）。**未確認** |
| HON | 11.3→10.1 | −1.8%／+0.7%／+10.3% | 2026Q2の純利益5,682M（分社の利益と推定）の扱い。**未確認** |
| IONQ | 19.7→48.5 | −9.4%／−13.7%／−68.3% | 公正価値の変動で四半期の純利益が±1〜2B動く〈−1,055・+754・+805・−1,868・+564〉。TTM 255Mは4つの合計で原本と一致。yfの暗黙の純利益は約804M。期の取り方か評価損益の扱い。**未確認** |
| IOT | 301.7→269.4 | −2.2%／+0.8%／+10.5% | 純利益の定義。TTM 90.6M。**未確認** |
| LYFT | 2.6→2.2 | −3.9%／+0.2%／+10.3% | 2025Q4の純利益2,755M（税の評価引当の戻しと推定)がTTM 2,810Mに入る。yfは別の扱い。**未確認** |
| SITM | 472.6→554.3 | −0.4%／+3.2%／−12.4% | 純利益が小さく〈TTM 26.5M〉、わずかな差が倍率を大きく動かす |

**部品が変えるフォワード／赤字の扱い（3銘柄）**: ONDS（yf trailing無・forward −286.5〈負の倍率が「PER」の列に出ていた〉→ 部品16.0。営業赤字でも営業外の利益でTTM純利益が+167M）、V（部品なし→ forward 24.8に落ちる）、ZETA（yf 235.9→部品なし〈純利益−2M〉→ forward 28.0）。**部品がNoneのときforwardに自動で落とす今の仕様は、PERの列の意味を銘柄ごとに変える**（`TANUKISCORE-PER-FORWARD-MIX-1`の残り）。

### 2-2. 時価総額・PSR

| 比べ | 結果 |
|---|---|
| 時価総額（yf対 株価×SECの株数） | 97銘柄で一致62・株数18・株価の時点17。株数の差が大きい銘柄は **ASTS −23%**（複数クラスの株の可能性、未確認）・IONQ −14%・ONDS −14%・RXRX −13%・SOUN −10%・RCAT −10%・RKLB −6%・TSLA（制限付株、DILUTION STEP 0のgap） |
| STONKSのPSR（yf時価総額÷Layer3 TTM売上）対 部品の`時価総額÷TTM売上` | 24銘柄中17で3%超: 全部が分子（株数と株価の時点）。分母は同じTTM売上 |
| yfの`price_to_sales`対 部品 | 97銘柄で38が3%超。上の差に加えて、分母（yfの売上の期や定義）の違いの候補（CEG +15%・CRWV +21%・PAYS +19%・HON +11%・MO −11%、未確認） |
| TANUKI `stock.html`のPSR（価格×**yf由来の`diluted_shares`**÷TTM売上）対 部品 | 95銘柄で24が3%超（ASTS −23%・IONQ −14%・RCAT −13%・LITE +12%） |
| HypeCoreの`psr`対 部品 | 53銘柄が5%超: `_poc.json`の最新月はHypeCoreの実行時点の値で、部品の今日の値より古い（時点の差。内訳は未分解） |
| ROTCEの`ptbv`対 部品の`p_tbv`（今日） | **69銘柄で差0**（同じ`shares_by_end`・同じ`tce_as_of`） |

**TANUKIの`diluted_shares`（1株理論株価の分母）**: 96/96が`yf_implied`。SECの加重平均の希薄化後株数と3%を超えて違うのが23銘柄（ASTS +30%・IONQ +16%・ONDS +16%・RXRX +15%・RCAT +12%・TSLA +12%・SOUN +11%・LITE −11%…）。これは**理論株価そのもの**（IV∝1÷株数）に効くので、本項目の「時価総額を1通りに」とは**分ける**（下の§3）。

## 3. 案

### 3-1. 寄せる順

| 段 | 内容 | 効くもの | 理由 |
|---|---|---|---|
| 0（部品） | 部品に**最新の行**（`current`、最新の日足の日の株価×その日までに申告された値）を足す。読み手は「部品のTTM・株数・TCEの値」を取り、**生の株価で割る**（`multiples.at_price(row, price)`）。月末の行は今のまま | 部品のみ（TANUKI・HypeCoreの定時の実行は日次、部品は週1回＋SEC更新の後。週の途中の株価に追従させるため） | 部品は前月末までで、読み手の今の値に使えない |
| 1 | TANUKIの`per`・`ps`（`data_fetcher.py` L804〜828）を部品から。部品がNoneの時は**空**にして、forwardは別の列（`per_forward_yf`）に分ける。`stock.html`のPSRは`components.ps`を読む（JSの再計算をやめる） | 表示・RICE/PER・Fフラグ（4銘柄が反転） | 試算でIV・判定・stage変化0。PSRの二重実装が消える |
| 2 | HypeCoreの`psr`・`trailing_pe`を部品から。**`forward_pe`・`peg`はyfのまま**（stageの入力、部品に過去が無い） | `_poc.json`の月次の表示のみ | stageを動かさない |
| 3 | STONKSの`psr`・`ev_sales`・時価総額を部品から（`valuation_fetcher.py`のmarket_cap・EVを部品の`market_cap`・`enterprise_value`に） | STONKSの画面の表示のみ | 24銘柄で最大−28%（ASTS）。点数は読まない |
| 4 | ROTCEのP/TBV履歴のTCEを時点の値に（`rotce.py`の履歴で`tce_inputs(as_of=その期の最初の申告日)`） | ROTCE `_summary.json`のパーセンタイル（31銘柄の履歴が変わる、シグナル変化0、ALAB・CELHが5pt超） | 部品と同じ時点の規則。ROTCEはmethod_changeの印が別 |
| 5 | 名前の付け直し（キーの改名、§3-2） | 画面のJS・一覧の作り直し（CHECK-INV） | 値の差し替え（1〜4）が安定してから。同時にやらない |

最大EPSのPERは**計算を部品に寄せない**（分母が「TTM純利益＋SBC÷TTMの加重平均株数」で別の概念）。名前だけ付け直す。

### 3-2. 「PER」の名前の付け直し案（`NAMING_CONVENTIONS.md`の規則1〈出所の接尾辞〉・規則2〈期間〉に合わせる）

| 今のキー（`components.`） | 中身 | 案 |
|---|---|---|
| `per` | yfのtrailing（無ければforward〈`per_is_forward`〉） | **`per_gaap_ttm_sec`**（部品。時価総額÷TTM純利益、赤字・欠けはNone）。forwardは別に**`per_forward_yf`**（コンセンサス。出所がyfだと明示） |
| `per_adjusted` | 今の株価÷調整後EPS（EPS ANALYZERのTTM） | **`per_adjusted_ttm`** |
| `max_eps_per` | 今の株価÷((TTM純利益＋SBC)÷TTM加重平均株数) | **`per_max_eps_ttm`** |
| `ps`（yfの`price_to_sales`） | yfの時価総額÷yfの売上 | **`ps_ttm_sec`**（部品。時価総額÷TTM売上） |
| `ev_ebitda` | yf | 変更なし（部品に無い）。**`ev_ebitda_yf`**にするかは次の段で |
| 画面の列名「GAAP PER」「最大EPSのPER」 | | 「PER（GAAP・TTM）」「PER（最大EPS）」「PER（調整後）」「PER（先行・予想）」 |

改名は画面のJS（`tanuki_score/index.html`の`COL_KEYS`・`stock.html`・EPS ANALYZER）・`dcf_validity_checker.py`・`output_inventory`の作り直しを伴う。値の差し替えと別の段（段5）にする。

### 3-3. 時価総額は1通りに

案: **株価×部品の株数（SECの加重平均の希薄化後・分割換算）**を表示・倍率の標準にする。DILUTIONの`share_count_change`と同じ`shares_by_end`の株数なので、株数の基準が1つになる。yfの`market_cap`は**残すが、使い道を限る**: ①部品の検証（`valuation_gap`として、部品の時価総額との差が10%を超える銘柄に印〈ASTS・IONQ・RCAT等の株数の違いが見える〉）②部品が欠ける銘柄（V）の代替表示（出所を明記して`_yf`）。

**TANUKIの`diluted_shares`（理論株価の分母）は本項目で変えない**。変えると23銘柄で3%超（ASTS +30%等）理論株価が動く（概算、IV∝1÷株数。試算していない）ので、DILUTIONの株数の基準を決めた後に、別の件として全銘柄の試算にかける。

### 3-4. P/TBVのTCEを時点の値に（試算）

`ptbv.py`: ROTCEの履歴の各四半期について、その期の最初の申告日（`first_filed`）までに申告されたTCEで作り直した。

- 履歴のP/TBVが変わる銘柄 31（変化の最大: IONQ +714%・CELH +96%・SPIR +75%・ZETA +64%・SNPS +56%・KO +23%・AAPL +18%）
- 割安・割高のシグナルの変化 **0**
- パーセンタイルが5pt以上動く: ALAB 64→73、CELH 29→35
- 方針の案: **採る**。部品のP/TBVは既にこの規則で、読み手を揃えると、同じ銘柄・同じ月で値が2つにならない

## 4. 全銘柄の試算（`tools/sim`、TANUKI→HypeCore→TANUKI、96銘柄、`hook_adopt.py`）

フック: TANUKI `data_fetcher`・HypeCoreの`_md_get_attributes`をラップして、`trailing_pe`＝部品の今日の`per`（赤字・欠けはNone）、`price_to_sales`＝部品の今日の時価総額÷TTM売上に差し替えた。前（フックなし）・後を同じ条件で回した。`git status`は変化なし（公開のデータに触れない）。

| 見るもの | 結果 |
|---|---|
| 理論株価 | 変化0 |
| 判定（`tanuki_score`） | 変化0 |
| funda点・timing点 | 変化0 |
| 保有9銘柄（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA） | 理論株価・判定・funda・timingすべて変化0。表示の`per`・`ps`は変わる |
| HypeCoreのstage（96銘柄、月次の履歴ごと） | 変化0 |
| `sim.py scope` | **判定・funda・乖離率・理論株価・stageが変わる銘柄 0**（method_changeの印は付かない） |
| 変わる値 | `components.ps` 96銘柄（24銘柄が5%超）・`components.per` 78銘柄（15銘柄が5%超）・`rice_per_ratio` 54銘柄×3シナリオ・`per_is_forward` 3銘柄（ONDS 真→偽、V・ZETA 偽→真）、HypeCoreの月次の`psr`・`trailing_pe`（174キー） |
| DCF妥当性のFフラグ | 4銘柄が反転: ADSK・CELH・ZETA（真→偽）、PAYS（偽→真） |
| STONKS | `psr`・`ev_sales`・時価総額は別の読み手（部品に差し替える段3）で表示のみ。点数に使われない（`analyzer.py`に`psr`の参照なし）ので試算の対象外 |
| TANUKI SCORE（画面） | 理論株価・判定・funda・timingが変わらないので点数は変わらない。列「GAAP PER」の値が78銘柄で変わる |

**5%を超えて動く銘柄の一覧**は§2-1の表（PER 12銘柄＋ONDS・V・ZETA）。PSRの24銘柄は§2-2（原因は分子の株数と株価の時点で、分母は同じTTM売上）。決定(a)と同じ「原本で確かめた」一覧は、yf側の値に原本が無いため、**SEC側（純利益のTTM・四半期の値）だけ**を原本で確認した。yf側の純利益の内訳が要る銘柄（CELH・CRM・HON・IONQ・IOT・LYFT）は「未確認」と書いた。

## 5. 未公開の4つとの重なりと公開の順の案

| 未公開 | 重なる箇所 | 影響 |
|---|---|---|
| PARSER-LAYER3 段1（revert済み） | `layer3_builder.py`・`bs_lines.py`・`cf_lines.py`・`val_guard.py`＝借入金・純資産・負債合計の定義 | 部品の**EV・P/TBV**（長期借入金・純資産・NCI）が変わる。`per`・`ps`は変わらない。コードは重ならない |
| TANUKI-CALC-CONTINUITY-1 | `q4_implied.py`（負のQ4の扱い）、`stock.html`の2220行付近、`pipeline.py` | 部品のTTM純利益・売上が、負のQ4が出る銘柄で変わる（TASK・FICO・ASTS等、−1,000ドルの丸めだけ）。`stock.html`はMULTIPLESが触る996〜1011・1230行と**ハンクが別**で衝突しない |
| DILUTION-DEFINITION-UNIFY-1 | `common/dilution.py`（新設、`shares_by_end`と同じ株数）、`pipeline.py`、`stock.html` 1608行、`tanuki_score/index.html` 512行 | 株数の基準が**同じ関数**（衝突なし）。MULTIPLESの時価総額の案はこれと揃う |
| DEFICIT-QUALITY-ADOPT-1 | `profitability.py`（関数を追加。`multiples.py`が使う`quarterly_points`・`ttm_series`・`annual_points`は**変更なし**）、`stonks_silo/analyzer.py`・`index.html`、`tanuki_score/index.html` 337・470・528行、`pipeline.py` | MULTIPLESはSTONKSの`pipeline.py`・`valuation_fetcher.py`、`tanuki_score/index.html`の`COL_KEYS`・列名を触る。`index.html`は行が別 |

**公開の順の案**: PARSER段1 → CONTINUITY → DILUTION → DEFICIT → **MULTIPLES（最後）**。理由: ①MULTIPLESは試算でIV・判定・stageが動かない表示の変更で、先の4つが理論株価・funda・EV・TCEを動かした後の状態で部品の値を取り直すのが安全 ②`pipeline.py`・`stock.html`・`tanuki_score/index.html`に他の3つが触れていて、先に公開するほうがrebaseの衝突が少ない ③公開の前に、前の公開の上にrebaseして**この試算を回し直す**（特に、PARSER・CONTINUITYの後の部品の値で「IV・判定・stageの変化0」が保たれること、DEFICITの後のFフラグ4銘柄の反転）。PSR・PERの表示のみの変更なので`method_change`の印は不要の見込み（`sim.py scope`が0銘柄）。ただしROTCE（段4）はパーセンタイルの変化があり印の要否を別に決める。

## 6. 決めてほしいこと

1. **PER**: 部品がNoneのとき`forward_pe`に自動で落とす今の仕様をやめ、空にして`per_forward_yf`を別の列にしてよいか（ONDS・V・ZETAの列の意味が変わる）
2. **時価総額**: 株価×部品の株数に1通りにし、yfは検証（10%超に印）と部品が欠ける銘柄の代替だけに残す案でよいか。`diluted_shares`（理論株価の分母）は本項目では変えない案でよいか
3. **部品に`current`の行**を足し、読み手は生の株価で割る方式でよいか（週1回の再計算に頼らない）
4. **P/TBVのTCEを時点の値**にしてよいか（31銘柄の履歴が変わる、シグナルは変わらない）
5. **Vの株数の欠け**（Layer3に`shares_diluted`なし）を先に直すか、Vを例外にするか
