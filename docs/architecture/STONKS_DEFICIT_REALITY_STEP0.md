# STONKS-DEFICIT-REALITY-EPIC-1 ①② の STEP 0（2026-11-25、読み取りと手元の試算だけ。コード・データは変えていない）

依頼: ①資金の持ち（runway）の流出を四半期のTTMにする（STONKS と TANUKI をそろえる）、②黒字化の予測から現金を伴わない評価損益を除く。試算、判定への効き方、method_change の要否、公開の順と scope。
**試算の方法**（scratchpad の `runway.py`・`noncash.py`・`stonks_trial.py`・`scan_tags.py`。公開のデータ・コードは変えていない）: ①は Layer3（`build_ticker_store`・`calc_ttm_series`）の直近4四半期の営業CF・設備投資と `SECReader.get_net_cash` の現金で、TANUKI の82銘柄＋STONKS の RKLB・ZS（84銘柄）を計算し、STONKS は `stonks_silo/pipeline.py` を出力先だけ scratchpad に替えて全17銘柄を回して公開の `results.json` と照合した（基準は17/17一致）。②は `common/profitability.py` の `quarterly_points`・`ttm_series`・`breakeven` をそのまま使い、基準は公開の `profitability.json` と16/16一致（黒字の銘柄を除く）。

## 結論（先に）

1. **①の規則は「Layer3 の直近4四半期の営業CF − 設備投資（`common/sec_data/fcf.py::free_cash_flow`）」で、STONKS と TANUKI の両方に同じ関数で渡す**。今は STONKS が最新の年次（`records[latest_year]["cf"]`）、TANUKI の `computed_runway_months` が最新の年次の FCF（`fcf_history[-1]`）を使っていて、**2つとも年次**（現金だけ四半期優先で、期末がずれる）。
2. **SOUN は 24.2か月（SAFE）→ 19.3か月（WATCH）で、原本の約18〜19か月と一致する**（2026Q3の現金 207.6M・TTM の営業CF −117.9M・設備投資 11.4M → FCF −129.3M → 月 10.8M）。
3. STONKS 17銘柄のうち runway が変わるのは7銘柄（ASTS・AVAV・CRWV・IONQ・JOBY・RKLB・SOUN）。**runway の判定（SAFE/WATCH/DANGER）が変わるのは IONQ（SAFE→WATCH、40.0→17.5か月）と SOUN（SAFE→WATCH）**。STONKS の総合の判定が変わるのは IONQ だけ（10x_CANDIDATE 83 → PROMISING 71）。SOUN は 10x_CANDIDATE のまま（90→78）。
4. **TANUKI の理論株価・stage は変わらない**。funda への効きは、runway が12か月未満のときの −30 の減点だけ（STONKS の値を優先し、無い銘柄は `computed_runway_months`）。**新しく12か月を下回るのは SOFI だけ**（23.4 → 8.9か月、funda 100 → 70、判定 WATCH のまま、理論株価は同じ）。SOFI は金融（貸出の組成が営業CFに入る）で、資金の持ちが意味を持たない。
5. **現金側と流出側の期末は、84銘柄のうち82が一致**（TTM の期末＝Layer3 の現金の最新の期末）。ずれるのは CPRT（現金の系列が2023-04-30で止まっている。FCF がプラスで runway は出ない）と SITM（`cash_and_equivalents` の系列が空。四半期ファイルの現金は同じ2026Q3）。実装では「期末が違えば TTM を使わず年次にフォールバックして印」にする。
6. **②は営業利益から、XBRL のタグの許可リスト（条件付き対価の公正価値の変動・のれん／無形資産／長期性資産の減損）の値を足し戻す方式を勧める**。84銘柄のうち62銘柄に許可リストのタグの値があるが、直近TTMの営業利益の10%以上動くのは6銘柄（ABBV・ELF・MRVL・VST〈黒字〉・AVAV・SOUN）。**黒字化の予測が変わるのは SOUN（PREDICTED 0.72四半期・R² 0.696 → TOO_FAR 15.0四半期・R² 0.964）・RKLB（10.4 → 12.2四半期）・S（14.6 → 14.7）で、分類が変わるのは SOUN だけ**。STONKS の総合の判定は変わらない。
7. ②は TANUKI の理論株価・判定・HypeCore の stage に効かない（`profitability.json` の読み手は STONKS の黒字化パス・TANUKI の report.txt の `Breakeven_Estimate` 行と構造的赤字の種類の表示・`stage_v2` の並行計算・STONKS の対象の選び方〈営業利益の符号は変わらない〉だけ）。
8. 順は **①→②**、どちらも method_change を登録する（①は STONKS の runway・総合と TANUKI の funda〈SOFI〉が動く。②は STONKS の黒字化パスと `profitability.json` の分類〈SOUN〉が動く）。

## 1. ① 資金の持ちの流出

### 1-1. 今の基準（コードで確認）

| | 現金 | 流出 | 判定 |
|---|---|---|---|
| STONKS `_analyze_runway()`（`stonks_silo/analyzer.py`） | `get_runway_cash()`（四半期優先: 現金＋短期投資。非流動AFSは含めない） | 最新の**年次**（`records[latest_year]["cf"]`）の営業CF・設備投資 → `free_cash_flow(ocf, capex)/12` | runway ≥24か月 SAFE／12〜24 WATCH／12未満 DANGER。FCF≥0 は SAFE |
| TANUKI `computed_runway_months`（`pipeline.py` の `_load_extra_data`） | 同じ `get_runway_cash()` | 最新の**年次**FCF（`fcf_history[-1]["fcf"]`）の絶対値 ÷ 12。条件: FCF<0 かつ（直近四半期EPS<0 または FCF<0 または現金<100M） | 表示と funda の減点（<12か月で −30）。**STONKS の値を優先し、STONKS に無い銘柄だけ**この値 |

つまり**現金は四半期（2026Q3）、流出は年次（2025）で、期末が約9か月ずれる**。流出が加速している銘柄ほど資金の持ちが長く出る。

### 1-2. 案（規則）

**Layer3 の直近4四半期（営業CF − 設備投資）を TTM の FCF とし、`月の流出 ＝ TTM の FCF ÷ 12`、`資金の持ち ＝ 現金 ÷ |月の流出|`（FCF≥0 は無限〈SAFE〉）**。式は `common/sec_data/fcf.py::free_cash_flow`（設備投資の欠けは今の扱いのまま。1つの規則にするのは FCF 定義の段5）。計算は **`common/sec_data/reader.py` に 1か所（例: `get_runway_burn(ticker, store)`）** を置き、STONKS の `_analyze_runway()` と TANUKI の `computed_runway_months` の両方が呼ぶ。

- 4四半期がそろわない・営業CFが欠ける銘柄（RCAT: `latest_period_missing`）は、今の年次にフォールバックして `basis="annual_fallback"` の印を残す（出力に出す）。
- 現金の期末（Layer3 の現金の最新の期末）と TTM の期末を比べ、違えばフォールバック＋印。
- 判定の閾値（24・12か月）と、runway<12か月の funda の減点（−30）は変えない。

### 1-3. 試算（STONKS 17銘柄）

〔現金は2026Q3など最新の四半期。旧＝今の公開の値、新＝TTM。runway はか月〕

| 銘柄 | 現金 | 年次FCF | TTM FCF | runway 旧 → 新 | runway の判定 | 総合の判定 |
|---|---|---|---|---|---|---|
| **SOUN（保有）** | 207.6M | −103.1M | −129.3M | 24.2 → **19.3** | SAFE → **WATCH** | 10x_CANDIDATE のまま（90 → 78） |
| **IONQ** | 997.9M | −299.6M | −685.6M | 40.0 → **17.5** | SAFE → **WATCH** | 10x_CANDIDATE 83 → **PROMISING 71** |
| CRWV（保有） | 4,184M | −7,251M | −20,421M | 6.9 → 2.5 | DANGER のまま | WATCH のまま |
| ASTS | 5,299M | −1,136M | −1,799M | 56.0 → 35.3 | SAFE のまま | PROMISING のまま |
| JOBY | 2,198M | −564M | −785M | 46.8 → 33.6 | SAFE のまま | WATCH のまま |
| AVAV | 580M | −165M | −45M | 42.3 → 155.5 | SAFE のまま | PROMISING のまま |
| RKLB | 4,608M | −322M | −326M | 171.8 → 169.8 | SAFE のまま | 変化なし |
| ESTC・FROG・GTLB・LYFT・NET・RBRK・S・SITM・ZS | — | プラス | プラス | 出ない（SAFE） | 変化なし | 変化なし |
| RCAT | 242.7M | −6.6M | 取れない（4四半期がそろわない） | 出ない（UNKNOWN） | 年次のまま＋印 | 変化なし |

**SOUN と原本**: 10-Q（0001840856-26-000028、2026-11-09提出）の2026Q3の現金 207.6M、TTM の営業CF −117.9M（Layer3 の TTM と一致。−109.5M〈前年同期のTTM〉→ −117.9M）、設備投資 11.4M。TTM の FCF −129.3M で **19.3か月**（原本の約18〜19か月）。直近1四半期の営業CF −36.0M を年率にすると 17.3か月で、直近の悪化の方向は TTM でも出る（年次 24.2 → TTM 19.3）。

### 1-4. 試算（TANUKI の `computed_runway_months`）

STONKS に無い銘柄が対象。新しい規則では「TTM の FCF<0」の銘柄に出る（今は年次の FCF<0 の銘柄）。出る値が変わる・新しく出るのは3銘柄:

| 銘柄 | 旧（年次） | 新（TTM） | funda・判定 |
|---|---|---|---|
| **SOFI（保有）** | 23.4 | **8.9** | **12か月未満 → funda −30（100 → 70）、WATCH のまま、理論株価は同じ（30.03）**。TTM の FCF −10.5B（貸出の組成が営業CFに入る金融の型）。DANGER 相当の表示が出る |
| COHR | 28.3 | 15.8 | 12か月以上のまま。変化なし |
| AMZN | 出ない（年次の FCF +7.7B） | 29.2（TTM の FCF −46.9B。設備投資が重い） | 12か月以上。変化なし |

（funda の確認は `pipeline.py` を3銘柄だけ、`computed_runway_months` を新しい値に差し替えて回した。SOFI は funda 100 → 70・WATCH のまま・理論株価 30.03 のまま、COHR・AMZN は変化なし。）

### 1-5. 現金側と流出側の期末

84銘柄のうち **82銘柄で TTM の期末 ＝ Layer3 の現金の最新の期末**（SOUN: 2026-09-30・AVAV: 2026-08-01・ESTC: 2026-10-31 など会計年度のずれも一致）。ずれ・欠け: CPRT（現金の系列が 2023-04-30 で止まっている。FCF がプラスで runway 対象外）、SITM（`cash_and_equivalents` の系列が空。`get_net_cash` の現金は四半期ファイル 2026Q3 で TTM と同じ）。→ 実装では、現金の期末と TTM の期末の差を確かめ、現金のほうが古い／新しいときは TTM を使わず年次にフォールバックして `basis` に残す。

### 1-6. 理論株価・判定・stage への効き

- **理論株価・stage: 変わらない**（runway は DCF・HypeCore の stage に入らない）。
- **判定（funda 経由）**: runway<12か月の −30 だけ。STONKS の 17銘柄では新しく12か月を下回る銘柄は無い（CRWV は元から）。TANUKI の `computed_runway_months` では **SOFI だけ新しく −30**（WATCH のまま）。
- STONKS の総合の判定: IONQ（10x_CANDIDATE → PROMISING）。
- method_change: **要**（STONKS の runway・総合の判定・TANUKI の funda〈SOFI〉・`computed_runway_months`〈SOFI・COHR・AMZN〉が動く）。ID `STONKS-DEFICIT-REALITY-EPIC-1-1`、scope は `method_change_scope.py` の前後の差（STONKS: ASTS・AVAV・CRWV・IONQ・JOBY・RKLB・SOUN。TANUKI: SOFI・COHR・AMZN。約10銘柄）。

## 2. ② 黒字化の予測から現金を伴わない評価損益を除く

### 2-1. 今の仕組み

`common/profitability.py::compute`: Layer3 の `operating_income` の四半期（`first_val`、その時点で最初に申告された値）の TTM と売上の TTM から営業利益率を作り、**直近8点の回帰の傾きと R²≥0.5 で黒字化の時期を出す**（`breakeven`）。TTM の営業利益<0 のときだけ。営業利益に入る現金を伴わない評価損益（条件付き対価の公正価値の変動・減損）は、そのまま回帰に入る。

### 2-2. 機械的な見分け方（案）

**XBRL のタグの許可リスト**を1つの設定（例 `config/noncash_operating_items.json`）に置く。銘柄名は書かない。各タグに符号（正＝営業利益を押し下げる費用・損失）と理由を付ける。試算に使った許可リスト:

| 種類 | タグ（us-gaap） |
|---|---|
| 条件付き対価（買収のアーンアウト）の公正価値の変動 | `BusinessCombinationContingentConsiderationArrangementsChangeInAmountOfContingentConsiderationLiability1`（正＝負債の増加＝費用、負＝評価益） |
| のれんの減損 | `GoodwillImpairmentLoss`・`GoodwillAndIntangibleAssetImpairment` |
| 無形資産の減損 | `ImpairmentOfIntangibleAssetsExcludingGoodwill`・`ImpairmentOfIntangibleAssetsFinitelived`・`ImpairmentOfIndefinitelivedIntangibleAssetsExcludingGoodwill` |
| 長期性資産の減損 | `ImpairmentOfLongLivedAssetsHeldForUse`・`ImpairmentOfLongLivedAssetsToBeDisposedOf`・`AssetImpairmentCharges`・`OtherAssetImpairmentCharges` |

- 値の取り方: 同じタグが損益計算書にもCFの非現金の調整にも出る。10-Q の**3か月の値**があればそれ、無ければ**累計の差**（同じ会計年度の連続する累計）、Q4 は**年次 − 9か月累計**で1四半期の値を作る（`company_facts` の事実から。申告日の新しい値を使う）。
- 営業利益から除く: `営業利益（調整後）＝営業利益 ＋ Σ（許可リストのタグの四半期の値）`（正は足し戻し＝減損、負は引き＝評価益）。回帰に使う TTM の営業利益率はこの調整後。**営業利益の符号が変わる銘柄は無い**（試算）。
- **許可リスト外の候補を拾う印**: タグ名が評価損益・減損・条件付き対価らしく（`Impairment`・`ContingentConsideration`・`FairValueAdjust`・`ChangeInFairValue` など）、直近4四半期の合計が TTM の営業利益（または売上の2%）の10%以上のもの。84銘柄の試算では4件のみ: ABBV（`PaymentForContingentConsiderationLiabilityOperatingActivities` 3,398M＝現金の支払いで評価損益ではない）・IONQ（`FairValueAdjustmentOfWarrants` −1,254M。通常は営業外で営業利益に入らない〔営業利益に入るかは、XBRL の表示の関係〈presentation/calculation linkbase〉を読まないと分からない〕）・SITM（2件、小さい）。→ 候補は許可リストに入れず、印だけ（自動では除かない）。
- 営業利益に入るか（損益計算書の営業利益より上の行か）は、タグだけでは決まらない（IONQ のワラントのように営業外のこともある）。許可リストは「営業費用の行に載る」ことが多いタグだけにして、**営業外に載ることが分かったタグは許可リストに入れない**（理由を設定に書く）。

### 2-3. 試算（84銘柄、`profitability.json` の全銘柄）

- 許可リストのタグの値がある銘柄 62。直近TTMの営業利益の**10%以上動くのは6銘柄**: ABBV（21,233 → 27,373M。黒字）・ELF（172 → 206M）・MRVL（1,561 → 1,995M）・VST（4,232 → 4,656M）の黒字側4つと、**AVAV（TTM の営業利益 −252.6M → −11.9M。のれん・無形資産・長期性資産の減損の足し戻し）・SOUN（−69.3M → −221.0M。条件付き対価の評価益 151.7M を除く）**。
- 黒字化の予測の値・分類が変わるのは **SOUN・RKLB・S** の3銘柄、R² だけ変わるのが RCAT（0.175 → 0.001、LOW_FIT のまま）・AVAV（0.859 → 0.441、NO_TREND のまま）。**黒字（TTM の営業利益>0）の銘柄は予測の対象外で変わらない**。

| 銘柄 | 公開（今） | ②（調整後） |
|---|---|---|
| **SOUN（保有）** | PREDICTED 0.72四半期（2026年）、R² 0.696、「黒字化目前」 | **TOO_FAR 15.0四半期（約2030年）、R² 0.964、「黒字化は遠い」** |
| RKLB | TOO_FAR 10.42四半期（R² 0.905） | TOO_FAR 12.21四半期（R² 0.889） |
| S | TOO_FAR 14.64（R² 0.982） | TOO_FAR 14.70（R² 0.982） |
| RCAT | LOW_FIT（R² 0.175） | LOW_FIT（R² 0.001） |
| AVAV | NO_TREND（R² 0.859） | NO_TREND（R² 0.441） |
| JOBY | PREDICTED 0.01四半期 | 変化なし（調整は −8.0M） |
| ほか（STONKS の ESTC・FROG・GTLB・IONQ・NET・RBRK・LYFT・ZS・ASTS・CRWV・SITM） | — | 変化なし |

**SOUN の R²**: 公開の R² 0.696 は、営業利益が条件付き対価の評価益（2025年Q1 +128.1M・2025年Q4・2026年の9か月 +67.1M）で振れた系列に当てはめた値。評価益を除くと、TTM の営業利益率は直線に近く（R² 0.964）、傾きは 0.066/四半期と小さく、0に届くのは15四半期後（会社の目標の調整後 EBITDA 黒字〈2027年後半〉と整合）。

**構造的赤字（FCF）の銘柄の型（12銘柄）**: 評価損益の足し戻しで変わるのは SOUN だけで、CRWV ほかは変わらない。
- 営業利益の傾きが下向き（`NO_TREND`、評価損益のタグ無し）: **CRWV**（TTM の営業利益率 −0.76%、直近8点が下向き。構造的赤字で営業利益も赤字）・AVAV（調整後は営業利益が −11.9M でほぼ0だが、傾きは下向きのまま）。
- 黒字化が遠い（`TOO_FAR`）: IONQ（8.6四半期）・RKLB（10.4 → 12.2）・**SOUN（②で 0.72 → 15.0）**。
- 4四半期がそろわない・当てはまりが低い: ASTS（NO_DATA）・RCAT（LOW_FIT）。
- 営業利益は黒字で FCF だけ赤字（設備投資が重い型）: AMZN・CEG・COHR（①の TTM の資金の持ちでは、AMZN 29.2・COHR 15.8か月）。
- 営業利益の黒字化の予測がそもそも区分なし: SOFI（金融）。
- 黒字化目前（`PREDICTED`）: JOBY（0.01四半期）・**SOUN（②の前）**。

### 2-4. 除けない銘柄（タグが無い）の扱い（案）

出力に **`noncash_adjustment`** を持たせる（表示だけ）:
- `applied`: 許可リストのタグの値があり、直近TTMの営業利益の2%以上を除いた（SOUN・AVAV・RKLB など。除いた額と種類を併記）
- `none`: 許可リストのタグが無い、または2%未満（評価損益の有無を確認できない銘柄を含む。**「無い」と「確認できない」は区別できない**ので、印の文言は「調整なし」とし、営業CFとの食い違い〈`|TTM営業利益 − TTM営業CF|/売上`〉の大きい銘柄は参考の印を付ける案も）
- `candidate`: 許可リスト外の候補（上の4件。自動では除かない）

### 2-5. 理論株価・判定・stage への効き

- **TANUKI の理論株価・判定・funda・HypeCore の stage: 変わらない**。`profitability.json` の読み手は、STONKS の黒字化パス（`_analyze_profitability_path`）・TANUKI の report.txt の `Breakeven_Estimate` 行と `structural_deficit_kind` の表示・`stage_v2` の並行計算（SOUN の収益の部品が near_profit → loss）・STONKS の対象の選び方（営業利益の符号。②で符号が変わる銘柄は無い）だけ（コードの読み手で確認）。
- **STONKS の総合の判定: 変わらない**（17銘柄を ②だけ／①＋②で回した。SOUN は 10x_CANDIDATE 90 のまま、黒字化パスの `op_breakeven_year` が 2026 → 2030）。
- method_change: **要**（`profitability.json` の分類と STONKS の黒字化パスが動く）。ID `STONKS-DEFICIT-REALITY-EPIC-1-2`、scope は STONKS の `results.json` の前後の差（`profitability_path.*`: SOUN・RKLB・S・RCAT・AVAV）。`profitability.json` 自体は `method_change_scope.py` のスナップショットの対象外のため、別に差の表を残す。

## 3. 公開の順と scope（①と②を別々に）

| | ① 資金の持ちの流出 | ② 黒字化の予測から評価損益を除く |
|---|---|---|
| 変えるもの | `common/sec_data/reader.py`（TTMの流出の1か所）・`stonks_silo/analyzer.py::_analyze_runway`・`pipeline.py` の `computed_runway_months`・テスト・出力に `basis`（ttm／annual_fallback）と期末 | `config/noncash_operating_items.json`（許可リスト）・`common/profitability.py`（調整後の営業利益・`noncash_adjustment`）・`profitability.json` の再生成・テスト |
| 影響 | STONKS 7銘柄の runway・IONQ の総合・SOFI の funda | `profitability.json`（SOUN・RKLB・S・RCAT・AVAV）・STONKS の黒字化パス・`stage_v2` の並行計算（SOUN） |
| method_change | `STONKS-DEFICIT-REALITY-EPIC-1-1`、約10銘柄 | `STONKS-DEFICIT-REALITY-EPIC-1-2`、約5銘柄 |
| 公開 | 計算方法の変更の手順（試算→scope→手動の連鎖〈TANUKI→HypeCore→TANUKI。STONKS も回す〉→次の定時の確認） | 同じ。`profitability.json` の作り直しは Market Data Daily の後の段と同じ呼び出しで、同じ公開にまとめる |
| 順 | **先**（不具合の直し。小〜中） | ①の確認の後（中。許可リストの設定が要る） |

## 4. 決めてほしいこと

- **(a)** ①の規則（Layer3 の直近4四半期の営業CF − 設備投資）で、STONKS と TANUKI を同じ関数にそろえてよいか。4四半期がそろわない銘柄（RCAT）は年次にフォールバック＋印でよいか。
- **(b)** 金融（SOFI）の扱い: 資金の持ちの表示・funda の減点から外す（`is_financial_ticker` の業種）か、TTM のまま出すか。外さないと **SOFI（保有）の funda が 100 → 70**（WATCH のまま）。
- **(c)** 判定の閾値（24・12か月）と、12か月未満の funda 減点（−30）は今のままでよいか（IONQ・SOUN が SAFE → WATCH になるだけで、減点は変わらない）。
- **(d)** ②は XBRL の許可リスト方式（銘柄名を書かない設定）でよいか。許可リストに減損（のれん・無形・長期性資産）を含めるか、条件付き対価の公正価値の変動だけにするか（減損を含めると AVAV の営業利益が −252.6 → −11.9M に動く）。Layer3 の項目にする案（`PARSER-LAYER3-SINGLE-SOURCE-1` の段階）は、②のあとでよいか。
- **(e)** 印（`noncash_adjustment`＝applied／none／candidate）を出す場所（STONKS の黒字化パスの行・`profitability.json`・TANUKI の report.txt）と文言。
- **(f)** 公開の日（①を日曜の連鎖〈11-29〉の後、②をその次、など）。

## 5. 限界

- ①の TANUKI への funda の効きは、`computed_runway_months` を差し替えて3銘柄（SOFI・COHR・AMZN）だけ確かめた。全銘柄の `chain`（TANUKI→HypeCore→TANUKI）の試算は、実装の後に scope を作るときに回す。
- ②は `company_facts` の事実から四半期の値を作る簡易な方法（申告日の新しい値、Q4＝年次−9か月）。Layer3 の `first_val`（その時点で最初に申告された値）の semantics とは一致しないので、実装では Layer3 の項目にするか、同じ規則に合わせる。
- 営業利益に入るかどうかは XBRL の表示の関係を読んでいない（許可リストの根拠は「営業費用に載ることが多いタグ」）。許可リスト外の候補（IONQ のワラントなど）は営業外のことが多い。
- ②の「許可リストに入れた減損」は、会社が営業利益に含めて開示しているもの（営業費用の行）を前提にした。
