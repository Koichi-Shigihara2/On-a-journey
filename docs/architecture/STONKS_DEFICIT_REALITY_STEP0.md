# STONKS-DEFICIT-REALITY-EPIC-1 ①② の STEP 0（2026-11-25、読み取りと手元の試算だけ。コード・データは変えていない）。③④ は §6（2026-11-27）

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

## 5. 実装と試算（2026-11-26。ブランチ2本、未公開）と公開の手順

決定（(a)〜(f)）: ①は TTM の FCF を `reader.py` の1か所に置き STONKS と TANUKI の両方が呼ぶ（TTMが取れない・期末がずれる銘柄は年次に戻して印）、(b) 金融は runway と減点の対象から外す、(c) 閾値（24・12か月）と減点（−30）は変えない（FINDINGS に1行）、(d) ②の許可リストは条件付き対価の公正価値の変動とのれん・無形資産・長期性資産の減損（設定ファイル、銘柄名なし）、(e) 印は applied・none・candidate、(f) ①は 11-26（手順46の後）・②は 11-27 に公開。**今日は kaihatsu にコードを入れていない**（このdocsと FINDINGS だけ）。

### 5-1. ブランチ（origin に控えを置いた）

| | ブランチ | コミット | 中身 |
|---|---|---|---|
| ① | `stonks-runway-ttm` | `9911c3feb8`（実装）→ `c419b16d26`（金融の判定の直し）→ scope | `common/sec_data/reader.py::get_runway_burn`（1か所）・`stonks_silo/analyzer.py::_analyze_runway`（TTM・金融は対象外・`burn_basis`）・`pipeline.py`（`computed_runway_months`・`computed_runway_basis`・report.txt の `Runway_Basis`）・STONKS の画面（流出の基準のツールチップ）・テスト `tests/test_runway_ttm.py`（8件） |
| ② | `profitability-noncash` | `1f8d1539ff`（実装）→ `ef11a55627`（scope） | `config/noncash_operating_items.json`（許可リスト）・`config/sec_concept_definitions.json`（Layer3 の4項目: `contingent_consideration_change`・`goodwill_impairment`・`intangible_impairment`・`longlived_impairment`）・`q4_implied.py`（Q4 を年次−9か月で作る）・`ttm_calculator.py`（TTM出力の対象外）・`common/profitability.py`（足し戻し・`noncash_adjustment`）・STONKS の黒字化パス・report.txt の `Breakeven_Noncash`・テスト `tests/test_profitability_noncash.py`（5件） |

どちらも `kaihatsu` から切り、pytest（①3073・②3071通過）・rcc NG 0・audit 0。

### 5-2. ① の試算（TANUKI→HypeCore→TANUKI と STONKS の全17銘柄。前＝kaihatsu、後＝ブランチ。同じデータ）

STEP 0 の試算と同じ結果になった。
- **理論株価・判定・funda・stage の変化: 0**（82銘柄、保有9銘柄とも）。SOFI は**対象外**になった（`computed_runway_basis=financial_excluded`、`computed_runway_months` なし。funda は 100 のまま。STEP 0 の試算では 23.4 → 8.9か月で funda 100 → 70 だった）。
- **SOUN: 24.2 → 19.3か月（SAFE → WATCH）**（10-Q の約18〜19か月）。**IONQ: 40.0 → 17.5か月（SAFE → WATCH）、総合 10x_CANDIDATE 83 → PROMISING 71**。SOUN の総合は 10x_CANDIDATE のまま（90 → 78）。
- STONKS の runway が変わるのは7銘柄（ASTS 56.0 → 35.3・AVAV 42.3 → 155.5・CRWV 6.9 → 2.5・IONQ・JOBY 46.8 → 33.6・RKLB 171.8 → 169.8・SOUN）。TANUKI の `computed_runway_months` は ASTS・AVAV・CRWV・IONQ・JOBY・SOUN（STONKS の値と同じ）と COHR（28.3 → 15.8）・AMZN（なし → 29.2）・SOFI（23.4 → 対象外）。
- 基準の内訳: TTM 79銘柄・**年次に戻したのは2銘柄**（RCAT: `ocf_ttm_incomplete`〈4四半期がそろわない〉、CPRT: `cash_end_mismatch:2023-04-30!=2026-07-31`〈現金の系列が古い。FCFがプラスで runway は出ない〉）・金融の対象外1（SOFI）。SITM は現金の期末を確かめられないが TTM（`cash_end_unverified`、FCF がプラスで runway は出ない）。
- **scope（`method_change_scope.py`、ID `STONKS-DEFICIT-REALITY-EPIC-1-1`）**: 17銘柄（STONKS の `runway.*` と `summary`・`overall_score`・`overall_verdict`〈IONQ〉と TANUKI の `computed_runway_months`）。理論株価・funda・判定・stage が変わる銘柄は **0**（SOFI の funda は金融の対象外で変わらない）。公開の直前に作り直す。

### 5-3. ② の試算（同じ方法。`profitability.json` は試算の間だけ作り直した値を置いた）

- **理論株価・判定・funda・stage の変化: 0**（TANUKI→HypeCore→TANUKI、82銘柄）。STONKS の総合の判定も変わらない（SOUN は 10x_CANDIDATE 90 のまま）。
- **SOUN: 黒字化の予測 PREDICTED 0.72四半期（2026年、R² 0.696、収益の段階 near_profit）→ TOO_FAR 14.99四半期（2030年、R² 0.964、loss）**。STONKS の黒字化パスの `op_breakeven_year` 2026 → 2030。STEP 0 の簡易な試算と同じ値。
- `profitability.json` の84銘柄のうち変わるのは5銘柄（分類が変わるのは SOUN だけ）: RKLB 10.42 → 12.21四半期（TOO_FAR のまま）・S 14.64 → 14.70・RCAT R² 0.175 → 0.001（LOW_FIT のまま）・AVAV R² 0.859 → 0.749（NO_TREND のまま）。`noncash_adjustment` は applied 12・candidate 2（IONQ のワラント・SITM）・none 70。STONKS の17銘柄では AVAV・RKLB・SOUN が applied、IONQ・SITM が candidate。
- **scope（ID `STONKS-DEFICIT-REALITY-EPIC-1-2`）**: 1銘柄（SOUN: `stonks.profitability_path.op_breakeven_year`・`op_breakeven_reason`・`verdict_reason`・`summary`）。`profitability.json` 自体は `method_change_scope.py` のスナップショットの対象外のため、上の5銘柄の表をここに残す。

**STEP 0 の簡易な試算との違い**（Layer3 の `first_val` と同じ四半期の値で作り直した結果）:
- 最初に Layer3 の4項目を足しただけでは Q4（年次−9か月）が作られず、SOUN は足し戻しが −67.1M（Q1〜Q3 2026 だけ）・PREDICTED 1.07四半期になった。**`q4_implied.py` の Q4 の対象に4項目を足して**、SOUN は −151.7M・TOO_FAR 14.99四半期・R² 0.964 になり、STEP 0 と同じになった。
- **AVAV**: 足し戻し 151.3M（STEP 0 は 240.7M）、R² 0.749（STEP 0 は 0.441）。STEP 0 は `GoodwillImpairmentLoss` と `GoodwillAndIntangibleAssetImpairment` を両方足していて、重なる分が二重に入っていた。Layer3 は項目ごとに優先順位で1つのタグを採る（二重にならない）。**VST**: 269M（STEP 0 は 424M）、同じ理由。ABBV・ELF・MRVL は同じ。
- 分類（PREDICTED／TOO_FAR／NO_TREND）が変わるのは SOUN だけで、STEP 0 と同じ。

### 5-4. 公開の手順（①は明日の依頼文、②は明後日の依頼文で行う）

共通（`CLAUDE_CODE_PROCEDURES.md`「計算方法の変更の手順」）。公開の日の朝に、前の公開の確認（手順46・①の確認）が済んでいることを確かめる。
1. kaihatsu を最新にし（bot の差分だけなら追いつき）、ブランチを kaihatsu に rebase（競合が出たら止まる）。
2. **試算と scope を作り直す**（公開の直前のデータで）: 前（kaihatsu）と後（ブランチ）を同じデータで `tools/sim`（`chain`）と STONKS で回し、`method_change_scope.py snapshot/diff --id … --write`。ノイズ（市場データの更新による判定の変化）は前後を同時に回して除く。
3. ゲート3種（pytest・rcc `--fail-on-ng`・audit）、`build_output_inventory.py` の作り直しが要るか確認。
4. `git merge --no-ff <ブランチ>` → push（kaihatsu）。
5. **② だけ**: `python common/profitability.py --all` で `docs/common/profitability.json` を作り直して同じ公開に入れる（Market Data Weekly の段と同じ呼び出し）。続けて `python common/stage_v2.py --all`（並行計算の収益の部品が SOUN で near_profit → loss になる）。
6. 手動の連鎖: `gh workflow run TANUKI_VALUATION_Update.yml`（→ 終わったら）`HypeCore_Update.yml`（→ TANUKI が自動で起動）と、**STONKS** `gh workflow run Stonks_Silo_Update.yml`（①②とも results.json が変わる）。
7. 確認: 1回目の🛠（`score_watcher`）／`method_change` の印の銘柄が scope と一致、2回目で印が付かない、理論株価・判定が前の試算の値と同じ。次回の確認手順に足す（①: 次の定時の後、②: 次の Market Data Weekly〔日曜〕の後）。

| | ① 11-26 | ② 11-27 |
|---|---|---|
| ブランチ | `stonks-runway-ttm` | `profitability-noncash` |
| ID | `STONKS-DEFICIT-REALITY-EPIC-1-1` | `STONKS-DEFICIT-REALITY-EPIC-1-2` |
| 公開で作り直すもの | なし（STONKS の `results.json` は手動の STONKS で） | `profitability.json`・`stage_v2` の並行計算・STONKS |
| 次回の確認 | 11-27 朝の定時の後（印0・runway が公開直後の値と同じ・SOFI が対象外のまま・RCAT・CPRT が `annual_fallback`） | 11-30 朝（日曜の Market Data Weekly の後。`profitability.json` が同じ値・SOUN が TOO_FAR のまま） |
| BACKLOG | `[[STONKS-DEFICIT-REALITY-EPIC-1]]` の①を済、②は残す | ②を済、③④は残す |

### 5-4b. 公開の後の追加の決まり（チャット側の補足、2026-11-26）

- 公開の後、**画面（Playwright）でも確かめる**: ①の公開の後に、SOFI が STONKS・TANUKI の runway の対象外になったこと（TANUKI の report.txt の `Runway_Basis`・stock.html・STONKS の画面に SOFI の runway が出ない）と、IONQ の STONKS の総合判定が PROMISING になったこと（STONKS の画面）。
- **worktree `wt_nc`（`profitability-noncash`）は②の公開まで残し、②の後に片付ける**（`git worktree remove` と、公開済みのブランチの扱い）。
- **次回の確認手順は、手順46（UPSIDE の2回目）と同じ11-26朝の回にまとめる**（`CLAUDE_CODE_START.md` の手順46に、①の確認〔印が scope の銘柄と一致・runway の値・SOFI 対象外・RCAT と CPRT が `annual_fallback`〕を足す。②の確認は11-27の公開の後の別の回）。

### 5-5. 残り（この STEP 0 の範囲外）

- FINDINGS に1行: **runway の減点 −30 は段の形**（12か月未満で一律 −30）。DILUTION と同じく傾斜にするかは次の束ねで。
- STONKS の画面の流出の基準は、ツールチップだけ（年次に戻した銘柄に「年」の添え字）。画面の表示は公開の日の Playwright で確かめる。

## 6. ③④ の STEP 0（2026-11-27、読み取りと手元の試算だけ。コード・データは変えていない）

依頼: ③ MD&A の本文の保存（場所は `common/`・対象は STONKS の17銘柄と保有9銘柄・Results of Operations と Liquidity and Capital Resources の節）、④ 開示本文からのリスクの印（参考表示だけ。判定・理論株価に入れない。4種: 事業継続の疑義・内部統制の重大な不備・人員削減と再編〈8-K〉・ATM の残りと四半期の売却額。表示は STONKS と TAIL）。
**限界（先に）**: この環境に `SEC_USER_AGENT_EMAIL` が無く、**SEC の本文は1本も取っていない**（値は会話・文書に書かない決まり）。大きさ・取得の回数は設計値と既存コードから出した。印の試しは、手元の材料（`company_facts.json` の XBRL・保存済みの TAIL の `ctrl/`・BACKLOG の経緯に書かれた原本の確認）で出した。本文の実測が要る所は §6-6 に「実測で確かめること」として分けた。
**対象の決め方**: `results.json` の `tickers`（17）∪ `positions_index.json`（9）。重なる CRWV・SOUN を除いて **24銘柄**。銘柄名は手で書かず、この2つの一覧から導く。

### 6-1. 今の仕組み（コードで確認）

| 見るもの | 今 |
|---|---|
| 取得 | `src/tail/sec_items_fetcher.py`（保有9銘柄だけ）。10-K の Item 7・10-Q の Part I Item 2 を `extract_item_section` で最大 **60,000字**まで取り（`_max_chars`）、**保存は `section_text[:2000]` だけ**（`excerpt`）。`excerpt_ja` は Grok の翻訳（有料） |
| 保存の中身 | SOUN 2026Q3 の `mda/SOUN/latest.json` は「Item 2. … The following discussion and analysis …」の定型の冒頭2,000字で終わり、**Results of Operations も Liquidity も入っていない** |
| 取得の無駄 | `fetch_and_save_all_items` が3項目（risk_factors・legal_proceedings・mda）を順に回し、**項目ごとに `_fetch_filing_text` で同じ書類を取り直す**（書類1本につき3回のダウンロード） |
| 切れる危険 | Liquidity and Capital Resources は MD&A の**後ろの方**にある。大きい会社の 10-K は Item 7 が60,000字を超えうるので、今の上限のままでは Liquidity が切れる（本文の実測で確かめる、§6-6） |
| 近い既存の経路 | `src/tail/sec_ctrl_fetcher.py`（10-Q の Item 4 の内部統制。正規表現だけで `effective`・`material_weaknesses` を出す。AI なし。**10-Q だけで 10-K の Item 9A は無い**）。8-K は RSS では見ていない（`edgar_rss_monitor.py` は 10-Q・10-K だけ） |

### 6-2. ③ の案（保存の形・場所・大きさ・回数）

**場所と形**（common の下に1か所。TAIL・STONKS・TANUKI が同じものを読む）:
- `common/filing_text/`（新設）。抽出は `extract_item_section`（目次の誤検知の除外・単語内スペースの対応つき）を共通へ移し、**TAIL の `sec_items_fetcher` も同じ関数を呼ぶ**（二重にしない）。節の切り分けは Item の中の見出し（`Results of Operations`・`Liquidity and Capital Resources`。揺れ: `Comparison of …`・`Liquidity` 単独・`Capital Resources and Liquidity` を許す）で機械で切る
- 保存: `common/filing_text/data/{T}/{期}.json`（期は `2026Q3`・`2025FY`、TAIL の `mda/` と同じ呼び名）。中身は `{ticker, period, form, accession, filing_date, report_date, source_url, extractor_version, sections: {results_of_operations: {text, chars, status}, liquidity: {text, chars, status}}}`。`status` は `ok`・`missing`（見出しが無い）・`truncated`（上限で切った）。**取れなかった節は空で `missing` と残す**（0やスキップで埋めない）。Item 全体の全文は持たない
- 翻訳（`excerpt_ja`）は共通には持たない。TAIL の画面の訳は今のまま TAIL 側

**大きさ**（設計値。実測は §6-6）:
- 1節の上限を **40,000字**（`truncated` の印つき）、Item 全体の読み込み上限は 200,000字に上げる（今の60,000だと Liquidity が切れる）
- 24銘柄 × （最新の10-K 1本＋その後の10-Q 最大3本）＝ **最大96書類 × 2節**。1節を平均1.5万字とすると約2.9MB、上限いっぱいなら最大7.7MB。1回の追加は新しい提出1本につき 0.03〜0.08MB
- 保持は **直近8四半期（10-Q・10-K 合わせて9本程度）**。④の ATM の「前の四半期との比べ」に最低5本が要る。古い期は書き手が消す（数は手で書かず、読み手が要る一番長い期間から導く。`retention.py` と同じ考え方）

**取得の回数**:
- 初回: 24銘柄 × 最大4書類 ＝ **最大96回**（書類の本体1回ずつ。0.5秒待ちの既存の間隔で約1分。SEC の上限10件/秒より十分遅い）
- 以後: 新しい提出があった銘柄だけ。日曜の `SEC_Data_Update` が鮮度の確認で `submissions` を取っている（`_freshness.json` の REFRESHED）ので、**その結果に乗せて1銘柄1書類**。年に24銘柄×4回＝約96回
- TAIL の3項目が書類を3回取っていたのを、共通の1回の取得に寄せると、保有9銘柄の取得は1/3になる（ついで。範囲は決めてもらう）

### 6-3. ④ 4種の印: 機械で見分ける方法と AI を使う部分

| 印 | XBRL（`company_facts`） | 本文・8-K の決まった言い回し | AI が要る所 | 今ある経路 |
|---|---|---|---|---|
| **事業継続の疑義**（going concern） | **無い**（標準タグを84銘柄すべてで探して0件） | 注記と MD&A の Liquidity に出る定型文（ASC 205-40）: `substantial doubt` と `going concern` が同じ文に。**否定・緩和の文は別**（`no substantial doubt`・`alleviated`・`does not raise`）。この区別は正規表現で足りる | **不要**。判別できない文は「要確認」と出し、印にしない | 無い（③の本文が前提） |
| **内部統制の重大な不備** | **無い**（dei の `IcfrAuditorAttestationFlag` は監査人の証明の有無で、不備の有無ではない） | 10-Q Item 4・10-K Item 9A の `not effective`・`material weakness` | **不要** | **ある**: `sec_ctrl_fetcher.py`（保有9銘柄の10-Q だけ）。STONKS の15銘柄と 10-K の Item 9A が無い |
| **人員削減と再編**（8-K） | 費用の計上後にしか出ない: `RestructuringCharges`（84銘柄中56）・`…NumberOfPositionsEliminatedPeriodPercent`（10）。**出るのは10-Q の提出後で、発表より遅れる。SOUN の2026年の27%削減は無い**（SOUN の最新の再編タグは2023年の40%・180人） | **8-K の Item 2.05**（Costs Associated with Exit or Disposal Activities）。提出日と `items` は SEC の `submissions` の応答に入っている（今の鮮度の確認が取っている応答。手元の `submissions.json` は縮約版で `items`・`form` を持たない → 保存する欄を足す）。人数・%・費用の範囲は本文の `approximately N%`・`workforce`・`reduction in force` | 検知は**不要**。数字の抜き出しは正規表現を先に、取れない書き方のときだけ Grok に該当段落を渡す（数字が原文にそのまま在ることを検算） | 無い（`earnings_pending.py` は 8-K Item 2.02 の提出日を使う設計だが「手元に無い」と書いてある。同じ `items` の取り込みで足りる） |
| **ATM の残りと四半期の売却額** | 標準のタグでは**ATM を他の増資と分けられない**（`ProceedsFromIssuanceOfCommonStock`・`StockIssuedDuringPeriodValueNewIssues` は公募・SBC も含む）。残りの枠は無い | 検知: 10-Q の `at-the-market`・`Sales Agreement`・`Equity Distribution Agreement`・`Open Market Sale Agreement`、8-K Item 1.01・目論見書補足〈424B5〉の提出 | **要る**: 「残り $300.0 million」「当四半期に X 株を売り、純手取り $Y」は会社ごとに文が違う。Grok に原文の該当文を渡し、**原文の文と数字を一緒に出す**（数字が原文の文字列に在ることを検算。ATM を持つと検知した銘柄だけ、新しい提出ごと〈年に数十回〉） | 無い |

### 6-4. ATM と `share_count_change` のつなぎ方

`common/dilution.py::share_count_change` は**過去3年の実績**（加重平均の希薄化後株数の年率。分割の調整つき）。ATM は**これから起きうる上限**と**直近の実際**という別の量なので、**計算には混ぜず、同じ分母で並べて見せる**:
- 残りの枠 ÷ 時価総額（＝最大の追加の希薄化率）。時価総額は `common/shares_base.py` の株数（TANUKI-SHARES-SINGLE-SOURCE-1）× 株価。分母を `share_count_change` の「期末の株数」と同じ基準にそろえる
- 四半期の売却額 ÷ 時価総額（＝直近の実際）。**検算**: 売却額 ≒ 株数の増え（分割の調整後）× 平均の株価。大きくずれる銘柄は、ATM 以外の増資・SBC・転換が混ざっている印
- 表示は `share_count_change` の行の隣に1行（「ATM 残り $300M＝時価総額の X%」）。`dilution_penalty`（funda の減点）には**入れない**（参考表示。決定済み）
- 将来 ATM の分を減点に入れるなら、`share_count_change` の分子（実際の増え）と二重に数えないよう、「未使用の枠」だけを別の項にする（今回は範囲外）

### 6-5. 今の24銘柄で印が付きそうなもの（試し。本文の取得なし）

| 印 | 付きそうな銘柄 | 根拠（何で見たか） |
|---|---|---|
| 内部統制の重大な不備 | **SOUN・CRWV**（保有）。ほか保有7銘柄は有効 | 保存済みの `ctrl/{T}/latest.json`（2026Q3）: 2銘柄は `effective=false`・不備の抜き出し12件、7銘柄は `effective=true`・0件。STONKS の15銘柄は未取得（`sec_ctrl_fetcher` の対象が保有だけ） |
| 人員削減と再編 | **SOUN**（2026-11-05 の 8-K、人員の27%・現金の費用16〜19M）。XBRL に出る: **ESTC**（2026-10-31 の期に人員7%、2026-11-20 提出）・**NET**（2026-06-30 の再編費150.7M）・**CELH**（2026-06-30 に80.9M・2025-09-30 に246.7M）・**S**（2026-07-31 に24.4M）・TSLA（2025-09-30 に238M） | SOUN は BACKLOG の経緯（11-22 の原本の確認）。XBRL は `company_facts` の再編のタグ（四半期の値は同じ開始日の累計の差で作った）。**SOUN は XBRL には無く、8-K の経路だけが拾える** |
| ATM（大型の増資の気配） | 実際の売却額は本文を取るまで分からない。気配（`ProceedsFromIssuanceOfCommonStock` の四半期）: **RKLB**（2026Q2 1,079M・Q3 2,382M。株数の年率 +10.5%）・IONQ（2025Q3 1,000M。+25.4%）・ASTS（2025Q3 114M・2026Q1 81M）・SOFI（2025Q3 1,700M）・RCAT（2025Q3 173M。+41.4%）。**SOUN は残り300M**（BACKLOG の経緯）だが XBRL には無い | `company_facts` の CF の発行額と `share_count_change`。**増資の額と ATM の額は別**（公募・転換・ワラントも入る）ので、実測で ATM の文を取って確かめる |
| 事業継続の疑義 | **不明**（本文を取っていない）。BACKLOG の経緯に SOUN の記載は無い（現金207.6M・約18〜19か月） | 本文の取得が要る |

### 6-6. 段の案・決めてほしいこと・実測で確かめること

**段の案**（読み手と書き手が増えるので1つずつ）: ④-1 内部統制を24銘柄に広げ、10-K の Item 9A も足す（今の経路の延長。AI なし）→ ③ 本文の保存（共通部品。TAIL の取得の寄せ直しを含めるか決める）→ ④-2 going concern（③の本文に正規表現）→ ④-3 8-K の `items` の保存と人員削減の印 → ④-4 ATM（AI の抽出つき）→ 表示（STONKS・TAIL が同じ `docs/common/risk_marks/{T}.json` を読む）。method_change は不要（理論株価・判定・funda・stage に入れない）。

**決めてほしいこと**:
- **(a)** ③の保存の場所を `common/filing_text/`、節は2つだけ（Item 全体は持たない）、保持を直近8四半期でよいか。TAIL の取得も同じ関数・1回の取得に寄せてよいか（今は書類1本を3回取る）
- **(b)** ④-1 から始めてよいか（内部統制だけ今の経路で24銘柄・10-K に広げる。AI なし・費用なし）
- **(c)** ATM の抽出に Grok（有料）を使ってよいか。使うなら ATM と検知した銘柄だけ・新しい提出ごと（年に数十回）。使わないなら ATM は「あり／なし」の印だけで金額は出さない
- **(d)** going concern は正規表現の2値（疑義あり／緩和済み）＋判別できない文は「要確認」（印にしない）でよいか
- **(e)** 本文の実測の許可: `SEC_USER_AGENT_EMAIL` を置いた回に、読み取りだけ（24銘柄の最新の10-Q 各1本＋10-K 1本、約50回・10件/秒以下）で、下の「実測で確かめること」を埋める
- **(f)** 8-K の `items` の保存（`submissions` の応答の欄を足す。追加のリクエストは0）を、④-3 の前提としてよいか

**実測で確かめること**（(e) の後に §6 へ書き足す）: 節の見出しで Results of Operations と Liquidity が24銘柄すべてで切れるか（`missing` の数）、1節の実際の文字数の分布（上限40,000字の妥当性）、Item 7 が60,000字を超える銘柄の数、going concern の定型文の当たり（SOUN ほか）、ATM の文の書き方の種類（正規表現で足りる銘柄の数）、`items` に `2.05` が付く8-K の直近1年の件数。

## 7. 限界（①②の STEP 0 の時点）

- ①の TANUKI への funda の効きは、`computed_runway_months` を差し替えて3銘柄（SOFI・COHR・AMZN）だけ確かめた。全銘柄の `chain`（TANUKI→HypeCore→TANUKI）の試算は、実装の後に scope を作るときに回す。
- ②は `company_facts` の事実から四半期の値を作る簡易な方法（申告日の新しい値、Q4＝年次−9か月）。Layer3 の `first_val`（その時点で最初に申告された値）の semantics とは一致しないので、実装では Layer3 の項目にするか、同じ規則に合わせる。
- 営業利益に入るかどうかは XBRL の表示の関係を読んでいない（許可リストの根拠は「営業費用に載ることが多いタグ」）。許可リスト外の候補（IONQ のワラントなど）は営業外のことが多い。
- ②の「許可リストに入れた減損」は、会社が営業利益に含めて開示しているもの（営業費用の行）を前提にした。
