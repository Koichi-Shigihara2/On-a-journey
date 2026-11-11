# SECTOR-MASTER-ADOPT-1 段4 STEP 0（2026-11-11、読み取りと手元の試算のみ。実装・公開はしない）

EPS ANALYZERの区分（`sector_classifier_v2.py`・`config/sectors.yaml`の9区分・`cik_lookup.csv`の`eps_sector`）を、業種のマスター（industry主・sector・SIC控え）からの写像に置き換える案の調査。

## 1. 今の決め方と、置き換える案

### 今（`SectorClassifierV2.classify`、`pipeline.process_one_ticker`が呼ぶ）
優先順位: `eps_sector`（cik_lookup.csvの手設定）> SICコード（sectors.yamlの`sic_codes`）> 会社名のキーワード > ティッカーのキーワード（`sectors.yaml`の`keywords`、単語境界）。当たらなければ区分なし（除外項目も成熟度の監視も無し）。
- `eps_sector`の手設定は3銘柄: RKLB（宇宙・航空）・SPIR（Technology）・VRT（Technology）。読み手は分類器だけ（grep: src・common・scripts・docs・.github）
- SICは`get_company_metadata`（SECのsubmissions）から。会社名も同じ

### 案（写像）
- 区分 = `config/sector_master.json`の`eps_category`（`common/sector_master.py`の`EPS_CATEGORY_BY_INDUSTRY`が先、無ければ`EPS_CATEGORY_BY_SECTOR`。industry主・SIC控えの決定どおり）。分類器の`classify`はマスターを読むだけにする（優先順位・`sic_codes`・`keywords`・`eps_sector`列の読み込みを廃止）
- `sectors.yaml`は9区分の`name`と`exclusions`（区分別の調整項目の除外・`maturity_watch`）だけを残す（`keywords`・`sic_codes`は消す）
- 写像で全95銘柄に区分が付く（今は41銘柄が「出ない」）
- 手設定3銘柄: **RKLB**（宇宙・航空）は写像と同じなので外す。**SPIR・VRT**（Technology）は写像（一般消費財 / 製造 / EV）と違うが、理由の記録が無い（cik_lookup.csvの備考は空、登録は2026-05）。段1の決定（「理由の無い例外は残さない」）に従い写像に寄せる（下の3のとおり調整後EPSは変わらない）。**`config/sector_exceptions.json`に足す例外は0件**（RCATは手設定でなくSIC 7372〈ソフトウェアのSIC〉による分類で、写像は宇宙・航空）。例外が要るようになったら、`damodaran_industry`と同じ形（銘柄→区分、理由つき）で`eps_category`の欄を足す

## 2. 区分の読み手と、マスターの別の区分・段1〜3との重なり

| 読み手 | 区分で決まること | 読む処理 | 重なり |
|---|---|---|---|
| `sectors.yaml`の`exclusions` → `adjustment_detector.detect_adjustments` | 区分ごとに、調整後EPSに足し戻さない項目（Technology: 在庫評価損・政府補助金・為替・デリバティブ・一過性税効果。SaaS・宇宙航空・通信: SBC・無形資産償却ほか。フィンテック: ローン公正価値・貸倒引当金。ヘルスケア: M&A統合・IPR&D。製造: 在庫評価損・政府補助金。REIT: 減価償却・資産売却損益〈`is_reit`は区分名に「不動産」を含むかで判定〉） | EPS ANALYZER（調整後EPS） | 「景気循環株」（在庫評価損を除く理由に近い）・「金融」とは別の目的。重ならない（下の「フィンテック / 銀行」だけ要整合） |
| `maturity_monitor.MaturityMonitor.monitor`（`SECTOR_THRESHOLD_MAP`・区分名の小文字） | SBC寄与率・SBC/売上高の警告の閾値 | EPS ANALYZER（**出力は標準出力のログだけ**。`quarterly.json`・summary.jsonに保存しない） | なし。注: 表のキーが区分名と合わず（「一般消費財 / 製造 / ev・半導体」対yamlの「…/ EV」）、Technology・製造・宇宙航空・通信は`default`の閾値に落ちる（今も同じ） |
| 区分の出力（`quarterly.json`の`sector`） | 読み手なし（画面・TANUKI・TAIL・STONKS・HypeCoreはgrepで0件） | — | — |
| TANUKI（理論株価・funda・最大EPS・RICE） | 区分は直接読まない。調整後EPS（`quarterly.json`・`annual.json`・`ttm.json`・`summary.json`）経由で`per_adjusted`・FCFの一過性費用（`fcf_outlier`）・最大EPSの分母に効く | TANUKI | 区分→調整後EPS→TANUKIの1経路だけ |
| STONKS・HypeCore・TAIL・画面（EPSの画面） | 調整後EPSを読まない／区分を表示しない（EPSの画面は`summary.json`の値を出すだけ） | — | なし |

マスターの別の区分・段1〜3との重なり:
- **金融（段3）との重なり1件**: 写像は`sector=="Financial Services"` → 「フィンテック / 銀行」だが、段3で V・MSCI は金融でない（SIC 7389）。写像のままだと V・MSCI にローン公正価値・貸倒引当金の除外が付く（今の値は変わらない: Vは変化0、MSCIは調整後EPSが16四半期で微小に動く）。**案: 写像を`financial`に従わせる**（`financial`がtrueなら「フィンテック / 銀行」、`sector`が金融でも`financial`がfalseなら「Technology」）。試算（`mapfin`）では MSCI の調整後EPSは今と同じ（22.25）、V も同じ
- 景気循環株（industryの語）・区分なし（銀行・保険・REIT・バイオ）・メガテック（段2）・Damodaranの業種（段1）とは目的が別で、重ならない（調整項目の除外は「事業の性質」の分類。区分なしの REIT は EPS の「不動産 (REITs)」と同じ対象だが、EPSの区分は残る）

## 3. 全銘柄の試算（EPS ANALYZERを全95銘柄回し、TANUKIへ波及）

手順: `scratchpad/eps_run.py`（SEC・Grok・Alpha Vantageを呼ばず、手元のcompany_facts・submissionsで回す。`before`は今の分類器）。前提の確認: `before`の調整後EPSは公開の`docs/.../adjusted_eps_analyzer/data`と95銘柄中93銘柄で完全に一致（差の2銘柄はAlpha VantageのEPS差分の検知〈SOUN・CELH〉を試算で切ったため）。TANUKIは`tools/sim`のchain＋`eps_redirect.py`（EPSの置き場だけを試算の出力に差し替え）。出力は`%TEMP%\epssim\`・`%TEMP%insim\`（`eps_cmp.json`・`cmp_e4_*.txt`）。

- **区分が付く・変わる: 44銘柄**（今の「出ない」41 ＋ RCAT・SPIR・VRTの3）。写像後の内訳: Technology 50・一般消費財/製造/EV 21・宇宙・航空 9・通信・インフラ 6・ヘルスケア/バイオ 5・フィンテック/銀行 3（SOFI・V・MSCI）・不動産 1（CSGP）
- **調整後EPSが変わる: 16銘柄**（44のうち、足し戻す項目が実際にある銘柄）。TTM調整後EPSが変わるのは14銘柄。**保有銘柄は0**（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA）
- 大きいもの: AVAV 5.27→1.76（宇宙・航空はSBC・無形資産償却を足し戻さない）・HEI 6.84→6.01・LITE −69.05→−73.01・ASTS −1.69→−2.21・JOBY −0.53→−0.74・LOAR 0.76→0.63・VST 5.90→5.69・TER 9.33→9.19・ENTG 4.35→4.13 など。小さいもの: CEG・HQY・MSCI・QBTS・KULR・XOM
- **TANUKI**（`tools/sim`のchain、IV・判定・funda・stage・RICE）: **理論株価が変わる 0・判定が変わる 0**。HypeCoreのstageも変化0。変わるのは10銘柄の次の欄だけ: `components.per_adjusted`（AVAV 23.42→69.94・HEI 50.44→57.42・LOAR 104.82→127.02・VST 22.19→23.01・CEG・HQY・MSCI の微小）と、`fcf_outlier`の注記・一過性費用の根拠（ENTG 一過性費用$62M→$24M・QBTS・TER は「EPSアナライザーの同じ期の調整に無い」へ。どれも「上方乖離のため除外しない」の判定で理論株価は変わらない）
- STONKS: 調整後EPSを読まないので影響なし
- 成熟度の監視（ログだけの出力）: 警告の数 42→66（区分が付いて閾値の判定が走る銘柄が増える。値・判定に出力なし）
- `mapexc`（SPIR・VRT・RCATを例外でTechnologyに残す場合）: 区分の変化41・調整後EPSが変わる15銘柄。RCATの分（27四半期）だけ減る。TANUKIの結果は`map`と同じ

### 区分が付く・変わる44銘柄

| 銘柄 | 今の区分 | マスターの写像 | yfinance sector / industry | 調整後EPSが変わる四半期 | TTM調整後EPS（前→後） | 保有 |
|---|---|---|---|---|---|---|
| AMZN | （出ない） | 一般消費財 / 製造 / EV | Consumer Cyclical / Internet Retail | 0 | 13.691 → 13.691 |  |
| ASTS | （出ない） | 通信・インフラ | Technology / Communication Equipment | 23 | -1.689 → -2.213 |  |
| AVAV | （出ない） | 宇宙・航空 | Industrials / Aerospace & Defense | 34 | 5.274 → 1.764 |  |
| BROS | （出ない） | 一般消費財 / 製造 / EV | Consumer Cyclical / Restaurants | 0 | 0.907 → 0.907 |  |
| CAKE | （出ない） | 一般消費財 / 製造 / EV | Consumer Cyclical / Restaurants | 0 | 4.877 → 4.877 |  |
| CART | （出ない） | 一般消費財 / 製造 / EV | Consumer Cyclical / Internet Retail | 0 | 3.243 → 3.243 |  |
| CAT | （出ない） | 一般消費財 / 製造 / EV | Industrials / Farm & Heavy Construction Machinery | 0 | 27.129 → 27.129 |  |
| CEG | （出ない） | 通信・インフラ | Utilities / Utilities - Independent Power Producers | 20 | 9.141 → 9.108 |  |
| CIX | （出ない） | 一般消費財 / 製造 / EV | Industrials / Security & Protection Services | 0 | 1.877 → 1.877 |  |
| COHR | （出ない） | Technology | Technology / Scientific & Technical Instruments | 0 | 7.449 → 7.449 |  |
| CPRT | （出ない） | 一般消費財 / 製造 / EV | Industrials / Specialty Business Services | 0 | 1.578 → 1.578 |  |
| CSGP | （出ない） | 不動産 (REITs) | Real Estate / Real Estate Services | 0 | 1.146 → 1.146 |  |
| DELL | （出ない） | Technology | Technology / Computer Hardware | 0 | 18.119 → 18.119 |  |
| ELF | （出ない） | 一般消費財 / 製造 / EV | Consumer Defensive / Household & Personal Products | 0 | 3.989 → 3.989 |  |
| ENTG | （出ない） | Technology | Technology / Semiconductor Equipment & Materials | 35 | 4.345 → 4.125 |  |
| FICO | （出ない） | Technology | Technology / Software - Application | 0 | 41.927 → 41.927 |  |
| FLYW | （出ない） | Technology | Technology / Software - Infrastructure | 0 | 1.187 → 1.187 |  |
| GEV | （出ない） | 一般消費財 / 製造 / EV | Industrials / Specialty Industrial Machinery | 0 | 40.350 → 40.350 |  |
| HEI | （出ない） | 宇宙・航空 | Industrials / Aerospace & Defense | 39 | 6.842 → 6.011 |  |
| HON | （出ない） | 一般消費財 / 製造 / EV | Industrials / Conglomerates | 0 | 24.911 → 24.911 |  |
| HQY | （出ない） | ヘルスケア / バイオ | Healthcare / Health Information Services | 33 | 4.903 → 4.862 |  |
| JOBY | （出ない） | 宇宙・航空 | Industrials / Airports & Air Services | 25 | -0.526 → -0.741 |  |
| KLAC | （出ない） | Technology | Technology / Semiconductor Equipment & Materials | 0 | 4.302 → 4.302 |  |
| KULR | （出ない） | Technology | Technology / Electronic Components | 1 | -2.170 → -2.170 |  |
| LITE | （出ない） | 通信・インフラ | Technology / Communication Equipment | 37 | -69.050 → -73.005 |  |
| LOAR | （出ない） | 宇宙・航空 | Industrials / Aerospace & Defense | 14 | 0.759 → 0.627 |  |
| LRCX | （出ない） | Technology | Technology / Semiconductor Equipment & Materials | 0 | 6.976 → 6.976 |  |
| LYFT | （出ない） | Technology | Technology / Software - Application | 0 | 0.164 → 0.164 |  |
| MO | （出ない） | 一般消費財 / 製造 / EV | Consumer Defensive / Tobacco | 0 | 5.024 → 5.024 |  |
| MSCI | （出ない） | フィンテック / 銀行 | Financial Services / Financial Data & Stock Exchanges | 16 | 22.250 → 22.196 |  |
| PAYS | （出ない） | Technology | Technology / Software - Infrastructure | 0 | 0.389 → 0.389 |  |
| PM | （出ない） | 一般消費財 / 製造 / EV | Consumer Defensive / Tobacco | 0 | 7.380 → 7.380 |  |
| QBTS | （出ない） | Technology | Technology / Computer Hardware | 10 | -0.307 → -0.307 |  |
| RCAT | Technology | 宇宙・航空 | Industrials / Aerospace & Defense | 27 | -0.744 → -0.855 |  |
| SCCO | （出ない） | 一般消費財 / 製造 / EV | Basic Materials / Copper | 0 | 7.691 → 7.691 |  |
| SPIR | Technology | 一般消費財 / 製造 / EV | Industrials / Specialty Business Services | 0 | -2.293 → -2.293 |  |
| TASK | （出ない） | Technology | Technology / Information Technology Services | 0 | 1.435 → 1.435 |  |
| TDY | （出ない） | Technology | Technology / Scientific & Technical Instruments | 0 | 25.915 → 25.915 |  |
| TER | （出ない） | Technology | Technology / Semiconductor Equipment & Materials | 38 | 9.326 → 9.190 |  |
| V | （出ない） | フィンテック / 銀行 | Financial Services / Credit Services | 0 | 14.052 → 14.052 |  |
| VRT | Technology | 一般消費財 / 製造 / EV | Industrials / Electrical Equipment & Parts | 0 | 6.030 → 6.030 |  |
| VST | （出ない） | 通信・インフラ | Utilities / Utilities - Independent Power Producers | 30 | 5.902 → 5.692 |  |
| WMT | （出ない） | 一般消費財 / 製造 / EV | Consumer Defensive / Discount Stores | 0 | 2.764 → 2.764 |  |
| XOM | （出ない） | 一般消費財 / 製造 / EV | Energy / Oil & Gas Integrated | 3 | 9.740 → 9.740 |  |

## 4. 区分の付き方の食い違い（RCAT・SPIR・VRT）— 案と理由

| 銘柄 | yfinance（マスター） | 今のEPSの区分 | 写像 | 調整後EPSの変化 |
|---|---|---|---|---|
| RCAT | Industrials / Aerospace & Defense（SIC 7372） | Technology（SICがソフトウェアのコードのため） | 宇宙・航空 | あり（27四半期、TTM −0.744→−0.855） |
| SPIR | Industrials / Specialty Business Services（SIC 4899） | Technology（`eps_sector`手設定） | 一般消費財 / 製造 / EV | なし |
| VRT | Industrials / Electrical Equipment & Parts（SIC 3679） | Technology（`eps_sector`手設定） | 一般消費財 / 製造 / EV | なし |

**案: 3銘柄ともマスター（yfinanceのindustry）に寄せる**。理由:
1. マスターの決まり（industry主・SICは控え）。RCATはSIC 7372（ソフトウェア）が会社の実態（ドローン・防衛）と合わないのがそもそもの食い違いで、SICを先にする今の順序が原因
2. SPIR・VRTの手設定は理由の記録が無く（登録の備考は空）、段1の決定（理由の無い例外は残さない）と同じ扱い。調整後EPSは変わらない（Technologyと製造で、この2銘柄に実際にある項目は同じ）
3. RCATだけ値が動く（TTM調整後EPS −0.744→−0.855、足し戻すSBC・無形資産償却が無くなる）。TANUKIの結果は変わらない（RCATの最新四半期の調整後EPSは同じで、変わるのはTTMだけ。RCATはTANUKIの対象でlatest.jsonに差が出ない）。宇宙・航空の除外（SBC・無形資産償却は足し戻さない）の方がRKLB・LOAR・AVAVと同じ扱いで一貫する
4. 反対の案（Technologyに残す）は例外3件（理由を後付けで書く）になり、写像の価値が減る。残す場合は`sector_exceptions.json`に`eps_category`の欄を足し、理由つきで書く

## 5. 実装の形（STEP 0の案。実装は別の指示）

1. `common/sector_master.py`: `eps_category(sector, industry, financial)`に`financial`を渡し、金融でない`Financial Services`は「Technology」（V・MSCI）。マスターの`eps_category`の欄を作り直す（Market Data Weeklyの作り直しで入る）
2. `sector_classifier_v2.py`: `classify`をマスターの`eps_category`を返すだけにする（マスターに無い銘柄は`sector_master.eps_category(sector, industry, financial)`をattributesから）。`eps_sector`列・`classify_by_sic`・`classify_by_keywords`を廃止
3. `config/sectors.yaml`: `keywords`・`sic_codes`を消し、`name`・`exclusions`だけ。`test_sector_master.py`の「写像の表の名前が今の読み手の名前と同じ」は残す
4. `config/cik_lookup.csv`の`eps_sector`列は空にして読まない（列の削除は登録の手順・テスト・一覧に触るので別の件）
5. 計算方法の変更の手順（scope `SECTOR-MASTER-ADOPT-1-4`: EPS 16銘柄・TANUKI 10銘柄・IV/判定の変化 0）。EPSが動く変更は派生（`ttm`・`annual`・`summary`）を同時に作り直し、手元の試算のEPSは公開と一致しない場合がある（[[METHOD-CHANGE-NOTICE-ACCURACY-1]]の注意。今回の手元の試算は公開のEPSと93/95銘柄一致）
6. `SECTOR_THRESHOLD_MAP`の名前の食い違い（Technology・製造・宇宙航空・通信が`default`に落ちる）は、成熟度の監視がログだけの出力なので今回は直さない（FINDINGS）
