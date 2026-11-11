# PARSER-LAYER3-SINGLE-SOURCE-1 STEP 0（2026-11-11、読み取りと手元の試算のみ。実装・公開はしない）

parser（`common/sec_data/parser.py`、年次`annual_YYYY.json`・四半期`quarterly_YYYYQn.json`）の値を、Layer3（`layer3_builder.build_ticker_store`。companyfactsとLayer2の候補定義から作り、`cf_lines`・`bs_lines`で欠けを補う）から作る案の調査。
「Layer3」はこの文書では`build_ticker_store`の結果（41項目。申告の行の補いを含む）を指す。

道具（再現）: `tools/sim/parser_l3/`（`parser_vs_l3.py`＝parserとLayer3を全銘柄・全期・全項目で比べる、`parser_overlay.py`＝parserの値をLayer3の値に置き換えた試算用のファイルを作る、`parser_redirect.py`＝`tools/sim/sim.py chain --hook`でparserの読みだけを差し替える）。値が違う2,884セルは`docs/architecture/PARSER_LAYER3_STEP0_diff_cells.csv`。

## 比べ方（注意）
- parserのファイルには期末日が無い（期のラベル「2026Q2」・「2025」だけ）ので、その期の各項目の値と一致するLayer3のエントリの期末日の多数決で期末日を決め、同じ期末日のLayer3の値と比べた。多数決で決まらない期は「期末不明」として除いた: **4,884期のうち2,324期**（Layer3は年次8年・四半期約28期しか持たず〈保持期間、[[LAYER3-RETENTION-PERIOD-1]]〉、parserは年次を2006年から持つ）。比べたのは2,560期・約6.8万セル
- Layer3は同じ期に2つの値を持つ: `val`（最新の申告。改訂後）と`first_val`（その期を最初に申告した値）。parserは多くの項目で原本（`first_val`）の値で、「parserの値が`first_val`と同じで`val`と違う」改訂の違いが1,876セルある。これを「原本と同じ」として分けた

## 1. parserが今書く項目と、Layer3での置き換え

parserの項目は33（bs 15・pl 9・cf 6・shares 2・other 1、ほかに期ごとの`*_provenance`と`bs_tag_sources`）。Layer3は41項目。セルの数は比べた期（期末が決まった2,560期）だけ。

| parserの項目 | 年次: 同じ | 年次: 原本の値と同じ（Layer3の最新の値は改訂後） | 年次: 違う | 年次: parserだけ | 年次: Layer3だけ | 四半期: 同じ | 四半期: 原本と同じ | 四半期: 違う | 四半期: parserだけ | 四半期: Layer3だけ |
|---|---|---|---|---|---|---|---|---|---|---|
| bs.cash_and_equivalents | 578 | 18 | 8 | 76 | 10 | 1750 | 13 | 2 | 22 | 32 |
| bs.current_assets | 600 | 14 | 1 | 65 | 0 | 1827 | 5 | 0 | 3 | 0 |
| bs.current_liabilities | 597 | 16 | 1 | 65 | 1 | 1818 | 14 | 0 | 3 | 0 |
| bs.goodwill | 546 | 19 | 1 | 56 | 19 | 1579 | 3 | 0 | 3 | 63 |
| bs.insurance_reserves | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| bs.intangible_assets_excl_goodwill | 551 | 17 | 1 | 56 | 20 | 1536 | 1 | 0 | 3 | 70 |
| bs.long_term_debt | 480 | 6 | 20 | 53 | 49 | 1295 | 10 | 18 | 1 | 310 |
| bs.minority_interest | 182 | 2 | 0 | 21 | 54 | 513 | 0 | 3 | 0 | 175 |
| bs.preferred_stock | 294 | 1 | 0 | 26 | 61 | 664 | 0 | 1 | 2 | 335 |
| bs.restricted_cash | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| bs.short_term_debt | 388 | 5 | 39 | 44 | 70 | 965 | 0 | 40 | 4 | 445 |
| bs.short_term_investments | 320 | 0 | 3 | 34 | 39 | 869 | 0 | 4 | 3 | 169 |
| bs.stockholders_equity | 599 | 30 | 6 | 68 | 3 | 1727 | 86 | 8 | 3 | 28 |
| bs.total_assets | 604 | 15 | 1 | 65 | 0 | 1841 | 9 | 0 | 3 | 0 |
| bs.total_liabilities | 504 | 9 | 107 | 64 | 0 | 1801 | 25 | 0 | 3 | 24 |
| cf.buyback | 465 | 6 | 0 | 0 | 0 | 968 | 15 | 6 | 17 | 102 |
| cf.capital_expenditure | 604 | 21 | 75 | 0 | 5 | 0 | 0 | 0 | 0 | 1835 |
| cf.depreciation_and_amortization | 654 | 23 | 9 | 8 | 0 | 1146 | 25 | 565 | 22 | 39 |
| cf.free_cash_flow | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| cf.operating_cash_flow | 682 | 21 | 0 | 0 | 0 | 1708 | 93 | 9 | 16 | 25 |
| cf.stock_based_compensation | 645 | 9 | 0 | 0 | 0 | 1544 | 37 | 18 | 6 | 76 |
| other.rpo | 0 | 0 | 0 | 544 | 0 | 1253 | 5 | 38 | 46 | 13 |
| pl.cost_of_revenue | 595 | 36 | 0 | 2 | 2 | 1494 | 92 | 0 | 0 | 77 |
| pl.eps_basic | 627 | 35 | 1 | 0 | 0 | 1614 | 94 | 0 | 2 | 66 |
| pl.eps_diluted | 626 | 35 | 1 | 1 | 0 | 1607 | 96 | 2 | 2 | 71 |
| pl.gross_profit | 391 | 21 | 0 | 224 | 0 | 976 | 81 | 0 | 0 | 609 |
| pl.net_income | 685 | 21 | 0 | 0 | 0 | 1742 | 104 | 3 | 0 | 4 |
| pl.operating_income | 629 | 25 | 0 | 48 | 0 | 1620 | 89 | 0 | 0 | 95 |
| pl.research_and_development | 517 | 30 | 2 | 0 | 0 | 1201 | 67 | 1 | 0 | 45 |
| pl.revenue | 684 | 19 | 1 | 0 | 0 | 1753 | 66 | 2 | 0 | 25 |
| pl.selling_and_marketing | 478 | 27 | 5 | 0 | 0 | 779 | 40 | 4 | 0 | 23 |
| pl.selling_general_and_administrative | 309 | 29 | 0 | 0 | 0 | 841 | 42 | 0 | 0 | 16 |
| shares.shares_basic | 628 | 38 | 1 | 0 | 0 | 1615 | 87 | 0 | 2 | 71 |
| shares.shares_diluted | 619 | 39 | 1 | 8 | 0 | 1611 | 90 | 0 | 2 | 105 |

合計: 年次 同じ16,081・原本と同じ587・違う284・parserだけ1,528・Layer3だけ333、四半期 同じ41,657・原本と同じ1,289・違う724・parserだけ168・Layer3だけ4,948（符号だけ違う3）。

### 置き換えの扱い（項目ごと）

| 区分 | 項目 | 扱いの案 |
|---|---|---|
| そのまま置き換えられる（定義が同じ。違うのは改訂・端数） | revenue・net_income・operating_income・gross_profit・cost_of_revenue・R&D・S&M・SG&A・eps_basic/diluted・shares_basic/diluted・operating_cash_flow・stock_based_compensation・buyback・cash_and_equivalents・short_term_investments・current_assets・current_liabilities・goodwill・intangible_assets_excl_goodwill・stockholders_equity・total_assets・minority_interest・preferred_stock・long_term_debt・short_term_debt | Layer3から作る。値の違いは下の3・4のとおり（改訂の違い・parserの誤り・Layer3の誤りが混ざる）。四半期の借入金は`bs_lines`の補いを正にする（CAKEの前年度末の列の型が直る） |
| Layer3の定義を先に直す | **total_liabilities**（年次で違う107セル。Layer3の候補に`LiabilitiesAndStockholdersEquity`が入っており、`Liabilities`が無い期に総資産と同じ値を採る。KO 2025: Layer3 104.8B・parser 72.6B〈総資産−純資産〉）・**depreciation_and_amortization**（四半期で違う565セル。parserは複数タグの合算〈`MERGE_ALL_TAGS_FIELDS`〉、Layer3は最初の候補だけ。ZS 2024年: parser 14.6M・Layer3 66.3M〈Layer3が正〉、CSGP 2026Q3: parser 44M・Layer3 98M）・**capital_expenditure**（年次で違う75セル。parserは`cf_lines`を正にしたFCFの定義〈段5〉、Layer3の候補はタグだけ。DDOG 2023: 27.6M対62.4M） | Layer3の候補の定義を「合算の規則・恒等式・`cf_lines`」に寄せてから、parserを置き換える。先にparserを置き換えると、Layer3の誤りがそのまま出力に出る |
| Layer3に項目が無い | free_cash_flow（年次のみ）・insurance_reserves・restricted_cash | free_cash_flowは`common/sec_data/fcf.py`の関数（OCF − max(0, \|CapEx\| − \|リース\|)）で作る。insurance_reserves・restricted_cashは読み手がコードに無い（grep: src・common・scripts）ので廃止を提案 |
| Layer3にあってparserに無い | 四半期のcapital_expenditure（1,835セル）・四半期のgoodwill等の0埋め（`bs_lines`の「貸借対照表に行なし」）・total_liabilities（四半期） | 足すかは読み手次第。0埋めは「欠け」と「0」の区別を変える（TANUKIの純現金の`cash_missing`・STONKSの0の扱い）ので、parserに無い項目は足さずに始める案（下の試算S2は足した場合） |
| parserだけの値 | 年次の古い年（Layer3の保持期間の外）、rpo年次（Layer3は四半期のみ。544セル） | Layer3の保持期間（年次8年）より長い年次に読み手がある（`get_roe_avg`は10年、FCFの履歴5〜10年）。**保持期間を年次10年以上に延ばすか、古い年だけparserを残すか**（案: 延ばす） |

## 2. 読み手（`docs/architecture/output_inventory/sec_layer.json`の読み手と、コード内のフィールド名の照合）

annual_YYYY.jsonの読み手（テスト以外）は17ファイル、quarterly_*.jsonの読み手は6ファイル。`reader.py`（`SECReader`）が両方を読み、多くの処理がそこを通る。

| 読み手 | 年次 | 四半期 | 読む項目 | 用途 |
|---|---|---|---|---|
| `SECReader`（`reader.py`: get_annual・get_annual_range・get_quarterly・get_quarterly_range・get_fcf_5yr_avg・get_roe_avg・get_net_cash・get_rpo・get_diluted_shares・get_latest_revenue・get_fcf_list_with_dates・get_eps_diluted） | ○（5〜10年） | ○（最新・8期） | cash・LTD・STD・STI・equity・total_assets・capex・FCF・OCF・rpo・eps_diluted・net_income・revenue・shares | TANUKI・STONKSの入口 |
| TANUKI `data_fetcher.py` | ○（FCF・RICEの年次） | ○（最新の純現金・rpo） | capex・D&A・FCF・OCF・SBC・rpo・net_income・R&D・revenue・S&M | 理論株価の入力（FCF基準・成長率・RICE・SBC補正） |
| TANUKI `pipeline.py` | ○（最新年次のBS・年次FCFの系列） | ○（最新） | cash・current_assets/liabilities・LTD・STD・STI・equity・total_assets/liabilities・buyback・capex・D&A・FCF・SBC・GP・NI・OI・R&D・revenue・S&M・shares_diluted | 画面・report.txtの値・財務の健全性 |
| TANUKI `core_calculator.py`・`adjustments.py` | ○ | ○ | rpo・LTD・STD・STI・R&D・revenue | RPO補正・純現金・RICE |
| STONKS `fetcher.py`・`analyzer.py`・`financial_as_of.py`・`pipeline.py` | ○（直近年次、3年） | ○ | cash・STI・equity・capex・D&A・FCF・OCF・SBC・COGS・GP・NI・OI・R&D・revenue・S&M・SG&A・shares_diluted | STONKSのスコア・runway・財務の期末日 |
| `roic.py`（ROIC共有部品） | ○ | — | cash・LTD・STD・equity・operating_income | HypeCore・TANUKIのROIC/WACC |
| `stonks_flag_rule.py`・`registration_validator.py` | ○ | — | operating_income・revenue・net_income | 登録の検証・stonks_siloのフラグ |
| EPS ANALYZER `execution_metrics.py`・`pipeline.py` | ○ | — | revenue・net_income | 実行力の指標の売上（EPS本体はcompanyfactsを直接読む） |
| `screening/dcf_validity_checker.py` | ○ | — | cash・LTD・STD・equity・revenue | 候補のスクリーニング |
| `report_consistency_check.py`（rcc） | ○ | ○ | 21項目 | CHECK群（鮮度・恒等式・FCFほか） |
| TTM（`ttm_calculator.py`）・Layer3の読み手（TAIL・ROTCE・HypeCoreの一部） | — | — | companyfactsから直接（parserを経由しない） | **parserを置き換えても変わらない** |

期のまとめ: TANUKIは年次5〜10年（FCF・ROE）と最新の四半期（純現金）、STONKSは直近年次、rccは全期。TTMは別経路（`normalized/`・`ttm/`）で、parserの置き換えの対象外。

## 3. 束ねた3件は置き換えで直るか

| 件 | 結果 |
|---|---|
| SOUN 2021年のSPAC（OCF −0.86M、正しくは−66.2M）・VRT 2018・2019年 | **Layer3の`val`（最新の申告）で直る**（SOUN 2021 OCF: `val` −66.177M・`first_val` −0.864M。net_incomeも−79.5M対−0.98M）。**`first_val`だと直らない**（原本は器の会社の値）。置き換えは「最新の申告の値（val）」で行う必要がある。VRT 2018 OCF: `val` −221.9M・parser −0.71Mも同じ |
| ZSのD&A（2024・2025年） | **直る**（Layer3 66.3M・104.4M、parser 14.6M・16.8M。四半期も同じ型）。ただしLayer3のD&Aの定義（合算の規則）を先に確かめる |
| CAKEの前年度末の列（2026Q2のshort_term_debt） | **直る**（Layer3は`bs_lines`の補いで0、parser 68.8M〈前年度末の値〉）。副作用: 2026Q3は負債が長期から短期へ移り（Layer3: long_term_debt 0・short_term_debt 563.7M）、parserは long_term_debt 561.3M・short_term_debtなし。合計はほぼ同じで、内訳が違う |

## 4. 全銘柄の試算（`tools/sim`の前後比較。parserの値をLayer3の値に置き換えて、TANUKI → HypeCore → TANUKIを回す。STONKSは外部取得が要るため回さず、値が変わるセルの数で見る）

置き換えの規則の違いで3通り（`parser_overlay.py`）:
- **S1**: 違う値（`val`・`first_val`のどちらとも違う1,008セル）を`first_val`で置き換え。parserが原本の値と同じセル（改訂の違い）は置き換えない
- **S3**: parserと違う値すべて（2,884セル。改訂の違いを含む）をLayer3の`val`（最新）で置き換え
- **S2**: S1 ＋ parserに無い項目（Layer3だけの5,281セル、0を含む）を足す

| 試算 | 置き換えるセル | 理論株価が変わる銘柄 | 判定が変わる銘柄 | 保有銘柄の変化 | HypeCore |
|---|---|---|---|---|---|
| S1 | 1,008 | 24 | 3（ABBV TRIM→HOLD・DDOG BUY→HOLD・HQY WATCH→HOLD） | 0 | stageは直近3か月で変化0（出力の`price_iv_ratio`だけ動く） |
| S3 | 2,884 | 37 | 4（上の3 ＋ IOT TRIM→HOLD） | NVDA +2.6%・SOFI +33.4%・TSLA +24.6%（判定は変わらず） | stage変化0 |
| S2 | 6,289 | 48 | 8（S1の3 ＋ ALAB HOLD→TRIM・FLYW BUY→HOLD・GEV HOLD→BUY・S HOLD→TRIM・SOUN WATCH→SELL） | NVDA −7.8%・PLTR −4.1%・SOFI −32.3%・SOUN 6.41→−1.79（WATCH→SELL） | stage変化0 |

大きい変化（S1）: DDOG 259.42→147.03（−43.3%、年次capexの定義）・ENTG +41.0%（年次BS）・ABBV +31.8%・CAKE +29.1%（四半期BS）・INTU +27.7%・AMD −21.7%・AVAV −20.8%・FLYW +15.2%。調整後EPS（EPS ANALYZER）はcompanyfactsを直接読むため変化0。S2の大きな変化（SITM +311%・S −175%・SOUN・SOFIほか）は、parserに無い項目をLayer3の0や値で足したことで「欠け」と「0」の区別が変わるため（足さない案が安全）。

### 段ごとの試算（S1、置き換える項目を分けて回した）

| 置き換える項目 | セル | 理論株価が変わる銘柄 | 判定の変化 | 保有 |
|---|---|---|---|---|
| 四半期の貸借対照表 | 76 | 4（CAKE +29.1%・KO −3.2%・HWM −1.1%・ADSK −0.6%） | 0 | 0 |
| 年次の貸借対照表 | 188 | 14（ENTG +41.0%・ABBV +31.8%〈TRIM→HOLD〉・AMD −21.7%・AVAV −20.8%・COHR・GOOGL・QBTS・ONDS・LLY・RCAT・LRCX・BSY・ADSK・HWM） | 1（ABBV） | 0 |
| 年次のCF | 84 | 8（DDOG −43.3%〈BUY→HOLD〉・INTU +27.7%・FLYW +15.2%・IONQ・HQY〈WATCH→HOLD〉・SPIR・RCAT・RMBS） | 2（DDOG・HQY） | 0 |
| 年次のPL・株式数・RPO | 12 | 4（AVAV +15.6%・RCAT・BBAI・TER） | 0 | 0 |
| 四半期のCF・PL・株式数・RPO | 648 | 0 | 0 | 0 |

STONKS（24銘柄）: 年次で値が変わるセルはS1で32セル・13銘柄（capex 13・cash 5・D&A 5・equity 4ほか）、**直近年次のセルは0**（STONKSは直近年次を読む）。S3は91セル・16銘柄。

保有銘柄（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA）: S1は変化0。S3・S2では上のとおり動く（改訂の違い・足した0の影響で、そのままは採れない）。

## 5. 段の案と各段の変化の見込み

先に決めること（チャット側）: (a) 置き換えの値は`val`（最新）か`first_val`（原本）か。SPAC（SOUN・VRT）は`val`でないと直らず、改訂の違い（APPの売上等）は`val`で動く。案: `val`を基本にし、動く銘柄の一覧を段ごとに確かめる。(b) Layer3の保持期間を年次10年に延ばすか。(c) parserに無い項目（0埋めを含む）を足すか（案: 足さない）。

| 段 | 内容 | 変化の見込み（S1の試算） | 備考 |
|---|---|---|---|
| 0 | Layer3の定義を先に直す: total_liabilitiesの候補（`LiabilitiesAndStockholdersEquity`を外し恒等式）・D&Aの合算の規則・capex（`cf_lines`）・符号の3セル | parserは変えないので出力0 | 以降の段の前提（FINDINGSに1行） |
| 1 | 四半期の貸借対照表（net_cashの入力。CAKE型） | 4銘柄（CAKE +29%）、判定0、保有0 | BACKLOGの記載（CAKEは約3%の過小評価）より前後比較は大きい（+29%）。借入金の内訳〈長期→短期〉の違いが入っているため、CAKEを個別に確かめる |
| 2 | 四半期のCF・PL・株式数・RPO | 648セル、理論株価0（TTMは別経路） | 画面・rccの値が変わる。リスク小 |
| 3 | 年次のPL・株式数・RPO | 4銘柄（AVAV +15.6%・RCAT・BBAI・TER）、判定0 | AVAVを個別に確認 |
| 4 | 年次のCF（SOUNのSPACの年・DDOGのcapex） | 8銘柄、判定2（DDOG・HQY）、保有0 | DDOG −43%は`cf_lines`のcapexの定義が正かを先に確かめる（parserは`cf_lines`を反映済み。Layer3のcapexが`cf_lines`でない可能性） |
| 5 | 年次の貸借対照表 | 14銘柄、判定1（ABBV）、保有0 | ENTG +41%・ABBV +32%の中身（短期負債の有無）を個別に確かめる |
| 6 | parserの廃止（`parser.py`と`annual_*`・`quarterly_*`の書き手）、free_cash_flow（`fcf.py`）・insurance_reserves・restricted_cashの扱い。`fixed_registry`（凍結）・`fact_overrides`・`dimension_aggregate`・SPAC・fy_tagの衝突の検知の移し先 | 段5までに動いた分だけ | parserの特例をLayer3側へ移す設計が要る。大 |

各段で計算方法の変更の手順（scope・派生ファイル・読み合い・印）。段1〜5は理論株価・判定が動く銘柄があるので、段ごとにscopeを出す。
