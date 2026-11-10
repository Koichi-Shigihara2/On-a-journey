# SECTOR-MASTER-ADOPT-1 段3 STEP 0（2026-11-11、読み取りと手元の試算のみ。実装・公開はしない）

「金融」の判定を、yfinanceの`sector=="Financial Services"`の文字列比較から、業種のマスター（`config/sector_master.json`の`financial`、sector＋SIC）に寄せる案の調査。

## 1. 金融の判定をしている箇所（grep: `Financial Services`・`financial`・`_is_insurance`・`is_financial`・`金融`、src・common・scripts・docs）

| # | 場所 | 判定の中身 | 効くもの | 読む処理 |
|---|---|---|---|---|
| 1 | `calculator/fcfe.py:75 is_financial_institution_ticker` ← `config/financial_institution_config.json`（`tickers:["SOFI"]`の手書き） | 手書きの1銘柄 | FCFEエクイティDCF（参考表示。判定・理論株価に不使用） | TANUKI（`pipeline.py:3322`） |
| 2 | `common/sec_data/reader.py:757-764 get_net_cash`の金融ガード | 保険（industryに"insurance"・ticker 20本・sector=="Insurance"）→ 負債0、`sector=="Financial Services"`→ 短期負債0（`fintech_st_debt_excluded`）。金融ガードの銘柄はファイナンス・リースも足さない | 純現金・純負債（理論株価のEV→株主価値、runway、画面の純負債） | TANUKI（`data_fetcher.py:929`）・STONKS（`stonks_silo/pipeline.py:121`）・rcc CHECK-49（`ttm`ではなく直接呼ぶ） |
| 3 | `calculator/rice.py:324-331 RICE_EXCLUDED_SECTORS`（`Financial Services`・`Insurance`・`Real Estate`ほか） | sectorの文字列 | RICE（理論株価・判定には不使用。画面とTANUKI SCOREに出る） | TANUKI（`core_calculator.py:755`） |
| 4 | `calculator/adjustments.py:478`（RPOの適用率: 保険0%、`Financial Services`→Fintech 50%） | sectorの文字列 | RPOの現在価値（理論株価に足される） | TANUKI（`core_calculator.py:480`） |
| 5 | `calculator/adjustments.py:380 _is_insurance`（industry・ticker 20本・sector） | 保険・マネージドヘルスケア（UNH・CVS等はsector=Healthcare） | 上の2・4で使う | TANUKI |
| 6 | `calculator/wacc.py:36`・`data_fetcher.py:104 SECTOR_DEFAULT_BETA["Financial Services"]=1.30` | βが取れないときの業種の既定 | WACC | TANUKI |
| 7 | `pipeline.py:1644`（`"Financial" in sector`。表示の「N/A」）・`:2489`（注記）・`:2846`（PSの乖離検出から金融を除く） | sectorの部分一致 | report.txtの表示・PSの確かめだけ | TANUKI（値に不使用） |
| 8 | `report_consistency_check.py:2800`（`"financial" in sector`、PSの乖離から除く） | 同上 | rccの検査の対象外にするだけ | rcc |
| 9 | `docs/value-monitor/tanuki_valuation/stock.html:897 FCF_LOW_RELIABILITY_SECTORS=['Financial Services','Utilities']` | sectorの文字列（`d.components.sector`） | 画面の「FCFの信頼性が低い」の表示 | 画面（TANUKI） |
| 10 | `sectors.yaml`・`cik_lookup.csv eps_sector`・`sector_master.eps_category`の`フィンテック / 銀行` | EPS ANALYZERの区分 | EPSの業種別の除外・成熟度 | EPS（段4で別に） |
| 11 | `sector_rotation.py`・`market_pulse_redesign.py`の`SECTOR_MAP`・`growth_sanity`・`maturity_config`のDamodaran名 | ETFの写像・業種のベンチマークの名前 | 金融かどうかの判定ではない | Market Pulse・TANUKI（対象外） |

登録銘柄（マスター98）でyfinanceが`Financial Services`なのは **MSCI・SOFI・V の3つだけ**（銀行・保険は登録に無い）。マスターの`financial`は SOFI=true（SIC 6199）、V・MSCI=false（SIC 7389、MSCIはさらに`Financial Data & Stock Exchanges`除外）。つまり寄せて値が変わりうるのは V・MSCI。マスターが`financial`でsectorが`Financial Services`でない銘柄は0、SICが6000〜6799でsectorが金融でない銘柄も0。

## 2. 各箇所を寄せるか、別の概念として残すか

| # | 案 | 理由 |
|---|---|---|
| 1 | **寄せる**: FCFEの対象＝マスターの`financial`がtrueで、保険（5）でないもの。SOFIだけは今と同じ | 手書きの1銘柄の一覧をやめる。JPM・GSの登録後に自動で対象になる（参考表示専用なので判定には効かない）。保険にFCFEを当てるのは今も想定外なので除く |
| 2 | **寄せる**: `sector=="Financial Services"`→マスターの`financial`（マスターに無い銘柄＝登録直後はsector_onlyで今と同じ）。保険のガード（5）はそのまま | 「負債が顧客の預り・準備金で混ざる」のは金融の性質で、SICで金融でないV・MSCIには当たらない（Vの短期負債$2.996Bは普通の借入。今は捨てている）。保険は業種・tickerの別の概念で、sectorがHealthcareのUNH等にも当たるので残す |
| 3 | **寄せる**: `RICE_EXCLUDED_SECTORS`の`Financial Services`だけマスターの`financial`に。`Insurance`・`Consumer Defensive`・`Consumer Cyclical`・`Real Estate`・`Utilities`はそのまま（金融の判定ではない） | 「投資強度の定義が成立しない」のは資産が重い金融で、Vはカード決済のネットワーク（R&D・CapExがある）。ただしRICEが出るようになる（下の3） |
| 4 | **寄せる**: Fintech 50%→`financial`がtrueのとき。V・MSCIはsector既定（0%、`Non-SaaS`）へ | RPO（受注残）をFintechとして半分見る前提は、金融機関の将来収益見通しの代理。決済・指数提供は金融機関ではない。保険0%はそのまま |
| 5 | **残す**（別の概念） | 保険は`financial`の部分集合ではない（UNH・CVS・CI等のマネージドヘルスケアを含む）。マスターに「保険」の区分は無く、段3に入れない |
| 6 | **残す**（別の概念） | 業種の既定のβ（11のsector別）で、金融かどうかの判定ではない。V・MSCI・SOFIは実測のβ（CAPM 0.771・1.237・2.332）を使っていて既定は使われない（試算でも触っていない）。βの既定を業種のマスターのDamodaranに寄せるのは別の件 |
| 7 | **寄せる**（表示・確かめだけ。値は動かない）: 3つとも`is_financial_ticker`を呼ぶ | 1か所にそろえる。後回しでもよい（値に効かない） |
| 8 | **寄せる**（7と同じ。rccが読むのはcomponents.sectorのまま、マスターの`financial`を読むようにする） | 同上 |
| 9 | **後で**: `d.components`にマスターの`financial`を載せてから画面を寄せる（latest.jsonの新しいキー＝段3とは別の計算方法ではない表示の変更）。Utilitiesの分は別の概念（残す） | 画面はsectorしか持たず、マスターを読むには出力にキーを足す必要がある。V・MSCIは「FCF信頼性低」が消える |
| 10・11 | **段3の対象外**（10は段4、11は金融の判定でない） | |

寄せる実装の形: `common/sector_master.py`に`is_financial_ticker(ticker, sector=None)`を1つ足す（`maturity_config.is_mega_tech_ticker`の前例。マスターに銘柄が無ければ`financial()`のsector_only相当）。

## 3. 試算（`tools/sim`の前後比較。コードは変えず、`--hook`で2・3・4を差し替え）

手順: `python tools/sim/sim.py chain fin_before|fin_after`（after は scratchpad `fin_hook.py`。2（`SECReader.get_net_cash`）・3（`calculate_rice`）・4（`adjust_rpo`）で、マスターの`financial`がfalseの銘柄のsectorを「Financial Services以外」に差し替える）。全96銘柄（TANUKI 97、HypeCore 96）。出力は`%TEMP%\finsim\`（`cmp_fin_before_fin_after.txt`）。

- 理論株価が変わるのは **V・MSCI の2銘柄だけ**。判定の変化 0。保有銘柄の変化 0（SOFIは完全に同じ）
- HypeCore: stage・値は同じ（V・MSCIの`price_iv_ratio`が1.292→1.298・1.646→1.647に動くだけ。IVが動くため）

| 銘柄 | 理論株価 | 判定 | 純現金（net_cash） | RICE | RPOの調整 |
|---|---|---|---|---|---|
| V | 327.44 → 325.83（−0.5%） | HOLD→HOLD（upside −22.6%→−23.0%） | −7.07B → −10.07B（短期負債0→$2.996B。金融ガード`fintech_st_debt_excluded`→none、ファイナンス・リース 0.0） | 計算不可（セクター除外）→ 計算可（base 4.106、Q 1.01、CF転換 1.56、価値創造係数 2.00、ROIC/WACC 4.91） | 適用率50%（Fintech）→0%（RPO/Revenue=0.12 < 0.3で元々未適用。理論株価に影響なし） |
| MSCI | 399.08 → 398.79（−0.1%） | TRIM→TRIM（upside −39.2%→−39.3%） | 変化なし（短期負債は元々0。ガードがnoneになっただけ、ファイナンス・リース 0.0） | 計算不可 → 計算可（base 1.483、Q 1.11、CF転換 0.62、価値創造係数 2.00、ROIC/WACC 4.46） | 50%（Fintech、rpo_pv $21.3M）→0%（適用なし）。これが理論株価−0.1%の主因 |
| SOFI | 変化なし（34.22） | WATCH→WATCH | 変化なし | 変化なし | 変化なし |

- V・MSCIの内訳: Vの理論株価−0.5%は純負債が約$3.0B増える（短期借入を捨てなくなる）効果。MSCIの−0.1%はRPOの現在価値（$21M）が消える効果
- STONKS: V・MSCIは`stonks_silo=false`で、STONKSの画面・出力の対象外。純現金の変化は出ない（SOFIは変化なし）。STONKS SILOの`get_net_cash`は同じ関数なので、将来V・MSCIがtrueになれば純負債が$3.0B大きく出る
- RICEが出るようになるのは見た目の変化（TANUKI SCOREに載る項目）。判定・理論株価の計算には使わない
- 前後比較の再現: `%TEMP%\finsim\fin_before`・`fin_after`、hookは scratchpad `fin_hook.py`（実装の段では`common/sector_master.py`の関数で置き換える）

## 4. `config/financial_institution_config.json`の今後

- **廃止にする**（1を寄せた後）。理由: 持っていた責務は「対象ティッカーの列挙」だけ（`_meta`にも計算式を持たないと書いてある）で、マスターの`financial`（保険を除く）で決まる。`fcfe.resolve_financial_institution_config_path`・`_load_financial_institution_config`・`__init__.py`のexport・関連テスト・`workflow_dependencies.json`等の参照は実装の段で一緒に消す
- 「JPM/GSは追加予定だが`cik_lookup.csv`未登録。登録後にこのリストへ追加」の注記の移し先: `[[SECTOR-MASTER-ADOPT-1]]`の本文（段3の行）に「JPM・GSを登録するとマスターの`financial`で自動的にFCFEの対象になる（`register_ticker`のStep 1.6がマスターを作る）。登録後、FCFEの結果とマスターの区分を目視する」と書く。コード・設定には残さない
- 廃止の副作用: FCFEの対象が「手で足す」から「マスターで決まる」に変わる。登録銘柄では SOFI 以外に金融がいないので、今の出力は変わらない

## 5. rccのWARNが209→211になった2件

`bb99b0ffb5`の全件を見る変更によるもの。変更の前（`bb99b0ffb5~1`のworktree）と今のrccの出力を並べた差分:
- 前にあって今無い: NG-INV-5 roll_suspect（直った分）
- 今増えた2件（どちらも WARN-INV-6「誰も読まないキー」、市場データ層）: `prev`・`reason_label`（`common/market_data/daily/{T}.json`）。日足のファイルは行のリストで、これらのキーは古い行（日足の行の書き込みの`MARKETDATA-DAILY-ROW-WRITE-1`で付く印・訂正の理由）にだけあるため、先頭50・末尾50の窓では見えていなかった。書き手はあり、読み手が無いキー（新しいバグではなく、窓で隠れていた既存のキー）。直すかは別の判断（読み手を作る／書くのをやめる）
- 合計は NG 1→0、WARN 209→211（差の+2はこの2件）。
