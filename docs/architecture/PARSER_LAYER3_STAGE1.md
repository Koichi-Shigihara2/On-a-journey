# PARSER-LAYER3-SINGLE-SOURCE-1 段1（定義の直しと守りの実装。手元の試算まで・公開しない）

2026-11-11。計算方法の変更 ID（公開するとき）: `PARSER-LAYER3-SINGLE-SOURCE-1-1`。STEP 0 は `PARSER_LAYER3_STAGE1_STEP0.md`。
表: `PARSER_LAYER3_VAL_GUARD_hits.csv`（守りの当たり全件と原本での判定）・`PARSER_LAYER3_STAGE1_readers.csv`（四半期BSの読み手）・`PARSER_LAYER3_STAGE1_movers.csv`（動く銘柄・必須6銘柄・保有9銘柄・ASTS/VZの理由）・`PARSER_LAYER3_STAGE1_cells.csv`（置き換えるセル。STEP 0の527セルの版）・`PARSER_LAYER3_STAGE1_longtermdebt.csv`・`PARSER_LAYER3_STAGE1_equity_nci.csv`。

## 1. 調べ（原本の申告で確かめた結果）
- (ii)「要確認」14期の判定は csv の「原本で確かめた判定」に記入。**規則を直した**: 最新の申告で消えた行の額が、**同じ期間の別の行に同額で残っている**ときは付け替えなので当てない（DELL 2期＝ソフト開発費16M・70Mが同額のまま `PaymentsToAcquireBusinessesNetOfCashAcquired` に、ONDS 2021Q1＝有形固定資産58,281が同額のまま `PaymentsToAcquireInProcessResearchAndDevelopment` に付け替わっている）。額がどの行にも残らず消えた期だけ内訳の揃った申告の値を採る。全期で当て直して **13期・3銘柄**（APP 7・IONQ 5・ONDS 1）。IONQ は2026-02の10-Kでソフト開発費4,558K（2023）が消え、「その他の投資活動」5,846K（＝ソフト4,558K＋無形1,288K）に合算された＝額が単独で残る行は無い
- (iii) BSY 2020Q4の自社株買いは**原本からは決まらない**: 2023-02の10-Kだけ `PaymentsForRepurchaseOfCommonStock` が0（2021・2022の10-Kは83.975M）。83.975Mはその10-Kのどのタグにも無い。値は変えず印のまま
- 四半期BSの読み手は `PARSER_LAYER3_STAGE1_readers.csv`。**Layer3 を直接読む**のは TANUKI の pipeline・TAIL・倍率の履歴・収益の段階・ROTCE・ttm（定義の直しは段1の置き換えを待たず効く）、**parser の四半期を読む**のは `SECReader.get_net_cash`（TANUKI・STONKS の現金とRunway）。screening・ROIC は年次（段1で変えない）。値が変わるセルを読むのは TANUKI（借入金24銘柄・capex 3銘柄）と倍率の履歴・TAIL（純資産5銘柄、first_val で読む）。HypeCore は BS を読まない

## 2. 実装
| 場所 | 内容 |
|---|---|
| `common/sec_data/val_guard.py`（新） | (i) 単位の誤り: val÷first_val が10の累乗（|k|≥3）で隣の期に first_val が近いとき first_val を採る（`val_guard="unit_pow10"`・`val_latest`）。持ち株数・1株は |k|≥3 だけ（分割は val が正）。(iii) 隣の期から外れる val は値を変えず `val_suspect`。`marks_from_store`＝読み手に渡す印（直近3年の期だけ） |
| `layer3_builder.py` | タグごとの正規化の直後（Q4の逆算の前）に (i)。最後に (iii)。`_equity_ex_nci`（非支配持分込みの純資産から `minority_interest` を引く。値が無い期は値を残して `mark`＝0で埋めない。`source_tag` を `…-MinorityInterest` に変え ROTCE が二重に引かない） |
| `cf_lines.py` | (ii) `period_values`: 消えた行の額が他の行に無いとき、内訳の揃った申告のうち最新を採る。`guard` を `capex_periods`→`fill_layer3_capex` の `val_guard` に渡す |
| `bs_lines.py` | `complete_debt_record`（借入金と分類される名前が `concepts` にあるのに `lines` に無い記録は使わない）・`prefer_face_debt`（完全な記録の貸借対照表の行を正にする。long_term_debt は常に、short_term_debt は**行がタグより大きいときだけ**。`debt_basis="bs_lines"`・`val_tag_val`。`LongTermDebt` と `…Noncurrent` が同じ時点にあれば合計の行を除く＝RKLB） |
| `config/sec_concept_definitions.json` | short_term_debt の候補の先頭を `DebtCurrent` に（値0の内訳が先に勝って1年内返済が落ちる ABBV 型を防ぐ） |
| TANUKI | `data_fetcher`→`core_calculator`→`latest.json` の `val_guard_marks`、`report.txt` に「Layer3の値の守り ⚠️」行（最大5件）、`stock.html` に注意の枠。MO で確認: 無形資産2024-06-30（3.6B、再表示でなく日付の取り違え）が `suspect:neighbor_outlier` で届く |
| テスト | `tests/test_layer3_val_guard_stage1.py`（9件）。出力の一覧（INV-5）は `build_output_inventory.py --layer tanuki`・`--layer sec` で作り直した |

### short_term_debt を「流動の行の合計」にしなかった理由（機械で確かめた）
貸借対照表の行（完全な記録）を正として、タグの規則が合うか全銘柄で確かめた（`short_term_debt` 1,052期）:
| 規則 | 一致 |
|---|---|
| 先頭の1つ（今） | 916期（87%） |
| 合計のタグ `DebtCurrent` を先頭に | 916期（同じ。`DebtCurrent` がある期は行とほぼ一致〈183期中179〉） |
| 重ならない流動のタグを合計 | 960期（91%） |

重なりの判定（行の値と部分集合の合計が一致するか）: **足さない組**（内訳が含まれる）= `LongTermDebtAndCapitalLeaseObligationsCurrent`と`LongTermDebtCurrent`（3期中0）・転換社債の各タグと`LongTermDebtCurrent`（17期中0）・`CommercialPaper`と`ShortTermBorrowings`（6期中0）・`OtherLongTermDebtCurrent`と他（0）。**足す組**=`CommercialPaper`と`LongTermDebtCurrent`（39期中30）。**タグだけでは決まらない組**=`ShortTermBorrowings`と`LongTermDebtCurrent`（65期中28期で足す、AMAT・AMDは足すと過大）。したがって規則は「貸借対照表の行が取れた期はその行（足し算済み）、無い期は合計のタグ→先頭の1つ」にした。合計のタグ規則は全銘柄で合うとは言えない。

## 3. 全銘柄の試算（`tools/sim`、TANUKI→HypeCore→TANUKI。前＝段0の公開後の HEAD、後＝定義の直し＋守り＋四半期BSの置き換え685セル・57銘柄）
| 見るもの | 結果 |
|---|---|
| 定義の直しと守りだけ（置き換えなし） | 理論株価の変化は APP −0.1% の1銘柄だけ（capex 2023・2024の0→4.2M・4.8M）。判定0 |
| 理論株価（置き換えも） | 変化10銘柄: **CAKE +29.1%**・KO −3.2%・ABBV −3.0%・WMT −1.8%・MO −1.4%・HWM −1.1%・ADSK −0.5%・ELF +0.3%・AAPL −0.1%・APP −0.1%。5%超は **CAKE の1銘柄**。残り86銘柄は ±0.0% |
| 判定 | 変化 **0** |
| 保有9銘柄 | APP −0.1% のほかは ±0.0%、判定の変化0 |
| 必須の6銘柄 | SOFI・TSLA・DDOG・HQY は ±0.0%、ABBV −3.0%・CAKE +29.1%（理由は movers.csv） |
| HypeCore stage | 変化 **0**（96銘柄の出力が過去の月まで生成時刻以外一致） |
| STONKS | 変化 **0**: 純現金が変わる9銘柄は STONKS の対象外で、現金は不変（Runwayは現金だけ） |
| 調整後EPS | 変化 **0**（BSを読まない） |
| `get_net_cash` | 9銘柄の借入金だけ変わる（AAPL・ABBV・ADSK・CAKE・ELF・HWM・KO・MO・WMT）。現金は不変 |
| ttm/・normalized/ | 実質の変更は4銘柄の ttm（APP・IONQ・ONDS の capex、HWM の1株配当）。丸めだけ192ファイルは戻した |
| scope（公開するとき） | 16銘柄（判定・funda・乖離率・理論株価・stage が変わる10銘柄＋表示が変わる6銘柄）。`sim.py scope --id PARSER-LAYER3-SINGLE-SOURCE-1-1 s1_base s1_full2 --root .sim_out/s1trial`（書き込みは公開のとき） |

### 決定(a)の確認の一覧（原本の申告。全件は `PARSER_LAYER3_STAGE1_movers.csv`）
| 銘柄 | 変化 | 原本の申告での理由 |
|---|---|---|
| CAKE | +29.1% | 2026-11-02の10-Q 2026-09-29: 長期借入金は流動分563.7Mの行だけ。parserが前年度末の非流動561.3Mを残して二重に数えていた。**Layer3が正** |
| KO | −3.2% | 2026-10-28の10-Q 2026-10-02: 1年内返済4.923B。parserの1.495Bは2025-12-31のコマーシャルペーパー（古い値）。**Layer3が正** |
| ABBV | −3.0% | 2026-11-03の10-Q 2026-09-30: 1年内返済8.673B。旧は `ShortTermBorrowings`＝0 が先に勝って落ちていた。**新が正** |
| WMT | −1.8% | 2026-08-28の10-Q: 短期借入10.479B＋1年内返済3.470B。旧は前者だけ。**新が正** |
| MO | −1.4% | 2026-10-29の10-Q: 1年内返済1.175B。旧は0。**新が正** |
| HWM | −1.1% | 短期借入390M＋1年内返済1M＝391M。旧は390M（parserは1M）。**新が正** |
| ADSK | −0.5% | NotesPayableCurrent 499M＋ShortTermBorrowings 994M＝1,493M。旧は片方ずつ。**新が正** |
| ELF | +0.3% | 非流動794.6M＋1年内返済30M。旧の長期826.7Mは合計のタグで短期30Mと二重。**新が正** |
| AAPL | −0.1% | コマーシャルペーパー1.997B＋1年内返済9.906B。旧は後者だけ。**新が正** |
| APP（保有） | −0.1% | 2026-02の10-Kに capex の行が無く0。2025-02の10-Kの有形固定資産4.246M・4.776M（2023・2024）を採る。**新が正** |
| ASTS・VZ | 0.0% | 純資産から非支配持分を引く（ASTS 2.398B→1.894B〈21%〉、VZ 107.29B→105.99B）。理論株価はBSの純資産を使わない |

## 4. 公開の前に（段1）
1. 公開は手順31（段0の定時の確認、期限11-16）が済んでから。**注意: このcommitをpushすると、2026-11-15（日）のSEC_Data_Updateが段1の定義で ttm/・normalized/ を作り直す**（APP・IONQ・ONDS・HWMの4ファイルが変わる）。手順31の①は「19銘柄のD&A」に加えてこの4ファイルの変化を想定内として確認する
2. 公開の手順: ttm/・normalized/ を `tools/sim/regen_derived.py` で作り直してcommit→`sim.py scope --write`→TANUKI→HypeCore→TANUKI
3. 取り込めなかったもの（FINDINGSに登録）: bs_lines の記録2,214件のうち328件（15%）は借入金の行が保存されていない古い記録で、貸借対照表の行を正にできない期（HON・ABBV・ELF等の一部）はタグのまま。取り直しにはSECの取得が要る
