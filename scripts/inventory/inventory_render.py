"""出力の一覧の書き出し（md）。json は build() の結果をそのまま書く（build_output_inventory.py の main）。

[[OUTPUT-INVENTORY-CHECK-1]]。今の段階の設定（NAME_SHORT 等）と解析の値は inventory_analysis から読む
（set_layer() が差し替えるため。R-A の終わりの見直し3、2026-10-23に分けた）。
"""
from __future__ import annotations

import posixpath
from collections import defaultdict
from typing import Dict, List, Tuple

import inventory_analysis as A


def md_escape(s: str) -> str:
    return s.replace("|", "\\|")


def render_md(inv: dict) -> str:
    L = []
    outs = inv["outputs"]
    files = [o for o in outs if o["group"] != "人が見る"]
    L.append(f"# {A.TITLE}")
    L.append("")
    L.append("[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。"
             f"機械が読む形は`{posixpath.basename(A.OUT_JSON)}`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。")
    L.append("")
    L.append("- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数")
    L.append("- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）")
    L.append("- 投資判断: 読み手（その場で計算を含む、テストを除く）に" + "・".join(f"`{p}`" for p, _ in A.DECISION_PREFIXES) + "があれば「効く」")
    L.append(f"- 残す／消す: {A.DECISION_NOTE}")
    L.append("")
    st = inv["stop_condition"]
    n_zero = sum(1 for o in files if o["group"] == "読み手0件")
    n_test = sum(1 for o in files if o["group"] == "テストだけ")
    n_dup = sum(1 for r in inv["rules"] if r["duplicated"])
    n_dup_layer = sum(1 for r in inv["rules"] if r["duplicated"] and r["in_layer"])
    L.append(f"**件数**: 出力 {len(outs)}（ファイル {len(files)}・人が見るもの {len(outs) - len(files)}）／読み手0件 {n_zero}／テストだけ {n_test}／"
             f"1件 {sum(1 for o in files if o['group'] == '1件')}／2件以上 {sum(1 for o in files if o['group'] == '2件以上')}／規則の二重 {n_dup}（検出式 {len(inv['rules'])}種のうち。{A.NAME_SHORT}のファイルを含むもの {n_dup_layer}）")
    if inv.get("screen"):
        sc = inv["screen"]
        by = defaultdict(int)
        for it in sc["items"]:
            by[it["status"]] += 1
        L.append("")
        L.append(f"**画面の項目**: 画面が読むキー {len(sc['items'])}（この段階の書き手あり {by['この段階の書き手あり']}・ほかの書き手あり {by['ほかの書き手あり']}・"
                 f"画面の中で作る値 {by['画面の中で作る値']}・**書き手なし {by['書き手なし']}**）／書き手があるのに誰も読まないキー **{len(sc['unread_keys'])}**")
    if inv.get("data_keys"):
        L.append("")
        L.append(f"**誰も読まないキー**: **{len(inv['data_keys']['unread_keys'])}**（この段階のJSONのキー {inv['data_keys']['written_key_count']} のうち。下の「誰も読まないキー」）")
    L.append("")
    L.append(f"**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない {len(st['outputs_with_writer_outside_layer'])}＋{len(st['unresolved_write_sites'])}件（{st['ratio_writer']:.1%}）、"
             f"読み方を決めきれない読み込み {len(st['python_read_sites_unresolved_in_files_mentioning_sec_data'])}件＋ファイル名で当たった行 {len(st.get('name_hit_sites_undetermined', []))}件"
             f"／解決できた読み込み {st['python_read_sites_into_layer_resolved']}件＋ファイル名で当たった行 {st.get('name_hit_sites_resolved', 0)}件（{st['ratio_reader']:.1%}）→ "
             + ("**当たる**" if st["hit"] else "当たらない"))
    L.append("")
    L.append("読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・"
             "ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。"
             "ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする")
    if st.get("name_hit_sites_undetermined"):
        L.append("")
        L.append("フォルダを決めきれない行（ファイル名で当たった行）:")
        for x in st["name_hit_sites_undetermined"]:
            L.append(f"- `{x['file']}` 行 {x['line']}（`{x['output']}`）: `{md_escape(x['text'])}`")
    L.append("")
    if inv["deleted"]:
        L.append("## 消したもの（段階2、" + {"HypeCore": "2026-10-21", "EPS": "2026-10-21", "STONKS": "2026-10-21",
                                            "市場データ層": "2026-10-22", "TAIL": "2026-10-22", "Portfolio": "2026-10-23",
                                            "マクロデータ層": "2026-10-23", "MACRO PULSE": "2026-10-23", "Market Pulse": "2026-10-23"}.get(A.NAME_SHORT, "2026-10-20") + "）")
        L.append("")
        for x in inv["deleted"]:
            L.append(f"- `{x['what']}`: {x['detail']}")
        L.append("")
    L.append("## 出力（読み手0件 → テストだけ → 1件 → 2件以上）")
    L.append("")
    L.append("| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for o in files:
        writers = "<br>".join(f"`{A.strip_layer(w['file'])}:{w['func']}`" for w in o["writers"])
        if o.get("writers_outside_layer"):
            writers += "<br>外: " + "<br>".join(f"`{w['file']}:{w['func']}`" for w in o["writers_outside_layer"])
        screens = "<br>".join(f"{s['page'].replace('docs/', '')}（{'直接' if s['how'] == '直接' else '経由'}）" for s in o["screens"]) or "—"
        rc = f"{o['reader_count']}"
        if o["reader_count_test"]:
            rc += f"・テスト{o['reader_count_test']}"
        if o["reader_count_compute"]:
            rc += f"（{o['reader_count_compute']}）"
        eff = "効く: " + "・".join(o["decision_effect"]) if o["decision_effect"] else "—"
        defs = "・".join(o["definitions"]) or "—"
        files_n = f"<br>（{o['files_tracked']}ファイル）" if o.get("files_tracked") is not None else ""
        L.append(f"| {o['group']} | `{md_escape(o['path'])}`{files_n} | {md_escape(o['note'])} | {writers} | {rc} | {screens} | {md_escape(eff)} | {defs} | {md_escape(o['decision'])} |")
    L.append("")
    L.append("### 人が見るもの（Step Summary・Discord" + ("・メール" if any(o["path"].startswith("メール（") for o in outs) else "") + "）")
    L.append("")
    L.append("| 出力 | 何か | 書き手 | 起動元のワークフロー |")
    L.append("|---|---|---|---|")
    for o in outs:
        if o["group"] != "人が見る":
            continue
        writers = "<br>".join(f"`{A.strip_layer(w['file'])}:{w['func']}`" for w in o["writers"])
        L.append(f"| `{md_escape(o['path'])}` | {md_escape(o['note'])} | {writers} | {'<br>'.join(o['launched_by']) or '—'} |")
    L.append("")
    L.append("## 読み手の内訳")
    L.append("")
    for o in files:
        L.append(f"### `{o['path']}`（{o['group']}）")
        if o["launched_by"]:
            L.append(f"- 起動元: {'・'.join(o['launched_by'])}")
        else:
            L.append("- 起動元: ワークフローから書き手を呼ぶ参照が見つからない")
        if o["top_keys"]:
            ks = "・".join(f"`{k['key']}`({k['reader_files']})" for k in o["top_keys"][:40])
            more = f" ほか{len(o['top_keys']) - 40}" if len(o["top_keys"]) > 40 else ""
            L.append(f"- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: {ks}{more}")
        groups: Dict[Tuple[str, str, str], List[dict]] = defaultdict(list)
        for r in o["readers"] + o["readers_compute"]:
            groups[(r["type"], r["how"], r["file"])].append(r)
        order = {"Python": 0, "JS・HTML": 1, "ワークフロー": 2, "テスト": 3}
        for (typ, how, f), rs in sorted(groups.items(), key=lambda kv: (order.get(kv[0][0], 9), kv[0][1], kv[0][2])):
            lines = "・".join(str(r["line"]) for r in rs[:12]) + (f" ほか{len(rs) - 12}" if len(rs) > 12 else "")
            vias = sorted({r["via"] for r in rs if r["via"] not in ("AST", "grep")})
            via = f"（経由 {'、'.join('`' + v + '`' for v in vias[:3])}{' ほか' if len(vias) > 3 else ''}）" if vias else ""
            L.append(f"- {typ}・{how}: `{f}` 行 {lines}{via}")
        for s in o["screens"]:
            if s["how"] != "直接":
                L.append(f"- 画面（{s['how']}）: `{s['page']}`")
        if not o["readers"] and not o["readers_compute"]:
            L.append("- 読み手なし")
        L.append("")
    if inv.get("data_keys"):
        L.append("## 誰も読まないキー")
        L.append("")
        L.append("この段階の出力のJSON（全ファイル、入れ子8段まで）のキーのうち、テストを除くコード（.py・.js・.html。書き手のファイルを含む）に"
                 "読みの形（`.key`・`['key']`〈代入でない〉・`.get('key')`）で出てこないもの。銘柄名・日付など小文字を含まないキーは除く。"
                 "Pythonで`for k in d`・`d.items()`のように回して読むものは拾えないので、消す前にキーの名前でgrepする")
        L.append("")
        if not inv["data_keys"]["unread_keys"]:
            L.append("なし")
        for u in inv["data_keys"]["unread_keys"]:
            L.append(f"- `{u['key']}`（{'、'.join('`' + s + '`' for s in u['sources'][:3])}）{(': ' + md_escape(u['note'])) if u.get('note') else ''}")
        L.append("")
    if inv.get("screen"):
        sc = inv["screen"]
        L.append("## 画面の項目")
        L.append("")
        scope = "全ファイル"
        L.append("画面（" + "・".join(f"`{p}`" for p in sc["pages"]) + "）のJSが読むキー（`.key`・`?.key`・`['key']`。"
                 "この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（"
                 f"{scope}、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。"
                 "画面の言葉は、その行と前の3行の日本語の文字列（推定）。")
        L.append("")
        L.append("### 書き手なし（画面が読むのに、どのJSONにも無いキー）")
        L.append("")
        for it in [i for i in sc["items"] if i["status"] == "書き手なし"]:
            L.append(f"- `{it['key']}`（`{it['page']}` 行 {'・'.join(str(x) for x in it['lines'][:6])}）{(': ' + md_escape(it['label'])) if it['label'] else ''}")
        L.append("")
        L.append("### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）")
        L.append("")
        if not sc["unread_keys"]:
            L.append("なし")
        for u in sc["unread_keys"]:
            L.append(f"- `{u['key']}`（{'、'.join('`' + s + '`' for s in u['sources'][:2])}）{(': ' + md_escape(u['note'])) if u.get('note') else ''}")
        L.append("")
        for pg in sc["pages"]:
            rows = [i for i in sc["items"] if i["page"] == pg]
            L.append(f"### `{pg}`（{len(rows)}項目）")
            L.append("")
            L.append("| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |")
            L.append("|---|---|---|---|---|")
            for it in rows:
                src = "<br>".join(f"`{s}`" for s in it["sources"][:2])
                L.append(f"| {md_escape(it['label']) or '—'} | `{it['key']}` | {it['status']}{('<br>' + src) if src else ''} | {'・'.join(str(x) for x in it['lines'][:4])} | {md_escape(it.get('decision', ''))} |")
            L.append("")
    if inv.get("screen", {}).get("sections") is not None:
        L.append("## 画面の区画")
        L.append("")
        L.append("HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後"
                 f"{A.NEAR_LINES}行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた"
                 "（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。")
        L.append("")
        for pg in sorted({x["page"] for x in inv["screen"]["sections"]}):
            L.append(f"### `{pg}`")
            L.append("")
            L.append("| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |")
            L.append("|---|---|---|")
            for x in [x for x in inv["screen"]["sections"] if x["page"] == pg]:
                ks = "<br>".join(f"`{k['key']}`（{k['status']}）" for k in x["keys"])
                L.append(f"| {md_escape(x['section'])} | {ks} | {md_escape(A.SECTION_DECISIONS.get(pg, ''))} |")
            L.append("")
    if inv.get("workflow_outputs"):
        L.append("## ワークフローが自分で書く出力（.pyの外）")
        L.append("")
        L.append("段階の.pyを動かすワークフローの中で、`$GITHUB_STEP_SUMMARY`に書く段。`.get()`で読むキーが、この段階のJSON（全ファイル）にあるか。")
        L.append("")
        for w in inv["workflow_outputs"]:
            L.append(f"- `{w['workflow']}`（行 {'・'.join(str(x) for x in w['lines'][:6])}）: "
                     + "・".join(f"`{k['key']}`（{'ある' if k['in_json'] else '**無い**'}）" for k in w["keys"]))
        L.append("")
    if inv.get("html_copies"):
        L.append("## 本番外のHTMLの複製の候補")
        L.append("")
        L.append("| ファイル | 行数 | 最後のcommit | 比べたページ（行数・複製の行のうち同じ行がページにある割合〈空行を除き、前後の空白を無視、difflibの順序つき一致〉・最後のcommit） |")
        L.append("|---|---|---|---|")
        for h in inv["html_copies"]:
            pages = "<br>".join(f"`{p['page']}`（{p['lines']}行・{p['same_lines']}行で{p['share_of_copy']:.0%}・{p['last_commit']}）" for p in h["pages"])
            L.append(f"| `{h['file']}` | {h['lines']} | {h['last_commit']} | {pages} |")
        L.append("")
    if inv.get("page_checks"):
        L.append("## 範囲外の確認（ページ）")
        L.append("")
        for c in inv["page_checks"]:
            L.append(f"### `{c['page']}`（{md_escape(c['title'])}、最後のcommit {c['last_commit']}）")
            L.append("")
            for r in c["reads"]:
                if "api_reads" in c and r["literal"].split("/")[0] in A.TOP_DIRS:
                    continue   # 下の GitHub の API の読みに出す
                w = "・".join(f"`{x}`" for x in r["writers"]) or "見つからない"
                L.append(f"- 読むJSON `{r['literal']}` → `{r['path']}`（{r['files']}ファイル）。書き手（.pyの書き込みの場所で照合）: {w}")
            if "api_reads" in c:
                L.append("- GitHub の API（contents）でリポジトリのファイルを読み書きするページ（パスはリポジトリの根から。0ファイルは、リポジトリに無いファイル）")
                for label, key in (("読む（fetchFile）", "api_reads"), ("書く（commitFile・複数ファイルの1commit）", "api_writes")):
                    L.append(f"  - {label}:")
                    for r in c[key]:
                        w = "・".join(f"`{x}`" for x in r["py_writers"]) or "なし"
                        L.append(f"    - `{r['path']}`（{r['files']}ファイル、行 {'・'.join(map(str, r['lines']))}）。.pyの書き手: {w}")
                L.append("  - 起動するワークフロー: " + ("・".join(f"`{x}`" for x in c["dispatches"]) or "なし"))
            L.append("- リンク・参照しているファイル: " + ("・".join(f"`{x}`" for x in c["linked_from"]) or "なし"))
            L.append("")
    L.append("## FINDINGS 2026-10-15 の読み手0件の6種（F3）")
    L.append("")
    L.append(f"| 名前 | {A.NAME_SHORT}か | この一覧の出力 | 書かれている場所（grep） |")
    L.append("|---|---|---|---|")
    for f in inv["f3_zero_reader"]:
        sites = "<br>".join(f"`{s['file']}:{s['line']}`" for s in f["sites"][:6]) + (f"<br>ほか{len(f['sites']) - 6}" if len(f["sites"]) > 6 else "")
        L.append(f"| `{f['name']}` | {'はい' if f['in_layer'] else 'いいえ（後の段階）'} | {('`' + f['output'] + '`') if f['output'] else '—'} | {sites or '—'} |")
    L.append("")
    L.append("## 規則が2か所以上にあるもの")
    L.append("")
    L.append(f"検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。{A.NAME_SHORT}のファイルを含むものだけでなく全部を示す。")
    L.append("")
    L.append(f"| 規則 | ファイル数 | 場所 | {A.NAME_SHORT}を含む |")
    L.append("|---|---|---|---|")
    for r in inv["rules"]:
        if not r["duplicated"]:
            continue
        sites = "<br>".join(f"`{s['file']}:{s['line']}`" for s in r["sites"])
        L.append(f"| {r['name']} | {len(r['files'])} | {sites} | {'はい' if r['in_layer'] else 'いいえ'} |")
    single = [r for r in inv["rules"] if not r["duplicated"]]
    if single:
        L.append("")
        L.append("1か所だけのもの: " + "・".join(f"{r['name']}（{len(r['files'])}）" for r in single))
    L.append("")
    np_dirs = A.NON_PRODUCTION_DIRS or sorted({x["dir"] for x in inv["non_production_code"]})
    L.append("## 本番外のコード（" + "・".join(np_dirs or A.LAYER_DIRS) + "）")
    L.append("")
    L.append("参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト")
    L.append("")
    if not A.NON_PRODUCTION_FILES:
        L.append("候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）")
        L.append("")
        if not inv["non_production_code"]:
            rows = [x for x in inv.get("non_production_scanned", []) if not x["file"].endswith("__init__.py")]
            L.append(f"候補は0件。見たファイル（{len(rows)}ファイル・{sum(x['lines'] for x in rows):,}行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:")
            L.append("")
            L.append("| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |")
            L.append("|---|---|---|---|---|---|")
            for x in rows:
                fmt = lambda xs: "<br>".join(f"`{v}`" for v in xs[:4]) + (f"<br>ほか{len(xs) - 4}" if len(xs) > 4 else "") or "—"  # noqa: E731
                L.append(f"| `{posixpath.basename(x['file'])}` | {x['lines']:,} | {fmt(x['workflows'])} | {fmt(x['code'])} | {fmt(x['same_dir'])} | {len(x['tests'])}件 |")
            L.append("")
    if A.NON_PRODUCTION_FILES:
        L.append("候補: 名前を挙げたもの（" + "・".join(f"`{posixpath.basename(f)}`" for f in A.NON_PRODUCTION_FILES) + "）と、段階のフォルダの.pyのうちワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの")
        L.append("")
    for d in np_dirs:
        rows = [x for x in inv["non_production_code"] if x["dir"] == d]
        total = sum(x["lines"] for x in rows)
        L.append(f"### `{d}`（{len(rows)}ファイル・{total:,}行）")
        L.append("")
        L.append("| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | " + ("実行の記録・注記 | " if A.NONPROD_NOTES else "") + "残す／消す／移す |")
        L.append("|---|---|---|---|---|---|" + ("---|" if A.NONPROD_NOTES else "") + "---|")
        for x in rows:
            fmt = lambda xs: "<br>".join(f"`{v}`" for v in xs) or "—"  # noqa: E731
            note = f"{md_escape(A.NONPROD_NOTES.get(x['file'], '')) or '—'} | " if A.NONPROD_NOTES else ""
            L.append(f"| `{x['file'].replace(d + '/', '')}` | {x['lines']:,} | {fmt(x['workflows'])} | {fmt(x['code'])} | {fmt(x['same_dir'])} | {fmt(x['tests'])} | {note}{md_escape(x['decision'])} |")
        L.append("")
    L.append("## 止める条件の内訳")
    L.append("")
    for k in ("outputs_with_writer_outside_layer", "unresolved_write_sites", "python_read_sites_unresolved_in_files_mentioning_sec_data"):
        items = st[k]
        L.append(f"- `{k}`: {len(items)}件")
        for it in items[:30]:
            L.append(f"  - {('`' + it + '`') if isinstance(it, str) else '`' + it['file'] + ':' + str(it['line']) + '` ' + it.get('func', '') + ' → `' + it['path'] + '`'}")
    L.append("")
    return "\n".join(L)
