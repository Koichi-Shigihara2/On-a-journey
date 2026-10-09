"""Market Pulse 実装C（指示書㉗、2026-09-30）: 段階2のニュースの見出し。

設計書 MARKET_PULSE_REDESIGN.md 4章の推奨のとおり、米国はCNBC MarketsとGoogle News ビジネス（US）のRSSから、
見出し・時刻・出典のリンクだけを取る（本文・要約は使わない）。ニュースはAIの入力に入れない（確定した事実のJSONだけの方針）。
取得に失敗した配信元は推測で埋めず、failedに記録する（data_qualityと画面の「取得できず」に使う）。

2026-10-09（[[MARKETPULSE-HEADLINES-NHK-STALE-1]]・[[MARKETPULSE-HEADLINES-JA-1]]）:
- NHK 経済を外した（旧URLが2026-08-08の内容を200 OKで返し続け、2か月前の見出しがstatus=okで並んでいた。日本の配信元は置かない〈チャット側の決定〉）。
- 配信元の最新記事が取得時刻から72時間より古ければ、その配信元をfailedに入れる（理由はstale。failed_reasonsで取得失敗と区別する）。
- 見出しをGrokで日本語に訳してtitle_jaに入れる（translate_titles()、1日1回まとめて。失敗したらNone→原文だけを出す）。
  訳は画面の表示だけに使い、判定・AIの見解の入力には渡さない。

2026-10-09（[[MARKETPULSE-HEADLINES-RELEVANCE-1]]、方式B）:
- 各配信元から16件取り、翻訳と同じ1回の呼び出しで「市況と関係あり」（market: true/false）も返させる。データには全件残し、
  画面はtrueを最大MAX_ITEMS件出し、falseは「関係の薄い見出し N件」に折りたたむ（split_for_display()）。
- 判定が使えないとき（失敗・形が崩れた・全件false）は、全件を今までどおり出す。判定はAIの見解の入力には渡さない。
"""
from __future__ import annotations

import calendar
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

SOURCES = [
    {"key": "cnbc_markets", "name": "CNBC Markets", "region": "米国",
     "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
    {"key": "google_business_us", "name": "Google News ビジネス（US）", "region": "米国", "publisher_suffix": True,
     "url": "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB?hl=en-US&gl=US&ceid=US:en"},
]
MAX_ITEMS = 20                    # 画面の本表に出す件数の上限（split_for_display()）
PER_SOURCE = 16                   # 各配信元から取る件数（関係の判定で隠れる分を補うため、表示の上限より多く取る）
MAX_CANDIDATES = PER_SOURCE * len(SOURCES)   # データに残す件数の上限（判定の対象。地域の枠は米国だけなので置かない）
STALE_HOURS = 72                  # 配信元の最新記事がこれより古ければ更新停止（stale）とみなす
DISPLAY_MAX_AGE_HOURS = 48        # 画面（本表・折りたたみ）に出すのは取得時刻からこの時間以内の見出しだけ（[[MARKETPULSE-HEADLINES-AGE-1]]。
                                  # 実行は米国の取引日の後だけなので、週明けの実行でも前の取引日の夕方以降のニュースは入る）。
                                  # index.htmlのHEADLINES_MAX_AGE_HOURSと同じ値（テストで一致を確かめる）

GROK_URL = "https://api.x.ai/v1/chat/completions"
GROK_MODEL = "grok-4.3"


def _default_fetch(url: str) -> bytes:
    import requests
    r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.content


_WS = re.compile(r"[ \t\n\r\f]+")


def _collapse_ws(s: str) -> str:
    """連続した空白（スペース・タブ・改行）を1つにまとめる。画面（HTML）は表示時にまとめるため、保存する値も表示と同じにする。
    全角スペース（U+3000）はHTMLでもまとめられないので残す。"""
    return _WS.sub(" ", s).strip()


def _published_utc(entry) -> Optional[str]:
    st = entry.get("published_parsed") or entry.get("updated_parsed")
    if not st:
        return None
    return datetime.fromtimestamp(calendar.timegm(st), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def split_publisher(title: str, publisher: Optional[str] = None):
    """Google Newsの見出しの末尾「 - 出典名」を分ける。(見出し, 出典名 or None)。
    RSSの<source>に出典名があれば、末尾がそれと一致するときだけ分ける（見出しの中の「 - 」を切らないため）。
    <source>が無いときは最後の「 - 」で分ける。"""
    publisher = _collapse_ws(publisher or "") or None
    if publisher:
        suffix = " - " + publisher
        if title.endswith(suffix) and len(title) > len(suffix):
            return title[: -len(suffix)].rstrip(), publisher
        return title, publisher
    head, sep, tail = title.rpartition(" - ")
    if sep and head.strip() and tail.strip():
        return head.rstrip(), tail.strip()
    return title, None


def _is_stale(newest_utc: Optional[str], now: datetime) -> bool:
    """最新記事の時刻が取得時刻からSTALE_HOURS時間より古ければTrue（ちょうど72時間は古くない）。
    時刻が1件も無ければ鮮度を確かめられないのでTrue（推測で新しいとみなさない）。"""
    if not newest_utc:
        return True
    newest = datetime.strptime(newest_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return now - newest > timedelta(hours=STALE_HOURS)


def fetch_headlines(fetch: Callable[[str], bytes] = _default_fetch, now: Optional[datetime] = None) -> Dict[str, Any]:
    """{"fetched_at": UTC, "items": [{title, published_utc, link, source, region, (publisher)}], "failed": [配信元の名前],
    "failed_reasons": {配信元の名前: "fetch_error" | "no_items" | "stale"}, "status"}。
    各配信元の新しい順にPER_SOURCE件、全体で時刻の新しい順にMAX_CANDIDATES件（同じ見出しは1件にまとめる）。
    画面に出す件数（MAX_ITEMS）への絞り込みはsplit_for_display()で行う。
    最新記事が取得時刻からSTALE_HOURS時間より古い配信元は見出しを使わず、failedに入れる（推測で埋めない）。
    statusは、failedの配信元を除いて見出しが1件以上あればok（failedが無い）かpartial、0件ならfailed。"""
    import feedparser
    now = now or datetime.now(timezone.utc)
    items: List[Dict[str, Any]] = []
    failed: List[str] = []
    reasons: Dict[str, str] = {}
    for s in SOURCES:
        try:
            feed = feedparser.parse(fetch(s["url"]))
            got = []
            for e in feed.entries:
                title = _collapse_ws(e.get("title") or "")
                link = (e.get("link") or "").strip()
                if not title or not link:
                    continue
                it = {"title": title, "published_utc": _published_utc(e), "link": link, "source": s["name"], "region": s["region"]}
                if s.get("publisher_suffix"):
                    it["title"], it["publisher"] = split_publisher(title, (e.get("source") or {}).get("title"))
                got.append(it)
            got.sort(key=lambda x: x["published_utc"] or "", reverse=True)
            if not got:
                failed.append(s["name"])
                reasons[s["name"]] = "no_items"
            elif _is_stale(got[0]["published_utc"], now):
                print(f"[WARN] ニュースの見出しの配信元が更新停止（最新記事 {got[0]['published_utc']}）: {s['name']}")
                failed.append(s["name"])
                reasons[s["name"]] = "stale"
            else:
                items.extend(got[:PER_SOURCE])
        except Exception as ex:
            print(f"[WARN] ニュースの見出しの取得に失敗: {s['name']} ({type(ex).__name__}: {ex})")
            failed.append(s["name"])
            reasons[s["name"]] = "fetch_error"
    seen, uniq = set(), []
    for it in sorted(items, key=lambda x: x["published_utc"] or "", reverse=True):
        k = it["title"].lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)
    uniq = uniq[:MAX_CANDIDATES]
    status = "failed" if not uniq else ("ok" if not failed else "partial")
    return {"fetched_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "items": uniq, "failed": failed, "failed_reasons": reasons,
            "status": status, "sources": [s["name"] for s in SOURCES]}


def _default_post(payload: dict, api_key: str) -> dict:
    import requests
    r = requests.post(GROK_URL, json=payload, timeout=60,
                      headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})
    r.raise_for_status()
    return r.json()


def translate_titles(titles: List[str], post: Callable[[dict, str], dict] = _default_post,
                     api_key: Optional[str] = None) -> Optional[Dict[str, Optional[list]]]:
    """見出しの一覧をGrokに1回で渡し、日本語訳と「市況と関係あり」の判定を返す。{"ja": [str] or None, "market": [bool] or None}。
    渡すのは見出しの文字だけ（sec_ctrl_fetcher._translate_excerpt()と同じ形、temperature 0）。
    - キーが無い・呼び出しに失敗した・応答がJSONの配列でない・件数が合わない・finish_reasonがstop以外（途中で切れた）: None
      （訳も判定も全件使わない。訳と見出しの取り違えを防ぐため、一部だけは使わない）
    - jaが1件でも空・文字列でない: 訳だけ全件None。marketが1件でも真偽値でない: 判定だけ全件None（＝画面は全件を表示）"""
    api_key = api_key if api_key is not None else os.getenv("XAI_API_KEY")
    if not titles or not api_key:
        return None
    prompt = (
        "以下は英語のニュースの見出しのJSON配列です。各見出しについて、次の2つを返してください。\n"
        "- ja: 自然で簡潔な日本語訳（固有名詞・ティッカー・数値はそのまま正確に残す）\n"
        "- market: 米国の株式・債券・為替・商品の相場、金融政策・経済指標、上場企業の業績・株価・M&A・資金調達に関係する見出しならtrue。"
        "買い物のセール、スポーツ、事故、生活・健康の話題、相場や企業業績との関係が書かれていない政治・社会・技術の話題ならfalse。\n"
        f"出力は同じ順に並べた、要素数{len(titles)}のJSON配列だけにしてください。"
        '各要素は {"ja": 文字列, "market": true または false}（説明や前置きは不要）。\n\n'
        + json.dumps(titles, ensure_ascii=False)
    )
    payload = {"model": GROK_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0, "max_tokens": 8000}
    try:
        choice = post(payload, api_key)["choices"][0]
        finish = choice.get("finish_reason")
        text = (choice["message"]["content"] or "").strip()
    except Exception as e:
        print(f"[WARN] 見出しの翻訳・判定に失敗: {type(e).__name__}: {e}")
        return None
    if finish != "stop":
        print(f"[WARN] 見出しの翻訳・判定の応答が途中で切れた（finish_reason={finish}）。訳も判定も使わない")
        return None
    try:
        m = re.search(r"\[.*\]", text, re.S)   # ```json ... ``` で囲まれて返る場合
        out = json.loads(m.group(0) if m else text)
    except Exception as e:
        print(f"[WARN] 見出しの翻訳・判定の応答がJSONでない: {type(e).__name__}: {e}")
        return None
    if not isinstance(out, list) or len(out) != len(titles):
        n = len(out) if isinstance(out, list) else type(out).__name__
        print(f"[WARN] 見出しの翻訳・判定の件数が合わない（{len(titles)}件に対して{n}）。訳も判定も使わない")
        return None
    rows = [o if isinstance(o, dict) else {} for o in out]
    ja = [r.get("ja") for r in rows]
    market = [r.get("market") for r in rows]
    if all(isinstance(x, str) and x.strip() for x in ja):
        ja = [_collapse_ws(x) for x in ja]
    else:
        print("[WARN] 見出しの訳が欠けている・形が違う。訳は全件使わない")
        ja = None
    if not all(isinstance(x, bool) for x in market):
        print("[WARN] 「市況と関係あり」の判定が真偽値でない。判定は全件使わない（全件を表示）")
        market = None
    return {"ja": ja, "market": market}


def annotate_headlines(h: Dict[str, Any], translate: Callable[[List[str]], Optional[dict]] = translate_titles) -> Dict[str, Any]:
    """h["items"]の各要素にtitle_ja（訳せなかったものはNone）とmarket（true/false、判定なしはNone）を加える。原文のtitleと全件は残す。
    h["translation"]に訳の結果（ok / failed / skipped）、h["relevance"]に判定の結果（ok / unavailable / all_false）を記録する。
    全件falseのときは判定を使わない（all_false。画面は全件を表示）。例外は外に出さない（毎晩の実行を止めない）。"""
    items = h.get("items") or []
    if not items:
        h["translation"] = {"status": "skipped"}
        h["relevance"] = {"status": "unavailable"}
        return h
    try:
        r = translate([x["title"] for x in items])
    except Exception as e:
        print(f"[WARN] 見出しの翻訳・判定に失敗: {type(e).__name__}: {e}")
        r = None
    ja = (r or {}).get("ja")
    market = (r or {}).get("market")
    if ja is None or len(ja) != len(items):
        ja = [None] * len(items)
    if market is None or len(market) != len(items):
        market = [None] * len(items)
    for x, t, mk in zip(items, ja, market):
        x["title_ja"] = t
        x["market"] = mk
    h["translation"] = {"status": "ok" if any(ja) else "failed", "model": GROK_MODEL}
    if any(mk is None for mk in market):
        rel = "unavailable"
    elif not any(market):
        rel = "all_false"
    else:
        rel = "ok"
    h["relevance"] = {"status": rel, "model": GROK_MODEL, "true": sum(1 for mk in market if mk is True),
                      "false": sum(1 for mk in market if mk is False)}
    return h


def _parse_utc(s: Optional[str]) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def recent_items(h: Dict[str, Any]) -> List[Dict[str, Any]]:
    """取得時刻（fetched_at）からDISPLAY_MAX_AGE_HOURS時間以内（ちょうど48時間を含む）の見出し。データは変えない。
    記事の時刻か取得時刻が無いものは、新しいと確かめられないので含めない。"""
    fetched = _parse_utc(h.get("fetched_at"))
    if fetched is None:
        return []
    out = []
    for x in h.get("items") or []:
        pub = _parse_utc(x.get("published_utc"))
        if pub is not None and fetched - pub <= timedelta(hours=DISPLAY_MAX_AGE_HOURS):
            out.append(x)
    return out


def split_for_display(h: Dict[str, Any]):
    """画面の出し分け（index.htmlのsplitHeadlines()と同じ規則）。(本表の見出し, 折りたたむ見出し)。
    対象は取得時刻から48時間以内の見出しだけ（recent_items()。古いものは本表にも折りたたみにも入れない）。
    relevance.statusがokなら、market=falseを折りたたみ、それ以外（true）を新しい順に最大MAX_ITEMS件。
    ただし48時間以内にtrueが1件も無いときは判定を使わない。ok以外（判定なし・全件false・判定の無い古いエントリ）も同じで、
    48時間以内を新しい順に最大MAX_ITEMS件を本表に出し、折りたたみは無し。48時間以内が0件なら両方空（画面は「取得できず」）。"""
    items = recent_items(h)
    if (h.get("relevance") or {}).get("status") == "ok":
        main = [x for x in items if x.get("market") is not False]
        if main:
            return main[:MAX_ITEMS], [x for x in items if x.get("market") is False]
    return items[:MAX_ITEMS], []
