"""Market Pulse 実装C（指示書㉗、2026-09-30）: 段階2のニュースの見出し。

設計書 MARKET_PULSE_REDESIGN.md 4章の推奨のとおり、米国はCNBC MarketsとGoogle News ビジネス（US）のRSSから、
見出し・時刻・出典のリンクだけを取る（本文・要約は使わない）。ニュースはAIの入力に入れない（確定した事実のJSONだけの方針）。
取得に失敗した配信元は推測で埋めず、failedに記録する（data_qualityと画面の「取得できず」に使う）。

2026-10-09（[[MARKETPULSE-HEADLINES-NHK-STALE-1]]・[[MARKETPULSE-HEADLINES-JA-1]]）:
- NHK 経済を外した（旧URLが2026-08-08の内容を200 OKで返し続け、2か月前の見出しがstatus=okで並んでいた。日本の配信元は置かない〈チャット側の決定〉）。
- 配信元の最新記事が取得時刻から72時間より古ければ、その配信元をfailedに入れる（理由はstale。failed_reasonsで取得失敗と区別する）。
- 見出しをGrokで日本語に訳してtitle_jaに入れる（translate_titles()、1日1回まとめて。失敗したらNone→原文だけを出す）。
  訳は画面の表示だけに使い、判定・AIの見解の入力には渡さない。
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
MAX_ITEMS = 20
PER_SOURCE = 8
REGION_MAX = {"米国": MAX_ITEMS}   # 地域ごとの上限（日本の枠はNHKを外したため無い）
STALE_HOURS = 72                  # 配信元の最新記事がこれより古ければ更新停止（stale）とみなす

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
    各配信元の新しい順にPER_SOURCE件、全体で時刻の新しい順にMAX_ITEMS件（同じ見出しは1件にまとめる）。
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
    seen, uniq, per_region = set(), [], {}
    for it in sorted(items, key=lambda x: x["published_utc"] or "", reverse=True):
        k = it["title"].lower()
        if k in seen or per_region.get(it["region"], 0) >= REGION_MAX.get(it["region"], MAX_ITEMS):
            continue
        seen.add(k)
        per_region[it["region"]] = per_region.get(it["region"], 0) + 1
        uniq.append(it)
    uniq = uniq[:MAX_ITEMS]
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
                     api_key: Optional[str] = None) -> Optional[List[str]]:
    """見出しの一覧をGrokで日本語に訳し、同じ順・同じ件数の訳の一覧を返す（1回の呼び出しでまとめて訳す）。
    キーが無い・呼び出しに失敗した・応答がJSONの文字列の配列でない・件数が合わないときはNone
    （訳と見出しの取り違えを防ぐため、一部だけは使わない）。
    渡すのは見出しの文字だけ（sec_ctrl_fetcher._translate_excerpt()と同じ形、temperature 0）。"""
    api_key = api_key if api_key is not None else os.getenv("XAI_API_KEY")
    if not titles or not api_key:
        return None
    prompt = (
        "以下は英語のニュースの見出しのJSON配列です。各見出しを自然で簡潔な日本語に訳してください。"
        "固有名詞・ティッカー・数値はそのまま正確に残してください。"
        f"出力は訳だけを同じ順に並べた、要素数{len(titles)}のJSONの文字列の配列だけにしてください（説明や前置きは不要）。\n\n"
        + json.dumps(titles, ensure_ascii=False)
    )
    payload = {"model": GROK_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0, "max_tokens": 3000}
    try:
        text = post(payload, api_key)["choices"][0]["message"]["content"].strip()
        m = re.search(r"\[.*\]", text, re.S)   # ```json ... ``` で囲まれて返る場合
        out = json.loads(m.group(0) if m else text)
    except Exception as e:
        print(f"[WARN] 見出しの翻訳に失敗: {type(e).__name__}: {e}")
        return None
    if not isinstance(out, list) or len(out) != len(titles) or not all(isinstance(x, str) and x.strip() for x in out):
        n = len(out) if isinstance(out, list) else type(out).__name__
        print(f"[WARN] 見出しの翻訳の件数・形式が合わない（{len(titles)}件に対して{n}）。訳は使わない")
        return None
    return [_collapse_ws(x) for x in out]


def add_title_ja(h: Dict[str, Any], translate: Callable[[List[str]], Optional[List[str]]] = translate_titles) -> Dict[str, Any]:
    """h["items"]の各要素にtitle_ja（訳せなかったものはNone）を加える。原文のtitleは残す。
    h["translation"]に結果（ok / failed / skipped）を記録する。例外は外に出さない（翻訳の失敗で毎晩の実行を止めない）。"""
    items = h.get("items") or []
    if not items:
        h["translation"] = {"status": "skipped"}
        return h
    try:
        ja = translate([x["title"] for x in items])
    except Exception as e:
        print(f"[WARN] 見出しの翻訳に失敗: {type(e).__name__}: {e}")
        ja = None
    if ja is None or len(ja) != len(items):
        ja = [None] * len(items)
    for x, t in zip(items, ja):
        x["title_ja"] = t
    h["translation"] = {"status": "ok" if any(ja) else "failed", "model": GROK_MODEL}
    return h
