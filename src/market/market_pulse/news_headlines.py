"""Market Pulse 実装C（指示書㉗、2026-09-30）: 段階2のニュースの見出し。

設計書 MARKET_PULSE_REDESIGN.md 4章の推奨のとおり、米国はCNBC MarketsとGoogle News ビジネス（US）、日本はNHK 経済のRSSから、
見出し・時刻・出典のリンクだけを取る（本文・要約は使わない）。ニュースはAIの入力に入れない（確定した事実のJSONだけの方針）。
取得に失敗した配信元は推測で埋めず、failedに記録する（data_qualityと画面の「取得できず」に使う）。
"""
from __future__ import annotations

import calendar
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

SOURCES = [
    {"key": "cnbc_markets", "name": "CNBC Markets", "region": "米国",
     "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
    {"key": "google_business_us", "name": "Google News ビジネス（US）", "region": "米国",
     "url": "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB?hl=en-US&gl=US&ceid=US:en"},
    {"key": "nhk_business", "name": "NHK 経済", "region": "日本", "url": "https://www3.nhk.or.jp/rss/news/cat5.xml"},
]
MAX_ITEMS = 20
PER_SOURCE = 8
REGION_MAX = {"米国": 14, "日本": 6}   # 時刻の新しい順だけで並べると日本の見出しが押し出されるため、地域ごとの上限を置く


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


def fetch_headlines(fetch: Callable[[str], bytes] = _default_fetch, now: Optional[datetime] = None) -> Dict[str, Any]:
    """{"fetched_at": UTC, "items": [{title, published_utc, link, source, region}], "failed": [配信元の名前], "status"}。
    各配信元の新しい順にPER_SOURCE件、全体で時刻の新しい順にMAX_ITEMS件（同じ見出しは1件にまとめる）。"""
    import feedparser
    now = now or datetime.now(timezone.utc)
    items: List[Dict[str, Any]] = []
    failed: List[str] = []
    for s in SOURCES:
        try:
            feed = feedparser.parse(fetch(s["url"]))
            got = []
            for e in feed.entries:
                title = _collapse_ws(e.get("title") or "")
                link = (e.get("link") or "").strip()
                if not title or not link:
                    continue
                got.append({"title": title, "published_utc": _published_utc(e), "link": link, "source": s["name"],
                            "region": s["region"]})
            got.sort(key=lambda x: x["published_utc"] or "", reverse=True)
            if not got:
                failed.append(s["name"])
            items.extend(got[:PER_SOURCE])
        except Exception as ex:
            print(f"[WARN] ニュースの見出しの取得に失敗: {s['name']} ({type(ex).__name__}: {ex})")
            failed.append(s["name"])
    seen, uniq, per_region = set(), [], {}
    for it in sorted(items, key=lambda x: x["published_utc"] or "", reverse=True):
        k = it["title"].lower()
        if k in seen or per_region.get(it["region"], 0) >= REGION_MAX.get(it["region"], MAX_ITEMS):
            continue
        seen.add(k)
        per_region[it["region"]] = per_region.get(it["region"], 0) + 1
        uniq.append(it)
    status = "ok" if not failed else ("failed" if len(failed) == len(SOURCES) else "partial")
    return {"fetched_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "items": uniq[:MAX_ITEMS], "failed": failed, "status": status,
            "sources": [s["name"] for s in SOURCES]}
