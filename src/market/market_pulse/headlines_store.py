"""Market Pulse の見出しの日ごとの積み足し保存（[[MARKETPULSE-MOVE-POSTMORTEM-EPIC-1]] ①、2026-11-01）。

大きく動いた日の事後分析（③の材料H「前日の引け〜当日の引けの見出し」）のため、配信元の見出しを
`docs/market-monitor/market-pulse/data/headlines/YYYY-MM-DD.json` に、取得のたびに重複を除いて積み足す。
既存の`market_data.json`の`headlines`（配信元ごとに16件・画面は48時間）とは別で、そちらは変えない。

- 書き込みはこのファイルの accumulate() の1か所。2つの回から使う:
  - 6時間ごとの取得（`.github/workflows/Market_Pulse_Headlines.yml` → headlines_fetch.py、Grokは呼ばない）
  - 夜の Market Pulse（collect_and_send.py の compute_implc()。訳・判定はその回の結果を写す）
- 振り分け: 記事の published を米国東部（America/New_York、夏時間・冬時間はzoneinfo）の日付にしたもの。
  時刻の無い記事は取得した時刻の米国東部の日付のファイルに入れ、`published_missing: true` の印を付ける。
- 重複の除き方: 正規化したリンク（クエリの`.tsrc`〈Yahoo〉・`oc`〈Googleの中継〉・`utm_*`とフラグメントを外し、
  スキーム・ホストを小文字）→ 題名（空白をまとめて小文字。Googleの末尾の出典名は news_headlines が切った後）。
  2本目以降の配信元は`also_in`に残す。同じリンクで題名が変わったら`title_updates`に足す（最初の題名を残す）。
- 止まった配信元（stale）・取得に失敗した配信元の記事は積まない（news_headlines.fetch_headlines の all_items に入らない）。
- 新しい記事も変更も無い回は何も書かない（ワークフローは commit しない）。変更があった回は、取得した時刻の
  米国東部の日付のファイルの`fetches`に1行足す（System Health の [N] が最新の取得の時刻を見る）。
- 公開の範囲は market_data.json の headlines と同じ（題名・時刻・リンク・出典・訳・判定。本文・要約は持たない）。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
HEADLINES_DIR = os.path.join(REPO_ROOT, "docs", "market-monitor", "market-pulse", "data", "headlines")
ET = ZoneInfo("America/New_York")
DROP_QUERY_KEYS = {".tsrc", "oc"}        # Yahoo の .tsrc=rss（全件）、Google の中継リンクの oc=5（全件）。2026-11-01 の取得で確認
DROP_QUERY_PREFIXES = ("utm_",)
_WS = re.compile(r"\s+")
_UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"


def normalize_link(url: str) -> str:
    """重複を除くためのリンク。スキーム・ホストを小文字にし、DROP_QUERY_KEYS・utm_* のクエリとフラグメントを外す。
    ほかのクエリは順番も値も残す（記事を区別する値を消さない）。"""
    p = urlsplit((url or "").strip())
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
         if k not in DROP_QUERY_KEYS and not k.lower().startswith(DROP_QUERY_PREFIXES)]
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path, urlencode(q), ""))


def title_key(title: str) -> str:
    return _WS.sub(" ", title or "").strip().lower()


def item_id(link_norm: str) -> str:
    """材料Hで見出しを指すID（正規化したリンクから決まる）。"""
    return "h-" + hashlib.sha1(link_norm.encode("utf-8")).hexdigest()[:12]


def _parse_utc(s: Optional[str]) -> Optional[datetime]:
    try:
        return datetime.strptime(s, _UTC_FMT).replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def et_date(dt: datetime) -> str:
    return dt.astimezone(ET).strftime("%Y-%m-%d")


def _load_day(date_et: str, data_dir: str = HEADLINES_DIR) -> Dict[str, Any]:
    # パスは os.path.join(data_dir, f"{date_et}.json") を直接書く（出力の一覧のスクリプトが引数の既定値からパスを解く）
    if os.path.exists(os.path.join(data_dir, f"{date_et}.json")):
        with open(os.path.join(data_dir, f"{date_et}.json"), encoding="utf-8") as f:
            return json.load(f)
    return {"date_et": date_et, "fetches": [], "items": []}


def _sort_items(items: List[dict]) -> None:
    items.sort(key=lambda x: (x.get("published_utc") or "", x["id"]), reverse=True)


def accumulate(items: Iterable[Dict[str, Any]], data_dir: str = HEADLINES_DIR, *, fetched_at: str, run: str,
               sources: Dict[str, str], annotations: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    """見出しを日ごとのファイルに積み足す。

    items: news_headlines.fetch_headlines() の all_items（{title, published_utc, link, source, (publisher)}）。
    fetched_at: 取得の時刻（UTC、%Y-%m-%dT%H:%M:%SZ）。run: 回の名前（scheduled・evening）。
    sources: 配信元ごとの結果（ok、または news_headlines の failed_reasons の値）。fetches に残す。
    annotations: 正規化したリンク → {title_ja, market}（夜の回の訳・判定。Noneの値は写さない）。
    返り値: {"written": [書いたファイルの日付], "new": 新しい記事の数, "updated": 変えた記事の数, "by_date": {日付: 新しい数}}。
    新しい記事も変更も無ければ何も書かない。"""
    fetched = _parse_utc(fetched_at)
    if fetched is None:
        raise ValueError(f"fetched_at の形が違う: {fetched_at!r}")
    annotations = annotations or {}
    days: Dict[str, Dict[str, Any]] = {}
    changed: set = set()
    by_date: Dict[str, int] = {}
    n_new = n_upd = 0

    def day(d: str) -> Dict[str, Any]:
        if d not in days:
            days[d] = _load_day(d, data_dir)
        return days[d]

    for it in items:
        title = _WS.sub(" ", it.get("title") or "").strip()
        link = (it.get("link") or "").strip()
        if not title or not link:
            continue
        ln = normalize_link(link)
        pub = _parse_utc(it.get("published_utc"))
        d = et_date(pub or fetched)
        doc = day(d)
        ann = annotations.get(ln) or {}
        by_link = {x["link_norm"]: x for x in doc["items"]}
        by_title = {title_key(x["title"]): x for x in doc["items"]}
        cur = by_link.get(ln)
        upd = False
        if cur is None and title_key(title) in by_title:
            # 別のリンクで同じ題名（Googleの中継リンクとCNBC・Yahooの記事など）: 最初の記事に配信元を足す
            cur = by_title[title_key(title)]
            if it["source"] != cur["source"] and not any(a["source"] == it["source"] for a in cur.get("also_in", [])):
                cur.setdefault("also_in", []).append({"source": it["source"], "link": link})
                upd = True
        elif cur is not None:
            if it["source"] != cur["source"] and not any(a["source"] == it["source"] for a in cur.get("also_in", [])):
                cur.setdefault("also_in", []).append({"source": it["source"], "link": link})
                upd = True
            if title != cur["title"] and not any(u["title"] == title for u in cur.get("title_updates", [])):
                cur.setdefault("title_updates", []).append({"title": title, "seen_at": fetched_at})
                upd = True
        if cur is not None:
            for k in ("title_ja", "market"):
                if ann.get(k) is not None and cur.get(k) is None and cur["link_norm"] == ln:
                    cur[k] = ann[k]
                    upd = True
            if upd:
                n_upd += 1
                changed.add(d)
            continue
        new = {"id": item_id(ln), "title": title, "published_utc": it.get("published_utc") if pub else None,
               "link": link, "link_norm": ln, "source": it["source"], "first_seen": fetched_at}
        if it.get("publisher"):
            new["publisher"] = it["publisher"]
        if pub is None:
            new["published_missing"] = True
        for k in ("title_ja", "market"):
            if ann.get(k) is not None:
                new[k] = ann[k]
        doc["items"].append(new)
        n_new += 1
        by_date[d] = by_date.get(d, 0) + 1
        changed.add(d)

    if not changed:
        return {"written": [], "new": 0, "updated": 0, "by_date": {}}
    fd = et_date(fetched)
    day(fd)["fetches"].append({"fetched_at": fetched_at, "run": run, "sources": dict(sources),
                               "new": n_new, "updated": n_upd, "new_by_date": dict(sorted(by_date.items()))})
    changed.add(fd)
    os.makedirs(data_dir, exist_ok=True)
    for date_et in sorted(changed):
        doc = days[date_et]
        _sort_items(doc["items"])
        with open(os.path.join(data_dir, f"{date_et}.json"), "w", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(doc, ensure_ascii=False, indent=1) + "\n")
    return {"written": sorted(changed), "new": n_new, "updated": n_upd, "by_date": dict(sorted(by_date.items()))}


def sources_status(h: Dict[str, Any]) -> Dict[str, str]:
    """fetch_headlines() の結果から、配信元ごとの ok／失敗の理由。"""
    reasons = h.get("failed_reasons") or {}
    return {name: reasons.get(name, "ok") for name in h.get("sources") or []}


def annotations_from(h: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """annotate_headlines() の後の h["items"] から、正規化したリンク → {title_ja, market}。"""
    return {normalize_link(x["link"]): {"title_ja": x.get("title_ja"), "market": x.get("market")}
            for x in h.get("items") or [] if x.get("link")}


PENDING_NAME = "headlines_pending.json"   # GitHub Actions の一時のフォルダ（RUNNER_TEMP、同じ job の段で共有）に置く


def save_pending(payload: Dict[str, Any], path: Optional[str] = None) -> Optional[str]:
    """この回の accumulate の入力（items・訳と判定・取得の時刻・回・配信元の結果）を一時のファイルに残す。
    push が衝突したら headlines_push.py が、見出しのファイルをリモートの版に戻してからこれで積み足し直す。
    path を省くと ${RUNNER_TEMP}/headlines_pending.json。RUNNER_TEMP が無い（手元）なら残さない（None）。"""
    if not path and not os.environ.get("RUNNER_TEMP"):
        return None
    path = path or os.path.join(os.environ.get("RUNNER_TEMP", ""), "headlines_pending.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return path


def load_pending(path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not path and not os.environ.get("RUNNER_TEMP"):
        return None
    path = path or os.path.join(os.environ.get("RUNNER_TEMP", ""), "headlines_pending.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def store_from_fetch(h: Dict[str, Any], run: str, *, annotate: bool = False,
                     data_dir: str = HEADLINES_DIR, pending_path: Optional[str] = None) -> Dict[str, Any]:
    """fetch_headlines() の結果（all_items）を積み足す。annotate=True なら h["items"] の訳・判定を写す（夜の回）。
    pending_path（既定は ${RUNNER_TEMP}/headlines_pending.json、手元では残さない）に、この回の入力を残す（積み足しの前に書く）。"""
    payload = {"items": h.get("all_items") or [], "fetched_at": h["fetched_at"], "run": run,
               "sources": sources_status(h), "annotations": annotations_from(h) if annotate else None}
    save_pending(payload, pending_path)
    return accumulate(payload["items"], data_dir, fetched_at=payload["fetched_at"], run=run,
                      sources=payload["sources"], annotations=payload["annotations"])
