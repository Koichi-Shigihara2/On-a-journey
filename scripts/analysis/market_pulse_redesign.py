"""Market Pulse再設計の設計書（docs/architecture/MARKET_PULSE_REDESIGN.md）の数値を再現するスクリプト。

指示書㉒・㉓（2026-09-26〜09-30）。設計書の数値はすべて本スクリプトの出力から転記している。

再現性のため、入力は固定する:
  - market_data.json と common/market_data/daily/ は、設計書作成時のcommit（DATA_REV）の内容を
    git archive / git show で読む（その後の日次更新・修復の影響を受けない）
  - daily/ に無い銘柄（^SOX・セクターETF11本）は yfinance から END_DATE までを取得する
    （取得結果はキャッシュディレクトリに保存し、2回目以降はそれを使う）
  - 日次の分析期間は 2021-01-04〜END_DATE

使い方:
  python scripts/analysis/market_pulse_redesign.py --out <出力JSON> [--cache <キャッシュディレクトリ>]

本番のコード・データは読むだけで変更しない。
"""
from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_REV = "dc0b548058"       # 設計書（指示書㉒）作成時のcommit
END_DATE = "2026-09-25"       # 日次データの最終日（この日を含む）
M7 = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"]
SECTORS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC"]
EXTRA = ["^SOX"] + SECTORS
SECTOR_MAP = {"Technology": "XLK", "Financial Services": "XLF", "Energy": "XLE", "Healthcare": "XLV",
              "Industrials": "XLI", "Consumer Cyclical": "XLY", "Consumer Defensive": "XLP", "Utilities": "XLU",
              "Basic Materials": "XLB", "Real Estate": "XLRE", "Communication Services": "XLC"}


def dist(labels):
    labels = [x for x in labels if x is not None]
    n = len(labels)
    return {"n": n, "share": {k: round(v / n * 100, 1) for k, v in Counter(labels).most_common()}} if n else {"n": 0, "share": {}}


# ── 入力（固定） ────────────────────────────────────────────
def load_inputs(cache):
    os.makedirs(cache, exist_ok=True)
    daily_dir = os.path.join(cache, f"daily_{DATA_REV}")
    if not os.path.isdir(os.path.join(daily_dir, "common")):
        raw = subprocess.run(["git", "-C", REPO, "archive", DATA_REV, "common/market_data/daily"],
                             capture_output=True, check=True).stdout
        tarfile.open(fileobj=io.BytesIO(raw)).extractall(daily_dir)
    md = json.loads(subprocess.run(["git", "-C", REPO, "show", f"{DATA_REV}:docs/market-monitor/market-pulse/data/market_data.json"],
                                   capture_output=True, check=True).stdout.decode("utf-8"))
    sp500 = json.loads(subprocess.run(["git", "-C", REPO, "show", f"{DATA_REV}:docs/market-monitor/market-pulse/data/sp500_tickers.json"],
                                      capture_output=True, check=True).stdout.decode("utf-8"))
    sp500 = sp500 if isinstance(sp500, list) else sp500.get("tickers", [])
    extra_path = os.path.join(cache, f"extra_{END_DATE}.pkl")
    if not os.path.exists(extra_path):
        import yfinance as yf
        end = (datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        raw = yf.download(EXTRA, start="2021-01-01", end=end, group_by="ticker", auto_adjust=False,
                          progress=False, threads=True)
        pd.DataFrame({s: raw[s]["Close"] for s in EXTRA}).to_pickle(extra_path)
    extra = pd.read_pickle(extra_path)
    extra.index = pd.to_datetime(extra.index).tz_localize(None)
    return os.path.join(daily_dir, "common", "market_data", "daily"), md, sp500, extra


def daily_close(daily_dir, sym):
    p = os.path.join(daily_dir, f"{sym}.json")
    if not os.path.exists(p):
        return None
    recs = json.load(open(p, encoding="utf-8"))["records"]
    s = pd.Series({r["date"]: r["close"] for r in recs
                   if isinstance(r.get("close"), (int, float)) and r["close"] > 0 and r["date"] <= END_DATE})
    s.index = pd.to_datetime(s.index)
    return s.sort_index()


# ── 段階ごとの判定ルール（設計書1章） ─────────────────────────
def s1_label(spx):
    if spx is None or pd.isna(spx): return None
    if spx >= 1.0: return "大幅高"
    if spx >= 0.3: return "上昇"
    if spx > -0.3: return "小動き"
    if spx > -1.0: return "下落"
    return "大幅安"


S2_THRESH = {"金利": 8.0, "原油": 3.0, "VIX": 10.0, "為替": 0.7}


def s2_label(tnx_bp, oil, vix, fx):
    vals = {"金利": tnx_bp, "原油": oil, "VIX": vix, "為替": fx}
    if all(v is None or pd.isna(v) for v in vals.values()):
        return None
    hit = [k for k, v in vals.items() if v is not None and not pd.isna(v) and abs(v) >= S2_THRESH[k]]
    return "同時に大きく動いたもの：" + ("・".join(hit) if hit else "なし")


def s2_category(label):
    if label is None: return None
    items = label.split("：", 1)[1]
    return items if "・" not in items else "2つ以上"


def s3_label(spx, adv, dec):
    if spx is None or pd.isna(spx) or adv is None or dec is None or (adv + dec) == 0:
        return None
    adr = adv / max(dec, 1)
    if spx >= 0.3:
        return "広がりのある上昇" if adr >= 1.5 else ("一部主導の上昇" if adr < 1.0 else "やや広がりのある上昇")
    if spx <= -0.3:
        return "広く売られた下落" if adr <= 0.67 else ("指数主導の下落" if adr > 1.0 else "やや広い下落")
    return "方向感なし"


def s4_label(risk, safe):
    if risk is None or safe is None or pd.isna(risk) or pd.isna(safe):
        return None
    if risk >= 0.3 and risk - safe >= 0.3: return "リスク資産へ"
    if safe >= 0.1 and risk <= -0.3: return "安全資産へ"
    if risk <= -0.3 and safe <= -0.1: return "全面安（現金化）"
    return "偏りなし"


def s5_label(gv):
    if gv is None or pd.isna(gv): return None
    if gv >= 0.5: return "グロース優勢"
    if gv <= -0.5: return "バリュー優勢"
    return "拮抗"


def s6_label_v1(sox_rel, m7_rel):
    """設計書初版（指示書㉒）のルール。正側は3文言、負側は1文言にまとめていた（非対称）。"""
    if sox_rel is None or m7_rel is None or pd.isna(sox_rel) or pd.isna(m7_rel): return None
    if sox_rel >= 1.0 and m7_rel >= 0.5: return "半導体・M7がけん引"
    if sox_rel >= 1.0: return "半導体がけん引"
    if m7_rel >= 0.5: return "M7がけん引"
    if sox_rel <= -1.0 or m7_rel <= -0.5: return "半導体・M7が重し"
    return "特定分野の突出なし"


def s6_label_sym(sox_rel, m7_rel):
    """修正版: 正負を対称にし、逆方向の日を分ける。"""
    if sox_rel is None or m7_rel is None or pd.isna(sox_rel) or pd.isna(m7_rel): return None
    su, sd = sox_rel >= 1.0, sox_rel <= -1.0
    mu, md_ = m7_rel >= 0.5, m7_rel <= -0.5
    if (su and md_) or (sd and mu): return "半導体とM7が逆方向"
    if su and mu: return "半導体・M7がけん引"
    if sd and md_: return "半導体・M7が重し"
    if su: return "半導体がけん引"
    if sd: return "半導体が重し"
    if mu: return "M7がけん引"
    if md_: return "M7が重し"
    return "特定分野の突出なし"


def weather_v3(s3, s4):
    if s3 is None or s4 is None: return None
    if s3 == "広く売られた下落" and s4 in ("安全資産へ", "全面安（現金化）"): return "嵐"
    if s3 in ("広がりのある上昇", "やや広がりのある上昇", "一部主導の上昇") and s4 != "安全資産へ": return "晴れ"
    return "曇り"


def weather_v1(s3, s4):
    if s3 is None or s4 is None: return None
    if s3 == "広く売られた下落" and s4 in ("安全資産へ", "全面安（現金化）"): return "嵐"
    if s3 in ("広がりのある上昇", "やや広がりのある上昇") and s4 in ("リスク資産へ", "偏りなし"): return "晴れ"
    return "曇り"


def weather_v4(s3, s4):
    if s3 is None or s4 is None: return None
    if s3 == "広く売られた下落" and s4 in ("安全資産へ", "全面安（現金化）"): return "嵐"
    if (s3 in ("広がりのある上昇", "やや広がりのある上昇", "一部主導の上昇") and s4 != "安全資産へ") or \
            (s3 == "方向感なし" and s4 == "リスク資産へ"): return "晴れ"
    return "曇り"


# ── 段階8: 基準率との差（先読みなし） ─────────────────────────
S8_MIN_N = 20
S8_DIFF_PT = 5.0
S8_Z = 1.96


def s8_labels(signal, close, horizon, z_gate=False):
    """日tについて、tの時点で結果が確定している過去の日（j ≤ t−horizon）だけを使い、
    同じシグナルの日の上昇割合と、全日（条件なし）の上昇割合＝基準率との差で判定する。"""
    fwd = (close.shift(-horizon) / close - 1) * 100
    up = (fwd > 0).astype(float).where(fwd.notna())
    sig = signal.tolist()
    labels, sig_flags, rows = [], [], []
    for i in range(len(sig)):
        if i < 260 or sig[i] is None:
            continue
        pool = up.iloc[: i - horizon + 1]
        pool = pool[pool.notna()]
        base = pool.mean()
        same = [up.iloc[j] for j in range(i - horizon + 1) if sig[j] == sig[i] and not np.isnan(up.iloc[j])]
        n = len(same)
        if n < S8_MIN_N:
            labels.append("件数不足")
            continue
        p = float(np.mean(same))
        diff = (p - base) * 100
        se = np.sqrt(base * (1 - base) / n) * 100
        z = diff / se if se else 0.0
        big = abs(diff) >= S8_DIFF_PT and (not z_gate or abs(z) >= S8_Z)
        lab = "平常時と差なし" if not big else "平常時より上昇が多い" if diff > 0 else "平常時より上昇が少ない"
        labels.append(lab)
        if lab != "平常時と差なし":
            sig_flags.append(abs(z) >= 1.96)
        rows.append((n, diff, z))
    ns = [r[0] for r in rows]
    return {"labels": dist(labels),
            "share_significant_among_diff_labels_pct": round(float(np.mean(sig_flags)) * 100, 1) if sig_flags else None,
            "n_median": float(np.median(ns)) if ns else None, "n_min": int(min(ns)) if ns else None, "n_max": int(max(ns)) if ns else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--cache", default=os.path.join(tempfile.gettempdir(), "mp_redesign_cache"))
    args = ap.parse_args()
    daily_dir, md, sp500, extra = load_inputs(args.cache)
    OUT = {"inputs": {"data_rev": DATA_REV, "end_date": END_DATE}}

    base = ["^GSPC", "^IXIC", "^TNX", "^VIX", "JPY=X", "CL=F", "GC=F", "SPY", "RSP", "HYG", "TLT", "GLD", "IVW", "IVE"] + M7
    C = pd.DataFrame({s: daily_close(daily_dir, s) for s in base})
    for s in EXTRA:
        C[s] = extra[s]
    C = C[C["^GSPC"].notna()].sort_index()
    R = C.pct_change(fill_method=None) * 100
    OUT["daily_period"] = [str(C.index[0].date()), str(C.index[-1].date()), int(len(C))]
    md = [e for e in md if e["date"][:10] <= "2026-09-26"]
    OUT["md_entries"] = [len(md), md[0]["date"][:10], md[-1]["date"][:10]]

    def ind(e, k, f="change_percent"):
        v = (e.get("indicators") or {}).get(k)
        return v.get(f) if isinstance(v, dict) else None

    def af(e, k):
        v = (e.get("asset_flow") or {}).get(k)
        return v.get("change_pct") if isinstance(v, dict) else None

    # 段階0（data_quality）
    import pandas_market_calendars as mcal
    cal = mcal.get_calendar("NYSE")

    def expected_close(iso):
        t = datetime.fromisoformat(iso).astimezone(timezone.utc)
        sched = cal.schedule(start_date=(t - timedelta(days=10)).date(), end_date=t.date())
        closed = sched[sched["market_close"] <= t]
        return closed.index[-1].strftime("%Y-%m-%d") if len(closed) else None

    def dq(e):
        exp = expected_close(e["date"])
        dates = {}
        for k in ["S&P500", "NASDAQ", "VIX指数", "米10年債", "ドル円", "WTI原油", "金（GOLD）"]:
            v = (e.get("indicators") or {}).get(k)
            if isinstance(v, dict) and v.get("date"):
                dates[k] = v["date"]
        for k, v in (e.get("asset_flow") or {}).items():
            if isinstance(v, dict) and v.get("date") and k != "short_bond":
                dates["af." + k] = v["date"]
        b = ((e.get("sentiment") or {}).get("breadth") or {}).get("date")
        if b:
            dates["breadth"] = b
        if not dates or exp is None:
            return "unknown"
        if dates.get("S&P500") is None or dates["S&P500"] < exp:
            return "stale"
        return "complete" if all(d >= exp for d in dates.values()) else "partial"

    q = [dq(e) for e in md]
    OUT["stage0_md"] = dist(q)
    OUT["stage0_md_since_0608"] = dist([x for x, e in zip(q, md) if e["date"][:10] >= "2026-06-08"])

    # 段階1
    OUT["stage1_md"] = dist([s1_label(ind(e, "S&P500")) for e in md])
    OUT["stage1_daily"] = dist([s1_label(x) for x in R["^GSPC"].iloc[1:]])
    tnx_bp = C["^TNX"].diff() * 100
    m7 = R[M7].mean(axis=1)
    tags = {
        "NASDAQ優位": (R["^IXIC"] - R["^GSPC"]) >= 0.5, "NASDAQ劣後": (R["^IXIC"] - R["^GSPC"]) <= -0.5,
        "SOX ±2%以上": R["^SOX"].abs() >= 2, "M7均等 vs S&P ±1pt以上": (m7 - R["^GSPC"]).abs() >= 1,
        "VIX ±10%以上": R["^VIX"].abs() >= 10, "10年債 ±8bp以上": tnx_bp.abs() >= 8,
        "ドル円 ±0.7%以上": R["JPY=X"].abs() >= 0.7, "原油 ±3%以上": R["CL=F"].abs() >= 3, "金 ±1.5%以上": R["GC=F"].abs() >= 1.5,
    }
    OUT["stage1_tags_daily"] = {k: round(v.iloc[1:].mean() * 100, 1) for k, v in tags.items()}

    # 段階2（文言:「同時に大きく動いたもの：…」）
    s2d = [s2_label(tnx_bp.iloc[i], R["CL=F"].iloc[i], R["^VIX"].iloc[i], R["JPY=X"].iloc[i]) for i in range(1, len(C))]
    OUT["stage2_daily"] = dist([s2_category(x) for x in s2d])

    def md_tnx(e):
        v = (e.get("indicators") or {}).get("米10年債")
        return v["change"] * 100 if isinstance(v, dict) and v.get("change") is not None else None

    OUT["stage2_md"] = dist([s2_category(s2_label(md_tnx(e), ind(e, "WTI原油"), ind(e, "VIX指数"), ind(e, "ドル円"))) for e in md])

    # 段階3（md・5年）
    s3_md = []
    for e in md:
        b = (e.get("sentiment") or {}).get("breadth") or {}
        s3_md.append(s3_label(ind(e, "S&P500"), b.get("advances"), b.get("declines")))
    OUT["stage3_md"] = dist(s3_md)
    rets = {}
    for t in sp500:
        s = daily_close(daily_dir, t)
        if s is not None and len(s) > 100:
            rets[t] = s.pct_change(fill_method=None)
    RR = pd.DataFrame(rets).reindex(C.index)
    adv, dec = (RR > 0.0001).sum(axis=1), (RR < -0.0001).sum(axis=1)
    OUT["stage3_daily"] = dist([s3_label(R["^GSPC"].iloc[i], adv.iloc[i], dec.iloc[i]) for i in range(1, len(C))])

    # 段階4
    risk_d, safe_d = R[["SPY", "HYG"]].mean(axis=1), R[["TLT", "GLD"]].mean(axis=1)
    OUT["stage4_daily"] = dist([s4_label(risk_d.iloc[i], safe_d.iloc[i]) for i in range(1, len(C))])
    s4_md = []
    for e in md:
        r, s = [af(e, "equity"), af(e, "hy_bond")], [af(e, "long_bond"), af(e, "gold")]
        s4_md.append(None if None in r or None in s else s4_label(sum(r) / 2, sum(s) / 2))
    OUT["stage4_md"] = dist(s4_md)

    # 段階5
    OUT["stage5_daily"] = dist([s5_label(x) for x in (R["IVW"] - R["IVE"]).iloc[1:]])
    OUT["stage5_md"] = dist([s5_label((((e.get("indicators") or {}).get("グロース対バリュー比") or {}).get("diff_percent"))) for e in md])

    # 段階6: 初版の非対称の確認と修正版
    sox_spx, m7_spx = R["^SOX"] - R["^GSPC"], m7 - R["^GSPC"]
    sox_rsp, m7_rsp = R["^SOX"] - R["RSP"], m7 - R["RSP"]
    rng = range(1, len(C))
    OUT["stage6_investigation"] = {
        "sox_minus_spx": {"mean": round(float(sox_spx.mean()), 3), "sd": round(float(sox_spx.std()), 3),
                          "ge_+1_pct": round(float((sox_spx >= 1).mean()) * 100, 1), "le_-1_pct": round(float((sox_spx <= -1).mean()) * 100, 1)},
        "m7_minus_spx": {"mean": round(float(m7_spx.mean()), 3), "sd": round(float(m7_spx.std()), 3),
                         "ge_+0.5_pct": round(float((m7_spx >= 0.5).mean()) * 100, 1), "le_-0.5_pct": round(float((m7_spx <= -0.5).mean()) * 100, 1)},
        "m7_minus_rsp": {"mean": round(float(m7_rsp.mean()), 3), "sd": round(float(m7_rsp.std()), 3),
                         "ge_+0.5_pct": round(float((m7_rsp >= 0.5).mean()) * 100, 1), "le_-0.5_pct": round(float((m7_rsp <= -0.5).mean()) * 100, 1)},
        "sox_minus_rsp": {"mean": round(float(sox_rsp.mean()), 3), "sd": round(float(sox_rsp.std()), 3),
                          "ge_+1_pct": round(float((sox_rsp >= 1).mean()) * 100, 1), "le_-1_pct": round(float((sox_rsp <= -1).mean()) * 100, 1)},
        "mixed_direction_pct": round(float((((sox_spx >= 1) & (m7_spx <= -0.5)) | ((sox_spx <= -1) & (m7_spx >= 0.5))).mean()) * 100, 1),
    }
    OUT["stage6_v1_vs_spx"] = dist([s6_label_v1(sox_spx.iloc[i], m7_spx.iloc[i]) for i in rng])
    OUT["stage6_sym_vs_spx"] = dist([s6_label_sym(sox_spx.iloc[i], m7_spx.iloc[i]) for i in rng])
    OUT["stage6_sym_vs_rsp"] = dist([s6_label_sym(sox_rsp.iloc[i], m7_rsp.iloc[i]) for i in rng])

    # セクター4象限（週次）
    W = C[SECTORS + ["SPY"]].resample("W-FRI").last().dropna()
    quad = {}
    for n in (4, 13):
        for lag in (1, 4):
            rs = W[SECTORS].div(W["SPY"], axis=0)
            ratio = rs / rs.rolling(n).mean() * 100
            mom = ratio / ratio.shift(lag) * 100
            qq = pd.DataFrame(np.where(ratio >= 100, np.where(mom >= 100, "Strong", "Weakening"),
                                       np.where(mom >= 100, "Improving", "Weak")), index=ratio.index, columns=SECTORS)
            qq = qq.where(ratio.notna() & mom.notna()).dropna(how="all")
            chg, runs = [], []
            for s in SECTORS:
                seq = qq[s].dropna().tolist()
                chg.append(sum(a != b for a, b in zip(seq, seq[1:])) / max(len(seq) - 1, 1))
                run = 1
                for a, b in zip(seq, seq[1:]):
                    if a == b: run += 1
                    else: runs.append(run); run = 1
                runs.append(run)
            fwd = W[SECTORS].pct_change(4).shift(-4).sub(W["SPY"].pct_change(4).shift(-4), axis=0) * 100
            good, bad = [], []
            for s in SECTORS:
                for d in qq.index:
                    v, f = qq.at[d, s], fwd.at[d, s]
                    if isinstance(v, str) and not pd.isna(f):
                        (good if v in ("Strong", "Improving") else bad).append(f)
            quad[f"ratio{n}w_mom{lag}w"] = {
                "weekly_change_rate_pct": round(float(np.mean(chg)) * 100, 1), "median_run_weeks": float(np.median(runs)),
                "mean_run_weeks": round(float(np.mean(runs)), 2),
                "fwd4w_excess_strong_improving": round(float(np.mean(good)), 3), "fwd4w_excess_weak_weakening": round(float(np.mean(bad)), 3),
                "latest": qq.iloc[-1].to_dict(), "latest_mom": {s: round(float(mom.iloc[-1][s]), 2) for s in SECTORS},
            }
    OUT["sector_quadrants"] = quad

    # 段階7: 監視銘柄（保有＋TAIL監視）。結論は上位2銘柄の名前と残りの件数
    pf = json.loads(subprocess.run(["git", "-C", REPO, "show", f"{DATA_REV}:docs/portfolio/data/portfolio.json"],
                                   capture_output=True, check=True).stdout.decode("utf-8"))
    held = sorted({t for b in pf["brokers"].values() for t in (b.get("positions") or {})})
    sys.path.insert(0, REPO)
    try:
        from src.tail.edgar_rss_monitor import get_monitored_tickers
        tail = sorted(set(get_monitored_tickers()))
    except Exception:
        tail = []
    from common.market_data.reader import get_attributes
    watch = sorted(set(held) | set(tail))
    wsec = {t: SECTOR_MAP.get((get_attributes(t) or {}).get("sector")) for t in watch}
    rec = quad["ratio13w_mom4w"]
    moms = rec["latest_mom"]
    strong_secs = sorted([s for s in SECTORS if moms[s] >= 100], key=lambda s: -moms[s])
    order = [t for s in strong_secs for t in sorted(t for t, ws in wsec.items() if ws == s)]
    OUT["stage7"] = {"watch": watch, "sector": wsec, "strong_or_improving_sectors_by_momentum": strong_secs,
                     "example_conclusion": (f"資金が向かっているセクターにいる監視銘柄：{'・'.join(order[:2])}"
                                            + (f" ほか{len(order) - 2}銘柄" if len(order) > 2 else "")) if order
                     else "資金が向かっているセクターにいる監視銘柄：なし"}

    # 段階8
    sig1 = pd.Series([s1_label(x) for x in R["^GSPC"]], index=C.index)
    vz = pd.Series(["VIX<15" if v < 15 else "VIX15-20" if v < 20 else "VIX20-30" if v < 30 else "VIX>=30" for v in C["^VIX"]], index=C.index)
    combo = pd.Series([f"{a}|{b}" if a else None for a, b in zip(sig1, vz)], index=C.index)
    OUT["stage8"] = {}
    for name, sig in (("段階1の区分", sig1), ("段階1×VIX水準", combo)):
        for h in (1, 5):
            OUT["stage8"][f"{name}・{h}営業日後・差5pt"] = s8_labels(sig, C["^GSPC"], h)
            OUT["stage8"][f"{name}・{h}営業日後・差5pt且つz1.96"] = s8_labels(sig, C["^GSPC"], h, z_gate=True)
    # 例: 最新日のシグナル（全期間の同じシグナルの日と、全日の基準率）
    OUT["stage8_example"] = {}
    for name, sig in (("段階1の区分", sig1), ("段階1×VIX水準", combo)):
        today = sig.iloc[-1]
        ex = {"date": str(C.index[-1].date()), "signal": today}
        for h in (1, 5, 20):
            fwd = (C["^GSPC"].shift(-h) / C["^GSPC"] - 1) * 100
            same = [fwd.iloc[j] for j in range(len(sig) - h) if sig.iloc[j] == today and not np.isnan(fwd.iloc[j])]
            allv = fwd.iloc[: len(sig) - h].dropna()
            p, b = float(np.mean([x > 0 for x in same])), float((allv > 0).mean())
            ex[f"{h}d"] = {"n": len(same), "up_pct": round(p * 100, 1), "base_up_pct": round(b * 100, 1),
                           "diff_pt": round((p - b) * 100, 1), "z": round((p - b) / np.sqrt(b * (1 - b) / len(same)), 2),
                           "mean_pct": round(float(np.mean(same)), 3)}
        OUT["stage8_example"][name] = ex

    # 天気
    rows = []
    for e, a3, a4 in zip(md, s3_md, s4_md):
        ai = {"曇": "曇り", "晴": "晴れ"}.get(e.get("judgment"), e.get("judgment"))
        rows.append((ai, a3, a4))
    wout = {}
    for nm, fn in (("v1", weather_v1), ("v3", weather_v3), ("v4", weather_v4)):
        pairs = [(ai, fn(a3, a4)) for ai, a3, a4 in rows if fn(a3, a4) is not None and ai in ("晴れ", "曇り", "嵐")]
        wout[nm] = {"n": len(pairs), "agree_pct": round(float(np.mean([a == b for a, b in pairs])) * 100, 1),
                    "rule_dist": dist([b for _, b in pairs])["share"],
                    "confusion": {f"AI {a} → ルール {b}": n for (a, b), n in Counter(pairs).items()}}
    wout["ai_dist"] = dist([ai for ai, a3, a4 in rows if weather_v3(a3, a4) is not None and ai in ("晴れ", "曇り", "嵐")])["share"]
    OUT["weather"] = wout

    # 新高値・Hindenburg（MP-08・18）
    P = pd.DataFrame({t: daily_close(daily_dir, t) for t in sp500}).reindex(C.index)
    hi_prev, lo_prev = P.rolling(252, min_periods=200).max().shift(1), P.rolling(252, min_periods=200).min().shift(1)
    hi_in, lo_in = P.rolling(252, min_periods=200).max(), P.rolling(252, min_periods=200).min()
    nh_l, nl_l = (P >= hi_in * 0.99).sum(axis=1), (P <= lo_in * 1.01).sum(axis=1)
    nh_s, nl_s = (P > hi_prev).sum(axis=1), (P < lo_prev).sum(axis=1)
    thr = P.notna().sum(axis=1) * 0.022
    valid = hi_prev.notna().sum(axis=1) > 400
    loose = (nh_l >= thr) & (nl_l >= thr)
    strict = (nh_s >= thr) & (nl_s >= thr)
    net = (P.pct_change(fill_method=None) > 0).sum(axis=1) - (P.pct_change(fill_method=None) < 0).sum(axis=1)
    mco = net.ewm(span=19, adjust=False).mean() - net.ewm(span=39, adjust=False).mean()
    standard = strict & (C["^GSPC"] > C["^GSPC"].shift(50)) & (mco < 0) & (nh_s <= 2 * nl_s)
    OUT["hindenburg"] = {"days": int(valid.sum()),
                         "nh_mean_loose": round(float(nh_l[valid].mean()), 1), "nh_mean_strict": round(float(nh_s[valid].mean()), 1),
                         "nl_mean_loose": round(float(nl_l[valid].mean()), 1), "nl_mean_strict": round(float(nl_s[valid].mean()), 1),
                         "loose_pct": round(float(loose[valid].mean()) * 100, 1), "strict_nhnl_only_pct": round(float(strict[valid].mean()) * 100, 1),
                         "standard_pct": round(float(standard[valid].mean()) * 100, 1),
                         "latest": {"loose": bool(loose.iloc[-1]), "standard": bool(standard.iloc[-1])}}

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(OUT, f, ensure_ascii=False, indent=1, default=str)
    print(f"saved: {args.out}")


if __name__ == "__main__":
    main()
