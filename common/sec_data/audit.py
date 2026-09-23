"""
common/sec_data/audit.py
SECデータ品質監査スクリプト

使用方法:
    python common/sec_data/audit.py              # 全銘柄監査（SECデータのみ）
    python common/sec_data/audit.py AVGO BKNG   # 特定銘柄のみ
    python common/sec_data/audit.py --check-beta # β乖離チェックも実施

終了コード:
    0 = 問題なし または 軽微な問題のみ
    1 = 重大問題あり（NI/OCF 全件 None）

Discord通知:
    環境変数 DISCORD_WEB_HOOK が設定されている場合に自動送信
"""

import json
import os
import sys
import glob
from datetime import datetime


TTM_DIR  = os.path.join(os.path.dirname(__file__), "ttm")
SEC_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "docs",
                        "value-monitor", "tanuki_valuation", "data")
EPS_DIR  = os.path.join(os.path.dirname(__file__), "..", "..", "docs",
                        "value-monitor", "adjusted_eps_analyzer", "data")
BETA_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                                "config", "beta_config.json")

# common/ をimportできるようrepo rootをsys.pathに追加
# （beta_fetcher.py・pipeline.py等の既存切替と同型のHAS_MARKET_DATA
# ガードパターンを踏襲）
_REPO_ROOT_FOR_IMPORT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT_FOR_IMPORT not in sys.path:
    sys.path.insert(0, _REPO_ROOT_FOR_IMPORT)

# [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序5: β乖離監査・カナダ企業判定を
# yfinance直接呼び出しからcommon.market_data.reader経由に切替
# （設計確定事項6「audit.pyとの役割分担」通り）。取得の実体は
# fetcher.py::fetch_weekly_attributes()に一本化済み（Market_Data_Weekly_
# Update.ymlが毎週日曜に生成）。
try:
    from common.market_data.reader import get_attributes as _md_get_attributes
    HAS_MARKET_DATA = True
except Exception:
    _md_get_attributes = None
    HAS_MARKET_DATA = False


def get_registered_tickers() -> list[str]:
    path = os.path.join(DATA_DIR, "tickers.json")
    if not os.path.exists(path):
        return []
    return json.load(open(path, encoding="utf-8")).get("tickers", [])


def audit_ticker(ticker: str) -> dict:
    """1銘柄の監査。返り値: {ticker, critical: [], warning: []}"""
    result = {"ticker": ticker, "critical": [], "warning": []}

    # カナダ企業チェック（IFRS/40-F）— 早期リターン前に必ず実行
    # [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序5でreader.get_attributes()
    # 経由に切替（2026-08-11）。attributes/未生成銘柄（fetcher.py未実行）は
    # Noneが返り、旧yfinance直接呼び出し失敗時と同じ「判定スキップ」の
    # 中立デフォルト挙動になる。
    try:
        if HAS_MARKET_DATA:
            _attrs = _md_get_attributes(ticker)
            if _attrs and _attrs.get("country") == "Canada":
                result["warning"].append(
                    "カナダ企業（IFRS/40-F）: TANUKI VALUATION・EPS非対応。登録前に要確認"
                )
    except Exception:
        pass

    ttm_path = os.path.join(TTM_DIR, f"{ticker}_ttm_series.json")
    if not os.path.exists(ttm_path):
        result["critical"].append("TTMファイルなし")
        return result

    series = json.load(open(ttm_path, encoding="utf-8"))
    if isinstance(series, dict):
        series = series.get("series", series.get("ttm_series", []))

    if not series:
        result["critical"].append("TTMエントリ0件")
        return result

    n = len(series)
    # フェーズC移行（2026-07-25、ttm_calculator.py snake_case化）でflowキーが
    # PascalCase→snake_caseに変わったが本チェックが追随しておらず、2026-07-26
    # のデータ再生成以降、全銘柄で「全件None」の誤検知が発生していた
    # （[[TTM-PASCALCASE-KEY-STALE-1]]対応）。
    ni_none  = sum(1 for s in series if s.get("flow", {}).get("net_income",           {}).get("val") is None)
    ocf_none = sum(1 for s in series if s.get("flow", {}).get("operating_cash_flow",  {}).get("val") is None)
    rev_none = sum(1 for s in series if s.get("flow", {}).get("revenue",             {}).get("val") is None)

    # 重大: 計算の根幹となるフィールドが全件 None
    if ni_none == n:
        result["critical"].append(f"NI全件None({n}件) → Q計算不可・RICE誤分類リスク")
    elif ni_none > 0:
        result["warning"].append(f"NI一部None({ni_none}/{n}件)")

    if ocf_none == n:
        result["critical"].append(f"OCF全件None({n}件) → Q計算不可")
    elif ocf_none > 0:
        result["warning"].append(f"OCF一部None({ocf_none}/{n}件)")

    # 警告: Revenue 欠損（1件程度は許容、全件は重大）
    if rev_none == n:
        result["critical"].append(f"Revenue全件None({n}件)")
    elif rev_none > 0:
        result["warning"].append(f"Revenue一部None({rev_none}/{n}件)")

    # TTMエントリ数が少ない
    if n < 3:
        result["warning"].append(f"TTMエントリ{n}件（3件未満・上場間もない可能性）")

    # 株数乖離チェック: EPS quarterly.json の希薄化株数 vs latest.json の希薄化株数
    # 5倍超の乖離はデータソース不一致の疑い
    _q_path = os.path.join(EPS_DIR, ticker, "quarterly.json")
    _l_path  = os.path.join(DATA_DIR, ticker, "latest.json")
    if os.path.exists(_q_path) and os.path.exists(_l_path):
        try:
            _qdata = json.load(open(_q_path, encoding="utf-8"))
            _qs = _qdata if isinstance(_qdata, list) else _qdata.get("quarters", [])
            _recent_q = sorted(
                [q for q in _qs if (q.get("filing_date") or "") >= "2022-01-01"],
                key=lambda q: q.get("filing_date", ""),
            )
            if _recent_q:
                _eps_shares = _recent_q[-1].get("diluted_shares") or 0
                _ld = json.load(open(_l_path, encoding="utf-8"))
                _val_shares = (_ld.get("components") or {}).get("diluted_shares") or 0
                if _eps_shares > 1e6 and _val_shares > 1e6:
                    _ratio = max(_eps_shares, _val_shares) / min(_eps_shares, _val_shares)
                    if _ratio >= 5.0:
                        result["warning"].append(
                            f"株数乖離{_ratio:.1f}x: EPS={_eps_shares/1e6:.1f}M"
                            f" vs DCF={_val_shares/1e6:.1f}M → データソース不一致疑い"
                        )
        except Exception:
            pass

    # UP-C構造等: 10-Qに株式数タグ（CommonStockSharesOutstanding/
    # WeightedAverageNumberOfDilutedSharesOutstanding）が存在しない銘柄を一覧化
    # （V等、複数株式クラス構造でこれらのタグを申告しない銘柄。
    #  ARCH-DATA-1のaudit.py拡張項目・未着手分）
    # Layer1（company_facts.json）を直接判定（[[SECDATA-STORAGE-FRAGMENTATION-1]]
    # フェーズB。旧: raw/{TICKER}_quarterly_raw.json経由の判定から切替。
    # raw/自体は2026-08-05に実消費者ゼロのデッドコードと判明し削除済み）
    _cf_path = os.path.join(SEC_DATA_DIR, ticker.upper(), "company_facts.json")
    if os.path.exists(_cf_path):
        try:
            _cf = json.load(open(_cf_path, encoding="utf-8"))
            _usgaap = _cf.get("facts", {}).get("us-gaap", {})
            _has_shares_q = False
            for _concept in (
                "CommonStockSharesOutstanding",
                "WeightedAverageNumberOfDilutedSharesOutstanding",
            ):
                _units = _usgaap.get(_concept, {}).get("units", {})
                for _entries in _units.values():
                    if any(e.get("form") == "10-Q" for e in _entries):
                        _has_shares_q = True
                        break
                if _has_shares_q:
                    break
            if not _has_shares_q:
                result["warning"].append(
                    "10-Qに株式数タグなし（UP-C構造・複数株式クラス等の可能性）"
                    " → yfinance実装株数へのフォールバック依存を確認"
                )
        except Exception:
            pass

    return result


def run_audit(tickers: list[str]) -> tuple[list[dict], list[dict]]:
    """監査実行。(critical_list, warning_list) を返す"""
    critical_tickers = []
    warning_tickers  = []

    for ticker in tickers:
        r = audit_ticker(ticker)
        if r["critical"]:
            critical_tickers.append(r)
        elif r["warning"]:
            warning_tickers.append(r)

    return critical_tickers, warning_tickers


def build_discord_message(
    tickers: list[str],
    critical: list[dict],
    warning: list[dict],
    run_date: str,
) -> str:
    total = len(tickers)
    ok    = total - len(critical) - len(warning)

    if not critical and not warning:
        return (
            f"✅ **SECデータ品質監査 完了** `{run_date}`\n"
            f"全{total}銘柄: 問題なし"
        )

    lines = [f"{'🔴' if critical else '🟡'} **SECデータ品質監査** `{run_date}`"]
    lines.append(f"対象: {total}銘柄  🟢正常: {ok}  🟡警告: {len(warning)}  🔴重大: {len(critical)}")
    lines.append("")

    if critical:
        lines.append("**🔴 重大問題（要対処）**")
        for r in critical:
            for msg in r["critical"]:
                lines.append(f"　`{r['ticker']}`: {msg}")
        lines.append("")
        tickers_str = " ".join(r["ticker"] for r in critical)
        lines.append(f"```")
        lines.append(f"python common/sec_data/update.py {tickers_str}")
        lines.append(f"python src/value/tanuki_valuation/pipeline.py {tickers_str}")
        lines.append(f"```")

    if warning:
        lines.append("**🟡 警告（軽微・要確認）**")
        for r in warning:
            for msg in r["warning"]:
                lines.append(f"　`{r['ticker']}`: {msg}")

    return "\n".join(lines)


def post_discord(message: str) -> bool:
    webhook = os.environ.get("DISCORD_WEB_HOOK", "")
    if not webhook:
        return False
    try:
        import urllib.error
        import urllib.request
        payload = json.dumps({"content": message}).encode("utf-8")
        # [[DISCORD-NOTIFY-403-SILENT-1]]: User-Agent未指定だとurllib既定の
        # "Python-urllib/x.y"が付き、Discord前段のCloudflareにerror code 1010
        # （HTTP 403）で拒否され通知が一度も届いていなかった。
        req = urllib.request.Request(
            webhook,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "On-a-journey-notifier/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status in (200, 204)
    except urllib.error.HTTPError as e:
        # webhook URL（トークンを含む）は出力しない
        print(f"Discord送信エラー: HTTP {e.code}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Discord送信エラー: {type(e).__name__}", file=sys.stderr)
        return False


def audit_beta_drift(tickers: list[str]) -> list[dict]:
    """
    beta_config.json と market_data層のβ実測値（reader.get_attributes()、
    yfinance .info由来）を比較し、乖離が大きい銘柄をリストアップする。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序5で、yfinance直接呼び出し
    （毎回ライブ取得・レート制限対策のtime.sleep併用）から
    common.market_data.reader.get_attributes()経由（Market_Data_Weekly_
    Update.ymlが週次生成する attributes/{TICKER}.json を読むだけ、
    ネットワークアクセスなし・sleep不要）に切替。beta_config.json自体が
    既にbeta_fetcher.py経由でmarket_data由来のため、本チェックの意味は
    「独立した外部ライブ値との比較」から「beta_config.jsonが週次
    attributes/より陳腐化していないかの内部整合性チェック」に変わる
    （BACKLOG確定事項6「audit.pyとの役割分担、両方維持」の想定通り）。

    market_dataのattributes/が未生成の銘柄は空リストを返す（graceful skip、
    reader.get_attributes()のNone返却をそのまま素通しする中立デフォルト）。
    """
    if not HAS_MARKET_DATA:
        print("  [beta] common.market_data.reader 未利用可 → βチェックをスキップ")
        return []

    if not os.path.exists(BETA_CONFIG_PATH):
        return []

    cfg       = json.load(open(BETA_CONFIG_PATH, encoding="utf-8"))
    overrides = cfg.get("overrides", {})
    DRIFT_THRESHOLD = 0.5  # この差分以上を「大きな乖離」と判定

    drift_list = []
    for ticker in tickers:
        try:
            attrs  = _md_get_attributes(ticker)
            yf_b   = attrs.get("beta") if attrs else None
            cfg_b  = overrides.get(ticker, {}).get("beta")
            src    = overrides.get(ticker, {}).get("source", "")

            if yf_b is None:
                continue
            if cfg_b is None:
                drift_list.append({
                    "ticker": ticker,
                    "cfg": None,
                    "yf":  round(yf_b, 3),
                    "diff": None,
                    "level": "critical",
                    "msg": f"beta_config未設定（yfinance={yf_b:.2f}）",
                })
                continue

            diff = abs(yf_b - cfg_b)
            if diff >= DRIFT_THRESHOLD:
                level = "critical" if diff >= 1.0 else "warning"
                drift_list.append({
                    "ticker": ticker,
                    "cfg":  cfg_b,
                    "yf":   round(yf_b, 3),
                    "diff": round(yf_b - cfg_b, 2),
                    "level": level,
                    "src":  src,
                    "msg":  f"β乖離 config={cfg_b} / yfinance={yf_b:.2f} (差{yf_b-cfg_b:+.2f})",
                })
            # ネットワークアクセスなし（ローカルattributes/{TICKER}.json読み取り
            # のみ）のため、旧yfinanceライブ呼び出し時代のレート制限対策
            # time.sleep(0.12)は不要になり削除。
        except Exception:
            pass

    return drift_list


def main():
    check_beta = "--check-beta" in sys.argv
    args       = [a for a in sys.argv[1:] if not a.startswith("--")]
    tickers    = args if args else get_registered_tickers()
    if not tickers:
        print("監査対象銘柄が見つかりません。tickers.json を確認してください。")
        sys.exit(1)

    run_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"=== SECデータ品質監査 {run_date} ===")
    print(f"対象: {len(tickers)}銘柄")
    print()

    critical, warning = run_audit(tickers)
    ok_count = len(tickers) - len(critical) - len(warning)

    print(f"🟢 正常: {ok_count}銘柄")
    if warning:
        print(f"🟡 警告: {len(warning)}銘柄")
        for r in warning:
            for msg in r["warning"]:
                print(f"   {r['ticker']:6s}: {msg}")
    if critical:
        print(f"🔴 重大: {len(critical)}銘柄")
        for r in critical:
            for msg in r["critical"]:
                print(f"   {r['ticker']:6s}: {msg}")
            for msg in r["warning"]:  # criticalと同一銘柄のwarningも表示
                print(f"   {r['ticker']:6s}: ⚠ {msg}")

    # ── β乖離チェック（--check-beta 指定時）──
    beta_drift = []
    if check_beta:
        print("\n=== β乖離チェック ===")
        beta_drift = audit_beta_drift(tickers)
        beta_critical = [d for d in beta_drift if d["level"] == "critical"]
        beta_warning  = [d for d in beta_drift if d["level"] == "warning"]
        if not beta_drift:
            print("🟢 β乖離: 問題なし")
        else:
            if beta_critical:
                print(f"🔴 重大β乖離: {len(beta_critical)}銘柄")
                for d in beta_critical:
                    print(f"   {d['ticker']:6s}: {d['msg']}")
            if beta_warning:
                print(f"🟡 β警告: {len(beta_warning)}銘柄")
                for d in beta_warning:
                    print(f"   {d['ticker']:6s}: {d['msg']}")
        if beta_drift:
            print("\n対処方法:")
            print("  python src/value/tanuki_valuation/beta_fetcher.py --dry-run")
            print("  python src/value/tanuki_valuation/beta_fetcher.py")

    # Discord通知
    message = build_discord_message(tickers, critical, warning, run_date)

    # β乖離情報を Discord メッセージに追記
    if check_beta and beta_drift:
        beta_lines = ["\n**⚡ β乖離検知**"]
        for d in beta_drift[:8]:  # 最大8件
            icon = "🔴" if d["level"] == "critical" else "🟡"
            beta_lines.append(f"　{icon} `{d['ticker']}`: {d['msg']}")
        if len(beta_drift) > 8:
            beta_lines.append(f"　... 他{len(beta_drift)-8}銘柄")
        beta_lines.append("\n```\npython src/value/tanuki_valuation/beta_fetcher.py\n```")
        message = message + "\n" + "\n".join(beta_lines) if message else "\n".join(beta_lines)

    if post_discord(message):
        print("\nDiscord通知: 送信完了")

    # GitHub Actions サマリー出力
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("## SECデータ品質監査\n\n")
            status = "🔴 重大問題あり" if critical else ("🟡 警告あり" if warning else "🟢 問題なし")
            f.write(f"**{status}** — 対象: {len(tickers)}銘柄  正常: {ok_count}  警告: {len(warning)}  重大: {len(critical)}\n\n")
            if critical:
                f.write("### 🔴 重大問題\n")
                for r in critical:
                    f.write(f"- `{r['ticker']}`: {', '.join(r['critical'])}\n")
                f.write("\n")
            if warning:
                f.write("### 🟡 警告\n")
                for r in warning:
                    f.write(f"- `{r['ticker']}`: {', '.join(r['warning'])}\n")
            if check_beta and beta_drift:
                f.write("\n### ⚡ β乖離\n")
                for d in beta_drift:
                    icon = "🔴" if d["level"] == "critical" else "🟡"
                    f.write(f"- {icon} `{d['ticker']}`: {d['msg']}\n")

    # 重大問題があれば exit 1
    if critical:
        print("\n重大問題あり → exit 1")
        sys.exit(1)


if __name__ == "__main__":
    main()
