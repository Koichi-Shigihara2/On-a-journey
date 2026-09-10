"""
daily_pick.py
TANUKI SCOREの特選銘柄を毎日選出し、Grok AIによる投資判断レポートを生成する。

フロー:
  1. 全銘柄をスコアリング（ファンダ/タイミング/分類）
  2. 選出ロジック（優先①分類変化 → ②Grokニュース → ③ファンダ上位未選出）
  3. GrokでAIレポート生成
  4. daily_pick.json / history.json を保存
"""
import json
import os
import sys
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DOCS         = PROJECT_ROOT / "docs"
TANUKI_DATA  = DOCS / "value-monitor" / "tanuki_valuation" / "data"
HYPE_DATA    = DOCS / "value-monitor" / "hypecore" / "data"
EPS_DATA     = DOCS / "value-monitor" / "adjusted_eps_analyzer" / "data"
MKT_PATH     = DOCS / "market-monitor" / "market-pulse" / "data" / "market_data.json"
OUT_DIR      = DOCS / "integrated-dashboard"
PICK_FILE    = OUT_DIR / "daily_pick.json"
HIST_FILE    = OUT_DIR / "history.json"

JST = timezone(timedelta(hours=9))

XAI_API_KEY = os.environ.get("XAI_API_KEY", "")
XAI_API_URL = "https://api.x.ai/v1/chat/completions"

# ── Data loaders ──────────────────────────────────────────────
def load_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def load_tickers():
    d = load_json(TANUKI_DATA / "tickers.json")
    return d.get("tickers", []) if isinstance(d, dict) else []

def load_market():
    data = load_json(MKT_PATH)
    return data[-1] if isinstance(data, list) and data else {}

def load_tanuki(ticker):
    return load_json(TANUKI_DATA / ticker / "latest.json") or {}

def load_hype(ticker):
    d = load_json(HYPE_DATA / f"{ticker}_poc.json")
    if not isinstance(d, dict):
        return {}
    monthly = d.get("monthly", [])
    return monthly[-1] if monthly else {}

def load_eps_summary():
    d = load_json(EPS_DATA / "summary.json")
    if not isinstance(d, dict):
        return {}
    return {t["ticker"]: t for t in d.get("tickers", [])}

def load_eps_annual_latest(ticker):
    d = load_json(EPS_DATA / ticker / "annual.json")
    if not isinstance(d, dict):
        return {}
    years = d.get("years", [])
    return years[0] if years else {}

def load_history():
    h = load_json(HIST_FILE)
    return h if isinstance(h, list) else []

def save_history(history):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(HIST_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

# ── Score all stocks ──────────────────────────────────────────
def score_all(tickers):
    """pipeline.py(latest.json)が計算済みの6分類をそのまま使う（ARCH-SCORE-SYNC-1）"""
    eps_summary = load_eps_summary()
    results     = []

    for ticker in tickers:
        tk  = load_tanuki(ticker)
        ep  = eps_summary.get(ticker, {})

        cat = tk.get("tanuki_score")
        f   = tk.get("funda_score")
        if cat is None or f is None:
            continue  # latest.json未生成(pipeline.py未実行)のtickerはスキップ

        results.append({
            "ticker":   ticker,
            "company":  ep.get("company_name", ticker),
            "funda":    f,
            "timing":   tk.get("timing_score") or 0,
            "category": cat,
        })

    return results

# ── Selection logic ───────────────────────────────────────────
# ARCH-SCORE-SYNC-1: TRIM/SELL/PASSは「特選銘柄」の候補から除外する
# （SELL判定銘柄が強気寄りの特選銘柄として表示される問題の解消）
ELIGIBLE_CATEGORIES = ("BUY", "WATCH", "GROWTH_PREMIUM", "HOLD")
CATEGORY_PRIORITY = {"BUY": 0, "WATCH": 1, "GROWTH_PREMIUM": 2, "HOLD": 3}

def select_ticker(stocks, history, today_str):
    """
    優先①: 前日から分類が変わった銘柄（BUY/WATCH/GROWTH_PREMIUM/HOLDのみ対象）
    優先②: Grokニュース検索で重大ニュースのある銘柄
    優先③: ファンダ上位・最長未選出
    """
    eligible = [s for s in stocks if s["category"] in ELIGIBLE_CATEGORIES]
    if not eligible:
        print("[daily_pick] 警告: BUY/WATCH/GROWTH_PREMIUM/HOLDが0件のため全銘柄を候補にフォールバック")
        eligible = stocks

    # 優先①: 前日の全分類と比較
    if history:
        yesterday = history[0]
        prev_cats = yesterday.get("all_categories", {})
        if prev_cats:
            changed = [
                s for s in eligible
                if prev_cats.get(s["ticker"]) and prev_cats[s["ticker"]] != s["category"]
            ]
            if changed:
                # カテゴリ重要度（BUY → WATCH → GROWTH_PREMIUM → HOLD の順で優先）
                changed.sort(key=lambda s: (CATEGORY_PRIORITY.get(s["category"], 9), -s["funda"]))
                best = changed[0]
                old_cat = prev_cats.get(best["ticker"], "不明")
                return best, f"分類変化: {old_cat} → {best['category']}"

    # 優先②: Grokニュース検索（API設定済みかつ良質銘柄のみ対象）
    if XAI_API_KEY:
        candidates = [s["ticker"] for s in eligible if s["category"] in ("BUY", "WATCH")][:20]
        if candidates:
            pick, reason = grok_news_search(candidates, today_str)
            if pick:
                s = next((x for x in eligible if x["ticker"] == pick), None)
                if s:
                    return s, reason

    # 優先③: ファンダ上位・最長未選出（直近の選出銘柄は除外）
    last_pick = history[0]["ticker"] if history else None
    candidates = [s for s in eligible if s["ticker"] != last_pick] or eligible

    pick_idx = {h["ticker"]: i for i, h in enumerate(history)}
    candidates.sort(key=lambda s: (-pick_idx.get(s["ticker"], len(history) + 1), -s["funda"]))
    return candidates[0], "ファンダスコア上位・最長未選出"

# ── Grok API helpers（collect_and_send.py と同方式） ─────────
def _call_grok(messages, temperature=0.3, max_tokens=4096):
    """grok-4.3への呼び出し（[[GROK-MODEL-PRICE-1]]対応: 従来はgrok-3-mini→
    grok-3→grok-2-1212の順でフォールバックしていたが、前2つは実際には
    grok-4.3へ自動ルーティングされ、grok-2-1212は廃止済み（API側でModel
    not found）と判明したため、実態に合わせ現行モデル名を明示する
    （フォールバックループ構造自体は一時的なネットワーク障害時の
    再試行として維持）"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {XAI_API_KEY}",
    }
    models = ["grok-4.3", "grok-4.3", "grok-4.3"]
    last_error = None
    for model in models:
        try:
            print(f"  [grok] 試行: {model}")
            resp = requests.post(
                XAI_API_URL,
                headers=headers,
                json={
                    "model":       model,
                    "messages":    messages,
                    "max_tokens":  max_tokens,
                    "temperature": temperature,
                },
                timeout=120,
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]
            print(f"  [grok] 成功: {model}")
            return text
        except Exception as e:
            print(f"  [grok] 失敗 ({model}): {e}")
            last_error = e
    raise last_error

def _extract_json(text):
    """テキストからJSONオブジェクトを抽出する（マークダウンコードブロック対応）"""
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # ```json ... ``` や ``` ... ``` の中身を取り出す
    start = text.find('{')
    end   = text.rfind('}')
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except json.JSONDecodeError:
            pass
    return None


def grok_news_search(ticker_list, date_str):
    """Grokで本日重大ニュースのある銘柄を1件検索する"""
    prompt = (
        f"以下の銘柄リストのうち、今日（{date_str}）重要なニュースがある"
        f"銘柄を1つ選び、その理由を50字以内で答えよ：{', '.join(ticker_list)}\n"
        f"回答はJSONオブジェクトのみ返すこと。\n"
        f'回答形式: {{"ticker": "XXXX", "reason": "理由"}}'
    )
    try:
        text   = _call_grok([{"role": "user", "content": prompt}], temperature=0.3, max_tokens=256)
        parsed = _extract_json(text)
        if not parsed:
            print(f"  [news] JSON解析失敗: {text[:120]}")
            return None, None
        ticker = parsed.get("ticker", "").upper().strip()
        reason = parsed.get("reason", "")
        if ticker in ticker_list:
            return ticker, f"{reason}（Grok選出）"
        print(f"  [news] 返答ticker '{ticker}' がリストにない、スキップ")
    except Exception as e:
        print(f"  [news] Error: {e}")
    return None, None

# ── Report generation ─────────────────────────────────────────
def _nested_get(d, *keys):
    """ネストした辞書を安全に辿る。
    キーが存在しても値がNoneの場合、dict.get(key, default)のdefaultは効かず
    None.get(...)でAttributeErrorになるため、各段でdict判定してから辿る。"""
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur

def build_data_package(stock, mkt):
    """Grokに渡す統合データパッケージを構築する"""
    ticker = stock["ticker"]
    tk     = load_tanuki(ticker)
    hc     = load_hype(ticker)
    ann    = load_eps_annual_latest(ticker)

    fcf_raw  = tk.get("fcf_base")
    fcf_base = (
        fcf_raw.get("base_fcf") if isinstance(fcf_raw, dict)
        else fcf_raw if isinstance(fcf_raw, (int, float))
        else None
    )
    rpo_raw = tk.get("rpo_adjustment")
    rpo_pv  = rpo_raw.get("rpo_pv") if isinstance(rpo_raw, dict) else None

    rice_raw = tk.get("rice")
    return {
        "ticker":       ticker,
        "company":      stock["company"],
        "funda_score":  stock["funda"],
        "timing_score": stock["timing"],
        "category":     stock["category"],
        "tanuki": {
            "intrinsic_value_per_share": tk.get("intrinsic_value_per_share"),
            "current_price":             tk.get("current_price"),
            "upside_percent":            tk.get("upside_percent"),
            "deviation_rate":            tk.get("deviation_rate"),
            "fcf_base":                  fcf_base,
            "growth_rate":               tk.get("growth", {}).get("rate") if isinstance(tk.get("growth"), dict) else None,
            "growth_source":             tk.get("growth", {}).get("source") if isinstance(tk.get("growth"), dict) else None,
            "wacc":                      tk.get("wacc", {}).get("value") if isinstance(tk.get("wacc"), dict) else None,
            "alpha":                     tk.get("alpha"),
            "alpha_was_capped":          tk.get("alpha_was_capped"),
            "rpo_pv":                    rpo_pv,
            "net_cash_per_share":        tk.get("bs_adjustment", {}).get("net_cash_per_share") if isinstance(tk.get("bs_adjustment"), dict) else None,
            "fcf_conversion_rate":       tk.get("fcf_estimation", {}).get("conversion_rate") if isinstance(tk.get("fcf_estimation"), dict) else None,
            "fcf_estimated":             tk.get("fcf_estimation", {}).get("estimated_fcf") if isinstance(tk.get("fcf_estimation"), dict) else None,
            "rd_adjustment":             tk.get("rd_capitalization", {}).get("rd_adjustment") if isinstance(tk.get("rd_capitalization"), dict) else None,
            "rice_score":                rice_raw.get("score") if isinstance(rice_raw, dict) else rice_raw,
        },
        "hypecore": {
            "stage":                  hc.get("stage"),
            "stage_label":            hc.get("stage_label"),
            "substage_label":         hc.get("substage_label"),
            "substage_watch":         hc.get("substage_watch"),
            "substage_next":          hc.get("substage_next"),
            "rev_yoy":                hc.get("rev_yoy"),
            # [[RULE40-DEFINITION-MISMATCH-1]]: 読み取り元（poc.json）のキーは
            # rule40_yoy_netmarginへ改名（2026-08-13）。本パッケージ自体の
            # 出力キー"rule40"はGrokプロンプト用の内部ラベルのため維持する。
            "rule40":                 hc.get("rule40_yoy_netmargin"),
            "peg_ratio":              hc.get("peg_ratio"),
            "short_pct_float":        hc.get("short_pct_float"),
            "eps_surprise":           hc.get("eps_surprise"),
            "expectation_score":      hc.get("expectation_score"),
            "fundamental_score":      hc.get("fundamental_score"),
            "momentum_score":         hc.get("momentum_score"),
            "price":                  hc.get("price"),
            "from_peak":              hc.get("from_peak"),
            # [[HYPECORE-MISC-NAMING-GAPS-1]]（2026-08-29）: 読み取り元
            # （poc.json）のキーがma200_dev→ma200_dev_local・
            # revenue_growth→revenue_growth_yf・buy_hold_ratio→buy_ratioへ
            # それぞれ改名されたため追従する。本パッケージ自体の出力キーは
            # [[RULE40-DEFINITION-MISMATCH-1]]と同じ理由（Grokプロンプト用の
            # 内部ラベル、フロントエンド非消費）により旧名のまま維持する。
            "ma200_dev":              hc.get("ma200_dev_local"),
            "ma50_dev":               hc.get("ma50_dev"),
            "forward_pe":             hc.get("forward_pe"),
            "psr":                    hc.get("psr"),
            "revenue_growth":         hc.get("revenue_growth_yf"),
            "earnings_growth":        hc.get("earnings_growth"),
            "analyst_upgrade_rate":   hc.get("analyst_upgrade_rate"),
            "analyst_downgrade_rate": hc.get("analyst_downgrade_rate"),
            "buy_hold_ratio":         hc.get("buy_ratio"),
        },
        "eps_annual": {
            "gaap_eps":             ann.get("gaap_eps"),
            "adjusted_eps":         ann.get("adjusted_eps"),
            "yoy_growth":           ann.get("yoy_growth"),
            "gaap_to_adj_positive": ann.get("gaap_to_adj_positive"),
            "adjustments": [
                {
                    "name":       a.get("name"),
                    "amount":     a.get("amount"),
                    "category":   a.get("category"),
                    "confidence": a.get("confidence"),
                }
                for a in ann.get("adjustment_items", ann.get("adjustments", []))[:8]
            ],
        },
        "market": {
            "judgment":                      mkt.get("judgment"),
            "fear_greed_score":              mkt.get("fear_greed", {}).get("score") if isinstance(mkt.get("fear_greed"), dict) else None,
            "vix":                           _nested_get(mkt, "indicators", "VIX指数", "value"),
            "vix9d":                         _nested_get(mkt, "indicators", "VIX9D（短期VIX）", "value"),
            "tech_pulse_score":              mkt.get("tech_pulse", {}).get("score") if isinstance(mkt.get("tech_pulse"), dict) else None,
            "tech_pulse_divergence":         _nested_get(mkt, "tech_pulse", "divergence", "value"),
            "tech_pulse_divergence_zscore":  _nested_get(mkt, "tech_pulse", "divergence", "zscore"),
            "qqq_vs_spy_20d":                _nested_get(mkt, "tech_pulse", "components", "qqq_vs_spy_20d"),
            "risk_off_score":                mkt.get("credit", {}).get("risk_off_score") if isinstance(mkt.get("credit"), dict) else None,
            "credit_stock":                  mkt.get("credit", {}).get("stock") if isinstance(mkt.get("credit"), dict) else None,
            "credit_bond":                   mkt.get("credit", {}).get("bond") if isinstance(mkt.get("credit"), dict) else None,
            "credit_credit":                 mkt.get("credit", {}).get("credit") if isinstance(mkt.get("credit"), dict) else None,
            "asset_flow": {
                "equity":      _nested_get(mkt, "asset_flow", "equity", "change_pct"),
                "long_bond":   _nested_get(mkt, "asset_flow", "long_bond", "change_pct"),
                "gold":        _nested_get(mkt, "asset_flow", "gold", "change_pct"),
                "hy_bond":     _nested_get(mkt, "asset_flow", "hy_bond", "change_pct"),
                "ultra_short": _nested_get(mkt, "asset_flow", "ultra_short_bond", "change_pct"),
            },
            "sub_scores": mkt.get("sub_scores"),
        },
    }

def generate_report(stock, mkt):
    """Grokで投資判断レポートを生成する"""
    data_pkg = build_data_package(stock, mkt)
    ticker   = stock["ticker"]
    company  = stock["company"]
    now_jst  = datetime.now(JST).strftime("%Y-%m-%d %H:%M JST")

    prompt = f"""あなたは投資アナリストです。以下のデータを統合して、\
{ticker}（{company}）の投資判断レポートを日本語で作成してください。

=== 指標の定義・コンセプト ===

【TANUKI VALUATION】
- 理論株価: Rm=10%固定（β=0）の市場独立DCFで算出。市場センチメントに依存しない内在価値。
- upside_percent: (理論株価 - 現在株価) / 現在株価 × 100。プラス=割安、マイナス=割高。
- alpha(α): ROE平均×内部留保率÷Rm×0.7で算出。企業の自己資本効率から導くプレミアム。セクター別上限あり。
- WACC: Rm=10%固定（β除外）。市場リスクに依存しない割引率。
- FCF: OCF - CapEx全額控除。SBCはOCFに含めたまま（意図的設計）。
- RPO_PV: 前年比超過の非連続需要分×営業利益率でPV化。αの外に加算。赤字企業はゼロ。
- P_t = V0×(1+α) + RPO_PV

【HypeCore】
- stage1=蓄積期: 実態改善中だが市場未認識。最良の仕込み場。
- stage2=上昇期: 市場が実態を認識し始めた局面。まだ上昇余地あり。
- stage3=陶酔期: 期待が実態を先行。過熱リスク。
- stage4=期待剥落期/崩壊期: 期待と実態の乖離が修正される局面。
- expectation_score: マイナス=市場の期待が過熱（株価が実態より高い）、プラス=過小評価。
- fundamental_score: プラス=ファンダが改善中。
- Rule of 40: 売上成長率(%) + FCFマージン(%)。40以上=健全、100超=極めて優秀。
- substage: フェーズ内の細分類。「入口」=まだ余地あり、「中盤」=継続、「出口」=転換注意。

【EPS Analyzer】
- GAAP EPS: 全コスト（SBC・のれん償却等）を含む法定EPS。
- Adjusted EPS: 一過性・非現金コストを除いた実力EPS。
- eps_ratio: (Adj - GAAP) / |GAAP| × 100。高いほど市場がGAAPで判断している場合に実力を過小評価しやすい。
- gaap_to_adj_positive: GAAPで赤字だがAdjでは黒字。市場の誤認リスクが最も高い状態。

【Market Pulse】
- fear_greed_score: 0=極度の恐怖（歴史的買い場）、50=中立、100=極度の強欲（売り検討）。
- risk_off_score: 0=RISK ON（株式に追い風）、67以上=RISK OFF（リスク回避）。
- tech_pulse: NASDAQベースのセンチメント。fear_greed_scoreとの乖離が大きい場合NASDAQ固有の動き。

【TANUKI SCORE分類】（pipeline.py算出の6分類。判定は全銘柄共通ロジック）
- BUY: ファンダ≥50 かつ 乖離率>20% かつ タイミング≥50 — 今仕込んでもよい
- WATCH: ファンダ≥50 かつ 乖離率0〜20% — 購入候補としてウォッチ
- GROWTH_PREMIUM: 乖離率<-30%・陶酔期以降(stage≥3)で本来はTRIM該当だが、
  現在の実績成長率が逆算必要成長率を上回っており正当化される状態
- HOLD: ファンダ良好だが上記いずれにも該当しない — 保有継続・新規不要
- TRIM: ファンダ≥50 かつ 乖離率<-30% かつ 陶酔期以降(stage≥3)で、
  成長率が必要水準に届かない — 一部利確検討
- SELL: 売上鈍化×Rule40<20×FCF悪化（ファンダ要因）、または
  崩壊期(stage≥4)×乖離率<-20%×MA200から-10%超下方乖離（技術的要因）
- PASS: ファンダ<25 — 対象外

=== 記述ルール ===

1. 各指標の数値と結論を直接繋げず、必ず因果関係を説明すること。
   ×『売上YoY73%と市場期待は高い』
   ○『売上YoY73%・Rule of 40=136と事業の実力は極めて高く、その結果として市場の期待値も高止まりしている』

2. 理論株価と現在株価を必ず両方記載すること。
   例：『理論株価481ドル（現在株価219ドル、乖離+122%）』

3. HypeCoreフェーズとTANUKI SCOREカテゴリが一見矛盾する場合
   （例：陶酔期なのにBUY）は必ずその理由を説明すること。

=== 定量データ ===
{json.dumps(data_pkg, ensure_ascii=False, indent=2)}

【作成日時】{now_jst}

以下のJSONキーを持つオブジェクトを返してください。各値は200〜400字の分析文です：

{{
  "fundamental": "【ファンダメンタル評価】売上成長・収益性・FCF・理論株価と現在株価の両方を明記した上で乖離率を評価。企業のビジネスモデルの強み・弱みを踏まえ、数値から結論への因果関係を説明する。",
  "expectation": "【期待値評価】HypeCoreフェーズ・expectation_score・substageを分析。Non-GAAP調整による市場の誤認可能性を評価。フェーズとTANUKI SCOREカテゴリが矛盾する場合はその理由を説明する。",
  "news": "【最近のニュース・時事】{ticker}に関する直近の重要な事象（契約・決算・規制・競合動向等）を記載。知識ベースで把握できる情報に基づき、株価に影響する事象を述べる。",
  "timing": "【タイミング評価】market_pulse（fear_greed・risk_off・tech_pulse）・HypeCoreフェーズ・TANUKI乖離率を統合評価。今買うべきか/待つべきか/売るべきかを数値から因果関係で導いて明確に示す。",
  "summary": "【総合所見】上記を統合した投資判断（2〜3文）。注目すべき次のトリガーイベントを明記。"
}}

本日（{now_jst}）時点の情報として分析してください。"""

    print(f"  [report] Calling Grok for {ticker}...")
    try:
        text   = _call_grok([{"role": "user", "content": prompt}], temperature=0.5, max_tokens=4096)
        parsed = _extract_json(text)
        if not parsed:
            raise ValueError(f"JSON解析失敗: {text[:200]}")
        # Ensure all required keys exist
        for key in ("fundamental", "expectation", "news", "timing", "summary"):
            if key not in parsed:
                parsed[key] = "（データ取得エラー）"
        return parsed
    except Exception as e:
        print(f"  [report] Error: {e}")
        return {
            "fundamental": "レポート生成エラー",
            "expectation": "レポート生成エラー",
            "news":        "レポート生成エラー",
            "timing":      "レポート生成エラー",
            "summary":     f"Grok APIの呼び出し中にエラーが発生しました: {e}",
        }

# ── Main ──────────────────────────────────────────────────────
def main(force: bool = False):
    now_jst   = datetime.now(JST)
    today_str = now_jst.strftime("%Y-%m-%d")
    print(f"[daily_pick] Starting — {today_str} JST")

    history = load_history()

    # [[WORKFLOW-FALLBACK-CRON-DUPLICATE-1]]横展開対応: TANUKI_Score_
    # Update.ymlもworkflow_run連鎖＋週末独立cron（土日は独立実行の設計
    # 意図があるが、TANUKI VALUATION側の連鎖が既に同日中に発火していた
    # 場合は無条件の重複実行になる）という同型構造を持つ。本パイプライン
    # は「1日1銘柄選出」が設計上の前提のため、当日分が既にhistoryに
    # 存在するなら重複実行とみなしスキップする（Grok呼び出し2箇所を含む
    # 後続処理を回避）。
    if not force and history and history[0].get("date") == today_str:
        print(
            f"[Idempotency Guard] {today_str}分は既に選出済み"
            f"（ticker={history[0].get('ticker')}）のため、意図的に"
            f"スキップします（[[WORKFLOW-FALLBACK-CRON-DUPLICATE-1]]対応: "
            f"重複実行によるGrok呼び出し防止。強制実行するには --force を指定）"
        )
        return

    mkt     = load_market()
    tickers = load_tickers()

    if not tickers:
        print("[daily_pick] No tickers found. Exiting.")
        sys.exit(1)

    # Score all
    print(f"[daily_pick] Scoring {len(tickers)} tickers...")
    stocks = score_all(tickers)
    stocks.sort(key=lambda s: (-s["funda"], -s["timing"]))

    # Select
    print("[daily_pick] Selecting ticker...")
    selected, reason = select_ticker(stocks, history, today_str)
    print(f"[daily_pick] Selected: {selected['ticker']} ({selected['category']}) — {reason}")

    # Build data package (used for both report generation and JSON output)
    data_pkg = build_data_package(selected, mkt)

    # Generate report
    if not XAI_API_KEY:
        print("[daily_pick] XAI_API_KEY not set — skipping report.")
        report = {
            "fundamental": "APIキー未設定のためレポートを生成できません。",
            "expectation":  "APIキー未設定のためレポートを生成できません。",
            "news":         "APIキー未設定のためレポートを生成できません。",
            "timing":       "APIキー未設定のためレポートを生成できません。",
            "summary":      "XAI_API_KEY 環境変数を設定してください。",
        }
    else:
        report = generate_report(selected, mkt)

    # Build output
    generated_at = now_jst.strftime("%Y-%m-%dT%H:%M:%S+09:00")
    output = {
        "generated_at":     generated_at,
        "ticker":           selected["ticker"],
        "company":          selected["company"],
        "selection_reason": reason,
        "funda_score":      selected["funda"],
        "timing_score":     selected["timing"],
        "category":         selected["category"],
        "report":           report,
        "tanuki":           data_pkg["tanuki"],
    }

    # Save daily_pick.json
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(PICK_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"[daily_pick] Saved → {PICK_FILE}")

    # Update history (keep 30 days, store all_categories for priority① detection)
    all_cats = {s["ticker"]: s["category"] for s in stocks}
    history = [e for e in history if e.get("date") != today_str]
    history.insert(0, {
        "date":             today_str,
        "ticker":           selected["ticker"],
        "company":          selected["company"],
        "selection_reason": reason,
        "category":         selected["category"],
        "funda_score":      selected["funda"],
        "timing_score":     selected["timing"],
        "all_categories":   all_cats,
    })
    history = history[:30]
    save_history(history)
    print(f"[daily_pick] History updated ({len(history)} entries)")
    print("[daily_pick] Done.")

if __name__ == "__main__":
    import argparse
    _parser = argparse.ArgumentParser(description="TANUKI Score daily_pick")
    _parser.add_argument("--force", action="store_true",
                          help="[[WORKFLOW-FALLBACK-CRON-DUPLICATE-1]]対応の冪等性ガード"
                               "（当日分が既に選出済みならスキップ）を無視して強制実行する"
                               "（手動re-run等の正当な再実行用）")
    _args = _parser.parse_args()
    main(force=_args.force)
