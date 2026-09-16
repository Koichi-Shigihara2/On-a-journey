"""
DCF前提・ROIC投下資本の妥当性チェッカー

TANUKI VALUATIONのDCF成長率前提・ROIC計算根拠が実績データと整合しているかを
機械的にチェックする。既存の latest.json / annual_{year}.json / {TICKER}_poc.json
を読み取るのみで、本番ファイルは一切変更しない。

2026-07-10のサテライト投資候補10銘柄の手作業チェック（MO/FICOで発見した
「成長率floor値張り付き」「株主資本マイナスによるROIC見かけ上の跳ね上がり」等）
を機械判定として91銘柄に展開した際のアドホックスクリプトを整理したもの。

チェック項目:
  A. 成長率前提と実績の乖離
     - growth_source=fcf_cagr で calculate_fcf_cagr() の growth_floor/growth_cap に
       完全一致(=実績と無関係な下駄履き値の可能性)
     - growth_rateとrev_cagr_3yr/5yr(実績)の乖離が10pt以上、または実績マイナス成長
       なのにgrowth_rateがプラス
  B. segment_detail.sourceの実態確認(GROWTH-SOURCE-LABEL-1を踏まえたラベル検証)
     - segment_configured=False なのに growth.source=segment_weighted と
       表示される銘柄を検知し、実際の根拠(recommended_g自動注入 / TTM実績注入)を推定
  C. SECデータ異常ジャンプ検知
     - 直近6年の年次売上でYoY倍率が2.0倍以上または0.5倍以下となる不連続を検知
     - [[DATA-JUMP-CHECK-GENERALIZE-1]]でsection/fieldパラメータ化。
       report_consistency_check.py側では売上総利益（上振れ5.0倍/下振れ0.2倍）・
       CapEx（上振れ8.0倍/下振れ0.15倍）にも展開済み（WARN-44・WARN-45）
  D. ROIC投下資本の妥当性
     - 直近期の株主資本マイナス、または投下資本が売上の15%未満(閾値は引数で調整可)
  E. HypeCore遷移確率のサンプル数
     - 現在ステージからの遷移観測数が5件未満(閾値は引数で調整可)
  F. EPS Analyzer PER比較([[SCREENING-SIGNAL-INTEGRATION-EPIC-1]]元EPS-ANALYZER-INTEGRATE-1)
     - components.per(GAAP PER)とcomponents.per_adjusted(調整後PER)の差が
       ±10x以上(SBC等の影響で見かけの割安度が歪んでいる可能性)
  G. RICE投資効率([[SCREENING-SIGNAL-INTEGRATION-EPIC-1]]元RICE-INTEGRATE-1)
     - rice.base.rice<1.0(低効率)をフラグ。TANUKI SCORE=BUYとの組み合わせ
       (割安だが再投資効率が低い)も記録する
  H. アナリストコンセンサスとの乖離([[SCREENING-SIGNAL-INTEGRATION-EPIC-1]]元ANALYST-VS-IV-INTEGRATE-1)
     - components.analyst_vs_ivの絶対値が50pt以上(TANUKIとアナリストの意見が
       大きく割れている銘柄、方向性も記録)

実行例:
    # 単一銘柄
    python common/screening/dcf_validity_checker.py AAPL

    # 複数銘柄を指定してJSON出力
    python common/screening/dcf_validity_checker.py AAPL FICO MO --output out.json

    # 引数なし = config/cik_lookup.csv の全銘柄(tanuki=trueかつlatest.json存在分)
    python common/screening/dcf_validity_checker.py --output all_validity.json

他スクリプトからの利用:
    from common.screening.dcf_validity_checker import check_ticker
    result = check_ticker(repo_root, "FICO")
"""
import argparse
import json
import os
import sys
from datetime import datetime

FCF_CAGR_FLOOR = 0.15
FCF_CAGR_CAP = 0.50
EPS = 1e-4


def _load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _get_annual_years(repo_root, ticker):
    sec_dir = os.path.join(repo_root, "common", "sec_data", "data", ticker)
    if not os.path.exists(sec_dir):
        return []
    return sorted(int(fn[7:11]) for fn in os.listdir(sec_dir)
                  if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit())


def _load_annual(repo_root, ticker, year):
    path = os.path.join(repo_root, "common", "sec_data", "data", ticker, f"annual_{year}.json")
    return _load_json(path)


def check_a_growth_vs_actual(latest):
    """A: 成長率前提と実績の乖離チェック"""
    g = latest.get("growth") or {}
    rate = g.get("rate")
    source = g.get("source")
    gs = latest.get("growth_sanity") or {}
    rev_3yr = gs.get("rev_cagr_3yr")
    rev_5yr = gs.get("rev_cagr_5yr")

    flag = False
    reasons = []

    if source == "fcf_cagr" and rate is not None:
        if abs(rate - FCF_CAGR_FLOOR) < EPS:
            flag = True
            reasons.append(f"fcf_cagr floor値({FCF_CAGR_FLOOR:.0%})に完全一致=下駄履き値の疑い")
        elif abs(rate - FCF_CAGR_CAP) < EPS:
            flag = True
            reasons.append(f"fcf_cagr cap値({FCF_CAGR_CAP:.0%})に完全一致=上限張り付きの疑い")

    if rate is not None:
        for label, rev_cagr in [("3yr", rev_3yr), ("5yr", rev_5yr)]:
            if rev_cagr is None:
                continue
            if rev_cagr < 0 and rate > 0:
                flag = True
                reasons.append(f"実績rev_cagr_{label}={rev_cagr:.1%}(マイナス成長)なのにgrowth_rate={rate:.1%}(プラス)")
            elif abs(rate - rev_cagr) >= 0.10:
                flag = True
                reasons.append(f"growth_rate={rate:.1%}とrev_cagr_{label}={rev_cagr:.1%}の乖離が{abs(rate-rev_cagr)*100:.1f}pt")

    return flag, reasons, {"rate": rate, "source": source, "rev_cagr_3yr": rev_3yr, "rev_cagr_5yr": rev_5yr}


def check_b_source_label(repo_root, latest, ticker):
    """B: segment_detail.sourceの実態確認(GROWTH-SOURCE-LABEL-1参照)"""
    g = latest.get("growth") or {}
    rate = g.get("rate")
    source = g.get("source")
    segment_configured = latest.get("segment_configured")
    seg_detail = (latest.get("growth_scenarios") or {}).get("segment") or {}
    displayed_source = seg_detail.get("source")

    flag = False
    real_source = displayed_source
    note = ""

    if source == "segment_weighted" and segment_configured is False:
        flag = True
        gs = latest.get("growth_sanity") or {}
        recommended_g = gs.get("recommended_g")
        rev_yoy = None
        poc_path = os.path.join(repo_root, "docs", "value-monitor", "hypecore", "data", f"{ticker}_poc.json")
        if os.path.exists(poc_path):
            try:
                monthly = _load_json(poc_path).get("monthly") or []
                if monthly:
                    rev_yoy = monthly[-1].get("rev_yoy")
            except Exception:
                pass
        if recommended_g is not None and rate is not None and abs(rate - recommended_g) < EPS:
            real_source = "recommended_g自動注入(中央値/decayフォールバック)"
        elif rev_yoy is not None and rate is not None and abs(rate - rev_yoy / 100) < 0.005:
            real_source = "TTM実績成長率注入(General 1segment placeholder)"
        else:
            real_source = "不明(推定不可)"
        note = f"表示上は「{displayed_source}」だがsegment_configured=Falseのため実際は{real_source}"

    return flag, note, {"displayed_source": displayed_source, "segment_configured": segment_configured,
                         "real_source_guess": real_source if flag else displayed_source}


def check_c_data_jump(repo_root, ticker, lookback_years=6, jump_ratio=2.0, down_ratio=None,
                       section="pl", field="revenue"):
    """C: SECデータ異常ジャンプ検知(直近lookback_years年の年次{section}.{field}で
    YoY倍率がjump_ratio倍以上/down_ratio倍以下)

    [[DATA-JUMP-CHECK-GENERALIZE-1]]でRevenue専用のハードコードから
    section/fieldパラメータ化した。Revenue呼び出し元（report_consistency_check.py
    CHECK-21・WARN-21）は本改修前と完全に同一のデフォルト引数
    （section="pl", field="revenue", jump_ratio=2.0, down_ratio=None→1/jump_ratio=0.5）
    で呼び出されるため動作は変わらない。

    down_ratio: 下振れ判定用の閾値。Noneの場合は1/jump_ratio（上振れ・下振れが
    互いに逆数の対称な閾値）を使用する。CapEx（上振れ8.0倍/下振れ0.15倍）のように
    上振れ・下振れが逆数の関係にない非対称な閾値が必要なフィールド向けに
    明示指定できるようにした。
    """
    if down_ratio is None:
        down_ratio = 1 / jump_ratio
    years = _get_annual_years(repo_root, ticker)
    target_years = years[-lookback_years:]
    vals = [(y, (_load_annual(repo_root, ticker, y).get(section) or {}).get(field)) for y in target_years]

    flag = False
    jumps = []
    for i in range(1, len(vals)):
        y0, v0 = vals[i - 1]
        y1, v1 = vals[i]
        if v0 is None or v1 is None or v0 == 0:
            continue
        ratio = v1 / v0
        if ratio >= jump_ratio or ratio <= down_ratio:
            flag = True
            jumps.append(f"{y0}({v0/1e6:.0f}M)→{y1}({v1/1e6:.0f}M) 倍率{ratio:.2f}x")

    return flag, jumps, vals


def check_d_invested_capital(repo_root, ticker, ic_revenue_threshold=0.15):
    """D: ROIC投下資本の妥当性(直近期の株主資本マイナス or 投下資本が売上のic_revenue_threshold未満)"""
    years = _get_annual_years(repo_root, ticker)
    if not years:
        return False, "SECデータなし", {}
    latest_year = years[-1]
    ann = _load_annual(repo_root, ticker, latest_year)
    bs = ann.get("bs") or {}
    pl = ann.get("pl") or {}
    equity = bs.get("stockholders_equity") or bs.get("total_equity") or 0
    lt_debt = bs.get("long_term_debt") or 0
    st_debt = bs.get("short_term_debt") or 0
    cash = bs.get("cash_and_equivalents") or 0
    invested_capital = equity + lt_debt + st_debt - cash
    revenue = pl.get("revenue") or 0

    flag = False
    reason = ""
    if equity < 0:
        flag = True
        severity = "投下資本自体もマイナス(ROIC定義不能)" if invested_capital <= 0 else "投下資本はプラス(自社株買い等で分母圧縮)"
        reason = f"{latest_year}期 株主資本マイナス(${equity/1e9:.2f}B)。{severity}"
    elif invested_capital > 0 and revenue > 0 and invested_capital < ic_revenue_threshold * revenue:
        flag = True
        reason = f"{latest_year}期 投下資本(${invested_capital/1e9:.2f}B)が売上(${revenue/1e9:.2f}B)の{ic_revenue_threshold:.0%}未満と小さく、ROICが跳ね上がりやすい構造"

    return flag, reason, {"fiscal_year": latest_year, "equity": equity, "invested_capital": invested_capital, "revenue": revenue}


def check_e_transition_sample(repo_root, ticker, min_transitions=5):
    """E: HypeCore遷移確率のサンプル数(現在ステージからの遷移観測がmin_transitions件未満)"""
    path = os.path.join(repo_root, "docs", "value-monitor", "hypecore", "data", f"{ticker}_poc.json")
    if not os.path.exists(path):
        return False, "hypecoreデータなし", None
    monthly = (_load_json(path).get("monthly")) or []
    if not monthly:
        return False, "monthly空", None
    cur_stage = monthly[-1]["stage"]
    cnt = {0: {}, 1: {}, 2: {}, 3: {}, 4: {}}
    for i in range(len(monthly) - 1):
        f_, t_ = monthly[i]["stage"], monthly[i + 1]["stage"]
        cnt[f_][t_] = cnt[f_].get(t_, 0) + 1
    tot = sum(cnt.get(cur_stage, {}).values())
    flag = tot < min_transitions
    reason = f"現在ステージS{cur_stage}からの遷移観測数={tot}件({min_transitions}件未満)" if flag else ""
    return flag, reason, {"current_stage": cur_stage, "transition_count": tot, "total_months": len(monthly)}


def check_f_eps_analyzer_delta(latest, delta_threshold=10.0):
    """F: EPS Analyzer PER比較(GAAP PERと調整後PERの乖離、
    [[SCREENING-SIGNAL-INTEGRATION-EPIC-1]]元EPS-ANALYZER-INTEGRATE-1参照)

    components.per(GAAP基準PER)・components.per_adjusted(調整後PER)の両方が
    存在しper>0の場合のみ判定。delta=per_adjusted-perがdelta_threshold(デフォルト
    ±10x)以上ならSBC等の影響で見かけの割安度が歪んでいる可能性としてフラグする。
    """
    comp = latest.get("components") or {}
    per = comp.get("per")
    per_adjusted = comp.get("per_adjusted")

    flag = False
    reason = ""
    delta = None
    if per is not None and per_adjusted is not None and per > 0:
        delta = per_adjusted - per
        if abs(delta) >= delta_threshold:
            flag = True
            reason = (f"調整後PERとGAAP PERの差が{delta:+.1f}xと大きく、"
                       f"SBC等の影響で見かけの割安度が歪んでいる可能性")

    return flag, reason, {"per_gaap": per, "per_adjusted": per_adjusted, "delta": delta}


def check_g_rice_efficiency(latest, low_threshold=1.0):
    """G: RICE投資効率チェック(rice.base.rice<low_threshold(デフォルト1.0=低効率)を
    フラグ。TANUKI SCORE=BUYとの組み合わせも記録する。[[SCREENING-SIGNAL-
    INTEGRATION-EPIC-1]]元RICE-INTEGRATE-1参照)

    rice.base.riceが存在しない銘柄(RICE Available=false、Revenue/CapExデータ
    不足等)はflag=Falseで理由のみ記録する。
    """
    rice = latest.get("rice") or {}
    base = rice.get("base") or {}
    rice_base = base.get("rice")
    bear = rice.get("bear") or {}
    bull = rice.get("bull") or {}
    tanuki_score = latest.get("tanuki_score")

    flag = False
    reason = ""
    buy_and_low_efficiency = False
    if rice_base is None:
        reason = "RICE計算不可（Revenue/CapExデータ不足等）"
    elif rice_base < low_threshold:
        flag = True
        buy_and_low_efficiency = tanuki_score == "BUY"
        if buy_and_low_efficiency:
            reason = (f"RICE={rice_base:.3f}で低効率(<{low_threshold:.1f})。"
                       f"TANUKI SCORE=BUY(割安判定)と組み合わさり"
                       f"「割安だが再投資効率が低い」候補")
        else:
            reason = f"RICE={rice_base:.3f}で低効率(<{low_threshold:.1f})"

    return flag, reason, {
        "rice_base": rice_base,
        "rice_bear": bear.get("rice"),
        "rice_bull": bull.get("rice"),
        "tanuki_score": tanuki_score,
        "buy_and_low_efficiency": buy_and_low_efficiency,
    }


def check_h_analyst_vs_iv(latest, threshold=50.0):
    """H: アナリストコンセンサスとTANUKI理論株価(IV)の乖離チェック(
    [[SCREENING-SIGNAL-INTEGRATION-EPIC-1]]元ANALYST-VS-IV-INTEGRATE-1参照)

    components.analyst_vs_iv((アナリスト目標株価中央値-IV)/IV*100)が存在し
    abs(analyst_vs_iv)>=threshold(デフォルト50pt)ならフラグする。正の値は
    アナリスト目標がIVより高い=TANUKIがアナリストより弱気、負の値は
    アナリスト目標がIVより低い=TANUKIがアナリストより強気であることを意味する。
    """
    comp = latest.get("components") or {}
    analyst_vs_iv = comp.get("analyst_vs_iv")

    flag = False
    reason = ""
    if analyst_vs_iv is not None and abs(analyst_vs_iv) >= threshold:
        flag = True
        direction = "弱気" if analyst_vs_iv > 0 else "強気"
        reason = (f"TANUKIとアナリストの意見が大きく割れている"
                  f"（analyst_vs_iv={analyst_vs_iv:+.1f}%、TANUKIが{direction}）")

    return flag, reason, {
        "analyst_vs_iv": analyst_vs_iv,
        "analyst_target_median": comp.get("analyst_target_median"),
        "current_price": comp.get("current_price"),
    }


def check_ticker(repo_root, ticker):
    """1銘柄についてA〜E全チェックを実行しdictで返す。latest.json不在ならNoneを返す"""
    latest_path = os.path.join(repo_root, "docs", "value-monitor", "tanuki_valuation", "data", ticker, "latest.json")
    if not os.path.exists(latest_path):
        return None
    latest = _load_json(latest_path)

    a_flag, a_reasons, a_detail = check_a_growth_vs_actual(latest)
    b_flag, b_note, b_detail = check_b_source_label(repo_root, latest, ticker)
    c_flag, c_jumps, _ = check_c_data_jump(repo_root, ticker)
    d_flag, d_reason, d_detail = check_d_invested_capital(repo_root, ticker)
    e_flag, e_reason, e_detail = check_e_transition_sample(repo_root, ticker)
    f_flag, f_reason, f_detail = check_f_eps_analyzer_delta(latest)
    g_flag, g_reason, g_detail = check_g_rice_efficiency(latest)
    h_flag, h_reason, h_detail = check_h_analyst_vs_iv(latest)

    return {
        "ticker": ticker,
        "A_growth_vs_actual": {"flag": a_flag, "reasons": a_reasons, "detail": a_detail},
        "B_source_label": {"flag": b_flag, "note": b_note, "detail": b_detail},
        "C_data_jump": {"flag": c_flag, "jumps": c_jumps},
        "D_invested_capital": {"flag": d_flag, "reason": d_reason, "detail": d_detail},
        "E_transition_sample": {"flag": e_flag, "reason": e_reason, "detail": e_detail},
        "F_eps_analyzer_delta": {"flag": f_flag, "reason": f_reason, "detail": f_detail},
        "G_rice_efficiency": {"flag": g_flag, "reason": g_reason, "detail": g_detail},
        "H_analyst_vs_iv": {"flag": h_flag, "reason": h_reason, "detail": h_detail},
        "any_flag": any([a_flag, b_flag, c_flag, d_flag, e_flag, f_flag, g_flag, h_flag]),
    }


def _all_tanuki_tickers(repo_root):
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from common.sec_data import tickers
    return tickers.get_tanuki_tickers()


def main():
    parser = argparse.ArgumentParser(description="DCF前提・ROIC投下資本の妥当性を機械チェックする")
    parser.add_argument("tickers", nargs="*", help="対象ティッカー(省略時はcik_lookup.csvでtanuki=trueの全銘柄)")
    parser.add_argument("--output", default=None, help="出力JSONパス(省略時は標準出力にサマリのみ表示)")
    args = parser.parse_args()

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    tickers = args.tickers or _all_tanuki_tickers(repo_root)

    results = []
    skipped = []
    for t in tickers:
        r = check_ticker(repo_root, t)
        if r is None:
            skipped.append(t)
        else:
            results.append(r)

    flagged = [r for r in results if r["any_flag"]]
    counts = {k: sum(1 for r in results if r[f"{k}_{name}"]["flag"])
              for k, name in [("A", "growth_vs_actual"), ("B", "source_label"), ("C", "data_jump"),
                               ("D", "invested_capital"), ("E", "transition_sample"),
                               ("F", "eps_analyzer_delta"), ("G", "rice_efficiency"),
                               ("H", "analyst_vs_iv")]}

    output = {
        "generated_at": datetime.now().isoformat(),
        "requested_count": len(tickers),
        "checked_count": len(results),
        "skipped_no_latest_json": skipped,
        "flagged_count": len(flagged),
        "flag_counts": counts,
        "results": results,
    }

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        print(f"出力完了: {args.output} (チェック対象{len(results)}件・フラグ該当{len(flagged)}件)")
    else:
        print(f"チェック対象: {len(results)}件 / フラグ該当: {len(flagged)}件")
        print("内訳:", counts)
        for r in flagged:
            flags = [k for k, name in [("A", "growth_vs_actual"), ("B", "source_label"), ("C", "data_jump"),
                                        ("D", "invested_capital"), ("E", "transition_sample"),
                                        ("F", "eps_analyzer_delta"), ("G", "rice_efficiency"),
                                        ("H", "analyst_vs_iv")]
                      if r[f"{k}_{name}"]["flag"]]
            print(f"  {r['ticker']}: {','.join(flags)}")


if __name__ == "__main__":
    main()
