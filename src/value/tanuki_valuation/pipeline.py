"""
TANUKI VALUATION - Pipeline v2.1
全ティッカーを処理し、latest.jsonを生成

使用方法:
    python pipeline.py
    python pipeline.py TSLA PLTR  # 特定ティッカーのみ

v2.1変更点:
    - AI検証機能（validator.py）を統合
    - latest.jsonに"validation"フィールドを追加

v2.2変更点:
    - cik_lookup.csv の tanuki 列対応（false の銘柄をスキップ）
"""

import json
import os
import sys
from datetime import date, datetime, timedelta
from typing import List, Optional

# 同一ディレクトリからのインポート
from data_fetcher import TanukiDataFetcher
from core_calculator import KoichiValuationCalculator
from validator import validate_calculation
from growth_sanity import check_growth_sanity, calc_fundamental_growth
import segment_config as _seg_cfg

_SCRIPT_DIR_FOR_IMPORT = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT_FOR_IMPORT = os.path.dirname(os.path.dirname(os.path.dirname(_SCRIPT_DIR_FOR_IMPORT)))
if _REPO_ROOT_FOR_IMPORT not in sys.path:
    sys.path.insert(0, _REPO_ROOT_FOR_IMPORT)
from common.sec_data.contracts import Classification  # GATE2-PHASE3B-1③-b
from common.sec_data.layer3_builder import (  # フェーズD Step2-1
    build_ticker_store,
    get_field_entries,
    get_long_term_debt_latest,
)

# common/market_data - [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-4:
# 次回決算日取得（yf.Ticker(ticker).calendar直接呼び出し）を
# common.market_data.reader経由に切替。他フィールドは着手順序4-2
# （data_fetcher.py切替）で既に対応済みのため、本ファイルではこの
# .calendar呼び出し1箇所のみが対象。data_fetcher.py/valuation_fetcher.py
# 切替と同じHAS_MARKET_DATAガードパターンを踏襲（sys.pathは上記
# _REPO_ROOT_FOR_IMPORTで解決済み）。
try:
    from common.market_data.reader import get_calendar as _md_get_calendar
    HAS_MARKET_DATA = True
except Exception:
    _md_get_calendar = None
    HAS_MARKET_DATA = False

# FCF-CONVRATE②（TRUST-SUMMARY-EPIC-1）: FCF実力推定の業種平均固定転換率
# （fcf_conversion_config.json の sector_conversion_rates）が、業界サイクルにより
# 年度ごとのFCFが大きく変動する銘柄を表現できない構造的限界があると原因分析で
# 確定した銘柄の個別リスト。cv・divergence_ratio 等の閾値による自動判定は、
# LLYがDOCNを両軸で完全に上回るなど数学的に分離不可能と判明したため採用しない。
# 将来3件目以降を追加する場合も、業界サイクル起因かどうかの個別原因分析を経てから
# 手動追加すること（閾値による自動追加は行わない）。
FCF_CYCLICAL_VOLATILITY_TICKERS = {"SITM", "LITE"}

# GROWTH-STRUCTURAL-MISMATCH-CANDIDATES-1（TRUST-SUMMARY-EPIC-1骨子②）:
# growth_sanityが警告を出す銘柄のうち、ハイパーグロース事業と成熟業種平均
# （Damodaran業種分類）との構造的なミスマッチが原因分析で確定した銘柄の
# 個別リスト。FCF_CYCLICAL_VOLATILITY_TICKERSと同型の手動リスト方式
# （閾値による自動判定は行わない）。TERのみ業界平均比ではなく自社実績比
# での警告のため、注記文言はTER専用に分岐させる（他13件と機械的に共通化
# しない）。Classification（BUY/WATCH等）には一切影響しない、注記表示のみ。
GROWTH_STRUCTURAL_MISMATCH_TICKERS = {
    "AMD", "NVDA", "ONDS", "ASTS", "BKNG", "BROS", "ELF", "KULR",
    "LLY", "TER", "XOM", "ALAB", "IONQ", "RCAT",
}

# FCF-CONVRATE②派生（KO-SPIR-CF-CAUSE-UNCONFIRMED-1）: FCF_CYCLICAL_VOLATILITY_TICKERSと
# 同型の個別ティッカーリスト方式だが、原因が「業界サイクル変動」ではなく銘柄固有の
# 一過性項目（税務訴訟・M&A偶発対価の精算・事業売却益等）であると10-K一次情報で
# 確定した銘柄向け。ティッカーごとに理由・金額・出所が異なる具体的な説明文を持つため、
# 単純なSetではなくティッカー→説明（dict）で保持する。
# KOのみ複数年度（2024年・2025年）にまたがる別々の一過性項目が存在するため、
# 年度キー付きのネスト辞書とする。SPIRは「NI/OCF乖離＝一過性」「減収＝構造的変化」の
# 2つの性質が異なる理由を区別して保持する（カテゴリキー）。
# 将来3件目以降を追加する場合も、閾値による自動判定ではなく個別の原因分析
# （10-K MD&A等の一次情報確認）を経てから手動追加すること。
FCF_TRANSIENT_ITEM_EXPLANATIONS = {
    "KO": {
        2024: "IRS移転価格税務訴訟（2007-2009年度分）の追徴課税$6.0Bを一括納付"
              "（2024年9月、控訴審係属中で還付可能性あり。10-K FY2024 Note 12参照）",
        2025: "2020年fairlife買収の偶発対価（業績連動マイルストーン）$6.1Bを最終決済"
              "（2025年3月。10-K FY2025 Note 17/18参照）",
    },
    "SPIR": {
        "ni_ocf_divergence": "2025年4月完了の海事(maritime)事業売却に伴う非現金の売却益"
                              "$154.3M（投資CF区分計上のためOCF算定上はNIから控除）が主因",
        "revenue_decline": "同じ海事事業売却による連結売上ベースの恒久的縮小",
    },
}


# CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1: スピンオフ・カーブアウト型／破産再生型の
# 法人再編でCIKが断絶しており、旧CIKへの接続を行わない方針が確定している銘柄。
# ①同一事業継続型（MRVL/GOOGL/AVGO/DELL）は cik_history.json 経由で旧CIKの
# データを実際に統合したため、この注記の対象ではない（DELLのみ非公開期間の
# 別注記がある。DELL_PRIVATE_PERIOD_NOTE参照）。
# ②スピンオフ・カーブアウト型は「親会社連結からの区分推定値」、③破産再生型は
# 「fresh-start会計で連続性自体が断絶」のため、旧データを実績として接続しない
# （FCF_CYCLICAL_VOLATILITY_TICKERS等と同型の手動リスト方式。閾値による
# 自動判定は行わない）。
CIK_DISCONTINUITY_TICKERS = {
    "CEG": "2022年2月ExelonからのスピンオフによりConstellation Energy Corp発足。"
           "スピンオフ以前のデータは親会社連結からの区分推定値であり接続していません。",
    "LITE": "2015年8月JDS Uniphase(JDSU)の会社分割によりLumentum Holdings発足。"
            "分割以前のデータは親会社(JDSU)連結からの区分推定値であり接続していません。",
    "ABBV": "2013年1月Abbott LaboratoriesからのスピンオフによりAbbVie Inc.発足。"
            "スピンオフ以前のデータは親会社連結からの区分推定値であり接続していません。",
    "GEV": "2024年4月General ElectricからのスピンオフによりGE Vernova Inc.発足。"
           "スピンオフ以前のデータは親会社連結からの区分推定値であり接続していません。",
    "SN": "2023年7月、香港上場JS Global LifestyleからのスピンオフによりSharkNinja, Inc.が"
          "米国単独上場。分離以前のデータは接続していません。",
    "CON": "2024年11月Select Medical Holdingsからの完全スピンオフによりConcentra Group "
           "Holdings発足（2024年7月に一部先行IPO）。分離以前のデータは親会社連結からの"
           "区分推定値であり接続していません。",
    "VST": "2016年のEnergy Future Holdings破産手続き(Chapter 11)に伴うfresh-start会計により"
           "Vistra Energy Corp発足。破産前後で会計上の連続性自体が断絶しているため接続していません。",
}

# DELL固有: 旧CIK(826083)を統合してもFY2013〜FY2016の自社10-K申告ギャップ
# （2013年LBOによる非公開化〜2016年EMC統合完了後の初回申告〈FY2017〉まで）は
# 埋まらない。実データ検証済み: FY2015・FY2016はFY2017 10-Kの比較年度再掲データ
# （本人データではない）のみ存在し、FY2014は比較年度ウィンドウにも入らず欠落する。
DELL_PRIVATE_PERIOD_NOTE = (
    "2013年LBOによる非公開化(最終自社10-K: FY2012, 期末2013-02-01)〜2016年EMC統合"
    "完了後の初回自社10-K(FY2017, filed 2017-03-31)まで、自社10-K申告自体が"
    "存在しない期間があります。FY2015/FY2016はFY2017 10-Kの比較年度再掲データ"
    "（本人データではない）のみ、FY2014は値が完全に欠落します。"
)


def _dilution_severity_info(dil_pct: float | None) -> tuple:
    """希薄化率から (severity, badge, report_comment) を返す"""
    if dil_pct is None:
        return None, None, None
    if dil_pct < 0:
        return "positive", "自社株買いによる株主還元 ✅", "株式数が減少し1株あたり価値が向上している。"
    elif dil_pct < 3:
        return "low", "許容範囲内 ✅", "希薄化はほぼ無視できるレベル。"
    elif dil_pct < 10:
        return "medium", "やや高い希薄化 ℹ️ 要注意", f"継続なら1株価値を年率{dil_pct:.1f}%毀損。SBC水準を確認推奨。"
    elif dil_pct < 30:
        return "high", "高い希薄化 ⚠️ SBC・増資を確認", f"継続なら1株価値を年率{dil_pct:.1f}%毀損。SBC・増資の内訳を確認。"
    elif dil_pct < 50:
        return "severe", "深刻な希薄化 ⚠️ 株主価値毀損リスク大", f"継続なら1株価値を年率{dil_pct:.1f}%毀損。株主価値毀損リスク大。"
    else:
        return "critical", "極度の希薄化 🚨 IPO直後か継続増資か要確認", f"IPO後の株式発行が主因の可能性。継続なら1株価値を年率{dil_pct:.1f}%毀損。"


def _calculate_erp(forward_eps, current_price, risk_free_rate: float = 0.043) -> tuple:
    """ERP（株式リスクプレミアム）と益利回りを計算する（[[ERP-DUAL-CALC-1]]）。

    ERP = Forward Earnings Yield - Risk Free Rate
    益利回り = ForwardEPS / 現在株価、Rf = WACC計算に使用した10年国債利回り

    従来`_save_result()`と`_generate_report()`がそれぞれ独立に同じ計算式を
    再実装していたため、共通関数に統合（丸めは呼び出し側の用途に応じて
    行う。本関数は丸めなしの生値を返す）。

    forward_epsが数値でない、またはcurrent_priceが0以下の場合は
    (None, None) を返す。
    """
    if (
        forward_eps is None
        or not isinstance(forward_eps, (int, float))
        or not current_price
        or current_price <= 0
    ):
        return None, None
    earnings_yield = forward_eps / current_price
    erp = earnings_yield - risk_free_rate
    return earnings_yield, erp


# [[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]（2026-09-05）: STONKS SILO
# （discover/stonks-silo/src/analyzer.py::_margin_breakeven()）が持つ
# 安全策一式（理由コード・傾き上限・異常値除外）をこちら側にも追加し、
# 「常に直近4点のOLS回帰」という回帰方式自体はSTONKS SILO側と揃えた
# （対象指標は調整後EPSのまま維持、STONKS SILO側のマージン比率とは別軸）。
#
# 閾値はEPS Analyzer対象100銘柄・全1723四半期の実データ分布から校正
# （マージン比率用に校正されたSTONKS SILO側の閾値をそのまま転用しない）:
# - 正常範囲は概ね-9.5〜+18.2（p1=-1.35, p99=+8.72, mean=1.20, stdev=4.10）。
#   唯一の極端な外れ値はLITE 2026Q4（adjusted_eps=-95.0）で、10-Kの通期
#   実績が単一四半期として誤抽出された別種のバグ
#   （[[EPS-LITE-ANNUAL-AS-QUARTERLY-1]]として別途新規登録、本タスクでは
#   対象外）。EPS_MAGNITUDE_CAP=30はこの唯一の異常値を確実に除外しつつ、
#   正常範囲の最大値(+18.2)に約65%の余裕を残す
# - 現在赤字の16銘柄で直近4四半期OLS傾きを実測した結果、LITEの異常値
#   混入分(-28.73/四半期)を除けば最大でも±0.31/四半期（CRWV）。
#   EPS_SLOPE_CAP_PER_QUARTER=2.0はこれに対し約6倍の余裕を持たせつつ、
#   LITEの異常混入ケースは1桁以上下回り確実に除外する
EPS_MAGNITUDE_CAP = 30.0          # |adjusted_eps|がこれを超える四半期は回帰対象から除外
EPS_SLOPE_CAP_PER_QUARTER = 2.0   # 傾きがこれを超える改善は信頼せずNO_TRENDとする
BREAKEVEN_HORIZON_QUARTERS = 20   # 5年（STONKS SILOのhorizon=5年と統一）


def compute_eps_breakeven(quarters_list: list, current_year: int) -> tuple:
    """
    EPS Analyzer quarterly.jsonの"quarters"（filing_date降順、直近が
    index 0）から、調整後EPSの直近4四半期OLS回帰で黒字化四半期を予測する。

    STONKS SILO（discover/stonks-silo/src/analyzer.py::_margin_
    breakeven()）と回帰の方式（常に直近4点のOLS）・安全策の考え方
    （理由コード・傾き上限・異常値除外）を揃えている。対象指標は
    調整後EPS（STONKS SILO側はマージン比率）のまま維持。

    Returns: (breakeven_estimate, breakeven_reason)
    reason codes: ACHIEVED / PREDICTED / NO_TREND / NO_DATA / TOO_FAR
    """
    candidates = quarters_list[:4]
    # 値が存在し・株式数構造が別物でなく（[[EPS-LOAR-1]]の
    # SHARE_STRUCTURE_MISMATCH）・EPS絶対値が異常でない
    # （EPS_MAGNITUDE_CAP以内）ものだけを回帰対象とする
    recent_eps = [
        q.get("adjusted_eps") for q in candidates
        if q.get("adjusted_eps") is not None
        and "SHARE_STRUCTURE_MISMATCH" not in (q.get("special_flags") or [])
        and abs(q.get("adjusted_eps")) <= EPS_MAGNITUDE_CAP
    ]
    if len(recent_eps) < 2:
        return None, "NO_DATA"
    if recent_eps[0] >= 0:
        # STONKS SILO側（_breakeven_estimate()の直近黒字ショートサーキット）
        # と同じ規約: 「達成済み」は年ではなくreasonのみで表す
        # （breakeven_estimateはNoneのまま）
        return None, "ACHIEVED"

    vals = list(reversed(recent_eps))  # oldest→newest
    n = len(vals)
    x_mean = (n - 1) / 2.0
    y_mean = sum(vals) / n
    denom = sum((i - x_mean) ** 2 for i in range(n))
    if denom == 0:
        return None, "NO_DATA"

    slope = sum((i - x_mean) * (vals[i] - y_mean) for i in range(n)) / denom
    if slope <= 0 or slope > EPS_SLOPE_CAP_PER_QUARTER:
        return None, "NO_TREND"

    intercept = y_mean - slope * x_mean
    x_zero = -intercept / slope
    quarters_until = max(x_zero - (n - 1), 0)
    if quarters_until > BREAKEVEN_HORIZON_QUARTERS:
        return None, "TOO_FAR"

    breakeven_year = round(current_year + quarters_until / 4.0)
    return breakeven_year, "PREDICTED"


class TanukiValuationPipeline:
    """TANUKI VALUATION パイプライン"""

    def __init__(self, output_dir: str = None, use_ai_validation: bool = True):
        self.fetcher = TanukiDataFetcher()
        self.use_ai_validation = use_ai_validation

        if output_dir:
            self.output_dir = output_dir
            repo_root = os.path.dirname(os.path.dirname(os.path.dirname(output_dir)))
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            repo_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
            self.output_dir = os.path.join(repo_root, "docs", "value-monitor", "tanuki_valuation", "data")

        self.repo_root = repo_root

        eps_data_dir = os.path.join(
            repo_root, "docs", "value-monitor", "adjusted_eps_analyzer", "data"
        )
        sec_data_dir = os.path.join(repo_root, "common", "sec_data", "data")
        self.calculator = KoichiValuationCalculator(
            eps_data_dir=eps_data_dir,
            sec_data_dir=sec_data_dir,
        )
        
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"   出力先: {self.output_dir}")

        # フェーズD Step2-1: Layer3ストアのticker単位キャッシュ。
        # 希薄化率・TTM信頼性判定・LTDebtフォールバック（2箇所）・
        # _estimate_ttm_operating_income()・_calc_moat_inputs()の
        # 6箇所が同一tickerでbuild_ticker_store()を呼ぶため、
        # company_facts.jsonの重複パースを避けるためインスタンス単位で
        # キャッシュする。
        self._layer3_store_cache: dict = {}

    def _get_layer3_store(self, ticker: str):
        """build_ticker_store()の結果をticker単位でキャッシュして返す（フェーズD Step2-1）。"""
        ticker = ticker.upper()
        if ticker not in self._layer3_store_cache:
            self._layer3_store_cache[ticker] = build_ticker_store(ticker)
        return self._layer3_store_cache[ticker]

    def run(self, tickers: Optional[List[str]] = None) -> dict:
        print("=" * 60)
        print("TANUKI VALUATION Phase 4 実行開始")
        print(f"  Koichi式 v{KoichiValuationCalculator.VERSION}（動的WACC + 3段階DCF + FCFベース自動判定 + AI検証）")
        print("=" * 60)
        
        if tickers is None:
            tickers = self._load_tickers_from_csv()
        else:
            tickers = self._filter_tanuki_tickers(tickers)

        results = {}
        success_count = 0
        error_count = 0
        validation_stats = {"pass": 0, "warn": 0, "fail": 0, "error": 0}

        for ticker in tickers:
            print(f"\n{'─' * 40}")
            print(f"🔄 処理中: {ticker}")
            print(f"{'─' * 40}")
            
            try:
                financials = self.fetcher.get_financials(ticker)
                
                if "error" in financials:
                    print(f"❌ {ticker} スキップ: {financials['error']}")
                    error_count += 1
                    continue
                
                if financials.get("diluted_shares", 0) <= 100_000:
                    print(f"❌ {ticker} スキップ: diluted_shares不足")
                    error_count += 1
                    continue

                self._inject_ttm_for_general_segment(ticker)

                # RICE-1: ROIC/WACC_Rm を事前計算してDCF前に注入
                # [[MOAT-SCORE-PARTIAL-NULL-1]]: 理由コードはmoat_score側の
                # None原因別処理にのみ使う。RICE-1(vc_factor)は値のNone判定
                # のみで動くため、理由コード追加による既存挙動への影響はない。
                _roic_wacc, _roic_reason = self._calc_roic_wacc_ratio(ticker)
                if _roic_wacc is not None:
                    financials["roic_wacc_ratio"] = _roic_wacc

                # ALPHA-REDESIGN-1: Moat Score計算インプットをDCF前に注入
                _roic_val = (_roic_wacc * 0.10) if _roic_wacc is not None else None
                _moat_inputs = self._calc_moat_inputs(ticker, roic=_roic_val, roic_reason=_roic_reason)
                financials.update(_moat_inputs)

                valuation = self.calculator.calculate_pt(financials)

                if "error" in valuation:
                    print(f"❌ {ticker} 計算エラー: {valuation['error']}")
                    error_count += 1
                    continue

                try:
                    validation = validate_calculation(
                        ticker, 
                        valuation, 
                        use_ai=self.use_ai_validation
                    )
                    valuation["validation"] = validation
                    
                    overall = validation.get("overall", "ERROR")
                    if overall == "PASS":
                        print(f"   ✅ 検証パス")
                        validation_stats["pass"] += 1
                    elif overall == "WARN":
                        print(f"   ⚠️  検証警告: {self._get_warn_details(validation)}")
                        validation_stats["warn"] += 1
                    else:
                        print(f"   ❌ 検証失敗: {self._get_warn_details(validation)}")
                        validation_stats["fail"] += 1
                        
                except Exception as e:
                    print(f"   ⚠️  検証エラー: {e}")
                    valuation["validation"] = {
                        "validated_at": datetime.now().strftime("%Y-%m-%d"),
                        "model": "error",
                        "checks": {},
                        "overall": "ERROR",
                        "ai_comment": str(e)
                    }
                    validation_stats["error"] += 1

                valuation = self._save_result(ticker, valuation, financials)
                results[ticker] = valuation
                success_count += 1
                
                per_share = valuation.get("intrinsic_value_per_share", 0)
                current = financials.get("current_price", 0)
                upside = valuation.get("upside_percent", 0)
                
                print(f"✅ {ticker} 完了:")
                print(f"   理論株価: ${per_share:,.2f}")
                print(f"   現在株価: ${current:,.2f}")
                print(f"   乖離率: {upside:+.1f}%")

            except Exception as e:
                print(f"❌ {ticker} 例外発生: {e}")
                error_count += 1
                import traceback
                traceback.print_exc()

        print("\n" + "=" * 60)
        print("🎉 TANUKI VALUATION 実行完了")
        print(f"   成功: {success_count} / 失敗: {error_count}")
        print(f"   検証結果: PASS={validation_stats['pass']} WARN={validation_stats['warn']} FAIL={validation_stats['fail']} ERROR={validation_stats['error']}")
        print(f"   出力先: {self.output_dir}")
        print("=" * 60)

        if results:
            self._save_tickers_index(list(results.keys()))

        return results

    def _load_tickers_from_csv(self) -> List[str]:
        """
        cik_lookup.csv から TANUKI 対象銘柄を取得

        tickers.get_tanuki_tickers()はtanuki列欠損時に対象外扱いとなる点に注意。
        現状cik_lookup.csv全行に値が設定されているため実害なし。
        """
        csv_path = os.path.join(self.repo_root, "config", "cik_lookup.csv")
        if not os.path.exists(csv_path):
            print(f"❌ エラー: {csv_path} が見つかりません")
            sys.exit(1)

        if self.repo_root not in sys.path:
            sys.path.insert(0, self.repo_root)
        from common.sec_data import tickers as _tickers

        all_tickers = _tickers.get_all_tickers()
        tanuki_set = set(_tickers.get_tanuki_tickers())
        skipped = [t for t in all_tickers if t not in tanuki_set]
        if skipped:
            print(f"   ℹ️  TANUKIスキップ銘柄: {', '.join(skipped)}")
        return [t for t in all_tickers if t in tanuki_set]

    def _filter_tanuki_tickers(self, tickers: List[str]) -> List[str]:
        """CLI引数等でticker明示指定時にtanuki=trueのみへ絞り込む。

        ZS-TICKERS-LEAK-1: run(tickers=None)の全銘柄バッチ経路のみ
        tanuki=trueへフィルタしており、tickers引数を明示指定した経路
        （CLI引数・latest.json再スキャン等での対象選定）はフィルタを
        一切通らず、tanuki=falseへ変更済みの銘柄（ZS等）でも無条件に
        処理・再生成してしまっていた（tickers.json混入の実害を確認済み）。

        `get_tanuki_tickers()`ではなく`get_registrable_tickers()`
        （2026-09-03新設）を使う: 前者はstatus=provisioning（登録処理中、
        [[REGISTER-FLOW-REDESIGN-1]]方針2）も除外するため、新規銘柄登録
        オーケストレーション（`common/registration/register_ticker.py`
        Step 3）がprovisioning中のティッカーを明示指定して実行できなく
        なってしまう。デフォルト・バッチ経路（`tickers=None`）は
        `_load_tickers_from_csv()`経由で引き続きprovisioningを除外する
        ため、スケジュール実行等が登録処理中のティッカーを誤って
        自動処理することはない。
        """
        if self.repo_root not in sys.path:
            sys.path.insert(0, self.repo_root)
        from common.sec_data import tickers as _tickers

        tanuki_set = set(_tickers.get_registrable_tickers("tanuki"))
        excluded = [t for t in tickers if t.upper() not in tanuki_set]
        if excluded:
            print(
                f"   ⚠️  tanuki=false のため除外: {', '.join(excluded)}"
                f"（cik_lookup.csvでtanuki=trueに変更しない限り処理されません）"
            )
        return [t for t in tickers if t.upper() in tanuki_set]

    def _get_warn_details(self, validation: dict) -> str:
        checks = validation.get("checks", {})
        failed = [k for k, v in checks.items() if not v.get("pass", True)]
        return ", ".join(failed) if failed else "unknown"

    def _load_eps_map(self) -> dict:
        if hasattr(self, "_eps_summary_cache"):
            return self._eps_summary_cache
        eps_path = os.path.join(
            self.repo_root, "docs", "value-monitor", "adjusted_eps_analyzer", "data", "summary.json"
        )
        result = {}
        if os.path.exists(eps_path):
            try:
                with open(eps_path, encoding="utf-8") as f:
                    data = json.load(f)
                result = {t["ticker"]: t for t in data.get("tickers", [])}
            except Exception:
                pass
        self._eps_summary_cache = result
        return result

    def _load_live_fg(self) -> float:
        """market_data.json の fear_greed.score を読む（ARCH-SCORE-SYNC-1: JS側calcTimingと同一入力に揃える）"""
        if hasattr(self, "_live_fg_cache"):
            return self._live_fg_cache
        fg = 50
        mkt_path = os.path.join(
            self.repo_root, "docs", "market-monitor", "market-pulse", "data", "market_data.json"
        )
        if os.path.exists(mkt_path):
            try:
                with open(mkt_path, encoding="utf-8") as f:
                    data = json.load(f)
                if data:
                    fg = data[-1].get("fear_greed", {}).get("score", 50)
            except Exception:
                pass
        self._live_fg_cache = fg
        return fg

    @staticmethod
    def _calc_timing(upside, fg, stage) -> int:
        """JS calcTiming(upside, fg, stage) のPython移植（ARCH-SCORE-SYNC-1）"""
        s = 0
        if upside is not None:
            if upside > 30:
                s += 40
            elif upside >= 10:
                s += 25
            elif upside >= 0:
                s += 10
        if fg < 30:
            s += 40
        elif fg < 50:
            s += 25
        elif fg < 70:
            s += 10
        if stage is not None:
            if stage == 1:
                s += 20
            elif stage == 2:
                s += 15
            elif stage == 3:
                s += 5
        return s

    @staticmethod
    def _calc_required_growth(valuation: dict, tv_g: float = 0.03) -> float | None:
        """
        逆DCF: 現在株価を正当化する必要成長率（5年CAGR）を計算（DCF-2）

        EV = FCF_5 / (WACC - tv_g) を解いて FCF_5 を求め、
        必要CAGR = (FCF_5 / FCF_0)^(1/5) - 1 を返す。
        tv_g: セクター別ターミナル成長率（WACC-1）。デフォルト 3.0%。
        データ不足や非正値の場合は None を返す。
        """
        comps        = valuation.get("components", {})
        price        = comps.get("current_price") or 0
        shares       = comps.get("diluted_shares") or comps.get("implied_shares") or 0
        fcf_base     = comps.get("fcf_base_used") or 0
        net_debt     = (valuation.get("financial_health", {}).get("net_debt") or 0)
        wacc_data    = valuation.get("wacc", {})
        wacc         = wacc_data.get("market_return", 0.10) if isinstance(wacc_data, dict) else 0.10

        if not (price > 0 and shares > 0 and fcf_base > 0 and wacc > tv_g):
            return None
        ev = price * shares + net_debt
        if ev <= 0:
            return None
        required_fcf5 = ev * (wacc - tv_g) / (1 + tv_g)
        if required_fcf5 <= 0:
            return None
        return (required_fcf5 / fcf_base) ** 0.2 - 1

    @staticmethod
    def _calc_dcf_reliability_policy_b(valuation: dict) -> str:
        """
        DCF_Reliability Policy B（FCF_Conversion_Rate方式向け、DCF-RELIABILITY-1）

        Policy A（revenue_floor適用＝FCF_Base直接方式向け）とは別軸の判定基準。
        fcf_outlier.detected / fcf_outlier.action / eps_invalid の
        組み合わせで LOW/NORMAL を判定する。

        eps_invalid は EPSアナライザー自体にreliabilityフラグが存在しないため、
        FCF_Conversion_Rate推定値が生FCFから大きく乖離している（divergence_ratio>=2.0、
        FCFEstimationResult.divergence_warningが非空）ことを代理指標として採用する。

        TANUKI-POLICYB-FIX-1（2026-07-11修正）: 従来は
        `transient_evidence.found`（一過性費用の証拠が"存在するか"）を見ていたが、
        これは乖離の"金額を十分説明できるか"（`analyze_fcf_outlier`の
        `transient_explains`判定・`action`フィールドに反映済み）とは別物。
        証拠が少額のみ存在し乖離を説明しきれない場合（例: FLYW FY2025、
        215%乖離に対し一過性費用$11M<必要説明額の20%）でも`found=True`となり
        誤ってNORMAL判定されていた。`action=="excluded"`（乖離が一過性費用で
        説明済みと判定された）を正しい代理指標として使う。

        DCF-REL-SYNC-1（2026-07-11）: `deviation_pct`（5年平均からの乖離%、
        `analyze_fcf_outlier`が算出）をreport.txt表示用に`fcf_outlier`へ追加した。
        判定ロジックへの数値閾値としての組み込みは検討したが（`action=="excluded"`
        でも乖離%が極端なら再度LOWに戻す案）、実データ上`action=="excluded"`と
        なる銘柄は全て`rule="latest_negative"`型（`deviation_pct`自体がNone）で
        対象母集団が存在しないため一旦見送り、判定はシンプルな
        `action=="excluded"`→NORMAL基準のまま維持する。

        POLICY-AB-TREND-BLIND-1（2026-09-09修正）: `analyze_fcf_outlier`は
        設計上、上方乖離（`rule=="deviation_large"`かつ`fcf_value>fcf_5yr_avg`）
        を一過性費用で`excluded`にしない（一過性コストはFCFを下押しする方向
        にしか働かないため、上方乖離を一過性コスト由来とするのは論理矛盾）。
        そのため黒字転換・拡大による健全な乖離であっても、`action`は常に
        `"flagged"`のままとなり恒常的にLOW判定され続けていた（例: S・ADBE・
        CELH・PLTR・TSLA等、tanuki=true全100銘柄中50銘柄が該当）。
        この上方乖離ケースについてのみ、直近2年連続黒字（`fcf_2yr_avg>0`、
        `components`経由で取得）を主基準に追加し、LOW判定対象から除外する
        （下方乖離・継続赤字＝正当な懸念〈SOFI/XOM等〉は本除外の対象外の
        ため引き続きLOWのまま）。

        判定表（更新後、上方乖離＋直近2年連続黒字の行を追加）:
          eps_invalid=true                                            → LOW（他条件に関わらず）
          eps_invalid=false, detected=true, action=="excluded"        → NORMAL
          eps_invalid=false, detected=true, 上方乖離かつfcf_2yr_avg>0  → NORMAL（新設）
          eps_invalid=false, detected=true, 上記いずれにも該当しない    → LOW
          eps_invalid=false, detected=false                           → NORMAL
        """
        fcf_outlier = valuation.get("fcf_outlier", {}) or {}
        detected = fcf_outlier.get("detected", False)
        explained = fcf_outlier.get("action") == "excluded"
        fcf_est = valuation.get("fcf_estimation", {}) or {}
        eps_invalid = bool(fcf_est.get("divergence_warning"))

        if eps_invalid:
            return "LOW"
        if detected and not explained:
            # POLICY-AB-TREND-BLIND-1: 上方乖離＋直近2年連続黒字は
            # 一過性費用の有無に関わらず健全なトレンド好転とみなし、
            # LOW判定対象から除外する
            comps = valuation.get("components", {}) or {}
            fcf_5yr_avg = comps.get("fcf_5yr_avg")
            fcf_2yr_avg = comps.get("fcf_2yr_avg")
            latest_fcf  = fcf_outlier.get("fcf_value")
            is_upward_trend_recovery = (
                fcf_outlier.get("rule") == "deviation_large"
                and latest_fcf is not None and fcf_5yr_avg
                and latest_fcf > fcf_5yr_avg
                and fcf_2yr_avg is not None and fcf_2yr_avg > 0
            )
            if not is_upward_trend_recovery:
                return "LOW"
        return "NORMAL"

    def _compute_tanuki_score(self, ticker: str, valuation: dict) -> dict:
        """TANUKI SCOREをパイプライン時に計算（JS classify/calcFundaのPython移植）"""
        upside   = valuation.get("upside_percent")
        fcf_base = valuation.get("fcf_base", {}).get("base_fcf")
        fcf_history = valuation.get("fcf_history", [])
        fcf_latest = fcf_history[-1].get("fcf") if fcf_history else None

        # [[RULE40-DEFINITION-MISMATCH-1]]: HypeCore側のpoc.jsonキーが
        # rule40 → rule40_yoy_netmarginへ改名されたことに追従（2026-08-13）。
        # [[HYPECORE-MISC-NAMING-GAPS-1]]③（2026-08-29）: 同様にma200_dev
        # → ma200_dev_localへ改名されたことに追従。
        rev_yoy = rule40_yoy_netmargin = stage = ma200_dev = None
        poc_path = os.path.join(
            self.repo_root, "docs", "value-monitor", "hypecore", "data", f"{ticker}_poc.json"
        )
        if os.path.exists(poc_path):
            try:
                with open(poc_path, encoding="utf-8") as f:
                    poc = json.load(f)
                monthly = poc.get("monthly") or []
                last = monthly[-1] if monthly else {}
                rev_yoy   = last.get("rev_yoy")
                rule40_yoy_netmargin = last.get("rule40_yoy_netmargin")
                stage     = last.get("stage")
                ma200_dev = last.get("ma200_dev_local")
            except Exception:
                pass

        eps_yoy = None
        eps_entry = self._load_eps_map().get(ticker, {})
        yoy_dec = eps_entry.get("yoy_growth")
        if yoy_dec is not None:
            eps_yoy = yoy_dec * 100

        # calcFunda (JS移植)
        funda = 0
        funda += 25 if rev_yoy is not None and rev_yoy > 20 else 15 if rev_yoy is not None and rev_yoy >= 0 else 0
        funda += 25 if rule40_yoy_netmargin is not None and rule40_yoy_netmargin > 40 else 15 if rule40_yoy_netmargin is not None and rule40_yoy_netmargin >= 20 else 0
        funda += 25 if eps_yoy is not None and eps_yoy > 20 else 15 if eps_yoy is not None and eps_yoy >= 0 else 0
        funda += 25 if fcf_base is not None and fcf_base > 0 else 0

        # Runway < 12ヶ月のペナルティ（資金枯渇リスク）
        stonks_path = os.path.join(
            self.repo_root, "docs", "value-monitor", "stonks-silo", "data", "results.json"
        )
        if os.path.exists(stonks_path):
            try:
                with open(stonks_path, encoding="utf-8") as f:
                    _stonks_ticker = json.load(f).get("tickers", {}).get(ticker, {})
                _runway_raw = _stonks_ticker.get("runway", {}).get("runway_months")
                if _runway_raw is None:
                    _runway_raw = valuation.get("computed_runway_months")
                if isinstance(_runway_raw, (int, float)) and _runway_raw < 12:
                    funda = max(0, funda - 30)
            except Exception:
                pass

        # 希薄化ペナルティ（3年平均年率 > 20% → -15, > 40% → -25）
        fin_health = valuation.get("financial_health", {})
        dilution_pct = fin_health.get("dilution_3yr_annual_pct")
        if dilution_pct is not None:
            if dilution_pct > 40:
                funda = max(0, funda - 25)
            elif dilution_pct > 20:
                funda = max(0, funda - 15)

        # classify (JS移植。BUY判定はJS同様 upside>20% かつ timing>=50 をゲートとする。ARCH-SCORE-SYNC-1）
        timing = self._calc_timing(upside, self._load_live_fg(), stage)

        fcf_est = valuation.get("fcf_estimation", {}).get("estimated_fcf")
        sell_funda = (
            rev_yoy is not None and rev_yoy < 0
            and rule40_yoy_netmargin is not None and rule40_yoy_netmargin < 20
            and (fcf_base is not None and fcf_base < 0
                 or (fcf_est is not None and fcf_base is not None and fcf_est < fcf_base * 0.8))
        )
        # sellTech（JS classify()移植。ARCH-SCORE-SYNC-1: Python側に未実装だった技術的SELL条件を追加）
        sell_tech = (
            stage is not None and stage >= 4
            and upside is not None and upside < -20
            and ma200_dev is not None and ma200_dev < -10
            and funda < 50
        )
        sell_reason = None
        if funda < 25:
            score = Classification.PASS
        elif sell_funda or sell_tech:
            score = Classification.SELL
            sell_reason = "funda" if sell_funda else "tech"
        elif funda >= 50:
            if upside is not None and upside < -30 and stage is not None and stage >= 3:
                # DCF-2: 逆DCFの必要成長率が現在TTM成長率を下回る場合は GROWTH_PREMIUM
                # （DCFが保守的でも現在の成長が市場の要求をすでに上回っている）
                # GROWTH-VERDICT-SEQUENCING-BUG-1: growth_sanity.phase1_growthは
                # recommended_g自動再計算（_save_result内）発火時に採用値へ更新される
                # ため、ここは常に実際にDCFへ採用された成長率を参照する。
                _ttm_g = valuation.get("growth_sanity", {}).get("phase1_growth")
                # WACC-1: セクター別ターミナル成長率を逆DCFに適用
                try:
                    from maturity_config import get_terminal_growth as _get_tv_g
                    _tv_g = _get_tv_g(ticker)
                except Exception:
                    _tv_g = 0.03
                _req_g = self._calc_required_growth(valuation, tv_g=_tv_g)
                if (_req_g is not None and _ttm_g is not None
                        and _req_g > 0.05       # 意味のある Required Growth（TV超え）
                        and _req_g < _ttm_g):   # 現在成長率がすでに Required Growth を超過
                    score = Classification.GROWTH_PREMIUM
                else:
                    score = Classification.TRIM
            elif upside is not None and upside > 20 and timing >= 50:
                score = Classification.BUY
            elif upside is not None and upside > 0:
                score = Classification.WATCH
            else:
                score = Classification.HOLD
        else:
            score = Classification.HOLD

        comment = self._generate_score_comment(score, upside, rev_yoy, rule40_yoy_netmargin, fcf_base, funda, fcf_latest)

        # FCF-OUTLIER-PREROUNDING-LOSS-1: Policy A/B丸め処理は score/comment を
        # 単純に上書きし、丸め前の分類（元々BUY/TRIM/HOLD等のどれだったか）を
        # 保持するフィールドが存在しなかった。丸め突入前にローカル変数を退避し、
        # 発火した場合のみ戻り値に含める（丸め未発生時はNoneのまま）。
        _pre_rounding_score = score
        _pre_rounding_comment = comment
        _rounded_by_policy = None

        # DCF_Reliability=LOW 丸め（Policy A: revenue_floor適用＝FCF_Base直接方式向け）
        # revenue_floor適用（FCF実績マイナス）は理論株価の信頼性が低いため
        # upside依存の判定を抑制して WATCH に統一する。SELL/PASS（ファンダ劣化）は維持。
        #
        # POLICYB-GATE-FIX-1（2026-07-11）横断調査で判明: fcf_floor_applied は
        # fcf_estimation.applied の真偽に関わらず計算される（core_calculator.py:
        # adjust_fcf()はraw fcfに対して先に floor 判定を行い、その後
        # fcf_estimation.applied=True ならDCFの実際の base_fcf は
        # conversion-rate推定値に差し替えられる＝floor値は使われない）。
        # そのためPolicy Aは「floor値が実際にDCFで使われるケース」
        # （= applied=False）に限定する。BROS/CEG/SOFI/SPIR等はfloor_applied>0だが
        # applied=TrueでDCFはconversion-rate推定値を使うため、Policy Aは対象外とし
        # Policy Bのみで判定する。
        _floor_applied = valuation.get("components", {}).get("fcf_floor_applied", 0) or 0
        _fcf_estimation = valuation.get("fcf_estimation", {}) or {}
        _policy_a_fires = _floor_applied > 0 and not _fcf_estimation.get("applied")
        if _policy_a_fires and score not in (Classification.SELL, Classification.PASS):
            score = Classification.WATCH
            comment = "DCF信頼性LOW(実績FCF赤字)のためupside依存判定を抑制→WATCH"
            sell_reason = None
            _rounded_by_policy = "A"

        # DCF_Reliability=LOW 丸め（Policy B: fcf_outlier未解消向け、DCF-RELIABILITY-1）
        # POLICYB-GATE-FIX-1（2026-07-11）: 従来はfcf_estimation.appliedをゲート条件にしており、
        # applied=False（raw_fcfフォールバック方式）の銘柄でPolicy Bが評価されず、
        # FCF実績プラス・fcf_outlier未説明フラグ（BKNG/RBRK等）が見逃されていた。
        # _calc_dcf_reliability_policy_b()自体はappliedを参照しないため、ゲートは
        # 「Policy Aが既に発火済みか（_policy_a_fires）」に置き換える。Policy A発火済みの
        # 場合はPolicy Aのメッセージ（実績FCF赤字）を優先し、Policy Bで上書きしない
        # （両者ともWATCH自体は同じだが、raw_fcf方式の銘柄に「FCF_Conversion_Rate方式」
        # という誤ったコメントが付くのを防ぐため）。
        if (not _policy_a_fires and score not in (Classification.SELL, Classification.PASS)
                and self._calc_dcf_reliability_policy_b(valuation) == "LOW"):
            score = Classification.WATCH
            if _fcf_estimation.get("applied"):
                comment = "DCF信頼性LOW(FCF_Conversion_Rate方式・要注意フラグ)のためupside依存判定を抑制→WATCH"
            else:
                comment = "DCF信頼性LOW(FCF_Base方式・外れ値未説明)のためupside依存判定を抑制→WATCH"
            sell_reason = None
            _rounded_by_policy = "B"

        return {
            "score": score, "funda_score": funda, "score_comment": comment,
            "timing": timing, "sell_reason": sell_reason,
            "pre_rounding_score": _pre_rounding_score if _rounded_by_policy else None,
            "pre_rounding_comment": _pre_rounding_comment if _rounded_by_policy else None,
            "rounded_by_policy": _rounded_by_policy,
        }

    def _generate_score_comment(self, score, upside, rev_yoy, rule40_yoy_netmargin, fcf_base, funda, fcf_latest=None) -> str:
        """スコアに基づくルールベースコメント（30字程度の日本語）"""
        parts = []
        if score == Classification.GROWTH_PREMIUM:
            parts.append("成長率が市場要求を上回る")
        if upside is not None:
            if   upside >  30: parts.append(f"割安圏({upside:.0f}%)で買い余地大")
            elif upside >  10: parts.append(f"割安({upside:.0f}%)傾向")
            elif upside >   0: parts.append(f"若干割安({upside:.0f}%)")
            elif upside > -20: parts.append("フェアバリュー近辺")
            else:              parts.append(f"割高({upside:.0f}%超)")
        if fcf_base is not None:
            if fcf_latest is not None and fcf_latest < 0:
                parts.append("FCFマイナス（投資フェーズ）")
            elif fcf_base > 0:
                parts.append("FCF黒字")
            else:
                parts.append("FCF赤字注意")
        if rev_yoy is not None:
            if   rev_yoy > 20: parts.append("売上成長加速")
            elif rev_yoy > 0:  parts.append("売上安定成長")
            else:              parts.append("売上成長停滞")
        return "、".join(parts[:3]) if parts else f"{score}（データ不足）"

    def _save_result(self, ticker: str, valuation: dict, financials: dict = None) -> dict:
        ticker_dir = os.path.join(self.output_dir, ticker)
        history_dir = os.path.join(ticker_dir, "history")
        os.makedirs(history_dir, exist_ok=True)

        # fcf_history等をスコア計算前に読み込み（fcf_latest判定のため）
        extra = self._load_extra_data(ticker, valuation)

        # ---- Growth Sanity Check（original phase1_growth で実行） ----
        _phase1_growth = (
            valuation.get("growth", {}).get("rate")
            or valuation.get("growth_scenarios", {}).get("primary", {}).get("rate")
            or 0
        )
        _phase1_growth_source = valuation.get("growth", {}).get("source")  # GROWTH-FLOOR-VERDICT-1
        _annual_revs = self._load_annual_revenues(ticker)
        _g_fund = self._calc_g_fundamental(ticker)
        _sector = self._load_beta_sector(ticker)
        _hype_info   = self._load_hype_info(ticker)      # GROWTH-1
        _hype_phase  = _hype_info.get("phase")
        _fcf_margins = [
            e["fcf_margin"] for e in extra.get("fcf_history", [])
            if e.get("fcf_margin") is not None
        ]
        try:
            _growth_sanity = check_growth_sanity(
                ticker=ticker,
                phase1_growth=_phase1_growth,
                sector=_sector,
                annual_revenues=_annual_revs,
                g_fundamental=_g_fund,
                ttm_actual=_phase1_growth if _phase1_growth > 0 else None,
                hype_phase=_hype_phase,                           # GROWTH-1
                hype_phase_label=_hype_info.get("stage_label"),
                hype_substage_label=_hype_info.get("substage_label"),
                fcf_margins=_fcf_margins or None,                  # TANUKI-DCF-1③
                growth_source=_phase1_growth_source,               # GROWTH-FLOOR-VERDICT-1
            )
        except Exception as _e:
            import logging as _logging
            _logging.getLogger(__name__).warning(f"[{ticker}] growth_sanity failed: {_e}")
            _growth_sanity = None

        # ---- recommended_g による DCF 自動再計算（segment_configured=False のみ） ----
        _phase1_growth_original = _phase1_growth
        _phase1_auto_adjusted = False
        _recommended_g = _growth_sanity.get("recommended_g") if _growth_sanity else None
        _is_seg_unconfigured = not extra.get("segment_configured", True)

        # DCF-1: 線形逓減DCFの適用条件
        # growth_model == "decay"（TTM>50%の高成長銘柄）かつ業界ベンチマーク取得済みの場合
        _tapering_g_end: float | None = None
        if (_growth_sanity
                and _growth_sanity.get("growth_model") == "decay"
                and _growth_sanity.get("industry_benchmark") is not None):
            _tapering_g_end = _growth_sanity["industry_benchmark"]

        if _is_seg_unconfigured and _recommended_g is not None and financials is not None:
            try:
                _seg_cfg.set_growth_override(ticker, _recommended_g)
                _valuation_adj = self.calculator.calculate_pt(financials, tapering_g_end=_tapering_g_end)
                if "error" not in _valuation_adj:
                    # VALIDATOR-IVPS-MISMATCH-1: valuationを新スナップショットに差し替える
                    # 場合、旧スナップショット（line 127由来）に対するvalidate_calculation()の
                    # 結果を使い回さず、新スナップショットに対して再実行する。使い回すと
                    # validation.checks（pt_shares_consistency等）が最終保存される
                    # intrinsic_value_per_share等と対応しないスナップショットを検証した
                    # 結果のまま残ってしまう。
                    try:
                        _new_validation = validate_calculation(
                            ticker, _valuation_adj, use_ai=self.use_ai_validation
                        )
                    except Exception as _ve:
                        import logging as _logging
                        _logging.getLogger(__name__).warning(
                            f"[{ticker}] re-validation after recommended_g re-run failed: {_ve}"
                        )
                        _new_validation = valuation.get("validation")
                    valuation = _valuation_adj
                    valuation["validation"] = _new_validation
                    extra = self._load_extra_data(ticker, valuation)
                    _phase1_auto_adjusted = True
                    valuation["phase1_growth"] = _recommended_g

                    # GROWTH-VERDICT-SEQUENCING-BUG-1: growth_sanityのverdict/warningsは
                    # 直前(605-617行)に初期計算値(_phase1_growth_original)で判定済みのまま
                    # なので、実際にDCF・IVへ採用された_recommended_gを基準に再判定する。
                    # recommended_g/recommended_g_median/growth_model/growth_model_reasonは
                    # 「_recommended_gがどう導出されたか」の説明のため初期計算パスの値を
                    # 維持する（ここで上書きすると、report.txt「推奨成長率内訳」欄の表示と
                    # 実際にDCFへ適用された_recommended_gが不一致になってしまう）。
                    try:
                        _adopted_source = valuation.get("growth", {}).get("source")
                        _growth_sanity_adopted = check_growth_sanity(
                            ticker=ticker,
                            phase1_growth=_recommended_g,
                            sector=_sector,
                            annual_revenues=_annual_revs,
                            g_fundamental=_g_fund,
                            ttm_actual=_recommended_g if _recommended_g > 0 else None,
                            hype_phase=_hype_phase,
                            hype_phase_label=_hype_info.get("stage_label"),
                            hype_substage_label=_hype_info.get("substage_label"),
                            fcf_margins=_fcf_margins or None,
                            growth_source=_adopted_source,
                        )
                        for _key in ("verdict", "warnings", "signals", "phase1_growth", "floor_hit"):
                            _growth_sanity[_key] = _growth_sanity_adopted[_key]
                    except Exception as _e2:
                        import logging as _logging
                        _logging.getLogger(__name__).warning(
                            f"[{ticker}] growth_sanity re-evaluation after recommended_g re-run failed: {_e2}"
                        )
            except Exception as _e:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    f"[{ticker}] DCF re-run with recommended_g failed: {_e}"
                )
            finally:
                _seg_cfg.clear_growth_override(ticker)

        # ── TANUKI-DCF-1③ FCFマージン悪化 → BEAR乗数補正 ──
        _fcf_margin_bear_mult = (_growth_sanity.get("fcf_margin_bear_multiplier", 1.0)
                                  if _growth_sanity else 1.0)
        _fcf_margin_note      = (_growth_sanity.get("fcf_margin_note") if _growth_sanity else None)
        _bear_mult_applied = False
        if _fcf_margin_bear_mult < 1.0 and financials is not None:
            try:
                _valuation_bear = self.calculator.calculate_pt(
                    financials,
                    tapering_g_end=_tapering_g_end,
                    bear_multiplier=0.7 * _fcf_margin_bear_mult,
                )
                if "error" not in _valuation_bear:
                    _sv_bear = _valuation_bear.get("scenario_valuations")
                    if _sv_bear and valuation.get("scenario_valuations"):
                        valuation["scenario_valuations"]["bear"] = _sv_bear.get("bear", {})
                    _bear_mult_applied = True
                    print(f"   [{ticker}] BEAR補正適用: bear_multiplier={0.7*_fcf_margin_bear_mult:.3f} ({_fcf_margin_note})")
            except Exception as _e:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    f"[{ticker}] BEAR multiplier re-run failed: {_e}"
                )

        # auto-adjustment フラグを extra に追加（_generate_report に渡すため）
        extra["phase1_growth_original"] = _phase1_growth_original
        extra["phase1_growth_auto_adjusted"] = _phase1_auto_adjusted
        extra["recommended_g"] = _recommended_g
        extra["fcf_margin_bear_mult_applied"] = _bear_mult_applied
        extra["fcf_margin_note"] = _fcf_margin_note

        # TANUKIスコアを先に計算してlatest.jsonとhistory.json両方に保存
        # growth_sanity は後で latest_data に追加されるが、
        # GROWTH_PREMIUM 判定（DCF-2）に必要なため事前に注入する
        valuation_enriched = {**valuation, **extra}
        valuation_enriched["growth_sanity"] = _growth_sanity
        score_data = self._compute_tanuki_score(ticker, valuation_enriched)

        latest_data = {k: v for k, v in valuation.items() if k != "calculation_steps"}
        latest_data["tanuki_score"]  = score_data.get("score")
        latest_data["funda_score"]   = score_data.get("funda_score")
        latest_data["score_comment"] = score_data.get("score_comment")
        latest_data["timing_score"]  = score_data.get("timing")
        latest_data["sell_reason"]   = score_data.get("sell_reason")
        # FCF-OUTLIER-PREROUNDING-LOSS-1: Policy A/B丸め発生時のみ非None
        latest_data["pre_rounding_score"]   = score_data.get("pre_rounding_score")
        latest_data["pre_rounding_comment"] = score_data.get("pre_rounding_comment")
        latest_data["rounded_by_policy"]    = score_data.get("rounded_by_policy")
        latest_data["matrix"] = self._compute_matrix_position(
            ticker, valuation, extra.get("fcf_history", [])
        )
        latest_path = os.path.join(ticker_dir, "latest.json")
        with open(latest_path, "w", encoding="utf-8") as f:
            json.dump(latest_data, f, ensure_ascii=False, indent=2)

        date_str = valuation.get("calculation_date", datetime.now().strftime("%Y-%m-%d"))
        filename_safe = date_str.replace(":", "-")
        history_path = os.path.join(history_dir, f"{filename_safe}.json")
        with open(history_path, "w", encoding="utf-8") as f:
            json.dump(valuation, f, ensure_ascii=False, indent=2)

        # history.json（時系列チャート用・軽量サマリ）に追記
        history_summary_path = os.path.join(ticker_dir, "history.json")
        history_summary = []
        if os.path.exists(history_summary_path):
            try:
                with open(history_summary_path, encoding="utf-8") as f:
                    history_summary = json.load(f)
            except Exception:
                history_summary = []

        # DESIGN: history.json はIV/株価等のチャート用データのみに絞る
        # （判定ラベルはTANUKI VALUATION/TANUKI SCOREで別ロジックのため混乱を招く。score_history.json側に集約）
        entry = {
            "date": date_str,
            "intrinsic_value_per_share": valuation.get("intrinsic_value_per_share"),
            "intrinsic_value_beta": valuation.get("intrinsic_value_beta"),
            "current_price": valuation.get("components", {}).get("current_price"),
            "ma200": valuation.get("components", {}).get("ma200"),
            "upside_percent": valuation.get("upside_percent"),
            "growth_rate": valuation.get("growth_scenarios", {}).get("primary", {}).get("rate"),
            "scenario_bear": (valuation.get("scenario_valuations") or {}).get("bear", {}).get("intrinsic_value_per_share"),
            "scenario_bull": (valuation.get("scenario_valuations") or {}).get("bull", {}).get("intrinsic_value_per_share"),
        }
        # 同日エントリを上書き
        history_summary = [e for e in history_summary if e.get("date") != date_str]
        history_summary.append(entry)
        history_summary.sort(key=lambda e: e.get("date", ""))

        with open(history_summary_path, "w", encoding="utf-8") as f:
            json.dump(history_summary, f, ensure_ascii=False, indent=2)

        # score_history.json（ACTION-2: 判定実績追跡用）
        score_history_path = os.path.join(ticker_dir, "score_history.json")
        score_history: list = []
        if os.path.exists(score_history_path):
            try:
                with open(score_history_path, encoding="utf-8") as f:
                    score_history = json.load(f)
            except Exception:
                score_history = []
        date_only = date_str[:10]
        sh_entry = {
            "date": date_only,
            "tanuki_score": score_data.get("score"),
            "funda_score": score_data.get("funda_score"),
            "price_at_judgment": valuation.get("components", {}).get("current_price"),
            "upside_pct": valuation.get("upside_percent"),
            "hype_phase": (_growth_sanity.get("hype_phase_used") if isinstance(_growth_sanity, dict) else None),
        }
        score_history = [e for e in score_history if e.get("date") != date_only]
        score_history.append(sh_entry)
        score_history.sort(key=lambda e: e.get("date", ""))
        with open(score_history_path, "w", encoding="utf-8") as f:
            json.dump(score_history, f, ensure_ascii=False, indent=2)

        # DESIGN: HYPECOREフェーズ履歴（hypecore_history/{TICKER}.json）
        self._save_hypecore_history(ticker, date_only)

        # 財務健全性・FCF履歴・次回決算日をlatest.jsonに追加保存（extraは上で取得済み）
        latest_data.update(extra)
        latest_data["growth_sanity"] = _growth_sanity
        latest_data["phase1_growth_original"] = _phase1_growth_original
        latest_data["phase1_growth_auto_adjusted"] = _phase1_auto_adjusted
        # DESIGN-1: ERP を latest.json に保存（フロント参考表示用）
        # [[ERP-DUAL-CALC-1]]: 計算式は_calculate_erp()に統合済み（_generate_report()と共通）
        _comps_ld = latest_data.get("components", {})
        _fwd_eps_ld = _comps_ld.get("forward_eps")
        _cp_ld = _comps_ld.get("current_price") or 0
        _wacc_ld = latest_data.get("wacc", {})
        _rf_ld = _wacc_ld.get("risk_free_rate", 0.043) if isinstance(_wacc_ld, dict) else 0.043
        _ey_ld, _erp_ld = _calculate_erp(_fwd_eps_ld, _cp_ld, _rf_ld)
        if _erp_ld is not None:
            latest_data["erp"] = round(_erp_ld, 4)
            latest_data["forward_earnings_yield"] = round(_ey_ld, 4)
        _dil_pct = (latest_data.get("financial_health") or {}).get("dilution_3yr_annual_pct")
        _dil_sev, _dil_badge, _ = _dilution_severity_info(_dil_pct)
        if _dil_sev:
            latest_data["dilution_severity"] = _dil_sev
            latest_data["dilution_comment"] = _dil_badge

        # analyst_vs_iv: アナリスト中央値とTANUKI IVの乖離率
        _iv_ld = latest_data.get("intrinsic_value_per_share")
        _at_median_ld = latest_data.get("components", {}).get("analyst_target_median")
        if _iv_ld and _iv_ld != 0 and _at_median_ld is not None:
            latest_data["components"]["analyst_vs_iv"] = round(
                (_at_median_ld - _iv_ld) / abs(_iv_ld) * 100, 1
            )

        # max_eps / max_eps_per / max_eps_reliability: (GAAP NI TTM + SBC TTM) / 希薄化後株式数
        _ni_ttm_me  = (latest_data.get("dupont") or {}).get("ni_ttm")
        _sbc_ttm_me = (latest_data.get("financial_health") or {}).get("sbc_ttm")
        _shares_me  = (latest_data.get("components") or {}).get("diluted_shares")
        _price_me   = (latest_data.get("components") or {}).get("current_price") or 0
        _max_eps_val     = None
        _max_eps_per_val = None
        _reliability_val = "LOW"
        if _shares_me and _shares_me > 0:
            if _ni_ttm_me is not None and _sbc_ttm_me is not None:
                _max_earn = _ni_ttm_me + _sbc_ttm_me
                if _max_earn > 0:
                    _max_eps_val     = round(_max_earn / _shares_me, 4)
                    _max_eps_per_val = round(_price_me / _max_eps_val, 1) if _price_me > 0 else None
                    _reliability_val = "HIGH"
            elif _ni_ttm_me is not None or _sbc_ttm_me is not None:
                _max_earn = (_ni_ttm_me or 0) + (_sbc_ttm_me or 0)
                if _max_earn > 0:
                    _max_eps_val     = round(_max_earn / _shares_me, 4)
                    _max_eps_per_val = round(_price_me / _max_eps_val, 1) if _price_me > 0 else None
                    _reliability_val = "MED"
        latest_data["components"]["max_eps"]             = _max_eps_val
        latest_data["components"]["max_eps_per"]         = _max_eps_per_val
        latest_data["components"]["max_eps_reliability"] = _reliability_val

        with open(latest_path, "w", encoding="utf-8") as f:
            json.dump(latest_data, f, ensure_ascii=False, indent=2)

        report_text = self._generate_report(ticker, valuation, score_data, extra,
                                             growth_sanity=_growth_sanity)
        self._save_report(ticker, report_text)

        print(f"   💾 保存: {latest_path}")

        # STDOUT-JSON-MISMATCH-1: recommended_gによる再計算（segment_configured=False
        # の銘柄が対象）でvaluationが差し替わることがあるため、呼び出し元へ最終的な
        # valuationを返す。呼び出し元は戻り値を使って理論株価等を表示・保持すること
        # （呼び出し元自身のvaluation変数は再代入されないため、そのまま使うと
        #  stdout表示と実際に保存されたJSON値が食い違う）
        return valuation

    def _compute_matrix_position(self, ticker: str, valuation: dict, fcf_history: list) -> dict:
        """MATRIX象限（①②③④）を判定（ARCH-SCORE-SYNC-1: latest.jsonに出力しJS側matrixBadge()の独自再計算を廃止）"""
        comps = valuation.get("components", {})
        rice = valuation.get("rice", {})
        upside = valuation.get("upside_percent")

        rice_available = rice.get("available", False)
        rice_note = rice.get("note", "")
        rice_base_data = rice.get("base", {})
        rice_base_val = rice_base_data.get("rice")

        if rice_available:
            matrix = "①投資効率系"
            key_metric_y = f"RICE = {rice_base_val:.3f}" if rice_base_val is not None else "RICE = N/A"
            qx = upside is not None and upside >= 0
            # RICE四分類: ≥3.0=高効率, 1.0〜3.0=中効率, 0〜1.0=低効率, <0=N/A（OCF赤字）
            if rice_base_val is None:
                rice_efficiency = "N/A"
            elif rice_base_val >= 3.0:
                rice_efficiency = "高効率"
            elif rice_base_val >= 1.0:
                rice_efficiency = "中効率"
            elif rice_base_val >= 0:
                rice_efficiency = "低効率"
            else:
                rice_efficiency = "N/A (OCF赤字)"
            qy = rice_base_val is not None and rice_base_val >= 3.0
            yH, yL = rice_efficiency, rice_efficiency
        elif "セクター除外" in rice_note:
            matrix = "②収益性系"
            roe = comps.get("roe_10yr_avg")
            roe_pct = roe * 100 if roe is not None else None
            _roe_n = comps.get("roe_years_used") or "?"
            _roe_outlier = comps.get("roe_outlier_adj", False)
            _roe_tag = " (outlier-adjusted)" if _roe_outlier else ""
            if roe_pct is not None:
                key_metric_y = f"ROE_avg ({_roe_n}yr) = {roe_pct:.1f}%{_roe_tag}"
            elif comps.get("roe_years_used", 0) == 0:
                key_metric_y = "ROE = N/A (負債超過)"
            else:
                key_metric_y = "ROE = N/A"
            qx = upside is not None and upside >= 0
            qy = roe_pct is not None and roe_pct >= 15
            yH, yL = "高ROE", "低ROE"
        elif "Q異常値" in rice_note:
            matrix = "③成長性系"
            qx = upside is not None and upside >= 0
            _rev_yoy_m3 = None
            _poc_m3 = os.path.join(
                self.repo_root, "docs", "value-monitor", "hypecore", "data", f"{ticker}_poc.json"
            )
            if os.path.exists(_poc_m3):
                try:
                    with open(_poc_m3, encoding="utf-8") as _pf:
                        _monthly_m3 = (json.load(_pf).get("monthly") or [])
                    if _monthly_m3:
                        _rev_yoy_m3 = _monthly_m3[-1].get("rev_yoy")
                except Exception:
                    pass
            qy = _rev_yoy_m3 is not None and _rev_yoy_m3 > 0
            key_metric_y = f"Revenue_Growth = {_rev_yoy_m3:.1f}%" if _rev_yoy_m3 is not None else "Revenue_Growth (see HYPECORE)"
            yH, yL = "高成長", "低成長"
        else:
            matrix = "④キャッシュ創出力系"
            # B-2: FCF_History最新年の実績マージンを使用（FCF_Base/Revenueではなく）
            # BUG-MATRIX4-1追補: fcf_history末尾がNone(未取得年)の銘柄向け
            #   reversed()で最新の非Noneエントリーを探して実績値を採用する
            fcf_margin = None
            if fcf_history:
                _fh_rev = comps.get("latest_revenue")
                for _fh_entry in reversed(fcf_history):
                    _fh_margin = _fh_entry.get("fcf_margin")
                    _fh_fcf   = _fh_entry.get("fcf")
                    if _fh_margin is not None:
                        fcf_margin = _fh_margin
                        break
                    if _fh_fcf is not None and _fh_rev:
                        fcf_margin = _fh_fcf / _fh_rev * 100
                        break
            if fcf_margin is None:
                # フォールバック: fcf_floor_applied>0 は実績FCFマイナスを示すため
                # revenue_floor正値をY軸に使わず N/A とする
                _floor = comps.get("fcf_floor_applied", 0) or 0
                if _floor <= 0:
                    fcb = comps.get("fcf_base_used")
                    rev = comps.get("latest_revenue")
                    if fcb and rev:
                        fcf_margin = fcb / rev * 100
            key_metric_y = f"FCF_Margin = {fcf_margin:.1f}%" if fcf_margin is not None else "FCF_Margin = N/A"
            qx = upside is not None and upside >= 0
            qy = fcf_margin is not None and fcf_margin >= 15
            yH, yL = "高FCF", "低FCF"

        xL = "割安" if qx else "割高"
        yL_label = yH if qy else yL
        quadrant = ("右上" if qy else "右下") if qx else ("左上" if qy else "左下")
        label = f"{xL}×{yL_label}"

        return {
            "matrix": matrix, "quadrant": quadrant, "label": label,
            "key_metric_y": key_metric_y, "qx": qx, "qy": qy,
        }

    def _generate_report(self, ticker: str, valuation: dict, score_data: dict, extra: dict = None, growth_sanity: dict = None) -> str:
        """銘柄別統合レポートをプレーンテキストで生成"""
        if extra is None:
            extra = {}
        fcf_history = extra.get("fcf_history", [])
        fin_health = extra.get("financial_health", {})
        segments = extra.get("segments", [])
        next_earnings = extra.get("next_earnings_date", "N/A")
        _seg_ttm_applied = extra.get("segment_ttm_applied", False)
        _seg_configured = extra.get("segment_configured", True)
        _phase1_auto_adjusted = extra.get("phase1_growth_auto_adjusted", False)
        _phase1_growth_original = extra.get("phase1_growth_original")
        _recommended_g = extra.get("recommended_g")
        _bear_mult_applied = extra.get("fcf_margin_bear_mult_applied", False)
        _fcf_margin_note   = extra.get("fcf_margin_note")
        # [[REPORT-TXT-CAPM-IV-MISSING-1]]: stock.html表示済みだがreport.txt
        # 未出力だったフィールド群（dupontはextra、他はvaluation由来）
        _dupont = extra.get("dupont")

        now = valuation.get("calculation_date", datetime.now().strftime("%Y-%m-%d"))
        comps = valuation.get("components", {})
        rice = valuation.get("rice", {})
        scenarios = valuation.get("scenario_valuations") or {}
        fcf_est = valuation.get("fcf_estimation", {})
        wacc_data = valuation.get("wacc", {})

        current_price = comps.get("current_price", 0) or 0
        upside = valuation.get("upside_percent")
        base_iv_top = valuation.get("intrinsic_value_per_share")
        wacc_val = wacc_data.get("value", 0) if isinstance(wacc_data, dict) else 0
        beta = comps.get("beta", "N/A")
        rpo_pv = comps.get("rpo_pv", 0) or 0
        sector = comps.get("sector", "")
        industry = comps.get("industry", "")
        roe = comps.get("roe_10yr_avg")

        score = score_data.get("score", "N/A")
        funda_score = score_data.get("funda_score", "N/A")
        score_comment = score_data.get("score_comment", "N/A")
        pre_rounding_score = score_data.get("pre_rounding_score")

        # --- Matrix position ---
        rice_available = rice.get("available", False)
        rice_note = rice.get("note", "")
        rice_base_data = rice.get("base", {})
        rice_base_val = rice_base_data.get("rice")

        _mp = self._compute_matrix_position(ticker, valuation, fcf_history)
        matrix = _mp["matrix"]
        quadrant = _mp["quadrant"]
        label = _mp["label"]
        key_metric_y = _mp["key_metric_y"]
        qx = _mp["qx"]
        qy = _mp["qy"]

        # --- Scenario valuations ---
        bear_sc = scenarios.get("bear", {})
        base_sc = scenarios.get("base", {})
        bull_sc = scenarios.get("bull", {})
        bear_g = (bear_sc.get("growth_rate") or 0) * 100
        base_g = (base_sc.get("growth_rate") or 0) * 100
        bull_g = (bull_sc.get("growth_rate") or 0) * 100
        bear_iv = bear_sc.get("intrinsic_value_per_share") or 0
        base_iv = base_sc.get("intrinsic_value_per_share") or base_iv_top or 0
        bull_iv = bull_sc.get("intrinsic_value_per_share") or 0

        def dev(iv):
            if iv and current_price:
                return (iv - current_price) / current_price * 100
            return 0.0

        fcf_conv = fcf_est.get("conversion_rate", "N/A")
        fcf_industry = fcf_est.get("sector", "") or industry

        # --- RICE components ---
        rice_q = rice.get("q", "N/A")
        rice_cf = rice.get("cf_conversion", "N/A")
        rice_cf_adj = rice.get("cf_adj")          # R&D除外CF（CapExのみ投資強度）
        rice_wacc = rice.get("wacc", "N/A")
        rice_bear_d = rice.get("bear", {})
        rice_bull_d = rice.get("bull", {})
        sbc_adjusted = "SBC補正済み" in rice_note
        excl_reason = rice_note if not rice_available else "N/A"
        is_financial = "Financial" in (sector or "") or "Financial" in (excl_reason or "")

        # --- EPS data ---
        eps_summary_entry = self._load_eps_map().get(ticker, {})
        adj_eps = eps_summary_entry.get("adjusted_eps")
        gaap_eps_val = eps_summary_entry.get("gaap_eps")
        eps_diff = eps_summary_entry.get("eps_diff")
        yoy_growth = eps_summary_entry.get("yoy_growth")
        yoy_pct = f"{yoy_growth * 100:.1f}" if yoy_growth is not None else "N/A"

        adj_items_str = "N/A"
        recent_quarters = []
        q_path = os.path.join(
            self.repo_root, "docs", "value-monitor", "adjusted_eps_analyzer", "data", ticker, "quarterly.json"
        )
        if os.path.exists(q_path):
            try:
                with open(q_path, encoding="utf-8") as f:
                    q_data = json.load(f)
                # quarters[0] = 最新、降順で格納
                quarters = q_data.get("quarters", [])
                if quarters:
                    items = [a.get("item_id", "") for a in quarters[0].get("adjustments", [])]
                    adj_items_str = "/".join(items) if items else "N/A"
                    for q in quarters[:4]:
                        fy = q.get("fiscal_year", "?")
                        qt = q.get("quarter", "?")
                        q_label = f"{fy} Q{qt}" if isinstance(qt, int) else f"{fy} {qt}"
                        recent_quarters.append({
                            "label": q_label,
                            "actual": q.get("adjusted_eps"),
                        })
            except Exception:
                pass

        # --- HypeCore data ---
        poc_path = os.path.join(
            self.repo_root, "docs", "value-monitor", "hypecore", "data", f"{ticker}_poc.json"
        )
        hype_phase = "N/A"
        hype_alpha = valuation.get("alpha", "N/A")
        hype_signal = "N/A"
        phase_history = []
        short_int = "N/A"
        rev_yoy_hype = None
        rec_mean = None
        if os.path.exists(poc_path):
            try:
                with open(poc_path, encoding="utf-8") as f:
                    poc = json.load(f)
                monthly = poc.get("monthly", [])
                if monthly:
                    last = monthly[-1]
                    stage = last.get("stage")
                    stage_label = last.get("stage_label", f"Phase{stage}")
                    hype_phase = f"Phase{stage} ({stage_label})" if stage else "N/A"
                    substage = last.get("substage_label", "")
                    substage_watch = last.get("substage_watch", "")
                    hype_signal = f"{substage} — {substage_watch}" if substage else "N/A"
                    if (hype_signal != "N/A" and "売上・EPSは強い" in hype_signal
                            and yoy_growth is not None and yoy_growth < 0):
                        hype_signal = hype_signal.replace("売上・EPSは強い", "売上は強いがEPS前年比マイナス")
                    if (hype_signal != "N/A" and "EPSの悪化も確認" in hype_signal
                            and yoy_growth is not None and yoy_growth > 0):
                        hype_signal = hype_signal.replace(
                            "実体も崩壊中",
                            "売上低迷・EPS改善中"
                        ).replace(
                            "売上・EPSの悪化も確認される本格的な下落局面。",
                            f"売上は低迷だがEPS前年比{yoy_growth * 100:+.0f}%で改善中。実体回復の兆しを慎重に見極める局面。"
                        )
                    phase_history = [(m.get("month", "?"), m.get("stage"), m.get("stage_label", "")) for m in monthly[-6:]]
                    si = last.get("short_pct_float")
                    if si is not None:
                        short_int = f"{si * 100:.1f}"
                    rev_yoy_hype = last.get("rev_yoy")
                    rec_mean = last.get("recommendation_mean")
            except Exception:
                pass

        # --- STONKS SILO data ---
        stonks_path = os.path.join(
            self.repo_root, "docs", "value-monitor", "stonks-silo", "data", "results.json"
        )
        stonks_data = {}
        if os.path.exists(stonks_path):
            try:
                with open(stonks_path, encoding="utf-8") as f:
                    stonks_data = json.load(f).get("tickers", {}).get(ticker, {})
            except Exception:
                pass

        short_target = "yes" if stonks_data else "no"

        runway_raw = stonks_data.get("runway", {}).get("runway_months") if stonks_data else None
        if runway_raw is None and extra:
            runway_raw = extra.get("computed_runway_months")
        if isinstance(runway_raw, (int, float)):
            runway_m = f"{runway_raw:.1f}"
        elif runway_raw is not None:
            runway_m = str(runway_raw)
        else:
            runway_m = "N/A"

        rev_growth_raw = stonks_data.get("deficit_quality", {}).get("revenue_growth_pct") if stonks_data else None
        if isinstance(rev_growth_raw, dict):
            valid = {k: v for k, v in rev_growth_raw.items() if v is not None}
            latest_key = max(valid.keys()) if valid else None
            rev_growth_val = valid.get(latest_key) if latest_key else None
        elif isinstance(rev_growth_raw, (int, float)):
            rev_growth_val = rev_growth_raw
        else:
            rev_growth_val = None

        if rev_growth_val is not None:
            rev_growth_str = f"{rev_growth_val:.1f}"
        elif rev_yoy_hype is not None:
            rev_growth_str = f"{rev_yoy_hype:.1f}"
        else:
            rev_growth_str = "N/A"

        # --- Build report lines ---
        L = []
        L.append(f"{ticker} INTEGRATED INVESTMENT REPORT")
        L.append(f"Generated: {now}")
        L.append(f"Price: ${current_price:,.2f}" if current_price else "Price: N/A")
        # [[REPORT-TXT-CAPM-IV-MISSING-1]]⑥: 品質ゲート結果(PASS/WARN/FAIL)は
        # 分析の信頼性に関わるため冒頭で開示する（validator.pyのrun_basic_checks
        # 結果、pipeline.py本体でvaluation["validation"]へ格納済み）
        _validation = valuation.get("validation") or {}
        _val_overall = _validation.get("overall")
        if _val_overall == "PASS":
            L.append("Validation_Status: PASS (品質ゲート: 全チェック通過)")
        elif _val_overall in ("WARN", "FAIL"):
            _val_failed = self._get_warn_details(_validation)
            L.append(f"Validation_Status: {_val_overall} ⚠️ (要注意チェック: {_val_failed})")
        L.append("")
        if ticker in CIK_DISCONTINUITY_TICKERS:
            L.append(f"⚠️ CIK断絶(構造的境界): {CIK_DISCONTINUITY_TICKERS[ticker]}")
            L.append("")
        if ticker == "DELL":
            L.append(f"ℹ️ データ期間注記: {DELL_PRIVATE_PERIOD_NOTE}")
            L.append("")
        # --- Timing score components ---
        ma200 = comps.get("ma200")
        ma200_dev = None
        if ma200 and current_price:
            ma200_dev = (current_price / ma200 - 1) * 100
        rec_label = "N/A"
        if rec_mean is not None:
            if rec_mean <= 1.5:
                rec_label = f"{rec_mean:.2f} (Strong Buy)"
            elif rec_mean <= 2.5:
                rec_label = f"{rec_mean:.2f} (Buy)"
            elif rec_mean <= 3.5:
                rec_label = f"{rec_mean:.2f} (Hold)"
            elif rec_mean <= 4.5:
                rec_label = f"{rec_mean:.2f} (Sell)"
            else:
                rec_label = f"{rec_mean:.2f} (Strong Sell)"

        # アナリスト目標株価（comps から取得）
        _at_median = comps.get("analyst_target_median")
        _at_mean   = comps.get("analyst_target_mean")
        _at_low    = comps.get("analyst_target_low")
        _at_high   = comps.get("analyst_target_high")
        _at_count  = comps.get("analyst_count")
        _at_rec    = comps.get("analyst_rec_key", "")
        _at_vs_iv  = comps.get("analyst_vs_iv")

        # recommendationKey → 短縮ラベル
        _rec_key_map = {
            "strong_buy": "Strong Buy", "buy": "Buy", "hold": "Hold",
            "underperform": "Underperform", "sell": "Sell",
        }
        _at_rec_label = _rec_key_map.get(_at_rec, _at_rec.title() if _at_rec else "N/A")

        # Analyst_Consensus の拡充形式
        if _at_median is not None:
            _at_range = f"${_at_low:.0f}-${_at_high:.0f}" if _at_low and _at_high else "N/A"
            _at_vs_iv_str = f"{_at_vs_iv:+.1f}%" if _at_vs_iv is not None else "N/A"
            _analyst_str = (
                f"{_at_rec_label} | Target: ${_at_median:.0f} median ({_at_range})"
                f" | {_at_count or '?'} analysts | vs IV: {_at_vs_iv_str}"
            )
        else:
            _analyst_str = rec_label

        timing_lines = [
            f"  Deviation_Rate: {upside:+.1f}%" if upside is not None else "  Deviation_Rate: N/A",
            f"  MA200_Deviation: {ma200_dev:+.1f}%" if ma200_dev is not None else "  MA200_Deviation: N/A",
            f"  HypeCore_Phase: {hype_phase}",
            f"  Analyst_Consensus: {_analyst_str}",
        ]

        L.append("[1. TANUKI SCORE]")
        L.append(f"Classification: {score}")
        # FCF-OUTLIER-PREROUNDING-LOSS-1: Policy A/B丸めで元々何だったかを表示
        if pre_rounding_score is not None:
            L.append(f"Classification_Pre_Rounding: {pre_rounding_score}")
        L.append(f"Funda_Score: {funda_score}/100")
        L.append("Timing_Score (components):")
        L.extend(timing_lines)
        L.append(f"Comment: {score_comment}")
        L.append("Definition:")
        L.append("")
        L.append("Funda_Score: Composite score of financial health,")
        L.append("growth, and profitability (0-100)")
        L.append("Timing_Score: Reference indicators — deviation from IV,")
        L.append("MA200 momentum, hype phase, analyst consensus")
        L.append("(composite score requires FG index, shown as components)")
        L.append("BUY: Funda>=50 AND upside>10% AND Timing>=50")
        L.append("WATCH: Funda>=50 AND upside 0-20%")
        L.append("HOLD: Funda good, within tolerance range")
        L.append("TRIM: Funda good, overvalued(>-30%), post-euphoria, required growth exceeds TTM_YoY")
        L.append("GROWTH_PREMIUM: Would-be-TRIM, but TTM_YoY growth already exceeds required growth")
        L.append("SELL: Funda deteriorating or long-term downtrend")
        L.append("PASS: Funda<25, excluded from consideration")
        L.append("")
        L.append("")
        L.append("[2. MATRIX POSITION]")
        L.append(f"Matrix: {matrix}")
        L.append(f"Quadrant: {quadrant}")
        L.append(f"Label: {label}")
        L.append(f"Key_Metric_Y: {key_metric_y}")
        L.append(f"Deviation_Rate: {upside:+.1f}%" if upside is not None else "Deviation_Rate: N/A")
        L.append("Thresholds:")
        L.append("  RICE_Threshold: 3.0/1.0/0 (>=3.0=high, 1.0-3.0=medium, 0-1.0=low, <0=undefined OCF-negative)")
        L.append("  ROE_Threshold: 15% (above=high profitability)")
        L.append("  FCFMargin_Threshold: 15% (above=high cash generation)")
        L.append("  Deviation_Threshold: 0% (positive=undervalued)")
        L.append("Definition:")
        L.append("")
        L.append("Matrix①(投資効率系): Y=RICE, X=Deviation Rate")
        L.append("RICE = (G × VC_Factor × Q × CF) / WACC")
        L.append("Applied to: RICE-calculable tickers")
        _roe_n_def = comps.get("roe_years_used") or 10
        L.append(f"Matrix②(収益性系): Y=ROE_{_roe_n_def}yr_avg, X=Deviation Rate")
        L.append("Applied to: Sector-excluded tickers")
        L.append("(Consumer/Financial/Utilities/RealEstate/Insurance)")
        L.append("Matrix③(成長性系): Y=Revenue_Growth%, X=Runway(years)")
        L.append("Applied to: Q-anomaly tickers (deep deficit)")
        L.append("Matrix④(キャッシュ創出力系): Y=FCF_Margin%, X=Deviation Rate")
        L.append("Applied to: All tickers with FCF data")
        L.append("Deviation_Rate = (Intrinsic_Value - Current_Price)")
        L.append("/ Current_Price x 100")
        L.append("Positive deviation = undervalued (intrinsic > current)")
        L.append("")
        L.append("")
        L.append("[3. TANUKI VALUATION]")
        L.append(f"Current_Price: ${current_price:,.2f}" if current_price else "Current_Price: N/A")
        L.append(f"Intrinsic_Value_BASE: ${base_iv:,.2f}" if base_iv else "Intrinsic_Value_BASE: N/A")
        L.append(f"Deviation_BASE: {dev(base_iv):+.1f}%")
        if _phase1_auto_adjusted and _phase1_growth_original is not None and _recommended_g is not None:
            L.append(f"Growth_Rate_Original: {_phase1_growth_original*100:.1f}% (TTM実績)")
            _gs_model_here = growth_sanity.get("growth_model") if growth_sanity else None
            _model_suffix = "逓減モデル" if _gs_model_here == "decay" else "中央値ベース"
            L.append(f"Growth_Rate_Adjusted: {_recommended_g*100:.1f}% (推奨値・{_model_suffix})")
        L.append("Scenarios:")
        if _phase1_auto_adjusted:
            L.append(f"BEAR: Growth={bear_g:.1f}%, IV=${bear_iv:,.2f} （推奨値ベース）")
            L.append(f"BASE: Growth={base_g:.1f}%, IV=${base_iv:,.2f} （推奨値ベース）")
            L.append(f"BULL: Growth={bull_g:.1f}%, IV=${bull_iv:,.2f} （推奨値ベース）")
        else:
            L.append(f"BEAR: Growth={bear_g:.1f}%, IV=${bear_iv:,.2f}, Deviation={dev(bear_iv):+.1f}%")
            L.append(f"BASE: Growth={base_g:.1f}%, IV=${base_iv:,.2f}, Deviation={dev(base_iv):+.1f}%")
            L.append(f"BULL: Growth={bull_g:.1f}%, IV=${bull_iv:,.2f}, Deviation={dev(bull_iv):+.1f}%")
        # [[REPORT-TXT-CAPM-IV-MISSING-1]]⑤: return_metrics（BASEシナリオの
        # 期待リターン、future_valuesベース）を要約表示。stock.htmlの
        # future-tableは全年度・全シナリオを表形式で出すが、report.txtでは
        # BASEシナリオの主要年（1/3/5年後）＋5年年率換算のみに要約する。
        _return_metrics = valuation.get("return_metrics")
        if _return_metrics and current_price:
            _ro_parts = []
            for _yr_key in ("1年後", "3年後", "5年後"):
                _rm = _return_metrics.get(_yr_key)
                if _rm:
                    _ro_parts.append(f"{_yr_key}: ${_rm['future_value']:,.2f} ({_rm['expected_return_pct']:+.1f}%)")
            if _ro_parts:
                L.append(f"Return_Outlook(BASE): {' / '.join(_ro_parts)}")
            _rm5 = _return_metrics.get("5年後")
            if _rm5 and _rm5.get("future_value"):
                _ann_rate = (( _rm5["future_value"] / current_price) ** (1/5) - 1) * 100
                L.append(f"  5yr_Annualized_Return: {_ann_rate:+.1f}%/yr")
        # TANUKI-DCF-1②: segment_configured銘柄でrecommended_gと乖離がある場合に警告表示
        if not _phase1_auto_adjusted and _recommended_g is not None and _phase1_growth_original is not None:
            _g_diff = (_phase1_growth_original - _recommended_g) * 100
            _warn = f"  ⚠️ +{_g_diff:.1f}pt above recommended" if _g_diff >= 5.0 else ""
            L.append(f"Growth_Rate_Rec: {_recommended_g*100:.1f}% (recommended{_warn})")
        if _bear_mult_applied and _fcf_margin_note:
            L.append(f"FCF_Margin_Bear_Adj: {_fcf_margin_note}")
        # TTM_YoY_Growth（実績TTM YoY成長率 vs DCF BASE成長率の乖離を表示）
        # A-2: [4]の"CAGR_max"(rev_cagr_3yr/5yr/phase1_gの最大値)と区別するため TTM_YoY_Growth に改称
        _ttm_rev = rev_growth_val if rev_growth_val is not None else rev_yoy_hype
        if _ttm_rev is not None:
            L.append(f"TTM_YoY_Growth: {_ttm_rev:.1f}%")
            _delta = base_g - _ttm_rev
            _sign = "premium" if _delta >= 0 else "discount"
            L.append(f"(DCF Base vs TTM actual: {_delta:+.1f}pt {_sign})")
        wacc_pct = wacc_val * 100 if isinstance(wacc_val, (int, float)) else None
        _beta_is_default = False
        _beta_stale_days = None
        try:
            from datetime import date as _date_cls
            _bc_path = os.path.join(self.repo_root, "config", "beta_config.json")
            with open(_bc_path, encoding="utf-8") as _bcf:
                _bc = json.load(_bcf)
            _bc_override = _bc.get("overrides", {}).get(ticker, {})
            _beta_is_default = _bc_override.get("reason") == "デフォルト設定"
            _bc_as_of = _bc_override.get("as_of")
            if _bc_as_of:
                _beta_stale_days = (_date_cls.today() - _date_cls.fromisoformat(_bc_as_of)).days
        except Exception:
            pass
        _dr_primary = wacc_data.get("market_return", 0.10) if isinstance(wacc_data, dict) else 0.10
        L.append(f"Discount_Rate_Primary: {_dr_primary*100:.2f}% (DCF discount rate used)")
        if wacc_pct is not None:
            _wacc_suffix = " (β=sector default、個別検証推奨)" if _beta_is_default else ""
            L.append(f"WACC_CAPM_Reference: {wacc_pct:.2f}%{_wacc_suffix}")
        else:
            L.append("WACC_CAPM_Reference: N/A")
        # [[REPORT-TXT-CAPM-IV-MISSING-1]]①: WACC_CAPM_Referenceで実際に
        # 割り引いたIV（参考①β込みWACC・参考②Rf理論上限）を直後に併記する。
        # 本セクション末尾の定義文が「WACC_CAPM_ReferenceでのIVを参照」と
        # 既に言及していたにもかかわらず、その値自体が出力されていなかった
        # 自己言及の矛盾を解消する。
        _ivps_beta = valuation.get("intrinsic_value_beta")
        _upside_beta = valuation.get("upside_percent_beta")
        if _ivps_beta:
            L.append(f"  参考①β込みWACC_IV: ${_ivps_beta:,.2f} (Deviation: {_upside_beta:+.1f}%)"
                      if _upside_beta is not None else f"  参考①β込みWACC_IV: ${_ivps_beta:,.2f}")
        _ivps_rf = valuation.get("intrinsic_value_rf")
        _upside_rf = valuation.get("upside_percent_rf")
        if _ivps_rf:
            L.append(f"  参考②Rf理論上限_IV: ${_ivps_rf:,.2f} (Deviation: {_upside_rf:+.1f}%)"
                      if _upside_rf is not None else f"  参考②Rf理論上限_IV: ${_ivps_rf:,.2f}")
        terminal_g_used = comps.get("terminal_growth_used")
        terminal_g_pct = terminal_g_used * 100 if isinstance(terminal_g_used, (int, float)) else None
        L.append(f"Terminal_Growth_Rate: {terminal_g_pct:.1f}%" if terminal_g_pct is not None else "Terminal_Growth_Rate: N/A")
        _beta_stale_flag = f"  ⚠️ override設定から{_beta_stale_days}日経過、要更新確認" if (_beta_stale_days is not None and _beta_stale_days > 90) else ""
        L.append(f"Beta: {beta}{_beta_stale_flag}")
        # --- Valuation Gap Analysis (reverse DCF) ---
        if upside is not None and upside <= -50 and current_price > 0 and base_iv > 0:
            _gap_shares = comps.get("diluted_shares") or 0
            _gap_nd     = fin_health.get("net_debt") or 0
            _gap_fcf    = comps.get("fcf_base_used") or 0
            _gap_rm     = wacc_data.get("market_return", 0.10) if isinstance(wacc_data, dict) else 0.10
            _gap_tvg    = terminal_g_used if isinstance(terminal_g_used, (int, float)) else 0.03
            if _gap_shares > 0 and _gap_fcf > 0 and _gap_rm > _gap_tvg:
                _gap_ev   = current_price * _gap_shares + _gap_nd
                _gap_term = _gap_ev * (_gap_rm - _gap_tvg) / (1 + _gap_tvg)
                _gap_rg   = (_gap_term / _gap_fcf) ** (1 / 5) - 1 if _gap_fcf > 0 and _gap_term > 0 else None
                L.append("Valuation_Gap_Analysis:")
                L.append(f"  DCF_IV:           ${base_iv:,.0f} (保守的FCFベース・現在実力値)")
                L.append(f"  Market_Price:     ${current_price:,.0f}")
                L.append(f"  Gap:              {upside:+.0f}% (市場が将来成長を先取り)")
                L.append(f"  Implied_FCF_2030: ${_gap_term/1e9:.2f}B (現在株価を正当化するために必要な5年後FCF)")
                L.append(f"  Current_FCF:      ${_gap_fcf/1e9:.2f}B")
                if _gap_rg is not None:
                    L.append(f"  Required_Growth:  {_gap_rg*100:.0f}%/yr (現在FCFから必要成長率を逆算)")
                else:
                    L.append("  Required_Growth:  N/A")
        # [[REPORT-TXT-CAPM-IV-MISSING-1]]⑧: FCF/RICE算出に使ったTTM期末日
        # （技術的な注記のため1行のみ）
        _fcf_ttm_end = valuation.get("fcf_ttm_end")
        if _fcf_ttm_end:
            L.append(f"FCF_TTM_End: {_fcf_ttm_end} (FCF/RICE算出に用いたTTM期末日)")
        if not fcf_est.get("applied", True):
            # raw_fcf フォールバック: 実際のFCFベース値を表示
            _fcf_base_used = comps.get("fcf_base_used", 0)
            _excl_avg      = comps.get("fcf_outlier_excl_avg")
            _rd_applied    = valuation.get("rd_capitalization", {}).get("applied", False)
            _fcf_list_len  = len(comps.get("fcf_list_raw") or [])
            _floor_applied = comps.get("fcf_floor_applied", 0) or 0
            # B-1: DCF_Reliability判定（revenue floorが使われた場合はLOW）
            _dcf_reliability = "LOW" if _floor_applied > 0 else "HIGH"
            _desc_parts = []
            if _excl_avg is not None:
                _n_rem = _fcf_list_len - 1 if _fcf_list_len > 1 else 1
                _desc_parts.append(f"外れ値除外後{_n_rem}yr平均")
            else:
                # B-1: 実データ年数で動的化
                _data_n = _fcf_list_len if _fcf_list_len > 0 else 5
                _method_label = {
                    "recent_1yr":       "直近1yr（CAGR減少）",
                    "recent_2yr":       f"直近2yr平均",
                    "avg_5yr_recovery": f"{_data_n}yr平均（回復判定）",
                    "avg_5yr":          f"{_data_n}yr平均",
                }.get(comps.get("fcf_base_method", ""), f"{_data_n}yr平均")
                _desc_parts.append(_method_label)
            if _rd_applied:
                _desc_parts.append("R&D補正")
            # B-1: 調整前後併記（revenue floor適用時）
            if _floor_applied > 0:
                _fcf_raw_avg = comps.get("fcf_5yr_avg", 0) or 0
                L.append(f"FCF_Base: ${_fcf_base_used/1e6:,.2f}M ({' + '.join(_desc_parts)}) [実績avg: ${_fcf_raw_avg/1e6:,.1f}M]")
                L.append(f"DCF_Reliability: LOW ⚠️ (FCF実績マイナス: revenue_floor適用, IV参考値)")
                L.append("  [Policy A: LOW時はBUY/TRIM/HOLD/WATCHをWATCHへ丸め。SELL/PASSは維持。")
                L.append("   FCF実績がマイナスのためIVはrevenue_floorベース推定値。IV絶対値より方向感参考用。]")
            else:
                L.append(f"FCF_Base: ${_fcf_base_used/1e6:,.2f}M ({' + '.join(_desc_parts)})")
                # POLICYB-GATE-FIX-1（2026-07-11）: floor未発火（FCF実績プラス）でも
                # fcf_outlierが未説明のまま残るケース（BKNG/RBRK等）をPolicy Bで捕捉する。
                _reliability_b_raw = self._calc_dcf_reliability_policy_b(valuation)
                if _reliability_b_raw == "LOW":
                    L.append("DCF_Reliability: LOW ⚠️ (FCF_Base方式: 要注意フラグ検出, IV参考値)")
                    L.append("  [Policy B: LOW時はBUY/TRIM/HOLD/WATCHをWATCHへ丸め。SELL/PASSは維持。")
                    L.append("   fcf_outlier未解消（一過性費用で説明不可）のため、IVは参考値扱い。]")
                    _dev_pct_polb_raw = (valuation.get("fcf_outlier") or {}).get("deviation_pct")
                    if _dev_pct_polb_raw is not None:
                        L.append(f"   [DCF-REL-SYNC-1: FCF実績が5年平均から{_dev_pct_polb_raw:.0f}%乖離]")
                else:
                    # [[DCF-RELIABILITY-LABEL-MISMATCH-1]]対応（2026-08-30）:
                    # _calc_dcf_reliability_policy_b()は常にLOW/NORMALの
                    # いずれかを返す一貫した関数だが、この分岐（FCF_Base方式）
                    # だけ従来HIGHと表示しており、FCF_Conversion_Rate方式側
                    # （下記、同じNORMAL戻り値）と表示語彙が食い違っていた。
                    # report.txtを横断的にパースする外部ツールが「非LOW側」を
                    # 単純な2値として扱えるよう、関数の戻り値と一致するNORMAL
                    # に統一する。
                    L.append("DCF_Reliability: NORMAL  (FCF実績プラス: 通常判定適用)")
            # FCF-EST-NOTE-DISPLAY-1: 「買収・統合関連」加算の控除・検出情報を表示
            # （生FCF安定(CV<0.3)のためこの分岐に来た銘柄は、控除自体が
            # estimated_fcfの計算に使われない＝表示すると誤解を招くため対象外とする）
            _fcf_note_fb = fcf_est.get("note", "") or ""
            if not _fcf_note_fb.startswith("生FCF安定"):
                _ma_excluded_fb = fcf_est.get("ma_addback_excluded") or 0
                _ma_skipped_fb  = fcf_est.get("ma_addback_detected_but_not_applied") or 0
                if _ma_excluded_fb > 0:
                    L.append(f"⚠️ 買収・統合関連加算${_ma_excluded_fb/1e6:,.0f}Mを控除後も調整済み")
                    L.append("    純利益がマイナスのため、生FCFへフォールバックしています。詳細は")
                    L.append("    BACKLOG_DONE.md [[CWAN-SNPS-MA-DISTORTION-1]]参照。]")
                elif _ma_skipped_fb > 0:
                    L.append(f"ℹ️ 買収・統合関連加算${_ma_skipped_fb/1e6:,.0f}Mを検出したが未控除")
                    L.append("   [控除すると生FCFを下回る（過小推定側）と判定されたため、方向性")
                    L.append("    ガードにより調整済み純利益をそのまま採用しています。詳細は")
                    L.append("    BACKLOG_DONE.md [[FCF-EST-DIRECTION-GUARD-1]]参照。]")
        else:
            L.append(f"FCF_Conversion_Rate: {fcf_conv} (Industry: {fcf_industry})")
            L.append("  [FCF_Conv: Adj_NI × rate = estimated FCF. Conservative conversion from")
            L.append("   adjusted net income; differs from OCF→FCF conversion rate.]")
            _fcf_est_val = fcf_est.get("estimated_fcf") or comps.get("fcf_base_used") or 0
            _fcf_adj_ni  = fcf_est.get("adj_net_income")
            if _fcf_est_val > 0:
                if _fcf_adj_ni:
                    L.append(f"DCF_FCF_Base: ${_fcf_est_val/1e6:,.0f}M (= Adj_NI ${_fcf_adj_ni/1e6:,.0f}M × FCF_Conv {fcf_conv})")
                else:
                    L.append(f"DCF_FCF_Base: ${_fcf_est_val/1e6:,.0f}M")
            # FCF-CONVRATE-DESIGN-LIMIT-1: Software_System_Mature/SaaS 自己補正チェック結果
            _sw_reclass = valuation.get("software_system_reclassification", {}) or {}
            if _sw_reclass.get("reclassify_recommended"):
                L.append(f"⚠️ Software_System分類見直し推奨: 現在={_sw_reclass.get('current_subgroup')} "
                         f"→ 推奨={_sw_reclass.get('recommended_subgroup')}")
                L.append(f"   [{_sw_reclass.get('note', '')}]")
            # FCF-CONVRATE-DESIGN-LIMIT-1: 新規銘柄の前受収益比率ベース暫定分類（境界近傍のみ表示）
            _sw_provisional = valuation.get("software_system_provisional", {}) or {}
            if _sw_provisional.get("is_provisional") and _sw_provisional.get("note"):
                L.append(f"⚠️ 要確認: Software_System分類は暫定（{_sw_provisional.get('note')}）")
            # FCF-CONVRATE②（TRUST-SUMMARY-EPIC-1）: 業種平均の固定転換率では
            # 業界サイクル変動を表現できない構造的限界が原因分析で確定した銘柄のみの
            # 個別ティッカーリスト。閾値による自動判定は数学的に不可能と判明したため
            # 手動リストとする（自動化しない）。将来追加時も個別の原因分析を経ること。
            if ticker in FCF_CYCLICAL_VOLATILITY_TICKERS:
                _cyclical_dr = fcf_est.get("divergence_ratio")
                if _cyclical_dr is not None:
                    L.append(f"⚠️ FCF実力推定に注意（業績サイクル変動）: 直近乖離 {_cyclical_dr}倍")
                    L.append("   [業界サイクルにより年度ごとのFCFが大きく変動するため、業種平均比率")
                    L.append("    による推定値と実際の乖離が大きくなっています。分類判定には使用しません。]")
            # FCF-CONVRATE①（TRUST-SUMMARY-EPIC-1）: セクターがsector_conversion_rates
            # に未収録のため、業種別の較正済みレートではなく汎用デフォルト値（0.70）が
            # 機械的に使われている状態を検知・明示する。②（FCF_CYCLICAL_VOLATILITY_
            # TICKERS）と異なり固定ティッカーリストは使わず、adjustments.py側で
            # 判定済みのrate_is_sector_defaultフラグのみで判定する。
            if fcf_est.get("rate_is_sector_default"):
                L.append("⚠️ FCF転換率が未検証（セクター未収録）: 業種平均比率ではなく")
                L.append(f"   デフォルト値（{fcf_conv}）を使用しています。分類判定には使用しません。")
            # FCF-CONVRATE②派生（KO-SPIR-CF-CAUSE-UNCONFIRMED-1）: 生FCFが銘柄固有の
            # 一過性項目（税務訴訟・M&A偶発対価の精算・事業売却益等）で押し下げられている
            # と10-K一次情報で確定した銘柄への注記。FCF_CYCLICAL_VOLATILITY_TICKERS
            # （業界サイクル起因）とは原因が異なるため区別して表示する。
            # Classification（BUY/WATCH等）には影響しない。
            if ticker in FCF_TRANSIENT_ITEM_EXPLANATIONS:
                _transient = FCF_TRANSIENT_ITEM_EXPLANATIONS[ticker]
                if ticker == "KO":
                    L.append("⚠️ FCF実力推定に注意（一過性項目により生FCFが押し下げ）:")
                    for _yr in sorted(_transient.keys()):
                        L.append(f"   [{_yr}年: {_transient[_yr]}]")
                    L.append("   [両年度の一過性項目を除いた正常化OCFは$12.8B(2024)/$13.5B(2025)相当で")
                    L.append("    NIの成長トレンドと整合。分類判定には使用しません。詳細はBACKLOG_DONE.md")
                    L.append("    [[KO-SPIR-CF-CAUSE-UNCONFIRMED-1]]参照。]")
                elif ticker == "SPIR":
                    L.append(f"⚠️ NI/OCF乖離に注意（一過性）: {_transient['ni_ocf_divergence']}")
                    L.append(f"⚠️ 減収に注意（構造的変化・継続）: {_transient['revenue_decline']}")
                    L.append("   [前者は一過性の会計事象、後者は今後も継続する事業規模の縮小として")
                    L.append("    区別してください。分類判定には使用しません。詳細はBACKLOG_DONE.md")
                    L.append("    [[KO-SPIR-CF-CAUSE-UNCONFIRMED-1]]参照。]")
            # FCF-EST-NOTE-DISPLAY-1: 「買収・統合関連」加算の控除・検出情報を表示
            # （CWAN-SNPS-MA-DISTORTION-1・FCF-EST-DIRECTION-GUARD-1・
            # FCF-EST-NET-BASIS-FIX-1で計算済みだがreport.txtに未表示だった情報）。
            # Classification（BUY/WATCH等）には影響しない。
            _ma_excluded = fcf_est.get("ma_addback_excluded") or 0
            _ma_skipped  = fcf_est.get("ma_addback_detected_but_not_applied") or 0
            if _ma_excluded > 0:
                L.append(f"⚠️ 買収・統合関連加算を控除: ${_ma_excluded/1e6:,.0f}M")
                L.append("   [無形資産償却費・M&A統合費用等の買収由来の非現金加算がAdj_NIに")
                L.append("    含まれたままだと推定FCFが過大になるため、FCF換算専用に控除して")
                L.append("    います。分類判定には使用しません。詳細はBACKLOG_DONE.md")
                L.append("    [[CWAN-SNPS-MA-DISTORTION-1]]参照。]")
            elif _ma_skipped > 0:
                L.append(f"ℹ️ 買収・統合関連加算${_ma_skipped/1e6:,.0f}Mを検出したが未控除")
                L.append("   [控除すると生FCFを下回る（過小推定側）と判定されたため、方向性")
                L.append("    ガードにより調整済み純利益をそのまま採用しています。詳細は")
                L.append("    BACKLOG_DONE.md [[FCF-EST-DIRECTION-GUARD-1]]参照。]")
            # DCF-RELIABILITY-1: Policy B（FCF_Conversion_Rate方式向けDCF_Reliability）
            _reliability_b = self._calc_dcf_reliability_policy_b(valuation)
            if _reliability_b == "LOW":
                L.append("DCF_Reliability: LOW ⚠️ (FCF_Conversion_Rate方式: 要注意フラグ検出, IV参考値)")
                L.append("  [Policy B: LOW時はBUY/TRIM/HOLD/WATCHをWATCHへ丸め。SELL/PASSは維持。")
                L.append("   fcf_outlier未解消（一過性費用で説明不可）または推定FCFが生FCFから")
                L.append("   大幅乖離（eps_invalid）のため、IVは参考値扱い。]")
                _dev_pct_polb = (valuation.get("fcf_outlier") or {}).get("deviation_pct")
                if _dev_pct_polb is not None:
                    L.append(f"   [DCF-REL-SYNC-1: FCF実績が5年平均から{_dev_pct_polb:.0f}%乖離]")
            else:
                L.append("DCF_Reliability: NORMAL  (FCF_Conversion_Rate方式: 通常判定適用)")
        # REPORT-6: DCF再現性ブロック（上から足すとIVになる完全構造）
        _dcf_comps_r6 = valuation.get("dcf_components", {})
        _dcf_type_r6  = valuation.get("dcf_type", "two_stage")
        _hg_yrs_r6    = comps.get("high_growth_years") or 5
        _alpha_r6     = valuation.get("alpha", 0) or 0
        _go_pv_r6     = comps.get("growth_option_pv", 0) or 0
        _shares_r6    = comps.get("diluted_shares") or 0
        _shares_src_r6 = comps.get("_shares_source", "unknown")
        _nc_ps_r6     = (valuation.get("bs_adjustment") or {}).get("net_cash_per_share", 0) or 0
        _nc_total_r6  = _shares_r6 * _nc_ps_r6 if _shares_r6 > 0 else 0

        # Rm=10% ベースの内訳（IV式の実際の構成要素）
        _pv_fcf_rm = _dcf_comps_r6.get("pv_fcf_rm")   # Phase1+Phase2 or FCF PV (Rm=10%)
        _pv_tv_rm  = _dcf_comps_r6.get("pv_tv_rm")    # Terminal Value PV (Rm=10%)
        _v0_rm     = _dcf_comps_r6.get("v0_rm")        # V0 = FCF_PV + TV_PV (Rm=10%)
        _p1_rm     = _dcf_comps_r6.get("pv_phase1_rm") # 3段階: Phase1 PV (Rm)
        _p2_rm     = _dcf_comps_r6.get("pv_phase2_rm") # 3段階: Phase2 PV (Rm)

        # FCF_PV 行
        if _pv_fcf_rm is not None:
            if _dcf_type_r6 == "three_stage" and _p1_rm is not None:
                L.append(f"DCF_FCF_PV: ${_pv_fcf_rm/1e9:.2f}B"
                          f" (Ph1=${_p1_rm/1e9:.2f}B + Ph2=${_p2_rm/1e9:.2f}B, {_hg_yrs_r6}yr, pre-alpha, Rm=10%)")
            else:
                L.append(f"DCF_FCF_PV: ${_pv_fcf_rm/1e9:.2f}B ({_hg_yrs_r6}yr discounted FCF sum, pre-alpha, Rm=10%)")
        elif _dcf_type_r6 == "three_stage":
            _pv_fcf_capm = (_dcf_comps_r6.get("pv_phase1") or 0) + (_dcf_comps_r6.get("pv_phase2") or 0)
            L.append(f"DCF_FCF_PV: ${_pv_fcf_capm/1e9:.2f}B ({_hg_yrs_r6}yr discounted FCF sum, pre-alpha)")
        else:
            _pv_fcf_capm = _dcf_comps_r6.get("pv_high_growth") or comps.get("pv_high") or 0
            if _pv_fcf_capm:
                L.append(f"DCF_FCF_PV: ${_pv_fcf_capm/1e9:.2f}B ({_hg_yrs_r6}yr discounted FCF sum, pre-alpha)")

        # [[REPORT-TXT-CAPM-IV-MISSING-1]]④: maturity_profile（Phase1/Phase2の
        # 年数・成長率、3段階DCFの内訳。DCF_FCF_PVのPh1/Ph2金額と対になる
        # 「年数×成長率」の前提を明示する）
        _maturity_profile = valuation.get("maturity_profile")
        if _maturity_profile:
            _mp_p1 = _maturity_profile.get("phase1") or {}
            _mp_p2 = _maturity_profile.get("phase2") or {}
            if _mp_p1.get("years") and _mp_p2.get("years"):
                L.append(
                    f"Maturity_Profile: Phase1={_mp_p1['years']}yr@{(_mp_p1.get('growth') or 0)*100:.1f}%, "
                    f"Phase2={_mp_p2['years']}yr@{(_mp_p2.get('growth') or 0)*100:.1f}%"
                )

        # TV_PV 行
        _pv_tv_show = _pv_tv_rm if _pv_tv_rm is not None else (_dcf_comps_r6.get("pv_terminal") or comps.get("pv_terminal") or 0)
        if _pv_tv_show:
            L.append(f"DCF_TV_PV:  ${_pv_tv_show/1e9:.2f}B (terminal value present value, pre-alpha, Rm=10%)"
                     if _pv_tv_rm is not None
                     else f"DCF_TV_PV:  ${_pv_tv_show/1e9:.2f}B (terminal value present value, pre-alpha)")

        # V0 = FCF_PV + TV_PV
        if _v0_rm is not None:
            L.append(f"DCF_v0:     ${_v0_rm/1e9:.2f}B (= FCF_PV + TV_PV, Rm=10% enterprise value)")

        # Alpha
        # [[REPORT-TXT-CAPM-IV-MISSING-1]]⑦: alpha_was_capped（セクター別上限
        # 到達フラグ）を1行注記として付記
        _alpha_capped_suffix = " [CAPPED: セクター別上限到達]" if valuation.get("alpha_was_capped") else ""
        L.append(f"Alpha_Premium: {_alpha_r6:.4f} (HypeCore expectation premium; Rm basis){_alpha_capped_suffix}")

        # RPO_PV
        _rpo_excl = (valuation.get("rpo_adjustment") or {}).get("exclusion_reason", "")
        if _rpo_excl:
            L.append(f"RPO_PV: $0 ({_rpo_excl})")
        else:
            L.append(f"RPO_PV: ${rpo_pv/1e9:.3f}B" if rpo_pv >= 1e8
                     else f"RPO_PV: ${rpo_pv:,.0f}")
            if rpo_pv > 0:
                L[-1] += " (Remaining Performance Obligation premium)"

        # Growth_Option_PV
        if _go_pv_r6 > 0:
            L.append(f"Growth_Option_PV: ${_go_pv_r6/1e9:.3f}B (optional segment premium)")
        else:
            L.append("Growth_Option_PV: $0")

        # Equity bridge（ALPHA-REDESIGN-1: alpha非乗算のDCF_v0をそのまま使用）
        _pt_r6 = (_v0_rm or 0) + rpo_pv + _go_pv_r6
        _eq_val_r6 = _pt_r6 + _nc_total_r6
        if _v0_rm is not None:
            L.append(f"Equity_Value: ${_eq_val_r6/1e9:.2f}B"
                      f" (= v0 + RPO_PV + GO_PV"
                      + (f" + Net_Cash ${_nc_total_r6/1e9:.2f}B)" if _nc_total_r6 != 0 else ")"))
        if _shares_r6 > 0:
            L.append(f"Shares_Used: {_shares_r6/1e6:.1f}M (source: {_shares_src_r6})")
        iv_r6 = valuation.get("intrinsic_value_per_share")
        if iv_r6 is not None:
            L.append(f"→ Intrinsic_Value: ${iv_r6:.2f} (= Equity_Value ÷ Shares_Used)")

        # [[REPORT-TXT-CAPM-IV-MISSING-1]]③: sensitivity（WACC±1% × 高成長期間
        # 3パターンの3×3感応度マトリクス）。stock.htmlは全9セルを表として
        # 表示するが、report.txtでは同じ3×3を簡潔なタブ区切り表で要約する
        _sensitivity = valuation.get("sensitivity")
        if _sensitivity and _sensitivity.get("matrix"):
            _sn_wacc = _sensitivity.get("wacc_values") or []
            _sn_years = _sensitivity.get("growth_years") or []
            _sn_matrix = _sensitivity.get("matrix") or []
            if _sn_wacc and _sn_years and _sn_matrix:
                L.append("Sensitivity_Matrix (WACC×成長期間 → IV/share):")
                _sn_header = "  WACC\\Years  " + "  ".join(f"{y}yr" for y in _sn_years)
                L.append(_sn_header)
                for _sn_i, _sn_w in enumerate(_sn_wacc):
                    _sn_row = _sn_matrix[_sn_i] if _sn_i < len(_sn_matrix) else []
                    _sn_vals = "  ".join(f"${v:,.2f}" for v in _sn_row)
                    L.append(f"  {_sn_w*100:.1f}%       {_sn_vals}")

        # FCF外れ値詳細（既存）
        _fcf_outlier_r6 = valuation.get("fcf_outlier", {})
        if _fcf_outlier_r6.get("action") == "excluded" and _fcf_outlier_r6.get("fcf_value") is not None:
            _excl_fy_r6  = _fcf_outlier_r6.get("fiscal_year", "?")
            _excl_val_r6 = _fcf_outlier_r6.get("fcf_value", 0)
            _excl_n_r6   = max(len(comps.get("fcf_list_raw") or []) - 1, 1)
            _excl_thr_r6 = int((_fcf_outlier_r6.get("threshold_pct") or 0.6) * 100)
            _r6_base     = comps.get("fcf_base_used") or 0
            L.append(f"DCF_FCF_Base_Detail: ${_r6_base/1e6:,.1f}M (外れ値${_excl_val_r6/1e6:,.0f}M/FY{_excl_fy_r6}除外後{_excl_n_r6}点平均)")
            L.append(f"DCF_FCF_Base_Excluded: FY{_excl_fy_r6}=${_excl_val_r6/1e6:,.0f}M (outlier: >{_excl_thr_r6}% deviation from {_excl_n_r6}yr series)")
        L.append("Financial_Health:")
        net_debt = fin_health.get("net_debt")
        total_debt = fin_health.get("total_debt")
        cash = fin_health.get("cash_and_equivalents")
        sbc_ttm = fin_health.get("sbc_ttm")
        dilution_3yr = fin_health.get("dilution_3yr_annual_pct")
        _dil_sev, _dil_badge, _dil_rpt = _dilution_severity_info(dilution_3yr)
        L.append(f"  Net_Debt: ${net_debt/1e9:+.2f}B (negative=net cash)" if net_debt is not None else "  Net_Debt: N/A")
        _st_invest = fin_health.get("short_term_investments") or 0
        _sti_estimated_zero = fin_health.get("sti_estimated_zero", False)
        _sti_last_zero_year = fin_health.get("sti_last_confirmed_zero_year")
        if total_debt is not None:
            if _st_invest > 0 or _sti_estimated_zero:
                # NVDA-STI-TAG-UNIDENTIFIED-1: cross_filing_tags経由の複数タグ
                # 合算近似値の場合、実額比残差率を注記する（get_net_cash()の
                # None→0扱いとは別扱い。この銘柄は0.9%程度の乖離のある近似値
                # であることを明示する）
                _sti_approx = fin_health.get("sti_approximated", False)
                _sti_residual = fin_health.get("sti_residual_pct")
                if _sti_approx and _sti_residual is not None:
                    _sti_note = f" (近似値、実額比+{_sti_residual*100:.2f}%)"
                elif _sti_estimated_zero:
                    # FY52WEEK-BS-FADEOUT-FALLBACK-1: 最新年度は完全欠損だが
                    # 過去の直近既知値が明示的0だったため真のゼロと推定した旨を注記
                    _sti_note = f" (推定ゼロ、最終確認: FY{_sti_last_zero_year})"
                else:
                    _sti_note = ""
                L.append(f"  Total_Debt: ${total_debt/1e9:.2f}B  Cash: ${cash/1e9:.2f}B  ST_Invest: ${_st_invest/1e9:.2f}B{_sti_note}")
            else:
                L.append(f"  Total_Debt: ${total_debt/1e9:.2f}B  Cash: ${cash/1e9:.2f}B")
        else:
            L.append("  Total_Debt: N/A")
        # FY52WEEK-BS-FADEOUT-FALLBACK-1: LTDebt/STDebtの個別内訳表示
        # （いずれかが推定ゼロの場合のみ表示。通常時はTotal_Debtの合算表示で足りる）
        _ltdebt_estimated_zero = fin_health.get("ltdebt_estimated_zero", False)
        _stdebt_estimated_zero = fin_health.get("stdebt_estimated_zero", False)
        if _ltdebt_estimated_zero or _stdebt_estimated_zero:
            _ltdebt_val = fin_health.get("long_term_debt") or 0
            _stdebt_val = fin_health.get("short_term_debt") or 0
            _ltdebt_last_zero_year = fin_health.get("ltdebt_last_confirmed_zero_year")
            _stdebt_last_zero_year = fin_health.get("stdebt_last_confirmed_zero_year")
            _lt_note = f" (推定ゼロ、最終確認: FY{_ltdebt_last_zero_year})" if _ltdebt_estimated_zero else ""
            _st_note = f" (推定ゼロ、最終確認: FY{_stdebt_last_zero_year})" if _stdebt_estimated_zero else ""
            L.append(f"  Debt内訳: LTDebt=${_ltdebt_val/1e9:.2f}B{_lt_note}  STDebt=${_stdebt_val/1e9:.2f}B{_st_note}")
        _nd_period = fin_health.get("net_debt_period")
        if _nd_period:
            L.append(f"  Net_Debt_Period: {_nd_period}")
        L.append(f"  SBC_TTM: ${sbc_ttm/1e6:,.0f}M" if sbc_ttm is not None else "  SBC_TTM: N/A")
        if dilution_3yr is not None:
            L.append(f"  Dilution_3yr_Annual: {dilution_3yr:+.2f}%/yr  {_dil_badge}")
            L.append(f"  Dilution_Comment: {_dil_rpt}")
            # 期中大規模発行フラグ: yf_implied株数と直近annual株数の乖離 > 5%
            _yf_implied_shares = comps.get("diluted_shares") or 0
            _ann_sec_shares = fin_health.get("shares_yr_now") or 0
            if _yf_implied_shares > 0 and _ann_sec_shares > 0:
                _shares_dev = (_yf_implied_shares - _ann_sec_shares) / _ann_sec_shares * 100
                if abs(_shares_dev) > 5:
                    _dir = "+" if _shares_dev > 0 else ""
                    L.append(f"  ⚠️ 期中に大規模な株式{'発行' if _shares_dev > 0 else '買戻し'}の可能性（{_dir}{_shares_dev:.1f}%乖離: yf={_yf_implied_shares/1e6:.0f}M vs SEC={_ann_sec_shares/1e6:.0f}M）")
        else:
            L.append("  Dilution_3yr_Annual: N/A")
        L.append(f"  Next_Earnings_Date: {next_earnings}")
        # [[REPORT-TXT-CAPM-IV-MISSING-1]]②: DuPont分解（純利益率×資産回転率×
        # 財務レバレッジ=ROE）。stock.htmlは4カードの詳細表示だが、report.txtでは
        # 分解式1行＋信頼性フラグのみに要約する
        if _dupont and not _dupont.get("excluded"):
            _du_nm = _dupont.get("net_margin")
            _du_at = _dupont.get("asset_turnover")
            _du_fl = _dupont.get("financial_leverage")
            _du_roe = _dupont.get("roe_decomposed")
            if None not in (_du_nm, _du_at, _du_fl, _du_roe):
                L.append(
                    f"  DuPont_ROE: 純利益率{_du_nm*100:.1f}% × 資産回転率{_du_at:.2f}x × "
                    f"財務レバレッジ{_du_fl:.2f}x = ROE{_du_roe*100:.1f}%"
                )
                if _dupont.get("reliability") == "LOW":
                    L.append(f"    ⚠️ DuPont_Reliability: LOW ({_dupont.get('reliability_reason', '')})")
        elif _dupont and _dupont.get("excluded"):
            # DuPont観測性統一（2026-08-29）: 従来はrevenue_too_small専用の
            # 固定文言だった（他の除外理由はdupontキー自体が付与されず
            # この分岐に到達しなかったため気づかれなかった）。除外理由が
            # 複数存在するようになったため、実際のreasonに応じた文言を表示する。
            _du_reason_label = {
                "revenue_too_small": "TTM売上僅少のため比率指標が無意味化",
                "negative_equity": "純資産（自己資本）がマイナスのため財務レバレッジが算出不能",
                "total_assets_unavailable": "総資産データが取得できないため算出不能",
                "equity_unavailable": "純資産（自己資本）データが取得できないため算出不能",
                "ni_ttm_unavailable": "TTM純利益が取得できないため算出不能",
                "revenue_unavailable": "TTM売上が取得できないため算出不能",
                "insufficient_data": "必要なデータが不足しているため算出不能",
            }.get(_dupont.get("reason"), f"reason={_dupont.get('reason', '不明')}")
            L.append(f"  DuPont_ROE: N/A ({_du_reason_label}・除外)")
        L.append("FCF_History:")
        if fcf_history:
            for fh in fcf_history:
                yr = fh.get("year", "?")
                fcf_v = fh.get("fcf")
                fcf_m = fh.get("fcf_margin")
                if fcf_v is not None:
                    fcf_str = f"${fcf_v/1e9:.2f}B"
                    if is_financial:
                        mgn_str = " (Financial sector: N/A)"
                    elif fcf_m is not None:
                        mgn_str = f" (FCF_Margin: {fcf_m:.1f}%)"
                    else:
                        mgn_str = ""
                    L.append(f"  {yr}: {fcf_str}{mgn_str}")
                else:
                    L.append(f"  {yr}: N/A")
            # FCF CAGR 3yr
            valid_fcf = [(fh["year"], fh["fcf"]) for fh in fcf_history if fh.get("fcf") is not None]
            if len(valid_fcf) >= 4:
                yr_old, fcf_old = valid_fcf[-4]
                yr_new, fcf_new = valid_fcf[-1]
                span = yr_new - yr_old  # 実際の年数(年次データ欠落時は3より大きくなる)
                if fcf_old > 0 and fcf_new > 0 and span > 0:
                    cagr = ((fcf_new / fcf_old) ** (1 / span) - 1) * 100
                    L.append(f"  FCF_CAGR_{span}yr: {cagr:+.1f}%")
        else:
            L.append("  N/A")
        L.append("Segment_Breakdown:")
        if segments:
            total_w = sum(s.get("weight", 0) for s in segments)
            for seg in segments:
                name = seg.get("name", "?")
                w = seg.get("weight", 0)
                g = seg.get("growth", 0)
                rev_est = seg.get("estimated_revenue", 0)
                pct = w * 100
                L.append(f"  {name}: ${rev_est/1e9:.1f}B ({pct:.0f}%, YoY {g*100:.0f}%)")
            seg_wg = sum(s.get("weight", 0) * s.get("growth", 0) for s in segments) / max(total_w, 1e-9)
            if not _seg_configured:
                if _phase1_auto_adjusted:
                    _seg_suffix = " (推奨値・中央値を自動適用)"
                elif _seg_ttm_applied:
                    _seg_suffix = " (TTM実績値を自動適用)"
                else:
                    _seg_suffix = " (デフォルト値)"
            else:
                _seg_suffix = ""
            L.append(f"  Segment_Weighted_Growth: {seg_wg*100:.1f}%{_seg_suffix}")
            L.append("  (= basis for TANUKI forward growth rate G)")
        else:
            L.append("  N/A (segment data not configured)")
        L.append("Definition:")
        L.append("")
        L.append("IV Formula (full chain):")
        L.append("  DCF_v0  = sum(FCF_t / (1+Rm)^t) + TV / (1+Rm)^n  [Rm=10%]")
        L.append("  P_t     = DCF_v0 + RPO_PV + Growth_Option_PV  (Alphaは参考値表示のみで計算には使用されません)")
        L.append("  Equity  = P_t + Net_Cash  (= P_t - Net_Debt)")
        L.append("  IV/share = Equity ÷ Shares_Used")
        L.append("Discount rate: Rm=10% (market return, Beta=0 benchmark).")
        L.append("This intentionally excludes market-risk beta to derive")
        L.append("fundamental value independent of market sentiment.")
        L.append("高β銘柄では市場WACC比でIVが高めに出る。これは市場リスクを")
        L.append("意図的に除外した本源価値の設計。市場リスク調整後は")
        L.append("WACC_CAPM_ReferenceでのIVを参照（割高/割安の絶対判断には両者を併用）")
        L.append("WACC_CAPM_Reference: Beta-adjusted CAPM estimate (reference only).")
        L.append("DCF_FCF_PV / DCF_TV_PV above use Rm=10% (sum = DCF_v0).")
        L.append("Alpha: HypeCore expectation premium (ROE × retention / Rm).")
        L.append("Capped per sector; MSFT-class: 1.0, SaaS: 0.8, etc.")
        L.append("RPO: Remaining Performance Obligation. Added outside alpha")
        L.append("to avoid double-counting (v9.0 fix). Applied only to")
        L.append("profitable SaaS with RPO/Revenue > 0.3.")
        L.append("Growth_Option_PV: Optional segment-level growth premium.")
        L.append("Equity bridge: adds Net_Cash (= -Net_Debt) per share.")
        L.append("Net_Debt: Total Debt - Cash - ST_Investments. Negative = net cash.")
        L.append("Lease liabilities excluded (ASC842/IFRS16 operating leases are not")
        L.append("interest-bearing debt; excluded by design. Adjusted EV approach")
        L.append("must be stated explicitly if leases are included.)")
        if sector == "Financial Services":
            L.append("⚠️ Financial_Institution_Note: For banks/fintech (e.g., SOFI), Total_Debt includes")
            L.append("customer deposits and funding liabilities — not comparable to corporate debt.")
            L.append("Net_Debt interpretation is limited; use equity-based metrics instead.")
        L.append("Dilution_3yr_Annual: Share count CAGR over 3 years")
        L.append("Share count source: SEC EDGAR XBRL diluted shares (split-adjusted)")
        L.append("3%/yr = meaningful shareholder dilution risk")
        L.append("FCF_CAGR_3yr: FCF compound growth rate (3yr)")
        L.append("Positive = improving cash generation trend")
        L.append("Segment_Weighted_Growth: Weighted avg of segment growth rates")
        L.append("For manually configured segments: used directly as DCF growth rate G")
        L.append("For unconfigured segments: TTM Revenue Growth is auto-applied,")
        L.append("then adjusted via recommended_g (median of historical CAGR,")
        L.append("industry benchmark, RR×ROIC)")
        L.append("Decay model (TTM/CAGR>50%): fixed 50% TTM + 50% Industry benchmark.")
        L.append("(GROWTH-1's HypeCore-phase-weighted variant was removed")
        L.append("2026-08-26; mixing a Timing-side signal into Funda-side")
        L.append("growth was a design inconsistency with no backtest evidence")
        L.append("of improved accuracy. See [[LAYER1-GROWTH-HYPEPHASE-DECAY-GAP-1]])")
        L.append("Next_Earnings_Date: Next quarterly earnings release")
        L.append("Valuation_Gap_Analysis: Reverse DCF calculation showing")
        L.append("what FCF growth rate the current market price implies.")
        L.append("Useful for growth stocks where DCF underestimates")
        L.append("market-priced future potential.")
        L.append("")
        L.append("")

        # --- [4. 成長率根拠] growth_sanity セクション ---
        has_sanity = growth_sanity is not None
        if has_sanity:
            gs_verdict = growth_sanity.get("verdict", "N/A")
            gs_p1g = growth_sanity.get("phase1_growth")
            gs_ind = growth_sanity.get("damodaran_industry")
            gs_bench = growth_sanity.get("industry_benchmark")
            gs_year = growth_sanity.get("damodaran_year")
            gs_signals = growth_sanity.get("signals", [])
            gs_warnings = growth_sanity.get("warnings", [])
            L.append("[4. 成長率根拠]")
            if _phase1_auto_adjusted and _recommended_g is not None:
                L.append(f"Phase1成長率 : {_recommended_g * 100:.1f}% (DCF適用値・推奨成長率)")
                # GROWTH-VERDICT-SEQUENCING-BUG-1: gs_p1g(growth_sanity.phase1_growth)は
                # 再判定後は_recommended_gと同値になるため、「推奨適用前」の値は
                # extra["phase1_growth_original"]（_phase1_growth_original）を使う。
                L.append(f"元成長率      : {_phase1_growth_original * 100:.1f}% (推奨適用前)" if _phase1_growth_original is not None else "元成長率 : N/A")
            else:
                L.append(f"Phase1成長率 : {gs_p1g * 100:.1f}%" if gs_p1g is not None else "Phase1成長率 : N/A")
            _gs_rec_median = growth_sanity.get("recommended_g_median")
            _gs_rec_g = growth_sanity.get("recommended_g")
            _gs_growth_model = growth_sanity.get("growth_model")
            _gs_growth_model_reason = growth_sanity.get("growth_model_reason")
            _phase_used = growth_sanity.get("hype_phase_used")
            _phase_suffix = f"  HypePhase={_phase_used}" if _phase_used is not None else ""
            if _seg_configured:
                # Layer 1: セグメント加重平均を DCF G として直接使用
                L.append(f"成長モデル: セグメント加重モデル（Layer 1）{_phase_suffix}")
                if gs_p1g is not None:
                    L.append(f"DCF適用値:  {gs_p1g * 100:.1f}%（セグメント加重平均 → DCF G として直接使用）")
                if _gs_rec_g is not None:
                    L.append(f"推奨成長率: {_gs_rec_g * 100:.1f}%（Layer 2 参考値・DCF未適用）")
            else:
                if _gs_growth_model:
                    _model_label = "逓減モデル" if _gs_growth_model == "decay" else "中央値モデル"
                    L.append(f"成長モデル: {_model_label}（{_gs_growth_model_reason}）{_phase_suffix}")
                if _gs_rec_g is not None:
                    L.append("推奨成長率内訳:")
                    if _gs_growth_model == "median" and _gs_rec_median is not None:
                        L.append(f"  中央値ベース:          {_gs_rec_median*100:.1f}%")
                    L.append(f"  最終推奨値:            {_gs_rec_g*100:.1f}%")
            L.append(f"判定         : {gs_verdict}")
            if gs_ind and gs_bench is not None:
                damo_tag = f" (Damodaran {gs_year})" if gs_year else ""
                L.append(f"業界ベンチマーク: {gs_ind} / {gs_bench * 100:.1f}%{damo_tag}")
            else:
                L.append("業界ベンチマーク: N/A")
            L.append("signals:")
            for sig in gs_signals + gs_warnings:
                L.append(f"  - {sig}")
            # GROWTH-STRUCTURAL-MISMATCH-CANDIDATES-1（TRUST-SUMMARY-EPIC-1骨子②）:
            # ハイパーグロース事業と成熟業種平均のミスマッチが原因分析で確定した
            # 銘柄のみへの注記。Classification（BUY/WATCH等）には影響しない。
            if ticker in GROWTH_STRUCTURAL_MISMATCH_TICKERS:
                if ticker == "TER":
                    L.append("  ⚠️ 成長率警告は業界平均比ではなく自社の直近実績成長率比によるもの"
                              "（半導体設備業界のサイクル的な低迷が実績CAGRを一時的に押し下げている"
                              "可能性。Classificationには影響なし）")
                else:
                    L.append("  ⚠️ ハイパーグロース事業と成熟業種平均（Damodaran業種分類）との"
                              "構造的ミスマッチ（Classificationには影響なし）")
            L.append("Definition:")
            L.append("")
            L.append("Growth Rate Methodology:")
            L.append("Layer 1 (Segment configured): Manual segment weighted average")
            L.append("Layer 2 (Segment unconfigured): recommended_g auto-applied to DCF")
            L.append("  If max(rev_cagr_3yr, rev_cagr_5yr, phase1_g) > 50%: decay model")
            L.append("    recommended_g = (min(max_growth,100%) + industry_benchmark) / 2")
            L.append("  Else: median of [rev_cagr_3yr, rev_cagr_5yr,")
            L.append("        industry_benchmark, g_fundamental] (≥2 required)")
            L.append("  (Note: CAGR_max in [4] = max of rev_cagr_3yr/5yr; distinct from TTM_YoY_Growth in [3])")
            L.append("Layer 3: TTM_YoY Revenue Growth (if recommended_g unavailable)")
            L.append("Layer 4: Default 15% (final fallback)")
            L.append("Industry benchmark: Damodaran 2025 fundgrEB.xls")
            L.append("")
            L.append("")

        # セクション番号: growth_sanity あり → [5]〜[8]、なし → [4]〜[7]
        n = 5 if has_sanity else 4
        L.append(f"[{n}. RICE METRICS]")
        L.append(f"Available: {str(rice_available).lower()}")
        L.append(f"Exclusion_Reason: {excl_reason}")
        if rice_available:
            L.append(f"BEAR: RICE={rice_bear_d.get('rice', 'N/A')}")
            L.append(f"BASE: RICE={rice_base_data.get('rice', 'N/A')}")
            L.append(f"BULL: RICE={rice_bull_d.get('rice', 'N/A')}")
        else:
            L.append("BEAR: RICE=N/A")
            L.append("BASE: RICE=N/A")
            L.append("BULL: RICE=N/A")
        L.append("Components (BASE scenario):")
        g_val = rice_base_data.get("growth_rate")
        L.append(f"G  = {g_val*100:.1f}%  [TANUKI forward growth rate]" if g_val is not None else "G  = N/A")
        q_display = "–" if (rice_q != "N/A" and isinstance(rice_q, (int, float)) and abs(rice_q) >= 100) else rice_q
        L.append(f"Q  = {q_display}   [OCF / (NetIncome + SBC), 3yr avg]")
        if rice_cf != "N/A":
            L.append(f"CF = {rice_cf}  [RevGrowth / InvestmentIntensity, 1yr lag, 3yr avg]")
        else:
            L.append("CF = N/A")
        if isinstance(rice_cf_adj, (int, float)) and rice_cf_adj > 0:
            L.append(f"CF_adj = {rice_cf_adj:.4f}  [R&D除外・CapExのみ投資強度]")
            _rice_base_adj = (rice_base_data.get("rice_adj") if rice_available else None)
            if isinstance(_rice_base_adj, (int, float)) and _rice_base_adj > 0:
                L.append(f"RICE_adj = {_rice_base_adj:.3f}  [参考：R&D資本化想定]")
        L.append(f"WACC = {rice_wacc * 100:.1f}%" if isinstance(rice_wacc, (int, float)) else f"WACC = {rice_wacc}")
        _vc_factor = rice.get("vc_factor")
        _roic_wacc_ratio = rice.get("roic_wacc_ratio")
        if _vc_factor is not None:
            L.append(f"VC_Factor = {_vc_factor:.2f}  [RICE-1: clamp(ROIC/WACC,0.3,2.0)]")
            if _roic_wacc_ratio is not None:
                L.append(f"ROIC_WACC_Ratio = {_roic_wacc_ratio:.2f}  [NOPAT/InvestedCapital / 0.10]")
        L.append(f"SBC_Adjusted: {str(sbc_adjusted).lower()}")
        L.append("Definition:")
        L.append("")
        L.append("RICE measures reinvestment efficiency and compounding power")
        L.append("G: Scenario-specific forward revenue growth rate")
        L.append("Q: Cash conversion quality")
        L.append("Q = OCF / (NetIncome + SBC), 3-year average (赤字年・利益ほぼゼロ年は除外)")
        L.append("SBC added back to denominator to normalize")
        L.append("for stock-based compensation distortion")
        L.append("CF: Capital efficiency of growth investment")
        L.append("CF = RevGrowthRate / InvestmentIntensity (1yr lag)")
        L.append("InvestmentIntensity = (R&D + CapEx) / Revenue, 3yr avg")
        L.append("VC_Factor (RICE-1): Value Creation Factor = clamp(ROIC/WACC, 0.3, 2.0)")
        L.append("ROIC > WACC: reinvestment creates value -> G amplified (up to 2x)")
        L.append("ROIC < WACC: reinvestment destroys value -> G penalized (down to 0.3x)")
        L.append("ROIC unavailable (pre-profit): VC_Factor=1.0 (backward compatible)")
        L.append("Discount_Rate_Primary: Actual discount rate applied in DCF (Rm=10%, Beta=0 benchmark)")
        L.append("WACC_CAPM_Reference: Beta-adjusted CAPM estimate (reference only, not used in DCF)")
        L.append("Interpretation: RICE>=3.0=high / 1.0-3.0=medium / 0-1.0=low / <0=undefined (OCF negative)")
        L.append("Note: RICE uses Rm=10% (Beta=0) as a universal benchmark")
        L.append("for cross-ticker comparability. DCF uses ticker-specific WACC.")
        L.append("RICE is a proprietary screening metric inspired by")
        L.append("Damodaran's reinvestment efficiency framework.")
        L.append("Use as relative ranking tool, not absolute valuation.")
        L.append("G used in RICE reflects the auto-adjusted recommended_g")
        L.append("for unconfigured segments.")
        L.append("")
        L.append("")
        L.append(f"[{n+1}. EPS ANALYZER]")
        L.append(f"Latest_Adjusted_EPS: ${adj_eps:.4f} (直近四半期値)" if isinstance(adj_eps, (int, float)) else "Latest_Adjusted_EPS: N/A")
        L.append(f"Latest_GAAP_EPS: ${gaap_eps_val:.4f}" if isinstance(gaap_eps_val, (int, float)) else "Latest_GAAP_EPS: N/A")
        L.append(f"Adjustment_Delta: ${eps_diff:.4f} ({adj_items_str})" if isinstance(eps_diff, (int, float)) else "Adjustment_Delta: N/A")
        L.append(f"YoY_Growth: {yoy_pct}%")
        L.append("Recent Surprises (Adjusted EPS):")
        if recent_quarters:
            for q in recent_quarters:
                act = q.get("actual")
                act_str = f"${act:.4f}" if isinstance(act, (int, float)) else "N/A"
                L.append(f"  {q['label']}: Actual={act_str} vs Est=N/A -> N/A%")
        else:
            L.append("  N/A")
        L.append("Definition:")
        L.append("")
        L.append("EPS_Source: SEC EDGAR XBRL (EPS Basic/Diluted tags). May differ from")
        L.append("press release figures if restatements or segment changes occurred.")
        L.append("Adjusted EPS: GAAP EPS excluding SBC, one-time charges,")
        L.append("M&A costs, and other non-recurring items")
        L.append("Adjustment items classified as:")
        L.append("SBC / Restructuring / Amortization / LegalSettlement /")
        L.append("AcquisitionCost / Other")
        L.append("Surprise Rate = (Actual - Estimate) / |Estimate| x 100")
        L.append("Positive surprise historically correlates with")
        L.append("short-term price appreciation (PEAD effect)")
        # PER comparison
        per_gaap = comps.get("per")
        per_adj = comps.get("per_adjusted")
        # TTM調整後EPSを quarterly.json から計算（Adjusted_EPS_PER の注記用・GAAP PERと同一TTM期）
        ttm_adj_eps_for_note = None
        try:
            _q_path = os.path.join(
                self.repo_root, "docs", "value-monitor", "adjusted_eps_analyzer", "data",
                ticker, "quarterly.json"
            )
            if os.path.exists(_q_path):
                with open(_q_path, encoding="utf-8") as _f:
                    _qdata = json.load(_f)
                _qs = _qdata.get("quarters", [])
                if len(_qs) >= 4:
                    _ttm = sum(q.get("adjusted_eps", 0) for q in _qs[:4])
                    if _ttm > 0:
                        ttm_adj_eps_for_note = _ttm
        except Exception:
            pass
        L.append("PER_Comparison:")
        if per_gaap is not None:
            if per_gaap > 0:
                L.append(f"  Market_PER_GAAP: {per_gaap:.1f}x")
            else:
                L.append("  Market_PER_GAAP: N/M (EPS negative)")
        else:
            L.append("  Market_PER_GAAP: N/A")
        if per_adj is not None:
            note = f" (TTM調整後EPS: ${ttm_adj_eps_for_note:.4f})" if isinstance(ttm_adj_eps_for_note, (int, float)) else ""
            L.append(f"  Adjusted_EPS_PER: {per_adj:.1f}x{note}")
            if per_gaap is not None:
                delta = per_adj - per_gaap
                sign = "cheaper on adjusted basis" if delta < 0 else "more expensive on adjusted basis"
                L.append(f"  Delta: {delta:+.1f}x ({sign})")
        else:
            L.append("  Adjusted_EPS_PER: N/A")
        forward_eps = comps.get("forward_eps")
        L.append("Forward_Estimates:")
        L.append(f"  Next_Earnings_Date: {next_earnings}")
        L.append("  Next_Quarter_EPS: N/A (no analyst estimates in data)")
        if forward_eps is not None:
            L.append(f"  Next_FY_EPS: ${forward_eps:.2f} (yfinance forward EPS)")
        else:
            L.append("  Next_FY_EPS: N/A (no analyst estimates in data)")
        L.append("Definition:")
        L.append("")
        L.append("Market_PER_GAAP: Price / GAAP EPS TTM (market-provided, trailing 12M)")
        L.append("Adjusted_EPS_PER: Price / Adjusted EPS TTM (self-calculated, same trailing 12M period)")
        L.append("Gap between the two reveals SBC and one-time impact")
        L.append("Forward_Estimates: Analyst consensus (external data)")
        L.append("Compare with TANUKI G scenario for cross-validation")
        L.append("")
        L.append("")
        L.append(f"[{n+2}. HYPECORE]")
        L.append(f"Current_Phase: {hype_phase}")
        if hype_alpha != "N/A" and isinstance(hype_alpha, (int, float)):
            L.append(f"Alpha_Premium: {round(hype_alpha, 2)}")
        else:
            L.append("Alpha_Premium: N/A")
        L.append(f"HYPE_Signal: {hype_signal}")
        L.append("Phase_History (recent 6 months):")
        if phase_history:
            for m_str, st, sl in phase_history:
                L.append(f"{m_str}: Phase{st} ({sl})")
        else:
            L.append("N/A")

        # Valuation multiples (market-provided via yfinance)
        per_val = comps.get("per")
        peg_val = comps.get("peg")
        ps_val = comps.get("ps")
        ev_ebitda_val = comps.get("ev_ebitda")
        L.append("Valuation_Multiples (market-provided, yfinance):")
        L.append(f"  PER: {per_val:.1f}x" if per_val is not None else "  PER: N/A")
        L.append(f"  PEG: {peg_val:.2f}" if peg_val is not None else "  PEG: N/A")
        L.append(f"  PS:  {ps_val:.1f}x" if ps_val is not None else "  PS:  N/A")
        L.append(f"  EV_EBITDA: {ev_ebitda_val:.1f}x" if ev_ebitda_val is not None else "  EV_EBITDA: N/A")
        if peg_val is not None:
            if peg_val < 1.0:
                L.append("  PEG_Signal: 割安水準（成長率対比でPEG<1.0）")
            elif peg_val <= 2.0:
                L.append("  PEG_Signal: 適正水準（成長率対比でPEG 1.0〜2.0）")
            else:
                L.append("  PEG_Signal: 割高水準（成長率対比でPEG>2.0）")
        if ps_val is not None:
            if ps_val > 20:
                L.append("  PS_Signal: 高水準（売上対比で高い期待が織り込み済み）")
            elif ps_val < 5:
                L.append("  PS_Signal: 低水準（売上対比で低い期待）")
            else:
                L.append("  PS_Signal: 適正水準")
            # A-6: yfinance PS と自社計算値の乖離検出（ステール値の可能性）
            _ps_shares = comps.get("diluted_shares") or comps.get("implied_shares") or 0
            _ps_rev    = comps.get("latest_revenue") or 0
            _is_fin    = "financial" in (sector or "").lower() or "bank" in (sector or "").lower()
            if current_price and _ps_shares and _ps_rev and not _is_fin:
                _ps_calc = (current_price * _ps_shares) / _ps_rev
                if _ps_calc > 0:
                    _ps_ratio = ps_val / _ps_calc
                    if _ps_ratio > 2.5 or _ps_ratio < 0.4:
                        L.append(
                            f"  PS_AnomalyWarn: yfinance PS({ps_val:.1f}x) 自社計算({_ps_calc:.1f}x)"
                            f" 乖離{_ps_ratio:.1f}倍 → ステール値の可能性"
                        )

        L.append("Definition:")
        L.append("")
        L.append("HypeCore measures market sentiment and hype lifecycle stage")
        L.append("Phase1 (黎明期/Dawn): Early awareness, pre-euphoria,")
        L.append("price below MA200, low momentum")
        L.append("Phase2 (期待拡大期/Expansion): Rising expectations,")
        L.append("price approaching or above MA200, momentum building")
        L.append("Phase3 (陶酔期/Euphoria): Peak sentiment, price well above")
        L.append("MA200, high RSI, strong volume surge")
        L.append("Phase4 (期待剥落期/Deflation): Sentiment reversal,")
        L.append("price falling from peak, expectation reset")
        L.append("Alpha: Growth expectation premium (reference value only,")
        L.append("not applied to IV calculation; see [3] Alpha_Premium)")
        L.append("Higher alpha in Phase1-2, lower in Phase3-4")
        L.append("HYPE_Signal: Combined judgment of Matrix quadrant")
        L.append("and HypeCore phase")
        # ── DESIGN-1: ERP（株式リスクプレミアム）参考表示 ──
        # [[ERP-DUAL-CALC-1]]: 計算式は_calculate_erp()に統合済み（_save_result()と共通）
        _fwd_eps_erp = comps.get("forward_eps")
        _rf_erp = wacc_data.get("risk_free_rate", 0.043) if isinstance(wacc_data, dict) else 0.043
        _ey, _erp = _calculate_erp(_fwd_eps_erp, current_price, _rf_erp)
        L.append("ERP (Equity Risk Premium, 参考表示):")
        if _erp is not None:
            L.append(f"  Forward_Earnings_Yield: {_ey*100:.2f}%  (ForwardEPS ${_fwd_eps_erp:.2f} / Price ${current_price:,.2f})")
            L.append(f"  Risk_Free_Rate (10Y):  {_rf_erp*100:.2f}%")
            L.append(f"  ERP: {_erp*100:.2f}%")
            if _erp >= 0.04:
                L.append("  ERP_Signal: 期待冷却圏（成長期待が市場に織り込まれていない）")
            elif _erp >= 0.02:
                L.append("  ERP_Signal: 期待中立〜やや冷却（成長期待は限定的）")
            elif _erp >= 0.0:
                L.append("  ERP_Signal: 期待やや過熱（成長期待が株価に織り込まれ始めている）")
            else:
                L.append("  ERP_Signal: 期待過熱圏（市場が将来成長を過度に先取りしている）")
        else:
            L.append("  N/A (ForwardEPS 未取得)")

        L.append("Valuation_Multiples: Market-provided values (yfinance),")
        L.append("NOT self-calculated. Distinct from TANUKI IV.")
        L.append("PER: Price / GAAP EPS (trailing)")
        L.append("PEG: PER / Forward EPS Growth Rate")
        L.append("PEG<1.0 = growth not fully priced in")
        L.append("PEG>2.0 = high growth expectation embedded")
        L.append("PS: Price / Revenue (TTM)")
        L.append("High PS = market expects significant margin expansion")
        L.append("EV/EBITDA: Debt-adjusted valuation multiple")
        L.append("ERP: Forward Earnings Yield - Risk Free Rate (10Y Treasury)")
        L.append("ERP reflects market EXPECTATION level, not price cheapness.")
        L.append("ERP<0%  = 期待過熱圏: market prices in aggressive future growth")
        L.append("ERP0-2% = 期待やや過熱: growth expectations building in price")
        L.append("ERP2-4% = 期待中立: moderate growth expectations")
        L.append("ERP≥4%  = 期待冷却圏: little growth expectation in price")
        L.append("Note: Uses ForwardEPS from yfinance (analyst consensus).")
        L.append("")
        L.append("")
        L.append(f"[{n+3}. STONKS SILO]")
        L.append(f"Short_Report_Target: {short_target}")
        L.append(f"Short_Interest: {short_int}%")
        L.append("Institutional_Ownership: N/A (not in data source)")
        L.append(f"Analyst_Consensus: {_analyst_str}")
        _ins_buy = comps.get("insider_buy_count")
        _ins_sell = comps.get("insider_sell_count")
        _ins_dir = comps.get("insider_net_direction")
        if _ins_buy is not None:
            L.append(f"Insider_Activity: {_ins_dir} | 買い{_ins_buy}件 / 売り{_ins_sell}件 | 直近90日")
        else:
            L.append("Insider_Activity: N/A")
        L.append(f"Runway_Months: {runway_m}" if runway_m != "N/A" else "Runway_Months: N/A (profitable or not in STONKS)")
        L.append(f"Revenue_Growth_YoY: {rev_growth_str}%")
        L.append(f"Next_Earnings_Date: {next_earnings}")
        breakeven_est = extra.get("breakeven_estimate")
        breakeven_reason_disp = extra.get("breakeven_reason")
        if breakeven_reason_disp == "ACHIEVED":
            L.append("Breakeven_Estimate: 黒字化：達成済み")
        elif breakeven_est is not None:
            L.append(f"Breakeven_Estimate: {breakeven_est}年頃（推定、reason=PREDICTED）")
        elif breakeven_reason_disp:
            _be_reason_m = {"NO_TREND": "黒字化予測不可（改善傾向なし）", "NO_DATA": "データ不足", "TOO_FAR": "5年超"}
            L.append(f"Breakeven_Estimate: {_be_reason_m.get(breakeven_reason_disp, breakeven_reason_disp)}")
        else:
            L.append("Breakeven_Estimate: N/A (profitable or insufficient data)")
        L.append("Definition:")
        L.append("")
        L.append("Short_Interest: % of float sold short")
        L.append("High short interest -> squeeze risk if positive catalyst")
        L.append("Institutional_Ownership: % held by institutions")
        L.append("High ownership -> stable shareholder base")
        L.append("Runway: Cash / Annual_FCF_Burn_Rate (years)")
        L.append("Critical for pre-profit companies; <1yr = distress risk")
        L.append("Revenue_Growth_YoY: TTM vs prior TTM")
        L.append("Note: TTM may reflect restated (continuing operations) figures for")
        L.append("companies that divested business units (e.g., APP Apps segment 2024).")
        L.append("Primary growth metric for RICE-excluded tickers")
        L.append("Breakeven_Estimate: Projected year when adjusted EPS turns positive")
        L.append("based on recent 4Q linear trend. Rough estimate only.")
        L.append("")
        L.append("==============================================")
        L.append("DISCLAIMER: This report is generated automatically")
        L.append("from financial data for reference purposes only.")
        L.append("Not investment advice.")

        return "\n".join(L)

    def _load_extra_data(self, ticker: str, valuation: dict) -> dict:
        """sec_data annual JSONとyfinanceからfcf_history/financial_health/next_earnings_dateを取得"""
        result = {}
        sec_dir = os.path.join(self.repo_root, "common", "sec_data", "data", ticker)
        comps = valuation.get("components", {})
        latest_revenue = comps.get("latest_revenue") or 0

        # --- FCF history & financial health from annual_YYYY.json ---
        fcf_history = []
        debt_cash_by_year = {}
        sbc_by_year = {}

        if os.path.exists(sec_dir):
            years_available = sorted([
                int(fn[7:11]) for fn in os.listdir(sec_dir)
                if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit()
            ], reverse=True)[:5]

            for yr in sorted(years_available):
                ann_path = os.path.join(sec_dir, f"annual_{yr}.json")
                try:
                    with open(ann_path, encoding="utf-8") as f:
                        ann = json.load(f)
                    cf = ann.get("cf", {})
                    bs = ann.get("bs", {})
                    rev = ann.get("pl", {}).get("revenue") or 0
                    fcf_val = cf.get("free_cash_flow")
                    fcf_margin = (fcf_val / rev * 100) if (fcf_val is not None and rev) else None
                    fcf_history.append({
                        "year": yr,
                        "fcf": fcf_val,
                        "fcf_margin": round(fcf_margin, 1) if fcf_margin is not None else None,
                    })
                    debt_cash_by_year[yr] = {
                        "lt_debt": bs.get("long_term_debt", 0) or 0,
                        "st_debt": bs.get("short_term_debt", 0) or 0,
                        "cash": bs.get("cash_and_equivalents", 0) or 0,
                    }
                    sbc_by_year[yr] = cf.get("stock_based_compensation")
                except Exception:
                    pass

        result["fcf_history"] = fcf_history

        # net_debt / SBC from latest available year
        if debt_cash_by_year:
            latest_yr = max(debt_cash_by_year.keys())

            # BUG-NETDEBT-4（同一時点原則）・BUG-NETDEBT-2/3（LTDebt正規化補完）・
            # セクターガード（保険/fintech）は reader.py::get_net_cash() に一本化済み
            # （ARCH-DATA-1残課題①）。calculate_pt() 内で既に計算済みの bs_adjustment
            # （get_net_cash() の戻り値をそのまま格納したもの）を再利用することで、
            # report.txt表示・TANUKI SCORE判定に使うBS値と、実際のDCF計算
            # （bs_adjustment.net_cash_per_share）に使うBS値を単一の計算経路に統一する。
            bs_adj = valuation.get("bs_adjustment", {})
            cash = bs_adj.get("cash", 0.0)
            st_invest = bs_adj.get("short_term_investments", 0.0)
            total_debt = bs_adj.get("long_term_debt", 0.0) + bs_adj.get("short_term_debt", 0.0)
            net_debt = -bs_adj.get("net_cash", 0.0)
            _net_debt_period = bs_adj.get("net_debt_period") or f"FY{latest_yr}"

            # total_debt=0 かつ total_liabilities が大きい場合は警告（金融機関等）
            _ann_total_liab = 0
            try:
                _ann_bs = json.load(open(os.path.join(sec_dir, f"annual_{latest_yr}.json"), encoding="utf-8")).get("bs", {})
                _ann_total_liab = _ann_bs.get("total_liabilities") or 0
            except Exception:
                pass
            if total_debt == 0 and _ann_total_liab > 1_000_000_000:
                print(f"[WARN] [{ticker}] total_debt=0 だが total_liabilities=${_ann_total_liab/1e9:.1f}B（金融機関等の可能性）")

            result["financial_health"] = {
                "total_debt": total_debt,
                "cash_and_equivalents": cash,
                "short_term_investments": st_invest,
                # FY52WEEK-BS-FADEOUT-FALLBACK-1: long_term_debt/short_term_debtの
                # 個別値を露出する（従来はtotal_debtへの合算のみでreport.txt側で
                # 個別表示できなかった）。
                "long_term_debt": bs_adj.get("long_term_debt", 0.0),
                "short_term_debt": bs_adj.get("short_term_debt", 0.0),
                "net_debt": net_debt,
                "sbc_ttm": sbc_by_year.get(latest_yr),
                "net_debt_period": _net_debt_period,
                # FY52WEEK-BS-NULL-SILENT-1 Phase A: get_net_cash()由来のcash_missing
                # をそのまま伝播する。get_net_cash()がavailable=Falseにフォールバック
                # 済みのためnet_debt自体は誤って使われないが、report.txt/latest.json
                # を直接参照する側にも「cashがNoneだった」ことを可視化する。
                "cash_missing": bs_adj.get("cash_missing", False),
                # NVDA-STI-TAG-UNIDENTIFIED-1: short_term_investmentsがticker_
                # restrictionsのcross_filing_tags経由の複数タグ合算近似値の場合
                # True・その残差率。report.txtのST_Invest行注記に使用する。
                "sti_approximated": bs_adj.get("sti_approximated", False),
                "sti_residual_pct": bs_adj.get("sti_residual_pct"),
                # FY52WEEK-BS-FADEOUT-FALLBACK-1: 過去の直近既知値が明示的0
                # だったため真のゼロと推定した場合True・その最終確認年度。
                # report.txtの該当行に「推定ゼロ（最終確認: FY20XX）」を注記する。
                "sti_estimated_zero": bs_adj.get("sti_estimated_zero", False),
                "sti_last_confirmed_zero_year": bs_adj.get("sti_last_confirmed_zero_year"),
                "ltdebt_estimated_zero": bs_adj.get("ltdebt_estimated_zero", False),
                "ltdebt_last_confirmed_zero_year": bs_adj.get("ltdebt_last_confirmed_zero_year"),
                "stdebt_estimated_zero": bs_adj.get("stdebt_estimated_zero", False),
                "stdebt_last_confirmed_zero_year": bs_adj.get("stdebt_last_confirmed_zero_year"),
            }
            # フォールバックRunway: stonks-siloにない銘柄でも資金枯渇リスクを検出
            # 条件: 直近四半期EPS<0, 直近年FCF<0, またはcash<$100M のいずれか
            _latest_fcf = fcf_history[-1]["fcf"] if fcf_history else None
            _latest_q_eps = self._load_eps_map().get(ticker, {}).get("gaap_eps")
            _needs_runway = (
                (_latest_q_eps is not None and _latest_q_eps < 0)
                or (_latest_fcf is not None and _latest_fcf < 0)
                or cash < 100_000_000
            )
            if _needs_runway and _latest_fcf is not None and _latest_fcf < 0:
                _monthly_burn = abs(_latest_fcf) / 12
                if _monthly_burn > 0:
                    result["computed_runway_months"] = cash / _monthly_burn

        # 希薄化率: 株式分割調整済みのLayer3 shares_diluted年次データを使用
        # （フェーズD Step2-1、normalized/からLayer3へ切替）
        _layer3_store_dil = self._get_layer3_store(ticker)
        if _layer3_store_dil is not None:
            try:
                shares_series = get_field_entries(_layer3_store_dil, "shares_diluted")
                annual_shares = sorted(
                    [e for e in shares_series if e.get("is_annual") and e.get("fp") == "FY" and e.get("val")],
                    key=lambda e: e.get("end", "")
                )
                if len(annual_shares) >= 4:
                    s_now_entry = annual_shares[-1]
                    s_3ago_entry = annual_shares[-4]
                    yr_now = s_now_entry.get("end", "")[:4]
                    yr_3ago = s_3ago_entry.get("end", "")[:4]

                    # 株式分割による不連続を検出・補正（最新基準に統一）
                    # 判定: FY値の前年比が2.5倍超 かつ 同一会計年度の四半期中央値と2.5倍超乖離
                    #   → 遡及的分割調整（stock split retroactive restatement）として補正
                    # 判定: FY値の前年比が2.5倍超 だが 四半期中央値と近い
                    #   → IPO等の実際の希薄化として補正なし
                    #
                    # 四半期グルーピングは年次end日を起点にtrailing 12ヶ月窓で行う
                    # （LRCX-SPLIT-1の教訓・CHECK-QREV-FYE-1と同型バグ）:
                    # 暦年ラベル（end[:4]）でグルーピングすると非12月決算企業
                    # （LRCX=6月期等）で誤判定する。LRCXの10:1分割（2024-10実施）は
                    # SECの比較開示ウィンドウの都合で、一部四半期のみが後続filingで
                    # 分割後ベースに遡及修正され、別四半期は未修正のまま残っていた。
                    # 暦年グルーピングだと同一暦年内に「修正済み・未修正」の四半期が
                    # 混在し、中央値が偶然「分割後基準」に一致して分割を見逃していた。
                    # [[LAYER3-SHARESDILUTED-TAG-GAP-1]]案2対応: Layer3の
                    # shares_dilutedはCommonStockSharesOutstanding
                    # （期末発行済株式数、BS概念）をフォールバックタグとして
                    # 含むため、加重平均（PL概念）でないエントリが混入する。
                    # 分割検知の中央値計算をWeightedAverageNumberOf
                    # DilutedSharesOutstanding由来のみに絞り、概念混在を防ぐ。
                    q_entries_all = [
                        e for e in shares_series
                        if not e.get("is_annual") and e.get("val") and e.get("end")
                        and e.get("source_tag") == "WeightedAverageNumberOfDilutedSharesOutstanding"
                    ]

                    raw_vals = [e["val"] for e in annual_shares]
                    adj_vals = list(raw_vals)
                    split_factor = 1.0
                    for i in range(len(raw_vals) - 1, 0, -1):
                        ratio = raw_vals[i] / raw_vals[i - 1] if raw_vals[i - 1] > 0 else 1.0
                        if ratio > 2.5 or ratio < 0.4:
                            # 四半期値との比較で遡及的分割調整か実際の希薄化かを判別
                            # （ARCH-DATA-1残課題①: quarters_in_trailing_window()に一本化。
                            #  quarterly.py::check_revenue_quality()と同一の窓計算を使用）
                            try:
                                from common.sec_data.utils import quarters_in_trailing_window
                                q_pairs = [(e["end"], e["val"]) for e in q_entries_all]
                                q_list = quarters_in_trailing_window(q_pairs, annual_shares[i]["end"])
                            except (ValueError, KeyError):
                                q_list = []
                            if q_list:
                                q_median = sorted(q_list)[len(q_list) // 2]
                                fy_q_ratio = raw_vals[i] / q_median if q_median > 0 else 1.0
                                is_split = fy_q_ratio > 2.5 or fy_q_ratio < 0.4
                            else:
                                is_split = False  # 四半期データなし → 実際の希薄化と判断
                            if is_split:
                                split_factor *= ratio
                                for j in range(i):
                                    adj_vals[j] = raw_vals[j] * split_factor

                    s_now = adj_vals[-1]
                    s_3ago = adj_vals[-4]

                    # SEC株式数がyfinance由来の株式数と10倍以上乖離している場合は
                    # XBRLの報告が信頼できない（UP-C構造等）→希薄化計算をスキップ
                    _yf_shares = comps.get("diluted_shares") or 0
                    if _yf_shares > 0 and s_now > 0:
                        _sec_yf_ratio = _yf_shares / s_now
                        if _sec_yf_ratio > 10 or _sec_yf_ratio < 0.1:
                            s_now = None  # sanity check fail → skip

                    if s_now and s_3ago > 0:
                        dilution_3yr = ((s_now / s_3ago) ** (1 / 3) - 1) * 100
                        if "financial_health" not in result:
                            result["financial_health"] = {}
                        result["financial_health"]["dilution_3yr_annual_pct"] = round(dilution_3yr, 2)
                        result["financial_health"]["shares_yr_now"] = s_now_entry["val"]
                        result["financial_health"]["shares_yr_3ago"] = s_3ago_entry["val"]
                        result["financial_health"]["shares_yr_now_label"] = yr_now
                        result["financial_health"]["shares_yr_3ago_label"] = yr_3ago
                        if abs(split_factor - 1.0) > 0.01:
                            result["financial_health"]["split_adjusted"] = True
                            result["financial_health"]["split_factor"] = round(split_factor, 4)
            except Exception:
                pass

        # --- DuPont分解（ROE = Net Margin × Asset Turnover × Financial Leverage）---
        try:
            _ttm_path_du = os.path.join(
                self.repo_root, "common", "sec_data", "ttm",
                f"{ticker}_ttm_series.json"
            )
            _sec_dir_du = os.path.join(self.repo_root, "common", "sec_data", "data", ticker)
            _q_files_du = sorted([
                f for f in os.listdir(_sec_dir_du)
                if f.startswith("quarterly_") and f.endswith(".json")
            ]) if os.path.exists(_sec_dir_du) else []

            _ni_ttm_du = None
            _rev_ttm_du = None
            _total_assets_du = None
            _equity_du = None
            _ttm_entry_du = None

            if os.path.exists(_ttm_path_du):
                with open(_ttm_path_du, encoding="utf-8") as _tf_du:
                    _ttm_s_du = json.load(_tf_du)
                _ttm_entry_du = (_ttm_s_du.get("series") or [None])[0]
                if _ttm_entry_du:
                    # [[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]: ttm_calculator.py::
                    # FLOW_FIELDSはsnake_case（"net_income"/"revenue"/"buyback"）
                    # で辞書を構築するが、ここは以前PascalCase（"NetIncome"等）を
                    # 参照しており常にNoneになっていた（全104銘柄でdupont未設定）。
                    _flow_du = _ttm_entry_du.get("flow") or {}
                    _ni_ttm_du = _flow_du.get("net_income", {}).get("val")
                    _rev_ttm_du = _flow_du.get("revenue", {}).get("val")
                    _buyback_ttm_du = _flow_du.get("buyback", {}).get("val")
                    # buyback_ttmをfinancial_healthに追記（キャッシュトラップ検出用）
                    if _buyback_ttm_du is not None and "financial_health" in result:
                        result["financial_health"]["buyback_ttm"] = abs(_buyback_ttm_du)

            _du_period = None
            if _q_files_du:
                with open(os.path.join(_sec_dir_du, _q_files_du[-1]), encoding="utf-8") as _qf_du:
                    _du_full = json.load(_qf_du)
                _du_bs = _du_full.get("bs", {})
                _total_assets_du = _du_bs.get("total_assets")
                _equity_du = _du_bs.get("stockholders_equity")
                _du_period = _du_full.get("period") or _q_files_du[-1]
                # シガーバット用: 純流動資産/時価総額 を計算
                _cur_assets_du = _du_bs.get("current_assets")
                _cur_liab_du   = _du_bs.get("current_liabilities")
                if _cur_assets_du is not None and _cur_liab_du is not None:
                    _nca = _cur_assets_du - _cur_liab_du
                    _shares_du = comps.get("diluted_shares") or 0
                    _price_du  = comps.get("current_price") or 0
                    _mktcap_du = _shares_du * _price_du
                    if _mktcap_du > 0:
                        result["net_current_assets_ratio"] = round(_nca / _mktcap_du, 4)

            # ROE-DUPONT-4: TTM売上が極小（$15M未満）の銘柄は比率指標が無意味化するため除外
            # TANUKI-ROE-3: $10M→$15Mに引き上げ（QBTS: TTM Revenue=$12.4Mが$10M閾値では
            # 除外対象外のままnet_margin=-2008%等の極端値が表示され続けていた問題への対応）
            if _rev_ttm_du is not None and _rev_ttm_du < 15_000_000:
                result["dupont"] = {"excluded": True, "reason": "revenue_too_small"}
            elif (
                _ni_ttm_du is not None
                and _rev_ttm_du and _rev_ttm_du > 0
                and _total_assets_du and _total_assets_du > 0
                and _equity_du and _equity_du > 0
            ):
                _net_margin_du = _ni_ttm_du / _rev_ttm_du
                _asset_turn_du = _rev_ttm_du / _total_assets_du
                _fin_lev_du = _total_assets_du / _equity_du
                result["dupont"] = {
                    "ni_ttm": _ni_ttm_du,
                    "revenue_ttm": _rev_ttm_du,
                    "total_assets": _total_assets_du,
                    "equity": _equity_du,
                    "net_margin": round(_net_margin_du, 4),
                    "asset_turnover": round(_asset_turn_du, 4),
                    "financial_leverage": round(_fin_lev_du, 4),
                    "roe_decomposed": round(_net_margin_du * _asset_turn_du * _fin_lev_du, 4),
                    # ROE-DUPONT-2: Net Debt実装(net_debt_period)と同水準のトレーサビリティ
                    "dupont_bs_period": _du_period,
                }

                # ROE-DUPONT-1: 単四半期NI集中チェック（一過性要因の検出・DCF_Reliability=LOWと同形式）
                # TTM4四半期のうち最大1Qのnet_incomeがTTM合計の60%超を占める場合は信頼性LOWとする
                # 分母は Layer3 store から再構成した直近4四半期合計を使う（ttm_calculator側の
                # implied-Q4二重計上の影響を受けないよう、TTM seriesのni_ttmは分母に使わない）
                try:
                    _ni_q_count_du = _flow_du.get("net_income", {}).get("quarters_used")
                    if _ni_q_count_du == 4 and _ttm_entry_du is not None:
                        # フェーズD Step2-1: normalized/からLayer3へ切替。
                        # フィルタは既存動作を維持（is_annualのみ除外、
                        # is_ytdは除外しない。is_ytdフィルタの扱いは
                        # 本切替のスコープ外、別途判断）。
                        _layer3_store_du = self._get_layer3_store(ticker)
                        if _layer3_store_du is not None:
                            _ni_by_end_du: dict = {}
                            for e in get_field_entries(_layer3_store_du, "net_income"):
                                if (
                                    not e.get("is_annual")
                                    and e.get("val") is not None
                                    and e.get("end", "") <= _ttm_entry_du.get("ttm_end", "")
                                ):
                                    _ni_by_end_du[e["end"]] = e["val"]  # end日付で重複排除
                            _ni_entries_du = sorted(
                                _ni_by_end_du.items(), key=lambda kv: kv[0], reverse=True
                            )[:4]
                            if len(_ni_entries_du) == 4:
                                _ni_4q_sum_du = sum(v for _, v in _ni_entries_du)
                                _max_abs_q_du = max((v for _, v in _ni_entries_du), key=abs)
                                if _ni_4q_sum_du and abs(_max_abs_q_du) / abs(_ni_4q_sum_du) > 0.6:
                                    result["dupont"]["reliability"] = "LOW"
                                    result["dupont"]["reliability_reason"] = "単四半期NI集中（一過性要因の可能性）"
                except Exception:
                    pass
            else:
                # DuPont観測性統一（2026-08-29）: 従来はrevenue_too_small以外の
                # 計算不能ケース（特に負の純資産＝自社株買い等でstockholders_
                # equity<=0になる銘柄。ABBV/BKNG/DELL/FICO/MO/MSCI/PM/RBRKで
                # 実測確認済み。財務レバレッジ=総資産/純資産が負値・符号逆転して
                # 意味を成さないため計算不能とする）で"dupont"キー自体が
                # 無言で欠落し、QBTSのrevenue_too_small除外のような明示理由が
                # 付与されなかった。全ての計算不能ケースで理由を明示する。
                if _equity_du is not None and _equity_du <= 0:
                    _du_reason = "negative_equity"
                elif _total_assets_du is None or _total_assets_du <= 0:
                    _du_reason = "total_assets_unavailable"
                elif _equity_du is None:
                    _du_reason = "equity_unavailable"
                elif _ni_ttm_du is None:
                    _du_reason = "ni_ttm_unavailable"
                elif not _rev_ttm_du or _rev_ttm_du <= 0:
                    _du_reason = "revenue_unavailable"
                else:
                    _du_reason = "insufficient_data"
                result["dupont"] = {"excluded": True, "reason": _du_reason}
        except Exception as _e_du:
            # [[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]: 今回のキー名不一致自体は
            # 例外を投げず条件不成立で沈黙していたため本except自体は無関係
            # だったが、今後同種の異常（ファイル破損等）が沈黙しないよう
            # ログ出力を追加する（挙動は変更せず、DuPont分解のみスキップして
            # 処理を続行する）
            print(f"   [{ticker}] DuPont分解エラー: {_e_du}")

        # --- next_earnings_date from common.market_data.reader.get_calendar() ---
        # [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-4: yfinance直接呼び出し
        # （.calendar）をcommon.market_data.reader経由に切替（2026-08-11）。
        # 過去日の場合はリスト内の次の未来日を採用するロジックは維持
        # （Phase3-1修正）。reader側がcalendar未取得銘柄に対して返す空dictは
        # 中立デフォルトとして扱い、next_earnings_dateはresultに追加されない
        # （旧コードの「cal取得失敗・空」の場合と同じ挙動）。
        try:
            from datetime import date as _date_cls
            cal = _md_get_calendar(ticker) if HAS_MARKET_DATA else {}
            dates = cal.get("earnings_date") if cal else None
            if dates:
                today = _date_cls.today()
                _next = None
                for _d in dates:
                    try:
                        _d_val = _date_cls.fromisoformat(str(_d)[:10])
                        if _d_val >= today:
                            _next = _d_val
                            break
                    except Exception:
                        continue
                if _next is not None:
                    result["next_earnings_date"] = str(_next)
                else:
                    result["next_earnings_date"] = str(dates[0])  # 全て過去なら最後の値
        except Exception:
            pass

        # --- breakeven estimate from EPS quarterly.json (deficit tickers only) ---
        # [[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]: 計算本体は
        # compute_eps_breakeven()（モジュール直下、単体テスト可能）に
        # 切り出し済み
        eps_q_path = os.path.join(
            self.repo_root, "docs", "value-monitor", "adjusted_eps_analyzer", "data", ticker, "quarterly.json"
        )
        if os.path.exists(eps_q_path):
            try:
                with open(eps_q_path, encoding="utf-8") as f:
                    eps_q_data = json.load(f)
                quarters_list = eps_q_data.get("quarters", [])
                from datetime import datetime as _dt
                breakeven_estimate, breakeven_reason = compute_eps_breakeven(quarters_list, _dt.now().year)
                if breakeven_estimate is not None:
                    result["breakeven_estimate"] = breakeven_estimate
                if breakeven_reason is not None:
                    result["breakeven_reason"] = breakeven_reason
            except Exception:
                pass

        # --- segment_config ---
        seg_path = os.path.join(self.repo_root, "config", "segment_config.json")
        if os.path.exists(seg_path):
            try:
                with open(seg_path, encoding="utf-8") as f:
                    seg_conf = json.load(f).get(ticker, {})
                segs = seg_conf.get("segments", {})
                if not segs:
                    result["segment_configured"] = False
                elif segs and latest_revenue:
                    is_general_fallback = (len(segs) == 1 and "General" in segs)
                    seg_list = []
                    for name, info in segs.items():
                        w = info.get("weight", 0)
                        g = info.get("growth", 0)
                        # General fallback: TTMオーバーライドが注入されていれば表示用growthも合わせる
                        if is_general_fallback and name == "General":
                            ttm_override = _seg_cfg._GROWTH_OVERRIDES.get(ticker)
                            if ttm_override is not None:
                                g = ttm_override
                            # A-3: TTM売上が年次より新しければそちらを使用(非12月期銘柄の陳腐化対策)
                            _ttm_path = os.path.join(
                                self.repo_root, "common", "sec_data", "ttm",
                                f"{ticker}_ttm_series.json"
                            )
                            _ttm_rev_for_seg = latest_revenue
                            if os.path.exists(_ttm_path):
                                try:
                                    with open(_ttm_path, encoding="utf-8") as _tf:
                                        _ttm_s = json.load(_tf)
                                    _ttm_entry = (_ttm_s.get("series") or [None])[0]
                                    if _ttm_entry:
                                        # [[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]:
                                        # 同根のキー不一致（下記DuPont分解と同様）
                                        _rv = (_ttm_entry.get("flow") or {}).get("revenue", {}).get("val")
                                        if _rv and float(_rv) > latest_revenue:
                                            _ttm_rev_for_seg = float(_rv)
                                except Exception as _e_seg:
                                    # [[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]: 今回の
                                    # キー名不一致自体は例外を投げず沈黙していたが、
                                    # 今後同種の異常が沈黙しないようログ出力を追加
                                    print(f"   [{ticker}] Segment TTM売上フォールバックエラー: {_e_seg}")
                            est_rev = _ttm_rev_for_seg * w
                        else:
                            est_rev = latest_revenue * w
                        seg_list.append({
                            "name": name,
                            "weight": w,
                            "growth": g,
                            "estimated_revenue": est_rev,
                        })
                    result["segments"] = seg_list
                    if is_general_fallback:
                        result["segment_configured"] = False
                        result["segment_ttm_applied"] = ticker in _seg_cfg._GROWTH_OVERRIDES
                    else:
                        result["segment_configured"] = True
                        result["segment_ttm_applied"] = False
            except Exception:
                pass

        return result

    def _load_annual_revenues(self, ticker: str) -> list:
        """SEC年次データから売上高リストを古い順で返す"""
        sec_dir = os.path.join(self.repo_root, "common", "sec_data", "data", ticker)
        if not os.path.exists(sec_dir):
            return []
        years = sorted([
            int(fn[7:11]) for fn in os.listdir(sec_dir)
            if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit()
        ])
        revs = []
        for yr in years:
            try:
                with open(os.path.join(sec_dir, f"annual_{yr}.json"), encoding="utf-8") as f:
                    ann = json.load(f)
                rev = ann.get("pl", {}).get("revenue")
                if rev and rev > 0:
                    revs.append(float(rev))
            except Exception:
                pass
        return revs

    def _get_lt_debt_fallback(self, ticker: str) -> float:
        """annual JSONでlong_term_debtが欠落した場合の補完値を取得（BUG-NETDEBT-3共通化）。

        フェーズD Step2-1: normalized/経由（SECReader.get_lt_debt_from_
        normalized()）からLayer3経由（get_long_term_debt_latest()）へ
        切替。normalized/へのフォールバックは行わない（2026-08-06の
        SEC EDGAR照合で、両者が食い違う3銘柄〈RCAT/SPIR/CPRT〉全てで
        Layer3側がより正確・現行と確認済み。詳細は
        get_long_term_debt_latest()のdocstring参照）。
        """
        store = self._get_layer3_store(ticker)
        if store is None:
            return 0.0
        return get_long_term_debt_latest(store)

    def _calc_g_fundamental(self, ticker: str) -> float | None:
        """最新年次データから RR×ROIC ファンダメンタル成長率を計算"""
        sec_dir = os.path.join(self.repo_root, "common", "sec_data", "data", ticker)
        if not os.path.exists(sec_dir):
            return None
        years = sorted([
            int(fn[7:11]) for fn in os.listdir(sec_dir)
            if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit()
        ])
        if not years:
            return None
        try:
            with open(os.path.join(sec_dir, f"annual_{years[-1]}.json"), encoding="utf-8") as f:
                ann = json.load(f)
            pl = ann.get("pl", {})
            cf = ann.get("cf", {})
            bs = ann.get("bs", {})
            # FY52WEEK-BS-NULL-SILENT-1 Phase A: stockholders_equity/
            # cash_and_equivalentsはNone率がほぼ0-4%（全105銘柄実測）で、
            # Noneはほぼ確実にデータ異常のシグナル。従来は`or 0`で暗黙に
            # ゼロ化して成長率計算を続行していたが、DuPont分解
            # （_du_bs関連コード）と同じ「除外」方針に倣いNoneを返す。
            # long_term_debt/short_term_debtは真のゼロとの判別困難のため
            # 対象外（Phase B/C、従来通り`or 0`を維持）。
            _equity_se = bs.get("stockholders_equity")
            _equity_te = bs.get("total_equity")
            _cash_raw = bs.get("cash_and_equivalents")
            if _equity_se is None and _equity_te is None:
                return None
            if _cash_raw is None:
                return None
            # [[OPERATING-INCOME-EXTRACTION-GAP-1]]: `or 0`はNone（未報告）
            # を実測ゼロと区別できなくしていた。parser.py側の再構成
            # バックフィルにより大半の銘柄はoperating_incomeが取得できる
            # ようになったため、ここでNoneになるのは再構成も失敗した
            # 真の欠損のみ。calc_fundamental_growth()自体がnopat<=0を
            # 内部でNone扱いするため、この分岐を通ることによる下流への
            # 影響は無い（安全な変更）。
            _oi = pl.get("operating_income")
            if _oi is None:
                return None
            return calc_fundamental_growth(
                operating_income=_oi,
                tax_rate=0.21,
                total_equity=_equity_se or _equity_te or 0,
                total_debt=(bs.get("long_term_debt") or self._get_lt_debt_fallback(ticker)) + (bs.get("short_term_debt") or 0),
                cash=_cash_raw,
                capex=abs(cf.get("capital_expenditure") or cf.get("capital_expenditures") or 0),
                depreciation=cf.get("depreciation_and_amortization") or cf.get("depreciation_amortization") or 0,
                delta_working_capital=0,
            )
        except Exception:
            return None

    def _inject_ttm_for_general_segment(self, ticker: str) -> None:
        """General 1セグメント銘柄に TTM Revenue Growth を注入し、DCF成長率を実績ベースに変更する"""
        _seg_cfg._ensure_loaded()
        config = _seg_cfg._SEGMENT_CONFIG.get(ticker, {})
        segs = config.get("segments", {})
        if not (len(segs) == 1 and "General" in segs):
            return  # 個別セグメント設定あり → 介入しない

        ttm_g = None
        # 1st: poc.json から rev_yoy を取得
        poc_path = os.path.join(
            self.repo_root, "docs", "value-monitor", "hypecore", "data", f"{ticker}_poc.json"
        )
        if os.path.exists(poc_path):
            try:
                with open(poc_path, encoding="utf-8") as f:
                    monthly = json.load(f).get("monthly") or []
                if monthly:
                    ttm_g = monthly[-1].get("rev_yoy")
            except Exception:
                pass
        # 2nd: stonks-silo から rev_growth を取得
        if ttm_g is None:
            stonks_path = os.path.join(
                self.repo_root, "docs", "value-monitor", "stonks-silo", "data", "results.json"
            )
            if os.path.exists(stonks_path):
                try:
                    with open(stonks_path, encoding="utf-8") as f:
                        s_data = json.load(f).get("tickers", {}).get(ticker, {})
                    rv = s_data.get("deficit_quality", {}).get("rev_growth")
                    if isinstance(rv, (int, float)):
                        ttm_g = rv
                except Exception:
                    pass

        if ttm_g is not None:
            import math as _math
            # TTM > 100% またはinfは早期ステージ銘柄の外れ値 → フォールバック
            if _math.isfinite(ttm_g) and -50 <= ttm_g <= 100:
                _seg_cfg.set_growth_override(ticker, ttm_g / 100)
            else:
                _seg_cfg.clear_growth_override(ticker)
        else:
            _seg_cfg.clear_growth_override(ticker)

    def _calc_roic_wacc_ratio(self, ticker: str, wacc_rm: float = 0.10) -> tuple:
        """最新年次データから ROIC/WACC_Rm を計算（RICE-1 価値創造係数）

        ROIC = NOPAT / Invested_Capital
        NOPAT = Operating_Income × (1 - 21%)（実効税率固定）
        Invested_Capital = Equity + Net_Debt（総資本 - 現金）
        戻り値: (ROIC/wacc_rm または None, 理由コード)
        （例: ROIC=15%・WACC=10% → (1.5, "ok")）

        [[MOAT-SCORE-PARTIAL-NULL-1]]: 理由コードはcalculate_moat_score()が
        Noneの原因別に扱い（真の赤字は算入、それ以外は除外）を判定するために
        使う。RICE-1側（vc_factor）は理由コードを見ず、値がNoneかどうかのみで
        判定するため、本変更による既存挙動への影響はない。

        理由コード一覧:
          "ok"                      - 正常算出
          "reported_negative_oi"    - operating_incomeは取得できたがNOPAT<=0（真の赤字）
          "no_operating_income"     - operating_income自体が取得不能（標準タグ・
                                       parser.py再構成〈[[OPERATING-INCOME-
                                       EXTRACTION-GAP-1]]〉・TTMフォールバック
                                       いずれも失敗。2026-08-16時点で該当0件だが
                                       将来発生しうる真の欠損）
          "missing_equity_data"/"missing_cash_data" - BS項目欠損
          "negative_invested_capital" - 投下資本が非正（自社株買い等で自己資本が
                                       大幅マイナスの銘柄。実際に赤字なのではなく
                                       測定不能なだけ）
          "roic_non_positive"       - ROICが0以下（invested_capital算出後の異常値）
          "roic_diverged_over10"    - ROICが1000%以上（測定アーティファクトの
                                       疑い。2026-08-16時点で該当0件、未検証）
          "no_sec_dir"/"no_annual_data"/"exception" - データ自体が存在しない
        """
        sec_dir = os.path.join(self.repo_root, "common", "sec_data", "data", ticker)
        if not os.path.exists(sec_dir):
            return None, "no_sec_dir"
        years = sorted([
            int(fn[7:11]) for fn in os.listdir(sec_dir)
            if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit()
        ])
        if not years:
            return None, "no_annual_data"
        try:
            with open(os.path.join(sec_dir, f"annual_{years[-1]}.json"), encoding="utf-8") as f:
                ann = json.load(f)
            pl = ann.get("pl", {})
            bs = ann.get("bs", {})
            # [[OPERATING-INCOME-EXTRACTION-GAP-1]]: `or 0`によるNone→0
            # のすり替えは、真の欠損（未報告）と真の赤字（NOPAT<=0）を
            # 区別できなくしていた。`parser.py::_backfill_operating_income()`
            # がGP-R&D-SGA法・pretax調整法で既にannual_YYYY.json側を
            # 補完しているため、ここでNoneになるのは両方式とも失敗した
            # 真の欠損のみ。`_estimate_ttm_operating_income()`は追加の
            # 安全網として維持する（selling_and_marketing欠落50銘柄では
            # 依然失敗しうるが、削除は別作業とする）。
            oi = pl.get("operating_income")
            if oi is None:
                # OperatingIncomeLossタグを近年報告していない銘柄向けフォールバック
                # （XBRL-TAG-KLAC-1: KLACはFY2015 10-K以降、年次OperatingIncomeLossの
                #  タグ付けを行っておらず、pl.operating_incomeが恒常的にNoneになる。
                #  直近4四半期のGrossProfit-RD-SMからTTM営業利益を代替算出する）
                oi = self._estimate_ttm_operating_income(ticker)
            if oi is None:
                return None, "no_operating_income"
            nopat = oi * (1 - 0.21)
            if nopat <= 0:
                # ここに到達する時点でoiはNoneではない（真に報告/再構成された
                # 値）ため、赤字と判定してよい（[[MOAT-SCORE-PARTIAL-NULL-1]]
                # のroic_reason="reported_negative_oi"に対応）。
                return None, "reported_negative_oi"
            # FY52WEEK-BS-NULL-SILENT-1 Phase A: stockholders_equity/
            # cash_and_equivalentsはNone率がほぼ0-4%（全105銘柄実測）で、
            # Noneはほぼ確実にデータ異常のシグナル。従来は`or 0`で暗黙に
            # ゼロ化してinvested_capitalを算出し続けていたが、DuPont分解
            # と同じ「除外」方針に倣いNoneを返す（既存のinvested_capital<=0
            # →None→VC_Factor=1.0フォールバック安全弁はそのまま維持）。
            # long_term_debt/short_term_debtは真のゼロとの判別困難のため
            # 対象外（Phase B/C、従来通り`or 0`を維持）。
            _equity_se = bs.get("stockholders_equity")
            _equity_te = bs.get("total_equity")
            _cash_raw = bs.get("cash_and_equivalents")
            if _equity_se is None and _equity_te is None:
                return None, "missing_equity_data"
            if _cash_raw is None:
                return None, "missing_cash_data"
            equity = _equity_se or _equity_te or 0
            lt_debt = bs.get("long_term_debt") or self._get_lt_debt_fallback(ticker)
            st_debt = bs.get("short_term_debt") or 0
            cash = _cash_raw
            invested_capital = equity + lt_debt + st_debt - cash
            if invested_capital <= 0:
                return None, "negative_invested_capital"
            roic = nopat / invested_capital
            if roic <= 0:
                return None, "roic_non_positive"
            if roic >= 10.0:
                return None, "roic_diverged_over10"
            return roic / wacc_rm, "ok"
        except Exception:
            return None, "exception"

    def _estimate_ttm_operating_income(self, ticker: str) -> float | None:
        """OperatingIncomeLossタグが欠落している銘柄向けTTM営業利益フォールバック

        Layer3 storeの直近4四半期分 GrossProfit - RD - SM を合算する
        （XBRL-TAG-KLAC-1、フェーズD Step2-1でnormalized/からLayer3へ
        切替）。GrossProfit/RD/SMの3フィールドを独立に「直近4件」
        取得すると、いずれかのフィールドのタグ報告が停止・欠落している場合に
        期末日が食い違い、存在しないデータをdict.get(end, 0)で暗黙的に0円
        扱いしてしまう（LLY: RDが2022-2023年で停止したままGP/SMは2025-2026年
        分が取れてしまいR&D控除漏れでTTM営業利益を過大算出、ASTS: GPが2023年
        で停止しRD/SMは2025-2026年分のためGPが実質0扱いになる、という2種類の
        不具合が確認された）。3フィールド共通の期末日（intersection）でのみ
        合算し、共通期末日が4件未満の場合はフォールバック不可としてNoneを返す。

        フィルタは既存動作を維持（is_annualのみ除外、is_ytdは除外しない。
        is_ytdフィルタの扱いは本切替のスコープ外、別途判断）。
        """
        store = self._get_layer3_store(ticker)
        if store is None:
            return None
        try:
            def _by_end(field_name: str) -> dict:
                q = (x for x in get_field_entries(store, field_name) if not x.get("is_annual"))
                return {x["end"]: x["val"] for x in q}

            gp = _by_end("gross_profit")
            rd = _by_end("research_and_development")
            sm = _by_end("selling_and_marketing")
            common_ends = sorted(set(gp) & set(rd) & set(sm))[-4:]
            if len(common_ends) < 4:
                return None
            return sum(gp[end] - rd[end] - sm[end] for end in common_ends)
        except Exception:
            return None

    def _calc_moat_inputs(self, ticker: str, roic: float | None = None, roic_reason: str = "ok") -> dict:
        """Moat Score計算用インプット（gross_margin_3yr_avg, roic, fcf_margin_3yr_avg）を返す

        gross_margin_3yr_avg: Layer3 store の GrossProfit / Revenue 3年平均（フェーズD Step2-1）
        roic:                 呼び出し元から渡された ROIC値（ROIC = roic_wacc_ratio × Rm）
        roic_reason:          [[MOAT-SCORE-PARTIAL-NULL-1]] roicがNoneの理由コード
                               （`_calc_roic_wacc_ratio()`参照）。calculate_moat_score()が
                               「真の赤字は算入・それ以外は除外」を判定するために使う
        fcf_margin_3yr_avg:   annual SEC の free_cash_flow / revenue 3年平均
        """
        result: dict = {}

        if roic is not None:
            result["moat_roic"] = roic
        result["moat_roic_reason"] = roic_reason

        # gross_margin_3yr_avg: Layer3 storeから年次GrossProfit/Revenue
        # （フェーズD Step2-1、normalized/からLayer3へ切替）
        _layer3_store_moat = self._get_layer3_store(ticker)
        if _layer3_store_moat is not None:
            try:
                gp_annual  = [x for x in get_field_entries(_layer3_store_moat, "gross_profit") if x.get("is_annual")]
                rev_annual = [x for x in get_field_entries(_layer3_store_moat, "revenue")      if x.get("is_annual")]
                # end日付でマッチング（位置zipは年ズレを起こす。XBRL-TAG-KLAC-1で発見）
                gp_by_end = {x["end"]: x["val"] for x in gp_annual}
                pairs = [
                    gp_by_end[rev["end"]] / rev["val"]
                    for rev in rev_annual[-3:]
                    if rev.get("val") and rev["val"] > 0 and rev["end"] in gp_by_end
                ]
                if pairs:
                    result["moat_gross_margin_3yr"] = sum(pairs) / len(pairs)
                else:
                    # 年次GrossProfitタグが存在しない、または直近年とマッチしない
                    # （staleで古い年度のみ残っている）銘柄向けフォールバック
                    # （XBRL-TAG-KLAC-1: KLACはFY2022 10-K以降GrossProfitタグ自体を
                    #  廃止しており、四半期はRevenue-COGS逆算で補完済みだが年次は
                    #  未補完のまま。ASTS向け追加修正: gp_annualが存在していても
                    #  直近年とend日が一致せずpairsが0件になるケース（GrossProfit
                    #  タグの報告が途中で停止した銘柄）も同様にフォールバックする。
                    #  直近12四半期（≒3年）を合算した粗利率で代替する）
                    gp_q  = sorted(
                        (x for x in get_field_entries(_layer3_store_moat, "gross_profit") if not x.get("is_annual")),
                        key=lambda x: x["end"],
                    )[-12:]
                    rev_by_end_q = {
                        x["end"]: x["val"]
                        for x in get_field_entries(_layer3_store_moat, "revenue")
                        if not x.get("is_annual")
                    }
                    gp_sum = sum(x["val"] for x in gp_q if x["end"] in rev_by_end_q)
                    rev_sum = sum(rev_by_end_q[x["end"]] for x in gp_q if x["end"] in rev_by_end_q)
                    if gp_q and rev_sum > 0:
                        result["moat_gross_margin_3yr"] = gp_sum / rev_sum
            except Exception:
                pass

        # fcf_margin_3yr_avg: annual SEC data の free_cash_flow / revenue
        sec_dir = os.path.join(self.repo_root, "common", "sec_data", "data", ticker)
        if os.path.exists(sec_dir):
            try:
                years = sorted([
                    int(fn[7:11]) for fn in os.listdir(sec_dir)
                    if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit()
                ])
                fcf_margins = []
                for y in years[-3:]:
                    with open(os.path.join(sec_dir, f"annual_{y}.json"), encoding="utf-8") as f:
                        ann = json.load(f)
                    rev = ann.get("pl", {}).get("revenue") or 0
                    fcf = ann.get("cf", {}).get("free_cash_flow")
                    if fcf and rev and rev > 0:
                        fcf_margins.append(fcf / rev)
                if fcf_margins:
                    result["moat_fcf_margin_3yr"] = sum(fcf_margins) / len(fcf_margins)
            except Exception:
                pass

        return result

    def _load_hype_phase(self, ticker: str) -> int | None:
        """poc.json から最新の HypeCore フェーズ（1〜4）を返す（GROWTH-1）"""
        return self._load_hype_info(ticker).get("phase")

    def _load_hype_info(self, ticker: str) -> dict:
        """poc.json から最新フェーズ・stage_label・substage_label を返す"""
        poc_path = os.path.join(
            self.repo_root, "docs", "value-monitor", "hypecore", "data", f"{ticker}_poc.json"
        )
        if not os.path.exists(poc_path):
            return {}
        try:
            with open(poc_path, encoding="utf-8") as f:
                monthly = json.load(f).get("monthly") or []
            if monthly:
                last = monthly[-1]
                stage = last.get("stage")
                return {
                    "phase":          stage if isinstance(stage, int) and 1 <= stage <= 4 else None,
                    "stage_label":    last.get("stage_label"),
                    "substage_label": last.get("substage_label"),
                }
        except Exception:
            pass
        return {}

    @staticmethod
    def _hypecore_recommendation(stage, prev_stage, ma200_dev, from_peak) -> str | None:
        """hypecore.html の getRec() ロジックをPython移植（推奨タイトルのみ）"""
        if stage is None:
            return None
        ma = ma200_dev or 0
        fp = from_peak or 0
        ps = prev_stage if prev_stage is not None else stage
        if stage == 0 and ps == 4:
            return "強い買い推奨"
        if stage == 1:
            return "買い推奨"
        if stage == 2:
            return "保有継続"
        if stage == 3 and ma < 50:
            return "保有・要注意"
        if stage == 3 and ma >= 50:
            return "売り準備"
        if stage == 4 and ps == 3:
            return "売り推奨"
        if stage == 4:
            return "売り推奨"
        if stage == 0:
            return "様子見"
        return "保有継続"

    def _save_hypecore_history(self, ticker: str, date_only: str) -> None:
        """DESIGN: HYPECOREフェーズ履歴を hypecore_history/{TICKER}.json に日付単位で追記する。
        データソースは poc.json の monthly 最新エントリ（hype_results相当）。"""
        poc_path = os.path.join(
            self.repo_root, "docs", "value-monitor", "hypecore", "data", f"{ticker}_poc.json"
        )
        if not os.path.exists(poc_path):
            return
        try:
            with open(poc_path, encoding="utf-8") as f:
                monthly = json.load(f).get("monthly") or []
            if not monthly:
                return
            last = monthly[-1]
            prev = monthly[-2] if len(monthly) >= 2 else None
            stage = last.get("stage")
            entry = {
                "date": date_only,
                "lifecycle": last.get("stage_label"),
                "stage": stage,
                "phase": last.get("substage_label"),
                "recommendation": self._hypecore_recommendation(
                    stage, prev.get("stage") if prev else None,
                    last.get("ma200_dev_local"), last.get("from_peak"),
                ),
                "price": last.get("price"),
                "price_iv_ratio": last.get("price_iv_ratio"),
                "rev_yoy": last.get("rev_yoy"),
                # [[RULE40-DEFINITION-MISMATCH-1]]/[[HYPECORE-MISC-NAMING-GAPS-1]]③
                # : 読み取り元（poc.json）のキーはrule40→rule40_yoy_netmargin
                # （2026-08-13）・ma200_dev→ma200_dev_local（2026-08-29）へ
                # それぞれ改名されたため読み取り側は追従するが、
                # hypecore_history/{TICKER}.json自体の出力キーは"rule40"・
                # "ma200_dev"のまま維持する。理由: 本ファイルは日付単位で
                # 追記されていく累積履歴であり、過去に書き込み済みの全エントリを
                # 遡って改名しない限りキー名が新旧で混在してしまう。かつ
                # stock.html:632（`e.rule40`）・559（`e.ma200_dev`）が本
                # ファイルを直接参照しており、出力キー改名にはフロントエンド
                # 追従修正も必要になる。実害がない（TANUKI VALUATION内部の
                # 表示専用フィールド、ARCH-DATA-1系の計算ロジックには
                # 使われない）ため、出力キーは変更しない選択とした。
                "rule40": last.get("rule40_yoy_netmargin"),
                "ma200_dev": last.get("ma200_dev_local"),
                "from_peak": last.get("from_peak"),
            }
        except Exception:
            return

        hist_dir = os.path.join(self.output_dir, "hypecore_history")
        os.makedirs(hist_dir, exist_ok=True)
        hist_path = os.path.join(hist_dir, f"{ticker}.json")
        history: list = []
        if os.path.exists(hist_path):
            try:
                with open(hist_path, encoding="utf-8") as f:
                    history = json.load(f)
            except Exception:
                history = []
        history = [e for e in history if e.get("date") != date_only]
        history.append(entry)
        history.sort(key=lambda e: e.get("date", ""))
        with open(hist_path, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

    def _load_beta_sector(self, ticker: str) -> str | None:
        """beta_config.json の overrides[ticker].sector を返す（SECTOR_TO_DAMODARAN キー形式）"""
        beta_path = os.path.join(self.repo_root, "config", "beta_config.json")
        try:
            with open(beta_path, encoding="utf-8") as f:
                beta_cfg = json.load(f)
            return beta_cfg.get("overrides", {}).get(ticker, {}).get("sector")
        except Exception:
            return None

    def _save_report(self, ticker: str, report_text: str) -> None:
        ticker_dir = os.path.join(self.output_dir, ticker)
        report_path = os.path.join(ticker_dir, "report.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_text)
        print(f"   📄 レポート: {report_path}")

    def _save_tickers_index(self, success_tickers: List[str]) -> None:
        """tickers.json（TANUKI SCORE daily_pick.py等が読む対象銘柄一覧）を更新する。

        ZS-TICKERS-LEAK-1: 既存index_dataとのマージのみ（和集合）で、
        一度でも書き込まれた銘柄はtanuki=falseへ変更されても除去されない
        構造だった（latest.json存在確認のみでフラグを見ていなかったため）。
        merged集合をtanuki=true（tickers.py経由）でも絞り込み、
        バッチ実行のたびに現在のフラグ状態へ自己修復するようにする。
        """
        index_path = os.path.join(self.output_dir, "tickers.json")

        existing_tickers = []
        if os.path.exists(index_path):
            try:
                with open(index_path, encoding="utf-8") as f:
                    existing_tickers = json.load(f).get("tickers", [])
            except Exception:
                pass

        if self.repo_root not in sys.path:
            sys.path.insert(0, self.repo_root)
        from common.sec_data import tickers as _tickers
        tanuki_set = set(_tickers.get_tanuki_tickers())

        merged = sorted(set(existing_tickers) | set(success_tickers))
        valid = [
            t for t in merged
            if t in tanuki_set
            and os.path.exists(os.path.join(self.output_dir, t, "latest.json"))
        ]

        index_data = {
            "tickers": valid,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(valid)
        }

        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)

        print(f"   📋 tickers.json 更新: {valid}")

    def run_single(self, ticker: str) -> dict:
        return self.run([ticker]).get(ticker, {})


def main():
    import argparse
    parser = argparse.ArgumentParser(description="TANUKI VALUATION Pipeline")
    parser.add_argument("tickers", nargs="*", default=None, help="処理する銘柄コード（省略時は全銘柄）")
    args = parser.parse_args()

    pipeline = TanukiValuationPipeline(use_ai_validation=False)
    results = pipeline.run(args.tickers if args.tickers else None)
    sys.exit(0 if results else 1)


if __name__ == "__main__":
    main()
