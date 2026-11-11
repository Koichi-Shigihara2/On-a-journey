"""Layer3の定義を変えたときに、手元のcompany_factsから派生ファイル（normalized・ttm）だけを作り直す（SECは呼ばない）。

計算方法の変更の手順の2（公開の時点を1つに）。日曜のSEC_Data_Updateの 3.〜5. と同じ呼び出し:
  build_raw_table → normalize → save_normalized、build_ticker_store → calc_ttm_series → mark_basis_mix → save_ttm_series
使い方: python tools/sim/regen_derived.py [TICKER ...]（省略時は company_facts.json のある全銘柄）
出力: common/sec_data/normalized/{T}_quarterly_normalized.json・ttm/{T}_ttm_series.json（変わったファイルの数を表示）
"""
import contextlib
import glob
import io
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, REPO)
os.chdir(REPO)

from common.sec_data.basis_mix import from_store as basis_mix_from_store  # noqa: E402
from common.sec_data.fetcher import load_company_facts  # noqa: E402
from common.sec_data.layer3_builder import build_ticker_store  # noqa: E402
from common.sec_data.normalizer import normalize, save_normalized  # noqa: E402
from common.sec_data.quarterly import build_raw_table  # noqa: E402
from common.sec_data.ttm_calculator import calc_ttm_series, mark_basis_mix, save_ttm_series  # noqa: E402


def regen(ticker: str) -> bool:
    cf = load_company_facts(ticker)
    if cf is None:
        return False
    with contextlib.redirect_stdout(io.StringIO()):
        normalized = normalize(ticker, build_raw_table(ticker, cf))
        save_normalized(ticker, normalized)
        store = build_ticker_store(ticker)
        if store is None:
            return False
        series = calc_ttm_series(ticker, store)
        bm = basis_mix_from_store(store)
        mark_basis_mix(series, bm)
        save_ttm_series(ticker, series, basis_mix=bm)
        if bm:
            normalized["basis_mix"] = bm
            save_normalized(ticker, normalized)
    return True


if __name__ == "__main__":
    tickers = sys.argv[1:] or sorted(os.path.basename(os.path.dirname(p)) for p in
                                     glob.glob(os.path.join(REPO, "common", "sec_data", "data", "*", "company_facts.json")))
    ok = [t for t in tickers if regen(t)]
    print(f"regen: {len(ok)}/{len(tickers)} tickers")
