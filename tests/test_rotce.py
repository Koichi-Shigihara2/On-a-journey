"""
tests/test_rotce.py

[[ROTCE-PTBV-1]]（2026-10-10）: common/sec_data/rotce.py（ROTCE・P/TBV、参考表示専用）。

- TCE≤0ならROTCE・P/TBVはNone（理由コードtce_nonpositive）
- 非支配持分は、純資産が非支配持分込みのタグの期末日だけ引く
- 期末日に成分が無いとき年度末の値で補い、filled_from_annualを付ける
- 比較用の四半期が8期未満ならパーセンタイル・目安を出さない
- 目安のしきい値（P/TBV 25以下かつROTCE 50以上→自社比割安 等）
- 株式数の外れ値（千株単位の申告ミス）は使わず、同じ期末日の次の候補を使う
- 優先株がメザニンと同額の期末日は控除しない

実行方法:
    python -m pytest tests/test_rotce.py -v
"""

from common.sec_data import rotce

SE_TAG = rotce.SE_TAG
SE_NCI_TAG = rotce.SE_NCI_TAG
W_TAG = rotce.WEIGHTED_DILUTED_TAG

# 四半期末（暦年決算）。2023Q1〜2025Q4の12四半期
QEND = [f"{y}-{md}" for y in (2023, 2024, 2025) for md in ("03-31", "06-30", "09-30", "12-31")]


def _stock(end, val, tag, form="10-Q"):
    return {"end": end, "val": val, "source_tag": tag, "form": form, "filed": end, "period_days": 0,
            "is_annual": False}


def _store(se=None, gw=None, intang=None, pref=None, nci=None, ni=None, shares=None, se_tag=SE_TAG,
           annual_ends=("2023-12-31", "2024-12-31", "2025-12-31")):
    """Layer3（build_ticker_storeの戻り値）の最小構成"""
    ni = ni if ni is not None else {q: 25 for q in QEND}
    shares = shares if shares is not None else {q: 10 for q in QEND}
    se = se if se is not None else {q: 1_000 for q in QEND}
    f = {
        "stockholders_equity": [_stock(q, v, se_tag if isinstance(se_tag, str) else se_tag.get(q, SE_TAG))
                                for q, v in se.items()],
        "goodwill": [_stock(q, v, "Goodwill") for q, v in (gw or {}).items()],
        "intangible_assets_excl_goodwill": [
            _stock(q, v, "IntangibleAssetsNetExcludingGoodwillResolvedDerived", form)
            for q, (v, form) in ((q, x if isinstance(x, tuple) else (x, "10-Q")) for q, x in (intang or {}).items())],
        "preferred_stock": [_stock(q, v, "PreferredStockValue") for q, v in (pref or {}).items()],
        "minority_interest": [_stock(q, v, "MinorityInterest") for q, v in (nci or {}).items()],
        "net_income": [{"end": q, "val": v, "is_annual": False, "is_ytd": False} for q, v in ni.items()]
                      + [{"end": e, "val": 100, "is_annual": True} for e in annual_ends],
        "shares_diluted": [{"end": q, "val": v, "is_annual": False, "source_tag": W_TAG, "filed": q}
                           for q, v in shares.items()],
    }
    return {"fields": {k: {"entries": v} for k, v in f.items()}}


def _prices(px=20.0):
    return [{"date": q, "close": px} for q in QEND]


def _run(store, prices=None, company_facts=None):
    return rotce.compute_ticker("TEST", store=store, prices=prices if prices is not None else _prices(),
                                splits=[], company_facts=company_facts or {"facts": {"us-gaap": {}}})


class TestTce:
    def test_tce_nonpositive_gives_none(self):
        r = _run(_store(gw={q: 1_200 for q in QEND}))
        last = r["history"][-1]
        assert last["tce"] == -200
        assert last.get("rotce") is None and last.get("ptbv") is None
        assert last["reason"] == rotce.R_TCE_NONPOSITIVE
        assert r["current"]["reason"] == rotce.R_TCE_NONPOSITIVE
        assert r["current"].get("ptbv") is None
        assert r["percentile"]["signal"] is None

    def test_rotce_and_ptbv_values(self):
        # TCE=1,000−100−50−0=850、TTM純利益=100、P/TBV=20×10÷850
        r = _run(_store(gw={q: 100 for q in QEND}, intang={q: 50 for q in QEND}))
        last = r["history"][-1]
        assert last["tce"] == 850
        assert abs(last["rotce"] - 100 / 850) < 1e-12
        assert last["rotce_basis"] == "average"
        assert abs(last["ptbv"] - 200 / 850) < 1e-12

    def test_minority_interest_subtracted_only_with_nci_inclusive_equity_tag(self):
        nci = {q: 100 for q in QEND}
        parent = _run(_store(nci=nci, se_tag=SE_TAG))["history"][-1]
        assert parent["tce"] == 1_000  # StockholdersEquity（親会社持分）→ 引かない
        incl = _run(_store(nci=nci, se_tag=SE_NCI_TAG))["history"][-1]
        assert incl["tce"] == 900      # 非支配持分込みのタグ → 引く
        assert incl["components"]["minority_interest"] == 100

    def test_preferred_stock_subtracted(self):
        last = _run(_store(pref={q: 300 for q in QEND}))["history"][-1]
        assert last["tce"] == 700

    def test_preferred_in_temporary_equity_not_subtracted(self):
        cf = {"facts": {"us-gaap": {
            "PreferredStockValue": {"units": {"USD": [{"end": q, "val": 300, "form": "10-Q"} for q in QEND]}},
            "TemporaryEquityCarryingAmountAttributableToParent": {
                "units": {"USD": [{"end": q, "val": 300, "form": "10-Q"} for q in QEND]}},
        }}}
        r = _run(_store(pref={q: 300 for q in QEND}), company_facts=cf)
        assert r["history"][-1]["tce"] == 1_000
        assert rotce.R_PREFERRED_TEMPORARY_EQUITY in r["notes"]


class TestAnnualFill:
    def test_filled_from_annual_value_is_flagged(self):
        # 無形資産は年度末（12-31）にだけある → 翌年の四半期は年度末の値で補い、注記を付ける
        intang = {e: 50 for e in ("2022-12-31", "2023-12-31", "2024-12-31", "2025-12-31")}
        r = _run(_store(intang=intang, annual_ends=("2022-12-31", "2023-12-31", "2024-12-31", "2025-12-31")))
        row = next(h for h in r["history"] if h["end"] == "2025-09-30")
        assert row["tce"] == 950
        assert row["filled_from_annual"] is True
        assert row["filled_components"] == {"intangible_assets_excl_goodwill": "2024-12-31"}
        year_end = next(h for h in r["history"] if h["end"] == "2025-12-31")
        assert year_end["filled_from_annual"] is False

    def test_fill_with_zero_is_not_flagged(self):
        # 年度末の値が0（優先株0等）で補ってもTCEは変わらないので注記しない
        pref = {e: 0 for e in ("2022-12-31", "2023-12-31", "2024-12-31", "2025-12-31")}
        r = _run(_store(pref=pref, annual_ends=("2022-12-31", "2023-12-31", "2024-12-31", "2025-12-31")))
        row = next(h for h in r["history"] if h["end"] == "2025-09-30")
        assert row["tce"] == 1_000
        assert row["filled_from_annual"] is False

    def test_stale_component_is_missing(self):
        # 12か月より古い年度末の値しかない → 推測で埋めずcomponent_missing
        intang = {"2023-12-31": 50}
        r = _run(_store(intang=intang, annual_ends=("2023-12-31",)))
        row = next(h for h in r["history"] if h["end"] == "2025-06-30")
        assert row["tce"] is None
        assert row["reason"] == rotce.R_COMPONENT_MISSING
        assert row["missing_components"] == ["intangible_assets_excl_goodwill"]

    def test_never_reported_component_is_zero(self):
        r = _run(_store())
        assert r["history"][-1]["tce"] == 1_000
        assert r["history"][-1]["filled_from_annual"] is False


class TestPercentile:
    def test_fewer_than_8_quarters_has_no_percentile(self):
        qs = QEND[-10:]  # TTMの窓は7つ（10−3）
        r = _run(_store(se={q: 1_000 for q in qs}, ni={q: 25 for q in qs}, shares={q: 10 for q in qs}))
        assert r["percentile"]["n_quarters"] == 7
        assert r["percentile"]["signal"] is None
        assert r["percentile"]["reason"] == rotce.R_INSUFFICIENT_QUARTERS
        assert "ptbv" not in r["percentile"]

    def test_8_quarters_gives_percentile(self):
        qs = QEND[-11:]
        r = _run(_store(se={q: 1_000 for q in qs}, ni={q: 25 for q in qs}, shares={q: 10 for q in qs}))
        assert r["percentile"]["n_quarters"] == 8
        assert r["percentile"]["signal"] in (rotce.SIGNAL_CHEAP, rotce.SIGNAL_NEUTRAL, rotce.SIGNAL_RICH)

    def test_cheap_when_price_low_and_rotce_high(self):
        # 株価が過去最安・純利益が過去最高 → P/TBVは下位、ROTCEは上位 → 自社比割安
        ni = {q: 10 + i for i, q in enumerate(QEND)}
        prices = [{"date": q, "close": 50.0 - i} for i, q in enumerate(QEND)]
        r = _run(_store(ni=ni), prices=prices)
        p = r["percentile"]
        assert p["ptbv"] <= rotce.CHEAP_PTBV_MAX_PCTL and p["rotce"] >= rotce.CHEAP_ROTCE_MIN_PCTL
        assert p["signal"] == rotce.SIGNAL_CHEAP

    def test_rich_when_price_high_and_rotce_low(self):
        ni = {q: 40 - i for i, q in enumerate(QEND)}
        prices = [{"date": q, "close": 10.0 + i} for i, q in enumerate(QEND)]
        r = _run(_store(ni=ni), prices=prices)
        assert r["percentile"]["signal"] == rotce.SIGNAL_RICH

    def test_signal_thresholds(self):
        assert rotce.signal_for(25, 50) == rotce.SIGNAL_CHEAP
        assert rotce.signal_for(26, 50) == rotce.SIGNAL_NEUTRAL
        assert rotce.signal_for(75, 50) == rotce.SIGNAL_RICH
        assert rotce.signal_for(75, 51) == rotce.SIGNAL_NEUTRAL

    def test_percentile_of(self):
        assert rotce.percentile_of(5, [1, 2, 3, 4]) == 100.0
        assert rotce.percentile_of(0, [1, 2, 3, 4]) == 0.0
        assert rotce.percentile_of(2, [1, 2, 3, 4]) == 37.5  # (1 + 0.5) / 4


class TestShares:
    def test_outlier_shares_replaced_by_next_candidate(self):
        # 年度末の加重平均が千株単位（0.01）で申告されたとき、発行済株式数（10）を使う
        st = _store()
        st["fields"]["shares_diluted"]["entries"] = [
            e for e in st["fields"]["shares_diluted"]["entries"] if e["end"] != "2025-12-31"] + [
            {"end": "2025-12-31", "val": 0.01, "is_annual": True, "source_tag": W_TAG, "filed": "2026-02-01"},
            {"end": "2025-12-31", "val": 10, "is_annual": False, "source_tag": "CommonStockSharesOutstanding",
             "filed": "2026-02-01"},
        ]
        last = _run(st)["history"][-1]
        assert last["shares"] == 10
        assert last["shares_source"] == "period_end_outstanding"

    def test_current_uses_latest_close(self):
        prices = _prices(20.0) + [{"date": "2026-10-09", "close": 30.0}]
        c = _run(_store(), prices=prices)["current"]
        assert c["price"] == 30.0 and c["price_date"] == "2026-10-09"
        assert abs(c["ptbv"] - 30.0 * 10 / 1_000) < 1e-12
        assert c["quarter_end"] == "2025-12-31"


class TestAssumedZero:
    def test_assumed_zero_quarters_excluded_from_percentile(self):
        # のれんを2024-12-31に初めて申告 → それより前の期は0と仮定（assumed_zero）し、自社比の母数に入れない
        gw = {q: 100 for q in QEND if q >= "2024-12-31"}
        cf = {"facts": {"us-gaap": {"Goodwill": {"units": {"USD": [
            {"end": q, "val": 100, "form": "10-Q"} for q in gw]}}}}}
        r = _run(_store(gw=gw), company_facts=cf)
        assumed = [h["end"] for h in r["history"] if h.get("assumed_zero")]
        assert assumed == ["2023-12-31", "2024-03-31", "2024-06-30", "2024-09-30"]
        assert all(h["assumed_zero"] == ["goodwill"] for h in r["history"] if h.get("assumed_zero"))
        p = r["percentile"]
        assert p["n_quarters"] == 5            # 9期のうち0と仮定した4期を除く
        assert p["n_excluded_assumed_zero"] == 4
        assert p["signal"] is None and p["reason"] == rotce.R_INSUFFICIENT_QUARTERS

    def test_reported_quarters_have_no_assumed_zero(self):
        r = _run(_store(gw={q: 100 for q in QEND}),
                 company_facts={"facts": {"us-gaap": {"Goodwill": {"units": {"USD": [
                     {"end": q, "val": 100, "form": "10-Q"} for q in QEND]}}}}})
        assert not any(h.get("assumed_zero") for h in r["history"])
        assert r["percentile"]["n_quarters"] == 9


class TestLossMakers:
    def test_nonpositive_rotce_gets_no_signal(self):
        # 赤字（TTM純利益<0）→ ROTCE≤0 → 目安なし（rotce_nonpositive）。ROTCE・P/TBV自体は出す
        ni = {q: -10 - i for i, q in enumerate(QEND)}
        prices = [{"date": q, "close": 50.0 - i} for i, q in enumerate(QEND)]
        r = _run(_store(ni=ni), prices=prices)
        assert r["current"]["rotce"] < 0 and r["current"]["ptbv"] is not None
        assert r["percentile"]["signal"] is None
        assert r["percentile"]["reason"] == rotce.R_ROTCE_NONPOSITIVE
        assert "ptbv" not in r["percentile"]

    def test_zero_rotce_gets_no_signal(self):
        r = _run(_store(ni={q: 0 for q in QEND}))
        assert r["percentile"]["signal"] is None
        assert r["percentile"]["reason"] == rotce.R_ROTCE_NONPOSITIVE
