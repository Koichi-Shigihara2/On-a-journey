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
- [[ROTCE-QUARTERLY-ANNUALIZED-1]]（2026-10-11）: ROTCEは四半期の普通株主帰属純利益×4÷（前の四半期末と当四半期末の
  TCEの平均）。タグが無い四半期は純利益。比較用（TTM、rotce_ttm）は4四半期の合計÷5つの四半期末のTCEの平均で、
  自社比の目安はこの系列で計算する

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
        # TCE=1,000−100−50−0=850、四半期の純利益25×4=100、P/TBV=20×10÷850（TTMも100÷850）
        r = _run(_store(gw={q: 100 for q in QEND}, intang={q: 50 for q in QEND}))
        last = r["history"][-1]
        assert last["tce"] == 850
        assert abs(last["rotce"] - 100 / 850) < 1e-12
        assert last["rotce_basis"] == "quarter_average"
        assert last["prev_quarter_end"] == "2025-09-30"
        assert abs(last["rotce_ttm"] - 100 / 850) < 1e-12
        assert last["rotce_ttm_basis"] == "avg5"
        assert last["rotce_ttm_tce_ends"] == QEND[-5:]
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
        qs = QEND[-10:]  # 比較用（TTM）の窓は7つ（10−3）
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
    def test_goodwill_assumed_zero_stays_in_percentile(self):
        # のれんを2024-12-31に初めて申告 → それより前の期は0と仮定（assumed_zero）するが、
        # のれんは存在すれば貸借対照表に出るため母数には入れる
        gw = {q: 100 for q in QEND if q >= "2024-12-31"}
        cf = {"facts": {"us-gaap": {"Goodwill": {"units": {"USD": [
            {"end": q, "val": 100, "form": "10-Q"} for q in gw]}}}}}
        r = _run(_store(gw=gw), company_facts=cf)
        assumed = [h["end"] for h in r["history"] if h.get("assumed_zero")]
        assert assumed == [q for q in QEND if q < "2024-12-31"]
        assert all(h["assumed_zero"] == ["goodwill"] for h in r["history"] if h.get("assumed_zero"))
        assert not any(h.get("excluded_from_percentile") for h in r["history"])
        p = r["percentile"]
        assert p["n_quarters"] == 9             # 比較用（TTM）の窓がそろう9期
        assert p["n_excluded_assumed_zero"] == 0
        assert p["signal"] is not None

    def test_intangible_assumed_zero_excluded_from_percentile(self):
        # 無形資産を2024-12-31に初めて申告 → それより前の期は0と仮定し、母数から外す（注記だけの開示で漏れうる）
        it = {q: 50 for q in QEND if q >= "2024-12-31"}
        cf = {"facts": {"us-gaap": {"IntangibleAssetsNetExcludingGoodwill": {"units": {"USD": [
            {"end": q, "val": 50, "form": "10-Q", "accn": "a" + q} for q in it]}}}}}
        r = _run(_store(intang=it), company_facts=cf)
        excluded = [h["end"] for h in r["history"] if h.get("excluded_from_percentile")]
        assert excluded == [q for q in QEND if q < "2024-12-31"]
        assert all(h["assumed_zero"] == ["intangible_assets_excl_goodwill"]
                   for h in r["history"] if h.get("assumed_zero"))
        p = r["percentile"]
        assert p["n_quarters"] == 5            # 比較用（TTM）の9期のうち無形資産を0と仮定した4期を除く
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


def _cf_common_ni(vals, tag=rotce.COMMON_NI_DILUTED_TAG):
    """company_factsの普通株主帰属純利益（単独四半期、10-Q）"""
    from datetime import date, timedelta
    facts = []
    for q, v in vals.items():
        start = (date.fromisoformat(q) - timedelta(days=89)).isoformat()
        facts.append({"start": start, "end": q, "val": v, "form": "10-Q", "fp": "Q1", "fy": int(q[:4]),
                      "filed": q, "accn": "c" + q, "frame": None})
    return {"facts": {"us-gaap": {tag: {"units": {"USD": facts}}}}}


class TestQuarterlyAnnualized:
    def test_uses_common_ni_and_previous_quarter_average(self):
        # 普通株主帰属純利益（優先配当を引いた後）20 × 4 ÷（前の四半期末900と当四半期末1,100の平均1,000）
        se = {q: 1_000 for q in QEND}
        se["2025-09-30"], se["2025-12-31"] = 900, 1_100
        r = _run(_store(se=se), company_facts=_cf_common_ni({"2025-12-31": 20}))
        last = r["history"][-1]
        assert last["net_income_source"] == "common_diluted"
        assert last["quarter_net_income"] == 20
        assert last["prev_quarter_end"] == "2025-09-30" and last["tce_prev_quarter"] == 900
        assert abs(last["rotce"] - 80 / 1_000) < 1e-12
        assert last["rotce_basis"] == "quarter_average"
        # 比較用（TTM）は分子を四半期ごとに選ぶ（20＋25×3=95）÷ 5つの四半期末の平均（1,000×3＋900＋1,100）÷5=1,000
        assert last["ttm_net_income"] == 95
        assert abs(last["rotce_ttm"] - 95 / 1_000) < 1e-12
        assert r["current"]["rotce"] == last["rotce"] and r["current"]["net_income_source"] == "common_diluted"

    def test_falls_back_to_net_income_without_common_tag(self):
        r = _run(_store())
        assert all(h["net_income_source"] == rotce.NI_SOURCE_FALLBACK for h in r["history"])
        assert r["current"]["net_income_source"] == rotce.NI_SOURCE_FALLBACK

    def test_basic_tag_used_when_no_diluted(self):
        r = _run(_store(), company_facts=_cf_common_ni({"2025-12-31": 20}, tag=rotce.COMMON_NI_BASIC_TAG))
        assert r["history"][-1]["net_income_source"] == "common_basic"

    def test_end_only_when_previous_quarter_tce_nonpositive(self):
        gw = {"2025-09-30": 1_200}  # 前の四半期末のTCE=−200
        r = _run(_store(gw={**{q: 0 for q in QEND}, **gw}))
        last = r["history"][-1]
        assert last["rotce_basis"] == "quarter_end_only"
        assert abs(last["rotce"] - 100 / 1_000) < 1e-12

    def test_first_quarter_has_rotce_but_no_ttm(self):
        r = _run(_store())
        first = r["history"][0]
        assert first["rotce"] is not None and first["rotce_basis"] == "quarter_end_only"
        assert first["ttm_net_income"] is None and first.get("rotce_ttm") is None


class TestCompanyReported:
    ENTRY = {"ticker": "TEST", "metric": "ROTCE", "source": "IR p.1",
             "guidance": {"period": "FY2026", "value": 0.08}, "long_term_target": {"low": 0.2, "high": 0.3},
             "values": [{"quarter_end": "2025-09-30", "value": 0.09}, {"quarter_end": "2025-12-31", "value": 0.12}]}

    def test_block_has_diff_and_latest_registered(self):
        r = _run(_store())
        b = rotce.company_reported_block(self.ENTRY, r["history"], r["current"])
        last = b["values"][-1]
        assert last["quarter_end"] == "2025-12-31" and last["computed"] == r["history"][-1]["rotce"]
        assert abs(last["diff"] - (r["history"][-1]["rotce"] - 0.12)) < 1e-12
        assert b["latest_registered"] is True
        assert b["guidance"]["value"] == 0.08 and b["long_term_target"]["high"] == 0.3

    def test_unregistered_when_latest_quarter_missing(self):
        entry = {**self.ENTRY, "values": self.ENTRY["values"][:1]}
        r = _run(_store())
        b = rotce.company_reported_block(entry, r["history"], r["current"])
        assert b["latest_registered"] is False and b["last_registered_quarter_end"] == "2025-09-30"

    def test_unregistered_latest_quarters(self, tmp_path):
        import json
        (tmp_path / "TEST.json").write_text(json.dumps({"current": {"quarter_end": "2026-03-31"}}), encoding="utf-8")
        rows = rotce.unregistered_latest_quarters({"TEST": self.ENTRY, "NONE": self.ENTRY}, out_dir=str(tmp_path))
        assert {"ticker": "NONE", "reason": "no_output"} in rows
        t = next(x for x in rows if x["ticker"] == "TEST")
        assert t["latest_quarter_end"] == "2026-03-31" and t["last_registered_quarter_end"] == "2025-12-31"
        (tmp_path / "TEST.json").write_text(json.dumps({"current": {"quarter_end": "2025-12-31"}}), encoding="utf-8")
        assert not [x for x in rotce.unregistered_latest_quarters({"TEST": self.ENTRY}, out_dir=str(tmp_path))]

    def test_config_file_is_valid(self):
        reported = rotce.load_company_reported()
        assert "SOFI" in reported
        for t, m in reported.items():
            assert m.get("source")
            for v in m["values"]:
                assert 0 < abs(v["value"]) < 1   # 小数（7.1% → 0.071）
                assert len(v["quarter_end"]) == 10 and v.get("source")

    def test_check_61_message(self, tmp_path):
        import json
        from common.sec_data import report_consistency_check as rcc
        (tmp_path / "SOFI.json").write_text(json.dumps({"current": {"quarter_end": "2099-12-31"}}), encoding="utf-8")
        msgs = rcc._check_company_reported_rotce(out_dir=str(tmp_path))
        sofi = [m for t, m in msgs if t == "SOFI"]
        assert sofi and "WARN-61" in sofi[0] and "2099-12-31" in sofi[0]


class TestComparisonTtm:
    def test_ttm_average_skips_missing_and_nonpositive_ends(self):
        # 4四半期前（2024-12-31）のTCEが0以下・2025-03-31は純資産が無い → 残り3つの期末の平均
        se = {q: 1_000 for q in QEND}
        del se["2025-03-31"]
        gw = {q: 0 for q in QEND}
        gw["2024-12-31"] = 1_200
        r = _run(_store(se=se, gw=gw))
        last = r["history"][-1]
        assert last["rotce_ttm_tce_ends"] == ["2025-06-30", "2025-09-30", "2025-12-31"]
        assert last["rotce_ttm_basis"] == "avg3"
        assert abs(last["rotce_ttm"] - 100 / 1_000) < 1e-12

    def test_percentile_uses_ttm_series_not_quarterly(self):
        # 直近の四半期だけ一過性の大きな利益 → 見出しのROTCEは大きいが、比較用（TTM）で順位を付ける
        ni = {q: 25 for q in QEND}
        ni["2025-12-31"] = 1_000
        r = _run(_store(ni=ni))
        cur, last = r["current"], r["history"][-1]
        assert abs(cur["rotce"] - 4_000 / 1_000) < 1e-12
        assert abs(cur["rotce_ttm"] - 1_075 / 1_000) < 1e-12
        assert r["percentile"]["rotce_series"] == "rotce_ttm"
        assert r["percentile"]["rotce"] == rotce.percentile_of(
            last["rotce_ttm"], [h["rotce_ttm"] for h in r["history"] if h.get("rotce_ttm") is not None])

    def test_nonpositive_ttm_gets_no_signal_even_if_quarter_positive(self):
        ni = {q: -50 for q in QEND}
        ni["2025-12-31"] = 10
        r = _run(_store(ni=ni))
        assert r["current"]["rotce"] > 0 and r["current"]["rotce_ttm"] < 0
        assert r["percentile"]["reason"] == rotce.R_ROTCE_NONPOSITIVE
