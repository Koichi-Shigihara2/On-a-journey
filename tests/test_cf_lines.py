"""申告のCF計算書の行（cf_lines）とCapExの欠けの規則（[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段5a・段5b、2026-11-04）"""
import json
import os

import pytest

from common.sec_data import cf_lines as cl
from common.sec_data.fcf import CAPEX_NO_CF_LINE_MARK
from common.sec_data.parser import SECParser

LB = 'xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink"'


def _pre(concepts, wrap_xsd=False, flat=True):
    """CF計算書の表示の構造。concepts は上から順の行（us-gaap_X 形式の断片）"""
    locs = "".join(f'<link:loc xlink:type="locator" xlink:href="x.xsd#{c}" xlink:label="L{i}"/>'
                   for i, c in enumerate(["us-gaap_StatementOfCashFlowsAbstract"] + concepts))
    arcs = "".join(f'<link:presentationArc xlink:type="arc" xlink:from="L0" xlink:to="L{i + 1}" order="{i + 1}"/>'
                   for i in range(len(concepts)))
    link = (f'<link:presentationLink xlink:type="extended" xlink:role="http://x/role/CashFlow">{locs}{arcs}'
            f'</link:presentationLink>')
    body = f'<link:linkbase {LB}>{link}</link:linkbase>'
    if wrap_xsd:
        return (f'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:annotation><xs:appinfo>{body}'
                f'</xs:appinfo></xs:annotation></xs:schema>').encode()
    return body.encode()


def _inst(facts, prefix="us-gaap", ns="http://fasb.org/us-gaap/2024"):
    """facts: [(prefix, local, start, end, val)]"""
    ctxs, els = {}, []
    for i, (pfx, local, s, e, v) in enumerate(facts):
        cid = f"c_{s}_{e}"
        ctxs[cid] = (f'<xbrli:context id="{cid}"><xbrli:entity><xbrli:identifier scheme="x">1</xbrli:identifier>'
                     f'</xbrli:entity><xbrli:period><xbrli:startDate>{s}</xbrli:startDate><xbrli:endDate>{e}'
                     f'</xbrli:endDate></xbrli:period></xbrli:context>')
        els.append(f'<{pfx}:{local} contextRef="{cid}" unitRef="usd" decimals="-3">{v}</{pfx}:{local}>')
    return (f'<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:us-gaap="{ns}" '
            f'xmlns:lyft="http://lyft.com/2024">{"".join(ctxs.values())}{"".join(els)}</xbrli:xbrl>').encode()


CF = ["us-gaap_NetCashProvidedByUsedInOperatingActivities",
      "us-gaap_PaymentsToAcquireMarketableSecurities",
      "lyft_PaymentsToAcquirePropertyAndEquipmentAndScooterFleet",
      "us-gaap_PaymentsToAcquireBusinessesNetOfCashAcquired",
      "us-gaap_NetCashProvidedByUsedInInvestingActivities",
      "us-gaap_RepaymentsOfDebt",
      "us-gaap_NetCashProvidedByUsedInFinancingActivities"]


class TestClassify:
    @pytest.mark.parametrize("concept,kind", [
        ("lyft:PaymentsToAcquirePropertyAndEquipmentAndScooterFleet", "ppe"),
        ("us-gaap:PaymentsToAcquirePropertyPlantAndEquipment", "ppe"),
        ("us-gaap:PaymentsToAcquireOtherProductiveAssets", "ppe"),
        ("us-gaap:PaymentsForProceedsFromProductiveAssets", "ppe"),
        ("flyw:CapitalizationOfInternallyDevelopedSoftware", "software"),
        ("us-gaap:PaymentsToDevelopSoftware", "software"),
        ("us-gaap:PaymentsToAcquireIntangibleAssets", "other_acq"),
        ("us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired", "excluded"),
        ("us-gaap:PaymentsToAcquireMarketableSecurities", "excluded"),
        ("us-gaap:ProceedsFromSaleOfPropertyPlantAndEquipment", "excluded"),
        ("us-gaap:NetCashProvidedByUsedInInvestingActivities", "total"),
    ])
    def test_kind(self, concept, kind):
        assert cl.classify(concept) == kind


class TestExtract:
    def test_flat_tree_investing_section_only(self):
        inst = _inst([("us-gaap", "NetCashProvidedByUsedInOperatingActivities", "2025-01-01", "2025-12-31", 100),
                      ("lyft", "PaymentsToAcquirePropertyAndEquipmentAndScooterFleet", "2025-01-01", "2025-12-31", 52822000),
                      ("us-gaap", "NetCashProvidedByUsedInInvestingActivities", "2025-01-01", "2025-12-31", -60000000)])
        rec = cl.extract(_pre(CF), inst)
        assert rec["status"] == "ok"
        names = [x["concept"] for x in rec["lines"]]
        assert names[0] == "us-gaap:PaymentsToAcquireMarketableSecurities"   # 営業CFの合計の次から
        assert names[-1] == "us-gaap:NetCashProvidedByUsedInInvestingActivities"  # 投資CFの合計まで
        assert "us-gaap:RepaymentsOfDebt" not in names
        lyft = next(x for x in rec["lines"] if x["concept"].startswith("lyft:"))
        assert lyft["values"] == {"2025-01-01~2025-12-31": 52822000.0}

    def test_xsd_embedded_linkbase(self):
        """_pre.xml の無い申告（表示の構造が .xsd に埋め込まれている）も読める"""
        inst = _inst([("us-gaap", "NetCashProvidedByUsedInInvestingActivities", "2025-01-01", "2025-12-31", -1)])
        rec = cl.extract(_pre(CF, wrap_xsd=True), inst)
        assert rec["status"] == "ok"
        assert any(x["concept"].startswith("lyft:") for x in rec["lines"])

    def test_no_cf_statement(self):
        rec = cl.extract(_pre(["us-gaap_Revenues"]), _inst([]))
        assert rec["status"] == "no_cf_statement"


def _rec(accn, form, filed, lines):
    return {"accn": accn, "form": form, "filed": filed, "status": "ok", "role": "r",
            "lines": [{"concept": c, "values": v} for c, v in lines]}


TOT = "us-gaap:NetCashProvidedByUsedInInvestingActivities"


class TestRule:
    def _write(self, tmp_path, ticker, recs):
        for r in recs:
            cl.save_record(ticker, r["accn"], r, data_dir=str(tmp_path))

    def test_no_capex_line_is_zero_with_mark(self, tmp_path):
        """CF計算書に取得系の行が無い申告の期は0（印）。Layer3に値があっても置き換える"""
        self._write(tmp_path, "APP", [_rec("a1", "10-K", "2026-02-12", [
            ("us-gaap:PaymentsToAcquireIntangibleAssets", {"2025-01-01~2025-12-31": 28.3e6}),
            (TOT, {"2025-01-01~2025-12-31": -100e6})])])
        old = [{"end": "2025-12-31", "is_annual": True, "is_ytd": False, "val": 4.2e6}]
        out = cl.fill_layer3_capex("APP", old, data_dir=str(tmp_path))
        a = [e for e in out if e["end"] == "2025-12-31" and e["is_annual"]]
        assert len(a) == 1 and a[0]["val"] == 0.0 and a[0]["mark"] == CAPEX_NO_CF_LINE_MARK
        assert old[0]["val"] == 4.2e6  # 元のリストは変えない

    def test_custom_tag_fills_missing_quarters(self, tmp_path):
        """Layer3に無い期を独自タグの行で埋める（3か月の列・同じ申告のYTDの差。Q4は作らない）"""
        L = "lyft:PaymentsToAcquirePropertyAndEquipmentAndScooterFleet"
        self._write(tmp_path, "LYFT", [
            _rec("q1", "10-Q", "2025-05-01", [(L, {"2025-01-01~2025-03-31": 6.5e6}), (TOT, {"2025-01-01~2025-03-31": 1})]),
            _rec("q2", "10-Q", "2025-08-01", [(L, {"2025-01-01~2025-06-30": 20.8e6}), (TOT, {"2025-01-01~2025-06-30": 1})]),
            _rec("k", "10-K", "2026-02-11", [(L, {"2025-01-01~2025-12-31": 52.822e6}), (TOT, {"2025-01-01~2025-12-31": 1})]),
        ])
        out = cl.fill_layer3_capex("LYFT", [], data_dir=str(tmp_path))
        got = {(e["end"], e["is_annual"]): round(e["val"]) for e in out}
        assert got[("2025-03-31", False)] == 6_500_000
        assert got[("2025-06-30", False)] == 14_300_000
        assert got[("2025-12-31", True)] == 52_822_000
        assert ("2025-12-31", False) not in got  # Q4はTTMの逆算に任せる
        assert all(e["capex_basis"] == "cf_lines" for e in out)

    def test_existing_layer3_kept(self, tmp_path):
        self._write(tmp_path, "VZ", [_rec("k", "10-K", "2026-02-10", [
            ("us-gaap:PaymentsToAcquireOtherProductiveAssets", {"2025-01-01~2025-12-31": 17011e6}),
            (TOT, {"2025-01-01~2025-12-31": 1})])])
        old = [{"end": "2025-12-31", "is_annual": True, "is_ytd": False, "val": 1.0}]
        out = cl.fill_layer3_capex("VZ", old, data_dir=str(tmp_path))
        assert out == old

    def test_fetch_skips_saved_and_records_404(self, tmp_path, monkeypatch):
        cl.save_record("X", "0000000000-25-000001", {"status": "ok", "lines": [], "accn": "0000000000-25-000001",
                                                     "form": "10-Q", "filed": "2025-05-01"}, data_dir=str(tmp_path))
        calls = []

        class R:
            status_code = 404

        def fake_get(url, label, budget, js=False):
            calls.append(url)
            return R()
        monkeypatch.setattr(cl, "_get", fake_get)
        cf = {"facts": {"us-gaap": {"T": {"units": {"USD": [
            {"accn": "0000000000-25-000001", "form": "10-Q", "filed": "2025-05-01", "val": 1},
            {"accn": "0000000000-25-000002", "form": "10-K", "filed": "2026-02-01", "val": 1},
            {"accn": "0000000000-20-000003", "form": "10-K", "filed": "2020-02-01", "val": 1}]}}}}}
        n = cl.fetch_new_filings("X", 1, cf, data_dir=str(tmp_path))
        assert n == {"new": 1, "saved": 1, "retry_later": 0}
        assert len(calls) == 1  # 保存済み・2021年より前の申告は取らない
        rec = cl.load_records("X", data_dir=str(tmp_path))["0000000000-25-000002"]
        assert rec["status"] == "not_found"

    def test_rate_and_cap_in_code(self):
        assert cl.MIN_GAP_SEC >= 0.1  # 毎秒10件未満
        b = cl.Budget(1)
        b.used = 1
        with pytest.raises(RuntimeError):
            cl._get("http://x", "x", b)


class TestParserFcf:
    def test_missing_capex_is_missing_fcf(self):
        d = {"cf": {"operating_cash_flow": 100.0}}
        SECParser._set_free_cash_flow(d)
        assert "free_cash_flow" not in d["cf"]

    def test_zero_capex_gives_ocf(self):
        d = {"cf": {"operating_cash_flow": 100.0, "capital_expenditure": 0.0}}
        SECParser._set_free_cash_flow(d)
        assert d["cf"]["free_cash_flow"] == 100.0


class TestSavedRecords:
    """保存した cf_lines の値（過去の申告で変わらない値）"""

    def _annual(self, ticker, end):
        cp = cl.capex_periods(cl.load_records(ticker))
        return cp[(end, True)]

    def test_lyft_custom_tag_matches_10k(self):
        # 10-K（0001628280-26-006960）R8.htm「Purchases of property and equipment and scooter fleet」
        assert self._annual("LYFT", "2025-12-31")["val"] == 52822000
        assert self._annual("LYFT", "2024-12-31")["val"] == 83470000
        assert self._annual("LYFT", "2023-12-31")["val"] == 149819000

    def test_app_no_capex_line(self):
        a = self._annual("APP", "2025-12-31")
        assert a["no_capex_line"] and a["val"] == 0.0

    def test_vz_and_loar(self):
        assert self._annual("VZ", "2025-12-31")["val"] == 17011000000
        assert self._annual("LOAR", "2025-12-31")["val"] == pytest.approx(13.0e6, rel=0.01)

    def test_statuses_recorded(self):
        root = cl.DATA_DIR
        st = {}
        for t in os.listdir(root):
            d = os.path.join(root, t, "cf_lines")
            if os.path.isdir(d):
                for n in os.listdir(d):
                    s = json.load(open(os.path.join(d, n), encoding="utf-8"))["status"]
                    st[s] = st.get(s, 0) + 1
                    if s != "ok":
                        assert json.load(open(os.path.join(d, n), encoding="utf-8")).get("reason")
        assert set(st) <= {"ok", "no_cf_statement", "not_found"}
