"""申告のCF計算書の行（cf_lines）とCapExの規則（[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段5a・段5b・段5c、2026-11-04）"""
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
        ("us-gaap:PaymentsToAcquireOtherProductiveAssets", "ppe"),   # タグだけなら ppe（申告の中では line_kinds）
        ("us-gaap:PaymentsForProceedsFromProductiveAssets", "ppe"),
        ("us-gaap:PaymentsToAcquireEquipmentOnLease", "excluded"),   # 賃貸用の機器（CAT・DOCN）
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


class TestAmbiguousLines:
    """タグだけでは決まらない行（PaymentsToAcquireOtherProductiveAssets）は表示名、無ければ同じ申告のほかの行で決める"""
    O = "us-gaap:PaymentsToAcquireOtherProductiveAssets"
    P = "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment"

    def _kind(self, labels, other_ppe):
        lines = [{"concept": self.O, "values": {"p": 1.0}, **({"labels": labels} if labels else {})}]
        if other_ppe:
            lines.append({"concept": self.P, "values": {"p": 1.0}})
        return dict((x["concept"], k) for k, x in cl.line_kinds({"lines": lines}))[self.O]

    @pytest.mark.parametrize("label,kind", [
        ("Capital expenditures (including capitalized software)", "ppe"),        # VZ
        ("Capitalization of software development costs", "software"),            # SNPS
        ("Acquisition of assets", "excluded"),                                   # BROS
        ("Acquisition of businesses, net of cash acquired", "excluded"),         # LITE
        ("Payment for asset acquisitions", "excluded"),                          # LITE
        ("Purchase of assets held for sale", "excluded"),                        # PLTR
        ("Payment related to asset acquisition", "excluded"),                    # SOUN
    ])
    def test_label_rules(self, label, kind):
        assert self._kind([label, "Payments to Acquire Other Productive Assets"], other_ppe=True) == kind

    def test_without_labels(self):
        assert self._kind(None, other_ppe=False) == "ppe"
        assert self._kind(None, other_ppe=True) == "excluded"

    def test_label_texts_from_linkbase(self):
        lab = (f'<link:linkbase {LB}><link:labelLink xlink:type="extended">'
               f'<link:loc xlink:type="locator" xlink:href="x.xsd#us-gaap_PaymentsToAcquireOtherProductiveAssets" xlink:label="a"/>'
               f'<link:label xlink:type="resource" xlink:label="b" xlink:role="r1">Acquisition of assets</link:label>'
               f'<link:label xlink:type="resource" xlink:label="b" xlink:role="r2">Acquisition of assets</link:label>'
               f'<link:labelArc xlink:type="arc" xlink:from="a" xlink:to="b"/></link:labelLink></link:linkbase>').encode()
        assert cl.label_texts(lab, [self.O]) == {self.O: ["Acquisition of assets"]}

    def test_saved_records_have_labels(self):
        """タグだけでは決まらない行のある保存済みの申告には、表示名が付いている"""
        n = 0
        for t in os.listdir(cl.DATA_DIR):
            for rec in cl.load_records(t).values():
                for x in rec.get("lines") or []:
                    if x["concept"].endswith(":PaymentsToAcquireOtherProductiveAssets"):
                        assert x.get("labels"), (t, rec["accn"])
                        n += 1
        assert n > 0


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

    def test_same_value_keeps_layer3_entry(self, tmp_path):
        """値が同じ期は Layer3 のエントリをそのまま（段5c）"""
        self._write(tmp_path, "VZ", [_rec("k", "10-K", "2026-02-10", [
            ("us-gaap:PaymentsToAcquireOtherProductiveAssets", {"2025-01-01~2025-12-31": 17011e6}),
            (TOT, {"2025-01-01~2025-12-31": 1})])])
        old = [{"end": "2025-12-31", "is_annual": True, "is_ytd": False, "val": 17011e6, "source_tag": "X",
                "first_val": 17011e6, "first_filed": "2026-02-10"}]
        out = cl.fill_layer3_capex("VZ", old, data_dir=str(tmp_path))
        assert out == old

    def test_different_value_shifts_first_val_and_history(self, tmp_path):
        """値が違う期は Layer3 のエントリを残し、val・first_val・申告の履歴を同じ差だけずらす（最初の申告の時点を保つ）"""
        P = "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment"
        self._write(tmp_path, "HQY", [_rec("k", "10-K", "2026-03-20", [
            (P, {"2025-02-01~2026-01-31": 2e6}), ("us-gaap:PaymentsForSoftware", {"2025-02-01~2026-01-31": 46e6}),
            (TOT, {"2025-02-01~2026-01-31": 1})])])
        old = [{"end": "2026-01-31", "start": "2025-02-01", "is_annual": True, "is_ytd": False, "val": 2e6,
                "first_val": 1.9e6, "first_filed": "2025-03-20", "source_tag": "PaymentsToAcquirePropertyPlantAndEquipment",
                "_asof": [["2025-03-20", 1.9e6], ["2026-03-20", 2e6]]}]
        e = cl.fill_layer3_capex("HQY", old, data_dir=str(tmp_path))[0]
        assert e["val"] == 48e6 and e["first_val"] == pytest.approx(47.9e6)
        assert e["_asof"] == [["2025-03-20", pytest.approx(47.9e6)], ["2026-03-20", 48e6]]
        assert e["source_tag"] == "PaymentsToAcquirePropertyPlantAndEquipment" and e["capex_basis"] == "cf_lines"
        assert old[0]["val"] == 2e6  # 元のリストは変えない

    def test_q4_rebuilt_across_tags(self, tmp_path):
        """Q4の逆算は置き換えの後に作り直す（期によってタグが違っても作れる）"""
        P = "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment"
        self._write(tmp_path, "T", [_rec("k", "10-K", "2026-02-10", [
            (P, {"2025-01-01~2025-12-31": 100.0}), (TOT, {"2025-01-01~2025-12-31": 1})])])
        q = [{"end": e, "start": s, "is_annual": False, "is_ytd": False, "val": 20.0, "source_tag": "A",
              "first_val": 20.0, "first_filed": "2025-11-01"}
             for s, e in (("2025-01-01", "2025-03-31"), ("2025-04-01", "2025-06-30"), ("2025-07-01", "2025-09-30"))]
        old = q + [{"end": "2026-12-31", "is_annual": True, "is_ytd": False, "val": 1.0, "source_tag": "B"},
                   {"end": "2025-12-31", "start": "2025-10-01", "is_annual": False, "is_ytd": False, "is_implied": True,
                    "val": 999.0}]
        out = cl.fill_layer3_capex("T", old, data_dir=str(tmp_path))
        q4 = [e for e in out if e["end"] == "2025-12-31" and not e["is_annual"]]
        assert len(q4) == 1 and q4[0]["is_implied"] and q4[0]["val"] == 40.0

    def test_dimension_only_filing_not_used(self, tmp_path):
        """取得系の行が軸の無い値を1つも持たない申告（軸つきの値だけ）の期は使わない（FCX 2026年Q2）"""
        P = "us-gaap:PaymentsToAcquireProductiveAssets"
        self._write(tmp_path, "FCX", [
            _rec("q2a", "10-Q", "2025-08-01", [(P, {"2025-01-01~2025-06-30": 2433e6}), (TOT, {"2025-01-01~2025-06-30": 1})]),
            _rec("q3a", "10-Q", "2025-11-01", [(P, {"2025-01-01~2025-09-30": 3489e6}), (TOT, {"2025-01-01~2025-09-30": 1})]),
            _rec("q2b", "10-Q", "2026-08-01", [(P, {}), (TOT, {"2025-01-01~2025-06-30": 1, "2026-01-01~2026-06-30": 1})]),
        ])
        cp = cl.capex_periods(cl.load_records("FCX", data_dir=str(tmp_path)))
        assert cp[("2025-09-30", False)]["val"] == 1056e6

    def test_capex_is_ppe_plus_software(self, tmp_path):
        self._write(tmp_path, "S", [_rec("k", "10-K", "2026-03-01", [
            ("us-gaap:PaymentsToAcquirePropertyPlantAndEquipment", {"2025-02-01~2026-01-31": 5e6}),
            ("us-gaap:PaymentsToDevelopSoftware", {"2025-02-01~2026-01-31": 7e6}),
            ("us-gaap:PaymentsToAcquireEquipmentOnLease", {"2025-02-01~2026-01-31": 100e6}),
            ("us-gaap:PaymentsToAcquireIntangibleAssets", {"2025-02-01~2026-01-31": 3e6}),
            (TOT, {"2025-02-01~2026-01-31": 1})])])
        assert cl.capex_periods(cl.load_records("S", data_dir=str(tmp_path)))[("2026-01-31", True)]["val"] == 12e6
        assert cl.annual_capex_from_10k("S", "k", data_dir=str(tmp_path))["val"] == 12e6

    def test_fetch_skips_saved_and_records_404(self, tmp_path, monkeypatch):
        cl.save_record("X", "0000000000-25-000001", {"status": "ok", "lines": [], "accn": "0000000000-25-000001",
                                                     "form": "10-Q", "filed": "2025-05-01"}, data_dir=str(tmp_path))
        from common.sec_data import bs_lines
        bs_lines.save_record("X", "0000000000-25-000001", {"status": "ok", "lines": [], "accn": "0000000000-25-000001",
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
        assert bs_lines.load_records("X", data_dir=str(tmp_path))["0000000000-25-000002"]["status"] == "not_found"   # 貸借対照表の行も

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
