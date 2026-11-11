"""sim.py の --hook: parser の annual_*.json・quarterly_*.json の読みを、環境変数 PARSER_OVERLAY の置き場（無ければ元のまま）に差し替える。"""
import os, re


def apply(ctx):
    root = os.environ["PARSER_OVERLAY"]
    real_join = os.path.join
    real = real_join(os.getcwd(), "common", "sec_data", "data")
    rx = re.compile(r"^(annual|quarterly)_.+\.json$")

    def _join(a, *parts):
        r = real_join(a, *parts)
        if r.startswith(real):
            rel = r[len(real):].lstrip("\\/")
            segs = re.split(r"[\\/]", rel)
            if len(segs) == 2 and rx.match(segs[1]):
                alt = real_join(root, segs[0], segs[1])
                if os.path.exists(alt):
                    return alt
        return r
    os.path.join = _join
