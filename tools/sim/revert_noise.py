"""regen_derived.py の後、丸め（浮動小数点の末尾）と generated_at だけが変わったファイルを git checkout で戻す。
使い方: python tools/sim/revert_noise.py DIR...（例: common/sec_data/ttm）。実質の変更のあるファイルは残す"""
import json
import math
import os
import subprocess
import sys

SKIP = {"generated_at"}


def same(a, b):
    if isinstance(a, dict) and isinstance(b, dict):
        ks = (set(a) | set(b)) - SKIP
        return all(k in a and k in b and same(a[k], b[k]) for k in ks)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, float) or isinstance(b, float):
        try:
            return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-12)
        except (TypeError, ValueError):
            return False
    return a == b


def main(dirs):
    changed = subprocess.run(["git", "diff", "--name-only", "--", *dirs], capture_output=True, text=True).stdout.split()
    reverted = kept = 0
    for p in changed:
        old = subprocess.run(["git", "show", "HEAD:" + p], capture_output=True).stdout
        try:
            if same(json.loads(old.decode("utf-8")), json.load(open(p, encoding="utf-8"))):
                subprocess.run(["git", "checkout", "--", p], check=True)
                reverted += 1
                continue
        except Exception:
            pass
        kept += 1
    print(f"戻した（丸め・時刻だけ）: {reverted} / 残した（実質の変更）: {kept}")


if __name__ == "__main__":
    main(sys.argv[1:])
