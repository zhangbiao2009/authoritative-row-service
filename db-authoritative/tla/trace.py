#!/usr/bin/env python3
"""Condense a TLC counterexample into "what each step did and which variables changed".

Usage: python3 trace.py < out/suite/<job>.out
Shows only the key variables that changed; pass -a to show all variables at every step.
(Same tool as ../../memory-authoritative/tla/trace.py; only the list of variables differs.)
"""
import re
import sys

# The variables most worth watching in a counterexample (shown in this order)
KEY = ["up", "pc", "bpc", "vpc", "mapE", "snap", "dead", "opH", "bgr", "bver",
       "net", "wait", "resp", "db", "dedup", "dbEpoch", "blocked", "abs", "absKnown", "preCrash", "preAlt",
       "deletedGens", "unkGen", "retry", "failedGens", "okUpd", "failedUpd", "appliedUpd", "violation"]


def parse(lines):
    states, cur, var = [], None, None
    for line in lines:
        m = re.match(r"^State (\d+): <([^ >]+)", line)
        if m:
            cur = {"n": int(m.group(1)), "action": m.group(2), "vars": {}}
            states.append(cur)
            var = None
            continue
        if cur is None:
            continue
        m = re.match(r"^/\\ (\w+) = (.*)$", line)
        if m:
            var = m.group(1)
            cur["vars"][var] = m.group(2).strip()
        elif var and line.startswith(" "):
            cur["vars"][var] += " " + line.strip()
        else:
            var = None
    return states


def main():
    show_all = "-a" in sys.argv
    lines = sys.stdin.read().splitlines()
    for line in lines:
        if "is violated" in line or line.startswith("Error:") or "eadlock" in line:
            print(line)
    prev = {}
    for st in parse(lines):
        print(f"\n--- step {st['n']}: {st['action']}")
        names = KEY if not show_all else KEY + sorted(set(st["vars"]) - set(KEY))
        for k in names:
            v = st["vars"].get(k)
            if v is not None and (show_all or prev.get(k) != v):
                print(f"    {k} = {v}")
        prev = st["vars"]
    for line in lines:
        if "states generated" in line or line.startswith("Model checking completed"):
            print(line)


if __name__ == "__main__":
    main()
