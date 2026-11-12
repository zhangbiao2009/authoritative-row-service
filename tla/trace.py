#!/usr/bin/env python3
"""把 TLC 输出的反例压缩成"每一步做了什么、哪些变量变了"。

用法：./run.sh hole1 | python3 trace.py
只显示变化了的关键变量；加 -a 显示每一步的全部变量。
"""
import re
import sys

# 读反例时最值得看的变量（按这个顺序显示）
KEY = ["up", "pc", "bpc", "fpc", "vpc", "mapE", "snap", "dirty", "queued", "dead", "opH",
       "queue", "fitem", "net", "wait", "resp", "db", "dedup", "abs", "absKnown",
       "deletedGens", "violation"]


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
        print(f"\n--- 第 {st['n']} 步：{st['action']}")
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
