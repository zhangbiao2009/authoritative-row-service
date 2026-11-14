#!/usr/bin/env python3
"""Run the TLC acceptance suites for RowCache.tla, the model of ../alternative-design.md.

usage: python3 suite.py SUITE [-j JOBS] [-w WORKERS] [--only NAME[,NAME...]] [--sim-seconds N]

  quick      main + ext + claims + mutants + witnesses + sim: everything except liveness, in minutes
  main       all faults the write-behind design's "fixed" environment has, every fix on: must pass
  full       the same at the scale of the write-behind design's fixed.cfg (three entries): long, not part of quick
  ext        the faults of design.md holes 5-8 (cross-row rejection, queue timeout and retry while busy, a
             superseded instance), one at a time and all together, on top of the main faults: each must pass
  claims     what alternative-design.md says about each hole of design.md 13.2: some cannot occur here even
             without their fix (must pass), some remain and need the same fix (must fail without it)
  mutants    each planted bug (constant Mut): each must be caught
  witnesses  each W_* reachability claim: each must be violated
  sim        random simulation at a scale exhaustive search cannot reach, for --sim-seconds: no violation
  liveness   the temporal properties under weak fairness must hold; a liveness mutant must be caught
  all        quick + liveness

Every job runs in its own metadir (states/suite/<job>) and writes its config and its TLC log to
out/suite/<job>.cfg and out/suite/<job>.out. Exit status is 0 only if every job met its expectation.
The method is that of ../../memory-authoritative/correctness-properties.md; the two suites are kept alike on purpose.
"""
import argparse
import concurrent.futures as cf
import os
import re
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
JAR = os.environ.get("TLA2TOOLS", os.path.expanduser("~/.local/share/tla/tla2tools.jar"))

# Two clients, two operations each, every fault of the write-behind design's "fixed" environment, every fix on.
SMALL = {
    "Clients": "{c1, c2}", "Cols": "{1}", "MaxEntry": "2", "MaxGen": "2", "MaxVal": "2", "MaxOps": "2",
    "MaxStmtTimeouts": "1", "MaxDrops": "1", "MaxClientTimeouts": "1", "MaxCrashes": "1",
    "MaxFlips": "0", "MaxQueueTimeouts": "0", "MaxZombieStmts": "0",
    "RetryCreate": "TRUE", "RetryWhileBusy": "FALSE",
    "FixDedup": "TRUE", "FixFence": "TRUE", "FixRetryAnswer": "TRUE", "FixSdkRetry": "TRUE", "FixEpoch": "TRUE",
    "Mut": '"none"',
}
# Three operations per client: about 100 million states, minutes.
MAIN = dict(SMALL, MaxOps="3")
# The scale of memory-authoritative/tla/fixed.cfg: three operations per client and three entries. Not part of "quick".
FULL = dict(MAIN, MaxEntry="3")

EXT = {
    "ext_crossrow": dict(SMALL, MaxFlips="2"),
    "ext_sdk": dict(SMALL, MaxQueueTimeouts="1", RetryWhileBusy="TRUE"),
    "ext_epoch": dict(SMALL, MaxZombieStmts="1"),
    "ext_all": dict(SMALL, MaxFlips="2", MaxQueueTimeouts="1", RetryWhileBusy="TRUE", MaxZombieStmts="1"),
}
# Random simulation: three clients, two columns, every fault twice.
BIG = dict(SMALL, Clients="{c1, c2, c3}", Cols="{1, 2}", MaxEntry="4", MaxGen="3", MaxVal="4", MaxOps="4",
           MaxStmtTimeouts="2", MaxDrops="2", MaxClientTimeouts="2", MaxCrashes="2",
           MaxFlips="2", MaxQueueTimeouts="2", MaxZombieStmts="2", RetryWhileBusy="TRUE")
# jobs that explore tens of millions of states get three times the workers
HEAVY = {"main", "full", "ext_all"}

INVARIANTS = [
    "NoViolation", "MemMatchesAbs", "CacheCoherent", "StmtOnlyUnderOp", "DbMatchesAbs", "NoResurrection",
    "CreateDurable", "OneWriterPerKey", "LiveEntryInMap", "DbValsBelongToGen", "FencedRestart",
    "CreateFailureIsFinal", "UpdateAnswerSound",
]
ACTION_PROPERTIES = ["DbVerMonotonic"]
LIVENESS = ["EventuallyResponds", "BackgroundSettles", "NoLockLeak"]

# Claims of alternative-design.md about the holes of design.md 13.2: name -> (environment, expected outcome, note).
# "pass" without the fix means the hole cannot occur in this design; "violation" means it remains and the fix is needed
# (the passing counterpart of each of those is "main" or the ext environment named in the note).
CLAIMS = {
    "hole1_late_insert_no_dedup": (dict(SMALL, FixDedup="FALSE", RetryCreate="FALSE"), "pass",
                                   "no dedup table, no SDK retry: a late INSERT cannot exist once op is released"),
    "hole2_retry_no_dedup": (dict(SMALL, FixDedup="FALSE"), "violation",
                             "the SDK retry of a create still resurrects a deleted row without the dedup table (main)"),
    "hole3_no_startup_fence": (dict(SMALL, FixFence="FALSE"), "violation",
                               "the old process's statement still lands after the restart without the startup fence (main)"),
    "hole4_amb_then_not_executed": (dict(SMALL, Clients="{c1}", MaxClientTimeouts="0", MaxCrashes="0",
                                         RetryCreate="FALSE"), "pass",
                                    "ambiguous, fence, read: there is no later failure to misread"),
    "hole5_cross_row": (dict(SMALL, MaxFlips="2"), "pass",
                        "a rejection caused by another row needs no mechanism of its own (same environment as ext_crossrow)"),
    "hole6_no_dedup_lookup": (dict(SMALL, FixRetryAnswer="FALSE"), "violation",
                              "a create retry that finds another life is still answered 'exists' (main)"),
    "hole7_sdk_rule_off": (dict(SMALL, MaxQueueTimeouts="1", RetryWhileBusy="TRUE", FixSdkRetry="FALSE"), "violation",
                           "the SDK still must not report 'not executed' after an ambiguous attempt (ext_sdk)"),
    "hole8_no_epoch": (dict(SMALL, MaxZombieStmts="1", FixEpoch="FALSE"), "violation",
                       "a superseded instance still makes the cache of the new one stale without the epoch (ext_epoch)"),
    # two rules the write-behind design needs for correctness and this design does not
    "redundant_NoGenCheck": (dict(SMALL, Mut='"NoGenCheck"'), "pass",
                             "UPDATE / DELETE without 'AND gen = ?': the snapshot under op always has the row's gen"),
    "redundant_LoadNoRecheck": (dict(SMALL, Mut='"LoadNoRecheck"'), "pass",
                                "a load that does not re-check snap reloads the same row: wasteful, not wrong"),
}

# mutation -> the check expected to catch it first (any violation counts as caught)
MUTANTS = {
    "NoConnFence": "StmtOnlyUnderOp",
    "ReleaseOnAmbiguous": "CacheCoherent",
    "AmbIsFailure": "CacheCoherent",
    "UpdateNoVerCompare": "UpdateAnswerSound",
    "EvictNoDead": "LiveEntryInMap",
    "DedupDeletedWithRow": "NoResurrection",
    "LeakOpOnNotFound": "deadlock",
}
LIVE_MUTANTS = {"LeakOpOnNotFound": "NoLockLeak"}

WITNESSES = [
    "W_Delete", "W_Recreate", "W_Update", "W_Evict", "W_Reload", "W_Crash", "W_CrashWithStmtInFlight",
    "W_CrashWithWritePending", "W_AmbStmtInFlight", "W_FenceThenApplied", "W_FenceThenNotApplied",
    "W_CreateAmb", "W_DeleteAmb", "W_NotExecuted", "W_DedupClaimed", "W_RetryOfDeletedGen",
    "W_GetDuringWrite", "W_ClientGaveUp",
]
EXT_WITNESSES = {
    "W_Rejected": "ext_crossrow",
    "W_AmbWhileBlocked": "ext_crossrow",
    "W_RetryFindsOtherLife": "ext_sdk",
    "W_RetryQueuedBehindSelf": "ext_sdk",
    "W_QueueTimeout": "ext_sdk",
    "W_ZombieStmt": "ext_epoch",
    "W_ZombieStoppedByEpoch": "ext_epoch",
}


def cfg_text(consts, header, spec="Spec", invariants=(), properties=(), symmetry=None, deadlock=True):
    if symmetry is None:
        symmetry = "c2" in consts["Clients"]
    lines = [f"\\* {header} (generated by suite.py)", f"SPECIFICATION {spec}", "CONSTANTS"]
    lines += [f"    {k} = {v}" for k, v in consts.items()]
    if invariants:
        lines += ["INVARIANTS"] + [f"    {i}" for i in invariants]
    if properties:
        lines += ["PROPERTIES"] + [f"    {p}" for p in properties]
    if symmetry:
        lines.append("SYMMETRY Symm")
    if not deadlock:
        lines.append("CHECK_DEADLOCK FALSE")
    return "\n".join(lines) + "\n"


def safety_cfg(consts, header):
    return cfg_text(consts, header, invariants=INVARIANTS, properties=ACTION_PROPERTIES)


class Job:
    def __init__(self, name, expect, cfg, note="", sim_seconds=0):
        self.name, self.expect, self.cfg, self.note, self.sim_seconds = name, expect, cfg, note, sim_seconds


def classify(out):
    """Return (outcome, detail) from a TLC log."""
    m = re.search(r"Invariant (\w+) is violated", out)
    if m:
        detail = m.group(1)
        if detail == "NoViolation":
            vs = re.findall(r'violation = "(\w+)"', out)
            if vs:
                detail += f"={vs[-1]}"
        return "violation", detail
    m = re.search(r"Action property (\w+)", out)
    if m:
        return "violation", m.group(1)
    if "Temporal properties were violated" in out:
        return "violation", "temporal property"
    if "Deadlock reached" in out:
        return "violation", "deadlock"
    if "No error has been found" in out:
        return "pass", ""
    m = re.search(r"Error: (.*)", out)
    return "error", (m.group(1) if m else "TLC did not finish, see log")


def run_job(job, workers, heap):
    meta = os.path.join(HERE, "states", "suite", job.name)
    shutil.rmtree(meta, ignore_errors=True)
    os.makedirs(meta, exist_ok=True)
    os.makedirs(os.path.join(HERE, "out", "suite"), exist_ok=True)
    cfg_path = os.path.join(HERE, "out", "suite", job.name + ".cfg")
    with open(cfg_path, "w") as f:
        f.write(job.cfg)
    log = os.path.join(HERE, "out", "suite", job.name + ".out")
    heavy = job.name in HEAVY
    cmd = ["java", "-XX:+UseParallelGC", f"-Xmx{'16g' if heavy else heap}", "-cp", JAR, "tlc2.TLC",
           "-config", cfg_path, "-workers", str(workers * 3 if heavy else workers), "-metadir", meta]
    if job.sim_seconds:
        cmd += ["-simulate", "-depth", "300"]
    cmd.append("RowCache.tla")
    t0 = time.time()
    for attempt in range(2):
        timed_out = False
        with open(log, "w") as f:
            try:
                subprocess.run(cmd, cwd=HERE, stdout=f, stderr=subprocess.STDOUT, timeout=job.sim_seconds or None)
            except subprocess.TimeoutExpired:
                timed_out = True   # a simulation runs until it is stopped
        out = open(log).read()
        # Several JVMs starting at the same instant sometimes fail to unpack TLC's standard modules. Run it again, once.
        if "Could not parse module" not in out:
            break
    secs = time.time() - t0
    shutil.rmtree(meta, ignore_errors=True)
    got, detail = classify(out)
    counts = re.findall(r"(\d+) states generated, (\d+) distinct states found", out)
    depth = re.findall(r"depth of the complete state graph search is (\d+)", out)
    states = counts[-1][1] if counts else "?"
    if job.sim_seconds and timed_out and got == "error":
        checked = re.findall(r"Progress: ([\d,]+) states checked", out)
        got, detail, states = "pass", "no violation when stopped", (checked[-1] if checked else "?")
    return dict(name=job.name, expect=job.expect, got=got, detail=detail, states=states,
                depth=depth[-1] if depth else "-", trace=len(re.findall(r"^State \d+:", out, re.M)),
                secs=secs, ok=(got == job.expect), note=job.note)


def build(suite, only, sim_seconds):
    jobs = []
    want = lambda s: suite == s or (s != "full" and (suite == "all" or (suite == "quick" and s != "liveness")))
    if want("main"):
        jobs.append(Job("main", "pass", safety_cfg(MAIN, "main: all faults of the fixed environment, every fix on")))
    if want("full"):
        jobs.append(Job("full", "pass", safety_cfg(FULL, "full: main at the scale of the write-behind fixed.cfg")))
    if want("ext"):
        for name, env in EXT.items():
            jobs.append(Job(name, "pass", safety_cfg(env, f"{name}, expected to pass")))
    if want("claims"):
        for name, (env, expect, note) in CLAIMS.items():
            jobs.append(Job(name, expect, safety_cfg(env, f"claim {name}: {note}"), note=note))
    if want("mutants"):
        for m, why in MUTANTS.items():
            jobs.append(Job("mut_" + m, "violation", note=why,
                            cfg=safety_cfg(dict(MAIN, Mut=f'"{m}"'), f"mutant {m}, expected to be caught by {why}")))
    if want("witnesses"):
        for w in WITNESSES:
            jobs.append(Job(w, "violation", cfg_text(MAIN, f"witness {w}, must be violated", invariants=[w])))
        for w, env in EXT_WITNESSES.items():
            jobs.append(Job(w, "violation", note=env,
                            cfg=cfg_text(EXT[env], f"witness {w} in {env}, must be violated", invariants=[w])))
    if want("sim"):
        jobs.append(Job("sim_big", "pass", safety_cfg(BIG, "random simulation: 3 clients, 2 columns, every fault twice"),
                        sim_seconds=sim_seconds))
    if want("liveness"):
        jobs.append(Job("live", "pass",
                        cfg_text(SMALL, "liveness under weak fairness", spec="FairSpec", invariants=INVARIANTS,
                                 properties=ACTION_PROPERTIES + LIVENESS, symmetry=False)))
        for m, why in LIVE_MUTANTS.items():
            jobs.append(Job("live_mut_" + m, "violation", note=why,
                            cfg=cfg_text(dict(SMALL, Mut=f'"{m}"'), f"liveness mutant {m}, expected to be caught by {why}",
                                         spec="FairSpec", properties=[why], symmetry=False, deadlock=False)))
    if only:
        wanted = set(only.split(","))
        jobs = [j for j in jobs if j.name in wanted]
    return jobs


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("suite", choices=["quick", "main", "full", "ext", "claims", "mutants", "witnesses", "sim",
                                      "liveness", "all"])
    ap.add_argument("-j", "--jobs", type=int, default=6, help="TLC runs in parallel (default 6)")
    ap.add_argument("-w", "--workers", type=int, default=8, help="TLC workers per run (default 8)")
    ap.add_argument("--heap", default="6g", help="JVM heap per run (default 6g)")
    ap.add_argument("--only", default="", help="comma-separated job names")
    ap.add_argument("--sim-seconds", type=int, default=120, help="sim: how long to simulate (default 120)")
    a = ap.parse_args()
    jobs = build(a.suite, a.only, a.sim_seconds)
    results = []
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=a.jobs) as ex:
        for fut in cf.as_completed([ex.submit(run_job, j, a.workers, a.heap) for j in jobs]):
            r = fut.result()
            results.append(r)
            print(f"{'OK  ' if r['ok'] else 'FAIL'} {r['name']:<30} expect={r['expect']:<9} got={r['got']:<9} "
                  f"{r['detail']:<30} states={r['states']:<12} depth={r['depth']:<3} trace={r['trace']:<3} "
                  f"{r['secs']:.0f}s", flush=True)
    bad = [r for r in results if not r["ok"]]
    print(f"\n{len(results) - len(bad)}/{len(results)} jobs met their expectation ({time.time() - t0:.0f}s)")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
