#!/usr/bin/env python3
"""Run the TLC acceptance suites for RowService.tla (see ../correctness-properties.md).

usage: python3 suite.py SUITE [-j JOBS] [-w WORKERS] [--only NAME[,NAME...]] [--ref GIT_REF] [--full] [--sim-seconds N]

  quick      everything needed after a change to the model, in minutes: regress + holes + ext + mutants + witnesses + sim
  regress    the changed model with the faults and fixes of holes 5-8 switched off must be, state for state, the model
             at GIT_REF: same outcome and same state counts in every scenario. This carries the earlier long runs
             (base, fixed) over to the changed model without repeating them. --full adds base and fixed themselves (hours).
  holes      each hole's minimal scenario: without the fix it must fail, with the fix it must pass
  ext        the faults of holes 5-8, one at a time and all together, on top of all earlier faults at a small scale:
             each must pass
  mutants    each planted bug (constant Mut) in the environment of fixed.cfg, and each fix of holes 5-8 switched off in
             its ext environment: each must be caught
  witnesses  each W_* reachability claim: each must be violated (holes 1-4 in the fixed environment, 5-8 in ext)
  sim        random simulation of fixed_big.cfg (larger than any exhaustive run) for --sim-seconds: no violation
  liveness   live.cfg must pass; the liveness-only mutants must be caught by their temporal property
  all        quick + liveness

Every job runs in its own metadir (states/suite/<job>) and writes its TLC log to out/suite/<job>.out
(and its config to out/suite/<job>.cfg unless the config is a file in this directory).
Exit status is 0 only if every job met its expectation.
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

# The last version of the model before holes 5-8 were added: the default reference of "regress".
BASELINE_REF = "6b9291e"

# Holes 5-8 (design.md 13.2). NEW_OFF: their faults and their fixes all switched off; with these values the model
# is the model before they were added (that is what "regress" checks). NEW_FIX: faults off, fixes on.
NEW_OFF = {
    "MaxFlips": "0", "MaxQueueTimeouts": "0", "MaxZombieStmts": "0", "RetryWhileBusy": "FALSE",
    "FixCrossRow": "FALSE", "FixRetryAnswer": "FALSE", "FixSdkRetry": "FALSE", "FixEpoch": "FALSE",
}
NEW_FIX = dict(NEW_OFF, FixCrossRow="TRUE", FixRetryAnswer="TRUE", FixSdkRetry="TRUE", FixEpoch="TRUE")

# The environment of fixed.cfg: two clients, the faults of holes 1-4 enabled, all fixes on.
FIXED = {
    "Clients": "{c1, c2}", "Cols": "{1}", "MaxEntry": "3", "MaxGen": "2", "MaxVal": "2", "MaxOps": "3",
    "MaxStmtTimeouts": "1", "MaxDrops": "1", "MaxClientTimeouts": "1", "MaxCrashes": "1",
    "InsertCanBeLate": "TRUE", "RetryCreate": "TRUE",
    "FixDedup": "TRUE", "FixFence": "TRUE", "FixUnsureRetry": "TRUE", **NEW_FIX, "Mut": '"none"',
}

# The environment of live.cfg (smaller: liveness checking costs far more than safety checking).
LIVE = dict(FIXED, MaxEntry="2", MaxVal="1", MaxOps="2")

# The ext environments: the scale of live.cfg, every earlier fault, plus the fault of one of holes 5-8.
# One fault at a time keeps each run to a minute or two; ext_all enables the three together (about 120 million
# states), and fixed_big.cfg (sim) does the same at a larger scale by random simulation.
EXT = {
    "ext_crossrow": dict(LIVE, MaxFlips="2"),
    "ext_sdk": dict(LIVE, MaxQueueTimeouts="1", RetryWhileBusy="TRUE"),
    "ext_epoch": dict(LIVE, MaxZombieStmts="1"),
    "ext_all": dict(LIVE, MaxFlips="2", MaxQueueTimeouts="1", RetryWhileBusy="TRUE", MaxZombieStmts="1"),
}
# jobs that explore tens of millions of states get three times the workers, so that they do not set the pace of "quick"
HEAVY = {"ext_all", "reg_base_2col_ref", "reg_base_2col_new", "reg_base_ref", "reg_base_new",
         "reg_fixed_ref", "reg_fixed_new"}

INVARIANTS = [
    "NoViolation", "NoResurrection", "MemMatchesAbs", "QuiescentConsistent", "DbMatchesAbsWhenIdle",
    "CreateDurable", "OneWriterPerKey", "DirtyImpliesQueued", "QueuedIsTracked", "LiveEntryInMap",
    "NoDirtyEviction", "DbValsBelongToGen", "DbNotAhead", "CleanIsDurable", "FencedRestart",
    "CreateFailureIsFinal",
]
ACTION_PROPERTIES = ["DbVerMonotonic"]

# mutation -> the first check that catches it in the fixed environment (any violation counts as caught)
MUTANTS = {
    "NoVerCheck": "DbVerMonotonic",
    "NoGenCheck": "QuiescentConsistent",
    "FlushUpsert": "NoResurrection",
    "DedupDeletedWithRow": "NoResurrection",
    "NoMaskMerge": "CleanIsDurable",
    "EvictDirty": "NoDirtyEviction",
    "EvictNoDead": "LiveEntryInMap",
    "UpdateIgnoresDead": "NoDirtyEviction",
    "LoadNoRecheck": "MemMatchesAbs",
    "FlushNoOp": "QuiescentConsistent",
    "ReleaseOnAmbiguous": "QuiescentConsistent",
    "SelectConfirms": "QuiescentConsistent",
    "CreateNoLoad": "NoViolation",
    "DeleteKeepsDirty": "DirtyImpliesQueued",
    "LeakOpOnNotFound": "deadlock",
}

# fix of holes 5-8 -> (the ext environment in which switching it off must be caught, the check that catches it)
FIX_SWITCHES = {
    "FixCrossRow": ("ext_crossrow", "QuiescentConsistent"),
    "FixRetryAnswer": ("ext_sdk", "CreateFailureIsFinal"),
    "FixSdkRetry": ("ext_sdk", "CreateFailureIsFinal"),
    "FixEpoch": ("ext_epoch", "NoViolation"),
}

# mutants checked by a liveness property alone (no invariants, no deadlock check)
LIVE_MUTANTS = {
    "LeakOpOnNotFound": "NoLockLeak",
    "DeleteKeepsDirty": "EventuallyFlushed",
}

WITNESSES = [
    "W_Delete", "W_Recreate", "W_Evict", "W_Reload", "W_Crash", "W_CrashWithStmtInFlight",
    "W_LateStmt", "W_EffectiveLateStmt", "W_LateInsertOfDeletedGen", "W_OldUpdateAtRestart",
    "W_RetryOfDeletedGen", "W_DedupClaimed", "W_AmbThenNotExecuted", "W_FlushFailed",
    "W_UpdateDuringFlush", "W_UpdateDuringDelete", "W_ClientGaveUp",
]

# witness of a situation of holes 5-8 -> the ext environment in which it must be reachable
EXT_WITNESSES = {
    "W_CrossRowAfterAmb": "ext_crossrow",
    "W_FencedAttempt": "ext_crossrow",
    "W_NotExecutedAfterFence": "ext_crossrow",
    "W_RetryFindsOtherLife": "ext_sdk",
    "W_RetryQueuedBehindSelf": "ext_sdk",
    "W_RetryNotExecuted": "ext_sdk",
    "W_QueueTimeout": "ext_sdk",
    "W_ZombieStmt": "ext_epoch",
    "W_ZombieStoppedByEpoch": "ext_epoch",
}

HOLES = ["hole1", "hole2", "hole3", "hole3_insert", "hole4"]

# Holes 5-8: (minimal environment with every fix on, the fix whose absence is the hole). Generated, not files.
TINY = {
    "Clients": "{c1}", "Cols": "{1}", "MaxEntry": "1", "MaxGen": "1", "MaxVal": "0", "MaxOps": "2",
    "MaxStmtTimeouts": "0", "MaxDrops": "0", "MaxClientTimeouts": "0", "MaxCrashes": "0",
    "InsertCanBeLate": "FALSE", "RetryCreate": "FALSE",
    "FixDedup": "TRUE", "FixFence": "TRUE", "FixUnsureRetry": "TRUE", **NEW_FIX, "Mut": '"none"',
}
NEW_HOLES = {
    # an ambiguous INSERT/DELETE, a retry rejected by a cross-row constraint, then the other row changes
    "hole5": (dict(TINY, MaxStmtTimeouts="1", MaxDrops="1", InsertCanBeLate="TRUE", MaxFlips="2"), "FixCrossRow"),
    # create succeeds but the client gave up; the row is deleted and re-created; the retry is answered "exists"
    "hole6": (dict(TINY, Clients="{c1, c2}", MaxGen="2", MaxClientTimeouts="1", RetryCreate="TRUE"), "FixRetryAnswer"),
    # the retry times out queued behind its own first attempt (or is not executed) and is reported as failed
    "hole7": (dict(TINY, MaxOps="3", MaxDrops="1", MaxClientTimeouts="1", RetryCreate="TRUE",
                   MaxQueueTimeouts="1", RetryWhileBusy="TRUE"), "FixSdkRetry"),
    # a superseded instance writes after the new instance has started
    "hole8": (dict(TINY, MaxEntry="2", MaxVal="1", MaxOps="3", MaxCrashes="1", MaxZombieStmts="1"), "FixEpoch"),
}

# regress: scenarios compared between the reference model and the changed one. Scenarios that pass are compared by
# state counts; scenarios that fail (the holes) only by what is violated: TLC stops at the first counterexample it
# meets, and with several workers the counts at that moment vary from run to run.
REGRESS_FILES = ["hole1", "hole1_fixed", "hole2", "hole2_fixed", "hole3", "hole3_fixed",
                 "hole3_insert", "hole3_insert_fixed", "hole4", "hole4_fixed", "base_2col"]
REGRESS_FULL_FILES = ["base", "fixed"]


def cfg_text(consts, header, spec="Spec", invariants=(), properties=(), symmetry=True, deadlock=True,
             constraints=(), view=None):
    lines = [f"\\* {header} (generated by suite.py)", f"SPECIFICATION {spec}", "CONSTANTS"]
    lines += [f"    {k} = {v}" for k, v in consts.items()]
    if invariants:
        lines += ["INVARIANTS"] + [f"    {i}" for i in invariants]
    if properties:
        lines += ["PROPERTIES"] + [f"    {p}" for p in properties]
    if symmetry:
        lines.append("SYMMETRY Symm")
    for c in constraints:
        lines.append(f"CONSTRAINT {c}")
    if view:
        lines.append(f"VIEW {view}")
    if not deadlock:
        lines.append("CHECK_DEADLOCK FALSE")
    return "\n".join(lines) + "\n"


def read_cfg(path):
    """Parse one of the *.cfg files of this directory into the arguments of cfg_text."""
    consts, invs, props, constraints, symmetry, section = {}, [], [], [], False, None
    for line in open(path):
        line = line.split("\\*")[0].rstrip()
        if not line.strip():
            continue
        word = line.split()[0]
        if word in ("SPECIFICATION", "CONSTANTS", "INVARIANTS", "PROPERTIES"):
            section = word
        elif word == "SYMMETRY":
            symmetry = True
        elif word == "CONSTRAINT":
            constraints.append(line.split()[1])
        elif section == "CONSTANTS":
            k, v = line.split("=", 1)
            consts[k.strip()] = v.strip()
        elif section == "INVARIANTS":
            invs.append(line.strip())
        elif section == "PROPERTIES":
            props.append(line.strip())
    return dict(consts=consts, invariants=invs, properties=props, symmetry=symmetry, constraints=constraints)


class Job:
    def __init__(self, name, expect, cfg=None, cfg_file=None, note="", spec_dir=HERE, sim_seconds=0, pair=None):
        self.name, self.expect, self.cfg, self.cfg_file, self.note = name, expect, cfg, cfg_file, note
        self.spec_dir, self.sim_seconds, self.pair = spec_dir, sim_seconds, pair


def classify(out):
    """Return (outcome, detail) from a TLC log."""
    m = re.search(r"Invariant (\w+) is violated", out)
    if m:
        detail = m.group(1)
        if detail == "NoViolation":
            # the last state of the trace says which assertion inside an action fired
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
    if job.cfg_file:
        cfg_path = os.path.join(HERE, job.cfg_file)
    else:
        cfg_path = os.path.join(HERE, "out", "suite", job.name + ".cfg")
        with open(cfg_path, "w") as f:
            f.write(job.cfg)
    log = os.path.join(HERE, "out", "suite", job.name + ".out")
    heavy = job.name in HEAVY
    cmd = ["java", "-XX:+UseParallelGC", f"-Xmx{'16g' if heavy else heap}", "-cp", JAR, "tlc2.TLC",
           "-config", cfg_path, "-workers", str(workers * 3 if heavy else workers), "-metadir", meta]
    if job.sim_seconds:
        cmd += ["-simulate", "-depth", "300"]
    cmd.append("RowService.tla")
    t0 = time.time()
    for attempt in range(2):
        timed_out = False
        with open(log, "w") as f:
            try:
                subprocess.run(cmd, cwd=job.spec_dir, stdout=f, stderr=subprocess.STDOUT,
                               timeout=job.sim_seconds or None)
            except subprocess.TimeoutExpired:
                timed_out = True   # a simulation runs until it is stopped; subprocess.run has killed it
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
                generated=counts[-1][0] if counts else "?", depth=depth[-1] if depth else "?",
                trace=len(re.findall(r"^State \d+:", out, re.M)), secs=secs,
                ok=(job.expect is None or got == job.expect), note=job.note, pair=job.pair)


def declared_constants(spec_text):
    """The names declared in the CONSTANTS block of a spec."""
    block = spec_text.split("\nCONSTANTS\n", 1)[1].split("\n\n", 1)[0]
    return set(re.findall(r"^\s+(\w+)", block, re.M))


def regress_jobs(ref, full):
    ref_dir = os.path.join(HERE, "out", "suite", "ref")
    os.makedirs(ref_dir, exist_ok=True)
    try:
        ref_text = subprocess.run(["git", "show", f"{ref}:tla/RowService.tla"], cwd=HERE, check=True,
                                  capture_output=True, text=True).stdout
    except subprocess.CalledProcessError as e:
        sys.exit(f"regress: cannot read tla/RowService.tla at git ref {ref}: {e.stderr.strip()}")
    with open(os.path.join(ref_dir, "RowService.tla"), "w") as f:
        f.write(ref_text)
    known = declared_constants(ref_text)
    scenarios = {name: read_cfg(os.path.join(HERE, name + ".cfg"))
                 for name in REGRESS_FILES + (REGRESS_FULL_FILES if full else [])}
    # all the faults and fixes of holes 1-4 at the scale of live.cfg (checked for safety only, with symmetry)
    scenarios["fixed_small"] = dict(consts=dict(LIVE), invariants=list(INVARIANTS), properties=ACTION_PROPERTIES,
                                    symmetry=True, constraints=[])
    jobs = []
    for name, sc in scenarios.items():
        invs = [i for i in sc["invariants"] if re.search(rf"^{i} ==", ref_text, re.M)]
        common = dict(invariants=invs, properties=sc["properties"], symmetry=sc["symmetry"],
                      constraints=sc["constraints"])
        old = {k: v for k, v in sc["consts"].items() if k in known}
        new = dict(sc["consts"], **{k: v for k, v in NEW_OFF.items() if k not in known})
        jobs.append(Job(f"reg_{name}_ref", None, pair=name, spec_dir=ref_dir,
                        cfg=cfg_text(old, f"regress {name}: the model at {ref}", **common)))
        jobs.append(Job(f"reg_{name}_new", None, pair=name,
                        cfg=cfg_text(new, f"regress {name}: the changed model, holes 5-8 off, viewed through OldVars",
                                     view="OldVars", **common)))
    return jobs


def build(suite, only, ref, full, sim_seconds):
    jobs = []
    want = lambda s: suite == s or suite == "all" or (suite == "quick" and s != "liveness")
    if want("regress"):
        jobs += regress_jobs(ref, full)
    if want("ext"):
        for name, env in EXT.items():
            jobs.append(Job(name, "pass", cfg=cfg_text(env, f"{name}, expected to pass",
                                                       invariants=INVARIANTS, properties=ACTION_PROPERTIES)))
    if want("holes"):
        for h in HOLES:
            jobs.append(Job(h, "violation", cfg_file=h + ".cfg"))
            jobs.append(Job(h + "_fixed", "pass", cfg_file=h + "_fixed.cfg"))
        for h, (env, fix) in NEW_HOLES.items():
            sym = "c2" in env["Clients"]
            jobs.append(Job(h, "violation", note=f"{fix} off",
                            cfg=cfg_text(dict(env, **{fix: "FALSE"}), f"{h}: minimal scenario, {fix} off, expected to fail",
                                         invariants=INVARIANTS, properties=ACTION_PROPERTIES, symmetry=sym)))
            jobs.append(Job(h + "_fixed", "pass",
                            cfg=cfg_text(env, f"the {h} scenario with every fix on, expected to pass",
                                         invariants=INVARIANTS, properties=ACTION_PROPERTIES, symmetry=sym)))
    if want("mutants"):
        for m, why in MUTANTS.items():
            jobs.append(Job("mut_" + m, "violation", note=why,
                            cfg=cfg_text(dict(FIXED, Mut=f'"{m}"'), f"mutant {m}, expected to be caught by {why}",
                                         invariants=INVARIANTS, properties=ACTION_PROPERTIES)))
        for fix, (env, why) in FIX_SWITCHES.items():
            jobs.append(Job("off_" + fix, "violation", note=why,
                            cfg=cfg_text(dict(EXT[env], **{fix: "FALSE"}),
                                         f"{fix} off in {env}, expected to be caught by {why}",
                                         invariants=INVARIANTS, properties=ACTION_PROPERTIES)))
    if want("witnesses"):
        for w in WITNESSES:
            jobs.append(Job(w, "violation",
                            cfg=cfg_text(FIXED, f"witness {w}, must be violated", invariants=[w])))
        for w, env in EXT_WITNESSES.items():
            jobs.append(Job(w, "violation", note=env,
                            cfg=cfg_text(EXT[env], f"witness {w} in {env}, must be violated", invariants=[w])))
    if want("sim"):
        jobs.append(Job("sim_fixed_big", "pass", cfg_file="fixed_big.cfg", sim_seconds=sim_seconds))
    if want("liveness"):
        jobs.append(Job("live", "pass", cfg_file="live.cfg"))
        for m, why in LIVE_MUTANTS.items():
            jobs.append(Job("live_mut_" + m, "violation", note=why,
                            cfg=cfg_text(dict(LIVE, Mut=f'"{m}"'), f"liveness mutant {m}, expected to be caught by {why}",
                                         spec="FairSpec", properties=[why], symmetry=False, deadlock=False)))
    if only:
        wanted = set(only.split(","))
        jobs = [j for j in jobs if j.name in wanted or j.pair in wanted]
    return jobs


def compare_pairs(results):
    """regress: the reference model and the changed model must agree in every scenario."""
    bad = 0
    pairs = {}
    for r in results:
        if r["pair"]:
            pairs.setdefault(r["pair"], {})[r["name"].rsplit("_", 1)[1]] = r
    for name, p in sorted(pairs.items()):
        ref, new = p.get("ref"), p.get("new")
        if not ref or not new:
            continue
        if ref["got"] == "pass":
            # The depth is printed but not compared: with several workers TLC's reported depth varies from run to
            # run of the same model (seen as 18 or 19 on hole1_fixed); the two counts do not.
            same = all(ref[k] == new[k] for k in ("got", "generated", "states"))
            what = (f"generated {ref['generated']} / {new['generated']}, distinct {ref['states']} / {new['states']}, "
                    f"depth {ref['depth']} / {new['depth']}")
        else:
            same = (ref["got"], ref["detail"]) == (new["got"], new["detail"]) and ref["got"] != "error"
            what = f"{ref['got']} {ref['detail']} / {new['got']} {new['detail']}"
        bad += not same
        print(f"{'SAME' if same else 'DIFF'} {name:<22} reference / changed: {what}")
    return bad


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("suite", choices=["quick", "regress", "holes", "ext", "mutants", "witnesses", "sim",
                                      "liveness", "all"])
    ap.add_argument("-j", "--jobs", type=int, default=6, help="TLC runs in parallel (default 6)")
    ap.add_argument("-w", "--workers", type=int, default=8, help="TLC workers per run (default 8)")
    ap.add_argument("--heap", default="6g", help="JVM heap per run (default 6g)")
    ap.add_argument("--only", default="", help="comma-separated job names (or regress scenario names)")
    ap.add_argument("--ref", default=BASELINE_REF, help=f"regress: git ref of the reference model (default {BASELINE_REF})")
    ap.add_argument("--full", action="store_true", help="regress: also compare base and fixed (hours)")
    ap.add_argument("--sim-seconds", type=int, default=120, help="sim: how long to simulate (default 120)")
    a = ap.parse_args()
    jobs = build(a.suite, a.only, a.ref, a.full, a.sim_seconds)
    results = []
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=a.jobs) as ex:
        for fut in cf.as_completed([ex.submit(run_job, j, a.workers, a.heap) for j in jobs]):
            r = fut.result()
            results.append(r)
            expect = r["expect"] or "-"
            print(f"{'OK  ' if r['ok'] else 'FAIL'} {r['name']:<30} expect={expect:<9} got={r['got']:<9} "
                  f"{r['detail']:<30} states={r['states']:<12} trace={r['trace']:<3} {r['secs']:.0f}s", flush=True)
    bad = len([r for r in results if not r["ok"]])
    print(f"\n{len(results) - bad}/{len(results)} jobs met their expectation ({time.time() - t0:.0f}s)")
    bad += compare_pairs(results)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
