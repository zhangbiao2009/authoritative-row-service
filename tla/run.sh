#!/usr/bin/env bash
# Check RowService.tla with TLC. Usage: ./run.sh <config> [extra TLC args]
#   config: hole1 / hole2 / hole3 (current design, expected to fail with a counterexample)
#           base (current design without the preconditions of the three holes, expected to pass)
#           fixed (all faults enabled + the fixes, expected to pass)
#           base_2col, live (liveness under FairSpec), base_big / fixed_big (random simulation: add -simulate -depth 300)
#   The mutant, witness and liveness suites (../correctness-properties.md sections 7-9): python3 suite.py {holes|mutants|witnesses|liveness|all}
# Requires Java 11+ and tla2tools.jar (default location ~/.local/share/tla/, override with TLA2TOOLS).
# Environment: TLC_HEAP (JVM heap, default 8g), TLC_WORKERS (checker threads, default auto).
set -euo pipefail
cd "$(dirname "$0")"
cfg=${1:?usage: ./run.sh <config>}; shift
jar=${TLA2TOOLS:-$HOME/.local/share/tla/tla2tools.jar}
# Clean only this scenario's own directory (not TLC's -cleanup: it wipes all of states/ and breaks other scenarios running in parallel)
rm -rf "states/$cfg"
exec java -XX:+UseParallelGC -Xmx${TLC_HEAP:-8g} -cp "$jar" tlc2.TLC \
  -config "$cfg.cfg" -workers "${TLC_WORKERS:-auto}" -metadir "states/$cfg" "$@" RowService.tla
