#!/usr/bin/env bash
# 用 TLC 检查 RowService.tla。用法：./run.sh <配置名> [TLC 的其他参数]
#   配置名：hole1 / hole2 / hole3（当前设计，预期失败并给出反例）
#           base（当前设计去掉三个漏洞的前提，预期通过）
#           fixed（所有故障打开 + 修复方案，预期通过）
# 需要 Java 11+ 和 tla2tools.jar（默认放在 ~/.local/share/tla/，可以用 TLA2TOOLS 指定）。
# 环境变量：TLC_HEAP（JVM 堆，默认 8g）、TLC_WORKERS（检查线程数，默认 auto）。
set -euo pipefail
cd "$(dirname "$0")"
cfg=${1:?用法：./run.sh <配置名>}; shift
jar=${TLA2TOOLS:-$HOME/.local/share/tla/tla2tools.jar}
# 只清理本场景自己的目录（不用 TLC 的 -cleanup：它会清空整个 states/，影响并行运行的其他场景）
rm -rf "states/$cfg"
exec java -XX:+UseParallelGC -Xmx${TLC_HEAP:-8g} -cp "$jar" tlc2.TLC \
  -config "$cfg.cfg" -workers "${TLC_WORKERS:-auto}" -metadir "states/$cfg" "$@" RowService.tla
