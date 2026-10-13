#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./zk_start.sh

Start the example standalone ZooKeeper in the foreground.
Configuration: config/zoo.cfg (relative dataDir paths use the project root).
Press Ctrl+C to stop it.

Ubuntu/Debian: install default-jre-headless zookeeper libslf4j-java once.
Other installations: set ZOOKEEPER_HOME to the extracted distribution,
or set ZK_SERVER_CLASSPATH to the server jars and their dependencies.
EOF
}

fail() {
  echo "zk_start.sh: $*" >&2
  exit 1
}

if [[ $# -eq 1 && "$1" == --help ]]; then
  usage
  exit 0
fi
[[ $# -eq 0 ]] || fail "unexpected arguments; use --help for usage"

PROJECT_ROOT="$(dirname -- "$(readlink -f -- "${BASH_SOURCE[0]}")")"
CONFIG_PATH="${PROJECT_ROOT}/config/zoo.cfg"
[[ -f "${CONFIG_PATH}" ]] || fail "configuration was not found: ${CONFIG_PATH}"
command -v java >/dev/null || fail "Java was not found; install a Java runtime first"

# 在切换目录前解析安装路径，支持从仓库父目录或其他目录启动。
if [[ -n "${ZK_SERVER_CLASSPATH:-}" ]]; then
  # 显式 classpath 按调用目录解析；保留 Java 的目录通配符语义。
  IFS=: read -r -a CLASSPATH_ENTRIES <<<"${ZK_SERVER_CLASSPATH}"
  CLASSPATH=""
  for entry in "${CLASSPATH_ENTRIES[@]}"; do
    [[ -n "$entry" ]] || entry="."
    entry="$(realpath -m -- "$entry")"
    CLASSPATH+="${CLASSPATH:+:}${entry}"
  done
elif [[ -n "${ZOOKEEPER_HOME:-}" ]]; then
  ZK_DISTRIBUTION="$(realpath -m -- "${ZOOKEEPER_HOME}")"
  shopt -s nullglob
  SERVER_JARS=("${ZK_DISTRIBUTION}"/zookeeper-*.jar
               "${ZK_DISTRIBUTION}"/lib/zookeeper-*.jar)
  [[ ${#SERVER_JARS[@]} -gt 0 && -d "${ZK_DISTRIBUTION}/lib" ]] ||
    fail "ZOOKEEPER_HOME must contain the ZooKeeper binary distribution and lib/"
  CLASSPATH="${ZK_DISTRIBUTION}/*:${ZK_DISTRIBUTION}/lib/*"
elif [[ -f /usr/share/java/zookeeper.jar && -f /usr/share/java/slf4j-simple.jar ]]; then
  CLASSPATH="/usr/share/java/zookeeper.jar:/usr/share/java/slf4j-simple.jar"
else
  fail "ZooKeeper server jars were not found; install zookeeper and libslf4j-java, or set ZOOKEEPER_HOME / ZK_SERVER_CLASSPATH"
fi

cd "${PROJECT_ROOT}"
mkdir -p .cache/zk-local/data
echo "ZooKeeper configuration: ${CONFIG_PATH} (Ctrl+C to stop)"
# exec 让前台信号直接交给 JVM；不启动额外的后台进程。
exec java -Dzookeeper.admin.enableServer=false \
  -cp "${CLASSPATH}" org.apache.zookeeper.server.quorum.QuorumPeerMain \
  "${CONFIG_PATH}"
