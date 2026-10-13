#!/usr/bin/env bash

set -euo pipefail
set +m  # 子进程由 setsid 建立独立进程组，PID 同时作为组标识。

# 基础配置模块：命令行参数可以覆盖同名环境变量提供的默认值。
CALLER_DIR="${PWD}"
PROJECT_ROOT="$(realpath -m -- "$(dirname "${BASH_SOURCE[0]}")")"
BUILD_DIR="${BUILD_DIR:-${PROJECT_ROOT}/build}"
CLIENT_CONFIG="${CLIENT_CONFIG:-${PROJECT_ROOT}/config/rpc_client.json}"
SERVER_CONFIG="${SERVER_CONFIG:-${PROJECT_ROOT}/config/rpc_server.json}"
THREADS="${THREADS:-4}"
REQUESTS="${REQUESTS:-10000}"
PAYLOAD_SIZE="${PAYLOAD_SIZE:-64}"
BUILD_FIRST="${BUILD_FIRST:-0}"
START_SERVER="${START_SERVER:-0}"
SERVER_PID=""
CLIENT_PID=""
BUILD_PID=""
SHUTDOWN_TIMEOUT_SECONDS="${SHUTDOWN_TIMEOUT_SECONDS:-10}"
SERVER_LOG="${SERVER_LOG:-}"

# 帮助模块：说明脚本能够管理的构建、服务端和压测参数。
usage() {
  cat <<'EOF'
Usage: ./stress_test.sh [options]

Options:
  --build                Build the project before running the test
  --start-server         Start and stop a local rpc_server for this test
  --threads N            Concurrent client connections (default: 4)
  --requests N           Total requests (default: 10000)
  --payload-size N       Echo message size in bytes (default: 64)
  --client-config PATH   RPC client config file
  --server-config PATH   RPC server config file used with --start-server
  --build-dir PATH       Build directory (default: project build/)
  --server-log PATH      Server console output used with --start-server
  --shutdown-timeout N   Grace period before killing owned processes (1-60 seconds; default: 10)
  --help                 Show this help message

The script requires a running ZooKeeper. Without --start-server, the RPC
service must already be running and registered. Child programs run from the
project root and write RPC file logs to logs/. Relative CLI paths are resolved
from the caller's directory. Parameters can also be set with BUILD_DIR,
BUILD_FIRST, START_SERVER, THREADS, REQUESTS,
PAYLOAD_SIZE, CLIENT_CONFIG, SERVER_CONFIG, SERVER_LOG, and SHUTDOWN_TIMEOUT_SECONDS
environment variables.
EOF
}

fail() {
  echo "stress_test.sh: $*" >&2
  exit 1
}

require_value() {
  if [[ $# -lt 2 ]]; then
    fail "missing value for $1"
  fi
  if [[ -z "$2" ]]; then
    fail "empty value for $1"
  fi
}

# 参数解析模块：保留简单稳定的长选项接口，数值范围由压测客户端统一校验。
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      BUILD_FIRST=1
      shift
      ;;
    --start-server)
      START_SERVER=1
      shift
      ;;
    --threads)
      require_value "$@"
      THREADS="$2"
      shift 2
      ;;
    --requests)
      require_value "$@"
      REQUESTS="$2"
      shift 2
      ;;
    --payload-size)
      require_value "$@"
      PAYLOAD_SIZE="$2"
      shift 2
      ;;
    --client-config)
      require_value "$@"
      CLIENT_CONFIG="$2"
      shift 2
      ;;
    --server-config)
      require_value "$@"
      SERVER_CONFIG="$2"
      shift 2
      ;;
    --build-dir)
      require_value "$@"
      BUILD_DIR="$2"
      shift 2
      ;;
    --server-log)
      require_value "$@"
      SERVER_LOG="$2"
      shift 2
      ;;
    --shutdown-timeout)
      require_value "$@"
      SHUTDOWN_TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      fail "unknown option: $1"
      ;;
  esac
done

[[ "${SHUTDOWN_TIMEOUT_SECONDS}" =~ ^[1-9][0-9]?$ ]] &&
  (( SHUTDOWN_TIMEOUT_SECONDS <= 60 )) || fail "shutdown timeout must be an integer between 1 and 60"
command -v setsid >/dev/null || fail "setsid is required for child process cleanup"

# 路径始终按调用者工作目录解释，在构建或子进程 cd 前完成规范化。
absolute_path() {
  if [[ "$1" = /* ]]; then realpath -m -- "$1"
  else realpath -m -- "${CALLER_DIR}/$1"
  fi
}
CLIENT_CONFIG="$(absolute_path "${CLIENT_CONFIG}")"
SERVER_CONFIG="$(absolute_path "${SERVER_CONFIG}")"
BUILD_DIR="$(absolute_path "${BUILD_DIR}")"
SERVER_LOG="$(absolute_path "${SERVER_LOG:-${BUILD_DIR}/stress_test_server.log}")"

# 只操作本脚本创建的进程组；setsid 尚未完成时回退到直属子进程。
child_alive() {
  [[ -n "$1" ]] && { kill -0 -- "-$1" 2>/dev/null || kill -0 "$1" 2>/dev/null; }
}
signal_child() {
  local child_pid="$1" child_signal="$2"
  [[ -n "${child_pid}" ]] || return 0
  if kill -0 -- "-${child_pid}" 2>/dev/null; then
    kill "-${child_signal}" -- "-${child_pid}" 2>/dev/null || true
  elif kill -0 "${child_pid}" 2>/dev/null; then
    kill "-${child_signal}" "${child_pid}" 2>/dev/null || true
  fi
}
cleanup() {
  trap - EXIT
  trap '' INT TERM
  signal_child "${BUILD_PID}" TERM
  signal_child "${CLIENT_PID}" TERM
  signal_child "${SERVER_PID}" TERM
  local cleanup_deadline=$((SECONDS + SHUTDOWN_TIMEOUT_SECONDS + 1))
  while child_alive "${BUILD_PID}" || child_alive "${CLIENT_PID}" || child_alive "${SERVER_PID}"; do
    (( SECONDS < cleanup_deadline )) || break
    sleep 0.05
  done
  for child_pid in "${BUILD_PID}" "${CLIENT_PID}" "${SERVER_PID}"; do
    [[ -n "${child_pid}" ]] || continue
    signal_child "${child_pid}" KILL
    wait "${child_pid}" 2>/dev/null || true
  done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# 构建检查模块：显式要求构建或缺少目标程序时，复用项目构建脚本。
[[ -f "${CLIENT_CONFIG}" ]] || fail "client config not found: ${CLIENT_CONFIG}"
if [[ "${BUILD_FIRST}" == "1" || ! -x "${BUILD_DIR}/stress_test_client" ]]; then
  echo "Building RPC stress-test client..."
  BUILD_DIR="${BUILD_DIR}" setsid "${PROJECT_ROOT}/rpc_build.sh" &
  BUILD_PID=$!
  if wait "${BUILD_PID}"; then
    :
  else
    exit "$?"
  fi
fi
[[ -x "${BUILD_DIR}/stress_test_client" ]] || \
  fail "stress-test client not found: ${BUILD_DIR}/stress_test_client"

# 本地服务编排模块：可选启动服务端，并等待其完成 ZooKeeper 注册。
if [[ "${START_SERVER}" == "1" ]]; then
  [[ -f "${SERVER_CONFIG}" ]] || fail "server config not found: ${SERVER_CONFIG}"
  [[ -x "${BUILD_DIR}/rpc_server" ]] || \
    fail "RPC server not found: ${BUILD_DIR}/rpc_server"

  echo "Starting local RPC server (log: ${SERVER_LOG})..."
  mkdir -p -- "$(dirname "${SERVER_LOG}")"
  (
    cd "${PROJECT_ROOT}"
    exec setsid "${BUILD_DIR}/rpc_server" "${SERVER_CONFIG}"
  ) >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!

  for _ in {1..30}; do
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      tail -n 30 "${SERVER_LOG}" >&2 || true
      fail "RPC server exited during startup"
    fi
    if grep -q "RPC server started" "${SERVER_LOG}" 2>/dev/null; then
      break
    fi
    sleep 0.2
  done
  grep -q "RPC server started" "${SERVER_LOG}" 2>/dev/null || \
    fail "timed out waiting for the RPC server to start"
fi

# 压测执行模块：统一从仓库根目录运行，使日志位于项目的 logs/。
echo "Running stress test: threads=${THREADS}, requests=${REQUESTS}, payload=${PAYLOAD_SIZE} bytes"
(
  cd "${PROJECT_ROOT}"
  exec setsid "${BUILD_DIR}/stress_test_client" \
    --config "${CLIENT_CONFIG}" \
    --threads "${THREADS}" \
    --requests "${REQUESTS}" \
    --payload-size "${PAYLOAD_SIZE}"
) &
CLIENT_PID=$!
if wait "${CLIENT_PID}"; then
  exit 0
else
  exit "$?"
fi
