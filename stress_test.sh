#!/usr/bin/env bash

set -euo pipefail

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
  --server-log PATH      Log file used with --start-server
  --help                 Show this help message

The script requires ZooKeeper and a registered RPC service. Parameters can
also be set with BUILD_DIR, BUILD_FIRST, START_SERVER, THREADS, REQUESTS,
PAYLOAD_SIZE, CLIENT_CONFIG, SERVER_CONFIG, and SERVER_LOG environment variables.
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
    --help)
      usage
      exit 0
      ;;
    *)
      fail "unknown option: $1"
      ;;
  esac
done

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

# 构建检查模块：显式要求构建或缺少目标程序时，复用项目构建脚本。
[[ -f "${CLIENT_CONFIG}" ]] || fail "client config not found: ${CLIENT_CONFIG}"
if [[ "${BUILD_FIRST}" == "1" || ! -x "${BUILD_DIR}/stress_test_client" ]]; then
  echo "Building RPC stress-test client..."
  BUILD_DIR="${BUILD_DIR}" "${PROJECT_ROOT}/rpc_build.sh"
fi
[[ -x "${BUILD_DIR}/stress_test_client" ]] || \
  fail "stress-test client not found: ${BUILD_DIR}/stress_test_client"

# 进程清理模块：仅回收由本脚本启动的服务端，避免压测中断后遗留进程。
cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -TERM "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# 本地服务编排模块：可选启动服务端，并等待其完成 ZooKeeper 注册。
if [[ "${START_SERVER}" == "1" ]]; then
  [[ -f "${SERVER_CONFIG}" ]] || fail "server config not found: ${SERVER_CONFIG}"
  [[ -x "${BUILD_DIR}/rpc_server" ]] || \
    fail "RPC server not found: ${BUILD_DIR}/rpc_server"

  echo "Starting local RPC server (log: ${SERVER_LOG})..."
  mkdir -p -- "$(dirname "${SERVER_LOG}")"
  (
    cd "${BUILD_DIR}"
    exec ./rpc_server "${SERVER_CONFIG}"
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

# 压测执行模块：从构建目录运行，以保持日志和默认相对路径行为一致。
echo "Running stress test: threads=${THREADS}, requests=${REQUESTS}, payload=${PAYLOAD_SIZE} bytes"
(
  cd "${BUILD_DIR}"
  exec ./stress_test_client \
    --config "${CLIENT_CONFIG}" \
    --threads "${THREADS}" \
    --requests "${REQUESTS}" \
    --payload-size "${PAYLOAD_SIZE}"
)
