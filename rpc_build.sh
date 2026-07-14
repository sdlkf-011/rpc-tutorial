#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-${PROJECT_ROOT}/build}"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_JOBS="${BUILD_JOBS:-$(nproc)}"

"${PROJECT_ROOT}/protos/compile_proto.sh"

CMAKE_CONFIGURE_ARGS=(
  -S "${PROJECT_ROOT}"
  -B "${BUILD_DIR}"
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
)

if [[ -n "${VCPKG_ROOT:-}" ]]; then
  VCPKG_TOOLCHAIN="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
  if [[ ! -f "${VCPKG_TOOLCHAIN}" ]]; then
    echo "VCPKG_ROOT does not contain a vcpkg toolchain: ${VCPKG_ROOT}" >&2
    exit 1
  fi
  CMAKE_CONFIGURE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=${VCPKG_TOOLCHAIN}")
fi

cmake "${CMAKE_CONFIGURE_ARGS[@]}"
cmake --build "${BUILD_DIR}" --parallel "${BUILD_JOBS}"
