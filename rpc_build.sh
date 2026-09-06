#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-${PROJECT_ROOT}/build}"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_JOBS="${BUILD_JOBS:-$(nproc)}"

resolve_vcpkg_root() {
  if [[ -n "${VCPKG_ROOT:-}" ]]; then
    if [[ -d "${VCPKG_ROOT}" ]]; then
      readlink -f "${VCPKG_ROOT}"
    else
      printf '%s\n' "${VCPKG_ROOT}"
    fi
    return
  fi

  local vcpkg_bin
  vcpkg_bin="$(command -v vcpkg || true)"
  if [[ -n "${vcpkg_bin}" ]]; then
    vcpkg_bin="$(readlink -f "${vcpkg_bin}")"
    printf '%s\n' "${vcpkg_bin%/*}"
  fi
}

VCPKG_ROOT_VALUE="$(resolve_vcpkg_root)"
CMAKE_TOOLCHAIN_VALUE="${CMAKE_TOOLCHAIN_FILE:-}"
if [[ -z "${CMAKE_TOOLCHAIN_VALUE}" && -n "${VCPKG_ROOT_VALUE}" ]]; then
  CMAKE_TOOLCHAIN_VALUE="${VCPKG_ROOT_VALUE}/scripts/buildsystems/vcpkg.cmake"
fi

if [[ -n "${CMAKE_TOOLCHAIN_VALUE}" && ! -f "${CMAKE_TOOLCHAIN_VALUE}" ]]; then
  echo "CMake toolchain file was not found: ${CMAKE_TOOLCHAIN_VALUE}" >&2
  exit 1
fi
if [[ -n "${CMAKE_TOOLCHAIN_VALUE}" ]]; then
  CMAKE_TOOLCHAIN_VALUE="$(readlink -f "${CMAKE_TOOLCHAIN_VALUE}")"
fi

CMAKE_CONFIGURE_ARGS=(
  -S "${PROJECT_ROOT}"
  -B "${BUILD_DIR}"
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
)

if [[ -n "${CMAKE_TOOLCHAIN_VALUE}" ]]; then
  CMAKE_CONFIGURE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_VALUE}")
fi

# CMake caches dependency locations. Refresh only when the build environment
# changes, so a build directory can safely be reused after moving machines.
mkdir -p "${BUILD_DIR}"
ENVIRONMENT_SIGNATURE_FILE="${BUILD_DIR}/.rpc-build-environment"
ENVIRONMENT_SIGNATURE="$({
  printf 'toolchain=%s\n' "${CMAKE_TOOLCHAIN_VALUE}"
  printf 'vcpkg_root=%s\n' "${VCPKG_ROOT_VALUE}"
  printf 'triplet=%s\n' "${VCPKG_TARGET_TRIPLET:-${VCPKG_DEFAULT_TRIPLET:-}}"
  printf 'prefix_path=%s\n' "${CMAKE_PREFIX_PATH:-}"
  printf 'compiler=%s;%s\n' "${CC:-}" "${CXX:-}"
  printf 'generator=%s\n' "${CMAKE_GENERATOR:-}"
} | sha256sum | awk '{print $1}')"

if [[ -f "${BUILD_DIR}/CMakeCache.txt" ]]; then
  CACHED_ENVIRONMENT_SIGNATURE=""
  if [[ -f "${ENVIRONMENT_SIGNATURE_FILE}" ]]; then
    CACHED_ENVIRONMENT_SIGNATURE="$(<"${ENVIRONMENT_SIGNATURE_FILE}")"
  fi
  if [[ "${CACHED_ENVIRONMENT_SIGNATURE}" != "${ENVIRONMENT_SIGNATURE}" ]]; then
    echo "Build environment changed; refreshing CMake cache in ${BUILD_DIR}" >&2
    cmake -E remove -f "${BUILD_DIR}/CMakeCache.txt"
    cmake -E remove_directory "${BUILD_DIR}/CMakeFiles"
  fi
fi

cmake "${CMAKE_CONFIGURE_ARGS[@]}"
cmake --build "${BUILD_DIR}" --parallel "${BUILD_JOBS}"
printf '%s\n' "${ENVIRONMENT_SIGNATURE}" >"${ENVIRONMENT_SIGNATURE_FILE}"
