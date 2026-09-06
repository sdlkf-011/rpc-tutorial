#!/usr/bin/env bash

set -euo pipefail

PROTO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_FILE="${PROTO_DIR}/message.proto"

if [[ -n "${PROTOC:-}" ]]; then
  PROTOC_BIN="${PROTOC}"
elif command -v protoc >/dev/null 2>&1; then
  PROTOC_BIN="$(command -v protoc)"
else
  if [[ -n "${VCPKG_ROOT:-}" ]]; then
    VCPKG_ROOT_VALUE="${VCPKG_ROOT}"
  elif VCPKG_BIN="$(command -v vcpkg 2>/dev/null || true)"; [[ -n "${VCPKG_BIN}" ]]; then
    VCPKG_BIN="$(readlink -f "${VCPKG_BIN}")"
    VCPKG_ROOT_VALUE="${VCPKG_BIN%/*}"
  fi
fi

if [[ -z "${PROTOC_BIN:-}" && -n "${VCPKG_ROOT_VALUE:-}" ]]; then
  VCPKG_TRIPLET="${VCPKG_TARGET_TRIPLET:-${VCPKG_DEFAULT_TRIPLET:-x64-linux}}"
  PROTOC_BIN="${VCPKG_ROOT_VALUE}/installed/${VCPKG_TRIPLET}/tools/protobuf/protoc"
fi

if [[ -z "${PROTOC_BIN:-}" ]]; then
  echo "protoc was not found; set PROTOC, VCPKG_ROOT, or add protoc to PATH" >&2
  exit 1
fi

if [[ ! -x "${PROTOC_BIN}" ]]; then
  echo "protoc is not executable: ${PROTOC_BIN}" >&2
  exit 1
fi

"${PROTOC_BIN}" \
  --proto_path="${PROTO_DIR}" \
  --cpp_out="${PROTO_DIR}" \
  "${PROTO_FILE}"
