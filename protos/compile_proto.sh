#!/usr/bin/env bash

set -euo pipefail

PROTO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_FILE="${PROTO_DIR}/message.proto"

if [[ -n "${PROTOC:-}" ]]; then
  PROTOC_BIN="${PROTOC}"
elif command -v protoc >/dev/null 2>&1; then
  PROTOC_BIN="$(command -v protoc)"
elif [[ -n "${VCPKG_ROOT:-}" ]]; then
  VCPKG_TRIPLET="${VCPKG_TARGET_TRIPLET:-${VCPKG_DEFAULT_TRIPLET:-x64-linux}}"
  PROTOC_BIN="${VCPKG_ROOT}/installed/${VCPKG_TRIPLET}/tools/protobuf/protoc"
else
  echo "protoc was not found; set PROTOC or VCPKG_ROOT" >&2
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
