#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
command -v protoc >/dev/null
TOOLS_DIR=$(mktemp -d)
trap 'rm -rf "${TOOLS_DIR}"' EXIT
GOBIN="${TOOLS_DIR}" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12-0.20260120151049-f2248ac996af
GOBIN="${TOOLS_DIR}" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
cd "${ROOT_DIR}"
PATH="${TOOLS_DIR}:${PATH}" protoc --go_out=. --go_opt=paths=source_relative \
 --go-grpc_out=. --go-grpc_opt=paths=source_relative src/node/rpc/protocol/node.proto
