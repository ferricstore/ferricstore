#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLAYWRIGHT_VERSION="${PLAYWRIGHT_VERSION:-1.60.0}"
TOOLS_DIR="${TMPDIR:-/tmp}/ferricstore-browser-tools"

if [ ! -d "${TOOLS_DIR}/node_modules/playwright" ]; then
  mkdir -p "${TOOLS_DIR}"
  npm --prefix "${TOOLS_DIR}" install --silent --no-audit --no-fund "playwright@${PLAYWRIGHT_VERSION}" >/dev/null
fi

NODE_PATH="${TOOLS_DIR}/node_modules" node "${ROOT_DIR}/tools/dashboard-inspect.mjs" "$@"
