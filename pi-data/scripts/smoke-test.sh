#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB_BIN="${PI_DATA_DUCKDB_PATH:-${ROOT_DIR}/../duckdb}"
WORKSPACE_DIR="$(mktemp -d -t pi-data-smoke-XXXXXX)"
DB_PATH="${WORKSPACE_DIR}/smoke.duckdb"

if [[ -f "${HOME}/.bashrc" ]]; then
  source "${HOME}/.bashrc"
fi

PI_BIN="${PI_BIN:-}"
if [[ -z "${PI_BIN}" ]]; then
  PI_BIN="$(command -v pi || true)"
fi
if [[ -z "${PI_BIN}" && -x "${HOME}/.local/bin/pi" ]]; then
  PI_BIN="${HOME}/.local/bin/pi"
fi

if [[ -z "${PI_BIN}" ]]; then
  echo "pi CLI not found in PATH. Install pi before running this smoke test." >&2
  exit 1
fi

if [[ ! -x "${DUCKDB_BIN}" ]]; then
  echo "DuckDB CLI not found at ${DUCKDB_BIN}. Set PI_DATA_DUCKDB_PATH." >&2
  exit 1
fi

"${DUCKDB_BIN}" "${DB_PATH}" <<'SQL'
CREATE TABLE customers (id INTEGER, name VARCHAR, region VARCHAR);
INSERT INTO customers VALUES (1, 'Acme', 'NA'), (2, 'Globex', 'EMEA');
SQL

export PI_DATA_DATABASE="${DB_PATH}"
export PI_DATA_WORKSPACE="${WORKSPACE_DIR}/.pi-data"
export PI_DATA_DUCKDB_PATH="${DUCKDB_BIN}"
export PI_DATA_AUTO_OPEN_HTML="false"

"${PI_BIN}" install "${ROOT_DIR}" >/dev/null

if [[ -z "${PI_SMOKE_TEST_PROMPT:-}" ]]; then
  echo "Skipping pi prompt run. Set PI_SMOKE_TEST_PROMPT to run a non-interactive prompt." >&2
  exit 0
fi

"${PI_BIN}" -p -e "${ROOT_DIR}/index.ts" "${PI_SMOKE_TEST_PROMPT}"
