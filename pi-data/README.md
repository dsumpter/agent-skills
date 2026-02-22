# pi-data

pi-data is a pi-mono extension that adds data analysis tooling around DuckDB. It provides catalog discovery, SQL execution, result profiling, visualization, and notebook-style workflows.

## Capabilities

- Workspace session management
- Catalog search and table descriptions
- SQL execution with materialized results
- Result previews, profiling, aggregation, and export
- Vega-Lite visualization tooling (inline PNG + HTML)
- Notebook cells with execution and HTML export
- RLM planning/summarization utilities

## Configuration

Set environment variables or `~/.pi/agent/pi-data.json` to configure defaults.

- `PI_DATA_DUCKDB_PATH`: DuckDB CLI binary
- `PI_DATA_DATABASE`: Path to a DuckDB database
- `PI_DATA_WORKSPACE`: Workspace directory
- `PI_DATA_UV_PATH`: `uv` binary to run inline chart rendering
- `PI_DATA_MAX_RESULT_ROWS`: Default max rows to materialize
- `PI_DATA_INLINE_CHART_WIDTH`: Default inline chart width
- `PI_DATA_INLINE_CHART_HEIGHT`: Default inline chart height
- `PI_DATA_AUTO_OPEN_HTML`: Auto-open HTML dashboards (`true`/`false`)
  - Also respects tool parameters like `open` for `data_viz_html` and `data_dashboard`.
- `PI_DATA_UV_PATH`: Override the `uv` binary used for inline chart rendering.

## Smoke Test

Run the smoke test script:

```bash
./scripts/smoke-test.sh
```

Set `PI_SMOKE_TEST_PROMPT` to run a non-interactive prompt through pi.

## Development

Build the extension with:

```bash
cd pi-data
npm install
npm run build
```
