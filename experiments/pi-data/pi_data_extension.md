# Pi Data Extension: Complete Design & Implementation Plan

## Overview

**pi-data** is an extension for [pi-mono](https://github.com/badlogic/pi-mono) that transforms the pi coding agent into an enterprise data analyst. It enables data discovery, SQL execution, visualization, and notebook-style workflows—all with excellent agent and developer experience.

### Key Capabilities
- Enterprise data discovery and exploration
- SQL generation, validation, and execution
- Result analysis without context overflow (RLM patterns)
- Inline terminal charts and HTML dashboards
- Notebook-style iterative analysis with versioning

### Design Philosophy
1. **Results as handles, not payloads**: Never stream large datasets into LLM context. Materialize to disk, reference by `resultId`.
2. **Streaming progress**: Every operation emits real-time updates via `onUpdate()`.
3. **Two viz modes**: Fast inline PNG for iteration, rich HTML for sharing.
4. **Clarification TUI**: When intent is ambiguous, open an overlay to clarify before executing.

---

## References & Prior Art

Study these before implementing:

| Reference | What to Learn | URL |
|-----------|---------------|-----|
| **pi-mono** | Agent runtime, AgentTool interface, event system | https://github.com/badlogic/pi-mono |
| **pi-agent-core** | Tool definition, streaming, state management | https://github.com/badlogic/pi-mono/tree/main/packages/agent |
| **pi-subagents** | Clarification TUI, chain orchestration, agent frontmatter | https://github.com/nicobailon/pi-subagents |
| **pi-charts** | Vega-Lite → Python/Altair → inline PNG rendering | https://github.com/walterra/agent-tools/tree/main/packages/pi-charts |
| **visual-explainer** | HTML generation, browser open, template patterns | https://github.com/nicobailon/visual-explainer |
| **pi-interactive-shell** | PTY sessions, overlay patterns, streaming updates | https://github.com/nicobailon/pi-interactive-shell |
| **Anthropic Data Plugin** | Commands: /analyze, /explore, /write-query | https://github.com/anthropics/knowledge-work-plugins/tree/main/data |
| **Hex Notebook Agent** | Cell-based editing, auto-versioning, trust patterns | https://hex.tech/blog/introducing-notebook-agent/ |
| **RLM Paper** | Recursive decomposition for long context | https://arxiv.org/abs/2512.24601 |
| **Ax** | DSPy for TypeScript, RLM implementation | https://axllm.dev/ |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Interface                           │
│  ┌─────────────┐  ┌──────────────────┐  ┌───────────────────┐  │
│  │   Pi TUI    │  │ Clarify Overlay  │  │    Status Bar     │  │
│  └─────────────┘  └──────────────────┘  └───────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       pi-data Tools                             │
│  ┌──────────┐ ┌─────────┐ ┌─────────┐ ┌─────┐ ┌────────┐ ┌───┐ │
│  │ Catalog  │ │   SQL   │ │ Results │ │ Viz │ │Notebook│ │RLM│ │
│  └──────────┘ └─────────┘ └─────────┘ └─────┘ └────────┘ └───┘ │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Workspace State (.pi-data/)                    │
│  ┌──────────────┐ ┌─────────────────┐ ┌─────────────────────┐  │
│  │ session.json │ │ results/*.parquet│ │ notebooks/*.json   │  │
│  └──────────────┘ └─────────────────┘ └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         External                                │
│  ┌──────────────────────┐  ┌────────────────────────────────┐  │
│  │    Database CLI      │  │   Python/Altair (optional)     │  │
│  │  (user-provided)     │  │   for chart rendering          │  │
│  └──────────────────────┘  └────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Model

### Core Identifiers

```typescript
// Session: workspace + database profile
interface Session {
  sessionId: string;          // UUID
  workspacePath: string;      // e.g., ".pi-data/"
  profile: string;            // Database connection profile name
  createdAt: number;
  description?: string;
}

// Result: materialized query output
interface Result {
  resultId: string;           // e.g., "r_a7f3c2"
  sql: string;
  rowCount: number;
  columns: Column[];
  createdAt: number;
  tags?: string[];
}

interface Column {
  name: string;
  type: string;
  nullable: boolean;
}

// Artifact: generated file (chart, dashboard, export)
interface Artifact {
  artifactId: string;
  type: "png" | "html" | "csv" | "parquet";
  path: string;
  createdAt: number;
}

// Notebook: collection of cells
interface Notebook {
  notebookId: string;
  title: string;
  cells: Cell[];
  createdAt: number;
  updatedAt: number;
}

interface Cell {
  cellId: string;
  kind: "sql" | "markdown" | "viz";
  content: string;
  output?: CellOutput;
  version: number;
}

interface CellOutput {
  type: "result" | "artifact" | "text" | "error";
  resultId?: string;
  artifactId?: string;
  text?: string;
  error?: string;
  executedAt: number;
}
```

---

## Tool Specifications

### Tool 1: `data_session`

**Purpose**: Create or load a data workspace.

```typescript
const dataSession: AgentTool = {
  name: "data_session",
  label: "Data Session",
  description: "Create or load a data analysis workspace. Call this first before other data tools.",
  parameters: Type.Object({
    action: Type.Union([
      Type.Literal("create"),
      Type.Literal("load"),
      Type.Literal("info"),
    ]),
    workspacePath: Type.Optional(Type.String({ 
      description: "Path to workspace directory. Defaults to .pi-data/" 
    })),
    profile: Type.Optional(Type.String({ 
      description: "Database connection profile name" 
    })),
    description: Type.Optional(Type.String()),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. If action="create": mkdir workspace, write session.json
    // 2. If action="load": read session.json, validate
    // 3. If action="info": return current session state
    // Return: { sessionId, workspacePath, profile, recentResults[] }
  },
};
```

### Tool 2: `data_search`

**Purpose**: Keyword search across database catalog (schemas, tables, columns).

```typescript
const dataSearch: AgentTool = {
  name: "data_search",
  label: "Search Catalog",
  description: "Search database catalog for tables and columns matching keywords.",
  parameters: Type.Object({
    query: Type.String({ description: "Search keywords (e.g., 'revenue product customer')" }),
    schemas: Type.Optional(Type.Array(Type.String(), { description: "Limit to specific schemas" })),
    limit: Type.Optional(Type.Number({ default: 20 })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Query information_schema via DB CLI
    // 2. Search table names, column names, comments
    // 3. Rank by relevance
    // Return: { matches: [{ kind, fqdn, score, snippet }] }
  },
};
```

### Tool 3: `data_list_tables`

**Purpose**: List tables in a schema.

```typescript
const dataListTables: AgentTool = {
  name: "data_list_tables",
  label: "List Tables",
  description: "List all tables in a schema.",
  parameters: Type.Object({
    schema: Type.Optional(Type.String({ description: "Schema name. Defaults to public/default." })),
    like: Type.Optional(Type.String({ description: "Filter pattern (e.g., 'order%')" })),
    limit: Type.Optional(Type.Number({ default: 100 })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Run: SELECT table_name FROM information_schema.tables WHERE ...
    // Return: { tables: [{ name, schema, rowEstimate?, comment? }] }
  },
};
```

### Tool 4: `data_describe`

**Purpose**: Get detailed table schema and statistics.

```typescript
const dataDescribe: AgentTool = {
  name: "data_describe",
  label: "Describe Table",
  description: "Get table schema: columns, types, keys, and row estimate.",
  parameters: Type.Object({
    table: Type.String({ description: "Table name (can include schema: schema.table)" }),
    includeStats: Type.Optional(Type.Boolean({ default: false, description: "Include column statistics (slower)" })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Query information_schema.columns
    // 2. Query constraints for PK/FK
    // 3. Optionally get APPROX_COUNT_DISTINCT, null rates
    // Return: { table, columns: [...], primaryKey?, foreignKeys?, rowEstimate? }
  },
};
```

### Tool 5: `data_write_sql`

**Purpose**: Generate SQL from natural language intent.

```typescript
const dataWriteSql: AgentTool = {
  name: "data_write_sql",
  label: "Write SQL",
  description: "Generate SQL query from natural language intent. Uses discovered schema context.",
  parameters: Type.Object({
    intent: Type.String({ description: "What you want to query (e.g., 'monthly revenue by product')" }),
    tables: Type.Optional(Type.Array(Type.String(), { description: "Tables to use" })),
    dialect: Type.Optional(Type.String({ description: "SQL dialect: postgres, snowflake, bigquery, etc." })),
    constraints: Type.Optional(Type.Object({
      maxRows: Type.Optional(Type.Number({ description: "Add LIMIT clause" })),
      timeRange: Type.Optional(Type.String({ description: "e.g., 'last 12 months'" })),
    })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // This tool primarily structures the LLM's SQL generation.
    // 1. Fetch relevant table schemas if tables specified
    // 2. Return structured SQL with comments
    // Return: { sql, notes, assumptions }
  },
};
```

### Tool 6: `data_validate`

**Purpose**: Validate SQL query for safety and best practices.

```typescript
const dataValidate: AgentTool = {
  name: "data_validate",
  label: "Validate SQL",
  description: "Check SQL query for safety issues and best practices.",
  parameters: Type.Object({
    sql: Type.String({ description: "SQL query to validate" }),
    rules: Type.Optional(Type.Object({
      requireLimit: Type.Optional(Type.Boolean({ default: true })),
      forbidWrite: Type.Optional(Type.Boolean({ default: true })),
      maxEstimatedRows: Type.Optional(Type.Number()),
    })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Parse SQL (basic regex checks for INSERT/UPDATE/DELETE/DROP)
    // 2. Check for LIMIT clause
    // 3. Optionally run EXPLAIN to estimate cost
    // Return: { ok: boolean, issues: [{ severity, message, fixHint }] }
  },
};
```

### Tool 7: `data_execute`

**Purpose**: Execute SQL and materialize results. **This is the core workhorse tool.**

```typescript
const dataExecute: AgentTool = {
  name: "data_execute",
  label: "Execute SQL",
  description: "Run SQL query and materialize results to disk. Returns resultId for further analysis. Never returns raw rows to context.",
  parameters: Type.Object({
    sql: Type.String({ description: "SQL query to execute" }),
    maxRows: Type.Optional(Type.Number({ default: 10000, description: "Maximum rows to fetch" })),
    purpose: Type.Optional(Type.Union([
      Type.Literal("explore"),
      Type.Literal("analysis"),
      Type.Literal("viz"),
      Type.Literal("export"),
    ])),
    tags: Type.Optional(Type.Array(Type.String())),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Emit progress: "connecting"
    onUpdate?.({ content: [{ type: "text", text: "Connecting..." }], details: { phase: "connecting" } });
    
    // 2. Execute via DB CLI with streaming output
    // 3. Emit progress: "running", "fetching" with row counts
    onUpdate?.({ content: [{ type: "text", text: `Fetching: ${rowCount} rows...` }], details: { phase: "fetching", rows: rowCount } });
    
    // 4. Materialize to parquet/arrow in workspace
    // 5. Save metadata (sql, timestamp, columns, rowCount)
    // 6. Generate preview (first 5 rows)
    
    // Return summary + preview, NOT raw data
    // Return: { 
    //   resultId, 
    //   rowCount, 
    //   columns: [...], 
    //   preview: { columns, rows: first5Rows },
    //   warnings?: [...] 
    // }
  },
};
```

**Critical Implementation Detail**: The execute tool must:
1. Stream progress updates during execution
2. Materialize results to disk (parquet preferred, CSV fallback)
3. Return only metadata + small preview
4. Respect AbortSignal for cancellation

### Tool 8: `data_preview`

**Purpose**: Get a preview of a result.

```typescript
const dataPreview: AgentTool = {
  name: "data_preview",
  label: "Preview Result",
  description: "Get rows from a previously executed query result.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID from data_execute" }),
    limit: Type.Optional(Type.Number({ default: 20, description: "Number of rows (max 100)" })),
    offset: Type.Optional(Type.Number({ default: 0 })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Load result from workspace (parquet/CSV)
    // 2. Read rows with offset/limit
    // 3. Format as table
    // Return: { columns, rows, totalRows, hasMore }
  },
};
```

### Tool 9: `data_profile`

**Purpose**: Get statistical profile of result columns.

```typescript
const dataProfile: AgentTool = {
  name: "data_profile",
  label: "Profile Result",
  description: "Get statistical summary of result columns: nulls, distinct values, distributions.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID from data_execute" }),
    columns: Type.Optional(Type.Array(Type.String(), { description: "Specific columns to profile" })),
    sampleRows: Type.Optional(Type.Number({ default: 10000, description: "Rows to sample for profiling" })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Load result
    // 2. For each column: compute nulls, distinct estimate, min/max, top values
    // 3. For numeric: mean, std, quartiles
    // Return: { profiles: [{ column, type, nullPct, distinctEst, topValues?, numericStats? }] }
  },
};
```

### Tool 10: `data_aggregate`

**Purpose**: Run aggregations on an existing result.

```typescript
const dataAggregate: AgentTool = {
  name: "data_aggregate",
  label: "Aggregate Result",
  description: "Compute aggregations on a result without re-querying the database.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID from data_execute" }),
    groupBy: Type.Optional(Type.Array(Type.String(), { description: "Columns to group by" })),
    metrics: Type.Array(Type.Object({
      op: Type.Union([
        Type.Literal("count"),
        Type.Literal("sum"),
        Type.Literal("avg"),
        Type.Literal("min"),
        Type.Literal("max"),
        Type.Literal("distinct"),
      ]),
      column: Type.Optional(Type.String()),
      as: Type.String({ description: "Output column name" }),
    })),
    where: Type.Optional(Type.String({ description: "Filter expression" })),
    orderBy: Type.Optional(Type.String()),
    limit: Type.Optional(Type.Number({ default: 100 })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Load result
    // 2. Apply filter if specified
    // 3. Compute aggregates (use DuckDB CLI or in-memory if small)
    // 4. Save as new result
    // Return: { resultId: newResultId, rowCount, preview }
  },
};
```

### Tool 11: `data_export`

**Purpose**: Export result to file.

```typescript
const dataExport: AgentTool = {
  name: "data_export",
  label: "Export Result",
  description: "Export a result to CSV, Parquet, or Excel file.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID from data_execute" }),
    format: Type.Union([
      Type.Literal("csv"),
      Type.Literal("parquet"),
      Type.Literal("xlsx"),
    ]),
    path: Type.Optional(Type.String({ description: "Output path. Defaults to artifacts/" })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Load result
    // 2. Convert to specified format
    // 3. Write to path
    // Return: { artifactId, path, rowCount, bytes }
  },
};
```

### Tool 12: `data_viz_suggest`

**Purpose**: Generate a Vega-Lite spec for a result.

```typescript
const dataVizSuggest: AgentTool = {
  name: "data_viz_suggest",
  label: "Suggest Visualization",
  description: "Generate a Vega-Lite chart specification based on the data and goal.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID to visualize" }),
    goal: Type.String({ description: "What to show (e.g., 'trend over time', 'compare categories')" }),
    chartType: Type.Optional(Type.Union([
      Type.Literal("bar"),
      Type.Literal("line"),
      Type.Literal("scatter"),
      Type.Literal("area"),
      Type.Literal("pie"),
      Type.Literal("heatmap"),
    ])),
    constraints: Type.Optional(Type.Object({
      maxCategories: Type.Optional(Type.Number({ default: 20 })),
      colorScheme: Type.Optional(Type.String()),
    })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Load result schema
    // 2. Analyze column types (temporal, quantitative, nominal)
    // 3. Generate appropriate Vega-Lite spec
    // Return: { spec: VegaLiteSpec, rationale, dataMapping }
  },
};
```

### Tool 13: `data_viz_inline`

**Purpose**: Render chart as inline terminal image.

```typescript
const dataVizInline: AgentTool = {
  name: "data_viz_inline",
  label: "Render Inline Chart",
  description: "Render a Vega-Lite chart as PNG displayed inline in the terminal.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID with data to visualize" }),
    spec: Type.String({ description: "Vega-Lite JSON specification" }),
    width: Type.Optional(Type.Number({ default: 600 })),
    height: Type.Optional(Type.Number({ default: 400 })),
    theme: Type.Optional(Type.Union([Type.Literal("light"), Type.Literal("dark")])),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation (follow pi-charts pattern):
    // 1. Load result data
    // 2. Inject data into spec
    // 3. Call Python script with Altair to render PNG
    // 4. Return image for inline display
    // Return: { artifactId, mime: "image/png", inlineImage: true, path }
  },
};
```

**Python Script** (`python/render_vega.py`):
```python
#!/usr/bin/env python3
import sys
import json
import altair as alt
from vl_convert import vegalite_to_png

spec = json.load(sys.stdin)
png_data = vegalite_to_png(spec)
sys.stdout.buffer.write(png_data)
```

### Tool 14: `data_viz_html`

**Purpose**: Render chart as HTML file and open in browser.

```typescript
const dataVizHtml: AgentTool = {
  name: "data_viz_html",
  label: "Render HTML Chart",
  description: "Render a Vega-Lite chart as an interactive HTML file and open in browser.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID with data to visualize" }),
    spec: Type.String({ description: "Vega-Lite JSON specification" }),
    title: Type.Optional(Type.String()),
    open: Type.Optional(Type.Boolean({ default: true, description: "Open in browser" })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation (follow visual-explainer pattern):
    // 1. Load result data
    // 2. Generate HTML with Vega-Embed
    // 3. Write to artifacts/
    // 4. Open in browser if requested
    // Return: { artifactId, path, opened }
  },
};
```

### Tool 15: `data_dashboard`

**Purpose**: Build multi-chart HTML dashboard.

```typescript
const dataDashboard: AgentTool = {
  name: "data_dashboard",
  label: "Build Dashboard",
  description: "Create an interactive HTML dashboard with multiple charts.",
  parameters: Type.Object({
    title: Type.String({ description: "Dashboard title" }),
    items: Type.Array(Type.Object({
      title: Type.String(),
      resultId: Type.String(),
      spec: Type.String({ description: "Vega-Lite spec JSON" }),
    })),
    layout: Type.Optional(Type.Union([
      Type.Literal("grid"),
      Type.Literal("tabs"),
      Type.Literal("vertical"),
    ])),
    open: Type.Optional(Type.Boolean({ default: true })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Generate HTML with all charts
    // 2. Add layout CSS
    // 3. Include filter controls if applicable
    // 4. Write to artifacts/, open in browser
    // Return: { artifactId, path, chartCount }
  },
};
```

### Tool 16: `data_nb_create`

**Purpose**: Create a new notebook.

```typescript
const dataNbCreate: AgentTool = {
  name: "data_nb_create",
  label: "Create Notebook",
  description: "Create a new data analysis notebook.",
  parameters: Type.Object({
    title: Type.String({ description: "Notebook title" }),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Generate notebookId
    // 2. Create notebook JSON file
    // Return: { notebookId, title, path }
  },
};
```

### Tool 17: `data_nb_add_cell`

**Purpose**: Add a cell to a notebook.

```typescript
const dataNbAddCell: AgentTool = {
  name: "data_nb_add_cell",
  label: "Add Notebook Cell",
  description: "Add a SQL, markdown, or visualization cell to a notebook.",
  parameters: Type.Object({
    notebookId: Type.String(),
    kind: Type.Union([
      Type.Literal("sql"),
      Type.Literal("markdown"),
      Type.Literal("viz"),
    ]),
    content: Type.String({ description: "Cell content (SQL query, markdown, or viz spec)" }),
    afterCellId: Type.Optional(Type.String({ description: "Insert after this cell" })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Load notebook
    // 2. Create cell with new cellId
    // 3. Insert at position
    // 4. Save notebook
    // Return: { cellId, position }
  },
};
```

### Tool 18: `data_nb_run_cell`

**Purpose**: Execute a notebook cell.

```typescript
const dataNbRunCell: AgentTool = {
  name: "data_nb_run_cell",
  label: "Run Notebook Cell",
  description: "Execute a notebook cell and store the output.",
  parameters: Type.Object({
    notebookId: Type.String(),
    cellId: Type.String(),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Load notebook and cell
    // 2. Based on cell.kind:
    //    - sql: call data_execute, store resultId
    //    - markdown: render as text
    //    - viz: call data_viz_inline or data_viz_html
    // 3. Update cell.output
    // 4. Increment cell.version
    // 5. Save revision to revisions/
    // Return: { output: { type, resultId?, text?, artifactId? } }
  },
};
```

### Tool 19: `data_nb_export`

**Purpose**: Export notebook as HTML.

```typescript
const dataNbExport: AgentTool = {
  name: "data_nb_export",
  label: "Export Notebook",
  description: "Export notebook as a self-contained HTML file.",
  parameters: Type.Object({
    notebookId: Type.String(),
    includeData: Type.Optional(Type.Boolean({ default: true, description: "Embed result previews" })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Load notebook with all cells and outputs
    // 2. Render to HTML using template
    // 3. Embed charts, tables, markdown
    // 4. Write to artifacts/
    // Return: { artifactId, path }
  },
};
```

### Tool 20: `data_plan` (RLM)

**Purpose**: Decompose a complex question into analysis steps.

```typescript
const dataPlan: AgentTool = {
  name: "data_plan",
  label: "Plan Analysis",
  description: "Decompose a complex data question into concrete analysis steps. Use for questions that require multiple queries.",
  parameters: Type.Object({
    question: Type.String({ description: "The question to answer" }),
    tables: Type.Optional(Type.Array(Type.String(), { description: "Available tables" })),
    resultIds: Type.Optional(Type.Array(Type.String(), { description: "Existing results to build on" })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // This is primarily a structured prompt that guides the LLM to:
    // 1. Break question into 3-8 subquestions
    // 2. Identify which require SQL vs aggregation of existing results
    // 3. Order by dependency
    // Return: { plan: [{ stepId, action, tool, params, expectedOutput }] }
  },
};
```

### Tool 21: `data_summarize` (RLM)

**Purpose**: Summarize a large result into compact findings.

```typescript
const dataSummarize: AgentTool = {
  name: "data_summarize",
  label: "Summarize Result",
  description: "Generate a compact summary of a result's key findings. Use to avoid loading large datasets into context.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result to summarize" }),
    budgetTokens: Type.Optional(Type.Number({ default: 500, description: "Target summary length" })),
    strategy: Type.Optional(Type.Union([
      Type.Literal("profile+sample"),
      Type.Literal("topk"),
      Type.Literal("time-series"),
    ])),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // 1. Get profile of result
    // 2. Get sample rows
    // 3. Identify patterns, outliers, trends
    // 4. Format as concise summary
    // Return: { summary, keyFindings: [...], suggestedNextQueries: [...] }
  },
};
```

### Tool 22: `data_answer` (RLM)

**Purpose**: Synthesize final answer from evidence.

```typescript
const dataAnswer: AgentTool = {
  name: "data_answer",
  label: "Answer Question",
  description: "Synthesize a final answer from multiple result summaries. Use after gathering evidence with data_summarize.",
  parameters: Type.Object({
    question: Type.String({ description: "The original question" }),
    evidence: Type.Array(Type.Object({
      resultId: Type.String(),
      summary: Type.String({ description: "Summary of this result" }),
      note: Type.Optional(Type.String({ description: "How this relates to the question" })),
    })),
  }),
  execute: async (toolCallId, params, signal, onUpdate) => {
    // Implementation:
    // This structures the final synthesis step
    // Return: { answer, confidence, citations: [{ resultId, snippet }] }
  },
};
```

---

## Slash Commands (Prompt Templates)

Create these in `prompts/` directory:

### `/analyze` - prompts/analyze.md

```markdown
---
name: analyze
description: Answer data questions from quick lookups to full analyses
---

You are a data analyst. Answer the user's question using the data tools.

**Workflow:**
1. Search catalog to find relevant tables: `data_search`
2. Describe tables to understand schema: `data_describe`
3. Write and validate SQL: `data_write_sql`, `data_validate`
4. Execute query: `data_execute`
5. If needed, profile or aggregate results: `data_profile`, `data_aggregate`
6. Visualize if helpful: `data_viz_inline`
7. Provide clear answer with evidence

**Guidelines:**
- Always validate SQL before executing
- Use LIMIT for exploration queries
- Never dump large result sets—use profile/summary tools
- Cite result IDs in your answer

Question: $1
```

### `/explore` - prompts/explore.md

```markdown
---
name: explore
description: Profile and explore a dataset
---

Explore the specified table or dataset to understand its shape, quality, and patterns.

**Workflow:**
1. Describe the table: `data_describe` with stats
2. Execute a sample query: `data_execute` with LIMIT 1000
3. Profile the result: `data_profile`
4. Identify patterns, nulls, outliers
5. Suggest interesting analyses

Target: $1
```

### `/write-query` - prompts/write-query.md

```markdown
---
name: write-query
description: Generate optimized SQL
---

Generate an optimized SQL query for the user's request.

**Workflow:**
1. Search for relevant tables: `data_search`
2. Describe tables: `data_describe`
3. Generate SQL: `data_write_sql`
4. Validate: `data_validate`
5. Explain query structure and assumptions

Request: $1
```

### `/viz` - prompts/viz.md

```markdown
---
name: viz
description: Create a visualization
---

Create a visualization for the specified data or question.

**Workflow:**
1. If no resultId provided, execute necessary query
2. Suggest appropriate chart: `data_viz_suggest`
3. Render inline: `data_viz_inline`
4. Explain what the chart shows

Request: $1
```

### `/dashboard` - prompts/dashboard.md

```markdown
---
name: dashboard
description: Build a multi-chart dashboard
---

Build an interactive HTML dashboard for the specified topic.

**Workflow:**
1. Plan 3-5 charts that tell a story
2. Execute queries for each: `data_execute`
3. Generate specs: `data_viz_suggest`
4. Build dashboard: `data_dashboard`
5. Summarize key insights

Topic: $1
```

---

## Workspace State Structure

```
.pi-data/
├── session.json                    # Workspace metadata
│   {
│     "sessionId": "abc123",
│     "profile": "production-dwh",
│     "createdAt": 1708300000000,
│     "description": "Q4 revenue analysis"
│   }
│
├── results/
│   ├── r_a7f3c2.parquet            # Materialized query result
│   ├── r_a7f3c2.json               # Result metadata
│   │   {
│   │     "resultId": "r_a7f3c2",
│   │     "sql": "SELECT ...",
│   │     "rowCount": 12847,
│   │     "columns": [...],
│   │     "createdAt": 1708300100000,
│   │     "tags": ["revenue", "monthly"]
│   │   }
│   └── r_b8e4d1.parquet
│
├── artifacts/
│   ├── chart_001.png               # Rendered chart
│   ├── chart_001.json              # Artifact metadata
│   ├── dashboard_001.html          # HTML dashboard
│   └── export_001.csv              # Exported data
│
└── notebooks/
    ├── analysis_01.json            # Notebook definition
    │   {
    │     "notebookId": "analysis_01",
    │     "title": "Revenue Analysis",
    │     "cells": [
    │       {
    │         "cellId": "c1",
    │         "kind": "markdown",
    │         "content": "# Revenue Analysis",
    │         "version": 1
    │       },
    │       {
    │         "cellId": "c2",
    │         "kind": "sql",
    │         "content": "SELECT ...",
    │         "output": { "type": "result", "resultId": "r_a7f3c2" },
    │         "version": 3
    │       }
    │     ]
    │   }
    │
    └── analysis_01/
        └── revisions/
            ├── 1708300100000.json  # Auto-saved version
            └── 1708300200000.json
```

---

## Extension File Structure

```
pi-data/
├── package.json
├── README.md
├── SKILL.md                           # Agent skill instructions
├── index.ts                           # Extension entry point
│
├── src/
│   ├── tools/
│   │   ├── index.ts                   # Export all tools
│   │   ├── session.ts                 # data_session
│   │   ├── catalog.ts                 # data_search, data_list_tables, data_describe
│   │   ├── sql.ts                     # data_write_sql, data_validate, data_execute
│   │   ├── results.ts                 # data_preview, data_profile, data_aggregate, data_export
│   │   ├── viz.ts                     # data_viz_suggest, data_viz_inline, data_viz_html, data_dashboard
│   │   ├── notebook.ts                # data_nb_create, data_nb_add_cell, data_nb_run_cell, data_nb_export
│   │   └── rlm.ts                     # data_plan, data_summarize, data_answer
│   │
│   ├── cli/
│   │   ├── db-runner.ts               # Spawn wrapper for database CLI
│   │   └── parser.ts                  # Parse CLI output (CSV, JSON, etc.)
│   │
│   ├── state/
│   │   ├── workspace.ts               # Session persistence, paths
│   │   ├── result-store.ts            # Save/load results, metadata
│   │   ├── artifact-store.ts          # Manage artifacts
│   │   └── notebook-store.ts          # Notebook CRUD, versioning
│   │
│   ├── ui/
│   │   ├── clarify-overlay.ts         # TUI for metric clarification
│   │   ├── table-view.ts              # Terminal table formatting
│   │   └── progress.ts                # Progress display helpers
│   │
│   ├── render/
│   │   ├── vega-inline.ts             # PNG rendering via Python
│   │   ├── html-chart.ts              # HTML chart generation
│   │   ├── html-dashboard.ts          # Dashboard template
│   │   └── html-notebook.ts           # Notebook export
│   │
│   └── utils/
│       ├── id.ts                      # ID generation (resultId, etc.)
│       ├── format.ts                  # Number/date formatting
│       └── types.ts                   # Shared TypeScript types
│
├── templates/
│   ├── notebook.html                  # Notebook HTML template
│   ├── dashboard.html                 # Dashboard HTML template
│   └── chart.html                     # Single chart HTML template
│
├── prompts/
│   ├── analyze.md
│   ├── explore.md
│   ├── write-query.md
│   ├── viz.md
│   └── dashboard.md
│
├── python/
│   └── render_vega.py                 # Altair rendering script
│
└── test/
    ├── tools/
    │   ├── session.test.ts
    │   ├── catalog.test.ts
    │   ├── sql.test.ts
    │   ├── results.test.ts
    │   └── viz.test.ts
    └── integration/
        └── workflow.test.ts
```

---

## Implementation Plan

### Phase 1: Foundation (Day 1)

**Goal**: Basic tool structure, session management, SQL execution.

1. **Setup**
   - [ ] Initialize npm package with TypeScript
   - [ ] Add dependencies: `@sinclair/typebox`, `@mariozechner/pi-agent-core`
   - [ ] Create extension entry point (`index.ts`)

2. **State Management**
   - [ ] Implement `workspace.ts` - create/load workspace
   - [ ] Implement `result-store.ts` - save/load parquet, metadata

3. **Core Tools**
   - [ ] `data_session` - create/load workspace
   - [ ] `data_execute` - run SQL via CLI, materialize results
   - [ ] `data_preview` - read rows from result

4. **CLI Integration**
   - [ ] Implement `db-runner.ts` - spawn database CLI
   - [ ] Handle streaming output parsing
   - [ ] Support cancellation via AbortSignal

**Deliverable**: Can execute SQL and view results.

### Phase 2: Catalog & Discovery (Day 1-2)

**Goal**: Search and explore database schema.

1. **Catalog Tools**
   - [ ] `data_search` - keyword search via information_schema
   - [ ] `data_list_tables` - list tables in schema
   - [ ] `data_describe` - table schema and stats

2. **SQL Authoring**
   - [ ] `data_write_sql` - structured SQL generation
   - [ ] `data_validate` - safety checks

**Deliverable**: Can discover tables and write validated SQL.

### Phase 3: Result Analysis (Day 2)

**Goal**: Profile and aggregate results without context overflow.

1. **Analysis Tools**
   - [ ] `data_profile` - column statistics
   - [ ] `data_aggregate` - GROUP BY on results
   - [ ] `data_export` - CSV/Parquet export

2. **Table Formatting**
   - [ ] Implement `table-view.ts` - nice terminal tables
   - [ ] Handle wide tables, long values

**Deliverable**: Can analyze results efficiently.

### Phase 4: Visualization (Day 2-3)

**Goal**: Inline charts and HTML output.

1. **Chart Generation**
   - [ ] `data_viz_suggest` - generate Vega-Lite specs
   - [ ] `data_viz_inline` - render PNG via Python/Altair
   - [ ] `data_viz_html` - render HTML, open browser

2. **Dashboard**
   - [ ] `data_dashboard` - multi-chart HTML
   - [ ] Create HTML templates

3. **Python Integration**
   - [ ] Create `render_vega.py` script
   - [ ] Handle Python availability detection
   - [ ] Fallback to HTML-only when Python missing

**Deliverable**: Can create inline and HTML charts.

### Phase 5: Notebooks (Day 3)

**Goal**: Cell-based workflow with versioning.

1. **Notebook Tools**
   - [ ] `data_nb_create` - create notebook
   - [ ] `data_nb_add_cell` - add cells
   - [ ] `data_nb_run_cell` - execute cells
   - [ ] `data_nb_export` - export HTML

2. **Versioning**
   - [ ] Save revisions on cell update
   - [ ] Enable diff viewing

**Deliverable**: Can create and export notebooks.

### Phase 6: RLM & Polish (Day 3-4)

**Goal**: Long-context analysis, clarification UI, slash commands.

1. **RLM Tools**
   - [ ] `data_plan` - decompose questions
   - [ ] `data_summarize` - compact summaries
   - [ ] `data_answer` - synthesize with citations

2. **Clarification Overlay**
   - [ ] Implement `clarify-overlay.ts`
   - [ ] Integrate with ambiguous queries

3. **Slash Commands**
   - [ ] Create prompt templates
   - [ ] Test end-to-end workflows

**Deliverable**: Full-featured data analysis extension.

---

## Key Implementation Details

### Database CLI Integration (DuckDB)

The extension uses **DuckDB CLI** as the default database engine. DuckDB is ideal because:
- Zero setup (no server, embedded)
- Reads CSV/Parquet/JSON files directly
- Standard SQL syntax that transfers to other databases
- Fast columnar analytics engine
- Clean CLI with multiple output formats

**DuckDB CLI Usage:**
```bash
# Query a CSV file directly
duckdb -csv -c "SELECT * FROM 'data/sales.csv' LIMIT 10"

# Query parquet (what we use for result storage)
duckdb -json -c "SELECT * FROM 'results/r_abc.parquet'"

# Query with a persistent database
duckdb mydb.duckdb -csv -c "SELECT * FROM orders"

# Information schema for catalog discovery
duckdb mydb.duckdb -json -c "SELECT table_name FROM information_schema.tables"

# Output formats: -csv, -json, -markdown, -table, -line
```

**Implementation:**

```typescript
// src/cli/db-runner.ts

interface DbRunnerConfig {
  cliPath: string;           // Default: "duckdb"
  database?: string;         // Optional: path to .duckdb file, or ":memory:"
}

interface ExecuteOptions {
  sql: string;
  maxRows?: number;
  format?: "csv" | "json";
  signal?: AbortSignal;
}

interface ExecuteResult {
  columns: string[];
  rows: any[][];
  rowCount: number;
}

class DbRunner {
  constructor(config: DbRunnerConfig) { }
  
  async execute(options: ExecuteOptions): Promise<ExecuteResult> {
    const { sql, format = "json", signal } = options;
    const args = [
      this.config.database || ":memory:",
      `-${format}`,
      "-c",
      sql,
    ];
    
    const proc = spawn(this.config.cliPath || "duckdb", args, { signal });
    
    // Collect stdout
    const chunks: Buffer[] = [];
    for await (const chunk of proc.stdout) {
      chunks.push(chunk);
    }
    
    const output = Buffer.concat(chunks).toString();
    
    // Parse based on format
    if (format === "json") {
      const rows = JSON.parse(output);
      const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
      return { columns, rows, rowCount: rows.length };
    } else {
      // Parse CSV
      return parseCSV(output);
    }
  }
  
  // For streaming large results, write to parquet directly
  async executeToParquet(sql: string, outputPath: string): Promise<{ rowCount: number }> {
    const wrappedSql = `COPY (${sql}) TO '${outputPath}' (FORMAT PARQUET)`;
    await this.execute({ sql: wrappedSql, format: "csv" });
    
    // Get row count
    const countResult = await this.execute({
      sql: `SELECT COUNT(*) as count FROM '${outputPath}'`,
      format: "json",
    });
    
    return { rowCount: countResult.rows[0].count };
  }
}
```

### Inline Image Rendering

Follow the pi-charts pattern for terminal inline images:

```typescript
// src/render/vega-inline.ts

import { spawn } from "child_process";
import { join } from "path";

export async function renderVegaInline(
  spec: object,
  data: any[],
  options: { width?: number; height?: number }
): Promise<Buffer> {
  // Inject data into spec
  const fullSpec = { ...spec, data: { values: data } };
  
  // Call Python script
  const pythonPath = process.env.PYTHON_PATH || "python3";
  const scriptPath = join(__dirname, "../../python/render_vega.py");
  
  const proc = spawn(pythonPath, [scriptPath], {
    stdio: ["pipe", "pipe", "pipe"],
  });
  
  proc.stdin.write(JSON.stringify(fullSpec));
  proc.stdin.end();
  
  const chunks: Buffer[] = [];
  for await (const chunk of proc.stdout) {
    chunks.push(chunk);
  }
  
  return Buffer.concat(chunks);
}
```

### Streaming Progress Updates

All long-running tools should emit progress:

```typescript
// Example in data_execute

execute: async (toolCallId, params, signal, onUpdate) => {
  const phases = ["connecting", "running", "fetching", "materializing", "done"];
  
  onUpdate?.({
    content: [{ type: "text", text: "Connecting to database..." }],
    details: { phase: "connecting" },
  });
  
  const runner = new DbRunner(config);
  
  for await (const event of runner.execute({ sql: params.sql, signal })) {
    if (event.type === "progress") {
      onUpdate?.({
        content: [{ type: "text", text: `Fetching: ${event.rows} rows (${event.elapsed}s)` }],
        details: { phase: "fetching", rows: event.rows, elapsed: event.elapsed },
      });
    }
  }
  
  // ... rest of execution
}
```

### Clarification Overlay

For ambiguous queries, open a TUI overlay (follow pi-subagents pattern):

```typescript
// src/ui/clarify-overlay.ts

import type { ExtensionContext } from "@mariozechner/pi-agent-core";

interface ClarifyOptions {
  title: string;
  fields: Array<{
    name: string;
    label: string;
    options: Array<{ value: string; label: string }>;
    default?: string;
  }>;
}

export async function showClarifyOverlay(
  ctx: ExtensionContext,
  options: ClarifyOptions
): Promise<Record<string, string> | null> {
  // Use ctx.ui.custom() to render overlay
  // Return selected values or null if cancelled
}
```

---

## Testing Strategy

### Test Data Setup with DuckDB

DuckDB makes testing easy—no database server needed. Create sample data:

```bash
# Create test database with sample data
mkdir -p test/fixtures
cat > test/fixtures/setup.sql << 'EOF'
-- Create sample tables
CREATE TABLE customers (
  id INTEGER PRIMARY KEY,
  name VARCHAR,
  region VARCHAR,
  created_at DATE
);

CREATE TABLE orders (
  id INTEGER PRIMARY KEY,
  customer_id INTEGER,
  product VARCHAR,
  amount DECIMAL(10,2),
  created_at TIMESTAMP
);

CREATE TABLE products (
  id INTEGER PRIMARY KEY,
  name VARCHAR,
  category VARCHAR,
  price DECIMAL(10,2)
);

-- Insert sample data
INSERT INTO customers VALUES
  (1, 'Acme Corp', 'North America', '2024-01-15'),
  (2, 'Globex', 'Europe', '2024-02-20'),
  (3, 'Initech', 'North America', '2024-03-10');

INSERT INTO products VALUES
  (1, 'Widget Pro', 'Enterprise', 999.00),
  (2, 'Widget Basic', 'SMB', 99.00),
  (3, 'Widget Starter', 'Startup', 29.00);

INSERT INTO orders
SELECT 
  row_number() OVER () as id,
  (random() * 3 + 1)::INT as customer_id,
  CASE (random() * 3)::INT 
    WHEN 0 THEN 'Widget Pro'
    WHEN 1 THEN 'Widget Basic'
    ELSE 'Widget Starter'
  END as product,
  (random() * 1000 + 10)::DECIMAL(10,2) as amount,
  '2024-01-01'::TIMESTAMP + INTERVAL (random() * 365) DAY as created_at
FROM generate_series(1, 1000);
EOF

# Create the test database
duckdb test/fixtures/test.duckdb < test/fixtures/setup.sql
```

**Also support CSV fixtures:**
```bash
# Create CSV test files for file-based queries
cat > test/fixtures/sales.csv << 'EOF'
date,product,region,revenue
2024-01-01,Enterprise,NA,125000
2024-01-01,SMB,NA,45000
2024-01-01,Enterprise,EMEA,98000
2024-02-01,Enterprise,NA,132000
2024-02-01,SMB,NA,48000
EOF
```

### Unit Tests

- Test each tool in isolation with test DuckDB database
- Test state management (workspace, results, notebooks)
- Test rendering (Vega specs, HTML templates)

```typescript
// test/tools/sql.test.ts
import { describe, it, expect, beforeAll } from "vitest";
import { dataExecute } from "../src/tools/sql";

describe("data_execute", () => {
  beforeAll(() => {
    // Point to test database
    process.env.PI_DATA_DATABASE = "test/fixtures/test.duckdb";
  });

  it("executes SQL and returns resultId", async () => {
    const result = await dataExecute.execute(
      "test-1",
      { sql: "SELECT * FROM customers LIMIT 5" },
      new AbortController().signal,
      () => {}
    );
    
    expect(result.details.resultId).toMatch(/^r_/);
    expect(result.details.rowCount).toBe(5);
  });

  it("streams progress updates", async () => {
    const updates: any[] = [];
    
    await dataExecute.execute(
      "test-2",
      { sql: "SELECT * FROM orders" },
      new AbortController().signal,
      (update) => updates.push(update)
    );
    
    expect(updates.some(u => u.details?.phase === "fetching")).toBe(true);
  });
});
```

### Integration Tests

- End-to-end workflow: search → query → visualize
- Notebook workflow: create → add cells → run → export
- RLM workflow: plan → execute steps → answer

```typescript
// test/integration/workflow.test.ts
import { describe, it, expect } from "vitest";

describe("analysis workflow", () => {
  it("completes full discovery → query → viz flow", async () => {
    // 1. Search catalog
    const searchResult = await dataSearch.execute("test", { query: "revenue" }, ...);
    expect(searchResult.details.matches.length).toBeGreaterThan(0);
    
    // 2. Execute query
    const execResult = await dataExecute.execute("test", {
      sql: "SELECT product, SUM(amount) as revenue FROM orders GROUP BY product"
    }, ...);
    const resultId = execResult.details.resultId;
    
    // 3. Generate viz
    const vizResult = await dataVizSuggest.execute("test", {
      resultId,
      goal: "compare product revenue"
    }, ...);
    expect(vizResult.details.spec).toContain("bar");
  });
});
```

### Manual Testing

1. Install extension: `pi install ./pi-data`
2. Create test database: `duckdb test.duckdb < fixtures/setup.sql`
3. Test slash commands with test data
4. Verify inline chart rendering in supported terminals
5. Verify HTML dashboard opens correctly
6. Test cancellation mid-query

---

## Configuration

Support these config options in `~/.pi/agent/pi-data.json`:

```json
{
  "duckdbPath": "duckdb",
  "database": null,
  "workspacePath": ".pi-data",
  "pythonPath": "python3",
  "maxResultRows": 10000,
  "inlineChartWidth": 600,
  "inlineChartHeight": 400,
  "autoOpenHtml": true
}
```

| Option | Default | Description |
|--------|---------|-------------|
| `duckdbPath` | `"duckdb"` | Path to DuckDB CLI binary |
| `database` | `null` | Path to .duckdb file, or `null` for in-memory / file queries |
| `workspacePath` | `".pi-data"` | Directory for session state, results, artifacts |
| `pythonPath` | `"python3"` | Python binary for chart rendering |
| `maxResultRows` | `10000` | Default max rows to materialize |
| `inlineChartWidth` | `600` | Default chart width in pixels |
| `inlineChartHeight` | `400` | Default chart height in pixels |
| `autoOpenHtml` | `true` | Auto-open HTML dashboards in browser |

**Environment variables (override config):**
- `PI_DATA_DUCKDB_PATH` - DuckDB CLI path
- `PI_DATA_DATABASE` - Default database path
- `PI_DATA_WORKSPACE` - Workspace directory

---

## Error Handling

### Database Errors
- Parse error messages from CLI
- Provide actionable suggestions (e.g., "Column not found. Did you mean...?")

### Result Too Large
- If result exceeds maxRows, warn and suggest using aggregates
- Never load unbounded results into context

### Python Not Available
- Detect Python/Altair availability on first viz call
- Fallback to HTML-only rendering with clear message

### Cancelled Queries
- Respect AbortSignal throughout
- Clean up partial results
- Provide helpful "query cancelled" message

---

## Security Considerations

1. **No credential storage**: Only store profile names, not passwords
2. **Query validation**: Default `forbidWrite: true` to prevent mutations
3. **Path traversal**: Validate all file paths stay within workspace
4. **Injection prevention**: Never interpolate user input into SQL without validation

---

## Dependencies

```json
{
  "dependencies": {
    "@sinclair/typebox": "^0.32.0"
  },
  "peerDependencies": {
    "@mariozechner/pi-agent-core": "^0.52.0"
  }
}
```

**External dependencies:**

DuckDB CLI (required):
```bash
# macOS
brew install duckdb

# Linux
curl -LO https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
sudo mv duckdb /usr/local/bin/
```

Python dependencies (optional, for inline charts):
- `altair>=5.0.0`
- `vl-convert-python>=1.0.0`
- `pandas>=2.0.0`

---

## Success Criteria

The extension is complete when:

1. ✅ Can discover tables and columns via search
2. ✅ Can generate and validate SQL from natural language
3. ✅ Can execute queries with streaming progress
4. ✅ Results are materialized to disk, never overflowing context
5. ✅ Can profile and aggregate results
6. ✅ Can render inline charts in terminal (when Python available)
7. ✅ Can generate HTML charts and dashboards
8. ✅ Can create and export notebooks
9. ✅ Slash commands work end-to-end
10. ✅ Large dataset analysis works via RLM pattern

---

## Future Enhancements (Out of Scope for MVP)

- Semantic layer / ontology for metric definitions
- Persistent Python kernel for interactive analysis
- Git-backed notebook versioning
- Collaborative editing
- Scheduled query execution
- Query result caching across sessions
