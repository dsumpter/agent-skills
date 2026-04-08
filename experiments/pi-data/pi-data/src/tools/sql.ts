import { Type } from "@sinclair/typebox";
import { mkdir } from "fs/promises";
import { dirname } from "path";

import type { AgentTool } from "@mariozechner/pi-agent-core";

import { createDefaultRunner } from "../cli/db-runner.js";
import { resultDataPath, saveResultMetadata } from "../state/result-store.js";
import { getConfig } from "../state/config.js";
import { createWorkspace, loadWorkspace, resolveWorkspacePath } from "../state/workspace.js";
import { createResultId } from "../utils/id.js";
import type { Column, ResultPreview, ResultSummary } from "../utils/types.js";

const DEFAULT_MAX_ROWS = 10000;

function applyLimit(sql: string, maxRows: number): string {
  const trimmed = sql.trim().replace(/;$/, "");
  if (/\blimit\b/i.test(trimmed)) {
    return trimmed;
  }
  return `SELECT * FROM (${trimmed}) LIMIT ${maxRows}`;
}

async function ensureWorkspace(pathOverride?: string): Promise<string> {
  const workspacePath = resolveWorkspacePath(pathOverride);
  try {
    await loadWorkspace(workspacePath);
  } catch {
    await createWorkspace(workspacePath);
  }
  return workspacePath;
}

async function fetchColumns(filePath: string): Promise<Column[]> {
  const runner = createDefaultRunner();
  const describe = await runner.execute({
    sql: `DESCRIBE SELECT * FROM '${filePath}'`,
    format: "json",
  });

  return describe.rows.map((row) => ({
    name: String(row.column_name ?? row.column ?? ""),
    type: String(row.column_type ?? row.type ?? "unknown"),
    nullable: true,
  }));
}

async function fetchPreview(filePath: string): Promise<ResultPreview> {
  const runner = createDefaultRunner();
  const preview = await runner.execute({
    sql: `SELECT * FROM '${filePath}' LIMIT 5`,
    format: "json",
  });

  return {
    columns: preview.columns,
    rows: preview.rows.map((row) => preview.columns.map((column) => (row[column] ?? null) as any)),
  };
}

export const dataExecute: AgentTool = {
  name: "data_execute",
  label: "Execute SQL",
  description: "Run SQL query and materialize results to disk. Returns resultId for further analysis.",
  parameters: Type.Object({
    sql: Type.String({ description: "SQL query to execute" }),
    maxRows: Type.Optional(Type.Number({ default: DEFAULT_MAX_ROWS, description: "Maximum rows to fetch" })),
    purpose: Type.Optional(Type.Union([
      Type.Literal("explore"),
      Type.Literal("analysis"),
      Type.Literal("viz"),
      Type.Literal("export"),
    ])),
    tags: Type.Optional(Type.Array(Type.String())),
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params, signal, onUpdate) => {
    onUpdate?.({ content: [{ type: "text", text: "Connecting..." }], details: { phase: "connecting" } });

    const workspacePath = await ensureWorkspace(params.workspacePath);
    const resultId = createResultId();
    const outputPath = resultDataPath(workspacePath, resultId);

    await mkdir(dirname(outputPath), { recursive: true });

    const runner = createDefaultRunner();
    const config = getConfig();
    const maxRows = params.maxRows ?? config.maxResultRows ?? DEFAULT_MAX_ROWS;
    const limitedSql = applyLimit(params.sql, maxRows);

    onUpdate?.({ content: [{ type: "text", text: "Running query..." }], details: { phase: "running" } });

    const rowCount = await runner.executeToParquet(limitedSql, outputPath, signal);

    onUpdate?.({
      content: [{ type: "text", text: `Materialized ${rowCount} rows.` }],
      details: { phase: "materializing", rows: rowCount },
    });

    const columns = await fetchColumns(outputPath);
    const preview = await fetchPreview(outputPath);

    const metadata = {
      resultId,
      sql: limitedSql,
      rowCount,
      columns,
      createdAt: Date.now(),
      tags: params.tags,
    };

    await saveResultMetadata(workspacePath, metadata);

    const summary: ResultSummary = {
      resultId,
      rowCount,
      columns,
      preview,
    };

    return {
      content: [{ type: "text", text: `Result ${resultId} stored with ${rowCount} rows.` }],
      details: summary,
    };
  },
};
