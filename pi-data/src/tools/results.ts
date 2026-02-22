import { Type } from "@sinclair/typebox";
import { copyFile, mkdir, stat } from "fs/promises";
import { dirname, isAbsolute, join, resolve } from "path";

import type { AgentTool } from "@mariozechner/pi-agent-core";

import { createDefaultRunner } from "../cli/db-runner.js";
import { saveArtifactMetadata } from "../state/artifact-store.js";
import { loadResultMetadata, resultDataPath, saveResultMetadata } from "../state/result-store.js";
import { createWorkspace, loadWorkspace, resolveWorkspacePath } from "../state/workspace.js";
import { createArtifactId, createResultId } from "../utils/id.js";
import type { Artifact, ResultPreview, ResultSummary } from "../utils/types.js";

const DEFAULT_AGGREGATE_LIMIT = 100;
const NUMERIC_TYPES = ["int", "decimal", "numeric", "double", "float", "real", "bigint", "smallint", "tinyint"];

function escapeIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function isNumericType(type: string): boolean {
  const lower = type.toLowerCase();
  return NUMERIC_TYPES.some((token) => lower.includes(token));
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

function ensureWorkspacePath(workspacePath: string, targetPath: string): void {
  const resolvedWorkspace = resolve(workspacePath);
  const resolvedTarget = resolve(targetPath);
  if (resolvedTarget === resolvedWorkspace) {
    return;
  }
  const prefix = resolvedWorkspace.endsWith("/") ? resolvedWorkspace : `${resolvedWorkspace}/`;
  if (!resolvedTarget.startsWith(prefix)) {
    throw new Error("Export path must stay within workspace.");
  }
}

async function fetchColumns(filePath: string) {
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

async function fetchPreview(filePath: string, limit = 5): Promise<ResultPreview> {
  const runner = createDefaultRunner();
  const preview = await runner.execute({
    sql: `SELECT * FROM '${filePath}' LIMIT ${limit}`,
    format: "json",
  });

  return {
    columns: preview.columns,
    rows: preview.rows.map((row) => preview.columns.map((column) => (row[column] ?? null) as any)),
  };
}

export const dataPreview: AgentTool = {
  name: "data_preview",
  label: "Preview Result",
  description: "Get rows from a previously executed query result.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID from data_execute" }),
    limit: Type.Optional(Type.Number({ default: 20, description: "Number of rows (max 100)" })),
    offset: Type.Optional(Type.Number({ default: 0 })),
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = resolveWorkspacePath(params.workspacePath);
    await loadWorkspace(workspacePath);

    const metadata = await loadResultMetadata(workspacePath, params.resultId);
    const limit = Math.min(params.limit ?? 20, 100);
    const offset = params.offset ?? 0;

    const runner = createDefaultRunner();
    const dataPath = resultDataPath(workspacePath, params.resultId);
    const query = `SELECT * FROM '${dataPath}' LIMIT ${limit} OFFSET ${offset}`;
    const result = await runner.execute({ sql: query, format: "json" });

    const preview: ResultPreview = {
      columns: result.columns,
      rows: result.rows.map((row) => result.columns.map((column) => (row[column] ?? null) as any)),
    };

    return {
      content: [{ type: "text", text: `Previewed ${preview.rows.length} rows from ${params.resultId}.` }],
      details: {
        resultId: params.resultId,
        rowCount: metadata.rowCount,
        preview,
        hasMore: offset + limit < metadata.rowCount,
      },
    };
  },
};

export const dataProfile: AgentTool = {
  name: "data_profile",
  label: "Profile Result",
  description: "Get statistical summary of result columns: nulls, distinct values, distributions.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID from data_execute" }),
    columns: Type.Optional(Type.Array(Type.String(), { description: "Specific columns to profile" })),
    sampleRows: Type.Optional(Type.Number({ default: 10000, description: "Rows to sample for profiling" })),
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = resolveWorkspacePath(params.workspacePath);
    await loadWorkspace(workspacePath);

    const metadata = await loadResultMetadata(workspacePath, params.resultId);
    const runner = createDefaultRunner();
    const dataPath = resultDataPath(workspacePath, params.resultId);
    const sampleRows = params.sampleRows ?? metadata.rowCount;
    const source = sampleRows < metadata.rowCount
      ? `(SELECT * FROM '${dataPath}' LIMIT ${sampleRows})`
      : `'${dataPath}'`;
    const targetColumns = params.columns?.length
      ? metadata.columns.filter((column) => params.columns?.includes(column.name))
      : metadata.columns;

    const profiles = [] as Array<Record<string, unknown>>;
    const totalRows = Math.min(metadata.rowCount, sampleRows);

    for (const column of targetColumns) {
      const columnId = escapeIdentifier(column.name);
      const statsResult = await runner.execute({
        sql: `SELECT SUM(CASE WHEN ${columnId} IS NULL THEN 1 ELSE 0 END) as null_count, APPROX_COUNT_DISTINCT(${columnId}) as distinct_est FROM ${source}`,
        format: "json",
      });

      const nullCount = Number(statsResult.rows[0]?.null_count ?? 0);
      const distinctEst = Number(statsResult.rows[0]?.distinct_est ?? 0);

      const topValuesResult = await runner.execute({
        sql: `SELECT ${columnId} as value, COUNT(*) as count FROM ${source} GROUP BY ${columnId} ORDER BY count DESC LIMIT 5`,
        format: "json",
      });

      const topValues = topValuesResult.rows.map((row) => ({
        value: row.value,
        count: row.count,
      }));

      let numericStats: Record<string, unknown> | null = null;
      if (isNumericType(column.type)) {
        const numericResult = await runner.execute({
          sql: `SELECT MIN(${columnId}) as min, MAX(${columnId}) as max, AVG(${columnId}) as mean, STDDEV_SAMP(${columnId}) as stddev FROM ${source}`,
          format: "json",
        });
        numericStats = {
          min: numericResult.rows[0]?.min ?? null,
          max: numericResult.rows[0]?.max ?? null,
          mean: numericResult.rows[0]?.mean ?? null,
          stddev: numericResult.rows[0]?.stddev ?? null,
        };
      }

      profiles.push({
        column: column.name,
        type: column.type,
        nullPct: totalRows > 0 ? nullCount / totalRows : 0,
        distinctEst,
        topValues,
        numericStats,
      });
    }

    return {
      content: [{ type: "text", text: `Profiled ${profiles.length} columns.` }],
      details: { resultId: params.resultId, profiles },
    };
  },
};

export const dataAggregate: AgentTool = {
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
    limit: Type.Optional(Type.Number({ default: DEFAULT_AGGREGATE_LIMIT })),
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params, signal, onUpdate) => {
    const workspacePath = await ensureWorkspace(params.workspacePath);
    await loadResultMetadata(workspacePath, params.resultId);
    const runner = createDefaultRunner();
    const dataPath = resultDataPath(workspacePath, params.resultId);

    const groupBy = params.groupBy ?? [];
    const groupClause = groupBy.map((column) => escapeIdentifier(column)).join(", ");
    const metricClauses = params.metrics.map((metric) => {
      const op = metric.op.toLowerCase();
      if (!metric.column && op !== "count") {
        throw new Error(`Metric column required for ${metric.op}.`);
      }
      if (op === "distinct" && !metric.column) {
        throw new Error("Metric column required for distinct count.");
      }
      const columnRef = metric.column ? escapeIdentifier(metric.column) : "*";
      const expression = op === "distinct"
        ? `COUNT(DISTINCT ${columnRef})`
        : `${op.toUpperCase()}(${columnRef})`;
      return `${expression} AS ${escapeIdentifier(metric.as)}`;
    });

    const selectParts = [...(groupClause ? [groupClause] : []), ...metricClauses];
    const whereClause = params.where ? `WHERE ${params.where}` : "";
    const groupByClause = groupClause ? `GROUP BY ${groupClause}` : "";
    const orderByClause = params.orderBy ? `ORDER BY ${params.orderBy}` : "";
    const limitClause = `LIMIT ${params.limit ?? DEFAULT_AGGREGATE_LIMIT}`;

    const sql = `SELECT ${selectParts.join(", ")} FROM '${dataPath}' ${whereClause} ${groupByClause} ${orderByClause} ${limitClause}`.trim();

    onUpdate?.({ content: [{ type: "text", text: "Running aggregation..." }], details: { phase: "running" } });

    const resultId = createResultId();
    const outputPath = resultDataPath(workspacePath, resultId);
    const rowCount = await runner.executeToParquet(sql, outputPath, signal);

    const columns = await fetchColumns(outputPath);
    const preview = await fetchPreview(outputPath);
    const summary: ResultSummary = {
      resultId,
      rowCount,
      columns,
      preview,
    };

    await saveResultMetadata(workspacePath, {
      resultId,
      sql,
      rowCount,
      columns,
      createdAt: Date.now(),
    });

    return {
      content: [{ type: "text", text: `Aggregated result stored as ${resultId}.` }],
      details: summary,
    };
  },
};

export const dataExport: AgentTool = {
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
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = await ensureWorkspace(params.workspacePath);
    await loadResultMetadata(workspacePath, params.resultId);
    const runner = createDefaultRunner();

    const dataPath = resultDataPath(workspacePath, params.resultId);
    const artifactId = createArtifactId("export");
    const defaultPath = join(workspacePath, "artifacts", `${artifactId}.${params.format}`);
    const outputPath = params.path
      ? (isAbsolute(params.path) ? params.path : resolve(workspacePath, params.path))
      : defaultPath;

    ensureWorkspacePath(workspacePath, outputPath);
    await mkdir(dirname(outputPath), { recursive: true });

    if (params.format === "parquet") {
      await copyFile(dataPath, outputPath);
    } else {
      const format = params.format.toUpperCase();
      const options = params.format === "csv" ? "(FORMAT CSV, HEADER)" : `(FORMAT ${format})`;
      const sql = `COPY (SELECT * FROM '${dataPath}') TO '${outputPath}' ${options}`;
      await runner.execute({ sql, format: "json" });
    }

    const fileStats = await stat(outputPath);
    const artifact: Artifact = {
      artifactId,
      type: params.format,
      path: outputPath,
      createdAt: Date.now(),
      bytes: fileStats.size,
    };

    await saveArtifactMetadata(workspacePath, artifact);

    return {
      content: [{ type: "text", text: `Exported ${params.resultId} to ${outputPath}.` }],
      details: artifact,
    };
  },
};
