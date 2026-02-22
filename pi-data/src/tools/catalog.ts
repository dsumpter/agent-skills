import { Type } from "@sinclair/typebox";

import type { AgentTool } from "@mariozechner/pi-agent-core";

import { createDefaultRunner } from "../cli/db-runner.js";

interface CatalogMatch {
  kind: "table" | "column";
  fqdn: string;
  score: number;
  snippet: string;
}

function sanitizeLike(input: string): string {
  return input.replace(/[%_]/g, (match) => `\\${match}`);
}

function scoreMatch(target: string, queryTerms: string[]): number {
  const lower = target.toLowerCase();
  return queryTerms.reduce((score, term) => (lower.includes(term) ? score + 1 : score), 0);
}

export const dataSearch: AgentTool = {
  name: "data_search",
  label: "Search Catalog",
  description: "Search database catalog for tables and columns matching keywords.",
  parameters: Type.Object({
    query: Type.String({ description: "Search keywords (e.g., 'revenue product customer')" }),
    schemas: Type.Optional(Type.Array(Type.String(), { description: "Limit to specific schemas" })),
    limit: Type.Optional(Type.Number({ default: 20 })),
  }),
  execute: async (_toolCallId, params) => {
    const runner = createDefaultRunner();
    const terms = params.query.split(/\s+/).map((term) => term.toLowerCase()).filter(Boolean);
    const schemaFilter = params.schemas?.length
      ? `AND table_schema IN (${params.schemas.map((schema) => `'${schema}'`).join(", ")})`
      : "";

    const tableRows = await runner.execute({
      sql: `SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' ${schemaFilter}`,
      format: "json",
    });

    const columnRows = await runner.execute({
      sql: `SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE 1=1 ${schemaFilter}`,
      format: "json",
    });

    const matches: CatalogMatch[] = [];

    for (const row of tableRows.rows) {
      const fqdn = `${row.table_schema}.${row.table_name}`;
      const score = scoreMatch(fqdn, terms);
      if (score > 0) {
        matches.push({
          kind: "table",
          fqdn,
          score,
          snippet: `table ${fqdn}`,
        });
      }
    }

    for (const row of columnRows.rows) {
      const fqdn = `${row.table_schema}.${row.table_name}.${row.column_name}`;
      const score = scoreMatch(fqdn, terms);
      if (score > 0) {
        matches.push({
          kind: "column",
          fqdn,
          score,
          snippet: `column ${row.table_name}.${row.column_name}`,
        });
      }
    }

    matches.sort((a, b) => b.score - a.score);
    const limit = params.limit ?? 20;

    return {
      content: [{ type: "text", text: `Found ${matches.slice(0, limit).length} matches.` }],
      details: { matches: matches.slice(0, limit) },
    };
  },
};

export const dataListTables: AgentTool = {
  name: "data_list_tables",
  label: "List Tables",
  description: "List all tables in a schema.",
  parameters: Type.Object({
    schema: Type.Optional(Type.String({ description: "Schema name. Defaults to public/default." })),
    like: Type.Optional(Type.String({ description: "Filter pattern (e.g., 'order%')" })),
    limit: Type.Optional(Type.Number({ default: 100 })),
  }),
  execute: async (_toolCallId, params) => {
    const runner = createDefaultRunner();
    const schemaClause = params.schema ? `AND table_schema = '${params.schema}'` : "";
    const likeClause = params.like
      ? `AND table_name LIKE '${sanitizeLike(params.like)}' ESCAPE '\\'`
      : "";
    const limit = params.limit ?? 100;

    const tables = await runner.execute({
      sql: `SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' ${schemaClause} ${likeClause} LIMIT ${limit}`,
      format: "json",
    });

    const list = tables.rows.map((row) => ({
      schema: String(row.table_schema ?? ""),
      name: String(row.table_name ?? ""),
    }));

    return {
      content: [{ type: "text", text: `Found ${list.length} tables.` }],
      details: { tables: list },
    };
  },
};

export const dataDescribe: AgentTool = {
  name: "data_describe",
  label: "Describe Table",
  description: "Get table schema: columns, types, keys, and row estimate.",
  parameters: Type.Object({
    table: Type.String({ description: "Table name (can include schema: schema.table)" }),
    includeStats: Type.Optional(Type.Boolean({ default: false, description: "Include column statistics (slower)" })),
  }),
  execute: async (_toolCallId, params) => {
    const runner = createDefaultRunner();
    const tableName = params.table.includes(".") ? params.table : `main.${params.table}`;

    const columnsResult = await runner.execute({
      sql: `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema || '.' || table_name = '${tableName}' ORDER BY ordinal_position`,
      format: "json",
    });

    const columns = columnsResult.rows.map((row) => ({
      name: String(row.column_name ?? ""),
      type: String(row.data_type ?? ""),
      nullable: String(row.is_nullable ?? "YES").toUpperCase() === "YES",
    }));

    let stats: Record<string, unknown> | null = null;
    if (params.includeStats) {
      const sampleQuery = `SELECT COUNT(*) as row_count FROM ${tableName}`;
      const countResult = await runner.execute({ sql: sampleQuery, format: "json" });
      stats = { rowCount: countResult.rows[0]?.row_count ?? 0 };
    }

    return {
      content: [{ type: "text", text: `Described ${tableName}.` }],
      details: {
        table: tableName,
        columns,
        stats,
      },
    };
  },
};
