import { Type } from "@sinclair/typebox";

import type { AgentTool } from "@mariozechner/pi-agent-core";

export const dataWriteSql: AgentTool = {
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
  execute: async (_toolCallId, params) => {
    const maxRows = params.constraints?.maxRows ?? 100;
    const tables = params.tables?.length ? params.tables.join(", ") : "[tables not specified]";
    const dialect = params.dialect ?? "duckdb";

    const sql = `-- Dialect: ${dialect}\n-- Intent: ${params.intent}\n-- Tables: ${tables}\nSELECT * FROM ${params.tables?.[0] ?? "<table>"} LIMIT ${maxRows};`;

    return {
      content: [{ type: "text", text: "Drafted SQL template." }],
      details: {
        sql,
        notes: "Provide specific columns and filters before executing.",
        assumptions: {
          tables,
          dialect,
        },
      },
    };
  },
};
