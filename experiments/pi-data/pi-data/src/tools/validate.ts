import { Type } from "@sinclair/typebox";

import type { AgentTool } from "@mariozechner/pi-agent-core";

interface Issue {
  severity: "info" | "warning" | "error";
  message: string;
  fixHint?: string;
}

function containsWrite(sql: string): boolean {
  return /\b(insert|update|delete|drop|alter|create|truncate)\b/i.test(sql);
}

function hasLimit(sql: string): boolean {
  return /\blimit\b/i.test(sql);
}

export const dataValidate: AgentTool = {
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
  execute: async (_toolCallId, params) => {
    const issues: Issue[] = [];
    const requireLimit = params.rules?.requireLimit ?? true;
    const forbidWrite = params.rules?.forbidWrite ?? true;

    if (forbidWrite && containsWrite(params.sql)) {
      issues.push({
        severity: "error",
        message: "SQL contains write operations.",
        fixHint: "Remove INSERT/UPDATE/DELETE/DDL statements.",
      });
    }

    if (requireLimit && !hasLimit(params.sql)) {
      issues.push({
        severity: "warning",
        message: "SQL does not include a LIMIT clause.",
        fixHint: "Add a LIMIT for exploratory queries.",
      });
    }

    return {
      content: [{ type: "text", text: issues.length === 0 ? "SQL looks good." : "SQL has issues." }],
      details: {
        ok: issues.every((issue) => issue.severity !== "error"),
        issues,
      },
    };
  },
};
