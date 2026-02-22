import { Type } from "@sinclair/typebox";

import type { AgentTool } from "@mariozechner/pi-agent-core";

import { dataProfile } from "./results.js";
import { dataPreview } from "./results.js";

export const dataPlan: AgentTool = {
  name: "data_plan",
  label: "Plan Analysis",
  description: "Decompose a complex data question into concrete analysis steps.",
  parameters: Type.Object({
    question: Type.String({ description: "The question to answer" }),
    tables: Type.Optional(Type.Array(Type.String(), { description: "Available tables" })),
    resultIds: Type.Optional(Type.Array(Type.String(), { description: "Existing results to build on" })),
  }),
  execute: async (_toolCallId, params) => {
    const plan = [
      {
        stepId: "step_1",
        action: "Identify relevant tables and columns",
        tool: "data_search",
        params: { query: params.question },
        expectedOutput: "Candidate tables and columns",
      },
      {
        stepId: "step_2",
        action: "Describe key tables to confirm schema",
        tool: "data_describe",
        params: { table: params.tables?.[0] ?? "<table>" },
        expectedOutput: "Column list and metadata",
      },
      {
        stepId: "step_3",
        action: "Write and validate SQL",
        tool: "data_write_sql",
        params: { intent: params.question, tables: params.tables ?? [] },
        expectedOutput: "Draft SQL",
      },
      {
        stepId: "step_4",
        action: "Execute query",
        tool: "data_execute",
        params: { sql: "<sql>", maxRows: 10000 },
        expectedOutput: "ResultId with preview",
      },
      {
        stepId: "step_5",
        action: "Summarize results",
        tool: "data_summarize",
        params: { resultId: "<resultId>" },
        expectedOutput: "Key findings",
      },
    ];

    return {
      content: [{ type: "text", text: "Generated analysis plan." }],
      details: { plan },
    };
  },
};

export const dataSummarize: AgentTool = {
  name: "data_summarize",
  label: "Summarize Result",
  description: "Generate a compact summary of a result's key findings.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result to summarize" }),
    budgetTokens: Type.Optional(Type.Number({ default: 500, description: "Target summary length" })),
    strategy: Type.Optional(Type.Union([
      Type.Literal("profile+sample"),
      Type.Literal("topk"),
      Type.Literal("time-series"),
    ])),
  }),
  execute: async (_toolCallId, params, signal, onUpdate) => {
    onUpdate?.({ content: [{ type: "text", text: "Profiling result..." }], details: { phase: "profile" } });
    const profile = await dataProfile.execute("rlm-profile", { resultId: params.resultId }, signal, onUpdate);

    onUpdate?.({ content: [{ type: "text", text: "Sampling rows..." }], details: { phase: "sample" } });
    const preview = await dataPreview.execute("rlm-preview", { resultId: params.resultId, limit: 20 }, signal, onUpdate);

    const summary = `Summary based on ${params.strategy ?? "profile+sample"} for ${params.resultId}.`;
    const keyFindings = [
      "Profile and sample generated; inspect columns for trends.",
      "Use aggregations for deeper insights if needed.",
    ];

    return {
      content: [{ type: "text", text: summary }],
      details: {
        summary,
        keyFindings,
        suggestedNextQueries: ["Run data_aggregate for top categories."],
        profile: profile.details,
        preview: preview.details,
      },
    };
  },
};

export const dataAnswer: AgentTool = {
  name: "data_answer",
  label: "Answer Question",
  description: "Synthesize a final answer from multiple result summaries.",
  parameters: Type.Object({
    question: Type.String({ description: "The original question" }),
    evidence: Type.Array(Type.Object({
      resultId: Type.String(),
      summary: Type.String({ description: "Summary of this result" }),
      note: Type.Optional(Type.String({ description: "How this relates to the question" })),
    })),
  }),
  execute: async (_toolCallId, params) => {
    const citations = params.evidence.map((item) => ({
      resultId: item.resultId,
      snippet: item.summary.slice(0, 200),
    }));

    const answer = `Answer for: ${params.question}\nEvidence count: ${params.evidence.length}`;

    return {
      content: [{ type: "text", text: answer }],
      details: {
        answer,
        confidence: 0.5,
        citations,
      },
    };
  },
};
