import { Type } from "@sinclair/typebox";
import { mkdir, writeFile } from "fs/promises";
import { join } from "path";

import type { AgentTool } from "@mariozechner/pi-agent-core";

import { createDefaultRunner } from "../cli/db-runner.js";
import { renderDashboardHtml } from "../render/html-dashboard.js";
import { renderChartHtml } from "../render/html-chart.js";
import { renderVegaInline } from "../render/vega-inline.js";
import { saveArtifactMetadata } from "../state/artifact-store.js";
import { getConfig } from "../state/config.js";
import { loadResultMetadata, resultDataPath } from "../state/result-store.js";
import { createWorkspace, loadWorkspace, resolveWorkspacePath } from "../state/workspace.js";
import { createArtifactId } from "../utils/id.js";
import { openFile } from "../utils/open.js";
import type { Artifact } from "../utils/types.js";

const DEFAULT_MAX_ROWS = 500;

function inferVegaType(type: string): "quantitative" | "nominal" | "temporal" {
  const lower = type.toLowerCase();
  if (lower.includes("date") || lower.includes("time")) {
    return "temporal";
  }
  if (lower.includes("int") || lower.includes("decimal") || lower.includes("numeric") || lower.includes("double")) {
    return "quantitative";
  }
  return "nominal";
}

function chartKind(goal: string, chartType?: string): string {
  if (chartType) {
    return chartType;
  }
  const lower = goal.toLowerCase();
  if (lower.includes("trend") || lower.includes("over time")) {
    return "line";
  }
  if (lower.includes("distribution")) {
    return "bar";
  }
  return "bar";
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

async function loadResultData(resultId: string, workspacePath: string, limit = DEFAULT_MAX_ROWS) {
  const runner = createDefaultRunner();
  const dataPath = resultDataPath(workspacePath, resultId);
  const result = await runner.execute({
    sql: `SELECT * FROM '${dataPath}' LIMIT ${limit}`,
    format: "json",
  });

  return result.rows;
}

export const dataVizSuggest: AgentTool = {
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
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = await ensureWorkspace(params.workspacePath);
    const metadata = await loadResultMetadata(workspacePath, params.resultId);
    const columns = metadata.columns;
    const x = columns[0];
    const y = columns[1] ?? columns[0];
    const kind = chartKind(params.goal, params.chartType);

    const spec = {
      $schema: "https://vega.github.io/schema/vega-lite/v5.json",
      mark: kind,
      encoding: {
        x: { field: x.name, type: inferVegaType(x.type) },
        y: { field: y.name, type: inferVegaType(y.type) },
        color: columns[2]
          ? { field: columns[2].name, type: inferVegaType(columns[2].type) }
          : undefined,
      },
    };

    return {
      content: [{ type: "text", text: `Suggested a ${kind} chart.` }],
      details: {
        spec,
        rationale: `Using ${x.name} vs ${y.name} with a ${kind} mark.`,
        dataMapping: {
          x: x.name,
          y: y.name,
        },
      },
    };
  },
};

export const dataVizInline: AgentTool = {
  name: "data_viz_inline",
  label: "Render Inline Chart",
  description: "Render a Vega-Lite chart as PNG displayed inline in the terminal.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID with data to visualize" }),
    spec: Type.String({ description: "Vega-Lite JSON specification" }),
    width: Type.Optional(Type.Number({ default: 600 })),
    height: Type.Optional(Type.Number({ default: 400 })),
    theme: Type.Optional(Type.Union([Type.Literal("light"), Type.Literal("dark")])),
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = await ensureWorkspace(params.workspacePath);
    await loadResultMetadata(workspacePath, params.resultId);

    const config = getConfig();
    const data = await loadResultData(params.resultId, workspacePath);
    const parsedSpec = JSON.parse(params.spec) as Record<string, unknown>;
    parsedSpec.width = params.width ?? config.inlineChartWidth ?? parsedSpec.width;
    parsedSpec.height = params.height ?? config.inlineChartHeight ?? parsedSpec.height;
    parsedSpec.data = { values: data };

    const uvPath = config.uvPath;
    const pngData = await renderVegaInline(parsedSpec, uvPath);

    const artifactId = createArtifactId("chart");
    const outputPath = join(workspacePath, "artifacts", `${artifactId}.png`);
    await mkdir(join(workspacePath, "artifacts"), { recursive: true });
    await writeFile(outputPath, pngData);

    const artifact: Artifact = {
      artifactId,
      type: "png",
      path: outputPath,
      createdAt: Date.now(),
      bytes: pngData.length,
    };

    await saveArtifactMetadata(workspacePath, artifact);

    return {
      content: [{ type: "text", text: `Rendered chart to ${outputPath}.` }],
      details: { ...artifact, mime: "image/png", inlineImage: true },
    };
  },
};

export const dataVizHtml: AgentTool = {
  name: "data_viz_html",
  label: "Render HTML Chart",
  description: "Render a Vega-Lite chart as an interactive HTML file and open in browser.",
  parameters: Type.Object({
    resultId: Type.String({ description: "Result ID with data to visualize" }),
    spec: Type.String({ description: "Vega-Lite JSON specification" }),
    title: Type.Optional(Type.String()),
    open: Type.Optional(Type.Boolean({ default: true, description: "Open in browser" })),
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = await ensureWorkspace(params.workspacePath);
    await loadResultMetadata(workspacePath, params.resultId);

    const config = getConfig();

    const data = await loadResultData(params.resultId, workspacePath);
    const parsedSpec = JSON.parse(params.spec) as Record<string, unknown>;
    parsedSpec.data = { values: data };

    const title = params.title ?? `Chart ${params.resultId}`;
    const html = await renderChartHtml(title, parsedSpec);

    const artifactId = createArtifactId("chart");
    const outputPath = join(workspacePath, "artifacts", `${artifactId}.html`);
    await mkdir(join(workspacePath, "artifacts"), { recursive: true });
    await writeFile(outputPath, html, "utf-8");

    const artifact: Artifact = {
      artifactId,
      type: "html",
      path: outputPath,
      createdAt: Date.now(),
      bytes: Buffer.byteLength(html),
    };

    await saveArtifactMetadata(workspacePath, artifact);

    const shouldOpen = (params.open ?? true) && config.autoOpenHtml;
    if (shouldOpen) {
      openFile(outputPath);
    }

    return {
      content: [{ type: "text", text: `HTML chart saved to ${outputPath}.` }],
      details: { ...artifact, opened: shouldOpen },
    };
  },
};

export const dataDashboard: AgentTool = {
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
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = await ensureWorkspace(params.workspacePath);
    const specs = [] as Array<{ id: string; title: string; spec: object }>;
    const config = getConfig();

    for (const [index, item] of params.items.entries()) {
      await loadResultMetadata(workspacePath, item.resultId);
      const data = await loadResultData(item.resultId, workspacePath);
      const parsedSpec = JSON.parse(item.spec) as Record<string, unknown>;
      parsedSpec.data = { values: data };
      specs.push({ id: `chart_${index + 1}`, title: item.title, spec: parsedSpec });
    }

    const html = await renderDashboardHtml(params.title, specs, params.layout ?? "grid");

    const artifactId = createArtifactId("dashboard");
    const outputPath = join(workspacePath, "artifacts", `${artifactId}.html`);
    await mkdir(join(workspacePath, "artifacts"), { recursive: true });
    await writeFile(outputPath, html, "utf-8");

    const artifact: Artifact = {
      artifactId,
      type: "html",
      path: outputPath,
      createdAt: Date.now(),
      bytes: Buffer.byteLength(html),
    };

    await saveArtifactMetadata(workspacePath, artifact);

    const shouldOpen = (params.open ?? true) && config.autoOpenHtml;
    if (shouldOpen) {
      openFile(outputPath);
    }

    return {
      content: [{ type: "text", text: `Dashboard saved to ${outputPath}.` }],
      details: { ...artifact, chartCount: params.items.length, opened: shouldOpen },
    };
  },
};
