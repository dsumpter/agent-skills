import { Type } from "@sinclair/typebox";
import { mkdir, writeFile } from "fs/promises";
import { join } from "path";

import type { AgentTool } from "@mariozechner/pi-agent-core";

import { renderNotebookHtml } from "../render/html-notebook.js";
import { createDefaultRunner } from "../cli/db-runner.js";
import { saveArtifactMetadata } from "../state/artifact-store.js";
import { loadResultMetadata, resultDataPath } from "../state/result-store.js";
import { loadNotebook, saveNotebook, saveNotebookRevision } from "../state/notebook-store.js";
import { createWorkspace, loadWorkspace, resolveWorkspacePath } from "../state/workspace.js";
import { createArtifactId, createCellId, createNotebookId } from "../utils/id.js";
import type { Cell, CellOutput, Notebook } from "../utils/types.js";
import { dataExecute } from "./sql.js";
import { dataVizHtml, dataVizInline } from "./viz.js";

interface VizCellPayload {
  resultId: string;
  spec: string;
  mode?: "inline" | "html";
}

function findCellIndex(cells: Cell[], cellId: string | undefined): number {
  if (!cellId) {
    return cells.length;
  }
  const index = cells.findIndex((cell) => cell.cellId === cellId);
  return index === -1 ? cells.length : index + 1;
}

function buildPreviewTable(columns: string[], rows: Array<Array<string | number | boolean | null>>): string {
  if (columns.length === 0) {
    return "<p>No rows returned.</p>";
  }
  const header = `<tr>${columns.map((col) => `<th>${col}</th>`).join("")}</tr>`;
  const body = rows
    .map((row) => `<tr>${row.map((value) => `<td>${value ?? ""}</td>`).join("")}</tr>`)
    .join("");
  return `<table>${header}${body}</table>`;
}

async function fetchPreview(workspacePath: string, resultId: string) {
  const runner = createDefaultRunner();
  const dataPath = resultDataPath(workspacePath, resultId);
  const result = await runner.execute({
    sql: `SELECT * FROM '${dataPath}' LIMIT 5`,
    format: "json",
  });
  return {
    columns: result.columns,
    rows: result.rows.map((row) => result.columns.map((column) => (row[column] ?? null) as any)),
  };
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

export const dataNbCreate: AgentTool = {
  name: "data_nb_create",
  label: "Create Notebook",
  description: "Create a new data analysis notebook.",
  parameters: Type.Object({
    title: Type.String({ description: "Notebook title" }),
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = await ensureWorkspace(params.workspacePath);
    const notebookId = createNotebookId();
    const now = Date.now();
    const notebook: Notebook = {
      notebookId,
      title: params.title,
      cells: [],
      createdAt: now,
      updatedAt: now,
    };

    await saveNotebook(workspacePath, notebook);

    return {
      content: [{ type: "text", text: `Notebook ${notebookId} created.` }],
      details: { notebookId, title: params.title, path: join(workspacePath, "notebooks", `${notebookId}.json`) },
    };
  },
};

export const dataNbAddCell: AgentTool = {
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
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = await ensureWorkspace(params.workspacePath);
    const notebook = await loadNotebook(workspacePath, params.notebookId);

    const cell: Cell = {
      cellId: createCellId(),
      kind: params.kind,
      content: params.content,
      version: 1,
    };

    const insertAt = findCellIndex(notebook.cells, params.afterCellId);
    notebook.cells.splice(insertAt, 0, cell);
    notebook.updatedAt = Date.now();

    await saveNotebook(workspacePath, notebook);

    return {
      content: [{ type: "text", text: `Added ${cell.kind} cell to notebook.` }],
      details: { cellId: cell.cellId, position: insertAt },
    };
  },
};

export const dataNbRunCell: AgentTool = {
  name: "data_nb_run_cell",
  label: "Run Notebook Cell",
  description: "Execute a notebook cell and store the output.",
  parameters: Type.Object({
    notebookId: Type.String(),
    cellId: Type.String(),
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params, signal, onUpdate) => {
    const workspacePath = await ensureWorkspace(params.workspacePath);
    const notebook = await loadNotebook(workspacePath, params.notebookId);
    const cellIndex = notebook.cells.findIndex((cell) => cell.cellId === params.cellId);

    if (cellIndex === -1) {
      throw new Error(`Cell ${params.cellId} not found.`);
    }

    const cell = notebook.cells[cellIndex];
    let output: CellOutput;

    if (cell.kind === "sql") {
      const result = await dataExecute.execute("nb-run", { sql: cell.content, workspacePath }, signal, onUpdate);
      output = {
        type: "result",
        resultId: (result.details as any).resultId,
        executedAt: Date.now(),
      };
    } else if (cell.kind === "viz") {
      const payload = JSON.parse(cell.content) as VizCellPayload;
      if (payload.mode === "html") {
        const result = await dataVizHtml.execute(
          "nb-viz",
          { resultId: payload.resultId, spec: payload.spec, workspacePath },
          signal,
          onUpdate
        );
        output = {
          type: "artifact",
          artifactId: (result.details as any).artifactId,
          executedAt: Date.now(),
        };
      } else {
        const result = await dataVizInline.execute(
          "nb-viz",
          { resultId: payload.resultId, spec: payload.spec, workspacePath },
          signal,
          onUpdate
        );
        output = {
          type: "artifact",
          artifactId: (result.details as any).artifactId,
          executedAt: Date.now(),
        };
      }
    } else {
      output = {
        type: "text",
        text: cell.content,
        executedAt: Date.now(),
      };
    }

    cell.output = output;
    cell.version += 1;
    notebook.updatedAt = Date.now();

    await saveNotebook(workspacePath, notebook);
    await saveNotebookRevision(workspacePath, notebook);

    return {
      content: [{ type: "text", text: `Executed cell ${cell.cellId}.` }],
      details: { output },
    };
  },
};

export const dataNbExport: AgentTool = {
  name: "data_nb_export",
  label: "Export Notebook",
  description: "Export notebook as a self-contained HTML file.",
  parameters: Type.Object({
    notebookId: Type.String(),
    includeData: Type.Optional(Type.Boolean({ default: true, description: "Embed result previews" })),
    workspacePath: Type.Optional(Type.String({ description: "Override workspace directory" })),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = await ensureWorkspace(params.workspacePath);
    const notebook = await loadNotebook(workspacePath, params.notebookId);

    const cells = [] as Array<{ title: string; body: string }>;

    for (const cell of notebook.cells) {
      if (cell.kind === "markdown") {
        cells.push({ title: "Markdown", body: `<p>${cell.content}</p>` });
        continue;
      }

      if (cell.kind === "sql") {
        let body = `<pre>${cell.content}</pre>`;
        if (cell.output?.resultId && params.includeData) {
          await loadResultMetadata(workspacePath, cell.output.resultId);
          const preview = await fetchPreview(workspacePath, cell.output.resultId);
          body += buildPreviewTable(preview.columns, preview.rows);
        }
        cells.push({ title: "SQL", body });
        continue;
      }

      if (cell.kind === "viz") {
        cells.push({ title: "Visualization", body: `<pre>${cell.content}</pre>` });
      }
    }

    const html = await renderNotebookHtml(notebook.title, cells);
    const artifactId = createArtifactId("notebook");
    const outputPath = join(workspacePath, "artifacts", `${artifactId}.html`);
    await mkdir(join(workspacePath, "artifacts"), { recursive: true });
    await writeFile(outputPath, html, "utf-8");

    const artifact = {
      artifactId,
      type: "html",
      path: outputPath,
      createdAt: Date.now(),
      bytes: Buffer.byteLength(html),
    };

    await saveArtifactMetadata(workspacePath, artifact);

    return {
      content: [{ type: "text", text: `Notebook exported to ${outputPath}.` }],
      details: artifact,
    };
  },
};
