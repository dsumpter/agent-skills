import { describe, it, beforeAll, expect } from "vitest";
import { readFileSync } from "fs";
import { join } from "path";

import {
  dataDescribe,
  dataExecute,
  dataListTables,
  dataExport,
  dataDashboard,
  dataNbCreate,
  dataNbAddCell,
  dataNbRunCell,
  dataNbExport,
  dataVizHtml,
  dataVizInline,
} from "../src/tools/index.js";
import { createTempWorkspace, createTestDatabase, repoRoot } from "./helpers.js";

const workspacePath = createTempWorkspace();
const databasePath = createTestDatabase();

beforeAll(() => {
  process.env.PI_DATA_DATABASE = databasePath;
  process.env.PI_DATA_WORKSPACE = workspacePath;
  process.env.PI_DATA_DUCKDB_PATH = `${repoRoot()}/duckdb`;
  process.env.PI_DATA_AUTO_OPEN_HTML = "false";
});

describe("pi-data tools", () => {
  it("executes SQL and returns result summary", async () => {
    const result = await dataExecute.execute(
      "test-1",
      { sql: "SELECT * FROM customers;", workspacePath },
      new AbortController().signal,
      () => {}
    );

    const details = result.details as any;
    expect(details.resultId).toMatch(/^r_/);
    expect(details.rowCount).toBe(3);
    expect(details.preview.columns.length).toBeGreaterThan(0);
  });

  it("lists tables", async () => {
    const result = await dataListTables.execute("test-2", { schema: "main" });
    const details = result.details as any;
    expect(details.tables.some((table: any) => table.name === "customers")).toBe(true);
  });

  it("describes table schema", async () => {
    const result = await dataDescribe.execute("test-3", { table: "customers" });
    const details = result.details as any;
    expect(details.columns.some((column: any) => column.name === "name")).toBe(true);
  });

  it("exports results to nested path", async () => {
    const execResult = await dataExecute.execute(
      "test-4",
      { sql: "SELECT * FROM orders", workspacePath },
      new AbortController().signal,
      () => {}
    );

    const resultId = (execResult.details as any).resultId;
    const outputPath = join(workspacePath, "exports", "orders.csv");
    const exportResult = await dataExport.execute("test-5", {
      resultId,
      format: "csv",
      path: outputPath,
      workspacePath,
    });

    const details = exportResult.details as any;
    expect(details.path).toBe(outputPath);
  });

  it("applies dashboard layout class", async () => {
    const execResult = await dataExecute.execute(
      "test-6",
      { sql: "SELECT region, COUNT(*) as count FROM customers GROUP BY region", workspacePath },
      new AbortController().signal,
      () => {}
    );

    const resultId = (execResult.details as any).resultId;
    const spec = JSON.stringify({
      mark: "bar",
      encoding: { x: { field: "region", type: "nominal" }, y: { field: "count", type: "quantitative" } },
    });

    const dashboardResult = await dataDashboard.execute("test-7", {
      title: "Test Dashboard",
      layout: "vertical",
      open: false,
      items: [{ title: "By Region", resultId, spec }],
      workspacePath,
    });

    const html = readFileSync((dashboardResult.details as any).path, "utf-8");
    expect(html).toContain("layout-vertical");
  });

  it("runs notebook SQL cell and exports HTML", async () => {
    const createResult = await dataNbCreate.execute("nb-1", { title: "Test Notebook", workspacePath });
    const notebookId = (createResult.details as any).notebookId;

    const addResult = await dataNbAddCell.execute("nb-2", {
      notebookId,
      kind: "sql",
      content: "SELECT * FROM customers",
      workspacePath,
    });
    const cellId = (addResult.details as any).cellId;

    await dataNbRunCell.execute("nb-3", { notebookId, cellId, workspacePath }, new AbortController().signal, () => {});

    const exportResult = await dataNbExport.execute("nb-4", { notebookId, workspacePath, includeData: true });
    const exportPath = (exportResult.details as any).path;
    const html = readFileSync(exportPath, "utf-8");

    expect(html).toContain("Test Notebook");
    expect(html).toContain("SELECT * FROM customers");
  });

  it("renders HTML chart", async () => {
    const execResult = await dataExecute.execute(
      "test-8",
      { sql: "SELECT region, COUNT(*) as count FROM customers GROUP BY region", workspacePath },
      new AbortController().signal,
      () => {}
    );

    const resultId = (execResult.details as any).resultId;
    const spec = JSON.stringify({
      mark: "bar",
      encoding: { x: { field: "region", type: "nominal" }, y: { field: "count", type: "quantitative" } },
    });

    const vizResult = await dataVizHtml.execute("test-9", {
      resultId,
      spec,
      title: "Regions",
      open: false,
      workspacePath,
    });

    const html = readFileSync((vizResult.details as any).path, "utf-8");
    expect(html).toContain("Regions");
    expect(html).toContain("vegaEmbed");
  });

  it("renders inline PNG chart", async () => {
    const execResult = await dataExecute.execute(
      "test-10",
      { sql: "SELECT region, COUNT(*) as count FROM customers GROUP BY region", workspacePath },
      new AbortController().signal,
      () => {}
    );

    const resultId = (execResult.details as any).resultId;
    const spec = JSON.stringify({
      mark: "bar",
      encoding: { x: { field: "region", type: "nominal" }, y: { field: "count", type: "quantitative" } },
    });

    const vizResult = await dataVizInline.execute("test-11", {
      resultId,
      spec,
      width: 400,
      height: 300,
      workspacePath,
    });

    const details = vizResult.details as any;
    expect(details.mime).toBe("image/png");
    expect(details.inlineImage).toBe(true);
    const pngHeader = readFileSync(details.path).subarray(0, 8).toString("hex");
    expect(pngHeader).toBe("89504e470d0a1a0a");
  });
});
