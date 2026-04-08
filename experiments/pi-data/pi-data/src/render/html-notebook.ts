import { readFile } from "fs/promises";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";

export interface NotebookCellView {
  title: string;
  body: string;
}

export async function renderNotebookHtml(title: string, cells: NotebookCellView[]): Promise<string> {
  const baseDir = dirname(fileURLToPath(import.meta.url));
  const templatePath = resolve(baseDir, "..", "..", "templates", "notebook.html");
  const template = await readFile(templatePath, "utf-8");
  const cellHtml = cells
    .map((cell) => `\n  <div class="cell">\n    <h3>${cell.title}</h3>\n    ${cell.body}\n  </div>`)
    .join("\n");

  return template
    .replace(/{{title}}/g, title)
    .replace("{{cells}}", cellHtml);
}
