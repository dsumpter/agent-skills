import { readFile } from "fs/promises";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";

export async function renderChartHtml(title: string, spec: object): Promise<string> {
  const baseDir = dirname(fileURLToPath(import.meta.url));
  const templatePath = resolve(baseDir, "..", "..", "templates", "chart.html");
  const template = await readFile(templatePath, "utf-8");
  return template
    .replace(/{{title}}/g, title)
    .replace("{{spec}}", JSON.stringify(spec));
}
