import { readFile } from "fs/promises";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";

interface DashboardSpec {
  id: string;
  title: string;
  spec: object;
}

export async function renderDashboardHtml(
  title: string,
  specs: DashboardSpec[],
  layout: "grid" | "tabs" | "vertical" = "grid"
): Promise<string> {
  const baseDir = dirname(fileURLToPath(import.meta.url));
  const templatePath = resolve(baseDir, "..", "..", "templates", "dashboard.html");
  const template = await readFile(templatePath, "utf-8");

  const cards = specs
    .map((item) => `\n    <div class="card">\n      <h2>${item.title}</h2>\n      <div id="${item.id}"></div>\n    </div>`)
    .join("\n");

  const serializedSpecs = specs.map((item) => ({ id: item.id, spec: item.spec }));

  return template
    .replace(/{{title}}/g, title)
    .replace("{{layoutClass}}", layout)
    .replace("{{cards}}", cards)
    .replace("{{specs}}", JSON.stringify(serializedSpecs));
}
