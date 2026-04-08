import { mkdir, readFile, writeFile } from "fs/promises";
import { join } from "path";

import type { Notebook } from "../utils/types.js";

export function notebookPath(workspacePath: string, notebookId: string): string {
  return join(workspacePath, "notebooks", `${notebookId}.json`);
}

export function notebookRevisionDir(workspacePath: string, notebookId: string): string {
  return join(workspacePath, "notebooks", notebookId, "revisions");
}

export async function saveNotebook(workspacePath: string, notebook: Notebook): Promise<void> {
  await mkdir(join(workspacePath, "notebooks"), { recursive: true });
  await writeFile(notebookPath(workspacePath, notebook.notebookId), JSON.stringify(notebook, null, 2), "utf-8");
}

export async function loadNotebook(workspacePath: string, notebookId: string): Promise<Notebook> {
  const raw = await readFile(notebookPath(workspacePath, notebookId), "utf-8");
  return JSON.parse(raw) as Notebook;
}

export async function saveNotebookRevision(workspacePath: string, notebook: Notebook): Promise<void> {
  const revisionDir = notebookRevisionDir(workspacePath, notebook.notebookId);
  await mkdir(revisionDir, { recursive: true });
  const revisionPath = join(revisionDir, `${Date.now()}.json`);
  await writeFile(revisionPath, JSON.stringify(notebook, null, 2), "utf-8");
}
