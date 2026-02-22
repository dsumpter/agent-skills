import { mkdir, readFile, writeFile } from "fs/promises";
import { join } from "path";

import type { ResultMetadata } from "../utils/types.js";

export function resultDataPath(workspacePath: string, resultId: string): string {
  return join(workspacePath, "results", `${resultId}.parquet`);
}

export function resultMetaPath(workspacePath: string, resultId: string): string {
  return join(workspacePath, "results", `${resultId}.json`);
}

export async function saveResultMetadata(
  workspacePath: string,
  metadata: ResultMetadata
): Promise<void> {
  await mkdir(join(workspacePath, "results"), { recursive: true });
  await writeFile(resultMetaPath(workspacePath, metadata.resultId), JSON.stringify(metadata, null, 2), "utf-8");
}

export async function loadResultMetadata(
  workspacePath: string,
  resultId: string
): Promise<ResultMetadata> {
  const raw = await readFile(resultMetaPath(workspacePath, resultId), "utf-8");
  return JSON.parse(raw) as ResultMetadata;
}
