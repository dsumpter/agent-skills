import { mkdir, writeFile } from "fs/promises";
import { join } from "path";

import type { Artifact } from "../utils/types.js";

export function artifactMetaPath(workspacePath: string, artifactId: string): string {
  return join(workspacePath, "artifacts", `${artifactId}.json`);
}

export async function saveArtifactMetadata(
  workspacePath: string,
  artifact: Artifact
): Promise<void> {
  await mkdir(join(workspacePath, "artifacts"), { recursive: true });
  await writeFile(artifactMetaPath(workspacePath, artifact.artifactId), JSON.stringify(artifact, null, 2), "utf-8");
}
