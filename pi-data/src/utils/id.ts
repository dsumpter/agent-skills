import { randomUUID } from "crypto";

export function createSessionId(): string {
  return randomUUID();
}

export function createResultId(): string {
  return `r_${randomUUID().slice(0, 8)}`;
}

export function createArtifactId(prefix = "artifact"): string {
  return `${prefix}_${randomUUID().slice(0, 8)}`;
}

export function createNotebookId(): string {
  return `nb_${randomUUID().slice(0, 8)}`;
}

export function createCellId(): string {
  return `c_${randomUUID().slice(0, 8)}`;
}
