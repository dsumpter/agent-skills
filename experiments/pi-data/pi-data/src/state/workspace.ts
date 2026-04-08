import { mkdir, readFile, writeFile } from "fs/promises";
import { join, resolve } from "path";

import { getConfig } from "./config.js";
import { createSessionId } from "../utils/id.js";
import type { Session } from "../utils/types.js";

export function resolveWorkspacePath(pathOverride?: string): string {
  const config = getConfig();
  const workspacePath = pathOverride ?? config.workspacePath;
  return resolve(process.cwd(), workspacePath);
}

export function sessionPath(workspacePath: string): string {
  return join(workspacePath, "session.json");
}

export async function createWorkspace(
  workspacePath: string,
  profile?: string,
  description?: string
): Promise<Session> {
  await mkdir(workspacePath, { recursive: true });
  await mkdir(join(workspacePath, "results"), { recursive: true });
  await mkdir(join(workspacePath, "artifacts"), { recursive: true });
  await mkdir(join(workspacePath, "notebooks"), { recursive: true });

  const session: Session = {
    sessionId: createSessionId(),
    workspacePath,
    profile,
    description,
    createdAt: Date.now(),
  };

  await writeFile(sessionPath(workspacePath), JSON.stringify(session, null, 2), "utf-8");
  return session;
}

export async function loadWorkspace(workspacePath: string): Promise<Session> {
  const raw = await readFile(sessionPath(workspacePath), "utf-8");
  return JSON.parse(raw) as Session;
}
