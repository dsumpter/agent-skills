import { Type } from "@sinclair/typebox";
import { readdir, stat } from "fs/promises";
import { join } from "path";

import type { AgentTool } from "@mariozechner/pi-agent-core";

import { createWorkspace, loadWorkspace, resolveWorkspacePath } from "../state/workspace.js";
import type { Session } from "../utils/types.js";

interface SessionInfo {
  session: Session;
  recentResults: string[];
}

async function listRecentResults(workspacePath: string, limit = 5): Promise<string[]> {
  const resultsPath = join(workspacePath, "results");
  let entries: string[] = [];
  try {
    entries = await readdir(resultsPath);
  } catch {
    return [];
  }

  const resultFiles = entries.filter((entry) => entry.endsWith(".json"));
  const withTimes = await Promise.all(
    resultFiles.map(async (file) => ({
      file,
      mtimeMs: (await stat(join(resultsPath, file))).mtimeMs,
    }))
  );

  return withTimes
    .sort((a, b) => b.mtimeMs - a.mtimeMs)
    .slice(0, limit)
    .map((entry) => entry.file.replace(/\.json$/, ""));
}

export const dataSession: AgentTool = {
  name: "data_session",
  label: "Data Session",
  description: "Create or load a data analysis workspace. Call this first before other data tools.",
  parameters: Type.Object({
    action: Type.Union([
      Type.Literal("create"),
      Type.Literal("load"),
      Type.Literal("info"),
    ]),
    workspacePath: Type.Optional(Type.String({
      description: "Path to workspace directory. Defaults to .pi-data/",
    })),
    profile: Type.Optional(Type.String({
      description: "Database connection profile name",
    })),
    description: Type.Optional(Type.String()),
  }),
  execute: async (_toolCallId, params) => {
    const workspacePath = resolveWorkspacePath(params.workspacePath);

    let session: Session;
    if (params.action === "create") {
      session = await createWorkspace(workspacePath, params.profile, params.description);
    } else {
      session = await loadWorkspace(workspacePath);
    }

    const recentResults = await listRecentResults(workspacePath);
    const info: SessionInfo = { session, recentResults };

    return {
      content: [{ type: "text", text: `Workspace: ${workspacePath}` }],
      details: info,
    };
  },
};
