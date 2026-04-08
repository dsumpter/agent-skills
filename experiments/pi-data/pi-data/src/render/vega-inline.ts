import { spawn } from "child_process";
import { readFile } from "fs/promises";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";

export async function renderVegaInline(spec: object, uvPath: string): Promise<Buffer> {
  const baseDir = dirname(fileURLToPath(import.meta.url));
  const scriptPath = resolve(baseDir, "..", "..", "python", "render_vega.py");
  const proc = spawn(uvPath, ["run", scriptPath], { stdio: ["pipe", "pipe", "pipe"] });

  const outputChunks: Buffer[] = [];
  const errorChunks: Buffer[] = [];

  proc.stdout.on("data", (chunk) => outputChunks.push(Buffer.from(chunk)));
  proc.stderr.on("data", (chunk) => errorChunks.push(Buffer.from(chunk)));

  proc.stdin.write(JSON.stringify(spec));
  proc.stdin.end();

  let exitCode: number;
  try {
    exitCode = await new Promise<number>((resolvePromise, reject) => {
      proc.on("error", reject);
      proc.on("close", resolvePromise);
    });
  } catch (error) {
    const err = error as NodeJS.ErrnoException;
    if (err.code === "ENOENT") {
      throw new Error(`uv not found at "${uvPath}". Install uv or set PI_DATA_UV_PATH.`);
    }
    throw err;
  }

  if (exitCode !== 0) {
    const errorMessage = Buffer.concat(errorChunks).toString() || "Failed to render Vega chart";
    throw new Error(errorMessage.trim());
  }

  return Buffer.concat(outputChunks);
}

export async function loadTemplate(path: string): Promise<string> {
  return readFile(path, "utf-8");
}
