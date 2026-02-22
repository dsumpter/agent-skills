import { execFileSync } from "child_process";
import { mkdtempSync } from "fs";
import { readFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import { fileURLToPath } from "url";

const dirname = resolve(fileURLToPath(import.meta.url), "..");

export function repoRoot(): string {
  return resolve(dirname, "..", "..");
}

export function fixturesPath(file: string): string {
  return resolve(dirname, "fixtures", file);
}

export function createTempWorkspace(): string {
  return mkdtempSync(join(tmpdir(), "pi-data-"));
}

export function createTestDatabase(): string {
  const dbPath = join(tmpdir(), `pi-data-test-${Date.now()}.duckdb`);
  const duckdbPath = resolve(repoRoot(), "duckdb");
  const setupSql = readFileSync(fixturesPath("setup.sql"), "utf-8");
  execFileSync(duckdbPath, [dbPath], { input: setupSql });
  return dbPath;
}
