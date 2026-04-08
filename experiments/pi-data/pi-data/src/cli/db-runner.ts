import { spawn } from "child_process";

import { getConfig } from "../state/config.js";

export interface DbRunnerConfig {
  cliPath: string;
  database?: string | null;
}

export interface ExecuteOptions {
  sql: string;
  format?: "csv" | "json";
  signal?: AbortSignal;
}

export interface ExecuteResult {
  columns: string[];
  rows: Array<Record<string, unknown>>;
  rowCount: number;
}

export class DbRunner {
  private config: DbRunnerConfig;

  constructor(config: DbRunnerConfig) {
    this.config = config;
  }

  async execute(options: ExecuteOptions): Promise<ExecuteResult> {
    const format = options.format ?? "json";
    const database = this.config.database ?? ":memory:";
    const args = [database, `-${format}`, "-c", options.sql];

    const proc = spawn(this.config.cliPath, args, {
      signal: options.signal,
    });

    const chunks: Buffer[] = [];
    const errChunks: Buffer[] = [];

    proc.stdout.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    proc.stderr.on("data", (chunk) => errChunks.push(Buffer.from(chunk)));

    let exitCode: number;
    try {
      exitCode = await new Promise<number>((resolve, reject) => {
        proc.on("error", reject);
        proc.on("close", resolve);
      });
    } catch (error) {
      const err = error as NodeJS.ErrnoException;
      if (err.code === "ENOENT") {
        throw new Error(`DuckDB CLI not found at "${this.config.cliPath}". Set PI_DATA_DUCKDB_PATH or config.`);
      }
      throw err;
    }

    if (exitCode !== 0) {
      const message = Buffer.concat(errChunks).toString() || "DuckDB CLI failed";
      throw new Error(message.trim());
    }

    const output = Buffer.concat(chunks).toString().trim();
    if (!output) {
      return { columns: [], rows: [], rowCount: 0 };
    }

    if (format === "json") {
      const rows = JSON.parse(output) as Array<Record<string, unknown>>;
      const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
      return { columns, rows, rowCount: rows.length };
    }

    throw new Error("CSV parsing not implemented");
  }

  async executeToParquet(sql: string, outputPath: string, signal?: AbortSignal): Promise<number> {
    const copySql = `COPY (${sql}) TO '${outputPath}' (FORMAT PARQUET)`;
    await this.execute({ sql: copySql, format: "json", signal });

    const countResult = await this.execute({
      sql: `SELECT COUNT(*) as count FROM '${outputPath}'`,
      format: "json",
      signal,
    });

    const countValue = countResult.rows[0]?.count;
    return typeof countValue === "number" ? countValue : Number(countValue ?? 0);
  }
}

export function createDefaultRunner(): DbRunner {
  const config = getConfig();
  const cliPath = config.duckdbPath;
  const database = config.database;
  return new DbRunner({ cliPath, database });
}
