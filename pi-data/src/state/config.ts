import { readFileSync } from "fs";
import { homedir } from "os";
import { join } from "path";

export interface PiDataConfig {
  duckdbPath: string;
  database: string | null;
  workspacePath: string;
  uvPath: string;
  maxResultRows: number;
  inlineChartWidth: number;
  inlineChartHeight: number;
  autoOpenHtml: boolean;
}

const DEFAULT_CONFIG: PiDataConfig = {
  duckdbPath: "duckdb",
  database: null,
  workspacePath: ".pi-data",
  uvPath: "uv",
  maxResultRows: 10000,
  inlineChartWidth: 600,
  inlineChartHeight: 400,
  autoOpenHtml: true,
};

let cachedConfig: PiDataConfig | null = null;

function readConfigFile(): Partial<PiDataConfig> {
  const configPath = join(homedir(), ".pi", "agent", "pi-data.json");
  try {
    const raw = readFileSync(configPath, "utf-8");
    return JSON.parse(raw) as Partial<PiDataConfig>;
  } catch {
    return {};
  }
}

function parseNumber(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseBoolean(value: string | undefined, fallback: boolean): boolean {
  if (!value) {
    return fallback;
  }
  return value.toLowerCase() === "true";
}

export function getConfig(): PiDataConfig {
  if (cachedConfig) {
    return cachedConfig;
  }

  const fileConfig = readConfigFile();
  const config: PiDataConfig = {
    ...DEFAULT_CONFIG,
    ...fileConfig,
  };

  cachedConfig = {
    ...config,
    duckdbPath: process.env.PI_DATA_DUCKDB_PATH ?? config.duckdbPath,
    database: process.env.PI_DATA_DATABASE ?? config.database,
    workspacePath: process.env.PI_DATA_WORKSPACE ?? config.workspacePath,
    uvPath: process.env.PI_DATA_UV_PATH ?? config.uvPath,
    maxResultRows: parseNumber(process.env.PI_DATA_MAX_RESULT_ROWS, config.maxResultRows),
    inlineChartWidth: parseNumber(process.env.PI_DATA_INLINE_CHART_WIDTH, config.inlineChartWidth),
    inlineChartHeight: parseNumber(process.env.PI_DATA_INLINE_CHART_HEIGHT, config.inlineChartHeight),
    autoOpenHtml: parseBoolean(process.env.PI_DATA_AUTO_OPEN_HTML, config.autoOpenHtml),
  };

  return cachedConfig;
}
