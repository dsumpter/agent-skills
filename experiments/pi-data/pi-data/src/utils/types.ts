export interface Session {
  sessionId: string;
  workspacePath: string;
  profile?: string;
  createdAt: number;
  description?: string;
}

export interface Column {
  name: string;
  type: string;
  nullable: boolean;
}

export interface ResultMetadata {
  resultId: string;
  sql: string;
  rowCount: number;
  columns: Column[];
  createdAt: number;
  tags?: string[];
}

export interface ResultPreview {
  columns: string[];
  rows: Array<Array<string | number | boolean | null>>;
}

export interface ResultSummary {
  resultId: string;
  rowCount: number;
  columns: Column[];
  preview: ResultPreview;
}

export interface Artifact {
  artifactId: string;
  type: "png" | "html" | "csv" | "parquet" | "xlsx";
  path: string;
  createdAt: number;
  bytes?: number;
}

export interface Notebook {
  notebookId: string;
  title: string;
  cells: Cell[];
  createdAt: number;
  updatedAt: number;
}

export interface Cell {
  cellId: string;
  kind: "sql" | "markdown" | "viz";
  content: string;
  output?: CellOutput;
  version: number;
}

export interface CellOutput {
  type: "result" | "artifact" | "text" | "error";
  resultId?: string;
  artifactId?: string;
  text?: string;
  error?: string;
  executedAt: number;
}
