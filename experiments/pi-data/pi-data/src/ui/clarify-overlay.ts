import type { ExtensionContext } from "@mariozechner/pi-agent-core";

export interface ClarifyFieldOption {
  value: string;
  label: string;
}

export interface ClarifyField {
  name: string;
  label: string;
  options: ClarifyFieldOption[];
  default?: string;
}

export interface ClarifyOptions {
  title: string;
  fields: ClarifyField[];
}

export async function showClarifyOverlay(
  _ctx: ExtensionContext,
  _options: ClarifyOptions
): Promise<Record<string, string> | null> {
  return null;
}
