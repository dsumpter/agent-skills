# Vendored Extensions

This directory contains extensions copied or adapted from upstream projects.

## 02-pi-diff.ts

- Upstream package: `@heyhuynhgiabuu/pi-diff`
- Upstream repo: `https://github.com/buddingnewinsights/pi-diff`
- Source copied from: local global install at `/opt/homebrew/lib/node_modules/@heyhuynhgiabuu/pi-diff/src/index.ts`
- License: MIT (per upstream package metadata)

### Local changes

- Vendored into this repository as `pi-extensions/02-pi-diff.ts`
- Adjusted to skip overriding the `edit` tool when another non-builtin `edit` tool is already registered
- Intended to coexist with `pi-extensions/01-multi-edit.ts`
- Continues to provide the `write` override and diff rendering behavior

## 01-multi-edit.ts

- Source reference repo: `https://github.com/mitsuhiko/agent-stuff`
- Source copied from: `references/agent-stuff/pi-extensions/multi-edit.ts`

### Local changes

- Renamed from `multi-edit.ts` to `01-multi-edit.ts` so extension load order is explicit
- Otherwise kept functionally separate from `02-pi-diff.ts`
