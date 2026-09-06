---
name: summarize
description: "Fetch a URL or convert a local file (PDF/DOCX/HTML/etc.) into Markdown using `markit`, optionally summarize it."
---

Turn documents and URLs into **Markdown** so they can be inspected, quoted, and processed like normal text.

`markit` can fetch URLs directly; this skill mainly wraps it to make saving + summarizing convenient. The commands use the `markit` executable on `PATH` and do not install or update it; check the installed version with `markit --version`. The current package is `@shiftlabs/markit`.

## When to use

Use this skill when you need to:
- convert a web page into Markdown
- convert binary docs (PDF/DOCX/PPTX) into Markdown for analysis
- quickly produce a short summary of a long document before deeper work

## Quick usage

Install or update the CLI with:

```bash
npm install -g @shiftlabs/markit@latest
```

### Convert a URL or file to Markdown

Run from **this skill folder**:

```bash
markit --quiet <url-or-path>
```

To write Markdown to a temp file (prints the path), use the wrapper:

```bash
node to-markdown.mjs <url-or-path> --tmp
```

Tip: when summarizing, the script will **always** write the full converted Markdown to a unique temp `.md` file and will print a final "Hint" line with the path. `--tmp` prints a temp-file path instead of streaming the document; summary prompts can be passed with `--prompt` or as the extra argument after `--summary`.

Write Markdown to a specific file:

```bash
markit --quiet <url-or-path> > /tmp/doc.md
```

Or:

```bash
markit --quiet --output /tmp/doc.md <url-or-path>
```

### Convert + summarize with Inkling Free via Pi/OpenRouter

Summaries work best when you provide **what you want extracted** and the **audience/purpose**.

```bash
node to-markdown.mjs <url-or-path> --summary --prompt "Summarize focusing on X, for audience Y. Extract Z."
```

Or:

```bash
node to-markdown.mjs <url-or-path> --summary --prompt "Focus on security implications and action items."
```

Or, with the input first:

```bash
node to-markdown.mjs <url-or-path> --summary "Focus on security implications and action items."
```

This will:
1. convert to Markdown via `markit --quiet`
2. write the full Markdown to a temp `.md` file and print its path as a hint
3. run headless `pi --provider openrouter --model thinkingmachines/inkling:free` with tools, skills, extensions, prompt templates, themes, context files, and sessions disabled
