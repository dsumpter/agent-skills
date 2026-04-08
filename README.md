# agent-skills

Repository for Pi coding agent extensions, skills, themes, commands, references, and experiments.

## Layout

- `skills/` – reusable Pi skills
- `pi-extensions/` – custom Pi extensions
- `pi-themes/` – custom Pi themes
- `commands/` – prompt templates / command markdown files
- `references/` – upstream repos cloned as references
- `experiments/` – exploratory or archived work
- `evals/` – insurance analytics evaluation harness

## Included so far

- `skills/modal/`
- `skills/librarian/`
- `pi-extensions/multi-edit.ts`
- `pi-themes/tokyonight-storm.json`
- `experiments/pi-data/`

## Notes

- Skills and themes are auto-loaded through the local Pi package manifest.
- `pi-extensions/multi-edit.ts` is kept in the repo, but not auto-loaded right now because it conflicts with another installed package that also overrides the `edit` tool.

## Using this repo with Pi

This repo is set up as a local Pi package via `package.json`.
You can add it to Pi with:

```bash
pi install /Users/dsumpter/git_projects/agent-skills
```

Or add the path directly to `~/.pi/agent/settings.json` under `packages`.
