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
- `pi-extensions/01-multi-edit.ts`
- `pi-extensions/02-pi-diff.ts`
- `pi-themes/tokyonight-storm.json`
- `experiments/pi-data/`

## Notes

- Skills, themes, and extensions are auto-loaded through the local Pi package manifest.
- `02-pi-diff.ts` is a vendored copy of `pi-diff`, adjusted so it skips overriding `edit` when `multi-edit` is already loaded.

## Using this repo with Pi

This repo is set up as a local Pi package via `package.json`.
You can add it to Pi with:

```bash
pi install /Users/dsumpter/git_projects/agent-skills
```

Or add the path directly to `~/.pi/agent/settings.json` under `packages`.
