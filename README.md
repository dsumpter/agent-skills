# agent-skills

Repository for Pi coding agent extensions, skills, themes, commands, references, and experiments.

## Status

### Active Pi package content
- `skills/` – reusable Pi skills loaded by Pi
- `pi-extensions/` – active Pi extensions loaded by Pi
- `pi-themes/` – active Pi themes loaded by Pi
- `commands/` – prompt templates / command markdown files loaded by Pi

### Supporting content
- `references/` – upstream repos cloned locally for inspiration/adaptation
- `experiments/` – exploratory or archived work, including the insurance analytics eval harness

## Current active assets

### Skills
- `skills/modal/`
- `skills/librarian/`

### Extensions
- `pi-extensions/01-multi-edit.ts`

### Experimental
- `experiments/02-pi-diff.ts`

### Themes
- `pi-themes/tokyonight-storm.json`

### Experiments
- `experiments/pi-data/`
- `experiments/insurance-evals/`

## Provenance notes

- `pi-extensions/VENDORED.md` documents vendored extension sources and local modifications.
- `references/skills-vendored.md` documents copied skill sources and local provenance.

## Notes

- Skills, themes, and extensions are auto-loaded through the local Pi package manifest.
- `experiments/02-pi-diff.ts` is an experimental vendored copy of `pi-diff`, adjusted so it skips overriding `edit` when `multi-edit` is already loaded.
- `references/` is for local working copies of upstream repos; actual clones are typically ignored by git.

## Using this repo with Pi

This repo is set up as a local Pi package via `package.json`.
You can add it to Pi with:

```bash
pi install /Users/dsumpter/git_projects/agent-skills
```

Or add the path directly to `~/.pi/agent/settings.json` under `packages`.
