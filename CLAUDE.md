# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository status

This repository is currently a fresh checkout of `https://github.com/boggy011/TE.git` and contains only an empty `README.md` (title: `# TE`). There is no source code, build tooling, test suite, or architecture in place yet.

When code is added, update this file with:
- Build, lint, test, and run commands (including how to run a single test).
- High-level architecture that spans multiple files and isn't obvious from reading one file in isolation.
- Any non-default conventions a future Claude instance would need to know.

## Project knowledge base (`.claude/`)

Reference material to support work on this client is organized under `.claude/`:

- `.claude/databricks/` — Databricks-specific manuals, runbooks, workspace notes (clusters, jobs, Unity Catalog, etc.).
- `.claude/framework/` — Internal framework documentation (libraries, patterns, conventions specific to this client).
- `.claude/skills/` — Reusable skill / how-to material for recurring tasks.

These folders are intentionally seeded empty (`.gitkeep`) and will be populated with documents the user provides. Before starting a task, check the relevant subfolder for context — especially before making assumptions about Databricks setup or framework conventions.

## Working directory note

The repository is checked out under `C:\Users\TE627445\OneDrive - TE Connectivity\Databricks` (a OneDrive-synced path). Be aware that OneDrive may sync or lock files in the background; if a write fails unexpectedly, that is the most likely cause.
