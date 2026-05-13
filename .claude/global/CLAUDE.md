# Global Claude Configuration

@profiles/default.md
@user.md

## Read Permission Policy (CRITICAL - MUST APPLY TO ALL PROFILES AND PROJECTS)

**Reading files NEVER requires user approval.**

- The `Read`, `Glob`, `Grep`, and any other read-only tools MUST be used freely without asking for permission.
- This applies to ALL file paths, ALL directories, and ALL projects — no exceptions.
- NEVER ask "Can I read this file?" or "Do you want me to look at this?" — just read it.
- This rule overrides any other permission or approval workflow for read operations.
- Write, edit, delete, execute, deploy, and commit operations still require normal approval as configured.

**This is a GLOBAL rule. It MUST be enforced across every profile and every project, always.**

## Session Startup (MANDATORY)

**On EVERY new session start, auto-detect the current repo and display a project header banner.**

### Detection Logic
1. Read the project-level CLAUDE.md from `~/.claude/projects/<path>/CLAUDE.md` (auto-loaded by Claude Code based on working directory)
2. From the `@profiles/<client>.md` reference, derive the client name (e.g., `schunk.md` = "Schunk Client")
3. From the `@profiles/<client>/<project>.md` reference, read project name, description, and stack
4. Get current git branch from `git branch --show-current`

### Banner Format
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Profile: <client name>
  Project: <project name>
  Stack:   <tech stack>
  Branch:  <current git branch>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

- If no project CLAUDE.md exists, show: `Project: <folder name> (no project config)`
- Show this BEFORE responding to the user's first message
