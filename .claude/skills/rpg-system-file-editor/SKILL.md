---
name: rpg-system-file-editor
description: Edit RPG Companion App system files under systems/<system>/system (such as .rpgs and .rpg.json), including stats formulas, mechanics, views, resources wiring, and combat behavior. Use when asked to adapt rules behavior in an existing system, add/update RPG Script formulas, or troubleshoot system-file logic. Prefer MCP dev-tool validation when available, with local fallback checks.
---

# RPG System File Editor

## Overview
Edit RPG Companion App system-definition files safely and consistently. Reuse existing patterns from the target system, keep changes minimal and scoped, and verify behavior after edits.

## Core knowledge to use
- Repository layout for system behavior:
  - `systems/<system>/system/*.rpgs`
  - `systems/<system>/system/**/*.rpgs`
  - `systems/<system>/system/**/*.rpg.json`
- RPG Script formula style already used in the codebase:
  - `calc` and `base` stats
  - `when { ... }` branches
  - composable operators (`map`, `filter`, `add`, `divide`, `findFirst`, etc.)
- Stat component and formula reference docs when needed:
  - `/Users/blastervla/Repositories/RPGCompanionApp/app/docs_output/Stat`

## Workflow
1) Confirm scope
- Identify system id, target files/folders, and intended behavior change.
- Convert user intent into explicit formula or data-flow requirements before editing.

2) Inspect local patterns first
- Read nearby files in the same feature area before implementing.
- Match naming, branching style, and function composition used by that system.
- Prefer extending current stats/mechanics over introducing new abstractions.

3) Implement in small, composable steps
- Add intermediate stats first, then final outputs.
- Handle null/empty/unknown inputs explicitly.
- Preserve current behavior unless the request requires a behavior change.
- Avoid broad refactors unless explicitly requested.

4) Validate (MCP-first)
- For multi-file edits or renames:
  - run `impact_analysis` first to scope blast radius.
  - use `codemod_batch` for renames (`dry_run=true` first, then apply). Keep default `all_or_nothing=true`.
  - run `symbol_graph` and require `has_cycles=false`.
- Always run `build_systems` (prefer `structured_diagnostics=true` when available).
- If behavior changed, run targeted `run_tests`.
- If MCP is unavailable in the current session:
  - verify config with `codex mcp list`
  - restart session if server was recently added
  - run local lightweight checks and state limitations.

5) Report
- Summarize changed files and key formula/logic updates.
- Include validation results:
  - impact scope
  - cycle check
  - build result
  - targeted test result (if run)
- Call out assumptions and compatibility decisions.

## Guardrails
- Edit only requested systems/files.
- Do not rename existing stat ids unless requested.
- Do not remove backward compatibility without explicit approval.
- Keep formulas deterministic and readable.

## Typical requests
- "Update combat logic under `systems/<system>/system/combat_system/`."
- "Add or modify derived stats in `stats.rpgs`."
- "Wire a new rules condition into system mechanics/views."
