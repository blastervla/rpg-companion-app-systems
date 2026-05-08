---
name: rpg-resource-instance-editor
description: Create or modify RPG Companion App resource instances (.json or .rpg) under systems/<system>/resource_instances using stats.rpgs schemas. Use when asked to edit, bulk update, or generate instances for 5e or 5e2024 (including imports from external JSON). Always run scripts/validate_resource_instances.py and then scripts/format_resource_instances.py after changes.
---

# RPG Resource Instance Editor

## Overview
Create or edit resource instance files for the RPG Companion App using the system schemas and repo tooling. Keep changes minimal, update updated_at, and validate/format every time.

## Workflow
1) Clarify scope
- Required: system (5e or 5e2024), resource type(s), target files, create vs edit.
- Ask if missing: exact resource ids, filenames, or source data for imports.
- If a URL is provided, fetch it with available tools; if blocked, ask for a local file.

2) Inspect schema
- Open stats definitions in `systems/<system>/resources/<resource_id>/stats.rpgs`.
- Use the schema to confirm allowed stat names and types.
- Follow nested resource types (resource<...>) to validate leaf structures.

3) Large-scale impact scan (recommended for bulk edits)
- Use MCP symbol tools before editing large batches:
  - `list_symbols(base_path, query?, kinds?)` to confirm target declarations and declared types.
  - `find_references(base_path, symbol, include_declarations=true)` to measure blast radius.
  - `type_at_position(base_path, file_path, line, column)` when a formula or identifier meaning is unclear.
- For any stat id or shared value you change in many files, run `find_references` first and save the count.
- After edits, re-run `find_references` on the same symbol(s) to ensure references are still valid and intentional.

4) Locate or create instances
- Instances live in `systems/<system>/resource_instances/`.
- Files are `.json` or `.rpg` (gzipped JSON). Use Python gzip/json or the helper scripts below.
- Use `rg` to find existing instances by `resource_id`, `stats.id`, or `stats.name.value`.
- For new instances, copy a close existing file and edit in place.

5) Apply changes
- Only change what the user asked for.
- Do not add stats that are not in stats.rpgs (no legacy allowances).
- Every stat is an object with a `value` key. Nested resources are objects with `resource_id` and `stats`.
- `stats.id` is a plain string (not `{ "value": ... }`). Every resource object should have one.
- Update `stats.updated_at.value` for:
  - the top-level resource in the file, and
  - any leaf resource objects you modified (for example `resource_id: "damage_dice"`).
- Use an ISO timestamp with microseconds, e.g. `2026-02-03T10:19:37.096744`.
- When removing entries from resource-array stats, add their ids to `remove_ids` so merges can delete them.
- If a resource object is missing `stats.id`, add one before relying on updated_at or merge behavior.

6) Validate then format (required)
- Run validator:
  - `python3 scripts/validate_resource_instances.py --system <system>`
  - or `python3 scripts/validate_resource_instances.py --file <path>`
- Fix any reported errors.
- Then run formatter:
  - `python3 scripts/format_resource_instances.py --system <system>`

7) Report
- Summarize which files changed and the validator result.
- For large/bulk edits, include pre/post `find_references` counts for key symbols.
- If formatter touched additional files, call it out.

## Helper resources
- `scripts/validate_resource_instances.py`: schema validation.
- `scripts/format_resource_instances.py`: formatting.
- `scripts/update_updated_at_for_changed_resources.py`: backfill updated_at for resources changed between git refs.
- `scripts/update_resource_instances_updated_at.py`: update updated_at for files changed in a git range.
- `scripts/add_missing_resource_ids.py`: add/normalize missing `stats.id` (string) and update updated_at.
- `scripts/add_remove_ids_for_stat_cap_terms.py`: remove stat-cap trigger terms via `remove_ids`.
- `scripts/fix_damage_dice_dice_field.py`: fix `dice` -> `dices` in damage_dice variants.
- `scripts/fix_monster_actions_missing_resource_id.py`: wrap legacy monster actions missing resource_id.

## Notes and gotchas
- Missing `stats.updated_at` on nested resources (e.g., `monster_action`) prevents app updates; add `stats.id` (string) + updated_at on those leaves.
- When backfilling updated_at across commits, use the precise git range that includes the real changes and excludes format-only commits.
- If you delete entries from resource arrays, use `remove_ids` so the merge logic can actually remove them.
- `remove_ids` lives inside the stat object for a resource-array stat and is a list of resource ids (`stats.id` strings) to delete. The resource type is implied by the stat schema, so `remove_ids` items are just ids (no resource_id field inside the list). Example:
```json
{
  "resource_id": "trigger_condition_or",
  "stats": {
    "terms": {
      "value": [
        {
          "resource_id": "trigger_condition_and",
          "stats": {
            "terms": {
              "value": []
            },
            "id": "356a0ba2-f020-463b-a59b-9868ca2faa62",
            "updated_at": {
              "value": "2026-02-07T11:05:12.123456"
            }
          }
        }
      ],
      "remove_ids": [
        "356a0ba2-f020-463b-a59b-9868ca2faa62"
      ],
      "id": "03055e68-342c-4d36-a1e1-cf012f3d7ee7",
      "updated_at": {
        "value": "2026-02-07T11:05:12.123456"
      }
    },
    "id": "....",
    "updated_at": {
      "value": "2026-02-07T11:05:12.123456"
    }
  }
}
```

## Typical requests
- "5e2024 backgrounds should all have Common and two Choose 1 language choices."
- "Fix 5e2024 monk unarmed strikes to be dexterity-based instead of strength."
- "Import class data from external JSON and create a 5e2024 class instance."
