# Resource instance notes

## Layout
- Systems live in `systems/<system>`.
- Instances live in `systems/<system>/resource_instances`.
- Schemas live in `systems/<system>/resources/<resource_id>/stats.rpgs`.

## File format
- Instance files are `.json` or `.rpg`.
- `.rpg` files are gzipped JSON. Use `scripts/ri_utils.py` or a small Python script to edit.
- Every stat is an object with a `value` field.
- Nested resources use `{ "resource_id": "...", "stats": { ... } }`.

## updated_at
- Update `stats.updated_at.value` on the top-level resource.
- Update `stats.updated_at.value` for any leaf resource objects you modified.
- Use ISO 8601 with microseconds.

## Validation and formatting
- Validate first:
  - `python3 scripts/validate_resource_instances.py --system <system>`
  - `python3 scripts/validate_resource_instances.py --file <path>`
- Format after validation:
  - `python3 scripts/format_resource_instances.py --system <system>`

## Search tips
- Find resource types: `rg -n '"resource_id": "<id>"' systems/<system>/resource_instances`
- Find by stats id: `rg -n '"id": "<id>"' systems/<system>/resource_instances`
- Find by name: `rg -n '"name":' systems/<system>/resource_instances`
