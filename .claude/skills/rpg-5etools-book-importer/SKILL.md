---
name: rpg-5etools-book-importer
description: Import or assess a 5etools book or source for RPG Companion systems, including cloning 5etools-src, reading a book JSON or source id, determining dependency-aware resource creation order, updating or creating importer logic, generating resource instances, auditing mechanics and counters, and validating the result. Use when Codex is asked to onboard a new 5etools book, source, class/species/feat/background batch, or create a reusable import pipeline for RPG Companion.
---

# RPG 5etools Book Importer

## Overview

Use this skill to turn a 5etools book or source into a repeatable RPG Companion import workflow. Start from a book JSON URL or a source id, inventory the content, determine dependency order, update importer scripts or create new ones, generate resource instances, audit the output for safe mechanics, and validate everything before moving on.

## Inputs

Collect these inputs up front:
- The 5etools book JSON URL if available, for example `data/book/book-<source>.json`
- The 5etools source id, for example `EFA`
- The target RPG Companion system, usually `5e2024` or `5e`
- The target repo for generated content if it differs from the current workspace
- Any scope constraints, such as player-facing only vs full-book import

If the user gives only a source id, derive the book JSON path from `5etools-src` after cloning or browsing.

## Workflow

### 1. Build Context
Clone `5etools-src` into a temporary working directory if a local clone is not already available.
Use the book JSON and `data/*.json` source files to inventory what the book adds.
Search by source id across 5etools data to find relevant resource files, for example feats, backgrounds, races, classes, spells, items, magic variants, monsters, vehicles, rewards, objects, and encounter tables.
Do source-aware comparison against the target system. If the same name already exists under another source, treat the requested source variant as still missing unless the user explicitly wants to reuse the existing version.
Summarize what is actually new relative to the target RPG Companion system before editing anything.

### 2. Map Book Content To RPG Companion Resource Types
Determine which 5etools content maps cleanly to existing RPG Companion resources and which does not.
Split findings into:
- Safe imports now
- Imports that need system-definition updates first
- Content that remains description-only or needs a new resource type

In this app, prefer the local mapping rules actually used by the system rather than the raw 5etools type names. In particular, objects, vehicles, and rewards often map to `item` resources, encounter-table style content can map to `encounter_template`, and weapon or armor-like content may belong in `weapon` or `armor` resources rather than `item`.
Do not trust raw 5etools item type codes blindly. Spot-check the item name and rules text, because some source entries need explicit local type overrides to avoid misclassifying the resource.
Do not assume every 5etools object should be imported immediately.

### 3. Determine Dependency Order Before Generating Resources
Work out resource dependencies before importing any instances.
Use the dependency-order guidance in [dependency_order.md](./references/dependency_order.md).

Typical order:
1. Source records
2. Enumerated or system-definition changes required to represent the new content
3. Leaf resources referenced by others, such as spells and items
4. Item-like resources, including local mappings for objects, vehicles, rewards, and generic variants when the app models them as `item`, `weapon`, or `armor`
5. Resources that player-facing content embeds directly, such as feats
6. Backgrounds and other resources that embed feats or items
7. Species, classes, subclasses, and other higher-level resources once their referenced leaves exist
8. Monsters and other DM-facing stat blocks
9. Encounter-template style content, but only after the required monsters exist locally

If a feat or background references a spell introduced by the same book, import the spell first so the feat/background can embed it mechanically.
If a class or subclass references source-specific item-like support content introduced by the same book, such as an object, vehicle, reward, or summoned construct/stat block, import that support content first if the local app can model it safely.
If the local system embeds subclasses inside the parent class resource, follow that pattern instead of inventing separate subclass files.
If a newly imported source spell belongs to a new class introduced by the same book, patch the spell's reverse class tags after the class exists locally.
If encounter-table content maps to `encounter_template`, import all referenced monsters first.
If a resource can only be partially represented because dependencies are missing, fix the missing dependencies first or explicitly leave the mechanic description-only.

### 4. Inspect Local System Constraints
Read nearby local patterns before writing import logic.
For `5e2024`, inspect:
- `systems/<system>/system/...` for feat types, system-definition filters, and any mechanics that must be extended first
- `systems/<system>/resource_instances/` for representative local resource shapes
- `systems/<system>/system/resources/<resource_id>/stats.rpgs` for the allowed stats and nested resource structure

If the task touches system files, use the `rpg-system-file-editor` skill.
If the task creates or edits resource instances, use the `rpg-resource-instance-editor` skill.

### 5. Prefer Reusable Importers Over One-Off Manual Imports
If more than a handful of resources need similar transformation, create or extend a reusable importer script in the workspace `scripts/` directory.
Keep importers narrow and explicit:
- Read only the relevant 5etools source files
- Convert only safe, well-understood mechanics
- Emit warnings for unsupported or ambiguous cases instead of guessing
- Prefer description-only output over incorrect mechanics

Importer responsibilities usually include:
- Resource naming and ids
- Source mapping
- Prerequisite rendering
- Description rendering from 5etools entries
- Safe effect generation for deterministic mechanics
- Warning on unsupported choices, upgrades, or override mechanics
- Generic-variant requirement resolution against local core base items when importing `magicvariant` content
- Concrete cloning of eligible base weapons or armor into applied source-specific variants when bonuses and requirements can be represented safely
- Preserving the local class/subclass structure used by the target system
- Adding starting-equipment package B gold placeholders when the system models them as selectable item fallbacks
- Keeping generated descriptions source-derived; do not inject importer provenance text such as chapter paths or `Imported from ...`

### 6. Encode Only Safe Mechanics
When importing effects:
- Add deterministic spell grants mechanically if the local referenced spells exist
- Mark `add_spell` effects as passive
- Mark `use_spell` effects as active with charges and recharge when the feat grants a cast without a slot
- Add charge-only `type: none` effects when a feature has explicit limited uses but the full mechanic is not safely automatable
- Add passive stat effects only when the target stat path is known and the rule is unambiguous

Do not import mechanics solely because 5etools normalized metadata suggests them if the feat text does not support a safe local encoding.
Do not invent ASI selectors, spell choices, or upgrade/override behavior unless the system already models that pattern cleanly.
For class imports, prefer mechanically encoding only:
- granted spells
- proficiencies
- attacks-per-action progression
- explicit charge counters

Leave description-first when the local model does not cleanly support:
- companion linkage or summon ownership
- plan systems or replicated-item inventories
- transformation systems or model-swapping gear
- conditional recharge overrides
- once-per-turn riders that need contextual combat hooks
- partial proficiency groups that do not map to a local enum cleanly

Examples of safe fallbacks:
- Keep spell-choice feats description-only but add a counter if the uses-per-rest rule is explicit
- Keep spell-modification upgrades description-only if the local effect model cannot override the base spell behavior safely
- Keep source-specific species variants distinct; do not silently reuse another source's version just because the names match
- Prefer cloning concrete base weapons or armor into applied source-specific variants when the local app can represent the bonuses deterministically; otherwise keep generic variants description-first
- Use the existing gold-fallback placeholder pattern for class/background equipment package B entries instead of omitting them

### 7. Audit The Generated Output
Do not stop at schema validation.
Audit the generated resources against the 5etools source:
- Spot-check representative resources manually
- Compare every deterministic spell grant against the source text
- Check effect typing: passive vs active
- Check counters: constant vs proficiency-bonus scaling, short-rest vs long-rest recharge
- Check descriptions for accidental importer metadata; remove any `Imported from ...` or similar provenance text
- Remove counters or mechanics that conflict with existing base-resource counters instead of shipping contradictory state

Classify remaining gaps clearly:
- Mechanically encoded
- Counter-only
- Description-only due to model limitations

### 8. Validate And Format
For resource instance changes, always run:
```bash
python3 scripts/validate_resource_instances.py --system <system>
python3 scripts/format_resource_instances.py --system <system>
```

If system files changed, prefer MCP dev-tool build validation if available.
State clearly what was validated and what remains unmodeled.

## Output Expectations

When reporting back:
- Summarize what the book adds
- Call out any required system-definition changes
- List the actual importer or resource changes made
- Distinguish safe mechanical imports from description-only imports
- Mention all validation commands run
- Suggest the next dependency-ordered import step

## Notes

Prefer `rg` for searching local files and 5etools content.
Use temporary clone locations such as `/tmp/...` for 5etools assessment unless the user wants a persistent checkout.
If a command needs network access or writes outside the workspace, request approval cleanly and proceed once granted.
