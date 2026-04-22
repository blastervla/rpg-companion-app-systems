# Dependency Order

Determine import order from the actual cross-resource references, not from the order resources appear in the book.

## Heuristics

Import in this order unless the book proves otherwise:
1. Source records
2. System-definition changes needed to represent the new content
3. Leaf resources referenced by others
4. Item-like resources, including local mappings for objects, vehicles, rewards, and generic variants across `item`, `weapon`, and `armor`
5. Composite player resources that depend on those leaves, such as feats
6. Backgrounds and other resources that embed feats or item references
7. Species, classes, and subclasses
8. Monsters and DM-facing stat blocks
9. Encounter-template style content after its monster dependencies are present

If the target system embeds subclasses inside a parent class resource, treat the class import as the subclass import step too.

## Typical Resource Groups

### Source records
- `source_<id>.json`

### System-definition prerequisites
- New feat types
- New enumerated values
- New filters or selectors
- New resource types or stat support

### Leaf resources
- Spells
- Items
- Costs, weights, and other embedded primitives if imported separately
- Source-specific variants that must exist under the correct source even if another source uses the same display name

### Item-like resources
- Native items
- Generic variants from `data/magicvariants.json`
- Vehicles when the local app models them as items
- Rewards when the local app models them as items
- Objects when the local app models them as items
- Armor or weapon-like entries when the local app models them as `armor` or `weapon` instead of `item`

### Composite player resources
- Feats that reference spells
- Backgrounds that grant feats
- Backgrounds that depend on item records or equipment mappings
- Class features that depend on imported spells or items
- Class or subclass support resources introduced by the same book, such as object-like constructs, vehicles, rewards, or other item-like content that should land before the class import

### Core player resources
- Species or races, including source-specific variants like an EFA version distinct from an older source version with the same name
- Classes
- Subclasses
- Reverse spell-class patching for newly introduced source spells that belong to those classes

### DM-facing resources
- Monsters
- Encounter templates after monsters exist locally

## Dependency Checks

Before generating each resource group, ask:
- Does this resource embed another resource by id or full payload?
- Does this entry actually belong in `item`, `weapon`, or `armor` locally?
- Does this resource grant a spell, feat, item, class feature, or monster introduced by the same book?
- Does this resource require a new feat type or other system-definition change before it can be selected or displayed?
- Is the same-name local resource from the wrong source, meaning the source-specific variant is still missing?
- Does a later resource upgrade or override a mechanic introduced by an earlier one?

## Safe Import Rules

If a resource depends on a local spell resource for mechanical embedding, add the spell first.
If encounter-table content maps to `encounter_template`, add the referenced monsters first.
If objects, vehicles, or rewards are modeled as items in the local app, import them in the item phase rather than inventing a new type.
If a class or subclass depends on source-specific object-like or reward-like support content, import that support content before the class or subclass so the later import can reference it cleanly.
If a local system stores subclasses as embedded archetypes, follow that local structure rather than generating standalone subclass resources.
If a class import introduces a new class-specific spell from the same source, patch the spell's `classes` field after the class resource exists locally.
If a generic variant applies to base weapons or armor, import it from `data/magicvariants.json` into the local `weapon` or `armor` model rather than forcing it into `item`.
If the local app can represent the variant bonuses deterministically, clone each eligible plain base weapon or armor into a concrete applied resource instead of shipping only a generic summary resource.
If you generate concrete applied resources, remove or avoid redundant generic summary resources for that same source.
If deriving generic-variant base item lists from local resources, exclude modified named magic weapons or armor and prefer plain base resources.
If a feat upgrades a base feature but the local model cannot override the base counter cleanly, keep the upgrade description-only.
If a class has starting-equipment package A and a gold-only package B, preserve both by importing the package B fallback as the existing gold-placeholder item pattern when that is how the local system handles it.
If a choice is unresolved, prefer:
- counter-only, if the uses are explicit
- description-only, if the choice affects the actual mechanic

## Audit Checklist

After generation, confirm:
- Every embedded spell exists locally
- Deterministic spell grants match source text and level gating
- Counters use the right recharge cadence
- Proficiency-based counters use calculated charges
- No duplicate or contradictory counters exist across base and upgrade feats
- Source-specific variants were not incorrectly satisfied by same-name resources from another source
- Encounter templates reference monsters that already exist locally
