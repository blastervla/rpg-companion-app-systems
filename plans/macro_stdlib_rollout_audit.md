# Macro Stdlib Rollout Audit

## Scope
- Branch: codex-macro-stdlib-rollout
- Refactor type: define/import stdlib extraction and call-site replacement (no intended gameplay changes).

## Validation Executed
- `python3 scripts/validate_resource_instances.py --system 5e --system 5e2024 --system pf2e`
- Result: `OK: 16096 resource instance file(s) validated.`
- Compiler parity commands executed:
  - `/Users/blastervla/Repositories/rpg-script-code-extension/bundled_tools/darwin-arm64/refresh_system_builder --base=/tmp/rpg-macro-stdlib-baseline/systems --output=/tmp/rpg-macro-stdlib-baseline-output --clean --structured-diagnostics`
  - `/Users/blastervla/Repositories/rpg-script-code-extension/bundled_tools/darwin-arm64/refresh_system_builder --base=/tmp/rpg-macro-stdlib-rollout/systems --output=/tmp/rpg-macro-stdlib-rollout-output --clean --structured-diagnostics`
- Both compile runs succeeded (`errors: 0`, warnings match baseline).
- Strict artifact diff observed expected noise only:
  - `system.composed.json` now contains `"macros": []` in rollout outputs.
  - `resources.json` has `updated_at` timestamp churn (build-time metadata).
- Equivalence check after normalizing noise keys (`macros`, `updated_at`) is `true` for:
  - `5e/system.composed.json`
  - `5e2024/system.composed.json`
  - `pf2e/system.composed.json`
  - `5e/resources.json`
  - `5e2024/resources.json`
  - `pf2e/resources.json`

## Diff Artifact (Current Batch)
 AGENTS.md                                          |  18 +
 .../character_sheet_sections/06_abilities.rpgs     |  91 +--
 .../character_sheet_sections/07_saving_throws.rpgs |  97 +--
 .../system/character_sheet_sections/08_skills.rpgs | 650 ++-------------------
 systems/5e/system/character_stats.rpgs             |  18 +-
 .../system/mechanics/edit_skill_proficiency.rpgs   |  18 +-
 .../resources/armor/mechanics/armor_attune.rpgs    |   4 +-
 .../resources/item/mechanics/item_attune.rpgs      |   4 +-
 .../5e/system/resources/levelled_amount/stats.rpgs |   8 +-
 .../levelled_armor_proficiency/stats.rpgs          |   8 +-
 .../resources/levelled_choice_text/stats.rpgs      |   8 +-
 .../resources/levelled_damage_dice/stats.rpgs      |   2 +
 .../resources/levelled_description/stats.rpgs      |   8 +-
 .../5e/system/resources/levelled_effect/stats.rpgs |   2 +
 .../levelled_skill_proficiency/stats.rpgs          |   8 +-
 .../5e/system/resources/levelled_spell/stats.rpgs  |   8 +-
 .../resources/levelled_value_calculator/stats.rpgs |   2 +
 .../5e/system/resources/levelled_weapon/stats.rpgs |   8 +-
 .../levelled_weapon_proficiency/stats.rpgs         |   8 +-
 systems/5e/system/resources/monster/stats.rpgs     |  14 +-
 .../system/resources/selectable_armors/stats.rpgs  |  23 +-
 .../system/resources/selectable_items/stats.rpgs   |  23 +-
 .../system/resources/selectable_weapons/stats.rpgs |  23 +-
 .../resources/weapon/mechanics/weapon_attune.rpgs  |   4 +-
 .../character_sheet_sections/06_abilities.rpgs     |  91 +--
 .../character_sheet_sections/07_saving_throws.rpgs |  97 +--
 .../system/character_sheet_sections/08_skills.rpgs | 650 ++-------------------
 systems/5e2024/system/character_stats.rpgs         |  18 +-
 .../system/mechanics/edit_skill_proficiency.rpgs   |  18 +-
 .../resources/armor/mechanics/armor_attune.rpgs    |   4 +-
 .../resources/item/mechanics/item_attune.rpgs      |   4 +-
 .../system/resources/levelled_amount/stats.rpgs    |   8 +-
 .../levelled_armor_proficiency/stats.rpgs          |   8 +-
 .../resources/levelled_choice_text/stats.rpgs      |   8 +-
 .../resources/levelled_damage_dice/stats.rpgs      |   2 +
 .../resources/levelled_description/stats.rpgs      |   8 +-
 .../system/resources/levelled_effect/stats.rpgs    |   2 +
 .../levelled_skill_proficiency/stats.rpgs          |   8 +-
 .../system/resources/levelled_spell/stats.rpgs     |   8 +-
 .../resources/levelled_value_calculator/stats.rpgs |   2 +
 .../system/resources/levelled_weapon/stats.rpgs    |   8 +-
 .../levelled_weapon_proficiency/stats.rpgs         |   8 +-
 systems/5e2024/system/resources/monster/stats.rpgs |  14 +-
 .../system/resources/selectable_armors/stats.rpgs  |  23 +-
 .../system/resources/selectable_items/stats.rpgs   |  23 +-
 .../system/resources/selectable_weapons/stats.rpgs |  23 +-
 .../resources/weapon/mechanics/weapon_attune.rpgs  |   4 +-
 .../character_sheet_sections/06_abilities.rpgs     |  91 +--
 .../character_sheet_sections/07_saving_throws.rpgs |  88 +--
 .../system/character_sheet_sections/08_skills.rpgs | 206 +------
 systems/pf2e/system/character_stats.rpgs           |  52 +-
 .../system/mechanics/edit_save_proficiency.rpgs    |  18 +-
 .../system/mechanics/edit_skill_proficiency.rpgs   |  18 +-
 .../system/resources/levelled_amount/stats.rpgs    |   8 +-
 .../levelled_armor_proficiency/stats.rpgs          |   8 +-
 .../resources/levelled_choice_text/stats.rpgs      |   8 +-
 .../resources/levelled_damage_dice/stats.rpgs      |   2 +
 .../resources/levelled_description/stats.rpgs      |   8 +-
 .../system/resources/levelled_effect/stats.rpgs    |   2 +
 .../system/resources/levelled_rank_cap/stats.rpgs  |   8 +-
 .../resources/levelled_save_proficiency/stats.rpgs |   8 +-
 .../levelled_skill_proficiency/stats.rpgs          |   8 +-
 .../system/resources/levelled_spell/stats.rpgs     |   8 +-
 .../resources/levelled_value_calculator/stats.rpgs |   2 +
 .../system/resources/levelled_weapon/stats.rpgs    |   8 +-
 .../levelled_weapon_proficiency/stats.rpgs         |   8 +-
 .../pf2e/system/resources/lore_skill/stats.rpgs    |   8 +-
 .../system/resources/progression_grant/stats.rpgs  |   8 +-
 .../system/resources/selectable_armors/stats.rpgs  |  23 +-
 .../system/resources/selectable_items/stats.rpgs   |  23 +-
 .../system/resources/selectable_weapons/stats.rpgs |  23 +-
 71 files changed, 451 insertions(+), 2289 deletions(-)
