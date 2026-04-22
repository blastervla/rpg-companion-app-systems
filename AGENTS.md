# Agent Delegation Notes (PF2e Import QA)

When doing manual, source-vs-output review work for PF2e resources in this repo, use subagents as follows:

1. Model requirement
- Use a two-tier model policy:
  - Triage/first-pass review: `gpt-5.1-codex-mini` (cost-efficient pass to surface likely gaps).
  - Final/high-risk semantic sign-off: `gpt-5.4-mini` (authoritative pass before closing a resource batch).
- `GPT-5.4-Mini` is required for final sign-off on high-risk resources:
  - class
  - feat
  - spell
  - archetype
- For lower-risk resources (for example many item/armor/weapon/background spot-checks), `gpt-5.1-codex-mini` is acceptable unless results are ambiguous.

2. Delegation shape
- Delegate one resource instance per subagent task (for example, one agent for Cleric).
- Each subagent must read:
  - the full pf2etools source JSON content for that resource
  - the full generated `.rpg.json` resource instance content

3. Expected subagent output
- Return only concrete findings:
  - missing data
  - incorrect data
  - malformed/unclear rendering text
  - incorrectly inferred effects/properties
- Include exact paths and specific keys/sections.
- Do not perform one-off manual resource fixes; fixes must be made in importer/system logic.

4. Review quality bar
- Prioritize semantic correctness over schema-only correctness.
- Flag any hallucinated effects or unsupported assumptions.
- If uncertain, mark as uncertain instead of guessing.
- If a `gpt-5.1-codex-mini` triage result is uncertain or conflicts with expected behavior, escalate that resource to `gpt-5.4-mini` before taking action.

5. RPG Script guardrails (learned in-session; always apply)
- Boolean expressions do not short-circuit. Every operand/path in a condition must be null-safe on its own.
- Use `?` on the final node too when a stat may be undefined (example: `$view?.foo?`).
- Remember `$view` scope: it is local to the current resource/view context. Cross-resource cases may require `$parent.$view` or `$g.$view`.
- Keep view-state vs committed-state explicit. If UI must react before save, use effective stats (`$view_value ?? committed_value`).
- Do not rely on indexed stats for dynamic/contextual runtime filtering. Indexed values are snapshot-like.
- Never bind one control stat directly to another control stat (for example, `select` writing into another `select` view field).
- Resource render/edit paths require compatible base stat shapes. Avoid feeding non-base/calc structures where a base resource is required.
- Prefer explicit typed paths over `meta_stat` whenever feasible for safety/performance.
- For ephemeral UI choices (like strike number), read from view-effective stats rather than auto-saving committed state.
- For mechanics/conditions, avoid direct nullable resource dereferences in boolean expressions; compute null-safe helper flags first and use those.
- Event-name scoping matters: on non-character resources, `event_names` gets resource-scoped/suffixed. For global/system events (`turnAdvanced`, `roundAdvanced`, etc.), use `calculated_event_names` to subscribe to the unsuffixed global event.

6. RPG Script Macros, Imports, and Stdlib Conventions
- Use `define` for repeated formulas/effect fragments/view fragments. Keep macros small, explicit, and behavior-revealing; avoid opaque "do-everything" macros.
- Prefer stable macro signatures with named args at call sites.
- `import` only brings in `define` declarations; imported stats/views/mechanics are ignored.
- Local `define` declarations override imported macro names. Avoid relying on override behavior unless intentional and documented.
- Transitive imports are allowed, but keep chains shallow (prefer at most two hops from caller to leaf macro source).
- Apply all null-safety guardrails inside macro bodies too: no short-circuit assumptions, and every nullable path must be independently safe.
- Stdlib placement:
  - Put cross-system helpers in `systems/_stdlib/*.rpgs`.
  - Put system-specific wrappers/adapters in `systems/<system>/system/macros/system_common.rpgs`.
- Import pattern:
  - Refactored `.rpgs` files should import system overlay macros (for example `system/macros/system_common.rpgs` via relative path).
  - System overlay macro files should import from `systems/_stdlib`.
- Migration policy:
  - Treat macro/import extraction as refactor-only by default.
  - Do not intentionally change gameplay behavior without explicit scope.
  - Require parity checks before/after each extraction batch; if parity fails, split and rollback the chunk.
