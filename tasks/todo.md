# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Audit and update all repo docs so the secure IBC path is documented as a global machine-local service required for this project but not scoped to this project.

## Success Criteria
- Every repo doc that describes IBC setup, startup, or runtime behavior is updated to reference the secure service consistently.
- The docs state that the secure IBC service is installed globally under `~/ibc` and `~/Library/LaunchAgents`, is required for this project, and is not project-scoped.
- The docs consistently use the service label `local.ibc-gateway`.
- A final audit confirms no contradictory IBC guidance remains in the updated doc set.

## Dependency Graph
- T1 -> T2 -> T3 -> T4

## Tasks
- [x] T1 Record the documentation audit plan
  depends_on: []
- [x] T2 Audit all repo docs that describe IBC setup, startup, or runtime behavior
  depends_on: [T1]
- [x] T3 Update the relevant docs to describe the secure global machine-local IBC service
  depends_on: [T2]
- [x] T4 Verify doc consistency and update review notes
  depends_on: [T3]

## Review
- Outcome: Updated the IBC documentation in `README.md`, `CLAUDE.md`, and `.codex/project-memory.md` so the secure path is consistently described as the global machine-local service `local.ibc-gateway`, required for this project but not scoped to this repo.
- Verification: Audited the doc surface with `rg` across the repo docs and manually re-read the updated IBC sections in `README.md`, `CLAUDE.md`, and `.codex/project-memory.md` to confirm the service label, install locations, and dependency model now match.
- Residual risk: the default Keychain service names remain compatibility defaults (`com.market-warehouse.ibc.*`), so the docs call that out explicitly to avoid implying those item names define the scope of the installed service.
