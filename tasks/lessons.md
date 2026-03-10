# Lessons

- Before finalizing a plan or design summary in a dirty worktree, re-run `git status --short` and inspect current diffs for touched runtime files so the plan matches the latest local code, not an earlier snapshot.
- When the user calls out stale documentation, audit every repo doc that describes runtime behavior, operations, architecture, or examples instead of updating only the most obvious file.
- When answering questions about the current storage path, inspect both ingestion scripts and the storage client so you distinguish the system of record from the publish sidecar.
- When a user corrects an operational assumption like a live database lock, re-check the actual process and file state immediately before continuing so recovery work is based on current runtime conditions rather than stale observations.
- When turning a one-off recovery into a productized fallback path, scope the provider chain to the repo's actual universe and calendar model instead of assuming one exchange-specific source is sufficient.
- When promoting a globally installed machine service, keep the launchd label scoped to the service itself rather than the repo or an old application name.
