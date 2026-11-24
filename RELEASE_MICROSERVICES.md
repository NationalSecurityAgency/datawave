# Releasing microservices

Microservices, APIs, starters, and shared parents have independent versions. Use
GitHub Actions to publish them:

| Workflow | Use it for |
| --- | --- |
| [Release Microservices](.github/workflows/microservice-release.yml) | One API, service, starter, or parent; also a POM-only aggregator release. |
| [Cascade Release Microservices](.github/workflows/microservice-cascade-release.yml) | A starting project and its transitive consumers, released in dependency order. |

## Before releasing

Merge the changes to the branch you intend to release and inspect the selected
project's `pom.xml`. The workflow derives the release version from that POM;
there is no separate release-version input. These workflows expect numeric
`major.minor.patch` versions, normally ending in `-SNAPSHOT`.

The repository must have `GHCR_WRITE_USER_NAME` and `GHCR_WRITE_ACCESS_TOKEN`
configured for repository commits/tags and package publication. The workflows
configure Java 11, Maven credentials for `github-datawave`, and GHCR login.
Complete relevant tests before releasing: the release command skips tests and
several quality checks. Only the cascade workflow runs the compose validation gate.

For a POM at `1.0.0-SNAPSHOT`:

| Inputs | Published version | Next development version |
| --- | --- | --- |
| `finalRelease=false`, `modifier=RC1` | `1.0.0-RC1` | `1.0.0-SNAPSHOT` |
| `finalRelease=true` | `1.0.0` | `1.0.1-SNAPSHOT` |

`modifier` is ignored for final releases. A release candidate is a real published
artifact with a tag; it is not a dry run. Final releases rebuild the project.

## Release one project

1. Open **Actions → Release Microservices → Run workflow**.
2. Choose the branch containing the changes and workflow definition.
3. Set `projectToRelease` to the specific project, for example
   `microservices/services/audit/service`.
4. Choose `finalRelease` and, for a candidate, `modifier`.
5. Leave `pomOnly=false` for an API, service, or starter.
6. Normally leave `bumpDownstream=false`. When enabled, it updates the selected
   project's parent and microservice dependency version properties from available
   releases before releasing it. It does not release downstream projects.
7. Run the workflow and verify the published Maven artifacts, applicable container
   images, and release tag.

The standalone workflow commits and pushes on the selected branch, for both
candidate and final releases. Unlike the cascade, it does not create a temporary
branch or offer a dry-run input. Select a dedicated branch if you want to keep
release commits off your main branch.

Tags use `<projectToRelease>-<releaseVersion>`, for example
`microservices/services/audit/service-1.0.0`.

## Release only an aggregator or parent POM

Set `pomOnly=true` in **Release Microservices** to release the selected POM without
building, changing, or deploying its child modules. The selected project must
declare `<packaging>pom</packaging>`; other packaging is rejected.

Set `projectToRelease` to the aggregator or parent project, choose the desired
`finalRelease` and `modifier`, and normally leave `bumpDownstream=false`. To
publish multiple POMs, run the workflow separately for each project. Run them
sequentially when using the same branch so their pushed commits do not compete.

If the selected POM is at `1.0.0-SNAPSHOT`, a final release publishes its `1.0.0` POM,
creates its path-based `1.0.0` tag, and advances that POM to `1.0.1-SNAPSHOT`.
The children retain their existing versions and published artifacts.

Aggregation and inheritance are separate Maven relationships. Check each child's
`<parent>` declaration to determine which parent POM its consumers need; listing
a child in an aggregator's `<modules>` does not establish inheritance.

POM-only mode applies Maven's non-recursive flag to the outer command and the
release plugin's forked builds, as described in the
[Maven Release Plugin FAQ](https://maven.apache.org/maven-release/maven-release-plugin/faq.html).
Dependency updates and the already-tagged version-advance step are also scoped
to the selected POM. Reusable workflow callers can pass `pomOnly: true` as well.

## Release a cascade

1. Open **Actions → Cascade Release Microservices → Run workflow** and select
   the branch to release.
2. Choose `startingProject`. Use `microservices/microservice-parent` for its
   broad downstream cascade, or choose a narrower API/starter/service.
3. Set `finalRelease` and `modifier`, keeping `dryRun=true` initially.
4. Inspect the plan, versions, existing-tag skips, and anticipated dependency
   updates in the logs.
5. Run again with the same selections and `dryRun=false` to publish.

The planner follows parent and dependency relationships, ignoring pinned version
differences. It includes the starting project and its transitive consumers; it
does not automatically release upstream dependencies. Ensure prerequisites
outside the plan are already published. You can inspect the project list locally:

```bash
python3 .github/release/cascade-plan.py microservices/microservice-parent
```

The cascade intentionally excludes aggregators declaring modules. Release those
separately with POM-only mode if needed.
The cascade has no `pomOnly` input.

An actual cascade creates `cascade-release/<run_id>` and releases projects
sequentially there. Before each release it updates that project's parent and
microservice dependency properties using available non-SNAPSHOT versions. Major
version updates are disabled. Final cascades exclude qualified versions; candidate
cascades allow the selected modifier as well as unqualified releases. These
updates use repository metadata, so review the resulting versions in the logs.

After releases, compose validation runs with snapshots disallowed:

- **Final release:** after validation passes, the temporary branch is merged back
  into the selected branch. Merge conflicts produce a PR for manual resolution.
  The temporary branch is retained if validation or merge-back does not complete.
- **Candidate release:** the temporary branch is deleted during cleanup, including
  after a failure. Release commits are not merged back.

Published artifacts and tags remain even if later releases, compose validation,
or merge-back fail. A failed cascade does not roll back publication.

## Existing tags and recovery

Both workflows check for the exact release tag before publishing. If it exists,
publication is skipped and the selected POM is advanced to the calculated next
development version if needed. In POM-only mode this does not update children.

A tag alone is not proof that deployment succeeded: preparation can create the
tag before publication fails. After a failed run, inspect the failed step and
verify both the tag and package repository before retrying. A normal rerun will
skip an existing tag rather than repair a missing deployment.

Also inspect the current POM version before retrying. If a final release already
advanced it to the next snapshot, a new run targets the next patch release,
not the previous version. For a cascade, inspect its temporary branch (if
retained), release logs, and any merge-back PR before choosing the recovery branch.
