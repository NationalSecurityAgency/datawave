# Releasing Microservices

The release process for DataWave Microservices is handled through a GitHub Actions workflow defined in `.github/workflows/microservice-release.yml`. This process currently handles one up patch releases or releases that contain a suffix (e.g. 1.0.0, 1.0.0-RC1).

## Preconditions

Before starting a release, ensure that:
- You have access to the appropriate repositories and branches.
- All code changes to be included in the release are merged and tested.

## Release Workflow Overview

1. **Access the Repository**: Navigate to the DataWave repository on GitHub.
   - Open your web browser and go to `https://github.com/NationalSecurityAgency/datawave`.
   - Ensure you are logged in with the appropriate permissions.

2. **Trigger the Workflow**:
   - Click on the "Actions" tab in the navigation bar.
   - In the list of workflows, find and click on "Release Microservices".
   - Click on "Run workflow" at the top-right of the workflow page.

3. **Input Parameters**:
   - **projectToRelease**: Choose the specific microservice or package to release. Options include:
     - `microservices/starters/audit`
     - `microservices/services/accumulo/api`
     - ... (and others as defined in the workflow).
   - **finalRelease**: Choose `true` if this is the final release, otherwise choose `false`. (A Final release will release the current patch version of the service. For example, if the poms are set to 1.0.0-SNAPSHOT, a final release will release 1.0.0, and set the poms to 1.0.1-SNAPSHOT)
   - **modifier**: Enter the version modifier, e.g., `RC1`. (This is ignored if finalRelease is true)

4. **Run the Workflow**:
   - After filling in all inputs, click "Run workflow" to start the release process.

5. **Post-Release**:
   - Check the workflow run logs for any errors.
   - Verify that the release has been tagged and pushed by viewing the tags and commits in the repository.

## Additional Notes

- The workflow handles both final and non-final releases.
- Ensure that secrets for container registry credentials (`USER_NAME`, `ACCESS_TOKEN`) are correctly set in the repository's secrets.
- Review the `.github/workflows/microservice-release.yml` file if custom configurations are needed for specific microservices.

## Cascade Releases

For releases that need to roll up through every project that depends on a given
microservice (for example, releasing `audit-api` and then re-releasing every
starter and service that consumes `audit-api`), use the **Cascade Release
Microservices** workflow defined in `.github/workflows/microservice-cascade-release.yml`.

### What it does

1. **Plan** – Maven’s reactor (`mvn -pl :<startArtifact> -amd`) is used to
   enumerate the starting project plus every transitive dependent **in build
   order**. The dependency graph is computed from the live POMs every run, so
   it cannot drift. The ordered list is printed as JSON in the job log.
2. **Release** – For each project in the planned order, the workflow calls
   `microservice-release.yml` (the existing single-project release workflow)
   with `bumpDownstream: true`. After each Maven `release:prepare release:perform`,
   the release workflow runs:
   ```
   mvn versions:update-parent versions:update-properties \
       -DallowSnapshots=false \
       -Dincludes=gov.nsa.datawave.microservice:<releasedArtifactId>
   ```
   from `microservices/`, which rewrites the version reference in every
   downstream POM that consumes the released artifact (whether via a
   `${version.datawave.X}` property or a `<parent>` block). The change is
   committed with the message `auto-update: bump <artifactId> to <version>`
   and pushed before the next project in the cascade is released.
3. **Compose validation** – After all releases complete, the workflow calls
   `.github/workflows/compose-tests.yml` (`allow-snapshots: false`). If the
   compose stack does not come up cleanly or the ingest/web tests fail, the
   entire cascade workflow is marked as failed.

### Inputs

- **startingProject** – path of the project to start from (same dropdown
  values as the single-project workflow, restricted to actual microservice
  modules).
- **finalRelease** – same semantics as the single-project workflow.
- **modifier** – same semantics as the single-project workflow (ignored when
  `finalRelease=true`).
- **dryRun** – **defaults to `true`**. With `dryRun=true` the workflow only
  computes and prints the cascade plan; it does **not** release, bump, push,
  or run compose. Always do a dry-run first to review the order, then re-run
  with `dryRun=false` to actually perform the cascade.

### Recovering from partial failures

There is no automatic rollback. If a release inside the cascade fails:

1. Investigate the failed project’s logs and resolve the root cause.
2. Re-trigger **Cascade Release Microservices** with the **failed project** as
   the new `startingProject`. The plan job will re-enumerate the remaining
   downstream cone, and the cascade resumes from there. Already-released
   projects upstream of the failure are not re-released because they are no
   longer in the dependent set of the failed project.

### Compose validation failures

If the cascade releases all projects successfully but the final compose gate
fails, the released artifacts have already been published. Investigate using
the `compose-tests` job logs, fix the underlying issue with a follow-up
commit/release, and re-run compose-tests until it passes.

