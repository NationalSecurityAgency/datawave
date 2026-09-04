# GitHub Scripts

This directory contains scripts related to GitHub functionalities. To use these scripts, ensure that the GitHub CLI (gh) is installed and configured on your system.

## Scripts

### manage_github_access.sh

This script manages user access to a list of GitHub repositories. It takes two arguments: a GitHub username and a command (`add` or `remove`). The script uses the GitHub API to grant or revoke write permissions for the specified user across the repositories. It is designed to be rerun safely, handling cases where permissions are already applied.

**Usage:**
```bash
./manage_github_access.sh <github-username> <add|remove>
```

### resolve-runner.sh

The reusable test workflows call this script to select their runner. By default,
they use the repository's `RUNNER_TYPE` Actions variable. A branch can opt into a
versioned self-hosted runner without changing that repository-wide default:

1. Add each permitted label to the comma-separated repository Actions variable
   `SUPPORTED_RUNNER_TYPES`, for example `self-hosted-v1,self-hosted-v2`.
2. On the branch being tested, create `.github/runner-version` containing exactly
   one supported label. Versioned `self-hosted-v<number>` and
   `runner-version-packer-v<number>` labels are accepted:

   ```bash
   printf '%s\n' 'self-hosted-v2' > .github/runner-version
   git add .github/runner-version
   git commit -m 'Use self-hosted runner v2 on this branch'
   ```

The branch file must match a supported versioned runner label and an entry in
`SUPPORTED_RUNNER_TYPES`. If it is absent, empty, malformed, or unsupported, CI
falls back to `RUNNER_TYPE`. The `configure-runner` job reports the selected source,
resolved runner, and fallback reason in its log and job summary.

Run the focused checks locally with:

```bash
.github/scripts/tests/resolve-runner-test.sh
.github/scripts/tests/workflow-runner-test.sh
```
