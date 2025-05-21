# GitHub Scripts

This directory contains scripts related to GitHub functionalities. To use these scripts, ensure that the GitHub CLI (gh) is installed and configured on your system.

## Scripts

### manage_github_access.sh

This script manages user access to a list of GitHub repositories. It takes two arguments: a GitHub username and a command (`add` or `remove`). The script uses the GitHub API to grant or revoke write permissions for the specified user across the repositories. It is designed to be rerun safely, handling cases where permissions are already applied.

**Usage:**
```bash
./manage_github_access.sh <github-username> <add|remove>
```

