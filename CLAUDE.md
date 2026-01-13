# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataWave is a Java-based ingest and query framework leveraging Apache Accumulo for fast, secure data access. It supports data fusion, distributed graph analysis, multi-tenant architectures, and fine-grained access control.

## Build Commands

```bash
# Standard build (without tests, recommended for development)
mvn -Ddeploy -Dtar -DskipTests -DskipITs clean install

# Full build with microservices, starters, and utilities
mvn -Ddocker-release -Dmicroservice-docker -Ddist -Dutils -Dservices -Dstarters clean install -T 1C

# Build with tests
mvn clean install

# Run a single test class
mvn test -Dtest=ClassName -pl module-path

# Run a single test method
mvn test -Dtest=ClassName#methodName -pl module-path

# Build Docker images for compose deployment
mvn -Pcompose -Dservices -Dmicroservice-docker -Dquickstart-docker -Ddeploy -Dtar -Ddist -DskipTests clean install
```

## Code Architecture

### Module Structure

- **warehouse/** - Core data processing: ingest pipelines, query engine, Accumulo iterators
  - `query-core/` - Query execution, JEXL evaluation, field index iterators
  - `ingest-core/` - MapReduce jobs for data ingestion
  - `accumulo-extensions/` - Custom Accumulo iterators and tablets

- **core/** - Shared libraries and utilities
  - `in-memory-accumulo/` - Test harness mimicking Accumulo (used extensively in tests)
  - `connection-pool/` - Accumulo client connection management
  - `utils/` - Common utilities (type-utils, metadata-utils, accumulo-utils)

- **web-services/** - REST API layer (Wildfly-based)
  - `query/` - Query submission and results
  - `security/` - Authentication/authorization integration

- **microservices/** - Spring Boot services (independently versioned)
  - `services/query/` - Query microservice
  - `services/authorization/` - Auth service
  - `starters/` - Spring Boot starters for common functionality

### Key Patterns

- **Query Execution**: Queries are parsed into JEXL AST, optimized, then executed via Accumulo iterators
- **Ingest**: MapReduce jobs transform source data into Accumulo mutations
- **Testing**: Most tests use `InMemoryAccumulo` for fast, isolated Accumulo simulation

## Git Workflow

- **Use merge, not rebase** when updating feature branches with integration
  - `git merge origin/integration` - preserves commit history, no force push needed
  - Do NOT use `git rebase` - requires force push, rewrites history
- Never force push to shared branches

## PR and Issue Formatting

- Use short, direct titles (5-10 words max)
- Keep descriptions concise and focused on substance
- Never include AI attribution, footers, or co-author lines, or files related to AI such as CLAUDE.md or AGENTS.md, or folders like .claude

## Issue #2443 - Accumulo API Migration

**CRITICAL: Run `/2443` before ANY #2443 work!**

- **Agent:** `.claude/agents/2443-manager.md` - Full workflow automation
- **Skill:** `.claude/skills/2443.md` - Entry point (invoke with `/2443`)
- **Tracker:** `.agent-work/Plans/Issue-2443-Accumulo-API-Migration.md`

### Quick Commands
| Command | What it does |
|---------|--------------|
| `/2443` | Analyze state, propose next 5 actions, execute with approval |
| `/2443 audit` | Full redundancy check |
| `/2443 sync` | Sync tracker with GitHub |

### Before Starting ANY #2443 Work

1. **Read the tracker** - Check for existing work on the same files/APIs
2. **Run `/2443-audit`** - Checks for redundancies and updates tracker
3. **Check stacked branches** - Some PRs must merge in order (#3253 → #3345 → #3227)
4. **Update tracker FIRST** - Before creating GitHub issues/PRs

### Creating New Work

1. **Create a sub-issue FIRST** before creating a PR:
   - Title: Short description of what's being fixed
   - Body must include "Part of #2443" to link to parent
   - Describe the non-public/deprecated API being replaced
   - List affected files

2. **PR must reference the sub-issue**:
   - Body must include "Fixes #XXXX" (the sub-issue number)
   - Body must include "Part of #2443"
   - No "Motivation" or "Test plan" sections needed

3. **Verify ALL instances are fixed**:
   - Before creating a PR, grep the entire codebase for the non-public API
   - Ensure ALL occurrences are addressed (not just some files)
   - Check both main code and test code

4. **Check for file overlaps**:
   ```bash
   # Check if your target file is already in an open PR
   gh pr list --search "2443 in:body" --state open --json number,files | \
     jq -r '.[] | select(.files[].path | contains("<filename>")) | "PR #\(.number)"'
   ```

Example issue body:
```
## Summary
Replace non-public X with Y.

## Non-Public Class
- `org.apache.accumulo.core.xxxImpl.ClassName`

## Solution
<describe approach>

## Files Affected
- path/to/file1.java
- path/to/file2.java

Part of #2443
```

Example PR body:
```
## Summary
- <bullet points of changes>

Fixes #XXXX
Part of #2443
```

## Key Dependencies

- Java 11
- Apache Accumulo 2.1.x
- Apache Hadoop 3.4.x
- Wildfly 17 (web services)
- Spring Boot (microservices)

# agents.md — Work Tracking Only (Never Added to Git)

## Non-negotiable rule

All tracking artifacts (plans, logs, scratch) MUST live outside version control.
They MUST NOT be committed, staged, or referenced by repository-tracked files.

If git is present:
- Tracking files MUST be ignored via `.git/info/exclude` or a global gitignore.
- Do NOT modify `.gitignore` for this purpose.

If git is not present:
- Tracking files MUST still be stored in a clearly "out-of-repo" location.

---

## 1) Canonical storage location (local-only)

Preferred location (inside repo, but permanently unversioned):
- `./.agent-work/`

Ignore mechanism (git):
- Add `/.agent-work/` to `.git/info/exclude` (local-only ignore file).
- Never stage anything under `.agent-work/`.

Fallback location (outside repo entirely):
- `../.agent-work/<repo-name>/` (sibling directory)
- or `~/.agent-work/<repo-name>/` (user home)

Tracking artifacts MUST NOT be created anywhere else.

---

## 2) Canonical work surfaces

### 2.1 Plans (dependency graphs)
Plans are the canonical representation of intended work and ordering.

Location:
- `.agent-work/Plans/<PlanName>.md`

### 2.2 Work Logs (execution trail)
Work logs are the canonical record of what actually happened.

Location:
- `.agent-work/WorkLog/YYYY-MM-DD-<topic>.md`

---

## 3) Plan format (dependency-aware)

Each plan doc MUST contain a `## TODO` section using this format:

```md
## TODO

- [ ] Task: <short name>
  - Goal:
    - <observable "done" conditions>
  - Scope:
    - Includes: <bullets>
    - Excludes: <bullets>
  - Depends on:
    - [ ] .agent-work/Plans/<OtherPlan>.md — Task: <task name>
    - [ ] <same doc> — Task: <task name>
  - Touches:
    - <paths / components expected to change>
  - Risks:
    - <what can go wrong>
  - Notes:
    - <constraints, gotchas, links>
```

### 3.1 Dependency gating (mandatory)

A task with dependencies MUST NOT be marked complete until every dependency is complete.

"Complete" means:

* implementation is finished (or doc-only outcome is finished),
* repository is green (build/tests),
* plan checkboxes and notes reflect current state.

### 3.2 No orphan work (mandatory)

No work may be performed unless it exists as a Task in a plan.
If new work is discovered mid-stream:

* add a new Task (or dependency Task) to the relevant plan before continuing.

---

## 4) Work log format (truthful execution trail)

A work log entry MUST be created/updated when:

* a task is started,
* a task is completed,
* scope changes,
* a decision is made that future work depends on,
* a rollback occurs.

Template:

```md
# YYYY-MM-DD — <topic>

## Context
- Plan: .agent-work/Plans/<PlanName>.md
- Task(s):
  - <Task name>
- Starting assumptions:
  - <bullets>

## Changes Made
- <outcomes, not "edited file X">

## Decisions
- Decision: <what>
  - Rationale: <why>
  - Alternatives: <brief>
  - Consequences: <enables/forbids>

## Verification
- Build/tests: <pass/fail + what was run>
- Notes: <non-obvious details>

## Follow-ups
- New Tasks added:
  - .agent-work/Plans/<PlanName>.md — Task: <task name>
- Blockers:
  - <blockers>

## Status
- Completed:
  - [x] <Task name>
- In progress:
  - [ ] <Task name>
```

Work logs MUST describe reality, not intent.

---

## 5) Definition of Done (tracking-only)

A task is "done" only when:

* its checkbox is checked in the plan,
* the work log includes: Changes Made, Decisions (if any), Verification, Status,
* dependency ordering remains valid,
* repository is green (build/tests).

---

## 6) Hard ban: never added to git

Tracking artifacts MUST NEVER be added to git:

* Never stage `.agent-work/**`
* Never commit `.agent-work/**`
* Never reference `.agent-work/**` from repository-tracked docs

If anything under `.agent-work/` becomes staged:

* unstage it immediately and remove it from the index before proceeding.
