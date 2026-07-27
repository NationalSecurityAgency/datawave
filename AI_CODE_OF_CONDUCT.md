# AI Code of Conduct

This document governs the use of AI and large language model (LLM) tooling by contributors to
DataWave. It applies to code, tests, documentation, issues, pull request descriptions, and review
comments — anything submitted to this repository or its associated sub-repositories.

It is not a ban. AI assistance is permitted and, used well, is welcome. What follows are the limits
that keep DataWave maintainable, correctly licensed, and safe to deploy.

Throughout, **MUST** and **MUST NOT** mark hard requirements; violations are grounds for closing a
contribution. **SHOULD** marks strong expectations that a maintainer may waive with a reason.

## 1. Human accountability

**The contributor is the author.** Submitting a change means you take full responsibility for it,
regardless of how it was produced. "The model wrote it" is not a defense, an excuse, or a mitigating
factor in review.

- You **MUST** be able to explain any line of your contribution — what it does, why it is there, and
  what happens if it is wrong — without consulting the tool that produced it.
- You **MUST NOT** submit code you do not understand. If you cannot describe the failure mode a
  change prevents, you are not ready to submit it.
- You **MUST** review AI-generated output line by line before it becomes part of a commit. Generated
  code is a draft, never a result.
- You remain responsible for your contribution after it merges, including for regressions traced back
  to it.

## 2. Disclosure

Contributors **MUST** disclose material AI assistance in the pull request description. A single line
is enough:

```
AI assistance: <tool/model>, used for <what>.
```

Material assistance means the model produced code, tests, or prose that survived into the diff in
substantially generated form, or performed the analysis that a change rests on. Autocomplete of a
line you were already writing, or using a model to look something up, does not require disclosure.

Disclosure is informational. It does not lower the bar for the change, and maintainers **MUST NOT**
reject a contribution solely because it was disclosed. Undisclosed material assistance, discovered
later, is treated as a trust problem rather than a technical one.

## 3. Provenance and licensing

DataWave is Apache 2.0. Every contribution must be cleanly licensed, and generative tooling makes
provenance harder to establish, not easier.

- You **MUST** have the right to contribute everything you submit under the project license. Model
  output does not launder code whose license you could not otherwise accept.
- You **MUST NOT** prompt a model to reproduce identifiable third-party code, or accept output you
  recognize as a verbatim reproduction of a specific project.
- If generated output looks like it came from a known implementation, you **MUST** verify its origin
  and license before submitting it, or rewrite it.
- You **MUST NOT** use AI tooling to strip, alter, or regenerate copyright notices, license headers,
  or attribution.

## 4. Data handling

Contributors **MUST NOT** submit any non-public data to a third-party AI service. This includes, and
is not limited to:

- Credentials, keys, certificates, tokens, and connection strings — including ones believed to be
  expired or from a test environment.
- Real or realistic data, security markings, visibility expressions, or authorization strings drawn
  from any deployment.
- Configuration, logs, stack traces, hostnames, or network topology from a non-public environment.
- Any material governed by an agreement, classification, or handling requirement that does not
  already permit disclosure to that service.

Sanitize before you prompt, not after. If you are unsure whether something is public, it is not.
Contributors operating under an organizational policy that is stricter than this section **MUST**
follow that policy; nothing here grants permission that policy withholds.

## 5. Security-sensitive code

Some parts of DataWave decide who is allowed to see what. Errors there are not ordinary bugs. This
covers, at minimum: authorization and authentication, column visibility and security-marking
handling, credential and certificate management, query auditing, and the ingest and query paths that
apply access controls.

- Changes in these areas **MUST** be human-authored in their essential logic. Use AI for scaffolding,
  tests, and review if you like; do not use it to decide access-control behavior.
- Such changes **MUST** carry a test that fails without the change, and **MUST** be disclosed under
  §2 even for assistance that would otherwise be immaterial.
- You **MUST NOT** rely on a model's judgment that something "looks secure." A model's confidence
  carries no evidentiary weight in review.
- Suspected vulnerabilities **MUST** be reported privately to the maintainers. Do not open a public
  issue or pull request describing them, and do not paste vulnerability details into a third-party AI
  service — including to ask whether they are exploitable.

## 6. Correctness and tests

Plausible-looking code that does nothing is the characteristic failure of generated contributions,
and tests are where it hides best.

- You **MUST** run the build and the affected tests locally before submitting. A green CI run you did
  not anticipate is not verification.
- Every test you submit **MUST** have been observed to fail against the unfixed or unimplemented
  behavior. Tests that pass unconditionally, assert on values they just computed, or swallow the
  exception they were meant to prove are not acceptable, whatever their origin.
- You **MUST NOT** submit code referencing APIs, configuration keys, or methods you have not confirmed
  exist in this codebase.
- Performance, scale, or benchmark claims **MUST** be measured. Do not repeat a model's estimate as
  though it were a measurement.
- You **SHOULD** prefer a smaller, verified change over a larger one you have partially checked.

## 7. Scale and scope

Generation is cheap; review is not. Volume that outpaces the maintainers' capacity to review is a
denial of service against the project, whatever the intent.

- You **MUST NOT** open bulk or campaign pull requests — mechanically similar changes across many
  files or modules — without agreeing on the scope with a maintainer first, in an issue.
- Pull requests **MUST** stay scoped to one logical change. Do not let a tool expand a fix into an
  unrelated refactor of everything it touched.
- You **MUST NOT** file AI-generated issues, bug reports, or vulnerability reports that you have not
  personally reproduced and confirmed.
- Drive-by "modernization," reformatting, comment insertion, and typo-fix sweeps generated in bulk
  **MUST NOT** be submitted. Formatting is governed by the project's existing tooling.

## 8. Automated and agentic tools

Autonomous or semi-autonomous agents operating against this repository are subject to additional
limits. A human **MUST** be accountable for every action such an agent takes.

- An agent **MUST NOT** push to shared branches, force-push, merge, or alter releases, tags, or CI
  configuration.
- An agent **MUST NOT** open a pull request, respond in a review thread, or close an issue without a
  human reviewing the content first.
- Automated accounts **MUST** be identifiable as such, and **MUST** be attributable to a responsible
  human contributor.
- An agent **MUST NOT** be given credentials beyond those needed for the task, and **MUST NOT** use
  project CI resources for its own generation or evaluation workloads.

## 9. Review and discussion

- Maintainers **MAY** use AI to assist review. Approval **MUST** come from a human who has read the
  change; a model's assessment is an input to that judgment, never a substitute for it.
- You **MUST NOT** use a model to generate argumentative volume in a review thread. Respond to review
  feedback in your own words, briefly.
- You **MUST NOT** re-submit a rejected change with cosmetic regeneration in place of addressing the
  reason it was rejected.
- Disagreement about a change is resolved on technical merit. Neither side gets to invoke a model as
  an authority.

## 10. Enforcement

Maintainers may, at their discretion:

- Ask a contributor how a change was produced, and expect a straight answer.
- Request that a contribution be rewritten, reduced, or explained before further review.
- Close a contribution that violates this document, with a reference to the section at issue.
- Decline further contributions from an account that repeatedly submits unreviewed generated
  material, misrepresents its provenance, or ignores agreed scope limits.

Good-faith mistakes are expected and are handled by asking for a fix. This document is aimed at
patterns, not at first offenses.

## 11. Revision

This document will need to change as tooling does. Propose amendments the same way as any other
change: open an issue describing what is not working, and why.
