---
name: check-shim-compatibility
description: Review shim compatibility with a local agent by tracing source selection, callers, build profiles, and related PRs. Use when implementing or reviewing versioned shims, shared shim callers, Spark/Databricks profiles, or Shimpler source-selection changes.
---

# Check shim compatibility

Use source review and observed build evidence to assess compatibility.
This skill guides the agent; it does not run an automatic compatibility scan.
Preserve the calling workflow's validation and publication requirements. Reading
this skill does not authorize comments, pushes, PR updates, or merges.

## Establish scope and revisions

Fetch the intended upstream target branch after verifying the remote repository.
Record the target SHA and PR head SHA, and inspect the complete diff, including
removed or renamed files. If earlier CI evidence is used, identify the exact
head, target revision, and profiles that run covered. A green check for an older
target does not establish compatibility with changes merged afterward.

Review versioned implementations, shared callers, Spark/Databricks profiles,
dependencies, and source-selection tools. For unrelated changes, explain why
this specialized review does not apply and follow the normal validation workflow.

## Trace source selection and callers

Read the affected module's sources, root `pom.xml` and `scala2.13/pom.xml`.
Use the current build implementation as the source of truth:

- `build/get_buildvers.py` enumerates supported build versions and exclusions.
- `build/shimpler_source_graph.py` defines profile inheritance.
- `build/shimpler_source_selectors.py` and `build/shimpler_source_resolver.py`
  define selector membership and which variant wins.
- `build/shimpler_source_generator.py` and `build/shim_source_task.py` generate
  the sources consumed by Maven.

Trace each changed shim API to its callers and sibling implementations across
applicable profiles and Scala versions. Inspect wildcard imports, fully qualified
and same-package uses, inherited methods, signatures, and cross-module callers
when relevant. File overlap or an absent explicit import cannot establish
compatibility by itself.

For a new profile, inspect every shim family that module requires, including
files outside the PR diff. For changed selectors or shared source-selection
tools, inspect the affected existing profiles too. Check for missing providers,
competing generated destinations, and API or dependency mismatches. Source
selection alone does not establish API compatibility.

## Inspect related PRs

Use the GitHub API or available read-only tools to inspect PRs targeting the
same repository and branch:

- Open, non-Draft PRs from all authors, without an age cutoff; paginate the
  results and exclude the current PR and any PR marked WIP in its title.
- Merged changes between the target revision actually covered by applicable
  successful validation and the current target. If that revision or profile
  coverage is unknown, inspect from the PR/target common ancestor instead.

Prioritize recent related merges without discarding older changes in that range.
Read actual diffs to identify dependencies: a new caller may need another PR's
provider, a new profile may need a shim variant, or two PRs may change the same
API or selector. A shared Scala path alone is not a reason to flag every peer.
Record relevant peer heads and explain each concrete interaction. An association
alone is not a defect or a reason to delay the PR.

For a suspected interaction, review both diffs against the current target to
distinguish a new compatibility issue from defects already present in either PR.
Leave merge-conflict checks to the existing CI pipeline. State which candidates
remain uninspected if time or access limits the review.

## Validate and report

Use applicable Blossom premerge evidence and the repository's build
instructions. Check that evidence covers the actual target revision, head, and
affected profiles, including newly added Databricks targets and Scala versions.
Build unverified affected profiles when needed. Do not repeat an equivalent
validated build merely to produce a separate compatibility result.

Report the reviewed head/target and relevant peer SHAs, concrete findings with
file/symbol/profile evidence, required dependency order, observed validation,
and remaining gaps. Keep existing target defects separate from new regressions.
If tools, dependencies, or GitHub access are unavailable, state the limitation;
do not claim a clean review or successful compilation. Refresh affected evidence
when relevant revisions or the build matrix change before publication or merge.
