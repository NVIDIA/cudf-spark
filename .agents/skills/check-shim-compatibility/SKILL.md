---
name: check-shim-compatibility
description: Check cudf-spark shim source selection on a PR combined with its current target branch, provide independent cross-PR advisories, and identify required profile validation. Use for versioned sources, shared shim callers, or Spark/Databricks build profiles.
---

# Check shim compatibility

Run from the cudf-spark repository root. This is a read-only preflight, not
authorization to publish, comment, push, or merge. Preserve the calling workflow's
validation, review, and mutation requirements.

The repository helper is `build/check_shim_compatibility.py` (Python 3.10+,
standard library only). The online modes need authenticated
`gh` and Git history; `review` and `advisory` require Git 2.38+ for
`merge-tree --write-tree`. Advisory filtering and joint checks may fetch observed
peer commits from the canonical GitHub repository over HTTPS. The lower-level
`related-prs` command discovers associations through the GitHub API without
fetching peer commits, filtering associations, or checking joint trees.
The helper never posts comments or changes GitHub state.

## Automated review evidence

The `Shim compatibility` workflow first classifies the complete PR diff using
the helper's applicability rules, using read-only repository/PR permissions.
Only shim implementations, shared production JVM sources that may call shims,
profile/build-matrix changes, and the preflight helper, workflow or skill run
the core `shim-compatibility` check and independent advisory. Ordinary
documentation-only and test-only PRs skip both scans. The classifier uses local
Git history without API file-count limits and includes deleted or moved source
paths. Scope detection failures fail the workflow rather than declare the PR
unrelated.
The core check exports immutable Git source blobs, checks `sql-plugin` on current
base and the proposed combined tree,
without depending on unrelated PR activity. No product code, Maven build, or
GPU test is executed. A separate `shim-related-prs` job automatically discovers
related PRs for relevant changes and publishes its own summary/artifact and
warning annotations. It runs independently, without making the core check wait.
API failures, moving peers, and advisory timeouts do not change the core result
or fail the workflow. An unavailable advisory is missing evidence, not a clean scan.

```bash
python3 build/check_shim_compatibility.py review \
  --base-ref origin/main --pr 12345 --output-dir /tmp/shim-review
python3 build/check_shim_compatibility.py advisory \
  --base-ref origin/main --pr 12345 --output-dir /tmp/shim-related-prs
```

`report.json` and `report.md` record exact head/base/combined-tree identities,
the checked matrix and new versus unchanged base findings.
The workflow publishes a summary and full artifact, with up to ten error
annotations for new static findings. Unchanged base debt is reported without
new-error annotations. `INCOMPLETE` fails
the check, retaining any static evidence already collected. Unrelated diffs are
reported as `NOT_APPLICABLE`. The automated source-selection scope is currently
`sql-plugin`; inspect other affected modules separately.

Copilot and Greptile reviewers should read both reports,
then report confirmed new gaps with file/symbol/profile and
observed output. If no execution/API tools or current report are available,
state that limitation. Never fabricate a successful run or report missing
access as a product defect. A green check is not merge authorization.

The workflow runs on PR opening, head updates, reopening, ready-for-review, and
target-branch changes. Title/body-only edits skip all jobs and use an isolated
concurrency group so they cannot cancel a useful scan. It does not run on every
merge to the target branch. Before
merge, compare the report's base/head to current refs and rerun when relevant
changes have landed. Required-check branch protection remains a repository
setting; this change does not enable it or the reviewers' execution features.

## Establish the proposed combination

1. Fetch the target branch from the upstream remote. Verify that it is the
   intended `NVIDIA/cudf-spark` branch; the examples below use `origin/main`.
2. Record the PR head and fetched base SHAs. If earlier validation exists,
   identify the **base SHA actually used by that run**, not today's PR base SHA.
   Record the build profiles it exercised too. Head-only green checks are not
   evidence for a changed base or matrix.
3. Inspect the diff for versioned sources, new/changed imports or other shim
   calls in shared source, release profiles, dependency selection, and CI matrix
   changes. For a genuinely unrelated change, report that scope and stop this
   specialized preflight; normal publication/merge requirements still apply.

## Independent related-PR advisory

For relevant work, run `advisory` alongside `review`, or read both current CI
reports. This is an automatic early warning, not a prerequisite for the core
check to pass. Its API failures or moving peer refs affect only the advisory;
report the limitation without invalidating a completed core report. Both modes
skip unrelated diffs without GitHub discovery requests.

For an existing PR (replace `12345`), the advisory command above writes reports
even if discovery fails. The lower-level `related-prs` command remains available
for manual investigation, including uncommitted work:

```bash
python3 build/check_shim_compatibility.py related-prs \
  --base-ref origin/main --pr 12345
```

For a new PR, omit `--pr`. Search PRs targeting the same repository and branch:

- **Open PRs:** all authors, including Drafts, without an age limit; exclude the
  PR being checked.
- **Merged PRs:** changes after the base actually covered by the latest
  applicable successful validation, through the fetched current target SHA.
  Pass that base as `--validated-base SHA` only when the run's exact base is
  known and it covered the affected profiles/shim APIs. Otherwise use the
  default head/target-branch common ancestor. The helper does not discover or
  verify prior CI coverage automatically.

Prioritize related merges from the last **48 hours** during inspection, while
retaining older changes in that commit range. An earlier green premerge check
only justifies narrowing the range if it included those changes and covered the
affected profiles/shim APIs. #16291 merged about 56 hours before #16037 and their
combination caused #16404. The goal is to catch concurrent PRs that each pass
their own checks but fail when combined.

The helper paginates the open PRs and examines PRs merged in that base-commit
range. It compares actual changed paths, explicit imports in our changed sources,
shim families across version directories, and matrix/implementation interactions.
Pure documentation/tooling file overlaps, such as two changes to `AGENTS.md`, do
not create an association. This API-based discovery records observed heads and
checks for API failures, truncated file lists, and base/open-head movement. The
lower-level `related-prs` command stops at these associations. It does not fetch
peer commits or run the impact filters.

After discovery returns, `advisory` refines open-peer associations using immutable
Git input blobs. Each impact proof compares the current target with that PR's
proposed combination with the target. A common-ancestor diff is not a substitute:
changes already present in the target must not be attributed to the PR. If the
combined tree or its inputs cannot be analyzed, retain the candidate as uncertain.

POM formatting and ordinary comments can be excluded when their build semantics
are unchanged. Root project metadata and reporting changes remain `UNKNOWN`;
the filter does not infer independence by tracking metadata aliases.
Scala-generation directives and their conditional XML payload remain semantic
inputs. Conditional Scala-specific configuration and default-profile edits are
conservatively retained. For existing profiles, configuration edits and versioned
sources can be excluded only when their bounded profile sets are disjoint,
including inheritance and both sides of selector changes. Source graphs must be
stable in both Scala POMs, and profile activation must be limited to its exact
buildver property. New/deleted profiles, graph drift, changed versioned declaration
identities, shared production callers, global build changes and unparseable inputs
retain the candidate with an uncertainty reason. A new or removed declaration can
change import modelling in other profiles, so disjoint selector sets alone do not
establish independence. An absent explicit import is not proof of independence.

Excluded candidates are recorded separately in `filtered_prs` with their reasons,
profile evidence and exact observed head/base identities; they are not completed
checks. The report lists their count and details. Filtered heads and own head/base
are revalidated even when every candidate was excluded. A moved filtered peer is
restored individually as `RETAINED_UNCERTAIN` and `NOT_CHECKED`. Independently
verified peer results and their warnings are preserved. A global API failure or
failure to verify own head/base invalidates all evidence that depends on those
inputs; report the advisory as `INCOMPLETE` and retain unverified exclusions.

The advisory establishes an overall deadline and reserves time for final
revalidation and report output. Filtering uses a shorter sub-budget before joint
checks; peer fetch and analysis timeouts must respect the remaining budget.
Unfinished filters retain candidates as `RETAINED_UNCERTAIN`, never as independent.
No new optional work starts after its deadline. Candidates not reached by joint
checking remain explicitly `NOT_CHECKED`, with the reason retained in the report.
A deadline must leave a report describing completed, incomplete and unchecked
work rather than rely on the CI job's outer timeout to stop discovery.

The automatic `advisory` command checks retained open peers with shim/profile
signals in priority order within its 300-second joint-check start budget and remaining
overall work time, without a fixed peer-count limit. It prioritizes explicit
family matches, then shim/matrix changes and newer PR numbers. A conflict or
incomplete check is recorded before proceeding to the next peer while time
remains. It fetches immutable observed heads without changing refs or the worktree,
exports source blobs, and compares three snapshots: current PR plus target, peer
plus target, and both PRs plus target. Synthetic Git commit objects preserve
ancestry for `merge-tree`; no checkout, build hooks, or peer code are executed.

Only findings absent from both individual combinations are reported as
`NEW_INTERACTION_FINDINGS`, with the affected profile, symbol/caller or duplicate
destination/providers. Existing individual defects are retained separately.
Merged peers are `ALREADY_IN_TARGET`; inspect the core report for that combination.
`COMBINATION_CONFLICT` means the two changes cannot be combined automatically.
API/fetch failures or changed peer identities produce `INCOMPLETE` evidence.
No new peer starts after the work budget expires; remaining candidates are
explicitly `NOT_CHECKED`. Started operations remain bounded by the overall
deadline and reserved finalization time. The report opens
with counts of related PRs, open candidates, attempted and completed static checks,
conflicts, incomplete evidence, unchecked peers, already-merged peers, and filtered
candidates. Completed counts exclude filtered candidates, conflicts and incomplete
evidence, including stale results found by
the final revalidation. These states warn without changing the core result.

`NO_NEW_INTERACTION_FINDINGS` reports only the modelled `sql-plugin` checks; it
does not establish API/signature or runtime compatibility. Such results and
already-merged peers stay in the report without association-only warning
annotations. Do not attribute findings from stale/incomplete evidence to the
current peer. The helper revalidates the analyzed peers and own head/base after
verification. It checks each peer separately, not combinations of multiple peers.

`NEEDS_REVIEW` is an association report, not a confirmed incompatibility.
For each match, determine whether it introduces a profile requiring our shim,
adds a shim required by our profile, changes the same API, or is independent.
Use source evidence and applicable validation to clear an independent match.
Do not delay a PR simply because another related PR remains open or Draft.

## Check source selection and validate

Check the **proposed combination with the current target branch**, using a
separate disposable worktree/snapshot when needed. Do not silently change the
author's worktree. Conflicting combinations need resolution and validation
before merge.

```bash
python3 build/check_shim_compatibility.py coverage
python3 build/check_shim_compatibility.py coverage --buildver 410db183
```

The default checks `sql-plugin` against the union of supported release profiles
in `pom.xml` and `scala2.13/pom.xml`, using `build/get_buildvers.py`. It honors
excluded releases and includes Databricks. Repeat `--module` to select additional
modules; use their applicable `--buildver` values for modules not built in every
profile. Module selection replaces the default, so explicitly include
`--module sql-plugin` when selecting it alongside another module.

The first version checks explicit imports of known module-local versioned
Scala/Java declarations, annotation validity/owner layout, and duplicate
generated source destinations. Shared implementations count as providers too.
Wildcard imports are listed for manual inspection. Fully qualified calls,
same-package uses, inherited APIs, signatures, cross-module providers,
Scala-specific source roots, and Maven's complete effective classpath are not
modelled. **A static `PASS` establishes only those source-selection checks.**
It is neither compilation nor CPU/GPU semantic evidence.

For new shim code, check every applicable profile in current main. For a new
profile, check all required shim families in that module, beyond the PR's file
list. Compile the affected profiles on the combined tree, including newly added
Databricks targets. Use the repository's build instructions and matching runtime
dependencies. Run applicable CPU/GPU tests when behavior changes; annotations
alone cannot select the correct behavioral implementation.

## Disposition and evidence

- `BLOCKED` (exit 1): an explicit import lacks a selected implementation, or
  multiple files select the same generated destination. Inspect and fix the
  finding, then repeat the check and matching compilation. Compare with the
  target branch to distinguish a new regression from existing base debt.
  Report base debt separately; do not change unrelated code to silence it.
- `INCOMPLETE` (exit 2): missing/malformed input, API/permission failure,
  truncated pagination, or moved refs. Report the reason; do not interpret it as
  no related PRs or permission to merge. A core `INCOMPLETE` fails the core check;
  an advisory `INCOMPLETE` only warns in CI and does not overwrite core evidence.
- A dependency on an open PR needs an explicit merge order and validation after
  that dependency lands. A justified Draft can describe the remaining work.
- Related PRs with complete coverage and valid combined-tree evidence may
  proceed through normal review and CI gates without an extra human approval.

Exit 0 means the requested check completed. The advisory or `related-prs` command
can return `NEEDS_REVIEW`, requiring disposition of its matches. No exit code
alone establishes merge readiness.

Report head/base SHAs, profile matrix, static findings, actual build/test
evidence, and remaining gaps. Include advisory matches and dispositions, or its
unavailability reason. A report with
`dirty: true` includes work in progress and must be rerun on the final committed
head before merge. Immediately before publication or merge, refresh the base,
current head and matrix. Relevant movement invalidates prior
evidence. The CI check is advisory until made required by repository settings;
it cannot prevent concurrent human merges.

## Manual verification

For a real-source #16404 replay, export `pom.xml`, `scala2.13/pom.xml`, and
`sql-plugin/src/main` from these commits into separate temporary directories.
Run the current helper with `--root DIRECTORY coverage --buildver 410db183`:

- `c5516e87ea261dd05b94117d1647820a0e85bb77`: must report missing
  `com.nvidia.spark.rapids.shims.ParquetMissingStructShims`.
- `1a20c46043b45c464809311e9e8fb55b8e7e0443`: that gap must be absent.

These are source-selection regressions. Do not describe them as a Databricks
build or runtime test.
