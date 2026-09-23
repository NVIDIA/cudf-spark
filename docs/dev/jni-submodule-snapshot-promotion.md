# JNI Submodule-Driven SNAPSHOT Promotion

## Status

This is the standalone design Draft for the **submodule + moving SNAPSHOT** alternative discussed
in [cudf-spark issue #16097](https://github.com/NVIDIA/cudf-spark/issues/16097). Its implementation
stack is separate from the earlier immutable-candidate/LKG Drafts. No file, branch, or review state
is shared between the two alternatives.

The new implementation Drafts are:

- cudf-spark: [#16107](https://github.com/NVIDIA/cudf-spark/pull/16107)
- cudf-spark-jni main: [#5191](https://github.com/NVIDIA/cudf-spark-jni/pull/5191)
- cudf-spark-jni `release/26.10`: [#5192](https://github.com/NVIDIA/cudf-spark-jni/pull/5192)
- Blossom: internal MR `!3047` (linked from the internal review slides)

All four are deliberately marked `[DO NOT REVIEW]` while the team compares alternatives.

## Relationship to the first alternative

| Question | Immutable candidate/LKG alternative | This submodule + moving SNAPSHOT alternative |
|---|---|---|
| Authoritative selection | Commit-qualified Maven coordinate | cudf-spark JNI gitlink |
| POM value | Contains candidate/JNI identity | Remains `26.10.0-SNAPSHOT` |
| Historical branch rebuild | Strong: unique artifact coordinate | Limited: active branches must upmerge main |
| Promotion | Validate candidate, then move LKG pin | Validate pointer PR, then move repository SNAPSHOT |
| Repository transition risk | Low; old and new coordinates coexist | Higher; URM/Sonatype/main cannot change atomically |
| Developer experience | Explicit immutable version | Familiar moving SNAPSHOT after upmerge |

The purpose of this Draft is not to replace or edit the first alternative. It gives reviewers a
complete second code path so the team can choose the desired trade-off.

## Decision summary

`cudf-spark` records its expected JNI source as a Git submodule. The Maven dependency remains the
release-train moving SNAPSHOT, for example `26.10.0-SNAPSHOT`; the JNI commit is not added to the
POM version. An update pull request changes the submodule pointer and contains any required
cudf-spark adaptation.

The pointer update pull request is the promotion transaction:

1. A trusted Jenkins job reads the exact JNI commit from the pull request gitlink.
2. It reuses a retained artifact bundle when the full build fingerprint matches, or builds the
   pinned source once on a cache miss.
3. It tests cudf-spark against that retained bundle before publication.
4. Only after the staged tests pass may the same bytes be published to internal URM and public
   Sonatype under the normal moving SNAPSHOT coordinate.
5. Blossom reads the URM copy and GitHub Actions reads the Sonatype copy. Both force-refresh the
   runtime classifier and verify that repository provenance and the JAR's embedded revision report
   the submodule commit.
6. The pull request may merge only after the required checks pass. JNI main and its release
   branches continue to advance independently; a failed pointer update leaves cudf-spark main on
   its previous pointer.

The project accepts the following operational constraint: active development branches must be
upmerged to current cudf-spark main. Long-term reproducibility of an arbitrary historical branch
is not a goal of the moving-SNAPSHOT design.

## Motivation

Today an independently published JNI SNAPSHOT can change what an unrelated cudf-spark build
downloads. JNI's own test result is not sufficient evidence that a JNI API or behavior change is
compatible with cudf-spark. The desired behavior is to make the source update and downstream
adaptation one reviewed Git transaction, without introducing a commit-qualified Maven version or
a separate permanent pin-promotion protocol.

The existing JNI JAR already contains `cudf-spark-jni-version-info.properties`; its `revision`
field identifies the JNI source commit used for that JAR. The new workflow promotes this existing
provenance to a required CI invariant.

## Goals

- Keep ordinary cudf-spark main, premerge, and nightly on a JNI revision proven compatible with
  the corresponding cudf-spark main.
- Keep the normal Maven coordinate and direct `mvn` developer workflow after upmerge.
- Publish the same staged runtime artifacts to URM and Sonatype only after downstream staged
  validation.
- Make a new JNI commit's first build an expected cache miss on the pointer update PR, not on
  unrelated pull requests.
- Make repository transition failures explicit and recoverable instead of reporting them as
  product regressions.
- Preserve agent assistance for monitoring and repair while keeping build, publication, and merge
  gates deterministic.

## Non-goals

- This design does not preserve a unique Maven coordinate for every historical JNI commit.
- It does not guarantee that an old, non-upmerged cudf-spark branch can be rebuilt indefinitely.
- It does not include private or hybrid repositories.
- It does not permit an agent or an untrusted fork pull request to publish artifacts.
- It does not auto-merge a pointer update without the repository's normal approval policy.

## Source and artifact identities

### Source identity

The authoritative source choice is the gitlink at:

```text
thirdparty/cudf-spark-jni -> <full JNI commit>
```

The JNI commit already pins its cuDF submodule. The full cuDF commit is nevertheless retained in
the build manifest for direct diagnostics.

### Maven identity

The POM continues to declare a simple release-train SNAPSHOT:

```xml
<cudf-spark-jni.version>26.10.0-SNAPSHOT</cudf-spark-jni.version>
```

Maven does not infer a dependency version from a Git submodule. Correctness instead comes from the
publication invariant: the latest SNAPSHOT may change only through a validated pointer transition,
and its provenance must match the pointer used by current cudf-spark main.

### Build fingerprint

Artifact reuse is allowed only when all material inputs match. The fingerprint includes at least:

- full JNI and cuDF commits;
- logical Maven version and classifier matrix;
- build-container image digests;
- CUDA toolkit, compiler, CMake, Maven, JDK, and target architecture;
- material native build options, including GDS, profiler, fault-injection, and GPU architecture
  settings.

The fingerprint is a SHA-256 of canonical JSON. It is not a replacement Maven version. It indexes
the trusted Jenkins retained-artifact cache.

The current Draft pipeline records the configured container image references. Before automated
cache hits are enabled, the production job must resolve those references to immutable image
digests and include them in the input map. Until then, callers must omit the cache-hit pair and
take the build-once cache-miss path.

## Artifact records

The build job creates two related records:

1. A private retained build manifest containing the fingerprint, all staged file hashes, Jenkins
   run, and build inputs.
2. A small Maven attachment with classifier `provenance` and type `json`. It contains the source
   commits, fingerprint, logical version, and runtime artifact hashes. Both URM and Sonatype publish
   this attachment beside the JARs.

The Maven attachment lets GitHub Actions and Blossom verify the source revision without first
downloading every large classifier JAR. The final dual-repository gate still compares each
repository's generated checksum with the staged bytes.

## Success flow

```text
JNI commit B exists on the approved JNI release line
  |
  v
cudf-spark Draft PR changes gitlink A -> B
and includes required downstream adaptations
  |
  v
Pointer Preparation Jenkins Job
  |-- validates B is an upstream JNI commit
  |-- computes the build fingerprint
  |-- cache hit: restores the retained bundle
  `-- cache miss: builds B once and retains the bundle
  |
  v
Staged validation using an isolated Maven repository
  |-- JNI build and tests
  |-- cudf-spark compile/premerge-equivalent checks
  `-- required GPU integration coverage
  |
  v
Finalization gate: PR approved, mergeable, exact head unchanged
  |
  v
Publish the retained bytes to Sonatype and URM
  |
  v
Dual-repository checksum and provenance verification
  |                    |
  v                    v
GitHub Actions         Blossom premerge
Sonatype path          URM path
  |                    |
  `---------+----------'
            v
       PR may merge
            |
            v
cudf-spark main, URM latest, and Sonatype latest all represent B
```

## Failure behavior

### Build or staged test failure

- Do not publish the bundle.
- Do not move the cudf-spark pointer.
- Keep JNI development moving.
- An agent classifies the failure and prepares either a JNI repair or a cudf-spark adaptation on
  the same pointer update branch.

### One repository publication fails

- Retain the original staged bytes.
- Retry only the missing repository with those bytes.
- Never rebuild one side independently.
- Do not allow the PR to merge until both repositories contain matching payloads.

### Published provenance does not match the pointer

- Fail closed with a dependency-transition diagnostic.
- Do not silently rebuild or accept the moving latest artifact.
- If the pull request is behind current main, require an upmerge and rerun.
- If current main itself disagrees, treat the transition as an incident and restore consistency
  using the retained bytes or an explicit rollback PR.

### Pointer PR fails after publication

The normal path publishes only after staged validation and final approval, so the post-publication
window should be short. If final repository-path checks nevertheless fail, the job must not merge.
The release coordinator either republishes the same retained bytes to the failed repository or
restores the previous accepted snapshot. Rebuilding in place is forbidden.

## The non-atomic transition window

GitHub merge, Sonatype metadata, and URM metadata cannot change atomically. Publishing B before
merging its pointer PR briefly makes moving latest newer than main; merging first briefly makes main
newer than moving latest.

The proposed MVP minimizes, but does not pretend to eliminate, this window:

- finalization starts only after the exact PR head is approved, mergeable, and staged-green;
- the publisher uses retained bytes and immediately performs repository verification;
- ordinary premerge checks compare JAR provenance with the checked-out pointer and report a
  dependency transition or required upmerge;
- the pointer PR is merged promptly through the repository's approved merge mechanism;
- affected in-flight builds are rerun after upmerge rather than treated as product failures.

Whether cudf-spark should adopt a merge queue or a narrowly scoped automated merge step to reduce
the window further is an explicit review decision. This Draft does not grant new merge authority.

## Repository changes

### cudf-spark

- Add `thirdparty/cudf-spark-jni` pinned to the matching JNI release line.
- Keep `cudf-spark-jni.version` as the ordinary moving SNAPSHOT in both Scala POM trees.
- Add a deterministic utility that:
  - reads the gitlink without requiring recursive checkout;
  - verifies both POM trees use the same simple SNAPSHOT;
  - verifies a local JAR's embedded JNI revision; and
  - verifies the small Maven provenance attachment from Sonatype or URM.
- Add the Sonatype check to GitHub Actions and the URM check to Blossom premerge. Each path also
  force-refreshes the actual Maven classifier used by the build and checks its embedded JNI SHA;
  repository metadata alone is not accepted as proof of the resolved dependency.
- Include this design document in the pointer-guard PR.

### cudf-spark-jni

- Replace the commit-qualified candidate-version helper with a moving-SNAPSHOT artifact manifest
  helper.
- Validate staged runtime JARs by reading their existing embedded JNI and cuDF revisions.
- Calculate the build fingerprint and per-file SHA-256/SHA-1 checksums.
- Allow `ci/deploy.sh` to attach the provenance JSON as Maven classifier `provenance`.
- Do not change the project's Maven version as part of candidate preparation.

The helper must be available on each actively promoted JNI release line. The main-branch change is
the source change; release-line backports are mechanical rollout work and must land before that
release line uses the new job.

### Blossom Jenkins

- Convert the existing candidate/pin Draft into a pointer-driven preparation/finalization job.
- Extend JNI nightly to support build-and-archive without deploy. Existing scheduled behavior stays
  in compatibility mode until the new flow completes a shadow canary.
- Reuse a retained build only when the caller supplies both its Jenkins build number and the full
  expected fingerprint, and the archived manifest matches both. The durable index that discovers
  that pair is a rollout review item; omitting the pair deliberately takes the cache-miss path.
- Run downstream staged validation from an isolated Maven repository.
- Publish the same bundle to Sonatype and URM only during finalization.
- Verify repository metadata, provenance, and payload checksums.
- Serialize the legacy nightly publisher and pointer finalizer with one Jenkins lock held until
  dual-repository verification completes.
- Trigger/retrigger the normal GitHub Actions and Blossom checks for the exact pointer PR head.
- Never create a second pin PR: the input pointer PR is the promotion request.

## Local developer behavior

After upmerging main, developers continue to run ordinary Maven commands. The POM resolves the
validated release-train SNAPSHOT. Initializing the JNI submodule is unnecessary for an ordinary
cudf-spark build; it is needed only to inspect or build the pinned JNI source.

A developer intentionally changing the pointer must use the preparation workflow or build the JNI
submodule locally and install its JARs into an isolated Maven repository. Direct Maven resolution
cannot select unpublished source merely from the gitlink.

## Automation and agent boundaries

### Pointer Update Agent

- Detects a JNI release-line commit worth testing.
- Prepares the pointer update and downstream compatibility changes.
- Does not publish artifacts or merge the PR.

### Failure Triage Agent

- Watches the preparation job and both downstream CI paths.
- Classifies source compatibility, deterministic test failure, infrastructure failure, and
  repository-transition failure.
- Prepares tested repairs and attaches evidence to the same Draft.

### Transition Monitor Agent

- Detects prolonged disagreement among cudf-spark main, URM provenance, and Sonatype provenance.
- Alerts and recommends retry or rollback from retained bytes.
- Cannot override deterministic gates or repository permissions.

### Jenkins jobs

Jenkins owns trusted checkout, cache lookup, native build, artifact retention, staged validation,
credentialed publication, checksum verification, and CI triggering. Pass/fail and provenance
decisions are deterministic and are not delegated to an LLM.

## Rollout

1. Merge no behavior by default: retain the current nightly publication path while the new jobs are
   opt-in and dry-run first.
2. Land the JNI manifest/deploy contract on main and the active release line.
3. Land the cudf-spark submodule and provenance checks without changing the POM version.
4. Land the Blossom preparation/finalization job with publication and merge disabled by default.
5. Run a shadow cache-miss build and staged downstream test; retain its manifest and timings.
6. Run one approved dual-publication canary and verify both repository paths.
7. Enable pointer-driven publication for one release train.
8. Disable independent dev-SNAPSHOT publication only after the pointer flow has demonstrated
   recovery, rollback, and acceptable transition time.

## Acceptance criteria

- A new JNI pointer produces exactly one trusted native build on cache miss.
- Cache hit and cache miss feed identical downstream stages.
- No artifact is published before staged downstream validation passes.
- URM and Sonatype provenance both report the exact gitlink JNI SHA.
- The actual Maven classifier resolved into each CI cache embeds that same JNI SHA.
- Required runtime classifier checksums match the retained staged bundle.
- A failed pointer PR leaves cudf-spark main on the previous pointer.
- An ordinary up-to-main PR does not build JNI from source.
- Direct Maven builds continue to use the release-train moving SNAPSHOT.
- The finalization job cannot publish an unapproved, changed, or unmergeable PR head.
- A transition mismatch produces an actionable upmerge/transition diagnostic.

## Review decisions still required

1. Which exact cudf-spark checks form the pre-publication staged gate?
2. Where should retained bundles and fingerprint indexes live, and for how long?
3. Should finalization use the existing merge mechanism, a merge queue, or remain a human-triggered
   publish-then-merge operation?
4. What maximum transition window is acceptable before automatic rollback?
5. Which release branches receive the initial JNI helper backport?
6. Which actor or label is allowed to request a credentialed pointer finalization build?
