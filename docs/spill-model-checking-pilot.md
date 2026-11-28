# Spill Model Checking Pilot

## Scope

Check one `SpillableDeviceBufferHandle` with two actors: a spiller and a caller that may materialize or close the handle. The first implementation covers device-to-host spilling. Host-to-disk and direct device-to-disk spilling follow after the initial model and replay harness are validated.

The model uses a finite set of lifecycle events: acquire and release a materialized reference, start and complete a host copy, publish the host handle, close the device handle, synchronize the device, and release the original device resource. It records whether the handle is open, whether a spill is active, where a recoverable copy exists, and which references remain live. GPU work completion is an explicit event rather than an assumption about host-thread ordering.

## Milestones

1. Specify legal transitions and observable outcomes independently from the implementation. Bound the first search to one handle, two actors, one outstanding materialized reference, and one spill attempt.
2. Add a test-local Scala state explorer that enumerates enabled actions, deduplicates states, and reports the shortest trace to an invariant violation. Check that an intentionally invalid transition is rejected.
3. Check safety properties: no release while a borrower or GPU reader can still use the device allocation; a successful spill leaves a recoverable copy; device release occurs at most once; close ultimately releases owned resources after in-flight work finishes. State any fairness assumption separately from safety.
4. Add controlled tests against the actual spill framework at the copy, publication, and pre-release boundaries. Compare outcomes with the model using representative generated schedules, including close during spill and materialize during spill. Use the existing spill test fixture and targeted test suite.
5. Evaluate the model with cudf-spark. Export the finite state graph as `State(state_id)` and `Edge(source_id, target_id)` relations, and evaluate a first CTL subset with Spark DataFrames while the RAPIDS plugin executes eligible joins and set operations on the GPU. Start with `EX`, `E[p U q]`, and `AG p`. Compare every satisfying-state set with the test-local CPU explorer, and inspect the physical plan and GPU metrics to verify that evaluation actually used cudf-spark. Keep graph generation and the reference result independent from the GPU evaluator.
6. Extend the model and replay tests to host-to-disk and direct device-to-disk paths, including spill-file cleanup. Treat disk-full and copy failures according to the framework's documented terminal-failure contract.

## GPU Evaluation Design

The first GPU evaluator uses iterative predecessor joins to compute least fixed points. For `E[p U q]`, start with states satisfying `q`, then add states satisfying `p` that have an edge to the current set until no new states appear. `EX p` is one predecessor join. `AG p` is the complement of `E[true U not p]`. The Spark driver controls iteration; the physical plan must show which relational operations run on the GPU. Add universal-until after the first operators agree with the CPU reference.

Represent initial states separately from transitions. Define deadlock semantics before evaluating CTL, such as adding self-loops to terminal states, and use the same semantics in both evaluators. Check convergence and satisfying-state sets, not just the Boolean answer at the initial state. Test the query engine on graphs large enough to exercise its GPU path while retaining small counterexample graphs for debugging.

## Acceptance

- The model and GPU evaluation run as targeted tests without timing-based sleeps.
- The explorer terminates within its stated bounds and returns a reproducible shortest counterexample when an invariant is broken.
- Replay tests exercise the real GPU spill path and verify both contents and resource lifetime.
- The pilot reports its bounds and does not claim coverage of unmodeled CUDA behavior or the complete Spark executor.
- The GPU evaluation milestone reports which operators ran on the GPU, agrees with the CPU reference for the supported CTL formulas, and clearly identifies any CPU fallback.

## Pilot Result

Milestones 1–3 are implemented in `SpillLifecycleModelSuite`. The state explorer checks one handle, one spill attempt, one borrower, and one GPU read event. It explores all enabled transitions within those bounds and retains a shortest trace for each reached state. The GPU read completion is abstract, not a delayed CUDA kernel.

Milestone 5 is implemented in `SpillGpuModelCheckingSuite`. It exports the explorer's reachable states and transitions as Spark relations, adds self-loops to terminal states, and evaluates `EX`, `E[p U q]`, and `AG p`. A separate Scala set-based evaluator checks the satisfying-state set for each formula, including an `AG` property that fails at the initial state and a safety property that holds there. The Spark driver controls fixed-point convergence and transfers the current state-ID set between iterations; predecessor and allowed-state joins, union, distinct, and the `AG` complement execute as DataFrame queries. The test checks that the expected joins, union, and aggregation run on the GPU. RDD input scans and final result collection remain on the CPU; this is not a fully device-resident model checker. Larger generated graphs and operator-by-operator performance measurements remain future work.

A narrow part of milestone 4 is implemented by `SpillReleaseConformanceTrace`. A latch pauses the real device-buffer spill after host publication but before `postSpill` synchronization. The test records actual `spilling`, host ownership, handle device-reference ownership, and close state before and after a concurrent close, then resumes the store's existing synchronization. It retains the publication event even if close clears the host field. `SpillGpuModelCheckingSuite` checks `AG` over this observed four-state path with both CPU and GPU evaluators. On the unfixed model base, the path yields `StartSpillAndPublishHost, Close` as a release-order counterexample; on the independent fix at `cb254e9549`, the handle reference survives until synchronization and the same property holds. Set `SPILL_CONFORMANCE_EXPECTED=unsafe` or `safe` when running the test on those respective revisions to assert the expected outcome.

This is conformance evidence for one forced real-code schedule, not exhaustive extraction of the production transition system. `handleOwnsDeviceRef` means the handle's reference exists, not that the physical allocation is alive or a native kernel is still using it. The bounded Scala lifecycle model still represents intended behavior; the observed trace is a separate graph. The associated device-release fix remains on a separate branch. Other close/materialize schedules, copy and disk failures, and milestone 6 remain outside this conformance slice.

Validation: `mvn -B -ntp -pl tests -am -Dbuildver=350 -DwildcardSuites=com.nvidia.spark.rapids.spill.SpillLifecycleModelSuite,com.nvidia.spark.rapids.spill.SpillGpuModelCheckingSuite package` exercises the model and GPU evaluator.
