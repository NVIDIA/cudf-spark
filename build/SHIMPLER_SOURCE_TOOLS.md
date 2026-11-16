<!--
Copyright (c) 2026, NVIDIA CORPORATION.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Shared Shimpler source tools

The four Apache-licensed `shimpler_source_*.py` modules in this directory are
vendored unchanged from the `shimpler-source-tools` directory of public
`NVIDIA/spark-rapids-common` at merged PR #64 commit
`2dab44f16f04e96471f6ab63d86fd2495f54bf5c`.

The public POMs no longer include the old Databricks targets. The
`shimpler_source_targets.py` allowlist is empty, so unknown target names fail
validation. External names cannot be inheritance roots.

The copies are checked in so Maven generation remains offline. Update all four
from one reviewed common commit, update `shimpler_source_tools.sha256`, then
verify `python3 -m unittest discover -s build/tests -p 'test_shimpler*.py'`,
full source-set parity across both Scala POM graphs, and the required clean
multi-shim build before publishing the consumer change.

This repository retains its own Ant adapter, Maven profiles, and build-version
policy. The shared modules contain no repository-specific version matrix.
