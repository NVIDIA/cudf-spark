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

# Spark installation dependency generator

`thirdparty/spark-rapids-common` is a submodule pinned to a compatible commit of
`NVIDIA/spark-rapids-common`. The Databricks build runs
`spark-home-deps/gen-spark-home-deps.py` from that checkout.
`build/prepare-common-submodule.sh` initializes the pinned commit when needed.
The Databricks build runs it locally, and CI runs it before copying source to a node.
Release branches should pin their own tested common commit. Updating the pin
requires an explicit consumer change; a build does not follow common's `main`.
