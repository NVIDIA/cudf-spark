<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: CC-BY-4.0
-->

# cuDF Microbenchmarks

Measures fine-grained CPU vs. GPU performance without Spark overhead on in-memory data.

## Contents
- [ ] Implement MicroBenchRunner
- [ ] Run microbenchmarks

## Implement MicroBenchRunner

Fill in the three TODO methods following the docstrings.

## Run Microbenchmarks

Generate data first (reuse from GenData output), then run:
```bash
./run_micro_benchmark.sh --mode all --data-path data/bench_data_<rows>_rows.parquet --rows <rows>
```

Note that the specified number of rows will be coalesced into a single cuDF table.
A large table size (>1GB) will demonstrate better GPU performance.

## Next Steps

To profile and iteratively optimize GPU performance, use the **udf-optimize-cudf** skill.
