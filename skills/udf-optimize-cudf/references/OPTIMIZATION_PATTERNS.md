<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: CC-BY-4.0
-->

# cuDF Optimization Patterns

## Guidelines

- **Rule of thumb:** fewer cuDF API calls typically results in better performance. Look for ways to collapse multiple operations into fewer calls.
- Explore the cuDF repo `java/src/<main|test>/java/ai/rapids/cudf` to find alternative cuDF methods.

## Profiling Signals

| Signal | Where to look | What it means | Action |
|---|---|---|---|
| High invocation count | `nvtx_sum` Instances column | Loops or too many small kernels | Batch into fewer calls |
| Low GPU utilization | `kernel_time / wall_time` | Launch/memory overhead dominates | Reduce total API calls |
| Many `make_*_column` calls | `nvtx_sum` | Excessive intermediate columns | Shorten transformation chains |
| Expensive kernel | `nvtx_sum` | Look for cheaper API (e.g., regex &rarr; stringReplace, stringSplitRecord &rarr; stringSplit) | Swap to cheaper cuDF API |
| GPU slower than CPU at large scale | Speedup results | Algorithm has serial dependencies that don't parallelize well | Rethink overall algorithm to maximize columnar parallelism and reduce divergence |
| Many gather/scatter or struct unpacking ops | `nvtx_sum` | Non-contiguous memory access patterns | Use APIs that leverage contiguous access (e.g., operate on cuDF child columns directly) |
