---
layout: page
title: GPU Core Dumps
nav_order: 9
parent: Developer Overview
---
# GPU Core Dumps

## Overview

When the GPU segfaults and generates an illegal access exception, it can be difficult to know
what the GPU was doing at the time of the exception. GPU operations execute asynchronously, so what
the CPU was doing at the time the GPU exception was noticed often has little to do with what
triggered the exception. GPU core dumps can provide useful clues when debugging these errors, as
they contain the state of the GPU at the time the exception occurred on the GPU.

The GPU driver can be configured to write a GPU core dump when the GPU segfaults via environment
variable settings for the process. The challenges for the NVIDIA cuDF plugin for Apache Spark use
case are getting the environment variables set on the executor processes and then copying the GPU
core dump file to a distributed filesystem after it is generated on the local filesystem by the
driver.

## Environment Variables

The following environment variables are useful for controlling GPU core dumps. See the
[GPU core dump support section of the CUDA-GDB documentation](https://docs.nvidia.com/cuda/cuda-gdb/index.html#gpu-core-dump-support)
for more details.

### `CUDA_ENABLE_COREDUMP_ON_EXCEPTION`

Set to `1` to trigger a GPU core dump on a GPU exception.

### `CUDA_COREDUMP_FILE`

The filename to use for the GPU core dump file. Relative paths to the process current working
directory are supported. The pattern `%h` in the filename will be expanded to the hostname, and
the pattern `%p` will be expanded to the process ID. If the filename corresponds with a named pipe,
the GPU core dump data will be written to the named pipe by the GPU driver.

### `CUDA_ENABLE_LIGHTWEIGHT_COREDUMP`

Set to `1` to generate a lightweight core dump that omits the local, shared, and global memory
dumps. Disabled by default. Lightweight core dumps still show the code location that triggered
the exception and therefore can be a good option when one only needs to know what kernel(s) were
running at the time of the exception and which one triggered the exception.

### `CUDA_ENABLE_CPU_COREDUMP_ON_EXCEPTION`

Set to `0` to prevent the GPU driver from causing a CPU core dump of the process after the GPU
core dump is written. Enabled by default.

### `CUDA_COREDUMP_SHOW_PROGRESS`

Set to `1` to print progress messages to the process stderr as the GPU core dump is generated. This
is only supported on newer GPU drivers (e.g.: those that are CUDA 12 compatible).

## YARN Log Aggregation

The log aggregation feature of YARN can be leveraged to copy GPU core dumps to the same place that
YARN collects container logs. When enabled, YARN will collect all files in a container's log
directory to a distributed filesystem location. YARN will automatically expand the pattern
`<LOG_DIR>` in a container's environment variables to the container's log directory which is useful
when configuring `CUDA_COREDUMP_FILE` to place the GPU core dump in the appropriate place for
log aggregation. Note that YARN log aggregation may be configured to have relatively low file size
limits which may interfere with successful collection of large GPU core dump files.

The following Spark configuration settings will enable GPU lightweight core dumps and have the
core dump files placed in the container log directory:

```text
spark.executorEnv.CUDA_ENABLE_COREDUMP_ON_EXCEPTION=1
spark.executorEnv.CUDA_ENABLE_LIGHTWEIGHT_COREDUMP=1
spark.executorEnv.CUDA_COREDUMP_FILE="<LOG_DIR>/executor-%h-%p.nvcudmp"
```

## Simplified Core Dump Handling

There is rudimentary support for simplified setup of GPU core dumps in the cuDF plugin.
This currently only works on Spark standalone clusters, since there is currently no way for a driver
plugin to programmatically override executor environment variable settings for Spark-on-YARN or
Spark-on-Kubernetes. In the future with a driver that is compatible with CUDA 12.1 or later,
the cuDF plugin could leverage GPU driver APIs to programmatically configure GPU core dump
support on executor startup.

To enable the simplified core dump handling, set `spark.rapids.gpu.coreDump.dir` to a directory to
use for GPU core dumps. Distributed filesystem URIs are supported. This leverages named pipes and
background threads to copy the GPU core dump data to the distributed filesystem. Note that anything
that causes early, abrupt termination of the process such as throwing from a C++ destructor will
often terminate the process before the dump write can be completed. These abrupt terminations should
be fixed when discovered.
