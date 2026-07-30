---
layout: page
title: Compute Sanitizer
nav_order: 7
parent: Developer Overview
---

# Using Compute Sanitizer with the NVIDIA cuDF plugin for Apache Spark

Compute Sanitizer is a functional correctness checking suite included in the CUDA toolkit.
This suite contains multiple tools that can perform different type of checks. Of main interest to
the cuDF plugin is the `memcheck` tool that is capable of precisely detecting and
attributing out of bounds and misaligned memory access errors in CUDA applications.

To use Compute Sanitizer with a single worker, you can start the worker with `compute-sanitizer`
in front of the worker start command.

Here is an example that starts up a worker in standalone mode, sanitizes it and the shell until
the shell exits (using Ctrl+D) while stopping the worker process at the end.

```base
compute-sanitizer --target-processes all bash -c " \
${SPARK_HOME}/sbin/start-worker.sh $master_url & \
$SPARK_HOME/bin/spark-shell; \
${SPARK_HOME}/sbin/stop-worker.sh"
```

To use Compute Sanitizer in a distributed Spark standalone cluster, follow these
steps:
  * Create a "fake" Java home, for example, in `/opt/compute-sanitizer-java`:
```bash
mkdir -p /opt/compute-sanitizer-java/bin
```
  * Create a "fake" java binary:
```bash
echo 'compute-sanitizer java "$@"' > /opt/compute-sanitizer-java/bin/java
chmod +x /opt/compute-sanitizer-java/bin/java
```
  * When launching jobs, set the following Spark configuration:
```bash
--conf spark.executorEnv.JAVA_HOME="/opt/compute-sanitizer-java"
```

Now each executor will use the "fake" java binary instead of the real one, and run under
`compute-sanizer`. The executor `stdout` may produce errors like these:
```console
========= COMPUTE-SANITIZER
========= Program hit invalid device context (error 201) on CUDA API call to cuCtxGetDevice.
=========     Saved host backtrace up to driver entry point at error
=========     Host Frame: [0x24331b]
=========                in /lib/x86_64-linux-gnu/libcuda.so.1
=========     Host Frame:uct_cuda_base_query_devices_common [0x6209]
=========                in /usr/lib/ucx/libuct_cuda.so.0.0.0
=========     Host Frame:uct_md_query_tl_resources [0x12976]
=========                in /usr/lib/libuct.so.0.0.0
=========     Host Frame: [0x17b17]
=========                in /usr/lib/libucp.so.0.0.0
=========     Host Frame: [0x191c0]
=========                in /usr/lib/libucp.so.0.0.0
=========     Host Frame: [0x19674]
=========                in /usr/lib/libucp.so.0.0.0
=========     Host Frame:ucp_init_version [0x1a4e3]
=========                in /usr/lib/libucp.so.0.0.0
=========     Host Frame:Java_org_openucx_jucx_ucp_UcpContext_createContextNative [0x38ff]
=========                in /tmp/jucx4889414997471006264/libjucx.so
=========     Host Frame: [0x7fdbc9017da7]
=========                in
```

# Enable Line Numbers

To help with debugging, you can build `libcudf` with line numbers enabled:
```bash
cmake .. <other options> -DCUDA_ENABLE_LINEINFO=ON
```
