---
layout: page
title: Memory Debugging
nav_order: 10
parent: Developer Overview
---

## Memory Leak Debugging

### Detecting Leaks
The JVM uses a garbage collector that is generally triggered based off of the JVM heap running out
of free space. This means that if we rely on GC to free unused GPU memory or off heap memory we
are going to effectively leak that memory. This is because the JVM's GC will not be triggered if
we run out of GPU memory or off heap memory. Instead, we make everything that can hold a reference
to memory that is not tracked directly by the JVM an
[AutoClosable](https://docs.oracle.com/javase/8/docs/api/java/lang/AutoCloseable.html). In many
cases we extend this to include reference counting too. But all of this results in a lot of
complexity and potentially bugs. To deal with these bugs inside the CUDF java code we
[track](https://github.com/rapidsai/cudf/blob/main/java/src/main/java/ai/rapids/cudf/MemoryCleaner.java)
each of these objects and if the garbage collector causes the memory to be freed instead of a proper
close we will output a warning like the following in the logs.

```
ERROR DeviceMemoryBuffer: A DEVICE BUFFER WAS LEAKED (ID: 5 30a000200)
ERROR HostMemoryBuffer: A HOST BUFFER WAS LEAKED (ID: 6 7fb62b07f570)
```

There are different messages for different resources that have different amounts of information.
Memory buffers generally will include the type of buffer that leaked, an ID for the buffer, and
the last number is a HEX representation of the physical address. These are here to help identify
the same buffer in other contexts.

In the case of a `ColumnVector`, or a `HostColumnVector` the messages are slightly different.

```
ERROR ColumnVector: A DEVICE COLUMN VECTOR WAS LEAKED (ID: 15 7fb5f94d8fa0)
ERROR HostColumnVector: A HOST COLUMN VECTOR WAS LEAKED (ID: 19)
```

Here the ID can still be used for cross-referencing, in some cases. But the host column vector
has no address. That is because it is made up of one or more `HostMemoryBuffer`, and it is here
to help debug why one of them leaked as it holds references to them. A `ColumnVector` includes
an address, which is the address of the C++ `cudf::column` that actually holds the memory at
the C++ level. We generally don't reach into the memory held by a `cudf::column` directly, but
this allows us to track it.

Sadly there are some resources that we currently don't track. The biggest one is `Table`. `Table`
does not directly allocate any device memory, but it references one or more C++ class instances
off heap.

### Debugging Leaks

Once a leak is detected we can turn on a special system property on the command line,
`-Dai.rapids.refcount.debug=true`, to get a lot more information about the leak. We encourage
you to also enable assertions in these cases too, `-ea`. The reason this is off by default is that
it keeps track of the stack traces each time a buffer or column had its reference count
incremented or decremented. Getting a stack trace is an expensive operation, and it can also
use up a lot of the JVM heap, so we have it off by default. The output of this looks like

```
ERROR ColumnVector: A DEVICE COLUMN VECTOR WAS LEAKED (ID: 7 7f756c1337e0)
23/12/27 20:21:32 ERROR MemoryCleaner: Leaked vector (ID: 7): 2023-12-27 20:21:20.0002 GMT: INC
java.lang.Thread.getStackTrace(Thread.java:1564)
ai.rapids.cudf.MemoryCleaner$RefCountDebugItem.<init>(MemoryCleaner.java:336)
ai.rapids.cudf.MemoryCleaner$Cleaner.addRef(MemoryCleaner.java:90)
ai.rapids.cudf.ColumnVector.incRefCountInternal(ColumnVector.java:298)
ai.rapids.cudf.ColumnVector.<init>(ColumnVector.java:120)
```

or

```
ERROR MemoryCleaner: double free ColumnVector{rows=6, type=INT32, nullCount=Optional[0], offHeap=(ID: 15 0)} (ID: 15): 2023-12-27 20:12:34.0607 GMT: INC
java.lang.Thread.getStackTrace(Thread.java:1564)
ai.rapids.cudf.MemoryCleaner$RefCountDebugItem.<init>(MemoryCleaner.java:336)
ai.rapids.cudf.MemoryCleaner$Cleaner.addRef(MemoryCleaner.java:90)
...
2023-12-27 20:12:34.0607 GMT: DEC
java.lang.Thread.getStackTrace(Thread.java:1564)
ai.rapids.cudf.MemoryCleaner$RefCountDebugItem.<init>(MemoryCleaner.java:336)
ai.rapids.cudf.MemoryCleaner$Cleaner.delRef(MemoryCleaner.java:98)
ai.rapids.cudf.ColumnVector.close(ColumnVector.java:262)
...
2023-12-27 20:12:34.0607 GMT: DEC
java.lang.Thread.getStackTrace(Thread.java:1564)
ai.rapids.cudf.MemoryCleaner$RefCountDebugItem.<init>(MemoryCleaner.java:336)
ai.rapids.cudf.MemoryCleaner$Cleaner.delRef(MemoryCleaner.java:98)
ai.rapids.cudf.ColumnVector.close(ColumnVector.java:262)
...
```

Here an `INC` indicates that the reference count was incremented and a `DEC` indicates that it
was decremented. With the stack traces we can walk through the code and try to line up which `DEC`s
were supposed to go with which `INC`s and hopefully find out where something went wrong. Like a
double free or a leak.

### Bookkeeping each thread's memory allocation

Sometimes we'll encounter exceptions like "com.nvidia.spark.rapids.jni.CpuSplitAndRetryOOM:
CPU OutOfMemory: could not split inputs and retry." This is because the CPU memory is exhausted and
they cannot be spilled. But theoretically, we have a OOM state machine to fallback most of the
threads to a state where everything is spillable. So such kind of exception is not indeed expected.
In such cases, we really want to know the status of each thread's memory allocation.
We can enable the following system property:

```
-Dai.rapids.memory.bookkeep=true
```

or

```
-Dai.rapids.memory.bookkeep=true
-Dai.rapids.memory.bookkeep.callstack=true
```

The first option will log how much CPU is allocated by each thread but not freed, at the time
when the OOM state machine decides that one of the threads need to be spitted and retried, which
is not very often and usually means something went wrong. The second option will take a step
further and log where each piece of memory is allocated. This is very useful for debugging, but
it will generate a lot of overhead in bookkeeping, so it's not recommended to enable it in
production.

## Low Level GPU Allocation Logging.

The leak/double free detection and debugging is great. But it does not give us visibility into
what is actually happening with memory allocation. It does not let us see leaks at the C++ level.
These should be very rare, but we have found them in the past. It also does not let us see things
like fragmentation when we are using a pooling allocator. RMM has logging that we can enable at
startup to help us see at a very low level what is happening. `spark.rapids.memory.gpu.debug` can
be set to either `STDERR` or `STDOUT` to see everything that is happening with the allocation.

```
1979932,14:21:19.998533,allocate,0x30a000000,256,0x2
1980043,14:21:32.661875,free,0x30a000000,256,0x2
```

The format of this is not really documented anywhere, but it uses the
[logging_resource_adaptor](https://github.com/rapidsai/rmm/blob/main/cpp/include/rmm/mr/logging_resource_adaptor.hpp)
to log when an allocation succeeded or failed and when memory was freed. The current format
appears to be.

```
Thread,Time,Action,Pointer,Size,Stream
```

  * `Thread` is the thread ID (C++ not java)
  * `Time` is the time in a 24-hour format including micro second level granularity
  * `Action` is one of `allocate`, `allocate failure`, or `free`.
  * `Pointer` is the address of the pointer, except for a failure where it is null.
  * `Size` is the size of the allocation
  * `Stream` is the CUDA stream that the operation happened on.

Be aware that there are a lot of things to keep in mind here. Our spill framework often will
wait for an allocation to fail before it starts to spill, so seeing a `allocate failure` does
not mean that something terrible happened. `Size` is the size for the allocation at this point.
Because RMM allows for different layers of code in between the call to RMM and the logging the
size might be different from what the user actually requested. RMM often will try and "align" the
allocation. Meaning round it up to a multiple of a set number of bytes. We try to get the logger as
close to the actual allocator as possible. But just be careful.

Also know that the address here should correspond to the address in the leak debugging if and only
if it was a `DeviceMemoryBuffer` that was allocated. In this case, where it is a `cudf::column` the
pointers will not line up.

Also be aware that it is possible for us to logically slice a buffer. So you might see a large
allocation, and then have buffers that show up in java that reference a subsection of that original
allocation.

This cannot answer a lot of questions directly, but we can write some simple scripts to parse
the log and answer questions. We can answer questions like "What is the amount of GPU memory
allocated at any point in time?" With that we can look to see when an allocation failed and get
an idea of how bad fragmentation is by seeing how large the allocation is vs the amount of memory
that should be available on the GPU. We could go even further and track the address and size of
each allocation to see what is the maximum available buffer size. But that would assume that we
are using a pooling allocator like `arena`, which cannot modify the virtual memory tables to help
fix fragmentation. We can look for memory leaks. Any address where the allocation and free calls
don't cancel each other out. You should filter out null pointers and zero size allocations as you
don't need to free a nullptr if the allocation size was 0.

### Missing Functionality

We are still missing some functionality that would be helpful with debugging issues in addition to
tracking [Scalar](https://github.com/rapidsai/cudf/issues/8227) and
[Table](https://github.com/rapidsai/cudf/issues/14677) values.

We don't have any [host memory logging](https://github.com/NVIDIA/cudf-spark/issues/10102) like
we do for RMM. This might change when/if we go to RMM for host memory allocation too, but for now
if you need this you probably are going to have to write something yourself.

We don't have a good way to [track spill](https://github.com/NVIDIA/cudf-spark/issues/8752)/log
what is or is not spillable. The spill framework typically will use reference counting to know
when it is the only one holding a reference to memory and then make the data spillable at that
point.  This means that if we don't get the reference counting right we end up never spilling

We also don't have a way to
[log exactly what was spilled](https://github.com/NVIDIA/cudf-spark/issues/10103)
and what was read back. We can probably guess that this is happening from other logs, but it
would be really nice to have a way to actually capture it.
