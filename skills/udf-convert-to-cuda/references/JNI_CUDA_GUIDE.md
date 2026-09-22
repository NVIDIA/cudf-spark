<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: CC-BY-4.0
-->

# JNI and CUDA RapidsUDF Guide

## RapidsUDF Contract

The RapidsUDF interface provides a way to run a CPU UDF on the GPU when using the RAPIDS Accelerator for Apache Spark. The interface provides a single method you need to override called `evaluateColumnar`. The CPU UDF method must remain on the native RapidsUDF class so Spark can fall back to the CPU if a surrounding plan cannot run on the GPU.

`evaluateColumnar(int numRows, ColumnVector... args)` receives columnar forms of the same inputs as the CPU UDF. All input columns should have `numRows` rows. Scalar inputs may be expanded into full columns by the RAPIDS Accelerator, so do not rely on detecting scalar-vs-column input.

The returned `ColumnVector` must have `numRows` rows and a cuDF type that matches the Spark return type:

| Spark Type | cuDF Type |
|---|---|
| BooleanType | BOOL8 |
| ByteType | INT8 |
| ShortType | INT16 |
| IntegerType | INT32 |
| LongType | INT64 |
| FloatType | FLOAT32 |
| DoubleType | FLOAT64 |
| DecimalType | DECIMAL32, DECIMAL64, DECIMAL128 * |
| DateType | TIMESTAMP_DAYS |
| TimestampType | TIMESTAMP_MICROSECONDS |
| StringType | STRING |
| ArrayType | LIST of element type |
| MapType | LIST of STRUCT(key, value) |
| StructType | STRUCT of fields |

For example, if the CPU UDF returns the Spark type ArrayType(MapType(StringType, StringType)) then evaluateColumnar must return a column of type LIST(LIST(STRUCT(STRING,STRING))).

*Note: cuDF's DECIMAL32 corresponds to precision <= 9 digits, DECIMAL64 corresponds to 9 < precision <= 18 digits, and DECIMAL128 corresponds to 18 < precision <= 38 digits. Precision greater than 38 digits is unsupported.
Note that cuDF decimals use a negative scale relative to Spark DecimalType. For example, Spark DecimalType(precision=11, scale=2) would translate to cuDF type DECIMAL64(scale=-2).

For `ArrayType(elementType, containsNull)`, the LIST parent null mask represents null arrays. Child nulls represent null array elements and must match the `containsNull` contract. Either preserve child nulls deliberately or reject them explicitly.

## Java Wrapper

Use `NativeDepsLoader.loadNativeDeps(new String[] {"rapidsudfjni"})` from a synchronized loader. Call it from `evaluateColumnar`, not a static initializer, because the Spark driver may not have the executor CUDA runtime.

Pass input columns to JNI with `ColumnVector.getNativeView()`. Wrap the native result with `new ColumnVector(nativeHandle)`.

Do not close input `ColumnVector`s. The RAPIDS Accelerator owns them. Closing inputs can cause double-close errors.

## JNI and Native Ownership

JNI arguments are non-owning pointers:
```cpp
auto input = reinterpret_cast<cudf::column_view const*>(j_input);
```

The native function must allocate and return an owning `cudf::column`:
```cpp
std::unique_ptr<cudf::column> result = compute(*input);
return reinterpret_cast<jlong>(result.release());
```

Never return a pointer to an input view, child view, stack object, or a column owned by a temporary that will be destroyed before Java wraps it.

Catch `std::bad_alloc`, `std::invalid_argument`, and `std::exception`, then throw Java exceptions with `JNIEnv::ThrowNew`.

## CUDA/libcudf Implementation

Start with libcudf column APIs before writing custom kernels. Use custom CUDA kernels when the operation requires fused logic, custom reductions, or logic unavailable in cuDF Java/libcudf primitives.

### Checklist

- Validate input types and row counts in Java before crossing JNI when possible
- Validate libcudf types again in JNI for native safety
- Preserve Spark null semantics
- Prefer `cudf::column_view`/`cudf::lists_column_view` for input views
- Return `std::unique_ptr<cudf::column>`
- Avoid host copies in the final implementation
- Prefer public libcudf APIs; avoid using `cudf::detail`
- Keep one native function focused on one UDF operation

### Correctness Pitfalls

- **Null values of fixed-width columns are undefined memory.** Check the null mask (`cudf::bit_is_set(...)` or `column_device_view::is_valid(...)`) before reading element values.
- **Empty list/string columns have no offsets.** Accessing the offsets child of an empty list or string column is undefined behavior. Handle the empty case early (e.g., return `cudf::make_empty_column(...)`).
- **Use `cudf::have_same_types(a, b)` for type comparison**, not `a.type() == b.type()` — equality misses differences such as decimal scale.
- **`cudf::size_type` is `int32_t`. LIST offsets are always `int32_t`.** String offsets may be `int32_t` or `int64_t` for large strings.
- **Nested column null masks must agree across levels.** When constructing LIST/STRUCT output yourself, ensure parent and child null masks are consistent.
- **`CUDF_EXPECTS` conditions must be pure predicates** — side effects inside the condition may only execute in debug builds.

### Useful Patterns

- `rmm::device_uvector<T>`: temporary device output buffers that can be released into a `cudf::column`
- `rmm::exec_policy_nosync(stream)`: pass the intended CUDA stream to Thrust algorithms (prefer the `_nosync` variant unless you need an implicit host-device sync)
- `cudf::make_empty_column(...)`: return correctly typed empty outputs
- `cudf::make_numeric_column(...)`: allocate fixed-width output columns with a null mask
- `cudf::bitmask_and(cudf::table_view({...}))`: combine input validity masks for output null semantics
- `cudf::lists_column_view`: inspect list offsets, child columns, parent null masks, and nested list shapes
- `cudf::strings_column_view`: inspect string chars/offsets when implementing string kernels
- `cudf::create_null_mask(...)`: create all-valid, all-null, or uninitialized masks for new outputs
- CUB and Thrust APIs: useful for scans, reductions, transforms, selection, and sorting when libcudf does not provide the exact operation

### Memory Allocation

- All device allocations must go through the active RMM memory resource.
- Use libcudf factories or RMM types such as `rmm::device_uvector<T>` and `rmm::device_buffer`; avoid direct calls to `cudaMalloc`, `cudaMallocAsync`, or other ad hoc device allocators.
- Use the output MR for returned columns when the API exposes one; use `cudf::get_current_device_resource_ref()` for short-lived temporary buffers.
- Use RMM pinned memory for large host buffers. Small CPU-only metadata may use normal C++ containers.

Example allocating CUB scratch buffers through RMM:

```cpp
size_t temp_storage_bytes = 0;
cub::DeviceScan::InclusiveSum(nullptr, temp_storage_bytes, in, out, n, stream.value());
rmm::device_buffer temp_storage(temp_storage_bytes, stream, cudf::get_current_device_resource_ref());
cub::DeviceScan::InclusiveSum(temp_storage.data(), temp_storage_bytes, in, out, n, stream.value());
```

### Stream and MR Plumbing

Top-level native functions should accept stream and MR as the last two parameters, with defaults:

```cpp
std::unique_ptr<cudf::column> my_op(
    cudf::column_view const& input,
    rmm::cuda_stream_view stream      = cudf::get_default_stream(),
    rmm::device_async_resource_ref mr = cudf::get_current_device_resource_ref());
```

Use the passed-in `mr` for the returned column and `cudf::get_current_device_resource_ref()` for short-lived temporaries. Propagate `stream` to every libcudf call, Thrust call, and kernel launch — do not introduce `rmm::cuda_stream_default` inside the implementation.

### Kernel Launch Discipline

Always check kernel launches; silent launch failures cause downstream corruption.

```cpp
my_kernel<<<grid, block, 0, stream.value()>>>(args);
CUDF_CHECK_CUDA(stream.value());
```

Prefer `cuda::std::` (e.g. `cuda::std::min`, `cuda::std::sqrt`, `cuda::std::numeric_limits<T>`) over `std::` inside `__device__` and `CUDF_HOST_DEVICE` code.

Avoid synchronizing in the hot path except when required to fetch output sizes or while debugging.

### Output Construction

For variable-size list outputs:
1. Compute per-row child sizes on device, using zero for null parent rows.
2. Prefix-sum sizes into an `INT32` offsets column of length `numRows + 1`.
3. Allocate the child column from the final offset, fill it on device, and set child nulls if `containsNull=true`.
4. Assemble the LIST column from offsets, child column, parent null mask, and parent null count.

For string outputs, construct proper offsets, chars, and null masks. For scalar numeric outputs, prefer libcudf transforms/reductions where possible.

## Debugging

Rerun tests with `-Ddebug.memory.leaks=true` to enable Java refcount debugging; this catches leaked `ColumnVector`, `Table`, `Scalar`, and Java-owned buffer objects.
Note that it does **not** catch native memory leaks; use RMM RAII patterns to ensure all native allocations are freed.

For native kernel memory errors, run the comparison test under Compute Sanitizer:

```bash
compute-sanitizer --tool memcheck mvn test <flags>
```
