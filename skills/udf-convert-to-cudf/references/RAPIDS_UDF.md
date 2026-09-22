<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: CC-BY-4.0
-->

# Background: RAPIDS Accelerated UDFs

These instructions document how to implement a GPU version of an existing CPU UDF using the RapidsUDF interface. The RapidsUDF interface provides a way to run a CPU UDF on the GPU when using the RAPIDS Accelerator for Apache Spark.

## Implementation

The original CPU implementation is in the `evaluate` method. To make a UDF run on the GPU, you must implement the RapidsUDF interface, which provides a single method you need to override called `evaluateColumnar`. The `evaluateColumnar` function should use pre-existing cuDF methods from the [Java APIs of RAPIDS cudf](https://docs.rapids.ai/api/cudf-java/legacy) to perform the UDF computation by operating on cudF ColumnVectors.

Note that you must keep both CPU and GPU evaluate methods, so that the UDF will still work if a higher-level operation involving the Rapids UDF falls back to the CPU.

Refer to examples/ for example RapidsUDF implementations.

## Interpreting Inputs

The RAPIDS Accelerator will pass columnar forms of the same inputs for the CPU version of the UDF into the `args` array. For example, if the CPU UDF expects two inputs, a String and an Integer, then the evaluateColumnar method will be invoked with an array of two cuDF ColumnVector instances of type STRING and INT32 respectively.

Note that passing scalar inputs to a RAPIDS accelerated UDF is supported with limitations. The scalar value will be replicated into a full column before being passed to evaluateColumnar. Therefore the UDF implementation cannot easily detect the difference between a scalar input and a columnar input.

The implementation of evaluateColumnar must return a column with the specified numRows, equal to the input number of rows. All input columns will contain the same number of rows.

## Generating output

evaluateColumnar must return a ColumnVector of an appropriate cuDF type to match the result type of the original UDF.

The following table shows the mapping of Spark types to equivalent cuDF columnar types:

| Spark Type    | cuDF Type                                  |
|---------------|--------------------------------------------|
| BooleanType   | BOOL8                                      |
| ByteType      | INT8                                       |
| ShortType     | INT16                                      |
| IntegerType   | INT32                                      |
| LongType      | INT64                                      |
| FloatType     | FLOAT32                                    |
| DoubleType    | FLOAT64                                    |
| DecimalType   | DECIMAL32, DECIMAL64, DECIMAL128 *         |
| DateType      | TIMESTAMP_DAYS                             |
| TimestampType | TIMESTAMP_MICROSECONDS                     |
| StringType    | STRING                                     |
| NullType      | INT8                                       |
| ArrayType     | LIST of the underlying element type        |
| MapType       | LIST of STRUCT of the key and value types  |
| StructType    | STRUCT of all the field types              |

For example, if the CPU UDF returns the Spark type `ArrayType(MapType(StringType, StringType))` then evaluateColumnar must return a column of type `LIST(LIST(STRUCT(STRING,STRING)))`.

*Note: cuDF's DECIMAL32 corresponds to precision <= 9 digits, DECIMAL64 corresponds to 9 < precision <= 18 digits, and DECIMAL128 corresponds to 18 < precision <= 38 digits. Precision greater than 38 digits is unsupported.

Note that cuDF decimals use a negative scale relative to Spark DecimalType. For example, Spark DecimalType(precision=11, scale=2) would translate to cuDF type DECIMAL64(scale=-2).

## Debugging

When debugging, it may be helpful to print data type information about cuDF objects. For example, to get information about a ColumnVector:

```java
System.out.println("Param 1 info:" + param1Column);
```

Example output:

```text
Param 1 info: ColumnVector{rows=10, type=INT32, nullCount=Optional.empty, offHeap=(ID: 880 7d1d4c5951e0)}
```

To print the actual values in a column or table, use `TableDebug`:

```java
TableDebug debugger = TableDebug.get();
debugger.debug("Param 1 data:", param1Column);
```

Note that you should NEVER call this from production code, since it causes a device-to-host copy.

## Managing Memory

The Java memory model is not friendly for doing GPU operations because the JVM makes the assumption that everything we're trying to do is in heap memory. **Therefore, you must free the GPU resources in a timely manner with try-finally blocks**, calling `close()` to release GPU resources and `incRefCount()` to increment reference counts.

The JVM's garbage collector is generally triggered when the JVM heap runs out of free space, but not necessarily when the GPU memory runs out. 
To prevent these GPU memory leaks, the cuDF Java code tracks these objects, and if the garbage collector causes the memory to be freed instead of a proper close, it will output a warning like the following:

```text
ERROR ColumnVector: A DEVICE COLUMN VECTOR WAS LEAKED (ID: 15 7fb5f94d8fa0)
```

These messages are an indication that an object on the GPU was not properly closed. Once a leak is detected, the Spark driver/executor `extraJavaOptions` can be set to `-Dai.rapids.refcount.debug=true -ea` to get a stack trace for the leak.

The user will run the unit test and provide tracebacks if memory leaks occur to help you debug the issue.

For Scala, use `withResource` and `closeOnExcept` from the `Arm` object for resource management.

**Note:** Avoid placing the input ColumnVectors (those passed in `args`) in try-finally or try-with-resources blocks. The RAPIDS Accelerator will close the input columns for you. For example, avoid doing this:

```java
ColumnVector param1 = args[0];
try {
  // Do something with param1
} finally {
  param1.close();
}
```

This will result in a double-close error:

```text
java.lang.IllegalStateException: Close called too many times ColumnVector{rows=10, type=INT32, nullCount=Optional.empty, offHeap=(ID: 637 0)}
```
