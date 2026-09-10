/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf;

import ai.rapids.cudf.ColumnVector;
import com.nvidia.spark.RapidsUDF;
// TODO: add imports for CPU UDF's base type, e.g.:
//   import org.apache.hadoop.hive.ql.exec.UDF;
//   import org.apache.spark.sql.api.java.UDFn;

/**
 * Template for a native CUDA RapidsUDF.
 *
 * 1. Rename this class and file to {@code <CamelName>NativeRapidsUDF}.
 * 2. Match the CPU UDF's Spark contract:
 *    - Hive UDF       : add {@code extends org.apache.hadoop.hive.ql.exec.UDF}
 *    - Java typed UDF : add {@code implements UDFn<T1,...,R>} alongside {@code RapidsUDF}
 *    - Scala CPU UDF  : implement the equivalent {@code UDFn<...>} contract.
 *                       Invoke the Scala UDF via reflection from {@code call(...)}.
 * 3. Add the CPU evaluation method.
 * 4. Update {@code evaluateColumnar} and {@code evaluateNative} as needed to match the signature.
 */
public class PlaceholderUDFNameNativeRapidsUDF implements RapidsUDF {

    // TODO: copy the original CPU evaluation method here (evaluate / call).

    @Override
    public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("Unexpected argument count: " + args.length);
        }
        if (numRows != args[0].getRowCount()) {
            throw new IllegalArgumentException(
                "Expected " + numRows + " rows, received " + args[0].getRowCount());
        }

        NativeUDFLoader.ensureLoaded();
        return new ColumnVector(evaluateNative(args[0].getNativeView()));
    }

    private static native long evaluateNative(long inputView);
}
