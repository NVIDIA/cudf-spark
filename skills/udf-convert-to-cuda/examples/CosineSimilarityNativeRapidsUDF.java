/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf;

import ai.rapids.cudf.ColumnVector;
import com.nvidia.spark.RapidsUDF;
import org.apache.spark.sql.api.java.UDF2;

import scala.collection.mutable.WrappedArray;

/**
 * Native CUDA RapidsUDF example for cosine similarity over two LIST(FLOAT32) columns.
 */
public class CosineSimilarityNativeRapidsUDF
        implements UDF2<WrappedArray<Float>, WrappedArray<Float>, Float>, RapidsUDF {
    @Override
    public Float call(WrappedArray<Float> v1, WrappedArray<Float> v2) {
        if (v1 == null || v2 == null) {
            return null;
        }
        if (v1.length() != v2.length()) {
            throw new IllegalArgumentException("Array lengths must match: "
                + v1.length() + " != " + v2.length());
        }

        double dotProduct = 0;
        double magnitude1 = 0;
        double magnitude2 = 0;
        for (int i = 0; i < v1.length(); i++) {
            float f1 = v1.apply(i);
            float f2 = v2.apply(i);
            dotProduct += f1 * f2;
            magnitude1 += f1 * f1;
            magnitude2 += f2 * f2;
        }
        return (float) (dotProduct / (Math.sqrt(magnitude1) * Math.sqrt(magnitude2)));
    }

    @Override
    public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Unexpected argument count: " + args.length);
        }
        if (numRows != args[0].getRowCount() || numRows != args[1].getRowCount()) {
            throw new IllegalArgumentException("Input row count mismatch");
        }

        NativeUDFLoader.ensureLoaded();
        return new ColumnVector(cosineSimilarity(args[0].getNativeView(), args[1].getNativeView()));
    }

    private static native long cosineSimilarity(long vectorView1, long vectorView2);
}
