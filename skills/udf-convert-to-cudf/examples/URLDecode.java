/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import ai.rapids.cudf.*;
import com.nvidia.spark.RapidsUDF;
import org.apache.spark.sql.api.java.UDF1;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/** Decode URL-encoded strings. */
public class URLDecode implements UDF1<String, String>, RapidsUDF {
  /** Row-by-row implementation that executes on the CPU */
  @Override
  public String call(String s) {
    String result = null;
    if (s != null) {
      try {
        result = URLDecoder.decode(s, "utf-8");
      } catch (IllegalArgumentException ignored) {
        result = s;
      } catch (UnsupportedEncodingException e) {
        // utf-8 is a builtin, standard encoding, so this should never happen
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  /** Columnar implementation that runs on the GPU */
  @Override
  public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
    // The CPU implementation takes a single string argument, so similarly
    // there should only be one column argument of type STRING.
    if (args.length != 1) {
      throw new IllegalArgumentException("Unexpected argument count: " + args.length);
    }
    ColumnVector input = args[0];
    if (numRows != input.getRowCount()) {
      throw new IllegalArgumentException("Expected " + numRows + " rows, received " + input.getRowCount());
    }
    if (!input.getType().equals(DType.STRING)) {
      throw new IllegalArgumentException("Argument type is not a string column: " +
          input.getType());
    }

    // The cudf urlDecode does not convert '+' to a space, so do that as a pre-pass first.
    // All intermediate results are closed to avoid leaking GPU resources.
    try (Scalar plusScalar = Scalar.fromString("+");
         Scalar spaceScalar = Scalar.fromString(" ");
         ColumnVector replaced = input.stringReplace(plusScalar, spaceScalar)) {
      return replaced.urlDecode();
    }
  }
}
