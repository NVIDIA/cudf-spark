/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import java.net.URLDecoder

import ai.rapids.cudf._
import com.nvidia.spark.RapidsUDF
import Arm.{withResource, closeOnExcept}

/** Decode URL-encoded strings. */
class URLDecode extends Function1[String, String] with RapidsUDF with Serializable {
  /** Row-by-row implementation that executes on the CPU */
  override def apply(s: String): String = {
    Option(s).map { s =>
      try {
        URLDecoder.decode(s, "utf-8")
      } catch {
        case _: IllegalArgumentException => s
      }
    }.orNull
  }

  /** Columnar implementation that runs on the GPU */
  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    // The CPU implementation takes a single string argument, so similarly
    // there should only be one column argument of type STRING.
    require(args.length == 1, s"Unexpected argument count: ${args.length}")
    val input = args.head
    require(numRows == input.getRowCount, s"Expected $numRows rows, received ${input.getRowCount}")
    require(input.getType == DType.STRING, s"Argument type is not a string: ${input.getType}")

    // The cudf urlDecode does not convert '+' to a space, so do that as a pre-pass first.
    // All intermediate results are closed using withResource to avoid leaking GPU resources.
    withResource(Scalar.fromString("+")) { plusScalar =>
      withResource(Scalar.fromString(" ")) { spaceScalar =>
        withResource(input.stringReplace(plusScalar, spaceScalar)) { replaced =>
          replaced.urlDecode()
        }
      }
    }
  }
}
