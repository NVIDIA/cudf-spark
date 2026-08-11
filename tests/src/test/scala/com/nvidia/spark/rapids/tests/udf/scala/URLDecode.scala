/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tests.udf.scala

import java.net.URLDecoder

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import com.nvidia.spark.RapidsUDF

/**
 * A Scala user-defined function (UDF) that decodes URL-encoded strings.
 * This class demonstrates how to implement a Scala UDF that also
 * provides a RAPIDS implementation that can run on the GPU when the query
 * is executed with the RAPIDS Accelerator for Apache Spark.
 */
class URLDecode extends Function[String, String] with RapidsUDF with Serializable {
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
    // All intermediate results are closed to avoid leaking GPU resources.
    val plusScalar = Scalar.fromString("+")
    try {
      val spaceScalar = Scalar.fromString(" ")
      try {
        val replaced = input.stringReplace(plusScalar, spaceScalar)
        try {
          replaced.urlDecode()
        } finally {
          replaced.close()
        }
      } finally {
        spaceScalar.close()
      }
    } finally {
      plusScalar.close()
    }
  }
}
