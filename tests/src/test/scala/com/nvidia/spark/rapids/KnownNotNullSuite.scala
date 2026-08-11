/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.functions.col

class KnownNotNullSuite extends SparkQueryCompareTestSuite {
  // Technically "GpuKnownNotNull" itself supports all the types, although in reality it
  // is only ever going to use it on 8 primitive types(byte, short, int, long,
  // float, double, boolean, char).
  // So here are tests for only 3 of the primitive types to verify the functionality of
  // the expression "GpuKnownNotNull".

  testSparkResultsAreEqual("KnownNotNull in Scala UDF-Int",
      intsFromCsv,
      enableCsvConf) { frame =>
    val plusOne = frame.sparkSession.udf.register("plusOne", new IntPlusOne())
    frame.select(plusOne(col("ints_1")))
  }

  testSparkResultsAreEqual("KnownNotNull in Scala UDF-Long",
    longsFromCSVDf,
    enableCsvConf) { frame =>
    val plusOne = frame.sparkSession.udf.register("plusOne", new LongPlusOne())
    frame.select(plusOne(col("longs")))
  }

  testSparkResultsAreEqual("KnownNotNull in Scala UDF-Short",
    shortsFromCsv,
    enableCsvConf) { frame =>
    val plusOne = frame.sparkSession.udf.register("plusOne", new ShortPlusOne())
    frame.select(plusOne(col("shorts")))
  }

}

/**
 * A Rapids UDF for test only.
 */
trait PlusOne extends RapidsUDF with Serializable {

  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    require(args.length == 1,
      s"Unexpected argument count: ${args.length}")
    require(numRows == args.head.getRowCount,
      s"Expected $numRows rows, received ${args.head.getRowCount}")

    withResource(Scalar.fromInt(1)) { sOne =>
      args.head.add(sOne, args.head.getType)
    }
  }
}

class IntPlusOne extends Function[Int, Int] with PlusOne {
  override def apply(v: Int): Int = v + 1
}

class LongPlusOne extends Function[Long, Long] with PlusOne {
  override def apply(v: Long): Long = v + 1
}

class ShortPlusOne extends Function[Short, Short] with PlusOne {
  override def apply(v: Short): Short = (v + 1).toShort
}
