
/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{TestArrayUDT, TestNestedStructUDT, TestPrimitiveUDT}
import org.apache.spark.sql.types._

class ParquetUDTSuite extends SparkQueryCompareTestSuite {
  def writeUdtVec: (SparkSession, File) => Unit = 
    (spark, file) => {
      val df = spark.range(1).selectExpr(
        """NAMED_STRUCT(
          |  'f0', CAST(id AS STRING),
          |  'f1', NAMED_STRUCT(
          |    'a', CAST(id + 1 AS INT),
          |    'b', CAST(id + 2 AS LONG),
          |    'c', CAST(id + 3.5 AS DOUBLE)
          |  ),
          |  'f2', CAST(id + 4 AS INT),
          |  'f3', ARRAY(id + 5, id + 6)
          |) AS s
        """.stripMargin
      ).coalesce(1)
      df.write.parquet(file.getCanonicalPath)
    }

  def readUdtVec: File => (SparkSession => DataFrame) = 
    file => spark =>  {
      // specify schema
      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("f0", StringType)
              .add("f1", new TestNestedStructUDT())
              .add("f2", new TestPrimitiveUDT())
              .add("f3", new TestArrayUDT())
          )
      spark.read.schema(userDefinedSchema).parquet(file.getCanonicalPath)
    }

  def testUdtFallbackVec(nested: Boolean): Unit = {
    testGpuReadFallback("reading UDT fallback in vectorized reader nested column " + 
        (if (nested) "en" else "dis") + "abled",
      "FileSourceScanExec",
      readUdtVec,
      writeUdtVec,
      execsAllowedNonGpu = Seq("ColumnarToRowExec", "FileSourceScanExec", "ShuffleExchangeExec"),
      conf = new SparkConf().set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
          .set(SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key, nested.toString)
    ) {
      frame => frame
    }
  }

  Seq(true, false).foreach(testUdtFallbackVec)
}
