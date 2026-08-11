/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{array, col, map, struct}
import org.apache.spark.sql.types.DecimalType

class RowBasedUDFSuite extends SparkQueryCompareTestSuite {

  val cpuEnabledConf: SparkConf = new SparkConf()
    .set(RapidsConf.ENABLE_CPU_BASED_UDF.key, "true")

  val csvAndCpuEnabledConf: SparkConf = enableCsvConf().setAll(cpuEnabledConf.getAll)

  testSparkResultsAreEqual("Row Based Scala UDF-Boolean",
      booleanDf,
      cpuEnabledConf) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF", new NoopUDF[java.lang.Boolean]())
    frame.select(noopUDF(col("bools")))
  }

  testSparkResultsAreEqual("Row Based Scala UDF-Short",
      shortsFromCsv,
      csvAndCpuEnabledConf) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF", new NoopUDF[java.lang.Short]())
    frame.select(noopUDF(col("shorts")))
  }

  testSparkResultsAreEqual("Row Based Scala UDF-Mixed(int,long,double,string)",
      mixedDfWithNulls,
      cpuEnabledConf) { frame =>
    val iNoopUDF = frame.sparkSession.udf.register("iNoopUDF", new NoopUDF[java.lang.Integer]())
    val lNoopUDF = frame.sparkSession.udf.register("lNoopUDF", new NoopUDF[java.lang.Long]())
    val dNoopUDF = frame.sparkSession.udf.register("dNoopUDF", new NoopUDF[java.lang.Double]())
    val sNoopUDF = frame.sparkSession.udf.register("sNoopUDF", new NoopUDF[String]())
    frame.select(
        iNoopUDF(col("ints")),
        lNoopUDF(col("longs")),
        dNoopUDF(col("doubles")),
        sNoopUDF(col("strings")))
  }

  testSparkResultsAreEqual("Row Based Scala UDF-Date",
      datesDf,
      cpuEnabledConf) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF", new NoopUDF[Date]())
    frame.select(noopUDF(col("dates")))
  }

  testSparkResultsAreEqual("Row Based Scala UDF-Timestamp",
      timestampsDf,
      cpuEnabledConf) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF", new NoopUDF[Timestamp]())
    frame.select(noopUDF(col("timestamps")))
  }

  testSparkResultsAreEqual("Row Based Scala UDF-Decimal",
      mixedDf(_, 1),
      cpuEnabledConf) { frame =>
    // Scala UDF returns a Decimal(38, 18) by default and there is no way to specify the
    // precision for Scala functions. So here uses the Java version of `register` to do it.
    frame.sparkSession.udf.register("NoopUDF", (dec: java.math.BigDecimal) => dec,
      DecimalType(15, 5))
    frame.selectExpr("NoopUDF(decimals)")
  }

  testSparkResultsAreEqual("Row Based Scala UDF-Array(Int)",
    mixedDfWithNulls,
    cpuEnabledConf) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF", new NoopUDF[Seq[java.lang.Integer]]())
    frame.select(noopUDF(array("ints", "ints")))
  }

  testSparkResultsAreEqual("Row Based Scala UDF-Array(Array(Int))",
    mixedDfWithNulls,
    cpuEnabledConf) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF",
      new NoopUDF[Seq[Seq[java.lang.Integer]]]())
    frame.select(noopUDF(array(array("ints", "ints"))))
  }

  testSparkResultsAreEqual("Row Based Scala UDF-Struct(Int, Double, String)",
      mixedDfWithNulls,
      cpuEnabledConf) { frame =>
    // UDF accepting a struct as row, can not return the input Row directly, since Row is
    // not supported as the returned type of an UDF in Spark.
    // Then here convert it to a Tuple3 to return.
    val noopUDF = frame.sparkSession.udf.register("NoopUDF",
      (row: Row) => (
        row(0).asInstanceOf[java.lang.Integer],
        row(1).asInstanceOf[java.lang.Double],
        row(2).asInstanceOf[String]))
    frame.select(noopUDF(struct("ints", "doubles", "strings")))
  }

  testSparkResultsAreEqual("Row Based Scala UDF-Map(String -> Long)",
      mixedDfWithNulls,
      cpuEnabledConf) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF",
      new NoopUDF[Map[String, java.lang.Long]]())
    // keys should not be null, so filter nulls out first.
    frame.filter("strings != null")
      .select(noopUDF(map(col("strings"), col("longs"))))
  }
}

/** An UDF for test only, returning the input directly. */
class NoopUDF[T] extends Function[T, T] with Serializable {
  override def apply(v: T): T = v
}
