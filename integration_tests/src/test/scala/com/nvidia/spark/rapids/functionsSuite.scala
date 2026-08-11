/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import com.nvidia.spark.functions._

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.nvidia.SparkTestBase
import org.apache.spark.sql.types._

class functionsSuite extends SparkTestBase {
  test("basic 0 arg df_udf") {
    val zero = df_udf(() => lit(0))
    withSparkSession{ spark =>
      spark.udf.register("zero", zero)
      assertSame(Array(
        Row(0L, 0),
        Row(1L, 0)),
        spark.range(2).selectExpr("id", "zero()").collect())
      assertSame(Array(
        Row(0L, 0),
        Row(1L, 0)),
        spark.range(2).select(col("id"), zero()).collect())
    }
  }

  test("basic 1 arg df_udf") {
    val inc = df_udf((input: Column) => input + 1)
    withSparkSession { spark =>
      spark.udf.register("inc", inc)
      assertSame(Array(
        Row(0L, 1L),
        Row(1L, 2L)),
        spark.range(2).selectExpr("id", "inc(id)").collect())
      assertSame(Array(
        Row(0L, 1L),
        Row(1L, 2L)),
        spark.range(2).select(col("id"), inc(col("id"))).collect())
    }
  }


  test("basic 2 arg df_udf") {
    val add = df_udf((a: Column, b:Column) => a + b)
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 0L),
        Row(1L, 2L)),
        spark.range(2).selectExpr("id", "add(id, id)").collect())
      assertSame(Array(
        Row(0L, 0L),
        Row(1L, 2L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"))).collect())
    }
  }

  test("basic 3 arg df_udf") {
    val add = df_udf((a: Column, b:Column, c:Column) => a + b + c)
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 0L),
        Row(1L, 3L)),
        spark.range(2).selectExpr("id", "add(id, id, id)").collect())
      assertSame(Array(
        Row(0L, 0L),
        Row(1L, 3L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), col("id"))).collect())
    }
  }

  test("basic 4 arg df_udf") {
    val add = df_udf((a: Column, b:Column, c:Column, d:Column) => a + b + c + d)
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 1L),
        Row(1L, 4L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id)").collect())
      assertSame(Array(
        Row(0L, 1L),
        Row(1L, 4L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1), col("id"))).collect())
    }
  }

  test("basic 5 arg df_udf") {
    val add = df_udf((a: Column, b:Column, c:Column, d:Column, e:Column) =>
      a + b + c + d + e)
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 5L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1)").collect())
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 5L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1))).collect())
    }
  }

  test("basic 6 arg df_udf") {
    val add = df_udf((a: Column, b:Column, c:Column, d:Column, e:Column, f:Column) =>
      a + b + c + d + e + f)
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 6L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1, id)").collect())
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 6L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1), col("id"))).collect())
    }
  }

  test("basic 7 arg df_udf") {
    val add = df_udf((a: Column, b:Column, c:Column, d:Column, e:Column,
                                f:Column, g:Column) => a + b + c + d + e + f + g)
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 7L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1, id, id)").collect())
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 7L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1), col("id"), col("id"))).collect())
    }
  }

  test("basic 8 arg df_udf") {
    val add = df_udf((a: Column, b:Column, c:Column, d:Column, e:Column,
                                f:Column, g:Column, h:Column) => a + b + c + d + e + f + g + h)
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 9L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1, id, id, 2)").collect())
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 9L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1), col("id"), col("id"), lit(2))).collect())
    }
  }

  test("basic 9 arg df_udf") {
    val add = df_udf((a: Column, b:Column, c:Column, d:Column, e:Column,
                                f:Column, g:Column, h:Column, i:Column) =>
      a + b + c + d + e + f + g + h + i)
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 10L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1, id, id, 2, id)").collect())
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 10L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1), col("id"), col("id"), lit(2), col("id"))).collect())
    }
  }

  test("basic 10 arg df_udf") {
    val add = df_udf((a: Column, b:Column, c:Column, d:Column, e:Column,
                                f:Column, g:Column, h:Column, i:Column, j:Column) =>
      a + b + c + d + e + f + g + h + i + j)
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 11L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1, id, id, 2, id, id)").collect())
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 11L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1), col("id"), col("id"), lit(2), col("id"), col("id"))).collect())
    }
  }

  test("nested df_udf") {
    val add = df_udf((a: Column, b:Column) => a + b)
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 22L),
        Row(1L, 25L)),
        spark.range(2).selectExpr("id", "add(add(id, 12), add(add(id, id), 10))").collect())
    }
  }

  test("complex df_udf") {
    val extractor = df_udf((json: Column) => {
      val schema = StructType(Seq(StructField("values", ArrayType(LongType))))
      val extracted_json = from_json(json, schema, Map.empty[String, String])
      aggregate(extracted_json("values"),
        lit(0L),
        (a, b) => coalesce(a, lit(0L)) + coalesce(b, lit(0L)),
        a => a)
    })
    withSparkSession { spark =>
      import spark.implicits._
      spark.udf.register("extractor", extractor)
      assertSame(Array(
        Row(6L),
        Row(3L)),
        Seq("""{"values":[1,2,3]}""",
        """{"values":[1, null, null, 2]}""").toDF("json").selectExpr("extractor(json)").collect())
    }
  }

  test("j basic 0 arg df_udf") {
    val zero = df_udf(new UDF0[Column] {
      override def call(): Column = lit(0)
    })
    withSparkSession{ spark =>
      spark.udf.register("zero", zero)
      assertSame(Array(
        Row(0L, 0),
        Row(1L, 0)),
        spark.range(2).selectExpr("id", "zero()").collect())
      assertSame(Array(
        Row(0L, 0),
        Row(1L, 0)),
        spark.range(2).select(col("id"), zero()).collect())
    }
  }

  test("jbasic 1 arg df_udf") {
    val inc = df_udf(new UDF1[Column, Column] {
      override def call(a: Column): Column = a + 1
    })
    withSparkSession { spark =>
      spark.udf.register("inc", inc)
      assertSame(Array(
        Row(0L, 1L),
        Row(1L, 2L)),
        spark.range(2).selectExpr("id", "inc(id)").collect())
      assertSame(Array(
        Row(0L, 1L),
        Row(1L, 2L)),
        spark.range(2).select(col("id"), inc(col("id"))).collect())
    }
  }

  test("jbasic 2 arg df_udf") {
    val add = df_udf(new UDF2[Column, Column, Column] {
      override def call(a: Column, b:Column): Column = a + b
    })
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 0L),
        Row(1L, 2L)),
        spark.range(2).selectExpr("id", "add(id, id)").collect())
      assertSame(Array(
        Row(0L, 0L),
        Row(1L, 2L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"))).collect())
    }
  }

  test("jbasic 3 arg df_udf") {
    val add = df_udf(new UDF3[Column, Column, Column, Column] {
      override def call(a: Column, b: Column, c: Column): Column = a + b + c
    })
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 0L),
        Row(1L, 3L)),
        spark.range(2).selectExpr("id", "add(id, id, id)").collect())
      assertSame(Array(
        Row(0L, 0L),
        Row(1L, 3L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), col("id"))).collect())
    }
  }

  test("jbasic 4 arg df_udf") {
    val add = df_udf(new UDF4[Column, Column, Column, Column, Column] {
      override def call(a: Column, b:Column, c:Column, d:Column): Column = a + b + c + d
    })
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 1L),
        Row(1L, 4L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id)").collect())
      assertSame(Array(
        Row(0L, 1L),
        Row(1L, 4L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1), col("id"))).collect())
    }
  }

  test("jbasic 5 arg df_udf") {
    val add = df_udf(new UDF5[Column, Column, Column, Column, Column, Column] {
      override def call(a: Column, b: Column, c: Column, d: Column, e: Column): Column =
        a + b + c + d + e
    })
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 5L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1)").collect())
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 5L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1))).collect())
    }
  }

  test("jbasic 6 arg df_udf") {
    val add = df_udf(new UDF6[Column, Column, Column, Column, Column, Column, Column] {
      override def call(a: Column, b:Column, c:Column, d:Column, e:Column, f:Column) =
      a + b + c + d + e + f
    })
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 6L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1, id)").collect())
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 6L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1), col("id"))).collect())
    }
  }

  test("jbasic 7 arg df_udf") {
    val add = df_udf(new UDF7[Column, Column, Column, Column, Column, Column, Column,
      Column] {
      override def call(a: Column, b:Column, c:Column, d:Column, e:Column,
                                f:Column, g:Column): Column = a + b + c + d + e + f + g
    })
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 7L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1, id, id)").collect())
      assertSame(Array(
        Row(0L, 2L),
        Row(1L, 7L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1), col("id"), col("id"))).collect())
    }
  }

  test("jbasic 8 arg df_udf") {
    val add = df_udf(new UDF8[Column, Column, Column, Column, Column, Column, Column,
      Column, Column] {
      override def call(a: Column, b: Column, c: Column, d: Column, e: Column,
                        f: Column, g: Column, h: Column): Column = a + b + c + d + e + f + g + h
    })
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 9L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1, id, id, 2)").collect())
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 9L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1), col("id"), col("id"), lit(2))).collect())
    }
  }

  test("jbasic 9 arg df_udf") {
    val add = df_udf(new UDF9[Column, Column, Column, Column, Column, Column, Column,
      Column, Column, Column] {
      override def call(a: Column, b:Column, c:Column, d:Column, e:Column,
                                f:Column, g:Column, h:Column, i:Column): Column =
      a + b + c + d + e + f + g + h + i
    })
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 10L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1, id, id, 2, id)").collect())
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 10L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1), col("id"), col("id"), lit(2), col("id"))).collect())
    }
  }

  test("jbasic 10 arg df_udf") {
    val add = df_udf(new UDF10[Column, Column, Column, Column, Column, Column, Column,
      Column, Column, Column, Column] {
      override def call(a: Column, b:Column, c:Column, d:Column, e:Column,
                                f:Column, g:Column, h:Column, i:Column, j:Column): Column =
      a + b + c + d + e + f + g + h + i + j
    })
    withSparkSession { spark =>
      spark.udf.register("add", add)
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 11L)),
        spark.range(2).selectExpr("id", "add(id, id, 1, id, 1, id, id, 2, id, id)").collect())
      assertSame(Array(
        Row(0L, 4L),
        Row(1L, 11L)),
        spark.range(2).select(col("id"), add(col("id"), col("id"), lit(1),
          col("id"), lit(1), col("id"), col("id"), lit(2), col("id"), col("id"))).collect())
    }
  }
}