/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
package org.apache.iceberg.spark.source

import org.apache.iceberg.TableProperties
import org.scalatest.funsuite.AnyFunSuite

class GpuSparkWriteSuite extends AnyFunSuite {

  test("translateIcebergWriteProperties: adds 'compression' when only Iceberg key is present") {
    val writeProps = Map(TableProperties.PARQUET_COMPRESSION -> "zstd")
    val result = GpuSparkWrite.translateIcebergWriteProperties(writeProps)
    assert(result("compression") === "zstd")
    assert(result(TableProperties.PARQUET_COMPRESSION) === "zstd")
  }

  test("translateIcebergWriteProperties: explicit 'compression' is preserved") {
    val writeProps = Map(
      TableProperties.PARQUET_COMPRESSION -> "zstd",
      "compression" -> "snappy")
    val result = GpuSparkWrite.translateIcebergWriteProperties(writeProps)
    assert(result("compression") === "snappy",
      "explicit 'compression' must not be overwritten by the Iceberg codec key")
  }

  test("translateIcebergWriteProperties: missing Iceberg key returns map unchanged") {
    val writeProps = Map("write.parquet.row-group-size-bytes" -> "134217728")
    val result = GpuSparkWrite.translateIcebergWriteProperties(writeProps)
    assert(result === writeProps)
    assert(!result.contains("compression"))
  }
}
