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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.parquet

import com.nvidia.spark.rapids.RapidsConf.ParquetFooterReaderType
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType}
import org.apache.spark.sql.types.{StructField, StructType, VariantType}

class GpuParquetVariantFooterSuite extends AnyFunSuite {
  test("Variant schemas bypass native footer pruning") {
    val variant = StructType(Seq(StructField("v", VariantType, nullable = true)))
    val arrayVariant = StructType(Seq(StructField(
      "array", ArrayType(VariantType, containsNull = true), nullable = true)))
    val mapVariant = StructType(Seq(StructField(
      "map", MapType(StringType, VariantType, valueContainsNull = true), nullable = true)))
    val structVariant = StructType(Seq(StructField("struct", StructType(Seq(
      StructField("v", VariantType, nullable = true))), nullable = true)))
    val supported = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)))

    assert(!GpuParquetFileFilterHandler.useNativeFooterReader(
      ParquetFooterReaderType.NATIVE, variant))
    assert(!GpuParquetFileFilterHandler.useNativeFooterReader(
      ParquetFooterReaderType.NATIVE, arrayVariant))
    assert(!GpuParquetFileFilterHandler.useNativeFooterReader(
      ParquetFooterReaderType.NATIVE, mapVariant))
    assert(!GpuParquetFileFilterHandler.useNativeFooterReader(
      ParquetFooterReaderType.NATIVE, structVariant))
    assert(GpuParquetFileFilterHandler.useNativeFooterReader(
      ParquetFooterReaderType.NATIVE, supported))
    assert(!GpuParquetFileFilterHandler.useNativeFooterReader(
      ParquetFooterReaderType.JAVA, supported))
  }
}
