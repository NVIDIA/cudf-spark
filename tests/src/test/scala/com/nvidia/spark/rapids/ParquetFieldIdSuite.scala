/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import java.io.File

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/** Tests for field IDs in Parquet metadata */
class ParquetFieldIdSuite extends SparkQueryCompareTestSuite {

  private def withId(idx: Int): Metadata = {
    new MetadataBuilder().putLong("parquet.field.id", idx).build()
  }

  private def getSchemaAndData(): (StructType, Seq[Row]) = {
    val schema = StructType(Seq(
      StructField("c1", IntegerType, nullable = true, metadata = withId(11)),
      StructField("c2", IntegerType, nullable = true, metadata = withId(22))))
    val data = Seq(Row(1, 1), Row(2, 2), Row(33, 33))
    (schema, data)
  }

  test("enable write field IDs") {
    assume(cmpSparkVersion(3, 3, 0) >= 0)
    val tempFile = File.createTempFile("stats", ".parquet")
    try {
      val (schema, data) = getSchemaAndData()
      // write field IDs by default
      withGpuSparkSession(spark => spark.createDataFrame(data.asJava, schema).write
          .mode("overwrite").parquet(tempFile.getAbsolutePath))

      // Parquet files are in a directory
      val d = new File(tempFile.getAbsolutePath)
      assert(d.exists && d.isDirectory)
      // filter part-*.parquet file
      val parquetFiles = d.listFiles.filter(_.isFile).filter(f => f.getName.startsWith("part-")
          && f.getName.endsWith(".parquet"))
      parquetFiles.foreach { parquetFile =>
        withResource(ParquetFileReader.open(HadoopInputFile.fromPath(
          new Path(parquetFile.getAbsolutePath), new Configuration()))) { reader =>
          val schemaInFile = reader.getFooter.getFileMetaData.getSchema
          assert(schemaInFile.getFields.get(0).getId().intValue() == 11)
          assert(schemaInFile.getFields.get(1).getId().intValue() == 22)
        }
      }
    } finally {
      tempFile.delete()
    }
  }

  test("disable write field IDs") {
    assume(cmpSparkVersion(3, 3, 0) >= 0)
    val tempFile = File.createTempFile("stats", ".parquet")
    val disableWriteFieldId = new SparkConf().set("spark.sql.parquet.fieldId.write.enabled",
      "false")

    try {
      val (schema, data) = getSchemaAndData()
      withGpuSparkSession(spark => spark.createDataFrame(data.asJava, schema).write
          .mode("overwrite").parquet(tempFile.getAbsolutePath),
        // disable write field IDs
        conf = disableWriteFieldId)

      // Parquet files are in a directory
      val d = new File(tempFile.getAbsolutePath)
      assert(d.exists && d.isDirectory)
      // filter part-*.parquet file
      val parquetFiles = d.listFiles.filter(_.isFile).filter(f => f.getName.startsWith("part-")
          && f.getName.endsWith(".parquet"))
      parquetFiles.foreach { parquetFile =>
        withResource(ParquetFileReader.open(HadoopInputFile.fromPath(
          new Path(parquetFile.getAbsolutePath), new Configuration()))) { reader =>
          val schemaInFile = reader.getFooter.getFileMetaData.getSchema
          // should not write field IDs
          assert(schemaInFile.getFields.get(0).getId == null)
          assert(schemaInFile.getFields.get(1).getId == null)
        }
      }
    } finally {
      tempFile.delete()
    }
  }
}
