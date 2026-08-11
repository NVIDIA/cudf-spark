/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader

import org.apache.spark.SparkConf

class ParquetCompressionCodecSuite extends SparkQueryCompareTestSuite {

  test("Parquet compression codec, large data size") {
    testCompression(rowNum = 10000)
  }

  test("Parquet compression codec, small data size") {
    assume(false, "blocked by https://github.com/rapidsai/cudf/issues/14017")
    testCompression(rowNum = 5)
  }

  @scala.annotation.nowarn(
    "msg=method readAllFootersInParallel in class ParquetFileReader is deprecated")
  private def getTableCompressionCodec(dir: String): Seq[String] = {
    val config = new Configuration()
    val path = new Path(dir)
    val fs = path.getFileSystem(config)
    val codecs = for {
      footer <- ParquetFileReader.readAllFootersInParallel(config, fs.getFileStatus(path)).asScala
      block <- footer.getParquetMetadata.getBlocks.asScala
      column <- block.getColumns.asScala
    } yield column.getCodec.name()
    codecs.distinct.toSeq
  }

  def testCompression(rowNum: Int): Unit = {
    var codec = Seq("UNCOMPRESSED", "SNAPPY")
    // zstd is available in spark 3.2.0 and later
    if (VersionUtils.isSpark320OrLater) codec = codec :+ "ZSTD"
    val data = (0 until rowNum).map(i => (i % 2, "string" + i))
    codec.foreach { compressionCodec =>
      withTempPath { tmpDir =>
        withGpuSparkSession(
          { spark =>
            spark.createDataFrame(data).toDF("c1", "c2")
                .repartition(1).write.mode("overwrite")
                .partitionBy("c1").parquet(tmpDir.getCanonicalPath)
          },
          new SparkConf().set("spark.sql.parquet.compression.codec", compressionCodec.toLowerCase))

        val path = s"${tmpDir.getPath.stripSuffix("/")}/c1=1"
        val realCompressionCodecs = getTableCompressionCodec(path)
        assert(realCompressionCodecs.forall(_ == compressionCodec))
      }
    }
  }
}
