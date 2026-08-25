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

package org.apache.spark.sql.rapids

import java.io.{DataOutputStream, File}
import java.util.Arrays

import com.nvidia.spark.rapids.{GpuRangeExec, RapidsConf, SparkQueryCompareTestSuite}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.SerializeFromObjectExec
import org.apache.spark.sql.functions.{col, spark_partition_id}

class SequenceFileRddScanSuite extends SparkQueryCompareTestSuite {
  private val replaceConfKey =
    "spark.rapids.sql.format.sequencefile.rddScan.physicalReplace.enabled"

  private val records = Seq(
    Array[Byte](0, 1) -> Array[Byte](10, 11, 12),
    Array.emptyByteArray -> Array[Byte](20),
    Array[Byte](2, 3, 4) -> Array.emptyByteArray)

  private final class RawValueBytes(bytes: Array[Byte]) extends SequenceFile.ValueBytes {
    override def writeUncompressedBytes(out: DataOutputStream): Unit = out.write(bytes)

    override def writeCompressedBytes(out: DataOutputStream): Unit = {
      throw new UnsupportedOperationException("RawValueBytes only supports uncompressed data")
    }

    override def getSize: Int = bytes.length
  }

  private def writeSequenceFile(file: File): Unit = {
    val conf = new Configuration()
    val writer = SequenceFile.createWriter(
      conf,
      SequenceFile.Writer.file(new Path(file.toURI)),
      SequenceFile.Writer.keyClass(classOf[BytesWritable]),
      SequenceFile.Writer.valueClass(classOf[BytesWritable]),
      SequenceFile.Writer.compression(CompressionType.NONE))
    try {
      records.foreach { case (key, value) =>
        writer.appendRaw(key, 0, key.length, new RawValueBytes(value))
      }
    } finally {
      writer.close()
    }
  }

  private def source(
      spark: SparkSession,
      file: File): RDD[(BytesWritable, BytesWritable)] = {
    spark.sparkContext.newAPIHadoopFile(
      file.getAbsolutePath,
      classOf[SequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable])
  }

  private def copiedDataFrame(spark: SparkSession, file: File): DataFrame = {
    import spark.implicits._
    source(spark, file).map { case (key, value) =>
      (Arrays.copyOfRange(key.getBytes, 0, key.getLength),
        Arrays.copyOfRange(value.getBytes, 0, value.getLength))
    }.toDF("key", "value")
  }

  private def swappedDataFrame(spark: SparkSession, file: File): DataFrame = {
    import spark.implicits._
    source(spark, file).map { pair =>
      (pair._2.copyBytes(), pair._1.copyBytes())
    }.toDF("value", "key")
  }

  private def extraMapDataFrame(spark: SparkSession, file: File): DataFrame = {
    import spark.implicits._
    source(spark, file).map { pair =>
      (pair._1.copyBytes(), pair._2.copyBytes())
    }.map(identity).toDF("key", "value")
  }

  private def capturedDataFrame(
      spark: SparkSession,
      file: File,
      extraBytes: Int): DataFrame = {
    import spark.implicits._
    source(spark, file).map { pair =>
      (Arrays.copyOf(pair._1.getBytes, pair._1.getLength + extraBytes),
        pair._2.copyBytes())
    }.toDF("key", "value")
  }

  private def replacementConf(allowCpuPlan: Boolean = false): SparkConf = {
    val conf = new SparkConf().set(replaceConfKey, "true")
    if (allowCpuPlan) {
      conf.set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "SerializeFromObjectExec,ExternalRDDScanExec,ProjectExec")
    }
    conf
  }

  private def gpuScan(df: DataFrame): GpuSequenceFileRDDScanExec = {
    val scans = df.queryExecution.executedPlan.collect {
      case scan: GpuSequenceFileRDDScanExec => scan
    }
    assert(scans.length == 1,
      s"Expected one GpuSequenceFileRDDScanExec:\n${df.queryExecution.executedPlan}")
    scans.head
  }

  private def assertCpuRddScan(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(plan.collect { case _: GpuSequenceFileRDDScanExec => true }.isEmpty,
      s"GpuSequenceFileRDDScanExec must not replace this query:\n$plan")
    assert(plan.collect { case _: SerializeFromObjectExec => true }.nonEmpty,
      s"Expected the original SerializeFromObjectExec:\n$plan")
  }

  private def collectPairs(df: DataFrame): Seq[(Seq[Byte], Seq[Byte])] = {
    df.collect().map { row =>
      (row.getAs[Array[Byte]](0).toSeq, row.getAs[Array[Byte]](1).toSeq)
    }.toSeq
  }

  private def bag[T](values: Seq[T]): Map[T, Int] =
    values.groupBy(identity).map { case (value, copies) => value -> copies.size }

  test("replace the exact copy closure and preserve key/value bytes") {
    withTempPath { file =>
      writeSequenceFile(file)
      withGpuSparkSession({ spark =>
        val df = copiedDataFrame(spark, file)
        assert(gpuScan(df).sourceColumns ==
          Seq(SequenceFileRddReadProof.Key, SequenceFileRddReadProof.Value))
        assert(bag(collectPairs(df)) == bag(records.map { case (key, value) =>
          key.toSeq -> value.toSeq
        }))
      }, replacementConf())
    }
  }

  test("replace the swapped closure and preserve value/key provenance") {
    withTempPath { file =>
      writeSequenceFile(file)
      withGpuSparkSession({ spark =>
        val df = swappedDataFrame(spark, file)
        assert(gpuScan(df).sourceColumns ==
          Seq(SequenceFileRddReadProof.Value, SequenceFileRddReadProof.Key))
        assert(bag(collectPairs(df)) == bag(records.map { case (key, value) =>
          value.toSeq -> key.toSeq
        }))
      }, replacementConf())
    }
  }

  test("an additional RDD map keeps the CPU path and its result") {
    withTempPath { file =>
      writeSequenceFile(file)
      withGpuSparkSession({ spark =>
        val df = extraMapDataFrame(spark, file)
        assertCpuRddScan(df)
        assert(bag(collectPairs(df)) == bag(records.map { case (key, value) =>
          key.toSeq -> value.toSeq
        }))
      }, replacementConf(allowCpuPlan = true))
    }
  }

  test("a captured closure keeps the CPU path and preserves its result") {
    withTempPath { file =>
      writeSequenceFile(file)
      withGpuSparkSession({ spark =>
        val df = capturedDataFrame(spark, file, extraBytes = 1)
        assertCpuRddScan(df)
        assert(bag(collectPairs(df)) == bag(records.map { case (key, value) =>
          (key.toSeq :+ 0.toByte) -> value.toSeq
        }))
      }, replacementConf(allowCpuPlan = true))
    }
  }

  test("spark_partition_id keeps the CPU path and partition semantics") {
    withTempPath { file =>
      writeSequenceFile(file)
      withGpuSparkSession({ spark =>
        val df = copiedDataFrame(spark, file)
          .select(col("key"), col("value"), spark_partition_id().as("partition_id"))
        assertCpuRddScan(df)
        val actual = df.collect().map { row =>
          ((row.getAs[Array[Byte]](0).toSeq, row.getAs[Array[Byte]](1).toSeq), row.getInt(2))
        }.toSeq
        assert(bag(actual.map(_._1)) == bag(records.map { case (key, value) =>
          key.toSeq -> value.toSeq
        }))
        assert(actual.forall(_._2 == 0))
      }, replacementConf(allowCpuPlan = true))
    }
  }

  test("a rejected SerializeFromObjectExec still converts its GPU-capable child") {
    val conf = replacementConf(allowCpuPlan = true)
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "SerializeFromObjectExec,MapElementsExec,DeserializeToObjectExec,ProjectExec")
    withGpuSparkSession({ spark =>
      import spark.implicits._
      val df = spark.range(4).map(value => value + 1L).toDF("value")
      val plan = df.queryExecution.executedPlan
      assert(plan.collect { case _: SerializeFromObjectExec => true }.nonEmpty,
        s"Expected the rejected SerializeFromObjectExec to remain:\n$plan")
      assert(plan.collect { case _: GpuRangeExec => true }.nonEmpty,
        s"Expected the rejected node's GPU-capable child to be converted:\n$plan")
      assert(df.collect().map(_.getLong(0)).toSeq == Seq(1L, 2L, 3L, 4L))
    }, conf)
  }
}
