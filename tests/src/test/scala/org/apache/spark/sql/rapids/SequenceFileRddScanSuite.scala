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

import java.io.{DataOutputStream, File, RandomAccessFile}
import java.util.{Arrays, Random}

import scala.concurrent.duration._

import com.nvidia.spark.rapids.{GpuMetric, GpuRangeExec, RapidsConf, SparkQueryCompareTestSuite,
  TestUtils}
import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ChecksumFileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.{CompressionCodec, DefaultCodec}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
import org.apache.hadoop.util.ReflectionUtils
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.SerializeFromObjectExec
import org.apache.spark.sql.functions.{col, spark_partition_id}

class SequenceFileRddScanSuite extends SparkQueryCompareTestSuite with Eventually {
  private val replaceConfKey = RapidsConf.SEQUENCEFILE_RDD_READ_ENABLED.key
  private val splitMaxSizeKey = "mapreduce.input.fileinputformat.split.maxsize"
  private val compressionTypes =
    Seq(CompressionType.NONE, CompressionType.RECORD, CompressionType.BLOCK)

  private val records = Seq(
    Array[Byte](0, 1) -> Array[Byte](10, 11, 12),
    Array.emptyByteArray -> Array[Byte](20),
    Array[Byte](2, 3, 4) -> Array.emptyByteArray)
  private val recordPairs = records.map { case (key, value) => key.toSeq -> value.toSeq }

  private final class RawValueBytes(bytes: Array[Byte]) extends SequenceFile.ValueBytes {
    override def writeUncompressedBytes(out: DataOutputStream): Unit = out.write(bytes)

    override def writeCompressedBytes(out: DataOutputStream): Unit = {
      throw new UnsupportedOperationException("RawValueBytes does not support RECORD compression")
    }

    override def getSize: Int = bytes.length
  }

  private def lzoCodec(conf: Configuration): CompressionCodec = {
    val codecClass = Class.forName("io.airlift.compress.lzo.LzoCodec")
      .asSubclass(classOf[CompressionCodec])
    ReflectionUtils.newInstance(codecClass, conf)
  }

  private def intBytes(value: Int): Array[Byte] = Array(
    (value >>> 24).toByte,
    (value >>> 16).toByte,
    (value >>> 8).toByte,
    value.toByte)

  private def payload(fileIndex: Int, rowIndex: Int, size: Int): Array[Byte] = {
    val bytes = Array.fill[Byte](size)((rowIndex % 251).toByte)
    bytes(0) = fileIndex.toByte
    System.arraycopy(intBytes(rowIndex), 0, bytes, 1, Integer.BYTES)
    bytes
  }

  private def randomPayload(fileIndex: Int, rowIndex: Int, size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    new Random((fileIndex.toLong << 32) ^ rowIndex.toLong).nextBytes(bytes)
    bytes(0) = fileIndex.toByte
    System.arraycopy(intBytes(rowIndex), 0, bytes, 1, Integer.BYTES)
    bytes
  }

  private def createWriter(
      file: File,
      compression: CompressionType,
      useLzo: Boolean): SequenceFile.Writer = {
    val conf = new Configuration()
    val compressionOption = if (compression == CompressionType.NONE) {
      SequenceFile.Writer.compression(compression)
    } else {
      val codec = if (useLzo) {
        lzoCodec(conf)
      } else {
        val defaultCodec = new DefaultCodec
        defaultCodec.setConf(conf)
        defaultCodec
      }
      SequenceFile.Writer.compression(compression, codec)
    }
    SequenceFile.createWriter(
      conf,
      SequenceFile.Writer.file(new Path(file.toURI)),
      SequenceFile.Writer.keyClass(classOf[BytesWritable]),
      SequenceFile.Writer.valueClass(classOf[BytesWritable]),
      compressionOption)
  }

  private def writeFile(
      file: File,
      fileRecords: Seq[(Array[Byte], Array[Byte])],
      compression: CompressionType,
      useLzo: Boolean,
      syncEvery: Int)(append: (SequenceFile.Writer, Array[Byte], Array[Byte]) => Unit): Unit = {
    val writer = createWriter(file, compression, useLzo)
    try {
      fileRecords.zipWithIndex.foreach { case ((key, value), index) =>
        append(writer, key, value)
        if (syncEvery > 0 && (index + 1) % syncEvery == 0 && index + 1 < fileRecords.length) {
          writer.sync()
        }
      }
    } finally {
      writer.close()
    }
  }

  private def writeRawFile(
      file: File,
      fileRecords: Seq[(Array[Byte], Array[Byte])],
      compression: CompressionType,
      syncEvery: Int = 0): Unit = {
    writeFile(file, fileRecords, compression,
      useLzo = compression == CompressionType.BLOCK, syncEvery = syncEvery) {
      (writer, key, value) => writer.appendRaw(key, 0, key.length, new RawValueBytes(value))
    }
  }

  private def writeWritableFile(
      file: File,
      fileRecords: Seq[(Array[Byte], Array[Byte])],
      compression: CompressionType,
      syncEvery: Int = 0): Unit = {
    writeFile(file, fileRecords, compression, useLzo = false, syncEvery = syncEvery) {
      (writer, key, value) => writer.append(new BytesWritable(key), new BytesWritable(value))
    }
  }

  private def writeLzoPartFiles(
      directory: File, fileCount: Int, rowCount: Int, payloadSize: Int): Unit = {
    (0 until fileCount).foreach { fileIndex =>
      val fileRecords = (0 until rowCount).map { rowIndex =>
        intBytes(rowIndex) -> payload(fileIndex, rowIndex, payloadSize)
      }
      writeRawFile(new File(directory, f"part-$fileIndex%05d.seq"),
        fileRecords, CompressionType.BLOCK)
    }
  }

  private def source(
      spark: SparkSession,
      file: File,
      splitMaxSize: Option[Long] = None): RDD[(BytesWritable, BytesWritable)] = {
    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
    splitMaxSize.foreach(size => conf.setLong(splitMaxSizeKey, size))
    spark.sparkContext.newAPIHadoopFile(
      file.getAbsolutePath,
      classOf[SequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable],
      conf)
  }

  private def copiedDataFrame(
      spark: SparkSession,
      file: File,
      splitMaxSize: Option[Long] = None): DataFrame = {
    import spark.implicits._
    source(spark, file, splitMaxSize).map { case (key, value) =>
      (Arrays.copyOfRange(key.getBytes, 0, key.getLength),
        Arrays.copyOfRange(value.getBytes, 0, value.getLength))
    }.toDF("key", "value")
  }

  private def readWithHadoop(
      spark: SparkSession,
      file: File,
      splitMaxSize: Option[Long] = None): Seq[(Seq[Byte], Seq[Byte])] = {
    source(spark, file, splitMaxSize).map { case (key, value) =>
      (Arrays.copyOf(key.getBytes, key.getLength).toSeq,
        Arrays.copyOf(value.getBytes, value.getLength).toSeq)
    }.collect().toSeq
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

  private def truncate(file: File): Unit = {
    val randomAccess = new RandomAccessFile(file, "rw")
    try {
      randomAccess.setLength(file.length() - 1)
    } finally {
      randomAccess.close()
    }
    val path = new Path(file.toURI)
    path.getFileSystem(new Configuration()) match {
      case checksumFileSystem: ChecksumFileSystem =>
        val checksum = checksumFileSystem.getChecksumFile(path)
        if (checksumFileSystem.getRawFileSystem.exists(checksum)) {
          assert(checksumFileSystem.getRawFileSystem.delete(checksum, false))
        }
      case _ =>
    }
  }

  private def replacementConf(
      enabled: Boolean = true,
      allowCpuPlan: Boolean = false,
      batchSize: String = "2m",
      maxRows: Option[Int] = None,
      keepReadsInOrder: Boolean = true): SparkConf = {
    val conf = new SparkConf()
      .set(replaceConfKey, enabled.toString)
      .set("spark.sql.files.maxPartitionBytes", "32m")
      .set("spark.sql.files.openCostInBytes", "1")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, batchSize)
      .set(RapidsConf.MULTITHREAD_READ_NUM_THREADS.key, "2")
      .set(RapidsConf.MULTITHREAD_READ_MEMORY_LIMIT_ENABLED.key, "true")
      .set(RapidsConf.MULTITHREAD_READ_MEMORY_LIMIT_SIZE.key, "16m")
      .set(RapidsConf.MULTITHREAD_READ_MEMORY_LIMIT_TEST_PER_STAGE_POOL.key, "true")
      .set(RapidsConf.READER_MULTITHREADED_READ_KEEP_ORDER.key, keepReadsInOrder.toString)
    maxRows.foreach(rows => conf.set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, rows.toString))
    if (allowCpuPlan) {
      conf.set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "SerializeFromObjectExec,ExternalRDDScanExec,ProjectExec")
    }
    conf
  }

  private def withSequenceFileSession(
      conf: SparkConf)(test: (SparkSession, File) => Unit): Unit = {
    withTempPath { file =>
      writeRawFile(file, records, CompressionType.BLOCK)
      withGpuSparkSession(spark => test(spark, file), conf)
    }
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

  private def assertGpuRead(
      df: DataFrame,
      expected: Seq[(Seq[Byte], Seq[Byte])]): GpuSequenceFileRDDScanExec = {
    val scan = gpuScan(df)
    assert(bag(collectPairs(df)) == bag(expected))
    scan
  }

  private def assertCpuRead(df: DataFrame, expected: Seq[(Seq[Byte], Seq[Byte])]): Unit = {
    assertCpuRddScan(df)
    assert(bag(collectPairs(df)) == bag(expected))
  }

  private def gpuReadFailure(df: DataFrame): Exception = {
    gpuScan(df)
    intercept[Exception] {
      df.collect()
    }
  }

  test("replace the exact copy closure for BLOCK-LZO multi-file input") {
    withTempPath { directory =>
      assert(directory.mkdirs())
      writeLzoPartFiles(directory, fileCount = 3, rowCount = 32, payloadSize = 512)
      writeRawFile(new File(directory, "part-00003.seq"),
        Seq(Array.empty[Byte] -> Array.empty[Byte]), CompressionType.BLOCK)
      writeRawFile(new File(directory, "part-00004.seq"), Seq.empty, CompressionType.BLOCK)

      withGpuSparkSession({ spark =>
        val df = copiedDataFrame(spark, directory)
        val scan = assertGpuRead(df, readWithHadoop(spark, directory))
        assert(scan.sourceColumns ==
          Seq(SequenceFileRddReadProof.Key, SequenceFileRddReadProof.Value))
      }, replacementConf(keepReadsInOrder = false))
    }
  }

  compressionTypes.foreach { compression =>
    test(s"replace RDD read preserves $compression key and value bytes") {
      withTempPath { file =>
        val fileRecords = Seq(
          Array.emptyByteArray -> Array.emptyByteArray,
          intBytes(1) -> Array[Byte](0, 0, 0, 3, 'a'.toByte, 'b'.toByte, 'c'.toByte),
          intBytes(2) -> Array.fill[Byte](64 * 1024)(42.toByte))
        writeWritableFile(file, fileRecords, compression)

        withGpuSparkSession({ spark =>
          val expected = readWithHadoop(spark, file)
          val df = copiedDataFrame(spark, file)
          assertGpuRead(df, expected)
        }, replacementConf())
      }
    }
  }

  compressionTypes.foreach { compression =>
    test(s"replace RDD read preserves every record across $compression Hadoop splits") {
      withTempPath { file =>
        val fileRecords = (0 until 96).map { index =>
          intBytes(index) -> randomPayload(0, index, 4096)
        }
        writeWritableFile(file, fileRecords, compression, syncEvery = 8)

        withGpuSparkSession({ spark =>
          val splitMaxSize = Some(64 * 1024L)
          val expected = readWithHadoop(spark, file, splitMaxSize)
          val df = copiedDataFrame(spark, file, splitMaxSize)
          assert(assertGpuRead(df, expected).sourceRdd.getNumPartitions > 1)
        }, replacementConf())
      }
    }
  }

  test("replace RDD read emits multiple byte-bounded and row-bounded batches") {
    withTempPath { file =>
      val expected = (0 until 4).map { index =>
        intBytes(index) -> payload(0, index, 2048)
      }
      writeRawFile(file, expected, CompressionType.BLOCK)
      val expectedPairs = expected.map { case (key, value) => key.toSeq -> value.toSeq }

      withGpuSparkSession({ spark =>
        val df = copiedDataFrame(spark, file)
        val scan = assertGpuRead(df, expectedPairs)
        assert(scan.allMetrics(GpuMetric.NUM_OUTPUT_BATCHES).value > 1)
      }, replacementConf(batchSize = "8k"))

      withGpuSparkSession({ spark =>
        val df = copiedDataFrame(spark, file)
        val scan = assertGpuRead(df, expectedPairs)
        assert(scan.allMetrics(GpuMetric.NUM_OUTPUT_BATCHES).value >= 2)
      }, replacementConf(maxRows = Some(2)))
    }
  }

  test("early termination closes the completion-order RDD reader") {
    withTempPath { directory =>
      assert(directory.mkdirs())
      writeLzoPartFiles(directory, fileCount = 4, rowCount = 16, payloadSize = 2048)

      withGpuSparkSession({ spark =>
        val initialHostHandles = SpillFramework.stores.hostStore.numHandles
        val df = copiedDataFrame(spark, directory).limit(1)
        assert(df.collect().length == 1)
        assert(gpuScan(df).allMetrics(GpuMetric.NUM_OUTPUT_BATCHES).value > 0)
        eventually(timeout(10.seconds)) {
          assert(TestUtils.numRunningMultiFileReaderTasks == 0)
          assert(SpillFramework.stores.hostStore.numHandles == initialHostHandles)
        }
      }, replacementConf(batchSize = "32k", keepReadsInOrder = false)
        .set("spark.rapids.sql.exec.CollectLimitExec", "true")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "CollectLimitExec"))
    }
  }

  test("replace RDD read fails closed for oversized records and splits") {
    withTempPath { file =>
      writeRawFile(file,
        Seq(intBytes(0) -> payload(0, 0, 128 * 1024)), CompressionType.BLOCK)

      withGpuSparkSession({ spark =>
        val df = copiedDataFrame(spark, file)
        val error = gpuReadFailure(df)
        assert(exceptionContains(error, "record exceeds") &&
          exceptionContains(error, "decompressed batch limit"))
      }, replacementConf(batchSize = "64k"))
    }

    withTempPath { file =>
      writeRawFile(file, (0 until 5).map { index =>
        intBytes(index) -> payload(0, index, 2048)
      }, CompressionType.BLOCK)

      withGpuSparkSession({ spark =>
        val df = copiedDataFrame(spark, file, splitMaxSize = Some(1024 * 1024L))
        val scan = gpuScan(df)
        assert(scan.sourceRdd.getNumPartitions == 1)
        val error = intercept[Exception] {
          df.collect()
        }
        assert(exceptionContains(error, "split exceeds the 2 buffered batch limit"))
      }, replacementConf(batchSize = "8k"))
    }
  }

  compressionTypes.foreach { compression =>
    test(s"replace RDD read fails closed for a truncated $compression file") {
      withTempPath { file =>
        val fileRecords = (0 until 17).map { index =>
          intBytes(index) -> randomPayload(0, index, 1024)
        }
        writeWritableFile(file, fileRecords, compression, syncEvery = 4)
        truncate(file)

        withGpuSparkSession({ spark =>
          val df = copiedDataFrame(spark, file)
          val error = gpuReadFailure(df)
          assert(exceptionContains(error, "Truncated SequenceFile"), error.toString)
        }, replacementConf())
      }
    }
  }

  test("disabled RDD replacement preserves the original CPU plan and result") {
    withSequenceFileSession(
        replacementConf(enabled = false, allowCpuPlan = true)) { (spark, file) =>
      val df = copiedDataFrame(spark, file)
      assertCpuRead(df, recordPairs)
    }
  }

  test("replace the swapped closure and preserve value/key provenance") {
    withSequenceFileSession(replacementConf()) { (spark, file) =>
      val df = swappedDataFrame(spark, file)
      val scan = assertGpuRead(df, recordPairs.map(_.swap))
      assert(scan.sourceColumns ==
        Seq(SequenceFileRddReadProof.Value, SequenceFileRddReadProof.Key))
    }
  }

  test("an additional RDD map keeps the CPU path and its result") {
    withSequenceFileSession(replacementConf(allowCpuPlan = true)) { (spark, file) =>
      val df = extraMapDataFrame(spark, file)
      assertCpuRead(df, recordPairs)
    }
  }

  test("a captured closure keeps the CPU path and preserves its result") {
    withSequenceFileSession(replacementConf(allowCpuPlan = true)) { (spark, file) =>
      val df = capturedDataFrame(spark, file, extraBytes = 1)
      assertCpuRead(df, recordPairs.map { case (key, value) =>
        (key :+ 0.toByte) -> value
      })
    }
  }

  test("spark_partition_id keeps the CPU path and partition semantics") {
    withSequenceFileSession(replacementConf(allowCpuPlan = true)) { (spark, file) =>
      val df = copiedDataFrame(spark, file)
        .select(col("key"), col("value"), spark_partition_id().as("partition_id"))
      assertCpuRddScan(df)
      val actual = df.collect().map { row =>
        ((row.getAs[Array[Byte]](0).toSeq, row.getAs[Array[Byte]](1).toSeq), row.getInt(2))
      }.toSeq
      assert(bag(actual.map(_._1)) == bag(recordPairs))
      assert(actual.forall(_._2 == 0))
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
