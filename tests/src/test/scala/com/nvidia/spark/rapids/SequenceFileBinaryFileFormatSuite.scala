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

package com.nvidia.spark.rapids

import java.io.{DataOutputStream, File, RandomAccessFile}
import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ChecksumFileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.{CompressionCodec, DefaultCodec}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.types.BinaryType

class SequenceFileBinaryFileFormatSuite extends SparkQueryCompareTestSuite {
  private val formatName = "com.nvidia.spark.rapids.SequenceFileBinaryFileFormat"

  private final class RawValueBytes(bytes: Array[Byte]) extends SequenceFile.ValueBytes {
    override def writeUncompressedBytes(out: DataOutputStream): Unit = out.write(bytes)

    override def writeCompressedBytes(out: DataOutputStream): Unit = {
      throw new UnsupportedOperationException("RawValueBytes does not support RECORD compression")
    }

    override def getSize: Int = bytes.length
  }

  private def intBytes(value: Int): Array[Byte] = Array(
    (value >>> 24).toByte,
    (value >>> 16).toByte,
    (value >>> 8).toByte,
    value.toByte)

  private def payload(fileIndex: Int, rowIndex: Int, size: Int = 512): Array[Byte] = {
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

  private def lzoCodec(conf: Configuration): CompressionCodec = {
    val codecClass = Class.forName("io.airlift.compress.lzo.LzoCodec")
      .asSubclass(classOf[CompressionCodec])
    ReflectionUtils.newInstance(codecClass, conf)
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

  private def writeRawFile(
      file: File,
      records: Seq[(Array[Byte], Array[Byte])],
      compression: CompressionType,
      syncEvery: Int = 0): Unit = {
    val writer = createWriter(file, compression, useLzo = compression == CompressionType.BLOCK)
    try {
      records.zipWithIndex.foreach { case ((key, value), index) =>
        writer.appendRaw(key, 0, key.length, new RawValueBytes(value))
        syncIfNeeded(writer, index, records.length, syncEvery)
      }
    } finally {
      writer.close()
    }
  }

  private def writeWritableFile(
      file: File,
      records: Seq[(Array[Byte], Array[Byte])],
      compression: CompressionType,
      syncEvery: Int = 0): Unit = {
    val writer = createWriter(file, compression, useLzo = false)
    try {
      records.zipWithIndex.foreach { case ((key, value), index) =>
        writer.append(new BytesWritable(key), new BytesWritable(value))
        syncIfNeeded(writer, index, records.length, syncEvery)
      }
    } finally {
      writer.close()
    }
  }

  private def syncIfNeeded(
      writer: SequenceFile.Writer,
      index: Int,
      recordCount: Int,
      syncEvery: Int): Unit = {
    if (syncEvery > 0 && (index + 1) % syncEvery == 0 && index + 1 < recordCount) {
      writer.sync()
    }
  }

  private def read(spark: SparkSession, path: File): DataFrame = {
    spark.read.format(formatName).load(path.getAbsolutePath)
  }

  private def readWithHadoop(
      spark: SparkSession,
      path: File): Seq[(Seq[Byte], Seq[Byte])] = {
    spark.sparkContext.newAPIHadoopFile(
      path.getAbsolutePath,
      classOf[SequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable]).map { case (key, value) =>
        (java.util.Arrays.copyOf(key.getBytes, key.getLength).toSeq,
          java.util.Arrays.copyOf(value.getBytes, value.getLength).toSeq)
      }.collect().toSeq
  }

  private def collectRecords(df: DataFrame): Seq[(Seq[Byte], Seq[Byte])] = {
    df.collect().map { row =>
      (row.getAs[Array[Byte]](0).toSeq, row.getAs[Array[Byte]](1).toSeq)
    }.toSeq
  }

  private def scan(df: DataFrame): GpuFileSourceScanExec = {
    val scans = df.queryExecution.executedPlan.collect {
      case fileScan: GpuFileSourceScanExec => fileScan
    }
    assert(scans.length == 1, s"Expected one GPU file scan:\n${df.queryExecution.executedPlan}")
    scans.head
  }

  private def bag[T](values: Seq[T]): Map[T, Int] =
    values.groupBy(identity).map { case (value, copies) => value -> copies.size }

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

  private def gpuConf(
      batchSize: String = "2m",
      maxRows: Option[Int] = None,
      keepReadsInOrder: Boolean = true,
      maxPartitionBytes: String = "32m"): SparkConf = {
    val conf = new SparkConf()
      .set("spark.sql.files.maxPartitionBytes", maxPartitionBytes)
      .set("spark.sql.files.openCostInBytes", "1")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, batchSize)
      .set(RapidsConf.MULTITHREAD_READ_NUM_THREADS.key, "2")
      .set(RapidsConf.MULTITHREAD_READ_MEMORY_LIMIT_ENABLED.key, "true")
      .set(RapidsConf.MULTITHREAD_READ_MEMORY_LIMIT_SIZE.key, "16m")
      .set(RapidsConf.MULTITHREAD_READ_MEMORY_LIMIT_TEST_PER_STAGE_POOL.key, "true")
      .set(RapidsConf.READER_MULTITHREADED_READ_KEEP_ORDER.key, keepReadsInOrder.toString)
    maxRows.foreach(rows => conf.set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, rows.toString))
    conf
  }

  test("BLOCK LZO reads appendRaw key and value bytes") {
    withTempPath { directory =>
      assert(directory.mkdirs())
      val expected = (0 until 3).flatMap { fileIndex =>
        val records = (0 until 32).map { rowIndex =>
          intBytes(rowIndex) -> payload(fileIndex, rowIndex)
        }
        writeRawFile(new File(directory, f"part-$fileIndex%05d.seq"),
          records, CompressionType.BLOCK)
        records.map { case (key, value) => (key.toSeq, value.toSeq) }
      } :+ ((Seq.empty[Byte], Seq.empty[Byte]))
      writeRawFile(new File(directory, "part-00003.seq"),
        Seq(Array.empty[Byte] -> Array.empty[Byte]), CompressionType.BLOCK)
      writeRawFile(new File(directory, "part-00004.seq"), Seq.empty, CompressionType.BLOCK)

      withGpuSparkSession({ spark =>
        val df = read(spark, directory).select("key", "value")
        assert(df.schema.fields.forall(_.dataType == BinaryType))
        assert(bag(collectRecords(df)) == bag(expected))
        assert(scan(df).requiredSchema.fieldNames.sameElements(Array("key", "value")))
      }, gpuConf(keepReadsInOrder = false))
    }
  }

  Seq(CompressionType.NONE, CompressionType.RECORD, CompressionType.BLOCK).foreach { compression =>
    test(s"$compression bytes and projections match SequenceFileAsBinaryInputFormat") {
      withTempPath { file =>
        val records = Seq(
          Array.emptyByteArray -> Array.emptyByteArray,
          intBytes(1) -> Array[Byte](0, 0, 0, 3, 'a'.toByte, 'b'.toByte, 'c'.toByte),
          intBytes(2) -> Array.fill[Byte](64 * 1024)(42.toByte))
        writeWritableFile(file, records, compression)

        withGpuSparkSession({ spark =>
          val expected = readWithHadoop(spark, file)
          assert(expected.length == records.length)

          val both = read(spark, file).select("key", "value")
          assert(bag(collectRecords(both)) == bag(expected))

          val keys = read(spark, file).select("key")
          assert(bag(keys.collect().map(_.getAs[Array[Byte]](0).toSeq).toSeq) ==
            bag(expected.map(_._1)))
          assert(scan(keys).requiredSchema.fieldNames.sameElements(Array("key")))

          val values = read(spark, file).select("value")
          assert(bag(values.collect().map(_.getAs[Array[Byte]](0).toSeq).toSeq) ==
            bag(expected.map(_._2)))
          assert(scan(values).requiredSchema.fieldNames.sameElements(Array("value")))

          val countOnly = read(spark, file).select()
          assert(countOnly.collect().length == expected.length)
          assert(scan(countOnly).requiredSchema.isEmpty)
        }, gpuConf())
      }
    }
  }

  Seq(CompressionType.NONE, CompressionType.RECORD, CompressionType.BLOCK).foreach { compression =>
    test(s"$compression split reads have no missing or duplicate records") {
      withTempPath { file =>
        val records = (0 until 96).map { index =>
          intBytes(index) -> randomPayload(0, index, 4096)
        }
        writeWritableFile(file, records, compression, syncEvery = 8)

        withGpuSparkSession({ spark =>
          val expected = readWithHadoop(spark, file)
          assert(expected.length == records.length)
          val df = read(spark, file).select("key", "value")
          assert(scan(df).inputRDD.getNumPartitions > 1)
          val actual = collectRecords(df)
          assert(actual.length == records.length)
          assert(bag(actual) == bag(expected))
        }, gpuConf(maxPartitionBytes = "64k"))
      }
    }
  }

  test("one file emits multiple byte-bounded and row-bounded batches") {
    withTempPath { file =>
      val records = (0 until 4).map { index =>
        intBytes(index) -> payload(0, index, 2048)
      }
      writeRawFile(file, records, CompressionType.BLOCK)
      val expected = records.map { case (key, value) => (key.toSeq, value.toSeq) }

      withGpuSparkSession({ spark =>
        val bytes = read(spark, file).select("key", "value")
        assert(bag(collectRecords(bytes)) == bag(expected))
        assert(scan(bytes).allMetrics(GpuMetric.NUM_OUTPUT_BATCHES).value > 1)
      }, gpuConf(batchSize = "8k"))

      withGpuSparkSession({ spark =>
        val rows = read(spark, file).select()
        assert(rows.collect().length == records.length)
        assert(scan(rows).allMetrics(GpuMetric.NUM_OUTPUT_BATCHES).value >= 4)
      }, gpuConf(maxRows = Some(1)))
    }
  }

  test("oversized records and splits fail closed") {
    withTempPath { file =>
      writeRawFile(file,
        Seq(intBytes(0) -> payload(0, 0, 128 * 1024)), CompressionType.BLOCK)

      withGpuSparkSession({ spark =>
        val error = intercept[Exception] {
          read(spark, file).select("key", "value").collect()
        }
        assert(exceptionContains(error, "record exceeds") &&
          exceptionContains(error, "decompressed batch limit"))
      }, gpuConf(batchSize = "64k"))
    }

    withTempPath { file =>
      writeRawFile(file, (0 until 5).map { index =>
        intBytes(index) -> payload(0, index, 2048)
      }, CompressionType.BLOCK)

      withGpuSparkSession({ spark =>
        val error = intercept[Exception] {
          read(spark, file).select("key", "value").collect()
        }
        assert(exceptionContains(error, "split exceeds the 2 buffered batch limit") &&
          exceptionContains(error, "spark.sql.files.maxPartitionBytes"))
      }, gpuConf(batchSize = "8k"))
    }
  }

  Seq(CompressionType.NONE, CompressionType.RECORD, CompressionType.BLOCK).foreach { compression =>
    test(s"truncated $compression file fails closed") {
      withTempPath { file =>
        val records = (0 until 17).map { index =>
          intBytes(index) -> randomPayload(0, index, 1024)
        }
        writeWritableFile(file, records, compression, syncEvery = 4)
        truncate(file)

        withGpuSparkSession({ spark =>
          val error = intercept[Exception] {
            read(spark, file).select("value").collect()
          }
          assert(exceptionContains(error, "Truncated SequenceFile"), error.toString)
        }, gpuConf())
      }
    }
  }

  test("input_file_name stays with records across splits and chunks") {
    withTempPath { directory =>
      assert(directory.mkdirs())
      (0 until 2).foreach { fileIndex =>
        val records = (0 until 24).map { rowIndex =>
          intBytes(rowIndex) -> randomPayload(fileIndex, rowIndex, 2048)
        }
        writeRawFile(new File(directory, f"part-$fileIndex%05d.seq"),
          records, CompressionType.BLOCK, syncEvery = 4)
      }

      withGpuSparkSession({ spark =>
        val df = read(spark, directory).selectExpr("value", "input_file_name() AS file")
        val fileScan = scan(df)
        assert(fileScan.inputRDD.getNumPartitions > 1)
        assert(fileScan.queryUsesInputFile)
        val rows = df.collect()
        assert(rows.length == 48)
        rows.foreach { row =>
          val fileIndex = row.getAs[Array[Byte]](0)(0) & 0xff
          assert(new Path(row.getString(1)).getName == f"part-$fileIndex%05d.seq")
        }
        assert(fileScan.allMetrics(GpuMetric.NUM_OUTPUT_BATCHES).value > 2)
      }, gpuConf(
        batchSize = "32k",
        maxRows = Some(7),
        keepReadsInOrder = false,
        maxPartitionBytes = "16k"))
    }
  }

  test("limit closes unread chunks") {
    withTempPath { directory =>
      assert(directory.mkdirs())
      (0 until 4).foreach { fileIndex =>
        val records = (0 until 16).map { rowIndex =>
          intBytes(rowIndex) -> payload(fileIndex, rowIndex, 2048)
        }
        writeRawFile(new File(directory, f"part-$fileIndex%05d.seq"),
          records, CompressionType.BLOCK)
      }

      withGpuSparkSession({ spark =>
        val df = read(spark, directory).select("value").limit(1)
        assert(df.collect().length == 1)
        assert(scan(df).allMetrics(GpuMetric.NUM_OUTPUT_BATCHES).value > 0)
      }, gpuConf(batchSize = "32k", keepReadsInOrder = false)
        .set("spark.rapids.sql.exec.CollectLimitExec", "true")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "CollectLimitExec"))
    }
  }

  test("cost optimizer cannot move the GPU-only scan to CPU") {
    withTempPath { file =>
      writeRawFile(file, Seq(intBytes(0) -> payload(0, 0)), CompressionType.BLOCK)

      withGpuSparkSession({ spark =>
        val df = read(spark, file).select("value")
        assert(df.collect().length == 1)
        assert(scan(df).requiredSchema.fieldNames.sameElements(Array("value")))
      }, gpuConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
        .set("spark.rapids.sql.optimizer.cpu.exec.FileSourceScanExec", "0")
        .set("spark.rapids.sql.optimizer.gpu.exec.FileSourceScanExec", "1000000"))
    }
  }

  test("CPU execution fails instead of silently using a row reader") {
    withTempPath { file =>
      writeRawFile(file, Seq(intBytes(0) -> payload(0, 0)), CompressionType.BLOCK)

      withCpuSparkSession { spark =>
        val error = intercept[Exception] {
          read(spark, file).collect()
        }
        assert(exceptionContains(error, "internal GPU-only data source"))
      }
    }
  }
}
