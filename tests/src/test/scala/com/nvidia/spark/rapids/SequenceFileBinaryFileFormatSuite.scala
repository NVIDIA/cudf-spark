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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ChecksumFileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
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

  private def lzoCodec(conf: Configuration): CompressionCodec = {
    val codecClass = Class.forName("io.airlift.compress.lzo.LzoCodec")
      .asSubclass(classOf[CompressionCodec])
    ReflectionUtils.newInstance(codecClass, conf)
  }

  private def writeRawFile(
      file: File,
      records: Seq[(Array[Byte], Array[Byte])],
      compression: CompressionType): Unit = {
    val conf = new Configuration()
    val compressionOption = compression match {
      case CompressionType.BLOCK =>
        SequenceFile.Writer.compression(compression, lzoCodec(conf))
      case _ => SequenceFile.Writer.compression(compression)
    }
    val writer = SequenceFile.createWriter(
      conf,
      SequenceFile.Writer.file(new Path(file.toURI)),
      SequenceFile.Writer.keyClass(classOf[BytesWritable]),
      SequenceFile.Writer.valueClass(classOf[BytesWritable]),
      compressionOption)
    try {
      records.foreach { case (key, value) =>
        writer.appendRaw(key, 0, key.length, new RawValueBytes(value))
      }
    } finally {
      writer.close()
    }
  }

  private def read(spark: SparkSession, path: File): DataFrame = {
    spark.read.format(formatName).load(path.getAbsolutePath)
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

  private def gpuConf(
      batchSize: String = "2m",
      maxRows: Option[Int] = None): SparkConf = {
    val conf = new SparkConf()
      .set("spark.sql.files.maxPartitionBytes", "32m")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, batchSize)
      .set(RapidsConf.MULTITHREAD_READ_NUM_THREADS.key, "2")
      .set(RapidsConf.MULTITHREAD_READ_MEMORY_LIMIT_ENABLED.key, "true")
      .set(RapidsConf.MULTITHREAD_READ_MEMORY_LIMIT_SIZE.key, "16m")
      .set(RapidsConf.MULTITHREAD_READ_MEMORY_LIMIT_TEST_PER_STAGE_POOL.key, "true")
    maxRows.foreach(rows => conf.set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, rows.toString))
    conf
  }

  test("BLOCK LZO reads projected raw values from multiple files") {
    withTempPath { directory =>
      assert(directory.mkdirs())
      val expected = (0 until 3).flatMap { fileIndex =>
        val records = (0 until 1100).map { rowIndex =>
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
        assert(scan(df).inputRDD.getNumPartitions == 1)
        val actual = df.collect().map { row =>
          (row.getAs[Array[Byte]](0).toSeq, row.getAs[Array[Byte]](1).toSeq)
        }
        assert(actual.length == expected.length)
        assert(bag(actual) == bag(expected))
        val values = read(spark, directory).select("value").collect()
          .map(_.getAs[Array[Byte]](0).toSeq)
        assert(bag(values) == bag(expected.map(_._2)))
        val keys = read(spark, directory).select("key").collect()
          .map(_.getAs[Array[Byte]](0).toSeq)
        assert(bag(keys) == bag(expected.map(_._1)))
        val fileScan = scan(df)
        assert(fileScan.requiredSchema.fieldNames.sameElements(Array("key", "value")))
        assert(fileScan.allMetrics(GpuMetric.NUM_OUTPUT_BATCHES).value == 4)
      }, gpuConf())
    }
  }

  test("count-only projection reads rows without materializing columns") {
    withTempPath { file =>
      writeRawFile(file,
        (0 until 17).map(index => intBytes(index) -> payload(0, index)),
        CompressionType.BLOCK)

      withGpuSparkSession({ spark =>
        val df = read(spark, file).select()
        assert(df.collect().length == 17)
        assert(scan(df).requiredSchema.isEmpty)
      }, gpuConf())
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

  test("unsupported compression fails closed") {
    withTempPath { file =>
      writeRawFile(file, Seq(intBytes(0) -> payload(0, 0)), CompressionType.NONE)

      withGpuSparkSession({ spark =>
        val error = intercept[Exception] {
          read(spark, file).collect()
        }
        assert(exceptionContains(error, "requires BLOCK compression"))
      }, gpuConf())
    }
  }

  test("truncated BLOCK file fails closed") {
    withTempPath { file =>
      writeRawFile(file,
        (0 until 17).map(index => intBytes(index) -> payload(0, index)),
        CompressionType.BLOCK)
      val randomAccess = new RandomAccessFile(file, "rw")
      try {
        randomAccess.setLength(file.length() - 1)
      } finally {
        randomAccess.close()
      }
      val path = new Path(file.toURI)
      val fileSystem = path.getFileSystem(new Configuration())
        .asInstanceOf[ChecksumFileSystem]
      // Remote filesystems do not have local checksum sidecars to detect this truncation first.
      assert(fileSystem.getRawFileSystem.delete(fileSystem.getChecksumFile(path), false))

      withGpuSparkSession({ spark =>
        Seq(Seq("value"), Seq("key"), Seq.empty).foreach { projection =>
          val error = intercept[Exception] {
            val input = read(spark, file)
            if (projection.isEmpty) input.select().collect()
            else input.select(projection.head, projection.tail: _*).collect()
          }
          assert(exceptionContains(error, "Truncated SequenceFile block"),
            s"projection=$projection, error=$error")
        }
      }, gpuConf())
    }
  }

  test("decompressed output limits fail closed") {
    withTempPath { file =>
      writeRawFile(file,
        Seq(intBytes(0) -> payload(0, 0, 128 * 1024)),
        CompressionType.BLOCK)

      withGpuSparkSession({ spark =>
        val error = intercept[Exception] {
          read(spark, file).select("value").collect()
        }
        assert(exceptionContains(error, "decompressed output limit"))
      }, gpuConf("64k"))
    }

    withTempPath { file =>
      writeRawFile(file,
        (0 until 17).map(index => intBytes(index) -> payload(0, index)),
        CompressionType.BLOCK)

      withGpuSparkSession({ spark =>
        val error = intercept[Exception] {
          read(spark, file).select("value").collect()
        }
        assert(exceptionContains(error, "exceeds the 16 row file limit"))
      }, gpuConf(maxRows = Some(16)))
    }
  }
}
