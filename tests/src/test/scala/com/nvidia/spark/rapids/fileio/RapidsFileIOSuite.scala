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

package com.nvidia.spark.rapids.fileio

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.jni.fileio.{RapidsFileIO, RapidsInputFile, RapidsOutputFile}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path, RawLocalFileSystem}

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

/** Records which locations Spark opened and can serve a remote URI from a local test file. */
class RecordingFileIO(val options: JMap[String, String]) extends RapidsFileIO {
  @transient private lazy val local = new HadoopFileIO(new Configuration())

  private def location(path: String, operation: String): String = {
    Option(options.get("recordDir")).foreach { dir =>
      Files.write(new File(dir, operation).toPath, path.getBytes(UTF_8))
    }
    Option(options.get("target")).getOrElse(path)
  }

  override def newInputFile(path: String): RapidsInputFile =
    local.newInputFile(location(path, "read"))

  override def newOutputFile(path: String): RapidsOutputFile =
    local.newOutputFile(location(path, "write"))
}

/** Serves the test nameservice from disk using the filesystem configured on the Hadoop job. */
class LocalTestHdfs extends RawLocalFileSystem {
  override def getUri: URI = URI.create("hdfs://test-nn")
}

class MetadataOnlyTestHdfs extends LocalTestHdfs {
  override def open(path: Path, bufferSize: Int): FSDataInputStream =
    throw new AssertionError(s"Expected RapidsFileIO to open $path")
}

class RapidsFileIOSuite extends SparkQueryCompareTestSuite {
  private val prefix = "spark.sql.rapids.fileio."
  private val recording = classOf[RecordingFileIO].getName

  test("a direct implementation receives a snapshot of its original string options") {
    val conf = new SQLConf()
    conf.setConfString(prefix + "impl", recording)
    conf.setConfString(prefix + "Mixed.Key", "  01 TRUE  ")
    conf.setConfString(prefix + "empty", "")
    conf.setConfString("spark.sql.other.key", "unrelated")
    val config = FileIOConfig.from(conf)
    conf.setConfString(prefix + "Mixed.Key", "changed")

    val options = config.load(new Configuration()).asInstanceOf[RecordingFileIO].options
    assert(options.asScala.toMap == Map("Mixed.Key" -> "  01 TRUE  ", "empty" -> ""))
    intercept[UnsupportedOperationException] { options.put("new", "value") }
  }

  test("the resolver scopes each delegate and uses the job's Hadoop fallback") {
    withTempPath { root =>
      Files.createDirectories(root.toPath)
      val conf = new SQLConf()
      conf.setConfString(prefix + "impl", "ResolvingRapidsFileIO")
      conf.setConfString(prefix + "key1", "root")
      conf.setConfString(prefix + "s3.impl", recording)
      conf.setConfString(prefix + "s3.key1", " S3 ")
      conf.setConfString(prefix + "s3.target", new File(root, "object").toURI.toString)
      conf.setConfString(prefix + "s3.recordDir", root.getPath)
      conf.setConfString(prefix + "gs.impl", recording)
      conf.setConfString(prefix + "gs.key1", "GCS")

      val hadoop = new Configuration()
      FileSystem.setDefaultUri(hadoop, "s3://default-bucket")
      hadoop.setClass("fs.hdfs.impl", classOf[LocalTestHdfs], classOf[FileSystem])
      hadoop.setBoolean("fs.hdfs.impl.disable.cache", true)
      val io = FileIOConfig.from(conf).load(hadoop).asInstanceOf[ResolvingRapidsFileIO]
      val s3 = io.delegateFor(new Path("s3://bucket/a")).asInstanceOf[RecordingFileIO]
      assert(s3.options.asScala.toMap == Map("key1" -> " S3 ", "recordDir" -> root.getPath,
        "target" -> new File(root, "object").toURI.toString))
      assert(io.delegateFor(new Path("S3://another-bucket/b")) eq s3)
      val gs = io.delegateFor(new Path("gs://bucket/c")).asInstanceOf[RecordingFileIO]
      assert(gs.options.asScala.toMap == Map("key1" -> "GCS"))
      assert(io.delegateFor(new Path("hdfs://nn/a")).isInstanceOf[HadoopFileIO])
      assert(io.delegateFor(new Path("s3a://bucket/a")).isInstanceOf[HadoopFileIO])

      withResource(io.newOutputFile("S3://bucket/a").create())(_.write(42))
      assert(new String(Files.readAllBytes(new File(root, "write").toPath), UTF_8) ==
        "S3://bucket/a")
      withResource(io.newInputFile(new Path("/b")).open())(in => assert(in.read() == 42))
      assert(new String(Files.readAllBytes(new File(root, "read").toPath), UTF_8) ==
        "s3://default-bucket/b")

      val hdfsFile = new Path("hdfs", "test-nn", new File(root, "fallback").getAbsolutePath)
      withResource(io.newOutputFile(hdfsFile).create())(_.write(7))
      withResource(io.newInputFile(hdfsFile.toString).open())(in => assert(in.read() == 7))
    }
  }

  test("invalid explicit implementations identify the setting that selected them") {
    val conf = new SQLConf()
    assert(FileIOConfig.from(conf).load(new Configuration()).isInstanceOf[HadoopFileIO])
    for (name <- Seq("missing.TestFileIO", classOf[String].getName,
        classOf[FileIOWithoutMapConstructor].getName)) {
      conf.setConfString(prefix + "impl", name)
      val error = intercept[IllegalArgumentException] { FileIOConfig.from(conf) }
      assert(error.getMessage.contains(prefix + "impl=" + name))
      assert(error.getCause != null)
    }
    conf.setConfString(prefix + "impl", classOf[ResolvingRapidsFileIO].getName)
    conf.setConfString(prefix + "s3.impl", "missing.TestFileIO")
    val io = FileIOConfig.from(conf).load(new Configuration())
    val error = intercept[IllegalArgumentException] { io.newOutputFile("s3://bucket/file") }
    assert(error.getMessage.contains(prefix + "s3.impl=missing.TestFileIO"))
  }

  for (format <- Seq("parquet", "orc")) {
    test(s"filtered $format scans use Hadoop only for file discovery") {
      withTempPath { data =>
        withCpuSparkSession(spark =>
          spark.range(0, 12, 1, 1).write.format(format).save(data.getPath))
        val part = data.listFiles().find(_.getName.endsWith("." + format)).get
        val location = new Path("hdfs", "test-nn", part.getAbsolutePath).toString
        val conf = new SparkConf().set(prefix + "impl", "ResolvingRapidsFileIO")
          .set(prefix + "hdfs.impl", recording).set(prefix + "hdfs.target", part.toURI.toString)
        val rows = withGpuSparkSession(spark => spark.read.format(format).schema("id BIGINT")
          .option("fs.hdfs.impl", classOf[MetadataOnlyTestHdfs].getName)
          .option("fs.hdfs.impl.disable.cache", "true")
          .load(location).where("id >= 5 AND id < 8").collect(), conf)
        assert(rows.map(_.getLong(0)).toSeq == Seq(5L, 6L, 7L))
      }
    }

    test(s"GPU $format writes and both reader factories use the configured FileIO") {
      withTempPath { root =>
        Files.createDirectories(root.toPath)
        val data = new File(root, "data").getAbsolutePath
        val conf = new SparkConf()
        if (format == "parquet") {
          conf.set(prefix + "impl", recording).set(prefix + "recordDir", root.getPath)
        } else {
          conf.set(prefix + "impl", "ResolvingRapidsFileIO")
            .set(prefix + "file.impl", recording).set(prefix + "file.recordDir", root.getPath)
        }
        withGpuSparkSession(spark => spark.range(0, 12, 1, 2).write.format(format).save(data), conf)
        assert(new File(root, "write").isFile)
        val expected = withCpuSparkSession(spark =>
          spark.read.format(format).load(data).collect().map(_.getLong(0)).sorted.toSeq)
        for (reader <- Seq("PERFILE", "MULTITHREADED")) {
          val readMarker = new File(root, "read").toPath
          Files.deleteIfExists(readMarker)
          conf.set(s"spark.rapids.sql.format.$format.reader.type", reader)
          val actual = withGpuSparkSession(spark =>
            spark.read.format(format).load(data).collect().map(_.getLong(0)).sorted.toSeq, conf)
          assert(actual == expected)
          assert(Files.exists(readMarker), reader)
        }
      }
    }
  }
}

class FileIOWithoutMapConstructor extends RecordingFileIO(Map.empty[String, String].asJava)
