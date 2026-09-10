/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil.fullyDelete
import org.apache.hadoop.fs.Path
import org.apache.orc.{OrcConf, OrcFile, StripeInformation}
import org.apache.orc.impl.RecordReaderImpl

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.rapids.{ExecutionPlanCaptureCallback, MyDenseVector, MyDenseVectorUDT}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.types._

/**
 * This corresponds to the Spark class:
 * org.apache.spark.sql.execution.datasources.orc.OrcQueryTest
 */
class OrcQuerySuite extends SparkQueryCompareTestSuite {

  private def getSchema: StructType = new StructType(Array(
    StructField("c0", DataTypes.IntegerType),
    StructField("c1", new MyDenseVectorUDT)
  ))

  private def getData: Seq[Row] = Seq(Row(1, new MyDenseVector(Array(0.25, 2.25, 4.25))))

  private def getDf(spark: SparkSession): DataFrame = {
    spark.createDataFrame(
      SparkContext.getOrCreate().parallelize(getData, numSlices = 1),
      getSchema)
  }

  private def withHadoopConf[T](
      hadoopConf: Configuration,
      optionName: String,
      optionValue: Option[String])(body: => T): T = {
    val previousValue = Option(hadoopConf.getRaw(optionName))
    try {
      optionValue match {
        case Some(value) => hadoopConf.set(optionName, value)
        case None => hadoopConf.unset(optionName)
      }
      body
    } finally {
      previousValue match {
        case Some(value) => hadoopConf.set(optionName, value)
        case None => hadoopConf.unset(optionName)
      }
    }
  }

  private def assertGpuOrcWriteWithOptions(
      spark: SparkSession,
      options: Map[String, String]): Unit = {
    withTempPath { outputPath =>
      ExecutionPlanCaptureCallback.startCapture()
      spark.range(10).write.mode("overwrite")
        .options(options)
        .orc(outputPath.getCanonicalPath)
      val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
      assert(plans.nonEmpty, "Did not capture GPU write plan")
      ExecutionPlanCaptureCallback.assertContains(plans(0), "GpuDataWritingCommandExec")
    }
  }

  Seq("orc", "").foreach { v1List =>
    val sparkConf = new SparkConf().set("spark.sql.sources.useV1SourceList", v1List)
    testGpuWriteFallback(
      "Writing User Defined Type(UDT) to ORC fall back, source list is (" + v1List + ")",
      "DataWritingCommandExec",
      spark => getDf(spark),
      // WriteFilesExec is a new operator from Spark version 340, for simplicity, add it here for
      // all Spark versions.
      execsAllowedNonGpu = Seq("DataWritingCommandExec", "WriteFilesExec", "ShuffleExchangeExec"),
      conf = sparkConf
    ) { frame =>
      val tempFile = File.createTempFile("orc-test-udt-write", ".orc")
      try {
        frame.write.mode("overwrite").orc(tempFile.getAbsolutePath)
      } finally {
        fullyDelete(tempFile)
      }
    }
  }

  private val unsupportedOrcEncodingOptions = Seq(
    OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getAttribute -> "1.0",
    OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getHiveConfName -> "1.0",
    OrcConf.DIRECT_ENCODING_COLUMNS.getAttribute -> "value",
    OrcConf.BLOOM_FILTER_COLUMNS.getAttribute -> "value")

  private val nullOrcEncodingOptions = Seq(
    OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getAttribute,
    OrcConf.DIRECT_ENCODING_COLUMNS.getAttribute)

  Seq("orc" -> "ORC in V1 source list", "" -> "empty V1 source list").foreach {
    case (v1List, sourceListDescription) =>
    unsupportedOrcEncodingOptions.foreach { case (optionName, optionValue) =>
      val sparkConf = new SparkConf().set("spark.sql.sources.useV1SourceList", v1List)
      testGpuWriteFallback(
        s"ORC write falls back for per-write option $optionName, $sourceListDescription",
        "DataWritingCommandExec",
        spark => spark.range(10).selectExpr("CAST(id AS STRING) AS value"),
        execsAllowedNonGpu = Seq(
          "DataWritingCommandExec", "WriteFilesExec", "ShuffleExchangeExec"),
        conf = sparkConf
      ) { frame =>
        withTempPath { outputPath =>
          frame.write.mode("overwrite")
            .option(optionName, optionValue)
            .orc(outputPath.getCanonicalPath)
        }
      }

      testGpuWriteFallback(
        s"ORC write falls back for Hadoop configuration $optionName, " +
          sourceListDescription,
        "DataWritingCommandExec",
        spark => spark.range(10).selectExpr("CAST(id AS STRING) AS value"),
        execsAllowedNonGpu = Seq(
          "DataWritingCommandExec", "WriteFilesExec", "ShuffleExchangeExec"),
        conf = sparkConf
      ) { frame =>
        val hadoopConf = frame.sparkSession.sparkContext.hadoopConfiguration
        withHadoopConf(hadoopConf, optionName, Some(optionValue)) {
          withTempPath { outputPath =>
            frame.write.mode("overwrite").orc(outputPath.getCanonicalPath)
          }
        }
      }
    }

    test(s"ORC write stays on GPU for empty direct-encoding columns, " +
        sourceListDescription) {
      val sparkConf = new SparkConf().set("spark.sql.sources.useV1SourceList", v1List)
      withGpuSparkSession({ spark =>
        assertGpuOrcWriteWithOptions(spark,
          Map(OrcConf.DIRECT_ENCODING_COLUMNS.getAttribute -> ""))
      }, sparkConf)
    }

    nullOrcEncodingOptions.foreach { optionName =>
      test(s"ORC write stays on GPU for null per-write option $optionName, " +
          sourceListDescription) {
        val sparkConf = new SparkConf().set("spark.sql.sources.useV1SourceList", v1List)
        withGpuSparkSession({ spark =>
          val hadoopConf = spark.sparkContext.hadoopConfiguration
          withHadoopConf(hadoopConf, optionName, None) {
            assertGpuOrcWriteWithOptions(spark, Map(optionName -> (null: String)))
          }
        }, sparkConf)
      }
    }

    test(s"ORC write stays on GPU for substituted empty direct-encoding columns, " +
        sourceListDescription) {
      val sparkConf = new SparkConf().set("spark.sql.sources.useV1SourceList", v1List)
      withGpuSparkSession({ spark =>
        val emptyVariable = "rapids.test.orc.empty"
        val hadoopConf = spark.sparkContext.hadoopConfiguration
        withHadoopConf(hadoopConf, emptyVariable, Some("")) {
          assertGpuOrcWriteWithOptions(spark, Map(
            OrcConf.DIRECT_ENCODING_COLUMNS.getAttribute -> ("${" + emptyVariable + "}")))
        }
      }, sparkConf)
    }
  }

  Seq("orc", "").foreach { v1List =>
    val sparkConf = new SparkConf().set("spark.sql.sources.useV1SourceList", v1List)
    Seq(false, true).foreach { prolepticGregorian =>
      Seq(
        "struct" -> "named_struct('date', CAST('1001-01-01' AS DATE)) AS value",
        "array" -> "array(CAST('1001-01-01' AS DATE)) AS value"
      ).foreach { case (nestedType, selectExpr) =>
        testGpuWriteFallback(
          s"ORC date write falls back without calendar metadata, source list is ($v1List), " +
            s"proleptic=$prolepticGregorian, nested type=$nestedType",
          "DataWritingCommandExec",
          spark => spark.range(1).selectExpr(selectExpr),
          execsAllowedNonGpu = Seq(
            "DataWritingCommandExec", "WriteFilesExec", "ShuffleExchangeExec"),
          conf = sparkConf
        ) { frame =>
          withTempPath { outputPath =>
            frame.write.mode("overwrite")
              .option(OrcConf.PROLEPTIC_GREGORIAN.getAttribute, prolepticGregorian.toString)
              .orc(outputPath.getCanonicalPath)
          }
        }
      }
    }
  }

  Seq("orc", "").foreach { v1List =>
    val sparkConf = new SparkConf().set("spark.sql.sources.useV1SourceList", v1List)
    test(s"Write Spark version into ORC file metadata, source list is ($v1List)") {
      withGpuSparkSession({ spark =>
        withTempPath { path =>
          ExecutionPlanCaptureCallback.startCapture()
          spark.range(1).repartition(1).write.orc(path.getCanonicalPath)
          val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
          assert(plans.nonEmpty, "Did not capture GPU write plan")
          ExecutionPlanCaptureCallback.assertContains(plans(0), "GpuDataWritingCommandExec")

          val partFiles = path.listFiles()
            .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
          assert(partFiles.length === 1)

          val orcFilePath = new Path(partFiles.head.getAbsolutePath)
          val readerOptions = OrcFile.readerOptions(new Configuration())
          withResourceIfAllowed(OrcFile.createReader(orcFilePath, readerOptions)) { reader =>
            val version = UTF_8.decode(
              reader.getMetadataValue("org.apache.spark.version")).toString
            assert(version === SPARK_VERSION_SHORT)
          }
        }
      }, sparkConf)
    }
  }

  /**
   * file meta is: struct<c0:int,c1:array<double>>,
   * The MyDenseVectorUDT type is converted to array<double> in the ORC file.
   * Gpu can read this ORC file when not specifying the schema
   */
  testSparkReadResultsAreEqual("Reading User Defined Type(UDT) from ORC, not specify schema",
    (file: File) => (spark: SparkSession) => {
      // not specify schema
      spark.read.orc(file.getCanonicalPath())
    },
    (spark: SparkSession, file: File) => {
      val df = getDf(spark)
      df.write.orc(file.getCanonicalPath)
    }
  ) {
    frame => frame
  }

  Seq("orc", "").foreach { v1List =>
    val sparkConf = new SparkConf().set("spark.sql.sources.useV1SourceList", v1List)
    testSparkReadResultsAreEqual(
      s"SPARK-36663 read numeric-only ORC field names, source list is ($v1List)",
      (file: File) => (spark: SparkSession) => spark.read.orc(file.getCanonicalPath),
      (spark: SparkSession, file: File) => {
        spark.sql(
          """
            |SELECT
            |  'a' AS `1`,
            |  named_struct('20', 'b', '30', named_struct('40', 'c')) AS `50`,
            |  array(named_struct('123', 'd')) AS `789`,
            |  map('key', named_struct('456', 'e')) AS `012`
          """.stripMargin)
          .coalesce(1)
          .write
          .orc(file.getCanonicalPath)
      },
      conf = sparkConf,
      existClasses = if (v1List == "orc") "GpuFileSourceScanExec" else "GpuBatchScan") { frame =>
      frame.selectExpr(
        "`1`",
        "`50`.`20`",
        "`50`.`30`.`40`",
        "`789`[0].`123`",
        "`012`['key'].`456`")
    }
  }

  /**
   * file meta is: struct<c0:int,c1:array<double>>,
   * The MyDenseVectorUDT type is converted to array<double> in the ORC file.
   */
  testGpuReadFallback("Reading User Defined Type(UDT) from ORC falls back when specify schema",
    "FileSourceScanExec",
    (file: File) => (spark: SparkSession) => {
      // specify schema
      spark.read.schema(getSchema).orc(file.getCanonicalPath())
    },
    (spark: SparkSession, file: File) => {
      val df = getDf(spark)
      df.write.orc(file.getCanonicalPath)
    },
    execsAllowedNonGpu = Seq("FileSourceScanExec", "ShuffleExchangeExec")
  ) {
    frame => frame
  }

  /**
   * Find a orc file in orcDir and get the columns encoding info for the first Stripe
   *
   * @param orcDir  orc file directory
   * @param columns column indices, it's the indices in ORC file, column 0 is always for the root
   *                meta. The indices is from a tree structure with depth-first iteration. For
   *                example: schema: the Ids in struct<struct<x1:int, x2: int>, y:int> are:
   *
   *                0: struct<struct<x1:int, x2: int>, y:int>
   *                     /                       \
   *                1: struct<x1:int, x2: int>    4: y:int
   *                   /          \
   *                2: x1:int     3: x2: int
   * @return (encodingKind, dictionarySize) list for each columns passed in
   */
  private def getDictEncodingInfo(orcDir: File, columns: Array[Int]): Array[(String, Int)] = {
    val orcFile = orcDir.listFiles(f => f.getName.endsWith(".orc"))(0)
    val p = new Path(orcFile.getCanonicalPath)
    val conf = OrcFile.readerOptions(new Configuration())

    // cdh321 and cdh330 use a lower ORC version, and the reader is not a AutoCloseable,
    // so use withResourceIfAllowed
    withResourceIfAllowed(OrcFile.createReader(p, conf)) { reader =>
      withResource(reader.rows().asInstanceOf[RecordReaderImpl]) { rows =>
        val stripe: StripeInformation = reader.getStripes.get(0)
        val stripeFooter = rows.readStripeFooter(stripe)
        columns.map(i => (stripeFooter.getColumns(i).getKind.toString,
            stripeFooter.getColumns(i).getDictionarySize))
      }
    }
  }

  /**
   * This corresponds to Spark case:
   * https://github.com/apache/spark/blob/v3.4.0/sql/core/src/test/scala/org/apache/spark/sql/
   * execution/datasources/orc/OrcQuerySuite.scala#L359
   */
  test("SPARK-5309 strings stored using dictionary compression in orc") {
    def getEncodings: SparkSession => Array[(String, Int)] = { spark =>
      withTempPath { tmpDir =>
        // first column dic is ("s0", "s1"), second column dic is ("s0", "s1", "s2")
        val data = (0 until 1000).map(i => ("s" + i % 2, "s" + i % 3))

        // write to ORC, columns is [_1, _2]
        spark.createDataFrame(data).coalesce(1).write.mode("overwrite")
            .orc(tmpDir.getAbsolutePath)

        // get columns encoding
        getDictEncodingInfo(tmpDir, columns = Array(1, 2))
      }
    }
    // get CPU encoding info
    val cpuEncodings = withCpuSparkSession(getEncodings)

    // get GPU encoding info
    ExecutionPlanCaptureCallback.startCapture()
    val gpuEncodings = withGpuSparkSession(getEncodings)
    val plan = ExecutionPlanCaptureCallback.getResultsWithTimeout()
    ExecutionPlanCaptureCallback.assertContains(plan(0), "GpuDataWritingCommandExec")

    assertResult(cpuEncodings)(gpuEncodings)
    gpuEncodings.foreach { case (encodingKind, _) =>
      assert(encodingKind.toUpperCase.contains("DICTIONARY"))
    }
  }

  private def getOrcFileSuffix(compression: String): String =
    if (Seq("NONE", "UNCOMPRESSED").contains(compression)) {
      ".orc"
    } else {
      s".${compression.toLowerCase()}.orc"
    }

  def checkCompressType(compression: Option[String], orcCompress: Option[String]): Unit = {
    withGpuSparkSession { spark =>
      withTempPath { file =>
        var writer = spark.range(0, 10).write
        writer = compression.map(t => writer.option("compression", t)).getOrElse(writer)
        writer = orcCompress.map(t => writer.option("orc.compress", t)).getOrElse(writer)
        // write ORC file on GPU
        writer.orc(file.getCanonicalPath)

        // expectedType: first use compression, then orc.compress
        var expectedType = compression.getOrElse(orcCompress.get)
        // ORC use NONE for UNCOMPRESSED
        if (expectedType == "UNCOMPRESSED") expectedType = "NONE"
        val maybeOrcFile = file.listFiles()
            .find(_.getName.endsWith(getOrcFileSuffix(expectedType)))
        assert(maybeOrcFile.isDefined)

        // check the compress type using ORC jar
        val orcFilePath = new Path(maybeOrcFile.get.getAbsolutePath)
        val conf = OrcFile.readerOptions(new Configuration())

        // the reader is not a AutoCloseable for Spark CDH, so use `withResourceIfAllowed`
        // 321cdh uses lower ORC: orc-core-1.5.1.7.1.7.1000-141.jar
        // 330cdh uses lower ORC: orc-core-1.5.1.7.1.8.0-801.jar
        withResourceIfAllowed(OrcFile.createReader(orcFilePath, conf)) { reader =>
          // check
          assert(expectedType === reader.getCompressionKind.name)
        }
      }
    }
  }

  private val supportedWriteCompressTypes = {
    // GPU ORC writing does not support ZLIB, LZ4, refer to GpuOrcFileFormat
    val supportedWriteCompressType = ListBuffer("UNCOMPRESSED", "NONE", "ZSTD", "SNAPPY")
    // Cdh321, Cdh330 does not support ZSTD, refer to the Cdh Class:
    // org.apache.spark.sql.execution.datasources.orc.OrcOptions
    // Spark 31x do not support lz4, zstd
    if (isCdh321 || isCdh330 || isCdh332 || !VersionUtils.isSpark320OrLater) {
      supportedWriteCompressType -= "ZSTD"
    }
    supportedWriteCompressType
  }

  test("SPARK-16610: Respect orc.compress (i.e., OrcConf.COMPRESS) when compression is unset") {
    // Respect `orc.compress` (i.e., OrcConf.COMPRESS).
    supportedWriteCompressTypes.foreach { orcCompress =>
      checkCompressType(None, Some(orcCompress))
    }

    // make pairs, e.g.: [("UNCOMPRESSED", "NONE"), ("NONE", "SNAPPY"), ("SNAPPY", "ZSTD") ... ]
    val pairs = supportedWriteCompressTypes.sliding(2).toList.map(pair => (pair.head, pair.last))

    // "compression" overwrite "orc.compress"
    pairs.foreach { case (compression, orcCompress) =>
      checkCompressType(Some(compression), Some(orcCompress))
    }
  }

  test("Compression options for writing to an ORC file (SNAPPY, ZLIB and NONE)") {
    supportedWriteCompressTypes.foreach { compression =>
      checkCompressType(Some(compression), None)
    }
  }

  test("orc dictionary encoding with lots of rows for nested types") {
    assume(false, "TODO: move this case to scale test in future")
    val rowsNum = 1000000

    // orc.dictionary.key.threshold default is 0.8
    // orc.dictionary.early.check default is true
    // orc.row.index.stride default is 10000
    // For more details, refer to
    // https://github.com/apache/orc/blob/v1.7.4/java/core/src/java/org/apache/orc/OrcConf.java
    // If the first 10000 rows has less than 10000 * 0.8 cardinality, then use dictionary encoding.
    // Only String column will use dictionary encoding. Refer to the ORC code:
    // https://github.com/apache/orc/tree/v1.7.4/java/core/src/java/org/apache/orc/impl/writer
    val cardinality = (10000 * 0.75).toInt

    def getEncodings: SparkSession => Array[(String, Int)] = { spark =>
      withTempPath { tmpDir =>
        // For more info about the column Ids, refer to the desc of `getDictEncodingInfo`
        val schema = StructType(Seq(              // column Id in ORC is: 0
          StructField("name", StringType),        // column Id in ORC is: 1, String type
          StructField("addresses", ArrayType(     // column Id in ORC is: 2
            StructType(Seq(                       // column Id in ORC is: 3
              StructField("street", StringType),  // column Id in ORC is: 4, String type
              StructField("city", StringType),    // column Id in ORC is: 5, String type
              StructField("state", StringType),   // column Id in ORC is: 6, String type
              StructField("zip", IntegerType)     // column Id in ORC is: 7
            )))),
          StructField("phone_numbers", MapType(   // column Id in ORC is: 8
            StringType,                           // column Id in ORC is: 9, String type
            ArrayType(                            // column Id in ORC is: 10
              StringType)))                       // column Id in ORC is: 11, String type
        ))

        val data = (0 until rowsNum).map(i => {
          val v = i % cardinality
          Row("John" + v,
            Seq(Row("123 Main St" + v, "Any town" + v, "CA" + v, v)),
            Map("home" + v -> Seq("111-222-3333" + v, "444-555-6666" + v)))
        })

        // Create the DataFrame
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

        // write to a ORC file
        df.coalesce(1).write.mode("overwrite").orc(tmpDir.getAbsolutePath)

        // get columns encodings
        // Only pass the String Ids
        getDictEncodingInfo(tmpDir, columns = Array(1, 4, 5, 6, 9, 11))
      }
    }

    // get CPU encoding info
    val cpuEncodings = withCpuSparkSession(getEncodings)

    // get GPU encoding info
    ExecutionPlanCaptureCallback.startCapture()
    val gpuEncodings = withGpuSparkSession(getEncodings)
    val plan = ExecutionPlanCaptureCallback.getResultsWithTimeout()
    ExecutionPlanCaptureCallback.assertContains(plan(0), "GpuDataWritingCommandExec")

    assertResult(cpuEncodings)(gpuEncodings)
    gpuEncodings.foreach { case (encodingKind, _) =>
      assert(encodingKind.toUpperCase.contains("DICTIONARY"))
    }
  }
}
