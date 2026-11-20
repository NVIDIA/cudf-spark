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
{"spark": "401"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.delta.rapids

import java.lang.reflect.Modifier

import com.nvidia.spark.rapids.{GpuOverrides, RapidsConf, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.delta.{DeltaProvider, NoDeltaProvider}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, DeltaParquetFileFormat, NumRecordsStats}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.commands.{WriteIntoDelta, WriteIntoDeltaLike}
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, VariantType}

class DeltaRuntimeShimSuite extends SparkQueryCompareTestSuite {
  test("delta provider resolves from the installed Delta Lake version") {
    val provider = DeltaProvider()
    assert(provider ne NoDeltaProvider)
    val expectedProvider = io.delta.VERSION match {
      case "4.0.0" | "4.0.1" => "Delta40xProvider"
      case "4.1.0" => "Delta41xProvider"
      case "4.2.0" => "Delta42xProvider"
      case "4.3.0" => "Delta43xProvider"
      case _ => fail(s"Unexpected Delta Lake version: ${io.delta.VERSION}")
    }
    assert(provider.getClass.getSimpleName == s"$expectedProvider$$")
  }

  test("Delta 4.2 runtime shim selection covers supported Spark versions") {
    Seq("4.0.1", "4.1.1").foreach { sparkVersion =>
      val shimClassName = DeltaRuntimeShim.getDelta42PlusShimClassName("4.2.0", sparkVersion)
      assert(shimClassName.exists(_.contains("delta42x")))
    }
  }

  test("Delta 4.2 runtime shim selection rejects unsupported combinations") {
    val unsupported = Seq(
      ("4.2.0", "4.0.0"),
      ("4.2.0", "4.1.0"),
      ("4.2.0", "4.1.2"),
      ("4.2.1", "4.0.1"))

    unsupported.foreach { case (deltaVersion, sparkVersion) =>
      assert(DeltaRuntimeShim.getDelta42PlusShimClassName(deltaVersion, sparkVersion).isEmpty)
    }
    assert(DeltaRuntimeShim.getShimClassName("4.2.1").isEmpty)
  }

  test("Delta 4.3 runtime shim selection covers supported Spark versions") {
    Seq("4.0.1", "4.1.1").foreach { sparkVersion =>
      val shimClassName = DeltaRuntimeShim.getDelta42PlusShimClassName("4.3.0", sparkVersion)
      assert(shimClassName.exists(_.contains("delta43x")))
    }
  }

  test("Delta 4.3 runtime shim selection rejects unsupported combinations") {
    val unsupported = Seq(
      ("4.3.0", "4.0.0"),
      ("4.3.0", "4.1.0"),
      ("4.3.0", "4.1.2"),
      ("4.3.1", "4.0.1"))

    unsupported.foreach { case (deltaVersion, sparkVersion) =>
      assert(DeltaRuntimeShim.getDelta42PlusShimClassName(deltaVersion, sparkVersion).isEmpty)
    }
    assert(DeltaRuntimeShim.getShimClassName("4.3.1").isEmpty)
  }

  test("existing Delta versions use the pre-4.2 runtime shim selection") {
    val expectedShim = if (SPARK_VERSION.startsWith("4.0.")) "delta40x" else "delta41x"
    Seq("2.1.0", "3.3.3", "4.0.0", "4.1.0").foreach { deltaVersion =>
      assert(DeltaRuntimeShim.getDelta42PlusShimClassName(deltaVersion, "4.0.1").isEmpty)
      assert(DeltaRuntimeShim.getShimClassName(deltaVersion).exists(_.contains(expectedShim)))
    }
  }

  test("Delta 3.3 runtime shim selection requires Delta 3.3") {
    Seq("3.3.0", "3.3.3").foreach { deltaVersion =>
      assert(DeltaRuntimeShim.getDelta33ShimClassName(deltaVersion).exists(_.contains("delta33x")))
    }
    Seq("3.2.1", "3.1.0", "4.0.0").foreach { deltaVersion =>
      assert(DeltaRuntimeShim.getDelta33ShimClassName(deltaVersion).isEmpty)
    }
  }

  test("claimed DeltaParquetFileFormat scan runs on GPU") {
    withGpuSparkSession { spark =>
      withTempPath { tablePath =>
        val path = tablePath.getCanonicalPath
        spark.range(0, 5).toDF("id").write.parquet(path)
        val parquetScan = spark.read.parquet(path).queryExecution.sparkPlan.collectFirst {
          case s: FileSourceScanExec => s
        }.getOrElse(fail("Expected FileSourceScanExec"))
        val deltaFormat = DeltaParquetFileFormat(Protocol(), Metadata())
        val deltaScan = parquetScan.copy(
          relation = parquetScan.relation.copy(fileFormat = deltaFormat)(spark))
        val planned = GpuOverrides(spark).apply(deltaScan)
        assert(planned.isInstanceOf[GpuFileSourceScanExec])
      }
    }
  }

  test("GPU write factory has no default implementation") {
    val method = classOf[DeltaRuntimeShim].getMethod(
      "createGpuWrite", classOf[GpuDeltaLog], classOf[WriteIntoDelta])
    assert(Modifier.isAbstract(method.getModifiers))
  }

  test("Delta 4.2 and 4.3 GPU writes use the runtime-specific GPU implementation") {
    assume(Seq("4.2.0", "4.3.0").contains(io.delta.VERSION))
    val deltaLog = mock[DeltaLog]
    val cpuWrite = WriteIntoDelta(
      deltaLog,
      SaveMode.Append,
      new DeltaOptions(Map.empty[String, String], new SQLConf),
      partitionColumns = Nil,
      configuration = Map.empty,
      data = mock[Dataset[Row]])
    val gpuWrite = DeltaRuntimeShim.createGpuWrite(
      new GpuDeltaLog(deltaLog, new RapidsConf(Map.empty[String, String])), cpuWrite)

    val expectedClass = io.delta.VERSION match {
      case "4.2.0" => "GpuWriteIntoDelta42x"
      case "4.3.0" => "GpuWriteIntoDelta43x"
      case version => fail(s"Unexpected Delta Lake version: $version")
    }
    assert(gpuWrite.getClass.getSimpleName == expectedClass)
    assert(gpuWrite.isInstanceOf[GpuWriteIntoDeltaBase])
    assert(gpuWrite.isInstanceOf[LeafRunnableCommand])
    assert(gpuWrite.isInstanceOf[ImplicitMetadataOperation])
    assert(gpuWrite.withNewWriterConfiguration(Map("key" -> "value"))
      .getClass.getSimpleName == expectedClass)

    val accessor = classOf[WriteIntoDeltaLike]
      .getMethod("ReplaceWhereExprsAndDataFilterPresenceInExprs")
    assert(accessor.invoke(gpuWrite) != null)
  }

  test("Delta 4.3 DELETE and UPDATE validate mismatched record counts") {
    assume(io.delta.VERSION == "4.3.0")
    val deltaLog = mock[DeltaLog]
    val mismatchStats = NumRecordsStats(
      numLogicalRecordsAddedPartial = 2L,
      numLogicalRecordsRemovedPartial = 1L,
      numDeletionVectorRecordsAdded = 0L,
      numDeletionVectorRecordsRemoved = 0L,
      numFilesAddedWithoutNumRecords = 0L,
      numFilesRemovedWithoutNumRecords = 0L,
      numLogicalRecordsAddedInFilesWithDeletionVectorsPartial = 2L)

    def assertValidation(spark: org.apache.spark.sql.SparkSession): Unit = {
      spark.conf.set("spark.databricks.delta.numRecordsValidation.enabled", "true")
      Seq(
        () => DeltaRuntimeShim33x.validateDeleteNumRecords(spark, deltaLog, mismatchStats),
        () => DeltaRuntimeShim33x.validateUpdateNumRecords(spark, deltaLog, mismatchStats)
      ).foreach { validate =>
        val error = intercept[Exception](validate())
        assert(error.getMessage.contains("DELTA_NUM_RECORDS_MISMATCH"))
      }
    }

    withCpuSparkSession(assertValidation)
    withGpuSparkSession(assertValidation)
  }

  test("Delta 4.3 GPU transactions use the 4.3 writer behavior") {
    assume(io.delta.VERSION == "4.3.0")
    val parquetOutputTimestampType = "spark.sql.parquet.outputTimestampType"
    val parquetWriterVersion = "parquet.writer.version"

    withGpuSparkSession { spark =>
      withTempPath { tablePath =>
        val gpuDeltaLog = GpuDeltaLog.forTable(
          spark,
          tablePath.getAbsolutePath,
          Map.empty,
          new RapidsConf(Map.empty[String, String]))
        val txn = gpuDeltaLog.startTransaction()
        assert(txn.getClass.getSimpleName == "GpuOptimisticTransaction43x")

        val getWriterOptions = txn.getClass.getDeclaredMethods
          .find(_.getName == "getWriterOptions")
          .getOrElse(fail("Delta 4.3 transaction does not define getWriterOptions"))
        getWriterOptions.setAccessible(true)
        val writeOptions = new DeltaOptions(Map(
          parquetOutputTimestampType -> "TIMESTAMP_MICROS"),
          spark.sessionState.conf)
        val resolvedOptions = getWriterOptions.invoke(txn, Some(writeOptions))
          .asInstanceOf[Map[String, String]]

        assert(resolvedOptions(parquetOutputTimestampType) == "TIMESTAMP_MICROS")
        assert(resolvedOptions(parquetWriterVersion) == "v1")

        val encodeVariantStatsIfNeeded = txn.getClass.getDeclaredMethods
          .find(_.getName == "encodeVariantStatsIfNeeded")
          .getOrElse(fail("Delta 4.3 transaction does not define encodeVariantStatsIfNeeded"))
        encodeVariantStatsIfNeeded.setAccessible(true)
        val statsCollector = Literal.create(null,
          StructType(Seq(StructField("value", IntegerType))))
        val integerSchema = Seq(AttributeReference("value", IntegerType)())
        val variantSchema = Seq(AttributeReference("value", VariantType)())

        assert(encodeVariantStatsIfNeeded.invoke(txn, integerSchema, statsCollector) eq
          statsCollector)
        assert(encodeVariantStatsIfNeeded.invoke(txn, variantSchema, statsCollector)
          .getClass.getSimpleName == "EncodeNestedVariantAsZ85String")
      }
    }
  }
}
