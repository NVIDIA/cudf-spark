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
import java.util

import scala.annotation.nowarn

import com.nvidia.spark.rapids.{RapidsConf, SparkQueryCompareTestSuite, TestUtils}
import com.nvidia.spark.rapids.delta.{DeltaProvider, NoDeltaProvider}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, NumRecordsStats}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.{WriteIntoDelta, WriteIntoDeltaLike}
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, VariantType}

class DeltaRuntimeShimSuite extends SparkQueryCompareTestSuite {
  private def isDelta43: Boolean = DeltaRuntimeShim.isDelta43Version(io.delta.VERSION)

  test("Delta 4.3 test profile selects the required runtime") {
    sys.props.get("rapids.test.delta43.version") match {
      case Some(expectedVersion) =>
        assert(isDelta43, s"delta43-tests must run against Delta 4.3.x, found ${io.delta.VERSION}")
        assert(io.delta.VERSION == expectedVersion)
      case None =>
        assert(!isDelta43,
          s"rapids.test.delta43.version must be set when testing Delta ${io.delta.VERSION}")
        cancel("delta43-tests profile is not active")
    }
  }

  test("delta provider resolves from the installed Delta Lake version") {
    val provider = DeltaProvider()
    assert(provider ne NoDeltaProvider)
    val expectedProvider = io.delta.VERSION match {
      case "4.0.0" | "4.0.1" => "Delta40xProvider"
      case "4.1.0" => "Delta41xProvider"
      case "4.2.0" => "Delta42xProvider"
      case _ if isDelta43 => "Delta43xProvider"
      case _ => fail(s"Unexpected Delta Lake version: ${io.delta.VERSION}")
    }
    assert(provider.getClass.getSimpleName == s"$expectedProvider$$")
  }

  test("Delta 4.2 runtime shim selection covers supported Spark versions") {
    Seq("4.0.1", "4.1.1").foreach { sparkVersion =>
      val shimClassName = DeltaRuntimeShim.getDelta42ShimClassName("4.2.0", sparkVersion)
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
      val error = intercept[IllegalStateException] {
        DeltaRuntimeShim.getDelta42ShimClassName(deltaVersion, sparkVersion)
      }
      assert(error.getMessage.contains(deltaVersion))
      assert(error.getMessage.contains(sparkVersion))
    }
  }

  test("Delta 4.3 runtime shim selection covers supported Spark versions") {
    for {
      sparkVersion <- Seq("4.0.1", "4.1.1")
      deltaVersion <- Seq("4.3.0", "4.3.1", "4.3.2", "4.3.2.2")
    } {
      val shimClassName = DeltaRuntimeShim.getDelta43ShimClassName(deltaVersion, sparkVersion)
      assert(shimClassName.exists(_.contains("delta43x")))
    }
  }

  test("Delta 4.3 runtime shim selection rejects unsupported combinations") {
    val unsupported = Seq(
      ("4.3.0", "4.0.0"),
      ("4.3.0", "4.1.0"),
      ("4.3.0", "4.1.2"),
      ("4.3.2.2", "4.1.2"),
      ("4.3.", "4.0.1"),
      ("4.3.x", "4.0.1"))

    unsupported.foreach { case (deltaVersion, sparkVersion) =>
      val error = intercept[IllegalStateException] {
        DeltaRuntimeShim.getDelta43ShimClassName(deltaVersion, sparkVersion)
      }
      assert(error.getMessage.contains(deltaVersion))
      assert(error.getMessage.contains(sparkVersion))
    }
  }

  test("existing Delta versions use the pre-4.2 runtime shim selection") {
    Seq("2.1.0", "3.3.3", "4.0.0", "4.1.0").foreach { deltaVersion =>
      assert(DeltaRuntimeShim.getDelta42ShimClassName(deltaVersion, "4.0.1").isEmpty)
      assert(DeltaRuntimeShim.getDelta43ShimClassName(deltaVersion, "4.0.1").isEmpty)
    }
    assert(DeltaRuntimeShim.getDelta42ShimClassName("4.3.0", "4.0.1").isEmpty)
    Seq("4.2.0", "4.3", "4.4.0").foreach { deltaVersion =>
      assert(DeltaRuntimeShim.getDelta43ShimClassName(deltaVersion, "4.0.1").isEmpty, deltaVersion)
    }
  }

  test("GPU write factory has no default implementation") {
    val method = classOf[DeltaRuntimeShim].getMethod(
      "createGpuWrite", classOf[GpuDeltaLog], classOf[WriteIntoDelta])
    assert(Modifier.isAbstract(method.getModifiers))
  }

  test("Delta 4.2 and 4.3 GPU writes use the runtime-specific GPU implementation") {
    assume(io.delta.VERSION == "4.2.0" || isDelta43)
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
      case _ if isDelta43 => "GpuWriteIntoDelta43x"
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
    assume(isDelta43)
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
    assume(isDelta43)
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

        assert(CaseInsensitiveMap(resolvedOptions)(parquetOutputTimestampType) ==
          "TIMESTAMP_MICROS")
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

  test("Delta 4.3 is_managed_location filtering applies from 4.3.1 onward") {
    Seq("4.2.0", "4.3.", "4.3.x", "4.3.0", "4.3.0-rc1", "4.3.0-SNAPSHOT", "4.4.0").foreach {
      version => assert(!DeltaRuntimeShim.isDelta431OrLater(version), version)
    }
    Seq("4.3.1", "4.3.1-SNAPSHOT", "4.3.2", "4.3.2.2", "4.3.10", "4.3.2147483648").foreach {
      version => assert(DeltaRuntimeShim.isDelta431OrLater(version), version)
    }
    if (isDelta43) {
      assert(DeltaRuntimeShim.filterIsManagedLocationProperty ==
        DeltaRuntimeShim.isDelta431OrLater(io.delta.VERSION))
    } else {
      assert(!DeltaRuntimeShim.filterIsManagedLocationProperty)
    }
  }

  test("GPU Delta catalog matches CPU is_managed_location metadata behavior") {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    withTempPath { rootDir =>
      val warehouseDir = new java.io.File(rootDir, "warehouse")
      val conf = new SparkConf()
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.sql.warehouse.dir", warehouseDir.getAbsolutePath)
      TestUtils.withGpuSparkSession(conf) { spark =>
        val catalogObj = spark.sessionState.catalogManager.catalog("spark_catalog")
        assert(catalogObj.isInstanceOf[DeltaCatalog],
          s"Expected DeltaCatalog for spark_catalog, got ${catalogObj.getClass.getName}")
        val cpuCatalog = catalogObj.asInstanceOf[DeltaCatalog]
        val gpuCatalog = DeltaRuntimeShim.getGpuDeltaCatalog(
          cpuCatalog, new RapidsConf(spark.sessionState.conf))

        val cpuDir = new java.io.File(rootDir, "cpu_tbl")
        val gpuDir = new java.io.File(rootDir, "gpu_tbl")
        val suffix = java.util.UUID.randomUUID().toString.replace("-", "")
        val cpuName = s"delta_mloc_cpu_$suffix"
        val gpuName = s"delta_mloc_gpu_$suffix"

        def makeProps(dir: java.io.File): util.Map[String, String] = {
          val props = new util.HashMap[String, String]()
          props.put(TableCatalog.PROP_PROVIDER, "delta")
          props.put(TableCatalog.PROP_LOCATION, dir.toURI.toString)
          props.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
          props.put("custom_prop", "abc")
          props
        }

        try {
          val cpuStaged = (cpuCatalog.stageCreate(
            Identifier.of(Array("default"), cpuName),
            schema,
            Array.empty[Transform],
            makeProps(cpuDir)): @nowarn("msg=stageCreate.*is deprecated"))
          cpuStaged.commitStagedChanges()

          val gpuStaged = (gpuCatalog.stageCreate(
            Identifier.of(Array("default"), gpuName),
            schema,
            Array.empty[Transform],
            makeProps(gpuDir)): @nowarn("msg=stageCreate.*is deprecated"))
          gpuStaged.commitStagedChanges()

          val cpuCfg = DeltaLog.forTable(spark, cpuDir.getAbsolutePath)
            .unsafeVolatileSnapshot.metadata.configuration
          val gpuCfg = DeltaLog.forTable(spark, gpuDir.getAbsolutePath)
            .unsafeVolatileSnapshot.metadata.configuration

          assert(gpuCfg == cpuCfg)
          assert(gpuCfg.get("custom_prop").contains("abc"))
          assert(gpuCfg.contains(TableCatalog.PROP_IS_MANAGED_LOCATION) ==
            !DeltaRuntimeShim.isDelta431OrLater(io.delta.VERSION))
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS default.$cpuName")
          spark.sql(s"DROP TABLE IF EXISTS default.$gpuName")
        }
      }
    }
  }
}
