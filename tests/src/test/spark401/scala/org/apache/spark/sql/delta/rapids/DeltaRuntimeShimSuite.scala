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
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.delta.rapids

import java.lang.reflect.{InvocationTargetException, Modifier}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{RapidsConf, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.delta.{DeltaProvider, NoDeltaProvider}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, NumRecordsStats}
import org.apache.spark.sql.delta.commands.{WriteIntoDelta, WriteIntoDeltaLike}
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.internal.SQLConf
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
    Seq("4.0.1", "4.1.1").foreach { sparkVersion =>
      val shimClassName = DeltaRuntimeShim.getDelta43ShimClassName("4.3.0", sparkVersion)
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
    assert(DeltaRuntimeShim.getDelta43ShimClassName("4.2.0", "4.0.1").isEmpty)
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

  test("Delta 4.3 uses Spark 4.1's V1 saveAsTable marker") {
    assume(io.delta.VERSION == "4.3.0" && org.apache.spark.SPARK_VERSION.startsWith("4.1."))
    val v1Options = new DeltaOptions(
      Map("__v1_save_as_table_overwrite" -> "true"), new SQLConf)
    assert(DeltaRuntimeShim.isV1WriterSaveAsTableOverwrite(v1Options, SaveMode.Overwrite))
    assert(!DeltaRuntimeShim.isV1WriterSaveAsTableOverwrite(v1Options, SaveMode.Append))
    val v2Options = new DeltaOptions(Map.empty[String, String], new SQLConf)
    assert(!DeltaRuntimeShim.isV1WriterSaveAsTableOverwrite(v2Options, SaveMode.Overwrite))
  }

  test("Delta 4.3 prepared replacement copies retain GPU dispatch and prepared fields") {
    assume(io.delta.VERSION == "4.3.0")
    withCpuSparkSession { spark =>
      val deltaLog = mock[DeltaLog]
      val gpuDeltaLog = new GpuDeltaLog(deltaLog, new RapidsConf(Map.empty[String, String]))
      val originalData = spark.range(3).toDF()
      val cpuWrite = WriteIntoDelta(deltaLog, SaveMode.Overwrite,
        new DeltaOptions(Map("replaceUsing" -> "id"), spark.sessionState.conf),
        Nil, Map.empty, originalData)
      // Reflection keeps this suite compilable against older Delta runtimes sharing these shims.
      val adapterClass = Class.forName(
        "org.apache.spark.sql.delta.rapids.delta43x.GpuPreparedReplaceWrite43x")
      val prepared = adapterClass.getConstructors.head.newInstance(gpuDeltaLog, cpuWrite)
        .asInstanceOf[WriteIntoDelta]
      val materializedData = spark.range(2, 5).toDF()
      val preparedOptions = new DeltaOptions(Map(
        "replaceUsing" -> "id", "targetAlias" -> "__replace_using_table_alias__"),
        spark.sessionState.conf)
      val copyMethod = adapterClass.getMethods.find(m =>
        m.getName == "copy" && m.getParameterCount == 9).get
      val copied = copyMethod.invoke(prepared, deltaLog, SaveMode.Overwrite, preparedOptions,
        Nil, Map("prepared" -> "true"), materializedData, None, None, Boolean.box(true))
        .asInstanceOf[WriteIntoDelta]
      assert(copied.getClass == adapterClass)
      assert(copied.data eq materializedData)
      assert(copied.options eq preparedOptions)
      assert(copied.configuration == Map("prepared" -> "true"))
      assert(adapterClass.getMethod("copy$default$9").invoke(copied) == Boolean.box(true))
      val configured = copied.withNewWriterConfiguration(Map("updated" -> "true"))
      assert(configured.getClass == adapterClass)
      assert(configured.data eq materializedData)
      assert(configured.configuration == Map("updated" -> "true"))
    }
  }

  test("Delta 4.3 replacement factory retains upstream command and by-name preparation") {
    assume(io.delta.VERSION == "4.3.0")
    withCpuSparkSession { spark =>
      withTempPath { tablePath =>
        val deltaLog = DeltaLog.forTable(spark, tablePath.getAbsolutePath)
        val cpuWrite = WriteIntoDelta(deltaLog, SaveMode.Overwrite,
          new DeltaOptions(Map("replaceUsing" -> "id"), spark.sessionState.conf),
          Nil, Map.empty, spark.range(3).toDF())
        val gpuWrite = DeltaRuntimeShim.createGpuWrite(
          new GpuDeltaLog(deltaLog, new RapidsConf(Map.empty[String, String])), cpuWrite)
        assert(gpuWrite.getClass.getSimpleName == "GpuInsertReplaceOnOrUsingCommand")
        val cpuCommand = gpuWrite.getClass.getMethod("cpuCommand").invoke(gpuWrite)
        assert(cpuCommand.getClass.getSimpleName == "DeltaInsertReplaceOnOrUsingCommand")
        assert(cpuCommand.getClass.getMethod("byName").invoke(cpuCommand) == Boolean.box(true))
        val configured = gpuWrite.withNewWriterConfiguration(Map("updated" -> "true"))
        assert(configured.configuration == Map("updated" -> "true"))
        assert(configured.data eq cpuWrite.data)
        val rules = DeltaProvider().getRunnableCommandRules
        assert(rules.keys.exists(_.getSimpleName == "DeltaInsertReplaceOnOrUsingCommand"))
      }
    }
  }

  Seq("retry", "exhausted", "fatal").foreach { outcome =>
    test(s"Delta 4.3 replacement materialization cleanup on $outcome") {
      assume(io.delta.VERSION == "4.3.0")
      val conf = new SparkConf()
        .set("spark.databricks.delta.insertReplaceOnOrUsing.materializeSource", "all")
      withCpuSparkSession({ spark =>
        withTempPath { tablePath =>
          val deltaLog = DeltaLog.forTable(spark, tablePath.getAbsolutePath)
          val data = spark.range(3).selectExpr("id", "id * 10 AS unused")
          val cpuWrite = WriteIntoDelta(deltaLog, SaveMode.Overwrite,
            new DeltaOptions(Map("replaceUsing" -> "id"), spark.sessionState.conf),
            Nil, Map.empty, data)
          val gpuWrite = DeltaRuntimeShim.createGpuWrite(
            new GpuDeltaLog(deltaLog, new RapidsConf(Map.empty[String, String])), cpuWrite)
          val adapt = gpuWrite.getClass.getDeclaredMethod("adaptedCommand")
          adapt.setAccessible(true)
          val command = adapt.invoke(gpuWrite)
          val materializedRDD = command.getClass.getMethod("materializedSourceRDD")
          val execute = command.getClass.getDeclaredMethods
            .find(_.getName == "withMaterializeRetryAndStats").get
          execute.setAccessible(true)
          val maxAttempts = spark.conf.get(
            "spark.databricks.delta.merge.materializeSource.maxAttempts", "4").toInt
          val sources = ArrayBuffer.empty[RDD[_]]
          val fatal = new IllegalStateException("non-retryable replacement failure")
          val callback: WriteIntoDelta => String = prepared => {
            // The adapter must survive each upstream prepareWriteCmd copy, and materialization
            // must retain columns that are not referenced by the matching condition.
            assert(prepared.getClass.getSimpleName == "GpuPreparedReplaceWrite43x")
            assert(prepared.data.columns.toSeq == Seq("id", "unused"))
            assert(sources.forall(_.getStorageLevel == org.apache.spark.storage.StorageLevel.NONE))
            val rdd = materializedRDD.invoke(command).asInstanceOf[Option[RDD[_]]].get
            sources += rdd
            if (outcome == "fatal") {
              throw fatal
            } else if (sources.size == 1 || outcome == "exhausted") {
              throw new SparkException("CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND",
                Map("rddBlockId" -> s"rdd_${rdd.id}_0"), null)
            }
            "done"
          }
          if (outcome == "retry") {
            assert(execute.invoke(command, spark, callback) == "done")
            assert(sources.size == 2)
          } else {
            val error = intercept[InvocationTargetException] {
              execute.invoke(command, spark, callback)
            }.getCause
            if (outcome == "fatal") {
              assert(error eq fatal)
              assert(sources.size == 1)
            } else {
              assert(error.getMessage.contains("DELTA_MERGE_MATERIALIZE_SOURCE_FAILED_REPEATEDLY"))
              assert(sources.size == maxAttempts)
            }
          }
          assert(materializedRDD.invoke(command) == None)
          assert(sources.forall(_.getStorageLevel == org.apache.spark.storage.StorageLevel.NONE))
        }
      }, conf)
    }
  }

  test("Delta 4.3 atomic and partial overwrite classification remains distinct") {
    assume(io.delta.VERSION == "4.3.0")
    Seq(
      (Map("replaceWhere" -> "id = 1"), true, true),
      (Map("replaceOn" -> "target.id = source.id"), true, true),
      (Map("replaceUsing" -> "id"), true, true),
      (Map("partitionOverwriteMode" -> "dynamic"), false, true),
      (Map("partitionOverwriteMode" -> "static"), false, false)
    ).foreach { case (options, atomic, partial) =>
      val deltaOptions = new DeltaOptions(options, new SQLConf)
      assert(deltaOptions.getClass.getMethod("isInsertAtomicReplaceOp")
        .invoke(deltaOptions) == Boolean.box(atomic))
      assert(deltaOptions.getClass.getMethod("isInsertPartialOverwriteOp")
        .invoke(deltaOptions) == Boolean.box(partial))
    }
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
}
