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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.delta.delta42x

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.delta43x.Delta43xProvider
import com.nvidia.spark.rapids.parquet.{GpuParquetMultiFilePartitionReaderFactory,
  GpuParquetPartitionReaderFactory}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.delta.serverSidePlanning._
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

class ServerSidePlannedScanSuite extends AnyFunSuite {
  private val schema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("nested", StructType(Seq(StructField("a.b", StringType))))))

  private class Client(plan: ScanPlan) extends ServerSidePlanningClient {
    var calls = 0
    var closes = 0
    var request: Option[(String, String, Option[Filter], Option[Seq[String]], Option[Int])] = None
    override def canConvertFilters(filters: Array[Filter]): Boolean = true
    override def planScan(database: String, table: String, filter: Option[Filter],
        projection: Option[Seq[String]], limit: Option[Int]): ScanPlan = {
      calls += 1
      request = Some((database, table, filter, projection, limit))
      plan
    }
    override def close(): Unit = { closes += 1 }
  }

  test("both providers retain upstream V2 rules alongside the server-side scan rule") {
    val upstreamRules = DeltaV2ScanRule.rules
    assert(upstreamRules.nonEmpty)
    for (rules <- Seq(Delta42xProvider.getScans, Delta43xProvider.getScans)) {
      assert(upstreamRules.keySet.subsetOf(rules.keySet))
      assert(rules.contains(classOf[ServerSidePlannedScan]))
      assert(rules.size == upstreamRules.size + 1)
    }
  }

  test("reuse Delta's exact planning request, cached result and client lifecycle") {
    val credentials = new ScanPlanStorageCredentials {
      override def configure(conf: Configuration): Unit = {
        conf.set("fs.s3a.session.token", "temporary-test-token")
        conf.setBoolean("fs.s3a.impl.disable.cache", true)
      }
    }
    val plan = ScanPlan(Seq(ScanFile("s3a://bucket/a%20b.parquet", 123, "PARQUET")),
      Some(credentials))
    val client = new Client(plan)
    val required = StructType(Seq(schema("nested")))
    val filters: Array[Filter] = Array(EqualTo("id", 7), GreaterThan("id", 3))
    val cpu = new ServerSidePlannedScan(null, "catalog.db", "table", schema, client,
      filters, required, Some(5))
    val access = new ServerSidePlannedScanAccess(cpu)
    val gpu = GpuServerSidePlannedScan(cpu, access, new RapidsConf(Map.empty[String, String]))
    assert(client.calls == 0) // Constructing/tagging the adapter must not contact the server.
    assert(access.tableSchema == schema)
    assert(gpu.readSchema() == required)
    assert(access.plan eq plan) // Also exercise factory-before-partitions ordering.
    assert(gpu.planInputPartitions().toSeq == cpu.planInputPartitions().toSeq)
    assert(gpu.planInputPartitions().toSeq == Seq(
      ServerSidePlannedFileInputPartition("s3a://bucket/a%20b.parquet", 123, "PARQUET")))
    assert(client.request.contains(("catalog.db", "table", Some(And(filters(0), filters(1))),
      Some(Seq("nested.`a.b`")), Some(5))))
    assert(client.calls == 1)
    assert(client.closes == 1)
    val conf = new Configuration(false)
    access.plan.credentials.foreach(_.configure(conf))
    assert(conf.get("fs.s3a.session.token") == "temporary-test-token")
    assert(conf.getBoolean("fs.s3a.impl.disable.cache", false))
    assert(gpu.withInputFile() eq gpu)
  }

  test("scan equality depends only on the original Delta scan") {
    val client = new Client(ScanPlan(Seq.empty))
    def scan = new ServerSidePlannedScan(null, "db", "table", schema, client,
      Array.empty, schema, None)
    val cpu = scan
    def gpu(s: ServerSidePlannedScan) = GpuServerSidePlannedScan(s,
      new ServerSidePlannedScanAccess(s), new RapidsConf(Map.empty[String, String]))
    val first = gpu(cpu)
    val second = gpu(cpu)
    assert(first == second)
    assert(second == first)
    assert(first.hashCode() == second.hashCode())
    assert(Set(first, second).size == 1)
    assert(first != gpu(scan))
    assert(!first.equals(cpu))
    assert(client.calls == 0)
  }

  test("empty plans and full projections preserve Delta's planning semantics") {
    val client = new Client(ScanPlan(Seq.empty))
    val cpu = new ServerSidePlannedScan(null, "db", "table", schema, client,
      Array.empty, schema, None)
    val gpu = GpuServerSidePlannedScan(cpu, new ServerSidePlannedScanAccess(cpu),
      new RapidsConf(Map.empty[String, String]))
    assert(gpu.planInputPartitions().isEmpty)
    assert(client.request.contains(("db", "table", None, None, None)))
    assert(client.calls == 1)
    assert(client.closes == 1)
  }

  test("reader adapter preserves whole file ranges and rejects unsupported formats") {
    var received: Option[FilePartition] = None
    val delegate = new PartitionReaderFactory {
      override def createReader(p: InputPartition): PartitionReader[InternalRow] =
        throw new UnsupportedOperationException
      override def createColumnarReader(p: InputPartition): PartitionReader[ColumnarBatch] = {
        received = Some(p.asInstanceOf[FilePartition])
        null // No GPU resources are created by this test.
      }
    }
    val factory = new ServerSidePlannedGpuReaderFactory(delegate)
    val partition = ServerSidePlannedFileInputPartition("file:/tmp/a%20b.parquet", 987, "PaRqUeT")
    assert(factory.supportColumnarReads(partition))
    factory.createColumnarReader(partition)
    val file = received.get.files.head
    assert(file.filePath == SparkPath.fromPathString(partition.filePath))
    assert(file.start == 0)
    assert(file.length == 987)
    assert(file.partitionValues.numFields == 0)
    intercept[UnsupportedOperationException] {
      factory.createColumnarReader(partition.copy(fileFormat = "orc"))
    }
    intercept[IllegalStateException] { factory.createReader(partition) }
  }

  test("reader factories retain server credentials, schemas and empty local filters") {
    val spark = SparkSession.builder().master("local[1]").appName("server-side-scan-test")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.ssp.session.setting", "session-value")
      .getOrCreate()
    try {
      val tagClient = new Client(ScanPlan(Seq.empty))
      val tagScan = new ServerSidePlannedScan(spark, "db", "table", schema, tagClient,
        Array.empty, schema, None)
      val rule = Delta42xProvider.getScans(classOf[ServerSidePlannedScan])
      val supported = new ServerSidePlannedScanMeta(tagScan,
        new RapidsConf(Map.empty[String, String]), None, rule)
      supported.tagForGpu()
      assert(supported.canThisBeReplaced)
      assert(supported.convertToGpu().readSchema() == schema)
      val disabled = new ServerSidePlannedScanMeta(tagScan,
        new RapidsConf(Map("spark.rapids.sql.format.parquet.read.enabled" -> "false")), None, rule)
      disabled.tagForGpu()
      assert(!disabled.canThisBeReplaced)
      val unsupportedSchema = StructType(Seq(StructField("interval", CalendarIntervalType)))
      val unsupportedScan = new ServerSidePlannedScan(spark, "db", "table", unsupportedSchema,
        tagClient, Array.empty, unsupportedSchema, None)
      val unsupported = new ServerSidePlannedScanMeta(unsupportedScan,
        new RapidsConf(Map.empty[String, String]), None, rule)
      unsupported.tagForGpu()
      assert(!unsupported.canThisBeReplaced)
      for (setting <- Seq("ignoreMissingFiles", "ignoreCorruptFiles")) {
        val key = s"spark.sql.files.$setting"
        val previous = spark.conf.get(key)
        try {
          spark.conf.set(key, "true")
          val skippedFiles = new ServerSidePlannedScanMeta(tagScan,
            new RapidsConf(Map.empty[String, String]), None, rule)
          skippedFiles.tagForGpu()
          assert(!skippedFiles.canThisBeReplaced)
        } finally {
          spark.conf.set(key, previous)
        }
      }
      for (requiredSchema <- Seq(
          StructType(Seq(StructField("id", IntegerType, nullable = false))),
          StructType(Seq(StructField("nested", StructType(Seq(
            StructField("a.b", StringType, nullable = false)))))))) {
        val requiredScan = new ServerSidePlannedScan(spark, "db", "table", requiredSchema,
          tagClient, Array.empty, requiredSchema, None)
        val required = new ServerSidePlannedScanMeta(requiredScan,
          new RapidsConf(Map.empty[String, String]), None, rule)
        required.tagForGpu()
        assert(!required.canThisBeReplaced)
      }
      assert(tagClient.calls == 0 && tagClient.closes == 0)

      for (readerType <- Seq("PERFILE", "MULTITHREADED", "COALESCING")) {
        val credentials = new ScanPlanStorageCredentials {
          override def configure(conf: Configuration): Unit = {
            conf.set("fs.s3a.session.token", "scan-specific-token")
            conf.setBoolean("fs.s3a.impl.disable.cache", true)
          }
        }
        val client = new Client(ScanPlan(Seq.empty, Some(credentials)))
        val required = StructType(Seq(schema("id")))
        val scan = new ServerSidePlannedScan(spark, "db", "table", schema, client,
          Array(EqualTo("id", 7)), required, Some(1))
        val gpu = GpuServerSidePlannedScan(scan, new ServerSidePlannedScanAccess(scan),
          new RapidsConf(Map("spark.rapids.sql.format.parquet.reader.type" -> readerType)))
        val factory = gpu.createReaderFactory().asInstanceOf[ServerSidePlannedGpuReaderFactory]
        val conf = factory.reader match {
          case r: GpuParquetPartitionReaderFactory =>
            assert(readerType == "PERFILE")
            assert(r.dataSchema == schema && r.readDataSchema == required)
            assert(r.partitionSchema.isEmpty && r.filters.isEmpty)
            r.broadcastedConf
          case r: GpuParquetMultiFilePartitionReaderFactory =>
            assert(readerType != "PERFILE")
            assert(r.dataSchema == schema && r.readDataSchema == required)
            assert(r.partitionSchema.isEmpty && r.filters.isEmpty)
            r.broadcastedConf
          case other => fail(s"Unexpected reader factory: ${other.getClass}")
        }
        try {
          assert(conf.value.value.get("ssp.session.setting") == "session-value")
          assert(conf.value.value.get("fs.s3a.session.token") == "scan-specific-token")
          assert(conf.value.value.getBoolean("fs.s3a.impl.disable.cache", false))
          assert(gpu.planInputPartitions().isEmpty)
          assert(client.calls == 1 && client.closes == 1)
        } finally {
          conf.destroy()
        }
      }
    } finally {
      spark.stop()
    }
  }

  test("planning failures propagate without reflection wrappers") {
    val client = new Client(ScanPlan(Seq.empty)) {
      override def planScan(database: String, table: String, filter: Option[Filter],
          projection: Option[Seq[String]], limit: Option[Int]): ScanPlan = {
        throw new IllegalArgumentException("planning failed")
      }
    }
    val scan = new ServerSidePlannedScan(null, "db", "table", schema, client,
      Array.empty, schema, None)
    val error = intercept[IllegalArgumentException] {
      new ServerSidePlannedScanAccess(scan).plan
    }
    assert(error.getMessage == "planning failed")
  }
}
