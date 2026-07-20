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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg.spark.source

import java.nio.file.Files
import java.util.{Collections, HashMap => JHashMap}

import com.nvidia.spark.rapids.iceberg.spark.RapidsSparkSessionCatalog
import org.apache.commons.io.FileUtils
import org.apache.iceberg.spark.SparkReadOptions
import org.apache.iceberg.spark.source.SparkTable
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{mock, spy, verify, when}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * End-to-end suite for the rapids iceberg session-catalog drop-in. Configures
 * `spark.sql.catalog.spark_catalog` with `RapidsSparkSessionCatalog` against a
 * hadoop iceberg catalog rooted in a tmpdir warehouse and exercises the
 * per-table session conf merging path through `Table.newScanBuilder`.
 *
 * Merged scan options are observed via a spy on the loaded table's iceberg
 * delegate so assertions stay on the public `Table.newScanBuilder` boundary
 * rather than reaching into RapidsSparkTable's internals.
 */
class RapidsSparkTableSuite
    extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private val warehouse = Files.createTempDirectory("rapids-iceberg-e2e").toFile
  private val ns = Array("default")
  private val tbl = "test_tbl"
  private val tablePrefix =
    s"spark.rapids.iceberg.table-setting.spark_catalog.default.$tbl"
  private val catalogPrefix =
    "spark.rapids.iceberg.catalog-setting.spark_catalog"
  private val globalPrefix = "spark.rapids.iceberg.global-setting"

  // Non-default values, picked so a "merged from session conf" assertion can't
  // be satisfied by iceberg defaults or by an accidental fallback.
  private val splitSize = "111111111"
  private val planningLookback = "444"
  private val openFileCost = "5555555"

  override def beforeAll(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("RapidsSparkTableSuite")
        .set("spark.sql.catalog.spark_catalog",
            "com.nvidia.spark.rapids.iceberg.spark.RapidsSparkSessionCatalog")
        .set("spark.sql.catalog.spark_catalog.type", "hadoop")
        .set("spark.sql.catalog.spark_catalog.warehouse", warehouse.toURI.toString)
        .set("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
    spark.sql(s"CREATE TABLE default.$tbl (id long) USING iceberg")
    spark.sql(s"INSERT INTO default.$tbl VALUES (1), (2)")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    TrampolineUtil.cleanupAnyExistingSession()
    if (warehouse.exists()) {
      FileUtils.deleteDirectory(warehouse)
    }
  }

  override def afterEach(): Unit = {
    // Sweep any per-test spark.rapids.iceberg.* runtime confs. The baseline
    // catalog wiring lives in SparkConf (set in beforeAll) so nothing else
    // needs to be preserved.
    spark.conf.getAll.keys.toList
        .filter(_.startsWith("spark.rapids.iceberg."))
        .foreach(spark.conf.unset)
  }

  private def emptyOptions: CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(Collections.emptyMap[String, String]())

  private def loadedRapidsTable: RapidsSparkTable = {
    val catalog = spark.sessionState.catalogManager
        .catalog("spark_catalog").asInstanceOf[TableCatalog]
    catalog.loadTable(Identifier.of(ns, tbl)).asInstanceOf[RapidsSparkTable]
  }

  // Wrap a spy of the given iceberg delegate in a fresh RapidsSparkTable so
  // the caller can capture the options the wrapper forwards to the delegate.
  private def captureMergedOptionsFor(
      loaded: RapidsSparkTable,
      catalogName: String = "spark_catalog",
      explicitOptions: CaseInsensitiveStringMap = emptyOptions): CaseInsensitiveStringMap = {
    val delegateSpy = spy(loaded.delegate())
    val wrapper = new RapidsSparkTable(delegateSpy, catalogName)
    wrapper.newScanBuilder(explicitOptions)
    val captor = ArgumentCaptor.forClass(classOf[CaseInsensitiveStringMap])
    verify(delegateSpy).newScanBuilder(captor.capture())
    captor.getValue
  }

  private def captureMergedOptions(
      catalogName: String = "spark_catalog",
      explicitOptions: CaseInsensitiveStringMap = emptyOptions): CaseInsensitiveStringMap =
    captureMergedOptionsFor(loadedRapidsTable, catalogName, explicitOptions)

  // Create an ad-hoc iceberg table under `default.<tblName>` with the given
  // TBLPROPERTIES, hand the loaded RapidsSparkTable and table name to the
  // body, and drop the table afterwards. Used by precedence tests that need a
  // table-scoped conf path matching the ad-hoc table name.
  private def withTableHavingTblProperty[T](
      tblName: String, propKey: String, propValue: String)(
      body: (RapidsSparkTable, String) => T): T = {
    spark.sql(s"CREATE TABLE default.$tblName (id long) USING iceberg")
    try {
      spark.sql(s"ALTER TABLE default.$tblName SET TBLPROPERTIES " +
        s"('$propKey' = '$propValue')")
      val catalog = spark.sessionState.catalogManager
          .catalog("spark_catalog").asInstanceOf[TableCatalog]
      val loaded = catalog.loadTable(Identifier.of(ns, tblName))
          .asInstanceOf[RapidsSparkTable]
      body(loaded, tblName)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS default.$tblName")
    }
  }

  test("scan succeeds and catalog wraps loaded tables with RapidsSparkTable") {
    // Sanity: an actual scan returns rows; the wrapper does not break reads.
    assert(spark.table(s"default.$tbl").count() == 2)
    assert(loadedRapidsTable.isInstanceOf[RapidsSparkTable])
  }

  test("session conf is a pure pass-through when no override is set") {
    val merged = captureMergedOptions()
    // No spark.rapids.iceberg.* conf is set for this table, so the wrapper
    // must forward the original (empty) options unchanged.
    assert(merged.isEmpty)
  }

  test("single recognized suffix is merged into the scan options") {
    spark.conf.set(s"$tablePrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
    assert(!merged.containsKey(SparkReadOptions.LOOKBACK))
    assert(!merged.containsKey(SparkReadOptions.FILE_OPEN_COST))
  }

  test("all three recognized suffixes are merged into the scan options") {
    spark.conf.set(s"$tablePrefix.read-split-target-size", splitSize)
    spark.conf.set(s"$tablePrefix.read-split-planning-lookback", planningLookback)
    spark.conf.set(s"$tablePrefix.read-split-open-file-cost", openFileCost)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
    assert(merged.get(SparkReadOptions.LOOKBACK) == planningLookback)
    assert(merged.get(SparkReadOptions.FILE_OPEN_COST) == openFileCost)
  }

  private def explicitMap(key: String, value: String): CaseInsensitiveStringMap = {
    val m = new JHashMap[String, String]()
    m.put(key, value)
    new CaseInsensitiveStringMap(m)
  }

  test("precedence: explicit option beats table-setting") {
    spark.conf.set(s"$tablePrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions(
      explicitOptions = explicitMap(SparkReadOptions.SPLIT_SIZE, "536870912"))
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == "536870912")
  }

  test("precedence: explicit option beats catalog-setting") {
    spark.conf.set(s"$catalogPrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions(
      explicitOptions = explicitMap(SparkReadOptions.SPLIT_SIZE, "536870912"))
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == "536870912")
  }

  test("precedence: explicit option beats global-setting") {
    spark.conf.set(s"$globalPrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions(
      explicitOptions = explicitMap(SparkReadOptions.SPLIT_SIZE, "536870912"))
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == "536870912")
  }

  test("table-scoped confs for a different table are ignored") {
    spark.conf.set(
      "spark.rapids.iceberg.table-setting.spark_catalog.default.other_tbl." +
        "read-split-target-size",
      splitSize)
    val merged = captureMergedOptions()
    assert(merged.isEmpty)
  }

  test("unrecognized table-scoped suffix is rejected") {
    spark.conf.set(s"$tablePrefix.read-split-bogus", "1")
    val ex = intercept[IllegalArgumentException] {
      captureMergedOptions()
    }
    assert(ex.getMessage.contains("Unrecognized suffix"))
    assert(ex.getMessage.contains("read-split-bogus"))
  }

  test("dot in catalog component is rejected when a table-scoped conf would apply") {
    spark.conf.set(
      s"spark.rapids.iceberg.table-setting.hadoop.prod.default.$tbl." +
        "read-split-target-size",
      splitSize)
    val ex = intercept[IllegalArgumentException] {
      captureMergedOptions(catalogName = "hadoop.prod")
    }
    assert(ex.getMessage.contains("ambiguous"))
    assert(ex.getMessage.contains("hadoop.prod"))
  }

  test("dot in catalog component is a pure pass-through when no conf applies") {
    // Even with a catalog name containing '.', simply having the wrapper in
    // place must not break scans for tables the user has not explicitly tuned.
    val merged = captureMergedOptions(catalogName = "hadoop.prod")
    assert(merged.isEmpty)
  }

  test("catalog-scoped conf is merged into the scan options") {
    spark.conf.set(s"$catalogPrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
  }

  test("global-scoped conf is merged into the scan options") {
    spark.conf.set(s"$globalPrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
  }

  test("precedence: table-setting beats catalog-setting") {
    spark.conf.set(s"$catalogPrefix.read-split-target-size", "111")
    spark.conf.set(s"$tablePrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
  }

  test("precedence: table-setting beats global-setting") {
    spark.conf.set(s"$globalPrefix.read-split-target-size", "111")
    spark.conf.set(s"$tablePrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
  }

  test("precedence: table-setting beats catalog- and global-setting together") {
    spark.conf.set(s"$globalPrefix.read-split-target-size", "111")
    spark.conf.set(s"$catalogPrefix.read-split-target-size", "222")
    spark.conf.set(s"$tablePrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
  }

  test("precedence: catalog-setting beats global-setting") {
    spark.conf.set(s"$globalPrefix.read-split-target-size", "111")
    spark.conf.set(s"$catalogPrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
  }

  test("precedence: table-setting beats iceberg TBLPROPERTIES") {
    // TBLPROPERTIES is iceberg's fallback; the wrapper merges the session conf
    // into the options it forwards, so iceberg sees the session-conf value as
    // an explicit scan option and uses it instead of TBLPROPERTIES.
    withTableHavingTblProperty("tbl_vs_tblprops",
        "read.split.target-size", "999") { (loaded, t) =>
      spark.conf.set(
        s"spark.rapids.iceberg.table-setting.spark_catalog.default.$t." +
          "read-split-target-size",
        splitSize)
      val merged = captureMergedOptionsFor(loaded)
      assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
    }
  }

  test("precedence: catalog-setting beats iceberg TBLPROPERTIES") {
    withTableHavingTblProperty("cat_vs_tblprops",
        "read.split.target-size", "999") { (loaded, _) =>
      spark.conf.set(s"$catalogPrefix.read-split-target-size", splitSize)
      val merged = captureMergedOptionsFor(loaded)
      assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
    }
  }

  test("precedence: global-setting beats iceberg TBLPROPERTIES") {
    withTableHavingTblProperty("glob_vs_tblprops",
        "read.split.target-size", "999") { (loaded, _) =>
      spark.conf.set(s"$globalPrefix.read-split-target-size", splitSize)
      val merged = captureMergedOptionsFor(loaded)
      assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
    }
  }

  test("scopes merge independently per suffix") {
    // table-setting contributes target-size, catalog-setting contributes
    // lookback, global-setting contributes open-file-cost. The merged options
    // must reflect each scope contributing its own suffix without one scope
    // shadowing another.
    spark.conf.set(s"$tablePrefix.read-split-target-size", splitSize)
    spark.conf.set(s"$catalogPrefix.read-split-planning-lookback", planningLookback)
    spark.conf.set(s"$globalPrefix.read-split-open-file-cost", openFileCost)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
    assert(merged.get(SparkReadOptions.LOOKBACK) == planningLookback)
    assert(merged.get(SparkReadOptions.FILE_OPEN_COST) == openFileCost)
  }

  test("unrecognized catalog-scoped suffix is rejected") {
    spark.conf.set(s"$catalogPrefix.read-split-bogus", "1")
    val ex = intercept[IllegalArgumentException] {
      captureMergedOptions()
    }
    assert(ex.getMessage.contains("Unrecognized suffix"))
    assert(ex.getMessage.contains("read-split-bogus"))
  }

  test("unrecognized global-scoped suffix is rejected") {
    spark.conf.set(s"$globalPrefix.read-split-bogus", "1")
    val ex = intercept[IllegalArgumentException] {
      captureMergedOptions()
    }
    assert(ex.getMessage.contains("Unrecognized suffix"))
    assert(ex.getMessage.contains("read-split-bogus"))
  }

  test("global conf still applies when catalog name contains '.'") {
    // Global confs don't carry a catalog in the key, so they aren't ambiguous
    // and must apply even to tables whose catalog name has '.' in it.
    spark.conf.set(s"$globalPrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions(catalogName = "hadoop.prod")
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
  }

  test("catalog-scoped conf applies when catalog name contains '.'") {
    // catalog-setting keys carry the catalog name as the only variable
    // component after the scope marker, so a '.' in the catalog name doesn't
    // produce ambiguity at this scope.
    spark.conf.set(
      "spark.rapids.iceberg.catalog-setting.hadoop.prod.read-split-target-size",
      splitSize)
    val merged = captureMergedOptions(catalogName = "hadoop.prod")
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
  }

  test("iceberg TBLPROPERTIES is honored when no session conf applies") {
    // No spark.rapids.iceberg.* conf is set at any scope, so the wrapper
    // forwards options unchanged. Iceberg then falls back to TBLPROPERTIES;
    // verify our wrapper exposes the property so the iceberg-side resolution
    // has it to read.
    withTableHavingTblProperty("tblprops_only",
        "read.split.target-size", splitSize) { (loaded, _) =>
      val merged = captureMergedOptionsFor(loaded)
      // Wrapper doesn't set SPLIT_SIZE (no session conf), so iceberg is the
      // one consulting TBLPROPERTIES. Confirm the property is visible on the
      // loaded table — that's the channel iceberg reads from.
      assert(!merged.containsKey(SparkReadOptions.SPLIT_SIZE))
      assert(loaded.properties().get("read.split.target-size") == splitSize)
    }
  }

  test("wrap returns a RapidsSparkTable for SparkTable inputs") {
    // RapidsSparkTable's constructor eagerly reads delegate.table().name() to
    // derive namespace and tableName, so the mock has to wire that chain.
    val sparkTable = mock(classOf[SparkTable])
    val icebergTable = mock(classOf[org.apache.iceberg.Table])
    when(sparkTable.table()).thenReturn(icebergTable)
    when(icebergTable.name()).thenReturn("iceCat.default.store_sales")
    val wrapped = RapidsSparkSessionCatalog.wrap("spark_catalog", sparkTable)
    assert(wrapped.isInstanceOf[RapidsSparkTable])
    assert(wrapped.asInstanceOf[RapidsSparkTable].delegate() eq sparkTable)
  }

  test("wrap passes through non-SparkTable inputs unchanged") {
    val other = mock(classOf[Table])
    val wrapped = RapidsSparkSessionCatalog.wrap("spark_catalog", other)
    assert(wrapped eq other)
  }
}
