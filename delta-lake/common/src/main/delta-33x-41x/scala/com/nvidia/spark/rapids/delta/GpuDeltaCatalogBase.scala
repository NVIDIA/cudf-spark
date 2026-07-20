/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
 *
 * This file was derived from DeltaDataSource.scala in the
 * Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.nvidia.spark.rapids.delta

import com.nvidia.spark.rapids.RapidsConf
import java.util
import java.util.Locale
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, StagingTableCatalog, SupportsWrite, Table, TableCapability, TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, V1Write, WriteBuilder}
import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaConfigs, DeltaErrors, DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.{TableCreationModes, WriteIntoDelta}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.rapids.{DeltaTrampoline, GpuDeltaLog, GpuWriteIntoDelta}
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.ShimTrampolineUtil
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType

/**
 * Base class for GPU Delta Catalog shared across Delta versions.
 * Version-specific subclasses provide the GpuCreateDeltaTableCommand.
 */
abstract class GpuDeltaCatalogBase(
    val cpuCatalog: DeltaCatalog,
    val rapidsConf: RapidsConf)
  extends StagingTableCatalog
  with DeltaLogging {

  val spark: SparkSession = cpuCatalog.spark

  // Abstract method for version-specific GpuCreateDeltaTableCommand
  protected def createGpuCreateDeltaTableCommand(
      withDb: CatalogTable,
      existingTableOpt: Option[CatalogTable],
      mode: SaveMode,
      writer: Option[GpuWriteIntoDelta],
      operation: TableCreationModes.CreationMode,
      isByPath: Boolean,
      tableCreateFunc: Option[CatalogTable => Unit]): Unit

  /** copied from trait SupportsPathIdentifier */
  private def supportSQLOnFile: Boolean = spark.sessionState.conf.runSQLonFile

  /** copied from trait SupportsPathIdentifier */
  private def hasDeltaNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 && DeltaSourceUtils.isDeltaDataSourceName(ident.namespace().head)
  }

  protected lazy val isUnityCatalog: Boolean = {
    @scala.annotation.tailrec
    def findRequiredField(clazz: Class[_]): java.lang.reflect.Field = {
      if (clazz == null) {
        throw new IllegalStateException(
          s"Unable to locate Delta catalog field isUnityCatalog on " +
            s"${cpuCatalog.getClass.getName}; add a version-specific shim if this changed.")
      }
      try {
        clazz.getDeclaredField("isUnityCatalog")
      } catch {
        case _: NoSuchFieldException => findRequiredField(clazz.getSuperclass)
      }
    }

    val field = findRequiredField(cpuCatalog.getClass)
    field.setAccessible(true)
    field.getBoolean(cpuCatalog)
  }

  protected def isPathIdentifier(ident: Identifier): Boolean = {
    // Should be a simple check of a special PathIdentifier class in the future
    try {
      supportSQLOnFile && hasDeltaNamespace(ident) && new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  // Members declared in org.apache.spark.sql.connector.catalog.CatalogPlugin
  override def initialize(
      name: String,
      conf: org.apache.spark.sql.util.CaseInsensitiveStringMap): Unit = {
    // nothing to initialize
  }

  override def name(): String = "GpuDeltaCatalog"

  // Members declared in org.apache.spark.sql.connector.catalog.TableCatalog
  // Fallback to the CPU for now
  override def alterTable(
      ident: Identifier,
      changes: TableChange*): Table = {
    cpuCatalog.alterTable(ident, changes: _*)
  }

  override def dropTable(ident: Identifier): Boolean = {
    cpuCatalog.dropTable(ident)
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    cpuCatalog.listTables(namespace)
  }

  def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    cpuCatalog.renameTable(oldIdent, newIdent)
  }

  /**
   * Creates a Delta table using GPU for writing the data
   *
   * @param ident              The identifier of the table
   * @param schema             The schema of the table
   * @param partitions         The partition transforms for the table
   * @param allTableProperties The table properties that configure the behavior of the table or
   *                           provide information about the table
   * @param writeOptions       Options specific to the write during table creation or replacement
   * @param sourceQuery        A query if this CREATE request came from a CTAS or RTAS
   * @param operation          The specific table creation mode, whether this is a
   *                           Create/Replace/Create or Replace
   */
  protected def createDeltaTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      allTableProperties: util.Map[String, String],
      writeOptions: Map[String, String],
      sourceQuery: Option[DataFrame],
      operation: TableCreationModes.CreationMode
      ): Table = recordFrameProfile(
    "DeltaCatalog", "createDeltaTable") {
    // These two keys are tableProperties in data source v2 but not in v1, so we have to filter
    // them out. Otherwise property consistency checks will fail.
    val tableProperties = allTableProperties.asScala.filterKeys {
      case TableCatalog.PROP_LOCATION => false
      case TableCatalog.PROP_PROVIDER => false
      case TableCatalog.PROP_COMMENT => false
      case TableCatalog.PROP_OWNER => false
      case TableCatalog.PROP_EXTERNAL => false
      case "path" => false
      case "option.path" => false
      case _ => true
    }.toMap
    val (partitionColumns, maybeBucketSpec, maybeClusterBySpec) =
      DeltaTrampoline.convertTransforms(partitions)
    cpuCatalog.validateClusterBySpec(maybeClusterBySpec, schema)
    // Check partition columns are not IDENTITY columns.
    partitionColumns.foreach { colName =>
      if (ColumnWithDefaultExprUtils.isIdentityColumn(schema(colName))) {
        throw DeltaErrors.identityColumnPartitionNotSupported(colName)
      }
    }
    val newSchema = schema
    val newPartitionColumns = partitionColumns
    val newBucketSpec = maybeBucketSpec
    val conf = spark.sessionState.conf
    allTableProperties.asScala
      .get(DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.key)
      .foreach(StatisticsCollection.validateDeltaStatsColumns(schema, partitionColumns, _))
    val isByPath = isPathIdentifier(ident)
    if (isByPath && !conf.getConf(DeltaSQLConf.DELTA_LEGACY_ALLOW_AMBIGUOUS_PATHS)
      && allTableProperties.containsKey("location")
      // The location property can be qualified and different from the path in the identifier, so
      // we check `endsWith` here.
      && Option(allTableProperties.get("location")).exists(!_.endsWith(ident.name()))
    ) {
      throw DeltaErrors.ambiguousPathsInCreateTableException(
        ident.name(), allTableProperties.get("location"))
    }
    val location = if (isByPath) {
      Option(ident.name())
    } else {
      Option(allTableProperties.get("location"))
    }
    val id = {
      TableIdentifier(ident.name(), ident.namespace().lastOption)
    }
    val locUriOpt = location.map(CatalogUtils.stringToURI)
    val existingTableOpt = cpuCatalog.getExistingTableIfExists(id)
    // PROP_IS_MANAGED_LOCATION indicates that the table location is not user-specified but
    // system-generated. The table should be created as managed table in this case.
    val isManagedLocation = Option(allTableProperties.get(TableCatalog.PROP_IS_MANAGED_LOCATION))
      .exists(_.equalsIgnoreCase("true"))
    // Note: Spark generates the table location for managed tables in
    // `DeltaCatalog#delegate#createTable`, so `isManagedLocation` should never be true if
    // Unity Catalog is not involved. For safety we also check `isUnityCatalog` here.
    val respectManagedLoc = isUnityCatalog
    val tableType = if (location.isEmpty || (isManagedLocation && respectManagedLoc)) {
      CatalogTableType.MANAGED
    } else {
      CatalogTableType.EXTERNAL
    }
    val loc = locUriOpt
      .orElse(existingTableOpt.flatMap(_.storage.locationUri))
      .getOrElse(spark.sessionState.catalog.defaultTablePath(id))
    val storage = DataSource.buildStorageFormatFromOptions(writeOptions)
      .copy(locationUri = Option(loc))
    val commentOpt = Option(allTableProperties.get("comment"))


    val tableDesc = new CatalogTable(
      identifier = id,
      tableType = tableType,
      storage = storage,
      schema = newSchema,
      provider = Some(DeltaSourceUtils.ALT_NAME),
      partitionColumnNames = newPartitionColumns,
      bucketSpec = newBucketSpec,
      properties = tableProperties,
      comment = commentOpt
    )

    val withDb =
      cpuCatalog.verifyTableAndSolidify(
        tableDesc,
        None,
        maybeClusterBySpec
      )

    val writer = sourceQuery.map { df =>
      val deltaLog = DeltaLog.forTable(spark, new Path(loc))
      val cpuWriter = WriteIntoDelta(
        deltaLog,
        operation.mode,
        new DeltaOptions(withDb.storage.properties, spark.sessionState.conf),
        withDb.partitionColumnNames,
        withDb.properties ++ commentOpt.map("comment" -> _),
        df,
        Some(tableDesc),
        schemaInCatalog = if (newSchema != schema) Some(newSchema) else None)
      val gpuDeltaLog = new GpuDeltaLog(deltaLog, rapidsConf)
      GpuWriteIntoDelta(gpuDeltaLog, cpuWriter)
    }


    // We should invoke the Spark catalog plugin API to create the table, to
    // respect third party catalogs. Note: only handle CREATE TABLE for now, we
    // should support CTAS later.
    // TODO: Spark `V2SessionCatalog` mistakenly treat tables with location as EXTERNAL table.
    //       Before this bug is fixed, we should only call the catalog plugin API to create tables
    //       if UC is enabled to replace `V2SessionCatalog`.
    val tableCreateFunc = if (isUnityCatalog && sourceQuery.isEmpty) {
      Some[CatalogTable => Unit](v1Table => {
        val t = DeltaTrampoline.getV1Table(v1Table)
        cpuCatalog.createTable(ident, t.columns(), t.partitioning, t.properties)
      })
    } else {
      None
    }

    createGpuCreateDeltaTableCommand(
      withDb,
      existingTableOpt,
      operation.mode,
      writer,
      operation,
      isByPath = isByPath,
      tableCreateFunc = tableCreateFunc)

    cpuCatalog.loadTable(ident)
  }

  override def loadTable(ident: Identifier): Table =
    cpuCatalog.loadTable(ident)

  override def loadTable(ident: Identifier, timestamp: Long): Table =
    cpuCatalog.loadTable(ident, timestamp)


  override def loadTable(ident: Identifier, version: String): Table =
    cpuCatalog.loadTable(ident, version)

  private def getProvider(properties: util.Map[String, String]): String = {
    Option(properties.get("provider"))
      .getOrElse(spark.sessionState.conf.getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME))
  }

  override def createTable(
      ident: Identifier,
      columns: Array[org.apache.spark.sql.connector.catalog.Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    createTable(
      ident,
      ShimTrampolineUtil.v2ColumnsToStructType(columns),
      partitions,
      properties)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    recordFrameProfile("DeltaCatalog", "createTable") {
      cpuCatalog.createTable(ident, schema, partitions, properties)
    }

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageReplace") {
      if (DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))) {
        new GpuStagedDeltaTableV2(
          ident,
          schema,
          partitions,
          properties,
          TableCreationModes.Replace)
      } else {
        cpuCatalog.stageReplace(ident, schema, partitions, properties)
      }
    }

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageCreateOrReplace") {
      if (DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))) {
        new GpuStagedDeltaTableV2(
          ident,
          schema,
          partitions,
          properties,
          TableCreationModes.CreateOrReplace
        )
      } else {
        cpuCatalog.stageCreateOrReplace(ident, schema, partitions, properties)
      }
    }

  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    if (DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))) {
      createGpuStagedDeltaTableV2(
        ident,
        schema,
        partitions,
        properties,
        TableCreationModes.Create
      )
    } else {
      cpuCatalog.stageCreate(ident, schema, partitions, properties)
    }
  }

  private def getTablePropsAndWriteOptions(properties: util.Map[String, String])
  : (util.Map[String, String], Map[String, String]) = {
    val props = new util.HashMap[String, String]()
    // Options passed in through the SQL API will show up both with an "option." prefix and
    // without in Spark 3.1, so we need to remove those from the properties
    val optionsThroughProperties = properties.asScala.collect {
      case (k, _) if k.startsWith(TableCatalog.OPTION_PREFIX) =>
        k.stripPrefix(TableCatalog.OPTION_PREFIX)
    }.toSet
    val writeOptions = new util.HashMap[String, String]()
    properties.asScala.foreach { case (k, v) =>
      if (!k.startsWith(TableCatalog.OPTION_PREFIX) && !optionsThroughProperties.contains(k)) {
        // Add to properties
        props.put(k, v)
      } else if (optionsThroughProperties.contains(k)) {
        writeOptions.put(k, v)
      }
    }
    (props, writeOptions.asScala.toMap)
  }

  private def expandTableProps(
      props: util.Map[String, String],
      options: Map[String, String],
      conf: SQLConf): Unit = {
    if (conf.getConf(DeltaSQLConf.DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS)) {
      // Legacy behavior
      options.foreach { case (k, v) => props.put(k, v) }
    } else {
      options.foreach { case (k, v) =>
        // Continue putting in Delta prefixed options to avoid breaking workloads
        if (k.toLowerCase(Locale.ROOT).startsWith("delta.")) {
          props.put(k, v)
        }
      }
    }
  }

  override def tableExists(ident: Identifier): Boolean = cpuCatalog.tableExists(ident)

  protected def createGpuStagedDeltaTableV2(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String],
      operation: TableCreationModes.CreationMode): StagedTable = {
    new GpuStagedDeltaTableV2(ident, schema, partitions, properties, operation)
  }

  /**
   * A staged delta table, which creates a HiveMetaStore entry and appends data if this was a
   * CTAS/RTAS command. We have a ugly way of using this API right now, but it's the best way to
   * maintain old behavior compatibility between Databricks Runtime and OSS Delta Lake.
   */
  private[rapids] class GpuStagedDeltaTableV2(
      ident: Identifier,
      override val schema: StructType,
      val partitions: Array[Transform],
      override val properties: util.Map[String, String],
      operation: TableCreationModes.CreationMode
      ) extends StagedTable with SupportsWrite {

    private var asSelectQuery: Option[DataFrame] = None
    private var writeOptions: Map[String, String] = Map.empty

    override def partitioning(): Array[Transform] = partitions

    override def commitStagedChanges(): Unit = recordFrameProfile(
      "DeltaCatalog", "commitStagedChanges") {
      val conf = spark.sessionState.conf
      val (props, sqlWriteOptions) = getTablePropsAndWriteOptions(properties)
      if (writeOptions.isEmpty && sqlWriteOptions.nonEmpty) {
        writeOptions = sqlWriteOptions
      }
      expandTableProps(props, writeOptions, conf)
      createDeltaTable(
        ident,
        schema,
        partitions,
        props,
        writeOptions,
        asSelectQuery,
        operation
      )
    }

    override def name(): String = ident.name()

    override def abortStagedChanges(): Unit = {}

    override def capabilities(): util.Set[TableCapability] = {
      Set(V1_BATCH_WRITE).asJava
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      writeOptions = info.options.asCaseSensitiveMap().asScala.toMap
      new DeltaV1WriteBuilder
    }

    /*
     * WriteBuilder for creating a Delta table.
     */
    private class DeltaV1WriteBuilder extends WriteBuilder {
      override def build(): V1Write = new V1Write {
        override def toInsertableRelation(): InsertableRelation = {
          new InsertableRelation {
            override def insert(data: DataFrame, overwrite: Boolean): Unit = {
              asSelectQuery = Option(data)
            }
          }
        }
      }
    }
  }
}
