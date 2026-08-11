/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids

import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.nvidia.spark.rapids.RapidsConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, StagingTableCatalog, SupportsWrite, Table, TableCapability, TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, V1Write, WriteBuilder}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.catalog.{BucketTransform, DeltaCatalog}
import org.apache.spark.sql.delta.commands.{TableCreationModes, WriteIntoDelta}
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait GpuDeltaCatalogBase extends StagingTableCatalog {
  val spark: SparkSession

  val cpuCatalog: DeltaCatalog

  val rapidsConf: RapidsConf

  protected def buildGpuCreateDeltaTableCommand(
      rapidsConf: RapidsConf,
      table: CatalogTable,
      existingTableOpt: Option[CatalogTable],
      mode: SaveMode,
      query: Option[LogicalPlan],
      operation: TableCreationModes.CreationMode,
      tableByPath: Boolean): LeafRunnableCommand

  protected def getExistingTableIfExists(table: TableIdentifier): Option[CatalogTable]

  protected def verifyTableAndSolidify(
      tableDesc: CatalogTable,
      query: Option[LogicalPlan]): CatalogTable

  protected def createGpuStagedDeltaTableV2(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String],
      operation: TableCreationModes.CreationMode): StagedTable = {
    new GpuStagedDeltaTableV2(ident, schema, partitions, properties, operation)
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
      operation: TableCreationModes.CreationMode): Table = {
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
    val (partitionColumns, maybeBucketSpec) = convertTransforms(partitions)
    val newSchema = schema
    val newPartitionColumns = partitionColumns
    val newBucketSpec = maybeBucketSpec
    val conf = spark.sessionState.conf

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
    val existingTableOpt = getExistingTableIfExists(id)
    val loc = locUriOpt
      .orElse(existingTableOpt.flatMap(_.storage.locationUri))
      .getOrElse(spark.sessionState.catalog.defaultTablePath(id))
    val storage = DataSource.buildStorageFormatFromOptions(writeOptions)
      .copy(locationUri = Option(loc))
    val tableType =
      if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
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

    val withDb = verifyTableAndSolidify(tableDesc, None)

    val writer = sourceQuery.map { df =>
      WriteIntoDelta(
        DeltaLog.forTable(spark, new Path(loc)),
        operation.mode,
        new DeltaOptions(withDb.storage.properties, spark.sessionState.conf),
        withDb.partitionColumnNames,
        withDb.properties ++ commentOpt.map("comment" -> _),
        df,
        schemaInCatalog = if (newSchema != schema) Some(newSchema) else None)
    }

    val gpuCreateTableCommand = buildGpuCreateDeltaTableCommand(
      rapidsConf,
      withDb,
      existingTableOpt,
      operation.mode,
      writer,
      operation,
      tableByPath = isByPath)
    gpuCreateTableCommand.run(spark)

    cpuCatalog.loadTable(ident)
  }

  override def loadTable(ident: Identifier): Table = cpuCatalog.loadTable(ident)

  private def getProvider(properties: util.Map[String, String]): String = {
    Option(properties.get("provider"))
      .getOrElse(spark.sessionState.conf.getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME))
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    if (DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))) {
      createDeltaTable(
        ident,
        schema,
        partitions,
        properties,
        Map.empty,
        sourceQuery = None,
        TableCreationModes.Create
      )
    } else {
      throw new IllegalStateException(s"Cannot create non-Delta tables")
    }
  }

  override def stageReplace(
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
        TableCreationModes.Replace
      )
    } else {
      throw new IllegalStateException(s"Cannot create non-Delta tables")
    }
  }

  override def stageCreateOrReplace(
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
        TableCreationModes.CreateOrReplace
      )
    } else {
      throw new IllegalStateException(s"Cannot create non-Delta tables")
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
      throw new IllegalStateException(s"Cannot create non-Delta tables")
    }
  }

// Copy of V2SessionCatalog.convertTransforms, which is private.
  private def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case BucketTransform(numBuckets, bucketCols, sortCols) =>
        bucketSpec = Some(BucketSpec(
          numBuckets, bucketCols.map(_.fieldNames.head), sortCols.map(_.fieldNames.head)))

      case _ =>
        throw DeltaErrors.operationNotSupportedException(s"Partitioning by expressions")
    }

    (identityCols.toSeq, bucketSpec)
  }

  /**
   * A staged Delta table, which creates a HiveMetaStore entry and appends data if this was a
   * CTAS/RTAS command. We have a ugly way of using this API right now, but it's the best way to
   * maintain old behavior compatibility between Databricks Runtime and OSS Delta Lake.
   */
  protected class GpuStagedDeltaTableV2(
      ident: Identifier,
      override val schema: StructType,
      val partitions: Array[Transform],
      override val properties: util.Map[String, String],
      operation: TableCreationModes.CreationMode
  ) extends StagedTable with SupportsWrite {

    private var asSelectQuery: Option[DataFrame] = None
    private var writeOptions: Map[String, String] = Map.empty

    override def partitioning(): Array[Transform] = partitions

    override def commitStagedChanges(): Unit = {
      val conf = spark.sessionState.conf
      val props = new util.HashMap[String, String]()
      // Options passed in through the SQL API will show up both with an "option." prefix and
      // without in Spark 3.1, so we need to remove those from the properties
      val optionsThroughProperties = properties.asScala.collect {
        case (k, _) if k.startsWith("option.") => k.stripPrefix("option.")
      }.toSet
      val sqlWriteOptions = new util.HashMap[String, String]()
      properties.asScala.foreach { case (k, v) =>
        if (!k.startsWith("option.") && !optionsThroughProperties.contains(k)) {
          // Do not add to properties
          props.put(k, v)
        } else if (optionsThroughProperties.contains(k)) {
          sqlWriteOptions.put(k, v)
        }
      }
      if (writeOptions.isEmpty && !sqlWriteOptions.isEmpty) {
        writeOptions = sqlWriteOptions.asScala.toMap
      }
      if (conf.getConf(DeltaSQLConf.DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS)) {
        // Legacy behavior
        writeOptions.foreach { case (k, v) => props.put(k, v) }
      } else {
        writeOptions.foreach { case (k, v) =>
          // Continue putting in Delta prefixed options to avoid breaking workloads
          if (k.toLowerCase(Locale.ROOT).startsWith("delta.")) {
            props.put(k, v)
          }
        }
      }
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

    override def capabilities(): util.Set[TableCapability] = Set(V1_BATCH_WRITE).asJava

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

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    cpuCatalog.alterTable(ident, changes: _*)
  }

  override def dropTable(ident: Identifier): Boolean = cpuCatalog.dropTable(ident)

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    cpuCatalog.renameTable(oldIdent, newIdent)
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    throw new IllegalStateException("should not be called")
  }

  override def tableExists(ident: Identifier): Boolean = cpuCatalog.tableExists(ident)

  override def initialize(s: String, caseInsensitiveStringMap: CaseInsensitiveStringMap): Unit = {
    throw new IllegalStateException("should not be called")
  }

  override def name(): String = cpuCatalog.name()

  private def supportSQLOnFile: Boolean = spark.sessionState.conf.runSQLonFile

  private def hasDeltaNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 && DeltaSourceUtils.isDeltaDataSourceName(ident.namespace().head)
  }

  private def isPathIdentifier(ident: Identifier): Boolean = {
    // Should be a simple check of a special PathIdentifier class in the future
    try {
      supportSQLOnFile && hasDeltaNamespace(ident) && new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }
}


/**
 * A trait for handling table access through delta.`/some/path`. This is a stop-gap solution
 * until PathIdentifiers are implemented in Apache Spark.
 */
trait SupportsPathIdentifier extends TableCatalog { self: GpuDeltaCatalogBase =>

  private def supportSQLOnFile: Boolean = spark.sessionState.conf.runSQLonFile

  protected lazy val catalog: SessionCatalog = spark.sessionState.catalog

  private def hasDeltaNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 && DeltaSourceUtils.isDeltaDataSourceName(ident.namespace().head)
  }

  protected def isPathIdentifier(ident: Identifier): Boolean = {
    // Should be a simple check of a special PathIdentifier class in the future
    try {
      supportSQLOnFile && hasDeltaNamespace(ident) && new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  protected def isPathIdentifier(table: CatalogTable): Boolean = {
    isPathIdentifier(table.identifier)
  }

  protected def isPathIdentifier(tableIdentifier: TableIdentifier) : Boolean = {
    isPathIdentifier(Identifier.of(tableIdentifier.database.toArray, tableIdentifier.table))
  }
}
