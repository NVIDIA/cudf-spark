/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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


package org.apache.spark.sql.rapids

import java.util.{Locale, ServiceConfigurationError, ServiceLoader}

import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.GpuParquetFileFormat
import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.apache.commons.lang3.reflect.ConstructorUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2
import org.apache.spark.sql.execution.streaming.sources.{RateStreamProvider, TextSocketSourceProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.{FileStreamSinkShims, RapidsErrorUtils, SchemaUtilsShims}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.{HadoopFSUtils, ThreadUtils, Utils}

/**
 * A truncated version of Spark DataSource that converts to use the GPU version of
 * InsertIntoHadoopFsRelationCommand for FileFormats we support.
 * This does not support DataSource V2 writing at this point because at the time of
 * copying, it did not.
 */
abstract class GpuDataSourceBase(
    sparkSession: SparkSession,
    className: String,
    paths: Seq[String] = Nil,
    userSpecifiedSchema: Option[StructType] = None,
    partitionColumns: Seq[String] = Seq.empty,
    bucketSpec: Option[BucketSpec] = None,
    options: Map[String, String] = Map.empty,
    catalogTable: Option[CatalogTable] = None,
    origProvider: Class[_]) extends Logging {

  protected def originalProvidingInstance() = origProvider.getConstructor().newInstance()

  protected def newHadoopConfiguration(): Configuration =
    sparkSession.sessionState.newHadoopConfWithOptions(options)

  protected val caseInsensitiveOptions = CaseInsensitiveMap(options)
  protected val equality = sparkSession.sessionState.conf.resolver

  /**
   * Whether or not paths should be globbed before being used to access files.
   */
  def globPaths: Boolean = {
    options.get(GpuDataSourceBase.GLOB_PATHS_KEY).forall(_ == "true")
  }

  bucketSpec.foreach { bucket =>
    SchemaUtilsShims.checkColumnNameDuplication(
      bucket.bucketColumnNames, "in the bucket definition", equality)
    SchemaUtilsShims.checkColumnNameDuplication(
      bucket.sortColumnNames, "in the sort definition", equality)
  }

  /**
   * Get the schema of the given FileFormat, if provided by `userSpecifiedSchema`, or try to infer
   * it. In the read path, only managed tables by Hive provide the partition columns properly when
   * initializing this class. All other file based data sources will try to infer the partitioning,
   * and then cast the inferred types to user specified dataTypes if the partition columns exist
   * inside `userSpecifiedSchema`, otherwise we can hit data corruption bugs like SPARK-18510.
   * This method will try to skip file scanning whether `userSpecifiedSchema` and
   * `partitionColumns` are provided. Here are some code paths that use this method:
   *   1. `spark.read` (no schema): Most amount of work. Infer both schema and partitioning columns
   *   2. `spark.read.schema(userSpecifiedSchema)`: Parse partitioning columns, cast them to the
   *     dataTypes provided in `userSpecifiedSchema` if they exist or fallback to inferred
   *     dataType if they don't.
   *   3. `spark.readStream.schema(userSpecifiedSchema)`: For streaming use cases, users have to
   *     provide the schema. Here, we also perform partition inference like 2, and try to use
   *     dataTypes in `userSpecifiedSchema`. All subsequent triggers for this stream will re-use
   *     this information, therefore calls to this method should be very cheap, i.e. there won't
   *     be any further inference in any triggers.
   *
   * @param format the file format object for this DataSource
   * @param getFileIndex [[InMemoryFileIndex]] for getting partition schema and file list
   * @return A pair of the data schema (excluding partition columns) and the schema of the partition
   *         columns.
   */
  private def getOrInferFileFormatSchema(
      format: FileFormat,
      getFileIndex: () => InMemoryFileIndex): (StructType, StructType) = {
    lazy val tempFileIndex = getFileIndex()

    val partitionSchema = if (partitionColumns.isEmpty) {
      // Try to infer partitioning, because no DataSource in the read path provides the partitioning
      // columns properly unless it is a Hive DataSource
      tempFileIndex.partitionSchema
    } else {
      // maintain old behavior before SPARK-18510. If userSpecifiedSchema is empty used inferred
      // partitioning
      if (userSpecifiedSchema.isEmpty) {
        val inferredPartitions = tempFileIndex.partitionSchema
        inferredPartitions
      } else {
        val partitionFields = partitionColumns.map { partitionColumn =>
          userSpecifiedSchema.flatMap(_.find(c => equality(c.name, partitionColumn))).orElse {
            val inferredPartitions = tempFileIndex.partitionSchema
            val inferredOpt = inferredPartitions.find(p => equality(p.name, partitionColumn))
            if (inferredOpt.isDefined) {
              logDebug(
                s"""Type of partition column: $partitionColumn not found in specified schema
                   |for $format.
                   |User Specified Schema
                   |=====================
                   |${userSpecifiedSchema.orNull}
                   |
                   |Falling back to inferred dataType if it exists.
                 """.stripMargin)
            }
            inferredOpt
          }.getOrElse {
            throw RapidsErrorUtils.
              partitionColumnNotSpecifiedError(format.toString, partitionColumn)
          }
        }
        StructType(partitionFields)
      }
    }

    val dataSchema = userSpecifiedSchema.map { schema =>
      StructType(schema.filterNot(f => partitionSchema.exists(p => equality(p.name, f.name))))
    }.orElse {
      // Remove "path" option so that it is not added to the paths returned by
      // `tempFileIndex.allFiles()`.
      format.inferSchema(
        sparkSession,
        caseInsensitiveOptions - "path",
        SparkShimImpl.filesFromFileIndex(tempFileIndex))
    }.getOrElse {
      throw RapidsErrorUtils.dataSchemaNotSpecifiedError(format.toString)
    }

    // We just print a waring message if the data schema and partition schema have the duplicate
    // columns. This is because we allow users to do so in the previous Spark releases and
    // we have the existing tests for the cases (e.g., `ParquetHadoopFsRelationSuite`).
    // See SPARK-18108 and SPARK-21144 for related discussions.
    try {
      SchemaUtilsShims.checkColumnNameDuplication(
        (dataSchema ++ partitionSchema).map(_.name),
        "in the data schema and the partition schema",
        equality)
    } catch {
      case e: AnalysisException => logWarning(e.getMessage)
    }

    (dataSchema, partitionSchema)
  }


  /**
   * Create a resolved `BaseRelation` that can be used to read data from or write data into this
   * `DataSource`
   *
   * @param checkFilesExist Whether to confirm that the files exist when generating the
   *                        non-streaming file based datasource. StructuredStreaming jobs already
   *                        list file existence, and when generating incremental jobs, the batch
   *                        is considered as a non-streaming file based data source. Since we know
   *                        that files already exist, we don't need to check them again.
   */
  def resolveRelation(checkFilesExist: Boolean = true): BaseRelation = {
    val relation = (originalProvidingInstance(), userSpecifiedSchema) match {
      // TODO: Throw when too much is given.
      case (dataSource: SchemaRelationProvider, Some(schema)) =>
        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions, schema)
      case (dataSource: RelationProvider, None) =>
        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
      case (_: SchemaRelationProvider, None) =>
        throw RapidsErrorUtils.schemaNotSpecifiedForSchemaRelationProviderError(className)
      case (dataSource: RelationProvider, Some(schema)) =>
        val baseRelation =
          dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
        if (!DataType.equalsIgnoreCompatibleNullability(baseRelation.schema, schema)) {
          throw RapidsErrorUtils.userSpecifiedSchemaMismatchActualSchemaError(schema,
            baseRelation.schema)
        }
        baseRelation

      // We are reading from the results of a streaming query. Load files from the metadata log
      // instead of listing them using HDFS APIs.
      case (format: FileFormat, _)
          if FileStreamSinkShims.hasMetadata(
            caseInsensitiveOptions.get("path").toSeq ++ paths,
            newHadoopConfiguration(),
            sparkSession.sessionState.conf) =>
        val basePath = new Path((caseInsensitiveOptions.get("path").toSeq ++ paths).head)
        val fileCatalog = FileStreamSinkShims.newMetadataLogFileIndex(sparkSession, basePath,
          caseInsensitiveOptions, userSpecifiedSchema)
        val dataSchema = userSpecifiedSchema.orElse {
          // Remove "path" option so that it is not added to the paths returned by
          // `fileCatalog.allFiles()`.
          format.inferSchema(
            sparkSession,
            caseInsensitiveOptions - "path",
            SparkShimImpl.filesFromFileIndex(fileCatalog))
        }.getOrElse {
          throw RapidsErrorUtils.
            dataSchemaNotSpecifiedError(format.toString, fileCatalog.allFiles().mkString(","))
        }

        HadoopFsRelation(
          fileCatalog,
          partitionSchema = fileCatalog.partitionSchema,
          dataSchema = dataSchema,
          bucketSpec = None,
          format,
          caseInsensitiveOptions)(sparkSession)

      // This is a non-streaming file based datasource.
      case (format: FileFormat, _) =>
        val useCatalogFileIndex = sparkSession.sessionState.conf.manageFilesourcePartitions &&
          catalogTable.isDefined && catalogTable.get.tracksPartitionsInCatalog &&
          catalogTable.get.partitionColumnNames.nonEmpty
        val (fileCatalog, dataSchema, partitionSchema) = if (useCatalogFileIndex) {
          val defaultTableSize = sparkSession.sessionState.conf.defaultSizeInBytes
          val index = new CatalogFileIndex(
            sparkSession,
            catalogTable.get,
            catalogTable.get.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))
          (index, catalogTable.get.dataSchema, catalogTable.get.partitionSchema)
        } else {
          val globbedPaths = checkAndGlobPathIfNecessary(
            checkEmptyGlobPath = true, checkFilesExist = checkFilesExist)
          val index = createInMemoryFileIndex(globbedPaths)
          val (resultDataSchema, resultPartitionSchema) =
            getOrInferFileFormatSchema(format, () => index)
          (index, resultDataSchema, resultPartitionSchema)
        }

        HadoopFsRelation(
          fileCatalog,
          partitionSchema = partitionSchema,
          dataSchema = dataSchema.asNullable,
          bucketSpec = bucketSpec,
          format,
          caseInsensitiveOptions)(sparkSession)

      case _ =>
        throw RapidsErrorUtils.invalidDataSourceError(className)
    }

    relation match {
      case hs: HadoopFsRelation =>
        SchemaUtilsShims.checkSchemaColumnNameDuplication(
          hs.dataSchema,
          "in the data schema",
          equality)
        SchemaUtilsShims.checkSchemaColumnNameDuplication(
          hs.partitionSchema,
          "in the partition schema",
           equality)
        DataSourceUtils.verifySchema(hs.fileFormat, hs.dataSchema)
      case _ =>
        SchemaUtilsShims.checkSchemaColumnNameDuplication(
          relation.schema,
          "in the data schema",
           equality)
    }

    relation
  }

  /** Returns an [[InMemoryFileIndex]] that can be used to get partition schema and file list. */
  private def createInMemoryFileIndex(globbedPaths: Seq[Path]): InMemoryFileIndex = {
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new InMemoryFileIndex(
      sparkSession, globbedPaths, options, userSpecifiedSchema, fileStatusCache)
  }

  /**
   * Checks and returns files in all the paths.
   */
  private def checkAndGlobPathIfNecessary(
      checkEmptyGlobPath: Boolean,
      checkFilesExist: Boolean): Seq[Path] = {
    val allPaths = caseInsensitiveOptions.get("path") ++ paths
    GpuDataSourceBase.checkAndGlobPathIfNecessary(allPaths.toSeq, newHadoopConfiguration(),
      checkEmptyGlobPath, checkFilesExist, enableGlobbing = globPaths)
  }
}

object GpuDataSourceBase extends Logging {

  /** A map to maintain backward compatibility in case we move data sources around. */
  private val backwardCompatibilityMap: Map[String, String] = {
    val jdbc = classOf[JdbcRelationProvider].getCanonicalName
    val json = classOf[JsonFileFormat].getCanonicalName
    val parquet = classOf[GpuParquetFileFormat].getCanonicalName
    val csv = classOf[CSVFileFormat].getCanonicalName
    val libsvm = "org.apache.spark.ml.source.libsvm.LibSVMFileFormat"
    val orc = "org.apache.spark.sql.hive.orc.OrcFileFormat"
    val nativeOrc = classOf[OrcFileFormat].getCanonicalName
    val socket = classOf[TextSocketSourceProvider].getCanonicalName
    val rate = classOf[RateStreamProvider].getCanonicalName

    Map(
      "org.apache.spark.sql.jdbc" -> jdbc,
      "org.apache.spark.sql.jdbc.DefaultSource" -> jdbc,
      "org.apache.spark.sql.execution.datasources.jdbc.DefaultSource" -> jdbc,
      "org.apache.spark.sql.execution.datasources.jdbc" -> jdbc,
      "org.apache.spark.sql.json" -> json,
      "org.apache.spark.sql.json.DefaultSource" -> json,
      "org.apache.spark.sql.execution.datasources.json" -> json,
      "org.apache.spark.sql.execution.datasources.json.DefaultSource" -> json,
      "org.apache.spark.sql.parquet" -> parquet,
      "org.apache.spark.sql.parquet.DefaultSource" -> parquet,
      "org.apache.spark.sql.execution.datasources.parquet" -> parquet,
      "org.apache.spark.sql.execution.datasources.parquet.DefaultSource" -> parquet,
      "org.apache.spark.sql.hive.orc.DefaultSource" -> orc,
      "org.apache.spark.sql.hive.orc" -> orc,
      "org.apache.spark.sql.execution.datasources.orc.DefaultSource" -> nativeOrc,
      "org.apache.spark.sql.execution.datasources.orc" -> nativeOrc,
      "org.apache.spark.ml.source.libsvm.DefaultSource" -> libsvm,
      "org.apache.spark.ml.source.libsvm" -> libsvm,
      "com.databricks.spark.csv" -> csv,
      "org.apache.spark.sql.execution.streaming.TextSocketSourceProvider" -> socket,
      "org.apache.spark.sql.execution.streaming.RateSourceProvider" -> rate
    )
  }

  /**
   * Class that were removed in Spark 2.0. Used to detect incompatibility libraries for Spark 2.0.
   */
  private val spark2RemovedClasses = Set(
    "org.apache.spark.sql.DataFrame",
    "org.apache.spark.sql.sources.HadoopFsRelationProvider",
    "org.apache.spark.Logging")

  private def handleServiceConfigError(e: ServiceConfigurationError, context: String): Unit = {
    val cause = e.getCause
    if (cause.isInstanceOf[NoClassDefFoundError]) {
      val className = cause.getMessage.replaceAll("/", ".")
      if (spark2RemovedClasses.contains(className)) {
        throw new ClassNotFoundException(
          "Detected an incompatible DataSourceRegister. Please remove the " +
            s"incompatible library from classpath or upgrade it. Error: ${e.getMessage}", e)
      }
    }
    logWarning(s"Skipping broken DataSourceRegister provider$context: ${e.getMessage}")
  }

  def lookupDataSourceWithFallback(className: String, conf: SQLConf): Class[_] = {
    val cls = GpuDataSourceBase.lookupDataSource(className, conf)
    // `providingClass` is used for resolving data source relation for catalog tables.
    // As now catalog for data source V2 is under development, here we fall back all the
    // [[FileDataSourceV2]] to [[FileFormat]] to guarantee the current catalog works.
    // [[FileDataSourceV2]] will still be used if we call the load()/save() method in
    // [[DataFrameReader]]/[[DataFrameWriter]], since they use method `lookupDataSource`
    // instead of `providingClass`.
    val fallbackCls = ConstructorUtils.invokeConstructor(cls) match {
      case f: FileDataSourceV2 => f.fallbackFileFormat
      case _ => cls
    }
    // convert to GPU version
    fallbackCls
  }

  /** Given a provider name, look up the data source class definition. */
  def lookupDataSource(provider: String, conf: SQLConf): Class[_] = {
    val provider1 = backwardCompatibilityMap.getOrElse(provider, provider) match {
      case name if name.equalsIgnoreCase("orc") &&
          conf.getConf(SQLConf.ORC_IMPLEMENTATION) == "native" =>
        classOf[OrcDataSourceV2].getCanonicalName
      case name if name.equalsIgnoreCase("orc") &&
          conf.getConf(SQLConf.ORC_IMPLEMENTATION) == "hive" =>
        "org.apache.spark.sql.hive.orc.OrcFileFormat"
      case "com.databricks.spark.avro" if conf.replaceDatabricksSparkAvroEnabled =>
        "org.apache.spark.sql.avro.AvroFileFormat"
      case name => name
    }
    val provider2 = s"$provider1.DefaultSource"
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[DataSourceRegister], loader)

    // ServiceLoader iteration can throw ServiceConfigurationError if an incompatible
    // or broken DataSourceRegister provider is on the classpath. Use a manual loop
    // to skip broken providers and continue iteration.
    // hasNext() and next() are handled separately because hasNext() can also
    // throw ServiceConfigurationError.
    val providers = {
      val buf = new scala.collection.mutable.ListBuffer[DataSourceRegister]()
      val iter = serviceLoader.iterator()
      var done = false
      while (!done) {
        val hasMore = try {
          iter.hasNext
        } catch {
          case e: ServiceConfigurationError =>
            // Assume more entries may exist;
            // let next() advance past the broken entry.
            handleServiceConfigError(e, " (hasNext)")
            true
        }
        if (!hasMore) {
          done = true
        } else {
          try {
            val provider = iter.next()
            if (provider.shortName().equalsIgnoreCase(provider1)) {
              buf += provider
            }
          } catch {
            case e: ServiceConfigurationError =>
              handleServiceConfigError(e, "")
          }
        }
      }
      buf.toList
    }

    providers match {
      // the provider format did not match any given registered aliases
      case Nil =>
        try {
          Try(loader.loadClass(provider1)).orElse(Try(loader.loadClass(provider2))) match {
            case Success(dataSource) =>
              // Found the data source using fully qualified path
              dataSource
            case Failure(error) =>
              if (provider1.startsWith("org.apache.spark.sql.hive.orc")) {
                throw RapidsErrorUtils.orcNotUsedWithHiveEnabledError()
              } else if (provider1.toLowerCase(Locale.ROOT) == "avro" ||
                provider1 == "com.databricks.spark.avro" ||
                provider1 == "org.apache.spark.sql.avro") {
                throw RapidsErrorUtils.failedToFindAvroDataSourceError(provider1)
              } else if (provider1.toLowerCase(Locale.ROOT) == "kafka") {
                throw RapidsErrorUtils.failedToFindKafkaDataSourceError(provider1)
              } else {
                throw new ClassNotFoundException(
                  s"Failed to find data source: $provider1. Please find packages at " +
                    "http://spark.apache.org/third-party-projects.html",
                  error)
              }
          }
        } catch {
          case e: NoClassDefFoundError => // This one won't be caught by Scala NonFatal
            // NoClassDefFoundError's class name uses "/" rather than "." for packages
            val className = e.getMessage.replaceAll("/", ".")
            if (spark2RemovedClasses.contains(className)) {
              throw new ClassNotFoundException(s"$className was removed in Spark 2.0. " +
                "Please check if your library is compatible with Spark 2.0", e)
            } else {
              throw e
            }
        }
      case head :: Nil =>
        // there is exactly one registered alias
        head.getClass
      case sources =>
        // There are multiple registered aliases for the input. If there is single datasource
        // that has "org.apache.spark" package in the prefix, we use it considering it is an
        // internal datasource within Spark.
        val sourceNames = sources.map(_.getClass.getName)
        val internalSources = sources.filter(_.getClass.getName.startsWith("org.apache.spark"))
        if (internalSources.size == 1) {
          logWarning(s"Multiple sources found for $provider1 (${sourceNames.mkString(", ")}), " +
            s"defaulting to the internal datasource (${internalSources.head.getClass.getName}).")
          internalSources.head.getClass
        } else {
          throw RapidsErrorUtils.findMultipleDataSourceError(provider1, sourceNames)
        }
    }
  }

  /**
   * The key in the "options" map for deciding whether or not to glob paths before use.
   */
  val GLOB_PATHS_KEY = "__globPaths__"

  /**
   * Checks and returns files in all the paths.
   */
  private[sql] def checkAndGlobPathIfNecessary(
      pathStrings: Seq[String],
      hadoopConf: Configuration,
      checkEmptyGlobPath: Boolean,
      checkFilesExist: Boolean,
      numThreads: Integer = 40,
      enableGlobbing: Boolean): Seq[Path] = {
    val qualifiedPaths = pathStrings.map { pathString =>
      val path = new Path(pathString)
      val fs = path.getFileSystem(hadoopConf)
      path.makeQualified(fs.getUri, fs.getWorkingDirectory)
    }

    // Split the paths into glob and non glob paths, because we don't need to do an existence check
    // for globbed paths.
    val (globPaths, nonGlobPaths) = qualifiedPaths.partition(SparkHadoopUtil.get.isGlobPath)

    val globbedPaths =
      try {
        ThreadUtils.parmap(globPaths, "globPath", numThreads) { globPath =>
          val fs = globPath.getFileSystem(hadoopConf)
          val globResult = if (enableGlobbing) {
            SparkHadoopUtil.get.globPath(fs, globPath)
          } else {
            qualifiedPaths
          }

          if (checkEmptyGlobPath && globResult.isEmpty) {
            throw RapidsErrorUtils.dataPathNotExistError(globPath.toString)
          }

          globResult
        }.flatten
      } catch {
        case e: SparkException => throw e.getCause
      }

    if (checkFilesExist) {
      try {
        ThreadUtils.parmap(nonGlobPaths, "checkPathsExist", numThreads) { path =>
          val fs = path.getFileSystem(hadoopConf)
          if (!fs.exists(path)) {
            throw RapidsErrorUtils.dataPathNotExistError(path.toString)
          }
        }
      } catch {
        case e: SparkException => throw e.getCause
      }
    }

    val allPaths = globbedPaths ++ nonGlobPaths
    if (checkFilesExist) {
      val (filteredOut, filteredIn) = allPaths.partition { path =>
        HadoopFSUtils.shouldFilterOutPathName(path.getName)
      }
      if (filteredIn.isEmpty) {
        logWarning(
          s"All paths were ignored:\n  ${filteredOut.mkString("\n  ")}")
      } else {
        logDebug(
          s"Some paths were ignored:\n  ${filteredOut.mkString("\n  ")}")
      }
    }

    allPaths
  }

}
