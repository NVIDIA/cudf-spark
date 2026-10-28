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
package com.nvidia.spark.rapids.tests.delta

import java.lang.reflect.Proxy
import java.util

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{And, Filter, GreaterThanOrEqual, IsNotNull}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Creates Delta's real server-side planned table without an external planning service. */
class ServerSidePlannedTestCatalog extends TableCatalog {
  private val deltaPackage = "org.apache.spark.sql.delta.serverSidePlanning."
  private var catalogName: String = _
  private var root: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
    root = options.get("path")
  }

  override def name(): String = catalogName

  override def loadTable(ident: Identifier): Table = {
    val spark = SparkSession.active
    val selected = new Path(root, "selected")
    val fileSchema = spark.read.parquet(selected.toString).schema
    val schema = ident.name() match {
      case "required" => fileSchema.add(StructField("absent", IntegerType, nullable = false))
      case _ => fileSchema
    }
    // Delta is optional and has incompatible versions in the test matrix. Resolve
    // its 4.2/4.3 APIs only when this catalog is actually used.
    val loader = Thread.currentThread().getContextClassLoader
    val fileClass = loader.loadClass(deltaPackage + "ScanFile")
    def scanFiles(path: Path): List[AnyRef] = {
      path.getFileSystem(spark.sparkContext.hadoopConfiguration).listStatus(path)
        .filter(_.getPath.getName.endsWith(".parquet"))
        .sortBy(_.getPath.toString)
        .map { status =>
          fileClass.getConstructor(classOf[String], java.lang.Long.TYPE, classOf[String])
            .newInstance(status.getPath.toString, Long.box(status.getLen), "parquet")
            .asInstanceOf[AnyRef]
        }.toList
    }
    val planConstructor = loader.loadClass(deltaPackage + "ScanPlan")
      .getConstructor(loader.loadClass("scala.collection.immutable.Seq"), classOf[Option[_]])
    def scanPlan(files: List[AnyRef]): AnyRef =
      planConstructor.newInstance(files, None).asInstanceOf[AnyRef]

    val files = ident.name() match {
      case "empty" => List.empty[AnyRef]
      case "missing" | "corrupt" =>
        List(fileClass.getConstructor(classOf[String], java.lang.Long.TYPE, classOf[String])
          .newInstance(new Path(root, ident.name() + ".parquet").toString,
            Long.box(16), "parquet").asInstanceOf[AnyRef])
      case _ => scanFiles(selected)
    }
    val plan = scanPlan(files)
    val pushFilters = ident.name() == "pushed"
    val matchingFiles = if (pushFilters) scanFiles(new Path(root, "matching")) else Nil
    // Only these predicates are faithfully evaluated by this fixture's file selection.
    def supportedFilter(filter: Filter): Boolean = filter match {
      case GreaterThanOrEqual("id", 10L) | IsNotNull("id") => true
      case And(left, right) => supportedFilter(left) && supportedFilter(right)
      case _ => false
    }
    def hasLowerBound(filter: Filter): Boolean = filter match {
      case GreaterThanOrEqual("id", 10L) => true
      case And(left, right) => hasLowerBound(left) || hasLowerBound(right)
      case _ => false
    }
    val clientClass = loader.loadClass(deltaPackage + "ServerSidePlanningClient")
    val client = Proxy.newProxyInstance(loader, Array(clientClass), (proxy, method, args) => {
      method.getName match {
        case "planScan" if pushFilters =>
          val filter = args(2).asInstanceOf[Option[Filter]]
          require(filter.forall(supportedFilter), s"Unsupported planned filter: $filter")
          scanPlan(if (filter.exists(hasLowerBound)) matchingFiles else files ++ matchingFiles)
        case "planScan" => plan
        case "canConvertFilters" =>
          Boolean.box(pushFilters && args(0).asInstanceOf[Array[Filter]].forall(supportedFilter))
        case "close" => null
        case "toString" => "FixedManifestPlanningClient"
        case "hashCode" => Int.box(System.identityHashCode(proxy))
        case "equals" => Boolean.box(proxy.asInstanceOf[AnyRef] eq args(0))
        case other => throw new UnsupportedOperationException(other)
      }
    })
    loader.loadClass(deltaPackage + "ServerSidePlannedTable")
      .getConstructor(classOf[SparkSession], classOf[String], classOf[String],
        classOf[StructType], clientClass)
      .newInstance(spark, "db", ident.name(), schema, client).asInstanceOf[Table]
  }

  override def listTables(namespace: Array[String]): Array[Identifier] =
    Array("table", "empty", "pushed", "required", "missing", "corrupt")
      .map(Identifier.of(namespace, _))

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: util.Map[String, String]): Table = throw new UnsupportedOperationException

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException

  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException
}
