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
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.tests.datasourcev2

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.{Expressions, NullOrdering, SortDirection,
  SortOrder, Transform}
import org.apache.spark.sql.connector.read.{Batch, HasPartitionKey, InputPartition,
  PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsReportOrdering,
  SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A row-based V2 source whose partitions are individually sorted but interleave when partitions
 * with the same key are concatenated. Spark 4.2 can therefore enable sorted merge when it groups
 * the duplicate `id` partitions for a storage-partitioned sort-merge join.
 */
object GroupPartitionsSortedDataSource {
  val SCHEMA = StructType(Array(
    StructField("id", IntegerType, nullable = false),
    StructField("value", IntegerType, nullable = true)))

  def partitions(
      side: String,
      direction: SortDirection,
      nullOrdering: NullOrdering): Array[GroupPartitionsSortedInputPartition] = {
    def rows(key: Int, values: Integer*): GroupPartitionsSortedInputPartition = {
      val sortedValues = values.sortWith { (left, right) =>
        compare(left, right, direction, nullOrdering) < 0
      }
      GroupPartitionsSortedInputPartition(key, sortedValues.map(key -> _).toArray)
    }

    side match {
      case "left" =>
        Array(
          rows(1, null, 10, 30),
          rows(1, null, 20, 40),
          rows(2, 5, 25),
          rows(3, 15))
      case "right" =>
        Array(
          rows(1, null, 10, 30),
          rows(1, null, 20, 40),
          rows(2, 5, 25),
          rows(3, 15))
      case other =>
        throw new IllegalArgumentException(
          s"Expected side=left or side=right, found $other")
    }
  }

  private def compare(
      left: Integer,
      right: Integer,
      direction: SortDirection,
      nullOrdering: NullOrdering): Int = {
    val ascending = direction == SortDirection.ASCENDING
    (left, right) match {
      case (null, null) => 0
      case (null, _) => if (nullOrdering == NullOrdering.NULLS_FIRST) -1 else 1
      case (_, null) => if (nullOrdering == NullOrdering.NULLS_FIRST) 1 else -1
      case _ =>
        val result = Integer.compare(left, right)
        if (ascending) result else -result
    }
  }
}

case class GroupPartitionsSortedInputPartition(
    key: Int,
    rows: Array[(Int, Integer)])
    extends InputPartition with HasPartitionKey {
  override def partitionKey(): InternalRow = new GenericInternalRow(Array[Any](key))
}

class GroupPartitionsSortedDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    GroupPartitionsSortedDataSource.SCHEMA

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val side = Option(options.get("side")).getOrElse {
      throw new IllegalArgumentException("The side option is required")
    }
    val direction = Option(options.get("direction")).getOrElse("asc") match {
      case "asc" => SortDirection.ASCENDING
      case "desc" => SortDirection.DESCENDING
      case other => throw new IllegalArgumentException(s"Unknown direction: $other")
    }
    val nullOrdering = Option(options.get("nulls")).getOrElse("default") match {
      case "default" => direction.defaultNullOrdering()
      case "first" => NullOrdering.NULLS_FIRST
      case "last" => NullOrdering.NULLS_LAST
      case other => throw new IllegalArgumentException(s"Unknown null ordering: $other")
    }
    val partitions =
      GroupPartitionsSortedDataSource.partitions(side, direction, nullOrdering)
    new GroupPartitionsSortedTable(partitions, direction, nullOrdering)
  }
}

class GroupPartitionsSortedTable(
    partitions: Array[GroupPartitionsSortedInputPartition],
    direction: SortDirection,
    nullOrdering: NullOrdering)
    extends Table with SupportsRead {
  override def name(): String = classOf[GroupPartitionsSortedDataSource].getName

  override def schema(): StructType = GroupPartitionsSortedDataSource.SCHEMA

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new GroupPartitionsSortedScan(partitions, direction, nullOrdering)
}

class GroupPartitionsSortedScan(
    partitions: Array[GroupPartitionsSortedInputPartition],
    direction: SortDirection,
    nullOrdering: NullOrdering)
    extends ScanBuilder with Scan with Batch with SupportsReportOrdering
        with SupportsReportPartitioning {
  override def build(): Scan = this

  override def readSchema(): StructType = GroupPartitionsSortedDataSource.SCHEMA

  override def toBatch: Batch = this

  override def outputPartitioning(): KeyGroupedPartitioning =
    new KeyGroupedPartitioning(Array(Expressions.identity("id")), partitions.length)

  override def outputOrdering(): Array[SortOrder] = Array(
    Expressions.sort(Expressions.column("id"), direction, nullOrdering),
    Expressions.sort(Expressions.column("value"), direction, nullOrdering))

  override def planInputPartitions(): Array[InputPartition] =
    partitions.map(identity[InputPartition])

  override def createReaderFactory(): PartitionReaderFactory =
    GroupPartitionsSortedReaderFactory
}

object GroupPartitionsSortedReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val rows = partition.asInstanceOf[GroupPartitionsSortedInputPartition].rows
    new PartitionReader[InternalRow] {
      private var index = -1

      override def next(): Boolean = {
        index += 1
        index < rows.length
      }

      override def get(): InternalRow = {
        val (id, value) = rows(index)
        new GenericInternalRow(Array[Any](id, value))
      }

      override def close(): Unit = {}
    }
  }
}
