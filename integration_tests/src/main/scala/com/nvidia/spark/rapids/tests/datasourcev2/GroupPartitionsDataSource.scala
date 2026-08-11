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
package com.nvidia.spark.rapids.tests.datasourcev2

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.read.{Batch, HasPartitionKey, InputPartition,
  PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A deterministic row-based V2 source for storage-partitioned join tests.
 *
 * It reports identity partitioning by `id` and exposes each entry below as a separate input
 * partition. Keeping the source row-based also exercises the transition between a CPU scan and
 * GpuGroupPartitionsExec.
 */
object GroupPartitionsDataSource {
  val SCHEMA = StructType(Array(
    StructField("id", IntegerType, nullable = false),
    StructField("value", IntegerType, nullable = false)))

  // The two partitions for key 1 require GroupPartitionsExec to coalesce them.
  val LEFT_PARTITIONS = Array(
    GroupPartitionsInputPartition(1, Array((1, 40))),
    GroupPartitionsInputPartition(1, Array((1, 41))),
    GroupPartitionsInputPartition(2, Array((2, 10))),
    GroupPartitionsInputPartition(3, Array((3, 15))))

  // Key 1 is replicated for the two matching left partitions, while missing key 3 requires
  // an empty padded partition.
  val RIGHT_PARTITIONS = Array(
    GroupPartitionsInputPartition(1, Array((1, 100))),
    GroupPartitionsInputPartition(2, Array((2, 200))))
}

case class GroupPartitionsInputPartition(key: Int, rows: Array[(Int, Int)])
    extends InputPartition with HasPartitionKey {
  // Spark uses this key to align input partitions without introducing a shuffle.
  override def partitionKey(): InternalRow = new GenericInternalRow(Array[Any](key))
}

class GroupPartitionsDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    GroupPartitionsDataSource.SCHEMA

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    // Each side has a different partition layout so Spark must align them before the join.
    val partitions = Option(options.get("side")) match {
      case Some("left") => GroupPartitionsDataSource.LEFT_PARTITIONS
      case Some("right") => GroupPartitionsDataSource.RIGHT_PARTITIONS
      case other =>
        throw new IllegalArgumentException(
          s"Expected side=left or side=right, found $other")
    }
    new GroupPartitionsTable(partitions)
  }
}

class GroupPartitionsTable(partitions: Array[GroupPartitionsInputPartition])
    extends Table with SupportsRead {
  override def name(): String = classOf[GroupPartitionsDataSource].getName

  override def schema(): StructType = GroupPartitionsDataSource.SCHEMA

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new GroupPartitionsScan(partitions)
}

class GroupPartitionsScan(partitions: Array[GroupPartitionsInputPartition])
    extends ScanBuilder with Scan with Batch with SupportsReportPartitioning {
  override def build(): Scan = this

  override def readSchema(): StructType = GroupPartitionsDataSource.SCHEMA

  override def toBatch: Batch = this

  // The partition count matches planInputPartitions, and each partition supplies its concrete
  // value through HasPartitionKey.
  override def outputPartitioning(): KeyGroupedPartitioning =
    new KeyGroupedPartitioning(Array(Expressions.identity("id")), partitions.length)

  // Widen each element explicitly because Scala arrays are invariant.
  override def planInputPartitions(): Array[InputPartition] =
    partitions.map(identity[InputPartition])

  override def createReaderFactory(): PartitionReaderFactory = GroupPartitionsReaderFactory
}

object GroupPartitionsReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val rows = partition.asInstanceOf[GroupPartitionsInputPartition].rows
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
