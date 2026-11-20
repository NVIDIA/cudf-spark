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
{"spark":["350"],"carryForward":"sameMinor"}
{"spark":["400","411"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.nio.ByteBuffer
import java.util.Collections

import scala.collection.mutable

import com.nvidia.spark.rapids.{NoRuleDataFromReplacementRule, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.iceberg.IcebergDefaultTestUtils.fieldWithWriteDefault
import org.apache.iceberg.{BaseTable, PartitionSpec, Schema, TableMetadata, TableOperations,
  TableProperties}
import org.apache.iceberg.types.Types
import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

class GpuWriteDefaultsSuite extends AnyFunSuite {

  test("write default validation accepts supported primitives") {
    val field = fieldWithWriteDefault(1, "value", Types.IntegerType.get(), Int.box(9))

    assert(IcebergFormatVersionSupport.unsupportedWriteDefault(new Schema(field)).isEmpty)
  }

  test("write default validation reports unsupported nested fields") {
    val field = fieldWithWriteDefault(2, "created_at", Types.TimestampType.withoutZone(),
      Long.box(0L))
    val schema = new Schema(Types.NestedField.optional(
      1, "payload", Types.StructType.of(field)))

    assert(IcebergFormatVersionSupport.unsupportedWriteDefault(schema)
      .contains("payload.created_at" -> "timestamp"))
  }

  test("write planning rejects a write-only default independently of initial defaults") {
    val field = fieldWithWriteDefault(1, "payload", Types.FixedType.ofLength(2),
      ByteBuffer.wrap(Array[Byte](1, 2)))
    val schema = new Schema(field)
    assert(IcebergFormatVersionSupport.unsupportedDefault(schema).isEmpty)
    val metadata = TableMetadata.newTableMetadata(
      schema, PartitionSpec.unpartitioned(), "file:///write-default-test",
      Collections.singletonMap(TableProperties.FORMAT_VERSION, "3"))
    val ops = mock(classOf[TableOperations])
    when(ops.current()).thenReturn(metadata)
    val table = new BaseTable(ops, "write-default-test")
    val conf = new RapidsConf(Map("spark.rapids.sql.format.iceberg.v3.enabled" -> "true"))
    class WriteDefaultMeta extends RapidsMeta[AnyRef, AnyRef, AnyRef](
        new Object, conf, None, new NoRuleDataFromReplacementRule) {
      override val childPlans = Seq.empty
      override val childExprs = Seq.empty
      override val childScans = Seq.empty
      override val childParts = Seq.empty
      override val childDataWriteCmds = Seq.empty
      cannotBeReplacedReasons = Some(mutable.Set.empty[String])
      override def convertToGpu(): AnyRef = wrapped
      override def tagSelfForGpu(): Unit =
        IcebergFormatVersionSupport.tagForWriteSchema(table, schema, this)
      def reasons: Set[String] = cannotBeReplacedReasons.get.toSet
    }

    val meta = new WriteDefaultMeta
    meta.tagSelfForGpu()

    assert(meta.reasons == Set(
      "Iceberg write default for field 'payload' with type fixed[2] is not supported on GPU"))
  }
}
