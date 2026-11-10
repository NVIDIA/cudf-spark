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
{"spark":["350"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.io

import java.io.IOException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{JustRowsColumnarBatch, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.iceberg.{DataFile, PartitionSpec, Schema, StructLike}
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.types.{Type, Types}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

class GpuRevisitableDataWriterSuite extends AnyFunSuite {
  private class RecordingWriter extends GpuRevisitableDataWriter(null, null, null, 1024) {
    val opened = ArrayBuffer.empty[RecordingFileWriter]
    var active = 0
    var peakOpen = 0

    class RecordingFileWriter(val specId: Int, val partition: StructLike)
      extends FileWriter[SpillableColumnarBatch, DataWriteResult] {
      val dataFile: DataFile = mock[DataFile]
      var rowCount = 0
      var closeCount = 0
      var closeFailure: Option[IOException] = None

      override def write(batch: SpillableColumnarBatch): Unit = withResource(batch) { _ =>
        assert(closeCount == 0)
        rowCount += batch.numRows()
      }

      override def length(): Long = rowCount

      override def close(): Unit = {
        closeFailure.foreach { failure =>
          closeFailure = None
          throw failure
        }
        closeCount += 1
        assert(closeCount == 1)
        active -= 1
      }

      override def result(): DataWriteResult = {
        assert(closeCount == 1)
        new DataWriteResult(dataFile)
      }
    }

    override protected def newWriter(spec: PartitionSpec, partition: StructLike):
        FileWriter[SpillableColumnarBatch, DataWriteResult] = {
      assert(active == 0, "The preceding partition writer must close before opening another")
      val writer = new RecordingFileWriter(spec.specId(), partition)
      opened += writer
      active += 1
      peakOpen = math.max(peakOpen, active)
      writer
    }
  }

  private def spec(fieldType: Type, specId: Int = 0): PartitionSpec = {
    val schema = new Schema(Types.NestedField.optional(1, "f", fieldType))
    PartitionSpec.builderFor(schema).withSpecId(specId).identity("f").build()
  }

  private def identity(partition: StructLike): Option[Long] = {
    Option(partition.get(0, classOf[java.lang.Number]))
      .map(value => java.lang.Double.doubleToLongBits(value.doubleValue()))
  }

  Seq(Types.FloatType.get(), Types.DoubleType.get()).foreach { fieldType =>
    test(s"$fieldType partitions reuse adjacent writers and allow bounded revisits") {
      val partitionSpec = spec(fieldType)
      // Reuse one mutable partition to verify that the writer snapshots its key.
      val partition = GenericRecord.create(partitionSpec.partitionType())
      val inputs = Seq(Some(-0.0d), Some(-0.0d), Some(0.0d), Some(-0.0d),
        None, Some(Double.NaN), Some(Double.NaN)) ++ (1 to 100).map(i => Some(i.toDouble))
      val writer = new RecordingWriter
      withResource(writer) { _ =>
        inputs.foreach { value =>
          val boxed: AnyRef = value.map { number =>
            if (fieldType == Types.FloatType.get()) {
              Float.box(number.toFloat): AnyRef
            } else {
              Double.box(number): AnyRef
            }
          }.orNull
          partition.set(0, boxed)
          writer.write(new JustRowsColumnarBatch(1), partitionSpec, partition)
        }
        assert(writer.active == 1)
      }
      writer.close()
      val expectedKeys = Seq(Some(java.lang.Double.doubleToLongBits(-0.0d)),
        Some(java.lang.Double.doubleToLongBits(0.0d)),
        Some(java.lang.Double.doubleToLongBits(-0.0d)), None,
        Some(java.lang.Double.doubleToLongBits(Double.NaN))) ++
        (1 to 100).map(i => Some(java.lang.Double.doubleToLongBits(i.toDouble)))
      assert(writer.opened.map(w => identity(w.partition)).toSeq == expectedKeys)
      assert(writer.opened.take(5).map(_.rowCount).toSeq == Seq(2, 1, 1, 1, 2))
      assert(writer.opened.map(_.rowCount).sum == inputs.length)
      assert(writer.peakOpen == 1)
      assert(writer.active == 0)
      assert(writer.opened.forall(_.closeCount == 1))
      assert(writer.result().dataFiles().asScala.toSeq == writer.opened.map(_.dataFile).toSeq)
    }
  }

  test("a new spec closes the preceding writer even when partition values match") {
    val firstSpec = spec(Types.DoubleType.get())
    val secondSpec = spec(Types.DoubleType.get(), specId = 1)
    val partition = GenericRecord.create(firstSpec.partitionType())
    partition.set(0, Double.box(1.0d))
    val writer = new RecordingWriter
    withResource(writer) { _ =>
      writer.write(new JustRowsColumnarBatch(1), firstSpec, partition)
      writer.write(new JustRowsColumnarBatch(1), secondSpec, partition)
    }
    assert(writer.opened.map(_.specId).toSeq == Seq(0, 1))
    assert(writer.opened.forall(_.closeCount == 1))
    assert(writer.result().dataFiles().size() == 2)
  }

  test("an unused writer closes without opening files") {
    val writer = new RecordingWriter
    intercept[IllegalArgumentException] {
      writer.result()
    }
    writer.close()
    writer.close()
    assert(writer.opened.isEmpty)
    assert(writer.result().dataFiles().isEmpty)
  }

  test("a closed writer rejects new input") {
    val partitionSpec = spec(Types.DoubleType.get())
    val partition = GenericRecord.create(partitionSpec.partitionType())
    val writer = new RecordingWriter
    writer.close()
    withResource(new JustRowsColumnarBatch(1)) { batch =>
      intercept[IllegalArgumentException] {
        writer.write(batch, partitionSpec, partition)
      }
    }
    assert(writer.opened.isEmpty)
  }

  test("a failed partition close does not open a replacement writer") {
    val partitionSpec = spec(Types.DoubleType.get())
    val partition = GenericRecord.create(partitionSpec.partitionType())
    val writer = new RecordingWriter
    withResource(writer) { _ =>
      partition.set(0, Double.box(-0.0d))
      writer.write(new JustRowsColumnarBatch(1), partitionSpec, partition)
      val failure = new IOException("failed to close partition writer")
      writer.opened.head.closeFailure = Some(failure)
      partition.set(0, Double.box(0.0d))
      withResource(new JustRowsColumnarBatch(1)) { batch =>
        val thrown = intercept[IOException] {
          writer.write(batch, partitionSpec, partition)
        }
        assert(thrown eq failure)
      }
      assert(writer.opened.size == 1)
      assert(writer.active == 1)
    }
    assert(writer.active == 0)
    assert(writer.result().dataFiles().size() == 1)
  }
}
