/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import ai.rapids.cudf.{Rmm, RmmAllocationMode, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExecBase, SerializeBatchDeserializeHostBuffer, SerializeConcatHostBuffersDeserializeBatch}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class SerializationSuite extends AnyFunSuite
  with BeforeAndAfterAll {


  override def beforeAll(): Unit = {
    super.beforeAll()
    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    SpillFramework.initialize(new RapidsConf(new SparkConf))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SpillFramework.shutdown()
    Rmm.shutdown()
  }

  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1, 1, 1, 1, 1, 1, 1)
        .column("five", "two", null, null, "one", "one", "one", "one", "one", "one")
        .column(5.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType, DoubleType))
    }
  }

  /**
   * Creates an empty batch: has columns but numRows = 0
   */
  private def buildEmptyBatch(): ColumnarBatch = {
    GpuColumnVector.emptyBatchFromTypes(Array(IntegerType, FloatType))
  }

  /**
   * Creates a completely empty batch: 0 columns and 0 rows
   */
  private def buildEmptyBatchNoCols(): ColumnarBatch = {
    GpuColumnVector.emptyBatchFromTypes(Array.empty)
  }

  /**
   * Creates a "just rows" batch: no columns but numRows > 0.
   * Seen with a no-condition cross join followed by a count
   */
  private def buildJustRowsBatch(): ColumnarBatch = {
    new ColumnarBatch(Array.empty, 1234)
  }

  /**
   * Creates a "just rows" batch: no columns and numRows == 0
   * Seen with a no-condition cross join followed by a count
   */
  private def buildJustRowsBatchZeroRows(): ColumnarBatch = {
    new ColumnarBatch(Array.empty, 0)
  }

  private def createDeserializedHostBuffer(
      batch: ColumnarBatch): SerializeBatchDeserializeHostBuffer = {
    withResource(new SerializeBatchDeserializeHostBuffer(batch)) { obj =>
      // Return a deserialized form of the object as if it was read on the driver
      SerializationUtils.clone(obj)
    }
  }

  private def makeBroadcastBatch(
      gpuBatch: ColumnarBatch): SerializeConcatHostBuffersDeserializeBatch = {
    val attrs = GpuColumnVector.extractTypes(gpuBatch).map(t => AttributeReference("", t)())
    if (gpuBatch.numRows() == 0 && gpuBatch.numCols == 0) {
      GpuBroadcastExchangeExecBase.makeBroadcastBatch(
        Array.empty, Seq.empty, NoopMetric, NoopMetric, NoopMetric, SQLConf.get)
    } else if (gpuBatch.numCols() == 0) {
      new SerializeConcatHostBuffersDeserializeBatch(
        null,
        attrs,
        gpuBatch.numRows(),
        0L)
    } else {
      val buffer = createDeserializedHostBuffer(gpuBatch)
      // makeBroadcastBatch consumes `buffer`
      GpuBroadcastExchangeExecBase.makeBroadcastBatch(
        Array(buffer), attrs, NoopMetric, NoopMetric, NoopMetric, SQLConf.get)
    }
  }

  /**
   * Helper that calls `closeInternal` on our broadcast object, instead of `close`
   * because the broadcast object is be garbage collected, and not auto-closed by
   * Spark's MemoryStore.
   */
  def withBroadcast[T](b: SerializeConcatHostBuffersDeserializeBatch)
      (body: SerializeConcatHostBuffersDeserializeBatch => T): T = {
    try {
      body(b)
    } finally {
      b.closeInternal()
    }
  }

  /**
   * Helper to create a host-backed ColumnarBatch from a gpu-backed ColumnarBatch
   */
  def toHostBatch(gpuBatch: ColumnarBatch): ColumnarBatch = {
    val hostColumns = GpuColumnVector
        .extractColumns(gpuBatch)
        .safeMap(_.copyToHost().asInstanceOf[ColumnVector])
    new ColumnarBatch(hostColumns, gpuBatch.numRows())
  }

  test("broadcast driver serialize after deserialize") {
    val broadcast = withResource(buildBatch()){ makeBroadcastBatch }
    withBroadcast(broadcast) { _ =>
      // clone via serialization without manifesting the GPU batch
      withBroadcast(SerializationUtils.clone(broadcast)) { clonedObj =>
        assert(clonedObj.data != null)
        assertResult(broadcast.dataSize)(clonedObj.dataSize)
        assertResult(broadcast.numRows)(clonedObj.numRows)
        // try to clone it again from the cloned object
        SerializationUtils.clone(clonedObj).closeInternal()
      }
    }
  }

  test("broadcast driver obtain hostBatch") {
    withResource(buildBatch()) { gpuBatch =>
      withResource(toHostBatch(gpuBatch)) { expectedHostBatch =>
        val broadcast = makeBroadcastBatch(gpuBatch)
        withBroadcast(broadcast) { _ =>
          withResource(broadcast.hostBatch) { hostBatch1 =>
            TestUtils.compareBatches(expectedHostBatch, hostBatch1)
            withBroadcast(SerializationUtils.clone(broadcast)) { clonedObj =>
              withResource(clonedObj.hostBatch) { hostBatch2 =>
                TestUtils.compareBatches(expectedHostBatch, hostBatch2)
              }
            }
          }
        }
      }
    }
  }

  test("broadcast executor obtain hostBatch") {
    withResource(buildBatch()) { gpuBatch =>
      withResource(toHostBatch(gpuBatch)) { expectedHostBatch =>
        val broadcast = makeBroadcastBatch(gpuBatch)
        withBroadcast(broadcast) { _ =>
          withResource(broadcast.batch.getColumnarBatch) { materialized =>
            TestUtils.compareBatches(gpuBatch, materialized)
          }
          // the host batch here is obtained from the GPU batch since
          // we materialized
          withResource(broadcast.hostBatch) { hostBatch1 =>
            TestUtils.compareBatches(expectedHostBatch, hostBatch1)
            withBroadcast(SerializationUtils.clone(broadcast)) { clonedObj =>
              withResource(clonedObj.hostBatch) { hostBatch2 =>
                TestUtils.compareBatches(expectedHostBatch, hostBatch2)
              }
            }
          }
        }
      }
    }
  }

  test("broadcast executor ser/deser empty batch") {
    withResource(Seq(buildEmptyBatch(), buildEmptyBatchNoCols())) { batches =>
      batches.foreach { gpuExpected =>
        val broadcast = makeBroadcastBatch(gpuExpected)
        withBroadcast(broadcast) { _ =>
          withResource(broadcast.batch.getColumnarBatch) { gpuBatch =>
            TestUtils.compareBatches(gpuExpected, gpuBatch)
          }
          // clone via serialization after manifesting the GPU batch
          withBroadcast(SerializationUtils.clone(broadcast)) { clonedObj =>
            withResource(clonedObj.batch.getColumnarBatch) { gpuClonedBatch =>
              TestUtils.compareBatches(gpuExpected, gpuClonedBatch)
            }
            // try to clone it again from the cloned object
            SerializationUtils.clone(clonedObj).closeInternal()
          }
        }
      }
    }
  }

  test("broadcast executor ser/deser just rows") {
    withResource(Seq(buildJustRowsBatch(), buildJustRowsBatchZeroRows())) { batches =>
      batches.foreach { gpuExpected =>
        val broadcast = makeBroadcastBatch(gpuExpected)
        withBroadcast(broadcast) { _ =>
          withResource(broadcast.batch.getColumnarBatch) { gpuBatch =>
            TestUtils.compareBatches(gpuExpected, gpuBatch)
          }
          // clone via serialization after manifesting the GPU batch
          withBroadcast(SerializationUtils.clone(broadcast)) { clonedObj =>
            withResource(clonedObj.batch.getColumnarBatch) { gpuClonedBatch =>
              TestUtils.compareBatches(gpuExpected, gpuClonedBatch)
            }
            // try to clone it again from the cloned object
            SerializationUtils.clone(clonedObj).closeInternal()
          }
        }
      }
    }
  }

  test("broadcast executor serialize after deserialize") {
    withResource(buildBatch()) { gpuExpected =>
      val broadcast = makeBroadcastBatch(gpuExpected)
      withBroadcast(broadcast) { _ =>
        withResource(broadcast.batch.getColumnarBatch) { gpuBatch =>
          TestUtils.compareBatches(gpuExpected, gpuBatch)
        }
        // clone via serialization after manifesting the GPU batch
        withBroadcast(SerializationUtils.clone(broadcast)) { clonedObj =>
          withResource(clonedObj.batch.getColumnarBatch) { gpuClonedBatch =>
            TestUtils.compareBatches(gpuExpected, gpuClonedBatch)
          }
          // try to clone it again from the cloned object
          SerializationUtils.clone(clonedObj).closeInternal()
        }
      }
    }
  }

  test("broadcast reuse after spill") {
    withResource(buildBatch()) { gpuExpected =>
      val broadcast = makeBroadcastBatch(gpuExpected)
      withBroadcast(broadcast) { _ =>
        // spill first thing (we didn't materialize it in the executor)
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)
        broadcast.doWriteObject(oos)

        val inputStream =
          new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
        broadcast.doReadObject(inputStream)

        // use it now
        withResource(broadcast.batch.getColumnarBatch) { gpuBatch =>
          TestUtils.compareBatches(gpuExpected, gpuBatch)
        }
      }
    }
  }

  test("broadcast reuse materialized after spill") {
    withResource(buildBatch()) { gpuExpected =>
      val broadcast = makeBroadcastBatch(gpuExpected)
      withBroadcast(broadcast) { _ =>
        // materialize
        withResource(broadcast.batch.getColumnarBatch) { cb =>
          TestUtils.compareBatches(gpuExpected, cb)
        }

        // spill after materialization (from the GPU batch)
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)
        broadcast.doWriteObject(oos)

        val inputStream =
          new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
        val before = broadcast.batch
        // this is a noop, because `broadcast` has spillable already.
        broadcast.doReadObject(inputStream)
        assertResult(before)(broadcast.batch) // it is the same as before

        // use it now
        withResource(broadcast.batch.getColumnarBatch) { gpuBatch =>
          TestUtils.compareBatches(gpuExpected, gpuBatch)
        }
      }
    }
  }

  test("broadcast cloned use after spill") {
    withResource(buildBatch()) { gpuExpected =>
      val broadcast = makeBroadcastBatch(gpuExpected)
      withBroadcast(broadcast) { _ =>
        // spill first thing (we didn't materialize it in the executor)
        val baos = new ByteArrayOutputStream()
        SerializationUtils.serialize(broadcast, baos)
        val inputStream = new ByteArrayInputStream(baos.toByteArray)
        withBroadcast(SerializationUtils
          .deserialize[SerializeConcatHostBuffersDeserializeBatch](
            inputStream)) { materialized =>
          // this materializes a new batch from what was deserialized
          withResource(materialized.batch.getColumnarBatch) { gpuBatch =>
            TestUtils.compareBatches(gpuExpected, gpuBatch)
          }
        }
      }
    }
  }

  test("GpuSerializableBatch round-trips a GPU batch through Java serialization") {
    // buildBatch() makes a fresh batch each call; GpuSerializableBatch.writeObject consumes
    // (closes) the batch it wraps, so wrap an independent copy and compare against `expected`.
    withResource(buildBatch()) { expected =>
      val baos = new ByteArrayOutputStream()
      withResource(new org.apache.spark.sql.rapids.GpuSerializableBatch(buildBatch())) {
        serializable =>
          withResource(new ObjectOutputStream(baos)) { oos =>
            oos.writeObject(serializable)
          }
      }
      withResource(new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))) { ois =>
        val deserialized =
          ois.readObject().asInstanceOf[org.apache.spark.sql.rapids.GpuSerializableBatch]
        withResource(deserialized) { d =>
          TestUtils.compareBatches(expected, d.getBatch)
        }
      }
    }
  }
}
