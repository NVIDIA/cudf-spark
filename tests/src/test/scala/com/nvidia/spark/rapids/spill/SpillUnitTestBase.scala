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

package com.nvidia.spark.rapids.spill

import java.io.File
import java.math.RoundingMode

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.CodecType
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

trait SpillUnitTestBase extends FunSuiteWithTempDir with MockitoSugar {

  override def beforeEach(): Unit = {
    super.beforeEach()
    val sc = new SparkConf
    sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1024")
    sc.set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "false")
    SpillFramework.initialize(new RapidsConf(sc))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    SpillFramework.shutdown()
  }

  // -1 disables the host store limit
  protected final val hostSpillStorageSizes = Seq("-1", "1MB", "16MB")
  protected final val spillToDiskBounceBuffers = Seq("128KB", "2MB", "128MB")
  protected final val chunkedPackBounceBuffers = Seq("1MB", "8MB", "128MB")

  protected final def buildContiguousTable(): (ContiguousTable, Array[DataType]) = {
    val (tbl, dataTypes) = buildTable()
    withResource(tbl) { _ =>
      (tbl.contiguousSplit()(0), dataTypes)
    }
  }

  protected final def buildTableOfLongs(numRows: Int): (ContiguousTable, Array[DataType])= {
    val vals = (0 until numRows).map(_.toLong)
    withResource(HostColumnVector.fromLongs(vals: _*)) { hcv =>
      withResource(hcv.copyToDevice()) { cv =>
        withResource(new Table(cv)) { table =>
          (table.contiguousSplit()(0), Array[DataType](LongType))
        }
      }
    }
  }

  protected final def buildNonContiguousTableOfLongs(numRows: Int): (Table, Array[DataType])= {
    val vals = (0 until numRows).map(_.toLong)
    withResource(HostColumnVector.fromLongs(vals: _*)) { hcv =>
      withResource(hcv.copyToDevice()) { cv =>
        (new Table(cv), Array[DataType](LongType))
      }
    }
  }

  protected final def buildTable(): (Table, Array[DataType]) = {
    val tbl = new Table.TestBuilder()
      .column(5, null.asInstanceOf[java.lang.Integer], 3, 1)
      .column("five", "two", null, null)
      .column(5.0, 2.0, 3.0, 1.0)
      .decimal64Column(-5, RoundingMode.UNNECESSARY, 0, null, -1.4, 10.123)
      .build()
    val types: Array[DataType] =
      Seq(IntegerType, StringType, DoubleType, DecimalType(10, 5)).toArray
    (tbl, types)
  }

  protected final def buildTableWithDuplicate(): (Table, Array[DataType]) = {
    withResource(ColumnVector.fromInts(5, null.asInstanceOf[java.lang.Integer], 3, 1)) { intCol =>
      withResource(ColumnVector.fromStrings("five", "two", null, null)) { stringCol =>
        withResource(ColumnVector.fromDoubles(5.0, 2.0, 3.0, 1.0)) { doubleCol =>
          // add intCol twice
          (new Table(intCol, intCol, stringCol, doubleCol),
            Array(IntegerType, IntegerType, StringType, DoubleType))
        }
      }
    }
  }

  protected final def buildEmptyTable(): (Table, Array[DataType]) = {
    val (tbl, types) = buildTable()
    val emptyTbl = withResource(tbl) { _ =>
      withResource(ColumnVector.fromBooleans(false, false, false, false)) { mask =>
        tbl.filter(mask) // filter all out
      }
    }
    (emptyTbl, types)
  }

  protected final def testBufferFileDeletion(canShareDiskPaths: Boolean): Unit = {
    val (_, handle, _) = addContiguousTableToFramework()
    var path: File = null
    withResource(handle) { _ =>
      SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
      SpillFramework.stores.hostStore.spill(handle.approxSizeInBytes)
      assert(handle.host.isDefined)
      assert(handle.host.map(_.disk.isDefined).get)
      path = SpillFramework.stores.diskStore.getFile(handle.host.flatMap(_.disk).get.blockId)
      assert(path.exists)
    }
    assert(!path.exists)
  }

  protected final def addContiguousTableToFramework(): (
    Long, SpillableColumnarBatchFromBufferHandle, Array[DataType]) = {
    val (ct, dataTypes) = buildContiguousTable()
    val bufferSize = ct.getBuffer.getLength
    val handle = SpillableColumnarBatchFromBufferHandle(ct, dataTypes)
    (bufferSize, handle, dataTypes)
  }

  protected final def addTableToFramework(): (SpillableColumnarBatchHandle, Array[DataType]) = {
    // store takes ownership of the table
    val (tbl, dataTypes) = buildTable()
    val cb = withResource(tbl) { _ => GpuColumnVector.from(tbl, dataTypes) }
    val handle = SpillableColumnarBatchHandle(cb)
    (handle, dataTypes)
  }

  protected final def addZeroRowsTableToFramework(): (
    SpillableColumnarBatchHandle, Array[DataType]) = {
    val (table, dataTypes) = buildEmptyTable()
    val cb = withResource(table) { _ => GpuColumnVector.from(table, dataTypes) }
    val handle = SpillableColumnarBatchHandle(cb)
    (handle, dataTypes)
  }

  protected final def buildHostBatch(): (ColumnarBatch, Array[DataType]) = {
    val (ct, dataTypes) = buildContiguousTable()
    val hostCols = withResource(ct) { _ =>
      withResource(ct.getTable) { tbl =>
        (0 until tbl.getNumberOfColumns)
          .map(c => tbl.getColumn(c).copyToHost())
      }
    }.toArray
    (new ColumnarBatch(
      hostCols.zip(dataTypes).map { case (hostCol, dataType) =>
        new RapidsHostColumnVector(dataType, hostCol)
      }, hostCols.head.getRowCount.toInt), dataTypes)
  }

  protected final def buildHostBatchWithDuplicate(): (ColumnarBatch, Array[DataType]) = {
    val (ct, dataTypes) = buildContiguousTable()
    val hostCols = withResource(ct) { _ =>
      withResource(ct.getTable) { tbl =>
        (0 until tbl.getNumberOfColumns)
          .map(c => tbl.getColumn(c).copyToHost())
      }
    }.toArray
    hostCols.foreach(_.incRefCount())
    (new ColumnarBatch(
      (hostCols ++ hostCols).zip(dataTypes ++ dataTypes).map { case (hostCol, dataType) =>
        new RapidsHostColumnVector(dataType, hostCol)
      }, hostCols.head.getRowCount.toInt), dataTypes)
  }

  protected final def buildContiguousTable(start: Int, numRows: Int): ContiguousTable = {
    val vals = (0 until numRows).map(_.toLong + start)
    withResource(HostColumnVector.fromLongs(vals: _*)) { hcv =>
      withResource(hcv.copyToDevice()) { cv =>
        withResource(HostColumnVector.decimalFromLongs(-3, vals: _*)) { decHcv =>
          withResource(decHcv.copyToDevice()) { decCv =>
            withResource(new Table(cv, decCv)) { table =>
              table.contiguousSplit()(0)
            }
          }
        }
      }
    }
  }

  protected final def buildCompressedBatch(start: Int, numRows: Int): ColumnarBatch = {
    val codec = TableCompressionCodec.getCodec(
      CodecType.NVCOMP_LZ4, TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
    withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
      compressor.addTableToCompress(buildContiguousTable(start, numRows))
      withResource(compressor.finish()) { compressed =>
        GpuCompressedColumnVector.from(compressed.head)
      }
    }
  }

  protected final def decompressBatch(cb: ColumnarBatch): ColumnarBatch = {
    val schema = new StructType().add("i", LongType)
      .add("j", DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 3))
    val sparkTypes = GpuColumnVector.extractTypes(schema)
    val codec = TableCompressionCodec.getCodec(
      CodecType.NVCOMP_LZ4, TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
    withResource(codec.createBatchDecompressor(0, Cuda.DEFAULT_STREAM)) { decompressor =>
      val gcv = cb.column(0).asInstanceOf[GpuCompressedColumnVector]
      // we need to incRefCount since the decompressor closes its inputs
      gcv.getTableBuffer.incRefCount()
      decompressor.addBufferToDecompress(gcv.getTableBuffer, gcv.getTableMeta.bufferMeta())
      withResource(decompressor.finishAsync()) { decompressed =>
        MetaUtils.getBatchFromMeta(
          decompressed.head,
          MetaUtils.dropCodecs(gcv.getTableMeta),
          sparkTypes)
      }
    }
  }

  protected final def readWriteTestWithBatches(conf: SparkConf,
      shareDiskPaths: Boolean*): Unit = {
    assert(shareDiskPaths.nonEmpty)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(conf))

    shareDiskPaths.foreach { _ =>
      val (_, handle, dataTypes) = addContiguousTableToFramework()
      withResource(handle) { _ =>
        val expectedCt = withResource(handle.materialize(dataTypes)) { devbatch =>
          withResource(GpuColumnVector.from(devbatch)) { tmpTbl =>
            tmpTbl.contiguousSplit()(0)
          }
        }
        withResource(expectedCt) { _ =>
          val expectedBatch = withResource(expectedCt.getTable) { expectedTbl =>
            GpuColumnVector.from(expectedTbl, dataTypes)
          }
          withResource(expectedBatch) { _ =>
            assertResult(true)(
              SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes) > 0)
            assertResult(true)(
              SpillFramework.stores.hostStore.spill(handle.approxSizeInBytes) > 0)
            withResource(handle.materialize(dataTypes)) { actualBatch =>
              TestUtils.compareBatches(expectedBatch, actualBatch)
            }
          }
        }
      }
    }
  }

  protected final def testCloseWhileSpilling[T <: SpillableHandle](
      handle: T, store: SpillableStore[T], sleepBeforeCloseNanos: Long): Unit = {
    assert(handle.spillable)
    assertResult(1)(store.numHandles)
    val t1 = new Thread (() => {
      // cannot assert how much is spills because it depends on whether the handle
      // is already closed or not and we're trying to force both conditions
      // in this test to show that it handles potential races correctly
      store.spill(handle.approxSizeInBytes)
    })
    t1.start()

    // we observed that the race will typically trigger if sleeping between 0.1 and 1 millis
    Thread.sleep(sleepBeforeCloseNanos / 1000000L, (sleepBeforeCloseNanos % 1000000L).toInt)
    handle.close()
    t1.join()
    assertResult(0)(store.numHandles)
  }

  // This is a small monte carlo simulation where we test overlaying
  // closing buffers and spilling at difference delay points to tease out possible
  // race conditions. There's only one param/variable in the simulation, but it could
  // be extended to N params if needed
  protected final def monteCarlo(oneIteration: Long => Unit): Unit = {
    for (i <- 1L to 10L) {
      val nanos: Long = i * 100 * 1000
      oneIteration(nanos)
    }
  }

  protected final def addSpillableTableToFramework(): (SpillableTableHandle, Array[DataType]) = {
    val (tbl, dataTypes) = buildTable()
    val handle = SpillableTableHandle(tbl)
    (handle, dataTypes)
  }

  protected final def addSpillableTableWithDuplicateToFramework():
  (SpillableTableHandle, Array[DataType]) = {
    val (tbl, dataTypes) = buildTableWithDuplicate()
    val handle = SpillableTableHandle(tbl)
    (handle, dataTypes)
  }
}
