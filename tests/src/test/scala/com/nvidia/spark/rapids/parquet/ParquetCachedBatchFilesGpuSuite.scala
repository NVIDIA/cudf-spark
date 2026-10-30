/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids.parquet

import ai.rapids.cudf.{ColumnVector, CompressionType, HostMemoryBuffer,
  ParquetChunkedReader => CudfParquetChunkedReader, ParquetOptions, ParquetWriterOptions, Table}
import com.nvidia.spark.rapids.{SparkQueryCompareTestSuite, SpillableHostBuffer, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{CpuRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.spill.SpillFramework
import org.mockito.Mockito.{doAnswer, spy, times, verify}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.sql.types.{LongType, StringType, StructType}

class ParquetCachedBatchFilesGpuSuite extends SparkQueryCompareTestSuite {
  test("spilled source reuses one region and evicts it before loading another") {
    withGpuSparkSession { _ =>
      val handles = SpillFramework.stores.hostStore.numHandles
      val materialized = scala.collection.mutable.ArrayBuffer.empty[HostMemoryBuffer]
      val regions = (0 until 2).map { region =>
        val buffer = closeOnExcept(HostMemoryBuffer.allocate(16)) { allocated =>
          allocated.setBytes(0, Array.tabulate[Byte](16)(i => (region * 16 + i).toByte), 0, 16)
          allocated
        }
        val spillable = spy(SpillableHostBuffer(
          buffer, 16, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
        doAnswer((invocation: InvocationOnMock) => {
          // Any previous disk materialization must be released before the next allocation.
          assert(materialized.forall(_.getRefCount == 0))
          val result = invocation.callRealMethod().asInstanceOf[HostMemoryBuffer]
          materialized += result
          result
        }).when(spillable).getDataHostBuffer()
        (spillable, 16L)
      }
      withResource(regions.map(_._1)) { _ =>
        assert(SpillFramework.stores.hostStore.spill(Long.MaxValue) > 0)
        withResource(ParquetCachedBatchFiles.fromSpillableRegions(regions)) { source =>
          val bytes = new Array[Byte](8)
          source.readFully(0, bytes, 0, 8)
          assert(bytes.toSeq == (0 until 8).map(_.toByte))
          source.readFully(8, bytes, 0, 8)
          verify(regions.head._1, times(1)).getDataHostBuffer()
          // One read crosses the boundary; only one materialization stays live.
          source.readFully(12, bytes, 0, 8)
          assert(bytes.toSeq == (12 until 20).map(_.toByte))
          verify(regions(1)._1, times(1)).getDataHostBuffer()
          source.readFully(0, bytes, 0, 8)
          verify(regions.head._1, times(2)).getDataHostBuffer()
          assert(materialized.count(_.getRefCount > 0) == 1)
          // Invalid reads also retire the cached reference.
          intercept[IllegalArgumentException] {
            source.readFully(32, bytes, 0, 1)
          }
          assert(materialized.forall(_.getRefCount == 0))
        }
      }
      assert(SpillFramework.stores.hostStore.numHandles == handles)
    }
  }

  private val rows = 32768
  private val schema = new StructType().add("id", LongType).add("text", StringType)

  private def check(table: Table, start: Int, projected: Boolean): Int = {
    assert(table.getNumberOfColumns == (if (projected) 1 else 2))
    if (!projected) {
      withResource(table.getColumn(0).copyToHost()) { ids =>
        (0 until table.getRowCount.toInt).foreach { index =>
          assert(ids.getLong(index) == (start + index).toLong)
        }
      }
    }
    withResource(table.getColumn(if (projected) 0 else 1).copyToHost()) { text =>
      (0 until table.getRowCount.toInt).foreach { index =>
        val row = start + index
        assert(text.isNull(index) == (row % 5 == 0))
        if (row % 5 != 0) assert(text.getJavaString(index) == "dictionary-" + row % 11)
      }
    }
    start + table.getRowCount.toInt
  }

  test("GPU writer regions spill and logical column views decode") {
    val compression = CompressionType.SNAPPY
    withGpuSparkSession { _ =>
      val handles = SpillFramework.stores.hostStore.numHandles
      val ids = Array.tabulate[Long](rows)(_.toLong)
      val text = Array.tabulate[String](rows) { row =>
        if (row % 5 == 0) null else "dictionary-" + row % 11
      }
      withResource(ColumnVector.fromLongs(ids: _*)) { idColumn =>
        withResource(ColumnVector.fromStrings(text: _*)) { textColumn =>
          withResource(new Table(idColumn, textColumn)) { table =>
            withResource(new ParquetBufferConsumer(rows)) { encoded =>
              val options = ParquetWriterOptions.builder().withNonNullableColumns("id")
                .withNullableColumns("text").withCompressionType(compression)
                .withRowGroupSizeRows(1024).withRowGroupSizeBytes(8192).build()
              withResource(Table.writeParquetChunked(options, encoded)) { writer =>
                writer.write(table)
              }
              // Completed callbacks are no longer pinned by the writer or this test.
              assert(SpillFramework.stores.hostStore.spill(Long.MaxValue) > 0)
              val records = encoded.records.toVector
              assert(SpillFramework.stores.hostStore.numHandles == handles)
              assert(records.size > 1)
              assert(records.map(_.numRows.toLong).sum == rows)
              withResource(records.safeMap(record =>
                  ParquetCachedBatchFiles.stage(record, schema))) { staged =>
                // Exercise rematerialization of the input records as well as writer regions.
                assert(SpillFramework.stores.hostStore.spill(Long.MaxValue) > 0)
                withResource(ParquetCachedBatchFiles.logicalFile(staged)) { logical =>
                  assert(logical.numRows == rows)
                  withResource(Table.readParquet(ParquetOptions.DEFAULT, logical.buffers: _*)) {
                    result => assert(check(result, 0, projected = false) == rows)
                  }
                  val projection = ParquetOptions.builder().includeColumn("text").build()
                  withResource(new CudfParquetChunkedReader(
                      64 * 1024, 0, projection, logical.buffers: _*)) { reader =>
                    var nextRow = 0
                    var chunks = 0
                    while (reader.hasNext) {
                      withResource(reader.readChunk()) { result =>
                        nextRow = check(result, nextRow, projected = true)
                      }
                      chunks += 1
                    }
                    assert(nextRow == rows)
                    assert(chunks > 1)
                  }
                  logical.close()
                }
                // logicalFile borrows these handles rather than retiring the staged records.
                staged.foreach { file =>
                  withResource(file.getHostBuffer()) { buffer =>
                    assert(buffer.getLength == file.info.encodedBytes)
                  }
                }
              }
            }
          }
        }
      }
      assert(SpillFramework.stores.hostStore.numHandles == handles)
    }
  }

  test("a later-parent retry releases earlier logical-file references before retrying") {
    withGpuSparkSession { _ =>
      val initialHandles = SpillFramework.stores.hostStore.numHandles
      val record = withResource(new Table.TestBuilder()
          .column(Array[java.lang.Long](0L, 1L, 2L, 3L): _*)
          .column("a", null, "b", "c").build()) { table =>
        withResource(new ParquetBufferConsumer(4)) { encoded =>
          val options = ParquetWriterOptions.builder().withNonNullableColumns("id")
            .withNullableColumns("text").withCompressionType(CompressionType.SNAPPY).build()
          withResource(Table.writeParquetChunked(options, encoded))(_.write(table))
          val records = encoded.records.toVector
          assert(records.size == 1)
          records.head
        }
      }
      withResource(Vector.fill(2)(record).safeMap(ParquetCachedBatchFiles.stage(_, schema))) {
        staged =>
        var firstParent: HostMemoryBuffer = null
        var firstAttempts = 0
        var injected = false
        var releasedBeforeRetry = false
        RmmSpark.currentThreadIsDedicatedToTask(1)
        try {
          val materialize: ParquetCachedBatchFiles.StagedFile => HostMemoryBuffer = file => {
            if (file eq staged.head) {
              firstAttempts += 1
              if (firstAttempts > 1) {
                // Only the staged handle owns its resident buffer between attempts.
                assert(firstParent.getRefCount == 1)
                releasedBeforeRetry = true
              }
              val parent = file.getHostBuffer()
              firstParent = parent
              parent
            } else if (!injected) {
              assert(firstParent.getRefCount > 1, "The first parent must already be borrowed")
              injected = true
              throw new CpuRetryOOM("later logical-file parent")
            } else {
              file.getHostBuffer()
            }
          }
          withResource(ParquetCachedBatchFiles.logicalFile(staged, materialize)) { logical =>
            assert(injected && releasedBeforeRetry && firstAttempts == 2)
            withResource(Table.readParquet(ParquetOptions.DEFAULT, logical.buffers: _*)) { table =>
              assert(table.getRowCount == 8)
              withResource(table.getColumn(0).copyToHost()) { ids =>
                assert((0 until 8).map(ids.getLong(_)) == Seq(0L, 1L, 2L, 3L, 0L, 1L, 2L, 3L))
              }
              withResource(table.getColumn(1).copyToHost()) { text =>
                val expected = Seq("a", null, "b", "c", "a", null, "b", "c")
                expected.indices.foreach { index =>
                  assert(text.isNull(index) == (expected(index) == null))
                  if (expected(index) != null) assert(text.getJavaString(index) == expected(index))
                }
              }
            }
          }
          assert(firstParent.getRefCount == 1)
        } finally {
          RmmSpark.removeAllCurrentThreadAssociation()
        }
      }
      assert(SpillFramework.stores.hostStore.numHandles == initialHandles)
    }
  }

}
