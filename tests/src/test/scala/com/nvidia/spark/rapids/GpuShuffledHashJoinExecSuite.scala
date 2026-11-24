/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}

import scala.collection.JavaConverters._

import ai.rapids.cudf.{ColumnVector, HostMemoryBuffer, JCudfSerialization, Rmm, Table}
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.jni.{GpuRetryOOM, RmmSpark}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.rapids.execution.HashOuterJoinIterator
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Tests for shuffled hash join preparation and finalization. */
class GpuShuffledHashJoinExecSuite extends AnyFunSuite with MockitoSugar {
  private val metricMap = mock[Map[String, GpuMetric]]
  when(metricMap(any())).thenReturn(NoopMetric)

  // The test table size is 20 (= 4 * 5) bytes
  private val TARGET_SIZE_SMALL = 10L
  private val TARGET_SIZE_BIG = 1024L
  private val attrs = Array(AttributeReference("a1", IntegerType, nullable=false)())
  private val buildGoals = Seq(RequireSingleBatch, RequireSingleBatchWithFilter(GpuLiteral(true)))

  test("final outer join batch closes the released tracker if checkpoint fails") {
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      val built = mock[LazySpillableColumnarBatch]
      when(built.dataTypes).thenReturn(Array[DataType](IntegerType))
      val failure = new IllegalStateException("injected checkpoint failure")
      doThrow(failure).when(built).checkpoint()
      val tracker = mock[SpillableColumnarBatch]
      val joinOptions = new RapidsConf(new SparkConf()).getJoinOptions(TARGET_SIZE_BIG)
      withResource(new HashOuterJoinIterator(
          FullOuter, built, Seq.empty, None, Some(tracker), Iterator.empty,
          Seq.empty, attrs, None, joinOptions, GpuBuildRight,
          false, None, NoopMetric, NoopMetric)) { iter =>
        assert(intercept[IllegalStateException](iter.hasNext) eq failure)
        verify(tracker, times(1)).close()
      }
    }
  }

  test("final outer join batch closes the filtered table if iterator cleanup fails") {
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      val baseline = Rmm.getTotalBytesAllocated
      withResource(newOneIntColumnTable()) { table =>
        withResource(GpuColumnVector.from(table, Array[DataType](IntegerType))) { builtBatch =>
          val built = mock[LazySpillableColumnarBatch]
          when(built.dataTypes).thenReturn(Array[DataType](IntegerType))
          when(built.getBatch).thenReturn(builtBatch)
          val failure = new IllegalStateException("injected iterator cleanup failure")
          doThrow(failure).when(built).close()
          val tracker = withResource(ColumnVector.fromBooleans(true, false, true, false, true)) {
            mask =>
              SpillableColumnarBatch(
                new ColumnarBatch(Array(GpuColumnVector.from(mask.incRefCount(), BooleanType)), 5),
                SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          }
          val joinOptions = new RapidsConf(new SparkConf()).getJoinOptions(TARGET_SIZE_BIG)
          withResource(new HashOuterJoinIterator(
              FullOuter, built, Seq.empty, None, Some(tracker), Iterator.empty,
              Seq.empty, attrs, None, joinOptions, GpuBuildRight,
              false, None, NoopMetric, NoopMetric)) { iter =>
            assert(intercept[IllegalStateException](iter.hasNext) eq failure)
          }
        }
      }
      assert(Rmm.getTotalBytesAllocated == baseline)
    }
  }

  Seq(GpuBuildLeft, GpuBuildRight).foreach { buildSide =>
    test(s"final outer join batch retries filtering for $buildSide") {
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        val baseline = Rmm.getTotalBytesAllocated
        withResource(newOneIntColumnTable()) { table =>
          withResource(GpuColumnVector.from(table, Array[DataType](IntegerType))) { builtBatch =>
            val built = mock[LazySpillableColumnarBatch]
            when(built.dataTypes).thenReturn(Array[DataType](IntegerType))
            when(built.getBatch).thenReturn(builtBatch)
            val tracker = mock[SpillableColumnarBatch]
            val trackerBatch = withResource(
                ColumnVector.fromBooleans(true, false, true, false, true)) { mask =>
              new ColumnarBatch(Array(GpuColumnVector.from(mask.incRefCount(), BooleanType)), 5)
            }
            try {
              when(tracker.getColumnarBatch()).thenThrow(new GpuRetryOOM("final batch retry"))
                .thenReturn(trackerBatch)
              val joinOptions = new RapidsConf(new SparkConf()).getJoinOptions(TARGET_SIZE_BIG)
              withResource(new HashOuterJoinIterator(
                  FullOuter, built, Seq.empty, None, Some(tracker), Iterator.empty,
                  Seq.empty, attrs, None, joinOptions, buildSide,
                  false, None, NoopMetric, NoopMetric)) { iter =>
                assert(iter.hasNext)
                withResource(iter.next()) { batch =>
                  assert(batch.numRows() == 3)
                  val buildOrdinal = if (buildSide == GpuBuildLeft) 0 else 1
                  withResource(batch.column(buildOrdinal).asInstanceOf[GpuColumnVector]
                      .getBase.copyToHost()) { values =>
                    assert((0 until 3).map(i => values.getInt(i)) == Seq(1, 3, 5))
                  }
                  assert(batch.column(1 - buildOrdinal).numNulls() == 3)
                }
                assert(!iter.hasNext)
                verify(tracker, times(2)).getColumnarBatch()
                verify(tracker, times(1)).close()
                verify(built, times(1)).checkpoint()
                verify(built, times(1)).restore()
                verify(built, times(1)).allowSpilling()
              }
            } finally {
              val trackerCalls = mockingDetails(tracker).getInvocations.asScala
                .count(_.getMethod.getName == "getColumnarBatch")
              if (trackerCalls < 2) {
                trackerBatch.close()
              }
            }
          }
        }
        assert(Rmm.getTotalBytesAllocated == baseline)
      }
    }

    test(s"final outer join batch retries null column allocation for $buildSide") {
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        val baseline = Rmm.getTotalBytesAllocated
        val taskId = 1L
        RmmSpark.currentThreadIsDedicatedToTask(taskId)
        RmmSpark.getAndResetNumRetryThrow(taskId)
        try {
          withResource(newOneIntColumnTable()) { table =>
            withResource(GpuColumnVector.from(table, Array[DataType](IntegerType))) { builtBatch =>
              val built = mock[LazySpillableColumnarBatch]
              when(built.dataTypes).thenReturn(Array[DataType](IntegerType))
              when(built.getBatch).thenReturn(builtBatch)
              // Iterator cleanup follows filtering, so the next allocation belongs to the
              // second retry block rather than the tracker/filter retry block.
              doAnswer { _ =>
                RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
                  RmmSpark.OomInjectionType.GPU.ordinal, 0)
                null
              }.when(built).close()
              val tracker = withResource(
                  ColumnVector.fromBooleans(true, false, true, false, true)) { mask =>
                SpillableColumnarBatch(
                  new ColumnarBatch(
                    Array(GpuColumnVector.from(mask.incRefCount(), BooleanType)), 5),
                  SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
              }
              val joinOptions = new RapidsConf(new SparkConf()).getJoinOptions(TARGET_SIZE_BIG)
              withResource(new HashOuterJoinIterator(
                  FullOuter, built, Seq.empty, None, Some(tracker), Iterator.empty,
                  Seq.empty, attrs, None, joinOptions, buildSide,
                  false, None, NoopMetric, NoopMetric)) { iter =>
                assert(iter.hasNext)
                withResource(iter.next()) { batch =>
                  assert(batch.numRows() == 3)
                  val buildOrdinal = if (buildSide == GpuBuildLeft) 0 else 1
                  withResource(batch.column(buildOrdinal).asInstanceOf[GpuColumnVector]
                      .getBase.copyToHost()) { values =>
                    assert((0 until 3).map(i => values.getInt(i)) == Seq(1, 3, 5))
                  }
                  assert(batch.column(1 - buildOrdinal).numNulls() == 3)
                }
                assert(!iter.hasNext)
                assert(RmmSpark.getAndResetNumRetryThrow(taskId) == 1)
                verify(built, times(1)).close()
              }
            }
          }
        } finally {
          RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 0)
          RmmSpark.removeAllCurrentThreadAssociation()
        }
        assert(Rmm.getTotalBytesAllocated == baseline)
      }
    }
  }

  private def newOneIntColumnTable(): Table = {
    withResource(ColumnVector.fromInts(1, 2, 3, 4, 5)) { cudfCol =>
      new Table(cudfCol)
    }
  }

  private def testJoinPreparation(
      buildIter: Iterator[ColumnarBatch],
      buildGoal: CoalesceSizeGoal,
      buildAttrs: Seq[Attribute] = attrs,
      targetSize: Long = TARGET_SIZE_BIG,
      optimalCase: Boolean = false)
      (verifyBuiltData: Either[ColumnarBatch, Iterator[ColumnarBatch]] => Unit): Unit = {
    val mockStreamIter = mock[Iterator[ColumnarBatch]]
    when(mockStreamIter.hasNext).thenReturn(true)
    val conf = new RapidsConf(new SparkConf())
    val (builtData, _) = GpuShuffledHashJoinExec.prepareBuildBatchesForJoin(
      buildIter,
      mockStreamIter,
      targetSize,
      buildAttrs,
      buildGoal, None, metricMap, CoalesceReadOption(conf))

    verifyBuiltData(builtData)
    // build iterator should be drained
    assertResult(expected = false)(buildIter.hasNext)
    if (optimalCase) {
      verify(mockStreamIter, times(1)).hasNext
      verify(mockStreamIter, times(1)).next
    } else {
      verify(mockStreamIter, times(0)).hasNext
      verify(mockStreamIter, times(0)).next
    }
  }

  private def assertBatchColsAndRowsAndClose(batch: ColumnarBatch,
      expectedNumCols: Int, expectedNumRows: Int): Unit = {
    withResource(batch) { _ =>
      assertResult(expectedNumCols)(batch.numCols())
      assertResult(expectedNumRows)(batch.numRows())
    }
  }

  test("test empty build iterator") {
    buildGoals.foreach { goal =>
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        testJoinPreparation(Iterator.empty, goal) { builtData =>
          assert(builtData.isLeft)
          // we get an empty batch
          assertBatchColsAndRowsAndClose(builtData.left.get, 1, 0)
        }
      }
    }
  }

  test("test a batch of 0 cols and 0 rows") {
    buildGoals.foreach { goal =>
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        val buildIter = Iterator(GpuColumnVector.emptyBatchFromTypes(Array.empty))
        testJoinPreparation(buildIter, goal, Seq.empty) { builtData =>
          assert(builtData.isLeft)
          assertBatchColsAndRowsAndClose(builtData.left.get, 0, 0)
        }
      }
    }
  }

  test("test a batch of 1 col and 0 rows") {
    buildGoals.foreach { goal =>
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        val buildIter = Iterator(GpuColumnVector.emptyBatchFromTypes(attrs.map(_.dataType)))
        testJoinPreparation(buildIter, goal) { builtData =>
          assert(builtData.isLeft)
          assertBatchColsAndRowsAndClose(builtData.left.get, 1, 0)

        }
      }
    }
  }

  test("test a nonempty batch going over the limit") {
    buildGoals.foreach { goal =>
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        val buildIter = withResource(newOneIntColumnTable()) { testTable =>
          Iterator(GpuColumnVector.from(testTable, attrs.map(_.dataType)))
        }
        testJoinPreparation(buildIter, goal, targetSize = TARGET_SIZE_SMALL) { builtData =>
          assert(builtData.isRight)
          var batchCount = 0
          val builtIt = builtData.right.get
          builtIt.foreach { builtBatch =>
            batchCount += 1
            assertBatchColsAndRowsAndClose(builtBatch, 1, 5)
          }
          assert(batchCount == 1)
        }
      }
    }
  }

  test("test two batches going over the limit") {
    buildGoals.foreach { goal =>
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        val buildIter = withResource(newOneIntColumnTable()) { testTable =>
          closeOnExcept(GpuColumnVector.from(testTable, attrs.map(_.dataType))) { batch1 =>
            Iterator(batch1, GpuColumnVector.from(testTable, attrs.map(_.dataType)))
          }
        }
        testJoinPreparation(buildIter, goal, targetSize = TARGET_SIZE_SMALL) { builtData =>
          assert(builtData.isRight)
          var batchCount = 0
          val builtIt = builtData.right.get
          builtIt.foreach { builtBatch =>
            batchCount += 1
            assertBatchColsAndRowsAndClose(builtBatch, 1, 5)
          }
          assert(batchCount == 2)
        }
      }
    }
  }

  private def getSerializedBatch(tbl: Table): ColumnarBatch = {
    val outStream = new ByteArrayOutputStream()
    JCudfSerialization.writeToStream(tbl, outStream, 0, tbl.getRowCount)
    val dIn = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray))
    val header = new JCudfSerialization.SerializedTableHeader(dIn)
    closeOnExcept(HostMemoryBuffer.allocate(header.getDataLen, false)) { hostBuffer =>
      JCudfSerialization.readTableIntoBuffer(dIn, header, hostBuffer)
      SerializedTableColumn.from(header, hostBuffer)
    }
  }

  private def getSerializedBatch(numRows: Int): ColumnarBatch = {
    val outStream = new ByteArrayOutputStream()
    JCudfSerialization.writeRowsToStream(outStream, numRows)
    val dIn = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray))
    val header = new JCudfSerialization.SerializedTableHeader(dIn)
    closeOnExcept(HostMemoryBuffer.allocate(header.getDataLen, false)) { hostBuffer =>
      JCudfSerialization.readTableIntoBuffer(dIn, header, hostBuffer)
      SerializedTableColumn.from(header, hostBuffer)
    }
  }

  test("test a 0-column serialized batch, optimal case") {
    buildGoals.foreach { goal =>
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        val buildIter = Iterator(getSerializedBatch(5))
        testJoinPreparation(buildIter, goal, Seq.empty, optimalCase = true) { builtData =>
          assert(builtData.isLeft)
          assertBatchColsAndRowsAndClose(builtData.left.get, 0, 5)
        }
      }
    }
  }

  test("test a serialized batch, optimal case") {
    buildGoals.foreach { goal =>
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        val buildIter = withResource(newOneIntColumnTable()) { tbl =>
          Iterator(getSerializedBatch(tbl))
        }
        testJoinPreparation(buildIter, goal, optimalCase = true) { builtData =>
          assert(builtData.isLeft)
          assertBatchColsAndRowsAndClose(builtData.left.get, 1, 5)
        }
      }
    }
  }

  test("test two serialized batches, going over the limit") {
    buildGoals.foreach { goal =>
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        val buildIter = withResource(newOneIntColumnTable()) { tbl =>
          closeOnExcept(getSerializedBatch(tbl)) { serializedBatch1 =>
            Iterator(serializedBatch1, getSerializedBatch(tbl))
          }
        }
        testJoinPreparation(buildIter, goal, targetSize = TARGET_SIZE_SMALL) { builtData =>
          assert(builtData.isRight)
          var batchCount = 0
          val builtIt = builtData.right.get
          builtIt.foreach { builtBatch =>
            batchCount += 1
            assertBatchColsAndRowsAndClose(builtBatch, 1, 5)
          }
          assert(batchCount == 2)
        }
      }
    }
  }

  test("test two serialized batches, stating within the limit, optimal case") {
    buildGoals.foreach { goal =>
      TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
        val buildIter = withResource(newOneIntColumnTable()) { tbl =>
          closeOnExcept(getSerializedBatch(tbl)) { serializedBatch1 =>
            Iterator(serializedBatch1, getSerializedBatch(tbl))
          }
        }
        testJoinPreparation(buildIter, goal, optimalCase = true) { builtData =>
          assert(builtData.isLeft)
          assertBatchColsAndRowsAndClose(builtData.left.get, 1, 10)
        }
      }
    }
  }
}
