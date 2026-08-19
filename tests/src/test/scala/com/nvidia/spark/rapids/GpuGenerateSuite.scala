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

import java.util

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector, Table}
import ai.rapids.cudf.HostColumnVector.{BasicType, ListType, StructType}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StringType, StructType => SparkStructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuGenerateSuite
  extends RmmSparkRetrySuiteBase
    with SparkQueryCompareTestSuite {
  val rapidsConf = new RapidsConf(Map.empty[String, String])

  test("GpuExplode/GpuPosExplode elementSchema for array and map children") {
    // array element -> "col"; map element -> "key"/"value"; PosExplode prepends "pos"
    val arr = AttributeReference("a", ArrayType(IntegerType))()
    val map = AttributeReference("m", MapType(IntegerType, StringType))()
    assertResult(new SparkStructType().add("col", IntegerType, nullable = true))(
      GpuExplode(arr).elementSchema)
    assertResult(new SparkStructType()
        .add("pos", IntegerType, nullable = false)
        .add("col", IntegerType, nullable = true))(
      GpuPosExplode(arr).elementSchema)
    assertResult(new SparkStructType()
        .add("key", IntegerType, nullable = false)
        .add("value", StringType, nullable = true))(
      GpuExplode(map).elementSchema)
    assertResult(new SparkStructType()
        .add("pos", IntegerType, nullable = false)
        .add("key", IntegerType, nullable = false)
        .add("value", StringType, nullable = true))(
      GpuPosExplode(map).elementSchema)
  }

  def makeListColumn(
      numRows: Int,
      listSize: Int,
      includeNulls: Boolean,
      allNulls: Boolean): ColumnVector = {
    val list = util.Arrays.asList((0 until listSize): _*)
    val rows = (0 until numRows).map { r =>
      if (allNulls || includeNulls && r % 2 == 0) {
        null
      } else {
        list
      }
    }
    ColumnVector.fromLists(
      new ListType(true,
        new BasicType(true, DType.INT32)),
      rows: _*)
  }

  def makeMapColumn(
      numRows: Int,
      mapSize: Int,
      includeNulls: Boolean,
      allNulls: Boolean): ColumnVector = {
    val structType: StructType = new HostColumnVector.StructType(
      true,
      new HostColumnVector.BasicType(true, DType.INT32),
      new HostColumnVector.BasicType(true, DType.INT32))

    val rows = (0 until numRows).map { r =>
      if (allNulls || includeNulls && r % 2 == 0) {
        null
      } else {
        (0 until mapSize).map { k =>
          if (allNulls || includeNulls && k % 2 == 0) {
            null
          } else {
            new HostColumnVector.StructData(Integer.valueOf(k), Integer.valueOf(k + 1))
          }
        }
      }.asJava
    }

    ColumnVector.fromLists(
      new HostColumnVector.ListType(true, structType),
      rows: _*)
  }

  def makeBatch(
      numRows: Int,
      includeRepeatColumn: Boolean = true,
      includeNulls: Boolean = false,
      listSize: Int = 4,
      allNulls: Boolean = false,
      carryAlongColumnCount: Int = 1): (ColumnarBatch, Long) = {
    var inputSize: Long = 0L
    withResource(makeListColumn(numRows, listSize, includeNulls, allNulls)) { cvList =>
      inputSize +=
        withResource(cvList.getChildColumnView(0)) {
          _.getDeviceMemorySize
        }
      val batch = if (includeRepeatColumn) {
        val carryAlongColumnTypes = (0 until carryAlongColumnCount).map(_ => IntegerType)
        val dt: Array[DataType] = (carryAlongColumnTypes ++ Seq(ArrayType(IntegerType))).toArray
        val carryAlongColumns = (0 until carryAlongColumnCount).safeMap { c =>
          val elements = (0 until numRows).map { x =>
            // + c to add some differences per columnn
            val i = Int.box(x + c)
            if (allNulls || includeNulls && i % 2 == 0) {
              null
            } else {
              i
            }
          }.toSeq
          ColumnVector.fromBoxedInts(elements:_*)
        }
        withResource(carryAlongColumns) { _ =>
          carryAlongColumns.foreach { rc =>
            inputSize += listSize * rc.getDeviceMemorySize
          }
          withResource(new Table((carryAlongColumns :+ cvList): _*)) { tbl =>
            GpuColumnVector.from(tbl, dt)
          }
        }
      } else {
        val dt: Array[DataType] = Seq(ArrayType(IntegerType)).toArray
        withResource(new Table(cvList)) { tbl =>
          GpuColumnVector.from(tbl, dt)
        }
      }
      (batch, inputSize)
    }
  }

  def makeMapBatch(
      numRows: Int,
      includeRepeatColumn: Boolean = true,
      includeNulls: Boolean = false,
      mapSize: Int = 4,
      allNulls: Boolean = false,
      carryAlongColumnCount: Int = 1): (ColumnarBatch, Long) = {
    var inputSize: Long = 0L
    withResource(makeMapColumn(numRows, mapSize, includeNulls, allNulls)) { cvList =>
      inputSize +=
          withResource(cvList.getChildColumnView(0)) {
            _.getDeviceMemorySize
          }
      val batch = if (includeRepeatColumn) {
        val carryAlongColumnTypes = (0 until carryAlongColumnCount).map(_ => IntegerType)
        val dt: Array[DataType] =
          (carryAlongColumnTypes ++ Seq(MapType(IntegerType, IntegerType))).toArray
        val carryAlongColumns = (0 until carryAlongColumnCount).safeMap { c =>
          val elements = (0 until numRows).map { x =>
            // + c to add some differences per columnn
            val i = Int.box(x + c)
            if (allNulls || includeNulls && i % 2 == 0) {
              null
            } else {
              i
            }
          }.toSeq
          ColumnVector.fromBoxedInts(elements: _*)
        }
        withResource(carryAlongColumns) { _ =>
          carryAlongColumns.foreach { rc =>
            inputSize += mapSize * rc.getDeviceMemorySize
          }
          withResource(new Table((carryAlongColumns :+ cvList): _*)) { tbl =>
            GpuColumnVector.from(tbl, dt)
          }
        }
      } else {
        val dt: Array[DataType] = Seq(MapType(IntegerType, IntegerType)).toArray
        withResource(new Table(cvList)) { tbl =>
          GpuColumnVector.from(tbl, dt)
        }
      }
      (batch, inputSize)
    }
  }

  def checkSplits(splits: Array[Int], batch: ColumnarBatch): Unit = {
    withResource(GpuColumnVector.from(batch)) { tbl =>
      var totalRows = 0L
      // because concatenate does not work with 1 Table and I can't incRefCount a Table
      // this is used to close the concatenated table, which would otherwise be leaked.
      withResource(new ArrayBuffer[Table]) { tableToClose =>
        withResource(tbl.contiguousSplit(splits: _*)) { splitted =>
          splitted.foreach { ct =>
            totalRows += ct.getRowCount
          }
          // `getTable` causes Table to be owned by the `ContiguousTable` class
          // so they get closed when the `ContiguousTable`s get closed.
          val concatted = if (splitted.length == 1) {
            splitted(0).getTable
          } else {
            val tbl = Table.concatenate(splitted.map(_.getTable): _*)
            tableToClose += tbl
            tbl
          }
          // Compare row by row the input vs the concatenated splits
          withResource(GpuColumnVector.from(batch)) { inputTbl =>
            assertResult(concatted.getRowCount)(batch.numRows())
            (0 until batch.numCols()).foreach { c =>
              withResource(concatted.getColumn(c).copyToHost()) { hostConcatCol =>
                withResource(inputTbl.getColumn(c).copyToHost()) { hostInputCol =>
                  (0 until batch.numRows()).foreach { r =>
                    if (hostInputCol.isNull(r)) {
                      assertResult(true)(hostConcatCol.isNull(r))
                    } else {
                      if (hostInputCol.getType == DType.LIST) {
                        // exploding column
                        compare(hostInputCol.getList(r), hostConcatCol.getList(r))
                      } else {
                        compare(hostInputCol.getInt(r), hostConcatCol.getInt(r))
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("all null inputs") {
    val (batch, _) = makeBatch(numRows = 100, allNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      assertResult(0)(
        e.inputSplitIndices(batch, 1, false, 4).length)

      // Here we are going to split 99 times, since our targetSize is 1 Byte and we are going to
      // produce 100 rows. The row byte count for a null row is going to be sizeof(type), and
      // because we use `outer=true` we are expecting 4 bytes x 100 rows.
      assertResult(99)(
        e.inputSplitIndices(batch, generatorOffset = 1 , true, 4).length)
    }
  }

  test("all null inputs with single exploding column (generatorOffset == 0)") {
    // Regression for #11653: when the batch contains only the exploding column
    // and every row is null, estimatedOutputSizeBytes == 0 so
    // numSplitsForTargetSize == 0. Before the fix this passed 0 to
    // GpuBatchUtils.generateSplitIndices, which fails `require(numSplits > 0)`.
    val (batch, _) = makeBatch(numRows = 100, includeRepeatColumn = false,
      allNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      val splits = e.inputSplitIndices(batch, generatorOffset = 0, false, 4)
      assertResult(0)(splits.length)
    }
  }

  test("0-row batches short-circuits to no splits") {
    val (batch, _) = makeBatch(numRows = 0)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      assertResult(0)(
        e.inputSplitIndices(batch, 1, false, 1).length)
      assertResult(0)(
        e.inputSplitIndices(batch, generatorOffset = 1 , true, 1).length)
    }
  }

  test("1-row batches short-circuits to no splits") {
    val (batch, _) = makeBatch(numRows = 1)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      var splits = e.inputSplitIndices(batch, 1, false, 1)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      splits = e.inputSplitIndices(batch, generatorOffset = 1 , true, 1)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)
    }
  }

  class SpillableColumnarBatchException(
      spillable: SpillableColumnarBatch,
      var forceOOM: Boolean) extends SpillableColumnarBatch {
    override def numRows(): Int = spillable.numRows()
    override def setSpillPriority(priority: Long): Unit = spillable.setSpillPriority(priority)
    override def getColumnarBatch(): ColumnarBatch = {
      if (forceOOM) {
        forceOOM = false
        throw new GpuSplitAndRetryOOM(s"mock split and retry")
      }
      spillable.getColumnarBatch
    }
    override def sizeInBytes: Long = spillable.sizeInBytes
    override def dataTypes: Array[DataType] = spillable.dataTypes
    override def close(): Unit = spillable.close()

    override def incRefCount(): SpillableColumnarBatch = {
      spillable.incRefCount()
      this
    }
  }

  trait TestGenerator extends GpuExplodeBase {
    private var forceNumOOMs: Int = 0

    def doForceSplitAndRetry(numOOMs: Int): Unit = {
      forceNumOOMs = numOOMs
    }

    private def getTestBatchIterator(
        inputBatches: Iterator[SpillableColumnarBatch]): Iterator[SpillableColumnarBatch] = {
      inputBatches.map { ib =>
        if (forceNumOOMs > 0) {
          forceNumOOMs -= 1
          new SpillableColumnarBatchException(ib, true);
        } else {
          ib
        }
      }
    }

    override def generate(
        inputBatches: Iterator[SpillableColumnarBatch],
        generatorOffset: Int,
        outer: Boolean): Iterator[ColumnarBatch] = {
      super.generate(getTestBatchIterator(inputBatches), generatorOffset, outer)
    }
  }

  class TestExplode(child: Expression) extends GpuExplode(child) with TestGenerator
  class TestPosExplode(child: Expression) extends GpuPosExplode(child) with TestGenerator

  def doGenerateWithSplitAndRetry(
      generate: GpuGenerator,
      failingGenerate: TestGenerator,
      makeBatchFn:
        (Int, Boolean) => (ColumnarBatch, Long) =
          makeBatch(_, includeRepeatColumn = true, _, carryAlongColumnCount=2),
      outer: Boolean = false,
      includeNulls: Boolean = false) = {
    // numRows = 1: exercises the split code trying to save a 1-row scenario where
    // we are running OOM.
    // numRows = 2: is the regular split code
    val generatorOffset = 2 // 0 and 1 are carry-along columns, 2 == collection to explode

    (1 until 3).foreach { numRows =>
      val (expected, _) = makeBatchFn(numRows, includeNulls)
      val itNoFailures = new GpuGenerateIterator(
        Iterator.single(Array(
          SpillableColumnarBatch(expected, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))),
        generator = generate,
        generatorOffset,
        outer,
        NoopMetric,
        NoopMetric,
        NoopMetric)
      val expectedExploded = itNoFailures.next()
      withResource(expectedExploded) { _ =>
        assertResult(false)(itNoFailures.hasNext)
        // with numRows > 1, we follow regular split semantics, else
        // we split the exploding column.
        // - numRows = 1, numOOMs = 1:
        //   2 batches each with 2 rows (4 items in the original array).
        // - numRows = 1, numOOMs = 2:
        //   3 batches (two 1-row batches, and the last batch has 2 rows).
        // - numRows = 2, numOOMs = 1:
        //   2 batches, each with 4 rows
        // - numRows = 2, numOOMs = 2:
        //   3 batches (2-row, 2-row and a 4-row)
        (1 until 3).foreach { numOOMs =>
          val (actual, _) = makeBatchFn(numRows, includeNulls)
          val actualSpillable =
            SpillableColumnarBatch(actual, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)

          val it = new GpuGenerateIterator(
            Iterator.single(Array(actualSpillable)),
            generator = failingGenerate,
            generatorOffset,
            outer,
            NoopMetric,
            NoopMetric,
            NoopMetric)

          failingGenerate.doForceSplitAndRetry(numOOMs)

          // this should return 2 batches, each with half the output
          val results = new ArrayBuffer[ColumnarBatch]()
          if (includeNulls && numRows == 1) {
            // if we have 1 row and we specified outer,
            // we are going to generate a null list/map row.
            // We cannot handle this because we cannot split a null, but this
            // tests our special case.
            assertThrows[GpuSplitAndRetryOOM] {
              closeOnExcept(results) { _ =>
                while (it.hasNext) {
                  results.append(it.next())
                }
              }
            }
          } else {
            closeOnExcept(results) { _ =>
              while (it.hasNext) {
                results.append(it.next())
              }
            }

            withResource(results) { _ =>
              withResource(results.map(GpuColumnVector.from)) { resultTbls =>
                // toSeq is required for scala 2.13 here
                withResource(Table.concatenate(resultTbls.toSeq: _*)) { res =>
                  withResource(GpuColumnVector.from(expectedExploded)) { expectedTbl =>
                    TestUtils.compareTables(expectedTbl, res)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("explode: splitAndRetry try to split by rows else split the exploding column") {
    Seq(false, true).foreach { includeNulls =>
      doGenerateWithSplitAndRetry(
        GpuExplode(AttributeReference("foo", ArrayType(IntegerType))()),
        new TestExplode(AttributeReference("foo", ArrayType(IntegerType))()),
        includeNulls = includeNulls
      )
    }
  }

  test("explode (outer): splitAndRetry try to split by rows else split the exploding column") {
    Seq(false, true).foreach { includeNulls =>
      doGenerateWithSplitAndRetry(
        GpuExplode(AttributeReference("foo", ArrayType(IntegerType))()),
        new TestExplode(AttributeReference("foo", ArrayType(IntegerType))()),
        outer = true,
        includeNulls = includeNulls
      )
    }
  }

  test("posexplode: splitAndRetry try split by rows else split the exploding column") {
    Seq(false, true).foreach { includeNulls =>
      doGenerateWithSplitAndRetry(
        GpuPosExplode(AttributeReference("foo", ArrayType(IntegerType))()),
        new TestPosExplode(AttributeReference("foo", ArrayType(IntegerType))()),
        includeNulls = includeNulls
      )
    }
  }

  test("posexplode (outer): splitAndRetry try split by rows else split the exploding column") {
    Seq(false, true).foreach { includeNulls =>
      doGenerateWithSplitAndRetry(
        GpuPosExplode(AttributeReference("foo", ArrayType(IntegerType))()),
        new TestPosExplode(AttributeReference("foo", ArrayType(IntegerType))()),
        outer = true,
        includeNulls = includeNulls
      )
    }
  }

  test("explode map: on splitAndRetry try split by rows else split the exploding column") {
    Seq(false, true).foreach { includeNulls =>
      doGenerateWithSplitAndRetry(
        GpuExplode(AttributeReference("foo", MapType(IntegerType, IntegerType))()),
        new TestExplode(AttributeReference("foo", MapType(IntegerType, IntegerType))()),
        makeBatchFn = makeMapBatch(_, includeRepeatColumn = true, _, carryAlongColumnCount=2),
        includeNulls = includeNulls
      )
    }
  }

  test("explode map (outer): splitAndRetry try split by rows else split the exploding column") {
    Seq(false, true).foreach { includeNulls =>
      doGenerateWithSplitAndRetry(
        GpuExplode(AttributeReference("foo", MapType(IntegerType, IntegerType))()),
        new TestExplode(AttributeReference("foo", MapType(IntegerType, IntegerType))()),
        makeBatchFn = makeMapBatch(_, includeRepeatColumn = true, _, carryAlongColumnCount=2),
        outer = true,
        includeNulls = includeNulls
      )
    }
  }

  test("posexplode map: splitAndRetry try split by rows else split the exploding column") {
    Seq(false, true).foreach { includeNulls =>
      doGenerateWithSplitAndRetry(
        GpuPosExplode(AttributeReference("foo", MapType(IntegerType, IntegerType))()),
        new TestPosExplode(AttributeReference("foo", MapType(IntegerType, IntegerType))()),
        makeBatchFn = makeMapBatch(_, includeRepeatColumn = true, _, carryAlongColumnCount=2),
        includeNulls = includeNulls
      )
    }
  }

  test("posexplode map (outer): splitAndRetry try split by rows else split the exploding column") {
    Seq(false, true).foreach { includeNulls =>
      doGenerateWithSplitAndRetry(
        GpuPosExplode(AttributeReference("foo", MapType(IntegerType, IntegerType))()),
        new TestPosExplode(AttributeReference("foo", MapType(IntegerType, IntegerType))()),
        makeBatchFn = makeMapBatch(_, includeRepeatColumn = true, _, carryAlongColumnCount=2),
        outer = true,
        includeNulls = includeNulls
      )
    }
  }

  test("2-row batches split in half") {
    val (batch, inputSize) = makeBatch(numRows = 2)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      val target = inputSize/2
      var splits = e.inputSplitIndices(batch, 1, false, target)
      assertResult(1)(splits.length)
      assertResult(1)(splits(0))
      checkSplits(splits, batch)

      splits = e.inputSplitIndices(batch, generatorOffset = 1 , true, target)
      assertResult(1)(splits.length)
      assertResult(1)(splits(0))
      checkSplits(splits, batch)
    }
  }

  test("8-row batch splits in half") {
    val (batch, inputSize) = makeBatch(numRows = 8)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      val target = inputSize/2
      // here a split at 4 actually means produce two Tables, each with 4 rows.
      var splits = e.inputSplitIndices(batch, 1, false, target)
      assertResult(1)(splits.length)
      assertResult(4)(splits(0))
      checkSplits(splits, batch)

      splits = e.inputSplitIndices(batch, generatorOffset = 1 , true, target)
      assertResult(1)(splits.length)
      assertResult(4)(splits(0))
      checkSplits(splits, batch)
    }
  }

  // these next four tests exercise code that just uses the exploding column's size as the limit
  test("test batch with a single exploding column") {
    val (batch, _) = makeBatch(numRows = 100, includeRepeatColumn = false)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      val targetSize = 1600
      // 1600 == a single split
      var splits = e.inputSplitIndices(batch, 0, false, targetSize)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      // 800 == 1 splits (2 parts) right down the middle
      splits = e.inputSplitIndices(batch, 0, false, targetSize/2)
      assertResult(1)(splits.length)
      assertResult(50)(splits(0))
      checkSplits(splits, batch)

      // 400 == 3 splits (4 parts)
      splits = e.inputSplitIndices(batch, 0, false, targetSize/4)
      assertResult(3)(splits.length)
      assertResult(25)(splits(0))
      assertResult(50)(splits(1))
      assertResult(75)(splits(2))
      checkSplits(splits, batch)
    }
  }

  test("test batch with a single exploding column with nulls") {
    val (batch, inputSize) = makeBatch(numRows=100, includeRepeatColumn=false, includeNulls=true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      // with nulls it should be 1/2 that
      val targetSize = inputSize
      // 800 = no splits
      var splits = e.inputSplitIndices(batch, 0, false, targetSize)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      // 400 == 1 splits (2 parts) right down the middle
      splits = e.inputSplitIndices(batch, 0, false, targetSize/2)
      assertResult(1)(splits.length)
      assertResult(50)(splits(0))
      checkSplits(splits, batch)

      // 200 == 8 parts
      splits = e.inputSplitIndices(batch, 0, false, targetSize/4)
      assertResult(3)(splits.length)
      assertResult(25)(splits(0))
      assertResult(50)(splits(1))
      assertResult(75)(splits(2))
      checkSplits(splits, batch)
    }
  }

  test("outer: test batch with a single exploding column with nulls") {
    // for outer, size is the same (nulls are assumed not to occupy any space, which is not true)
    // but number of rows is not the same.
    val (batch, inputSize) = makeBatch(numRows=100, includeRepeatColumn=false, includeNulls=true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      // with nulls it should be 1/2 that
      val targetSize = inputSize
      // 800 = no splits
      assertResult(0)(
        e.inputSplitIndices(batch, 0, true, targetSize).length)

      // 400 == 1 splits (2 parts) right down the middle
      var splits = e.inputSplitIndices(batch, 0, true, targetSize/2)
      assertResult(1)(splits.length)
      assertResult(50)(splits(0))

      // 200 == 3 splits (4 parts)
      splits = e.inputSplitIndices(batch, 0, true, targetSize/4)
      assertResult(3)(splits.length)
      assertResult(25)(splits(0))
      assertResult(50)(splits(1))
      assertResult(75)(splits(2))
    }
  }

  test("outer, limit rows: test batch with a single exploding column with nulls") {
    // for outer, size is the same (nulls are assumed not to occupy any space, which is not true)
    // but number of rows is not the same.
    val (batch, inputSize) = makeBatch(numRows=100, includeRepeatColumn=false, includeNulls=true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      // with nulls it should be 1/2 that
      // 250 rows is the expected number of rows in this case:
      //   200 non-null rows (4 reps * 50 non-null) +
      //   50 for rows that had nulls, since those are produced as well.
      // no-splits
      assertResult(0)(
        e.inputSplitIndices(batch, 0, true, inputSize, maxRows = 250).length)

      // 1 split (2 parts)
      var splits = e.inputSplitIndices(batch, 0, true, inputSize, maxRows = 125)
      assertResult(1)(splits.length)
      assertResult(50)(splits(0))

      // 3 splits (4 parts)
      splits = e.inputSplitIndices(batch, 0, true, inputSize, maxRows = 63)
      assertResult(3)(splits.length)
      assertResult(25)(splits(0))
      assertResult(50)(splits(1))
      assertResult(75)(splits(2))
    }
  }

  test("test batch with a repeating column") {
    val (batch, inputSize) = makeBatch(numRows=100)
    withResource(batch) { _ =>
      val e = GpuExplode(null)

      // no splits
      var targetSize = inputSize //3200 Bytes
      var splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      // 1600 == 1 splits (2 parts) right down the middle
      targetSize = inputSize / 2
      splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(1)(splits.length)
      assertResult(50)(splits(0))
      checkSplits(splits, batch)

      targetSize = inputSize / 4
      splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(3)(splits.length)
      assertResult(25)(splits(0))
      assertResult(50)(splits(1))
      assertResult(75)(splits(2))
      checkSplits(splits, batch)

      targetSize = inputSize / 8
      splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(7)(splits.length)
      assertResult(13)(splits(0))
      assertResult(25)(splits(1))
      assertResult(38)(splits(2))
      assertResult(50)(splits(3))
      assertResult(63)(splits(4))
      assertResult(75)(splits(5))
      assertResult(88)(splits(6))
      checkSplits(splits, batch)
    }
  }

  test("test batch with a repeating column with nulls") {
    val (batch, _) = makeBatch(numRows=100, includeNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      val inputSize = 1600
      var targetSize = inputSize
      var splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      // 800 == 1 splits (2 parts) right down the middle
      targetSize = inputSize / 2
      splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(1)(splits.length)
      checkSplits(splits, batch)

      // 400 == 3 splits (4 parts)
      targetSize = inputSize / 4
      splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(3)(splits.length)
      checkSplits(splits, batch)

      // we estimate 1600 bytes in this scenario (1000 for repeating col, and 800 for exploding)
      // in this case we use 32 bytes as the target size, so that is ceil(1800/32) => 57 splits.
      splits = e.inputSplitIndices(batch, 1, false, 32)
      assertResult(49)(splits.length)
      checkSplits(splits, batch)

      // in this case we use 16 bytes as the target size, resulting in 1600/8 > 100 splits,
      // which is more than the number of input rows, so we fallback to splitting at most to
      // input number of rows 100. Since the input is half nulls, then we get 50 splits.
      splits = e.inputSplitIndices(batch, 1, false, 8)
      assertResult(50)(splits.length)
      checkSplits(splits, batch)
    }
  }

  test("outer: test batch with a repeating column with nulls") {
    val (batch, inputSize) = makeBatch(numRows = 100, includeNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      // the repeating column is 4 bytes (or 400 bytes total) repeated 4 times (or 1600)
      // 1600 == two splits
      var targetSize = inputSize // 2656 Bytes
      var splits = e.inputSplitIndices(batch, 1, true, targetSize)
      checkSplits(splits, batch)

      // 1600 == 1 splits (2 parts) right down the middle
      targetSize = inputSize / 2
      splits = e.inputSplitIndices(batch, 1, true, targetSize)
      checkSplits(splits, batch)

      // 800 == 3 splits (4 parts)
      targetSize = inputSize / 4
      splits = e.inputSplitIndices(batch, 1, true, targetSize)
      checkSplits(splits, batch)

      // we estimate 1800 bytes in this scenario (1000 for repeating col, and 800 for exploding)
      // this is slightly more splits from the outer=false case, since we weren't counting nulls
      // then.
      splits = e.inputSplitIndices(batch, 1, true, 32)
      assertResult(55)(splits.length)
      checkSplits(splits, batch)

      // in this case we use 16 bytes as the target size, resulting in 1800/16 > 100 splits,
      // which is more than the number of input rows, so we fallback to splitting at most to
      // input number of rows 100. Since the input is half nulls, then we get 50 splits.
      splits = e.inputSplitIndices(batch, 1, true, 16)
      assertResult(50)(splits.length)
      checkSplits(splits, batch)
    }
  }

  test("outer: test 1000 row batch with a repeating column with nulls") {
    val (batch, _) = makeBatch(numRows = 10000, includeNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      // the repeating column is 4 bytes (or 400 bytes total) repeated 4 times (or 1600)
      // 1600 == two splits
      var splits = e.inputSplitIndices(batch, 1, true, 1600)
      checkSplits(splits, batch)

      // 1600 == 1 splits (2 parts) right down the middle
      splits = e.inputSplitIndices(batch, 1, true, 800)
      checkSplits(splits, batch)

      // 800 == 3 splits (4 parts)
      splits = e.inputSplitIndices(batch, 1, true, 400)
      checkSplits(splits, batch)

      splits = e.inputSplitIndices(batch, 1, true, 100)
      checkSplits(splits, batch)
    }
  }

  test("if the row limit produces more splits, prefer splitting using maxRows") {
    val (batch, inputSize) = makeBatch(numRows = 10000, includeNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // by size try to no splits, instead we should get 2 (by maxRows)
      // we expect 40000 rows (4x1000 given 4 items in the list per row), but we included nulls,
      // so this should return 20000 rows (given that this is not outer)
      var splits = e.inputSplitIndices(batch, 1, false, inputSize, maxRows = 20000)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      splits = e.inputSplitIndices(batch, 1, false, inputSize, maxRows = 10000)
      assertResult(1)(splits.length)
      assertResult(5000)(splits(0))
      checkSplits(splits, batch)
    }
  }

  test("outer: if the row limit produces more splits, prefer splitting using maxRows") {
    val (batch, inputSize) = makeBatch(numRows = 10000, includeNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // by size try to no splits, instead we should get 2 (by maxRows)
      // we expect 40000 rows (4x1000 given 4 items in the list per row)
      val splits = e.inputSplitIndices(batch, 1, true, inputSize, maxRows = 20000)
      assertResult(1)(splits.length)
      assertResult(5000)(splits(0))
      checkSplits(splits, batch)
    }
  }

  /**
   * Test that getSplitsWithRetryAndClose handles OOM by halving the target size.
   * This directly exercises the retry logic in the getSplits path.
   */
  test("getSplitsWithRetry handles OOM by reducing target size") {
    val generator = GpuExplode(AttributeReference("foo", ArrayType(IntegerType))())
    val generatorOffset = 1 // carry-along column is at 0, explode column at 1
    // Use a large target size that would normally not split each input batch.
    val largeTargetSize = 1024L * 1024 * 1024 // 1GB

    val (batch, _) = makeBatch(numRows = 100, listSize = 4, carryAlongColumnCount = 1)
    closeOnExcept(batch) { _ =>
      // inject a split-retry oom
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
    }
    // Test getSplitsWithRetryAndClose that should handle the OOM and retry
    val splits = GpuGenerateUtils.getSplitsWithRetryAndClose(batch, generator,
      generatorOffset, outer = false, largeTargetSize, NoopMetric)

    var count = 0
    while(splits.hasNext) {
      withResource(splits.next()) { scbs =>
        assert(scbs.length == 1) // Only one batch for each bundle
        assertResult(50)(scbs.head.numRows()) // each has 100/2 rows
      }
      count = count + 1
    }
    assert(count == 2) // iterator contains two batches due to split-retry
  }
}
