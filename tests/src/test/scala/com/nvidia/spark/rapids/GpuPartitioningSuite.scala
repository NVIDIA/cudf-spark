/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import java.math.RoundingMode

import ai.rapids.cudf.{ColumnVector, Cuda, DefaultHostMemoryAllocator, DType, HostMemoryAllocator,
  HostMemoryBuffer, Scalar, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsConf.ShuffleKudoMode
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shims.GpuHashPartitioning
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Murmur3Hash, Pmod}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning}
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.{GpuShuffleExchangeExecBase, TrampolineUtil}
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, IntegerType, LongType,
  StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuPartitioningSuite extends AnyFunSuite with BeforeAndAfterEach {
  var rapidsConf = new RapidsConf(Map[String, String]())

  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1, 1, 1, 1, 1, 1, 1)
        .column("five", "two", null, null, "one", "one", "one", "one", "one", "one")
        .column(5.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        .decimal64Column(-3, RoundingMode.UNNECESSARY ,
          5.1, null, 3.3, 4.4e2, 0, -2.1e-1, 1.111, 2.345, null, 1.23e3)
        .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType, DoubleType,
        DecimalType(DType.DECIMAL64_MAX_PRECISION, 3)))
    }
  }

  /**
   * Retrieves the underlying column vectors for a batch. It increments the reference counts for
   * them if needed so the results need to be closed.
   */
  private def extractColumnVectors(batch: ColumnarBatch): Array[ColumnVector] = {
    if (TestUtils.isBatchPacked(batch)) {
      val packedColumn = batch.column(0).asInstanceOf[GpuPackedTableColumn]
      val table = packedColumn.getContiguousTable.getTable
      // The contiguous table is still responsible for closing these columns.
      (0 until table.getNumberOfColumns).map(i => table.getColumn(i).incRefCount()).toArray
    } else if (GpuCompressedColumnVector.isBatchCompressed(batch)) {
      val compressedColumn = batch.column(0).asInstanceOf[GpuCompressedColumnVector]
      val descr = compressedColumn.getTableMeta.bufferMeta.codecBufferDescrs(0)
      val codec = TableCompressionCodec.getCodec(
        descr.codec, TableCompressionCodec.makeCodecConfig(rapidsConf))
      withResource(codec.createBatchDecompressor(100 * 1024 * 1024L,
        Cuda.DEFAULT_STREAM)) { decompressor =>
        compressedColumn.getTableBuffer.incRefCount()
        decompressor.addBufferToDecompress(compressedColumn.getTableBuffer,
          compressedColumn.getTableMeta.bufferMeta)
        withResource(decompressor.finishAsync()) { outputBuffers =>
          val outputBuffer = outputBuffers.head
          // There should be only one
          withResource(
            MetaUtils.getTableFromMeta(outputBuffer, compressedColumn.getTableMeta)) { table =>
            (0 until table.getNumberOfColumns).map(i => table.getColumn(i).incRefCount()).toArray
          }
        }
      }
    } else {
      GpuColumnVector.extractBases(batch).map(_.incRefCount())
    }
  }

  private def buildSubBatch(batch: ColumnarBatch, startRow: Int, endRow: Int): ColumnarBatch = {
    withResource(extractColumnVectors(batch)) { columns =>
      val types = GpuColumnVector.extractTypes(batch)
      val sliced = columns.zip(types).map { case (c, t) =>
        GpuColumnVector.from(c.subVector(startRow, endRow), t)
      }
      new ColumnarBatch(sliced.toArray, endRow - startRow)
    }
  }

  private def compareBatches(expected: ColumnarBatch, actual: ColumnarBatch): Unit = {
    assertResult(expected.numRows)(actual.numRows)
    withResource(extractColumnVectors(expected)) { expectedColumns =>
      withResource(extractColumnVectors(actual)) { actualColumns =>
        assertResult(expectedColumns.length)(actualColumns.length)
        expectedColumns.zip(actualColumns).foreach { case (expected, actual) =>
          if (expected.getRowCount == 0) {
            assertResult(expected.getType)(actual.getType)
          } else {
            withResource(expected.equalToNullAware(actual)) { compareVector =>
              withResource(compareVector.all()) { compareResult =>
                assert(compareResult.getBoolean)
              }
            }
          }
        }
      }
    }
  }

  test("GPU partition") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf().set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val partitionIndices = Array(0, 2, 2)
      val gp = new GpuPartitioning {
        override val numPartitions: Int = partitionIndices.length
      }
      withResource(buildBatch()) { batch =>
        // `sliceInternalOnGpuAndClose` will close the batch, but in this test we want to
        // reuse it
        GpuColumnVector.incRefCounts(batch)
        val columns = GpuColumnVector.extractColumns(batch)
        val numRows = batch.numRows
        withResource(
            gp.sliceInternalOnGpuAndClose(numRows, partitionIndices, columns)) { partitions =>
          partitions.zipWithIndex.foreach { case (partBatch, partIndex) =>
            val startRow = partitionIndices(partIndex)
            val endRow = if (partIndex < partitionIndices.length - 1) {
              partitionIndices(partIndex + 1)
            } else {
              batch.numRows
            }
            val expectedRows = endRow - startRow
            assertResult(expectedRows)(partBatch.numRows)
            assert(TestUtils.isBatchPacked(partBatch))
            withResource(buildSubBatch(batch, startRow, endRow)) { expectedBatch =>
              compareBatches(expectedBatch, partBatch)
            }
          }
        }
      }
    }
  }

  test("GPU partition with lz4 compression") {
    testGpuPartitionWithCompression("lz4")
  }

  test("GPU partition with zstd compression") {
    testGpuPartitionWithCompression("zstd")
  }

  test("GPU partition with copy compression") {
    testGpuPartitionWithCompression("copy")
  }

  test("GPU kudo partitioning with serialization") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
        .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
        .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val partitionIndices = Array(0, 2, 2)
      val gp = new GpuPartitioning {
        override val numPartitions: Int = partitionIndices.length
      }
      withResource(buildBatch()) { batch =>
        GpuColumnVector.incRefCounts(batch)
        val columns = GpuColumnVector.extractColumns(batch)
        val numRows = batch.numRows
        withResource(gp.sliceInternalGpuOrCpuAndClose(
          numRows, partitionIndices, columns).map(_._1)) { partitions =>
          assertResult(2)(partitions(0).numRows())
          assertResult(8)(partitions(1).numRows())
        }
      }
    }
  }

  test("GPU kudo partitioning rejects oversized serialized batch") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
        .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
        .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val partitionIndices = Array(0, 2, 2)
      val gp = new GpuPartitioning {
        override val numPartitions: Int = partitionIndices.length
        // Force the oversized-batch guard to trip with a 10-byte limit.
        override protected[rapids] def maxGpuSerializedSliceBytes: Long = 10L
      }
      withResource(buildBatch()) { batch =>
        GpuColumnVector.incRefCounts(batch)
        val columns = GpuColumnVector.extractColumns(batch)
        val numRows = batch.numRows
        val ex = intercept[IllegalArgumentException] {
          gp.sliceInternalGpuOrCpuAndClose(numRows, partitionIndices, columns)
        }
        assert(ex.getMessage.contains("exceeding the"))
        assert(ex.getMessage.contains("reduce spark.rapids.sql.batchSizeBytes"))
      }
    }
  }

  /** Counts the host allocations made on the creating thread while `metric` is not timing. */
  private class TimerProbingHostAllocator(delegate: HostMemoryAllocator, metric: GpuMetric)
      extends HostMemoryAllocator {
    private val owner = Thread.currentThread()
    var allocations = 0
    var untimedAllocations = 0

    override def allocate(amount: Long, preferPinned: Boolean): HostMemoryBuffer = {
      probe()
      delegate.allocate(amount, preferPinned)
    }

    override def allocate(amount: Long): HostMemoryBuffer = {
      probe()
      delegate.allocate(amount)
    }

    private def probe(): Unit = {
      if (Thread.currentThread() eq owner) {
        allocations += 1
        // A timer that is already running refuses to activate. An idle one activates, so it is
        // stopped again at once, adding no time.
        if (metric.tryActivateTimer(Seq.empty)) {
          metric.deactivateTimer(0L, Seq.empty)
          untimedAllocations += 1
        }
      }
    }
  }

  Seq("CPU", "GPU").foreach { writeMode =>
    test(s"$writeMode kudo write mode slicing counts host allocation in the copy time") {
      TrampolineUtil.cleanupAnyExistingSession()
      val conf = new SparkConf()
          .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
          .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, writeMode)
      TestUtils.withGpuSparkSession(conf) { _ =>
        GpuShuffleEnv.init(new RapidsConf(conf))
        val partitionIndices = Array(0, 2, 2)
        val gp = partitioning(partitionIndices.length)
        assert(!gp.usesGPUShuffle)
        assertResult(writeMode == "GPU")(gp.usesKudoGPUSlicing)
        val copyTime = new LocalGpuMetric
        gp.setupDebugMetrics(Map(GpuMetric.COPY_TO_HOST_TIME -> copyTime))
        withResource(buildBatch()) { batch =>
          GpuColumnVector.incRefCounts(batch)
          val columns = GpuColumnVector.extractColumns(batch)
          val previous = DefaultHostMemoryAllocator.get()
          val allocator = new TimerProbingHostAllocator(previous, copyTime)
          DefaultHostMemoryAllocator.set(allocator)
          val parts = try {
            gp.sliceInternalGpuOrCpuAndClose(batch.numRows, partitionIndices, columns)
          } finally {
            DefaultHostMemoryAllocator.set(previous)
          }
          withResource(parts.map(_._1)) { _ =>
            assert(allocator.allocations > 0)
            assertResult(0)(allocator.untimedAllocations)
            assert(copyTime.value > 0)
          }
        }
      }
    }
  }

  /** Slices a rows-only batch and returns (partition, rows, columns) for each slice. */
  private def sliceRowsOnly(gp: GpuPartitioning, numRows: Int,
      partitionIndices: Array[Int]): Seq[(Int, Int, Int)] = {
    val parts = gp.sliceInternalGpuOrCpuAndClose(numRows, partitionIndices,
      Array.empty[GpuColumnVector])
    withResource(parts.map(_._1)) { _ =>
      parts.map { case (batch, partIndex) => (partIndex, batch.numRows, batch.numCols) }.toSeq
    }
  }

  private def partitioning(n: Int): GpuPartitioning = new GpuPartitioning {
    override val numPartitions: Int = n
  }

  /** Rows-only slicing that must hold in every shuffle mode. */
  private def assertRowsOnlySlicing(): Unit = {
    // The empty partition gets no slice, as on the CPU path.
    assertResult(Seq((0, 3, 0), (2, 4, 0), (3, 3, 0)))(
      sliceRowsOnly(partitioning(4), 10, Array(0, 3, 3, 7)))
    // Leading and trailing empty partitions leave the other partitions' ids alone.
    assertResult(Seq((1, 4, 0), (2, 6, 0)))(
      sliceRowsOnly(partitioning(5), 10, Array(0, 0, 4, 10, 10)))
    assertResult(Seq((0, 5, 0)))(sliceRowsOnly(partitioning(1), 5, Array(0)))
    assertResult(Seq.empty)(sliceRowsOnly(partitioning(2), 0, Array(0, 0)))
  }

  private def cacheOnlyShuffleConf(): SparkConf = new SparkConf()
      .set("spark.shuffle.manager", GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS)
      .set("spark.rapids.shuffle.mode", RapidsConf.RapidsShuffleManagerMode.CACHE_ONLY.toString)

  test("GPU shuffle slices a rows-only batch by row count") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = cacheOnlyShuffleConf()
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val gp = partitioning(1)
      assert(gp.usesGPUShuffle)
      assert(!gp.usesKudoGPUSlicing)
      assertRowsOnlySlicing()
    }
  }

  test("GPU kudo slicing slices a rows-only batch by row count") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf().set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      assert(partitioning(1).usesKudoGPUSlicing)
      assertRowsOnlySlicing()
    }
  }

  test("rows-only range partitioning round-trips through the GPU shuffle") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = cacheOnlyShuffleConf().set("spark.sql.adaptive.enabled", "false")
    TestUtils.withGpuSparkSession(conf) { spark =>
      // The literal key folds into the partitioning, so the exchange's child has no columns.
      val df = spark.range(0, 1000, 1, 4).select(lit(1L).as("k"))
          .repartitionByRange(4, col("k")).agg(count(lit(1)))
      val rowsOnlyRangeExchanges = df.queryExecution.executedPlan.collect {
        case e: GpuShuffleExchangeExecBase
            if e.outputPartitioning.isInstanceOf[RangePartitioning] && e.child.output.isEmpty => e
      }
      assert(rowsOnlyRangeExchanges.nonEmpty, df.queryExecution.executedPlan)
      assertResult(1000L)(df.collect().head.getLong(0))
    }
  }

  /** Hash partitions a rows-only batch and returns (partition, rows, columns) for each slice. */
  private def hashPartitionRowsOnly(keys: Seq[Expression], numPartitions: Int,
      numRows: Int): Seq[(Int, Int, Int)] = {
    val parts = GpuHashPartitioning(keys, numPartitions)
        .columnarEvalAny(new ColumnarBatch(Array.empty, numRows))
        .asInstanceOf[Array[(ColumnarBatch, Int)]]
    withResource(parts.map(_._1)) { _ =>
      parts.map { case (batch, partIndex) => (partIndex, batch.numRows, batch.numCols) }.toSeq
    }
  }

  /** The partition the CPU sends a row to when every key is a literal. */
  private def cpuHashPartition(keys: Seq[Literal], numPartitions: Int): Int =
    Pmod(new Murmur3Hash(keys), Literal(numPartitions)).eval(InternalRow.empty).asInstanceOf[Int]

  test("hash partitioning sends every row of a rows-only batch to the CPU's partition") {
    val kudoGpuWriteConf = new SparkConf().set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
    Seq(new SparkConf(), cacheOnlyShuffleConf(), kudoGpuWriteConf).foreach { conf =>
      TrampolineUtil.cleanupAnyExistingSession()
      TestUtils.withGpuSparkSession(conf) { _ =>
        GpuShuffleEnv.init(new RapidsConf(conf))
        // Nil is a HashPartitioning without expressions, which hashes every row to the seed.
        Seq(Seq(Literal(1L)), Seq(Literal(1L), Literal("a")), Seq(Literal(null, IntegerType)),
            Nil).foreach { keys =>
          val gpuKeys = keys.map(k => GpuLiteral(k.value, k.dataType))
          Seq(1, 4, 200).foreach { n =>
            // The other partitions receive no rows, so they get no slice.
            assertResult(Seq((cpuHashPartition(keys, n), 1000, 0)), s"keys $keys, $n parts")(
              hashPartitionRowsOnly(gpuKeys, n, 1000))
          }
        }
        assertResult(Seq.empty)(hashPartitionRowsOnly(Seq(GpuLiteral(1L, LongType)), 4, 0))
      }
    }
  }

  /** A key that references no column and still differs from row to row: row i holds i. */
  private case class RowIndexKey() extends GpuLeafExpression {
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = false

    override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
      withResource(Scalar.fromInt(0)) { zero =>
        GpuColumnVector.from(ColumnVector.sequence(zero, batch.numRows), IntegerType)
      }
  }

  test("hash partitioning splits a rows-only batch by each row's partition") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val gp = GpuHashPartitioning(Seq(RowIndexKey()), 4)
      val numRows = 1000
      // The expected offsets: the same rows partitioned with one column.
      val withColumn = withResource(Scalar.fromInt(0)) { zero =>
        GpuColumnVector.from(ColumnVector.sequence(zero, numRows), IntegerType)
      }
      val (expected, columns) =
        gp.partitionInternalAndClose(new ColumnarBatch(Array(withColumn), numRows))
      withResource(columns) { _ =>
        val sizes = expected.indices.map { i =>
          (if (i + 1 < expected.length) expected(i + 1) else numRows) - expected(i)
        }
        assert(sizes.count(_ > 0) > 1, s"the key should spread the rows, got $sizes")
      }
      val (rowsOnly, rowsOnlyColumns) =
        gp.partitionInternalAndClose(new ColumnarBatch(Array.empty, numRows))
      assert(rowsOnlyColumns.isEmpty)
      assertResult(expected.toSeq)(rowsOnly.toSeq)
    }
  }

  test("rows-only hash partitioning splits and retries in the shuffle exchange") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
    TestUtils.withGpuSparkSession(conf) { spark =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val numRows = 1000
      val rdd = spark.sparkContext.parallelize(Seq(0), 1).mapPartitions { _ =>
        Iterator(new ColumnarBatch(Array.empty, numRows))
      }
      val metrics = Map[String, GpuMetric]().withDefaultValue(NoopMetric)
      val dependency = GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(rdd, Seq.empty,
        GpuHashPartitioning(Seq(GpuLiteral(1L, LongType)), 4), Array.empty,
        new GpuColumnarBatchSerializer(metrics, Array.empty, ShuffleKudoMode.CPU,
          useKudo = false, kudoMeasureBufferCopy = false),
        useGPUShuffle = false, useMultiThreadedShuffle = false,
        rangeInputBatchingEnabled = false, metrics, Map.empty, Map.empty,
        enableOpTimeTrackingRdd = false)
      RmmSpark.currentThreadIsDedicatedToTask(1)
      val parts = try {
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        dependency.rdd.partitions.toSeq.flatMap { p =>
          dependency.rdd.iterator(p, TaskContext.get())
              .map(pair => (pair._1, pair._2.numRows, pair._2.numCols)).toList
        }
      } finally {
        RmmSpark.removeAllCurrentThreadAssociation()
      }
      assert(RmmSpark.getAndResetNumSplitRetryThrow(1) > 0)
      val part = cpuHashPartition(Seq(Literal(1L)), 4)
      assertResult(Seq((part, numRows / 2, 0), (part, numRows / 2, 0)))(parts)
    }
  }

  test("rows-only hash partitioning round-trips through the GPU shuffle") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = cacheOnlyShuffleConf().set("spark.sql.adaptive.enabled", "false")
    TestUtils.withGpuSparkSession(conf) { spark =>
      // The literal key folds into the partitioning, so the exchange's child has no columns.
      val df = spark.range(0, 1000, 1, 4).select(lit(1L).as("k"))
          .repartition(4, col("k")).agg(count(lit(1)))
      val rowsOnlyHashExchanges = df.queryExecution.executedPlan.collect {
        case e: GpuShuffleExchangeExecBase
            if e.outputPartitioning.isInstanceOf[HashPartitioning] && e.child.output.isEmpty => e
      }
      assert(rowsOnlyHashExchanges.nonEmpty, df.queryExecution.executedPlan)
      assertResult(1000L)(df.collect().head.getLong(0))
    }
  }

  private def testGpuPartitionWithCompression(codecName: String): Unit = {
    val conf = new SparkConf()
        .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, codecName)
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val partitionIndices = Array(0, 2, 2)
      val gp = new GpuPartitioning {
        override val numPartitions: Int = partitionIndices.length
      }
      withResource(buildBatch()) { batch =>
        // `sliceInternalOnGpuAndClose` will close the batch, but in this test we want to
        // reuse it
        GpuColumnVector.incRefCounts(batch)
        val columns = GpuColumnVector.extractColumns(batch)
        val numRows = batch.numRows
        withResource(
          gp.sliceInternalOnGpuAndClose(numRows, partitionIndices, columns)) { partitions =>
          partitions.zipWithIndex.foreach { case (partBatch, partIndex) =>
            val startRow = partitionIndices(partIndex)
            val endRow = if (partIndex < partitionIndices.length - 1) {
              partitionIndices(partIndex + 1)
            } else {
              batch.numRows
            }
            val expectedRows = endRow - startRow
            assertResult(expectedRows)(partBatch.numRows)
            val columns = (0 until partBatch.numCols).map(i => partBatch.column(i))
            columns.foreach { column =>
              // batches with any rows should be compressed, and
              // batches with no rows should not be compressed.
              val actualRows = column match {
                case c: GpuCompressedColumnVector =>
                  val rows = c.getTableMeta.rowCount
                  assert(rows != 0)
                  rows
                case c: GpuPackedTableColumn =>
                  val rows = c.getContiguousTable.getRowCount
                  assert(rows == 0)
                  rows
                case _ =>
                  throw new IllegalStateException(
                    "column should either be compressed or packed")
              }
              assertResult(expectedRows)(actualRows)
            }
            if (GpuCompressedColumnVector.isBatchCompressed(partBatch)) {
              GpuCompressedColumnVector.incRefCounts(partBatch)
              val handle = SpillableColumnarBatch(partBatch, -1)
              withResource(buildSubBatch(batch, startRow, endRow)) { expectedBatch =>
                withResource(handle.getColumnarBatch()) { cb =>
                  compareBatches(expectedBatch, cb)
                }
              }
            }
          }
        }
      }
    }
  }
}
