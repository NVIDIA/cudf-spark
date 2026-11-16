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

package org.apache.spark.sql.rapids.execution

import java.io.ObjectInputStream
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import ai.rapids.cudf.{JCudfSerialization, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsConf.ShuffleKudoMode
import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM
import com.nvidia.spark.rapids.shims.GpuHashPartitioning

import org.apache.spark.{HashPartitioner, MapOutputTrackerMaster, ShuffleDependency, SparkConf, SparkEnv, SparkException, TaskContext}
import org.apache.spark.rdd.{DeterministicLevel, MapPartitionsRDD, RDD}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.execution.metric.{SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.LongAccumulator

object GpuRoundRobinShuffleSuite {
  @volatile var reducerCompleted = new CountDownLatch(1)
  @volatile var resultBlocked = new CountDownLatch(1)
  @volatile var releaseResult = new CountDownLatch(1)
  @volatile var roundRobinOutputLost = new CountDownLatch(1)
  val beforeFirstTakeResult = new AtomicReference[() => Unit](() => ())

  def await(latch: CountDownLatch): Unit = {
    require(latch.await(30, TimeUnit.SECONDS), "Timed out waiting for shuffle test handshake")
  }

  def holdResult(): Unit = {
    if (Thread.currentThread().getName.contains("task-result-getter")) {
      beforeFirstTakeResult.getAndSet(() => ()).apply()
      resultBlocked.countDown()
      await(releaseResult)
    }
  }

  private def fetchFailure(
      shuffleId: Int,
      mapIndex: Int,
      reduceIndex: Int,
      message: String): FetchFailedException = {
    new FetchFailedException(SparkEnv.get.blockManager.blockManagerId,
      shuffleId, 0L, mapIndex, reduceIndex, message, null)
  }

  private def loseOutput(shuffleId: Int): Unit = {
    SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      .unregisterAllMapAndMergeOutput(shuffleId)
  }

  private def splitOnce(
      injections: LongAccumulator,
      shouldSplit: () => Boolean = () => true): GpuRoundRobinPartitioning = {
    new GpuRoundRobinPartitioning(4) {
      private var injected = false

      override def columnarEvalAny(batch: ColumnarBatch): Any = {
        if (!injected && shouldSplit()) {
          injected = true
          injections.add(1)
          withResource(batch) { _ => throw new GpuSplitAndRetryOOM("injected round-robin split") }
        } else {
          super.columnarEvalAny(batch)
        }
      }
    }
  }
}

// Block Java result decoding on the driver until the scheduler has retried the map stage.
class RoundRobinShuffleResult(val rows: Array[Long]) extends Serializable {
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    GpuRoundRobinShuffleSuite.holdResult()
  }
}

class GpuRoundRobinShuffleSuite extends SparkQueryCompareTestSuite {
  import GpuRoundRobinShuffleSuite.{fetchFailure, loseOutput, splitOnce}

  private def conf(sortBefore: Boolean): SparkConf = new SparkConf()
    .set("spark.sql.execution.sortBeforeRepartition", sortBefore.toString)

  private def dependency(
      spark: SparkSession,
      batchSizes: Seq[Int],
      partitioning: GpuPartitioning = GpuRoundRobinPartitioning(4),
      inputLevel: DeterministicLevel.Value = DeterministicLevel.DETERMINATE,
      trackOpTime: Boolean = false): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val input = new MapPartitionsRDD[ColumnarBatch, Int](
      spark.sparkContext.parallelize(0 until 8, 8), (_, index, _) => {
        if (index != 7) {
          Iterator.empty
        } else {
          var start = 0
          batchSizes.iterator.map { size =>
            val ids = (start until start + size).map(Int.box)
            start += size
            withResource(new Table.TestBuilder().column(ids: _*).build()) { table =>
              GpuColumnVector.from(table, Array(IntegerType))
            }
          }
        }
      }) {
      override protected def getOutputDeterministicLevel: DeterministicLevel.Value = inputLevel
    }
    dependency(spark, input, partitioning, trackOpTime)
  }

  private def dependency(
      spark: SparkSession,
      input: RDD[ColumnarBatch],
      partitioning: GpuPartitioning,
      trackOpTime: Boolean): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val metrics = Map[String, GpuMetric]().withDefaultValue(NoopMetric)
    GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(input,
      Seq(AttributeReference("id", IntegerType, nullable = false)()), partitioning,
      Array(IntegerType),
      new GpuColumnarBatchSerializer(metrics, Array(IntegerType), ShuffleKudoMode.CPU,
        useKudo = false, kudoMeasureBufferCopy = false),
      useGPUShuffle = false, useMultiThreadedShuffle = false,
      rangeInputBatchingEnabled = false, metrics,
      SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(spark.sparkContext), Map.empty,
      opTimeNewShuffleWrite = if (trackOpTime) Some(NoopMetric) else None)
  }

  private def assignments(
      dep: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch]): Array[(Int, Int)] = {
    dep.rdd.sparkContext.runJob(dep.rdd,
      (iter: Iterator[Product2[Int, ColumnarBatch]]) => iter.flatMap { pair =>
        val batch = pair._2
        // The exchange iterator owns these host slices and closes them when it advances.
        (0 until batch.numRows()).map(i => (batch.column(0).getInt(i), pair._1))
      }.toArray, Seq(7)).flatten
  }

  private def shuffledRows(
      spark: SparkSession,
      dep: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch]): RDD[Long] = {
    val metrics = SQLShuffleReadMetricsReporter.createShuffleReadMetrics(spark.sparkContext) ++
      Seq(GpuMetric.NUM_PARTITIONS, GpuMetric.PARTITION_SIZE).map { name =>
        name -> SQLMetrics.createMetric(spark.sparkContext, name)
      }
    new ShuffledBatchRDD(dep, metrics).mapPartitions { batches =>
      batches.flatMap { batch =>
        withResource(batch) { _ =>
          val column = batch.column(0).asInstanceOf[SerializedTableColumn]
          withResource(JCudfSerialization.unpackHostColumnVectors(
            column.header, column.hostBuffer)) { columns =>
            (0 until batch.numRows()).map(i => columns(0).getInt(i).toLong)
          }
        }
      }
    }
  }

  Seq(false, true).foreach { sortBefore =>
    test(s"round-robin batch cuts require indeterminate shuffle output, sort=$sortBefore") {
      withGpuSparkSession({ spark =>
        GpuShuffleEnv.init(new RapidsConf(spark.sparkContext.getConf))
        val first = dependency(spark, Seq(10))
        // With four reducers, a five-row cut restarts the next batch at a different reducer
        // than continuing the original batch would: 5 is not divisible by 4.
        val retry = dependency(spark, Seq(5, 5))
        val firstRows = assignments(first)
        val retryRows = assignments(retry)
        val expected = (0 until 10).toArray
        assert(firstRows.map(_._1).sorted.toSeq === expected.toSeq)
        assert(retryRows.map(_._1).sorted.toSeq === expected.toSeq)

        // Keeping one completed reducer while rerunning the others can lose or duplicate rows.
        val mixed = firstRows.filter(_._2 == 0) ++ retryRows.filter(_._2 != 0)
        assert(mixed.map(_._1).sorted.toSeq != expected.toSeq)
        assert(first.rdd.outputDeterministicLevel === DeterministicLevel.INDETERMINATE)
        assert(retry.rdd.outputDeterministicLevel === DeterministicLevel.INDETERMINATE)
      }, conf(sortBefore))
    }

    test(s"round-robin fetch failure cannot reuse a completed reducer, sort=$sortBefore") {
      withGpuSparkSession({ spark =>
        val df = spark.range(0, 1000, 1, 8)
          .repartition(4, col("id") % 4).repartition(4)
        val exchange = df.queryExecution.executedPlan.collectFirst {
          case e: GpuShuffleExchangeExecBase
              if e.outputPartitioning.isInstanceOf[RoundRobinPartitioning] => e
        }.getOrElse(fail("GPU round-robin exchange not found"))
        val shuffleId = exchange.shuffleDependencyColumnar.shuffleId
        val reducerCompleted = new CountDownLatch(1)
        GpuRoundRobinShuffleSuite.reducerCompleted = reducerCompleted
        val result = df.queryExecution.toRdd.mapPartitionsWithIndex { (index, rows) =>
          if (index == 1 && TaskContext.get().stageAttemptNumber() == 0) {
            // The driver result handler runs after DAGScheduler records reducer 0 as finished.
            GpuRoundRobinShuffleSuite.await(GpuRoundRobinShuffleSuite.reducerCompleted)
            throw fetchFailure(shuffleId, 0, index, "injected round-robin fetch failure")
          }
          rows.map(_.getLong(0))
        }
        try {
          val error = intercept[SparkException] {
            spark.sparkContext.runJob(result, (rows: Iterator[Long]) => rows.toArray,
              (index: Int, _: Array[Long]) => {
                if (index == 0) {
                  reducerCompleted.countDown()
                }
              })
          }
          assert(error.getMessage.contains(
            "A shuffle map stage with indeterminate output was failed and retried"))
        } finally {
          reducerCompleted.countDown()
        }
      }, conf(sortBefore))
    }
  }

  test("round-robin map stage can retry after its own input fetch fails") {
    Seq(false, true).foreach { sortBefore =>
      withGpuSparkSession({ spark =>
        val upstream = spark.sparkContext.parallelize(0 until 8, 8)
          .map(i => (i, i)).partitionBy(new HashPartitioner(8))
        val upstreamId = upstream.dependencies.head
          .asInstanceOf[ShuffleDependency[Int, Int, Int]].shuffleId
        val input = new MapPartitionsRDD[ColumnarBatch, (Int, Int)](upstream,
          (_, index, keys) => {
            keys.foreach(_ => ())
            if (index != 7) {
              Iterator.empty
            } else {
              if (TaskContext.get().stageAttemptNumber() == 0) {
                // No reader of the round-robin shuffle has started, so a rerun is safe.
                throw fetchFailure(upstreamId, index, index, "injected input fetch failure")
              }
              Iterator.single(withResource(new Table.TestBuilder()
                  .column((0 until 1002).map(Int.box): _*).build()) { table =>
                GpuColumnVector.from(table, Array(IntegerType))
              })
            }
          })
        val dep = dependency(spark, input, GpuRoundRobinPartitioning(4), trackOpTime = true)
        val rows = shuffledRows(spark, dep)
        // A serializability check must not mark a reader as submitted or retain a prior snapshot.
        SparkEnv.get.closureSerializer.newInstance().serialize(rows)
        assert(rows.collect().sorted.toSeq === (0L until 1002L))
        // A second action can still reuse intact output from the same dependency.
        SparkEnv.get.closureSerializer.newInstance().serialize(rows)
        assert(rows.collect().sorted.toSeq === (0L until 1002L))
      }, conf(sortBefore))
    }
  }

  test("round-robin map-only jobs can rerun before any reader is submitted") {
    withGpuSparkSession({ spark =>
      GpuShuffleEnv.init(new RapidsConf(spark.sparkContext.getConf))
      val dep = dependency(spark, Seq(1002))
      for (_ <- 0 until 2) {
        scala.concurrent.Await.result(spark.sparkContext.submitMapStage(dep),
          scala.concurrent.duration.Duration.Inf)
        loseOutput(dep.shuffleId)
      }
      val rows = shuffledRows(spark, dep)
      assert(rows.collect().sorted.toSeq === (0L until 1002L))
      loseOutput(dep.shuffleId)
      // A completed job does not prove that its caller has discarded the old results.
      val error = intercept[SparkException] { rows.collect() }
      assert(error.getMessage.contains("Cannot retry a GPU round-robin shuffle"))
    }, conf(sortBefore = false))
  }

  test("round-robin take cannot mix partial results across jobs") {
    val callbacks = new AtomicInteger()
    def takeWithLostOutput(rows: RDD[Long], shuffleId: Int,
        beforeRerun: () => Unit): Array[Long] = {
      GpuRoundRobinShuffleSuite.releaseResult = new CountDownLatch(0)
      GpuRoundRobinShuffleSuite.beforeFirstTakeResult.set(() => {
        callbacks.incrementAndGet()
        loseOutput(shuffleId)
        beforeRerun()
      })
      try {
        rows.map(v => new RoundRobinShuffleResult(Array(v))).take(2000).flatMap(_.rows)
      } finally {
        GpuRoundRobinShuffleSuite.beforeFirstTakeResult.set(() => ())
      }
    }
    withCpuSparkSession({ spark =>
      val df = spark.range(0, 1002, 1, 8).repartition(4)
      val exchange = df.queryExecution.executedPlan.collectFirst {
        case e: org.apache.spark.sql.execution.exchange.ShuffleExchangeExec => e
      }.getOrElse(fail("CPU round-robin exchange not found"))
      val rows = df.queryExecution.toRdd.map(_.getLong(0))
      assert(takeWithLostOutput(rows, exchange.shuffleDependency.shuffleId, () => ())
        .sorted.toSeq === (0L until 1002L))
      assert(callbacks.get() === 1)
    }, conf(sortBefore = false))
    withGpuSparkSession({ spark =>
      GpuShuffleEnv.init(new RapidsConf(spark.sparkContext.getConf))
      var splitLater = false
      val injections = spark.sparkContext.longAccumulator("take cross-job split")
      val partitioning = splitOnce(injections, () => splitLater)
      val dep = dependency(spark, Seq(1002), partitioning)
      val rows = shuffledRows(spark, dep)
      val error = intercept[SparkException] {
        takeWithLostOutput(rows, dep.shuffleId, () => splitLater = true)
      }
      assert(error.getMessage.contains("Cannot retry a GPU round-robin shuffle"))
      assert(callbacks.get() === 2)
      assert(injections.value === 0L)
    }, conf(sortBefore = false))
  }

  for (intermediateShuffle <- Seq(false, true); reuseShuffle <- Seq(false, true)) {
    test(s"round-robin delayed results, intermediate=$intermediateShuffle, reuse=$reuseShuffle") {
      withGpuSparkSession({ spark =>
        GpuShuffleEnv.init(new RapidsConf(spark.sparkContext.getConf))
        val injections = spark.sparkContext.longAccumulator("retried round-robin split")
        var injectOnFirstAttempt = false
        val partitioning = splitOnce(injections,
          () => TaskContext.get().stageAttemptNumber() > 0 || injectOnFirstAttempt)
        // A split produces 501 + 501 rows; 501 is not divisible by the four reducers, so
        // restarting round-robin on the second half would change the row assignments.
        val dep = dependency(spark, Seq(1002), partitioning)
        val direct = shuffledRows(spark, dep)
        // Hash by the original round-robin partition, so a changed batch cut remains visible
        // after the intermediate shuffle instead of restoring stable partitions by row ID.
        val shuffled = if (intermediateShuffle) {
          direct.mapPartitionsWithIndex { (index, rows) =>
            rows.map(v => (index, v))
          }.partitionBy(new HashPartitioner(4)).values
        } else {
          direct
        }
        if (reuseShuffle) {
          assert(shuffled.collect().sorted.toSeq === (0L until 1002L))
          injectOnFirstAttempt = true
        }
        val shuffleId = dep.shuffleId
        val failedShuffleId = if (intermediateShuffle) {
          shuffled.dependencies.head.rdd.dependencies.head
            .asInstanceOf[ShuffleDependency[Int, Long, Long]].shuffleId
        } else {
          shuffleId
        }
        val mapRddId = dep.rdd.id
        GpuRoundRobinShuffleSuite.resultBlocked = new CountDownLatch(1)
        val releaseResult = new CountDownLatch(1)
        GpuRoundRobinShuffleSuite.releaseResult = releaseResult
        val roundRobinOutputLost = new CountDownLatch(1)
        GpuRoundRobinShuffleSuite.roundRobinOutputLost = roundRobinOutputLost
        val listener = new SparkListener {
          override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
            if (intermediateShuffle && event.stageInfo.rddInfos.exists(_.id == shuffled.id)) {
              // Model loss of both shuffles on one executor: the result can still read the
              // intermediate output, but retrying its failed fetch also needs the lost RR map.
              loseOutput(dep.shuffleId)
              roundRobinOutputLost.countDown()
            }
            if (GpuRoundRobinShuffleSuite.resultBlocked.getCount == 0 &&
                event.stageInfo.rddInfos.exists(_.id == mapRddId)) {
              // Keep reducer 0 unrecorded until the scheduler has decided to retry the map stage.
              releaseResult.countDown()
            }
          }
        }
        spark.sparkContext.addSparkListener(listener)
        try {
          val result = shuffled.mapPartitionsWithIndex { (index, rows) =>
            if (index == 1 && TaskContext.get().stageAttemptNumber() == 0) {
              GpuRoundRobinShuffleSuite.await(GpuRoundRobinShuffleSuite.resultBlocked)
              if (intermediateShuffle) {
                GpuRoundRobinShuffleSuite.await(GpuRoundRobinShuffleSuite.roundRobinOutputLost)
              }
              throw fetchFailure(failedShuffleId, if (intermediateShuffle) index else 7, index,
                "fetch failure before driver accepts reducer 0")
            }
            rows
          }
          val collected = new Array[Array[Long]](result.getNumPartitions)
          val error = intercept[SparkException] {
            spark.sparkContext.runJob(result,
              (rows: Iterator[Long]) => new RoundRobinShuffleResult(rows.toArray),
              (index: Int, value: RoundRobinShuffleResult) => collected(index) = value.rows)
            // If a regression allows completion, still report any missing or duplicated IDs.
            assert(collected.flatten.sorted.toSeq === (0L until 1002L))
          }
          assert(Seq("Cannot retry a GPU round-robin shuffle",
            "Task with indeterminate results from old attempt succeeded")
            .exists(error.getMessage.contains))
          assert(injections.value === 0L)
        } finally {
          releaseResult.countDown()
          roundRobinOutputLost.countDown()
          spark.sparkContext.removeSparkListener(listener)
        }
      }, conf(sortBefore = false))
    }
  }

  test("hash and single-partition exchanges can still retry after a fetch failure") {
    withGpuSparkSession({ spark =>
      GpuShuffleEnv.init(new RapidsConf(spark.sparkContext.getConf))
      val partitionings = Seq(GpuRoundRobinPartitioning(1), GpuSinglePartitioning,
        GpuHashPartitioning(Seq(GpuLiteral(1, IntegerType)), 4))
      partitionings.foreach { partitioning =>
        val dep = dependency(spark, Seq(1002), partitioning)
        val shuffleId = dep.shuffleId
        val retries = spark.sparkContext.longAccumulator("successful shuffle retries")
        val result = shuffledRows(spark, dep).mapPartitionsWithIndex { (index, rows) =>
          if (index == 0 && TaskContext.get().stageAttemptNumber() == 0) {
            throw fetchFailure(shuffleId, 7, index, "injected determinate shuffle fetch failure")
          }
          if (TaskContext.get().stageAttemptNumber() > 0) {
            retries.add(1)
          }
          rows
        }
        assert(result.collect().sorted.toSeq === (0L until 1002L))
        assert(retries.value > 0)
      }
    }, conf(sortBefore = false))
  }

  test("split-and-retry changes round-robin assignments for determinate input") {
    withGpuSparkSession({ spark =>
      GpuShuffleEnv.init(new RapidsConf(spark.sparkContext.getConf))
      val injections = spark.sparkContext.longAccumulator("round-robin split injection")
      val partitioning = splitOnce(injections)
      val first = assignments(dependency(spark, Seq(10)))
      val retry = dependency(spark, Seq(10), partitioning)
      val retried = assignments(retry)
      assert(injections.value === 1L)
      assert(retried.map(_._1).sorted.toSeq === (0 until 10))
      assert(retried.toMap != first.toMap)
      assert(retry.rdd.outputDeterministicLevel === DeterministicLevel.INDETERMINATE)
    }, conf(sortBefore = false))
  }

  test("exchange determinism survives operation-time tracking and all input levels") {
    Seq(false, true).foreach { sortBefore =>
      withGpuSparkSession({ spark =>
        Seq(DeterministicLevel.DETERMINATE, DeterministicLevel.UNORDERED,
          DeterministicLevel.INDETERMINATE).foreach { level =>
          Seq(false, true).foreach { trackOpTime =>
            val partitionings = Seq(GpuRoundRobinPartitioning(4), GpuRoundRobinPartitioning(1),
              GpuSinglePartitioning, GpuHashPartitioning(Seq(GpuLiteral(1, IntegerType)), 4))
            partitionings.foreach { partitioning =>
              val dep = dependency(spark, Seq(10), partitioning, level, trackOpTime)
              val expected = if (partitioning == GpuRoundRobinPartitioning(4)) {
                DeterministicLevel.INDETERMINATE
              } else {
                level
              }
              assert(dep.rdd.outputDeterministicLevel === expected)
            }
          }
        }
      }, conf(sortBefore))
    }
  }
}
