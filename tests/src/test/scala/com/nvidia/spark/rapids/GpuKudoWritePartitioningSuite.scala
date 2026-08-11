/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.collection.mutable

import ai.rapids.cudf.{HostColumnVector, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsConf.ShuffleKudoMode
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.jni.kudo.DumpOption
import com.nvidia.spark.rapids.shims.GpuHashPartitioning
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.{GpuShuffleExchangeExecBase, TrampolineUtil}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch


object GpuKudoWritePartitioningSuite {
  // Test data constants (10 rows per batch)
  val testIntValues: Array[java.lang.Integer] = Array(9,
    null.asInstanceOf[java.lang.Integer], 8, 7, 6, 5, 4, 3, 2, 1)
  val testStringValues: Array[String] = Array("nine", "eight", null, null, "six",
    "five", "four", "three", "two", "one")
  val dataTypes: Array[DataType] = Array(IntegerType, StringType)

  /**
   * Builds a ColumnarBatch using the test data
   * This is a standalone function to avoid serialization issues
   */
  def buildBatchInPartition(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
      .column(testIntValues(0), testIntValues(1), testIntValues(2),
        testIntValues(3), testIntValues(4), testIntValues(5),
        testIntValues(6), testIntValues(7), testIntValues(8),
        testIntValues(9))
      .column(testStringValues(0), testStringValues(1),
        testStringValues(2), testStringValues(3), testStringValues(4),
        testStringValues(5), testStringValues(6), testStringValues(7),
        testStringValues(8), testStringValues(9))
      .build()) { table =>
      GpuColumnVector.from(table, dataTypes)
    }
  }

  /**
   * Creates the expected data set for a single batch (10 rows)
   */
  def createExpectedBatchData(): Set[(Option[Integer], Option[String])] = {
    testIntValues.zip(testStringValues).map {
      case (intVal, stringVal) =>
        val intOpt = if (intVal == null) None else Option(intVal.asInstanceOf[Integer])
        (intOpt, Option(stringVal))
    }.toSet
  }

  /**
   * Extract all rows from a ColumnarBatch as tuples of (row index, integer value, string value)
   * This is a standalone function to avoid serialization issues
   */
  def extractRowsFromBatch(batch: ColumnarBatch): Seq[(Int, Option[Int], Option[String])] = {
    val rows = mutable.ArrayBuffer[(Int, Option[Int], Option[String])]()

    // Regular GpuColumnVector - extract directly
    val gpuVecs = GpuColumnVector.extractBases(batch)

    withResource(gpuVecs.safeMap(_.copyToHost())) { hostVecs =>
      val intCol = hostVecs(0).asInstanceOf[HostColumnVector]
      val stringCol = hostVecs(1).asInstanceOf[HostColumnVector]

      for (i <- 0 until batch.numRows()) {
        val intVal = if (intCol.isNull(i)) None else Some(intCol.getInt(i))
        val stringVal = if (stringCol.isNull(i)) None else Some(stringCol.getJavaString(i))
        rows.append((i, intVal, stringVal))
      }
    }
    rows.toSeq
  }

  /**
   * Deserialize SlicedSerializedColumnVector batches by serializing them to a stream,
   * then deserializing back and using the coalesce iterator to fully deserialize them.
   * This follows the same pattern as GpuShuffleCoalesceSuite.
   */
  def deserializeSlicedBatches(
      slicedBatches: Seq[ColumnarBatch],
      dataTypes: Array[DataType],
      serializer: GpuColumnarBatchSerializer): Seq[ColumnarBatch] = {
    // Serialize the sliced batches to a stream
    val byteOutputStream = new ByteArrayOutputStream()
    withResource(serializer.newInstance().serializeStream(byteOutputStream)) {
        serializationStream =>
      slicedBatches.foreach { batch =>
        serializationStream.writeKey(0)
        serializationStream.writeValue(batch)
      }
    }

    // Deserialize from the stream to get KudoSerializedTableColumn batches
    val byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray)
    val deserializationStream = serializer.newInstance().deserializeStream(byteInputStream)
    val kudoBatchesIter = deserializationStream.asKeyValueIterator.map(_._2)
      .asInstanceOf[Iterator[ColumnarBatch]]

    // Set up minimal metrics for the coalescing operation (required by API)
    val metricsMap = Map[String, GpuMetric]().withDefaultValue(NoopMetric)

    // Create coalesce read option for kudo GPU mode
    val readOption = com.nvidia.spark.rapids.CoalesceReadOption(
      kudoEnabled = true,
      kudoMode = ShuffleKudoMode.GPU,
      kudoDebugMode = DumpOption.Never,
      kudoDebugDumpPrefix = None,
      useAsync = false
    )

    // Use the coalesce iterator to fully deserialize the batches
    val coalesceIter =
      com.nvidia.spark.rapids.GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(
        kudoBatchesIter, Long.MaxValue, dataTypes, readOption, metricsMap)

    // Collect all deserialized batches
    coalesceIter.toSeq
  }

  /**
   * Deserialize a single SlicedSerializedColumnVector batch individually,
   * preserving its structure without coalescing with other batches.
   */
  def deserializeSingleSlicedBatch(
      slicedBatch: ColumnarBatch,
      dataTypes: Array[DataType],
      serializer: GpuColumnarBatchSerializer): Seq[ColumnarBatch] = {
    deserializeSlicedBatches(Seq(slicedBatch), dataTypes, serializer)
  }
}

class GpuKudoWritePartitioningSuite extends AnyFunSuite with BeforeAndAfterEach
    with TableDrivenPropertyChecks {
  var rapidsConf = new RapidsConf(Map[String, String]())

  override def beforeEach(): Unit = {
    super.beforeEach()
    TrampolineUtil.cleanupAnyExistingSession()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    TrampolineUtil.cleanupAnyExistingSession()
  }

  private val expectedTotalRows = 20 // 10 rows per batch × 2 batches

  /**
   * Creates a SparkConf configured for GPU Kudo write mode
   */
  private def createKudoSparkConf(): SparkConf = {
    new SparkConf()
      .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
      .set(RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.key, "true")
      .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
  }

  /**
   * Creates a GPU Kudo serializer for the test data types
   */
  private def createSerializer(): GpuColumnarBatchSerializer = {
    val serializerMetrics = Map[String, GpuMetric]().withDefaultValue(NoopMetric)
    new GpuColumnarBatchSerializer(serializerMetrics, GpuKudoWritePartitioningSuite.dataTypes,
      ShuffleKudoMode.GPU, useKudo = true, kudoMeasureBufferCopy = false)
  }

  /**
   * Creates an input RDD that produces test batches
   */
  private def createInputRDD(spark: org.apache.spark.sql.SparkSession):
      org.apache.spark.rdd.RDD[ColumnarBatch] = {
    spark.sparkContext.parallelize(Seq(0, 1), numSlices = 1)
      .mapPartitions { _ =>
        // Return 2 batches (one for each element in Seq(0, 1))
        Iterator(GpuKudoWritePartitioningSuite.buildBatchInPartition(),
          GpuKudoWritePartitioningSuite.buildBatchInPartition())
      }
  }

  /**
   * Sets up the shuffle dependency for testing
   */
  private def setupShuffleDependency(
      spark: org.apache.spark.sql.SparkSession,
      inputRDD: org.apache.spark.rdd.RDD[ColumnarBatch],
      serializer: GpuColumnarBatchSerializer,
      numPartitions: Int):
      org.apache.spark.ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val gpuPartitioning = GpuHashPartitioning(
      Seq(GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "key")),
      numPartitions)

    val outputAttributes = Seq(
      AttributeReference("id", IntegerType, nullable = true)(ExprId(0)),
      AttributeReference("name", StringType, nullable = true)(ExprId(1)))

    val writeMetrics = Map[String, SQLMetric]()
    val metrics = Map[String, GpuMetric]().withDefaultValue(NoopMetric)

    val dependency = GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(
      inputRDD,
      outputAttributes,
      gpuPartitioning,
      GpuKudoWritePartitioningSuite.dataTypes,
      serializer,
      useGPUShuffle = false,
      useMultiThreadedShuffle = false,
      metrics,
      writeMetrics,
      Map.empty,
      None,
      Seq.empty,
      enableOpTimeTrackingRdd = false)
    dependency
  }

  /**
   * Collects batches from the shuffle dependency, optionally injecting OOM for retry testing
   * Returns: (batches, totalRowsSeen)
   */
  private def collectBatches(
      dependency: org.apache.spark.ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
      injectOOM: Boolean): (mutable.ArrayBuffer[(Int, ColumnarBatch)], Long) = {
    val allPartitionedBatches = mutable.ArrayBuffer[(Int, ColumnarBatch)]()
    var totalRowsSeen = 0L
    var firstIteration = true

    if (injectOOM) {
      // Associate the current thread with a task (required for OOM injection)
      RmmSpark.currentThreadIsDedicatedToTask(1)
    }

    try {
      dependency.rdd.partitions.foreach { partition =>
        val partitionIterator = dependency.rdd.iterator(partition,
          org.apache.spark.TaskContext.get())

        // Inject a split OOM right before the first call to next() if requested
        if (injectOOM && firstIteration && partitionIterator.hasNext) {
          val threadId = RmmSpark.getCurrentThreadId
          RmmSpark.forceSplitAndRetryOOM(threadId, 1,
            RmmSpark.OomInjectionType.GPU.ordinal, 0)
          firstIteration = false
        }

        while (partitionIterator.hasNext) {
          val result = partitionIterator.next()
          val partitionId = result._1
          val batch = result._2

          // Increment ref counts before collecting (batches are managed by rddWithPartitionIds
          // and will be closed automatically, so we need to take ownership)
          if (batch.numCols() > 0 &&
              batch.column(0).isInstanceOf[SlicedSerializedColumnVector]) {
            SlicedSerializedColumnVector.incRefCount(batch)
          } else {
            GpuColumnVector.incRefCounts(batch)
          }
          allPartitionedBatches.append((partitionId, batch))
          totalRowsSeen += batch.numRows()
        }
      }
    } finally {
      if (injectOOM) {
        RmmSpark.removeAllCurrentThreadAssociation()
      }
    }

    (allPartitionedBatches, totalRowsSeen)
  }

  /**
   * Verifies that the collected batches match the expected test data.
   * Note: This method does NOT close the input batches - caller is responsible for cleanup.
   */
  private def verifyBatchContents(
      allPartitionedBatches: mutable.ArrayBuffer[(Int, ColumnarBatch)],
      totalRowsSeen: Long,
      serializer: GpuColumnarBatchSerializer): Unit = {
    // Verify total rows are preserved
    assert(totalRowsSeen == expectedTotalRows,
      s"Expected $expectedTotalRows total rows (10 per batch × 2 batches), got $totalRowsSeen")

    // Deserialize SlicedSerializedColumnVector batches
    val slicedBatches = allPartitionedBatches.map(_._2).toSeq
    val deserializedBatches = GpuKudoWritePartitioningSuite.deserializeSlicedBatches(
      slicedBatches, GpuKudoWritePartitioningSuite.dataTypes, serializer)

    try {
      // Extract row data from all deserialized batches
      val allPartitionedRows = deserializedBatches.flatMap { batch =>
        GpuKudoWritePartitioningSuite.extractRowsFromBatch(batch)
      }

      val partitionedDataValues = allPartitionedRows.map { case (_, intVal, stringVal) =>
        (intVal, stringVal)
      }.toSet

      // Create expected data set (twice since we have 2 batches)
      val expectedSingleBatchData = GpuKudoWritePartitioningSuite.createExpectedBatchData()
      val expectedDataValues = expectedSingleBatchData ++ expectedSingleBatchData

      // Verify deserialized result matches original
      assert(partitionedDataValues == expectedDataValues,
        s"Deserialized data doesn't match original. " +
        s"Partitioned: $partitionedDataValues, Expected: $expectedDataValues")
    } finally {
      deserializedBatches.foreach(_.close())
    }
  }

  /**
   * Extracts batch data from a sequence of sliced batches by deserializing each
   * individually and collecting the row data.
   * Returns: Sequence of batch data, where each element is the data from one sliced batch
   */
  private def extractBatchDataFromSlicedBatches(
      slicedBatches: Seq[ColumnarBatch],
      serializer: GpuColumnarBatchSerializer):
      Seq[Seq[(Option[Integer], Option[String])]] = {
    slicedBatches.map { slicedBatch =>
      val deserializedBatches =
        GpuKudoWritePartitioningSuite.deserializeSingleSlicedBatch(
          slicedBatch, GpuKudoWritePartitioningSuite.dataTypes, serializer)
      try {
        deserializedBatches.flatMap { batch =>
          GpuKudoWritePartitioningSuite.extractRowsFromBatch(batch)
        }.map { case (_, intVal, stringVal) =>
          // Convert Option[Int] to Option[Integer]
          (intVal.map(i => i.asInstanceOf[java.lang.Integer]), stringVal)
        }
      } finally {
        deserializedBatches.foreach(_.close())
      }
    }
  }

  /**
   * Verifies that the split retry worked correctly by comparing against
   * the baseline batches from a non-split run:
   * - One of the baseline batches was split into 2 batches
   * - The other baseline batch remains as 1 batch
   * - The split batches together contain exactly the rows from the original batch
   * - Split batches end up in the same partition as the original baseline batch
   * - No rows are duplicated or missing
   * Note: This method does NOT close the input batches - caller is responsible for cleanup.
   */
  private def verifySplitRetryStructure(
      baselineBatchData: Seq[(Int, Seq[(Option[Integer], Option[String])])],
      allPartitionedBatches: mutable.ArrayBuffer[(Int, ColumnarBatch)],
      serializer: GpuColumnarBatchSerializer): Unit = {
    // Deserialize each sliced batch individually to preserve structure
    val splitPartitionIds = allPartitionedBatches.map(_._1).toSeq
    val slicedBatches = allPartitionedBatches.map(_._2).toSeq
    val splitBatchDataSeq =
      extractBatchDataFromSlicedBatches(slicedBatches, serializer)
    val splitBatchData = splitPartitionIds.zip(splitBatchDataSeq)

    // Group batches by partition ID to handle interleaved splits across partitions
    val baselineByPartition = baselineBatchData.groupBy(_._1)
    val splitByPartition = splitBatchData.groupBy(_._1)

    // Verify we have the same partitions in both baseline and split
    assert(baselineByPartition.keySet == splitByPartition.keySet,
      s"Partitions should match. Baseline: ${baselineByPartition.keySet}, " +
      s"Split: ${splitByPartition.keySet}")

    var foundAnySplit = false

    // Process each partition separately
    baselineByPartition.foreach { case (partitionId, baselineBatchesForPartition) =>
      val splitBatchesForPartition = splitByPartition(partitionId)

      // Compare batches within this partition
      var baselineIdx = 0
      var splitIdx = 0
      var foundSplitInPartition = false

      while (baselineIdx < baselineBatchesForPartition.length &&
             splitIdx < splitBatchesForPartition.length) {
        val (_, baselineBatch) = baselineBatchesForPartition(baselineIdx)
        val (currentSplitPartitionId, currentSplitBatch) =
          splitBatchesForPartition(splitIdx)

        // Verify partition IDs match
        assert(currentSplitPartitionId == partitionId,
          s"Split batch partition ID should match. Expected: $partitionId, " +
          s"Got: $currentSplitPartitionId")

        // Determine if this is a split batch or a matching batch
        val isSplit = currentSplitBatch.length != baselineBatch.length
        val splitBatchToCompare = if (isSplit) {
          // This batch was split - find the second part in the same partition
          assert(splitIdx + 1 < splitBatchesForPartition.length,
            s"Split batch should be followed by another batch in partition " +
            s"$partitionId")
          assert(!foundSplitInPartition,
            s"Found multiple split batches in partition $partitionId, " +
            s"expected only one per partition")
          foundSplitInPartition = true
          foundAnySplit = true

          val (splitPartitionId2, splitBatch2) =
            splitBatchesForPartition(splitIdx + 1)

          // Verify both batches in the split are in the same partition
          assert(splitPartitionId2 == partitionId,
            s"Both parts of split batch should be in partition $partitionId. " +
            s"Part 2 is in partition $splitPartitionId2")

          // Verify both batches in the split are non-empty
          assert(currentSplitBatch.nonEmpty,
            "First part of split batch should be non-empty")
          assert(splitBatch2.nonEmpty,
            "Second part of split batch should be non-empty")

          // Verify no overlap between the two split batches
          val splitBatch1Set = currentSplitBatch.toSet
          val splitBatch2Set = splitBatch2.toSet
          val overlap = splitBatch1Set.intersect(splitBatch2Set)
          assert(overlap.isEmpty,
            "Batches that form the split should not overlap")

          currentSplitBatch ++ splitBatch2
        } else {
          currentSplitBatch
        }

        // Explicitly compare row values: verify against baseline
        val splitBatchSet = splitBatchToCompare.toSet
        val baselineSet = baselineBatch.toSet

        // Verify row counts match
        assert(splitBatchToCompare.length == baselineBatch.length,
          s"Split batch should have ${baselineBatch.length} rows " +
          s"(same as baseline), but got ${splitBatchToCompare.length}")

        // Verify every row in baseline appears in split batch
        val missingRows = baselineSet -- splitBatchSet
        assert(missingRows.isEmpty,
          s"Baseline batch has rows not found in split batch: $missingRows")

        // Verify no extra rows in split batch (every row in split appears in baseline)
        val extraRows = splitBatchSet -- baselineSet
        assert(extraRows.isEmpty,
          s"Split batch has rows not found in baseline batch: $extraRows")

        // Explicitly verify all row values match
        assert(splitBatchSet == baselineSet,
          s"Split batch should contain exactly the same rows as " +
          s"baseline batch at partition $partitionId, baseline index " +
          s"$baselineIdx. Baseline: $baselineSet, Split: $splitBatchSet")

        // Move indices forward
        baselineIdx += 1
        splitIdx += (if (isSplit) 2 else 1)
      }

      // Verify we processed all batches for this partition
      assert(baselineIdx == baselineBatchesForPartition.length,
        s"Did not process all baseline batches for partition $partitionId " +
        s"(processed $baselineIdx of ${baselineBatchesForPartition.length})")
      assert(splitIdx == splitBatchesForPartition.length,
        s"Did not process all split batches for partition $partitionId " +
        s"(processed $splitIdx of ${splitBatchesForPartition.length})")
    }

    assert(foundAnySplit, "Did not find any split batch")
  }


  val partitionCounts = Table("numPartitions", 1, 2, 4, 20)

  test("GPU Kudo write partitioning and serialization - with split retry") {
    forAll(partitionCounts) { numPartitions =>
      TrampolineUtil.cleanupAnyExistingSession()
      val conf = createKudoSparkConf()
      TestUtils.withGpuSparkSession(conf) { spark =>
        GpuShuffleEnv.init(new RapidsConf(conf))

        val serializer = createSerializer()
        val inputRDD = createInputRDD(spark)
        val dependency = setupShuffleDependency(spark, inputRDD, serializer, numPartitions)

        // Part 1: Run without split to establish baseline batch data
        val (baselineBatches, baselineTotalRowsSeen) =
          collectBatches(dependency, injectOOM = false)

        verifyBatchContents(baselineBatches, baselineTotalRowsSeen, serializer)

        // Extract and save baseline batch data with partition IDs
        val baselinePartitionIds = baselineBatches.map(_._1).toSeq
        val slicedBaselineBatches = baselineBatches.map(_._2).toSeq
        val baselineBatchDataSeq =
          extractBatchDataFromSlicedBatches(slicedBaselineBatches, serializer)
        val baselineBatchData = baselinePartitionIds.zip(baselineBatchDataSeq)

        baselineBatches.foreach(_._2.close())

        // Part 2: Run with OOM injection to trigger split retry
        val inputRDD2 = createInputRDD(spark)
        val dependency2 = setupShuffleDependency(spark, inputRDD2, serializer, numPartitions)

        val (splitBatches, splitTotalRowsSeen) =
          collectBatches(dependency2, injectOOM = true)

        val retryCount = RmmSpark.getAndResetNumSplitRetryThrow(1)
        assert(retryCount > 0,
          s"Expected at least one split retry, but saw $retryCount retries")

        verifyBatchContents(splitBatches, splitTotalRowsSeen, serializer)
        verifySplitRetryStructure(baselineBatchData, splitBatches, serializer)
        splitBatches.foreach(_._2.close())
      }
    }
  }
}
