/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import java.io.File
import java.nio.file.Files

import scala.collection.JavaConverters._

import ai.rapids.cudf.{ColumnVector, ContiguousTable, Cuda, HostColumnVector, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.CodecType
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

class GpuCoalesceBatchesSuite extends SparkQueryCompareTestSuite {
  val rapidsConf = new RapidsConf(Map.empty[String, String])

  def getCapturedPlan(): SparkPlan = {
    val capturedPlans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
    assert(capturedPlans.length == 1,
      s"Expected to capture exactly one plan: ${capturedPlans.mkString("\n")}")
    ExecutionPlanCaptureCallback.extractExecutedPlan(capturedPlans.head)
  }

  test("test with small input batches") {
    withGpuSparkSession(spark => {
      val testData = mixedDf(spark, numSlices = 1)
      val gpuRowToColumnarExec = GpuRowToColumnarExec(testData.queryExecution.sparkPlan,
        TargetSize(1))
      val gpuCoalesceBatches = GpuCoalesceBatches(gpuRowToColumnarExec, TargetSize(100000))
      val rdd = gpuCoalesceBatches.doExecuteColumnar()
      val part = rdd.partitions.head
      val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
      val batches = rdd.compute(part, context)

      // assert final results are correct
      assert(batches.hasNext)
      withResource(batches.next()) { batch =>
        assert(batch.numCols() == 5)
        assert(batch.numRows() == 14)
        assert(GpuColumnVector.isTaggedAsFinalBatch(batch))
        assert(!batches.hasNext)
      }

      // assert metrics are correct
      assert(gpuCoalesceBatches.metrics(GpuMetric.NUM_OUTPUT_ROWS).value == 14)
      assert(gpuCoalesceBatches.metrics(GpuMetric.NUM_OUTPUT_BATCHES).value == 1)
    }, new SparkConf().set(RapidsConf.METRICS_LEVEL.key, "DEBUG"))
  }

  test("test multiple output batches") {
    withGpuSparkSession(spark => {
      val testData = mixedDf(spark, numSlices = 1)
      val gpuRowToColumnarExec = GpuRowToColumnarExec(testData.queryExecution.sparkPlan,
        TargetSize(1))
      val gpuCoalesceBatches = GpuCoalesceBatches(gpuRowToColumnarExec, TargetSize(50))
      val rdd = gpuCoalesceBatches.doExecuteColumnar()
      val part = rdd.partitions.head
      val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
      val batches = rdd.compute(part, context)

      var totalRows = 0
      var totalBatches = 0
      while(batches.hasNext) {
        totalBatches += 1
        withResource(batches.next()) { batch =>
          assertResult(5)(batch.numCols())
          assertResult(!batches.hasNext)(GpuColumnVector.isTaggedAsFinalBatch(batch))
          totalRows += batch.numRows()
        }
      }
      assert(totalBatches > 1)
      assertResult(14)(totalRows)

      // assert metrics are correct
      assertResult(14)(gpuCoalesceBatches.metrics(GpuMetric.NUM_OUTPUT_ROWS).value)
      assertResult(totalBatches)(gpuCoalesceBatches.metrics(GpuMetric.NUM_OUTPUT_BATCHES).value)
    }, new SparkConf().set(RapidsConf.METRICS_LEVEL.key, "DEBUG"))
  }

  test("require single batch") {

    val conf = makeBatchedBytes(1, enableCsvConf())
      .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "1")
      .set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "1")
      .set(RapidsConf.STABLE_SORT.key, "true")
      .set("spark.sql.shuffle.partitions", "1")

    withGpuSparkSession(spark => {

      val df = longsCsvDf(spark)

      // GpuSortExec requires a single batch if out of core sore is disabled.
      val df2 = df
        .sort(df.col("longs"))

      // execute the plan
      ExecutionPlanCaptureCallback.startCapture()
      df2.collect()
      val executedPlan = getCapturedPlan()

      val coalesce = executedPlan
        .find(_.isInstanceOf[GpuCoalesceBatches]).get
        .asInstanceOf[GpuCoalesceBatches]

      assert(coalesce.goal == RequireSingleBatch)
      assert(coalesce.goal.asInstanceOf[CoalesceSizeGoal].targetSizeBytes == Long.MaxValue)

      assert(coalesce.longMetric(GpuMetric.NUM_OUTPUT_BATCHES).value == 1)

    }, conf)
  }

  // this was copied from Spark ArrowUtils
  /** Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes */
  private def toArrowType(dt: DataType, timeZoneId: String): ArrowType = dt match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(8 * 2, true)
    case IntegerType => new ArrowType.Int(8 * 4, true)
    case LongType => new ArrowType.Int(8 * 8, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case StringType => ArrowType.Utf8.INSTANCE
    case BinaryType => ArrowType.Binary.INSTANCE
    // case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale)
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case TimestampType =>
      if (timeZoneId == null) {
        throw new UnsupportedOperationException(
          s"${TimestampType.catalogString} must supply timeZoneId parameter")
      } else {
        new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
      }
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
  }

  // this was copied from Spark ArrowUtils
  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  private def toArrowField(
      name: String, dt: DataType, nullable: Boolean, timeZoneId: String): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(name, fieldType,
          Seq(toArrowField("element", elementType, containsNull, timeZoneId)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(name, fieldType,
          fields.map { field =>
            toArrowField(field.name, field.dataType, field.nullable, timeZoneId)
          }.toSeq.asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(name, mapType,
          Seq(toArrowField(MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, keyType, nullable = false)
              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
            nullable = false,
            timeZoneId)).asJava)
      case dataType =>
        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId), null)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  private def setupArrowBatch(withArrayType:Boolean = false): (ColumnarBatch, StructType) = {
    val rootAllocator = new RootAllocator(Long.MaxValue)
    val allocator = rootAllocator.newChildAllocator("int", 0, Long.MaxValue)
    val vector1 = toArrowField("int1", IntegerType, nullable = true, null)
      .createVector(allocator).asInstanceOf[IntVector]
    vector1.allocateNew()
    val vector2 = toArrowField("int2", IntegerType, nullable = true, null)
      .createVector(allocator).asInstanceOf[IntVector]
    vector2.allocateNew()
    val vector3 = toArrowField("array", ArrayType(IntegerType), nullable = true, null)
      .createVector(allocator).asInstanceOf[ListVector]
    vector3.allocateNew()
    val elementVector = vector3.getDataVector.asInstanceOf[IntVector]

    (0 until 10).foreach { i =>
      vector1.setSafe(i, i)
      vector2.setSafe(i + 1, i)
      elementVector.setSafe(i, i)
      vector3.startNewValue(i)
      elementVector.setSafe(0, 1)
      elementVector.setSafe(1, 2)
      vector3.endValue(i, 2)
    }
    elementVector.setValueCount(22)
    vector3.setValueCount(11)

    vector1.setNull(10)
    vector1.setValueCount(11)
    vector2.setNull(0)
    vector2.setValueCount(11)

    val baseVectors = Seq(new ArrowColumnVector(vector1), new ArrowColumnVector(vector2))
    val columnVectors = if (withArrayType) {
      baseVectors :+ new ArrowColumnVector(vector3)
    } else {
      baseVectors
    }
    val schemaBase = Seq(StructField("int1", IntegerType), StructField("int2", IntegerType))
    val schemaSeq = if (withArrayType) {
      schemaBase :+ StructField("array", ArrayType(IntegerType))
    } else {
      schemaBase
    }
    val schema = StructType(schemaSeq)
    (new ColumnarBatch(columnVectors.toArray), schema)
  }

  test("test host coalesce close semantics") {
    val hostData = Seq(Seq(1, 2), Nil)
    val batchFactory: Iterator[Seq[Int]] => ColumnarBatch = { iter =>
      val hostColumn =
        new RapidsHostColumnVector(IntegerType, HostColumnVector.fromInts(iter.next():_ *))
      new ColumnarBatch(Array(hostColumn), hostColumn.getRowCount.toInt)
    }
    val hostBatchIter = new AutoCloseColumnBatchIterator[Seq[Int]](hostData.iterator, batchFactory)
    val coalesceIter =
      new HostToGpuCoalesceIterator(
        hostBatchIter,
        TargetSize(1000),
        new StructType().add("ints", IntegerType),
        numInputRows = NoopMetric,
        numInputBatches = NoopMetric,
        numOutputRows = NoopMetric,
        numOutputBatches = NoopMetric,
        streamTime = NoopMetric,
        concatTime = NoopMetric,
        copyBufTime = NoopMetric,
        opTime = NoopMetric,
        opName = "concat test",
        useArrowCopyOpt = false)
    coalesceIter.foreach { batch =>
      // The main point of the test is to verify there's no double-close of a host batch.
      withResource(batch) { _ =>
        assertResult(1)(batch.numCols())
        assertResult(2)(batch.numRows())
      }
    }
  }

  test("test HostToGpuCoalesceIterator with arrow valid") {
    val (batch, schema) = setupArrowBatch(false)
    val iter = Iterator.single(batch)

    val hostToGpuCoalesceIterator = new HostToGpuCoalesceIterator(iter,
      TargetSize(1024),
      schema: StructType,
      WrappedGpuMetric(new SQLMetric("t1", 0)),
      WrappedGpuMetric(new SQLMetric("t2", 0)),
      WrappedGpuMetric(new SQLMetric("t3", 0)),
      WrappedGpuMetric(new SQLMetric("t4", 0)),
      WrappedGpuMetric(new SQLMetric("t5", 0)),
      WrappedGpuMetric(new SQLMetric("t6", 0)),
      WrappedGpuMetric(new SQLMetric("t7", 0)),
      WrappedGpuMetric(new SQLMetric("t8", 0)),
      "testcoalesce",
      useArrowCopyOpt = true)

    hostToGpuCoalesceIterator.initNewBatch(batch)
    assert(hostToGpuCoalesceIterator.batchBuilder.
      isInstanceOf[GpuColumnVector.GpuArrowColumnarBatchBuilder])
  }

  test("test HostToGpuCoalesceIterator with arrow array") {
    val (batch, schema) = setupArrowBatch(true)
    val iter = Iterator.single(batch)

    val hostToGpuCoalesceIterator = new HostToGpuCoalesceIterator(iter,
      TargetSize(1024),
      schema: StructType,
      WrappedGpuMetric(new SQLMetric("t1", 0)),
      WrappedGpuMetric(new SQLMetric("t2", 0)),
      WrappedGpuMetric(new SQLMetric("t3", 0)),
      WrappedGpuMetric(new SQLMetric("t4", 0)),
      WrappedGpuMetric(new SQLMetric("t5", 0)),
      WrappedGpuMetric(new SQLMetric("t6", 0)),
      WrappedGpuMetric(new SQLMetric("t7", 0)),
      WrappedGpuMetric(new SQLMetric("t8", 0)),
      "testcoalesce",
      useArrowCopyOpt = true)

    hostToGpuCoalesceIterator.initNewBatch(batch)
    // array isn't supported should fall back
    assert(hostToGpuCoalesceIterator.batchBuilder.
      isInstanceOf[GpuColumnVector.GpuColumnarBatchBuilder])
  }

  test("test GpuArrowColumnarBatchBuilder retains reference of ArrowBuf") {
    val rootAllocator = new RootAllocator(Long.MaxValue)
    val allocator = rootAllocator.newChildAllocator("int", 0, Long.MaxValue)
    val vector1 = toArrowField("int", IntegerType, nullable = true, null)
      .createVector(allocator).asInstanceOf[IntVector]
    val vector2 = toArrowField("int", IntegerType, nullable = true, null)
      .createVector(allocator).asInstanceOf[IntVector]
    vector1.allocateNew(10)
    vector2.allocateNew(10)
    (0 until 10).foreach { i =>
      vector1.setSafe(i, i)
      vector2.setSafe(i, i)
    }
    val schema = StructType(Seq(StructField("int", IntegerType)))
    val batches = Seq(
      new ColumnarBatch(Array(new ArrowColumnVector(vector1)), vector1.getValueCount),
      new ColumnarBatch(Array(new ArrowColumnVector(vector2)), vector1.getValueCount)
    )
    val hostToGpuCoalesceIterator = new HostToGpuCoalesceIterator(batches.iterator,
      TargetSize(1024),
      schema: StructType,
      WrappedGpuMetric(new SQLMetric("t1", 0)),
      WrappedGpuMetric(new SQLMetric("t2", 0)),
      WrappedGpuMetric(new SQLMetric("t3", 0)),
      WrappedGpuMetric(new SQLMetric("t4", 0)),
      WrappedGpuMetric(new SQLMetric("t5", 0)),
      WrappedGpuMetric(new SQLMetric("t6", 0)),
      WrappedGpuMetric(new SQLMetric("t7", 0)),
      WrappedGpuMetric(new SQLMetric("t8", 0)),
      "testcoalesce",
      useArrowCopyOpt = true)

    val allocatedMemory = allocator.getAllocatedMemory
    hostToGpuCoalesceIterator.initNewBatch(batches.head)
    hostToGpuCoalesceIterator.addBatchToConcat(batches.head)
    hostToGpuCoalesceIterator.addBatchToConcat(batches(1))

    // Close columnar batches
    batches.foreach(cb => cb.close())

    // Verify that buffers are not deallocated
    assertResult(allocatedMemory)(allocator.getAllocatedMemory)

    // Verify that buffers are deallocated after concat is done
    hostToGpuCoalesceIterator.cleanupConcatIsDone()
    assertResult(0L)(allocator.getAllocatedMemory)
  }

  test("test HostToGpuCoalesceIterator with arrow config off") {
    val (batch, schema) = setupArrowBatch()
    val iter = Iterator.single(batch)

    val hostToGpuCoalesceIterator = new HostToGpuCoalesceIterator(iter,
      TargetSize(1024),
      schema: StructType,
      WrappedGpuMetric(new SQLMetric("t1", 0)),
      WrappedGpuMetric(new SQLMetric("t2", 0)),
      WrappedGpuMetric(new SQLMetric("t3", 0)),
      WrappedGpuMetric(new SQLMetric("t4", 0)),
      WrappedGpuMetric(new SQLMetric("t5", 0)),
      WrappedGpuMetric(new SQLMetric("t6", 0)),
      WrappedGpuMetric(new SQLMetric("t7", 0)),
      WrappedGpuMetric(new SQLMetric("t8", 0)),
      "testcoalesce",
      useArrowCopyOpt = false)

    hostToGpuCoalesceIterator.initNewBatch(batch)
    assert(hostToGpuCoalesceIterator.batchBuilder.
      isInstanceOf[GpuColumnVector.GpuColumnarBatchBuilder])
  }

  test("coalesce HostColumnarToGpu") {

    val conf = makeBatchedBytes(1)
      .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "1")
      .set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "50000")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "FileSourceScanExec")
      .set("spark.rapids.sql.exec.FileSourceScanExec", "false") // force Parquet read onto CPU
      .set("spark.sql.shuffle.partitions", "1")
      // this test isn't valid when AQE is enabled because the FileScan happens as part of
      // a query stage that runs on the CPU, wrapped in a CPU Exchange, with a ColumnarToRow
      // transition inserted
      .set("spark.sql.adaptive.enabled", "false")
      // Disable out of core sort so a single batch is required
      .set(RapidsConf.STABLE_SORT.key, "true")

    val dir = Files.createTempDirectory("spark-rapids-test").toFile
    val path = new File(dir,
      s"HostColumnarToGpu-${System.currentTimeMillis()}.parquet").getAbsolutePath

    try {
      // convert csv test data to parquet
      withCpuSparkSession(spark => {
        longsCsvDf(spark).write.parquet(path)
      }, conf)

      withGpuSparkSession(spark => {
        val df = spark.read.parquet(path)
        val df2 = df
          .sort(df.col("longs"))

        // execute the plan
        ExecutionPlanCaptureCallback.startCapture()
        df2.collect()
        val executedPlan = getCapturedPlan()

        // ensure that the plan does include the HostColumnarToGpu step
        val hostColumnarToGpu = executedPlan
          .find(_.isInstanceOf[HostColumnarToGpu]).get
          .asInstanceOf[HostColumnarToGpu]

        assert(hostColumnarToGpu.goal == TargetSize(50000))

        val gpuCoalesceBatches = executedPlan
          .find(_.isInstanceOf[GpuCoalesceBatches]).get
          .asInstanceOf[GpuCoalesceBatches]

        assert(gpuCoalesceBatches.goal == RequireSingleBatch)
        assert(gpuCoalesceBatches.goal.asInstanceOf[CoalesceSizeGoal].targetSizeBytes
            == Long.MaxValue)

      }, conf)
    } finally {
      dir.delete()
    }
  }

  test("not require single batch") {

    val conf = makeBatchedBytes(1)
      .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "1")
      .set("spark.sql.shuffle.partitions", "1")

    withGpuSparkSession(spark => {

      val df = mixedDf(spark, numSlices = 14)

      // A coalesce step is added after the filter to help with the case where much of the
      // data is filtered out.  The select is there to prevent the coalesce from being
      // the last thing in the plan which will cause the coalesce to be optimized out.
      val df2 = df
        .filter(df.col("ints").gt(90)).select(df.col("decimals"))

      val coalesce = df2.queryExecution.executedPlan
        .find(_.isInstanceOf[GpuCoalesceBatches]).get
        .asInstanceOf[GpuCoalesceBatches]

      assert(coalesce.goal != RequireSingleBatch)
      assert(coalesce.goal.asInstanceOf[CoalesceSizeGoal].targetSizeBytes == 1)

      // assert the metrics start out at zero
      assert(coalesce.additionalMetrics("numInputBatches").value == 0)
      assert(coalesce.longMetric(GpuMetric.NUM_OUTPUT_BATCHES).value == 0)

      // execute the plan
      df2.collect()

      // assert the metrics are correct
      assert(coalesce.longMetric(GpuMetric.NUM_OUTPUT_BATCHES).value == 11)

    }, conf)
  }

  def testCompressedBatches(maxCompressedBatchMemoryLimit: Long): Unit = {
    val coalesceTargetBytes = 8000
    val stop = 10000
    var start = 0
    var numBatchRows = 100
    var expectedEnd = 0
    val batchIter = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = if (start < stop) {
        true
      } else {
        expectedEnd = start
        false
      }
      override def next(): ColumnarBatch = {
        val batch = buildCompressedBatch(start, numBatchRows)
        start += batch.numRows
        numBatchRows *= 2
        batch
      }
    }

    val schema = new StructType().add("i", LongType)
      .add("j", DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 3))
    val dummyMetric = WrappedGpuMetric(new SQLMetric("ignored"))
    val coalesceIter = new GpuCompressionAwareCoalesceIterator(
      batchIter,
      GpuColumnVector.extractTypes(schema),
      TargetSize(coalesceTargetBytes),
      maxCompressedBatchMemoryLimit,
      dummyMetric,
      dummyMetric,
      dummyMetric,
      dummyMetric,
      dummyMetric,
      dummyMetric,
      dummyMetric,
      "test concat",
      TableCompressionCodec.makeCodecConfig(rapidsConf))

    var expected = 0
    while (coalesceIter.hasNext) {
      withResource(coalesceIter.next()) { batch =>
        assertResult(2)(batch.numCols)
        assertResult(!coalesceIter.hasNext)(GpuColumnVector.isTaggedAsFinalBatch(batch))
        val Array(longCol, decCol) = GpuColumnVector.extractBases(batch)
        withResource(longCol.copyToHost) { longHcv =>
          withResource(decCol.copyToHost) { decHcv =>
            (0 until longHcv.getRowCount.toInt).foreach { i =>
              assertResult(expected)(longHcv.getLong(i))
              assertResult(expected)(decHcv.getLong(i))
              assertResult(BigDecimal(expected, 3).bigDecimal)(decHcv.getBigDecimal(i))
              expected += 1
            }
          }
        }
      }
    }
    assertResult(expectedEnd)(expected)
  }

  test("all compressed low memory limit") {
    testCompressedBatches(0)
  }

  test("all compressed high memory limit") {
    testCompressedBatches(Long.MaxValue)
  }

  test("mixed compressed and uncompressed low memory limit") {
    testMixedCompressedUncompressed(0)
  }

  test("mixed compressed and uncompressed high memory limit") {
    testMixedCompressedUncompressed(Long.MaxValue)
  }

  def testMixedCompressedUncompressed(maxCompressedBatchMemoryLimit: Long): Unit = {
    val coalesceTargetBytes = 8000
    val stop = 10000
    var start = 0
    var numBatchRows = 100
    var nextBatchCompressed = false
    var expectedEnd = 0
    val batchIter = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = if (start < stop) {
        true
      } else {
        expectedEnd = start
        false
      }
      override def next(): ColumnarBatch = {
        val batch = if (nextBatchCompressed) {
          buildCompressedBatch(start, numBatchRows)
        } else {
          buildUncompressedBatch(start, numBatchRows)
        }
        nextBatchCompressed = !nextBatchCompressed
        start += batch.numRows
        numBatchRows *= 2
        batch
      }
    }

    val schema = new StructType().add("i", LongType)
      .add("j", DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 3))
    val dummyMetric = WrappedGpuMetric(new SQLMetric("ignored"))
    val coalesceIter = new GpuCompressionAwareCoalesceIterator(
      batchIter,
      GpuColumnVector.extractTypes(schema),
      TargetSize(coalesceTargetBytes),
      maxCompressedBatchMemoryLimit,
      dummyMetric,
      dummyMetric,
      dummyMetric,
      dummyMetric,
      dummyMetric,
      dummyMetric,
      dummyMetric,
      "test concat",
      TableCompressionCodec.makeCodecConfig(rapidsConf))

    var expected = 0
    while (coalesceIter.hasNext) {
      withResource(coalesceIter.next()) { batch =>
        assertResult(2)(batch.numCols)
        assertResult(!coalesceIter.hasNext)(GpuColumnVector.isTaggedAsFinalBatch(batch))
        val Array(longCol, decCol) = GpuColumnVector.extractBases(batch)
        withResource(longCol.copyToHost) { longHcv =>
          withResource(decCol.copyToHost) { decHcv =>
            (0 until longHcv.getRowCount.toInt).foreach { i =>
              assertResult(expected)(longHcv.getLong(i))
              assertResult(expected)(decHcv.getLong(i))
              assertResult(BigDecimal(expected, 3).bigDecimal)(decHcv.getBigDecimal(i))
              expected += 1
            }
          }
        }
      }
    }
    assertResult(expectedEnd)(expected)
  }

  private def buildContiguousTable(start: Int, numRows: Int): ContiguousTable = {
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

  private def buildUncompressedBatch(start: Int, numRows: Int): ColumnarBatch = {
    withResource(buildContiguousTable(start, numRows)) { ct =>
      GpuColumnVector.from(ct.getTable,
        Array[DataType](LongType, DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 3)))
    }
  }

  private def buildCompressedBatch(start: Int, numRows: Int): ColumnarBatch = {
    val codec = TableCompressionCodec.getCodec(
      CodecType.NVCOMP_LZ4, TableCompressionCodec.makeCodecConfig(rapidsConf))
    withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
      compressor.addTableToCompress(buildContiguousTable(start, numRows))
      withResource(compressor.finish()) { compressed =>
        GpuCompressedColumnVector.from(compressed.head)
      }
    }
  }

  /** tests for the filtering mode */

  test("test entering filtering mode with not enough nulls") {
    // We are entering the filtering mode during processing the second batch (3 null rows),
    // but the filtered rows number (12 = 5 + 7) is still too big, so it blows up before
    // switching into the filtering mode.
    val dataWithNulls = (1 to 3).map(_ => null) ++ (6 to 12).map(Integer.valueOf)
    val coalIter = newGpuCoalesceIteratorForNullFiltering(Iterator(
      newOneIntColumnBatch((1 to 5).map(Integer.valueOf)),
      newOneIntColumnBatch(dataWithNulls)
    ))
    assert(coalIter.hasNext)
    // batch is too big to fit the rows number as an int
    assertThrows[IllegalStateException](coalIter.next())
    // we are on the way to the filtering mode but not reach yet.
    assert(!coalIter.isInFilteringMode)
  }

  test("test entering filtering mode with enough nulls") {
    // We succeed in entering the filtering mode after processing the second
    // batch (8 null rows), because the filtered rows number (7 = 5 + 2) is smaller
    // than the threshold (10) of filtering mode now.
    val dataWithNulls = (1 to 8).map(_ => null) ++ (6 to 7).map(Integer.valueOf)
    val coalIter = newGpuCoalesceIteratorForNullFiltering(Iterator(
      newOneIntColumnBatch((1 to 5).map(Integer.valueOf)),
      newOneIntColumnBatch(dataWithNulls)
    ))
    assert(coalIter.hasNext)
    withResource(coalIter.next()) { cb =>
      assertResult(expected = 7)(cb.numRows())
    }
    // only one batch and it should be in filtering mode
    assert(!coalIter.hasNext)
    assert(coalIter.isInFilteringMode)
  }

  test("test in filtering mode with not enough nulls") {
    // We are already in the filtering mode after processing the second batch, but
    // the total filtered rows number (12 = 5 + 2 + 5) is still too big when the
    // third batch comes, so it blows up.
    val dataWithNulls = (1 to 8).map(_ => null) ++ (6 to 7).map(Integer.valueOf)
    val dataWithNulls2 = (1 to 8).map(_ => null) ++ (8 to 12).map(Integer.valueOf)
    val coalIter = newGpuCoalesceIteratorForNullFiltering(Iterator(
      newOneIntColumnBatch((1 to 5).map(Integer.valueOf)),
      newOneIntColumnBatch(dataWithNulls),
      newOneIntColumnBatch(dataWithNulls2)
    ))
    assert(coalIter.hasNext)
    // batch is too big to fit the rows number as an int
    assertThrows[IllegalStateException](coalIter.next())
    assert(coalIter.isInFilteringMode)
  }

  test("test in filtering mode with enough nulls") {
    // We are already in the filtering mode after processing the second batch, and even
    // including the third batch, the total filtered rows number (9 = 5 + 2 + 2) is still
    // smaller than the threshold (10) of filtering mode.
    val dataWithNulls = (1 to 8).map(_ => null) ++ (6 to 7).map(Integer.valueOf)
    val dataWithNulls2 = (1 to 8).map(_ => null) ++ (8 to 9).map(Integer.valueOf)
    val coalIter = newGpuCoalesceIteratorForNullFiltering(Iterator(
      newOneIntColumnBatch((1 to 5).map(Integer.valueOf)),
      newOneIntColumnBatch(dataWithNulls),
      newOneIntColumnBatch(dataWithNulls2)
    ))
    assert(coalIter.hasNext)
    withResource(coalIter.next()) { cb =>
      assertResult(expected = 9)(cb.numRows())
    }
    // only one batch and it should be in filtering mode
    assert(!coalIter.hasNext)
    assert(coalIter.isInFilteringMode)
  }

  private def newGpuCoalesceIteratorForNullFiltering(
      iter: Iterator[ColumnarBatch],
      sparkTypes: Array[DataType] = Array(IntegerType)): GpuCoalesceIterator = {
    val goal = RequireSingleBatchWithFilter(
      GpuIsNotNull(GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), ""))
    )
    new GpuCoalesceIterator(iter, sparkTypes, goal, NoopMetric, NoopMetric, NoopMetric,
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, "NullFilteringModeTest") {
      override protected val filteringModeRowsThreshold: Int = 10
    }
  }

  private def newOneIntColumnBatch(data: Seq[Integer]): ColumnarBatch = {
    withResource(ColumnVector.fromBoxedInts(data: _*)) { cudfCol =>
      withResource(new Table(cudfCol)) { table =>
        GpuColumnVector.from(table, Array(IntegerType))
      }
    }
  }
}
