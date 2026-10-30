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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.JavaConverters._

import ai.rapids.cudf.{ColumnVector, ContiguousTable, Cuda, HostColumnVector, PinnedMemoryPool,
  Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.CudfTestHelper.withRecordedHostAllocations
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.CodecType
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseLargeVariableWidthVector,
  BaseVariableWidthVector, BigIntVector, BitVector, DateDayVector, FieldVector, Float4Vector,
  Float8Vector, IntVector, LargeVarBinaryVector, LargeVarCharVector, SmallIntVector,
  TimeStampMicroTZVector, TinyIntVector, VarBinaryVector, VarCharVector}
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

class GpuCoalesceBatchesSuite extends SparkQueryCompareTestSuite {
  val rapidsConf = new RapidsConf(Map.empty[String, String])

  test("coalesce preserves child output ordering") {
    withGpuSparkSession { spark =>
      val child = spark.range(1000).sort("id").queryExecution.executedPlan
      assert(child.outputOrdering.nonEmpty)

      val coalesce = GpuCoalesceBatches(child, TargetSize(1024))
      assertResult(child.outputOrdering)(coalesce.outputOrdering)
    }
  }

  test("buildNonEmptyBatchFromTypes sums the row counts of rows-only batches") {
    val rowsOnly = Array(new ColumnarBatch(Array.empty, 3), new ColumnarBatch(Array.empty, 4))
    withResource(ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(rowsOnly, Array.empty)) {
      batch =>
        assertResult(0)(batch.numCols())
        assertResult(7)(batch.numRows())
    }
    val tooManyRows = Array(
      new ColumnarBatch(Array.empty, Int.MaxValue), new ColumnarBatch(Array.empty, 1))
    val e = intercept[IllegalArgumentException] {
      ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(tooManyRows, Array.empty)
    }
    assert(e.getMessage.contains("Cannot concatenate"), e.getMessage)
  }

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

  private def setArrowValue(vector: FieldVector, i: Int, value: Any): Unit = (vector, value) match {
    case (v: BaseFixedWidthVector, null) => v.setNull(i)
    case (v: BaseVariableWidthVector, null) => v.setNull(i)
    case (v: BaseLargeVariableWidthVector, null) => v.setNull(i)
    case (v: BitVector, b: Boolean) => v.setSafe(i, if (b) 1 else 0)
    case (v: TinyIntVector, b: Byte) => v.setSafe(i, b)
    case (v: SmallIntVector, s: Short) => v.setSafe(i, s)
    case (v: IntVector, n: Int) => v.setSafe(i, n)
    case (v: DateDayVector, n: Int) => v.setSafe(i, n)
    case (v: BigIntVector, n: Long) => v.setSafe(i, n)
    case (v: TimeStampMicroTZVector, n: Long) => v.setSafe(i, n)
    case (v: Float4Vector, f: Float) => v.setSafe(i, f)
    case (v: Float8Vector, d: Double) => v.setSafe(i, d)
    case (v: VarCharVector, s: String) => v.setSafe(i, s.getBytes(StandardCharsets.UTF_8))
    case (v: LargeVarCharVector, s: String) => v.setSafe(i, s.getBytes(StandardCharsets.UTF_8))
    case (v: VarBinaryVector, b: Array[Byte]) => v.setSafe(i, b)
    case (v: LargeVarBinaryVector, b: Array[Byte]) => v.setSafe(i, b)
  }

  private def arrowVector(
      allocator: BufferAllocator,
      field: StructField,
      values: Seq[Any]): ArrowColumnVector = {
    val vector = toArrowField(field.name, field.dataType, field.nullable, "UTC")
      .createVector(allocator)
    vector.allocateNew()
    values.zipWithIndex.foreach { case (value, i) => setArrowValue(vector, i, value) }
    vector.setValueCount(values.length)
    new ArrowColumnVector(vector)
  }

  private def arrowBatch(
      allocator: BufferAllocator,
      schema: StructType,
      columns: Seq[Seq[Any]]): ColumnarBatch = {
    val vectors = schema.fields.toSeq.zip(columns).map { case (field, values) =>
      arrowVector(allocator, field, values)
    }
    new ColumnarBatch(vectors.toArray, columns.head.length)
  }

  private def hostValue(cv: RapidsHostColumnVector, dataType: DataType, i: Int): Any = {
    if (cv.isNullAt(i)) {
      null
    } else {
      dataType match {
        case BooleanType => cv.getBoolean(i)
        case ByteType => cv.getByte(i)
        case ShortType => cv.getShort(i)
        case IntegerType | DateType => cv.getInt(i)
        case LongType | TimestampType => cv.getLong(i)
        case FloatType => cv.getFloat(i)
        case DoubleType => cv.getDouble(i)
        case StringType => cv.getUTF8String(i).toString
        case BinaryType => cv.getBinary(i).toSeq
      }
    }
  }

  /** Checks each column of a GPU batch against the expected values of its rows. */
  private def assertColumns(
      schema: StructType,
      expected: Seq[Seq[Any]],
      gpuBatch: ColumnarBatch): Unit = {
    assertResult(expected.head.length)(gpuBatch.numRows())
    schema.fields.zip(expected).zipWithIndex.foreach { case ((field, values), c) =>
      withResource(gpuBatch.column(c).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
        val normalized = values.map {
          case b: Array[Byte] => b.toSeq
          case v => v
        }
        assertResult(normalized, field.name)(values.indices.map(hostValue(host, field.dataType, _)))
      }
    }
  }

  private def arrowCoalesce(
      batches: Iterator[ColumnarBatch],
      schema: StructType): HostToGpuCoalesceIterator = {
    new HostToGpuCoalesceIterator(batches, TargetSize(1024 * 1024), schema,
      NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric, "testcoalesce", useArrowCopyOpt = true)
  }

  test("HostToGpuCoalesceIterator copies Arrow vectors with the Arrow builder") {
    val schema = StructType(Seq(
      StructField("bool", BooleanType), StructField("byte", ByteType),
      StructField("short", ShortType), StructField("int", IntegerType),
      StructField("long", LongType), StructField("float", FloatType),
      StructField("double", DoubleType), StructField("date", DateType),
      StructField("ts", TimestampType), StructField("str", StringType),
      StructField("bin", BinaryType)))
    def rows(n: Int, seed: Int): Seq[Seq[Any]] = {
      // Every column has a null, in a different row for each, except in the last batch.
      def column(c: Int)(value: Int => Any): Seq[Any] =
        (0 until n).map(i => if (seed < 3 && (i + c) % n == 0) null else value(i))
      Seq(
        column(0)(i => (i + seed) % 2 == 0),
        column(1)(i => (i + seed).toByte),
        column(2)(i => (i * seed).toShort),
        column(3)(i => i * seed - 7),
        column(4)(i => i.toLong << 33 | seed),
        column(5)(i => i * 0.5f + seed),
        column(6)(i => i * 1.25 - seed),
        column(7)(i => 18000 + i + seed),
        column(8)(i => 1700000000000000L + i * 1000L + seed),
        column(9)(i => "s" * (i % 3) + seed),
        column(10)(i => Array.tabulate[Byte](i % 4)(j => (j + seed).toByte)))
    }
    // Sizes that do not divide into whole validity bytes.
    val inputRows = Seq(rows(3, 1), rows(13, 2), rows(4, 3))
    withResource(new RootAllocator(Long.MaxValue)) { allocator =>
      val gpuBatch = withResource(inputRows.safeMap(arrowBatch(allocator, schema, _))) { input =>
        val iter = arrowCoalesce(input.iterator, schema)
        iter.initNewBatch(input.head)
        assert(iter.batchBuilder.isInstanceOf[GpuColumnVector.GpuArrowColumnarBatchBuilder])
        iter.cleanupConcatIsDone()
        arrowCoalesce(input.iterator, schema).next()
      }
      withResource(gpuBatch) { _ =>
        // The builder copied the Arrow data, so closing the input freed it.
        assertResult(0L)(allocator.getAllocatedMemory)
        val expected = schema.indices.map(c => inputRows.flatMap(_(c)))
        assertColumns(schema, expected, gpuBatch)
      }
    }
  }

  test("HostToGpuCoalesceIterator lets an Arrow producer close its allocator at end of input") {
    // Like a CPU ArrowEvalPythonExec, the producer frees each batch when asked for the next one
    // and closes its allocator in the hasNext call that finds no more input.
    val schema = StructType(Seq(StructField("int", IntegerType), StructField("str", StringType)))
    val inputRows = (1 to 3).map(n => Seq(Seq(n, null, -n), Seq("a" * n, "b", null)))
    val allocator = new RootAllocator(Long.MaxValue)
    val input = new Iterator[ColumnarBatch] {
      private val pending = inputRows.iterator
      private var current: Option[ColumnarBatch] = None

      override def hasNext: Boolean = {
        current.foreach(_.close())
        current = None
        if (!pending.hasNext) {
          allocator.close()
        }
        pending.hasNext
      }

      override def next(): ColumnarBatch = {
        current = Some(arrowBatch(allocator, schema, pending.next()))
        current.get
      }
    }
    withResource(arrowCoalesce(input, schema).next()) { gpuBatch =>
      val expected = schema.indices.map(c => inputRows.flatMap(_(c)))
      assertColumns(schema, expected, gpuBatch)
    }
  }

  test("HostToGpuCoalesceIterator copies Arrow vectors that the producer refills in place") {
    // Like Spark's vectorized Parquet reader, the producer refills the vectors of the batch it
    // returned when asked for the next one.
    val schema = StructType(Seq(StructField("int", IntegerType), StructField("str", StringType)))
    val inputRows = Seq(Seq(Seq(1, 2, 3), Seq("a", "bb", "ccc")),
      Seq(Seq(4, 5, 6), Seq("x", "yy", "zzz")))
    withResource(new RootAllocator(Long.MaxValue)) { allocator =>
      withResource(new IntVector("int", allocator)) { ints =>
        withResource(new VarCharVector("str", allocator)) { strs =>
          val vectors = Seq(ints, strs)
          vectors.foreach(_.allocateNew())
          val batch = new ColumnarBatch(vectors.map(new ArrowColumnVector(_)).toArray)
          val input = inputRows.iterator.map { columns =>
            vectors.zip(columns).foreach { case (vector, values) =>
              vector.reset()
              values.zipWithIndex.foreach { case (value, i) => setArrowValue(vector, i, value) }
              vector.setValueCount(values.length)
            }
            batch.setNumRows(columns.head.length)
            batch
          }
          withResource(arrowCoalesce(input, schema).next()) { gpuBatch =>
            assertColumns(schema, schema.indices.map(c => inputRows.flatMap(_(c))), gpuBatch)
          }
        }
      }
    }
  }

  test("HostToGpuCoalesceIterator copies Arrow strings by row when their nulls hold data") {
    val schema = StructType(Seq(StructField("str", StringType)))
    withResource(new RootAllocator(Long.MaxValue)) { allocator =>
      val vector = new VarCharVector("str", allocator)
      vector.allocateNew()
      Seq("abc", "de", "f").zipWithIndex.foreach { case (value, i) =>
        vector.setSafe(i, value.getBytes(StandardCharsets.UTF_8))
      }
      // The null row keeps its two bytes.
      vector.setNull(1)
      vector.setValueCount(3)
      withResource(new ColumnarBatch(Array(new ArrowColumnVector(vector)), 3)) { batch =>
        withResource(arrowCoalesce(Iterator.single(batch), schema).next()) { gpuBatch =>
          assertColumns(schema, Seq(Seq("abc", null, "f")), gpuBatch)
          assert(!gpuBatch.column(0).asInstanceOf[GpuColumnVector].getBase.hasNonEmptyNulls)
        }
      }
    }
  }

  test("HostToGpuCoalesceIterator copies batches that mix Arrow and other column vectors") {
    // A source can return Arrow vectors next to Spark's own, such as for partition values.
    val schema = StructType(Seq(StructField("str", StringType), StructField("int", IntegerType)))
    val values = Seq(Seq("a", null, "bc"), Seq(1, 2, 3))
    withResource(new RootAllocator(Long.MaxValue)) { allocator =>
      val ints = new OnHeapColumnVector(values(1).length, IntegerType)
      values(1).zipWithIndex.foreach { case (v: Int, i) => ints.putInt(i, v) }
      val strings = arrowVector(allocator, schema.fields(0), values.head)
      withResource(new ColumnarBatch(Array(strings, ints), values(1).length)) { batch =>
        withResource(arrowCoalesce(Iterator.single(batch), schema).next()) { gpuBatch =>
          assertColumns(schema, values, gpuBatch)
        }
      }
    }
  }

  test("HostToGpuCoalesceIterator copies Arrow vectors with 64-bit offsets by row") {
    assume(cmpSparkVersion(3, 5, 0) >= 0, "Spark reads 64-bit offset vectors from 3.5.0 on")
    val schema = StructType(Seq(StructField("str", StringType), StructField("bin", BinaryType),
      StructField("int", IntegerType)))
    val values = Seq(Seq("abc", null, "", "xyz"),
      Seq(Array[Byte](1, 2), Array.emptyByteArray, null, Array[Byte](3)), Seq(1, 2, null, 4))
    withResource(new RootAllocator(Long.MaxValue)) { allocator =>
      val strings = new LargeVarCharVector("str", allocator)
      val binary = new LargeVarBinaryVector("bin", allocator)
      Seq(strings, binary).zip(values).foreach { case (vector, column) =>
        vector.allocateNew()
        column.zipWithIndex.foreach { case (value, i) => setArrowValue(vector, i, value) }
        vector.setValueCount(column.length)
      }
      val ints = arrowVector(allocator, schema.fields(2), values(2))
      withResource(new ColumnarBatch(Array(new ArrowColumnVector(strings),
          new ArrowColumnVector(binary), ints), values.head.length)) { batch =>
        withResource(arrowCoalesce(Iterator.single(batch), schema).next()) { gpuBatch =>
          assertColumns(schema, values, gpuBatch)
        }
      }
    }
  }

  test("HostToGpuCoalesceIterator copies many small Arrow batches into shared host columns") {
    val schema = StructType(Seq(StructField("long", LongType), StructField("str", StringType),
      StructField("bin", BinaryType)))
    // Sizes and value lengths vary, so chunks fill up on rows in some batches and on bytes in
    // others.
    val numBatches = 60
    val inputRows = (0 until numBatches).map { b =>
      val n = 1 + (b * 7) % 11
      val longs = (0 until n).map(i => if ((b + i) % 5 == 0) null else (b * 100 + i).toLong)
      val strs = (0 until n).map(i => if ((b + i) % 4 == 0) null else "x" * ((b * i) % 37))
      val bins = (0 until n).map(i => Array.fill[Byte]((b + i) % 23)(b.toByte))
      Seq(longs, strs, bins)
    }
    withResource(new RootAllocator(Long.MaxValue)) { allocator =>
      val (gpuBatch, hostAllocations) =
        withResource(inputRows.safeMap(arrowBatch(allocator, schema, _))) { input =>
          withRecordedHostAllocations { recorder =>
            (arrowCoalesce(input.iterator, schema).next(), recorder.count)
          }
        }
      withResource(gpuBatch) { _ =>
        // A host column for each batch and column would take at least one allocation each.
        assert(hostAllocations < numBatches * schema.length,
          s"$hostAllocations host allocations for $numBatches batches")
        assertResult(0L)(allocator.getAllocatedMemory)
        val expected = schema.indices.map(c => inputRows.flatMap(_(c)))
        assertColumns(schema, expected, gpuBatch)
      }
    }
  }

  test("HostToGpuCoalesceIterator coalesces Arrow batches of empty strings") {
    // The bytes copied set how many rows an output batch takes. Empty strings copy only their
    // offsets, so without them the limit would be one input batch.
    val schema = StructType(Seq(StructField("str", StringType)))
    val inputRows = Seq.fill(5)(Seq(Seq("", null, "")))
    withResource(new RootAllocator(Long.MaxValue)) { allocator =>
      val output = withResource(inputRows.safeMap(arrowBatch(allocator, schema, _))) { input =>
        arrowCoalesce(input.iterator, schema).toList
      }
      withResource(output) { _ =>
        assertResult(1, "output batches")(output.length)
        assertColumns(schema, Seq(inputRows.flatMap(_.head)), output.head)
      }
    }
  }

  test("HostToGpuCoalesceIterator keeps row order across bulk and row-by-row Arrow batches") {
    val schema = StructType(Seq(StructField("str", StringType)))
    // A batch whose null rows hold bytes is copied row by row, into a host column that the
    // batches after it append to, in bulk where they can.
    val values = Seq(Seq("a", null, "bc"), Seq("g"), Seq("x", null, ""), Seq("d", "ef", null),
      Seq(null, "y"))
    val nullsHoldBytes = Seq(false, false, true, false, true)
    withResource(new RootAllocator(Long.MaxValue)) { allocator =>
      val input = values.zip(nullsHoldBytes).map { case (batchValues, dirty) =>
        val vector = new VarCharVector("str", allocator)
        vector.allocateNew()
        batchValues.zipWithIndex.foreach {
          case (null, i) if dirty =>
            vector.setSafe(i, "dirty".getBytes(StandardCharsets.UTF_8))
            vector.setNull(i)
          case (value, i) => setArrowValue(vector, i, value)
        }
        vector.setValueCount(batchValues.length)
        new ColumnarBatch(Array(new ArrowColumnVector(vector)), batchValues.length)
      }
      withResource(arrowCoalesce(input.iterator, schema).next()) { gpuBatch =>
        assertColumns(schema, Seq(values.flatten), gpuBatch)
      }
      input.foreach(_.close())
    }
  }

  test("HostToGpuCoalesceIterator bounds the host memory of a column whose Arrow batches " +
      "alternate between bulk and row-by-row copies") {
    // A batch whose null row keeps its bytes needs a row-by-row copy. Starting a host column at
    // every switch would hold up to 16 MiB for each such batch.
    val schema = StructType(Seq(StructField("str", StringType)))
    val value = ("x" * 100).getBytes(StandardCharsets.UTF_8)
    val numRows = 100
    val numBatches = 20
    val hostLimit = 1L << 30
    HostAlloc.initialize(hostLimit)
    try {
      withResource(new RootAllocator(Long.MaxValue)) { allocator =>
        val iter = arrowCoalesce(Iterator.empty, schema)
        (0 until numBatches).foreach { b =>
          val vector = new VarCharVector("str", allocator)
          vector.allocateNew()
          (0 until numRows).foreach(i => vector.setSafe(i, value))
          if (b % 2 == 1) {
            vector.setNull(0)
          }
          vector.setValueCount(numRows)
          withResource(new ColumnarBatch(Array(new ArrowColumnVector(vector)), numRows)) {
            batch =>
              if (b == 0) {
                iter.initNewBatch(batch)
              }
              iter.addBatchToConcat(batch)
          }
        }
        // Only the plugin's host memory, not the Arrow input
        val hostBytes =
          HostAlloc.getUsageRatio() * (PinnedMemoryPool.getTotalPoolSizeBytes + hostLimit)
        iter.cleanupConcatIsDone()
        val dataBytes = numBatches * numRows * (value.length + 4L)
        assert(hostBytes < 3 * dataBytes, s"$hostBytes host bytes for $dataBytes bytes of data")
      }
    } finally {
      HostAlloc.initialize(-1)
    }
  }

  test("arrowBulkCopy copies supported Arrow vectors and leaves the others to the caller") {
    // Both copies build the same values, so only the return value shows which one ran. A
    // vector left to the caller must not ask for a host column, which would count its rows.
    def bulkCopy(cv: ArrowColumnVector, dataType: DataType, rows: Int): (Long, Int) = {
      var calls = 0
      val copied = withResource(new RapidsHostColumnBuilder(
          GpuColumnVector.convertFrom(dataType, true), rows)) { builder =>
        HostColumnarToGpu.arrowBulkCopy(cv, dataType, rows, (_: Long) => {
          calls += 1
          builder
        })
      }
      (copied, calls)
    }
    def bytes(values: Seq[Any]): Seq[Any] =
      values.map(v => if (v == null) null else v.toString.getBytes(StandardCharsets.UTF_8))
    withResource(new RootAllocator(Long.MaxValue)) { allocator =>
      Seq(
        StructField("bool", BooleanType) -> Seq(true, null, false),
        StructField("byte", ByteType) -> Seq(1.toByte, null, 3.toByte),
        StructField("short", ShortType) -> Seq(1.toShort, null, 3.toShort),
        StructField("int", IntegerType) -> Seq(1, null, 3),
        StructField("long", LongType) -> Seq(1L, null, 3L),
        StructField("float", FloatType) -> Seq(1.5f, null, 3f),
        StructField("double", DoubleType) -> Seq(1.5, null, 3.0),
        StructField("date", DateType) -> Seq(1, null, 3),
        StructField("ts", TimestampType) -> Seq(1L, null, 3L),
        StructField("str", StringType) -> Seq("a", null, "bc"),
        StructField("bin", BinaryType) -> bytes(Seq("a", null, "bc"))
      ).foreach { case (field, values) =>
        withResource(arrowVector(allocator, field, values)) { cv =>
          val (copied, calls) = bulkCopy(cv, field.dataType, values.length)
          assert(copied >= 0 && calls == 1, s"${field.name}: copied $copied with $calls calls")
        }
      }

      // Spark reads Arrow vectors with 64-bit offsets from 3.5.0 on.
      val large: Seq[(String, ArrowColumnVector, DataType)] =
        if (cmpSparkVersion(3, 5, 0) >= 0) {
          val largeStrings = new LargeVarCharVector("str", allocator)
          val largeBinary = new LargeVarBinaryVector("bin", allocator)
          Seq(largeStrings, largeBinary).foreach { v =>
            v.allocateNew()
            v.setSafe(0, "a".getBytes(StandardCharsets.UTF_8))
            v.setNull(1)
            v.setValueCount(2)
          }
          Seq(("64-bit offsets", new ArrowColumnVector(largeStrings), StringType),
            ("64-bit binary offsets", new ArrowColumnVector(largeBinary), BinaryType))
        } else {
          Seq.empty
        }
      val nullWithBytes = new VarCharVector("str", allocator)
      nullWithBytes.allocateNew()
      nullWithBytes.setSafe(0, "ab".getBytes(StandardCharsets.UTF_8))
      nullWithBytes.setSafe(1, "c".getBytes(StandardCharsets.UTF_8))
      nullWithBytes.setNull(0)
      nullWithBytes.setValueCount(2)
      val others = large ++ Seq(
        ("a null holding bytes", new ArrowColumnVector(nullWithBytes), StringType),
        ("more values than rows",
          arrowVector(allocator, StructField("int", IntegerType), Seq(1, 2, 3)), IntegerType))
      withResource(others.map(_._2)) { _ =>
        others.foreach { case (name, cv, dataType) =>
          assertResult((-1L, 0), name)(bulkCopy(cv, dataType, 2))
        }
      }
    }
  }

  test("test GpuArrowColumnarBatchBuilder does not keep the Arrow buffers it copied") {
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
    vector1.setValueCount(10)
    vector2.setValueCount(10)
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

    hostToGpuCoalesceIterator.initNewBatch(batches.head)
    hostToGpuCoalesceIterator.addBatchToConcat(batches.head)
    hostToGpuCoalesceIterator.addBatchToConcat(batches(1))

    // Close columnar batches
    batches.foreach(cb => cb.close())

    // Verify that the buffers were deallocated with the batches
    assertResult(0L)(allocator.getAllocatedMemory)

    withResource(hostToGpuCoalesceIterator.concatAllAndPutOnGPU()) { gpuBatch =>
      assertColumns(schema, Seq((0 until 10) ++ (0 until 10)), gpuBatch)
    }
    hostToGpuCoalesceIterator.cleanupConcatIsDone()
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
