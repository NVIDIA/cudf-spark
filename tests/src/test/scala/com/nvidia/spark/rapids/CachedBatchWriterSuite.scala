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

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.mutable

import ai.rapids.cudf.{ColumnVector, CompressionType, DType, Rmm, Table, TableWriter}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableFromBatchColumns
import com.nvidia.spark.rapids.parquet.{ParquetCachedBatchSerializer, ParquetOutputFileFormat}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.mockito.ArgumentMatchers.{any, isA}
import org.mockito.Mockito.{doAnswer, mock, mockStatic, spy, times, verify, when}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.PCBSSchemaHelper
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY


/**
 * Unit tests for cached batch writing
 */
class CachedBatchWriterSuite extends SparkQueryCompareTestSuite {

  class TestResources extends AutoCloseable {
    assert(Rmm.isInitialized, "Need to use this within Spark GPU session, or it may fail to " +
      "release column vector.")
    val byteCv1 = ColumnVector.fromBytes(1)
    val byteCv3 = ColumnVector.fromBytes(3)
    val byteCv456 = ColumnVector.fromBytes(4, 5, 6)

    override def close(): Unit = {
      byteCv1.close()
      byteCv3.close()
      byteCv456.close()
    }
  }

  test("convert columnar batch to cached batch on single col table with 0 rows in a batch") {
    withGpuSparkSession(_ =>
      withResource(new TestResources()) { resources =>
        val (_, spyGpuCol0) = getCudfAndGpuVectors(resources)
        val cb = new ColumnarBatch(Array(spyGpuCol0), 0)
        val ser = new ParquetCachedBatchSerializer
        val dummySchema = new StructType(
          Array(
            StructField("empty", ByteType, false),
            StructField("empty", ByteType, false),
            StructField("empty", ByteType, false)))
        val listOfPCB = ser.compressColumnarBatchWithParquet(
          cb, dummySchema, dummySchema,
          BYTES_ALLOWED_PER_BATCH, false)
        assert(listOfPCB.isEmpty)
      })
  }

  test("convert large columnar batch to cached batch on single col table") {
    withGpuSparkSession(_ =>
      withResource(new TestResources()) { resources =>
        val (spyCol0, spyGpuCol0) = getCudfAndGpuVectors(resources)
        val splitAt = 2086912
        testCompressColBatch(resources, Array(spyCol0), Array(spyGpuCol0), splitAt)
        verify(spyCol0).split(splitAt)
      })
  }

  test("convert large columnar batch to cached batch on multi-col table") {
    withGpuSparkSession(_ =>
      withResource(new TestResources()) { resources =>
        val (spyCol0, spyGpuCol0) = getCudfAndGpuVectors(resources)
        val splitAt = Seq(695637, 1391274, 2086911, 2782548)
        testCompressColBatch(resources, Array(spyCol0, spyCol0, spyCol0),
          Array(spyGpuCol0, spyGpuCol0, spyGpuCol0), splitAt: _*)
        verify(spyCol0, times(3)).split(splitAt: _*)
      })
  }

  test("convert large InternalRow iterator to cached batch single col") {
    withGpuSparkSession(_ =>
      withResource(new TestResources()) { resources =>
        val (_, spyGpuCol0) = getCudfAndGpuVectors(resources)
        val cb = new ColumnarBatch(Array(spyGpuCol0), ROWS)
        val mockByteType = mock(classOf[ByteType])
        when(mockByteType.defaultSize).thenReturn(1024)
        val schema = Seq(AttributeReference("field0", mockByteType, true)())
        testColumnarBatchToCachedBatchIterator(cb, schema)
      })
  }

  test("convert large InternalRow iterator to cached batch multi-col") {
    withGpuSparkSession(_ =>
      withResource(new TestResources()) { resources1 =>
        val (_, spyGpuCol0) = getCudfAndGpuVectors(resources1)
        withResource(new TestResources()) { resources2 =>
          val (_, spyGpuCol1) = getCudfAndGpuVectors(resources2)
          withResource(new TestResources()) { resources3 =>
            val (_, spyGpuCol2) = getCudfAndGpuVectors(resources3)
            val cb = new ColumnarBatch(Array(spyGpuCol0, spyGpuCol1, spyGpuCol2), ROWS)
            val mockByteType = mock(classOf[ByteType])
            when(mockByteType.defaultSize).thenReturn(1024)
            val schema = Seq(AttributeReference("field0", mockByteType, true)(),
              AttributeReference("field1", mockByteType, true)(),
              AttributeReference("field2", mockByteType, true)())

            testColumnarBatchToCachedBatchIterator(cb, schema)
          }
        }
      })
  }

  test("test useCompression conf is honored") {
    val ser = new ParquetCachedBatchSerializer()
    val schema = new StructType().add("value", "string")
    List(false, true).foreach { comp =>
      val opts = ser.getParquetWriterOptions(comp, schema)
      assert(
        (if (comp) {
          CompressionType.SNAPPY
        } else {
          CompressionType.NONE
        }) == opts.getCompressionType)
    }
  }

  test("cache empty columnar batch on GPU") {
    withGpuSparkSession(writeAndConsumeEmptyBatch)
  }

  test("PCBS round trip preserves nested and top-level NullType columns") {
    withGpuSparkSession { spark =>
      setActiveSession(spark)
      val ser = new ParquetCachedBatchSerializer
      val values = Array(1, 2, 3)
      val numRows = values.length
      val nestedType = StructType(Array(
        StructField("a", IntegerType, nullable = true),
        StructField("code", NullType, nullable = true)))
      val arrayType = ArrayType(NullType, containsNull = true)
      val mapType = MapType(IntegerType, NullType, valueContainsNull = true)
      // Doubly-nested array<struct<..null..>> exercises the .nested() NullType round-trip.
      val arrayOfStructType = ArrayType(nestedType, containsNull = true)
      val origAttrs: Seq[Attribute] = Seq(
        AttributeReference("properties", nestedType, nullable = true)(),
        AttributeReference("code", NullType, nullable = true)(),
        AttributeReference("arr", arrayType, nullable = true)(),
        AttributeReference("m", mapType, nullable = true)(),
        AttributeReference("arr_of_struct", arrayOfStructType, nullable = true)())
      // The serializer rewrites NullType to ByteType on disk; build that schema the way it does.
      val cachedSchema = PCBSSchemaHelper.getSupportedSchemaFromUnsupported(origAttrs).toStructType
      val origSchema = origAttrs.toStructType

      val listOfPCB = withResource(
        buildNullTypeBatch(values, nestedType, arrayType, mapType, arrayOfStructType)) {
        gpuCB => ser.compressColumnarBatchWithParquet(gpuCB, cachedSchema, origSchema,
          BYTES_ALLOWED_PER_BATCH, useCompression = false)
      }

      val conf = TrampolineUtil.getSparkConf(spark)
      val cachedRdd = spark.sparkContext.parallelize[CachedBatch](listOfPCB, numSlices = 1)
      val cbRdd = ser.convertCachedBatchToColumnarBatch(cachedRdd, origAttrs, origAttrs, conf)
      val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
      TrampolineUtil.setTaskContext(context)
      try {
        val batches = cbRdd.compute(cbRdd.partitions.head, context)
        assert(batches.hasNext)
        val cb = batches.next()
        assert(cb.numRows() == numRows)
        val structCol = cb.column(0)
        val aChild = structCol.getChild(0)
        val codeChild = structCol.getChild(1)
        val topNull = cb.column(1)
        val arrCol = cb.column(2)
        val mapCol = cb.column(3)
        val arrStructCol = cb.column(4)
        for (i <- 0 until numRows) {
          assert(!structCol.isNullAt(i))
          assert(aChild.getInt(i) == values(i))
          assert(codeChild.isNullAt(i), s"nested struct NullType child must be null at row $i")
          assert(topNull.isNullAt(i), s"top-level NullType column must be null at row $i")
          val arr = arrCol.getArray(i)
          assert(arr.numElements() == 1)
          assert(arr.isNullAt(0), s"array NullType element must be null at row $i")
          val m = mapCol.getMap(i)
          assert(m.numElements() == 1)
          assert(m.keyArray().getInt(0) == values(i))
          assert(m.valueArray().isNullAt(0), s"map NullType value must be null at row $i")
          val arrStruct = arrStructCol.getArray(i)
          assert(arrStruct.numElements() == 1)
          val innerStruct = arrStruct.getStruct(0, 2)
          assert(innerStruct.getInt(0) == values(i), s"array<struct> int child at row $i")
          assert(innerStruct.isNullAt(1),
            s"array<struct> nested NullType child must be null at row $i")
        }
        assert(!batches.hasNext)
      } finally {
        TrampolineUtil.unsetTaskContext()
        context.markTaskComplete()
      }
    }
  }

  private def writeAndConsumeEmptyBatch(spark: SparkSession): Unit = {
    setActiveSession(spark)
    val schema = Seq(AttributeReference("_col0", IntegerType, true)())
    val columnarBatch0 = FuzzerUtils.createColumnarBatch(schema.toStructType, 0)
    val columnarBatch = FuzzerUtils.createColumnarBatch(schema.toStructType, 10)
    // increase count to verify the result later
    columnarBatch.column(0).asInstanceOf[GpuColumnVector].getBase.incRefCount()
    columnarBatch0.column(0).asInstanceOf[GpuColumnVector].getBase.incRefCount()

    val rdd = spark.sparkContext.parallelize(Seq(columnarBatch, columnarBatch0))
    val storageLevel = MEMORY_ONLY
    val conf = TrampolineUtil.getSparkConf(spark)
    val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)

    // Default Serializer round trip
    val defaultSer = new DefaultCachedBatchSerializer
    val toClose = ListBuffer[ColumnarBatch]()
    val irRdd = rdd.flatMap { batch =>
      val hostBatch = if (batch.column(0).isInstanceOf[GpuColumnVector]) {
        withResource(batch) { batch =>
          new ColumnarBatch(batch.safeMap(_.copyToHost()).toArray, batch.numRows())
        }
      } else {
        batch
      }
      toClose += hostBatch
      hostBatch.rowIterator().asScala
    }

    val expectedCachedRdd = defaultSer
      .convertInternalRowToCachedBatch(irRdd, schema, storageLevel, conf)
    val expectedCbRdd = defaultSer
      .convertCachedBatchToColumnarBatch(expectedCachedRdd, schema, schema, conf)
    val defaultBatches = expectedCbRdd.compute(expectedCbRdd.partitions.head, context)

    // PCBS round trip
    val ser = new ParquetCachedBatchSerializer
    val cachedRdd = ser.convertColumnarBatchToCachedBatch(rdd, schema, storageLevel, conf)
    val cbRdd = ser.convertCachedBatchToColumnarBatch(cachedRdd, schema, schema, conf)
    TrampolineUtil.setTaskContext(context)
    try {
      val batches = cbRdd.compute(cbRdd.partitions.head, context)

      // Read the batches to consume the cache
      assert(batches.hasNext)
      assert(defaultBatches.hasNext)
      val cb = batches.next()
      val expectedCb = defaultBatches.next()
      val hostCol = cb.column(0)
      val hostColExpected = expectedCb.column(0)
      assert(cb.numRows() == 10)
      for (i <- 0 until 10) {
        assert(hostCol.isNullAt(i) == hostColExpected.isNullAt(i))
        if (!hostCol.isNullAt(i)) {
          assert(hostCol.getInt(i) == hostColExpected.getInt(i))
        }
      }
      assert(!batches.hasNext)
      assert(!defaultBatches.hasNext)
    } finally {
      toClose.foreach(cb => cb.close())
      TrampolineUtil.unsetTaskContext()
      context.markTaskComplete()
    }
  }

  val ROWS = 3 * 1024 * 1024

  private def getCudfAndGpuVectors(resources: TestResources): (ColumnVector, GpuColumnVector) = {
    val spyCol = spy(resources.byteCv1)
    when(spyCol.getRowCount).thenReturn(ROWS)
    val mockDtype = mock(classOf[DType])
    when(mockDtype.getSizeInBytes).thenReturn(1024)
    val spyGpuCol = spy(GpuColumnVector.from(spyCol, ByteType))
    when(spyCol.getDeviceMemorySize).thenReturn(1024L * ROWS)
    (spyCol, spyGpuCol)
  }

  val _2GB = 2L * 1024 * 1024 * 1024
  val APPROX_PAR_META_DATA = 10 * 1024 * 1024 // we are estimating 10MB
  val BYTES_ALLOWED_PER_BATCH = _2GB - APPROX_PAR_META_DATA

  private def whenSplitCalled(cb: ColumnarBatch, testResources: TestResources,
      splitPoints: Int*): Unit = {
    val rows = cb.numRows()
    val eachRowSize = cb.numCols() * 1024
    val rowsAllowedInABatch = BYTES_ALLOWED_PER_BATCH / eachRowSize
    val spillOver = cb.numRows() % rowsAllowedInABatch
    val splitRange = scala.Range(rowsAllowedInABatch.toInt, rows, rowsAllowedInABatch.toInt)
    scala.Range(0, cb.numCols()).indices.foreach { i =>
      val spyCol = cb.column(i).asInstanceOf[GpuColumnVector].getBase
      val splitCols0 = splitRange.indices.map { _ =>
        val spySplitCol = spy(testResources.byteCv456)
        when(spySplitCol.getRowCount()).thenReturn(rowsAllowedInABatch)
        spySplitCol
      }
      val splitCols = if (spillOver > 0) {
        val splitCol = spy(testResources.byteCv3)
        when(splitCol.getRowCount()).thenReturn(spillOver)
        splitCols0 :+ splitCol
      } else {
        splitCols0
      }

      // copy splitCols because ParquetCachedBatchSerializer.compressColumnarBatchWithParquet is
      // responsible to close the copied splits
      doAnswer(_ => copyOf(splitCols)).when(spyCol).split(splitPoints: _*)
    }
  }

  def copyOf(in: Seq[ColumnVector]): Array[ColumnVector] = {
    val buffers = ArrayBuffer[ColumnVector]()
    in.foreach(e => buffers += e.copyToColumnVector())
    buffers.toArray
  }

  def checkSize(table: Table): Unit = {
    val tableSize = table.getColumn(0).getType.getSizeInBytes * table.getRowCount
    if (tableSize > Int.MaxValue) {
      fail(s"Parquet file went over the allowed limit of $BYTES_ALLOWED_PER_BATCH")
    }
  }

  private def testCompressColBatch(
     testResources: TestResources,
     cudfCols: Array[ColumnVector],
     gpuCols: Array[org.apache.spark.sql.vectorized.ColumnVector], splitAt: Int*): Unit = {
    // mock static method for Table
    val theTableMock = mockStatic(classOf[Table], (_: InvocationOnMock) => {
      val ret = mock(classOf[TableWriter])
      doAnswer( invocation =>
        checkSize(invocation.getArgument(0, classOf[Table]))
      ).when(ret).write(isA(classOf[Table]))
      ret
    })
    val cb = new ColumnarBatch(gpuCols, ROWS)
    try {
      whenSplitCalled(cb, testResources, splitAt: _*)
      val ser = new ParquetCachedBatchSerializer
      val dummySchema = new StructType(
        gpuCols.map( _ => StructField("empty", ByteType, false)))
      ser.compressColumnarBatchWithParquet(cb, dummySchema, dummySchema,
        BYTES_ALLOWED_PER_BATCH, false)
    } finally {
      theTableMock.close()
      gpuCols(0).close()
    }
  }

  private def testColumnarBatchToCachedBatchIterator(
      cb: ColumnarBatch,
      schema: Seq[AttributeReference]): Unit = {

    val cbIter = new Iterator[ColumnarBatch] {
      val queue = new mutable.Queue[ColumnarBatch]
      queue += cb

      override def hasNext: Boolean = queue.nonEmpty

      override def next(): ColumnarBatch = {
        queue.dequeue()
      }
    }
    val ser = new ParquetCachedBatchSerializer

    val producer = new ser.CachedBatchIteratorProducer[ColumnarBatch](cbIter, schema, schema,
      withCpuSparkSession(spark => spark.sparkContext.broadcast(new SQLConf().getAllConfs)))
    val mockParquetOutputFileFormat = mock(classOf[ParquetOutputFileFormat])
    var totalSize = 0L
    val mockRecordWriter = new RecordWriter[Void, InternalRow] {
      val estimatedSize = schema.indices.length * 1024
      var thisBatchSize = 0L

      override def write(k: Void, v: InternalRow): Unit = {
        thisBatchSize += estimatedSize
        if (thisBatchSize > BYTES_ALLOWED_PER_BATCH) {
          fail(s"Parquet file went over the allowed limit of $BYTES_ALLOWED_PER_BATCH")
        }
      }

      override def close(taskAttemptContext: TaskAttemptContext): Unit = {
        totalSize += thisBatchSize
        thisBatchSize = 0
      }
    }
    when(mockParquetOutputFileFormat.getRecordWriter(any(), any())).thenReturn(mockRecordWriter)
    withResource(producer.getColumnarBatchToCachedBatchIterator
        .asInstanceOf[producer.ColumnarBatchToCachedBatchIterator]) {
      cachedBatchIter =>
        cachedBatchIter.setParquetOutputFileFormat(mockParquetOutputFileFormat)
        val totalRows = cachedBatchIter.foldLeft(0)(_ + _.numRows)
        assert(totalRows == ROWS)
    }
    assert(totalSize == ROWS * schema.indices.length * 1024L)
  }

  private def buildNullTypeBatch(values: Array[Int], nestedType: StructType,
      arrayType: ArrayType, mapType: MapType, arrayOfStructType: ArrayType): ColumnarBatch = {
    val numRows = values.length
    val cols = new ArrayBuffer[GpuColumnVector]()
    closeOnExcept(cols) { buf =>
      // The code child is an all-null INT8 column (cudf models NullType as INT8).
      buf += withResource(ColumnVector.fromInts(values: _*)) { aChild =>
        withResource(GpuColumnVector.columnVectorFromNull(numRows, NullType)) { codeChild =>
          GpuColumnVector.from(
            ColumnVector.makeStruct(numRows.toLong, aChild, codeChild), nestedType)
        }
      }
      buf += GpuColumnVector.fromNull(numRows, NullType)
      // array<null>: makeList makes one entry per input column, so each row is a 1-element [null].
      buf += withResource(GpuColumnVector.columnVectorFromNull(numRows, NullType)) { elem =>
        GpuColumnVector.from(ColumnVector.makeList(elem), arrayType)
      }
      // map<int,null> is list<struct<key,value>> in cudf: one entry/row, int key -> null value.
      buf += withResource(ColumnVector.fromInts(values: _*)) { keyChild =>
        withResource(GpuColumnVector.columnVectorFromNull(numRows, NullType)) { valChild =>
          withResource(ColumnVector.makeStruct(numRows.toLong, keyChild, valChild)) { entry =>
            GpuColumnVector.from(ColumnVector.makeList(entry), mapType)
          }
        }
      }
      // One struct entry per row; its code child is all-null INT8 (NullType two levels deep).
      buf += withResource(ColumnVector.fromInts(values: _*)) { aChild =>
        withResource(GpuColumnVector.columnVectorFromNull(numRows, NullType)) { codeChild =>
          withResource(ColumnVector.makeStruct(numRows.toLong, aChild, codeChild)) { structElem =>
            GpuColumnVector.from(ColumnVector.makeList(structElem), arrayOfStructType)
          }
        }
      }
      new ColumnarBatch(buf.toArray, numRows)
    }
  }
}
