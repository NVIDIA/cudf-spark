/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{HostMemoryBuffer, Schema, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuTextBasedPartitionReader.LineDelimitedReadChunk
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.json.rapids.JsonPartitionReader
import org.apache.spark.sql.types._

class JsonScanRetrySuite extends RmmSparkRetrySuiteBase {
  private val dataSchema = StructType(Seq(
    StructField("a", IntegerType), StructField("b", StringType)))
  private val parsedOptions = new JSONOptions(Map.empty[String, String], "UTC", "_corrupt_record")

  private def collectTableRows(table: Table): Seq[(Option[Int], Option[String])] = {
    withResource(table.getColumn(0).copyToHost()) { a =>
      withResource(table.getColumn(1).copyToHost()) { b =>
        (0 until table.getRowCount.toInt).map { i =>
          (if (a.isNull(i)) None else Some(a.getInt(i)),
            if (b.isNull(i)) None else Some(b.getJavaString(i)))
        }
      }
    }
  }

  test("JSON retries without splitting and preserves values") {
    withReader("""{"a":1,"b":"value é"}""" + "\n", dataSchema, 1024 * 1024) { reader =>
      RmmSpark.getAndResetNumRetryThrow(1)
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val (actual, batches) = collectReaderRowsAndBatchCount(reader)
      assert(actual == Seq((Some(1), Some("value é"))))
      assert(batches == 1)
      assert(RmmSpark.getAndResetNumRetryThrow(1) > 0)
    }
  }

  test("JSON split preserves malformed rows and skips empty lines") {
    val lines = Seq("""{"a":0,"b":"value"}""", "not json", "", " \t",
      """{"a":2,"b":null}""", """{"b":"missing"}""")
    withReader(lines.mkString("", "\n", "\n"), dataSchema, 1024 * 1024) { reader =>
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 2,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assert(collectReaderRows(reader) == Seq((Some(0), Some("value")),
        (None, None), (Some(2), None), (None, Some("missing"))))
    }
  }

  test("JSON split preserves a chunk containing only a malformed record") {
    val value = "long" * 40
    val text = Seq(s"""{"a":0,"b":"$value"}""", "not json").mkString("", "\n", "\n")
    withReader(text, dataSchema, 1024 * 1024) { reader =>
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val (actual, batches) = collectReaderRowsAndBatchCount(reader)
      assert(actual == Seq((Some(0), Some(value)), (None, None)))
      assert(batches == 2)
    }
  }

  test("JSON single record split failure is terminal") {
    withReader("""{"a":1,"b":"one"}""" + "\n", dataSchema, 1024 * 1024) { reader =>
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val error = intercept[IOException](reader.next())
      assert(error.getCause.isInstanceOf[GpuSplitAndRetryOOM])
      assert(!reader.next())
    }
  }

  test("closing JSON retry input releases pending slices without parsing them") {
    val bytes = (0 until 4).map(i => s"""{"a":$i,"b":"value"}""" + "\n")
      .mkString.getBytes(StandardCharsets.UTF_8)
    val buffer = HostMemoryBuffer.allocate(bytes.length)
    buffer.setBytes(0, bytes, 0, bytes.length)
    var attempts = 0
    val chunks = RmmRapidsRetryIterator.withRetry(LineDelimitedReadChunk(buffer),
      (chunk: LineDelimitedReadChunk) => chunk.split()) { chunk =>
      attempts += 1
      if (attempts == 1) throw new GpuSplitAndRetryOOM("split for test")
      chunk.buffer.getLength
    }
    withResource(chunks) { _ =>
      assert(chunks.next() < bytes.length)
      assert(chunks.hasNext)
    }
    assert(attempts == 2)
    assert(buffer.getRefCount == 0)
  }

  private def withReader[T](text: String, readSchema: StructType, maxBytes: Long,
      options: Map[String, String] = Map.empty,
      wrapTables: Iterator[Table] => Iterator[Table] = identity[Iterator[Table]] _)
      (fn: JsonPartitionReader => T): T = {
    val file = Files.createTempFile("json-reader-retry", ".json")
    Files.write(file, text.getBytes(StandardCharsets.UTF_8))
    try {
      withResource(new JsonPartitionReader(new Configuration(),
        PartitionedFileUtilsShim.newPartitionedFile(
          InternalRow.empty, file.toString, 0, Files.size(file)),
        dataSchema, readSchema,
        new JSONOptions(options, "UTC", "_corrupt_record"), 1024, maxBytes,
        Map[String, GpuMetric]().withDefaultValue(NoopMetric)) {
        override protected def readToTables(
            dataBufferer: HostLineBufferer,
            cudfDataSchema: Schema,
            readDataSchema: StructType,
            cudfReadDataSchema: Schema,
            isFirstChunk: Boolean,
            decodeTime: GpuMetric): Iterator[Table] = {
          wrapTables(super.readToTables(dataBufferer, cudfDataSchema, readDataSchema,
            cudfReadDataSchema, isFirstChunk, decodeTime))
        }
      })(fn)
    } finally {
      Files.deleteIfExists(file)
    }
  }

  private def collectReaderRowsAndBatchCount(reader: JsonPartitionReader)
      : (Seq[(Option[Int], Option[String])], Int) = {
    val rows = ArrayBuffer[(Option[Int], Option[String])]()
    var batches = 0
    while (reader.next()) {
      withResource(reader.get()) { batch =>
        withResource(GpuColumnVector.from(batch)) { table =>
          rows ++= collectTableRows(table)
        }
      }
      batches += 1
    }
    (rows.toSeq, batches)
  }

  private def collectReaderRows(reader: JsonPartitionReader)
      : Seq[(Option[Int], Option[String])] = {
    collectReaderRowsAndBatchCount(reader)._1
  }

  for (maxBytes <- Seq(1L, 1024L * 1024); emptyOnly <- Seq(false, true)) {
    test(s"JSON reader skips repeated empty root arrays: bytes=$maxBytes, emptyOnly=$emptyOnly") {
      val emptyArrays = Seq("[]", " [ \t ] ", "[]")
      val lines = if (emptyOnly) emptyArrays else {
        Seq("""{"a":0,"b":"first"}""") ++ emptyArrays :+
          """{"a":1,"b":"last"}"""
      }
      val expected = if (emptyOnly) Seq.empty else {
        Seq((Some(0), Some("first")), (Some(1), Some("last")))
      }
      withReader(lines.mkString("", "\n", "\n"), dataSchema, maxBytes) { reader =>
        assert(collectReaderRows(reader) == expected)
      }
    }
  }

  for (encoding <- Seq(None, Some("UTF-8"), Some("US-ASCII"));
      maxBytes <- Seq(1L, 1024L * 1024)) {
    test(s"JSON reader handles BOM-prefixed empty arrays: encoding=$encoding, bytes=$maxBytes") {
      val lines = Seq("""{"a":0,"b":"first"}""", "[]", "\uFEFF[]",
        "\uFEFF \t[ \t ]", " \uFEFF[]", """{"a":1,"b":"last"}""")
      val options = encoding.map(e => Map("encoding" -> e, "lineSep" -> "\n"))
        .getOrElse(Map.empty[String, String])
      val invalidRows = Seq.fill(if (encoding.isEmpty) 1 else 3)((None, None))
      val expected = Seq((Some(0), Some("first"))) ++ invalidRows :+
        ((Some(1), Some("last")))
      withReader(lines.mkString("", "\n", "\n"), dataSchema, maxBytes, options) { reader =>
        assert(collectReaderRows(reader) == expected)
      }
    }
  }

  for (maxBytes <- Seq(1L, 1024L * 1024)) {
    test(s"JSON reader preserves null recovery for BOM-prefixed nonempty arrays: bytes=$maxBytes") {
      val lines = Seq("""{"a":0,"b":"first"}""",
        "\uFEFF" + """[{"a":1,"b":"array"}]""",
        "\uFEFF \t" + """[{"a":2,"b":"array"}]""",
        """{"a":3,"b":"last"}""")
      // Preserve the existing GPU recovery result until root-array expansion is supported.
      val expected = Seq((Some(0), Some("first")), (None, None), (None, None),
        (Some(3), Some("last")))
      withReader(lines.mkString("", "\n", "\n"), dataSchema, maxBytes) { reader =>
        assert(collectReaderRows(reader) == expected)
      }
    }
  }

  test("JSON reader recursively splits escaped values after filtering empty arrays") {
    val records = Seq("""{"a":0,"b":"first\n\"quoted\" é"}""") ++
      (1 until 16).map(i => s"""{"a":$i,"b":"value,$i é"}""")
    val lines = records.flatMap(record => Seq(record, "[]", "\uFEFF [ \t ]"))
    withReader(lines.mkString("", "\n", "\n"), dataSchema, 1024 * 1024) { reader =>
      RmmSpark.getAndResetNumSplitRetryThrow(1)
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 2,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val (actual, batches) = collectReaderRowsAndBatchCount(reader)
      val expected = Seq((Some(0), Some("first\n\"quoted\" é"))) ++
        (1 until 16).map(i => (Some(i), Some(s"value,$i é")))
      assert(actual == expected)
      assert(batches >= 3)
      assert(RmmSpark.getAndResetNumSplitRetryThrow(1) >= 2)
    }
  }

  for (values <- Seq(Seq("long" * 40, "short"), Seq("short", "long" * 40))) {
    test(s"JSON reader splits around a large record and empty arrays: ${values.head.length}") {
      val lines = values.zipWithIndex.flatMap { case (value, i) =>
        Seq(s"""{"a":$i,"b":"$value"}""", "[]", "[]")
      }
      withReader(lines.mkString("", "\n", "\n"), dataSchema, 1024 * 1024) { reader =>
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        val (actual, batches) = collectReaderRowsAndBatchCount(reader)
        assert(actual == values.zipWithIndex.map { case (v, i) => (Some(i), Some(v)) })
        assert(batches == 2)
      }
    }
  }

  for ((line, name) <- Seq(("[", "truncated"), ("[bad", "invalid token"),
      ("[\u0000", "invalid control character"))) {
    test(s"JSON reader retains malformed array candidates: $name") {
      withReader(line + "\n", dataSchema, 1024 * 1024) { reader =>
        assert(collectReaderRows(reader) == Seq((None, None)))
      }
    }
  }

  for (encoding <- Seq("UTF-8", "US-ASCII")) {
    test(s"JSON reader retains UTF-32-looking invalid arrays with explicit $encoding") {
      val line = "[\u0000\u0000\u0000]\u0000\u0000\u0000\n"
      val options = Map("encoding" -> encoding, "lineSep" -> "\n")
      withReader(line, dataSchema, 1024 * 1024, options) { reader =>
        assert(collectReaderRows(reader) == Seq((None, None)))
      }
    }
  }

  for (encoding <- Seq(None, Some("UTF-8"), Some("US-ASCII"));
      maxBytes <- Seq(1L, 1024L * 1024);
      forceSplit <- Seq(false, true) if maxBytes > 1 || !forceSplit) {
    test(s"JSON BOM handling matches encoding: $encoding, bytes=$maxBytes, split=$forceSplit") {
      val lines = Seq("\uFEFF" + """{"a":0,"b":"first"}""",
        "\uFEFF" + """{"a":1,"b":"one"}""",
        "\uFEFF \t" + """{"a":2,"b":"two"}""",
        " \uFEFF" + """{"a":3,"b":"invalid"}""",
        "\uFEFF\uFEFF" + """{"a":4,"b":"invalid"}""",
        "\uFEFF", "\uFEFF \t")
      val quotedBom = if (encoding.contains("US-ASCII")) Seq.empty else {
        Seq("""{"a":5,"b":"""" + "\uFEFFquoted\"}")
      }
      val text = (lines ++ quotedBom :+ """{"a":6,"b":"last"}""").mkString("", "\n", "\n")
      val options = encoding.map(e => Map("encoding" -> e, "lineSep" -> "\n"))
        .getOrElse(Map.empty[String, String])
      val bomRows = if (encoding.isEmpty) {
        Seq((Some(1), Some("one")), (Some(2), Some("two")),
          (None, None), (None, None), (None, None))
      } else Seq.fill(6)((None, None))
      val quotedRows = if (quotedBom.isEmpty) Seq.empty else Seq((Some(5), Some("\uFEFFquoted")))
      val expected = Seq((Some(0), Some("first"))) ++ bomRows ++ quotedRows :+
        ((Some(6), Some("last")))
      withReader(text, dataSchema, maxBytes, options) { reader =>
        if (forceSplit) {
          RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 2,
            RmmSpark.OomInjectionType.GPU.ordinal, 0)
        }
        assert(collectReaderRows(reader) == expected)
      }
    }
  }

  for (readSchema <- Seq(dataSchema, StructType(Seq(StructField("b", StringType))),
      StructType(Seq.empty))) {
    test(s"JSON reader drains all split outputs and applies projection: $readSchema") {
      val text = (0 until 16).map(i => s"""{"a":$i,"b":"value,$i é"}""" + "\n").mkString
      withReader(text, readSchema, 1024 * 1024) { reader =>
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 2,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        var rows = 0
        var batches = 0
        val values = ArrayBuffer[String]()
        while (reader.next()) {
          withResource(reader.get()) { batch =>
            assert(batch.numCols() == readSchema.length)
            rows += batch.numRows()
            if (readSchema.nonEmpty) {
              withResource(GpuColumnVector.from(batch)) { table =>
                val columnIndex = if (readSchema.length == 2) 1 else 0
                withResource(table.getColumn(columnIndex).copyToHost()) { host =>
                  (0 until batch.numRows()).foreach(i => values += host.getJavaString(i))
                }
              }
            }
          }
          batches += 1
        }
        assert(rows == 16)
        assert(batches >= 3)
        if (readSchema.nonEmpty) assert(values.toSeq == (0 until 16).map(i => s"value,$i é"))
      }
    }
  }

  test("closing JSON reader closes pending split input without consuming it") {
    var hasNextCalls = 0
    var nextCalls = 0
    var closeCalls = 0
    val text = (0 until 16).map(i => s"""{"a":$i,"b":"value"}""" + "\n").mkString
    withReader(text, dataSchema, 1024 * 1024, wrapTables = tables => {
      new Iterator[Table] with AutoCloseable {
        override def hasNext: Boolean = {
          hasNextCalls += 1
          tables.hasNext
        }
        override def next(): Table = {
          nextCalls += 1
          tables.next()
        }
        override def close(): Unit = {
          closeCalls += 1
          withResource(tables.asInstanceOf[AutoCloseable]) { _ => () }
        }
      }
    }) { reader =>
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 2,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assert(reader.next())
      withResource(reader.get()) { batch => assert(batch.numRows() < 16) }
      val hasNextBeforeClose = hasNextCalls
      reader.close()
      reader.close()
      assert(hasNextCalls == hasNextBeforeClose)
      assert(nextCalls == 1)
      assert(closeCalls == 1)
      assert(!reader.next())
    }
  }

  test("JSON preserves input byte batching without OOM") {
    val text = (0 until 16).map(i => s"""{"a":$i,"b":"value"}""" + "\n").mkString
    withReader(text, dataSchema, 1024 * 1024) { reader =>
      assert(reader.next())
      withResource(reader.get()) { batch => assert(batch.numRows() == 16) }
      assert(!reader.next())
    }
    withReader(text, dataSchema, 16) { reader =>
      var rows = 0
      var batches = 0
      while (reader.next()) {
        withResource(reader.get()) { batch => rows += batch.numRows() }
        batches += 1
      }
      assert(rows == 16)
      assert(batches > 1)
    }
  }

  test("JSON readToTable remains compatible with retry without splitting") {
    withResource(FilterEmptyHostLineBuffererFactory.createBufferer(100,
      Array('\n'.toByte))) { bufferer =>
      val bytes = """{"a":1,"b":"value"}""".getBytes(StandardCharsets.UTF_8)
      bufferer.add(bytes, 0, bytes.length)
      val cudfSchema = GpuJsonReadCommon.makeSchema(dataSchema)
      val opts = GpuJsonReadCommon.baseCudfJsonOptionsBuilder().withLines(true).build()
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      withResource(JsonPartitionReader.readToTable(bufferer, cudfSchema, NoopMetric,
        opts, "JSON", null)) { table =>
        withResource(GpuJsonReadCommon.convertTableToDesiredType(table,
          dataSchema, parsedOptions)) { columns =>
          withResource(new Table(columns: _*)) { typed =>
            assert(collectTableRows(typed) == Seq((Some(1), Some("value"))))
          }
        }
      }
    }
  }
}
