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

package org.apache.spark.sql.rapids

import java.io.File
import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.concurrent.duration._

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * This is intended to give us a place to verify that we can cover all
 * of the allowed parquet formats as described in
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
 * A lot of this testing code is based off of similar Spark tests.
 */
class ParquetFormatScanSuite extends SparkQueryCompareTestSuite with Eventually {
  implicit class RecordConsumerDSL(consumer: RecordConsumer) {
    def message(f: => Unit): Unit = {
      consumer.startMessage()
      f
      consumer.endMessage()
    }

    def group(f: => Unit): Unit = {
      consumer.startGroup()
      f
      consumer.endGroup()
    }

    def field(name: String, index: Int)(f: => Unit): Unit = {
      consumer.startField(name, index)
      f
      consumer.endField(name, index)
    }
  }

  /**
   * Waits for all tasks on all executors to be finished.
   */
  protected def waitForTasksToFinish(spark: SparkSession): Unit = {
    eventually(timeout(10.seconds)) {
      assert(spark.sparkContext.statusTracker
          .getExecutorInfos.map(_.numRunningTasks()).sum == 0)
    }
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withTempDir(spark: SparkSession)(f: File => Unit): Unit = {
    val dir = Utils.createTempDir()
    try {
      f(dir)
      waitForTasksToFinish(spark)
    } finally {
      Utils.deleteRecursively(dir)
    }
  }

  /**
   * A testing Parquet [[WriteSupport]] implementation used to write manually constructed Parquet
   * records with arbitrary structures.
   */
  private class DirectWriteSupport(schema: MessageType, metadata: Map[String, String])
      extends WriteSupport[RecordConsumer => Unit] {

    private var recordConsumer: RecordConsumer = _

    override def init(configuration: Configuration): WriteContext = {
      new WriteContext(schema, metadata.asJava)
    }

    override def write(recordWriter: RecordConsumer => Unit): Unit = {
      recordWriter.apply(recordConsumer)
    }

    override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
      this.recordConsumer = recordConsumer
    }
  }

  /**
   * Writes arbitrary messages conforming to a given `messageType` to a Parquet file located by
   * `path`. Records are produced by `recordWriters`.
   */
  def writeDirectMessage(path: String,
      messageType: MessageType,
      recordWriters: (RecordConsumer => Unit)*): Unit = {
    writeDirect(path, messageType, Map.empty[String, String], recordWriters: _*)
  }

  /**
   * Writes arbitrary messages conforming to a given `messageType` to a Parquet file located by
   * `path` with given user-defined key-value `metadata`. Records are produced by `recordWriters`.
   */
  // Spark 4.2 deprecates this Builder(Path) constructor, but older supported versions require it.
  @scala.annotation.nowarn("cat=deprecation")
  def writeDirect(
      path: String,
      messageType: MessageType,
      metadata: Map[String, String],
      recordWriters: (RecordConsumer => Unit)*): Unit = {
    val testWriteSupport = new DirectWriteSupport(messageType, metadata)
    /**
     * Provide a builder for constructing a parquet writer - after PARQUET-248 directly constructing
     * the writer is deprecated and should be done through a builder. The default builders include
     * Avro - but for raw Parquet writing we must create our own builder.
     */
    class ParquetWriterBuilder() extends
        ParquetWriter.Builder[RecordConsumer => Unit, ParquetWriterBuilder](new Path(path)) {
      override def getWriteSupport(conf: Configuration) = testWriteSupport

      override def self() = this
    }
    val parquetWriter = new ParquetWriterBuilder().build()
    try recordWriters.foreach(parquetWriter.write) finally parquetWriter.close()
  }

  /**
   * Writes arbitrary messages conforming to a given `schema` to a Parquet file located by `path`.
   * Records are produced by `recordWriters`.
   */
  def writeDirect(path: String, schema: String, recordWriters: (RecordConsumer => Unit)*): Unit = {
    writeDirect(path, schema, Map.empty[String, String], recordWriters: _*)
  }

  /**
   * Writes arbitrary messages conforming to a given `schema` to a Parquet file located by `path`
   * with given user-defined key-value `metadata`. Records are produced by `recordWriters`.
   */
  def writeDirect(
      path: String,
      schema: String,
      metadata: Map[String, String],
      recordWriters: (RecordConsumer => Unit)*): Unit = {
    val messageType: MessageType = MessageTypeParser.parseMessageType(schema)
    writeDirect(path, messageType, metadata, recordWriters: _*)
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case bd: java.math.BigDecimal => BigDecimal(bd)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] => seq.map {
        case b: java.lang.Byte => b.byteValue
        case s: java.lang.Short => s.shortValue
        case i: java.lang.Integer => i.intValue
        case l: java.lang.Long => l.longValue
        case f: java.lang.Float => f.floatValue
        case d: java.lang.Double => d.doubleValue
        case x => x
      }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
    answer.map(prepareRow)
  }

  private def compare(obj1: Any, obj2: Any): (Boolean, String) = (obj1, obj2) match {
    case (null, null) => (true, "")
    case (null, _) => (false, s"NULL NOT EQUAL NULL != $obj2/(${obj2.getClass})")
    case (_, null) => (false, s"NULL NOT EQUAL $obj1/(${obj1.getClass}) != NULL")
    case (a: Array[_], b: Array[_]) =>
      if (a.length != b.length) {
        (false, s"LENGTHS NOT EQUAL ${a.length} ${b.length}\n${a.toList}\n${b.toList}")
      } else {
        val problems = a.zip(b).map { case (l, r) => compare(l, r) }.filter(!_._1).map(_._2)
        if (problems.isEmpty) {
          (true, "")
        } else {
          (false, problems.mkString("\n"))
        }
      }
    case (a: Map[_, _], b: Map[_, _]) =>
      if (a.size != b.size) {
        (false, s"LENGTHS NOT EQUAL ${a.size} ${b.size}\n${a.toList}\n${b.toList}")
      } else {
        val problems = a.keys.flatMap { aKey =>
          b.keys.find(bKey => compare(aKey, bKey)._1)
              .map(bKey => compare(a(aKey), b(bKey)))
        }.filter(!_._1).map(_._2)
        if (problems.isEmpty) {
          (true, "")
        } else {
          (false, problems.mkString("\n"))
        }
      }
    case (a: Iterable[_], b: Iterable[_]) =>
      if (a.size != b.size) {
        (false, s"LENGTHS NOT EQUAL ${a.size} ${b.size}\n${a.toList}\n${b.toList}")
      } else {
        val problems = a.zip(b).map { case (l, r) => compare(l, r) }.filter(!_._1).map(_._2)
        if (problems.isEmpty) {
          (true, "")
        } else {
          (false, problems.mkString("\n"))
        }
      }
    case (a: Product, b: Product) =>
      compare(a.productIterator.toSeq, b.productIterator.toSeq)
    case (a: Row, b: Row) =>
      compare(a.toSeq, b.toSeq)
    // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
    case (a: Double, b: Double) =>
      if (java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)) {
        (true, "")
      } else {
        (false, s"DOUBLES NOT EQUAL $a $b")
      }
    case (a: Float, b: Float) =>
      if (java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)) {
        (true, "")
      } else {
        (false, s"FLOATS NOT EQUAL $a $b")
      }
    case (a, b) =>
      if (a == b) {
        (true, "")
      } else {
        (false, s"VALUES NOT EQUAL $a $b")
      }
  }

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row]): Unit = {
    val areEqual = compare(prepareAnswer(expectedAnswer), prepareAnswer(sparkAnswer))
    assert(areEqual._1, areEqual._2)
  }

  Seq("NATIVE", "JAVA").foreach { parserType =>
    val conf = new SparkConf()
        .set("spark.rapids.sql.format.parquet.reader.footer.type", parserType)

    test(s"STRING $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required BINARY str_test (UTF8);
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/STRING_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("str_test", 0) {
                rc.addBinary(Binary.fromString("TEST"))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("str_test", 0) {
                rc.addBinary(Binary.fromString("STRING"))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row("TEST"), Row("STRING")), data)
        }
      }, conf = conf)
    }

    test(s"ENUM $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required BINARY enum_test (ENUM);
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/ENUM_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("enum_test", 0) {
                rc.addBinary(Binary.fromString("A"))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("enum_test", 0) {
                rc.addBinary(Binary.fromString("B"))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row("A"), Row("B")), data)
        }
      }, conf = conf)
    }

    test(s"JSON $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required BINARY json_test (JSON);
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/JSON_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("json_test", 0) {
                rc.addBinary(Binary.fromString("{}"))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("json_test", 0) {
                rc.addBinary(Binary.fromString("{\"a\": 100}"))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row("{}"), Row("{\"a\": 100}")), data)
        }
      }, conf = conf)
    }

    test(s"BINARY $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required BINARY bin_test;
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/BIN_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("bin_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(Array[Byte](1, 2, 3)))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("bin_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(Array[Byte](4, 5, 6)))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Array[Byte](1, 2, 3)), Row(Array[Byte](4, 5, 6))), data)
        }
      }, conf = conf)
    }

    test(s"FIXED_LEN_BYTE_ARRAY(16) BINARY $parserType") {
      assume(isSpark340OrLater)
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required fixed_len_byte_array(16) test;
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/FIXED_BIN16_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("test", 0) {
                rc.addBinary(Binary.fromString("1234567890123456"))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("test", 0) {
                rc.addBinary(Binary.fromString("ABCDEFGHIJKLMNOP"))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row("1234567890123456".getBytes),
            Row("ABCDEFGHIJKLMNOP".getBytes)), data)
        }
      }, conf = conf)
    }

    test(s"FIXED_LEN_BYTE_ARRAY(16) binaryAsString $parserType") {
      assume(isSpark340OrLater)
      // Parquet does not let us tag a FIXED_LEN_BYTE_ARRAY with utf8 to make it a string
      // Spark ignores binaryAsString for it, so we need to test that we do the same
      val conf = new SparkConf()
          .set("spark.rapids.sql.format.parquet.reader.footer.type", parserType)
          .set("spark.sql.parquet.binaryAsString", "true")
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required fixed_len_byte_array(16) test;
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/FIXED_BIN16_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("test", 0) {
                rc.addBinary(Binary.fromString("1234567890123456"))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("test", 0) {
                rc.addBinary(Binary.fromString("ABCDEFGHIJKLMNOP"))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row("1234567890123456".getBytes),
            Row("ABCDEFGHIJKLMNOP".getBytes)), data)
        }
      }, conf = conf)
    }

    test(s"FIXED_LEN_BYTE_ARRAY(16) String in Schema $parserType") {
      assume(isSpark340OrLater)
      // Parquet does not let us tag a FIXED_LEN_BYTE_ARRAY with utf8 to make it a string
      // Spark also fails the task if we try to read it as a String so we should verify that
      // We also throw an exception.
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required fixed_len_byte_array(16) test;
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/FIXED_BIN16_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("test", 0) {
                rc.addBinary(Binary.fromString("1234567890123456"))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("test", 0) {
                rc.addBinary(Binary.fromString("ABCDEFGHIJKLMNOP"))
              }
            }
          })

          try {
            spark.read.schema(StructType(Seq(StructField("test", StringType))))
                .parquet(testPath).collect()
            fail("We read back in some data, but we expected an exception...")
          } catch {
            case _: SparkException =>
              // It would be nice to verify that the exception is what we expect, but we are not
              //  doing CPU vs GPU so we will just doing a GPU pass here.
          }
        }
      }, conf = conf)
    }

    test(s"BSON $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required BINARY bson_test (BSON);
            |}
        """.stripMargin

        // Yes this is not actually valid BSON data
        withTempDir(spark) { dir =>
          val testPath = dir + "/BSON_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("bson_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(Array[Byte](1, 2, 3)))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("bson_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(Array[Byte](4, 5, 6)))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Array[Byte](1, 2, 3)), Row(Array[Byte](4, 5, 6))), data)
        }
      }, conf = conf)
    }

    // Older parquet JAVA code does not support UUID at all, and on the newer versions that do
    // support Spark does not support the UUID type.

    test(s"int32 $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int32 byte_test (INT_8);
            |  required int32 short_test (INT_16);
            |  required int32 int_test;
            |  required int32 int_test2 (INT_32);
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/INT32_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("byte_test", 0) {
                rc.addInteger(22)
              }
              rc.field("short_test", 1) {
                rc.addInteger(300)
              }
              rc.field("int_test", 2) {
                rc.addInteger(1)
              }
              rc.field("int_test2", 3) {
                rc.addInteger(3)
              }
            }
          }, { rc =>
            rc.message {
              rc.field("byte_test", 0) {
                rc.addInteger(-21)
              }
              rc.field("short_test", 1) {
                rc.addInteger(-301)
              }
              rc.field("int_test", 2) {
                rc.addInteger(-2)
              }
              rc.field("int_test2", 3) {
                rc.addInteger(-4)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(22, 300, 1, 3),
            Row(-21, -301, -2, -4)), data)
        }
      }, conf = conf)
    }

    test(s"int32 - unsigned $parserType") {
      assumeSpark320orLater
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int32 ubyte_test (UINT_8);
            |  required int32 ushort_test (UINT_16);
            |  required int32 uint_test (UINT_32);
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/UINT32_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("ubyte_test", 0) {
                rc.addInteger(22)
              }
              rc.field("ushort_test", 1) {
                rc.addInteger(300)
              }
              rc.field("uint_test", 2) {
                rc.addInteger(1)
              }
            }
          }, { rc =>
            rc.message {
              rc.field("ubyte_test", 0) {
                rc.addInteger(21)
              }
              rc.field("ushort_test", 1) {
                rc.addInteger(301)
              }
              rc.field("uint_test", 2) {
                rc.addInteger(2)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(22, 300, 1),
            Row(21, 301, 2)), data)
        }
      }, conf = conf)
    }

    test(s"int32 - int8 OVERFLOW $parserType") {
      // This is not a part of the spec. I just wanted to see if we were doing the same thing
      // as spark in this odd corner case, and it looks like we are...
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int32 byte_test (INT_8);
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/INT32_INT8_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("byte_test", 0) {
                rc.addInteger(Byte.MinValue - 10)
              }
            }
          }, { rc =>
            rc.message {
              rc.field("byte_test", 0) {
                rc.addInteger(Short.MaxValue)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          // Spark is returning a -1 for this case...
          sameRows(Seq(Row(118), Row(-1)), data)
        }
      }, conf = conf)
    }

    test(s"int64 $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int64 int_test;
            |  required int64 int_test2 (INT_64);
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/INT64_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addLong(22)
              }
              rc.field("int_test2", 1) {
                rc.addLong(300)
              }
            }
          }, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addLong(-21)
              }
              rc.field("int_test2", 1) {
                rc.addLong(-301)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(22, 300),
            Row(-21, -301)), data)
        }
      }, conf = conf)
    }

    test(s"int64 - unsigned $parserType") {
      assumeSpark320orLater
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int64 uint_test (UINT_64);
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/UINT64_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("uint_test", 0) {
                rc.addLong(22)
              }
            }
          }, { rc =>
            rc.message {
              rc.field("uint_test", 0) {
                rc.addLong(21)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(22), Row(21)), data)
        }
      }, conf = conf)
    }

    test(s"int32 Decimal $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int32 int_test (DECIMAL(9,2));
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/DEC32_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addInteger(1)
              }
            }
          }, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addInteger(2)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(BigDecimal("0.01")), Row(BigDecimal("0.02"))), data)
        }
      }, conf = conf)
    }

    test(s"int64 Decimal $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int64 int_test (DECIMAL(9,2));
            |  required int64 long_test (DECIMAL(15,0));
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/DEC64_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addLong(1)
              }
              rc.field("long_test", 1) {
                rc.addLong(100)
              }
            }
          }, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addLong(2)
              }
              rc.field("long_test", 1) {
                rc.addLong(201)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(BigDecimal("0.01"), BigDecimal("100")),
            Row(BigDecimal("0.02"), BigDecimal("201"))), data)
        }
      }, conf = conf)
    }

    //https://github.com/NVIDIA/spark-rapids/issues/6915
    test(s"FIXED_LEN_BYTE_ARRAY(16) Decimal $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required fixed_len_byte_array(16) int_test (DECIMAL(9,2));
            |  required fixed_len_byte_array(16) long_test (DECIMAL(15,0));
            |  required fixed_len_byte_array(16) bin_test (DECIMAL(38,4));
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/FIXED_BIN16_DEC_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
              }
              rc.field("long_test", 1) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
              }
              rc.field("bin_test", 2) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
              }
              rc.field("long_test", 1) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
              }

              rc.field("bin_test", 2) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(BigDecimal("0.01"), BigDecimal("1"), BigDecimal("0.0001")),
            Row(BigDecimal("0.02"), BigDecimal("2"), BigDecimal("0.0002"))), data)
        }
      }, conf = conf)
    }

    test(s"FIXED_LEN_BYTE_ARRAY(15) Decimal $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required fixed_len_byte_array(15) int_test (DECIMAL(9,2));
            |  required fixed_len_byte_array(15) long_test (DECIMAL(15,0));
            |  required fixed_len_byte_array(15) bin_test (DECIMAL(35,4));
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/FIXED_BIN15_DEC_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
              }
              rc.field("long_test", 1) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
              }
              rc.field("bin_test", 2) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
              }
              rc.field("long_test", 1) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
              }

              rc.field("bin_test", 2) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(BigDecimal("0.01"), BigDecimal("1"), BigDecimal("0.0001")),
            Row(BigDecimal("0.02"), BigDecimal("2"), BigDecimal("0.0002"))), data)
        }
      }, conf = conf)
    }

    test(s"binary Decimal $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required binary int_test (DECIMAL(9,2));
            |  required binary long_test (DECIMAL(15,0));
            |  required binary bin_test (DECIMAL(35,4));
            |}
            """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/BINARY_DEC_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x01)))
              }
              rc.field("long_test", 1) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
              }
              rc.field("bin_test", 2) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("int_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x02)))
              }
              rc.field("long_test", 1) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x02)))
              }

              rc.field("bin_test", 2) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0xFF.toByte)))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(BigDecimal("0.01"), BigDecimal("1"), BigDecimal("0.0001")),
            Row(BigDecimal("0.02"), BigDecimal("2"), BigDecimal("-0.0001"))), data)
        }
      },
        // To make the CPU happy we need to turn off the vectorized reader
        new SparkConf().set("spark.sql.parquet.enableVectorizedReader", "false")
            .set("spark.rapids.sql.format.parquet.reader.footer.type", parserType))
    }

    test(s"binary Decimal2 $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required binary bin_test (DECIMAL(19,0));
            |  required binary big_bin_test (DECIMAL(38,0));
            |}
            """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/BINARY_DEC2_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("bin_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
              }
              rc.field("big_bin_test", 1) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0xFF.toByte)))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("bin_test", 0) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x02)))
              }
              rc.field("big_bin_test", 1) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x02)))
              }
            }
          }, { rc =>
            rc.message {
              rc.field("bin_test", 0) {
                // This one will do an overflow in the size of the binary data,
                // but the top bytes are zero so it is okay
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03)))
              }
              rc.field("big_bin_test", 1) {
                rc.addBinary(Binary.fromConstantByteArray(
                  Array(0x00)))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(BigDecimal("1"), BigDecimal("-1")),
            Row(BigDecimal("2"), BigDecimal("2")),
            Row(BigDecimal("3"), BigDecimal("0"))), data)
        }
      },
        // To make the CPU happy we need to turn off the vectorized reader
        new SparkConf().set("spark.sql.parquet.enableVectorizedReader", "false")
            .set("spark.rapids.sql.format.parquet.reader.footer.type", parserType))
    }

    test(s"Date $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int32 date_test (DATE);
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/DATE_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("date_test", 0) {
                rc.addInteger(1)
              }
            }
          }, { rc =>
            rc.message {
              rc.field("date_test", 0) {
                rc.addInteger(2)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(new Date(1 * 24 * 60 * 60 * 1000)),
            Row(new Date(2 * 24 * 60 * 60 * 1000))), data)
        }
      }, conf = conf)
    }

    // TIME_MILLIS and TIME_MICROS are not supported by Spark

    test(s"Timestamp (MILLIS AND MICROS) $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int64 millis_test (TIMESTAMP_MILLIS);
            |  required int64 micros_test (TIMESTAMP_MICROS);
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/TIMESTAMP_MM_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("millis_test", 0) {
                rc.addLong(1000)
              }
              rc.field("micros_test", 1) {
                rc.addLong(2)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Timestamp.valueOf("1970-01-01 00:00:01.0"),
            Timestamp.valueOf("1970-01-01 00:00:00.000002"))), data)
        }
      }, conf = conf)
    }

    // NANO TIMESTAMPS ARE NOT SUPPORTED BY SPARK

    test(s"nz timestamp (MILLIS AND MICROS) $parserType") {
      assumeSpark330orLater
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int64 millis_test (TIMESTAMP(MILLIS, false));
            |  required int64 micros_test (TIMESTAMP(MICROS, false));
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/TIMESTAMP_MM_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("millis_test", 0) {
                rc.addLong(1000)
              }
              rc.field("micros_test", 1) {
                rc.addLong(2)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Timestamp.valueOf("1970-01-01 00:00:01.0"),
            Timestamp.valueOf("1970-01-01 00:00:00.000002"))), data)
        }
      },
        // disable timestampNTZ for parquet for 3.4+ tests to pass
        new SparkConf().set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
            .set("spark.rapids.sql.format.parquet.reader.footer.type", parserType))
    }

    // This is not a supported feature in 3.4.0 yet for the plugin
    ignore(s"nz timestamp spark enabled (MILLIS AND MICROS) $parserType") {
      assumeSpark340orLater
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required int64 millis_test (TIMESTAMP(MILLIS, false));
            |  required int64 micros_test (TIMESTAMP(MICROS, false));
            |}
        """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/TIMESTAMP_MM_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("millis_test", 0) {
                rc.addLong(1000)
              }
              rc.field("micros_test", 1) {
                rc.addLong(2)
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(LocalDateTime.parse("1970-01-01T00:00:01.0"),
            LocalDateTime.parse("1970-01-01T00:00:00.000002"))), data)
        }
      },
        new SparkConf().set("spark.sql.parquet.inferTimestampNTZ.enabled", "true")
            .set("spark.rapids.sql.format.parquet.reader.footer.type", parserType))
    }
    // INTERVAL is not supported by Spark
    // TIME is not supported by Spark

    // LISTS
    test(s"BASIC LIST $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required group my_list (LIST) {
            |    repeated group list {
            |      required int32 element;
            |    }
            |  }
            |  optional group other_name_list (LIST) {
            |    repeated group element {
            |      required binary str (UTF8);
            |    }
            |  }
            |}
          """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/BASIC_LIST_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("my_list", 0) {
                rc.field("list", 0) {
                  rc.field("element", 0) {
                    rc.addInteger(0)
                    rc.addInteger(1)
                  }
                }
              }
              rc.field("other_name_list", 1) {
                rc.field("element", 0) {
                  rc.field("str", 0) {
                    rc.addBinary(Binary.fromString("TEST"))
                    rc.addBinary(Binary.fromString("NAME"))
                  }
                }
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Array(0, 1), Array("TEST", "NAME"))), data)
        }
      }, conf = conf)
    }

    // https://github.com/NVIDIA/spark-rapids/issues/6967
    test(s"NO GROUP LIST $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  repeated int32 data;
            |  repeated binary str (UTF8);
            |}
                """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/NO_GROUP_LIST_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("data", 0) {
                rc.addInteger(0)
                rc.addInteger(1)
              }
              rc.field("str", 1) {
                rc.addBinary(Binary.fromString("TEST"))
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Array(0, 1), Array("TEST"))), data)
        }
      }, conf = conf)
    }

    // https://github.com/NVIDIA/spark-rapids/issues/6967
    test(s"SINGLE GROUP LIST $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message avro_style {
            |  required group f (LIST) {
            |    repeated int32 array;
            |  }
            |}
              """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/SINGLE_GROUP_LIST_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("f", 0) {
                rc.group {
                  rc.field("array", 0) {
                    rc.addInteger(0)
                    rc.addInteger(1)
                  }
                }
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Array(0, 1))), data)
        }
      }, conf = conf)
    }

    // https://github.com/NVIDIA/spark-rapids/issues/6967
    test(s"SINGLE GROUP TUPLE LIST $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message test {
            |  required group my_list (LIST) {
            |    repeated group element {
            |      required binary str (UTF8);
            |      required int32 num;
            |    }
            |  }
            |}
              """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/SINGLE_GROUP_TUPLE_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("my_list", 0) {
                rc.field("element", 0) {
                  rc.field("str", 0) {
                    rc.addBinary(Binary.fromString("TEST"))
                    rc.addBinary(Binary.fromString("DATA"))
                  }
                  rc.field("num", 1) {
                    rc.addInteger(0)
                    rc.addInteger(1)
                  }
                }
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Array(Row("TEST", 0), Row("DATA", 1)))), data)
        }
      }, conf = conf)
    }

    // https://github.com/NVIDIA/spark-rapids/issues/6968
    ignore(s"SPECIAL ARRAY LIST $parserType") {
      // From the parquet spec
      // If the repeated field is a group with one field and is named either array or uses the
      // LIST-annotated group's name with _tuple appended then the repeated type is the element
      // type and elements are required.
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required group my_list (LIST) {
            |    repeated group array {
            |      required int32 item;
            |    }
            |  }
            |}
          """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/SPECIAL_ARRAY_LIST_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("my_list", 0) {
                rc.field("array", 0) {
                  rc.field("item", 0) {
                    rc.addInteger(0)
                    rc.addInteger(1)
                  }
                }
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Array(Row(0), Row(1)))), data)
        }
      }, conf = conf)
    }

    // https://github.com/NVIDIA/spark-rapids/issues/6968
    ignore(s"SPECIAL _TUPLE LIST $parserType") {
      // From the parquet spec
      // If the repeated field is a group with one field and is named either array or uses the
      // LIST-annotated group's name with _tuple appended then the repeated type is the element
      // type and elements are required.
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required group my_list (LIST) {
            |    repeated group my_list_tuple {
            |      required int32 item;
            |    }
            |  }
            |}
          """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/SPECIAL_TUPLE_LIST_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("my_list", 0) {
                rc.field("my_list_tuple", 0) {
                  rc.field("item", 0) {
                    rc.addInteger(0)
                    rc.addInteger(1)
                  }
                }
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Array(Row(0), Row(1)))), data)
        }
      }, conf = conf)
    }

    // MAPS

    test(s"BASIC MAP $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required group my_map (MAP) {
            |    repeated group key_value {
            |      required int32 key;
            |      required int32 value;
            |    }
            |  }
            |  required group my_other_map (MAP) {
            |    repeated group keys_and_values {
            |      required int32 primary;
            |      required int32 secondary;
            |    }
            |  }
            |}
          """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/BASIC_MAP_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("my_map", 0) {
                rc.field("key_value", 0) {
                  rc.field("key", 0) {
                    rc.addInteger(0)
                    rc.addInteger(1)
                  }
                  rc.field("value", 1) {
                    rc.addInteger(2)
                    rc.addInteger(3)
                  }
                }
              }
              rc.field("my_other_map", 1) {
                rc.field("keys_and_values", 0) {
                  rc.field("primary", 0) {
                    rc.addInteger(0)
                    rc.addInteger(1)
                  }
                  rc.field("secondary", 1) {
                    rc.addInteger(2)
                    rc.addInteger(3)
                  }
                }
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Map(0 -> 2, 1 -> 3), Map(0 -> 2, 1 -> 3))), data)
        }
      }, conf = conf)
    }

    test(s"DUPLICATE KEY MAP $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required group my_map (MAP) {
            |    repeated group key_value {
            |      required int32 key;
            |      required int32 value;
            |    }
            |  }
            |}
          """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/BASIC_MAP_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("my_map", 0) {
                rc.field("key_value", 0) {
                  rc.field("key", 0) {
                    rc.addInteger(0)
                    rc.addInteger(0)
                  }
                  rc.field("value", 1) {
                    rc.addInteger(2)
                    rc.addInteger(3)
                  }
                }
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Map(0 -> 3))), data)
        }
      }, conf = conf)
    }

    // Spec says that value can be "omitted" but Spark does not support it.

    // https://github.com/NVIDIA/spark-rapids/issues/6970
    ignore(s"MAP_KEY_VALUE $parserType") {
      withGpuSparkSession(spark => {
        val schema =
          """message spark {
            |  required group my_map (MAP_KEY_VALUE) {
            |    repeated group map {
            |      required int32 key;
            |      required int32 value;
            |    }
            |  }
            |}
          """.stripMargin

        withTempDir(spark) { dir =>
          val testPath = dir + "/MAP_KEY_VALUE_TEST.parquet"
          writeDirect(testPath, schema, { rc =>
            rc.message {
              rc.field("my_map", 0) {
                rc.field("map", 0) {
                  rc.field("key", 0) {
                    rc.addInteger(0)
                    rc.addInteger(1)
                  }
                  rc.field("value", 1) {
                    rc.addInteger(2)
                    rc.addInteger(3)
                  }
                }
              }
            }
          })

          val data = spark.read.parquet(testPath).collect()
          sameRows(Seq(Row(Map(0 -> 2, 1 -> 3))), data)
        }
      }, conf = conf)
    }
  }
}