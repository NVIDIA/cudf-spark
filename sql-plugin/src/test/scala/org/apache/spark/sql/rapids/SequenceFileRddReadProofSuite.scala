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

package org.apache.spark.sql.rapids

import java.nio.file.Files
import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
import org.apache.xbean.asm9.Opcodes
import org.apache.xbean.asm9.tree.{InsnNode, MethodInsnNode, MethodNode, TypeInsnNode, VarInsnNode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.SerializeFromObjectExec
import org.apache.spark.util.Utils

class SequenceFileRddReadProofSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder()
      .master("local[1]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      super.afterAll()
    }
  }

  private def source(): RDD[(BytesWritable, BytesWritable)] = {
    spark.sparkContext.newAPIHadoopFile(
      "file:/sequence-file-proof-does-not-read-input",
      classOf[SequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable])
  }

  private def inspect(df: DataFrame): SequenceFileRddReadProof.Result = {
    val serialize = df.queryExecution.sparkPlan.collectFirst {
      case plan: SerializeFromObjectExec => plan
    }.getOrElse(fail(s"SerializeFromObjectExec not found:\n${df.queryExecution.sparkPlan}"))
    SequenceFileRddReadProof.inspect(serialize)
  }

  private def assertProven(
      df: DataFrame,
      expected: Seq[SequenceFileRddReadProof.SourceColumn] =
        Seq(SequenceFileRddReadProof.Key, SequenceFileRddReadProof.Value)): Unit = {
    inspect(df) match {
      case SequenceFileRddReadProof.Proven(columns, _) => assert(columns == expected)
      case rejected => fail(s"expected proof, found $rejected")
    }
  }

  private def assertRejected(df: DataFrame, expectedReason: String = ""): Unit = inspect(df) match {
    case SequenceFileRddReadProof.Rejected(reason) => assert(reason.contains(expectedReason))
    case proof => fail(s"expected rejection, found $proof")
  }

  private def binaryDataFrame(
      rdd: RDD[(Array[Byte], Array[Byte])],
      names: (String, String) = "key" -> "value"): DataFrame = {
    val session = spark
    import session.implicits._
    rdd.toDF(names._1, names._2)
  }

  test("prove supported binary copies") {
    val ranges = binaryDataFrame(source().map { case (key, value) =>
      (Arrays.copyOfRange(key.getBytes, 0, key.getLength),
        Arrays.copyOfRange(value.getBytes, 0, value.getLength))
    })
    val copies = binaryDataFrame(source().map { pair =>
      (pair._1.copyBytes(), Arrays.copyOf(pair._2.getBytes, pair._2.getLength))
    })

    assertProven(ranges)
    assertProven(copies)
  }

  test("track key and value provenance") {
    val df = binaryDataFrame(source().map { pair =>
      (pair._2.copyBytes(), pair._1.copyBytes())
    }, "first" -> "second")

    assertProven(df, Seq(SequenceFileRddReadProof.Value, SequenceFileRddReadProof.Key))
  }

  test("reject partial and mismatched copies") {
    val partial = binaryDataFrame(source().map { pair =>
      (pair._1.copyBytes(),
        Arrays.copyOfRange(pair._2.getBytes, 1, pair._2.getLength))
    })
    val mismatched = binaryDataFrame(source().map { pair =>
      (Arrays.copyOf(pair._1.getBytes, pair._2.getLength), pair._2.copyBytes())
    })

    assertRejected(partial)
    assertRejected(mismatched, "copyOf does not copy the full writable payload")
  }

  test("reject raw backing arrays and captured state") {
    val raw = binaryDataFrame(source().map { pair =>
      (pair._1.getBytes, pair._2.copyBytes())
    })
    val counter = new AtomicInteger()
    val captured = binaryDataFrame(source().map { pair =>
      counter.incrementAndGet()
      (pair._1.copyBytes(), pair._2.copyBytes())
    })

    assertRejected(raw, "Tuple2 is not constructed from two proven binary values")
    assertRejected(captured, "user closure captures state")
  }

  test("reject casts that change closure failure behavior") {
    val df = binaryDataFrame(source().map { pair =>
      val invalid = pair._1.asInstanceOf[String]
      (pair._1.copyBytes(),
        Arrays.copyOf(pair._2.getBytes, pair._2.getLength + invalid.length))
    })

    assertRejected(df, "unsupported type instruction")
  }

  test("reject a mismatched tuple accessor invocation kind") {
    val method = new MethodNode(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "bad",
      "(Lscala/Tuple2;)Lscala/Tuple2;", null, null)
    method.instructions.add(new TypeInsnNode(Opcodes.NEW, "scala/Tuple2"))
    method.instructions.add(new InsnNode(Opcodes.DUP))
    Seq("_1", "_2").foreach { accessor =>
      method.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0))
      method.instructions.add(new MethodInsnNode(Opcodes.INVOKEINTERFACE,
        "scala/Tuple2", accessor, "()Ljava/lang/Object;", true))
      method.instructions.add(new TypeInsnNode(Opcodes.CHECKCAST,
        "org/apache/hadoop/io/BytesWritable"))
      method.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL,
        "org/apache/hadoop/io/BytesWritable", "copyBytes", "()[B", false))
    }
    method.instructions.add(new MethodInsnNode(Opcodes.INVOKESPECIAL, "scala/Tuple2", "<init>",
      "(Ljava/lang/Object;Ljava/lang/Object;)V", false))
    method.instructions.add(new InsnNode(Opcodes.ARETURN))
    method.maxLocals = 1

    val verifier = SequenceFileRddReadProof.getClass
      .getDeclaredMethod("interpret", classOf[MethodNode])
    verifier.setAccessible(true)
    val result = verifier.invoke(SequenceFileRddReadProof, method)
      .asInstanceOf[Either[String, IndexedSeq[SequenceFileRddReadProof.SourceColumn]]]
    result match {
      case Left(reason) => assert(reason.contains("unsupported method"))
      case proof => fail(s"expected rejection, found $proof")
    }
  }

  test("reject additional RDD transformations") {
    val df = binaryDataFrame(source().filter(_._1.getLength > 0).map { pair =>
      (pair._1.copyBytes(), pair._2.copyBytes())
    })

    assertRejected(df, "expected NewHadoopRDD")
  }

  test("reject NewHadoopRDD subclasses") {
    val derived = new NewHadoopRDD[BytesWritable, BytesWritable](
      spark.sparkContext,
      classOf[SequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable],
      new Configuration(spark.sparkContext.hadoopConfiguration)) {}
    val df = binaryDataFrame(derived.map { pair =>
      (pair._1.copyBytes(), pair._2.copyBytes())
    })

    assertRejected(df, "NewHadoopRDD subclasses")
  }

  test("reject checkpoint requests before materialization") {
    val checkpointDir = Files.createTempDirectory("sequence-file-proof-checkpoint").toFile
    try {
      spark.sparkContext.setCheckpointDir(checkpointDir.getAbsolutePath)
      val mapped = source().map { pair =>
        (pair._1.copyBytes(), pair._2.copyBytes())
      }
      mapped.checkpoint()

      assertRejected(binaryDataFrame(mapped), "has checkpoint state")
    } finally {
      Utils.deleteRecursively(checkpointDir)
    }
  }

  test("reject mapPartitions even when its parent is the source") {
    val df = binaryDataFrame(source().mapPartitions { records =>
      records.map { pair =>
        (pair._1.copyBytes(), pair._2.copyBytes())
      }
    })

    assertRejected(df, "not the Spark RDD.map wrapper")
  }

  test("reject a non-binary tuple serializer") {
    val session = spark
    import session.implicits._
    val df = source().map { pair =>
      (pair._1.copyBytes(), pair._2.getLength)
    }.toDF("key", "length")

    assertRejected(df)
  }
}
