/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.nvidia

import java.io.File
import java.nio.file.Files
import java.util.{Locale, TimeZone}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.rapids.shims.TrampolineUtilShim

object SparkSessionHolder extends Logging {
  private var spark = createSparkSession()
  private var origConf = spark.conf.getAll
  private var origConfKeys = origConf.keys.toSet

  private def setAllConfs(confs: Array[(String, String)]): Unit = confs.foreach {
    case (key, value) if spark.conf.get(key, null) != value =>
      spark.conf.set(key, value)
    case _ => // No need to modify it
  }

  private def createSparkSession(): SparkSession = {
    TrampolineUtilShim.cleanupAnyExistingSession()

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    Locale.setDefault(Locale.US)

    val builder = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions", "com.nvidia.spark.DFUDFPlugin")
      .config("spark.sql.warehouse.dir", sparkWarehouseDir.getAbsolutePath)
      .appName("dataframe udf tests")

    builder.getOrCreate()
  }

  private def reinitSession(): Unit = {
    spark = createSparkSession()
    origConf = spark.conf.getAll
    origConfKeys = origConf.keys.toSet
  }

  def sparkSession: SparkSession = {
    if (SparkSession.getActiveSession.isEmpty) {
      reinitSession()
    }
    spark
  }

  def resetSparkSessionConf(): Unit = {
    if (SparkSession.getActiveSession.isEmpty) {
      reinitSession()
    } else {
      setAllConfs(origConf.toArray)
      val currentKeys = spark.conf.getAll.keys.toSet
      val toRemove = currentKeys -- origConfKeys
      if (toRemove.contains("spark.shuffle.manager")) {
        // cannot unset the config so need to reinitialize
        reinitSession()
      } else {
        toRemove.foreach(spark.conf.unset)
      }
    }
    logDebug(s"RESET CONF TO: ${spark.conf.getAll}")
  }

  def withSparkSession[U](conf: SparkConf, f: SparkSession => U): U = {
    resetSparkSessionConf()
    logDebug(s"SETTING  CONF: ${conf.getAll.toMap}")
    setAllConfs(conf.getAll)
    logDebug(s"RUN WITH CONF: ${spark.conf.getAll}\n")
    f(spark)
  }

  private lazy val sparkWarehouseDir: File = {
    new File(System.getProperty("java.io.tmpdir")).mkdirs()
    val path = Files.createTempDirectory("spark-warehouse")
    val file = new File(path.toString)
    file.deleteOnExit()
    file
  }
}

/**
 * Base to be able to run tests with a spark context
 */
trait SparkTestBase extends AnyFunSuite with BeforeAndAfterAll {
  def withSparkSession[U](f: SparkSession => U): U = {
    withSparkSession(new SparkConf, f)
  }

  def withSparkSession[U](conf: SparkConf, f: SparkSession => U): U = {
    SparkSessionHolder.withSparkSession(conf, f)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TrampolineUtilShim.cleanupAnyExistingSession()
  }

  def assertSame(expected: Any, actual: Any, epsilon: Double = 0.0,
                 path: List[String] = List.empty): Unit = {
    def assertDoublesAreEqualWithinPercentage(expected: Double,
                                              actual: Double, path: List[String]): Unit = {
      if (expected != actual) {
        if (expected != 0) {
          val v = Math.abs((expected - actual) / expected)
          assert(v <= epsilon,
            s"$path: ABS($expected - $actual) / ABS($actual) == $v is not <= $epsilon ")
        } else {
          val v = Math.abs(expected - actual)
          assert(v <= epsilon, s"$path: ABS($expected - $actual) == $v is not <= $epsilon ")
        }
      }
    }
    (expected, actual) match {
      case (a: Float, b: Float) if a.isNaN && b.isNaN =>
      case (a: Double, b: Double) if a.isNaN && b.isNaN =>
      case (null, null) =>
      case (null, other) => fail(s"$path: expected is null, but actual is $other")
      case (other, null) => fail(s"$path: expected is $other, but actual is null")
      case (a: Array[_], b: Array[_]) =>
        assert(a.length == b.length,
          s"$path: expected (${a.toList}) and actual (${b.toList}) lengths don't match")
        a.indices.foreach { i =>
          assertSame(a(i), b(i), epsilon, path :+ i.toString)
        }
      case (a: Map[_, _], b: Map[_, _]) =>
        throw new IllegalStateException(s"Maps are not supported yet for comparison $a vs $b")
      case (a: Iterable[_], b: Iterable[_]) =>
        assert(a.size == b.size,
          s"$path: expected (${a.toList}) and actual (${b.toList}) lengths don't match")
        var i = 0
        a.zip(b).foreach {
          case (l, r) =>
            assertSame(l, r, epsilon, path :+ i.toString)
            i += 1
        }
      case (a: Product, b: Product) =>
        assertSame(a.productIterator.toSeq, b.productIterator.toSeq, epsilon, path)
      case (a: Row, b: Row) =>
        assertSame(a.toSeq, b.toSeq, epsilon, path)
      // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
      case (a: Double, b: Double) if epsilon <= 0 =>
        java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
      case (a: Double, b: Double) if epsilon > 0 =>
        assertDoublesAreEqualWithinPercentage(a, b, path)
      case (a: Float, b: Float) if epsilon <= 0 =>
        java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
      case (a: Float, b: Float) if epsilon > 0 =>
        assertDoublesAreEqualWithinPercentage(a, b, path)
      case (a, b) =>
        assert(a == b, s"$path: $a != $b")
    }
  }
}
