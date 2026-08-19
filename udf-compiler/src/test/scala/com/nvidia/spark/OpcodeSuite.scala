/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.collection.mutable

import com.nvidia.spark.rapids.RapidsConf
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{udf => makeUdf}
import org.apache.spark.sql.functions.{log => logalias}

class OpcodeSuite extends AnyFunSuite {

  val conf: SparkConf = new SparkConf()
    .set("spark.sql.extensions", "com.nvidia.spark.udf.Plugin")
    .set("spark.rapids.sql.udfCompiler.enabled", "true")
     // ANSI is disabled due to https://github.com/NVIDIA/spark-rapids/issues/11633
    .set("spark.sql.ansi.enabled", "false")
    .set(RapidsConf.EXPLAIN.key, "true")

  val spark: SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("OpcodeSuite")
      .config(conf)
      .getOrCreate()

  import spark.implicits._

  // Utility Function for checking equivalency of Dataset type
  def checkEquiv[T](ds1: Dataset[T], ds2: Dataset[T]) : Unit = {
    assert(udfIsCompiled(ds1))
    compareResult(ds1, ds2)
  }

  def checkEquivNotCompiled[T](ds1: Dataset[T], ds2: Dataset[T]): Unit = {
    assert(!udfIsCompiled(ds1))
    compareResult(ds1, ds2)
  }

  def compareResult[T](ds1: Dataset[T], ds2: Dataset[T]): Unit = {
    ds1.collect.zip(ds2.collect).foreach{case (x, y) => assert(x == y)}
  }

  def udfIsCompiled[T](ds: Dataset[T]): Boolean = {
    !ds.queryExecution.analyzed.toString().contains("UDF")
  }

  // START OF TESTS

  test("not boolean") {
    val myudf: Boolean => Boolean = { x => !x }
    val u = makeUdf(myudf)
    val dataset = List(true, false, true, false).toDS().repartition(1)
    val result = dataset.withColumn("new", u(col("value")))
    val equiv = dataset.withColumn("new", not(col("value")))
    checkEquiv(result, equiv)
  }

  test("and boolean") {
    val myudf: (Boolean, Boolean) => Boolean = { (a, b) => a && b }
    val u = makeUdf(myudf)
    val df = List((false, false), (false, true), (true, false), (true, true))
      .toDF("x", "y")
      .repartition(1)
    val result = df.withColumn("new", u(col("x"), col("y")))
    val equiv = df.withColumn("new", col("x") && col("y"))
    checkEquiv(result, equiv)
  }

  test("not(and boolean)") {
    val myudf: (Boolean, Boolean) => Boolean = { (a, b) => !(a && b) }
    val u = makeUdf(myudf)
    val df = List((false, false), (false, true), (true, false), (true, true))
      .toDF("x", "y")
      .repartition(1)
    val result = df.withColumn("new", u(col("x"), col("y")))
    val equiv = df.withColumn("new", !(col("x") && col("y")))
    checkEquiv(result, equiv)
  }

  test("or boolean") {
    val myudf: (Boolean, Boolean) => Boolean = { (a, b) => a || b }
    val u = makeUdf(myudf)
    val df = List((false, false), (false, true), (true, false), (true, true))
      .toDF("x", "y")
      .repartition(1)
    val result = df.withColumn("new", u(col("x"), col("y")))
    val equiv = df.withColumn("new", col("x") || col("y"))
    checkEquiv(result, equiv)
  }

  // conditional tests, all but test0 fall back to JVM execution
  test("conditional floats") {
    val myudf: Float => Float = { x =>
      val t =
        if (x > 1.0f && x < 3.7f) {
          (if (x > 1.1f && x < 2.0f) 1.0f else 1.1f) + 24.0f
        } else {
          if (x < 0.1f) 2.3f else 4.1f
        }
      t + 2.2f
    }
    val u = makeUdf(myudf)
    val dataset = List(2.0f).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(27.300001f))
    checkEquiv(result, ref)
    val dataset2 = List(4.0f).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(6.3f))
    checkEquiv(result2, ref2)
  }

  test("conditional doubles") {
    val myudf: Double => Double = { x =>
      val t =
        if (x > 1.0 && x <= 3.7) {
          (if (x >= 1.1 && x < 2.1) 1.0 else 1.1) + 24.0
        } else {
          if (x < 1.1) 2.3 else 4.1
        }
      t + 2.2
    }
    val u = makeUdf(myudf)
    val dataset = List(1.0).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(4.5))
    checkEquiv(result, ref)
    val dataset2 = List(2.0).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(27.2))
    checkEquiv(result2, ref2)
  }

  test("conditional ints") {
    val myudf: Int => Int = { x =>
      val t =
        if (x > 1 && x < 5) {
          10
        } else {
          if (x > 7) 20 else 30
        }
      t + 5
    }
    val u = makeUdf(myudf)
    val dataset = List(2).toDS()
    val result = dataset.withColumn("new",u(col("value")))
    val ref = dataset.withColumn("new", lit(15))
    checkEquiv(result, ref)
    val dataset2 = List(8).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(25))
    checkEquiv(result2, ref2)
  }

  test("conditional longs") {
    val myudf: Long => Long = { x =>
      val t =
        if (x > 1L && x < 5L) {
          10L
        } else {
          if (x > 7L) 20L else 30L
        }
      t + 5L
    }
    val u = makeUdf(myudf)
    val dataset = List(2L).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(15L))
    checkEquiv(result, ref)
    val dataset2 = List(8L).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(25L))
    checkEquiv(result2, ref2)
  }

  // tests for load and store operations, also cover +/-/* operators for int,long,double,float
  test("LLOAD_<n> odd") {
    val dataset = List((1,1L,1L)).toDF("x","y","z")
    val myudf: (Int, Long, Long) => Long = (_, b, c) => {
      (b+c)*c-b
    }
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u(col("x"),col("y"),col("z")))
    val ref = dataset.withColumn("new",(col("y")+col("z"))*col("z") - col("y"))
    checkEquiv(result, ref)
  }

  test("DLOAD_<n> odd") {
    val dataset = List((1,1.0,1.0)).toDF("x","y","z")
    val myudf: (Int, Double, Double) => Double = (_, b, c) => {
      (b+c)*b-c
    }
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new", u(col("x"),col("y"),col("z")))
    val ref = dataset.withColumn("new",(col("y")+col("z"))*col("y")-col("z"))
    checkEquiv(result, ref)
  }

  test("DLOAD_<n> even") {
    val dataset = List((1.0,1.0)).toDF("x","y")
    val myudf: (Double, Double) => Double = (a,b) => {
      (a+b)*a-b
    }
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u(col("x"),col("y")))
    val ref = dataset.withColumn("new",(col("x")+col("y"))*col("x")-col("y"))
    checkEquiv(result, ref)
  }

  test("LLOAD_<n> even") {
    val dataset = List((1L,1L)).toDF("x","y")
    val myudf: (Long, Long) => Long = (a,b) => {
      (a+b)*a-b
    }
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u(col("x"),col("y")))
    val ref = dataset.withColumn("new",(col("x")+col("y"))*col("x")-col("y"))
    checkEquiv(result, ref)
  }

  test("ILOAD_<n> all") {
    val dataset = List((1,1,1,1)).toDF("x","y","z","w")
    val myudf: (Int, Int, Int, Int) => Int = (a,b,c,d) => {
      (a+b-c)*d
    }
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u(col("x"),col("y"),col("z"),col("w")))
    val ref = dataset.withColumn("new",(col("z")+col("y")-col("z"))*col("w"))
    checkEquiv(result, ref)
  }

  test("FLOAD_<n> all") {
    val dataset = List((1.0f,1.0f,1.0f,1.0f)).toDF("x","y","z","w")
    val myudf: (Float, Float, Float, Float) => Float = (a,b,c,d) => {
      (a+b-c)*d
    }
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u(col("x"),col("y"),col("z"),col("w")))
    val ref = dataset.withColumn("new",(col("x")+col("y")-col("z"))*col("w"))
    checkEquiv(result, ref)
  }

  test("ISTORE_<n> all") {
    val myudf: () => Int = () => {
      val myInt : Int = 1
      val myInt3 : Int = myInt
      val myInt4 : Int = myInt * myInt3
      myInt4
    }
    val dataset = List(1).toDS()
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new",lit(1))
    checkEquiv(result, ref)
  }

  test("DSTORE_<n> even") {
    val myudf: () => Double = () => {
      val myDoub : Double = 0.0
      val myDoub2 : Double = 1.0 - myDoub
      myDoub2
    }
    val dataset = List(1).toDS()
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new",lit(1.0))
    checkEquiv(result, ref)
  }

  test("DSTORE_<n> odd") {
    val myudf: (Int) => Double = (_) => {
      val myDoub : Double = 1.0
      val myDoub2 : Double = 1.0 * myDoub
      myDoub2
    }
    val dataset = List(1).toDS()
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new",lit(1.0))
    checkEquiv(result, ref)
  }

  test("ALOAD_0") {
    val myudf: (String,String,String,String) => String = (a, _, _, _) => {
      a
    }
    val dataset = List(("a","b","c","d")).toDF("x","y","z","w")
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u(col("x"),col("y"),col("z"),col("w")))
    val ref = dataset.withColumn("new",col("x"))
    checkEquiv(result, ref)
  }

  test("ALOAD_1") {
    val myudf: (String,String,String,String) => String = (_, b, _, _) => {
      b
    }
    val dataset = List(("a","b","c","d")).toDF("x","y","z","w")
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u(col("x"),col("y"),col("z"),col("w")))
    val ref = dataset.withColumn("new",col("y"))
    checkEquiv(result, ref)
  }

  test("ALOAD_2") {
    val myudf: (String,String,String,String) => String = (_, _, c, _) => {
      c
    }
    val dataset = List(("a","b","c","d")).toDF("x","y","z","w")
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u(col("x"),col("y"),col("z"),col("w")))
    val ref = dataset.withColumn("new",col("z"))
    checkEquiv(result, ref)
  }

  test("ALOAD_3") {
    val myudf: (String,String,String,String) => String = (_, _, _, d) => {
      d
    }
    val dataset = List(("a","b","c","d")).toDF("x","y","z","w")
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u(col("x"),col("y"),col("z"),col("w")))
    val ref = dataset.withColumn("new",col("w"))
    checkEquiv(result, ref)
  }

  test("ASTORE_1,2,3") {
    val myudf: (String) => String = (a) => {
      val myString : String = a
      val myString2 : String = myString
      val myString3 : String = myString2
      myString3
    }
    val dataset = List("a").toDS()
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u(col("value")))
    val ref = dataset.withColumn("new",col("value"))
    checkEquiv(result, ref)
  }

  test("FSTORE_1,2,3") {
    val myudf: (Float) => Float = (a) => {
      val myFloat : Float = a
      val myFloat2 : Float = myFloat + a
      val myFloat3 : Float = myFloat2 + a
      myFloat3
    }
    val dataset = List(5.0f).toDS()
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new",col("value")*3)
    checkEquiv(result, ref)
  }


  test("LSTORE_2") {
    val myudf: (Long) => Long = (a) => {
      val myLong : Long = a
      myLong
    }
    val dataset = List(5L).toDS()
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new",col("value"))
    checkEquiv(result, ref)
  }

  test("LSTORE_3") {
    val myudf: (Int, Long) => Long = (_, b) => {
      val myLong : Long = b
      myLong
    }
    val dataset = List((1,5L)).toDF("x","y")
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new", u(col("x"),col("y")))
    val ref = dataset.withColumn("new",col("y"))
    checkEquiv(result, ref)
  }

  test("Boolean check") {
    val myudf: () => Boolean = () => {
      val myBool : Boolean = true
      myBool
    }
    val dataset = List(true).toDS()
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new",u())
    val ref = dataset.withColumn("new",lit(true))
    checkEquiv(result, ref)
  }

  // the test covers the functionality of LDC as well as ASTORE_0.
  test("LDC tests") {
    // A nested object, LDCTests, is used to ensure ldc is used instead of
    // ldc_w.
    // Without this nested object, "a" would be added to the constant pool of
    // OpcodeSuite, and, depending on the number of entries in the constant
    // pool, ldc_w might be generated.
    // With this nested object, we know "a" would be added to the constant pool
    // of LDCTests and the constant pool of LDCTests is small enough to
    // guarantee the generation of ldc.
    object LDCTests {
      def run(): Unit = {
        val myudf: () => (String) = () => {
          val myString : String = "a"
          myString
        }
        val u = makeUdf(myudf)
        val dataset = List("a").toDS()
        val result = dataset.withColumn("new", u())
        val ref = dataset.withColumn("new",lit("a"))
        checkEquiv(result, ref)
      }
    }
    LDCTests.run
  }

  // this test makes sure we can handle udfs with more than 2 args
  test("UDF 4 args") {
    val myudf: (Int, Int, Int, Int) => Int = (w,x,y,z) => { w+x+y+z }
    val u = makeUdf(myudf)
    val dataset = List((1,2,3,4),(2,3,4,5),(3,4,5,6),(4,5,6,7)).toDF("x","y","z","w")
    val result = dataset.withColumn("new", u(col("x"), col("y"), col("z"), col("w")))
    val ref = dataset.withColumn("new", col("x")+col("y")+col("z")+col("w"))
    checkEquiv(result, ref)
  }

  // this test covers getstatic and invokevirtual, shows we can handle math ops (only acos/asin)
  test("math functions - trig - (a)sin and (a)cos") {
    val myudf1: Double => Double = x => { math.cos(x) }
    val u1 = makeUdf(myudf1)
    val myudf2: Double => Double = x => { math.sin(x) }
    val u2 = makeUdf(myudf2)
    val myudf3: Double => Double = x => { math.acos(x) }
    val u3 = makeUdf(myudf3)
    val myudf4: Double => Double = x => { math.asin(x) }
    val u4 = makeUdf(myudf4)
    val dataset = List(1.0,2.0,3.0).toDS()
    val result = dataset.withColumn("new",
      u1(col("value")) + u2(col("value")) + u3(col("value")) + u4(col("value")))
    val ref = dataset.withColumn("new",
      cos(col("value"))+sin(col("value"))+acos(col("value"))+asin(col("value")))
    checkEquiv(result, ref)
  }

  test("math functions - trig - (a)tan(h) and cosh") {
    val myudf1: Double => Double = x => { math.tan(x) }
    val u1 = makeUdf(myudf1)
    val myudf2: Double => Double = x => { math.atan(x) }
    val u2 = makeUdf(myudf2)
    val myudf3: Double => Double = x => { math.cosh(x) }
    val u3 = makeUdf(myudf3)
    val myudf4: Double => Double = x => { math.tanh(x) }
    val u4 = makeUdf(myudf4)
    val dataset = List(1.0,2.0,3.0).toDS()
    val result = dataset.withColumn("new",
      u1(col("value")) + u2(col("value")) + u3(col("value")) + u4(col("value")))
    val ref = dataset.withColumn("new",
      tan(col("value")) + atan(col("value")) + cosh(col("value")) + tanh(col("value")))
    checkEquiv(result, ref)
  }

  test("math functions - abs, ceil, floor") {
    val myudf1: Double => Double = x => { math.abs(x) }
    val u1 = makeUdf(myudf1)
    val myudf2: Double => Double = x => { math.ceil(x) }
    val u2 = makeUdf(myudf2)
    val myudf3: Double => Double = x => { math.floor(x) }
    val u3 = makeUdf(myudf3)
    val dataset = List(-0.5,0.5).toDS()
    val result = dataset.withColumn("new", u2(u1(col("value")))+u3(u1(col("value"))))
    val ref = dataset.withColumn("new", ceil(abs(col("value"))) + floor(abs(col("value"))))
    checkEquiv(result, ref)
  }

  test("math functions - exp, log, log10, sqrt") {
    val myudf1: Double => Double = x => { math.exp(x) }
    val u1 = makeUdf(myudf1)
    val myudf2: Double => Double = x => { math.log(x) }
    val u2 = makeUdf(myudf2)
    val myudf3: Double => Double = x => { math.log10(x) }
    val u3 = makeUdf(myudf3)
    val myudf4: Double => Double = x => { math.sqrt(x) }
    val u4 = makeUdf(myudf4)
    val dataset = List(2.0,5.0).toDS()
    val result = dataset.withColumn("new",
      u1(col("value")) + u2(col("value")) + u3(col("value")) + u4(col("value")))
    val ref = dataset.withColumn("new",
      exp(col("value"))+logalias(col("value"))+log10(col("value"))+sqrt(col("value")))
    checkEquiv(result, ref)
  }

  test("FSTORE_0, LSTORE_1") {
    val myudf: () => Float = () => {
      val myFloat : Float = 1.0f
      myFloat
    }
    val dataset = List(5.0f).toDS()
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new",lit(1.0f))
    checkEquiv(result, ref)
  }

  test("LSTORE_0") {
    val myudf: () => Long = () => {
      val myLong : Long = 1L
      myLong
    }
    val dataset = List(1L).toDS()
    val u = makeUdf(myudf)
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new",lit(1L))
    checkEquiv(result, ref)
  }

  test("ILOAD") {
    val myudf: (Int, Int, Int, Int, Int, Long, Float, Double) => Int = (_, _, _, _, e, _, _, _) => {
      e
    }
    val u = makeUdf(myudf)
    val dataset = List((1,2,3,4,5,1L,1.0f,1.0)).toDF("a","b","c","d","e","f","g","h")
    val result = dataset.withColumn("new",
      u(col("a"),col("b"),col("c"),col("d"),
        col("e"),col("f"),col("g"),col("h")))
    val ref = dataset.withColumn("new", lit(5))
    checkEquiv(result, ref)
  }

  test("LLOAD") {
    val myudf: (Int, Int, Int, Int, Int, Long, Float, Double) => Long = {
      (_, _, _, _, _, f, _, _) => f
    }
    val u = makeUdf(myudf)
    val dataset = List((1,2,3,4,5,1L,1.0f,1.0)).toDF("a","b","c","d","e","f","g","h")
    val result = dataset.withColumn("new",
      u(col("a"),col("b"),col("c"),col("d"),
        col("e"),col("f"),col("g"),col("h")))
    val ref = dataset.withColumn("new", lit(1L))
    checkEquiv(result, ref)
  }

  test("FLOAD") {
    val myudf: (Int, Int, Int, Int, Int, Long, Float, Double) => Float = {
      (_, _, _, _, _, _, g, _) => g
    }
    val u = makeUdf(myudf)
    val dataset = List((1,2,3,4,5,1L,1.0f,1.0)).toDF("a","b","c","d","e","f","g","h")
    val result = dataset.withColumn("new",
      u(col("a"),col("b"),col("c"),col("d"),
        col("e"),col("f"),col("g"),col("h")))
    val ref = dataset.withColumn("new", lit(1.0f))
    checkEquiv(result, ref)
  }

  test("DLOAD") {
    val myudf: (Int, Int, Int, Int, Int, Long, Float, Double) => Double = {
      (_, _, _, _, _, _, _, h) => h
    }
    val u = makeUdf(myudf)
    val dataset = List((1,2,3,4,5,1L,1.0f,1.0)).toDF("a","b","c","d","e","f","g","h")
    val result = dataset.withColumn("new",
      u(col("a"),col("b"),col("c"),col("d"),
        col("e"),col("f"),col("g"),col("h")))
    val ref = dataset.withColumn("new", lit(1.0))
    checkEquiv(result, ref)
  }

  test("Cast Double to Int") {
    val myudf: () => Int = () => {
      var myVar : Double = 0.0
      myVar = myVar + 1.0
      val myVar2 : Int = myVar.toInt
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1))
    checkEquiv(result, ref)
  }

  test("Cast Float to Int") {
    val myudf: () => Int = () => {
      var myVar : Float = 0.0f
      myVar = myVar + 1.0f + 2.0f
      val myVar2 : Int = myVar.toInt
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(3))
    checkEquiv(result, ref)
  }

  test("Cast Long to Int") {
    val myudf: () => Int = () => {
      var myVar : Long = 0L
      myVar = myVar + 1L
      val myVar2 : Int = myVar.toInt
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1L))
    checkEquiv(result, ref)
  }

  test("Cast Int to Long") {
    val myudf: () => Long = () => {
      var myVar : Int = 0
      myVar = myVar + 1 + 2 + 3 + 4 + 5
      val myVar2 : Long = myVar.toLong
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(15L))
    checkEquiv(result, ref)
  }

  test("Cast Float to Long") {
    val myudf: () => Long = () => {
      var myVar : Float = 0.0f
      myVar = myVar + 1.0f + 2.0f
      val myVar2 : Long = myVar.toLong
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(3L))
    checkEquiv(result, ref)
  }

  test("Cast Double to Long") {
    val myudf: () => Long = () => {
      var myVar : Double = 0.0
      myVar = myVar + 1.0
      val myVar2 : Long = myVar.toLong
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1L))
    checkEquiv(result, ref)
  }


  test("Cast Int to Float") {
    val myudf: () => Float = () => {
      var myVar : Int = 0
      myVar = myVar + 1 + 2 + 3 + 4 + 5
      val myVar2 : Float = myVar.toFloat
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(15.0f))
    checkEquiv(result, ref)
  }

  test("Cast Long to Float") {
    val myudf: () => Float = () => {
      var myVar : Long = 0L
      myVar = myVar + 1L
      val myVar2 : Float = myVar.toFloat
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1.0f))
    checkEquiv(result, ref)
  }

  test("Cast Double to Float") {
    val myudf: () => Float = () => {
      var myVar : Double = 0.0
      myVar = myVar + 1.0
      val myVar2 : Float = myVar.toFloat
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1.0f))
    checkEquiv(result, ref)
  }

  test("Cast Int to Double") {
    val myudf: () => Double = () => {
      var myVar : Int = 0
      myVar = myVar + 1 + 2 + 3 + 4 + 5
      val myVar2 : Double = myVar.toDouble
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(15.0))
    checkEquiv(result, ref)
  }

  test("Cast Long to Double") {
    val myudf: () => Double = () => {
      var myVar : Long = 0L
      myVar = myVar + 1L
      val myVar2 : Double = myVar.toDouble
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(1.0))
    checkEquiv(result, ref)
  }

  test("Cast Float to Double") {
    val myudf: () => Double = () => {
      var myVar : Float = 0.0f
      myVar = myVar + 1.0f + 2.0f
      val myVar2 : Double = myVar.toDouble
      myVar2
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(3.0))
    checkEquiv(result, ref)
  }


  test("Cast Int to Short") {
    val myudf: () => Short = () => {
      val myVar : Int = 1
      val myVar2 : Short = myVar.toShort
      myVar2
    }
    val u = makeUdf(myudf)
    val myShort : Short = 1
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(myShort))
    checkEquiv(result, ref)
  }

  test("Cast Int to Byte") {
    val myudf: () => Byte = () => {
      val myVar : Int = 1
      val myVar2 : Byte = myVar.toByte
      myVar2
    }
    val u = makeUdf(myudf)
    val myByte : Byte = 1
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit(myByte))
    checkEquiv(result, ref)
  }

  test("ALOAD opcode") {
    val myudf: (Int, Int, Int, Int, String) => String = (_, _, _, _, e) => {
      e
    }
    val u = makeUdf(myudf)
    val dataset = List((1,2,3,4,"a")).toDF("x","y","z","w","a")
    val result = dataset.withColumn("new", u(col("x"),col("y"),col("z"),col("w"),col("a")))
    val ref = dataset.withColumn("new", lit("a"))
    checkEquiv(result, ref)
  }

  test("IFNONNULL opcode") {
    val myudf: (String, String) => String = (a,b) => {
      if (null == a) {
        a
      } else {
        b
      }
    }
    val u = makeUdf(myudf)
    val dataset = List(("","z")).toDF("x","y")
    val result = dataset.withColumn("new", u(col("x"),col("y")))
    val ref = dataset.withColumn("new", lit("z"))
    checkEquiv(result, ref)
  }

  test("IFNULL opcode") {
    val myudf: (String, String) => String = (a,b) => {
      if (null!=a) {
        b
      } else {
        a
      }
    }
    val u = makeUdf(myudf)
    val dataset = List(("","z")).toDF("x","y")
    val result = dataset.withColumn("new", u(col("x"),col("y")))
    val ref = dataset.withColumn("new", lit("z"))
    checkEquiv(result, ref)
  }

  test("IFNE opcode") {
    val myudf: (Double, Double) => Double = (a,b) => {
      if (a==b) {
        b
      } else {
        a
      }
    }
    val u = makeUdf(myudf)
    val dataset = List((1.0,2.0)).toDF("x","y")
    val result = dataset.withColumn("new", u(col("x"),col("y")))
    val ref = dataset.withColumn("new", lit(1.0))
    checkEquiv(result, ref)
  }

  test("IFEQ opcode") {
    val myudf: (Double, Double) => Double = (a,b) => {
      if (a != b) {
        b
      } else {
        a
      }
    }
    val u = makeUdf(myudf)
    val dataset = List((1.0,2.0)).toDF("x","y")
    val result = dataset.withColumn("new", u(col("x"),col("y")))
    val ref = dataset.withColumn("new", lit(2.0))
    checkEquiv(result, ref)
  }

  test("Conditional simplification - if (c) true else false => c") {
    val myudf: (Int) => Boolean = i => {
      if (i < 20) true else false
    }
    val u = makeUdf(myudf)
    val dataset = List(20, 19).toDF("x")
    val result = dataset.withColumn("new", u(col("x")))
    assert(udfIsCompiled(result))
    assert(!result.queryExecution.analyzed.toString.contains("if"))
  }

  test("Conditional simplification - if (c) false else true => !c") {
    val myudf: (Int) => Boolean = i => {
      if (i < 20) false else true
    }
    val u = makeUdf(myudf)
    val dataset = List(20, 19).toDF("x")
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", col("x") >= 20)
    checkEquiv(result, ref)
    assert(!result.queryExecution.analyzed.toString.contains("if"))
  }

  test("LDC_W opcode") {
    val myudf: () => String = () => {
      val myString : String = "a"
      myString
    }
    val u = makeUdf(myudf)
    val dataset = List(1.0).toDS()
    val result = dataset.withColumn("new", u())
    val ref = dataset.withColumn("new", lit("a"))
    checkEquiv(result, ref)
  }

  test("DUP opcode") {
    val myudf: (Int) => Int = (a) => {
      val mine : Int = (a + a)*a
      val mine2 : Int = a*mine + a
      mine2
    }
    val u = makeUdf(myudf)
    val dataset = List(2).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(18))
    checkEquiv(result, ref)
  }

  test("FALLBACK TO CPU: math functions - unsupported") {
    val myudf1: Double => Double = x => { math.log1p(x) }
    val u1 = makeUdf(myudf1)
    val dataset = List(2.0,5.0).toDS()
    val result = dataset.withColumn("new", u1(col("value")))
    val ref = dataset.withColumn("new", logalias(col("value")+1.0))
    checkEquivNotCompiled(result, ref)
  }

  test("FALLBACK TO CPU: exception handling - unsupported") {
    val myudf1: Int => Int = { a =>
      var x:Int = 0
      try {
        x = a
      } catch {
        case _: Exception => { }
      }
      x
    }

    val u1 = makeUdf(myudf1)
    val dataset = List(1, 5, 3, 7).toDS()
    val result = dataset.withColumn("new", u1(col("value")))
    val ref = dataset.withColumn("new", col("value"))
    checkEquivNotCompiled(result, ref)
  }

  test("conditional doubles test2") {
    val myudf: Double => Double = { x =>
      val t =
        if (x < 2.0 && x < 3.7) {
          5.0
        } else {
          6.0
        }
      t + 2.2
    }
    val u = makeUdf(myudf)
    val dataset = List(1.0).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(7.2))
    checkEquiv(result, ref)
    val dataset2 = List(3.0).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(8.2))
    checkEquiv(result2, ref2)
  }

  test("conditional doubles test3") {
    val myudf: Double => Double = { x =>
      val t =
        if (x <= 2.0 && x <= 3.7) {
          5.0
        } else {
          6.0
        }
      t + 2.2
    }
    val u = makeUdf(myudf)
    val dataset = List(1.0).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(7.2))
    checkEquiv(result, ref)
    val dataset2 = List(3.0).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(8.2))
    checkEquiv(result2, ref2)
  }

  test("conditional doubles test4") {
    val myudf: Double => Double = { x =>
      val t =
        if (x > 2.0 && x >= 3.7) {
          5.0
        } else {
          6.0
        }
      t + 2.2
    }
    val u = makeUdf(myudf)
    val dataset = List(1.0).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(8.2))
    checkEquiv(result, ref)
    val dataset2 = List(4.0).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(7.2))
    checkEquiv(result2, ref2)
  }

  test("conditional ints test2") {
    val myudf: Int => Int = { x =>
      val t =
        if (x > 1 && x <= 3) {
          (if (x >= 2 && x < 3) 0 else 1) + 2
        } else {
          if (x != 1) 3 else 4
        }
      t + 5
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(9))
    checkEquiv(result, ref)
    val dataset2 = List(2).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(7))
    checkEquiv(result2, ref2)
  }

  test("conditional ints test3") {
    val myudf: Int => Int = { x =>
      val t =
        if (x > 1 && x <= 3) {
          (if (x >= 2 && x < 3) -1 else 1) + 2
        } else {
          if (x == 1) 3 else 4
        }
      t + 5
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(8))
    checkEquiv(result, ref)
    val dataset2 = List(2).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(6))
    checkEquiv(result2, ref2)
  }

  test("double div and mod") {
    val myudf: Double => Double = { x =>
      val ret : Double = (-x / 2.0) % 2.0
      ret
    }
    val u = makeUdf(myudf)
    val dataset = List(-8.0).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(0.0))
    checkEquiv(result, ref)
    val dataset2 = List(-9.0).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(0.5))
    checkEquiv(result2, ref2)
  }

  test("float div and mod") {
    val myudf: Float => Float = { x =>
      val temp1 : Float = -x / 2.0f
      val ret : Float = temp1 % 2.0f
      ret
    }
    val u = makeUdf(myudf)
    val dataset = List(-8.0f).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(0.0f))
    checkEquiv(result, ref)
    val dataset2 = List(-9.0f).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(0.5f))
    checkEquiv(result2, ref2)
  }


  test("int div and mod") {
    val myudf: Int => Int = { x =>
      val temp1 : Int = -x / 2
      val ret : Int = temp1 % 2
      ret
    }
    val u = makeUdf(myudf)
    val dataset = List(-8).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(0))
    checkEquiv(result, ref)
    val dataset2 = List(-10).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(1))
    checkEquiv(result2, ref2)
  }

  test("long div and mod") {
    val myudf: Long => Long = { x =>
      val temp1 : Long = -x / 2
      val ret : Long = temp1 % 2
      ret
    }
    val u = makeUdf(myudf)
    val dataset = List(-8L).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(0L))
    checkEquiv(result, ref)
    val dataset2 = List(-10L).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(1L))
    checkEquiv(result2, ref2)
  }

  test("int bitwise") {
    val myudf: Int => Int = { x =>
      val temp2 : Int = (x >> 2) >>> 1
      val temp3 : Int = temp2 << 1
      val ret : Int = x & (x | (temp2 ^ temp3))
      ret
    }
    val u = makeUdf(myudf)
    val dataset = List(16).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(16))
    checkEquiv(result, ref)
  }

  test("long bitwise") {
    val myudf: Long => Long = { x =>
      val ret : Long = x & (x | ((x >> 2) >>> 1 ^ (x >> 2) >>> 1 << 1))
      ret
    }
    val u = makeUdf(myudf)
    val dataset = List(16L).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(16L))
    checkEquiv(result, ref)
  }

  test("TABLESWITCH TEST") {
    val myudf: (Int) => Int = a => {
      a match {
        case 2 => a+a;
        case 3 => a*a;
        case _ => a-a
      }
    }

    val u = makeUdf(myudf)
    val dataset = List(2).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(4))
    checkEquiv(result, ref)
    val dataset2 = List(3).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(9))
    checkEquiv(result2, ref2)
    val dataset3 = List(4).toDS()
    val result3 = dataset3.withColumn("new", u(col("value")))
    val ref3 = dataset3.withColumn("new", lit(0))
    checkEquiv(result3, ref3)
  }

  test("LOOKUPSWITCH TEST") {
    val myudf: (Int) => Int = a => {
      a match {
        case 1 => a+a;
        case 100 => a*a;
        case _ => a-a
      }
    }

    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(2))
    checkEquiv(result, ref)
    val dataset2 = List(100).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(10000))
    checkEquiv(result2, ref2)
    val dataset3 = List(4).toDS()
    val result3 = dataset3.withColumn("new", u(col("value")))
    val ref3 = dataset3.withColumn("new", lit(0))
    checkEquiv(result3, ref3)
  }

  test("float constant in a function call") {
    val myudf: (Float) => Float = _ => {
      val myFloat : Float = math.abs(-2.0f)
      myFloat
    }
    val u = makeUdf(myudf)
    val dataset = List(1.0f).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(2.0f))
    checkEquiv(result, ref)
  }

  test("int constant in a function call") {
    val myudf: (Int) => Int = _ => {
      val myInt : Int = math.abs(-2)
      myInt
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(2))
    checkEquiv(result, ref)
  }

  test("conditional ints - AND(LT,LT)") {
    val myudf: Int => Int = { x =>
      val t =
        if (x < 1 && x < 3) {
          2
        } else {
          3
        }
      t + 5
    }
    val u = makeUdf(myudf)
    val dataset = List(0).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(7))
    checkEquiv(result, ref)
    val dataset2 = List(2).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(8))
    checkEquiv(result2, ref2)
  }

  test("conditional ints - AND(LTE,LTE)") {
    val myudf: Int => Int = { x =>
      val t =
        if (x <= 1 && x <= 3) {
          2
        } else {
          3
        }
      t + 5
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(7))
    checkEquiv(result, ref)
    val dataset2 = List(2).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(8))
    checkEquiv(result2, ref2)
  }

  test("conditional ints - AND(LTE,LT)") {
    val myudf: Int => Int = { x =>
      val t =
        if (x <= 1 && x < 3) {
          2
        } else {
          3
        }
      t + 5
    }
    val u = makeUdf(myudf)
    val dataset = List(1).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(7))
    checkEquiv(result, ref)
    val dataset2 = List(2).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(8))
    checkEquiv(result2, ref2)
  }

  test("conditional ints - AND(GT,GT)") {
    val myudf: Int => Int = { x =>
      val t =
        if (x > 1 && x > 3) {
          2
        } else {
          3
        }
      t + 5
    }
    val u = makeUdf(myudf)
    val dataset = List(0).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(8))
    checkEquiv(result, ref)
    val dataset2 = List(4).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(7))
    checkEquiv(result2, ref2)
  }


  test("conditional ints - AND(GT,GTE)") {
    val myudf: Int => Int = { x =>
      val t =
        if (x > 1 && x >= 3) {
          2
        } else {
          3
        }
      t + 5
    }
    val u = makeUdf(myudf)
    val dataset = List(0).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(8))
    checkEquiv(result, ref)
    val dataset2 = List(3).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(7))
    checkEquiv(result2, ref2)
  }

  test("conditional ints - OR(GT,GTE)") {
    val myudf: Int => Int = { x =>
      val t =
        if (x < 1 && x <= 3) {
          2
        } else {
          3
        }
      t + 5
    }
    val u = makeUdf(myudf)
    val dataset = List(0).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(7))
    checkEquiv(result, ref)
    val dataset2 = List(4).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(8))
    checkEquiv(result2, ref2)
  }

  test("conditional longs - AND(LT,LT)") {
    val myudf: Long => Long = { x =>
      val t =
        if (x < 1L && x < 3L) {
          2L
        } else {
          3L
        }
      t + 5L
    }
    val u = makeUdf(myudf)
    val dataset = List(0L).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(7L))
    checkEquiv(result, ref)
    val dataset2 = List(2L).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(8L))
    checkEquiv(result2, ref2)
  }

  test("conditional longs - AND(LTE,LTE)") {
    val myudf: Long => Long = { x =>
      val t =
        if (x <= 1L && x <= 3L) {
          2L
        } else {
          3L
        }
      t + 5L
    }
    val u = makeUdf(myudf)
    val dataset = List(1L).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(7L))
    checkEquiv(result, ref)
    val dataset2 = List(2L).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(8L))
    checkEquiv(result2, ref2)
  }

  test("conditional longs - AND(LTE,LT)") {
    val myudf: Long => Long = { x =>
      val t =
        if (x <= 1L && x < 3L) {
          2L
        } else {
          3L
        }
      t + 5L
    }
    val u = makeUdf(myudf)
    val dataset = List(1L).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(7L))
    checkEquiv(result, ref)
    val dataset2 = List(2L).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(8L))
    checkEquiv(result2, ref2)
  }

  test("conditional longs - AND(GT,GT)") {
    val myudf: Long => Long = { x =>
      val t =
        if (x > 1L && x > 3L) {
          2L
        } else {
          3L
        }
      t + 5L
    }
    val u = makeUdf(myudf)
    val dataset = List(0L).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(8L))
    checkEquiv(result, ref)
    val dataset2 = List(4L).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(7L))
    checkEquiv(result2, ref2)
  }

  test("conditional longs - AND(GT,GTE)") {
    val myudf: Long => Long = { x =>
      val t =
        if (x > 1L && x >= 3L) {
          2L
        } else {
          3L
        }
      t + 5L
    }
    val u = makeUdf(myudf)
    val dataset = List(0L).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(8L))
    checkEquiv(result, ref)
    val dataset2 = List(3L).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(7L))
    checkEquiv(result2, ref2)
  }

  test("conditional longs - OR(GT,GTE)") {
    val myudf: Long => Long = { x =>
      val t =
        if (x < 1L && x <= 3L) {
          2L
        } else {
          3L
        }
      t + 5L
    }
    val u = makeUdf(myudf)
    val dataset = List(0L).toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", lit(7L))
    checkEquiv(result, ref)
    val dataset2 = List(4L).toDS()
    val result2 = dataset2.withColumn("new", u(col("value")))
    val ref2 = dataset2.withColumn("new", lit(8L))
    checkEquiv(result2, ref2)
  }

  test("FALLBACK TO CPU: loops") {
    val myudf: (Int, Int) => Int = (a,b) => {
      var myVar : Int = 0
      for (_ <- a to b){
        myVar = myVar + 1
      }
      myVar
    }
    val u = makeUdf(myudf)
    val dataset = List((1,5)).toDF("x","y")
    val result = dataset.withColumn("new", u(col("x"),col("y")))
    val ref = dataset.withColumn("new", lit(5))
    checkEquivNotCompiled(result, ref)
  }

  // Tests for string ops
  test("java lang string builder test - append") {
    // We do not support java.lang.StringBuilder officially in the udf compiler,
    // but string + string in Scala generates some code with
    // java.lang.StringBuilder. For that reason, we have some tests with
    // java.lang.StringBuilder.
    val myudf: (String, String, Boolean) => String = (a,b,c) => {
      val sb = new java.lang.StringBuilder()
      if (c) {
        sb.append(a)
        sb.append(" ")
        sb.append(b)
        sb.toString + "@@@" + " true"
      } else {
        sb.append(b)
        sb.append(" ")
        sb.append(a)
        sb.toString + "!!!" + " false"
      }
    }
    val u = makeUdf(myudf)
    val dataset = List(("Hello", "World", false),
                       ("Oh", "Hello", true)).toDF("x","y","z").repartition(1)
    val result = dataset.withColumn("new", u(col("x"),col("y"),col("z")))
    val ref = List(("Hello", "World", false, "World Hello!!! false"),
                   ("Oh", "Hello", true, "Oh Hello@@@ true")).toDF
    checkEquiv(result, ref)
  }

  test("string test - + concat") {
    val myudf: (String, String) => String = (a,b) => {
      a+b
    }
    val u = makeUdf(myudf)
    val dataset = List(("Hello"," World"),("Oh"," Hello")).toDF("x","y").repartition(1)
    val result = dataset.withColumn("new", u(col("x"),col("y")))
    val ref = dataset.withColumn("new", concat(col("x"),col("y")))
    checkEquiv(result, ref)
  }

  test("string test - concat method") {
    val myudf: (String, String) => String = (a,b) => {
      a.concat(b)
    }
    val u = makeUdf(myudf)
    val dataset = List(("Hello"," World"),("Oh"," Hello")).toDF("x","y").repartition(1)
    val result = dataset.withColumn("new", u(col("x"),col("y")))
    val ref = dataset.withColumn("new", concat(col("x"),col("y")))
    checkEquiv(result, ref)
  }

  test("string test - equalsIgnoreCase") {
    val myudf: (String, String) => Boolean = (a,b) => {
      a.equalsIgnoreCase(b)
    }
    val u = makeUdf(myudf)
    val dataset = List(("Hello","hELLO"),("helLo","HeLLo")).toDF("x","y").repartition(1)
    val result = dataset.withColumn("new", u(col("x"),col("y")))
    val ref = dataset.withColumn("new", upper(col("x")).equalTo(upper(col("y"))))
    checkEquiv(result, ref)
  }

  test("string test - toUpperCase") {
    val myudf: (String) => String = a => {
      a.toUpperCase()
    }
    val u = makeUdf(myudf)
    val dataset = List("hello").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("HELLO"))
    checkEquiv(result, ref)
  }

  test("string test - toLowerCase") {
    val myudf: (String) => String = a => {
      a.toLowerCase()
    }
    val u = makeUdf(myudf)
    val dataset = List("HELLO").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("hello"))
    checkEquiv(result, ref)
  }

  test("string test - trim") {
    val myudf: (String) => String = a => {
      a.trim()
    }
    val u = makeUdf(myudf)
    val dataset = List("   hello   ").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("hello"))
    checkEquiv(result, ref)
  }

  test("string test - subtring - start index") {
    val myudf: (String) => String = a => {
      a.substring(2)
    }
    val u = makeUdf(myudf)
    val dataset = List("hello").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("llo"))
    checkEquiv(result, ref)
  }

  test("string test - subtring - start and end index") {
    val myudf: (String) => String = a => {
      a.substring(2,7)
    }
    val u = makeUdf(myudf)
    val dataset = List("hello sir").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("llo s"))
    checkEquiv(result, ref)
  }

  test("string test - replace character") {
    val myudf: (String) => String = a => {
      a.replace("r","s")
    }
    val u = makeUdf(myudf)
    val dataset = List("rocket").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("socket"))
    checkEquiv(result, ref)
  }

  test("string test - replace character sequence") {
    val myudf: (String) => String = a => {
      a.replace("r","fr")
    }
    val u = makeUdf(myudf)
    val dataset = List("rocket").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("frocket"))
    checkEquiv(result, ref)
  }

  test("string test - startsWith") {
    val myudf: (String) => Boolean = a => {
      a.startsWith("ro")
    }
    val u = makeUdf(myudf)
    val dataset = List("rocket", "frocket").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", col("x").startsWith("ro"))
    checkEquiv(result, ref)
  }

  test("string test - endsWith") {
    val myudf: (String) => Boolean = a => {
      a.endsWith("et")
    }
    val u = makeUdf(myudf)
    val dataset = List("rocket", "frocknah").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", col("x").endsWith("et"))
    checkEquiv(result, ref)
  }

  test("string test - equals") {
    val myudf: (String, String) => Boolean = (a,b) => {
      a.equals(b)
    }
    val u = makeUdf(myudf)
    val dataset = List(("rocket", "frocket"),("yep", "yep")).toDF("x","y").repartition(1)
    val result = dataset.withColumn("new", u(col("x"),col("y")))
    val ref = dataset.withColumn("new", lit(col("x").equalTo(col("y"))))
    checkEquiv(result, ref)
  }

  test("string test - length") {
    val myudf: (String) => Int = a => {
      a.length()
    }
    val u = makeUdf(myudf)
    val dataset = List("rocket","").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", length(col("x")))
    checkEquiv(result, ref)
  }

  test("string test - isEmpty") {
    val myudf: (String) => Boolean = a => {
      a.isEmpty()
    }
    val u = makeUdf(myudf)
    val dataset = List("rocket","").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit(length(col("x")).equalTo(0)))
    checkEquiv(result, ref)
  }

  test("string test - valueOf - Boolean") {
    val myudf: (Boolean) => String = a => {
      String.valueOf(a)
    }
    val u = makeUdf(myudf)
    val dataset = List(true).toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("true"))
    checkEquiv(result, ref)
  }

  test("string test - valueOf - Char") {
    val myudf: (Char) => String = a => {
      String.valueOf(a)
    }
    val u = makeUdf(myudf)
    val dataset = List(5).toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("5"))
    checkEquiv(result, ref)
  }

  test("string test - valueOf - Double") {
    val myudf: (Double) => String = a => {
      String.valueOf(a)
    }
    val u = makeUdf(myudf)
    val dataset = List(2.0).toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("2.0"))
    checkEquiv(result, ref)
  }

  test("string test - valueOf - Float") {
    val myudf: (Float) => String = a => {
      String.valueOf(a)
    }
    val u = makeUdf(myudf)
    val dataset = List(2.0f).toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("2.0"))
    checkEquiv(result, ref)
  }

  test("string test - valueOf - Int") {
    val myudf: (Int) => String = a => {
      String.valueOf(a)
    }
    val u = makeUdf(myudf)
    val dataset = List(2).toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("2"))
    checkEquiv(result, ref)
  }

  test("string test - valueOf - Long") {
    val myudf: (Long) => String = a => {
      String.valueOf(a)
    }
    val u = makeUdf(myudf)
    val dataset = List(2L).toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit("2"))
    checkEquiv(result, ref)
  }

  test("FALLBACK TO CPU: string test - charAt") {
    val myudf: (String) => Int = a => {
      a.charAt(2).toInt
    }
    val u = makeUdf(myudf)
    val dataset = List("mh7mmmm").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit(55)) // utf8 for "7"
    checkEquivNotCompiled(result, ref)
  }

  test("string test - contains") {
    val myudf: (String) => Boolean = a => {
      a.contains("hmmm")
    }
    val u = makeUdf(myudf)
    val dataset = List("mh7mmmm","mhmmmm").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", col("x").contains("hmmm"))
    checkEquiv(result, ref)
  }

  test("string test - indexOf - case 1 - str,int") {
    val myudf: (String) => Int = a => {
      a.indexOf("c",1)
    }
    val u = makeUdf(myudf)
    val dataset = List("abcde").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit(2))
    checkEquiv(result, ref)
  }

  test("FALLBACK TO CPU: string test - indexOf - case 2 - char - single quotes,int") {
    val myudf: (String) => Int = a => {
      a.indexOf('c',1)
    }
    val u = makeUdf(myudf)
    val dataset = List("abcde").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit(2))
    checkEquivNotCompiled(result, ref)
  }

  test("FALLBACK TO CPU: string test - indexOf - case 3 - char - utf value,int") {
    val myudf: (String) => Int = a => {
      a.indexOf(99,1)
    }
    val u = makeUdf(myudf)
    val dataset = List("abcde").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit(2))
    checkEquivNotCompiled(result, ref)
  }

  test("FALLBACK TO CPU: string test - indexOf - case 4 - char - utf value") {
    val myudf: (String) => Int = a => {
      a.indexOf(99)
    }
    val u = makeUdf(myudf)
    val dataset = List("abcde").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit(2))
    checkEquivNotCompiled(result, ref)
  }

  test("FALLBACK TO CPU: string test - indexOf - case 5 - char - single quotes") {
    val myudf: (String) => Int = a => {
      a.indexOf('c')
    }
    val u = makeUdf(myudf)
    val dataset = List("abcde").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit(2))
    checkEquivNotCompiled(result, ref)
  }

  test("string test - indexOf - case 6 - str") {
    val myudf: (String) => Int = a => {
      a.indexOf("c")
    }
    val u = makeUdf(myudf)
    val dataset = List("abcde").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit(2))
    checkEquiv(result, ref)
  }

  // this test is an expected fallback to JVM execution due to no support for String.codePointAt
  // method
  test("FALLBACK TO CPU: string test - codePointAt method. ") {
    val myudf: (String) => Int = a => {
      a.codePointAt(2)
    }
    val u = makeUdf(myudf)
    val dataset = List("abcde").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit(99))
    checkEquivNotCompiled(result, ref)
  }

  // this test is an expected fallback to JVM execution due to no support for String.matches method
  test("FALLBACK TO CPU: string test - matches method. ") {
    val myudf: (String) => Boolean = a => {
      a.matches("[a-z]*.txt" )
    }
    val u = makeUdf(myudf)
    val dataset = List("thisfile.txt").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit(true))
    checkEquivNotCompiled(result, ref)
  }

  test("string test - replaceAll method") {
    val myudf: (String) => String = a => {
      a.replaceAll("m{2}.m{2}","replaced")
    }
    val u = makeUdf(myudf)
    val dataset = List("mmhmm.txt").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit("replaced.txt"))
    checkEquiv(result, ref)
  }

  test("string test - split method - case 1") {
    val myudf: (String) => Array[String] = a => {
      a.split("l{2}.l{2}")
    }
    val u = makeUdf(myudf)
    val dataset = List("firstllollsecond").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit(Array("first","second")))
    checkEquiv(result, ref)
  }

  test("string test - split method - case 2") {
    val myudf: (String) => Array[String] = a => {
      a.split("l{2}.l{2}",3)
    }
    val u = makeUdf(myudf)
    val dataset = List("firstllollsecondllollthirdllollfourth").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit(Array("first","second","thirdllollfourth")))
    checkEquiv(result, ref)
  }

  test("string test - getBytes - case 1 - default platform charset") {
    val myudf: (String) => Array[Byte] = a => {
      a.getBytes()
    }
    val u = makeUdf(myudf)
    val dataset = List("this is the string").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit("this is the string".getBytes()))
    checkEquiv(result, ref)
  }

  test("string test - getBytes - case 2 - charsetName -- string") {
    val myudf: (String) => Array[Byte] = a => {
      a.getBytes("US-ASCII")
    }
    val u = makeUdf(myudf)
    val dataset = List("this is the string").toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new",lit("this is the string".getBytes("US-ASCII")))
    checkEquiv(result, ref)
  }

  test("Float - isNaN - True") {
    val myudf: (Float) => Boolean = a => {
      a.isNaN()
    }
    val u = makeUdf(myudf)
    val dataset = List(Float.NaN).toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit(Float.NaN.isNaN))
    checkEquiv(result, ref)
  }

  test("Float - isNaN - False") {
    val myudf: (Float) => Boolean = a => {
      a.isNaN()
    }
    val u = makeUdf(myudf)
    val dataset = List(2f).toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit(false))
    checkEquiv(result, ref)
  }

  test("Double - isNaN - True") {
    val myudf: (Double) => Boolean = a => {
      a.isNaN()
    }
    val u = makeUdf(myudf)
    val dataset = List(Double.NaN).toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit(Double.NaN.isNaN))
    checkEquiv(result, ref)
  }

  test("Double - isNaN - False") {
    val myudf: (Float) => Boolean = a => {
      a.isNaN()
    }
    val u = makeUdf(myudf)
    val dataset = List(2D).toDF("x").repartition(1)
    val result = dataset.withColumn("new", u(col("x")))
    val ref = dataset.withColumn("new", lit(false))
    checkEquiv(result, ref)
  }

  test("FALLBACK TO CPU: Non-literal date time pattern") {
    val myudf: (String, String) => Int = (a, pattern) => {
      val formatter = DateTimeFormatter.ofPattern(pattern)
      val ldt = LocalDateTime.parse(a, formatter)
      ldt.getDayOfMonth
    }
    val u = makeUdf(myudf)
    val dataset = Seq(("2020-08-20 01:23:45", "yyyy-MM-dd HH:mm:ss"))
      .toDF("DateTime", "Pattern").repartition(1)
    val result = dataset.withColumn("dayOfMonth", u(col("DateTime"), col("Pattern")))
    val ref = dataset.withColumn("dayOfMonth", dayofmonth(col("DateTime")))
    checkEquivNotCompiled(result, ref)
  }

  test("FALLBACK TO CPU: Unsupported date time pattern") {
    val myudf: (String) => Int = a => {
      val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
      val ldt = LocalDateTime.parse(a, formatter)
      ldt.getDayOfMonth
    }
    val u = makeUdf(myudf)
    val dataset = Seq("2020-08-20T01:23:45").toDF("DateTime").repartition(1)
    val result = dataset.withColumn("dayOfMonth", u(col("DateTime")))
    val ref = dataset.withColumn("dayOfMonth", dayofmonth(col("DateTime")))
    checkEquivNotCompiled(result, ref)
  }

  test("Get day of month from LocalDateTime string") {
    val myudf: (String) => Int = a => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
      val ldt = LocalDateTime.parse(a, formatter)
      ldt.getDayOfMonth
    }
    val u = makeUdf(myudf)
    val dataset = Seq("2020-08-20T01:23:45").toDF("DateTime").repartition(1)
    val result = dataset.withColumn("dayOfMonth", u(col("DateTime")))
    val ref = dataset.withColumn("dayOfMonth", dayofmonth(col("DateTime")))
    checkEquiv(result, ref)
  }

  test("Get hour from LocalDateTime string") {
    val myudf: (String) => Int = a => {
      val formatter = DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss")
      val ldt = LocalDateTime.parse(a, formatter)
      ldt.getHour
    }
    val u = makeUdf(myudf)
    val dataset = Seq("08-20-2020 01:23:45").toDF("DateTime").repartition(1)
    val result = dataset.withColumn("hour", u(col("DateTime")))
    val ref = dataset.withColumn("hour", hour(to_timestamp(col("DateTime"), "MM-dd-yyyy HH:mm:ss")))
    checkEquiv(result, ref)
  }

  test("Get minute from LocalDateTime string") {
    val myudf: (String) => Int = a => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val ldt = LocalDateTime.parse(a, formatter)
      ldt.getMinute
    }
    val u = makeUdf(myudf)
    val dataset = Seq("2020-08-20 01:23:45").toDF("DateTime").repartition(1)
    val result = dataset.withColumn("minute", u(col("DateTime")))
    val ref = dataset.withColumn("minute", minute(col("DateTime")))
    checkEquiv(result, ref)
  }

  test("get month from LocalDataTime string") {
    val myudf: (String) => Int = a => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MMM-dd")
      val ldt = LocalDateTime.parse(a, formatter)
      ldt.getMonthValue
    }
    val u = makeUdf(myudf)
    val dataset = Seq("2020-Aug-20").toDF("DateTime").repartition(1)
    val result = dataset.withColumn("month", u(col("DateTime")))
    val ref = dataset.withColumn("month", month(to_timestamp(col("DateTime"), "yyyy-MMM-dd")))
    checkEquiv(result, ref)
  }

  test("get second from LocalDateTime string") {
    val myudf: (String) => Int = a => {
      val formatter = DateTimeFormatter.ofPattern("HH:mm:ss")
      val ldt = LocalDateTime.parse(a, formatter)
      ldt.getSecond
    }
    val u = makeUdf(myudf)
    val dataset = Seq("01:23:45").toDF("DateTime").repartition(1)
    val result = dataset.withColumn("month", u(col("DateTime")))
    val ref = dataset.withColumn("month", second(col("DateTime")))
    checkEquiv(result, ref)
  }

  test("get year from LocalDateTime string") {
    val myudf: (String) => Int = a => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val ldt = LocalDateTime.parse(a, formatter)
      ldt.getYear
    }
    val u = makeUdf(myudf)
    val dataset = Seq("2020-08-20").toDF("DateTime").repartition(1)
    val result = dataset.withColumn("month", u(col("DateTime")))
    val ref = dataset.withColumn("month", year(col("DateTime")))
    checkEquiv(result, ref)
  }

  test("FALLBACK TO CPU: Get hour from zoned LocalDateTime string") {
    val myudf: (String) => Int = a => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZZZZZ'['VV']'")
      val ldt = LocalDateTime.parse(a, formatter)
      ldt.getHour
    }
    val u = makeUdf(myudf)
    val dataset = Seq("2011-12-03T10:15:30+01:00[Europe/Paris]",
                      "2011-12-03T10:15:30+09:00[Asia/Tokyo]").toDF("DateTime").repartition(1)
    val result = dataset.withColumn("hour", u(col("DateTime")))
    val ref = dataset.withColumn("hour", lit(10))
    checkEquivNotCompiled(result, ref)
  }

  test("Get hour from pattern with escaped text") {
    val myudf: (String) => Int = a => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'''VX'HH:mm:ss'''''Z'''")
      val ldt = LocalDateTime.parse(a, formatter)
      ldt.getHour
    }
    val u = makeUdf(myudf)
    val dataset = Seq("2011-12-03'VX10:15:30''Z'").toDF("DateTime").repartition(1)
    val result = dataset.withColumn("hour", u(col("DateTime")))
    val ref = dataset.withColumn("hour", lit(myudf("2011-12-03'VX10:15:30''Z'")))
    checkEquiv(result, ref)
  }

  test("Empty string construction") {
    val myudf: () => String = () => ""
    val u = makeUdf(myudf)
    val dataset = spark.sql("select null").repartition(1)
    val result = dataset.select(u().as("emptyStr"))
    val ref = dataset.select(lit("").as("emptyStr"))
    checkEquiv(result, ref)
  }

  test("Empty array construction - Boolean") {
    val myudf: () => Array[Boolean] = () => Array.empty[Boolean]
    val u = makeUdf(myudf)
    val dataset = spark.sql("select null").repartition(1)
    val result = dataset.select(u().as("emptyArrOfBoolean"))
    val ref = dataset.select(lit(Array.empty[Boolean]).as("emptyArrOfBoolean"))
    checkEquiv(result, ref)
  }

  test("Empty array construction - Byte") {
    val myudf: () => Array[Byte] = () => Array.empty[Byte]
    val u = makeUdf(myudf)
    val dataset = spark.sql("select null").repartition(1)
    val result = dataset.select(u().as("emptyArrOfByte"))
    val ref = dataset.select(lit(Array.empty[Byte]).as("emptyArrOfByte"))
    checkEquiv(result, ref)
  }

  test("Empty array construction - Short") {
    val myudf: () => Array[Short] = () => Array.empty[Short]
    val u = makeUdf(myudf)
    val dataset = spark.sql("select null").repartition(1)
    val result = dataset.select(u().as("emptyArrOfShort"))
    val ref = dataset.select(lit(Array.empty[Short]).as("emptyArrOfShort"))
    checkEquiv(result, ref)
  }

  test("Empty array construction - Int") {
    val myudf: () => Array[Int] = () => Array.empty[Int]
    val u = makeUdf(myudf)
    val dataset = spark.sql("select null").repartition(1)
    val result = dataset.select(u().as("emptyArrOfInt"))
    val ref = dataset.select(lit(Array.empty[Int]).as("emptyArrOfInt"))
    checkEquiv(result, ref)
  }

  test("Empty array construction - Long") {
    val myudf: () => Array[Long] = () => Array.empty[Long]
    val u = makeUdf(myudf)
    val dataset = spark.sql("select null").repartition(1)
    val result = dataset.select(u().as("emptyArrOfLong"))
    val ref = dataset.select(lit(Array.empty[Long]).as("emptyArrOfLong"))
    checkEquiv(result, ref)
  }

  test("Empty array construction - Float") {
    val myudf: () => Array[Float] = () => Array.empty[Float]
    val u = makeUdf(myudf)
    val dataset = spark.sql("select null").repartition(1)
    val result = dataset.select(u().as("emptyArrOfFloat"))
    val ref = dataset.select(lit(Array.empty[Float]).as("emptyArrOfFloat"))
    checkEquiv(result, ref)
  }

  test("Empty array construction - Double") {
    val myudf: () => Array[Double] = () => Array.empty[Double]
    val u = makeUdf(myudf)
    val dataset = spark.sql("select null").repartition(1)
    val result = dataset.select(u().as("emptyArrOfDbl"))
    val ref = dataset.select(lit(Array.empty[Double]).as("emptyArrOfDbl"))
    checkEquiv(result, ref)
  }

  test("Empty array construction - String") {
    val myudf: () => Array[String] = () => Array.empty[String]
    val u = makeUdf(myudf)
    val dataset = spark.sql("select null").repartition(1)
    val result = dataset.select(u().as("emptyArrOfStr"))
    val ref = dataset.select(lit(Array.empty[String]).as("emptyArrOfStr"))
    checkEquiv(result, ref)
  }

  test("final static method call inside UDF") {
    def simple_func2(str: String): Int = {
      str.length
    }
    def simple_func1(str: String): Int = {
      simple_func2(str) + simple_func2(str)
    }
    val myudf: (String) => Int = str => {
      simple_func1(str)
    }
    val u = makeUdf(myudf)
    val dataset = List("hello", "world").toDS()
    val result = dataset.withColumn("new", u(col("value")))
    val ref = dataset.withColumn("new", length(col("value")) + length(col("value")))
    checkEquiv(result, ref)
  }

  test("FALLBACK TO CPU: class method call inside UDF") {
    class C {
      def simple_func(str: String): Int = {
        str.length
      }
      val myudf: (String) => Int = str => {
        simple_func(str)
      }
      def apply(dataset: Dataset[String]): Dataset[Row] = {
        val u = makeUdf(myudf)
        dataset.withColumn("new", u(col("value")))
      }
    }
    val runner = new C
    val dataset = List("hello", "world").toDS()
    val result = runner(dataset)
    val ref = dataset.withColumn("new", length(col("value")))
    checkEquivNotCompiled(result, ref)
  }

  test("final class method call inside UDF") {
    final class C {
      def simple_func(str: String): Int = {
        str.length
      }
      val myudf: (String) => Int = str => {
        simple_func(str)
      }
      def apply(dataset: Dataset[String]): Dataset[Row] = {
        val u = makeUdf(myudf)
        dataset.withColumn("new", u(col("value")))
      }
    }
    val runner = new C
    val dataset = List("hello", "world").toDS()
    val result = runner(dataset)
    val ref = dataset.withColumn("new", length(col("value")))
    checkEquiv(result, ref)
  }

  test("FALL BACK TO CPU: object method call inside UDF") {
    object C {
      def simple_func(str: String): Int = {
        str.length
      }
      val myudf: (String) => Int = str => {
        simple_func(str)
      }
      def apply(dataset: Dataset[String]): Dataset[Row] = {
        val u = makeUdf(myudf)
        dataset.withColumn("new", u(col("value")))
      }
    }
    val dataset = List("hello", "world").toDS()
    val result = C(dataset)
    val ref = dataset.withColumn("new", length(col("value")))
    checkEquivNotCompiled(result, ref)
  }

  test("final object method call inside UDF") {
    final object C {
      def simple_func(str: String): Int = {
        str.length
      }
      val myudf: (String) => Int = str => {
        simple_func(str)
      }
      def apply(dataset: Dataset[String]): Dataset[Row] = {
        val u = makeUdf(myudf)
        dataset.withColumn("new", u(col("value")))
      }
    }
    val dataset = List("hello", "world").toDS()
    val result = C(dataset)
    val ref = dataset.withColumn("new", length(col("value")))
    checkEquiv(result, ref)
  }

  test("super class final method call inside UDF") {
    class B {
      final def simple_func(str: String): Int = {
        str.length
      }
    }
    class D extends B {
      val myudf: (String) => Int = str => {
        simple_func(str)
      }
      def apply(dataset: Dataset[String]): Dataset[Row] = {
        val u = makeUdf(myudf)
        dataset.withColumn("new", u(col("value")))
      }
    }
    val runner = new D
    val dataset = List("hello", "world").toDS()
    val result = runner(dataset)
    val ref = dataset.withColumn("new", length(col("value")))
    checkEquiv(result, ref)
  }

  test("FALLBACK TO CPU: final class calls super class method inside UDF") {
    class B {
      def simple_func(str: String): Int = {
        str.length
      }
    }
    final class D extends B {
      val myudf: (String) => Int = str => {
        simple_func(str)
      }
      def apply(dataset: Dataset[String]): Dataset[Row] = {
        val u = makeUdf(myudf)
        dataset.withColumn("new", u(col("value")))
      }
    }
    val runner = new D
    val dataset = List("hello", "world").toDS()
    val result = runner(dataset)
    val ref = dataset.withColumn("new", length(col("value")))
    checkEquivNotCompiled(result, ref)
  }

  test("FALLBACK TO CPU: super class method call inside UDF") {
    class B {
      def simple_func(str: String): Int = {
        str.length
      }
    }
    class D extends B {
      val myudf: (String) => Int = str => {
        simple_func(str)
      }
      def apply(dataset: Dataset[String]): Dataset[Row] = {
        val u = makeUdf(myudf)
        dataset.withColumn("new", u(col("value")))
      }
    }
    val runner = new D
    val dataset = List("hello", "world").toDS()
    val result = runner(dataset)
    val ref = dataset.withColumn("new", length(col("value")))
    checkEquivNotCompiled(result, ref)
  }

  test("FALLBACK TO CPU: capture a var in class") {
    class C {
      var capturedArg: Int = 4
      val myudf: (String) => Int = str => {
        str.length + capturedArg
      }
      def apply(dataset: Dataset[String]): Dataset[Row] = {
        val u = makeUdf(myudf)
        dataset.withColumn("new", u(col("value")))
      }
    }
    val runner = new C
    val dataset = List("hello", "world").toDS()
    val result = runner(dataset)
    val ref = dataset.withColumn("new", length(col("value")) + runner.capturedArg)
    checkEquivNotCompiled(result, ref)
  }

  test("FALLBACK TO CPU: capture a var outside class") {
    val capturedArg: Int = 4
    class C {
      val myudf: (String) => Int = str => {
        str.length + capturedArg
      }
      def apply(dataset: Dataset[String]): Dataset[Row] = {
        val u = makeUdf(myudf)
        dataset.withColumn("new", u(col("value")))
      }
    }
    val runner = new C
    val dataset = List("hello", "world").toDS()
    val result = runner(dataset)
    val ref = dataset.withColumn("new", length(col("value")) + capturedArg)
    checkEquivNotCompiled(result, ref)
  }

  test("capture a primitive var in method") {
    def run() = {
      val capturedArg: Int = 4
      val myudf: (String) => Int = str => {
        str.length + capturedArg
      }
      val u = makeUdf(myudf)
      val dataset = List("hello", "world").toDS()
      val result = dataset.withColumn("new", u(col("value")))
      val ref = dataset.withColumn("new", length(col("value")) + capturedArg)
      checkEquiv(result, ref)
    }
    run
  }

  test("throw a SparkException object") {
    def run(x: Int) = {
      val myudf: (Int) => Boolean = i => {
        if (i < 0 || i >= x) {
          throw new SparkException(s"Fold number must be in range [0, $x), but got $i.")
        }
        true
      }
      val u = makeUdf(myudf)
      val dataset = List(2, 20).toDS()
      val result = dataset.withColumn("new", u(col("value")))
      val ref = dataset.withColumn("new", lit(true))
      checkEquiv(result, ref)
    }
    run(30)
    try {
      run(20)
    } catch {
      case e: RuntimeException =>
        // in new versions of spark, the message has extra information, so we use contains.
        assert(e.getMessage.contains("Fold number must be in range [0, 20), but got 20."))
    }
  }

  test("Conditional array buffer processing") {
    def cond(s: String): Boolean = {
      s == null || s.trim.isEmpty
    }

    def transform(str: String): String = {
      if (cond(str)) {
        null
      } else {
        if (str.toLowerCase.startsWith("@@@@")) {
          "######" + str.substring("@@@@".length)
        } else if (str.toLowerCase.startsWith("######")) {
          "@@@@" + str.substring("######".length)
        } else {
          str
        }
      }
    }

    val u = makeUdf((x: String, y: String, z: Boolean) => {
      var r = new mutable.ArrayBuffer[String]()
      r = r :+ x
      if (!cond(y)) {
        r = r :+ y

        if (z) {
          r = r :+ transform(y)
        }
      }
      if (z) {
        r = r :+ transform(x)
      }
      r.distinct.toArray
    })

    val dataset = List(("######hello", null),
      ("world", "######hello"),
      ("", "@@@@target")).toDF("x", "y")
    val result = dataset.withColumn("new", u(col("x"), col("y"), lit(true)))
    val ref = List(("######hello", null, Array("######hello", "@@@@hello")),
      ("world", "######hello", Array("world", "######hello", "@@@@hello")),
      ("", "@@@@target", Array("", "@@@@target", "######target", null))).toDF
    checkEquiv(result, ref)
  }

  test("compile child expresion in explode") {
    val myudf: (String) => Array[String] = a => {
      a.split(",")
    }
    val u = makeUdf(myudf)
    val dataset = List("first,second").toDF("x").repartition(1)
    val result = dataset.withColumn("new", explode(u(col("x"))))
    val ref = List(("first,second","first"),("first,second","second")).toDF("x","new")
    checkEquiv(result,ref)
  }
}
