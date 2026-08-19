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

import scala.collection.mutable
import scala.util.parsing.combinator.RegexParsers

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.JSONUtils
import com.nvidia.spark.rapids.shims.{GetJsonObjectShim, ShimExpression}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, ExpectsInputTypes, Expression, GetJsonObject}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuGetStructField
import org.apache.spark.sql.rapids.catalyst.expressions.{GpuCombinable, GpuExpressionCombiner, GpuExpressionEquals}
import org.apache.spark.sql.rapids.test.CpuGetJsonObject
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

// Copied from Apache Spark org/apache/spark/sql/catalyst/expressions/jsonExpressions.scala
sealed trait PathInstruction
object PathInstruction {
  case object Subscript extends PathInstruction
  case object Wildcard extends PathInstruction
  case object Key extends PathInstruction
  case class Index(index: Long) extends PathInstruction
  case class Named(name: String) extends PathInstruction
}

object JsonPathParser extends RegexParsers {
  // Mirrors JSONUtils.MAX_PATH_DEPTH from spark-rapids-jni (get_json_object.hpp).
  // Duplicated here to avoid triggering JNI native library loading during
  // Driver-side plan conversion (see github.com/NVIDIA/spark-rapids/issues/14184).
  val MAX_PATH_DEPTH: Int = 16

  import PathInstruction._

  def root: Parser[Char] = '$'

  def long: Parser[Long] = "\\d+".r ^? {
    case x => x.toLong
  }

  // parse `[*]` and `[123]` subscripts
  def subscript: Parser[List[PathInstruction]] =
    for {
      operand <- '[' ~> ('*' ^^^ Wildcard | long ^^ Index) <~ ']'
    } yield {
      Subscript :: operand :: Nil
    }

  // parse `.name` or `['name']` child expressions
  def named: Parser[List[PathInstruction]] =
    for {
      name <- '.' ~> "[^\\.\\[]+".r | "['" ~> GetJsonObjectShim.partRegexpInNamed.r <~ "']"
    } yield {
      Key :: Named(name) :: Nil
    }

  // child wildcards: `..`, `.*` or `['*']`
  def wildcard: Parser[List[PathInstruction]] =
    (".*" | "['*']") ^^^ List(Wildcard)

  def node: Parser[List[PathInstruction]] =
    wildcard |
      named |
      subscript

  val expression: Parser[List[PathInstruction]] = {
    phrase(root ~> rep(node) ^^ (x => x.flatten))
  }

  def parse(str: String): Option[List[PathInstruction]] = {
    this.parseAll(expression, str) match {
      case Success(result, _) =>
        Some(result)

      case _ =>
        None
    }
  }

  def filterInstructionsForJni(instructions: List[PathInstruction]): List[PathInstruction] =
    instructions.filter {
      // The JNI implementation does not need/use these types.
      case Subscript => false
      case Key => false
      case _ => true
    }

  def fallbackCheck(instructions: List[PathInstruction]): Boolean =
    instructions.length > JsonPathParser.MAX_PATH_DEPTH

  def unzipInstruction(instruction: PathInstruction): (String, String, Long) = {
    instruction match {
      case Subscript => ("subscript", "", -1)
      case Key => ("key", "", -1)
      case Wildcard => ("wildcard", "", -1)
      case Index(index) => ("index", "", index)
      case Named(name) => ("named", name, -1)
    }
  }

  def convertToJniObject(instructions: List[PathInstruction]): 
      Array[JSONUtils.PathInstructionJni] = {
    instructions.map { instruction =>
      val (tpe, name, index) = unzipInstruction(instruction)
      new JSONUtils.PathInstructionJni(tpe match {
        case "wildcard" => JSONUtils.PathInstructionType.WILDCARD
        case "index" => JSONUtils.PathInstructionType.INDEX
        case "named" => JSONUtils.PathInstructionType.NAMED
        case other =>
          throw new IllegalArgumentException(s"Internal Error should not see a $other instruction")
      }, name, index)
    }.toArray
  }

  def containsUnsupportedPath(instructions: List[PathInstruction]): Boolean = {
    // Gpu GetJsonObject is not supported if JSON path contains wildcard [*]
    // see https://github.com/NVIDIA/spark-rapids/issues/10216
    instructions.exists {
      case Wildcard => true
      case Named("*")  => true
      case _ => false
    }
  }

  def normalize(instructions: List[PathInstruction]): String = {
    // convert List[PathInstruction] to String
    "$" + instructions.map {
      case Subscript | Key => ""
      case Wildcard => "[*]"
      case Index(index) => s"[$index]"
      case Named(name) => s"['$name']"
      case _ => throw new IllegalArgumentException(s"Invalid instruction in path")
    }.mkString
  }
}

class GpuGetJsonObjectMeta(
    expr: GetJsonObject,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule
  ) extends BinaryExprMeta[GetJsonObject](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    val lit = GpuOverrides.extractLit(expr.right)
    lit.foreach { l =>
      val instructions = JsonPathParser.parse(l.value.asInstanceOf[UTF8String].toString)
      val updated = instructions.map(JsonPathParser.filterInstructionsForJni)
      if (updated.exists(JsonPathParser.fallbackCheck)) {
        willNotWorkOnGpu(s"get_json_object on GPU does not support more " +
          s"than ${JsonPathParser.MAX_PATH_DEPTH} nested paths." +
          instructions.map(i => s" (Found ${i.length})").getOrElse(""))
      }
    }
  }

  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    GpuGetJsonObject(lhs, rhs)(
      conf.testGetJsonObjectSavePath, conf.testGetJsonObjectSaveRows)
  }
}

case class GpuMultiGetJsonObject(json: Expression,
                                 paths: Seq[Option[List[PathInstruction]]],
                                 output: StructType)(targetBatchSize: Long)
  extends GpuExpression with ShimExpression {

  override def otherCopyArgs: Seq[AnyRef] =
    targetBatchSize.asInstanceOf[java.lang.Long] ::
      Nil

  override def dataType: DataType = output

  override def nullable: Boolean = false

  override def prettyName: String = "multi_get_json_object"

  lazy private val jniInstructions = paths.map { p =>
    p.map(JsonPathParser.convertToJniObject)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val nullIndexes = jniInstructions.zipWithIndex.filter {
      case (None, _) => true
      case _ => false
    }.map(_._2)

    val validPathsWithIndexes = jniInstructions.zipWithIndex.flatMap {
      case (Some(arr), idx) => Some((java.util.Arrays.asList(arr: _*), idx))
      case _ => None
    }

    var validPathsIndex = 0
    val validPaths = validPathsWithIndexes.map(_._1)
    withResource(new Array[ColumnVector](validPaths.length)) { validPathColumns =>
      withResource(json.columnarEval(batch)) { input =>
        // Last argument -1 indicates to use automatically calculated parallelism
        withResource(JSONUtils.getJsonObjectMultiplePaths(input.getBase,
          java.util.Arrays.asList(validPaths: _*), 4 * targetBatchSize,
          -1)) { chunkedResult =>
          chunkedResult.foreach { cr =>
            validPathColumns(validPathsIndex) = cr.incRefCount()
            validPathsIndex += 1
          }
        }

        withResource(new Array[ColumnVector](paths.length)) { columns =>
          if (nullIndexes.nonEmpty) {
            val nullCol = withResource(GpuScalar.from(null, StringType)) { s =>
              ColumnVector.fromScalar(s, batch.numRows())
            }
            withResource(nullCol) { _ =>
              nullIndexes.foreach { idx =>
                columns(idx) = nullCol.incRefCount()
              }
            }
          }

          validPathsWithIndexes.map(_._2).zipWithIndex.foreach {
            case (toIndex, fromIndex) =>
              columns(toIndex) = validPathColumns(fromIndex).incRefCount()
          }
          GpuColumnVector.from(ColumnVector.makeStruct(batch.numRows(), columns: _*), dataType)
        }
      }
    }
  }

  override def children: Seq[Expression] = Seq(json)
}

class GetJsonObjectCombiner(private val exp: GpuGetJsonObject) extends GpuExpressionCombiner {
  private var outputLocation = 0
  /**
   * A mapping between an expression and where in the output struct of
   * the MultiGetJsonObject will the output be.
   */
  private val toCombine = mutable.HashMap.empty[GpuExpressionEquals, Int]
  addExpression(exp)

  override def toString: String = s"GetJsonObjCombiner $toCombine"

  override def hashCode: Int = {
    // We already know that we are GetJsonObject, and what we can combine is based
    // on the json column being the same.
    "GetJsonObject".hashCode + (exp.json.semanticHash() * 17)
  }

  override def equals(o: Any): Boolean = o match {
    case other: GetJsonObjectCombiner =>
      exp.json.semanticEquals(other.exp.json) &&
        // We don't support multi-get with the save path for verify yet
        exp.savePathForVerify.isEmpty &&
        other.exp.savePathForVerify.isEmpty
    case _ => false
  }

  override def addExpression(e: Expression): Unit = {
    val localOutputLocation = outputLocation
    outputLocation += 1
    val key = GpuExpressionEquals(e)
    if (!toCombine.contains(key)) {
      toCombine.put(key, localOutputLocation)
    }
  }

  override def useCount: Int = toCombine.size

  private def fieldName(id: Int): String =
    s"_mgjo_$id"

  @scala.annotation.tailrec
  private def extractLit(exp: Expression): Option[GpuLiteral] = exp match {
    case l: GpuLiteral => Some(l)
    case a: Alias => extractLit(a.child)
    case _ => None
  }

  private lazy val multiGet: GpuMultiGetJsonObject = {
    val json = toCombine.head._1.e.asInstanceOf[GpuGetJsonObject].json
    val fieldsNPaths = toCombine.toSeq.map {
      case (k, id) =>
        (id, k.e)
    }.sortBy(_._1).map {
      case (id, e: GpuGetJsonObject) =>
        val parsedPath = extractLit(e.path).flatMap { s =>
          import GpuGetJsonObject._
          val str = s.value match {
            case u: UTF8String => u.toString
            case _ => null.asInstanceOf[String]
          }
          val pathInstructions = parseJsonPath(str)
          if (hasSeparateWildcard(pathInstructions)) {
            // If has separate wildcard path, should return all nulls
            None
          } else {
            // Filter out the unneeded instructions before we cache it
            pathInstructions.map(JsonPathParser.filterInstructionsForJni)
          }
        }
        (StructField(fieldName(id), e.dataType, e.nullable), parsedPath)
    }
    val dt = StructType(fieldsNPaths.map(_._1))
    val conf = SQLConf.get
    val targetBatchSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    GpuMultiGetJsonObject(json, fieldsNPaths.map(_._2), dt)(targetBatchSize)
  }

  override def getReplacementExpression(e: Expression): Option[Expression] = {
    toCombine.get(GpuExpressionEquals(e)).map { localId =>
      GpuGetStructField(multiGet, localId, Some(fieldName(localId)))
    }
  }
}

object GpuGetJsonObject {
  def parseJsonPath(path: GpuScalar): Option[List[PathInstruction]] = {
    if (path.isValid) {
      val pathStr = path.getValue.toString
      JsonPathParser.parse(pathStr)
    } else {
      None
    }
  }

  def parseJsonPath(pathStr: String): Option[List[PathInstruction]] = {
    if (pathStr != null) {
      JsonPathParser.parse(pathStr)
    } else {
      None
    }
  }

  /**
   * get_json_object(any_json, '$.*') always return null.
   * '$.*' will be parsed to be a single `Wildcard`.
   * Check whether has separated `Wildcard`
   *
   * @param instructions query path instructions
   * @return true if has separated `Wildcard`, false otherwise.
   */
  def hasSeparateWildcard(instructions: Option[List[PathInstruction]]): Boolean = {
    import PathInstruction._
    def hasSeparate(ins: List[PathInstruction], idx: Int): Boolean = {
      if (idx == 0) {
        ins(0) match {
          case Wildcard => true
          case _ => false
        }
      } else {
        (ins(idx - 1), ins(idx)) match {
          case (Key, Wildcard) => false
          case (Subscript, Wildcard) => false
          case (_, Wildcard) => true
          case _ => false
        }
      }
    }

    if (instructions.isEmpty) {
      false
    } else {
      val list = instructions.get
      list.indices.exists { idx => hasSeparate(list, idx) }
    }
  }
}

case class GpuGetJsonObject(
     json: Expression,
     path: Expression)(
  val savePathForVerify: Option[String],
  val saveRowsForVerify: Int)
    extends GpuBinaryExpressionArgsAnyScalar
        with ExpectsInputTypes
        with GpuCombinable {
  import GpuGetJsonObject._

  // Get a Hadoop conf for the JSON Object
  val hconf: Option[SerializableConfiguration] = savePathForVerify.map { _ =>
    val spark = SparkSession.active
    new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
  }
  val seed = System.nanoTime()

  override def otherCopyArgs: Seq[AnyRef] = Seq(savePathForVerify,
    saveRowsForVerify.asInstanceOf[java.lang.Integer])

  override def left: Expression = json
  override def right: Expression = path
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def nullable: Boolean = true
  override def prettyName: String = "get_json_object"

  private var cachedInstructions:
      Option[Option[List[PathInstruction]]] = None

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val fromGpu = cachedInstructions.getOrElse {
      val pathInstructions = parseJsonPath(rhs)
      val checkedPathInstructions = if (hasSeparateWildcard(pathInstructions)) {
        // If has separate wildcard path, should return all nulls
        None
      } else {
        // Filter out the unneeded instructions before we cache it
        pathInstructions.map(JsonPathParser.filterInstructionsForJni)
      }

      cachedInstructions = Some(checkedPathInstructions)
      checkedPathInstructions
    } match {
      case Some(instructions) => {
        val jniInstructions = JsonPathParser.convertToJniObject(instructions)
        JSONUtils.getJsonObject(lhs.getBase, jniInstructions)
      }
      case None => GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, StringType)
    }

    // Below is only for testing purpose
    savePathForVerify.foreach { debugPath =>
      closeOnExcept(fromGpu) { _ =>
        val path = rhs.getValue.asInstanceOf[UTF8String]
        withResource(CpuGetJsonObject.getJsonObjectOnCpu(lhs, path)) { fromCpu =>
          // verify result, save diffs if have
          CpuGetJsonObject.verify(isLegacy = false, seed,
            lhs.getBase, path, fromGpu, fromCpu, debugPath, saveRowsForVerify,
            hconf.get.value)
        }
      }
    }

    fromGpu
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  @transient
  private lazy val combiner = new GetJsonObjectCombiner(this)

  override def getCombiner(): GpuExpressionCombiner = combiner
}
