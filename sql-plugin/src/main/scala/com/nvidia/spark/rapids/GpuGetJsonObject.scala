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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.JSONUtils
import com.nvidia.spark.rapids.shims.GetJsonObjectShim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, GetJsonObject}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.test.CpuGetJsonObject
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

object JsonPathParser {
  // Mirrors JSONUtils.MAX_PATH_DEPTH from spark-rapids-jni (get_json_object.hpp).
  // Duplicated here to avoid triggering JNI native library loading during
  // Driver-side plan conversion (see github.com/NVIDIA/spark-rapids/issues/14184).
  val MAX_PATH_DEPTH: Int = 16

  import PathInstruction._

  def parse(str: String): Option[List[PathInstruction]] = {
    GetJsonObjectShim.parse(str)
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

object GpuGetJsonObjectMeta {
  private[rapids] def parseLiteralPath(value: Any): Option[List[PathInstruction]] = {
    Option(value).map(_.asInstanceOf[UTF8String].toString).flatMap { path =>
      JsonPathParser.parse(path)
    }
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
      val instructions = GpuGetJsonObjectMeta.parseLiteralPath(l.value)
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
        with GpuBatchableExpression {
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

  // ---- GpuBatchableExpression: sibling get_json_object calls over the SAME
  // json column batch into ONE native multi-path call. The verify-mode side
  // channel (CpuGetJsonObject.verify in doColumnar) is not batchable and keeps
  // its per-column path via batchEligible = false.
  override def batchEligible: Boolean = savePathForVerify.isEmpty

  // Input child identity: siblings sharing the same `json` child share one
  // multi-path native call (the native multi-path API takes ONE input column).
  override def batchGroupKey: Any = json

  override def batchInput: Expression = json

  // The batch path can see the rhs still alias-wrapped; the per-column path
  // instead receives a GpuScalar rhs (doColumnar).
  @scala.annotation.tailrec
  private def litPathOf(exp: Expression): Option[GpuLiteral] = exp match {
    case l: GpuLiteral => Some(l)
    case a: GpuAlias => litPathOf(a.child)
    case _ => None
  }

  private def checkedInstructionsOf(member: GpuGetJsonObject)
      : Option[List[PathInstruction]] = {
    val pathInstructions = litPathOf(member.path).flatMap { l =>
      GpuGetJsonObjectMeta.parseLiteralPath(l.value)
    }
    if (hasSeparateWildcard(pathInstructions)) {
      // A separate wildcard path always returns nulls
      None
    } else {
      // Filter out the unneeded instructions before handing them to JNI
      pathInstructions.map(JsonPathParser.filterInstructionsForJni)
    }
  }

  override def batchEval(siblings: Seq[Expression],
      inputs: Seq[GpuColumnVector]): Seq[ColumnVector] = {
    val members = siblings.map(_.asInstanceOf[GpuGetJsonObject])
    val jniPaths = members.map(checkedInstructionsOf)
    val input = inputs.head.getBase
    if (members.length == 1) {
      // Byte-identical cost to the per-column path: one getJsonObject call
      jniPaths.head match {
        case Some(instructions) =>
          Seq(JSONUtils.getJsonObject(input, JsonPathParser.convertToJniObject(instructions)))
        case None =>
          Seq(GpuColumnVector.columnVectorFromNull(input.getRowCount.toInt, StringType))
      }
    } else {
      val nullIndexes = jniPaths.zipWithIndex.collect { case (None, idx) => idx }
      val validPathsWithIndexes = jniPaths.zipWithIndex.collect {
        case (Some(arr), idx) =>
          (java.util.Arrays.asList(JsonPathParser.convertToJniObject(arr): _*), idx)
      }
      val validPaths = validPathsWithIndexes.map(_._1)
      val results = new Array[ColumnVector](members.length)
      try {
        if (validPaths.nonEmpty) {
          val targetBatchSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(SQLConf.get)
          // Last argument -1 indicates to use automatically calculated parallelism
          withResource(JSONUtils.getJsonObjectMultiplePaths(input,
              java.util.Arrays.asList(validPaths: _*), 4 * targetBatchSize, -1)) {
            chunkedResult =>
              validPathsWithIndexes.map(_._2).zipWithIndex.foreach {
                case (toIndex, fromIndex) =>
                  results(toIndex) = chunkedResult(fromIndex).incRefCount()
              }
          }
        }
        if (nullIndexes.nonEmpty) {
          val nullCol = withResource(GpuScalar.from(null, StringType)) { s =>
            ColumnVector.fromScalar(s, input.getRowCount.toInt)
          }
          withResource(nullCol) { _ =>
            nullIndexes.foreach { idx => results(idx) = nullCol.incRefCount() }
          }
        }
        results.toSeq
      } catch {
        case t: Throwable =>
          results.foreach { c => if (c != null) c.close() }
          throw t
      }
    }
  }
}
