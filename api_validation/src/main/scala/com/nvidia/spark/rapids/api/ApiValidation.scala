/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.api

import scala.reflect.api
import scala.reflect.runtime.universe._

import com.nvidia.spark.rapids._

import org.apache.spark.internal.Logging

object ApiValidation extends Logging {
  // Method to take in a fully qualified case class name and return case class accessors in the
  // form of list of tuples(parameterName, Type)
  def getCaseClassAccessors[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.sorted
    .collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }

  // Method to convert string to TypeTag where string is fully qualified class Name
  def classToTypeTag[A](execName: Class[_ <: _]): TypeTag[A] = {
    val runTimeMirror = runtimeMirror(execName.getClassLoader) // obtain runtime mirror
    val classSym = runTimeMirror.staticClass(execName.getName)
    // obtain class symbol for `execNameObject`
    val tpe = classSym.selfType // obtain type object for `execNameObject`
    // create a type tag which contains above type object
    TypeTag(runTimeMirror, new api.TypeCreator {
      def apply[U <: api.Universe with Singleton](m: api.Mirror[U]): U#Type =
        if (m eq runTimeMirror) tpe.asInstanceOf[U#Type]
        else throw new IllegalArgumentException(s"Type tag defined in $runTimeMirror cannot be " +
          s"migrated to other mirrors.")
    })
  }

  val enabledExecs = List (
    "[org.apache.spark.sql.execution.joins.SortMergeJoinExec]",
    "[org.apache.spark.sql.execution.aggregate.HashAggregateExec]",
    "[org.apache.spark.sql.execution.CollectLimitExec]"
  )

  def printHeaders(a: String, appender: StringBuilder): Unit = {
    appender.append("\n*************************************************************************" +
      "***********************\n")
    appender.append(a+"\n")
    appender.append("Spark parameters                                                   " +
      " Plugin parameters" + "\n")
    appender.append("----------------------------------------------------------------------------" +
      "--------------------\n")
  }

  def main(args: Array[String]): Unit ={
    val appender = new StringBuilder
    val gpuExecs = GpuOverrides.execs // get all the execs
    // get the keys Eg: class org.apache.spark.sql.execution.aggregate.HashAggregateExec
    val gpuKeys = gpuExecs.keys
    var printNewline = false

    val sparkToShimMap = Map("3.2.0" -> "spark320")
    val sparkVersion = ShimLoader.getShimVersion.toString
    val shimVersion = sparkToShimMap(sparkVersion)

    gpuKeys.foreach { e =>
      // Get SparkExecs argNames and types
      val sparkTypes = classToTypeTag(e)

      // Proceed only if the Exec is not enabled
      if (!enabledExecs.contains(sparkTypes.toString().replace("TypeTag", ""))) {
        val sparkParameters = getCaseClassAccessors(sparkTypes).map(m => m.name -> m.info)

        // Get GpuExecs argNames and Types.
        // Note that for some there is no 1-1 mapping between names
        // Some Execs are in different packages.
        val execType = sparkTypes.tpe.toString.split('.').last
        val gpu = execType match {
          case "BroadcastExchangeExec" => s"org.apache.spark.sql.rapids.execution.Gpu" + execType
          case "BroadcastHashJoinExec" => s"com.nvidia.spark.rapids.shims." + shimVersion +
            ".Gpu" + execType
          case "FileSourceScanExec" => s"org.apache.spark.sql.rapids.shims." + shimVersion +
            ".Gpu" + execType
          case "CartesianProductExec" => s"org.apache.spark.sql.rapids.Gpu" + execType
          case "BroadcastNestedLoopJoinExec" =>
            s"com.nvidia.spark.rapids.shims." + shimVersion + ".Gpu" + execType
          case "SortMergeJoinExec" | "ShuffledHashJoinExec" =>
            s"com.nvidia.spark.rapids.shims." + shimVersion + ".GpuShuffledHashJoinExec"
          case "SortAggregateExec" => s"com.nvidia.spark.rapids.GpuHashAggregateExec"
          case _ => s"com.nvidia.spark.rapids.Gpu" + execType
        }

        // TODO: Add error handling if Type is not present
        val gpuTypes = classToTypeTag(ShimReflectionUtils.loadClass(gpu))

        val sparkToGpuExecMap = Map(
          "org.apache.spark.sql.catalyst.expressions.Expression" ->
            "com.nvidia.spark.rapids.GpuExpression",
          "org.apache.spark.sql.catalyst.expressions.NamedExpression" ->
            "com.nvidia.spark.rapids.GpuExpression",
          "org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression" ->
            "org.apache.spark.sql.rapids.GpuAggregateExpression",
          "org.apache.spark.sql.execution.command.DataWritingCommand" ->
            "com.nvidia.spark.rapids.GpuDataWritingCommand",
          "org.apache.spark.sql.execution.joins.BuildSide" ->
            "org.apache.spark.sql.execution.joins.BuildSide")
          .withDefaultValue("sparkKeyNotPresent")

        val gpuParameters = getCaseClassAccessors(gpuTypes).map(m => m.name -> m.info)

        if (sparkParameters.length != gpuParameters.length) {
          if (!printNewline) {
            appender.append("\n\n")
            printNewline = true
          }
          printHeaders(s"Parameter lengths don't match between Execs\nSparkExec - " +
            sparkTypes.toString().replace("TypeTag", "") + "\nGpuExec - " +
            gpuTypes.toString().replace("TypeTag", "") + "\nSpark code has " +
            sparkParameters.length + " parameters where as plugin code has " +
            gpuParameters.length + " parameters", appender)
          val paramLength = if (sparkParameters.length > gpuParameters.length) {
            sparkParameters.length
          } else {
            gpuParameters.length
          }
          // Print sparkExec parameter type and gpuExec parameter type in same line
          for (i <- 0 until paramLength) {
            if (i < sparkParameters.length) {
              appender.append(s"%-65s".format(sparkParameters(i)._2.toString.substring(3)) + " | ")
            } else {
              appender.append(s"%-65s".format(" ") + " | ")
            }
            if (i < gpuParameters.length) {
              appender.append(s"%s".format(gpuParameters(i)._2.toString.substring(3)))
            }
            appender.append("\n")
          }
        } else {
          // Here we need to extract the types from each GPUExec and Spark Exec and
          // compare. For Eg: GpuExpression with Expression.
          var isFirst = true
          for (i <- sparkParameters.indices) {
            val sparkSimpleName = sparkParameters(i)._2.toString.substring(3)
            val gpuSimpleName = gpuParameters(i)._2.toString.substring(3)

            val sparkKey = sparkSimpleName.split('[').last.replace("]", "")
            val gpuKey = sparkSimpleName.replace(sparkKey, sparkToGpuExecMap(sparkKey))

            if (sparkParameters(i)._2 != gpuParameters(i)._2 && gpuKey != gpuSimpleName) {
              if (isFirst) {
                printHeaders(s"Types differ for below parameters in this Exec\nSparkExec  - " +
                  sparkTypes.toString().replace("TypeTag", "") + "\nGpuExec - " +
                  gpuTypes.toString().replace("TypeTag", ""), appender)
                isFirst = false
              }
              appender.append(s"%-65s".format(sparkSimpleName) + " | ")
              appender.append(s"%s".format(gpuSimpleName) + "\n") + "\n"
            }
          }
        }
      }
    }
    log.info(appender.toString())
  }
}
