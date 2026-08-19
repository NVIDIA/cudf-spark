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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.GpuExec
import com.nvidia.spark.rapids.shims.ShimLeafExecNode
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.ExplainUtils
import org.apache.spark.sql.rapids.shims.SparkStringUtilsShims
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.util.Utils

/** GPU implementation of Spark's `DataSourceScanExec` */
trait GpuDataSourceScanExec extends ShimLeafExecNode with GpuExec {
  def relation: BaseRelation
  def tableIdentifier: Option[TableIdentifier]

  protected val nodeNamePrefix: String = ""

  override val nodeName: String = {
    s"Scan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  // Metadata that describes more details of this scan.
  protected def metadata: Map[String, String]

  protected lazy val maxMetadataValueLength =
    sparkSession.sessionState.conf.maxMetadataStringLength

  override def simpleString(maxFields: Int): String = {
    val metadataEntries = metadata.toSeq.sorted.map {
      case (key, value) =>
        key + ": " + SparkStringUtilsShims.abbreviate(redact(value), maxMetadataValueLength)
    }
    val metadataStr = truncatedString(metadataEntries, " ", ", ", "", maxFields)
    redact(
      s"$nodeNamePrefix$nodeName${truncatedString(output, "[", ",", "]", maxFields)}$metadataStr")
  }

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  /**
   * Shorthand for calling redactString() without specifying redacting rules
   */
  protected def redact(text: String): String = {
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, text)
  }

  /**
   * The data being read in.  This is to provide input to the tests in a way compatible with
   * `InputRDDCodegen` which all implementations used to extend.
   */
  def inputRDDs(): Seq[RDD[InternalRow]]
}

object GpuDataSourceScanExec {
  /**
   * Convert a sequence of `Path`s to a metadata string. When the length of metadata string
   * exceeds `stopAppendingThreshold`, stop appending paths for saving memory.
   */
  def buildLocationMetadata(paths: Seq[Path], stopAppendingThreshold: Int): String = {
    val metadata = new StringBuilder("[")
    var index: Int = 0
    while (index < paths.length && metadata.length < stopAppendingThreshold) {
      if (index > 0) {
        metadata.append(", ")
      }
      metadata.append(paths(index).toString)
      index += 1
    }
    metadata.append("]")
    metadata.toString
  }
}
