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

package org.apache.spark.sql.hive.rapids

import java.nio.charset.{Charset, StandardCharsets}
import java.time.ZoneId

import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuUserDefinedFunction.udfTypeSig

import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.{HiveGenericUDF, HiveSimpleUDF}
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.hive.rapids.GpuHiveTextFileUtils._
import org.apache.spark.sql.hive.rapids.shims.HiveProviderCmdShims
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.types._

class HiveProviderImpl extends HiveProviderCmdShims {

  /**
   * Builds the expression rules that are specific to spark-hive Catalyst nodes.
   */
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(
      GpuOverrides.expr[HiveSimpleUDF](
        "Hive UDF, the UDF can choose to implement a RAPIDS accelerated interface to" +
            " get better performance",
        ExprChecks.projectOnly(
          udfTypeSig,
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param", udfTypeSig, TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[HiveSimpleUDF](a, conf, p, r) {

          val function = createFunction(a)
          private val opRapidsFunc = function match {
            case rapidsUDF: RapidsUDF => Some(rapidsUDF)
            case _ => None
          }

          override def tagExprForGpu(): Unit = {
            if (opRapidsFunc.isEmpty && !this.conf.isCpuBasedUDFEnabled) {
              willNotWorkOnGpu(s"Hive SimpleUDF ${a.name} implemented by " +
                  s"${a.funcWrapper.functionClassName} does not provide a GPU implementation " +
                  s"and CPU-based UDFs are not enabled by `${RapidsConf.ENABLE_CPU_BASED_UDF.key}`")
            }
          }

          override def convertToGpuImpl(): GpuExpression = {
            opRapidsFunc.map { _ =>
              // We use the original HiveGenericUDF `deterministic` method as a proxy
              // for simplicity.
              GpuHiveSimpleUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()),
                a.dataType,
                a.deterministic)
            }.getOrElse {
              // This `require` is just for double check.
              require(this.conf.isCpuBasedUDFEnabled)
              GpuRowBasedHiveSimpleUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()))
            }
          }
        }),
      GpuOverrides.expr[HiveGenericUDF](
        "Hive Generic UDF, the UDF can choose to implement a RAPIDS accelerated interface to" +
            " get better performance",
        ExprChecks.projectOnly(
          udfTypeSig,
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param", udfTypeSig, TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[HiveGenericUDF](a, conf, p, r) {
          val function = createFunction(a)
          private val opRapidsFunc = function match {
            case rapidsUDF: RapidsUDF => Some(rapidsUDF)
            case _ => None
          }

          override def tagExprForGpu(): Unit = {
            if (opRapidsFunc.isEmpty && !this.conf.isCpuBasedUDFEnabled) {
              willNotWorkOnGpu(s"Hive GenericUDF ${a.name} implemented by " +
                  s"${a.funcWrapper.functionClassName} does not provide a GPU implementation " +
                  s"and CPU-based UDFs are not enabled by `${RapidsConf.ENABLE_CPU_BASED_UDF.key}`")
            }
          }

          override def convertToGpuImpl(): GpuExpression = {
            opRapidsFunc.map { _ =>
              // We use the original HiveGenericUDF `deterministic` method as a proxy
              // for simplicity.
              GpuHiveGenericUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()),
                a.dataType,
                a.deterministic,
                a.foldable)
            }.getOrElse {
              // This `require` is just for double check.
              require(this.conf.isCpuBasedUDFEnabled)
              GpuRowBasedHiveGenericUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()))
            }
          }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    Seq(
      GpuOverrides.exec[HiveTableScanExec](
        desc = "Scan Exec to read Hive delimited text tables",
        ExecChecks(
          TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
          TypeSig.all),
        (p, conf, parent, r) => new SparkPlanMeta[HiveTableScanExec](p, conf, parent, r) {

          private def flagIfUnsupportedStorageFormat(storage: CatalogStorageFormat): Unit = {
            if (storage.inputFormat.getOrElse("") != textInputFormat) {
              willNotWorkOnGpu(s"unsupported input-format found: ${storage.inputFormat}, " +
                s"only $textInputFormat is currently supported")
            }

            if (storage.serde.getOrElse("") != lazySimpleSerDe) {
              willNotWorkOnGpu(s"unsupported serde found: ${storage.serde}, " +
                s"only $lazySimpleSerDe is currently supported")
            }

            val serializationFormat = storage.properties.getOrElse(serializationKey, "")
            if (serializationFormat != ctrlASeparatedFormat) {
              willNotWorkOnGpu(s"unsupported serialization format found: " +
                s"$serializationFormat, " +
                s"only \'^A\' separated text input (i.e. serialization.format=1) " +
                s"is currently supported")
            }

            val lineTerminator = storage.properties.getOrElse(lineDelimiterKey, newLine)
            if (lineTerminator != newLine) {
              willNotWorkOnGpu(s"unsupported line terminator found: " +
                s"$lineTerminator, " +
                s"only newline (\\n) separated text input  is currently supported")
            }

            if (!storage.properties.getOrElse(escapeDelimiterKey, "").equals("")) {
              willNotWorkOnGpu("escapes are not currently supported")
              // "serialization.escape.crlf" matters only if escapeDelimiterKey is set
            }

            if (!storage.properties.getOrElse("skip.header.line.count", "0").equals("0")) {
              willNotWorkOnGpu("header line skipping is not supported")
            }

            if (!storage.properties.getOrElse("skip.footer.line.count", "0").equals("0")) {
              willNotWorkOnGpu("footer line skipping is not supported")
            }

            if (storage.properties.getOrElse("serialization.last.column.takes.rest", "")
                .equalsIgnoreCase("true")) {
              // We could probably support this if we used a limit on the split, but why bother
              // until a customer wants it.
              willNotWorkOnGpu("\"serialization.last.column.takes.rest\" is not supported")
            }

            val charset = Charset.forName(
              storage.properties.getOrElse("serialization.encoding", "UTF-8"))
            if (!(charset.equals(StandardCharsets.US_ASCII) ||
                charset.equals(StandardCharsets.UTF_8))) {
              willNotWorkOnGpu("only UTF-8 and ASCII are supported as the charset")
            }
          }

          private def checkIfEnabled(): Unit = {
            if (!this.conf.isHiveDelimitedTextEnabled) {
              willNotWorkOnGpu("Hive text I/O has been disabled. To enable this, " +
                               s"set ${RapidsConf.ENABLE_HIVE_TEXT} to true")
            }
            if (!this.conf.isHiveDelimitedTextReadEnabled) {
              willNotWorkOnGpu("reading Hive delimited text tables has been disabled, " +
                               s"to enable this, set ${RapidsConf.ENABLE_HIVE_TEXT_READ} to true")
            }
          }

          private def flagIfUnsupportedType(tableRelation: HiveTableRelation,
                                            column: AttributeReference): Unit = {
            if (hasUnsupportedType(column)) {
              willNotWorkOnGpu(s"column ${column.name} of table " +
                s"${tableRelation.tableMeta.qualifiedName} has type ${column.dataType}, " +
                s"unsupported for Hive text tables. ")
            }
          }

          private def flagIfUnsupported(tableRelation: HiveTableRelation): Unit = {
            // Check Storage format settings.
            flagIfUnsupportedStorageFormat(tableRelation.tableMeta.storage)

            lazy val hasTimestamps = wrapped.output.exists { att =>
              TrampolineUtil.dataTypeExistsRecursively(att.dataType,
                dt => dt.isInstanceOf[TimestampType])
            }

            // Check if datatypes are all supported.
            tableRelation.dataCols.foreach(flagIfUnsupportedType(tableRelation, _))

            // Check TBLPROPERTIES as well.
            // `timestamp.formats` might be set in TBLPROPERTIES or SERDEPROPERTIES,
            // or both. (If both, TBLPROPERTIES is honoured.)
            if ((!tableRelation.tableMeta.properties.getOrElse("timestamp.formats", "")
                  .equals("")
                || !tableRelation.tableMeta.storage.properties.getOrElse("timestamp.formats", "")
                  .equals("")) && hasTimestamps) {
              willNotWorkOnGpu("custom timestamp formats are not currently supported")
            }
          }

          override def tagPlanForGpu(): Unit = {
            checkIfEnabled()
            val tableRelation = wrapped.relation
            // Check that the table and all participating partitions
            // are '^A' separated.
            flagIfUnsupported(tableRelation)

            if (tableRelation.isPartitioned) {
              val parts = tableRelation.prunedPartitions.getOrElse(Seq.empty)
              parts.map(_.storage).foreach(flagIfUnsupportedStorageFormat)
              val origProps = tableRelation.tableMeta.storage.properties
              if (!parts.map(_.storage.properties).forall(_ == origProps)) {
                willNotWorkOnGpu("individual partitions have different properties")
              }
            }

            val sparkSession = SparkSessionUtils.sessionFromPlan(wrapped)
            val hadoopConf = sparkSession.sessionState.newHadoopConf()
            lazy val hasBooleans = wrapped.output.exists { att =>
              TrampolineUtil.dataTypeExistsRecursively(att.dataType, dt => dt == BooleanType)
            }
            val extendedBool =
              hadoopConf.getBoolean("hive.lazysimple.extended_boolean_literal", false)
            if (extendedBool && hasBooleans) {
              willNotWorkOnGpu("extended boolean parsing is not supported")
            }

            lazy val hasFloats = wrapped.output.exists { att =>
              TrampolineUtil.dataTypeExistsRecursively(att.dataType, dt => dt == FloatType)
            }

            if (!this.conf.shouldHiveReadFloats && hasFloats) {
              willNotWorkOnGpu("reading of floats has been disabled set " +
                  s"${RapidsConf.ENABLE_READ_HIVE_FLOATS} to true to enable this.")
            }

            lazy val hasDoubles = wrapped.output.exists { att =>
              TrampolineUtil.dataTypeExistsRecursively(att.dataType, dt => dt == DoubleType)
            }

            if (!this.conf.shouldHiveReadDoubles && hasDoubles) {
              willNotWorkOnGpu("reading of doubles has been disabled set " +
                  s"${RapidsConf.ENABLE_READ_HIVE_DOUBLES} to true to enable this.")
            }

            lazy val hasDecimals = wrapped.output.exists { att =>
              TrampolineUtil.dataTypeExistsRecursively(att.dataType,
                dt => dt.isInstanceOf[DecimalType])
            }

            if (!this.conf.shouldHiveReadDecimals && hasDecimals) {
              willNotWorkOnGpu("reading of decimal typed values has been disabled set " +
                  s"${RapidsConf.ENABLE_READ_HIVE_DECIMALS} to true to enable this.")
            }

            val types = wrapped.schema.map(_.dataType).toSet
            if (types.exists(GpuOverrides.isOrContainsTimestamp(_))) {
              if (!GpuOverrides.isUTCTimezone()) {
                willNotWorkOnGpu("Only UTC timezone is supported. " +
                  s"Current timezone settings: (JVM : ${ZoneId.systemDefault()}, " +
                  s"session: ${SQLConf.get.sessionLocalTimeZone}). ")
              }
            }
          }

          override def convertToGpu(): GpuExec = {
            GpuHiveTableScanExec(wrapped.requestedAttributes,
              wrapped.relation,
              wrapped.partitionPruningPred)
          }
        })
    ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap
}
