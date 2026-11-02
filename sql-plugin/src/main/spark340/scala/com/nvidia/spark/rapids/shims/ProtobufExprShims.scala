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

/*** spark-rapids-shim-json-lines
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "410db183"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import scala.reflect.ClassTag
import scala.util.Try

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.rapids.GpuFromProtobuf
import org.apache.spark.sql.rapids.protobuf.{FlattenedFieldDescriptor, FlattenedSchemaArrays,
  ProtobufFieldInfo, ProtobufMessageDescriptor, ProtobufSchemaExtractor,
  ProtobufSchemaValidator}
import org.apache.spark.sql.types._

/**
 * Spark 3.4+ optional integration for spark-protobuf expressions.
 *
 * The module can be external or runtime-provided, so these rules must be registered by reflection.
 */
object ProtobufExprShims extends org.apache.spark.internal.Logging {
  private[this] val protobufDataToCatalystClassName =
    "org.apache.spark.sql.protobuf.ProtobufDataToCatalyst"

  /** GPU expression rules, or an empty map when the optional spark-protobuf APIs cannot load. */
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    try {
      val clazz = ShimReflectionUtils.loadClass(protobufDataToCatalystClassName)
        .asInstanceOf[Class[_ <: UnaryExpression]]
      Map(clazz.asInstanceOf[Class[_ <: Expression]] -> fromProtobufRule(clazz))
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => Map.empty
    }
  }

  private def fromProtobufRule(clazz: Class[_ <: UnaryExpression]): ExprRule[_ <: Expression] = {
    // Name the rule config and docs after ProtobufDataToCatalyst instead of UnaryExpression.
    implicit val tag: ClassTag[UnaryExpression] = ClassTag(clazz)
    GpuOverrides.expr[UnaryExpression](
      "Decode a BinaryType column (protobuf) into a Spark SQL struct",
      ExprChecks.unaryProject(
        // Output support depends on the protobuf descriptor, so tagExprForGpu checks each field.
        TypeSig.all,
        TypeSig.all,
        TypeSig.BINARY,
        TypeSig.BINARY),
      (e, conf, p, r) => new UnaryExprMeta[UnaryExpression](e, conf, p, r) {

        private var failOnErrors: Boolean = _
        private var malformedRowsAsNullFields: Boolean = _
        private var flatSchema: FlattenedSchemaArrays = _

        override def tagExprForGpu(): Unit = {
          val planned = e.dataType match {
            case st: StructType => planGpuDecode(st)
            case other =>
              Left(s"Only StructType output is supported for from_protobuf, got $other")
          }
          planned match {
            case Left(reason) => willNotWorkOnGpu(reason)
            case Right(_) =>
          }
        }

        private def planGpuDecode(schema: StructType): Either[String, Unit] = for {
          exprInfo <- SparkProtobufCompat.extractExprInfo(e)
          plannerOptions <- SparkProtobufCompat.parsePlannerOptions(exprInfo.options)
          _ <- checkSupportedOptions(exprInfo.options)
          msgDesc <- SparkProtobufCompat.resolveMessageDescriptor(exprInfo)
          // Every field is decoded and returned until schema projection is supported.
          flatFields <- flattenMessage(schema, msgDesc, exprInfo.messageName,
            plannerOptions.enumsAsInts, parentIdx = -1, depth = 0, pathPrefix = "",
            Vector.empty)
          arrays = ProtobufSchemaValidator.toFlattenedSchemaArrays(flatFields.toArray)
          // The JNI constructor validates the schema; fail here rather than on executors.
          _ <- Try(arrays.toJniDescriptor).toEither.left.map { t =>
            s"Protobuf schema is not supported by the GPU decoder: ${t.getMessage}"
          }
        } yield {
          failOnErrors = plannerOptions.failOnErrors
          malformedRowsAsNullFields = plannerOptions.malformedRowsAsNullFields
          flatSchema = arrays
        }

        private def checkSupportedOptions(options: Map[String, String]): Either[String, Unit] = {
          val unsupported = SparkProtobufCompat.unsupportedOptions(options)
          Either.cond(unsupported.isEmpty, (),
            s"from_protobuf options are not supported yet on GPU: ${unsupported.mkString(",")}")
        }

        private def checkFieldsSupported(
            schema: StructType,
            fieldsInfoMap: Map[String, ProtobufFieldInfo],
            pathOf: String => String): Either[String, Unit] = {
          val reasons = schema.fields.flatMap { sf =>
            fieldsInfoMap.get(sf.name).filterNot(_.isSupported).map { info =>
              s"${pathOf(sf.name)}: ${info.unsupportedReason.getOrElse("unknown reason")}"
            }
          }
          Either.cond(reasons.isEmpty, (),
            s"Fields not supported for from_protobuf: ${reasons.mkString(", ")}")
        }

        /**
         * Appends the fields of one message, each followed by the fields of its nested message,
         * because the JNI decoder expects every child after its parent.
         */
        private def flattenMessage(
            schema: StructType,
            msgDesc: ProtobufMessageDescriptor,
            messageName: String,
            enumsAsInts: Boolean,
            parentIdx: Int,
            depth: Int,
            pathPrefix: String,
            flatFields: Vector[FlattenedFieldDescriptor])
            : Either[String, Vector[FlattenedFieldDescriptor]] = {
          def pathOf(name: String): String =
            if (pathPrefix.isEmpty) name else s"$pathPrefix.$name"
          val location =
            if (pathPrefix.isEmpty) "root message" else s"protobuf field '$pathPrefix'"
          val empty: Either[String, Vector[FlattenedFieldDescriptor]] = Right(flatFields)
          for {
            _ <- SparkProtobufCompat.checkNoFieldsMissingFromSchema(schema, msgDesc, location)
            fieldsInfoMap <- ProtobufSchemaExtractor.analyzeAllFields(
              schema, msgDesc, enumsAsInts, messageName)
            _ <- checkFieldsSupported(schema, fieldsInfoMap, pathOf)
            result <- schema.fields.foldLeft(empty) { (fields, sf) =>
              fields.flatMap(flattenField(sf, fieldsInfoMap(sf.name), msgDesc, messageName,
                enumsAsInts, parentIdx, depth, pathOf(sf.name), _))
            }
          } yield result
        }

        private def flattenField(
            sf: StructField,
            info: ProtobufFieldInfo,
            msgDesc: ProtobufMessageDescriptor,
            messageName: String,
            enumsAsInts: Boolean,
            parentIdx: Int,
            depth: Int,
            path: String,
            flatFields: Vector[FlattenedFieldDescriptor])
            : Either[String, Vector[FlattenedFieldDescriptor]] = {
          val nestedSchema = sf.dataType match {
            case st: StructType => Some(st)
            case ArrayType(st: StructType, _) => Some(st)
            case _ => None
          }
          val outputTypeOpt = sf.dataType match {
            case _ if nestedSchema.isDefined => Some(DType.STRUCT.getTypeId.getNativeId)
            case ArrayType(elemType, _) => GpuFromProtobuf.sparkTypeToCudfIdOpt(elemType)
            case other => GpuFromProtobuf.sparkTypeToCudfIdOpt(other)
          }
          for {
            outputType <- outputTypeOpt.toRight(
              s"Unsupported Spark type for protobuf field '$path': ${sf.dataType}")
            flatField <- ProtobufSchemaValidator.toFlattenedFieldDescriptor(
              path, sf, info, parentIdx, depth, outputType, isOutput = true)
            withField = flatFields :+ flatField
            result <- nestedSchema match {
              case Some(st) =>
                for {
                  childDesc <- Try(msgDesc.findField(sf.name).flatMap(_.messageDescriptor))
                    .toOption.flatten.toRight(
                      s"Failed to resolve the message type of protobuf field '$path'")
                  fields <- flattenMessage(st, childDesc, s"$messageName.${sf.name}",
                    enumsAsInts, parentIdx = flatFields.size, depth + 1, path, withField)
                } yield fields
              case None => Right(withField)
            }
          } yield result
        }

        override def convertToGpu(child: Expression): GpuExpression =
          GpuFromProtobuf(e.dataType.asInstanceOf[StructType], flatSchema, failOnErrors,
            malformedRowsAsNullFields, child)
      }
    )
  }

}
