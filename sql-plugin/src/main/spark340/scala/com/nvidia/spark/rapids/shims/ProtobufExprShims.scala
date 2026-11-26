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
{"spark":["340"],"carryForward":"sameMinor"}
{"spark":["350","400","420"],"carryForward":"all"}
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import scala.reflect.ClassTag
import scala.util.Try

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.rapids.GpuFromProtobuf
import org.apache.spark.sql.rapids.protobuf.{FieldReads, FlattenedFieldDescriptor,
  FlattenedSchemaArrays, ProtobufExprInfo, ProtobufFieldDescriptor, ProtobufFieldInfo,
  ProtobufMessageDescriptor, ProtobufPlannerOptions, ProtobufSchemaExtractor,
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
          // Spark returns an empty schema when it cannot represent any field of the message,
          // and the decoder cannot build a struct column without children.
          _ <- Either.cond(schema.fields.nonEmpty, (),
            "from_protobuf without any field in the Spark schema is not supported on GPU")
          exprInfo <- SparkProtobufCompat.extractExprInfo(e)
          plannerOptions <- SparkProtobufCompat.parsePlannerOptions(exprInfo.options)
          _ <- checkSupportedOptions(exprInfo.options)
          msgDesc <- SparkProtobufCompat.resolveMessageDescriptor(exprInfo)
          // Fields the query does not read are still validated, like Spark does, but are
          // returned as nulls. Equivalent deterministic decodes in the same plan read the union
          // so GpuTieredProject CSE still decodes the message once; reference identity still
          // matches non-deterministic inputs where semanticEquals returns false.
          reads = FieldReads.analyze(this,
            equivalentDecode(e, exprInfo, plannerOptions, SparkProtobufCompat.extractExprInfo, _))
          flatFields <- flattenMessage(schema, msgDesc, exprInfo.messageName,
            plannerOptions.enumsAsInts, reads, parentVisible = true, parentIdx = -1, depth = 0,
            pathPrefix = "", Vector.empty)
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
            reads: FieldReads,
            parentVisible: Boolean,
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
            hidden <- SparkProtobufCompat.hiddenFieldsMissingFromSchema(schema, msgDesc, location)
            fieldsInfoMap <- ProtobufSchemaExtractor.analyzeAllFields(
              schema, msgDesc, enumsAsInts, messageName)
            _ <- checkFieldsSupported(schema, fieldsInfoMap, pathOf)
            outputFields <- schema.fields.foldLeft(empty) { (fields, sf) =>
              fields.flatMap(flattenField(sf, fieldsInfoMap(sf.name), msgDesc, messageName,
                enumsAsInts, reads.child(sf.name), parentVisible, parentIdx, depth,
                pathOf(sf.name), _))
            }
            result <- flattenHiddenFields(hidden, parentIdx, depth, outputFields)
          } yield result
        }

        /**
         * Appends the message fields Spark leaves out of its schema, each followed by its own
         * fields, so that the decoder parses and validates the whole omitted subtree. They go
         * last at every level, because GpuFromProtobuf drops the trailing columns the decoder
         * returns for them.
         */
        private def flattenHiddenFields(
            hidden: Seq[ProtobufFieldDescriptor],
            parentIdx: Int,
            depth: Int,
            flatFields: Vector[FlattenedFieldDescriptor])
            : Either[String, Vector[FlattenedFieldDescriptor]] = {
          val start: Either[String, Vector[FlattenedFieldDescriptor]] = Right(flatFields)
          hidden.foldLeft(start) { (fields, fieldDesc) =>
            fields.flatMap { current =>
              for {
                hiddenField <- ProtobufSchemaValidator.toHiddenMessageFieldDescriptor(fieldDesc,
                  parentIdx = parentIdx, depth = depth,
                  outputTypeId = DType.STRUCT.getTypeId.getNativeId)
                children <- hiddenFieldsOf(fieldDesc)
                result <- flattenHiddenFields(children, parentIdx = current.size, depth + 1,
                  current :+ hiddenField)
              } yield result
            }
          }
        }

        /** The declared fields of a hidden message, which are hidden messages themselves. */
        private def hiddenFieldsOf(
            fieldDesc: ProtobufFieldDescriptor): Either[String, Seq[ProtobufFieldDescriptor]] =
          Try {
            fieldDesc.messageDescriptor
              .map(msg => msg.fieldNames.flatMap(msg.findField))
              .getOrElse(Seq.empty)
          }.toEither.left.map { t =>
            s"Failed to read the message type of protobuf field '${fieldDesc.name}': " +
              s"${t.getMessage}"
          }

        private def flattenField(
            sf: StructField,
            info: ProtobufFieldInfo,
            msgDesc: ProtobufMessageDescriptor,
            messageName: String,
            enumsAsInts: Boolean,
            reads: Option[FieldReads],
            parentVisible: Boolean,
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
          // Non-nullable fields (proto2 required) stay output under a visible parent so
          // UnsafeProjection never dereferences nulls in nullable=false struct slots.
          val isVisible = reads.isDefined || (parentVisible && !sf.nullable)
          for {
            outputType <- outputTypeOpt.toRight(
              s"Unsupported Spark type for protobuf field '$path': ${sf.dataType}")
            flatField <- ProtobufSchemaValidator.toFlattenedFieldDescriptor(
              path, sf, info, parentIdx, depth, outputType, isOutput = isVisible)
            withField = flatFields :+ flatField
            result <- nestedSchema match {
              case Some(st) =>
                for {
                  childDesc <- Try(msgDesc.findField(sf.name).flatMap(_.messageDescriptor))
                    .toOption.flatten.toRight(
                      s"Failed to resolve the message type of protobuf field '$path'")
                  // The children of a field that is not read are not read either.
                  fields <- flattenMessage(st, childDesc, s"$messageName.${sf.name}",
                    enumsAsInts, reads.getOrElse(FieldReads.Unread), parentVisible = isVisible,
                    parentIdx = flatFields.size, depth + 1, path, withField)
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

  /**
   * Spark loads identical descriptor files into distinct byte arrays, so two independent
   * `from_protobuf` calls fail Catalyst `semanticEquals` even though they convert to identical
   * `GpuFromProtobuf` nodes when given the same output mask. Compare deterministic decodes by
   * descriptor content and normalized planner options so GPU tiered projection CSE stays intact.
   */
  private[shims] def equivalentDecode(
      target: UnaryExpression,
      targetInfo: ProtobufExprInfo,
      targetOptions: ProtobufPlannerOptions,
      extractInfo: Expression => Either[String, ProtobufExprInfo],
      candidate: Expression): Boolean = {
    if ((candidate eq target) || candidate.semanticEquals(target)) {
      true
    } else if (target.deterministic && candidate.getClass == target.getClass &&
        candidate.deterministic && candidate.dataType == target.dataType &&
        candidate.asInstanceOf[UnaryExpression].child.semanticEquals(target.child)) {
      (for {
        info <- extractInfo(candidate)
        opts <- SparkProtobufCompat.parsePlannerOptions(info.options)
      } yield {
        SparkProtobufCompat.unsupportedOptions(info.options).isEmpty &&
          info.messageName == targetInfo.messageName &&
          info.descriptorSource == targetInfo.descriptorSource &&
          opts == targetOptions
      }).getOrElse(false)
    } else {
      false
    }
  }

}
