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

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.window.GpuBaseWindowExecMeta

import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference, Expression, GetArrayStructFields, GetStructField, UnaryExpression
}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.rapids.GpuFromProtobuf
import org.apache.spark.sql.rapids.protobuf.{
  FlattenedFieldDescriptor,
  FlattenedSchemaArrays,
  ProtobufFieldInfo,
  ProtobufMessageDescriptor,
  ProtobufSchemaExtractor,
  ProtobufSchemaValidator
}
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

  private final case class DecodePlan(
      decodedSchema: StructType,
      flatSchema: FlattenedSchemaArrays)

  private def fromProtobufRule(clazz: Class[_ <: UnaryExpression]): ExprRule[_ <: Expression] = {
    // Name the rule config and docs after ProtobufDataToCatalyst instead of UnaryExpression.
    implicit val tag: ClassTag[UnaryExpression] = ClassTag(clazz)
    GpuOverrides.expr[UnaryExpression](
      "Decode a BinaryType column (protobuf) into a Spark SQL struct",
      ExprChecks.unaryProject(
        // Use TypeSig.all here because schema projection determines which fields
        // actually need GPU support. Detailed type checking is done in tagExprForGpu.
        TypeSig.all,
        TypeSig.all,
        TypeSig.BINARY,
        TypeSig.BINARY),
      (e, conf, p, r) => new UnaryExprMeta[UnaryExpression](e, conf, p, r)
          with ConsumerDependentExprMeta {

        private var fullSchema: StructType = _
        private var failOnErrors: Boolean = _
        private var malformedRowsAsNullFields: Boolean = _
        private var decodePlan: DecodePlan = _
        // Full-output plan to switch to if a plan reading the pruned struct runs on the CPU.
        private var unprunedPlan: Option[DecodePlan] = None
        // Plans after the decoding plan that receive the decoded struct as input.
        private var structConsumers: Seq[SparkPlanMeta[_]] = Seq.empty

        override def tagExprForGpu(): Unit = {
          val planned = e.dataType match {
            case st: StructType =>
              fullSchema = st
              planGpuDecode()
            case other =>
              Left(s"Only StructType output is supported for from_protobuf, got $other")
          }
          planned match {
            case Left(reason) => willNotWorkOnGpu(reason)
            case Right(_) =>
          }
        }

        private def planGpuDecode(): Either[String, Unit] = for {
          exprInfo <- SparkProtobufCompat.extractExprInfo(e)
          plannerOptions <- SparkProtobufCompat.parsePlannerOptions(exprInfo.options)
          _ <- checkSupportedOptions(exprInfo.options)
          msgDesc <- SparkProtobufCompat.resolveMessageDescriptor(exprInfo)
          _ <- SparkProtobufCompat.validateDescriptorGraph(fullSchema, msgDesc)
          fieldsInfoMap <- ProtobufSchemaExtractor.analyzeAllFields(
            fullSchema, msgDesc, plannerOptions.enumsAsInts, exprInfo.messageName)
          _ <- checkTopLevelFieldsSupported(fieldsInfoMap)
          outputIndices = outputTopLevelIndices(fieldsInfoMap, msgDesc)
          plan <- buildDecodePlan(fieldsInfoMap, msgDesc, outputIndices,
            nestedFieldRequirements, plannerOptions.enumsAsInts)
          // Keep the struct decodable in full while its consumers can still end up on the CPU.
          unpruned <- if (structConsumers.isEmpty || plan.decodedSchema == fullSchema) {
            Right(None)
          } else {
            buildDecodePlan(fieldsInfoMap, msgDesc, fullSchema.fields.indices.toArray,
              Map.empty, plannerOptions.enumsAsInts).map(Some(_)).left.map { reason =>
              s"Decoded protobuf struct is read by later operators, which need a full decode: " +
                reason
            }
          }
        } yield {
          failOnErrors = plannerOptions.failOnErrors
          malformedRowsAsNullFields = plannerOptions.malformedRowsAsNullFields
          decodePlan = plan
          unprunedPlan = unpruned
          overrideDataType(plan.decodedSchema)
        }

        private def buildDecodePlan(
            fieldsInfoMap: Map[String, ProtobufFieldInfo],
            msgDesc: ProtobufMessageDescriptor,
            outputIndices: Array[Int],
            nestedReqs: Map[String, Option[Set[String]]],
            enumsAsInts: Boolean): Either[String, DecodePlan] = for {
          flatFields <- flattenSchema(
            fieldsInfoMap, msgDesc, outputIndices.toSet, nestedReqs, enumsAsInts)
          arrays = ProtobufSchemaValidator.toFlattenedSchemaArrays(flatFields)
          // The JNI constructor validates the schema; fail here rather than on executors.
          _ <- Try(arrays.toJniDescriptor).toEither.left.map { t =>
            s"Protobuf schema is not supported by the GPU decoder: ${t.getMessage}"
          }
        } yield DecodePlan(buildDecodedSchema(outputIndices, buildPrunedFieldsMap(nestedReqs)),
          arrays)

        override def recheckConsumersAfterTag(): Unit = unprunedPlan.foreach { unpruned =>
          if (willUseGpuCpuBridge || structConsumers.exists(readsStructOnCpu)) {
            decodePlan = unpruned
            unprunedPlan = None
            overrideDataType(fullSchema)
          }
        }

        // CPU plans, and consumer expressions moved into the CPU bridge, read the struct with
        // Spark's field ordinals.
        private def readsStructOnCpu(consumer: SparkPlanMeta[_]): Boolean = {
          def bridgedRead(expr: BaseExprMeta[_]): Boolean =
            (expr.willUseGpuCpuBridge &&
              expr.wrapped.asInstanceOf[Expression].references.exists { attr =>
                protobufOutputExprIds.contains(attr.exprId)
              }) || expr.childExprs.exists(bridgedRead)
          (!consumer.canThisBeReplaced && !consumer.shouldThisBeRemoved) ||
            consumer.childExprs.exists(bridgedRead)
        }

        private def checkSupportedOptions(options: Map[String, String]): Either[String, Unit] = {
          val unsupported = SparkProtobufCompat.unsupportedOptions(options)
          Either.cond(unsupported.isEmpty, (),
            s"from_protobuf options are not supported yet on GPU: ${unsupported.mkString(",")}")
        }

        // Every root field is decoded, even when projected out, to keep Spark's validation.
        private def checkTopLevelFieldsSupported(
            fieldsInfoMap: Map[String, ProtobufFieldInfo]): Either[String, Unit] = {
          val reasons = fullSchema.fields.flatMap { sf =>
            fieldsInfoMap.get(sf.name).filterNot(_.isSupported).map { info =>
              s"${sf.name}: ${info.unsupportedReason.getOrElse("unknown reason")}"
            }
          }
          Either.cond(reasons.isEmpty, (),
            s"Fields not supported for from_protobuf: ${reasons.mkString(", ")}")
        }

        /** Top-level fields the query reads, plus proto2 required fields Spark validates. */
        private def outputTopLevelIndices(
            fieldsInfoMap: Map[String, ProtobufFieldInfo],
            msgDesc: ProtobufMessageDescriptor): Array[Int] = {
          val requiredFieldNames = analyzeRequiredFields(fieldsInfoMap.keySet)
          // Pruning a proto2 required field would hide a missing value that Spark reports.
          val protoRequiredFieldNames = fieldsInfoMap.collect {
            case (name, info) if info.isRequired => name
          }.toSet
          val outputFieldNames = requiredFieldNames ++ protoRequiredFieldNames
          augmentNestedRequirementsForValidation(msgDesc)
          fullSchema.fields.zipWithIndex.collect {
            case (sf, idx) if outputFieldNames.contains(sf.name) => idx
          }
        }

        /**
         * Flattens every root field and its selected nested children for the JNI decoder.
         * Fields outside outputIndices are decoded only for validation.
         */
        private def flattenSchema(
            fieldsInfoMap: Map[String, ProtobufFieldInfo],
            msgDesc: ProtobufMessageDescriptor,
            outputIndices: Set[Int],
            nestedReqs: Map[String, Option[Set[String]]],
            enumsAsInts: Boolean): Either[String, Array[FlattenedFieldDescriptor]] = {
          val flatFields = mutable.ArrayBuffer[FlattenedFieldDescriptor]()

          // pathPrefix is the dot-path of ancestor fields (empty for top-level).
          def addFieldWithChildren(
              sf: StructField,
              info: ProtobufFieldInfo,
              parentIdx: Int,
              depth: Int,
              containingMsgDesc: ProtobufMessageDescriptor,
              isOutput: Boolean,
              pathPrefix: String = ""): Either[String, Unit] = {
            val currentIdx = flatFields.size
            val outputTypeOpt = sf.dataType match {
              // Repeated message field: ArrayType(StructType) - element type is STRUCT
              case ArrayType(_: StructType, _) | _: StructType =>
                Some(DType.STRUCT.getTypeId.getNativeId)
              case ArrayType(elemType, _) => GpuFromProtobuf.sparkTypeToCudfIdOpt(elemType)
              case other => GpuFromProtobuf.sparkTypeToCudfIdOpt(other)
            }
            val path = if (pathPrefix.isEmpty) sf.name else s"$pathPrefix.${sf.name}"
            for {
              outputType <- outputTypeOpt.toRight(
                s"Unsupported Spark type for protobuf field '${sf.name}': ${sf.dataType}")
              flatField <- ProtobufSchemaValidator.toFlattenedFieldDescriptor(
                path, sf, info, parentIdx, depth, outputType, isOutput)
              _ = flatFields += flatField
              // Repeated message parents and plain struct parents share the same child
              // expansion path; the flat parent entry's isRepeated flag distinguishes them.
              _ <- sf.dataType match {
                case st: StructType if containingMsgDesc != null =>
                  addChildFieldsFromStruct(
                    st, containingMsgDesc, sf.name, currentIdx, depth, isOutput, pathPrefix)
                case ArrayType(st: StructType, _) if containingMsgDesc != null =>
                  addChildFieldsFromStruct(
                    st, containingMsgDesc, sf.name, currentIdx, depth, isOutput, pathPrefix)
                case _ => Right(())
              }
            } yield ()
          }

          // Applies nested schema pruning at arbitrary depth using path-based
          // lookup into nestedReqs.
          def addChildFieldsFromStruct(
              st: StructType,
              containingMsgDesc: ProtobufMessageDescriptor,
              fieldName: String,
              parentIdx: Int,
              parentDepth: Int,
              isOutput: Boolean,
              pathPrefix: String): Either[String, Unit] = {
            val path = if (pathPrefix.isEmpty) fieldName else s"$pathPrefix.$fieldName"
            val filteredFields = nestedReqs.get(path) match {
              case Some(Some(childNames)) if isOutput =>
                st.fields.filter(f => childNames.contains(f.name))
              case _ =>
                st.fields
            }
            for {
              // containingMsgDesc is the descriptor of the message that directly contains
              // fieldName.
              parentField <- containingMsgDesc.findField(fieldName).toRight(
                s"Nested field '$fieldName' not found in protobuf descriptor at '$path'")
              childMsgDesc <- parentField.messageDescriptor.toRight(
                s"Nested field '$fieldName' at '$path' did not resolve to a message type")
              _ <- forEachUntilLeft(filteredFields.toSeq) { childSf =>
                for {
                  childFd <- childMsgDesc.findField(childSf.name).toRight(
                    s"Nested field '${childSf.name}' not found in protobuf " +
                      s"descriptor for message at '$path'")
                  childInfo <- ProtobufSchemaExtractor
                    .extractFieldInfo(childSf, childFd, enumsAsInts)
                  _ <- Either.cond(childInfo.isSupported, (),
                    s"Nested field '${childSf.name}' at '$path': " +
                      childInfo.unsupportedReason.getOrElse("unsupported type"))
                  _ <- addFieldWithChildren(
                    childSf, childInfo, parentIdx, parentDepth + 1, childMsgDesc, isOutput, path)
                } yield ()
              }
            } yield ()
          }

          forEachUntilLeft(fullSchema.fields.indices) { idx =>
            val sf = fullSchema.fields(idx)
            addFieldWithChildren(sf, fieldsInfoMap(sf.name), -1, 0, msgDesc,
              outputIndices.contains(idx))
          }.map(_ => flatFields.toArray)
        }

        private def forEachUntilLeft[A](items: Seq[A])(
            f: A => Either[String, Unit]): Either[String, Unit] =
          items.foldLeft[Either[String, Unit]](Right(()))((acc, item) => acc.flatMap(_ => f(item)))

        /**
         * Analyze which fields are actually required by downstream operations.
         * Traverses parent plan nodes upward while they can still see the decoded struct,
         * collecting struct field references from ProjectExec, FilterExec, AggregateExec,
         * SortExec and WindowExec, then returns the set of required top-level field names.
         * Plans that receive the struct are recorded in structConsumers.
         *
         * @param allFieldNames All field names in the full schema
         * @return Set of field names that are actually required
         */
        private def analyzeRequiredFields(allFieldNames: Set[String]): Set[String] = {
          val fieldReqs = mutable.Map[String, Option[Set[String]]]()
          protobufOutputExprIds = Set.empty
          structConsumers = Seq.empty
          var hasDirectStructRef = false
          val holder = () => { hasDirectStructRef = true }

          val startingPlanMeta = RapidsMeta.findParentPlanMeta(this)
          var currentMeta = startingPlanMeta
          var safeToPrune = true

          while (currentMeta.isDefined && safeToPrune && !hasDirectStructRef) {
            val planMeta = currentMeta.get
            val allowSemanticReferenceMatch = startingPlanMeta.exists(_ eq planMeta)
            planMeta.wrapped match {
              case p: ProjectExec =>
                if (allowSemanticReferenceMatch) {
                  // Only the decoding plan's aliases carry the pruned type to later plans.
                  protobufOutputExprIds ++= p.projectList.collect {
                    case alias: org.apache.spark.sql.catalyst.expressions.Alias
                        if isProtobufStructReference(alias.child, allowSemanticReferenceMatch) =>
                      alias.exprId
                  }
                }
                p.projectList.foreach(
                  collectStructFieldReferences(
                    _, fieldReqs, holder, allowSemanticReferenceMatch))
              case f: org.apache.spark.sql.execution.FilterExec =>
                collectStructFieldReferences(
                  f.condition, fieldReqs, holder, allowSemanticReferenceMatch)
              case a: org.apache.spark.sql.execution.aggregate.BaseAggregateExec =>
                val exprs = a.aggregateExpressions ++ a.groupingExpressions
                exprs.foreach(
                  collectStructFieldReferences(
                    _, fieldReqs, holder, allowSemanticReferenceMatch))
              case s: org.apache.spark.sql.execution.SortExec =>
                val exprs = s.sortOrder
                exprs.foreach(
                  collectStructFieldReferences(
                    _, fieldReqs, holder, allowSemanticReferenceMatch))
              case window: SparkPlan if SparkShimImpl.isWindowFunctionExec(window) =>
                planMeta match {
                  case windowMeta: GpuBaseWindowExecMeta[_] =>
                    windowMeta.getInputWindowExpressions.foreach(
                      collectStructFieldReferences(
                        _, fieldReqs, holder, allowSemanticReferenceMatch))
                  case _ =>
                    logDebug("Schema pruning disabled: WindowExec metadata does not expose " +
                      "cross-version window expressions")
                    safeToPrune = false
                }
              case other =>
                // Keep schema projection conservative for less common plan shapes that read
                // the decoded struct. Those plans fall back to full-schema decode until we add
                // dedicated coverage for pruning through them.
                logDebug(s"Schema pruning disabled: unrecognized plan node " +
                  s"${other.getClass.getSimpleName} reads from_protobuf output")
                safeToPrune = false
            }
            val passesStruct = planMeta.wrapped.asInstanceOf[SparkPlan].output.exists { attr =>
              protobufOutputExprIds.contains(attr.exprId)
            }
            currentMeta = if (!safeToPrune || hasDirectStructRef || !passesStruct) {
              // Later plans cannot read a struct that this plan does not output.
              None
            } else {
              planMeta.parent match {
                case Some(next: SparkPlanMeta[_])
                    if allowSemanticReferenceMatch || passesConvertedChildTypes(planMeta) =>
                  structConsumers :+= next
                  Some(next)
                case _ =>
                  // The query result, or a plan that only reports Spark's declared types,
                  // receives the struct, so keep it whole.
                  holder()
                  None
              }
            }
          }

          // An empty fieldReqs also subsumes the "no relevant expressions collected" case.
          if (!safeToPrune || hasDirectStructRef || fieldReqs.isEmpty) {
            structConsumers = Seq.empty
            allFieldNames
          } else {
            nestedFieldRequirements = fieldReqs.toMap
            fieldReqs.keySet.toSet
          }
        }

        // These plan metas pass their child's converted output types through to their parent.
        private def passesConvertedChildTypes(planMeta: SparkPlanMeta[_]): Boolean =
          planMeta.wrapped match {
            case _: org.apache.spark.sql.execution.FilterExec |
                 _: org.apache.spark.sql.execution.SortExec => true
            case _ => false
          }

        /**
         * Nested field requirements: maps a field path to child requirements.
         * Keys are dot-separated paths from the protobuf root:
         *   - "level1" -> Some(Set("level2"))          (top-level struct pruning)
         *   - "level1.level2" -> Some(Set("level3"))   (deep nested pruning)
         *   - "field" -> None                           (whole field needed)
         *
         * Top-level names (keys without dots) also determine which fields are decoded.
         */
        private var nestedFieldRequirements: Map[String, Option[Set[String]]] = Map.empty
        private var protobufOutputExprIds: Set[
          org.apache.spark.sql.catalyst.expressions.ExprId] = Set.empty
        private lazy val protobufOutputExprId
          : Option[org.apache.spark.sql.catalyst.expressions.ExprId] =
          parent.flatMap { meta =>
            meta.wrapped match {
              case alias: org.apache.spark.sql.catalyst.expressions
                    .Alias if alias.child.semanticEquals(e) =>
                Some(alias.exprId)
              case _ => None
            }
          }

        private def getFieldName(ordinal: Int, nameOpt: Option[String],
            schema: StructType): String = {
          nameOpt.getOrElse {
            if (ordinal < schema.fields.length) schema.fields(ordinal).name
            else s"_$ordinal"
          }
        }

        /**
         * Navigate the protobuf descriptor tree by following a path of field names.
         * Returns the message descriptor at the end of the path, or None.
         */
        private def resolveProtoMsgDesc(
            rootDesc: ProtobufMessageDescriptor,
            pathParts: Seq[String]): Option[ProtobufMessageDescriptor] = {
          pathParts.headOption match {
            case None => Some(rootDesc)
            case Some(head) =>
              rootDesc.findField(head).flatMap(_.messageDescriptor).flatMap { childDesc =>
                if (pathParts.tail.isEmpty) Some(childDesc)
                else resolveProtoMsgDesc(childDesc, pathParts.tail)
              }
          }
        }

        /** Preserve validation-only nested fields that generic wire skipping cannot validate. */
        private def augmentNestedRequirementsForValidation(
            rootMsgDesc: ProtobufMessageDescriptor): Unit = {
          val augmentedRequirements = nestedFieldRequirements.map {
            case entry @ (pathKey, Some(childNames)) =>
              val pathParts = pathKey.split("\\.").toSeq
              resolveProtoMsgDesc(rootMsgDesc, pathParts) match {
                case Some(childMsgDesc) =>
                  resolveSchemaAtPath(fullSchema, pathParts).map { schemaType =>
                    val childDescriptors = schemaType.fields.flatMap { sf =>
                      childMsgDesc.findField(sf.name).map(sf.name -> _)
                    }
                    val validationSensitiveChildren = childDescriptors.collect {
                      case (name, fd) if fd.protoTypeName == "MESSAGE" || fd.isRepeated => name
                    }.toSet
                    if (!validationSensitiveChildren.subsetOf(childNames)) {
                      pathKey -> None
                    } else {
                      val requiredChildNames = childDescriptors.collect {
                        case (name, fd) if fd.isRequired => name
                      }.toSet
                      if (requiredChildNames.subsetOf(childNames)) entry
                      else pathKey -> Some(childNames ++ requiredChildNames)
                    }
                  }.getOrElse(entry)
                case None => entry
              }
            case other => other
          }
          val wholePaths = augmentedRequirements.collect {
            case (path, None) => path
          }.toSeq
          nestedFieldRequirements = augmentedRequirements.filterNot { case (path, _) =>
            wholePaths.exists(whole => path.startsWith(s"$whole."))
          }
        }

        /**
         * Navigate the Spark schema tree by following a dot-separated path of
         * field names. Returns the StructType at the end of the path, unwrapping
         * ArrayType(StructType) along the way, or None if the path is invalid.
         */
        private def resolveSchemaAtPath(
            root: StructType, path: Seq[String]): Option[StructType] =
          path.foldLeft(Option(root)) { (current, name) =>
            current.flatMap(_.fields.find(_.name == name)).flatMap(_.dataType match {
              case st: StructType => Some(st)
              case ArrayType(st: StructType, _) => Some(st)
              case _ => None
            })
          }

        /**
         * Walk a GetStructField chain upward until it reaches the protobuf
         * reference expression, returning the sequence of field names forming
         * the access path. Returns None if the chain does not terminate at a
         * protobuf reference.
         *
         * Example: for `GetStructField(GetStructField(decoded, a_ord), b_ord)`
         *   → Some(Seq("a", "b"))
         */
        private def resolveFieldAccessChain(
            expr: Expression,
            allowSemanticReferenceMatch: Boolean): Option[Seq[String]] = {
          expr match {
            case GetStructField(child, ordinal, nameOpt) =>
              if (isProtobufStructReference(child, allowSemanticReferenceMatch)) {
                Some(Seq(getFieldName(ordinal, nameOpt, fullSchema)))
              } else {
                resolveFieldAccessChain(child, allowSemanticReferenceMatch).flatMap { parentPath =>
                  resolveSchemaAtPath(fullSchema, parentPath).map { parentSchema =>
                    parentPath :+ getFieldName(ordinal, nameOpt, parentSchema)
                  }
                }
              }
            case _ if isProtobufStructReference(expr, allowSemanticReferenceMatch) =>
              Some(Seq.empty)
            case _ =>
              None
          }
        }

        private def addNestedFieldReq(
            fieldReqs: mutable.Map[String, Option[Set[String]]],
            parentKey: String,
            childName: String): Unit = {
          fieldReqs.get(parentKey) match {
            case Some(None) => // Already need whole field, keep it
            case Some(Some(existing)) =>
              fieldReqs(parentKey) = Some(existing + childName)
            case None =>
              fieldReqs(parentKey) = Some(Set(childName))
          }
        }

        /**
         * Register pruning requirements at every level of a field access path.
         * For path = ["a", "b"] with leafName = "c":
         *   "a" -> needs child "b"
         *   "a.b" -> needs child "c"
         */
        private def registerPathRequirements(
            fieldReqs: mutable.Map[String, Option[Set[String]]],
            path: Seq[String],
            leafName: String): Unit = {
          for (i <- path.indices) {
            val pathKey = path.take(i + 1).mkString(".")
            val childName = if (i < path.length - 1) path(i + 1) else leafName
            addNestedFieldReq(fieldReqs, pathKey, childName)
          }
        }

        private def isStructLike(dataType: DataType): Boolean = dataType match {
          case _: StructType => true
          case ArrayType(_: StructType, _) => true
          case _ => false
        }

        private def collectStructFieldReferences(
            expr: Expression,
            fieldReqs: mutable.Map[String, Option[Set[String]]],
            hasDirectStructRefHolder: () => Unit,
            allowSemanticReferenceMatch: Boolean): Unit = {
          expr match {
            case GetStructField(child, ordinal, nameOpt) =>
              resolveFieldAccessChain(child, allowSemanticReferenceMatch) match {
                case Some(parentPath) =>
                  resolveSchemaAtPath(fullSchema, parentPath) match {
                    case Some(parentSchema) =>
                      val fieldName = getFieldName(ordinal, nameOpt, parentSchema)
                      if (parentPath.isEmpty) {
                        // Direct top-level access: decoded.field_name (whole field)
                        fieldReqs(fieldName) = None
                      } else {
                        registerPathRequirements(fieldReqs, parentPath, fieldName)
                        if (isStructLike(expr.dataType)) {
                          fieldReqs((parentPath :+ fieldName).mkString(".")) = None
                        }
                      }
                    case None =>
                      collectStructFieldReferences(
                        child, fieldReqs, hasDirectStructRefHolder, allowSemanticReferenceMatch)
                  }
                case None =>
                  collectStructFieldReferences(
                    child, fieldReqs, hasDirectStructRefHolder, allowSemanticReferenceMatch)
              }

            case gasf: GetArrayStructFields =>
              resolveFieldAccessChain(gasf.child, allowSemanticReferenceMatch) match {
                case Some(parentPath) if parentPath.nonEmpty =>
                  registerPathRequirements(fieldReqs, parentPath, gasf.field.name)
                  if (isStructLike(gasf.dataType)) {
                    fieldReqs((parentPath :+ gasf.field.name).mkString(".")) = None
                  }
                case Some(parentPath) if parentPath.isEmpty =>
                  logDebug("Schema pruning disabled: unexpected direct protobuf reference in " +
                    "GetArrayStructFields")
                  hasDirectStructRefHolder()
                case _ =>
                  gasf.children.foreach { child =>
                    collectStructFieldReferences(
                      child, fieldReqs, hasDirectStructRefHolder, allowSemanticReferenceMatch)
                  }
              }

            case alias: org.apache.spark.sql.catalyst.expressions.Alias =>
              if (!isProtobufStructReference(alias.child, allowSemanticReferenceMatch)) {
                collectStructFieldReferences(
                  alias.child, fieldReqs, hasDirectStructRefHolder, allowSemanticReferenceMatch)
              } else if (!protobufOutputExprIds.contains(alias.exprId)) {
                // Other aliases publish Spark's declared struct type to later plans.
                hasDirectStructRefHolder()
              }

            case _ =>
              if (isProtobufStructReference(expr, allowSemanticReferenceMatch)) {
                hasDirectStructRefHolder()
              }
              expr.children.foreach { child =>
                collectStructFieldReferences(
                  child, fieldReqs, hasDirectStructRefHolder, allowSemanticReferenceMatch)
              }
          }
        }

        private def buildPrunedFieldsMap(
            nestedReqs: Map[String, Option[Set[String]]]): Map[String, Seq[String]] = {
          nestedReqs.collect {
            case (pathKey, Some(childNames)) =>
              val pathParts = pathKey.split("\\.").toSeq
              resolveSchemaAtPath(fullSchema, pathParts) match {
                case Some(childSchema) =>
                  pathKey -> childSchema.fields.map(_.name).filter(childNames.contains).toSeq
                case None =>
                  // This path should be unreachable for valid schema paths, but keep the
                  // fallback deterministic rather than relying on Set iteration order.
                  pathKey -> childNames.toSeq.sorted
              }
          }
        }

        private def buildDecodedSchema(
            outputIndices: Array[Int],
            prunedFieldsMap: Map[String, Seq[String]]): StructType = {
          def applyPruning(field: StructField, prefix: String): StructField = {
            val path = if (prefix.isEmpty) field.name else s"$prefix.${field.name}"
            prunedFieldsMap.get(path) match {
              case Some(childNames) =>
                field.dataType match {
                  case ArrayType(st: StructType, cn) =>
                    val pruned = StructType(
                      st.fields.filter(f => childNames.contains(f.name))
                        .map(f => applyPruning(f, path)))
                    field.copy(dataType = ArrayType(pruned, cn))
                  case st: StructType =>
                    val pruned = StructType(
                      st.fields.filter(f => childNames.contains(f.name))
                        .map(f => applyPruning(f, path)))
                    field.copy(dataType = pruned)
                  case _ => field
                }
              case None =>
                field.dataType match {
                  case ArrayType(st: StructType, cn) =>
                    val recursed = StructType(st.fields.map(f => applyPruning(f, path)))
                    if (recursed != st) field.copy(dataType = ArrayType(recursed, cn))
                    else field
                  case st: StructType =>
                    val recursed = StructType(st.fields.map(f => applyPruning(f, path)))
                    if (recursed != st) field.copy(dataType = recursed)
                    else field
                  case _ => field
                }
            }
          }

          val decodedFields = outputIndices.map { idx =>
            applyPruning(fullSchema.fields(idx), "")
          }
          StructType(decodedFields)
        }

        /**
         * Check if an expression references the output of a protobuf decode expression.
         * This can be either:
         * 1. The ProtobufDataToCatalyst expression itself
         * 2. An AttributeReference that references the output of ProtobufDataToCatalyst
         *    (when accessing from a downstream ProjectExec)
         */
        private def isProtobufStructReference(
            expr: Expression,
            allowSemanticReferenceMatch: Boolean): Boolean = {
          // Catalyst may create duplicate ProtobufDataToCatalyst
          // instances for each GetStructField access. Match copies
          // by class + identical input child + identical decode
          // semantics so that
          // analyzeRequiredFields detects all field accesses in one
          // pass, keeping schema projection correct.
          def isEquivalentDecode: Boolean =
            allowSemanticReferenceMatch &&
              expr.getClass == e.getClass &&
              expr.children.nonEmpty &&
              e.children.nonEmpty &&
              ((expr.children.head eq e.children.head) ||
                expr.children.head.semanticEquals(
                  e.children.head)) &&
              SparkProtobufCompat.sameDecodeSemantics(expr, e)

          (expr eq e) || expr.semanticEquals(e) || isEquivalentDecode || (expr match {
            case attr: AttributeReference =>
              protobufOutputExprIds.contains(attr.exprId) ||
                protobufOutputExprId.exists(_ == attr.exprId)
            case _ => false
          })
        }

        override def convertToGpu(child: Expression): GpuExpression = {
          GpuFromProtobuf(decodePlan.decodedSchema, decodePlan.flatSchema, failOnErrors,
            malformedRowsAsNullFields, child)
        }
      }
    )
  }

}
