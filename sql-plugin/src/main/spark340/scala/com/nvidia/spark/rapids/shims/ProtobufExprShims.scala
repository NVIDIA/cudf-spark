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
{"spark": "341db"}
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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import scala.collection.mutable

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference, Expression, GetArrayStructFields, GetStructField, UnaryExpression
}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.rapids.GpuFromProtobuf
import org.apache.spark.sql.rapids.protobuf.{
  FlattenedFieldDescriptor,
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

  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    try {
      val clazz = ShimReflectionUtils.loadClass(protobufDataToCatalystClassName)
        .asInstanceOf[Class[_ <: UnaryExpression]]
      Map(clazz.asInstanceOf[Class[_ <: Expression]] -> fromProtobufRule)
    } catch {
      case _: ClassNotFoundException => Map.empty
      case e: Exception =>
        logWarning(s"Failed to load $protobufDataToCatalystClassName: ${e.getMessage}")
        Map.empty
      case e: Error =>
        logWarning(
          s"JVM error while loading $protobufDataToCatalystClassName: ${e.getMessage}")
        Map.empty
    }
  }

  private def fromProtobufRule: ExprRule[_ <: Expression] = {
    GpuOverrides.expr[UnaryExpression](
      "Decode a BinaryType column (protobuf) into a Spark SQL struct",
      ExprChecks.unaryProject(
        // Use TypeSig.all here because schema projection determines which fields
        // actually need GPU support. Detailed type checking is done in tagExprForGpu.
        TypeSig.all,
        TypeSig.all,
        TypeSig.BINARY,
        TypeSig.BINARY),
      (e, conf, p, r) => new UnaryExprMeta[UnaryExpression](e, conf, p, r) {

        private var fullSchema: StructType = _
        private var failOnErrors: Boolean = _
        private var preventCpuBridge: Boolean = false

        // Flattened schema variables for GPU decoding
        private var flatFieldNumbers: Array[Int] = _
        private var flatParentIndices: Array[Int] = _
        private var flatDepthLevels: Array[Int] = _
        private var flatWireTypes: Array[Int] = _
        private var flatOutputTypeIds: Array[Int] = _
        private var flatEncodings: Array[Int] = _
        private var flatIsRepeated: Array[Boolean] = _
        private var flatIsRequired: Array[Boolean] = _
        private var flatHasDefaultValue: Array[Boolean] = _
        private var flatIsOutput: Array[Boolean] = _
        private var flatDefaultInts: Array[Long] = _
        private var flatDefaultFloats: Array[Double] = _
        private var flatDefaultBools: Array[Boolean] = _
        private var flatDefaultStrings: Array[Array[Byte]] = _
        private var flatEnumValidValues: Array[Array[Int]] = _
        private var flatEnumNames: Array[Array[Array[Byte]]] = _
        // Indices in fullSchema for top-level fields visible in the returned decoded schema.
        private var decodedTopLevelIndices: Array[Int] = _

        override def tagExprForGpu(): Unit = {
          fullSchema = e.dataType match {
            case st: StructType => st
            case other =>
              willNotWorkOnGpu(
                s"Only StructType output is supported for from_protobuf, got $other")
              return
          }

          val exprInfo = SparkProtobufCompat.extractExprInfo(e) match {
            case Right(info) => info
            case Left(reason) =>
              willNotWorkOnGpu(reason)
              return
          }
          val plannerOptions = SparkProtobufCompat.parsePlannerOptions(exprInfo.options) match {
            case Right(opts) => opts
            case Left(reason) =>
              // Interpreted bridge evaluation can bypass Spark's option validation.
              preventCpuBridge = true
              willNotWorkOnGpu(reason)
              return
          }
          val enumsAsInts = plannerOptions.enumsAsInts
          failOnErrors = plannerOptions.failOnErrors
          val unsupportedOptions = SparkProtobufCompat.unsupportedOptions(exprInfo.options)
          if (unsupportedOptions.nonEmpty) {
            val keys = unsupportedOptions.mkString(",")
            willNotWorkOnGpu(
              s"from_protobuf options are not supported yet on GPU: $keys")
            return
          }
          val messageName = exprInfo.messageName

          val msgDesc = SparkProtobufCompat.resolveMessageDescriptor(exprInfo) match {
            case Right(desc) => desc
            case Left(reason) =>
              willNotWorkOnGpu(reason)
              return
          }

          SparkProtobufCompat.validateDescriptorGraphSyntax(fullSchema, msgDesc) match {
            case Left(reason) =>
              willNotWorkOnGpu(reason)
              return
            case Right(_) =>
          }

          // Step 1: Analyze all fields and build field info map
          val fieldsInfoMap =
            ProtobufSchemaExtractor.analyzeAllFields(fullSchema, msgDesc, enumsAsInts, messageName)
              .fold({ reason =>
                willNotWorkOnGpu(reason)
                return
              }, identity)

          // Step 2: Determine which fields are actually required by downstream operations
          val requiredFieldNames = analyzeRequiredFields(fieldsInfoMap.keySet)

          // Step 2b: Proto2 required fields must always be decoded so the GPU can
          // detect missing-required and null the struct row (PERMISSIVE) or throw
          // (FAILFAST), matching CPU behavior. Without this, schema projection
          // can prune a required field and the GPU silently produces a non-null
          // struct where CPU would have returned null.
          val protoRequiredFieldNames = fieldsInfoMap.collect {
            case (name, info) if info.isRequired => name
          }.toSet
          val outputFieldNames = requiredFieldNames ++ protoRequiredFieldNames

          // Step 2c: Preserve validation semantics when nested pruning selects only some children.
          augmentNestedRequirementsForValidation(msgDesc)

          // Step 3: Check if all fields to be decoded are supported
          val unsupportedRequired = outputFieldNames.filter { name =>
            fieldsInfoMap.get(name).exists(!_.isSupported)
          }

          if (unsupportedRequired.nonEmpty) {
            val reasons = unsupportedRequired.map { name =>
              val info = fieldsInfoMap(name)
              s"${name}: ${info.unsupportedReason.getOrElse("unknown reason")}"
            }
            willNotWorkOnGpu(
              s"Required fields not supported for from_protobuf: ${reasons.mkString(", ")}")
            return
          }

          // Step 4: Keep every root descriptor for Spark's root unknown-field/type validation,
          // while using isOutput to avoid materializing fields that were projected out.
          val outputTopLevelIndices = fullSchema.fields.zipWithIndex.collect {
            case (sf, idx) if outputFieldNames.contains(sf.name) => idx
          }
          val outputIndexSet = outputTopLevelIndices.toSet
          val indicesToDecode = fullSchema.fields.indices.toArray

          // Verify all fields to be decoded are actually supported
          // (This catches edge cases where field analysis might have issues)
          val unsupportedInDecode = indicesToDecode.filter { idx =>
            val sf = fullSchema.fields(idx)
            fieldsInfoMap.get(sf.name).exists(!_.isSupported)
          }
          if (unsupportedInDecode.nonEmpty) {
            val reasons = unsupportedInDecode.map { idx =>
              val sf = fullSchema.fields(idx)
              val info = fieldsInfoMap(sf.name)
              s"${sf.name}: ${info.unsupportedReason.getOrElse("unknown reason")}"
            }
            willNotWorkOnGpu(
              s"Fields not supported for from_protobuf: ${reasons.mkString(", ")}")
            return
          }

          // Step 5: Build flattened schema for GPU decoding.
          // The flattened schema represents nested fields with parent indices.
          // For pure scalar schemas, all fields are top-level (parentIdx == -1, depth == 0).
          {
            val flatFields = mutable.ArrayBuffer[FlattenedFieldDescriptor]()
            var step5Failed = false

            def failStep5(reason: String): Unit = {
              step5Failed = true
              willNotWorkOnGpu(reason)
            }

            // Helper to add a field and its children recursively.
            // pathPrefix is the dot-path of ancestor fields (empty for top-level).
            def addFieldWithChildren(
                sf: StructField,
                info: ProtobufFieldInfo,
                parentIdx: Int,
                depth: Int,
                containingMsgDesc: ProtobufMessageDescriptor,
                isOutput: Boolean,
                pathPrefix: String = ""): Unit = {

              val currentIdx = flatFields.size

              if (depth >= ProtobufSchemaValidator.MAX_NESTING_DEPTH) {
                failStep5(
                  "Protobuf nesting depth exceeds maximum supported depth of " +
                    ProtobufSchemaValidator.MAX_NESTING_DEPTH)
                return
              }

              val outputTypeOpt = sf.dataType match {
                case ArrayType(elemType, _) =>
                  elemType match {
                    case _: StructType =>
                      // Repeated message field: ArrayType(StructType) - element type is STRUCT
                      Some(DType.STRUCT.getTypeId.getNativeId)
                    case other =>
                      GpuFromProtobuf.sparkTypeToCudfIdOpt(other)
                  }
                case _: StructType =>
                  Some(DType.STRUCT.getTypeId.getNativeId)
                case other =>
                  GpuFromProtobuf.sparkTypeToCudfIdOpt(other)
              }
              val outputType = outputTypeOpt.getOrElse {
                failStep5(
                  s"Unsupported Spark type for protobuf field '${sf.name}': ${sf.dataType}")
                return
              }

              val path = if (pathPrefix.isEmpty) sf.name else s"$pathPrefix.${sf.name}"
              ProtobufSchemaValidator.toFlattenedFieldDescriptor(
                path,
                sf,
                info,
                parentIdx,
                depth,
                outputType,
                isOutput).fold({ reason =>
                failStep5(reason)
                return
              }, flatFields += _)

              // For nested struct types (including repeated message = ArrayType(StructType)), 
              // add child fields
              sf.dataType match {
                case st: StructType if containingMsgDesc != null =>
                  // Repeated message parents and plain struct parents share the same child
                  // expansion path; the flat parent entry's isRepeated flag distinguishes them.
                  addChildFieldsFromStruct(
                    st, containingMsgDesc, sf.name, currentIdx, depth, isOutput, pathPrefix)
                  
                case ArrayType(st: StructType, _) if containingMsgDesc != null =>
                  addChildFieldsFromStruct(
                    st, containingMsgDesc, sf.name, currentIdx, depth, isOutput, pathPrefix)
                  
                case _ => // Not a struct, no children to add
              }
            }
            
            // Helper to add child fields from a struct type.
            // Applies nested schema pruning at arbitrary depth using path-based
            // lookup into nestedFieldRequirements.
            def addChildFieldsFromStruct(
                st: StructType,
                containingMsgDesc: ProtobufMessageDescriptor,
                fieldName: String,
                parentIdx: Int,
                parentDepth: Int,
                isOutput: Boolean,
                pathPrefix: String): Unit = {
              val path = if (pathPrefix.isEmpty) fieldName else s"$pathPrefix.$fieldName"
              // containingMsgDesc is the descriptor of the message that directly contains
              // fieldName.
              val parentField = containingMsgDesc.findField(fieldName)
              if (parentField.isEmpty) {
                failStep5(
                  s"Nested field '$fieldName' not found in protobuf descriptor at '$path'")
                return
              } else {
                parentField.get.messageDescriptor match {
                  case Some(childMsgDesc) =>
                    val filteredFields = if (!isOutput) {
                      st.fields
                    } else {
                      nestedFieldRequirements.get(path) match {
                        case Some(Some(childNames)) =>
                          st.fields.filter(f => childNames.contains(f.name))
                        case _ =>
                          st.fields
                      }
                    }
                    filteredFields.foreach { childSf =>
                      childMsgDesc.findField(childSf.name) match {
                        case None =>
                          failStep5(
                            s"Nested field '${childSf.name}' not found in protobuf " +
                              s"descriptor for message at '$path'")
                          return
                        case Some(childFd) =>
                          ProtobufSchemaExtractor
                            .extractFieldInfo(childSf, childFd, enumsAsInts) match {
                            case Left(reason) =>
                              failStep5(reason)
                              return
                            case Right(childInfo) =>
                              if (!childInfo.isSupported) {
                                failStep5(
                                  s"Nested field '${childSf.name}' at '$path': " +
                                    childInfo.unsupportedReason.getOrElse("unsupported type"))
                                return
                              } else {
                                addFieldWithChildren(
                                  childSf, childInfo, parentIdx, parentDepth + 1, childMsgDesc,
                                  isOutput, path)
                              }
                          }
                      }
                    }
                  case None =>
                    failStep5(
                      s"Nested field '$fieldName' at '$path' did not resolve to a message type")
                    return
                }
              }
            }

            // Only add top-level fields that are actually required (schema projection).
            // This significantly reduces GPU memory and computation for schemas with many
            // fields when only a few are needed. Struct extractors remap ordinals from the
            // converted child schema during GPU conversion.
            decodedTopLevelIndices = outputTopLevelIndices
            indicesToDecode.foreach { schemaIdx =>
              if (!step5Failed) {
                val sf = fullSchema.fields(schemaIdx)
                val info = fieldsInfoMap(sf.name)
                addFieldWithChildren(sf, info, -1, 0, msgDesc, outputIndexSet.contains(schemaIdx))
              }
            }

            if (step5Failed) {
              return
            }

            // Populate flattened schema variables
            val flat = flatFields.toArray
            ProtobufSchemaValidator.validateFlattenedSchema(flat).fold({ reason =>
              failStep5(reason)
              return
            }, identity)
            val arrays = ProtobufSchemaValidator.toFlattenedSchemaArrays(flat)
            flatFieldNumbers = arrays.fieldNumbers
            flatParentIndices = arrays.parentIndices
            flatDepthLevels = arrays.depthLevels
            flatWireTypes = arrays.wireTypes
            flatOutputTypeIds = arrays.outputTypeIds
            flatEncodings = arrays.encodings
            flatIsRepeated = arrays.isRepeated
            flatIsRequired = arrays.isRequired
            flatHasDefaultValue = arrays.hasDefaultValue
            flatIsOutput = arrays.isOutput
            flatDefaultInts = arrays.defaultInts
            flatDefaultFloats = arrays.defaultFloats
            flatDefaultBools = arrays.defaultBools
            flatDefaultStrings = arrays.defaultStrings
            flatEnumValidValues = arrays.enumValidValues
            flatEnumNames = arrays.enumNames

            overrideDataType(buildDecodedSchema(buildPrunedFieldsMap()))
          }
        }

        /**
         * Analyze which fields are actually required by downstream operations.
         * Traverses parent plan nodes upward, collecting struct field references from
         * ProjectExec, FilterExec, and transparent pass-through nodes (AggregateExec,
         * SortExec, WindowExec, etc.), then returns the set of required top-level
         * field names.
         *
         * @param allFieldNames All field names in the full schema
         * @return Set of field names that are actually required
         */
        private def analyzeRequiredFields(allFieldNames: Set[String]): Set[String] = {
          val fieldReqs = mutable.Map[String, Option[Set[String]]]()
          protobufOutputExprIds = Set.empty
          var hasDirectStructRef = false
          val holder = () => { hasDirectStructRef = true }

          var currentMeta: Option[SparkPlanMeta[_]] = findParentPlanMeta()
          var safeToPrune = true
          val startingPlanMeta = currentMeta

          def advanceToParent(): Unit = {
            currentMeta = currentMeta.get.parent match {
              case Some(pm: SparkPlanMeta[_]) => Some(pm)
              case _ => None
            }
          }

          while (currentMeta.isDefined && safeToPrune) {
            val planMeta = currentMeta.get
            val allowSemanticReferenceMatch = currentMeta == startingPlanMeta
            planMeta.wrapped match {
              case p: ProjectExec =>
                val directAliases = p.projectList.collect {
                  case alias: org.apache.spark.sql.catalyst.expressions.Alias
                      if isProtobufStructReference(
                        alias.child, allowSemanticReferenceMatch) =>
                    alias.exprId
                }
                protobufOutputExprIds ++= directAliases
                p.projectList.foreach(
                  collectStructFieldReferences(
                    _, fieldReqs, holder, allowSemanticReferenceMatch))
                advanceToParent()
              case f: org.apache.spark.sql.execution.FilterExec =>
                collectStructFieldReferences(
                  f.condition, fieldReqs, holder, allowSemanticReferenceMatch)
                advanceToParent()
              case a: org.apache.spark.sql.execution.aggregate.BaseAggregateExec =>
                val exprs = a.aggregateExpressions ++ a.groupingExpressions
                exprs.foreach(
                  collectStructFieldReferences(
                    _, fieldReqs, holder, allowSemanticReferenceMatch))
                advanceToParent()
              case s: org.apache.spark.sql.execution.SortExec =>
                val exprs = s.sortOrder
                exprs.foreach(
                  collectStructFieldReferences(
                    _, fieldReqs, holder, allowSemanticReferenceMatch))
                advanceToParent()
              case w: org.apache.spark.sql.execution.window.WindowExec =>
                val exprs = w.windowExpression
                exprs.foreach(
                  collectStructFieldReferences(
                    _, fieldReqs, holder, allowSemanticReferenceMatch))
                advanceToParent()
              case other =>
                // Keep schema projection conservative for less common plan shapes above
                // from_protobuf. Those plans currently fall back to full-schema decode
                // until we add dedicated coverage for pruning through them.
                logDebug(s"Schema pruning disabled: unrecognized plan node " +
                  s"${other.getClass.getSimpleName} above from_protobuf")
                safeToPrune = false
            }
            if (planMeta.parent.isEmpty &&
                planMeta.wrapped.asInstanceOf[SparkPlan].output.exists { attr =>
                  protobufOutputExprIds.contains(attr.exprId)
                }) {
              holder()
            }
          }

          // An empty fieldReqs also subsumes the "no relevant expressions collected" case.
          if (!safeToPrune || hasDirectStructRef || fieldReqs.isEmpty) {
            allFieldNames
          } else {
            nestedFieldRequirements = fieldReqs.toMap
            fieldReqs.keySet.toSet
          }
        }

        /**
         * Find the parent SparkPlanMeta by traversing up the parent chain.
         */
        private def findParentPlanMeta(): Option[SparkPlanMeta[_]] = {
          def traverse(meta: Option[RapidsMeta[_, _, _]]): Option[SparkPlanMeta[_]] = {
            meta match {
              case Some(p: SparkPlanMeta[_]) => Some(p)
              case Some(p: RapidsMeta[_, _, _]) => traverse(p.parent)
              case _ => None
            }
          }
          traverse(parent)
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
          if (nestedFieldRequirements.isEmpty) return
          val augmentedRequirements = nestedFieldRequirements.map {
            case entry @ (pathKey, Some(childNames)) =>
              val pathParts = pathKey.split("\\.").toSeq
              resolveProtoMsgDesc(rootMsgDesc, pathParts) match {
                case Some(childMsgDesc) =>
                  val schemaType = resolveSchemaAtPath(fullSchema, pathParts)
                  if (schemaType != null) {
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
                  } else {
                    entry
                  }
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
         * ArrayType(StructType) along the way, or null if the path is invalid.
         */
        private def resolveSchemaAtPath(root: StructType, path: Seq[String]): StructType = {
          var current: StructType = root
          for (name <- path) {
            val field = current.fields.find(_.name == name).orNull
            if (field == null) return null
            field.dataType match {
              case st: StructType => current = st
              case ArrayType(st: StructType, _) => current = st
              case _ => return null
            }
          }
          current
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
                  val parentSchema = if (parentPath.isEmpty) fullSchema
                                     else resolveSchemaAtPath(fullSchema, parentPath)
                  if (parentSchema != null) {
                    Some(parentPath :+ getFieldName(ordinal, nameOpt, parentSchema))
                  } else {
                    None
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
                  val parentSchema = if (parentPath.isEmpty) fullSchema
                                     else resolveSchemaAtPath(fullSchema, parentPath)
                  if (parentSchema != null) {
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
                  } else {
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

        private def buildPrunedFieldsMap(): Map[String, Seq[String]] = {
          nestedFieldRequirements.collect {
            case (pathKey, Some(childNames)) =>
              val pathParts = pathKey.split("\\.").toSeq
              val childSchema = resolveSchemaAtPath(fullSchema, pathParts)
              if (childSchema != null) {
                val orderedNames = childSchema.fields
                  .map(_.name)
                  .filter(childNames.contains)
                  .toSeq
                pathKey -> orderedNames
              } else {
                // This path should be unreachable for valid schema paths, but keep the fallback
                // deterministic rather than relying on Set iteration order.
                pathKey -> childNames.toSeq.sorted
              }
          }
        }

        private def buildDecodedSchema(prunedFieldsMap: Map[String, Seq[String]]): StructType = {
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

          val decodedFields = decodedTopLevelIndices.map { idx =>
            applyPruning(fullSchema.fields(idx), "")
          }
          StructType(decodedFields.map(f => f.copy(nullable = true)))
        }

        override def preventsTreeBridgeOptimization: Boolean = preventCpuBridge

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
          if ((expr eq e) || expr.semanticEquals(e)) {
            return true
          }

          // Catalyst may create duplicate ProtobufDataToCatalyst
          // instances for each GetStructField access. Match copies
          // by class + identical input child + identical decode
          // semantics so that
          // analyzeRequiredFields detects all field accesses in one
          // pass, keeping schema projection correct.
          if (allowSemanticReferenceMatch &&
              expr.getClass == e.getClass &&
              expr.children.nonEmpty &&
              e.children.nonEmpty &&
              ((expr.children.head eq e.children.head) ||
                expr.children.head.semanticEquals(
                  e.children.head)) &&
              SparkProtobufCompat.sameDecodeSemantics(expr, e)) {
            return true
          }

          expr match {
            case attr: AttributeReference =>
              protobufOutputExprIds.contains(attr.exprId) ||
                protobufOutputExprId.exists(_ == attr.exprId)
            case _ => false
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = {
          val prunedFieldsMap = buildPrunedFieldsMap()
          val decodedSchema = buildDecodedSchema(prunedFieldsMap)

          GpuFromProtobuf(
            decodedSchema,
            flatFieldNumbers, flatParentIndices,
            flatDepthLevels, flatWireTypes, flatOutputTypeIds, flatEncodings,
            flatIsRepeated, flatIsRequired, flatHasDefaultValue, flatIsOutput, flatDefaultInts,
            flatDefaultFloats, flatDefaultBools, flatDefaultStrings, flatEnumValidValues,
            flatEnumNames, failOnErrors, child)
        }
      }
    )
  }

}
