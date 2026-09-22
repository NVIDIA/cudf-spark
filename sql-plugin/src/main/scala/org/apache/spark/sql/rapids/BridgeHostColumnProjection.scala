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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.RapidsHostColumnBuilder

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.execution.ExecSubqueryExpression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A projection that writes directly to RapidsHostColumnBuilders.
 *
 * CAUTION: the returned projection object should *not* be assumed to be thread-safe.
 */
abstract class BridgeHostColumnProjection {
  def apply(rows: Iterator[InternalRow],
            builders: Array[RapidsHostColumnBuilder]): Unit

  def initialize(partitionIndex: Int): Unit = {}
}

/**
 * Interpreted implementation of BridgeHostColumnProjection.
 * Falls back to direct expression evaluation when code generation fails.
 * Shares the same append logic as GpuCpuBridgeExpression.
 */
class InterpretedBridgeHostColumnProjection(expressions: Seq[Expression])
  extends BridgeHostColumnProjection {
  
  // Create optimized append functions once for each expression
  private val appendFunctions = expressions.map { expr =>
    BridgeHostColumnProjection.createOptimizedAppendFunction(expr.dataType, expr.nullable)
  }
  
  override def apply(rows: Iterator[InternalRow],
    builders: Array[RapidsHostColumnBuilder]): Unit = {
    rows.foreach { row =>
      expressions.zipWithIndex.foreach { case (expr, idx) =>
        val result = expr.eval(row)
        appendFunctions(idx)(result, builders(idx))
      }
    }
  }
}

/**
 * Factory for [[BridgeHostColumnProjection]]. Uses a code-generated implementation and falls
 * back to [[InterpretedBridgeHostColumnProjection]] if code generation fails.
 */
object BridgeHostColumnProjection
  extends CodeGeneratorWithInterpretedFallback[Seq[Expression], BridgeHostColumnProjection] {

  override protected def createCodeGeneratedObject(
      in: Seq[Expression]): BridgeHostColumnProjection = {
    // Let any codegen exception propagate; CodeGeneratorWithInterpretedFallback catches it
    // and calls createInterpretedObject.
    GenerateBridgeHostColumnProjection.generate(in, SQLConf.get.subexpressionEliminationEnabled)
  }

  override protected def createInterpretedObject(
      in: Seq[Expression]): BridgeHostColumnProjection = {
    new InterpretedBridgeHostColumnProjection(in)
  }

  /**
   * Describes how to append a single non-null scalar value to a RapidsHostColumnBuilder.
   * Single source of truth for scalar type dispatch, shared by both the interpreted append
   * closures (createOptimizedAppendFunction) and the generated code
   * (GenerateBridgeHostColumnProjection.generateBuilderAppendCode) so the two cannot drift.
   *
   * @param interp      appends a known-non-null boxed value at runtime (interpreted path)
   * @param codegen     given an already-typed value var and a builder ref, returns the Java
   *                    statement appending the known-non-null value (codegen path)
   * @param objectTyped true if the value is a reference type that may itself be null
   *                    (String/Binary/Decimal), which codegen must also guard on `value == null`
   */
  case class ScalarAppender(
      interp: (Any, RapidsHostColumnBuilder) => Unit,
      codegen: (String, String) => String,
      objectTyped: Boolean)

  /**
   * Returns the scalar appender for `dataType`, or None for NullType and nested types, which
   * each path handles on its own (codegen: recursive direct-to-builder; interpreted:
   * GpuRowToColumnConverter).
   */
  def scalarAppender(dataType: DataType): Option[ScalarAppender] = dataType match {
    case BooleanType => Some(ScalarAppender(
      (v, b) => b.append(v.asInstanceOf[Boolean]), (vv, br) => s"$br.append($vv);", false))
    case ByteType => Some(ScalarAppender(
      (v, b) => b.append(v.asInstanceOf[Byte]), (vv, br) => s"$br.append($vv);", false))
    case ShortType => Some(ScalarAppender(
      (v, b) => b.append(v.asInstanceOf[Short]), (vv, br) => s"$br.append($vv);", false))
    case IntegerType | DateType | _: YearMonthIntervalType => Some(ScalarAppender(
      (v, b) => b.append(v.asInstanceOf[Int]), (vv, br) => s"$br.append($vv);", false))
    case LongType | TimestampType | _: DayTimeIntervalType => Some(ScalarAppender(
      (v, b) => b.append(v.asInstanceOf[Long]), (vv, br) => s"$br.append($vv);", false))
    case FloatType => Some(ScalarAppender(
      (v, b) => b.append(v.asInstanceOf[Float]), (vv, br) => s"$br.append($vv);", false))
    case DoubleType => Some(ScalarAppender(
      (v, b) => b.append(v.asInstanceOf[Double]), (vv, br) => s"$br.append($vv);", false))
    case StringType => Some(ScalarAppender(
      (v, b) => b.appendUTF8String(v.asInstanceOf[UTF8String].getBytes),
      (vv, br) => s"$br.appendUTF8String($vv.getBytes());", true))
    case BinaryType => Some(ScalarAppender(
      (v, b) => b.appendByteList(v.asInstanceOf[Array[Byte]]),
      (vv, br) => s"$br.appendByteList($vv);", true))
    case _: DecimalType => Some(ScalarAppender(
      (v, b) => b.append(v.asInstanceOf[Decimal].toJavaBigDecimal),
      (vv, br) => s"$br.append($vv.toJavaBigDecimal());", true))
    case _ => None
  }

  /**
   * Creates an optimized append function for the specific data type and nullability.
   * Shared by InterpretedBridgeHostColumnProjection and GpuCpuBridgeExpression's interpreted path.
   */
  def createOptimizedAppendFunction(dataType: DataType,
      nullable: Boolean): (Any, RapidsHostColumnBuilder) => Unit = {
    import com.nvidia.spark.rapids.GpuRowToColumnConverter
    import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow

    scalarAppender(dataType) match {
      case Some(sa) =>
        if (nullable) {
          (value: Any, builder: RapidsHostColumnBuilder) =>
            if (value == null) builder.appendNull() else sa.interp(value, builder)
        } else {
          (value: Any, builder: RapidsHostColumnBuilder) => sa.interp(value, builder)
        }
      case None =>
        // Complex / NullType / unsupported types: fall back to GpuRowToColumnConverter.
        val converter = GpuRowToColumnConverter.getConverterForType(dataType, nullable)
        val resultRow = new SpecificInternalRow(Array(dataType))
        (value: Any, builder: RapidsHostColumnBuilder) => {
          resultRow.update(0, value)
          converter.append(resultRow, 0, builder)
        }
    }
  }

  /**
   * Returns a projection for the given StructType.
   *
   * CAUTION: the returned projection object is *not* thread-safe.
   */
  def create(schema: StructType): BridgeHostColumnProjection = create(schema.fields.map(_.dataType))

  /**
   * Returns a projection for the given Array of DataTypes.
   *
   * CAUTION: the returned projection object is *not* thread-safe.
   */
  def create(fields: Array[DataType]): BridgeHostColumnProjection = {
    create(fields.zipWithIndex.map(x => BoundReference(x._2, x._1, true)))
  }

  private def bridgeSerializer = Option(SparkEnv.get)
      .map(_.closureSerializer.newInstance())
      .getOrElse(new JavaSerializer(new SparkConf()).newInstance())

  private def cloneFunction(function: AnyRef): AnyRef =
    org.apache.spark.util.Utils.clone[AnyRef](function, bridgeSerializer)

  private def containsSubquery(arg: Any): Boolean = arg match {
    case _: ExecSubqueryExpression => true
    case e: Expression => e.productIterator.exists(containsSubquery)
    case Some(value) => containsSubquery(value)
    case _: DataType => false
    case s: Stream[_] => s.exists(containsSubquery)
    case s: Seq[_] => s.exists(containsSubquery)
    case m: Map[_, _] => m.exists { case (key, value) =>
      containsSubquery(key) || containsSubquery(value)
    }
    case (left, right) => containsSubquery(left) || containsSubquery(right)
    case _ => false
  }

  private def cloneExpressionPreservingSubqueries(expr: Expression): Expression = expr match {
    case subquery: ExecSubqueryExpression => subquery
    case _ if !containsSubquery(expr) => expr.clone()
    case _ =>
      def cloneAny(arg: Any): Any = arg match {
        case subquery: ExecSubqueryExpression => subquery
        case e: Expression => cloneExpressionPreservingSubqueries(e)
        case dt: DataType => dt
        case Some(value) => Some(cloneAny(value))
        case s: Stream[_] => s.map(cloneAny).force
        case s: Seq[_] => s.map(cloneAny)
        case m: Map[_, _] => m.map { case (key, value) => cloneAny(key) -> cloneAny(value) }
        case (left, right) => (cloneAny(left), cloneAny(right))
        case other => other
      }

      val copiedArgs = expr.productIterator.map(arg => cloneAny(arg).asInstanceOf[AnyRef]).toArray
      expr.makeCopy(copiedArgs)
  }

  private def refreshLambdaVariables(expr: Expression): Expression = {
    val variables = scala.collection.mutable.HashMap.empty[ExprId, NamedLambdaVariable]
    expr.transformUp {
      case variable: NamedLambdaVariable =>
        variables.getOrElseUpdate(variable.exprId,
          variable.newInstance().asInstanceOf[NamedLambdaVariable])
    }
  }

  private def cloneForBridge(expr: Expression): Expression = {
    refreshLambdaVariables(cloneExpressionPreservingSubqueries(expr)).transformUp {
      case udf: ScalaUDF => udf.copy(function = cloneFunction(udf.function))
    }
  }

  /**
   * Returns a projection for the given sequence of bound Expressions.
   */
  def create(exprs: Seq[Expression]): BridgeHostColumnProjection = {
    // Each projection instance owns its expression tree. Some Spark expressions keep transient
    // interpreter caches even when they are not marked stateful, and ScalaUDF carries a user
    // function object that must not be shared by worker-thread-local projections.
    createObject(exprs.map(cloneForBridge))
  }

  def create(expr: Expression): BridgeHostColumnProjection = create(Seq(expr))

  /**
   * Returns a projection for the given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): BridgeHostColumnProjection = {
    create(bindReferences(exprs, inputSchema))
  }
}

/**
 * Code generator for [[BridgeHostColumnProjection]]. Adapted from Spark's
 * `GenerateUnsafeProjection`: instead of packing each row into an `UnsafeRow`, it generates code
 * that evaluates the expressions and appends each result directly into a per-column
 * [[RapidsHostColumnBuilder]], so the output can be moved straight to the GPU.
 *
 * Because this mirrors Spark's generator, keep it aligned with the Spark version being built
 * against when touching the codegen template or supported-type checks.
 */
object GenerateBridgeHostColumnProjection extends
  CodeGenerator[Seq[Expression], BridgeHostColumnProjection] {

  case class Schema(dataType: DataType, nullable: Boolean)

  /**
   * Check if we can generate efficient code for this data type.
   * Returns false for types that would require fallback to GpuRowToColumnConverter in codegen.
   * This allows fast-fail before attempting expensive code generation.
   */
  private def canSupportDirectCodegen(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | DateType | 
           LongType | TimestampType | FloatType | DoubleType | StringType |
           BinaryType | _: DecimalType | NullType => true
      case ArrayType(elementType, _) => canSupportDirectCodegen(elementType)
      case st: StructType => st.fields.forall(f => canSupportDirectCodegen(f.dataType))
      case MapType(kt, vt, _) => canSupportDirectCodegen(kt) && canSupportDirectCodegen(vt)
      case _: YearMonthIntervalType => true
      case _: DayTimeIntervalType => true
      case _ => false
    }
  }

  /** Returns true iff we support this data type. */
  def canSupport(dataType: DataType): Boolean = UserDefinedType.sqlType(dataType) match {
    case NullType => true
    case _: AtomicType => true
    case t: StructType => t.forall(field => canSupport(field.dataType))
    case t: ArrayType if canSupport(t.elementType) => true
    case MapType(kt, vt, _) if canSupport(kt) && canSupport(vt) => true
    case _ => false
  }

  /**
   * Generate code to append a value directly to a RapidsHostColumnBuilder.
   * Generates direct append code without using converters.
   */
  private def generateBuilderAppendCode(
      ctx: CodegenContext,
      dataType: DataType,
      nullable: Boolean,
      valueVar: String,
      isNullVar: String,
      builderRef: String): String = {
    
    BridgeHostColumnProjection.scalarAppender(dataType) match {
      case Some(sa) =>
        if (nullable) {
          val pred = if (sa.objectTyped) s"$isNullVar || $valueVar == null" else isNullVar
          s"""
             |if ($pred) {
             |  $builderRef.appendNull();
             |} else {
             |  ${sa.codegen(valueVar, builderRef)}
             |}
           """.stripMargin
        } else {
          sa.codegen(valueVar, builderRef)
        }

      case None =>
        dataType match {
          case NullType =>
            s"$builderRef.appendNull();"

          case ArrayType(elementType, containsNull) =>
            generateArrayAppendCode(ctx, elementType, containsNull, nullable, valueVar,
              isNullVar, builderRef)

          case structType: StructType =>
            generateStructAppendCode(ctx, structType, nullable, valueVar, isNullVar, builderRef)

          case MapType(keyType, valueType, valueContainsNull) =>
            generateMapAppendCode(ctx, keyType, valueType, valueContainsNull, nullable,
              valueVar, isNullVar, builderRef)

          case _ =>
            // This should never be reached due to canSupportDirectCodegen check
            throw new UnsupportedOperationException(
              s"Cannot generate direct append code for unsupported type: $dataType. " +
              s"`canSupportDirectCodegen` check should have prevented this.")
        }
    }
  }

  private def generateArrayAppendCode(
      ctx: CodegenContext,
      elementType: DataType,
      containsNull: Boolean,
      nullable: Boolean,
      valueVar: String,
      isNullVar: String,
      builderRef: String): String = {
    
    val arrayVar = ctx.freshName("array")
    val numElementsVar = ctx.freshName("numElements")
    val iVar = ctx.freshName("i")
    val childBuilderVar = ctx.freshName("childBuilder")
    val elementVar = ctx.freshName("element")
    val elementIsNullVar = ctx.freshName("elementIsNull")
    
    // Generate code to append each element
    // generateBuilderAppendCode now throws for unsupported types instead of returning null
    val elementAppendCode = generateBuilderAppendCode(ctx, elementType, containsNull,
      elementVar, elementIsNullVar, childBuilderVar)
    
    val loopBody = s"""
       |boolean $elementIsNullVar = $arrayVar.isNullAt($iVar);
       |${CodeGenerator.javaType(elementType)} $elementVar = 
       |  ${CodeGenerator.getValue(arrayVar, elementType, iVar)};
       |$elementAppendCode
     """.stripMargin
    
    val nullCheck = if (nullable) {
      s"""
         |if ($isNullVar || $valueVar == null) {
         |  $builderRef.appendNull();
         |} else {
       """.stripMargin
    } else {
      ""
    }
    
    val closeNullCheck = if (nullable) "}" else ""
    
    s"""
       |$nullCheck
       |  org.apache.spark.sql.catalyst.util.ArrayData $arrayVar = $valueVar;
       |  int $numElementsVar = $arrayVar.numElements();
       |  ${classOf[RapidsHostColumnBuilder].getName} $childBuilderVar = $builderRef.getChild(0);
       |  for (int $iVar = 0; $iVar < $numElementsVar; $iVar++) {
       |    $loopBody
       |  }
       |  $builderRef.endList();
       |$closeNullCheck
     """.stripMargin
  }
  
  private def generateStructAppendCode(
      ctx: CodegenContext,
      structType: StructType,
      nullable: Boolean,
      valueVar: String,
      isNullVar: String,
      builderRef: String): String = {
    
    val structVar = ctx.freshName("struct")
    
    // Generate code for each field
    // generateBuilderAppendCode now throws for unsupported types instead of returning null
    val fieldAppends = structType.fields.zipWithIndex.map { case (field, idx) =>
      val fieldBuilderVar = ctx.freshName(s"fieldBuilder$idx")
      val fieldValueVar = ctx.freshName(s"fieldValue$idx")
      val fieldIsNullVar = ctx.freshName(s"fieldIsNull$idx")
      
      val fieldAppendCode = generateBuilderAppendCode(ctx, field.dataType, field.nullable,
        fieldValueVar, fieldIsNullVar, fieldBuilderVar)
      
      s"""
         |${classOf[RapidsHostColumnBuilder].getName} $fieldBuilderVar = 
         |  $builderRef.getChild($idx);
         |boolean $fieldIsNullVar = $structVar.isNullAt($idx);
         |${CodeGenerator.javaType(field.dataType)} $fieldValueVar = 
         |  ${CodeGenerator.getValue(structVar, field.dataType, idx.toString)};
         |$fieldAppendCode
       """.stripMargin
    }.mkString("\n")
    
    val nullCheck = if (nullable) {
      s"""
         |if ($isNullVar || $valueVar == null) {
         |  $builderRef.appendNull();
         |} else {
       """.stripMargin
    } else {
      ""
    }
    
    val closeNullCheck = if (nullable) "}" else ""
    
    s"""
       |$nullCheck
       |  org.apache.spark.sql.catalyst.InternalRow $structVar = $valueVar;
       |  $fieldAppends
       |  $builderRef.endStruct();
       |$closeNullCheck
     """.stripMargin
  }
  
  private def generateMapAppendCode(
      ctx: CodegenContext,
      keyType: DataType,
      valueType: DataType,
      valueContainsNull: Boolean,
      nullable: Boolean,
      valueVar: String,
      isNullVar: String,
      builderRef: String): String = {
    
    val mapVar = ctx.freshName("map")
    val numElementsVar = ctx.freshName("numElements")
    val iVar = ctx.freshName("i")
    val keysVar = ctx.freshName("keys")
    val valuesVar = ctx.freshName("values")
    val structBuilderVar = ctx.freshName("structBuilder")
    val keyBuilderVar = ctx.freshName("keyBuilder")
    val valueBuilderVar = ctx.freshName("valueBuilder")
    val keyVar = ctx.freshName("key")
    val valueElemVar = ctx.freshName("valueElem")
    val keyIsNullVar = ctx.freshName("keyIsNull")
    val valueIsNullVar = ctx.freshName("valueIsNull")
    
    // Generate code to append key
    val keyAppendCode = generateBuilderAppendCode(ctx, keyType, false, // keys are never null
      keyVar, keyIsNullVar, keyBuilderVar)
    
    // Generate code to append value
    val valueAppendCode = generateBuilderAppendCode(ctx, valueType, valueContainsNull,
      valueElemVar, valueIsNullVar, valueBuilderVar)
    
    
    val loopBody = s"""
       |boolean $keyIsNullVar = $keysVar.isNullAt($iVar);
       |${CodeGenerator.javaType(keyType)} $keyVar = 
       |  ${CodeGenerator.getValue(keysVar, keyType, iVar)};
       |$keyAppendCode
       |boolean $valueIsNullVar = $valuesVar.isNullAt($iVar);
       |${CodeGenerator.javaType(valueType)} $valueElemVar = 
       |  ${CodeGenerator.getValue(valuesVar, valueType, iVar)};
       |$valueAppendCode
       |$structBuilderVar.endStruct();
     """.stripMargin
    
    val nullCheck = if (nullable) {
      s"""
         |if ($isNullVar || $valueVar == null) {
         |  $builderRef.appendNull();
         |} else {
       """.stripMargin
    } else {
      ""
    }
    
    val closeNullCheck = if (nullable) "}" else ""
    
    s"""
       |$nullCheck
       |  org.apache.spark.sql.catalyst.util.MapData $mapVar = $valueVar;
       |  int $numElementsVar = $mapVar.numElements();
       |  org.apache.spark.sql.catalyst.util.ArrayData $keysVar = $mapVar.keyArray();
       |  org.apache.spark.sql.catalyst.util.ArrayData $valuesVar = $mapVar.valueArray();
       |  ${classOf[RapidsHostColumnBuilder].getName} $structBuilderVar = $builderRef.getChild(0);
       |  ${classOf[RapidsHostColumnBuilder].getName} $keyBuilderVar = 
       |    $structBuilderVar.getChild(0);
       |  ${classOf[RapidsHostColumnBuilder].getName} $valueBuilderVar = 
       |    $structBuilderVar.getChild(1);
       |  for (int $iVar = 0; $iVar < $numElementsVar; $iVar++) {
       |    $loopBody
       |  }
       |  $builderRef.endList();
       |$closeNullCheck
     """.stripMargin
  }

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    bindReferences(in, inputSchema)

  def generate(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean): BridgeHostColumnProjection = {
    create(canonicalize(expressions), subexpressionEliminationEnabled)
  }

  protected def create(references: Seq[Expression]): BridgeHostColumnProjection = {
    create(references, subexpressionEliminationEnabled = false)
  }

  private def create(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean): BridgeHostColumnProjection = {
    // Check upfront if we can support all types with direct codegen
    // This provides a fast-fail mechanism before attempting expensive codegen
    val unsupportedTypes = expressions.map(_.dataType).filterNot(canSupportDirectCodegen)
    if (unsupportedTypes.nonEmpty) {
      throw new UnsupportedOperationException(
        s"Cannot generate code for types: ${unsupportedTypes.mkString(", ")}. " +
        "Falling back to interpreted evaluation.")
    }
    
    val ctx = newCodeGenContext()

    // Generate code to evaluate all expressions with subexpression elimination
    val exprEvals = ctx.generateExpressions(expressions, subexpressionEliminationEnabled)
    
    // Evaluate all subexpressions once
    val evalSubexpr = ctx.subexprFunctionsCode
    
    // Generate append code for each expression
    val appendCodes = expressions.zip(exprEvals).zipWithIndex.map { 
      case ((expr, exprEval), idx) =>
        // Generate builder append code with fallback to GpuRowToColumnConverter
        val directAppendCode = generateBuilderAppendCode(ctx,
          expr.dataType, expr.nullable, exprEval.value.toString, 
          exprEval.isNull.toString, s"builders[$idx]")
        
        s"""
           |${exprEval.code}
           |$directAppendCode
         """.stripMargin
    }
    
    val processRowBody = Seq(
      s"$evalSubexpr",
      appendCodes.mkString("\n")
    )
    
    // Split into multiple methods if needed to avoid 64KB method size limit
    val loopBody = ctx.splitExpressions(
      expressions = processRowBody,
      funcName = "processRow",
      arguments = Seq(
        "org.apache.spark.sql.catalyst.InternalRow" -> ctx.INPUT_ROW,
        s"${classOf[RapidsHostColumnBuilder].getName}[]" -> "builders"
      )
    )

    val parentClassName = classOf[BridgeHostColumnProjection].getName
    val codeBody =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificBridgeHostColumnProjection(references);
         |}
         |
         |class SpecificBridgeHostColumnProjection extends $parentClassName {
         |
         |  private Object[] references;
         |  ${ctx.declareMutableStates()}
         |
         |  public SpecificBridgeHostColumnProjection(Object[] references) {
         |    this.references = references;
         |    ${ctx.initMutableStates()}
         |  }
         |
         |  public void initialize(int partitionIndex) {
         |    ${ctx.initPartition()}
         |  }
         |
         |  public void apply(scala.collection.Iterator<InternalRow> rows,
         |    ${classOf[RapidsHostColumnBuilder].getName}[] builders) {
         |    while(rows.hasNext()) {
         |      InternalRow ${ctx.INPUT_ROW} = (InternalRow) rows.next();
         |      $loopBody
         |    }
         |  }
         |
         |  ${ctx.declareAddedFunctions()}
         |}
       """.stripMargin

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BridgeHostColumnProjection]
  }
}
