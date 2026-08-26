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

import java.lang.invoke.{MethodHandleInfo, SerializedLambda}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
import org.apache.xbean.asm9.{ClassReader, Opcodes, Type}
import org.apache.xbean.asm9.tree._

import org.apache.spark.rdd.{MapPartitionsRDD, NewHadoopRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.{Alias, BoundReference, Expression, KnownNotNull,
  NamedExpression}
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, Invoke}
import org.apache.spark.sql.execution.{ExternalRDDScanExec, SerializeFromObjectExec, SparkPlan}
import org.apache.spark.sql.types.{BinaryType, ObjectType}
import org.apache.spark.storage.StorageLevel

/** Proves a narrow SequenceFile RDD-to-DataFrame conversion without executing its closure. */
object SequenceFileRddReadProof {
  sealed trait SourceColumn
  case object Key extends SourceColumn
  case object Value extends SourceColumn

  sealed trait Result
  /**
   * Relisting paths is not equivalent to preserving the proven source's configuration and splits.
   */
  final case class Proven(
      columns: Seq[SourceColumn],
      sourceRdd: NewHadoopRDD[Any, Any]) extends Result
  final case class Rejected(reason: String) extends Result

  private sealed trait Symbol
  private case object InputPair extends Symbol
  private final case class Writable(source: SourceColumn) extends Symbol
  private final case class Bytes(source: SourceColumn) extends Symbol
  private final case class Length(source: SourceColumn) extends Symbol
  private final case class Binary(source: SourceColumn) extends Symbol
  private case object Zero extends Symbol
  private final case class NewTuple(id: Int) extends Symbol
  private final case class OutputTuple(first: SourceColumn, second: SourceColumn) extends Symbol

  def inspect(plan: SparkPlan): Result = {
    try {
      inspectPlan(plan).fold(Rejected, identity)
    } catch {
      case e: LinkageError => Rejected(s"inspection failed: ${e.getClass.getSimpleName}")
      case NonFatal(e) => Rejected(s"inspection failed: ${e.getClass.getSimpleName}")
    }
  }

  private def inspectPlan(plan: SparkPlan): Either[String, Proven] = plan match {
    case serialize: SerializeFromObjectExec
        if serialize.getClass == classOf[SerializeFromObjectExec] =>
      serialize.child match {
        case scan: ExternalRDDScanExec[_]
            if scan.getClass == classOf[ExternalRDDScanExec[_]] =>
          for {
            serializerColumns <- inspectSerializer(serialize.serializer)
            rddProof <- inspectRdd(scan.rdd)
          } yield {
            val (closureColumns, source) = rddProof
            val columns = serializerColumns.map(closureColumns)
            Proven(columns, source)
          }
        case _: ExternalRDDScanExec[_] => Left("ExternalRDDScanExec subclasses are not supported")
        case other => Left(s"expected ExternalRDDScanExec, found ${other.nodeName}")
      }
    case _: SerializeFromObjectExec => Left("SerializeFromObjectExec subclasses are not supported")
    case other => Left(s"expected SerializeFromObjectExec, found ${other.nodeName}")
  }

  private def inspectSerializer(serializer: Seq[NamedExpression]): Either[String, Seq[Int]] = {
    if (serializer.length != 2) {
      Left(s"expected a two-column serializer, found ${serializer.length} columns")
    } else {
      val columns = serializer.map(serializerOrdinal)
      columns.collectFirst { case Left(reason) => reason } match {
        case Some(reason) => Left(reason)
        case None => Right(columns.collect { case Right(ordinal) => ordinal })
      }
    }
  }

  private def serializerOrdinal(expression: Expression): Either[String, Int] = {
    stripNullChecks(expression).flatMap {
      case invoke: Invoke if invoke.getClass == classOf[Invoke] &&
          invoke.dataType == BinaryType && invoke.arguments.isEmpty &&
          isTupleInput(invoke.targetObject) =>
        invoke.functionName match {
          case "_1" => Right(0)
          case "_2" => Right(1)
          case other => Left(s"unsupported tuple accessor $other")
        }
      case _: Invoke => Left("Invoke subclasses are not supported")
      case other => Left(s"unsupported serializer expression ${other.getClass.getSimpleName}")
    }
  }

  private def stripNullChecks(
      expression: Expression): Either[String, Expression] = expression match {
    case alias: Alias if alias.getClass == classOf[Alias] => stripNullChecks(alias.child)
    case _: Alias => Left("Alias subclasses are not supported")
    case known: KnownNotNull if known.getClass == classOf[KnownNotNull] =>
      stripNullChecks(known.child)
    case _: KnownNotNull => Left("KnownNotNull subclasses are not supported")
    case other => Right(other)
  }

  private def isTupleInput(expression: Expression): Boolean = expression match {
    case known: KnownNotNull if known.getClass == classOf[KnownNotNull] =>
      isTupleInput(known.child)
    case asserted: AssertNotNull if asserted.getClass == classOf[AssertNotNull] =>
      isTupleInput(asserted.child)
    case bound: BoundReference if bound.getClass == classOf[BoundReference] =>
      bound.ordinal == 0 && bound.dataType == ObjectType(classOf[Tuple2[_, _]])
    case _ => false
  }

  private def inspectRdd(
      rdd: RDD[_]): Either[String, (IndexedSeq[SourceColumn], NewHadoopRDD[Any, Any])] = rdd match {
    case mapped: MapPartitionsRDD[_, _]
        if mapped.getClass == classOf[MapPartitionsRDD[_, _]] =>
      for {
        _ <- requireUnmaterialized(mapped, "mapped RDD")
        source <- mapped.prev match {
          case source: NewHadoopRDD[_, _]
              if source.getClass == classOf[NewHadoopRDD[_, _]] =>
            Right(source.asInstanceOf[NewHadoopRDD[Any, Any]])
          case _: NewHadoopRDD[_, _] => Left("NewHadoopRDD subclasses are not supported")
          case null => Left("RDD lineage was cleared")
          case other => Left(s"expected NewHadoopRDD, found ${other.getClass.getSimpleName}")
        }
        _ <- requireUnmaterialized(source, "source RDD")
        _ <- requireBinaryInputFormat(source)
        function <- extractMapFunction(mapped)
        method <- loadLambdaMethod(function)
        columns <- verifyLambda(method)
      } yield (columns, source)
    case _: MapPartitionsRDD[_, _] => Left("MapPartitionsRDD subclasses are not supported")
    case other => Left(s"expected MapPartitionsRDD, found ${other.getClass.getSimpleName}")
  }

  private def requireUnmaterialized(rdd: RDD[_], name: String): Either[String, Unit] = {
    if (rdd.getStorageLevel != StorageLevel.NONE) {
      Left(s"$name is persisted")
    } else if (rdd.checkpointData.nonEmpty) {
      Left(s"$name has checkpoint state")
    } else if (rdd.getResourceProfile != null) {
      Left(s"$name has a resource profile")
    } else {
      Right(())
    }
  }

  private def requireBinaryInputFormat(rdd: NewHadoopRDD[Any, Any]): Either[String, Unit] = {
    val formatField = classOf[NewHadoopRDD[_, _]].getDeclaredFields
      .find(_.getName.endsWith("inputFormatClass"))
    formatField match {
      case Some(field) =>
        field.setAccessible(true)
        if (field.get(rdd) == classOf[SequenceFileAsBinaryInputFormat]) {
          Right(())
        } else {
          Left("NewHadoopRDD does not use SequenceFileAsBinaryInputFormat")
        }
      case None => Left("cannot locate NewHadoopRDD inputFormatClass")
    }
  }

  private def extractMapFunction(mapped: MapPartitionsRDD[_, _]): Either[String, AnyRef] = {
    val field = classOf[MapPartitionsRDD[_, _]].getDeclaredField("f")
    field.setAccessible(true)
    val wrapper = field.get(mapped).asInstanceOf[AnyRef]
    serializedLambda(wrapper).flatMap { lambda =>
      val descriptors = Map(
        "$anonfun$map$2" -> (
          "(Lscala/Function1;Lorg/apache/spark/TaskContext;ILscala/collection/Iterator;)" +
            "Lscala/collection/Iterator;"),
        "$anonfun$map$2$adapted" -> (
          "(Lscala/Function1;Lorg/apache/spark/TaskContext;Ljava/lang/Object;" +
            "Lscala/collection/Iterator;)Lscala/collection/Iterator;"))
      val isSparkMap = lambda.getImplMethodKind == MethodHandleInfo.REF_invokeStatic &&
        lambda.getImplClass == "org/apache/spark/rdd/RDD" &&
        lambda.getCapturingClass == lambda.getImplClass &&
        descriptors.get(lambda.getImplMethodName).contains(lambda.getImplMethodSignature) &&
        lambda.getFunctionalInterfaceClass == "scala/Function3" &&
        lambda.getFunctionalInterfaceMethodName == "apply" &&
        lambda.getFunctionalInterfaceMethodSignature ==
          "(Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;" &&
        lambda.getInstantiatedMethodType ==
          "(Lorg/apache/spark/TaskContext;Ljava/lang/Object;Lscala/collection/Iterator;)" +
            "Lscala/collection/Iterator;"
      if (!isSparkMap || lambda.getCapturedArgCount != 1) {
        Left("MapPartitionsRDD is not the Spark RDD.map wrapper")
      } else {
        lambda.getCapturedArg(0) match {
          case function: Function1[_, _] => Right(function.asInstanceOf[AnyRef])
          case _ => Left("RDD.map wrapper does not capture one Function1")
        }
      }
    }
  }

  private def loadLambdaMethod(function: AnyRef): Either[String, MethodNode] = {
    serializedLambda(function).flatMap { lambda =>
      if (lambda.getImplMethodKind != MethodHandleInfo.REF_invokeStatic) {
        Left("user closure is not a static lambda body")
      } else if (lambda.getCapturingClass != lambda.getImplClass) {
        Left("user closure implementation is in a different class")
      } else if (lambda.getFunctionalInterfaceClass != "scala/Function1" ||
          lambda.getFunctionalInterfaceMethodName != "apply" ||
          lambda.getFunctionalInterfaceMethodSignature !=
            "(Ljava/lang/Object;)Ljava/lang/Object;" ||
          lambda.getInstantiatedMethodType != "(Lscala/Tuple2;)Lscala/Tuple2;") {
        Left("user closure is not a Scala Tuple2 function")
      } else if (lambda.getCapturedArgCount != 0) {
        Left("user closure captures state")
      } else {
        loadClass(function, lambda.getImplClass).flatMap { reader =>
          val classNode = new ClassNode()
          reader.accept(classNode, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES)
          val methods = classNode.methods.toArray.collect {
            case method: MethodNode if method.name == lambda.getImplMethodName &&
                method.desc == lambda.getImplMethodSignature => method
          }
          if (methods.length == 1) {
            Right(methods.head)
          } else {
            Left("cannot uniquely locate the user closure bytecode")
          }
        }
      }
    }
  }

  private def serializedLambda(function: AnyRef): Either[String, SerializedLambda] = {
    val cls = function.getClass
    if (!cls.isSynthetic || !cls.getName.contains("$$Lambda$")) {
      Left(s"${cls.getName} is not a generated JVM lambda")
    } else {
      try {
        val method = cls.getDeclaredMethod("writeReplace")
        method.setAccessible(true)
        method.invoke(function) match {
          case lambda: SerializedLambda => Right(lambda)
          case _ => Left("lambda writeReplace did not return SerializedLambda")
        }
      } catch {
        case NonFatal(_) => Left("cannot serialize generated lambda")
      }
    }
  }

  private def loadClass(
      function: AnyRef,
      internalName: String): Either[String, ClassReader] = {
    val binaryName = internalName.replace('/', '.')
    try {
      val implClass = Class.forName(binaryName, false, function.getClass.getClassLoader)
      val expectedClasses = Seq[Class[_]](
        classOf[Tuple2[_, _]],
        classOf[BytesWritable],
        classOf[java.util.Arrays],
        classOf[MatchError])
      val usesCanonicalClasses = expectedClasses.forall { expected =>
        Class.forName(expected.getName, false, implClass.getClassLoader) eq expected
      }
      if (!usesCanonicalClasses) {
        Left(s"implementation class $binaryName resolves incompatible dependencies")
      } else {
        Option(implClass.getResourceAsStream(s"/$internalName.class")) match {
          case Some(in) =>
            try {
              val reader = new ClassReader(in)
              if (reader.getClassName == internalName) {
                Right(reader)
              } else {
                Left(s"bytecode class does not match $binaryName")
              }
            } finally {
              in.close()
            }
          case None => Left(s"cannot load bytecode for $binaryName")
        }
      }
    } catch {
      case _: LinkageError => Left(s"cannot resolve implementation class $binaryName")
      case NonFatal(_) => Left(s"cannot resolve implementation class $binaryName")
    }
  }

  private def verifyLambda(method: MethodNode): Either[String, IndexedSeq[SourceColumn]] = {
    val argumentTypes = Type.getArgumentTypes(method.desc)
    val forbiddenFlags = Opcodes.ACC_SYNCHRONIZED | Opcodes.ACC_NATIVE | Opcodes.ACC_ABSTRACT
    if ((method.access & Opcodes.ACC_STATIC) == 0 || (method.access & forbiddenFlags) != 0) {
      Left("user closure method is not a plain static method")
    } else if (argumentTypes.length != 1 ||
        argumentTypes.head.getInternalName != "scala/Tuple2" ||
        Type.getReturnType(method.desc).getInternalName != "scala/Tuple2") {
      Left("user closure is not Tuple2 => Tuple2")
    } else if (!method.tryCatchBlocks.isEmpty) {
      Left("user closure contains exception handling")
    } else {
      interpret(method)
    }
  }

  private def interpret(method: MethodNode): Either[String, IndexedSeq[SourceColumn]] = {
    val instructions = method.instructions.toArray
    val positions = instructions.zipWithIndex.toMap
    val locals = Array.fill[Option[Symbol]](method.maxLocals)(None)
    val stack = new ArrayBuffer[Symbol]()
    val nullTargets = ArrayBuffer[Int]()
    locals(0) = Some(InputPair)
    var index = 0
    var nextTupleId = 0
    var result: Either[String, IndexedSeq[SourceColumn]] =
      Left("user closure does not return a proven tuple")
    var done = false

    def fail(reason: String): Unit = {
      result = Left(reason)
      done = true
    }

    def pop(): Option[Symbol] = {
      if (stack.nonEmpty) Some(stack.remove(stack.length - 1)) else None
    }

    def popArgs(count: Int): Option[IndexedSeq[Symbol]] = {
      if (stack.length < count) {
        None
      } else {
        Some((0 until count).map(_ => pop().get).reverse)
      }
    }

    def replaceTuple(id: Int, output: OutputTuple): Unit = {
      stack.indices.foreach { i =>
        if (stack(i) == NewTuple(id)) {
          stack(i) = output
        }
      }
      locals.indices.foreach { i =>
        if (locals(i).contains(NewTuple(id))) {
          locals(i) = Some(output)
        }
      }
    }

    while (index < instructions.length && !done) {
      instructions(index) match {
        case _: LabelNode | _: LineNumberNode | _: FrameNode =>

        case variable: VarInsnNode => variable.getOpcode match {
          case Opcodes.ALOAD | Opcodes.ILOAD =>
            locals.lift(variable.`var`).flatten match {
              case Some(value) => stack += value
              case None => fail(s"read from unknown local ${variable.`var`}")
            }
          case Opcodes.ASTORE | Opcodes.ISTORE =>
            pop() match {
              case Some(value) => locals(variable.`var`) = Some(value)
              case None => fail("stack underflow while storing a local")
            }
          case opcode => fail(s"unsupported local-variable opcode $opcode")
        }

        case instruction: InsnNode => instruction.getOpcode match {
          case Opcodes.ICONST_0 => stack += Zero
          case Opcodes.DUP =>
            if (stack.nonEmpty) stack += stack.last else fail("stack underflow at DUP")
          case Opcodes.ARETURN =>
            pop() match {
              case Some(OutputTuple(first, second)) if stack.isEmpty =>
                val trailing = verifyFailureTail(instructions, index + 1, positions, nullTargets)
                result = trailing.map(_ => IndexedSeq(first, second))
                done = true
              case _ => fail("closure return value is not a proven binary tuple")
            }
          case opcode => fail(s"unsupported instruction opcode $opcode")
        }

        case instruction: IntInsnNode if instruction.operand == 0 => stack += Zero

        case instruction: LdcInsnNode if instruction.cst == Integer.valueOf(0) => stack += Zero

        case instruction: TypeInsnNode => instruction.getOpcode match {
          case Opcodes.CHECKCAST if instruction.desc ==
              "org/apache/hadoop/io/BytesWritable" &&
              stack.lastOption.exists(_.isInstanceOf[Writable]) =>
          case Opcodes.NEW if instruction.desc == "scala/Tuple2" =>
            stack += NewTuple(nextTupleId)
            nextTupleId += 1
          case opcode => fail(s"unsupported type instruction $opcode ${instruction.desc}")
        }

        case instruction: JumpInsnNode if instruction.getOpcode == Opcodes.IFNULL =>
          pop() match {
            case Some(InputPair) => nullTargets += positions(instruction.label)
            case _ => fail("null check is not on the input tuple")
          }

        case instruction: MethodInsnNode =>
          applyMethod(instruction, stack, pop _, popArgs _, replaceTuple) match {
            case Left(reason) => fail(reason)
            case Right(_) =>
          }

        case instruction =>
          fail(s"unsupported bytecode node ${instruction.getClass.getSimpleName}")
      }
      index += 1
    }
    result
  }

  private def applyMethod(
      instruction: MethodInsnNode,
      stack: ArrayBuffer[Symbol],
      pop: () => Option[Symbol],
      popArgs: Int => Option[IndexedSeq[Symbol]],
      replaceTuple: (Int, OutputTuple) => Unit): Either[String, Unit] = {
    (instruction.owner, instruction.name, instruction.desc,
      instruction.getOpcode, instruction.itf) match {
      case ("scala/Tuple2", "_1", "()Ljava/lang/Object;", Opcodes.INVOKEVIRTUAL, false) =>
        unary(pop, InputPair, Writable(Key), stack)
      case ("scala/Tuple2", "_2", "()Ljava/lang/Object;", Opcodes.INVOKEVIRTUAL, false) =>
        unary(pop, InputPair, Writable(Value), stack)
      case ("org/apache/hadoop/io/BytesWritable", "getBytes", "()[B",
          Opcodes.INVOKEVIRTUAL, false) =>
        writableUnary(pop, source => Bytes(source), stack)
      case ("org/apache/hadoop/io/BytesWritable", "getLength", "()I",
          Opcodes.INVOKEVIRTUAL, false) =>
        writableUnary(pop, source => Length(source), stack)
      case ("org/apache/hadoop/io/BytesWritable", "copyBytes", "()[B",
          Opcodes.INVOKEVIRTUAL, false) =>
        writableUnary(pop, source => Binary(source), stack)
      case ("java/util/Arrays", "copyOf", "([BI)[B", Opcodes.INVOKESTATIC, false) =>
        popArgs(2) match {
          case Some(IndexedSeq(Bytes(source), Length(same))) if source == same =>
            stack += Binary(source)
            Right(())
          case _ => Left("Arrays.copyOf does not copy the full writable payload")
        }
      case ("java/util/Arrays", "copyOfRange", "([BII)[B", Opcodes.INVOKESTATIC, false) =>
        popArgs(3) match {
          case Some(IndexedSeq(Bytes(source), Zero, Length(same))) if source == same =>
            stack += Binary(source)
            Right(())
          case _ => Left("Arrays.copyOfRange does not copy the full writable payload")
        }
      case ("scala/Tuple2", "<init>", "(Ljava/lang/Object;Ljava/lang/Object;)V",
          Opcodes.INVOKESPECIAL, false) =>
        (popArgs(2), pop()) match {
          case (Some(IndexedSeq(Binary(first), Binary(second))), Some(NewTuple(id))) =>
            replaceTuple(id, OutputTuple(first, second))
            Right(())
          case _ => Left("Tuple2 is not constructed from two proven binary values")
        }
      case _ =>
        Left(s"unsupported method ${instruction.owner}.${instruction.name}${instruction.desc}")
    }
  }

  private def unary(
      pop: () => Option[Symbol],
      expected: Symbol,
      output: Symbol,
      stack: ArrayBuffer[Symbol]): Either[String, Unit] = pop() match {
    case Some(value) if value == expected =>
      stack += output
      Right(())
    case _ => Left("method receiver does not have a proven source")
  }

  private def writableUnary(
      pop: () => Option[Symbol],
      output: SourceColumn => Symbol,
      stack: ArrayBuffer[Symbol]): Either[String, Unit] = pop() match {
    case Some(Writable(source)) =>
      stack += output(source)
      Right(())
    case _ => Left("BytesWritable method receiver does not have a proven source")
  }

  private def verifyFailureTail(
      instructions: Array[AbstractInsnNode],
      start: Int,
      positions: Map[AbstractInsnNode, Int],
      nullTargets: scala.collection.Iterable[Int]): Either[String, Unit] = {
    if (nullTargets.exists(_ < start)) {
      Left("input null check does not target the failure path")
    } else if (nullTargets.isEmpty) {
      Right(())
    } else {
      var throws = 0
      var invalid: Option[Int] = None
      instructions.drop(start).foreach {
        case instruction: MethodInsnNode if instruction.owner == "scala/MatchError" &&
            instruction.name == "<init>" && instruction.desc == "(Ljava/lang/Object;)V" &&
            instruction.getOpcode == Opcodes.INVOKESPECIAL && !instruction.itf =>
        case instruction: TypeInsnNode if instruction.getOpcode == Opcodes.NEW &&
            instruction.desc == "scala/MatchError" =>
        case instruction: JumpInsnNode if instruction.getOpcode == Opcodes.GOTO &&
            positions(instruction.label) >= start =>
        case instruction: InsnNode if instruction.getOpcode == Opcodes.ATHROW => throws += 1
        case instruction: VarInsnNode if instruction.getOpcode == Opcodes.ALOAD =>
        case instruction: InsnNode if instruction.getOpcode == Opcodes.DUP =>
        case _: LabelNode | _: LineNumberNode | _: FrameNode =>
        case instruction if invalid.isEmpty => invalid = Some(instruction.getOpcode)
        case _ =>
      }
      invalid match {
        case Some(opcode) => Left(s"unsupported instruction $opcode in pattern-match failure path")
        case None if throws == 1 => Right(())
        case None => Left("pattern-match failure path is not a single throw")
      }
    }
  }
}
