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
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import java.lang.reflect.Method
import java.nio.file.{Files, Paths}

import scala.util.Try

import com.nvidia.spark.rapids.ShimReflectionUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.rapids.protobuf._

private[shims] object SparkProtobufCompat extends Logging {
  private[this] val sparkProtobufUtilsObjectClassName =
    "org.apache.spark.sql.protobuf.utils.ProtobufUtils$"

  val SupportedOptions: Set[String] = Set("enums.as.ints", "mode")

  def extractExprInfo(e: Expression): Either[String, ProtobufExprInfo] = {
    for {
      messageName <- reflectMessageName(e)
      options <- reflectOptions(e)
      descriptorSource <- reflectDescriptorSource(e)
    } yield ProtobufExprInfo(messageName, descriptorSource, options)
  }

  def sameDecodeSemantics(left: Expression, right: Expression): Boolean = {
    (extractExprInfo(left), extractExprInfo(right)) match {
      case (Right(leftInfo), Right(rightInfo)) => leftInfo == rightInfo
      case _ => false
    }
  }

  def parsePlannerOptions(
      options: Map[String, String]): Either[String, ProtobufPlannerOptions] = {
    val mode = options.getOrElse("mode", "FAILFAST")
    if (mode.equalsIgnoreCase("DROPMALFORMED")) {
      Left("from_protobuf DROPMALFORMED mode is not supported on GPU")
    } else {
      val enumsAsInts = Try(options.getOrElse("enums.as.ints", "false").toBoolean)
        .toEither
        .left
        .map { _ =>
          "Invalid value for from_protobuf option 'enums.as.ints': " +
            s"'${options.getOrElse("enums.as.ints", "")}' (expected true/false)"
        }
      enumsAsInts.map(v =>
        ProtobufPlannerOptions(
          enumsAsInts = v,
          failOnErrors = mode.equalsIgnoreCase("FAILFAST")))
    }
  }

  def unsupportedOptions(options: Map[String, String]): Seq[String] =
    options.keys.filterNot(SupportedOptions.contains).toSeq.sorted

  def isGpuSupportedProtoSyntax(syntax: String): Boolean =
    syntax.nonEmpty && syntax != "null" && syntax != "PROTO3" && syntax != "EDITIONS"

  def resolveMessageDescriptor(
      exprInfo: ProtobufExprInfo): Either[String, ProtobufMessageDescriptor] = {
    Try(buildMessageDescriptor(exprInfo.messageName, exprInfo.descriptorSource))
      .toEither
      .left
      .map { t =>
        s"Failed to resolve protobuf descriptor for message '${exprInfo.messageName}': " +
          s"${t.getMessage}"
      }
      .map(new ReflectiveMessageDescriptor(_))
  }

  private def reflectMessageName(e: Expression): Either[String, String] =
    Try(PbReflect.invoke0[String](e, "messageName")).toEither.left.map { t =>
      s"Cannot read from_protobuf messageName via reflection: ${t.getMessage}"
    }

  private def reflectOptions(e: Expression): Either[String, Map[String, String]] = {
    Try(PbReflect.invoke0[scala.collection.Map[String, String]](e, "options"))
      .map(_.toMap)
      .toEither.left.map { _ =>
        "Cannot read from_protobuf options via reflection; falling back to CPU"
      }
  }

  private def reflectDescriptorSource(e: Expression): Either[String, ProtobufDescriptorSource] = {
    reflectDescFilePath(e).map(ProtobufDescriptorSource.DescriptorPath).orElse(
      reflectDescriptorBytes(e).map(ProtobufDescriptorSource.DescriptorBytes)).toRight(
      "from_protobuf requires a descriptor set (descFilePath or binaryFileDescriptorSet)")
  }

  private def reflectDescFilePath(e: Expression): Option[String] =
    Try(PbReflect.invoke0[Option[String]](e, "descFilePath")).toOption.flatten

  private def reflectDescriptorBytes(e: Expression): Option[Array[Byte]] = {
    val spark35Result = Try(PbReflect.invoke0[Option[Array[Byte]]](e, "binaryFileDescriptorSet"))
      .toOption.flatten
    spark35Result.orElse {
      val direct = Try(PbReflect.invoke0[Array[Byte]](e, "binaryDescriptorSet")).toOption
      direct.orElse {
        Try(PbReflect.invoke0[Option[Array[Byte]]](e, "binaryDescriptorSet")).toOption.flatten
      }
    }
  }

  private def buildMessageDescriptor(
      messageName: String,
      descriptorSource: ProtobufDescriptorSource): AnyRef = {
    val cls = ShimReflectionUtils.loadClass(sparkProtobufUtilsObjectClassName)
    val module = cls.getField("MODULE$").get(null)
    val buildMethod = cls.getMethod("buildDescriptor", classOf[String], classOf[scala.Option[_]])

    invokeBuildDescriptor(
      buildMethod,
      module,
      messageName,
      descriptorSource,
      filePath => Files.readAllBytes(Paths.get(filePath)))
  }

  private[shims] def invokeBuildDescriptor(
      buildMethod: Method,
      module: AnyRef,
      messageName: String,
      descriptorSource: ProtobufDescriptorSource,
      readDescriptorFile: String => Array[Byte]): AnyRef = {
    descriptorSource match {
      case ProtobufDescriptorSource.DescriptorBytes(bytes) =>
        buildMethod.invoke(module, messageName, Some(bytes)).asInstanceOf[AnyRef]
      case ProtobufDescriptorSource.DescriptorPath(filePath) =>
        try {
          buildMethod.invoke(module, messageName, Some(filePath)).asInstanceOf[AnyRef]
        } catch {
          case ex: java.lang.reflect.InvocationTargetException =>
            val cause = ex.getCause
            // Spark 3.5+ changed the descriptor payload from Option[String] to
            // Option[Array[Byte]] while keeping the same erased JVM signature.
            // Retry with file contents when the path-based invocation clearly hit that
            // binary-descriptor variant.
            if (cause != null && (cause.isInstanceOf[ClassCastException] ||
                cause.isInstanceOf[MatchError])) {
              Try {
                buildMethod.invoke(
                  module, messageName, Some(readDescriptorFile(filePath))).asInstanceOf[AnyRef]
              }.recoverWith { case retryEx =>
                val wrapped = buildDescriptorRetryFailure(cause, retryEx)
                wrapped.addSuppressed(ex)
                scala.util.Failure(wrapped)
              }.get
            } else {
              throw ex
            }
        }
    }
  }

  private def buildDescriptorRetryFailure(
      originalCause: Throwable,
      retryFailure: Throwable): RuntimeException = {
    val retryCause = unwrapInvocationFailure(retryFailure)
    new RuntimeException(
      s"Spark 3.5+ descriptor bytes retry failed after initial path invocation error " +
        s"(${describeThrowable(originalCause)}); retry error (${describeThrowable(retryCause)})",
      retryCause)
  }

  private def unwrapInvocationFailure(t: Throwable): Throwable = t match {
    case ex: java.lang.reflect.InvocationTargetException if ex.getCause != null => ex.getCause
    case other => other
  }

  private def describeThrowable(t: Throwable): String = {
    val suffix = Option(t.getMessage).filter(_.nonEmpty).map(msg => s": $msg").getOrElse("")
    s"${t.getClass.getSimpleName}$suffix"
  }

  private def typeName(t: AnyRef): String =
    if (t == null) "" else Try(PbReflect.invoke0[String](t, "name")).getOrElse(t.toString)

  private final class ReflectiveMessageDescriptor(raw: AnyRef) extends ProtobufMessageDescriptor {
    override lazy val syntax: String = PbReflect.getFileSyntax(raw, typeName)

    override def findField(name: String): Option[ProtobufFieldDescriptor] =
      Option(PbReflect.findFieldByName(raw, name)).map(new ReflectiveFieldDescriptor(_))
  }

  private final class ReflectiveFieldDescriptor(raw: AnyRef) extends ProtobufFieldDescriptor {
    override lazy val name: String = PbReflect.invoke0[String](raw, "getName")
    override lazy val fieldNumber: Int = PbReflect.getFieldNumber(raw)
    override lazy val protoTypeName: String = typeName(PbReflect.getFieldType(raw))
    override lazy val isRepeated: Boolean = PbReflect.isRepeated(raw)
    override lazy val isRequired: Boolean = PbReflect.isRequired(raw)
    override lazy val isInOneof: Boolean = PbReflect.getContainingOneof(raw) != null
    override lazy val enumMetadata: Option[ProtobufEnumMetadata] =
      if (protoTypeName == "ENUM") {
        Some(ProtobufEnumMetadata(PbReflect.getEnumValues(PbReflect.getEnumType(raw))))
      } else {
        None
      }
    override lazy val defaultValueResult: Either[String, Option[ProtobufDefaultValue]] =
      Try {
        if (PbReflect.hasDefaultValue(raw)) {
          PbReflect.getDefaultValue(raw) match {
            case Some(default) =>
              toDefaultValue(default, protoTypeName, enumMetadata).map(Some(_))
            case None =>
              Right(None)
          }
        } else {
          Right(None)
        }
      }.toEither.left.map { t =>
        s"Failed to read protobuf default value for field '$name': ${t.getMessage}"
      }.flatMap(identity)
    override lazy val messageDescriptor: Option[ProtobufMessageDescriptor] =
      if (protoTypeName == "MESSAGE") {
        Some(new ReflectiveMessageDescriptor(PbReflect.getMessageType(raw)))
      } else {
        None
      }
  }

  private def toDefaultValue(
      rawDefault: AnyRef,
      protoTypeName: String,
      enumMetadata: Option[ProtobufEnumMetadata]): Either[String, ProtobufDefaultValue] =
    protoTypeName match {
    case "BOOL" =>
      Right(ProtobufDefaultValue.BoolValue(
        rawDefault.asInstanceOf[java.lang.Boolean].booleanValue()))
    case "FLOAT" =>
      Right(ProtobufDefaultValue.FloatValue(
        rawDefault.asInstanceOf[java.lang.Float].floatValue()))
    case "DOUBLE" =>
      Right(ProtobufDefaultValue.DoubleValue(
        rawDefault.asInstanceOf[java.lang.Double].doubleValue()))
    case "STRING" =>
      Right(ProtobufDefaultValue.StringValue(
        if (rawDefault == null) null else rawDefault.toString))
    case "BYTES" =>
      Right(ProtobufDefaultValue.BinaryValue(extractBytes(rawDefault)))
    case "ENUM" =>
      val number = extractNumber(rawDefault).intValue()
      Right(enumMetadata.map(_.enumDefault(number))
        .getOrElse(ProtobufDefaultValue.EnumValue(number, rawDefault.toString)))
    case "INT32" | "UINT32" | "SINT32" | "FIXED32" | "SFIXED32" |
         "INT64" | "UINT64" | "SINT64" | "FIXED64" | "SFIXED64" =>
      Right(ProtobufDefaultValue.IntValue(extractNumber(rawDefault).longValue()))
    case other =>
      Left(
        s"Unsupported protobuf default value type '$other' for value ${rawDefault.toString}")
  }

  private def extractNumber(rawDefault: AnyRef): java.lang.Number = rawDefault match {
    case n: java.lang.Number => n
    case ref: AnyRef =>
      Try {
        ref.getClass.getMethod("getNumber").invoke(ref).asInstanceOf[java.lang.Number]
      }.getOrElse {
        throw new IllegalStateException(
          s"Unsupported protobuf numeric default value class: ${ref.getClass.getName}")
      }
  }

  private def extractBytes(rawDefault: AnyRef): Array[Byte] = rawDefault match {
    case bytes: Array[Byte] => bytes
    case ref: AnyRef =>
      Try {
        ref.getClass.getMethod("toByteArray").invoke(ref).asInstanceOf[Array[Byte]]
      }.getOrElse {
        throw new IllegalStateException(
          s"Unsupported protobuf bytes default value class: ${ref.getClass.getName}")
      }
  }

  private object PbReflect {
    private val cache = new java.util.concurrent.ConcurrentHashMap[String, Method]()

    private def protobufJavaVersion: String = Try {
      val rtCls = Class.forName("com.google.protobuf.RuntimeVersion")
      val domain = rtCls.getField("DOMAIN").get(null)
      val major = rtCls.getField("MAJOR").get(null)
      val minor = rtCls.getField("MINOR").get(null)
      val patch = rtCls.getField("PATCH").get(null)
      s"$domain-$major.$minor.$patch"
    }.getOrElse("unknown")

    private def cached(cls: Class[_], name: String, paramTypes: Class[_]*): Method = {
      val key = s"${cls.getName}#$name(${paramTypes.map(_.getName).mkString(",")})"
      cache.computeIfAbsent(key, _ => {
        try {
          cls.getMethod(name, paramTypes: _*)
        } catch {
          case ex: NoSuchMethodException =>
            throw new UnsupportedOperationException(
              s"protobuf-java method not found: ${cls.getSimpleName}.$name " +
                s"(protobuf-java version: $protobufJavaVersion). " +
                s"This may indicate an incompatible protobuf-java library version.",
              ex)
        }
      })
    }

    def invoke0[T](obj: AnyRef, method: String): T =
      cached(obj.getClass, method).invoke(obj).asInstanceOf[T]

    def invoke1[T](obj: AnyRef, method: String, arg0Cls: Class[_], arg0: AnyRef): T =
      cached(obj.getClass, method, arg0Cls).invoke(obj, arg0).asInstanceOf[T]

    def findFieldByName(msgDesc: AnyRef, name: String): AnyRef =
      invoke1[AnyRef](msgDesc, "findFieldByName", classOf[String], name)

    def getFieldNumber(fd: AnyRef): Int =
      invoke0[java.lang.Integer](fd, "getNumber").intValue()

    def getFieldType(fd: AnyRef): AnyRef = invoke0[AnyRef](fd, "getType")

    def isRepeated(fd: AnyRef): Boolean =
      invoke0[java.lang.Boolean](fd, "isRepeated").booleanValue()

    def isRequired(fd: AnyRef): Boolean =
      invoke0[java.lang.Boolean](fd, "isRequired").booleanValue()

    def getContainingOneof(fd: AnyRef): AnyRef = invoke0[AnyRef](fd, "getContainingOneof")

    def hasDefaultValue(fd: AnyRef): Boolean =
      invoke0[java.lang.Boolean](fd, "hasDefaultValue").booleanValue()

    def getDefaultValue(fd: AnyRef): Option[AnyRef] =
      Option(invoke0[AnyRef](fd, "getDefaultValue"))

    def getMessageType(fd: AnyRef): AnyRef = invoke0[AnyRef](fd, "getMessageType")

    def getEnumType(fd: AnyRef): AnyRef = invoke0[AnyRef](fd, "getEnumType")

    def getEnumValues(enumType: AnyRef): Seq[ProtobufEnumValue] = {
      import scala.collection.JavaConverters._
      val values = invoke0[java.util.List[_]](enumType, "getValues")
      values.asScala.map { v =>
        val ev = v.asInstanceOf[AnyRef]
        val num = invoke0[java.lang.Integer](ev, "getNumber").intValue()
        val enumName = invoke0[String](ev, "getName")
        ProtobufEnumValue(num, enumName)
      }.toSeq
    }

    def getFileSyntax(msgDesc: AnyRef, typeNameFn: AnyRef => String): String = Try {
      val fileDesc = invoke0[AnyRef](msgDesc, "getFile")
      val syntaxObj = invoke0[AnyRef](fileDesc, "getSyntax")
      typeNameFn(syntaxObj)
    }.getOrElse("")
  }
}
