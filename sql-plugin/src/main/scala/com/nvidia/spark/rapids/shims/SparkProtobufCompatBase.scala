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

package com.nvidia.spark.rapids.shims

import java.lang.reflect.Method
import java.nio.file.{Files, Paths}
import java.util.Locale

import scala.util.Try

import com.nvidia.spark.rapids.ShimReflectionUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.rapids.protobuf._

/**
 * Provides protobuf schema metadata through a common interface across Spark versions.
 * Subclasses handle version-specific APIs; reflection keeps spark-protobuf an optional dependency.
 */
private[shims] abstract class SparkProtobufCompatBase extends Logging {
  private[this] val sparkProtobufUtilsObjectClassName =
    "org.apache.spark.sql.protobuf.utils.ProtobufUtils$"

  /** Reads from_protobuf arguments; Left explains a missing or incompatible Spark API. */
  def extractExprInfo(e: Expression): Either[String, ProtobufExprInfo] = {
    for {
      messageName <- reflectMessageName(e)
      options <- reflectOptions(e)
      descriptorSource <- reflectDescriptorSource(e)
    } yield ProtobufExprInfo(messageName, descriptorSource, options)
  }

  /** Resolves the schema or returns a failure reason; lazy metadata reads may still throw. */
  def resolveMessageDescriptor(
      exprInfo: ProtobufExprInfo): Either[String, ProtobufMessageDescriptor] = {
    Try(buildMessageDescriptor(exprInfo.messageName, exprInfo.descriptorSource))
      .toEither
      .left
      .map { t =>
        val cause = t match {
          case ex: java.lang.reflect.InvocationTargetException if ex.getCause != null => ex.getCause
          case other => other
        }
        s"Failed to resolve protobuf descriptor for message '${exprInfo.messageName}': " +
          s"${cause.getMessage}"
      }
      .map(new ReflectiveMessageDescriptor(_))
  }

  private def reflectMessageName(e: Expression): Either[String, String] =
    Try(ProtobufReflection.invoke0[String](e, "messageName")).toEither.left.map { t =>
      s"Cannot read from_protobuf messageName via reflection: ${t.getMessage}"
    }

  private def reflectOptions(e: Expression): Either[String, Map[String, String]] = {
    Try(ProtobufReflection.invoke0[scala.collection.Map[String, String]](e, "options"))
      .map(_.toMap)
      .toEither.left.map { _ =>
        "Cannot read from_protobuf options via reflection; falling back to CPU"
      }
  }

  /** Each shim reads the descriptor argument exposed by its Spark expression version. */
  protected def reflectDescriptorSource(e: Expression): Either[String, ProtobufDescriptorSource]

  private def buildMessageDescriptor(
      messageName: String,
      descriptorSource: ProtobufDescriptorSource): AnyRef = {
    val cls = ShimReflectionUtils.loadClass(sparkProtobufUtilsObjectClassName)
    val module = cls.getField("MODULE$").get(null)
    val buildMethod = cls.getMethod("buildDescriptor", classOf[String], classOf[scala.Option[_]])

    val built = invokeBuildDescriptor(
      buildMethod,
      module,
      messageName,
      descriptorSource,
      filePath => Files.readAllBytes(Paths.get(filePath)))
    unwrapMessageDescriptor(built)
  }

  /** Shims override this when Spark wraps the descriptor together with extension metadata. */
  private[shims] def unwrapMessageDescriptor(raw: AnyRef): AnyRef = raw

  /** Supplies the path or bytes expected by Spark; readDescriptorFile is only needed for paths. */
  private[shims] def invokeBuildDescriptor(
      buildMethod: Method,
      module: AnyRef,
      messageName: String,
      descriptorSource: ProtobufDescriptorSource,
      readDescriptorFile: String => Array[Byte]): AnyRef

  private def typeName(t: AnyRef): String =
    if (t == null) "" else Try(ProtobufReflection.invoke0[String](t, "name")).getOrElse(t.toString)

  private def readFileSyntax(fileDesc: AnyRef): String =
    ProtobufReflection.getFileSyntax(fileDesc, typeName)

  /** Returns PROTO2/PROTO3, or an empty string when the syntax cannot be read. */
  private[shims] def readDescriptorSyntax(desc: AnyRef): String =
    Try(readFileSyntax(ProtobufReflection.getFile(desc))).getOrElse("")

  private final class ReflectiveMessageDescriptor(raw: AnyRef) extends ProtobufMessageDescriptor {
    override lazy val syntax: String = readDescriptorSyntax(raw)
    override lazy val javaStringCheckUtf8: Boolean = Try {
      val fileOptions =
        ProtobufReflection.invoke0[AnyRef](ProtobufReflection.getFile(raw), "getOptions")
      ProtobufReflection.invoke0[java.lang.Boolean](fileOptions, "getJavaStringCheckUtf8")
        .booleanValue()
    }.getOrElse(true)

    override def findField(name: String): Option[ProtobufFieldDescriptor] =
      Option(ProtobufReflection.findFieldByName(raw, name)).map(new ReflectiveFieldDescriptor(_))
  }

  private final class ReflectiveFieldDescriptor(raw: AnyRef) extends ProtobufFieldDescriptor {
    override lazy val name: String = ProtobufReflection.invoke0[String](raw, "getName")
    override lazy val fieldNumber: Int = ProtobufReflection.getFieldNumber(raw)
    override lazy val protoTypeName: String = typeName(ProtobufReflection.getFieldType(raw))
    override lazy val isRepeated: Boolean = ProtobufReflection.isRepeated(raw)
    override lazy val isRequired: Boolean = ProtobufReflection.isRequired(raw)
    override lazy val isInOneof: Boolean = ProtobufReflection.getContainingOneof(raw) != null
    override lazy val enumMetadata: Option[ProtobufEnumMetadata] =
      if (protoTypeName == "ENUM") {
        Some(ProtobufEnumMetadata(
          ProtobufReflection.getEnumValues(ProtobufReflection.getEnumType(raw))))
      } else {
        None
      }
    override lazy val explicitDefaultValue: Either[String, Option[ProtobufDefaultValue]] =
      Try {
        if (ProtobufReflection.hasDefaultValue(raw)) {
          ProtobufReflection.getDefaultValue(raw) match {
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
        Some(new ReflectiveMessageDescriptor(ProtobufReflection.getMessageType(raw)))
      } else {
        None
      }
    override lazy val referencedTypeSyntax: Option[String] = protoTypeName match {
      case "MESSAGE" =>
        Some(Try(readDescriptorSyntax(ProtobufReflection.getMessageType(raw))).getOrElse(""))
      case "ENUM" =>
        Some(Try(readDescriptorSyntax(ProtobufReflection.getEnumType(raw))).getOrElse(""))
      case _ => None
    }
  }

  /** Converts runtime defaults to plugin values; explicit enum aliases retain their names. */
  private[shims] def toDefaultValue(
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
        val value = rawDefault match {
          case _: java.lang.Number =>
            enumMetadata.map(_.defaultFromNumber(number))
              .getOrElse(ProtobufDefaultValue.EnumValue(number, number.toString))
          case descriptor =>
            // An explicit default can name any alias, not just the canonical name for its number.
            ProtobufDefaultValue.EnumValue(
              number, ProtobufReflection.invoke0[String](descriptor, "getName"))
        }
        Right(value)
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

  protected object ProtobufReflection {
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

    def getFile(desc: AnyRef): AnyRef = invoke0[AnyRef](desc, "getFile")

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

    def getFileSyntax(fileDesc: AnyRef, typeNameFn: AnyRef => String): String = Try {
      val syntaxObj = Try(invoke0[AnyRef](fileDesc, "getSyntax")).getOrElse {
        val fileProto = invoke0[AnyRef](fileDesc, "toProto")
        invoke0[AnyRef](fileProto, "getSyntax")
      }
      val syntax = typeNameFn(syntaxObj).trim
      if (syntax.isEmpty) "PROTO2" else syntax.toUpperCase(Locale.ROOT)
    }.getOrElse("")
  }
}


/**
 * Supports Spark 3.5+'s binary-descriptor API, reading path-based sources into bytes when needed.
 * Subclasses handle differences in the builder's return type.
 */
private[shims] abstract class BinarySparkProtobufCompat extends SparkProtobufCompatBase {
  override protected def reflectDescriptorSource(
      e: Expression): Either[String, ProtobufDescriptorSource] =
    Try(ProtobufReflection.invoke0[Option[Array[Byte]]](e, "binaryFileDescriptorSet"))
      .toEither.left.map(t => s"Cannot read binaryFileDescriptorSet: ${t.getMessage}")
      .flatMap(_.map(ProtobufDescriptorSource.DescriptorBytes.apply).toRight(
        "from_protobuf requires a binary descriptor set"))

  override private[shims] def invokeBuildDescriptor(
      buildMethod: Method,
      module: AnyRef,
      messageName: String,
      descriptorSource: ProtobufDescriptorSource,
      readDescriptorFile: String => Array[Byte]): AnyRef = {
    val bytes = descriptorSource match {
      case source: ProtobufDescriptorSource.DescriptorBytes => source.bytes
      case ProtobufDescriptorSource.DescriptorPath(path) => readDescriptorFile(path)
    }
    buildMethod.invoke(module, messageName, Some(bytes))
  }
}
