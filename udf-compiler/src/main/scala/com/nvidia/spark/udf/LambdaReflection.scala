/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.udf

import java.lang.invoke.SerializedLambda

import javassist.{ClassClassPath, ClassPool, CtBehavior, CtClass, CtField, CtMethod}
import javassist.bytecode.{CodeIterator, ConstPool, Descriptor}

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._

//
// Reflection using SerializedLambda and javassist.
//
// Provides the interface the class and the method that implements the body of the lambda
// used by the rest of the compiler.
//
class LambdaReflection private(private val classPool: ClassPool,
                               private val ctMethod: CtMethod,
                               val capturedArgs: Seq[Expression] = Seq()) {
  def lookupConstant(constPoolIndex: Int): Any = {
    constPool.getTag(constPoolIndex) match {
      case ConstPool.CONST_Integer => constPool.getIntegerInfo(constPoolIndex)
      case ConstPool.CONST_Long => constPool.getLongInfo(constPoolIndex)
      case ConstPool.CONST_Float => constPool.getFloatInfo(constPoolIndex)
      case ConstPool.CONST_Double => constPool.getDoubleInfo(constPoolIndex)
      case ConstPool.CONST_String => constPool.getStringInfo(constPoolIndex)
      case ConstPool.CONST_Class => constPool.getClassInfo(constPoolIndex)
      case _ => throw new SparkException("Unsupported constant")
    }
  }

  def lookupField(constPoolIndex: Int): CtField = {
    if (constPool.getTag(constPoolIndex) != ConstPool.CONST_Fieldref) {
      throw new SparkException("Unexpected index for field reference")
    }
    val fieldName = constPool.getFieldrefName(constPoolIndex)
    val descriptor = constPool.getFieldrefType(constPoolIndex)
    val className = constPool.getFieldrefClassName(constPoolIndex)
    classPool.getCtClass(className).getField(fieldName, descriptor)
  }

  def lookupBehavior(constPoolIndex: Int): CtBehavior = {
    if (constPool.getTag(constPoolIndex) == ConstPool.CONST_InterfaceMethodref) {
      val methodName = constPool.getInterfaceMethodrefName(constPoolIndex)
      val descriptor = constPool.getInterfaceMethodrefType(constPoolIndex)
      val className = constPool.getInterfaceMethodrefClassName(constPoolIndex)
      classPool.getCtClass(className).getMethod(methodName, descriptor)
    } else {
      if (constPool.getTag(constPoolIndex) != ConstPool.CONST_Methodref) {
        throw new SparkException(s"Unexpected index ${constPoolIndex} for method reference")
      }
      val methodName = constPool.getMethodrefName(constPoolIndex)
      val descriptor = constPool.getMethodrefType(constPoolIndex)
      val className = constPool.getMethodrefClassName(constPoolIndex)
      if (constPool.isConstructor(className, constPoolIndex) == 0) {
        classPool.getCtClass(className).getMethod(methodName, descriptor)
      } else {
        val params = Descriptor.getParameterTypes(descriptor, classPool)
        classPool.getCtClass(className).getDeclaredConstructor(params)
      }
    }
  }

  def lookupBootstrapMethod(constPoolIndex: Int): (CtMethod, Seq[Any]) = {
    if (constPool.getTag(constPoolIndex) != ConstPool.CONST_InvokeDynamic) {
      throw new SparkException(s"Unexpected index ${constPoolIndex} for bootstrap")
    }
    val bootstrapMethodIndex = constPool.getInvokeDynamicBootstrap(constPoolIndex)
    val bootstrapMethodsAttribute = ctMethod.getDeclaringClass.getClassFile.getAttributes
      .toArray.filter(_.isInstanceOf[javassist.bytecode.BootstrapMethodsAttribute])
    if (bootstrapMethodsAttribute.length != 1) {
      throw new SparkException(s"Multiple bootstrap methods attributes aren't supported")
    }
    val bootstrapMethods = bootstrapMethodsAttribute.head
      .asInstanceOf[javassist.bytecode.BootstrapMethodsAttribute].getMethods
    val bootstrapMethod = bootstrapMethods(bootstrapMethodIndex)
    val bootstrapMethodArguments = try {
      bootstrapMethod.arguments.map(lookupConstant)
    } catch {
      case _: Throwable =>
        throw new SparkException(s"only constants are supported as bootstrap method arguments")
    }
    val constPoolIndexMethodref = constPool.getMethodHandleIndex(bootstrapMethod.methodRef)
    val methodName = constPool.getMethodrefName(constPoolIndexMethodref)
    val descriptor = constPool.getMethodrefType(constPoolIndexMethodref)
    val className = constPool.getMethodrefClassName(constPoolIndexMethodref)
    (classPool.getCtClass(className).getMethod(methodName, descriptor), bootstrapMethodArguments)
  }

  def lookupClassName(constPoolIndex: Int): String = {
    if (constPool.getTag(constPoolIndex) != ConstPool.CONST_Class) {
      throw new SparkException("Unexpected index for class")
    }
    constPool.getClassInfo(constPoolIndex)
  }

  private val methodInfo = ctMethod.getMethodInfo

  val constPool = methodInfo.getConstPool

  private val codeAttribute = methodInfo.getCodeAttribute

  if (codeAttribute.getExceptionTable.size > 0) {
    throw new SparkException("exception handling (try-and-catch) isn't supported")
  }

  lazy val codeIterator: CodeIterator = codeAttribute.iterator

  lazy val parameters: Array[CtClass] = ctMethod.getParameterTypes

  lazy val ret: CtClass = ctMethod.getReturnType

  if (ret == CtClass.voidType) {
    throw new SparkException("Cannot construct Catalyst expression with void return type")
  }

  lazy val maxLocals: Int = codeAttribute.getMaxLocals
}

object LambdaReflection {
  def apply(function: AnyRef): LambdaReflection = {
    if (function.isInstanceOf[CtMethod]) {
      return LambdaReflection(function.asInstanceOf[CtMethod])
    }
    // writeReplace is supposed to return an object of SerializedLambda from
    // the function class (See
    // https://docs.oracle.com/javase/8/docs/api/java/lang/invoke/SerializedLambda.html).
    // With the object of SerializedLambda, we can get our hands on the class
    // and the method that implement the lambda body.
    val functionClass = function.getClass
    val writeReplace = functionClass.getDeclaredMethod("writeReplace")
    writeReplace.setAccessible(true)
    val serializedLambda = writeReplace.invoke(function)
        .asInstanceOf[SerializedLambda]
    val capturedArgs = Seq.tabulate(serializedLambda.getCapturedArgCount) { i =>
      val capturedArg = serializedLambda.getCapturedArg(i)
      if (capturedArg.isInstanceOf[Byte] || capturedArg.isInstanceOf[Char] ||
          capturedArg.isInstanceOf[Short] || capturedArg.isInstanceOf[Int] ||
          capturedArg.isInstanceOf[Long] || capturedArg.isInstanceOf[Float] ||
          capturedArg.isInstanceOf[Double] || capturedArg.isInstanceOf[Boolean]) {
        Literal(capturedArg)
      } else {
        // Add UnknownCapturedArg expressions to the list of captured arguments
        // until we figure out how to handle captured variables in various
        // situations.
        // This, however, lets us pass captured arguments as long as they are
        // not evaluated.
        Repr.UnknownCapturedArg()
      }
    }

    val classPool = new ClassPool(true)
    // Get the CtClass object for the class that capture the lambda.
    val ctClass = {
      val name = serializedLambda.getCapturingClass.replace('/', '.')
      val classForName = LambdaReflection.getClass(name)
      classPool.insertClassPath(new ClassClassPath(classForName))
      classPool.getCtClass(name)
    }
    // Get the CtMethod object for the method that implements the lambda body.
    val ctMethod = {
      val lambdaImplName = serializedLambda.getImplMethodName
      ctClass.getDeclaredMethod(lambdaImplName.stripSuffix("$adapted"))
    }
    new LambdaReflection(classPool, ctMethod, capturedArgs)
  }

  private def apply(ctMethod: CtMethod): LambdaReflection = {
    val ctClass = ctMethod.getDeclaringClass
    val classPool = ctClass.getClassPool
    new LambdaReflection(classPool, ctMethod)
  }

  def getClass(name: String): Class[_] = {
    // Don't use ShimLoader.loadClass because in the REPL use case the classes
    // compiled by REPL are accessible via the thread's context classloader,
    // but not by the parent classloader detected by the plugin.
    //
    // This code is executed after the Plugin and ShimLoader has already been initialized.
    // REPL classes are able to interact with the Plugin classes because the REPL
    // Thread context classloader is a descendant of the Plugin classloader.

    // scalastyle:off classforname
    Class.forName(name, true, Thread.currentThread().getContextClassLoader)
    // scalastyle:on classforname
  }

  def parseTypeSig(sig: String): Option[DataType] = {
    if (sig.head == '[') {
      parseTypeSig(sig.tail).map{elementType =>
        if (elementType == ByteType) {
          DataTypes.BinaryType
        } else {
          DataTypes.createArrayType(elementType)
        }
      }
    } else {
      sig match {
        case "Z" => Some(BooleanType)
        case "B" => Some(ByteType)
        case "S" => Some(ShortType)
        case "I" => Some(IntegerType)
        case "J" => Some(LongType)
        case "F" => Some(FloatType)
        case "D" => Some(DoubleType)
        case "Ljava.lang.String;" => Some(StringType)
        case _ => None
      }
    }
  }
}

