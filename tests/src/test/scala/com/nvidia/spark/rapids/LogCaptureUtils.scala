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

package com.nvidia.spark.rapids

import java.lang.{Boolean => JBoolean}
import java.lang.reflect.{InvocationHandler, Method, Proxy}

import scala.collection.mutable.ArrayBuffer

/**
 * Utility for capturing log messages during test execution.
 * Automatically detects and works with both Log4j 1.x and Log4j 2.x.
 */
object LogCaptureUtils {
  
  private val isLog4j2: Boolean = {
    try {
      Class.forName("org.apache.logging.log4j.core.LoggerContext")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }
  
  /**
   * Capture log messages from specific loggers during operation execution.
   * 
   * @param loggerNames Names of loggers to capture messages from
   * @param operation The operation to execute while capturing logs
   * @return Array of captured log messages
   */
  def captureLogsFrom(loggerNames: Seq[String])(operation: => Unit): Array[String] = {
    val logMessages = new ArrayBuffer[String]()
    
    val capturer = if (isLog4j2) {
      new Log4j2Capturer(loggerNames, logMessages)
    } else {
      new Log4j1Capturer(loggerNames, logMessages)
    }
    
    try {
      capturer.setup()
      operation
    } finally {
      capturer.cleanup()
    }
    
    logMessages.toArray
  }
}

/**
 * Base trait for log capture implementations.
 */
private trait LogCapturer {
  def setup(): Unit
  def cleanup(): Unit
}

/**
 * Log capturer implementation for Log4j 1.x using reflection.
 */
private class Log4j1Capturer(
    loggerNames: Seq[String],
    logMessages: ArrayBuffer[String]) extends LogCapturer {
  
  private val loggerClass = Class.forName("org.apache.log4j.Logger")
  private val levelClass = Class.forName("org.apache.log4j.Level")
  private val debugLevel = levelClass.getField("DEBUG").get(null)
  
  private val getLoggerMethod = loggerClass.getMethod("getLogger", classOf[String])
  private val getRootLoggerMethod = loggerClass.getMethod("getRootLogger")
  private val getLevelMethod = loggerClass.getMethod("getLevel")
  private val setLevelMethod = loggerClass.getMethod("setLevel", levelClass)
  
  private val loggers = loggerNames.map(name => 
    getLoggerMethod.invoke(null, name))
  private val rootLogger = getRootLoggerMethod.invoke(null)
  private val origLevels = loggers.map(logger => 
    getLevelMethod.invoke(logger))
  
  private val appenderClass = Class.forName("org.apache.log4j.Appender")
  
  private val appender = Proxy.newProxyInstance(
    getClass.getClassLoader,
    Array(appenderClass),
    new InvocationHandler {
      override def invoke(proxy: Any, method: Method, 
          args: Array[Object]): Object = {
        method.getName match {
          case "doAppend" if args != null && args.length > 0 =>
            val event = args(0)
            val getRenderedMessageMethod = event.getClass.getMethod(
              "getRenderedMessage")
            val message = getRenderedMessageMethod.invoke(event).toString
            logMessages.synchronized { logMessages += message }
            null
          case "getName" => "TestCaptureAppender"
          case "close" => null
          case "requiresLayout" => JBoolean.FALSE
          case "equals" =>
            if (args != null && args.length == 1) {
              JBoolean.valueOf(
                proxy.asInstanceOf[AnyRef] eq args(0).asInstanceOf[AnyRef])
            } else {
              JBoolean.FALSE
            }
          case "hashCode" => 
            Integer.valueOf(System.identityHashCode(proxy))
          case "toString" => "TestCaptureAppender"
          case _ => null
        }
      }
    }
  )
  
  override def setup(): Unit = {
    loggers.foreach(logger => setLevelMethod.invoke(logger, debugLevel))
    val addAppenderMethod = rootLogger.getClass.getMethod(
      "addAppender", appenderClass)
    addAppenderMethod.invoke(rootLogger, appender)
  }
  
  override def cleanup(): Unit = {
    val removeAppenderMethod = rootLogger.getClass.getMethod(
      "removeAppender", appenderClass)
    removeAppenderMethod.invoke(rootLogger, appender)
    
    loggers.zip(origLevels).foreach { case (logger, origLevel) =>
      if (origLevel != null) {
        setLevelMethod.invoke(logger, origLevel)
      }
    }
  }
}

/**
 * Log capturer implementation for Log4j 2.x using reflection.
 */
private class Log4j2Capturer(
    loggerNames: Seq[String],
    logMessages: ArrayBuffer[String]) extends LogCapturer {
  
  private val logManagerClass = Class.forName("org.apache.logging.log4j.LogManager")
  private val getContextMethod = logManagerClass.getMethod(
    "getContext", classOf[Boolean])
  private val context = getContextMethod.invoke(null, JBoolean.FALSE)
  
  private val getConfigurationMethod = context.getClass.getMethod("getConfiguration")
  private val config = getConfigurationMethod.invoke(context)
  
  private val levelClass = Class.forName("org.apache.logging.log4j.Level")
  private val debugLevel = levelClass.getField("DEBUG").get(null)
  
  private val appenderClass = Class.forName(
    "org.apache.logging.log4j.core.Appender")
  
  private val appender = Proxy.newProxyInstance(
    getClass.getClassLoader,
    Array(appenderClass),
    new InvocationHandler {
      override def invoke(proxy: Any, method: Method, 
          args: Array[Object]): Object = {
        method.getName match {
          case "append" if args != null && args.length > 0 =>
            val logEvent = args(0)
            val getMessageMethod = logEvent.getClass.getMethod("getMessage")
            val message = getMessageMethod.invoke(logEvent)
            val getFormattedMessageMethod = message.getClass.getMethod(
              "getFormattedMessage")
            val formattedMsg = getFormattedMessageMethod.invoke(message).toString
            logMessages.synchronized { logMessages += formattedMsg }
            null
          case "getName" => "TestCaptureAppender"
          case "isStarted" => JBoolean.TRUE
          case "isStopped" => JBoolean.FALSE
          case "getLayout" => null
          case "ignoreExceptions" => JBoolean.TRUE
          case "equals" =>
            if (args != null && args.length == 1) {
              JBoolean.valueOf(
                proxy.asInstanceOf[AnyRef] eq args(0).asInstanceOf[AnyRef])
            } else {
              JBoolean.FALSE
            }
          case "hashCode" => 
            Integer.valueOf(System.identityHashCode(proxy))
          case "toString" => "TestCaptureAppender"
          case _ => null
        }
      }
    }
  )
  
  private val getLoggerConfigMethod = config.getClass.getMethod(
    "getLoggerConfig", classOf[String])
  
  private val origLevels = loggerNames.map { name =>
    val loggerConfig = getLoggerConfigMethod.invoke(config, name)
    val getLevelMethod = loggerConfig.getClass.getMethod("getLevel")
    val origLevel = getLevelMethod.invoke(loggerConfig)
    (loggerConfig, origLevel)
  }
  
  override def setup(): Unit = {
    loggerNames.foreach { name =>
      val loggerConfig = getLoggerConfigMethod.invoke(config, name)
      val setLevelMethod = loggerConfig.getClass.getMethod(
        "setLevel", levelClass)
      setLevelMethod.invoke(loggerConfig, debugLevel)
      
      val addAppenderMethod = loggerConfig.getClass.getMethod(
        "addAppender", appenderClass, levelClass, 
        Class.forName("org.apache.logging.log4j.core.Filter"))
      addAppenderMethod.invoke(loggerConfig, appender, debugLevel, null)
    }
    
    val updateLoggersMethod = context.getClass.getMethod("updateLoggers")
    updateLoggersMethod.invoke(context)
  }
  
  override def cleanup(): Unit = {
    loggerNames.foreach { name =>
      val loggerConfig = getLoggerConfigMethod.invoke(config, name)
      val removeAppenderMethod = loggerConfig.getClass.getMethod(
        "removeAppender", classOf[String])
      removeAppenderMethod.invoke(loggerConfig, "TestCaptureAppender")
    }
    
    origLevels.foreach { case (loggerConfig, origLevel) =>
      if (origLevel != null) {
        val setLevelMethod = loggerConfig.getClass.getMethod(
          "setLevel", levelClass)
        setLevelMethod.invoke(loggerConfig, origLevel)
      }
    }
    
    val updateLoggersMethod = context.getClass.getMethod("updateLoggers")
    updateLoggersMethod.invoke(context)
  }
}

