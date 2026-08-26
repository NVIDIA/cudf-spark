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

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.io.Source

import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.scalatest.funsuite.AnyFunSuite

class ClassInitializationSuite extends AnyFunSuite with FQSuiteName {
  test("SparkShimImpl and GpuOverrides can be initialized concurrently") {
    assume(VersionUtils.isDataBricks ||
        (VersionUtils.isSpark && VersionUtils.cmpSparkVersion(3, 4, 0) >= 0),
      "The affected shimExecs initializers are only in Databricks or Spark 3.4+")

    var attempts = 0
    var result: ChildResult = null
    // Exit code 3 means the child missed the narrow window for suspending GpuOverrides during
    // class initialization. Retry that harness setup race in a fresh JVM, but do not retry an
    // observed deadlock or any other result.
    do {
      attempts += 1
      result = runChild()
    } while (result.exitCode == 3 && attempts < 3)

    assert(result.finished, s"Class-initialization reproducer timed out:\n${result.output}")
    assert(result.exitCode === 0,
      s"Class-initialization reproducer failed with exit code ${result.exitCode}:\n" +
        result.output)
  }

  private def runChild(): ChildResult = {
    val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val classPath = System.getProperty("java.class.path")
    val mainClass = GpuOverridesClassInitializationReproducer.getClass.getName.stripSuffix("$")

    val process = new ProcessBuilder(
      java,
      "-Dcom.nvidia.spark.rapids.runningTests=true",
      "-cp",
      classPath,
      mainClass)
        .redirectErrorStream(true)
        .start()

    val finished = process.waitFor(30, TimeUnit.SECONDS)
    if (!finished) {
      process.destroyForcibly()
      process.waitFor()
    }

    val source = Source.fromInputStream(process.getInputStream, "UTF-8")
    val output = try {
      source.mkString
    } finally {
      source.close()
    }

    ChildResult(finished, process.exitValue(), output)
  }

  private case class ChildResult(finished: Boolean, exitCode: Int, output: String)
}

/**
 * Runs in a child JVM because JVM class initialization can only happen once. The explicit thread
 * suspension is isolated to that process and makes the production lock ordering deterministic:
 *
 *  1. pause GpuOverrides while it owns its class-initialization lock;
 *  2. initialize SparkShimImpl until it waits for GpuOverrides;
 *  3. resume GpuOverrides so it reaches SparkShimImpl.ansiCastRule.
 *
 * With any cyclic dependency between GpuOverrides and SparkShimImpl, both threads remain stuck.
 */
object GpuOverridesClassInitializationReproducer {
  private val SparkShimImplClass = "com.nvidia.spark.rapids.shims.SparkShimImpl$"
  private val WaitForFrameSeconds = 5L
  private val WaitForCompletionSeconds = 5L

  def main(args: Array[String]): Unit = {
    System.exit(runReproducer())
  }

  private def runReproducer(): Int = {
    // Resolve the classes before suspending a thread so it cannot be paused while loading either
    // of the two classes needed by the other initializer.
    val loader = Thread.currentThread().getContextClassLoader
    Class.forName("com.nvidia.spark.rapids.GpuOverrides$", false, loader)
    Class.forName(SparkShimImplClass, false, loader)

    val overridesFailure = new AtomicReference[Throwable]()
    val overridesThread = daemonThread("initialize-gpu-overrides") {
      try {
        GpuOverrides.getTimeParserPolicy
      } catch {
        case t: Throwable => overridesFailure.set(t)
      }
    }
    overridesThread.start()

    if (!suspendOutsideClassLoading(overridesThread,
        "com.nvidia.spark.rapids.GpuOverrides$")) {
      printThreads("Could not pause GpuOverrides during class initialization",
        Seq(overridesThread))
      return 3
    }

    var overridesSuspended = true
    try {
      if (!hasFrame(overridesThread, "com.nvidia.spark.rapids.GpuOverrides$")) {
        printThreads("Missed the GpuOverrides initialization window", Seq(overridesThread))
        return 3
      }

      val shimsFailure = new AtomicReference[Throwable]()
      val shimsThread = daemonThread("initialize-spark-shims") {
        try {
          // This is the first-touch path from MultiFilePartitionReaderFactoryBase. A null
          // partition is sufficient because SparkShimImpl must initialize before dispatching the
          // method; after successful initialization the expected NPE is ignored.
          SparkShimImpl.getPartitionFiles(null)
        } catch {
          case _: NullPointerException =>
          case t: Throwable => shimsFailure.set(t)
        }
      }
      shimsThread.start()

      val shimInitializationStarted = waitForFrame(shimsThread, SparkShimImplClass)
      if (!shimInitializationStarted && shimsThread.isAlive) {
        printThreads("SparkShimImpl did not reach its shim initializer", Seq(shimsThread))
        return 4
      }

      invokeThreadControl("resume", overridesThread)
      overridesSuspended = false

      val completionDeadline =
        System.nanoTime() + TimeUnit.SECONDS.toNanos(WaitForCompletionSeconds)
      joinUntil(overridesThread, completionDeadline)
      joinUntil(shimsThread, completionDeadline)

      if (overridesThread.isAlive && shimsThread.isAlive &&
          hasFrame(overridesThread, "com.nvidia.spark.rapids.GpuOverrides$") &&
          hasFrame(shimsThread, SparkShimImplClass)) {
        printThreads("Reproduced SparkShimImpl/GpuOverrides class-initialization deadlock",
          Seq(overridesThread, shimsThread))
        return 2
      }

      if (overridesThread.isAlive || shimsThread.isAlive) {
        printThreads("Class initializers did not complete", Seq(overridesThread, shimsThread))
        return 5
      }

      val failures = Seq(overridesFailure.get(), shimsFailure.get()).filter(_ != null)
      if (failures.nonEmpty) {
        failures.foreach(_.printStackTrace(System.out))
        return 6
      }

      0
    } finally {
      if (overridesSuspended) {
        invokeThreadControl("resume", overridesThread)
      }
    }
  }

  private def daemonThread(name: String)(body: => Unit): Thread = {
    val thread = new Thread(new Runnable {
      override def run(): Unit = body
    }, name)
    thread.setDaemon(true)
    thread
  }

  private def waitForFrame(thread: Thread, classNamePrefix: String): Boolean = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WaitForFrameSeconds)
    while (thread.isAlive && System.nanoTime() < deadline) {
      if (hasFrame(thread, classNamePrefix)) {
        return true
      }
      Thread.sleep(1)
    }
    false
  }

  private def joinUntil(thread: Thread, deadlineNanos: Long): Unit = {
    val remainingNanos = deadlineNanos - System.nanoTime()
    if (thread.isAlive && remainingNanos > 0) {
      thread.join(math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)))
    }
  }

  private def suspendOutsideClassLoading(thread: Thread, className: String): Boolean = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WaitForFrameSeconds)
    while (thread.isAlive && System.nanoTime() < deadline) {
      if (hasSafeInitializationStack(thread, className)) {
        invokeThreadControl("suspend", thread)
        if (hasSafeInitializationStack(thread, className)) {
          return true
        }
        invokeThreadControl("resume", thread)
      }
      Thread.`yield`()
    }
    false
  }

  private def hasSafeInitializationStack(thread: Thread, className: String): Boolean = {
    val stack = thread.getStackTrace
    val isInitializingTarget = stack.exists { frame =>
      frame.getClassName == className &&
        (frame.getMethodName == "<init>" || frame.getMethodName == "<clinit>")
    }
    val isLoadingClass = stack.exists { frame =>
      val name = frame.getClassName
      name.startsWith("java.lang.ClassLoader") ||
        name.startsWith("java.util.jar.") ||
        name.startsWith("java.util.zip.") ||
        name.startsWith("jdk.internal.loader.")
    }
    isInitializingTarget && !isLoadingClass
  }

  private def hasFrame(thread: Thread, classNamePrefix: String): Boolean = {
    thread.getStackTrace.exists(_.getClassName.startsWith(classNamePrefix))
  }

  private def invokeThreadControl(methodName: String, thread: Thread): Unit = {
    // Thread.suspend/resume are intentionally invoked reflectively to avoid using deprecated APIs
    // in production code. This process is disposable and the parent has a hard timeout.
    classOf[Thread].getMethod(methodName).invoke(thread)
  }

  private def printThreads(message: String, threads: Seq[Thread]): Unit = {
    System.out.println(message)
    threads.foreach { thread =>
      System.out.println("\"" + thread.getName + "\" state=" + thread.getState)
      thread.getStackTrace.foreach(frame => System.out.println(s"\tat $frame"))
    }
  }
}
