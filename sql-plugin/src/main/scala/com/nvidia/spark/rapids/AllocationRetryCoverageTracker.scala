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

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.internal.Logging

/**
 * Memory allocation kind for retry coverage tracking.
 */
object AllocationKind extends Enumeration {
  type AllocationKind = Value
  val HOST, DEVICE = Value
}

/**
 * Utility object to track memory allocations that are not covered by retry methods.
 * 
 * This is a debugging tool to help ensure all memory allocation code paths are
 * protected by withRetry/withRetryNoSplit blocks, as required by the retry-OOM
 * handling mechanism in spark-rapids.
 * 
 * Tracking support:
 * - HOST memory: via HostAlloc hooks
 * - DEVICE memory: via RMM debug mode (onAllocated callback)
 * 
 * This feature is DISABLED by default. To enable, set environment variable:
 *   export SPARK_RAPIDS_RETRY_COVERAGE_TRACKING=true
 * 
 * For integration tests:
 *   SPARK_RAPIDS_RETRY_COVERAGE_TRACKING=true ./integration_tests/run_pyspark_from_build.sh
 * 
 * When enabled:
 * - Checks whether allocations happen while the current thread is executing inside the retry
 *   framework (withRetry/withRetryNoSplit)
 * - For uncovered allocations, captures a filtered stack trace for debugging
 * - Logs uncovered allocations to a CSV file under the JVM temp dir (see log output for path)
 * - Also logs warnings via Spark logging
 * 
 * Note: Enabling this feature turns on RMM debug mode which may impact performance.
 * Only use for debugging/testing purposes.
 * 
 * See: https://github.com/NVIDIA/spark-rapids/issues/13672
 */
object AllocationRetryCoverageTracker extends Logging {
  import AllocationKind._

  // Environment variable to enable retry coverage tracking (debug-only).
  // Kept here so other components can reference it without duplicating the name.
  final val RETRY_COVERAGE_TRACKING_ENV_VAR_NAME: String = "SPARK_RAPIDS_RETRY_COVERAGE_TRACKING"

  // Spark RAPIDS shims may live under a variety of packages, e.g.
  // - org.apache.spark.rapids
  // - org.apache.spark.shuffle.rapids
  // - org.apache.spark.sql.catalyst.rapids
  private val ORG_APACHE_SPARK_RAPIDS_CLASS_PATTERN =
    Pattern.compile("""^org\.apache\.spark(?:\.[\w$]+)*\.rapids\..*""")

  private def isSparkRapidsClassName(className: String): Boolean = {
    className.startsWith("com.nvidia.spark.rapids.") ||
      ORG_APACHE_SPARK_RAPIDS_CLASS_PATTERN.matcher(className).matches()
  }

  private def isRelevantStackClassName(className: String): Boolean = {
    isSparkRapidsClassName(className) || className.startsWith("ai.rapids.cudf.")
  }

  // Default output file path for uncovered allocations. Use the JVM temp dir so this works
  // cross-platform without hardcoding "/tmp".
  private val DEFAULT_OUTPUT_PATH: String = {
    val tmpDir = Option(System.getProperty("java.io.tmpdir")).getOrElse("/tmp")
    Paths.get(tmpDir, "uncovered_allocations.csv").toString
  }

  // Check environment variable - this works reliably across all processes
  val ENABLED: Boolean =
    "true".equalsIgnoreCase(System.getenv(RETRY_COVERAGE_TRACKING_ENV_VAR_NAME))

  @volatile private var headerWritten: Boolean = false

  // Track unique call stacks we've already logged to avoid duplicates
  private val loggedStacks = ConcurrentHashMap.newKeySet[String]()


  /**
   * Ensure header is written (thread-safe, lazy initialization).
   *
   * Note: `headerWritten` is a per-JVM variable that resets to `false` when a new Spark
   * application starts (i.e., a new JVM process). In integration tests, multiple test runs
   * or pytest-xdist workers may write to the same output file. We check if the file already
   * exists and is non-empty to avoid:
   *   1. Overwriting records from previous JVM processes
   *   2. Writing multiple CSV headers
   * This enables cross-process append mode for accumulating results across test runs.
   */
  private def ensureHeaderWritten(): Unit = {
    if (!headerWritten) {
      this.synchronized {
        if (!headerWritten) {
          val outputPath = Paths.get(DEFAULT_OUTPUT_PATH)
          // Check file existence because headerWritten only tracks state within this JVM.
          // Other JVM processes may have already written to the file.
          val shouldWriteHeader = try {
            !Files.exists(outputPath) || Files.size(outputPath) == 0L
          } catch {
            case _: Exception => true
          }

          if (shouldWriteHeader) {
            logWarning(s"Retry coverage tracking ACTIVE. Output: $DEFAULT_OUTPUT_PATH")
            writeToFileInternal("kind,call_stack", append = false)
          } else {
            // File already exists and is non-empty (written by another JVM process).
            // Skip writing header and append to preserve existing records.
            logWarning(s"Retry coverage tracking ACTIVE. Appending to: $DEFAULT_OUTPUT_PATH")
          }
          headerWritten = true
        }
      }
    }
  }

  /**
   * Check if a memory allocation is covered by a retry method.
   * If not covered and tracking is enabled, log to the output file.
   * 
   * @param kind The kind of memory allocation (HOST or DEVICE)
   */
  private def checkAllocationInternal(kind: AllocationKind): Unit = {
    // Consider an allocation "covered" if it happens while the current thread is executing
    // inside the retry framework (withRetry/withRetryNoSplit).
    //
    // When uncovered, we capture a filtered stack trace for debugging.
    if (!RetryStateTracker.isInRetryBlock) {
      // Ensure header is written before logging the first uncovered allocation
      ensureHeaderWritten()
      val stackTrace = Thread.currentThread().getStackTrace
      // Filter to only spark-rapids related frames for cleaner output
      val relevantStack = stackTrace
        .filter { element =>
          val className = element.getClassName
          isRelevantStackClassName(className) &&
            !className.contains("AllocationRetryCoverageTracker")
        }
        .map(_.toString)
        .mkString(" -> ")

      // Use the filtered stack as the key to avoid duplicate logging
      val stackKey = s"$kind:$relevantStack"
      
      // Only log if we haven't seen this exact stack before
      if (loggedStacks.add(stackKey)) {
        // Sanitize and escape the stack trace for CSV:
        //  - replace newlines/carriage returns to keep one record per line
        //  - escape embedded quotes and wrap in quotes
        val sanitizedStack = relevantStack
          .replace("\r", " ")
          .replace("\n", " ")
        val escapedStack = "\"" + sanitizedStack.replace("\"", "\"\"") + "\""
        writeToFile(s"$kind,$escapedStack", append = true)
        logWarning(s"Uncovered $kind allocation #${loggedStacks.size()}. Stack: $relevantStack")
      }
    }
  }

  /**
   * Check host memory allocation coverage.
   */
  @inline
  def checkHostAllocation(): Unit = {
    if (ENABLED) {
      checkAllocationInternal(HOST)
    }
  }

  /**
   * Check device (GPU) memory allocation coverage.
   */
  @inline
  def checkDeviceAllocation(): Unit = {
    if (ENABLED) {
      checkAllocationInternal(DEVICE)
    }
  }

  /**
   * Write a line to the output CSV file (internal, assumes lock is NOT held).
   */
  private def writeToFileInternal(line: String, append: Boolean): Unit = {
    try {
      withResource(new FileWriter(DEFAULT_OUTPUT_PATH, append)) { fw =>
        withResource(new BufferedWriter(fw)) { bw =>
          bw.write(line)
          bw.newLine()
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to write to retry coverage tracking file: $DEFAULT_OUTPUT_PATH", e)
    }
  }

  /**
   * Write a line to the output CSV file (thread-safe).
   */
  private def writeToFile(line: String, append: Boolean): Unit = {
    this.synchronized {
      writeToFileInternal(line, append)
    }
  }

  /**
   * Get the number of unique uncovered allocation patterns logged.
   */
  def getUncoveredCount: Int = loggedStacks.size()

  /**
   * Clear the logged stacks (useful for testing).
   */
  def clearLoggedStacks(): Unit = {
    loggedStacks.clear()
  }
}

