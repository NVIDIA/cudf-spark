/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import java.util.Locale

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.{ShuffleManagerShimUtils, SparkShimImpl}

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

class GpuShuffleEnv(rapidsConf: RapidsConf) extends Logging {
  private var shuffleCatalog: ShuffleBufferCatalog = _
  private var shuffleReceivedBufferCatalog: ShuffleReceivedBufferCatalog = _
  private var multithreadedCatalog: MultithreadedShuffleBufferCatalog = _

  private lazy val isRapidsShuffleConfigured: Boolean = {
    val conf = SparkEnv.get.conf
    conf.contains("spark.shuffle.manager") &&
      conf.get("spark.shuffle.manager") == GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS
  }

  lazy val rapidsShuffleCodec: Option[TableCompressionCodec] = {
    val codecName = rapidsConf.shuffleCompressionCodec.toLowerCase(Locale.ROOT)
    if (codecName == "none") {
      None
    } else {
      val codecConfigs = TableCompressionCodec.makeCodecConfig(rapidsConf)
      Some(TableCompressionCodec.getCodec(codecName, codecConfigs))
    }
  }

  def init(): Unit = {
    if (isRapidsShuffleConfigured) {
      shuffleCatalog =
          new ShuffleBufferCatalog()
      shuffleReceivedBufferCatalog =
          new ShuffleReceivedBufferCatalog()
      // Initialize MultithreadedShuffleBufferCatalog for MULTITHREADED mode when:
      // 1. skipMerge is enabled
      // 2. External Shuffle Service is disabled (ESS cannot access in-memory catalog)
      // 3. Off-heap memory limits are enabled (prevents OOM from unbounded buffer growth)
      if (rapidsConf.isMultiThreadedShuffleManagerMode) {
        if (!rapidsConf.isMultithreadedShuffleSkipMergeEnabled) {
          logInfo("MultithreadedShuffleBufferCatalog disabled - " +
            "spark.rapids.shuffle.multithreaded.skipMerge is false")
        } else if (GpuShuffleEnv.isExternalShuffleEnabled) {
          logWarning("MultithreadedShuffleBufferCatalog disabled - " +
            "External Shuffle Service (ESS) is enabled. ESS cannot access in-memory catalog. " +
            "Disable ESS (spark.shuffle.service.enabled=false) to use skipMerge feature.")
        } else if (!rapidsConf.offHeapLimitEnabled) {
          logWarning("MultithreadedShuffleBufferCatalog disabled - " +
            "spark.rapids.memory.host.offHeapLimit.enabled is false. " +
            "Without off-heap memory limits, shuffle buffers could grow unbounded and cause OOM. " +
            "Set spark.rapids.memory.host.offHeapLimit.enabled=true to use skipMerge feature.")
        } else {
          multithreadedCatalog = new MultithreadedShuffleBufferCatalog()
          logInfo("MultithreadedShuffleBufferCatalog enabled (ESS disabled, off-heap limits on)")
        }
      }
    }
  }

  def getCatalog: ShuffleBufferCatalog = shuffleCatalog

  def getReceivedCatalog: ShuffleReceivedBufferCatalog = shuffleReceivedBufferCatalog

  def getMultithreadedCatalog: Option[MultithreadedShuffleBufferCatalog] = {
    Option(multithreadedCatalog)
  }

  def getShuffleFetchTimeoutSeconds: Long = {
    SparkEnv.get.conf.getTimeAsSeconds("spark.network.timeout", "120s")
  }
}

object GpuShuffleEnv extends Logging {
  def isUCXShuffleAndEarlyStart(conf: RapidsConf): Boolean = {
    conf.isUCXShuffleManagerMode &&
      conf.shuffleTransportEarlyStart
  }

  val RAPIDS_SHUFFLE_CLASS: String = ShimLoader.getRapidsShuffleManagerClass

  /**
   * Check if the RAPIDS shuffle manager is configured via spark.shuffle.manager
   * and matches the current shim. This check doesn't require the shuffle manager
   * to be instantiated yet.
   */
  def isRapidsShuffleConfigured(sparkConf: SparkConf): Boolean = {
    sparkConf.contains("spark.shuffle.manager") &&
      sparkConf.get("spark.shuffle.manager") == RAPIDS_SHUFFLE_CLASS
  }

  @volatile private var env: GpuShuffleEnv = _

  def shutdown() = {
    // check for nulls in tests
    Option(SparkEnv.get)
      .map(_.shuffleManager)
      .collect { case sm: RapidsShuffleManagerLike => sm }
      .foreach(_.stop())

    // when we shut down, make sure we clear `env`, as the convention is that
    // `GpuShuffleEnv.init` will be called to re-establish it
    env = null
  }

  //
  // Functions below get called from the driver or executors
  //

  def isExternalShuffleEnabled: Boolean = {
    SparkEnv.get.blockManager.externalShuffleServiceEnabled
  }

  // Returns true if authentication is requested, which is not supported
  // by the RAPIDS Shuffle Manager
  def isSparkAuthenticateEnabled: Boolean = {
    val conf = SparkEnv.get.conf
    conf.getBoolean("spark.authenticate", false)
  }

  // Returns true if the order-independent (row-based) checksum feature is enabled.
  // This Spark 4.1+ feature (SPARK-51756, SPARK-53575) computes row-based checksums
  // in shuffle writers to detect non-deterministic output across task retries.
  // Not yet supported by the RAPIDS Shuffle Manager (would require GPU-side checksum).
  // Note: this is different from the older spark.shuffle.checksum.enabled (since Spark 3.2)
  // which is for IO-level corruption diagnosis and IS supported by RAPIDS shuffle.
  def isRowBasedChecksumEnabled: Boolean = {
    val conf = SparkEnv.get.conf
    SparkShimImpl.isRowBasedShuffleChecksumEnabled(SQLConf.get, conf)
  }

  //
  // The actual instantiation of the RAPIDS Shuffle Manager is lazy, and
  // this forces the initialization when we know we are ready in the driver and executor.
  //
  def initShuffleManager(): Unit = {
    val shuffleManager = SparkEnv.get.shuffleManager
    if (ShuffleManagerShimUtils.eagerlyInitialized) {
      // skip deferred init
    } else {
      shuffleManager match {
        case rapidsShuffleManager: RapidsShuffleManagerLike =>
          rapidsShuffleManager.initialize
        case _ =>
          val rsmLoaderViaShuffleManager = shuffleManager.getClass.getSuperclass.getInterfaces
            .collectFirst {
              case c if c.getName == classOf[RapidsShuffleManagerLike].getName => c.getClassLoader
            }
          val rsmLoaderDirect = classOf[RapidsShuffleManagerLike].getClassLoader
  
          throw new IllegalStateException(s"Cannot initialize the RAPIDS Shuffle Manager " +
            s"${shuffleManager}! Expected: an instance of RapidsShuffleManagerLike loaded by " +
            s"${rsmLoaderDirect}. Actual: ${shuffleManager} tagged with RapidsShuffleManagerLike " +
            s"loaded by: ${rsmLoaderViaShuffleManager}"
          )
      }
    }
  }

  def isRapidsShuffleAvailable(conf: RapidsConf): Boolean = {
    // the driver has `mgr` defined when this is checked
    val sparkEnv = SparkEnv.get
    if (sparkEnv == null) {
      // we may hit this in some tests that don't need to use the RAPIDS shuffle manager.
      false
    } else {
      val isRapidsManager = sparkEnv.shuffleManager.isInstanceOf[RapidsShuffleManagerLike]
      if (isRapidsManager) {
        validateRapidsShuffleManager(sparkEnv.shuffleManager.getClass.getName)
      }
      // executors have `env` defined when this is checked
      // in tests
      val isConfiguredInEnv = Option(env).exists(_.isRapidsShuffleConfigured)
      (isConfiguredInEnv || isRapidsManager) &&
        (conf.isMultiThreadedShuffleManagerMode ||
          (conf.isGPUShuffle && !isExternalShuffleEnabled &&
            !isSparkAuthenticateEnabled)) &&
        conf.isSqlExecuteOnGPU
    }
  }

  def useGPUShuffle(conf: RapidsConf): Boolean = {
    conf.shuffleManagerEnabled &&
      conf.isGPUShuffle &&
        isRapidsShuffleAvailable(conf)
  }

  def useMultiThreadedShuffle(conf: RapidsConf): Boolean = {
    conf.shuffleManagerEnabled &&
      conf.isMultiThreadedShuffleManagerMode &&
      isRapidsShuffleAvailable(conf)
  }

  def getCatalog: ShuffleBufferCatalog = if (env == null) {
    null
  } else {
    env.getCatalog
  }

  def getMultithreadedCatalog: Option[MultithreadedShuffleBufferCatalog] = {
    Option(env).flatMap(_.getMultithreadedCatalog)
  }

  private def validateRapidsShuffleManager(shuffManagerClassName: String): Unit = {
    if (shuffManagerClassName != RAPIDS_SHUFFLE_CLASS) {
      throw new IllegalStateException(s"RapidsShuffleManager class mismatch (" +
          s"${shuffManagerClassName} != $RAPIDS_SHUFFLE_CLASS). " +
          s"Check that configuration setting spark.shuffle.manager is correct for the Spark " +
          s"version being used.")
    }
  }

  //
  // Functions below only get called from the executor
  //

  def init(conf: RapidsConf): Unit = {
    val shuffleEnv = new GpuShuffleEnv(conf)
    shuffleEnv.init()
    env = shuffleEnv
  }

  def getReceivedCatalog: ShuffleReceivedBufferCatalog = env.getReceivedCatalog

  def rapidsShuffleCodec: Option[TableCompressionCodec] = env.rapidsShuffleCodec

  def shuffleFetchTimeoutSeconds: Long = env.getShuffleFetchTimeoutSeconds
}
