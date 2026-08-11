/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution.python

import ai.rapids.cudf.{Cuda, RmmAllocationMode}
import com.nvidia.spark.rapids.{GpuDeviceManager, RapidsConf}
import com.nvidia.spark.rapids.python.PythonConfEntries._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{CPUS_PER_TASK, EXECUTOR_CORES}
import org.apache.spark.internal.config.Python._
import org.apache.spark.sql.internal.SQLConf

object GpuPythonHelper extends Logging {

  private val sparkConf = SparkEnv.get.conf
  private lazy val rapidsConf = new RapidsConf(sparkConf)
  private lazy val gpuId = GpuDeviceManager.getDeviceId()
    .getOrElse(throw new IllegalStateException("No gpu id!"))
    .toString
  // Visible for tests
  lazy val isPythonPooledMemEnabled: Boolean = rapidsConf.get(PYTHON_POOLED_MEM)
    .getOrElse {
      val rmmMode = GpuDeviceManager.rmmModeFromConf(rapidsConf)
      var poolMode: Option[String] = None
      if ((rmmMode & RmmAllocationMode.CUDA_ASYNC) != 0) {
        poolMode = Some("ASYNC")
      } else if ((rmmMode & RmmAllocationMode.ARENA) != 0) {
        poolMode = Some("ARENA")
      }
      poolMode.foreach(poolType =>
        logWarning(s"$poolType pool is expected, but not supported by Rapids Python, " +
          s"fallback to normal pool")
      )
      // Clear the uvm flag which is not relevant to memory pool
      (rmmMode & (~RmmAllocationMode.CUDA_MANAGED_MEMORY)) != RmmAllocationMode.CUDA_DEFAULT
    }
  private lazy val isPythonUvmEnabled = rapidsConf.get(PYTHON_UVM_ENABLED)
    .getOrElse(rapidsConf.isUvmEnabled)
    .toString
  private lazy val (initAllocPerWorker, maxAllocPerWorker) = {
    val info = Cuda.memGetInfo()
    val maxFactionTotal = rapidsConf.get(PYTHON_RMM_MAX_ALLOC_FRACTION)
    val maxAllocTotal = (maxFactionTotal * info.total).toLong
    // Initialize pool size for all pythons workers. If the fraction is not set,
    // use half of the free memory as default.
    val initAllocTotal = rapidsConf.get(PYTHON_RMM_ALLOC_FRACTION)
      .map { fraction =>
        if (0 < maxFactionTotal && maxFactionTotal < fraction) {
          throw new IllegalArgumentException(s"The value of '$PYTHON_RMM_MAX_ALLOC_FRACTION' " +
            s"should not be less than that of '$PYTHON_RMM_ALLOC_FRACTION', but found " +
            s"$maxFactionTotal < $fraction")
        }
        (fraction * info.total).toLong
      }
      .getOrElse((0.5 * info.free).toLong)
    if (initAllocTotal > info.free) {
      logWarning(s"Initial RMM allocation(${initAllocTotal / 1024.0 / 1024} MB) for " +
        s"all the Python workers is larger than free memory(${info.free / 1024.0 / 1024} MB)")
    } else {
      logDebug(s"Configure ${initAllocTotal / 1024.0 / 1024}MB GPU memory for " +
        s"all the Python workers.")
    }

    // Calculate the pool size for each Python worker.
    val concurrentPythonWorkers = rapidsConf.get(CONCURRENT_PYTHON_WORKERS)
    if (0 < concurrentPythonWorkers) {
      (initAllocTotal / concurrentPythonWorkers, maxAllocTotal / concurrentPythonWorkers)
    } else {
      // When semaphore is disabled or invalid, use the number of cpu task slots instead.
      // Spark does not throw exception even the value of CPUS_PER_TASK is negative, so
      // return 1 if it is less than zero to continue the task.
      val cpuTaskSlots = sparkConf.get(EXECUTOR_CORES) / Math.max(1, sparkConf.get(CPUS_PER_TASK))
      (initAllocTotal / cpuTaskSlots, maxAllocTotal / cpuTaskSlots)
    }
  }

  def isPythonOnGpuEnabled(sqlConf: SQLConf, name: String = "spark"): Boolean = {
    val pythonEnabled = new RapidsConf(sqlConf).get(PYTHON_GPU_ENABLED)
    if (pythonEnabled) {
      checkPythonConfigs(sparkConf, name)
    }
    pythonEnabled
  }

  // Called in each task at the executor side
  def injectGpuInfo(funcs: Seq[(ChainedPythonFunctions, Long)],
      isPythonOnGpuEnabled: Boolean): Unit = {
    // Insert GPU related env(s) into `envVars` for all the PythonFunction(s).
    // Yes `PythonRunner` will only use the first one, but just make sure it will
    // take effect no matter the order changes or not.
    funcs.foreach(_._1.funcs.foreach { pyF =>
      pyF.envVars.put("CUDA_VISIBLE_DEVICES", gpuId)
      pyF.envVars.put("RAPIDS_PYTHON_ENABLED", isPythonOnGpuEnabled.toString)
      pyF.envVars.put("RAPIDS_UVM_ENABLED", isPythonUvmEnabled)
      pyF.envVars.put("RAPIDS_POOLED_MEM_ENABLED", isPythonPooledMemEnabled.toString)
      pyF.envVars.put("RAPIDS_POOLED_MEM_SIZE", initAllocPerWorker.toString)
      pyF.envVars.put("RAPIDS_POOLED_MEM_MAX_SIZE", maxAllocPerWorker.toString)
    })
  }

  // Not sure if need separate worker for databricks, will check it later.
  private val mapDefaultPythonModules = Map(
    ("spark", ("rapids.daemon", "rapids.worker")),
    ("databricks", ("rapids.daemon_databricks", "rapids.worker"))
  )

  // Check the related conf(s) to launch our rapids daemon or worker for
  // the GPU initialization when python on gpu enabled.
  // - python worker module if useDaemon is false, otherwise
  // - python daemon module.
  private[sql] def checkPythonConfigs(conf: SparkConf, name: String): Unit = synchronized {
    val (daemonModule, workerModule) = mapDefaultPythonModules(name)
    val allPythonModules = mapDefaultPythonModules.values
    val useDaemon = {
      val useDaemonEnabled = conf.get(PYTHON_USE_DAEMON)
      // This flag is ignored on Windows as it's unable to fork.
      !System.getProperty("os.name").startsWith("Windows") && useDaemonEnabled
    }
    if (useDaemon) {
      val oDaemon = conf.get(PYTHON_DAEMON_MODULE)
      if (oDaemon.nonEmpty) {
        val daemon = oDaemon.get
        val isAllowedDaemon = allPythonModules.exists(v => v._1 == daemon)
        if (!isAllowedDaemon) {
          throw new IllegalArgumentException("Python daemon module config conflicts." +
            s" Expect one of [${allPythonModules.map(v => v._1).toSet.mkString(", ")}]," +
            s" but found $daemon")
        }
      } else {
        // Set daemon only when not specified
        conf.set(PYTHON_DAEMON_MODULE, daemonModule)
      }
    } else {
      val oWorker = conf.get(PYTHON_WORKER_MODULE)
      if (oWorker.nonEmpty) {
        val worker = oWorker.get
        val isAllowedWorker = allPythonModules.exists(v => v._2 == worker)
        if (!isAllowedWorker) {
          throw new IllegalArgumentException("Python worker module config conflicts." +
            s" Expect one of (${allPythonModules.map(v => v._2).toSet.mkString(", ")})," +
            s" but found $worker")
        }
      } else {
        // Set worker only when not specified
        conf.set(PYTHON_WORKER_MODULE, workerModule)
      }
    }
  }
}
