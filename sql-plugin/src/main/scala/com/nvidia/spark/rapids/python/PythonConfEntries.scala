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

package com.nvidia.spark.rapids.python

import com.nvidia.spark.rapids.RapidsConf.{conf, RMM_POOL, UVM_ENABLED}

object PythonConfEntries {

  val PYTHON_GPU_ENABLED = conf("spark.rapids.sql.python.gpu.enabled")
    .doc("This is an experimental feature and is likely to change in the future." +
      " Enable (true) or disable (false) support for scheduling Python Pandas UDFs with" +
      " GPU resources. When enabled, pandas UDFs are assumed to share the same GPU that" +
      " the RAPIDs accelerator uses and will honor the python GPU configs")
    .booleanConf
    .createWithDefault(false)

  val CONCURRENT_PYTHON_WORKERS = conf("spark.rapids.python.concurrentPythonWorkers")
    .doc("Set the number of Python worker processes that can execute concurrently per GPU. " +
      "Python worker processes may temporarily block when the number of concurrent Python " +
      "worker processes started by the same executor exceeds this amount. Allowing too " +
      "many concurrent tasks on the same GPU may lead to GPU out of memory errors. " +
      ">0 means enabled, while <=0 means unlimited")
    .integerConf
    .createWithDefault(0)

  val PYTHON_RMM_ALLOC_FRACTION = conf("spark.rapids.python.memory.gpu.allocFraction")
    .doc("The fraction of total GPU memory that should be initially allocated " +
      "for pooled memory for all the Python workers. It supposes to be less than " +
      "(1 - $(spark.rapids.memory.gpu.allocFraction)), since the executor will share the " +
      "GPU with its owning Python workers. Half of the rest will be used if not specified")
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value for Python workers must be in [0, 1].")
    .createOptional

  val PYTHON_RMM_MAX_ALLOC_FRACTION = conf("spark.rapids.python.memory.gpu.maxAllocFraction")
    .doc("The fraction of total GPU memory that limits the maximum size of the RMM pool " +
      "for all the Python workers. It supposes to be less than " +
      "(1 - $(spark.rapids.memory.gpu.maxAllocFraction)), since the executor will share the " +
      "GPU with its owning Python workers. when setting to 0 it means no limit.")
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The value of maxAllocFraction for Python workers must be" +
      " in [0, 1].")
    .createWithDefault(0.0)

  val PYTHON_POOLED_MEM = conf("spark.rapids.python.memory.gpu.pooling.enabled")
    .doc("Should RMM in Python workers act as a pooling allocator for GPU memory, or" +
      " should it just pass through to CUDA memory allocation directly. When not specified," +
      s" It will honor the value of config '${RMM_POOL.key}', but now only 'DEFAULT' and" +
      " 'NONE' are supported. If 'ASYNC' or 'ARENA' is specified, it will fall back to" +
      " 'DEFAULT'.")
    .booleanConf
    .createOptional

  val PYTHON_UVM_ENABLED = conf("spark.rapids.python.memory.uvm.enabled")
    .doc(s"Similar with '${UVM_ENABLED.key}', but this conf is for" +
      s" python workers. When not specified, it will honor the value of config" +
      s" '${UVM_ENABLED.key}'. This is an experimental feature.")
    .internal()
    .booleanConf
    .createOptional

  // An empty function called by RapidsConf to initialize the config definitions above for
  // doc generation
  def init(): Unit = {}
}
