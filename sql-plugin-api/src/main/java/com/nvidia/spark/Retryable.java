/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark;

/**
 * An interface that can be used to retry the processing on non-deterministic
 * expressions on the GPU.
 *
 * GPU memory is a limited resource. When it runs out the RAPIDS Accelerator
 * for Apache Spark will use several different strategies to try and free more
 * GPU memory to let the query complete.
 * One of these strategies is to roll back the processioning for one task, pause
 * the task thread, then retry the task when more memory is available. This
 * works transparently for any stateless deterministic processing. But technically
 * an expression/UDF can be non-deterministic and/or keep state in between calls.
 * This interface provides a checkpoint method to save any needed state, and a
 * restore method to reset the state in the case of a retry.
 *
 * Please note that a retry is not isolated to a single expression, so a restore can
 * be called even after the expression returned one or more batches of results. And
 * each time checkpoint it called any previously saved state can be overwritten.
 */
public interface Retryable {
  /**
   * Save the state, so it can be restored in the case of a retry.
   * (This is called inside a Spark task context on executors.)
   */
  void checkpoint();

  /**
   * Restore the state that was saved by calling to "checkpoint".
   * (This is called inside a Spark task context on executors.)
   */
  void restore();
}
