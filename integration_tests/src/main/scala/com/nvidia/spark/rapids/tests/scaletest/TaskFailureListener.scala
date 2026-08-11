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
package com.nvidia.spark.rapids.tests.scaletest

import scala.collection.mutable.ListBuffer

import org.apache.spark.{Success, TaskEndReason}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

/**
 * This Listener Class is used to track the "retry tasks" in Spark jobs.
 * It is possible that a job finally finished successfully with tasks failed at first but succeeded
 * after retry while such exceptions cannot be caught by try-catch around a query execution.
 * The onTaskEnd event carries task failure reasons that we will record in the test report.
 */
class TaskFailureListener extends SparkListener {
  val taskFailures = new ListBuffer[TaskEndReason]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    taskEnd.reason match {
      case Success =>
      case reason => taskFailures += reason
    }
    super.onTaskEnd(taskEnd)
  }
}
