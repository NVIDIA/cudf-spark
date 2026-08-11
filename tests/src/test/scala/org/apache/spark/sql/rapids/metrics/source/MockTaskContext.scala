/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids.metrics.source

import java.io.Closeable
import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{AccumulatorV2, TaskCompletionListener, TaskFailureListener}

abstract class MockTaskContextBase(taskAttemptId: Long, partitionId: Int) extends TaskContext {

  val listeners = new ListBuffer[TaskCompletionListener]

  override def isCompleted(): Boolean = false

  override def isInterrupted(): Boolean = false

  override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
    listeners += listener
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): TaskContext = this

  override def stageId(): Int = 1

  override def stageAttemptNumber(): Int = 1

  override def partitionId(): Int = partitionId

  override def attemptNumber(): Int = 1

  override def taskAttemptId(): Long = taskAttemptId

  override def getLocalProperty(key: String): String = null

  override def resources(): Map[String, ResourceInformation] = Map()

  override def resourcesJMap(): util.Map[String, ResourceInformation] = resources().asJava

  override def taskMetrics(): TaskMetrics = new TaskMetrics

  override def getMetricsSources(sourceName: String): Seq[Source] = Seq.empty

  override private[spark] def killTaskIfInterrupted(): Unit = {}

  override def getKillReason() = None

  override def taskMemoryManager() = null

  override private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {}

  override private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit = {}

  override private[spark] def markInterrupted(reason: String): Unit = {}

  override private[spark] def markTaskFailed(error: Throwable): Unit = {}

  override private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = {}

  override private[spark] def fetchFailed = None

  override private[spark] def getLocalProperties = new Properties()

  def cpus(): Int = 2

  def numPartitions(): Int = 2

  def taskLocality(): TaskLocality.TaskLocality = TaskLocality.ANY

  /**
   * This is exposed to invoke the listeners onTaskCompletion
   */
  def markTaskComplete(): Unit = {
    listeners.foreach(_.onTaskCompletion(this))
  }

  /**
   * This method was introduced in Spark-3.5.1. It's not shimmed and added to the common class by
   * removing the override keyword.
   */
  def isFailed(): Boolean = false

  /**
   * These below methods were introduced in Spark-4. It's not shimmed and added to the common class
   * removing the override keyword.
   */

  private[spark] def interruptible(): Boolean = false

  private[spark] def pendingInterrupt(threadToInterrupt: Option[Thread], reason: String): Unit = {}

  private[spark] def createResourceUninterruptibly[T <: Closeable](
      resourceBuilder: => T): T = resourceBuilder

}
