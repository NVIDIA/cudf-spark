/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A trait for classes that are capable of collecting statistics on columnar data that's being
 * processed by a single write task in [[GpuFileFormatDataWriter]] - i.e. there should be one
 * instance per executor.
 *
 * newPartition event is only triggered if the relation to be written out is partitioned.
 */
trait ColumnarWriteTaskStatsTracker {

  /**
   * Process the fact that a new partition is about to be written.
   * Only triggered when the relation is partitioned by a (non-empty) sequence of columns.
   * NOTE: The partition values are stubbed for now as the original code only updated a
   *       count of partitions without examining the values.
   * //@param partitionValues The values that define this new partition.
   */
  def newPartition(partitionValues: InternalRow): Unit

  /**
   * Process the fact that a new file is about to be written.
   * @param filePath Path of the file into which future rows will be written.
   */
  def newFile(filePath: String): Unit

  /**
   * Process the fact that a file is finished to be written and closed.
   * @param filePath Path of the file.
   */
  def closeFile(filePath: String): Unit

  /**
   * Process a new column batch to update the tracked statistics accordingly.
   * The batch will be written to the most recently witnessed file (via `newFile`).
   * @note Call this function only once per `batch` to be written. If the batch is going to be
   *       split later because of a retry, that is OK, but don't call newBatch again with the
   *       splitted out parts.
   * @param filePath Path of the file which the batch is written to.
   * @param batch Current data batch to be processed.
   */
  def newBatch(filePath: String, batch: ColumnarBatch): Unit

  /**
   * Process the fact that how many writers are currently opened.
   * @param numWriters the current number of open writers
   */
  def writersNumber(numWriters: Int): Unit

  /**
   * Returns the final statistics computed so far.
   * @param taskCommitTime Time of committing the task.
   * @note This may only be called once. Further use of the object may lead to undefined behavior.
   * @return An object of subtype of `org.apache.spark.sql.execution.datasources.WriteTaskStats`,
   *         to be sent to the driver.
   */
  def getFinalStats(taskCommitTime: Long): WriteTaskStats
}


/**
 * A class implementing this trait is basically a collection of parameters that are necessary
 * for instantiating a (derived type of) [[ColumnarWriteTaskStatsTracker]] on all executors and then
 * process the statistics produced by them (e.g. save them to memory/disk, issue warnings, etc).
 * It is therefore important that such an objects is `Serializable`, as it will be sent
 * from the driver to all executors.
 */
trait ColumnarWriteJobStatsTracker extends Serializable {

  /**
   * Instantiates a [[ColumnarWriteTaskStatsTracker]], based on (non-transient) members of this
   * class.
   * To be called by executors.
   * @return A [[ColumnarWriteTaskStatsTracker]] instance to be used for computing stats
   *         during a write task
   */
  def newTaskInstance(): ColumnarWriteTaskStatsTracker

  /**
   * Process the given collection of stats computed during this job.
   * E.g. aggregate them, write them to memory / disk, issue warnings, whatever.
   * @param stats One `WriteTaskStats` object from each successful write task.
   * @param jobCommitTime Time of committing the job.
   */
  def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit
}
