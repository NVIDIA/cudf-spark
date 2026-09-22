/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.lore

import org.apache.hadoop.fs.Path

case class OutputLoreId(loreId: LoreId, partitionIds: Set[Int]) {
  def outputAllParitions: Boolean = partitionIds.isEmpty

  def shouldOutputPartition(partitionId: Int): Boolean = outputAllParitions ||
    partitionIds.contains(partitionId)
}

case class LoreOutputInfo(outputLoreId: OutputLoreId, pathStr: String) {
  def path: Path = new Path(pathStr)
}

object OutputLoreId {
  private val PARTITION_ID_RANGE_REGEX = raw"(\d+)-(\d+)".r("start", "end")
  private val PARTITION_ID_REGEX = raw"(\d+)".r("partitionId")
  private val PARTITION_ID_SEP_REGEX = raw" +".r

  private val OUTPUT_LORE_ID_SEP_REGEX = ", *".r
  private val OUTPUT_LORE_ID_REGEX =
    raw"(?<loreId>\d+)(\[(?<partitionIds>.*)\])?".r

  def apply(loreId: Int): OutputLoreId = OutputLoreId(loreId, Set.empty)

  def apply(inputStr: String): OutputLoreId = {
    OUTPUT_LORE_ID_REGEX.findFirstMatchIn(inputStr).map { m =>
      val loreId = m.group("loreId").toInt
      val partitionIds: Set[Int] = m.group("partitionIds") match {
        case partitionIdsStr if partitionIdsStr != null =>
          PARTITION_ID_SEP_REGEX.split(partitionIdsStr).flatMap {
            case PARTITION_ID_REGEX(partitionId) =>
              Seq(partitionId.toInt)
            case PARTITION_ID_RANGE_REGEX(start, end) =>
              start.toInt until end.toInt
            case "*" => Set.empty
            case partitionIdStr => throw new IllegalArgumentException(s"Invalid partition " +
              s"id: $partitionIdStr")
          }.toSet
        case null => {
          throw new IllegalArgumentException(s"Invalid output lore id string: $inputStr, " +
            s"partition ids not found!")
        }
      }
      OutputLoreId(loreId, partitionIds)
    }.getOrElse(throw new IllegalArgumentException(s"Invalid output lore ids: $inputStr"))
  }

  def parse(inputStr: String): OutputLoreIds = {
    require(inputStr != null, "inputStr should not be null")

    OUTPUT_LORE_ID_SEP_REGEX.split(inputStr).map(OutputLoreId(_)).map { outputLoreId =>
      outputLoreId.loreId -> outputLoreId
    }.toMap
  }
}


