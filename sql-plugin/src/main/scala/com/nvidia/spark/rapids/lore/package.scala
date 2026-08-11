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

package com.nvidia.spark.rapids

/**
 * Lore framework is used for dumping input data of a gpu executor to disk so that it can be
 * replayed in local environment for performance analysis.
 * <br>
 * When [[RapidsConf.TAG_LORE_ID_ENABLED]] is set, during the planning phase we will tag a lore
 * id to each gpu operator. Lore id is guaranteed to be unique within a query, and it's supposed
 * to be same for operators with same plan.
 * <br>
 * When [[RapidsConf.LORE_DUMP_IDS]] is set, during the execution phase we will dump the input
 * data of gpu operators with lore id to disk. The dumped data can be replayed in local
 * environment. The dumped data will reside in [[RapidsConf.LORE_DUMP_PATH]]. For more details,
 * please refer to `docs/dev/lore.md`.
 */
package object lore {
  type LoreId = Int
  type OutputLoreIds = Map[LoreId, OutputLoreId]
}
