/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.io

import org.apache.iceberg.MetadataColumns.{DELETE_FILE_PATH, DELETE_FILE_POS}

private[io] object GpuPositionDeleteFieldIds {
  val FILE_AND_POS_FIELD_IDS: Set[Integer] = Set(
    DELETE_FILE_PATH.fieldId(), DELETE_FILE_POS.fieldId())
}
